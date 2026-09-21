r"""R61 - the research apparatus calibration and the four repairs it exposed.

Hermetic unless marked. Every test runs against a research memory inside
``tmp_path``, a temporary worker registry, or pure arithmetic. The few that
need the owned substrates are marked ``owned_data`` and SKIP when those stores
are absent.

What is proven here, workstream by workstream:

    A cost budget     the canonical arithmetic and its UNITS; the same
                      turnover at different costs and different horizons
                      produces different drags; R60's three halts reproduce
                      exactly; the ceiling is derived from a frozen constant
                      and not from any observed return; an uncomputable budget
                      FAILS rather than passing on a cost of zero; the retired
                      scalar survives as a diagnostic and gates nothing
    B power           the injection achieves the IC it declares; the noise is
                      keyed by DATE so a stage prefix scores the same book the
                      full run scores; detection is the FULL governed path and
                      a discovery-only success is not one; the effect grid and
                      the detection rule are frozen before results
    C identity        thresholds are pre-registered; a PASS needs every check
                      MEASURED; an unmeasurable check yields INCONCLUSIVE and
                      never a soft pass; the date arithmetic that produced a
                      100% false rate is pinned
    D drawdown        a non-monotonic stream can NEVER report zero; both
                      layer-statistics owners emit the canonical concepts;
                      identical input gives identical drawdown on every
                      consuming surface; an ABSENT drawdown is typed, never
                      rendered as 0.0; the risk agent receives the canonical
                      concept
    E halts           one canonical record; NO_ALPHA_EVIDENCE is unreachable
                      from a cell that measured no return; idempotent; refused
                      once a layer has been revealed; the burden is charged
    F assignments     R60's real over-stuffed assignments are detected and
                      split; model routing is UNCHANGED
    G workers         one row per worker; a dead row reads INTERRUPTED, never
                      RUNNING; pid REUSE cannot resurrect a worker; the
                      operator surface has no kill verb
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import books as B
from paper_trader.alpha_agent.agents_v2 import contracts as C
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.agents_v2 import routing as RT
from paper_trader.alpha_agent.r57 import engine as K
from paper_trader.alpha_agent.r59 import engines as E
from paper_trader.alpha_agent.r59 import memory as M
from paper_trader.alpha_agent.r59 import native
from paper_trader.alpha_agent.r61 import assignments as AS
from paper_trader.alpha_agent.r61 import cost_budget as CB
from paper_trader.alpha_agent.r61 import drawdown as DD
from paper_trader.alpha_agent.r61 import halts as HALT
from paper_trader.alpha_agent.r61 import identity_audit as IA
from paper_trader.alpha_agent.r61 import power as PW
from paper_trader.alpha_agent.r61 import workers as W

REPO = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.filterwarnings(
    "ignore:All-NaN slice encountered:RuntimeWarning")

owned_data = pytest.mark.skipif(
    not (native.NATIVE_LAYER_DIR.exists() and native.LAYER_MANIFEST.exists()
         and r59.EQUITY_PANEL.exists()),
    reason="the owned research substrates are not present on this machine")


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def mem(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    return M.ResearchMemory(tmp_path / "research_memory.sqlite")


@pytest.fixture()
def pipe(mem, tmp_path):
    return P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


# =========================================================================== #
# WORKSTREAM A - the annualised cost budget
# =========================================================================== #
def test_a_units_are_exactly_as_documented():
    """one_way_turnover x 2 x cost_per_side x rebalances_per_year."""
    drag = CB.annualized_rebalance_cost_drag(
        one_way_turnover=0.5, cost_per_side=0.001, rebalances_per_year=10.0)
    assert drag == pytest.approx(0.5 * 2 * 0.001 * 10.0)
    assert CB.rebalances_per_year(21) == pytest.approx(12.0)
    assert CB.rebalances_per_year(5) == pytest.approx(50.4)
    assert CB.TRADING_SESSIONS_PER_YEAR == 252.0


def test_a_same_turnover_different_cost_is_a_different_burden():
    """THE R60 DEFECT. 0.60 one-way is not one economic fact."""
    eq = CB.evaluate_cost_budget(one_way_turnover=0.60, cost_per_side=0.0025,
                                 rebalance_interval_sessions=21)
    fut = CB.evaluate_cost_budget(one_way_turnover=0.60, cost_per_side=0.0005,
                                  rebalance_interval_sessions=21)
    assert eq["measured"] == pytest.approx(5.0 * fut["measured"])
    assert eq["measured"] != fut["measured"]
    # and the retired scalar could not tell them apart at all
    assert (eq["diagnostic_retired_gate_would_halt"]
            == fut["diagnostic_retired_gate_would_halt"] is True)


def test_a_same_turnover_different_horizon_is_a_different_burden():
    monthly = CB.evaluate_cost_budget(one_way_turnover=0.60, cost_per_side=0.0025,
                                      rebalance_interval_sessions=21)
    weekly = CB.evaluate_cost_budget(one_way_turnover=0.60, cost_per_side=0.0025,
                                     rebalance_interval_sessions=5)
    assert weekly["measured"] == pytest.approx(monthly["measured"] * 21 / 5)
    assert weekly["measured"] > monthly["measured"]


def test_a_the_three_r60_halts_reproduce_their_published_arithmetic():
    """R60's own numbers, to the reported precision, and the verdict flips
    exactly where the director said it should."""
    h5 = CB.evaluate_cost_budget(one_way_turnover=0.870, cost_per_side=0.0025,
                                 rebalance_interval_sessions=5, label="r60_08")
    commodity = CB.evaluate_cost_budget(one_way_turnover=0.598, cost_per_side=0.0005,
                                        rebalance_interval_sessions=21, label="r60_05")
    index = CB.evaluate_cost_budget(one_way_turnover=0.612, cost_per_side=0.0005,
                                    rebalance_interval_sessions=21, label="r60_07")
    assert h5["measured"] == pytest.approx(0.219, abs=5e-4)
    assert commodity["measured"] == pytest.approx(0.0072, abs=5e-5)
    assert index["measured"] == pytest.approx(0.0073, abs=5e-5)
    # The equity cell was correctly killed; the two futures cells were not.
    assert h5["state"] == CB.BUDGET_HALT
    assert commodity["state"] == CB.BUDGET_PASS
    assert index["state"] == CB.BUDGET_PASS
    # ... and all three were killed by the retired scalar.
    for r in (h5, commodity, index):
        assert r["diagnostic_retired_gate_would_halt"] is True


def test_a_ceiling_is_derived_from_a_frozen_constant_not_an_observed_return():
    assert CB.COST_BUDGET_CEILING == pytest.approx(
        CB.COST_BUDGET_CEILING_MULTIPLE * r59.GATE_MATERIALITY)
    assert CB.COST_BUDGET_CEILING == pytest.approx(0.03)
    # The old scalar's own world lands INSIDE the new ceiling, so the two
    # gates agree where the old one was calibrated.
    old_world = CB.evaluate_cost_budget(one_way_turnover=CB.RETIRED_TURNOVER_CEILING,
                                        cost_per_side=0.0025,
                                        rebalance_interval_sessions=21)
    assert old_world["measured"] == pytest.approx(0.024)
    assert old_world["passed"] is True


def test_a_no_post_result_threshold_tuning_is_possible():
    """The ceiling is a module constant tied to a frozen floor; nothing in the
    evaluation path can move it, and the source carries no return anywhere in
    its derivation."""
    src = (REPO / "alpha_agent" / "r61" / "cost_budget.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)
    assigns = {t.id for n in ast.walk(tree)
               if isinstance(n, ast.Assign)
               for t in n.targets if isinstance(t, ast.Name)}
    assert "COST_BUDGET_CEILING" in assigns
    # the only function that may set a ceiling takes it as a DEFAULT argument
    for fn in ("evaluate_cost_budget", "evaluate_frozen_spec"):
        obj = getattr(CB, fn)
        assert "ceiling" in obj.__code__.co_varnames


def test_a_an_uncomputable_budget_fails_closed():
    """A per-instrument cost vector states no scalar rate. Coercing that to
    0.0 would wave every futures cell through on a cost of nothing."""
    model = {"rate_per_side": "per market, 2-15 bp"}
    assert CB.cost_per_side_from_model(model) is None
    spec = {"cost_model": model, "parameters": {"cadence_sessions": 21}}
    out = CB.evaluate_frozen_spec(spec, one_way_turnover=0.6)
    assert out["state"] == CB.BUDGET_NOT_EVALUABLE
    assert out["passed"] is False
    assert any("scalar rate_per_side" in r for r in out["reasons"])
    # supplying the measured effective rate makes it evaluable
    ok = CB.evaluate_frozen_spec(spec, one_way_turnover=0.6,
                                 cost_per_side=0.0005)
    assert ok["state"] == CB.BUDGET_PASS
    assert ok["cost_per_side_source"] == "CALLER_MEASURED_EFFECTIVE"


def test_a_roll_cost_is_carried_not_dropped():
    without = CB.evaluate_cost_budget(one_way_turnover=0.6, cost_per_side=0.0005,
                                      rebalance_interval_sessions=21)
    with_roll = CB.evaluate_cost_budget(one_way_turnover=0.6, cost_per_side=0.0005,
                                        rebalance_interval_sessions=21,
                                        additional_ann_cost_drag=0.011)
    assert with_roll["measured"] == pytest.approx(without["measured"] + 0.011)
    assert with_roll["annualized_roll_or_other_cost_drag"] == 0.011


def test_a_hold_sessions_beats_cadence_for_a_tranche_book():
    spec = {"cost_model": {"rate_per_side": 0.0025},
            "parameters": {"cadence_sessions": 21, "hold_sessions": 5}}
    assert CB.rebalance_interval_from_spec(spec) == 5.0
    out = CB.evaluate_frozen_spec(spec, one_way_turnover=0.870)
    assert out["measured"] == pytest.approx(0.219, abs=5e-4)


def test_a_the_gate_schema_carries_the_new_ceiling_and_retires_the_scalar():
    th = C.Contracts().gate_schema()["canonical_statistical_gate"][
        "inherited_thresholds"]
    assert th["max_annualized_cost_drag"] == pytest.approx(
        CB.COST_BUDGET_CEILING)
    assert th["max_annualized_cost_drag_owner"] == CB.COST_BUDGET_OWNER
    assert "RETIRED" in th["max_turnover_per_decision_one_side_status"]
    ids = [c["id"] for c in C.Contracts().gate_schema()["adversarial_checks"][
        "machine_checked"]]
    assert "cost_budget_within_ceiling" in ids
    assert "turnover_within_ceiling" not in ids


def test_a_the_skeptic_machine_check_is_the_cost_budget():
    src = (REPO / "alpha_agent" / "agents_v2" / "pipeline.py").read_text(
        encoding="utf-8")
    assert "cost_budget_within_ceiling" in src
    assert "\"turnover_within_ceiling\":" not in src


def test_a_effective_cost_per_side_is_notional_weighted():
    traded = np.array([100.0, 300.0, 0.0])
    costs = np.array([0.0002, 0.0010, 0.9])
    got = CB.effective_cost_per_side(traded, costs)
    assert got == pytest.approx((100 * 0.0002 + 300 * 0.0010) / 400.0)
    # nothing traded -> a rate is undefined, and 0.0 would be a lie
    assert CB.effective_cost_per_side(np.zeros(3), costs) is None


# =========================================================================== #
# WORKSTREAM B - power calibration
# =========================================================================== #
def test_b_the_design_is_frozen_and_content_hashed():
    pr = PW.pre_registration()
    assert pr["is_alpha_experiment"] is False
    assert pr["registers_hypothesis"] is False
    assert pr["charges_search_burden"] is False
    assert pr["consumes_lockbox_budget"] is False
    assert pr["creates_forward_request"] is False
    assert pr["effect_grid_frozen_before_results"] is True
    assert pr["effect_grid"][0] == 0.0, "the null must be on the grid"
    assert len(pr["effect_grid"]) == len(set(pr["effect_grid"]))
    assert pr["effect_grid"] == sorted(pr["effect_grid"])
    assert pr["pre_registration_hash"]


def test_b_normal_scores_are_standardised_and_respect_the_mask():
    v = np.array([5.0, 1.0, 3.0, np.nan, 2.0, 100.0])
    mask = np.array([True, True, True, True, True, False])
    z = PW.normal_scores(v, mask)
    fin = np.isfinite(z)
    assert fin.tolist() == [True, True, True, False, True, False]
    assert float(z[fin].mean()) == pytest.approx(0.0, abs=1e-9)
    assert float(z[fin].std()) == pytest.approx(1.0, abs=1e-9)
    # monotone in the input, so the rank information is preserved exactly
    assert z[1] < z[4] < z[2] < z[0]


def test_b_detection_is_the_full_governed_path_not_discovery():
    """A cell that halts at D is a MISS however good D looked."""
    strong_L = {"ann_net_excess": 0.10, "t_net_excess": 5.0,
                "p_one_sided": 1e-7, "effective_observations": 60,
                "halves_ann_net_excess": [0.09, 0.11]}
    good_V = {"ann_net_excess": 0.05}
    hit = PW.detect({"L": strong_L, "V": good_V}, burden_denominator=1)
    assert hit["qualified"] is True and hit["strict"] is True
    # the same lockbox with a validation that never earned it
    weak_V = {"ann_net_excess": 0.0001}
    miss = PW.detect({"L": strong_L, "V": weak_V}, burden_denominator=1)
    assert miss["qualified"] is False
    assert "validation_material" in miss["failed_gates"]


def test_b_strict_detection_requires_both_halves_positive():
    L = {"ann_net_excess": 0.10, "t_net_excess": 5.0, "p_one_sided": 1e-7,
         "effective_observations": 60, "halves_ann_net_excess": [0.25, -0.05]}
    d = PW.detect({"L": L, "V": {"ann_net_excess": 0.05}},
                  burden_denominator=1)
    assert d["qualified"] is True
    assert d["strict"] is False


def test_b_burden_moves_detection_in_the_only_safe_direction():
    L = {"ann_net_excess": 0.05, "t_net_excess": 2.2, "p_one_sided": 0.02,
         "effective_observations": 60, "halves_ann_net_excess": [0.04, 0.06]}
    layers = {"L": L, "V": {"ann_net_excess": 0.03}}
    assert PW.detect(layers, burden_denominator=1)["qualified"] is True
    assert PW.detect(layers, burden_denominator=1000)["qualified"] is False


def test_b_mde_interpolates_and_refuses_to_extrapolate():
    curve = [{"rho": 0.0, "detection_rate": 0.0,
              "median_lockbox_ann_net_excess": -0.01,
              "median_lockbox_ann_gross_excess": 0.0},
             {"rho": 0.05, "detection_rate": 0.5,
              "median_lockbox_ann_net_excess": 0.04,
              "median_lockbox_ann_gross_excess": 0.06},
             {"rho": 0.10, "detection_rate": 1.0,
              "median_lockbox_ann_net_excess": 0.10,
              "median_lockbox_ann_gross_excess": 0.13}]
    m80 = PW.mde(curve, power=0.80)
    assert m80["reached"] is True
    assert 0.05 < m80["mde_rho"] < 0.10
    assert m80["mde_ann_net_excess"] is not None
    flat = [{"rho": r, "detection_rate": 0.1,
             "median_lockbox_ann_net_excess": 0.0,
             "median_lockbox_ann_gross_excess": 0.0}
            for r in (0.0, 0.05, 0.10)]
    m = PW.mde(flat, power=0.80)
    assert m["reached"] is False and m["mde_rho"] is None
    assert "not extrapolated" in m["note"]


def test_b_wilson_interval_is_honest_at_the_boundaries():
    at_zero = PW.wilson_interval(0, 120)
    at_one = PW.wilson_interval(120, 120)
    assert at_zero["point"] == 0.0 and at_zero["hi"] > 0.0
    assert at_one["point"] == 1.0 and at_one["lo"] < 1.0
    assert PW.wilson_interval(0, 0)["point"] is None


@owned_data
def test_b_the_injection_achieves_the_ic_it_declares():
    """The calibration's own treatment is MEASURED, not asserted."""
    from paper_trader.alpha_agent.r61 import calibration_panels as CP
    spec = CP.load(CP.COMMODITY_FUTURES)
    pre = PW.precompute(spec)
    eps = PW.noise_matrix(spec, pre, 7)
    for rho in (0.0, 0.10):
        got = PW.realised_ic(spec, pre, eps, rho)
        assert got["n_decisions_scored"] > 100
        assert got["mean_rank_ic"] == pytest.approx(rho, abs=0.03)


@owned_data
def test_b_the_noise_is_keyed_by_date_so_a_stage_prefix_is_reproducible():
    """Without this a D-stage run would score a different book from the one
    the L-stage run scores over the same dates, and the partition would leak
    noise."""
    from paper_trader.alpha_agent.r61 import calibration_panels as CP
    spec = CP.load(CP.COMMODITY_FUTURES)
    pre = PW.precompute(spec)
    a = PW.noise_matrix(spec, pre, 3)
    b = PW.noise_matrix(spec, pre, 3)
    assert np.array_equal(a, b)
    assert not np.array_equal(a, PW.noise_matrix(spec, pre, 4))


@owned_data
def test_b_precomputed_eligibility_is_the_books_own_rule():
    """The speed-up must change no number: the materialised mask has to equal
    what the eligibility rule returns, decision for decision."""
    from paper_trader.alpha_agent.r61 import calibration_panels as CP
    spec = CP.load(CP.US_LARGE_CAP)
    pre = PW.precompute(spec)
    for t in [int(x) for x in pre["decision_idx"][:25]]:
        assert np.array_equal(pre["eligible_full"][:, t],
                              K.eligibility(spec["panel"], t))


@owned_data
def test_b_r61_live_market_rule_equals_the_r60_rule_it_restates():
    """``calibration_panels.base_live`` re-derives R60's private rule rather
    than importing it. This is the evidence that the restatement is exact."""
    import sys
    root = REPO / "research" / "agents"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from campaign_r60_information_frontier import executors as EX
    from paper_trader.alpha_agent.r61 import calibration_panels as CP

    spec = CP.load(CP.COMMODITY_FUTURES)
    layer = spec["panel"]
    idx = PW.decision_indices(spec)
    for t in [int(x) for x in idx[::20]]:
        live = B.live_markets(layer, t)
        assert np.array_equal(CP.base_live(layer, t, live),
                              EX._base_live(layer, t, live))


@owned_data
def test_b_a_measured_cell_is_a_real_governed_run():
    from paper_trader.alpha_agent.r61 import calibration_panels as CP
    spec = CP.load(CP.COMMODITY_FUTURES)
    pre = PW.precompute(spec)
    null = PW.run_cell(spec, pre, rho=0.0, seed=1)
    strong = PW.run_cell(spec, pre, rho=0.20, seed=1)
    assert null["detected"] is False
    assert strong["detected"] is True and strong["state"] == "MEASURED"
    # a halted cell NEVER computes a lockbox, exactly as the runner would not
    if null["state"] == "HALTED":
        assert null["lockbox_computed"] is False
        assert "L" not in null["layers"]


# =========================================================================== #
# WORKSTREAM C - the identity bridge audit
# =========================================================================== #
def test_c_thresholds_are_pre_registered_and_hashed():
    pr = IA.pre_registration()
    assert pr["thresholds_frozen_before_measurement"] is True
    assert pr["is_alpha_experiment"] is False
    assert set(pr["thresholds"]) == set(IA.THRESHOLDS)
    assert pr["pre_registration_hash"]


def test_c_pass_requires_every_check_measured():
    measured = {
        "name_channel_false_match": {"measurable": True, "rate": 0.001},
        "temporal_plausibility": {"measurable": True, "rate": 0.01},
        "ambiguous_keys": {"measurable": True, "share": 0.01},
        "extreme_rank_contamination": {"measurable": True,
                                       "concentration_multiple": 1.0},
        "delisting_returns": {"measurable": True, "coverage": 1.0},
        "data_quality": {"measurable": True, "sign_error_rate": 0.0,
                         "reverting_spike_rate": 0.0},
        "attrition_correlation": {"measurable": True, "gap": 0.0},
    }
    assert IA.adjudicate(measured)["verdict"] == IA.PASS


def test_c_an_unmeasurable_check_is_never_a_soft_pass():
    measured = {
        "name_channel_false_match": {"measurable": False, "reason": "no data"},
        "temporal_plausibility": {"measurable": True, "rate": 0.01},
        "ambiguous_keys": {"measurable": True, "share": 0.01},
        "extreme_rank_contamination": {"measurable": True,
                                       "concentration_multiple": 1.0},
        "delisting_returns": {"measurable": True, "coverage": 1.0},
        "data_quality": {"measurable": True, "sign_error_rate": 0.0,
                         "reverting_spike_rate": 0.0},
        "attrition_correlation": {"measurable": True, "gap": 0.0},
    }
    out = IA.adjudicate(measured)
    assert out["verdict"] == IA.INCONCLUSIVE
    assert out["substrate_reuse_allowed"] is False
    assert "name_channel_false_match_rate_live" in out["unmeasurable_checks"]


def test_c_one_missed_threshold_fails_the_whole_audit():
    measured = {
        "name_channel_false_match": {"measurable": True, "rate": 0.50},
        "temporal_plausibility": {"measurable": True, "rate": 0.01},
        "ambiguous_keys": {"measurable": True, "share": 0.01},
        "extreme_rank_contamination": {"measurable": True,
                                       "concentration_multiple": 1.0},
        "delisting_returns": {"measurable": True, "coverage": 1.0},
        "data_quality": {"measurable": True, "sign_error_rate": 0.0,
                         "reverting_spike_rate": 0.0},
        "attrition_correlation": {"measurable": True, "gap": 0.0},
    }
    out = IA.adjudicate(measured)
    assert out["verdict"] == IA.FAIL
    assert out["substrate_reuse_allowed"] is False


def test_c_the_year_overflow_that_faked_a_100_percent_failure_is_pinned():
    """Shifting an open-ended sentinel by a year produced ``10000-12-31``,
    which sorts BEFORE ``2016-...`` in a string comparison and flagged every
    live ticker match as temporally implausible."""
    assert IA._shift_year(IA.OPEN_ENDED, +1) == IA.OPEN_ENDED
    assert len(IA._shift_year(IA.OPEN_ENDED, +1).split("-")[0]) == 4
    assert "2016-06-29" < IA._shift_year(IA.OPEN_ENDED, +1)
    assert IA._shift_year("2011-05-02", -1) == "2010-05-02"


def test_c_an_unknown_end_date_falsifies_nothing():
    """A NULL ``last_filing`` means unknown, not "stopped at first_filing"."""
    rows = [{"symbol": "AAL", "security_name": "American Airlines Group",
             "cik": "1", "first_quoted": "2013-12-09", "last_quoted": None,
             "cik_first_filing": "1993-01-01", "cik_last_filing": None,
             "cik_still_active": 0, "channel": "TICKER", "delisted": False},
            {"symbol": "AGE-200709", "security_name": "Edwards AG",
             "cik": "2", "first_quoted": "1972-03-22",
             "last_quoted": "2007-09-28",
             "cik_first_filing": "2012-03-21",
             "cik_last_filing": "2014-02-14",
             "cik_still_active": 0, "channel": "NAME", "delisted": True}]
    out = IA.measure_temporal_plausibility(rows)
    assert out["n"] == 2
    # the first overlaps fine; the second began filing five years after the
    # security stopped trading and is a PROVEN wrong match
    assert out["implausible"] == 1
    assert out["examples"][0]["cik"] == "2"


# =========================================================================== #
# WORKSTREAM D - drawdown
# =========================================================================== #
@pytest.mark.parametrize("rets,expected", [
    ([-0.30, 0.05, 0.05, 0.05], -0.30),
    ([-0.10, 0.20], -0.10),
    ([0.10, -0.25, 0.05], -0.25),
])
def test_d_a_non_monotonic_stream_can_never_report_zero(rets, expected):
    """THE DEFECT. The retired accumulator started its peak at the first NAV
    point, so the capital at risk never entered the comparison and a loss in
    the first period was invisible: these three all reported 0.0000."""
    got = DD.max_drawdown(np.asarray(rets))
    assert got == pytest.approx(expected, abs=1e-9)
    assert got < 0.0


def test_d_a_rising_stream_reports_a_measured_zero():
    """0.0 measured and None absent are different facts."""
    assert DD.max_drawdown([0.01] * 5) == 0.0
    assert DD.max_drawdown([]) is None


def test_d_absence_is_never_rendered_as_zero():
    absent = DD.read({}, DD.EXCESS_MAX_DRAWDOWN)
    assert absent["state"] == DD.NOT_MEASURED
    assert absent["value"] is None
    assert "not the same as zero" in absent["reason"]
    view = DD.risk_view({})
    assert view["rulable"] is False
    assert view["ruling_value"] is None


def test_d_legacy_keys_still_read_but_as_the_concept_they_hold():
    """``max_dd`` on a futures layer was ALWAYS the excess series."""
    assert DD.read({"max_dd": -0.2}, DD.EXCESS_MAX_DRAWDOWN)["value"] == -0.2
    assert DD.read({"strat_max_dd": -0.3},
                   DD.STRATEGY_MAX_DRAWDOWN)["value"] == -0.3
    # and a legacy key is NEVER silently read as a different concept
    assert DD.read({"max_dd": -0.2},
                   DD.STRATEGY_MAX_DRAWDOWN)["state"] == DD.NOT_MEASURED


def test_d_two_concepts_are_not_merged():
    strat = [0.10, -0.20, 0.05]
    bench = [0.08, -0.18, 0.04]
    got = DD.layer_drawdowns(strategy_returns=strat, benchmark_returns=bench)
    assert got[DD.STRATEGY_MAX_DRAWDOWN] < 0
    assert got[DD.EXCESS_MAX_DRAWDOWN] != got[DD.STRATEGY_MAX_DRAWDOWN]
    # a cash control makes them coincide - by a property of the book
    cash = DD.layer_drawdowns(strategy_returns=strat)
    assert cash["drawdown_control"] == "CASH"
    assert cash[DD.STRATEGY_MAX_DRAWDOWN] == cash[DD.EXCESS_MAX_DRAWDOWN]


def test_d_misaligned_series_are_refused_not_silently_truncated():
    with pytest.raises(DD.DrawdownRefusal):
        DD.layer_drawdowns(strategy_returns=[0.1, 0.2],
                           benchmark_returns=[0.1])


def test_d_surfaces_agree_only_when_both_measured_the_concept():
    a = {DD.STRATEGY_MAX_DRAWDOWN: -0.1791}
    b = {"strat_max_dd": -0.1791}
    assert DD.surfaces_agree(a, b)["agree"] is True
    # the R60 pairing: one surface measured it and one never did
    assert DD.surfaces_agree(a, {})["agree"] is False


def test_d_both_layer_stats_owners_emit_the_canonical_concepts():
    """Identical input, identical drawdown, on every consuming surface."""
    n = 40
    rng = np.random.default_rng(5)
    strat = rng.normal(0.004, 0.05, n)
    bench = rng.normal(0.003, 0.04, n)
    res = {"layers": np.array(["L"] * n), "dates": np.arange(n).astype(str),
           "strat_net": strat, "bench_net": bench,
           "strat_gross": strat + 0.002, "bench_gross": bench + 0.002,
           "turnover_oneway": np.full(n, 0.5), "cadence": 21, "horizon": 21}
    eq = K.layer_stats(res, "L")
    fu = native._layer_stats(res, "L")
    canonical = DD.layer_drawdowns(strategy_returns=strat,
                                   benchmark_returns=bench)
    for stats in (eq, fu):
        for concept in DD.CONCEPTS:
            assert stats[concept] == pytest.approx(canonical[concept])
        assert stats["drawdown_owner"] == DD.DRAWDOWN_OWNER
    # legacy aliases still carry the same numbers
    assert eq["strat_max_dd"] == pytest.approx(eq[DD.STRATEGY_MAX_DRAWDOWN])
    assert fu["max_dd"] == pytest.approx(fu[DD.EXCESS_MAX_DRAWDOWN])
    assert DD.surfaces_agree(eq, fu, concept=DD.STRATEGY_MAX_DRAWDOWN)[
        "agree"] is True


def test_d_no_second_local_drawdown_accumulator_remains_in_the_governed_path():
    """The two layer-statistics owners must DELEGATE, not recompute."""
    for rel in ("alpha_agent/r57/engine.py", "alpha_agent/r59/native.py"):
        src = (REPO / rel).read_text(encoding="utf-8")
        assert "DD.layer_drawdowns" in src, rel
        assert "np.maximum.accumulate(nav)" not in src, rel


def test_d_the_risk_agent_receives_the_canonical_concept():
    src = (REPO / "alpha_agent" / "agents_v2" / "briefs.py").read_text(
        encoding="utf-8")
    assert "DD.risk_view" in src
    assert DD.RISK_RULING_CONCEPT == DD.STRATEGY_MAX_DRAWDOWN


# =========================================================================== #
# WORKSTREAM E - pre-measurement halts
# =========================================================================== #
def test_e_no_alpha_evidence_is_unreachable_from_a_pre_measurement_halt():
    """A cell that computed no return has said NOTHING about alpha. Filing
    silence as evidence of absence is the mislabelling R60 refused."""
    assert r59.HO_NO_ALPHA_EVIDENCE in HALT.FORBIDDEN_OUTCOMES
    for reason in HALT.HALT_REASONS:
        assert HALT.outcome_for(reason) not in HALT.FORBIDDEN_OUTCOMES
    assert set(HALT.HALT_REASON_OUTCOMES.values()) == {
        r59.HO_DATA_HOLD, r59.HO_REJECTED}


def test_e_an_undeclared_halt_reason_is_refused():
    with pytest.raises(HALT.HaltRefusal):
        HALT.outcome_for("BECAUSE_I_SAID_SO")
    with pytest.raises(HALT.HaltRefusal):
        HALT.build(experiment_id="X", halt_reason="COST_BUDGET_EXCEEDED",
                   measured_metric="m", measured_value=1.0,
                   frozen_threshold=0.5,
                   outcome=r59.HO_NO_ALPHA_EVIDENCE)


def test_e_a_halt_without_both_numbers_is_not_auditable():
    for kw in ({"measured_value": None}, {"frozen_threshold": None},
               {"measured_metric": ""}):
        args = dict(experiment_id="X", halt_reason="DATA_COVERAGE_FLOOR",
                    measured_metric="coverage", measured_value=0.7,
                    frozen_threshold=0.8)
        args.update(kw)
        with pytest.raises(HALT.HaltRefusal):
            HALT.build(**args)


def test_e_the_record_carries_every_required_field():
    rec = HALT.build(experiment_id="H_1", halt_reason="COST_BUDGET_EXCEEDED",
                     measured_metric="annualized_cost_drag",
                     measured_value=0.219, frozen_threshold=0.03)
    for field in ("experiment_id", "halt_reason", "measured_metric",
                  "frozen_threshold", "timestamp", "alpha_layer_consumed",
                  "lockbox_consumed", "outcome", "burden_treatment"):
        assert field in rec
    assert rec["alpha_layer_consumed"] == "NONE"
    assert rec["lockbox_consumed"] == "NO"
    assert rec["burden_treatment"] == HALT.BURDEN_CHARGED
    assert rec["outcome"] == r59.HO_REJECTED
    assert rec["record_hash"]


def _preregister(pipe, tmp_path, *, agent="reversal-signal-agent"):
    pipe.perform("data-foundation-agent", "certify_data", dict(
        dataset_id="ds1", asset_classes=[r59.AC_US_EQUITY],
        pit_status=P.PIT_SAFE,
        availability_rule="filed", survivorship="retained"))
    pipe.perform("universe-construction-agent", "define_universe", dict(
        universe_id="u1", dataset_id="ds1", asset_class=r59.AC_US_EQUITY,
        rules="all", execution_representation="LONG_ONLY"))
    pipe.perform("feature-library-agent", "publish_features", dict(
        feature_set_id="fs1", universe_id="u1",
        features=[{"name": "f", "lag": 1, "source": "ds1",
                   "availability_instant": "close"}],
        leakage_check="PASS"))
    return pipe.perform("quant-research-director", "preregister", dict(
        owning_agent=agent, hypothesis="a halt test",
        asset_class=r59.AC_US_EQUITY, family="SHORT_TERM_REVERSAL",
        feature_set_id="fs1", horizon_sessions=5,
        parameters={"label": "T", "cadence_sessions": 21, "hold_sessions": 5},
        discovery_sample={"start": "2011-07-01", "end": "2018-01-01"},
        evaluation_sample={"validation": "2018-01-01", "lockbox": "2023-01-01"},
        cost_model={"rate_per_side": 0.0025}, expected_sign=1))


def test_e_the_halt_settles_charges_burden_and_is_idempotent(pipe, tmp_path):
    reg = _preregister(pipe, tmp_path)
    eid, sh = reg["experiment_id"], reg["spec_hash"]
    assert [r["experiment_id"] for r in pipe.open_pre_measurement_halts()] \
        == [eid]
    before = pipe.mem.burden()["total"]

    out = pipe.perform("reversal-signal-agent",
                       "record_pre_measurement_halt", dict(
                           experiment_id=eid, spec_hash=sh,
                           halt_reason="COST_BUDGET_EXCEEDED",
                           measured_metric="annualized_cost_drag",
                           measured_value=0.219, frozen_threshold=0.03))
    assert out["state"] == "RECORDED"
    assert out["outcome"] == r59.HO_REJECTED
    assert pipe.mem.get(eid)["outcome"] == r59.HO_REJECTED
    assert pipe.mem.burden()["total"] == before + 1
    assert pipe.open_pre_measurement_halts() == []

    again = pipe.perform("reversal-signal-agent",
                         "record_pre_measurement_halt", dict(
                             experiment_id=eid, spec_hash=sh,
                             halt_reason="COST_BUDGET_EXCEEDED",
                             measured_metric="annualized_cost_drag",
                             measured_value=0.219, frozen_threshold=0.03))
    assert again["state"] == "ALREADY_RECORDED"
    assert pipe.mem.burden()["total"] == before + 1


def test_e_a_halt_after_a_layer_was_measured_is_refused(pipe, tmp_path):
    """Then it is not pre-measurement, and ``reveal_stage`` owns it."""
    reg = _preregister(pipe, tmp_path)
    eid, sh = reg["experiment_id"], reg["spec_hash"]
    pipe.perform("reversal-signal-agent", "reveal_stage", dict(
        experiment_id=eid, spec_hash=sh, stage="D",
        stats={"ann_net_excess": 0.0001, "periods": 40}, evaluator="t"))
    with pytest.raises(P.PipelineRefusal) as exc:
        pipe.perform("reversal-signal-agent",
                     "record_pre_measurement_halt", dict(
                         experiment_id=eid, spec_hash=sh,
                         halt_reason="COST_BUDGET_EXCEEDED",
                         measured_metric="annualized_cost_drag",
                         measured_value=0.219, frozen_threshold=0.03))
    assert exc.value.code == "NOT_A_PRE_MEASUREMENT_HALT"


def test_e_a_halt_under_a_changed_spec_is_refused(pipe, tmp_path):
    reg = _preregister(pipe, tmp_path)
    with pytest.raises(P.PipelineRefusal) as exc:
        pipe.perform("reversal-signal-agent",
                     "record_pre_measurement_halt", dict(
                         experiment_id=reg["experiment_id"],
                         spec_hash="not-the-frozen-hash",
                         halt_reason="DATA_COVERAGE_FLOOR",
                         measured_metric="coverage", measured_value=0.7,
                         frozen_threshold=0.8))
    assert exc.value.code == "SPEC_CHANGED_AFTER_PREREGISTRATION"


def test_e_only_the_owning_agent_may_record_it(pipe, tmp_path):
    reg = _preregister(pipe, tmp_path)
    with pytest.raises(P.PipelineRefusal) as exc:
        pipe.perform("momentum-signal-agent",
                     "record_pre_measurement_halt", dict(
                         experiment_id=reg["experiment_id"],
                         spec_hash=reg["spec_hash"],
                         halt_reason="DATA_COVERAGE_FLOOR",
                         measured_metric="coverage", measured_value=0.7,
                         frozen_threshold=0.8))
    assert exc.value.code == "NOT_THE_OWNING_AGENT"


def test_e_there_is_exactly_one_registry():
    """The halt writes through the EXISTING memory and event stream."""
    src = (REPO / "alpha_agent" / "r61" / "halts.py").read_text(
        encoding="utf-8")
    assert "sqlite3" not in src
    assert "CREATE TABLE" not in src
    assert P.EV_PREMEASUREMENT_HALT == HALT.EV_PREMEASUREMENT_HALT


def test_e_the_verb_is_granted_to_every_signal_agent():
    c = C.Contracts()
    for agent in ("momentum-signal-agent", "reversal-signal-agent",
                  "trend-breadth-signal-agent", "volatility-liquidity-agent"):
        assert "record_pre_measurement_halt" in c.verbs_for(agent)
    # and to nobody else
    assert "record_pre_measurement_halt" not in c.verbs_for(
        "quant-research-director")
    assert C.validate() == []


# =========================================================================== #
# WORKSTREAM F - assignment sizing
# =========================================================================== #
def test_f_model_routing_is_unchanged():
    """The cost-efficient routing R58 landed must survive this release."""
    assert RT.ROUTING["quant-research-director"]["model"] == "opus"
    assert RT.ROUTING["validation-skeptic-agent"]["model"] == "opus"
    assert RT.ROUTING["data-foundation-agent"]["model"] == "haiku"
    assert RT.ROUTING["data-foundation-agent"]["maxTurns"] == 15
    for agent in RT._MEDIUM:
        assert RT.ROUTING[agent]["model"] == "sonnet"


def test_f_turn_budgets_are_read_from_routing_not_invented():
    assert AS.turn_budget() == RT.ROUTING["data-foundation-agent"]["maxTurns"]
    assert 1 <= AS.early_artifact_turn() < AS.turn_budget()


def test_f_a_multi_source_plan_is_split_into_bounded_assignments():
    plan = {"assignment_id": "DF5", "run_id": "R", "campaign_id": "C",
            "role": "data-foundation-agent",
            "sources": [{"source_id": "A"}, {"source_id": "B"},
                        {"source_id": "C"}]}
    out = AS.split_plan(plan)
    assert len(out) == 3
    assert [a["assignment_id"] for a in out] == ["DF5A", "DF5B", "DF5C"]
    for a in out:
        assert len(a["sources"]) == AS.MAX_SOURCES_PER_ASSIGNMENT
        assert 0 < len(a["questions"]) <= AS.MAX_QUESTIONS_PER_ASSIGNMENT
        assert a["write_artifact_by_turn"] < a["turn_budget"]
        assert a["artifact"].endswith(".json")
        assert AS.problems(a) == []
        assert a["split_from"] == "DF5"


def test_f_an_empty_plan_is_refused():
    with pytest.raises(AS.AssignmentRefusal):
        AS.split_plan({"assignment_id": "DF9", "sources": []})


def test_f_the_real_r60_assignments_are_detected_and_repaired():
    """DF4 carried 2 sources and DF5 carried 3, each to a 15-turn role, and
    each had to produce one certification per source."""
    base = REPO / "research" / "agents" / "campaign_r60_information_frontier"
    for name, n in (("ASSIGNMENT_DF4_SEC_STORES", 2),
                    ("ASSIGNMENT_DF5_LIGHT_PROBES", 3)):
        plan = json.loads((base / ("%s.json" % name)).read_text(
            encoding="utf-8"))
        audit = AS.audit_plan(plan)
        assert audit["sources"] == n
        assert audit["would_overrun"] is True
        assert audit["problems_before"]
        assert len(audit["bounded_assignments"]) == n
        assert audit["problems_after"] == []
        assert audit["model"] == "haiku" and audit["turn_budget"] == 15
    # a single-source plan is already bounded
    plan = json.loads((base / "ASSIGNMENT_DF1_EXTENSION_UNIVERSE.json"
                       ).read_text(encoding="utf-8"))
    assert AS.audit_plan(plan)["would_overrun"] is False


# =========================================================================== #
# WORKSTREAM G - the worker registry
# =========================================================================== #
def test_g_a_worker_is_running_then_completed(tmp_path):
    with W.worker("w1", run_id="RUN", root=tmp_path, command=["x"]):
        rep = W.report("RUN", root=tmp_path)
        assert rep["running"] == ["w1"]
        assert rep["by_state"]["RUNNING"] == 1
    rep = W.report("RUN", root=tmp_path)
    assert rep["running_count"] == 0
    assert rep["by_state"][W.STATE_COMPLETED] == 1
    assert rep["stale"] == []


def test_g_a_failing_worker_records_failed_not_completed(tmp_path):
    with pytest.raises(ValueError):
        with W.worker("w2", run_id="RUN", root=tmp_path):
            raise ValueError("boom")
    rows = W.rows("RUN", root=tmp_path)
    assert rows[0]["state"] == W.STATE_FAILED
    assert "boom" in rows[0]["exit_detail"]


def test_g_a_dead_row_reads_interrupted_never_running(tmp_path):
    W.register("ghost", run_id="RUN", root=tmp_path, pid=999999,
               command=["ghost"])
    rep = W.report("RUN", root=tmp_path)
    assert rep["running_count"] == 0
    assert rep["by_state"][W.STATE_INTERRUPTED] == 1
    assert rep["orphaned_workers"] == ["ghost"]
    # the STORED row is untouched: the registry records what the worker said
    assert W.rows("RUN", root=tmp_path)[0]["state"] == W.STATE_RUNNING


def test_g_pid_reuse_cannot_resurrect_a_worker(tmp_path):
    """Windows recycles pids. "Is pid 1234 alive" is the wrong question."""
    import os
    row = W.register("w3", run_id="RUN", root=tmp_path, pid=os.getpid())
    assert W.observed_state(row) == W.STATE_RUNNING
    imposter = dict(row, process_created_filetime=1)
    assert W.observed_state(imposter) == W.STATE_INTERRUPTED
    unidentifiable = dict(row, process_created_filetime=None)
    assert W.observed_state(unidentifiable) == W.STATE_INTERRUPTED


def test_g_one_worker_is_exactly_one_row(tmp_path):
    W.register("w4", run_id="RUN", root=tmp_path)
    W.finish("w4", run_id="RUN", root=tmp_path)
    W.register("w4", run_id="RUN", root=tmp_path)
    assert len(W.rows("RUN", root=tmp_path)) == 1
    assert len(list((tmp_path / "RUN").glob("*.json"))) == 1


def test_g_finish_refuses_a_non_terminal_state_and_an_unknown_worker(tmp_path):
    W.register("w5", run_id="RUN", root=tmp_path)
    with pytest.raises(W.WorkerRefusal):
        W.finish("w5", state=W.STATE_RUNNING, run_id="RUN", root=tmp_path)
    with pytest.raises(W.WorkerRefusal):
        W.finish("never-registered", run_id="RUN", root=tmp_path)


def test_g_assert_none_running_is_a_proof_not_a_hope(tmp_path):
    W.register("w6", run_id="RUN", root=tmp_path)
    with pytest.raises(W.WorkerRefusal):
        W.assert_none_running("RUN", root=tmp_path)
    W.finish("w6", run_id="RUN", root=tmp_path)
    assert W.assert_none_running("RUN", root=tmp_path)["running_count"] == 0


def test_g_the_registry_and_the_operator_surface_have_no_kill_verb():
    """The operator's reflex - hunting python.exe by NAME - is how an
    unrelated process or Claude Code gets terminated by mistake."""
    for rel in ("alpha_agent/r61/workers.py",
                "scripts/r61_research_workers.py"):
        src = (REPO / rel).read_text(encoding="utf-8")
        for forbidden in ("taskkill", "TerminateProcess", "os.kill",
                          "Stop-Process", "signal.SIGKILL", "proc.kill"):
            assert forbidden not in src, "%s mentions %s" % (rel, forbidden)


def test_g_the_heavy_job_budget_is_declared_and_checked(tmp_path):
    assert W.MAX_CONCURRENT_HEAVY_JOBS == 2
    for i in range(3):
        W.register("h%d" % i, run_id="RUN", root=tmp_path, kind=W.KIND_HEAVY)
    rep = W.report("RUN", root=tmp_path)
    assert rep["heavy_running_count"] == 3
    assert rep["heavy_over_budget"] is True


# =========================================================================== #
# Release-wide safety
# =========================================================================== #
def test_release_creates_no_order_fill_or_promotion():
    forbidden = ("create_order", "submit_order", "place_order", "create_fill",
                 "execute_trade", "promote_champion", "make_capital_eligible",
                 "request_forward_registration")
    for p in sorted((REPO / "alpha_agent" / "r61").glob("*.py")):
        src = p.read_text(encoding="utf-8")
        for f in forbidden:
            assert f not in src, "%s mentions %s" % (p.name, f)


def test_the_calibration_cannot_register_a_hypothesis():
    """It is infrastructure validation, not an alpha experiment."""
    for name in ("power.py", "calibration_panels.py", "identity_audit.py"):
        src = (REPO / "alpha_agent" / "r61" / name).read_text(encoding="utf-8")
        for verb in ("preregister", "submit_candidate", "record_result",
                     "freeze_forward", "mem.register"):
            assert verb not in src, "%s calls %s" % (name, verb)


def test_r61_safety_block_is_stamped_on_every_artifact():
    from paper_trader.alpha_agent import r61
    assert r61.SAFETY["registers_alpha_hypotheses"] is False
    assert r61.SAFETY["consumes_alpha_lockbox"] is False
    assert r61.SAFETY["creates_forward_requests"] is False
    assert r61.SAFETY["creates_orders"] is False
    assert r61.SAFETY["backfill"] is False
