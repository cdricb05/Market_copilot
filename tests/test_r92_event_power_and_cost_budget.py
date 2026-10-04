r"""R92 - event-time statistical power (FIX A) and event-book cost budget (FIX B).

RESEARCH ONLY. These tests prove two UNIT / CALIBRATION corrections and that
nothing else moved:

FIX A  alpha_agent.r61.mechanism_power classifies an EVENT_DRIVEN sample in
       EVENT-RETURN units (annualised net excess) from the merged independent
       lockbox observations, the per-observation volatility, the observation
       frequency, serial dependence and the multiplicity burden - not in
       cross-sectional rank-IC units that have no meaning for a 1-6 leg book.
FIX B  alpha_agent.r61.cost_budget annualises an event spec by its
       pre-registered independent observations per year, not by 252 / hold.

Tests A-J (power) and A-I (cost) follow the R92 contract list verbatim.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.r61 import cost_budget as CB
from paper_trader.alpha_agent.r61 import mechanism_power as MP
from paper_trader.alpha_agent.agents_v2 import event_book as EVB

REPO = Path(__file__).resolve().parents[1]
R91 = REPO / "research" / "agents" / "campaign_r91_alpha_search_liberation"

# A rates-like six-leg one-session window: ~0.4%/obs; an energy-like one: 2%.
RATES_VOL = 0.004
ENERGY_VOL = 0.02


def _event_sample(*, n_lockbox_obs, vol=RATES_VOL, epy=52.0, n_obs=None,
                  n_instruments=1, rho1=0.0, windows=1, clusters=None,
                  events=None, cadence=1, horizon=1):
    n_obs = n_obs if n_obs is not None else n_lockbox_obs * 4
    return MP.effective_sample(
        MP.EVENT_DRIVEN, n_instruments=n_instruments,
        n_events=events if events is not None else n_obs,
        n_event_clusters=clusters if clusters is not None else n_obs,
        n_decisions=n_obs, lockbox_decisions=n_lockbox_obs,
        cadence=cadence, horizon=horizon,
        n_observations=n_obs, n_lockbox_observations=n_lockbox_obs,
        event_return_vol=vol, event_return_vol_basis=MP.EVENT_VOL_BASIS_EXANTE,
        events_per_year=epy, serial_correlation=rho1, n_tested_windows=windows)


def _assess(sample, **kw):
    return MP.assess(structure=MP.EVENT_DRIVEN, sample=sample, **kw)


# --------------------------------------------------------------------------- #
# POWER A. dense event books are no longer unidentifiable because of leg count
# --------------------------------------------------------------------------- #
def test_power_a_dense_books_are_not_unidentifiable_merely_for_having_few_legs():
    for legs in (1, 2, 6):
        a = _assess(_event_sample(n_lockbox_obs=189, n_instruments=legs))
        assert a["mde_units"] == MP.MDE_UNITS_EVENT_RETURN
        assert a["power_class"] != MP.POWER_UNIDENTIFIABLE
        assert a["research_path"] != MP.PATH_NOT_ON_THIS_EXPRESSION
        assert a["mde"]["mde_rho"] is None
        assert a["mde"]["mde_annualised"] > 0
    one = _assess(_event_sample(n_lockbox_obs=189, n_instruments=1))
    six = _assess(_event_sample(n_lockbox_obs=189, n_instruments=6))
    assert one["power_class"] == six["power_class"]
    assert one["mde"]["mde_annualised"] == pytest.approx(six["mde"]["mde_annualised"])
    # The R91 baseline on the same sample, rank-IC units: unidentifiable.
    old = MP.effective_sample(MP.EVENT_DRIVEN, n_instruments=1, n_events=787,
                              n_event_clusters=787, n_decisions=787,
                              lockbox_decisions=189, cadence=1, horizon=1)
    base = _assess(old)
    assert base["mde_units"] == MP.MDE_UNITS_RANK_IC
    assert base["power_class"] == MP.POWER_UNIDENTIFIABLE
    assert "RANK_IC_FALLBACK" in base["mde"]["units_warning"]


def test_power_a2_the_answer_is_an_economic_return_magnitude():
    a = _assess(_event_sample(n_lockbox_obs=189))
    m = a["mde"]
    t_req = MP.t_required(burden_denominator=1)
    assert m["mde_per_event"] == pytest.approx(t_req * RATES_VOL / math.sqrt(189))
    assert m["mde_annualised"] == pytest.approx(m["mde_per_event"] * 52.0)
    assert m["units"] == MP.MDE_UNITS_EVENT_RETURN
    assert "per_event" in m["form"] and "observations_per_year" in m["form"]
    # bands are multiples of the frozen materiality floor
    assert [b[1] for b in MP.EVENT_CLASS_BANDS] == pytest.approx(
        [m_ * r59.GATE_MATERIALITY for _, m_ in MP.EVENT_CLASS_BAND_MULTIPLES])


# --------------------------------------------------------------------------- #
# POWER B. sparse sets below the observation floor stay unresearchable
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", (5, 10, 15, 35))
def test_power_b_sparse_event_sets_remain_historically_unqualifiable(n):
    a = _assess(_event_sample(n_lockbox_obs=n, vol=0.0001, epy=4.0))
    assert a["sample"]["lockbox_clears_obs_floor"] is False
    assert a["power_class"] == MP.POWER_UNIDENTIFIABLE
    assert "OBS_FLOOR %d" % r59.OBS_FLOOR in a["power_class_reason"]
    assert a["incubation_admissible_on_power"] is False
    r = MP.g7_ruling(a)
    assert r["closure"] == MP.CLOSURE_EXPRESSION_ONLY
    assert r["closure_is_permanent"] is False


def test_power_b2_the_floor_sits_exactly_at_obs_floor():
    below = _assess(_event_sample(n_lockbox_obs=r59.OBS_FLOOR - 1))
    at = _assess(_event_sample(n_lockbox_obs=r59.OBS_FLOOR))
    assert below["power_class"] == MP.POWER_UNIDENTIFIABLE
    assert at["sample"]["lockbox_clears_obs_floor"] is True


# --------------------------------------------------------------------------- #
# POWER C / D. overlap and clusters do not multiply the sample
# --------------------------------------------------------------------------- #
def test_power_c_overlapping_events_reduce_effective_n():
    merged = MP.effective_sample(MP.EVENT_DRIVEN, n_events=300, n_event_clusters=300,
                                 n_decisions=300, lockbox_decisions=150,
                                 cadence=1, horizon=2, n_lockbox_clusters=150,
                                 event_return_vol=RATES_VOL, events_per_year=40.0)
    assert merged["overlap_factor"] == 2.0
    assert merged["independent_lockbox_decisions"] == pytest.approx(75.0)
    book = MP.effective_sample(MP.EVENT_DRIVEN, n_events=300, n_event_clusters=300,
                               n_decisions=300, lockbox_decisions=150,
                               cadence=1, horizon=2, n_lockbox_observations=100,
                               event_return_vol=RATES_VOL, events_per_year=40.0)
    assert book["independent_lockbox_decisions"] == pytest.approx(100.0)
    assert book["lockbox_sample_basis"] == "MERGED_INDEPENDENT_LOCKBOX_OBSERVATIONS"
    # fewer independent observations -> a larger detectable effect
    a_full = _assess(_event_sample(n_lockbox_obs=150))
    a_merged = _assess(_event_sample(n_lockbox_obs=100))
    assert a_merged["mde"]["mde_annualised"] > a_full["mde"]["mde_annualised"]


def test_power_c2_the_event_book_merges_overlapping_windows_into_one_observation():
    n_m, n_t = 2, 400
    dates = np.arange(np.datetime64("2023-01-02"), np.datetime64("2023-01-02") + n_t)
    dates = dates[np.is_busday(dates)]
    ret = np.full((n_m, len(dates)), 0.001)
    layer = {"dates": dates, "symbols": ["A", "B"], "ret": ret,
             "roll": np.zeros_like(ret, dtype=np.uint8), "cost_per_side": [0.0005, 0.0005]}
    events = [{"event_id": "e%d" % i, "cluster_id": "c%d" % i,
               "event_timestamp": str(dates[10 + i]) + "T10:00:00Z",
               "available_at": str(dates[10 + i]) + "T10:00:00Z",
               "weights": {"A": 1.0}} for i in range(6)]       # six consecutive days
    pm = EVB.pre_measurement_sample(layer, events, hold=5)
    assert pm["layers"]["L"]["raw_events"] == 6
    assert pm["layers"]["L"]["merged_observations"] == 1        # windows overlap
    out = EVB.run_event_window_book(layer, events, stage="L", hold=5)
    assert out["layers"]["L"]["effective_observations"] == 1
    assert out["layers"]["L"]["raw_events"] == 6


def test_power_d_same_episode_events_do_not_multiply_sample_size():
    s = MP.effective_sample(MP.EVENT_DRIVEN, n_events=600, n_event_clusters=100,
                            n_decisions=600, lockbox_decisions=200, cadence=1, horizon=1)
    assert s["independent_decisions"] == 100
    assert s["raw_events"] == 600
    s2 = MP.effective_sample(MP.EVENT_DRIVEN, n_events=600, n_event_clusters=100,
                             n_decisions=600, lockbox_decisions=200, cadence=1, horizon=1,
                             n_lockbox_clusters=40, event_return_vol=RATES_VOL,
                             events_per_year=12.0)
    assert s2["independent_lockbox_decisions"] == pytest.approx(40.0)
    assert s2["lockbox_sample_basis"] == "LOCKBOX_CLUSTERS_OVER_OVERLAP"


def test_power_d2_serial_dependence_divides_the_sample():
    iid = _event_sample(n_lockbox_obs=100)
    dep = _event_sample(n_lockbox_obs=100, rho1=0.5)
    assert dep["serial_dependence_factor"] == pytest.approx((1 - 0.5) / (1 + 0.5))
    assert dep["independent_lockbox_decisions"] == pytest.approx(100 / 3)
    assert iid["independent_lockbox_decisions"] == 100
    assert dep["lockbox_clears_obs_floor"] is False      # 33.3 < 36


# --------------------------------------------------------------------------- #
# POWER E / F. volatility raises, observations lower, the detectable effect
# --------------------------------------------------------------------------- #
def test_power_e_higher_event_return_volatility_raises_the_required_effect():
    lo = _assess(_event_sample(n_lockbox_obs=189, vol=RATES_VOL))
    hi = _assess(_event_sample(n_lockbox_obs=189, vol=ENERGY_VOL))
    assert hi["mde"]["mde_annualised"] == pytest.approx(
        lo["mde"]["mde_annualised"] * ENERGY_VOL / RATES_VOL)
    assert MP.POWER_CLASSES.index(hi["power_class"]) >= \
        MP.POWER_CLASSES.index(lo["power_class"])


def test_power_f_more_independent_observations_lower_the_detectable_effect():
    prev = None
    for n in (40, 80, 189, 400, 800):
        a = _assess(_event_sample(n_lockbox_obs=n))
        if prev is not None:
            assert a["mde"]["mde_annualised"] < prev
            assert a["mde"]["mde_annualised"] == pytest.approx(
                prev * math.sqrt(n_prev / n))
        prev, n_prev = a["mde"]["mde_annualised"], n


# --------------------------------------------------------------------------- #
# POWER G / H. boundaries and final thresholds are untouched
# --------------------------------------------------------------------------- #
def test_power_g_d_v_l_boundaries_are_untouched():
    assert r59.DISCOVERY_START == "2011-07-01"
    assert r59.VALIDATION_START == "2018-01-01"
    assert r59.LOCKBOX_START == "2023-01-01"
    dates = np.arange(np.datetime64("2017-12-20"), np.datetime64("2018-01-12"))
    dates = dates[np.is_busday(dates)]
    # an entry whose window crosses the V boundary is purged; after it, V
    t_cross = int(np.searchsorted(dates, np.datetime64("2017-12-28")))
    assert EVB.layer_of_entry(dates, t_cross, 5) is None
    t_v = int(np.searchsorted(dates, np.datetime64("2018-01-03")))
    assert EVB.layer_of_entry(dates, t_v, 2) == "V"


def test_power_h_final_qualification_thresholds_are_unchanged():
    assert r59.BH_Q == 0.10 and MP.BH_Q == 0.10
    assert r59.OBS_FLOOR == 36 and MP.OBS_FLOOR == 36
    assert r59.GATE_MATERIALITY == 0.015
    assert MP.POWER_TARGET == 0.80
    assert MP.CLASS_BANDS == ((MP.POWER_STRONG, 0.03), (MP.POWER_FEASIBLE, 0.05),
                              (MP.POWER_MARGINAL, 0.10), (MP.POWER_WEAK, 0.15))
    a = _assess(_event_sample(n_lockbox_obs=189))
    ro = a["final_gate_read_only"]
    assert ro["changed_by_this_module"] is False
    assert ro["bh_q"] == 0.10 and ro["obs_floor"] == 36
    assert ro["materiality_floors"] == dict(r59.GATE_MATERIALITY_FLOORS)
    # a power class is a research PATH; it has no outcome and qualifies nothing
    assert "outcome" not in a and "qualified" not in a
    assert set(a["research_path"] for _ in [0]) <= set(MP.RESEARCH_PATHS)
    # the gate schema hash the skeptic freezes at pre-registration is not
    # derived from anything this module changed (pinned by the R89 suite),
    # and the cost-budget ceiling the schema inherits is unchanged.
    assert CB.COST_BUDGET_CEILING == pytest.approx(0.03)
    assert CB.COST_BUDGET_VERSION == "R61_ANNUALISED_COST_BUDGET_V1"
    assert MP.MECHANISM_POWER_VERSION == "R89_MECHANISM_AWARE_POWER_V1"


# --------------------------------------------------------------------------- #
# POWER I. multiplicity still applies
# --------------------------------------------------------------------------- #
def test_power_i_multiplicity_still_applies():
    base = _assess(_event_sample(n_lockbox_obs=189))
    burdened = _assess(_event_sample(n_lockbox_obs=189), burden_denominator=10)
    windows = _assess(_event_sample(n_lockbox_obs=189, windows=3))
    assert burdened["mde"]["mde_annualised"] > base["mde"]["mde_annualised"]
    assert windows["mde"]["mde_annualised"] > base["mde"]["mde_annualised"]
    assert windows["mde"]["effective_burden_denominator"] == 3
    assert burdened["mde"]["t_required"] == pytest.approx(
        MP.t_required(burden_denominator=10))
    both = _assess(_event_sample(n_lockbox_obs=189, windows=3), burden_denominator=10)
    assert both["mde"]["effective_burden_denominator"] == 30
    assert both["mde"]["mde_annualised"] > burdened["mde"]["mde_annualised"]


def test_power_i2_the_record_is_hash_bound_and_the_forward_minimum_is_in_event_units():
    a = _assess(_event_sample(n_lockbox_obs=189))
    assert MP.verify(a)
    a2 = dict(a)
    a2["power_class"] = MP.POWER_STRONG
    assert not MP.verify(a2)
    m = MP.minimum_forward_evidence(a["sample"], target_ic=0.05)
    assert m["units"] == MP.MDE_UNITS_EVENT_RETURN
    assert m["target_annual_return"] == pytest.approx(2 * r59.GATE_MATERIALITY)
    assert m["effective_decisions_required"] >= r59.OBS_FLOOR
    strong = MP.minimum_forward_evidence(a["sample"], target_ic=0.03)
    assert strong["effective_decisions_required"] >= m["effective_decisions_required"]
    with pytest.raises(ValueError):
        MP.minimum_forward_evidence(a["sample"], target_ic=0.07)


# --------------------------------------------------------------------------- #
# POWER J. R91 results do not become survivors retroactively
# --------------------------------------------------------------------------- #
def test_power_j_r91_results_do_not_become_historical_survivors():
    res = json.loads((R91 / "R91_EXPERIMENT_RESULTS.json").read_text(encoding="utf-8"))
    corrected = json.loads((R91 / "R91_POWER_ASSESSMENTS_CORRECTED.json")
                           .read_text(encoding="utf-8"))
    assert len(res["experiments"]) == 30
    for row in res["experiments"]:
        assert row["outcome"] == r59.HO_NO_ALPHA_EVIDENCE
        assert row["state"] in ("HALTED", "MEASURED", "MEASURED_TO_LOCKBOX")
    # Re-assess the 24 recorded expressions in event units with an energy /
    # rates-like volatility: every sparse one stays UNIDENTIFIABLE on the
    # floor, and no assessment carries an outcome or a qualification.
    for key, rec in corrected.items():
        n_l = rec["independent_lockbox_decisions"]
        s = _event_sample(n_lockbox_obs=n_l, vol=ENERGY_VOL, epy=max(1.0, n_l / 3.6))
        a = _assess(s)
        if n_l < r59.OBS_FLOOR:
            assert a["power_class"] == MP.POWER_UNIDENTIFIABLE, key
        assert "outcome" not in a
        assert a["incubation_admissible_on_power"] in (True, False)
    # The dense C04/C05 expressions (187-189 obs) acquire a CLASS, which is
    # a research path only; their recorded NO_ALPHA_EVIDENCE outcomes stand.
    dense = _assess(_event_sample(n_lockbox_obs=189, vol=ENERGY_VOL, epy=52.0))
    assert dense["mde_units"] == MP.MDE_UNITS_EVENT_RETURN
    assert dense["research_path"] in MP.RESEARCH_PATHS


# =========================================================================== #
# COST BUDGET
# =========================================================================== #
def _event_spec(freq=None, hold=1, raw=None, extra=None):
    p = {"holding_window_sessions": hold, "book": "FUTURES_EVENT_WINDOW",
         "label": "event"}
    if freq is not None:
        p["expected_independent_observations_per_year"] = freq
    if raw is not None:
        p["expected_raw_events_per_year"] = raw
    p.update(extra or {})
    return {"parameters": p, "horizon_sessions": hold,
            "cost_model": {"cost_model_id": "FUTURES_PER_MARKET_R38_PLUS_ROLL_V1"}}


def test_cost_a_five_events_a_year_are_annualised_as_five_not_252():
    out = CB.evaluate_frozen_spec(_event_spec(freq=5.0, hold=1),
                                  one_way_turnover=1.0, cost_per_side=0.0005)
    assert out["frequency_basis"] == CB.FREQUENCY_BASIS_EVENTS
    assert out["inputs"]["rebalances_per_year"] == 5.0
    assert out["measured"] == pytest.approx(5.0 * 1.0 * 2.0 * 0.0005)
    assert out["state"] == CB.BUDGET_PASS
    # the R91 baseline on the same hold: 252 / 1
    old = CB.evaluate_frozen_spec({"parameters": {"hold_sessions": 1},
                                   "cost_model": {}},
                                  one_way_turnover=1.0, cost_per_side=0.0005)
    assert old["inputs"]["rebalances_per_year"] == 252.0
    assert old["frequency_basis"] == CB.FREQUENCY_BASIS_INTERVAL


def test_cost_b_fifty_two_weekly_events_remain_fifty_two():
    out = CB.evaluate_frozen_spec(_event_spec(freq=52.0, hold=5),
                                  one_way_turnover=1.0, cost_per_side=0.0005)
    assert out["inputs"]["rebalances_per_year"] == 52.0
    assert out["measured"] == pytest.approx(52 * 2 * 0.0005)
    assert out["inputs"]["implied_rebalance_interval_sessions"] == pytest.approx(252 / 52)


def test_cost_c_clustering_reduces_the_count_but_never_the_per_event_cost():
    out = CB.evaluate_frozen_spec(_event_spec(freq=60.0, raw=100.0),
                                  one_way_turnover=1.0, cost_per_side=0.0005)
    per_event = 2 * 1.0 * 0.0005
    assert out["cost_per_event_round_trip"] == pytest.approx(per_event)
    assert out["measured"] == pytest.approx(60 * per_event)
    assert out["diagnostic_unmerged_raw_event_cost_drag"] == pytest.approx(100 * per_event)
    assert out["measured"] > 0


def test_cost_d_multi_leg_costs_are_charged_on_every_leg():
    four = CB.evaluate_event_cost_budget(events_per_year=10.0, turnover_per_leg=0.25,
                                         n_legs=4, cost_per_side=0.0005)
    assert four["inputs"]["one_way_turnover_per_event"] == pytest.approx(1.0)
    assert four["measured"] == pytest.approx(10 * 1.0 * 2 * 0.0005)
    assert CB.turnover_per_event_from_legs(0.5, 2) == 1.0
    with pytest.raises(CB.CostBudgetRefusal):
        CB.turnover_per_event_from_legs(0.5, 0)


def test_cost_e_entry_and_exit_are_both_represented():
    out = CB.evaluate_event_cost_budget(events_per_year=1.0,
                                        one_way_turnover_per_event=1.0,
                                        cost_per_side=0.001)
    assert out["inputs"]["entry_and_exit_factor"] == 2.0
    assert out["measured"] == pytest.approx(2 * 0.001)
    assert CB.annualized_event_cost_drag(events_per_year=1, one_way_turnover_per_event=1,
                                         cost_per_side=0.001) == pytest.approx(0.002)


def test_cost_f_g_overlap_nets_through_the_independent_count_and_real_changes_pay():
    # F: ten raw events that merge into four unit-capital observations do not
    # pay ten round trips ...
    merged = CB.evaluate_frozen_spec(_event_spec(freq=4.0, raw=10.0),
                                     one_way_turnover=1.0, cost_per_side=0.001)
    assert merged["measured"] == pytest.approx(4 * 2 * 0.001)
    assert merged["diagnostic_unmerged_raw_event_cost_drag"] == pytest.approx(10 * 2 * 0.001)
    # ... which is exactly what the canonical book charges per unit capital:
    # a merged observation carries the MEAN of its members' costs, not 0.
    n_t = 400
    dates = np.arange(np.datetime64("2023-01-02"), np.datetime64("2023-01-02") + n_t)
    dates = dates[np.is_busday(dates)]
    ret = np.full((1, len(dates)), 0.0)
    layer = {"dates": dates, "symbols": ["A"], "ret": ret,
             "roll": np.zeros_like(ret, dtype=np.uint8), "cost_per_side": [0.001]}
    ev = [{"event_id": "e%d" % i, "cluster_id": "c%d" % i,
           "event_timestamp": str(dates[20 + i]) + "T10:00:00Z",
           "available_at": str(dates[20 + i]) + "T10:00:00Z",
           "weights": {"A": 1.0}} for i in range(3)]
    out = EVB.run_event_window_book(layer, ev, stage="L", hold=5)
    obs = out["series"]["observations"]
    assert len(obs) == 1 and obs[0]["n_events"] == 3
    assert obs[0]["cost"] == pytest.approx(2 * 0.001)           # mean, not sum, not 0
    # G: ten events that do NOT overlap each pay a full round trip
    ev2 = [{"event_id": "e%d" % i, "cluster_id": "c%d" % i,
            "event_timestamp": str(dates[20 + 10 * i]) + "T10:00:00Z",
            "available_at": str(dates[20 + 10 * i]) + "T10:00:00Z",
            "weights": {"A": 1.0}} for i in range(10)]
    out2 = EVB.run_event_window_book(layer, ev2, stage="L", hold=5)
    assert out2["layers"]["L"]["effective_observations"] == 10
    assert all(o["cost"] == pytest.approx(2 * 0.001) for o in out2["series"]["observations"])
    separate = CB.evaluate_frozen_spec(_event_spec(freq=10.0, raw=10.0),
                                       one_way_turnover=1.0, cost_per_side=0.001)
    assert separate["measured"] == pytest.approx(10 * 2 * 0.001)


def test_cost_h_roll_costs_remain_included():
    without = CB.evaluate_event_cost_budget(events_per_year=10.0,
                                            one_way_turnover_per_event=1.0,
                                            cost_per_side=0.0005)
    with_roll = CB.evaluate_event_cost_budget(events_per_year=10.0,
                                              one_way_turnover_per_event=1.0,
                                              cost_per_side=0.0005,
                                              additional_ann_cost_drag=0.004)
    assert with_roll["measured"] == pytest.approx(without["measured"] + 0.004)
    assert with_roll["annualized_roll_or_other_cost_drag"] == 0.004
    via_spec = CB.evaluate_frozen_spec(_event_spec(freq=10.0), one_way_turnover=1.0,
                                       cost_per_side=0.0005, additional_ann_cost_drag=0.004)
    assert via_spec["measured"] == pytest.approx(with_roll["measured"])


def test_cost_h2_an_event_spec_without_a_frequency_fails_closed_never_252():
    out = CB.evaluate_frozen_spec(_event_spec(freq=None, hold=1),
                                  one_way_turnover=1.0, cost_per_side=0.0005)
    assert out["state"] == CB.BUDGET_NOT_EVALUABLE and out["passed"] is False
    assert out["inputs"]["rebalances_per_year"] is None
    assert any("never annualised by 252" in r for r in out["reasons"])
    assert CB.is_event_spec({"parameters": {"book": "FUTURES_EVENT_WINDOW"}})
    assert CB.is_event_spec({"parameters": {"structure": "EVENT_DRIVEN"}})
    assert not CB.is_event_spec({"parameters": {"cadence_sessions": 21}})


def test_cost_i_historical_r91_cost_results_are_unchanged():
    # (1) non-event specs still annualise by 252 / interval (the R61 world)
    monthly = CB.evaluate_frozen_spec({"parameters": {"cadence_sessions": 21},
                                       "cost_model": {"rate_per_side": 0.0025}},
                                      one_way_turnover=0.40)
    assert monthly["inputs"]["rebalances_per_year"] == 12.0
    assert monthly["measured"] == pytest.approx(0.024)
    # (2) the R91 runner's REALISED costs were computed by the event book,
    # not by the budget: every recorded layer is internally consistent with
    # 2 x |w| x rate per event (+ roll) at the certified panel rates, and the
    # implied per-side rate sits inside the panel's 2-15 bp range.
    res = json.loads((R91 / "R91_EXPERIMENT_RESULTS.json").read_text(encoding="utf-8"))
    checked = 0
    for row in res["experiments"]:
        for lay in ("D", "V", "L"):
            st = row.get(lay)
            if not st:
                continue
            drag = st["ann_cost_drag"]
            to = st["ann_oneway_turnover"]
            if to and to > 0:
                implied = drag / (2.0 * to)
                assert 0.0002 - 1e-9 <= implied <= 0.0015 * 1.6, (row["executor"], lay, implied)
                checked += 1
    assert checked >= 30
    # (3) the owner's ceiling and version are the frozen ones
    assert CB.COST_BUDGET_CEILING == pytest.approx(2 * r59.GATE_MATERIALITY)
