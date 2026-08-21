"""Release 33 - Predictive Edge Acquisition regression.

These tests protect the properties that decide whether any number the campaign
published means anything. Most of them run on synthetic data and need neither
the vendor nor the network; the artifact tests read what the campaign actually
froze and skip when the research root is absent.

Every guard added to the architecture audit is NEGATIVE-PROBED here: it is shown
failing against a deliberately broken source. A guard that has never been
observed to fail has not been shown to guard anything.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_PKG_PARENT = str(Path(__file__).resolve().parents[2])
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import r33
from paper_trader.alpha_agent.r33 import campaign as _campaign
from paper_trader.alpha_agent.r33 import contract as _contract
from paper_trader.alpha_agent.r33 import economic as _economic
from paper_trader.alpha_agent.r33 import features as _features
from paper_trader.alpha_agent.r33 import lockbox as _lockbox
from paper_trader.alpha_agent.r33 import models as _models
from paper_trader.alpha_agent.r33 import panel as _panel
from paper_trader.alpha_agent.r33 import partition as _partition
from paper_trader.alpha_agent.r33 import predictive as _predictive
from paper_trader.alpha_agent.r33 import registry as _registry
from paper_trader.alpha_agent.r33 import robustness as _robustness
from paper_trader.alpha_agent.r33 import universe as _universe

CAMPAIGN_DIR = r33.campaign_dir(_contract.CAMPAIGN_ID)


def _artifact(name: str):
    path = CAMPAIGN_DIR / name
    if not path.exists():
        pytest.skip(f"{name} not present; run the campaign first")
    return json.loads(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# The contract
# --------------------------------------------------------------------------- #
def test_alpha_result_may_be_pass_only_with_a_qualified_verdict():
    assert _contract.ALPHA_PASS_REQUIRES == _contract.VERDICT_QUALIFIED
    v = _campaign.build_verdict(qualified=[], data_blocked=False,
                                pit_blocked=False,
                                budget_exhausted_without_evaluation=False)
    assert v["primary_verdict"] == _contract.VERDICT_NO_EDGE
    assert v["alpha_result"] == _contract.RESULT_FAIL

    q = _campaign.build_verdict(qualified=["X:001"], data_blocked=False,
                                pit_blocked=False,
                                budget_exhausted_without_evaluation=False)
    assert q["primary_verdict"] == _contract.VERDICT_QUALIFIED
    assert q["alpha_result"] == _contract.RESULT_PASS


def test_a_blocked_campaign_can_never_report_alpha_pass():
    """The failure modes must not become an accidental success."""
    for kwargs in ({"data_blocked": True, "pit_blocked": False,
                    "budget_exhausted_without_evaluation": False},
                   {"data_blocked": False, "pit_blocked": True,
                    "budget_exhausted_without_evaluation": False},
                   {"data_blocked": False, "pit_blocked": False,
                    "budget_exhausted_without_evaluation": True}):
        v = _campaign.build_verdict(qualified=["X:001"], **kwargs)
        assert v["primary_verdict"] != _contract.VERDICT_QUALIFIED
        assert v["alpha_result"] == _contract.RESULT_FAIL


def test_every_target_declares_its_primary_metric_before_validation():
    for target in _contract.TARGETS:
        assert target in _contract.PRIMARY_METRIC
        assert target in _contract.FORECAST_BASELINE


def test_budgets_are_ceilings_and_sum_within_the_total():
    assert sum(_contract.MAX_CONFIGS.values()) >= _contract.MAX_CONFIGS_TOTAL
    assert _contract.MAX_CONFIGS_TOTAL == 170
    assert _contract.ADAPTIVE_SEARCH_ALLOWED is False
    assert _contract.DEEP_LEARNING_IN_SCOPE is False


def test_futures_implementability_is_refused_not_blurred():
    assert _contract.FUTURES_IMPLEMENTABILITY_CLAIMABLE is False
    assert (_contract.UNIVERSE_IMPLEMENTABILITY_STATE
            == _contract.SIGNAL_RESEARCH_VALID)


def test_the_contract_hash_is_stable():
    body = _contract.build(created_at="2026-08-21T00:00:00")
    assert _contract.verify(body)["stable"] is True


def test_release32_is_recorded_as_alpha_fail_not_as_a_success():
    assert _contract.R32_SYSTEM_RESULT == "PASS"
    assert _contract.R32_ALPHA_RESULT == "FAIL"
    assert _contract.R32_QUALIFIED_SLEEVES == 0


def test_superseded_v1_may_not_select_anything_in_v2():
    assert "r33_predictive_edge_v1" in _contract.SUPERSEDED_CAMPAIGNS
    rules = _contract.SUPERSEDED_EVIDENCE_RULES
    assert rules["may_select_finalists"] is False
    assert rules["may_influence_the_lockbox"] is False
    assert rules["may_reduce_the_multiple_testing_denominator"] is False
    assert rules["is_preserved_on_disk"] is True
    v1 = _contract.SUPERSEDED_CAMPAIGNS["r33_predictive_edge_v1"]
    assert v1["lockbox_reopened"] is False
    assert v1["measurements_must_reproduce_exactly"] is True


# --------------------------------------------------------------------------- #
# Universe selection rules
# --------------------------------------------------------------------------- #
def test_the_peg_rule_applies_to_fx_only():
    """A short-duration bond has low volatility for an honest reason."""
    assert _universe.PEG_RULE_APPLIES_TO == ("FX",)
    assert _universe.AC_FX == "FX"


def test_a_named_duplicate_preference_beats_the_longer_history():
    key = ("EURUSD", "DKKUSD")
    assert key in _universe.DUPLICATE_PREFERENCE
    winner, why = _universe.DUPLICATE_PREFERENCE[key]
    assert winner == "EURUSD"
    assert "band" in why.lower()


def test_composites_and_ratios_are_excluded_from_the_prediction_universe():
    for sym in ("$STOXX50", "$BCOM", "$USDX", "#CUGC"):
        assert sym in _universe.COMPOSITE_EXCLUSIONS
    assert "RATIO" in _universe.COMPOSITE_EXCLUSIONS["#CUGC"]


def test_series_diagnostics_measure_administered_and_pegged_behaviour():
    idx = pd.date_range("2000-01-03", periods=600, freq="B")
    pegged = pd.Series(np.r_[np.full(300, 100.0), np.full(300, 100.0)],
                       index=idx)
    d = _universe.series_diagnostics(pegged)
    assert d["zero_return_fraction"] > _universe.MAX_ZERO_RETURN_FRACTION
    assert d["annual_volatility"] < _universe.MIN_ANNUAL_VOLATILITY

    rng = np.random.default_rng(7)
    live = pd.Series(100.0 * np.exp(np.cumsum(rng.normal(0, 0.01, 600))),
                     index=idx)
    d2 = _universe.series_diagnostics(live)
    assert d2["zero_return_fraction"] < 0.01
    assert d2["annual_volatility"] > 0.10


def test_the_currency_diagnostic_reports_evidence_not_a_verdict():
    idx = pd.date_range("2000-01-03", periods=800, freq="B")
    rng = np.random.default_rng(3)
    fx = pd.Series(1.0 * np.exp(np.cumsum(rng.normal(0, 0.006, 800))),
                   index=idx)
    local = pd.Series(100.0 * np.exp(np.cumsum(rng.normal(0, 0.01, 800))),
                      index=idx)
    translated = local * fx
    beta_local = _universe.currency_denomination_diagnostic(local, fx)["beta"]
    beta_trans = _universe.currency_denomination_diagnostic(translated,
                                                            fx)["beta"]
    # A mechanically translated index carries the whole currency move.
    assert abs(beta_local) < 0.3
    assert beta_trans > 0.8


# --------------------------------------------------------------------------- #
# Panel geometry - the leakage controls
# --------------------------------------------------------------------------- #
def test_forecast_dates_do_not_overlap():
    cal = pd.DatetimeIndex(pd.date_range("2000-01-03", periods=3000, freq="B"))
    for h in _contract.HORIZONS:
        idx = _panel.forecast_dates(cal, horizon=h, min_history=252)
        assert len(idx) > 5
        steps = np.diff(idx)
        assert set(steps.tolist()) == {h}, "successive forecasts must not share a day"
        last = idx[-1] + _contract.IMPLEMENTATION_LAG_SESSIONS + h
        assert last < len(cal), "a decision must leave room for its own holding period"


def test_the_implementation_lag_is_one_session_and_is_applied():
    assert _contract.IMPLEMENTATION_LAG_SESSIONS == 1
    cal = pd.DatetimeIndex(pd.date_range("2000-01-03", periods=800, freq="B"))
    prices = pd.DataFrame({"A": np.linspace(100.0, 180.0, 800)}, index=cal)
    panel = {"calendar": cal, "prices": prices,
             "log_returns": np.log(prices).diff(),
             "cash_daily": pd.Series(0.0, index=cal),
             "benchmark": prices["A"], "meta": {"A": {"asset_class": "EQUITY_INDEX",
                                                      "economic_group": "G",
                                                      "currency": "USD"}}}
    obs = _panel.observation_returns(panel, horizon=20)
    i = _panel.forecast_dates(cal, horizon=20)[0]
    expected = prices["A"].iloc[i + 1 + 20] / prices["A"].iloc[i + 1] - 1.0
    assert obs.iloc[0]["A"] == pytest.approx(expected, rel=1e-12)


def test_a_market_that_stops_printing_is_unobserved_not_unchanged():
    assert _panel.MAX_FORWARD_FILL_SESSIONS == 5
    cal = pd.DatetimeIndex(pd.date_range("2000-01-03", periods=200, freq="B"))
    s = pd.Series(100.0, index=cal[:50])
    aligned = _panel._align(s, cal)
    assert aligned.iloc[:50].notna().all()
    assert aligned.iloc[50:55].notna().all(), "a short gap is carried"
    assert aligned.iloc[60:].isna().all(), "a long gap must NOT be carried"


# --------------------------------------------------------------------------- #
# Partition and embargo
# --------------------------------------------------------------------------- #
def test_a_decision_whose_holding_window_crosses_a_boundary_is_embargoed():
    cal = pd.DatetimeIndex(pd.date_range("2012-01-02", periods=600, freq="B"))
    near = pd.Timestamp(_contract.DISCOVERY_END)
    idx = np.arange(0, len(cal))
    dates = cal
    seg = _partition.assign(dates, horizon=60, calendar=cal,
                            decision_index=idx)
    tail = [s for d, s in zip(dates, seg)
            if pd.Timestamp(_contract.DISCOVERY_END) - pd.Timedelta(days=40)
            <= d <= near]
    assert _partition.SEG_EMBARGOED in tail, (
        "decisions that still hold across the boundary must be embargoed")


def test_no_random_split_is_permitted():
    block = _partition.contract_block(20)
    assert block["random_split_allowed"] is False
    assert block["true_forward_is_separate"] is True


def test_segments_are_contiguous_and_ordered():
    assert _contract.PANEL_START < _contract.DISCOVERY_END
    assert _contract.DISCOVERY_END < _contract.VALIDATION_START
    assert _contract.VALIDATION_START < _contract.VALIDATION_END
    assert _contract.VALIDATION_END < _contract.LOCKBOX_START


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
def test_the_scaler_is_fitted_on_training_rows_only():
    rng = np.random.default_rng(11)
    train = rng.normal(0.0, 1.0, size=(500, 3))
    later = rng.normal(50.0, 20.0, size=(200, 3))
    scaler = _models.fit_scaler(train)
    assert max(abs(m) for m in scaler["mu"]) < 0.5
    Z = _models.apply_scaler(scaler, later)
    assert np.abs(Z).max() > 3.0, (
        "the evaluation block must NOT be renormalised onto its own mean")


def test_hierarchical_shrinkage_spans_pooled_and_independent():
    rng = np.random.default_rng(5)
    X = rng.normal(size=(1200, 3))
    g = np.array(["a"] * 600 + ["b"] * 600)
    y = np.where(g == "a", X[:, 0], -X[:, 0]) + rng.normal(0, 0.1, 1200)

    full = _models.fit_hierarchical(X, y, g, alpha=1.0, shrink=1.0)
    indep = _models.fit_hierarchical(X, y, g, alpha=1.0, shrink=0.0)
    assert full["group_coef"]["a"] == full["group_coef"]["b"], (
        "shrink=1 is full pooling: every group shares one model")
    assert indep["group_coef"]["a"][1] > 0.5
    assert indep["group_coef"]["b"][1] < -0.5, (
        "shrink=0 is independent estimation and must recover opposite signs")


def test_hmm_state_beliefs_are_filtered_and_never_smoothed():
    """The filtered belief at t may not react to anything after t."""
    rng = np.random.default_rng(19)
    calm = rng.normal(0.0, 0.5, size=(400, 2))
    wild = rng.normal(0.0, 4.0, size=(400, 2))
    Z = np.vstack([calm, wild])
    spec = _models.fit_hmm(Z, n_states=2, seed=33)
    assert spec["states_are_filtered_only"] is True

    full = _models.hmm_filter_states(spec, Z)
    prefix = _models.hmm_filter_states(spec, Z[:500])
    assert np.allclose(full[:500], prefix, atol=1e-10), (
        "a filtered probability must not change when later data is removed")


def test_the_logistic_head_returns_calibrated_probabilities():
    rng = np.random.default_rng(23)
    X = rng.normal(size=(4000, 2))
    p = 1.0 / (1.0 + np.exp(-(0.8 * X[:, 0])))
    y = (rng.random(4000) < p).astype(float)
    spec = _models.fit_logistic(X, y, alpha=1.0)
    out = _models.predict(spec, X)
    assert out.min() >= 0.0 and out.max() <= 1.0
    assert abs(float(out.mean()) - float(y.mean())) < 0.05


# --------------------------------------------------------------------------- #
# Predictive scoring
# --------------------------------------------------------------------------- #
def test_oos_r2_is_measured_against_the_training_baseline_not_the_test_mean():
    y = np.array([1.0, 2.0, 3.0, 4.0] * 10)
    perfect = y.copy()
    good = _predictive.oos_r2(perfect, y, baseline=0.0)
    assert good["value"] == pytest.approx(1.0)
    # A baseline taken from the evaluation block's own mean would be a leak and
    # would make a useless forecast look neutral rather than bad.
    useless = np.full_like(y, 100.0)
    bad = _predictive.oos_r2(useless, y, baseline=float(y.mean()))
    assert bad["value"] < 0.0


def test_log_loss_skill_is_zero_for_the_base_rate_forecast():
    rng = np.random.default_rng(31)
    y = (rng.random(2000) < 0.55).astype(float)
    base = float(y.mean())
    res = _predictive.log_loss_skill(np.full(2000, base), y, base_rate=base)
    assert res["value"] == pytest.approx(0.0, abs=1e-9)


def test_qlike_rewards_a_better_variance_forecast():
    rng = np.random.default_rng(37)
    true_vol = np.abs(rng.normal(0.2, 0.05, 1500)) + 0.05
    realised = true_vol * np.abs(rng.normal(1.0, 0.15, 1500))
    worse = np.full(1500, float(true_vol.mean()))
    res = _predictive.qlike_skill(true_vol, realised, baseline_forecast=worse)
    assert res["value"] > 0.0


def test_rank_ic_is_computed_per_date_not_pooled_across_dates():
    """Sixty-six markets on one day are not sixty-six independent observations."""
    dates = pd.DatetimeIndex(sum([[pd.Timestamp("2020-01-%02d" % (d + 1))] * 20
                                  for d in range(10)], []))
    rng = np.random.default_rng(41)
    y = rng.normal(size=200)
    ic = _predictive.per_date_rank_ic(y, y, dates)
    assert len(ic) == 10
    assert np.allclose(ic.to_numpy(), 1.0)


# --------------------------------------------------------------------------- #
# The economic judge
# --------------------------------------------------------------------------- #
def test_cost_is_charged_on_traded_notional_both_sides():
    """A rebalance pays on the sell AND the buy."""
    dates = pd.DatetimeIndex(["2020-01-31", "2020-02-28"])
    W = pd.DataFrame({"A": [1.0, -1.0]}, index=dates)
    R = pd.DataFrame({"A": [0.0, 0.0]}, index=dates)
    cash = pd.Series([0.0, 0.0], index=dates)
    meta = {"A": {"asset_class": "EQUITY_INDEX"}}
    path = _economic.evaluate_book(W, R, cash, meta=meta, horizon=20)
    # first period buys 1.0; second period sells 1 and shorts 1 => 2.0 traded
    assert path["traded_notional"][0] == pytest.approx(1.0)
    assert path["traded_notional"][1] == pytest.approx(2.0)
    rate = _contract.COST_PER_SIDE_BPS["EQUITY_INDEX"] / 1e4
    assert path["costs"][1] == pytest.approx(2.0 * rate)


def test_the_volatility_matched_control_carries_the_books_risk():
    rng = np.random.default_rng(43)
    bench = rng.normal(0.01, 0.05, 400)
    book = rng.normal(0.004, 0.025, 400)
    cash = np.full(400, 0.001)
    ctrl = _economic.volatility_matched_control(book, bench, cash)
    assert ctrl["state"] == "OK"
    assert ctrl["weight"] == pytest.approx(
        np.std(book, ddof=1) / np.std(bench, ddof=1), rel=1e-9)
    assert ctrl["weight"] <= 1.0, "no leverage is available to this paper book"


def test_the_control_may_not_lever_even_when_the_book_is_wilder():
    rng = np.random.default_rng(47)
    bench = rng.normal(0.0, 0.01, 300)
    book = rng.normal(0.0, 0.20, 300)
    ctrl = _economic.volatility_matched_control(book, bench,
                                               np.zeros(300))
    assert ctrl["weight"] == 1.0


def test_excess_over_cash_is_reported_but_may_never_rank():
    decl = _economic.judge_declaration()
    assert decl["excess_over_cash_may_rank"] is False
    assert decl["primary_control"] == _contract.ECONOMIC_CONTROL
    assert "CASH" in decl["controls"]


def test_utility_charges_for_risk():
    """Same expected return, more volatility - utility must be lower.

    Scaling a return series multiplies its MEAN as well as its volatility, so
    the risky series would deserve the higher utility. The comparison only
    isolates the risk charge when the mean is held fixed.
    """
    rng = np.random.default_rng(53)
    noise = rng.normal(0.0, 1.0, 400)
    noise -= noise.mean()
    calm = 0.01 + 0.02 * noise
    wild = 0.01 + 0.10 * noise
    assert np.mean(calm) == pytest.approx(np.mean(wild), abs=1e-12)
    assert _economic.utility(wild, horizon=20) < _economic.utility(
        calm, horizon=20)


# --------------------------------------------------------------------------- #
# Registry, budget and lockbox discipline
# --------------------------------------------------------------------------- #
def _registry_for_test():
    return _registry.Registry(campaign_id="test", contract_hash="c",
                              judge_behaviour_hash="j")


def test_a_failed_configuration_stays_in_the_denominator():
    reg = _registry_for_test()
    reg.record(family=_contract.FAMILY_BASELINE, spec={"a": 1},
               stage="VALIDATION", result={"validation": {"state": "OK"}})
    reg.record(family=_contract.FAMILY_BASELINE, spec={"a": 2},
               stage="VALIDATION",
               result={"validation": {"state": "EXECUTION_ERROR"}})
    assert reg.denominator == 2, (
        "a configuration that failed still consumed a look at the data")


def test_a_family_cannot_exceed_its_frozen_budget():
    reg = _registry_for_test()
    cap = _contract.MAX_CONFIGS[_contract.FAMILY_REGIME]
    for i in range(cap):
        reg.record(family=_contract.FAMILY_REGIME, spec={"i": i},
                   stage="VALIDATION", result={})
    with pytest.raises(_registry.BudgetExceeded):
        reg.record(family=_contract.FAMILY_REGIME, spec={"i": cap},
                   stage="VALIDATION", result={})


def test_the_spec_hash_binds_the_contract_and_the_judge_behaviour():
    a = _registry.Registry(campaign_id="t", contract_hash="c1",
                           judge_behaviour_hash="j1")
    b = _registry.Registry(campaign_id="t", contract_hash="c1",
                           judge_behaviour_hash="j2")
    assert a.spec_hash({"x": 1}) != b.spec_hash({"x": 1}), (
        "a result measured under a different judge is a different result")


def test_private_series_are_stripped_from_the_frozen_artifact_only():
    reg = _registry_for_test()
    reg.record(family=_contract.FAMILY_BASELINE, spec={"a": 1},
               stage="VALIDATION",
               result={"validation": {"state": "OK"}, "_series": [1, 2, 3]})
    assert reg.rows[0]["result"]["_series"] == [1, 2, 3], (
        "the caller keeps the working series in memory")
    body = reg.artifact(created_at="2026-08-21")
    assert "_series" not in body["candidates"][0]["result"]


def test_the_lockbox_refuses_a_second_look():
    box = _lockbox.Lockbox(campaign_id="test")
    finalists = [{"candidate_id": "A", "spec_hash": "h1",
                  "family": _contract.FAMILY_POOLED}]
    box.freeze_finalists(finalists, selected_at="now", selection_basis="v")
    box.authorise("h1", family=_contract.FAMILY_POOLED, candidate_id="A",
                  at="now")
    with pytest.raises(_lockbox.LockboxViolation):
        box.authorise("h1", family=_contract.FAMILY_POOLED, candidate_id="A",
                      at="now")


def test_a_candidate_revised_after_a_lockbox_failure_cannot_be_resubmitted():
    box = _lockbox.Lockbox(campaign_id="test")
    box.freeze_finalists(
        [{"candidate_id": "A", "spec_hash": "h1",
          "family": _contract.FAMILY_POOLED}],
        selected_at="now", selection_basis="v")
    box.authorise("h1", family=_contract.FAMILY_POOLED, candidate_id="A",
                  at="now")
    with pytest.raises(_lockbox.LockboxViolation):
        box.authorise("h2-revised", family=_contract.FAMILY_POOLED,
                      candidate_id="A-fixed", at="now")


def test_the_lockbox_cannot_be_opened_before_the_finalists_are_frozen():
    box = _lockbox.Lockbox(campaign_id="test")
    with pytest.raises(_lockbox.LockboxViolation):
        box.authorise("h1", family=_contract.FAMILY_POOLED, candidate_id="A",
                      at="now")


def test_a_family_cannot_buy_extra_lockbox_attempts():
    box = _lockbox.Lockbox(campaign_id="test")
    too_many = [{"candidate_id": f"A{i}", "spec_hash": f"h{i}",
                 "family": _contract.FAMILY_POOLED}
                for i in range(_contract.MAX_LOCKBOX_PER_FAMILY + 1)]
    with pytest.raises(_lockbox.LockboxViolation):
        box.freeze_finalists(too_many, selected_at="now", selection_basis="v")


# --------------------------------------------------------------------------- #
# Robustness gates
# --------------------------------------------------------------------------- #
def test_an_unmeasurable_stability_check_fails_closed():
    res = _robustness.subperiod_stability(
        np.zeros(10), np.zeros(10),
        pd.DatetimeIndex(pd.date_range("2021-01-01", periods=10)), horizon=60)
    assert res["state"] == "INSUFFICIENT_PERIODS"
    assert res["single_subperiod_dependent"] is True, (
        "a stability check that could not run has not been passed")


def test_leave_one_market_out_detects_a_single_market_result():
    dates = pd.DatetimeIndex(pd.date_range("2021-01-29", periods=40, freq="ME"))
    rng = np.random.default_rng(59)
    W = pd.DataFrame({"WINNER": np.full(40, 0.5),
                      "OTHER": np.full(40, 0.5)}, index=dates)
    R = pd.DataFrame({"WINNER": np.full(40, 0.02),
                      "OTHER": rng.normal(0.0, 0.001, 40)}, index=dates)
    cash = pd.Series(0.0, index=dates)
    meta = {"WINNER": {"asset_class": "FX"}, "OTHER": {"asset_class": "FX"}}
    control = np.zeros(40)
    res = _robustness.leave_one_market_out(W, R, cash, meta=meta, horizon=20,
                                           control=control)
    assert res["worst_market_when_removed"] == "WINNER"
    assert res["single_market_dependent"] is True


def test_cost_sensitivity_requires_survival_at_every_multiplier():
    dates = pd.DatetimeIndex(pd.date_range("2021-01-29", periods=30, freq="ME"))
    # Alternating book: high turnover, tiny edge - dies as costs rise.
    W = pd.DataFrame({"A": [1.0, -1.0] * 15}, index=dates)
    R = pd.DataFrame({"A": [0.001, -0.001] * 15}, index=dates)
    cash = pd.Series(0.0, index=dates)
    meta = {"A": {"asset_class": "COMMODITY"}}
    res = _robustness.cost_sensitivity(W, R, cash, meta=meta, horizon=20,
                                       control=np.zeros(30))
    assert res["acceptable"] is False
    assert set(res["by_multiplier"]) == {
        str(m) for m in _contract.COST_SENSITIVITY_MULTIPLIERS}


# --------------------------------------------------------------------------- #
# The qualification gate
# --------------------------------------------------------------------------- #
def _passing_inputs():
    return {
        "candidate_id": "X:001",
        "validation": {"primary_value": 0.05},
        "lockbox": {"primary_value": 0.04,
                    "scored_dates": _contract.MIN_SCORED_FORECAST_DATES,
                    "economic": {"vs_controls": {
                        _contract.ECONOMIC_CONTROL: {"mean_excess": 0.002}},
                        "utility_improvement": 0.01}},
        "robustness": {"parameter_cliff": {"severe_cliff": False},
                       "leave_one_market_out": {"single_market_dependent": False},
                       "subperiod_stability": {"single_subperiod_dependent": False},
                       "cost_sensitivity": {"acceptable": True},
                       "point_in_time_integrity_pass": True},
        "survived_multiple_testing": True,
        "lockbox_accesses": 1,
    }


def test_the_gate_can_actually_pass_a_genuine_candidate():
    """A gate nothing can pass proves nothing when everything fails it."""
    res = _campaign.qualify(**_passing_inputs())
    assert res["qualified"] is True, res["failed_conditions"]


@pytest.mark.parametrize("mutate,expected", [
    (lambda k: k["lockbox"].update({"primary_value": -0.01}),
     "positive_oos_predictive_improvement_vs_baseline"),
    (lambda k: k["validation"].update({"primary_value": -0.01}),
     "predictive_improvement_same_sign_in_validation_and_lockbox"),
    (lambda k: k["lockbox"]["economic"]["vs_controls"].__setitem__(
        _contract.ECONOMIC_CONTROL, {"mean_excess": -0.001}),
     "positive_after_cost_excess_vs_risk_matched_control"),
    (lambda k: k["lockbox"]["economic"].update({"utility_improvement": -0.01}),
     "positive_after_cost_utility_improvement"),
    (lambda k: k.update({"survived_multiple_testing": False}),
     "survives_multiple_testing_procedure"),
    (lambda k: k["robustness"]["parameter_cliff"].update(
        {"severe_cliff": True}), "no_severe_parameter_cliff"),
    (lambda k: k["robustness"]["leave_one_market_out"].update(
        {"single_market_dependent": True}), "not_dependent_on_a_single_market"),
    (lambda k: k["robustness"]["subperiod_stability"].update(
        {"single_subperiod_dependent": True}),
     "not_dependent_on_a_single_subperiod"),
    (lambda k: k["robustness"]["cost_sensitivity"].update({"acceptable": False}),
     "acceptable_cost_sensitivity"),
    (lambda k: k.update({"lockbox_accesses": 2}),
     "lockbox_accessed_exactly_once"),
])
def test_every_qualification_condition_can_fail(mutate, expected):
    """Negative probe: each condition must be capable of rejecting."""
    kwargs = _passing_inputs()
    mutate(kwargs)
    res = _campaign.qualify(**kwargs)
    assert res["qualified"] is False
    assert expected in res["failed_conditions"]


def test_a_verdict_cannot_rest_on_fewer_scored_dates_than_the_contract_allows():
    kwargs = _passing_inputs()
    kwargs["lockbox"]["scored_dates"] = _contract.MIN_SCORED_FORECAST_DATES - 1
    res = _campaign.qualify(**kwargs)
    assert res["scored_dates_sufficient"] is False
    assert res["qualified"] is False
    assert "positive_oos_predictive_improvement_vs_baseline" in \
        res["failed_conditions"]


# --------------------------------------------------------------------------- #
# Feature registry
# --------------------------------------------------------------------------- #
def test_carry_is_declared_absent_where_the_estate_cannot_support_it():
    assert _features.CARRY_AVAILABILITY[_universe.AC_FX] is False
    assert _features.CARRY_AVAILABILITY[_universe.AC_COMMODITY] is False
    assert _features.CARRY_AVAILABILITY[_universe.AC_GOVT] is True
    for ac in (_universe.AC_FX, _universe.AC_COMMODITY):
        assert ac in _features.CARRY_ABSENCE_REASON


def test_a_structurally_absent_feature_is_not_filled_with_a_median():
    assert "bond_carry_slope" in _features.STRUCTURALLY_ABSENT_FILL_ZERO


def test_global_state_is_lagged_before_it_is_broadcast():
    assert _features.GLOBAL_STATE_LAG_SESSIONS >= 1


# --------------------------------------------------------------------------- #
# Frozen artifacts
# --------------------------------------------------------------------------- #
def test_the_campaign_artifacts_exist_and_carry_the_safety_block():
    required = ["research_contract.json", "data_inventory.json",
                "futures_universe.json", "pit_information_manifest.json",
                "feature_registry.json", "candidate_registry.json",
                "predictive_results.json", "economic_results.json",
                "multiple_testing.json", "lockbox_manifest.json",
                "robustness_results.json", "final_verdict.json"]
    for name in required:
        body = _artifact(name)
        safety = body.get("safety_block") or {}
        assert safety.get("creates_order") is False, name
        assert safety.get("promotes_model") is False, name
        assert safety.get("writes_operational_store") is False, name
        assert safety.get("may_spend_money") is False, name


def test_the_verdict_reports_both_results_and_does_not_bury_alpha():
    v = _artifact("final_verdict.json")
    assert v["system_result"] in ("PASS", "FAIL")
    assert v["alpha_result"] in ("PASS", "FAIL")
    assert v["primary_verdict"] in _contract.PRIMARY_VERDICTS
    if v["alpha_result"] == "PASS":
        assert v["primary_verdict"] == _contract.VERDICT_QUALIFIED
        assert v["qualified_candidates"]


def test_the_denominator_counts_every_executed_configuration():
    reg = _artifact("candidate_registry.json")
    v = _artifact("final_verdict.json")
    assert reg["denominator_executed_configurations"] == len(reg["candidates"])
    assert (v["denominator_executed_configurations"]
            == reg["denominator_executed_configurations"])
    assert reg["denominator_executed_configurations"] <= \
        _contract.MAX_CONFIGS_TOTAL


def test_the_lockbox_was_opened_at_most_once_per_finalist():
    box = _artifact("lockbox_manifest.json")
    served = [a["spec_hash"] for a in box["accesses"]]
    assert len(served) == len(set(served))
    assert box["access_count"] <= _contract.MAX_LOCKBOX_FINALISTS
    assert box["selection_used_lockbox"] is False
    assert box["retuning_after_lockbox_allowed"] is False


def test_the_point_in_time_probe_actually_ran_and_passed():
    v = _artifact("final_verdict.json")
    probe = v["point_in_time_probe"]
    assert probe["rows_checked"] > 1000
    assert probe["rows_mismatched"] == 0
    assert probe["pass"] is True


def test_the_universe_is_declared_signal_research_valid_only():
    uni = _artifact("futures_universe.json")
    assert uni["implementability"]["state"] == _contract.SIGNAL_RESEARCH_VALID
    assert uni["implementability"]["futures_implementability_claimable"] is False
    for m in uni["markets"]:
        assert m["implementability_state"] == _contract.SIGNAL_RESEARCH_VALID


def test_the_continuous_futures_finding_is_recorded_as_measured():
    inv = _artifact("data_inventory.json")
    cf = inv["continuous_futures_entitlement"]
    assert cf["count"] == len(cf["symbols"])
    assert cf["count"] <= 2, "the measured entitlement is one market"


def test_no_artifact_is_written_outside_the_research_root():
    root = r33.research_root().resolve()
    assert CAMPAIGN_DIR.resolve().is_relative_to(root)


def test_the_pit_manifest_excludes_revised_and_synthetic_information():
    pit = _artifact("pit_information_manifest.json")
    assert pit["spend"]["amount_spent"] == 0.0
    excluded = pit["excluded"]
    assert excluded["norgate_revised_economic_series"]["count"] == 106
    assert excluded["synthetic_or_proxy_event_data"]["state"] \
        == "NOT_PIT_ADMISSIBLE"


# --------------------------------------------------------------------------- #
# Negative probes of the architecture guards
# --------------------------------------------------------------------------- #
def _audit():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import audit_architecture as A
    return A


def test_the_release33_audit_check_passes_on_the_real_source():
    A = _audit()
    res = A.check_release33_predictive_edge(A._iter_source_files())
    failures = {k: v for k, v in res.items()
                if k != "modules_missing" and v is not True and v != []}
    assert not failures, failures


@pytest.mark.parametrize("owner,broken,key", [
    ("contract", "FUTURES_IMPLEMENTABILITY_CLAIMABLE = True",
     "futures_implementability_refused"),
    ("contract", "ADAPTIVE_SEARCH_ALLOWED = True", "adaptive_search_refused"),
    ("contract", "DEEP_LEARNING_IN_SCOPE = True", "deep_learning_out_of_scope"),
    ("contract", "DENOMINATOR_COUNTS_ALL_EXECUTED = False",
     "denominator_counts_all_executed"),
    ("contract", "RETUNING_AFTER_LOCKBOX_ALLOWED = True",
     "lockbox_single_access"),
    ("contract", 'COST_BASE = "ONE_WAY_TURNOVER"', "cost_base_traded_notional"),
])
def test_the_audit_guards_can_fail(monkeypatch, owner, broken, key):
    """Each guard is shown REJECTING a deliberately broken source."""
    A = _audit()
    real = A._read

    original = {name: (real(path) or "")
                for name, path in A.R33_OWNERS.items()}
    target_path = A.R33_OWNERS[owner]
    # Break exactly one declaration by flipping its declared value.
    flipped = original[owner]
    for good in (broken.split(" = ")[0] + " = " + v
                 for v in ("True", "False")):
        if good in flipped and good != broken:
            flipped = flipped.replace(good, broken)
            break
    else:
        flipped = flipped.replace('COST_BASE = "TRADED_NOTIONAL"', broken)

    def fake_read(path, *a, **kw):
        if str(path).replace("\\", "/") == target_path:
            return flipped
        return real(path, *a, **kw)

    monkeypatch.setattr(A, "_read", fake_read)
    res = A.check_release33_predictive_edge(A._iter_source_files())
    assert res[key] is not True, (
        f"guard {key} did not notice {broken!r}; a guard that never fails is "
        f"not a guard")


# --------------------------------------------------------------------------- #
# operational-write ATTRIBUTION
#
# The first validation gate inferred causality from a timestamp: any file under
# a protected store whose mtime landed on the campaign day was reported as
# written by Release 33. Paper Trader runs a long-lived Release-29 continuous
# information-collection service whose canonical job is to advance exactly that
# class of file on a 60-second cadence, so the gate blocked a clean commit
# because production was doing its job. These tests hold the repaired rule to
# both halves: an R33 write must still fail, and a canonical service heartbeat
# must not.
# --------------------------------------------------------------------------- #
def _attrib():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import r33_operational_write_attribution as W
    return W


SERVICE = "PAPER_TRADER_INFORMATION_COLLECTION"
_DAY = "2026-08-21"


def _write(path: Path, blob) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(blob) if not isinstance(blob, str) else blob,
                    encoding="utf-8")
    return path


def _canonical_store(root: Path, *, instance="inst-1", pid=4242,
                     iteration="collect_20260821T153640_04e0997c") -> Path:
    """A store in exactly the shape the running collection service leaves it."""
    d = root / "information_collection"
    _write(d / "collection_service_state.json", {
        "schema_version": "1.0.0", "service_id": SERVICE,
        "instance_id": instance, "pid": pid,
        "heartbeat_at": "2026-08-21T15:36:51.829845+00:00",
        "last_iteration_id": iteration})
    _write(d / "collection_service.lock", {
        "service_id": SERVICE, "instance_id": instance, "pid": pid,
        "acquired_at": "2026-08-21T15:15:03.626323+00:00",
        "heartbeat_at": "2026-08-21T15:36:51.829845+00:00"})
    _write(d / "collection_iteration_history.json", {
        "schema_version": "1.0.0", "service_id": SERVICE, "bounded_to": 500,
        "iterations": [{"service_id": SERVICE, "instance_id": instance,
                        "pid": pid, "iteration_id": iteration,
                        "state": "NO_NEW_INFORMATION"}]})
    _write(d / "source_runtime_health.json",
           {"schema_version": "1.0.0", "service_id": SERVICE, "sources": {}})
    _write(d / "logs" / "collection_service.log",
           json.dumps({"at": "2026-08-21T15:36:51.829285+00:00",
                       "event": "iteration", "iteration_id": iteration,
                       "state": "NO_NEW_INFORMATION"}) + "\n")
    return d


def test_canonical_collection_heartbeat_is_not_an_r33_write(tmp_path):
    """The exact failure that blocked the operator: the service advanced its
    own lock, state, history, health and log, and the gate called it R33."""
    W = _attrib()
    _canonical_store(tmp_path)
    rep = W.attribute(data_root=tmp_path, since_day=_DAY, repo=None)
    ic = rep["roots"]["information_collection"]
    assert ic["state"] == W.ATTRIBUTED, ic
    assert ic["checked"] == 5, ic["checked"]
    assert len(ic["attributed"]) == 5, ic["attributed"]
    assert {a["writer"] for a in ic["attributed"]} == {SERVICE}
    assert rep["r33_attributable"] == []
    assert rep["unattributed"] == []
    assert rep["ok"] is True


def test_an_r33_attributable_write_fails(tmp_path):
    """A Release-33 marker inside an operational file is an R33 write, no
    matter which directory it landed in or how plausible the filename is."""
    W = _attrib()
    d = _canonical_store(tmp_path)
    _write(d / "collection_service_state.json", {
        "schema_version": "1.0.0", "service_id": SERVICE,
        "instance_id": "inst-1", "pid": 4242,
        "written_by": "alpha_agent.r33.campaign",
        "campaign_id": "r33_predictive_edge_v2"})
    rep = W.attribute(data_root=tmp_path, since_day=_DAY, repo=None)
    assert rep["state"] == W.R33_ATTRIBUTABLE, rep["state"]
    assert rep["ok"] is False
    assert any("collection_service_state.json" in x
               for x in rep["r33_attributable"]), rep["r33_attributable"]


def test_an_r33_write_to_a_strict_store_fails(tmp_path):
    """Stores with no independent writer keep the strict rule: there is nobody
    else it could have been."""
    W = _attrib()
    _canonical_store(tmp_path)
    _write(tmp_path / "portfolio_decisions" / "decision_20260821.json",
           {"decision": "anything"})
    rep = W.attribute(data_root=tmp_path, since_day=_DAY, repo=None)
    assert rep["state"] == W.R33_ATTRIBUTABLE
    assert rep["ok"] is False
    assert any("portfolio_decisions" in x for x in rep["r33_attributable"])


def test_an_unknown_file_under_the_protected_root_fails(tmp_path):
    """A file the declared owner does not own is not acquitted by sitting in
    the owner's directory. Directory-level trust is exactly the hole."""
    W = _attrib()
    d = _canonical_store(tmp_path)
    _write(d / "something_else.json", {"service_id": SERVICE})
    rep = W.attribute(data_root=tmp_path, since_day=_DAY, repo=None)
    assert rep["state"] == W.UNATTRIBUTED, rep["state"]
    assert rep["ok"] is False
    assert any("UNRECOGNISED_FILE_UNDER_PROTECTED_ROOT" in x
               for x in rep["unattributed"]), rep["unattributed"]


@pytest.mark.parametrize("mutate,expected", [
    ("foreign_service_id", "WRITER_PROVENANCE_NOT_THE_DECLARED_SERVICE"),
    ("no_service_id", "WRITER_PROVENANCE_NOT_THE_DECLARED_SERVICE"),
    ("unparseable", "ATTRIBUTION_ERROR"),
    ("unknown_log_writer", "SERVICE_LOG_RECORD_NAMES_AN_UNKNOWN_WRITER"),
    ("log_not_ndjson", "SERVICE_LOG_RECORD_IS_NOT_JSON"),
    ("history_without_identity", "LATEST_ITERATION_CARRIES_NO_WRITER_IDENTITY"),
])
def test_attribution_failure_fails_closed(tmp_path, mutate, expected):
    """Unmeasurable is not innocent. Every way of failing to establish the
    writer has to block the commit rather than be waved through."""
    W = _attrib()
    d = _canonical_store(tmp_path)
    if mutate == "foreign_service_id":
        _write(d / "collection_service_state.json",
               {"service_id": "SOMETHING_ELSE", "instance_id": "x", "pid": 1})
    elif mutate == "no_service_id":
        _write(d / "source_runtime_health.json", {"sources": {}})
    elif mutate == "unparseable":
        _write(d / "source_runtime_health.json", "{ not json at all")
    elif mutate == "unknown_log_writer":
        _write(d / "logs" / "collection_service.log",
               json.dumps({"at": "2026-08-21T15:36:51Z", "event": "iteration",
                           "iteration_id": "collect_FROM_NOWHERE"}) + "\n")
    elif mutate == "log_not_ndjson":
        _write(d / "logs" / "collection_service.log",
               "2026-08-21 iteration finished\n")
    elif mutate == "history_without_identity":
        _write(d / "collection_iteration_history.json", {
            "service_id": SERVICE, "bounded_to": 500,
            "iterations": [{"service_id": SERVICE, "iteration_id": "x"}]})
    rep = W.attribute(data_root=tmp_path, since_day=_DAY, repo=None)
    assert rep["ok"] is False, mutate
    assert rep["state"] == W.UNATTRIBUTED, rep["state"]
    assert any(expected in x for x in rep["unattributed"]), rep["unattributed"]


def test_incoherent_service_identity_fails_closed(tmp_path):
    """State and lock are written in the same heartbeat. If they disagree about
    who is writing, the writer is not established and the gate must not guess."""
    W = _attrib()
    d = _canonical_store(tmp_path)
    _write(d / "collection_service.lock", {
        "service_id": SERVICE, "instance_id": "a-different-instance",
        "pid": 999})
    rep = W.attribute(data_root=tmp_path, since_day=_DAY, repo=None)
    assert rep["ok"] is False
    assert any("SERVICE_IDENTITY_INCOHERENT" in x
               for x in rep["unattributed"]), rep["unattributed"]


def test_untouched_operational_stores_are_not_examined(tmp_path):
    """The mtime window still bounds WHICH files are examined - it just no
    longer decides WHO wrote them. Yesterday's cycle is not re-litigated."""
    W = _attrib()
    _canonical_store(tmp_path)
    rep = W.attribute(data_root=tmp_path, since_day="2099-01-01", repo=None)
    assert rep["roots"]["information_collection"]["checked"] == 0
    assert rep["ok"] is True


def test_the_gate_cannot_ignore_the_information_collection_directory():
    """The cheapest way to make this gate green is to drop the root from the
    protected set or hand it an empty owned-file list. Both are caught."""
    W = _attrib()
    repo = Path(__file__).resolve().parents[1]
    decl = W.check_owner_declarations(repo)
    assert decl["ok"] is True, decl
    assert "information_collection" in W.OPERATIONAL_ROOTS
    assert "information_collection" in W.CONTINUOUS_SERVICE_ROOTS
    assert decl["owner_constants_match"] is True, decl["owner_constants_detail"]
    spec = W.CONTINUOUS_SERVICE_ROOTS["information_collection"]
    assert spec["provenance_required"] is True
    assert spec["json_files"] and spec["log_files"]
    # An exception is a NAMED OWNER, never a directory skip.
    assert spec["service_id"] == SERVICE
    assert spec["owner_module"] == "api.information_collection"


@pytest.mark.parametrize("break_it", [
    "drop_from_protection", "empty_owned_files", "no_provenance",
    "both_lists", "strict_and_continuous_overlap",
])
def test_declaration_guard_is_negative_probed(monkeypatch, break_it):
    """A guard that has never been observed to fail has not been shown to
    guard anything."""
    W = _attrib()
    spec = dict(W.CONTINUOUS_SERVICE_ROOTS["information_collection"])
    if break_it == "drop_from_protection":
        monkeypatch.setattr(W, "CONTINUOUS_SERVICE_ROOTS", {})
        monkeypatch.setattr(W, "OPERATIONAL_ROOTS",
                            tuple(W.STRICT_OPERATIONAL_ROOTS))
    elif break_it == "empty_owned_files":
        spec["json_files"] = ()
        monkeypatch.setattr(W, "CONTINUOUS_SERVICE_ROOTS",
                            {"information_collection": spec})
    elif break_it == "no_provenance":
        spec["provenance_required"] = False
        monkeypatch.setattr(W, "CONTINUOUS_SERVICE_ROOTS",
                            {"information_collection": spec})
    elif break_it == "both_lists":
        monkeypatch.setattr(W, "OPERATIONAL_ROOTS",
                            tuple(W.STRICT_OPERATIONAL_ROOTS))
    elif break_it == "strict_and_continuous_overlap":
        monkeypatch.setattr(
            W, "STRICT_OPERATIONAL_ROOTS",
            tuple(W.STRICT_OPERATIONAL_ROOTS) + ("information_collection",))
    decl = W.check_owner_declarations(None)
    assert decl["ok"] is False, (break_it, decl)


def test_r33_source_has_no_operational_write_path():
    """The static half of the invariant. It runs whether or not any store
    changed, so a quiet directory can never be mistaken for a clean campaign."""
    W = _attrib()
    repo = Path(__file__).resolve().parents[1]
    res = W.r33_source_operational_write_paths(repo)
    assert res["sources_scanned"] >= 16, res["sources_scanned"]
    assert res["clean"] is True, res["findings"][:5]


@pytest.mark.parametrize("injected,kind", [
    ('STORE = "information_collection"', "PROTECTED_ROOT_NAME"),
    ('F = "collection_service_state.json"', "OWNED_FILE_NAME"),
    ("from api.information_collection import heartbeat",
     "OPERATIONAL_OWNER_REF"),
    ("run_collection_iteration(root=None)", "MUTATING_OWNER_CALL"),
])
def test_source_guard_catches_an_injected_write_path(tmp_path, injected, kind):
    """Each way an R33 module could reach an operational store is caught."""
    W = _attrib()
    pkg = tmp_path / "alpha_agent" / "r33"
    pkg.mkdir(parents=True)
    (pkg / "campaign.py").write_text(
        "import numpy as np\n" + injected + "\n", encoding="utf-8")
    res = W.r33_source_operational_write_paths(tmp_path)
    assert res["clean"] is False, injected
    assert kind in {f["kind"] for f in res["findings"]}, res["findings"]


def test_frozen_r33_evidence_hashes_are_unchanged():
    """The safety-gate repair changed no measurement. Every frozen artifact
    must still hash to the value recorded before it."""
    W = _attrib()  # noqa: F841 - the repair under test is the reason to check
    root = r33.research_root()
    cdir = Path(root) / _contract.CAMPAIGN_ID
    if not cdir.exists():
        pytest.skip("research root absent")
    recorded = Path(r"D:\Temp\paper_trader_release33_predictive_edge_handoff"
                    r"\evidence\artifact_hashes.json")
    if not recorded.exists():
        pytest.skip("handoff evidence absent")
    want = json.loads(recorded.read_text(encoding="utf-8"))["artifacts"]
    import hashlib
    drift = []
    for name, meta in sorted(want.items()):
        p = cdir / name
        if not p.exists():
            drift.append(f"{name}:MISSING")
            continue
        got = hashlib.sha256(p.read_bytes()).hexdigest()
        if got != meta["sha256"]:
            drift.append(f"{name}:{got[:12]}!={meta['sha256'][:12]}")
    assert not drift, drift
    verdict = json.loads((cdir / "final_verdict.json").read_text("utf-8"))
    assert verdict["primary_verdict"] == "R33_NO_PREDICTIVE_EDGE"
    assert verdict["system_result"] == "PASS"
    assert verdict["alpha_result"] == "FAIL"
    assert verdict["qualified_candidates"] == []
    assert verdict["denominator_executed_configurations"] == 105


@pytest.mark.parametrize("key,strip", [
    ("attribution_is_provenance_based",
     "WRITER_PROVENANCE_NOT_THE_DECLARED_SERVICE"),
    ("attribution_is_provenance_based",
     "UNRECOGNISED_FILE_UNDER_PROTECTED_ROOT"),
    ("attribution_fails_closed", "Unmeasurable is not innocent"),
    ("attribution_refuses_time_whitelist", "never time-specific"),
])
def test_attribution_audit_guards_are_negative_probed(monkeypatch, key, strip):
    """Each new audit invariant is shown failing against a source that lost the
    property it asserts."""
    A = _audit()
    real = A._read
    target = "scripts/r33_operational_write_attribution.py"
    broken = real(target).replace(strip, "REMOVED")

    def fake_read(path, *a, **kw):
        if str(path).replace("\\", "/") == target:
            return broken
        return real(path, *a, **kw)

    monkeypatch.setattr(A, "_read", fake_read)
    res = A.check_release33_predictive_edge(A._iter_source_files())
    assert res[key] is not True, (
        f"guard {key} survived removal of {strip!r}; a guard that never fails "
        f"is not a guard")


def test_audit_notices_a_missing_attribution_owner(monkeypatch):
    """Deleting the attribution rule must block the build, not silently
    restore the mtime-as-causality behaviour."""
    A = _audit()
    real = A._read
    target = "scripts/r33_operational_write_attribution.py"

    def fake_read(path, *a, **kw):
        if str(path).replace("\\", "/") == target:
            return ""
        return real(path, *a, **kw)

    monkeypatch.setattr(A, "_read", fake_read)
    res = A.check_release33_predictive_edge(A._iter_source_files())
    assert res["attribution_owner_present"] is False
    assert res["attribution_is_provenance_based"] is False


def test_audit_notices_information_collection_losing_protection(monkeypatch):
    """The functional half of the invariant: a docstring promising provenance
    while the protected set has quietly lost a root is exactly what a
    substring check cannot see."""
    A = _audit()
    W = _attrib()
    monkeypatch.setattr(W, "CONTINUOUS_SERVICE_ROOTS", {})
    monkeypatch.setattr(W, "OPERATIONAL_ROOTS",
                        tuple(W.STRICT_OPERATIONAL_ROOTS))
    res = A.check_release33_predictive_edge(A._iter_source_files())
    assert res["information_collection_still_protected"] is not True


def test_audit_notices_an_operational_write_path_in_r33_source(monkeypatch):
    """If an R33 module ever gains a way to address an operational store, the
    audit blocks the build whether or not any store happened to change."""
    A = _audit()
    W = _attrib()
    real = W.r33_source_operational_write_paths
    monkeypatch.setattr(
        W, "r33_source_operational_write_paths",
        lambda repo: {"sources_scanned": 17, "clean": False,
                      "findings": [{"file": "alpha_agent/r33/campaign.py",
                                    "kind": "PROTECTED_ROOT_NAME",
                                    "token": "information_collection"}]})
    res = A.check_release33_predictive_edge(A._iter_source_files())
    assert res["r33_source_has_no_operational_write_path"] is not True
    monkeypatch.setattr(W, "r33_source_operational_write_paths", real)
