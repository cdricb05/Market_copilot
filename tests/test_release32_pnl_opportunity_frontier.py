"""Release 32 - PnL Opportunity Frontier targeted contracts.

Covers the data-source registry and its MEASURED point-in-time classifier,
InformationState timestamp semantics, the Strategy Sleeve contract, each
sleeve's deterministic core behaviour, the common economic judge (cash,
transaction costs, the volatility-matched control, marginal portfolio value),
common-overlap comparison, the bounded funnel and multiple-testing denominator,
the lockbox, the Information Purchase Gate, the Daily Multi-Asset Governance
contract (market-calendar states, stale-data blocking, event-fabric reuse,
no-churn hysteresis), the read-only API, UI structure, and the production
read-only proof.

Guards are NEGATIVE-PROBED wherever one exists: a guard that has never been
shown to fail is not proven, and Release 31 shipped two checks that reported OK
for work they never did.
"""
from __future__ import annotations

import importlib
import json
import re
from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent import r32
from paper_trader.alpha_agent.r32 import contract as _contract
from paper_trader.alpha_agent.r32 import frontier as _frontier
from paper_trader.alpha_agent.r32 import funnel as _funnel
from paper_trader.alpha_agent.r32 import governance as _gov
from paper_trader.alpha_agent.r32 import information_state as _istate
from paper_trader.alpha_agent.r32 import judge as _judge
from paper_trader.alpha_agent.r32 import panels as _panels
from paper_trader.alpha_agent.r32 import purchase_gate as _gate
from paper_trader.alpha_agent.r32 import sleeve as _sleeve
from paper_trader.alpha_agent.r32 import sources as _sources
from paper_trader.alpha_agent.r32.sleeves import cross_asset_trend as _cat
from paper_trader.alpha_agent.r32.sleeves import equity_beta_timing as _ebt
from paper_trader.alpha_agent.r32.sleeves import equity_selection as _es
from paper_trader.alpha_agent.r32.sleeves import event_driven as _ed
from paper_trader.alpha_agent.r32.sleeves import sector_rotation as _sr
from paper_trader.alpha_agent.r32.sleeves import volatility_risk_regime as _vrr

REPO = Path(__file__).resolve().parents[1]
UI = REPO / "api" / "ui" / "index.html"
APP = REPO / "api" / "app.py"
READ_MODEL = REPO / "api" / "pnl_opportunity_frontier.py"


# --------------------------------------------------------------------------- #
# Data source registry + the measured point-in-time classifier
# --------------------------------------------------------------------------- #
def test_every_zero_cost_source_declares_zero_marginal_cost():
    """Release 32 may spend nothing, so every source must already be paid for."""
    assert r32.MAY_SPEND_MONEY is False
    for s in _sources.ZERO_COST_SOURCES:
        assert float(s["marginal_cost_usd"]) == 0.0, s["source_id"]


def test_a_daily_series_that_changes_every_day_is_market_observable():
    v = _sources.classify_change_fingerprint(
        change_days=list(range(1, 29)) * 10, n_observations=300, n_changes=280)
    assert v["admissibility"] == _sources.PIT_MARKET_OBSERVABLE


def test_a_statistic_stamped_at_period_start_is_not_point_in_time():
    """The Release-32 headline finding, as arithmetic.

    Every owned macro statistical release changes value on the FIRST BUSINESS
    DAY OF THE PERIOD IT MEASURES - CPI for month M appears on day one of month
    M, roughly six weeks before publication. Reading it at its own timestamp is
    look-ahead every single period.
    """
    v = _sources.classify_change_fingerprint(
        change_days=[1, 1, 2, 1, 3, 1, 2, 1, 1, 4] * 3,
        n_observations=630, n_changes=30)
    assert v["admissibility"] == _sources.REVISED_NOT_PIT
    assert v["reason"] == (
        "STAMPED_AT_REFERENCE_PERIOD_START_NOT_PUBLICATION_DATE")
    assert v["period_start_fraction"] == 1.0


def test_revised_macro_is_not_admissible_as_history():
    assert _sources.REVISED_NOT_PIT not in _sources.ADMISSIBLE_FOR_HISTORY
    assert _sources.CURRENT_SNAPSHOT_ONLY not in _sources.ADMISSIBLE_FOR_HISTORY
    assert _sources.PIT_MARKET_OBSERVABLE in _sources.ADMISSIBLE_FOR_HISTORY


def test_a_mid_month_publication_pattern_is_not_silently_admitted():
    """NEGATIVE PROBE: infrequent updates that do NOT land at period start.

    Such a series might be genuinely release-dated - or might not be. The
    classifier must refuse to guess, rather than defaulting either way.
    """
    v = _sources.classify_change_fingerprint(
        change_days=[12, 13, 11, 14, 12, 13, 12, 11, 15, 12],
        n_observations=210, n_changes=10)
    assert v["admissibility"] == _sources.COVERAGE_LIMITED
    assert v["admissibility"] not in _sources.ADMISSIBLE_FOR_HISTORY


def test_prohibited_substitutions_name_every_known_trap():
    text = json.dumps(_sources.PROHIBITED_SUBSTITUTIONS)
    for phrase in ("revised macro", "current analyst snapshots",
                   "GDELT article text", "external reference links",
                   "current sector membership", "ETF history"):
        assert phrase in text, phrase


# --------------------------------------------------------------------------- #
# InformationState
# --------------------------------------------------------------------------- #
def test_information_state_declares_four_distinct_timestamps():
    assert _istate.TIMESTAMPS == ("observed_at", "published_at", "effective_at",
                                  "eligible_for_decision_at")


def test_an_observation_cannot_be_read_before_it_is_eligible():
    obs = _istate.Observation(
        source_id="norgate_economic", key="cpi", value=1.0,
        observed_at="2026-03-01", published_at="2026-04-12",
        effective_at="2026-03-31", eligible_for_decision_at="2026-04-12")
    state = _istate.InformationState(decision_at="2026-04-01")
    with pytest.raises(_istate.LookAheadViolation):
        state.add(obs)
    assert state.try_add(obs) is False
    later = _istate.InformationState(decision_at="2026-04-20")
    later.add(obs)
    assert later.get("norgate_economic", "cpi") == 1.0


def test_eligibility_may_not_precede_publication():
    """NEGATIVE PROBE: the impossible timestamp is refused at construction."""
    with pytest.raises(_istate.LookAheadViolation):
        _istate.Observation(source_id="x", key="k", value=1,
                            observed_at="2026-01-01",
                            published_at="2026-02-01",
                            eligible_for_decision_at="2026-01-15")


def test_an_inadmissible_source_cannot_enter_an_information_state():
    obs = _istate.Observation(
        source_id="norgate_economic", key="gdp", value=1.0,
        observed_at="2020-01-01", admissibility=_sources.REVISED_NOT_PIT)
    with pytest.raises(_istate.LookAheadViolation):
        _istate.InformationState(decision_at="2026-01-01").add(obs)


def test_no_publication_lag_can_repair_a_revised_series():
    assert _istate.publication_lag_days(_sources.REVISED_NOT_PIT) is None
    assert _istate.publication_lag_days(_sources.PIT_MARKET_OBSERVABLE) == 0


# --------------------------------------------------------------------------- #
# Strategy Sleeve contract
# --------------------------------------------------------------------------- #
def test_a_sleeve_never_owns_capital():
    opp = _sleeve.StrategyOpportunity(
        sleeve=_contract.SLEEVE_CROSS_ASSET_TREND, decision_date="2020-01-02",
        direction=_sleeve.DIRECTION_ROTATE, conviction=0.5,
        recommended_exposure={"EQUITY_US": 0.4})
    assert opp.owns_capital is False
    assert opp.creates_portfolio_target is False
    assert opp.creates_proposal is False
    assert opp.creates_order is False
    assert opp.research_book_is_not_a_portfolio_target is True
    assert _sleeve.STATES_THAT_OWN_CAPITAL == ()


def test_a_sleeve_may_not_lever_or_size_a_book():
    """NEGATIVE PROBE: gross exposure above 1.0 is refused."""
    with pytest.raises(_sleeve.SleeveViolation):
        _sleeve.StrategyOpportunity(
            sleeve=_contract.SLEEVE_CROSS_ASSET_TREND,
            decision_date="2020-01-02", direction=_sleeve.DIRECTION_LONG,
            conviction=1.0,
            recommended_exposure={"EQUITY_US": 0.8, "BOND_US_IG": 0.5})


def test_unallocated_weight_becomes_cash_not_a_rounding_error():
    opp = _sleeve.StrategyOpportunity(
        sleeve=_contract.SLEEVE_CROSS_ASSET_TREND, decision_date="2020-01-02",
        direction=_sleeve.DIRECTION_ROTATE, conviction=0.3,
        recommended_exposure={"EQUITY_US": 0.3})
    assert opp.cash_weight == pytest.approx(0.7)


def test_a_zero_exposure_opinion_is_legitimate():
    opp = _sleeve.StrategyOpportunity(
        sleeve=_contract.SLEEVE_EQUITY_BETA_TIMING, decision_date="2020-01-02",
        direction=_sleeve.DIRECTION_FLAT, conviction=1.0,
        recommended_exposure={})
    assert opp.cash_weight == pytest.approx(1.0)


def test_normalise_scales_down_but_never_inflates_conviction():
    """A 30 % opinion stays a 30 % opinion.

    Release 31's campaign v2 pinned cash to zero and forced a model that found
    nothing worth owning to hold 25 names.
    """
    kept = _sleeve.normalise_exposure({"A": 0.2, "B": 0.1})
    assert sum(kept.values()) == pytest.approx(0.3)
    scaled = _sleeve.normalise_exposure({"A": 1.0, "B": 1.0})
    assert sum(scaled.values()) == pytest.approx(1.0)


def test_spec_hash_binds_judge_behaviour_not_a_schema_name():
    spec = _sleeve.SleeveSpec(sleeve=_contract.SLEEVE_SECTOR_ROTATION,
                              family="momentum", params={"lookback": 126, "k": 3},
                              generate=lambda *a: [])
    assert spec.spec_hash("judge-A") != spec.spec_hash("judge-B")


# --------------------------------------------------------------------------- #
# Sleeve deterministic core behaviour
# --------------------------------------------------------------------------- #
def _synthetic_beta_panel(n=900):
    dates = [f"{2000 + i // 252:04d}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
             for i in range(n)]
    rising = np.linspace(100.0, 300.0, n)
    return {"ok": True, "dates": dates,
            "columns": {"BENCHMARK": rising,
                        "CASH_YIELD": np.full(n, 2.0),
                        "VIX": np.linspace(10.0, 40.0, n),
                        "YIELD_10Y": np.full(n, 3.0),
                        "YIELD_3M": np.full(n, 1.0)}}


def test_trend_owns_equity_only_above_its_own_moving_average():
    panel = _synthetic_beta_panel()
    idx = list(range(300, 880, 21))
    out = _ebt.gen_trend(panel, idx, {"ma": 200})
    assert out, "a monotonically rising index must produce opportunities"
    assert all(o.recommended_exposure.get(_ebt.INSTRUMENT) == 1.0 for o in out)

    falling = _synthetic_beta_panel()
    falling["columns"]["BENCHMARK"] = np.linspace(300.0, 100.0, 900)
    down = _ebt.gen_trend(falling, idx, {"ma": 200})
    assert all(not o.recommended_exposure for o in down)
    assert all(o.cash_weight == pytest.approx(1.0) for o in down)


def test_cross_asset_trend_holds_cash_when_no_leg_trends():
    n = 800
    dates = [f"2010-01-{(i % 28) + 1:02d}" for i in range(n)]
    falling = np.linspace(200.0, 100.0, n)
    panel = {"ok": True, "dates": dates,
             "columns": {leg: falling.copy() for leg in _cat.LEGS}}
    panel["columns"]["CASH_YIELD"] = np.full(n, 1.0)
    out = _cat.gen_tsmom(panel, list(range(300, 780, 21)), {"lookback": 252})
    assert out
    assert all(not o.recommended_exposure for o in out), (
        "every leg is falling, so the honest answer is all cash")


def test_sector_rotation_admits_real_estate_only_from_its_gics_introduction():
    panel = {"dates": ["2010-01-04", "2020-01-06"],
             "columns": {}, "legs": list(_sr._panels.SECTOR_LEGS)}
    early = _sr.admissible_sectors(panel, "2010-01-04")
    late = _sr.admissible_sectors(panel, "2020-01-06")
    assert "REAL_ESTATE" not in early, (
        "before 2016 Real Estate lived inside Financials; admitting both "
        "double-counts the same companies")
    assert "REAL_ESTATE" in late


def test_volatility_sleeve_owns_no_volatility_product():
    assert _vrr.INVESTABLE_INSTRUMENTS == (_vrr.INSTRUMENT,)
    assert _vrr.TRADABLE_VOLATILITY_PRODUCTS_OWNED is False


def test_event_sleeve_uses_only_deterministic_calendar_structure():
    dates = ["2021-01-27", "2021-01-28", "2021-01-29", "2021-02-01",
             "2021-02-02", "2021-03-19", "2021-12-27"]
    cal = _panels.calendar_features(dates)
    assert cal["month_end"][2] == 1.0
    assert cal["month_start"][3] == 1.0
    assert cal["triple_witching"][5] == 1.0, "third Friday of March"
    assert cal["santa_window"][6] == 1.0
    assert set(_ed.CALENDAR_EVENTS) <= set(cal)


def test_event_sleeve_declares_the_event_data_it_does_not_own():
    text = json.dumps(_ed.UNOWNED_EVENT_REQUIREMENTS)
    assert "SYNTHETIC_TEST_FIXTURE" in text
    assert "LOCAL_PROXY_NOT_A_MEASUREMENT" in text
    assert "COVERAGE_TOO_NARROW" in text


def test_the_control_sleeve_is_inherited_and_may_not_be_researched():
    assert _es.MAY_BE_RESEARCHED_IN_R32 is False
    assert _es.screening_specs() == []
    with pytest.raises(_funnel.ControlSleeveResearched):
        _funnel.assert_control_not_researched(_contract.CONTROL_SLEEVE)


# --------------------------------------------------------------------------- #
# The common economic judge
# --------------------------------------------------------------------------- #
def test_cost_is_charged_on_traded_notional_not_one_way_turnover():
    """Release 31 shipped the one-way bug; every net return understated cost."""
    prev = {"A": 0.5, "B": 0.5}
    tgt = {"A": 0.0, "B": 1.0}
    assert _judge.traded_notional(prev, tgt) == pytest.approx(1.0)
    assert _judge.traded_notional(None, tgt) == pytest.approx(1.0)
    assert _judge.transition_cost(prev, tgt, rate_per_side_bps=10.0) == (
        pytest.approx(0.001))


def test_cash_earns_the_observed_bill_yield_and_is_scored():
    n = 3
    ret = _judge.book_return({"A": 0.4}, {"A": 0.10},
                             cash_weight=0.6, cash_return=0.01)
    assert ret == pytest.approx(0.4 * 0.10 + 0.6 * 0.01)
    assert n == 3


def test_a_missing_instrument_return_is_treated_as_cash_not_dropped():
    """Dropping it would inflate the return of whatever remains."""
    ret = _judge.book_return({"A": 0.5, "B": 0.5}, {"A": 0.10},
                             cash_weight=0.0, cash_return=0.01)
    assert ret == pytest.approx(0.5 * 0.10 + 0.5 * 0.01)


def test_the_volatility_matched_control_carries_the_sleeves_risk():
    rng = np.random.default_rng(7)
    bench = rng.normal(0.01, 0.04, 240)
    cash = np.full(240, 0.002)
    sleeve = 0.5 * bench + 0.5 * cash
    ctl = _judge.volatility_matched_control(sleeve, bench, cash)
    assert ctl["ok"]
    assert ctl["equity_weight"] == pytest.approx(0.5, abs=0.02)
    assert ctl["leverage_available"] is False


def test_the_control_never_levers_above_the_benchmark():
    """NEGATIVE PROBE: a sleeve riskier than the benchmark gets w = 1.0.

    Leverage is not available to this project, so an imaginary levered control
    would be a comparison against something nobody could hold.
    """
    rng = np.random.default_rng(11)
    bench = rng.normal(0.01, 0.02, 240)
    ctl = _judge.volatility_matched_control(3.0 * bench, bench,
                                            np.full(240, 0.001))
    assert ctl["equity_weight"] == pytest.approx(1.0)


def test_beating_cash_alone_cannot_qualify_a_long_equity_sleeve():
    """The defect that superseded campaign v1, pinned as a contract.

    A book that simply holds the benchmark beats cash decisively and has no
    skill whatsoever. Its excess over the volatility-matched control must be
    approximately zero, and the campaign must rank on THAT.
    """
    rng = np.random.default_rng(3)
    bench = rng.normal(0.012, 0.04, 300)
    cash = np.full(300, 0.002)
    vs_cash = _judge.excess_significance(bench, cash)
    ctl = _judge.volatility_matched_control(bench, bench, cash)
    vs_matched = _judge.excess_significance(bench, ctl["series"])
    assert vs_cash["t_stat"] > 2.0, "holding equities beats bills"
    assert abs(vs_matched["mean_excess"]) < 1e-9, (
        "being the benchmark is not skill")
    assert _judge.behaviour_declaration()["primary_control"] == (
        "VOLATILITY_MATCHED_BENCHMARK_CASH_MIX")


def test_marginal_portfolio_value_can_favour_an_uncorrelated_loser():
    rng = np.random.default_rng(5)
    bench = rng.normal(0.010, 0.05, 300)
    diversifier = rng.normal(0.004, 0.01, 300)
    mpv = _judge.marginal_portfolio_value(diversifier, bench)
    assert mpv["improves"] is True
    assert abs(mpv["correlation_with_benchmark"]) < 0.3


def test_judge_behaviour_hash_changes_when_economics_change(monkeypatch):
    """NEGATIVE PROBE: a changed cost model must not reuse old candidates."""
    before = _judge.behaviour_hash()
    monkeypatch.setitem(_contract.COST_RATE_PER_SIDE_BPS,
                        _contract.SLEEVE_SECTOR_ROTATION, 99.0)
    assert _judge.behaviour_hash() != before


# --------------------------------------------------------------------------- #
# Funnel budgets, denominator and lockbox
# --------------------------------------------------------------------------- #
def _spec(sleeve, family, params, stage=_contract.STAGE_SCREENING, **kw):
    return _sleeve.SleeveSpec(sleeve=sleeve, family=family, params=params,
                              generate=lambda *a: [], stage=stage, **kw)


def test_screening_budget_is_a_number_not_a_sentence():
    fun = _funnel.Funnel(judge_behaviour_hash="h")
    for i in range(_contract.SCREENING_MAX_PER_SLEEVE):
        fun.record(_spec(_contract.SLEEVE_SECTOR_ROTATION, "momentum",
                         {"i": i}), {"scored": True, "n": 100})
    with pytest.raises(_funnel.BudgetExceeded):
        fun.check(_spec(_contract.SLEEVE_SECTOR_ROTATION, "momentum", {"i": 99}))


def test_every_executed_hypothesis_stays_in_the_denominator():
    fun = _funnel.Funnel(judge_behaviour_hash="h")
    fun.record(_spec(_contract.SLEEVE_SECTOR_ROTATION, "m", {"a": 1}),
               {"scored": True, "n": 100})
    fun.record(_spec(_contract.SLEEVE_SECTOR_ROTATION, "m", {"a": 2}),
               {}, state=_funnel.STATE_FAILED)
    fun.record(_spec(_contract.SLEEVE_SECTOR_ROTATION, "m", {"a": 3}),
               {}, state=_funnel.STATE_INSUFFICIENT_DECISIONS)
    assert fun.denominator == 3, (
        "a denominator that counts only survivors is a second selection")


def test_the_same_hypothesis_cannot_be_counted_twice():
    fun = _funnel.Funnel(judge_behaviour_hash="h")
    s = _spec(_contract.SLEEVE_SECTOR_ROTATION, "m", {"a": 1})
    fun.record(s, {"scored": True, "n": 100})
    with pytest.raises(_funnel.BudgetExceeded):
        fun.record(_spec(_contract.SLEEVE_SECTOR_ROTATION, "m", {"a": 1}),
                   {"scored": True, "n": 100})


def test_a_finalist_gets_exactly_one_lockbox_access():
    fun = _funnel.Funnel(judge_behaviour_hash="h")
    fun.authorise_lockbox("abc")
    with pytest.raises(_funnel.LockboxViolation):
        fun.authorise_lockbox("abc")


def test_no_retuning_is_possible_after_the_lockbox_opens():
    """NEGATIVE PROBE: the held-out sample stops being held out otherwise."""
    fun = _funnel.Funnel(judge_behaviour_hash="h")
    fun.authorise_lockbox("abc")
    with pytest.raises(_funnel.LockboxViolation):
        fun.check(_spec(_contract.SLEEVE_SECTOR_ROTATION, "m", {"a": 9},
                        stage=_contract.STAGE_QUALIFICATION))
    assert _contract.RETUNING_AFTER_LOCKBOX_ALLOWED is False


def test_the_funnel_refuses_to_research_the_control_sleeve():
    fun = _funnel.Funnel(judge_behaviour_hash="h")
    with pytest.raises(_funnel.ControlSleeveResearched):
        fun.check(_spec(_contract.CONTROL_SLEEVE, "anything", {}))


def test_a_recorded_row_keeps_its_return_path_for_the_caller_only():
    """The v2 defect: stripping private keys emptied the correlation map."""
    fun = _funnel.Funnel(judge_behaviour_hash="h")
    row = fun.record(_spec(_contract.SLEEVE_SECTOR_ROTATION, "m", {"a": 1}),
                     {"scored": True, "n": 100, "_dates": ["2020-01-02"],
                      "_net_path": np.array([0.01])})
    assert "_dates" in row, "the frontier needs the path"
    assert "_dates" not in fun.rows[0], "the frozen artifact must not carry it"


# --------------------------------------------------------------------------- #
# Frontier: correlation, clustering, cash
# --------------------------------------------------------------------------- #
def test_two_sleeves_moving_together_are_one_latent_bet():
    base = {f"2020-{m:02d}-01": 0.01 * (m % 5) for m in range(1, 13)}
    same = {k: v + 1e-6 for k, v in base.items()}
    other = {k: -v for k, v in base.items()}
    corr = _frontier.correlation_map({"A": base, "B": same, "C": other})
    clusters = _frontier.latent_clusters(corr)
    names = {n for c in clusters for n in c["cluster"]}
    assert "A" in names and "B" in names, (
        "asset labels do not equal diversification")


def test_correlation_reports_insufficient_overlap_rather_than_zero():
    """NEGATIVE PROBE: no shared dates must not read as 'uncorrelated'."""
    a = {"2020-01-01": 0.01, "2020-02-01": 0.02}
    b = {"2021-01-01": 0.01, "2021-02-01": 0.02}
    corr = _frontier.correlation_map({"A": a, "B": b})
    row = corr["A|B"]
    assert row["correlation"] is None
    assert row["n"] == 0


def test_the_frontier_is_not_an_allocator():
    body = _frontier.build(campaign_id="t", verdict={"sleeves": {}},
                           sleeve_paths={}, overlap={}, inherited={},
                           information_gaps=[])
    assert body["is_research_comparison_not_an_allocator"] is True
    assert body["produces_portfolio_target"] is False
    assert body["safety_block"]["creates_capital_allocation"] is False


# --------------------------------------------------------------------------- #
# Information Purchase Gate
# --------------------------------------------------------------------------- #
def test_the_gate_has_exactly_ten_conditions():
    assert len(_gate.CONDITIONS) == 10


def test_a_gap_failing_any_condition_is_not_a_purchase_candidate():
    row = _gate.evaluate_gap({"gap": "x", "conditions": {
        c: True for c in _gate.CONDITIONS[:9]}})
    assert row["state"] == _gate.STATE_NOT_A_CANDIDATE
    assert row["failed_conditions"] == [_gate.CONDITIONS[9]]


def test_the_gate_never_authorises_a_purchase_or_spends_money():
    body = _gate.build(campaign_id="t", gaps=[{"gap": "x", "conditions": {}}])
    assert body["total_spent_usd"] == 0.0
    assert body["release32_may_spend_money"] is False
    assert body["waits_for_a_vendor_sample"] is False
    assert all(r["purchase_authorised"] is False for r in body["gaps"])


def test_settled_provider_evaluations_are_recorded_so_they_stay_settled():
    text = json.dumps(_gate.PRIOR_EVALUATIONS)
    assert "Intrinio" in text and _gate.STATE_EVALUATED_DO_NOT_BUY in text


# --------------------------------------------------------------------------- #
# Daily Multi-Asset Governance contract
# --------------------------------------------------------------------------- #
def test_daily_reassessment_does_not_imply_daily_trading():
    out = _gov.hysteresis_decision(expected_utility_target=0.010,
                                   expected_utility_current=0.008,
                                   transition_costs=0.003,
                                   governance_hurdle=0.002)
    assert out["trades"] is False
    assert out["action"] == "NO_CHANGE"
    assert out["reassessment_happened"] is True


def test_a_change_worth_more_than_the_hurdle_is_evaluated():
    out = _gov.hysteresis_decision(expected_utility_target=0.030,
                                   expected_utility_current=0.008,
                                   transition_costs=0.003,
                                   governance_hurdle=0.002)
    assert out["trades"] is True
    assert out["action"] == "EVALUATE_CHANGE"


def test_a_closed_market_leaves_a_delta_pending_never_dropped():
    assert _gov.classify_delta(market_state=_gov.MARKET_CLOSED,
                               data_is_stale=False) == (
        _gov.DELTA_PENDING_MARKET_CLOSED)
    assert _gov.classify_delta(market_state=_gov.MARKET_HOLIDAY,
                               data_is_stale=False) == (
        _gov.DELTA_PENDING_MARKET_CLOSED)


def test_stale_data_fails_closed_even_when_the_market_is_open():
    """NEGATIVE PROBE: an open market must not override staleness."""
    assert _gov.classify_delta(market_state=_gov.MARKET_OPEN,
                               data_is_stale=True) == (
        _gov.DELTA_PENDING_STALE_DATA)


def test_an_unknown_calendar_state_fails_closed():
    assert _gov.classify_delta(market_state=_gov.MARKET_UNKNOWN,
                               data_is_stale=False) == _gov.DELTA_BLOCKED


def test_governance_reuses_the_existing_event_fabric():
    assert _gov.EVENT_FABRIC_OWNER == "engine.event_fabric"
    assert _gov.SECOND_EVENT_SYSTEM_ALLOWED is False


def test_multi_asset_nav_has_exactly_one_declared_future_owner():
    body = _gov.build_contract(campaign_id="t")
    assert body["multi_asset_nav_owner"] == "api.portfolio_valuation"
    assert body["asset_count_is_not_diversification"] is True
    assert body["sleeves_own_capital"] is False
    assert body["allocator_owns_capital"] is True


def test_hedge_substitution_requires_a_validated_policy_that_does_not_exist():
    assert _gov.UNRELATED_INSTRUMENT_HEDGE_SUBSTITUTION_ALLOWED is False
    assert _gov.HEDGE_POLICY_STATE == "NO_VALIDATED_HEDGE_POLICY_EXISTS"


def test_turnover_budget_concepts_exist_for_all_three_periods():
    """(1) The CONCEPTS are the part Release 32 was authorised to declare."""
    assert tuple(_gov.TURNOVER_BUDGET_PERIODS) == ("daily", "weekly", "monthly")
    assert set(_gov.TURNOVER_BUDGETS) == {"daily", "weekly", "monthly"}

    body = _gov.build_contract(created_at="2026-08-20T00:00:00")
    assert body["turnover_budget_periods"] == ["daily", "weekly", "monthly"]
    assert body["turnover_budget_concepts_declared"] is True
    assert body["turnover_budgets_are_future_governance_concepts"] is True


def test_no_numeric_turnover_budget_value_is_invented():
    """(2) Release 32 measured nothing that could calibrate a limit.

    Checked by AST on the real module rather than by reading the constant back,
    so a number reintroduced in any form - literal, tuple, nested dict - is
    caught rather than only the exact shape this test happens to import.
    """
    for period, value in _gov.TURNOVER_BUDGETS.items():
        assert value is None, f"{period} budget was given an invented value"

    aud = importlib.import_module("scripts.audit_architecture")
    gov_src = (REPO / "alpha_agent" / "r32" / "governance.py").read_text(
        encoding="utf-8")
    assert aud._r32_turnover_budget_literals(gov_src) == []

    body = _gov.build_contract(created_at="2026-08-20T00:00:00")
    assert body["turnover_budget_values_calibrated"] is False
    assert body["turnover_budget_value_state"] == "NOT_CALIBRATED"
    assert all(v is None for v in body["turnover_budgets"].values())


def test_the_invented_value_guard_can_actually_fail():
    """Negative probe for (2): the pre-repair module must be refused.

    Release 32 shipped ``{"daily": 0.05, "weekly": 0.15, "monthly": 0.35}``
    and no guard objected, which is the only reason it survived to be found.
    """
    aud = importlib.import_module("scripts.audit_architecture")
    pre_repair = 'TURNOVER_BUDGETS = {"daily": 0.05, "weekly": 0.15, "monthly": 0.35}\n'
    assert aud._r32_turnover_budget_literals(pre_repair) == ["0.05", "0.15", "0.35"]
    # and a value smuggled in through a comprehension is caught too
    assert aud._r32_turnover_budget_literals(
        "TURNOVER_BUDGETS = {p: 0.35 for p in PERIODS}\n") == ["0.35"]


def test_an_uncalibrated_budget_is_undecidable_not_zero():
    """(3) Not zero, not unlimited - a third state the caller must handle."""
    r = _gov.check_turnover_budget(period="daily", proposed_turnover=0.02)
    assert r["state"] == _gov.TURNOVER_BUDGET_UNDECIDABLE
    assert r["decidable"] is False
    assert r["within_budget"] is None
    assert r["limit"] is None
    assert r["means_zero_turnover"] is False
    assert r["means_unlimited_turnover"] is False

    # A zero budget would forbid every trade; an unlimited one would permit
    # every trade. The uncalibrated answer must differ from BOTH.
    assert r["state"] != _gov.TURNOVER_OVER_BUDGET
    assert r["state"] != _gov.TURNOVER_WITHIN_BUDGET
    for turnover in (0.0, 0.001, 0.5, 10.0):
        assert _gov.check_turnover_budget(
            period="daily", proposed_turnover=turnover)["within_budget"] is None

    body = _gov.build_contract(created_at="2026-08-20T00:00:00")
    assert body["uncalibrated_turnover_budget_means_zero_turnover"] is False
    assert body["uncalibrated_turnover_budget_means_unlimited_turnover"] is False
    assert body["uncalibrated_turnover_budget_is_undecidable"] is True


def test_a_calibrated_budget_would_decide_normally():
    """The undecidable state is the MISSING VALUE, not the mechanism.

    Without this, "always returns None" would satisfy the test above, and the
    contract would be unable to decide anything once Release 33 calibrates it.
    """
    try:
        _gov.TURNOVER_BUDGETS["daily"] = 0.05
        within = _gov.check_turnover_budget(period="daily", proposed_turnover=0.02)
        over = _gov.check_turnover_budget(period="daily", proposed_turnover=0.09)
    finally:
        _gov.TURNOVER_BUDGETS["daily"] = None

    assert within["state"] == _gov.TURNOVER_WITHIN_BUDGET
    assert within["decidable"] is True and within["within_budget"] is True
    assert over["state"] == _gov.TURNOVER_OVER_BUDGET
    assert over["within_budget"] is False
    assert _gov.TURNOVER_BUDGETS["daily"] is None, "the module was left mutated"


def test_an_unknown_budget_period_is_refused_rather_than_defaulted():
    with pytest.raises(KeyError):
        _gov.check_turnover_budget(period="quarterly", proposed_turnover=0.1)


def test_release32_remains_read_only_while_declaring_turnover_budgets():
    """(4) Declaring a budget concept must not make the release operational."""
    body = _gov.build_contract(created_at="2026-08-20T00:00:00")
    assert body["implemented_in_release_32"] is False
    assert body["runs_anything"] is False
    assert body["state"] == "DECLARED_FOR_RELEASE_33"
    assert body["daily_reassessment_implies_daily_trading"] is False

    gov_src = (REPO / "alpha_agent" / "r32" / "governance.py").read_text(
        encoding="utf-8")
    for forbidden in ("operational_book", "portfolio_decision", "daily_close",
                      "rebalance_execution", "place_order"):
        assert forbidden not in gov_src, forbidden


def test_the_future_turnover_budget_value_owner_is_explicit():
    """(5) An unnamed owner is how a placeholder becomes permanent."""
    owner = _gov.TURNOVER_BUDGET_VALUE_OWNER
    assert owner and owner.isupper()
    assert "RELEASE_33" in owner and "CALIBRATION_OWNER" in owner

    body = _gov.build_contract(created_at="2026-08-20T00:00:00")
    assert body["turnover_budget_value_owner"] == owner


def test_the_documentation_and_the_python_contract_agree_on_turnover_budgets():
    """(6) The doc already said Release 33 owns the values; the code did not."""
    doc = (REPO / "docs" / "DAILY_MULTI_ASSET_GOVERNANCE.md").read_text(
        encoding="utf-8")
    section = doc[doc.index("## D. Global turnover budget"):]
    section = section[:section.index("\n## ")]
    low = section.lower()

    for period in _gov.TURNOVER_BUDGET_PERIODS:
        assert period in low, period
    assert "not calibrated" in low or "uncalibrated" in low
    assert "release 33" in low
    assert "zero" in low, "the doc must say uncalibrated is not a zero budget"

    # The doc must invent no numbers either - the defect was a mismatch
    # between a document that deferred and code that decided.
    assert not re.findall(r"\b0\.\d+\b", section), section


# --------------------------------------------------------------------------- #
# Read-only API and production safety
# --------------------------------------------------------------------------- #
def test_the_route_is_declared_get_only():
    app = APP.read_text(encoding="utf-8")
    assert '"/v1/research/pnl-opportunity-frontier"' in app
    assert re.search(
        r'@app\.get\(\s*\n\s*"/v1/research/pnl-opportunity-frontier"', app)
    for verb in ("post", "put", "patch", "delete"):
        assert not re.search(
            r'@app\.' + verb + r'\(\s*\n\s*"/v1/research/pnl-opportunity-frontier"',
            app), verb


def test_the_route_has_an_owner_in_both_inventory_registries():
    """Release 31 registered only ``modules`` and broke the route contract."""
    inv = json.loads(
        (REPO / "docs" / "architecture" / "system_inventory.json")
        .read_text(encoding="utf-8"))
    owners = {r["prefix"]: r["owner"] for r in inv["route_ownership"]}
    assert owners.get("/v1/research/pnl-opportunity-frontier") == (
        "api/pnl_opportunity_frontier.py")
    paths = {m["path"] for m in inv["modules"]}
    assert "api/pnl_opportunity_frontier.py" in paths
    assert "alpha_agent/r32/" in paths


def test_the_read_model_writes_nothing():
    src = READ_MODEL.read_text(encoding="utf-8")
    for token in ("write_text(", "os.replace(", ".mkdir(", "requests.",
                  "httpx.", "sqlalchemy"):
        assert token not in src, token


def test_the_read_model_declares_the_canonical_order_badge():
    src = READ_MODEL.read_text(encoding="utf-8")
    assert "NO LIVE BROKER ORDERS" in src
    assert ">NO LIVE ORDERS</span>" not in src


def test_the_research_package_declares_every_safety_flag_false():
    block = r32.safety_block()
    for key in ("creates_capital_allocation", "creates_proposal",
                "creates_decision", "creates_order", "activates_sleeve",
                "promotes_model", "mutates_holdings", "mutates_cash",
                "enables_automation", "restarts_production",
                "writes_operational_store", "may_spend_money"):
        assert block[key] is False, key


def test_the_research_package_never_imports_an_operational_owner():
    for path in sorted((REPO / "alpha_agent" / "r32").rglob("*.py")):
        src = path.read_text(encoding="utf-8")
        assert not re.search(r"^\s*(from|import)\s+.*paper_trader\.api",
                             src, re.M), path


# --------------------------------------------------------------------------- #
# UI structure
# --------------------------------------------------------------------------- #
def _r32_region(html: str) -> str:
    start = html.index("R32 PNL OPPORTUNITY FRONTIER")
    end = html.index("R32 PNL OPPORTUNITY FRONTIER END", start + 1)
    return html[start:end]


def test_the_ui_region_carries_the_mandatory_safety_badges():
    region = _r32_region(UI.read_text(encoding="utf-8"))
    for badge in ("RESEARCH ONLY", "READ ONLY", "NO LIVE BROKER ORDERS",
                  "AUTOMATION OFF", "MANUAL REVIEW", "NO SLEEVE ACTIVATION"):
        assert badge in region, badge


def test_the_ui_region_carries_no_ambiguous_order_badge():
    region = _r32_region(UI.read_text(encoding="utf-8"))
    assert ">NO LIVE ORDERS</span>" not in region
    assert ">ORDERS DISABLED<" not in region


def test_the_ui_region_exposes_no_control_at_all():
    region = _r32_region(UI.read_text(encoding="utf-8"))
    for token in ("<button", "<input", "<select", "<form", "onclick=",
                  "Execute", "Approve", "Activate", "Promote", "Allocate",
                  "Create Order"):
        assert token not in region, token
    for dialog in ("alert(", "confirm("):
        assert dialog not in region, dialog


def test_the_ui_renderer_authors_no_double_escaping_entity():
    """Release 31's live defect: labels authored as HTML then escaped."""
    html = UI.read_text(encoding="utf-8")
    start = html.index("// Renders GET /v1/research/pnl-opportunity-frontier")
    end = html.index("window.loadPnlOpportunityFrontier =", start)
    block = html[start:end]
    for entity in ("&nbsp;", "&middot;", "&times;", "&amp;"):
        assert entity not in block, (
            f"{entity} is authored inside a slot that _r30esc will escape, so "
            "the operator would read the entity as text")


def test_the_ui_renderer_computes_no_research_mathematics():
    html = UI.read_text(encoding="utf-8")
    start = html.index("// Renders GET /v1/research/pnl-opportunity-frontier")
    end = html.index("window.loadPnlOpportunityFrontier =", start)
    block = html[start:end]
    for token in ("Math.sqrt", "corrcoef", "Math.log", "reduce(", "* 100)"):
        assert token not in block, token


# --------------------------------------------------------------------------- #
# Campaign contract
# --------------------------------------------------------------------------- #
def test_the_campaign_declares_six_sleeves_and_one_control():
    assert len(_contract.SLEEVES) == 6
    assert _contract.CONTROL_SLEEVE == _contract.SLEEVE_EQUITY_SELECTION
    assert len(_contract.NEW_SLEEVES) == 5


def test_cash_is_a_real_asset_choice_and_a_null_result_is_valid():
    assert _contract.CASH_IS_A_REAL_ASSET_CHOICE is True
    assert _contract.NULL_RESULT_IS_VALID is True
    assert _contract.NOT_REQUIRED_TO_ALLOCATE_EVERYWHERE is True


def test_superseded_campaigns_are_preserved_with_their_defects():
    for cid in ("r32_pnl_opportunity_frontier_v1",
                "r32_pnl_opportunity_frontier_v2",
                "r32_pnl_opportunity_frontier_v3"):
        row = _contract.SUPERSEDED_CAMPAIGNS[cid]
        assert row["defects"], cid
    assert _contract.SUPERSEDED_EVIDENCE_RULES["is_preserved_on_disk"] is True
    assert _contract.SUPERSEDED_EVIDENCE_RULES[
        "may_reduce_the_multiple_testing_denominator"] is False


def test_the_contract_hash_is_stable_and_excludes_the_environment():
    body = _contract.build(campaign_id="t", created_at="2026-08-20T00:00:00")
    again = _contract.build(campaign_id="t", created_at="2026-08-20T00:00:00")
    assert body["contract_hash"] == again["contract_hash"]


def test_budget_ceilings_match_the_declared_programme():
    assert _contract.SCREENING_MAX_PER_SLEEVE == 8
    assert _contract.QUALIFICATION_MAX_PER_SLEEVE == 24
    assert _contract.QUALIFICATION_MAX_TOTAL == 120
    assert _contract.NOVEL_MAX_PER_SLEEVE == 12
    assert _contract.NOVEL_MAX_TOTAL == 60
    assert _contract.NOVEL_MAX_DEPTH == 2
    assert _contract.LOCKBOX_MAX_FINALISTS_PER_SLEEVE == 2
    assert _contract.LOCKBOX_MAX_FINALISTS_TOTAL == 12
    assert _contract.LOCKBOX_MAX_ACCESSES_PER_FINALIST == 1


# --------------------------------------------------------------------------- #
# Panels and hold-window arithmetic
# --------------------------------------------------------------------------- #
def test_hold_return_uses_only_observed_prices():
    levels = np.array([100.0, 105.0, 110.0, 121.0])
    assert _panels.hold_return(levels, 0, 3) == pytest.approx(0.21)
    assert np.isnan(_panels.hold_return(levels, 2, 3))


def test_cash_uses_the_yield_observable_at_the_decision_not_after():
    dates = ["2020-01-01", "2020-01-31"]
    y = np.array([2.0, 99.0])
    got = _panels.hold_cash_return(y, dates, 0, 1)
    assert got == pytest.approx((2.0 / 100.0) * (30 / 365))


def test_a_masked_event_window_earns_cash_when_it_is_not_invested():
    levels = np.array([100.0, 110.0, 121.0, 133.1])
    mask = np.array([False, True, False, False])
    cash = np.full(4, 0.0)
    got = _panels.masked_hold_return(levels, mask, 0, 3, cash_daily=cash)
    assert got == pytest.approx(0.10)


def test_alignment_refuses_to_invent_a_value_for_a_leg_that_did_not_exist():
    a = {"available": True, "dates": ["2020-01-01", "2020-01-02"],
         "close": np.array([1.0, 2.0])}
    b = {"available": True, "dates": ["2020-01-02"], "close": np.array([9.0])}
    out = _panels.align({"A": a, "B": b}, require_all=False)
    assert np.isnan(out["columns"]["B"][0]), (
        "a state variable that had not been invented has no value")
    strict = _panels.align({"A": a, "B": b}, require_all=True)
    assert strict["dates"] == ["2020-01-02"]
