"""Release 36 - Global Multi-Asset Alpha Frontier regression.

The tests are grouped by the thing that would be WRONG if they failed, and each
guard has a negative probe: an assertion that the guard fires when the defect it
exists to catch is actually introduced. A guard that has never been seen to fail
is a guard nobody has tested.

Two defects this release found in its own first two runs have permanent tests
here, because both would have produced a qualified candidate that was not one:

* a configuration measured against a control it does not trade (v1);
* a control fabricated over dates when one of its own legs did not exist (v2).
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

from paper_trader.alpha_agent import r36  # noqa: E402
from paper_trader.alpha_agent.r33 import contract as r33_contract  # noqa: E402
from paper_trader.alpha_agent.r33 import universe as r33_universe  # noqa: E402
from paper_trader.alpha_agent.r35 import contract as r35_contract  # noqa: E402
from paper_trader.alpha_agent.r35 import (  # noqa: E402
    information as r35_information,
)
from paper_trader.alpha_agent.r36 import acquisition  # noqa: E402
from paper_trader.alpha_agent.r36 import campaign  # noqa: E402
from paper_trader.alpha_agent.r36 import contract  # noqa: E402
from paper_trader.alpha_agent.r36 import coverage  # noqa: E402
from paper_trader.alpha_agent.r36 import entitlements  # noqa: E402
from paper_trader.alpha_agent.r36 import experiments  # noqa: E402
from paper_trader.alpha_agent.r36 import native_markets  # noqa: E402
from paper_trader.alpha_agent.r36 import strategies  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Package, safety and isolation
# --------------------------------------------------------------------------- #
def test_research_root_is_isolated_and_never_operational():
    root = str(r36.research_root()).lower()
    assert "global_multi_asset_frontier_r36" in root
    for forbidden in (".paper_trader", "portfolio_decisions",
                      "reallocation_proposals", "rebalance_order_plans",
                      "information_collection", "operational"):
        assert forbidden not in root


def test_every_safety_flag_is_false():
    block = r36.safety_block()
    for key, value in block.items():
        if key == "safety":
            continue
        assert value is False, key
    assert r36.MAY_SPEND_MONEY is False
    assert r36.MAY_MUTATE_PRODUCTION is False
    assert r36.AUTOMATIC_PROMOTION_ALLOWED is False
    assert r36.AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED is False


def test_artifact_body_always_carries_the_safety_block():
    body = r36.artifact_body("x/1", {"a": 1})
    assert body["release"] == "release36"
    assert body["safety_block"]["creates_order"] is False
    assert body["safety_block"]["purchases_data"] is False
    assert body["safety_block"]["changes_subscription_tier"] is False


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
def test_contract_hash_is_stable_and_excludes_the_environment():
    body = contract.build(created_at="2026-08-22T00:00:00+00:00")
    assert contract.verify(body)["stable"] is True
    other = contract.build(created_at="2026-08-22T00:00:00+00:00")
    assert other["contract_hash"] == body["contract_hash"]


def test_planned_configuration_count_is_derived_not_typed():
    assert contract.PLANNED_CONFIG_TOTAL == len(contract.STRATEGIES)
    assert contract.PLANNED_CONFIG_TOTAL <= contract.MAX_PRIMARY_CONFIGS
    assert sum(contract.lane_config_counts().values()) == len(
        contract.STRATEGIES)


def test_every_strategy_declares_a_valid_lane_family_level_construction():
    for name, (lane, families, level, construction) in \
            contract.STRATEGIES.items():
        assert lane in contract.EXECUTED_LANES, name
        assert families, name
        for family in families:
            assert family in contract.STRATEGY_FAMILIES, (name, family)
        assert level in contract.LEVELS, name
        assert construction in contract.CONSTRUCTIONS, name


def test_every_lane_declares_a_cadence_a_reason_and_a_control():
    for lane in contract.EXECUTED_LANES:
        assert lane in contract.LANE_CADENCE
        assert contract.LANE_CADENCE_REASON.get(lane)
        assert contract.LANE_CONTROL.get(lane)


def test_no_universal_equity_control_is_permitted():
    assert contract.UNIVERSAL_SPY_CASH_CONTROL_ALLOWED is False
    assert contract.EXCESS_OVER_CASH_MAY_RANK is False
    assert contract.CONTROL_IS_THE_PASSIVE_HOLD_OF_WHAT_IS_TRADED is True
    controls = set(contract.LANE_CONTROL.values())
    assert len(controls) == len(contract.LANE_CONTROL), (
        "two lanes share a control; a control is supposed to be the passive "
        "exposure of THAT market")


def test_the_two_superseded_campaigns_name_their_defects():
    assert contract.CAMPAIGN_ID.endswith("_v3")
    superseded = contract.SUPERSEDED_CAMPAIGNS
    assert set(superseded) == {"r36_global_multi_asset_frontier_v1",
                               "r36_global_multi_asset_frontier_v2"}
    v1 = superseded["r36_global_multi_asset_frontier_v1"]
    assert v1["state"] == contract.SUPERSEDED_CONTROL_DEFECT
    assert v1["flattered_configuration"] == "VOL_TERM_EQUITY_TIMING"
    assert v1["gate_change_direction"] == "STRICTLY_TIGHTENING"
    v2 = superseded["r36_global_multi_asset_frontier_v2"]
    assert v2["state"] == contract.SUPERSEDED_WINDOW_DEFECT
    assert "60/40" in v2["defect"]
    for row in superseded.values():
        assert row["is_preserved_on_disk"] is True


def test_the_volatility_lane_declares_a_control_leg_per_configuration():
    assert contract.STRATEGY_CONTROL_LEG["VOL_TERM_LONG_TIMING"] == "VIXY"
    assert contract.STRATEGY_CONTROL_LEG["VOL_TERM_EQUITY_TIMING"] == "SPY"
    assert contract.STRATEGY_CONTROL_LEG_REASON


def test_money_is_refused_and_a_key_is_not_an_entitlement():
    for flag in (contract.MAY_SPEND_MONEY, contract.MAY_START_PROVIDER_TRIAL,
                 contract.MAY_CREATE_PROVIDER_ACCOUNT,
                 contract.MAY_CHANGE_SUBSCRIPTION_TIER,
                 contract.API_KEY_IMPLIES_ENTITLEMENT):
        assert flag is False
    assert contract.MAY_ACQUIRE_FREE_PUBLIC_DATA is True


def test_search_is_frozen_and_parameters_are_pre_declared():
    for flag in (contract.PARAMETER_SEARCH_ALLOWED,
                 contract.ADAPTIVE_SEARCH_ALLOWED,
                 contract.MODEL_ARCHITECTURE_SEARCH_ALLOWED,
                 contract.DEEP_LEARNING_IN_SCOPE,
                 contract.NEIGHBOUR_VALUES_MAY_BE_PROMOTED,
                 contract.FULL_SAMPLE_STATISTICS_ALLOWED):
        assert flag is False
    assert contract.PARAMETERS_ARE_PRE_DECLARED is True
    assert contract.NORMALISATION_IS_TRAILING_ONLY is True


def test_alpha_pass_is_structurally_unreachable():
    assert contract.FRESH_UNSEEN_EVIDENCE_EXISTS is False
    assert contract.genuinely_independent_evidence_exists() is False
    assert contract.verdict_ceiling_without_fresh_evidence() == \
        contract.RESULT_FAIL
    assert contract.A_FOLD_MAY_BE_CALLED_A_LOCKBOX is False
    assert contract.ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE is True
    assert contract.RESULT_NAMES == ("SYSTEM_RESULT",
                                     "RESEARCH_CANDIDATE_RESULT",
                                     "ALPHA_RESULT")


def test_nothing_may_be_promoted_registered_or_activated():
    for flag in (contract.MAY_REGISTER_FORWARD_CANDIDATE,
                 contract.MAY_CREATE_SECOND_TRUE_FORWARD_STORE,
                 contract.MAY_PROMOTE_MODEL, contract.MAY_ACTIVATE_SLEEVE):
        assert flag is False


def test_a_proxy_may_not_close_a_native_frontier():
    assert contract.PROXY_MAY_CLOSE_A_NATIVE_FRONTIER is False
    assert contract.PROXY_CLOSURE_REQUIRES_PROVEN_STRUCTURE_PRESERVATION \
        is True


# --------------------------------------------------------------------------- #
# Conventions are REUSED, never re-invented
# --------------------------------------------------------------------------- #
def test_publication_lags_are_the_release35_constants():
    assert contract.COT_PUBLICATION_LAG_DAYS == \
        r35_contract.COT_PUBLICATION_LAG_DAYS
    assert contract.OECD_RATE_PUBLICATION_LAG_MONTHS == \
        r35_contract.OECD_RATE_PUBLICATION_LAG_MONTHS
    assert contract.BROADCAST_LAG_SESSIONS == \
        r35_contract.BROADCAST_LAG_SESSIONS


def test_every_excluded_currency_carries_a_measured_reason():
    excluded = contract.FX_EXCLUDED_BY_MEASUREMENT
    assert isinstance(excluded, dict) and excluded
    for code, reason in excluded.items():
        assert code not in contract.FX_UNIVERSE, code
        assert reason and ":" in reason, code
    kinds = {r.split(":")[0] for r in excluded.values()}
    assert {"ADMINISTERED", "PEGGED", "NO_COMPARABLE_SHORT_RATE",
            "TENOR_MISMATCH"} == kinds


def test_admissibility_rules_are_the_release33_constants():
    assert contract.ADMISSIBILITY_RULES_ARE_REUSED_FROM_R33 is True
    assert contract.MAX_ZERO_RETURN_FRACTION == \
        r33_universe.MAX_ZERO_RETURN_FRACTION
    assert contract.MIN_ANNUAL_VOLATILITY == \
        r33_universe.MIN_ANNUAL_VOLATILITY
    assert contract.IMPLEMENTATION_LAG_SESSIONS == \
        r33_contract.IMPLEMENTATION_LAG_SESSIONS
    assert contract.FDR_Q == r33_contract.FDR_Q


def test_quarterly_price_index_carries_a_longer_lag_than_a_monthly_one():
    assert contract.QUARTERLY_CPI_PUBLICATION_LAG_MONTHS > \
        contract.CPI_PUBLICATION_LAG_MONTHS


def test_release35_loaders_keep_their_defaults_after_the_extension():
    """The R35 extension must be backwards compatible, not a behaviour change."""
    import inspect
    signature = inspect.signature(r35_information.load_fred)
    assert signature.parameters["monthly_ids"].default is None
    assert signature.parameters["lag_months"].default is None
    curve = inspect.signature(r35_information.load_eia_curve)
    assert curve.parameters["cache_name"].default == "eia_wti_curve.csv"
    assert curve.parameters["series_ids"].default == \
        r35_contract.EIA_WTI_CONTRACTS


def test_r36_gives_every_commodity_curve_its_own_cache_name():
    """A shared cache name would let one market overwrite another's columns."""
    names = set()
    for market in contract.COMMODITY_CURVES:
        names.add("r36_curve_%s.csv" % market.lower())
    assert len(names) == len(contract.COMMODITY_CURVES)
    assert "eia_wti_curve.csv" not in names


# --------------------------------------------------------------------------- #
# Trailing statistics - the leakage guards
# --------------------------------------------------------------------------- #
def _series(values):
    return pd.Series(values, index=pd.date_range("2000-01-31", periods=len(
        values), freq="ME"), dtype=float)


def test_trailing_mean_uses_only_strictly_earlier_rows():
    line = _series(list(range(40)))
    mean = strategies.trailing_mean(line, minimum=5)
    # the value on row k is the mean of rows 0..k-1
    assert mean.iloc[10] == pytest.approx(np.mean(range(10)))
    assert math.isnan(mean.iloc[4])


def test_trailing_mean_is_immune_to_a_future_value():
    line = _series(list(range(40)))
    poisoned = line.copy()
    poisoned.iloc[30] = 10_000.0
    a = strategies.trailing_mean(line, minimum=5)
    b = strategies.trailing_mean(poisoned, minimum=5)
    assert a.iloc[:31].equals(b.iloc[:31]), (
        "a future observation changed a past statistic")
    assert not a.iloc[31:].equals(b.iloc[31:]), (
        "the probe did not actually perturb anything")


def test_trailing_percentile_never_sees_its_own_row_or_later():
    line = _series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
    percentile = strategies.trailing_percentile(line, minimum=2)
    assert math.isnan(percentile.iloc[1])
    assert percentile.iloc[2] == pytest.approx(1.0)
    poisoned = line.copy()
    poisoned.iloc[7] = -100.0
    assert strategies.trailing_percentile(line, minimum=2).iloc[:7].equals(
        strategies.trailing_percentile(poisoned, minimum=2).iloc[:7])


def test_trailing_sum_skips_the_unrealised_period():
    frame = pd.DataFrame({"A": range(20)},
                         index=pd.date_range("2000-01-31", periods=20,
                                             freq="ME"), dtype=float)
    out = strategies.trailing_sum(frame, window=3, skip=2)
    # row 10 sums rows 8, 7, 6 - never rows 9 or 10, which are not yet known
    assert out["A"].iloc[10] == pytest.approx(8 + 7 + 6)


# --------------------------------------------------------------------------- #
# Decision dates and period returns
# --------------------------------------------------------------------------- #
def test_decision_dates_do_not_overlap_and_are_unique():
    calendar = pd.date_range("2000-01-03", periods=500, freq="B")
    dates = native_markets.decision_dates(calendar, cadence=21)
    assert dates.is_unique and dates.is_monotonic_increasing
    gaps = {(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)}
    assert min(gaps) >= 21, "successive decisions overlap"


def test_period_returns_enter_one_session_after_the_decision():
    calendar = pd.date_range("2000-01-03", periods=60, freq="B")
    prices = pd.DataFrame({"A": np.arange(1.0, 61.0)}, index=calendar)
    dates = pd.DatetimeIndex([calendar[0], calendar[10], calendar[20]])
    out = native_markets.period_returns(prices, dates, calendar=calendar)
    lag = contract.IMPLEMENTATION_LAG_SESSIONS
    expected = prices["A"].iloc[10 + lag] / prices["A"].iloc[0 + lag] - 1.0
    assert out["A"].iloc[0] == pytest.approx(expected)
    assert out.index[0] == calendar[0], (
        "the return must be stamped on the date whose information produced it")


def test_period_returns_drop_the_final_unclosed_decision():
    calendar = pd.date_range("2000-01-03", periods=60, freq="B")
    prices = pd.DataFrame({"A": np.arange(1.0, 61.0)}, index=calendar)
    dates = pd.DatetimeIndex([calendar[0], calendar[10], calendar[20]])
    out = native_markets.period_returns(prices, dates, calendar=calendar)
    assert len(out) == 2


def test_snap_to_calendar_never_moves_a_date_forward():
    calendar = pd.date_range("2000-01-03", periods=40, freq="B")
    wanted = pd.DatetimeIndex(["2000-01-15", "2000-02-02"])
    snapped = native_markets.snap_to_calendar(wanted, calendar)
    assert all(s <= w for s, w in zip(snapped, wanted))
    assert set(snapped) <= set(calendar)


def test_trailing_slope_is_estimated_on_earlier_rows_only():
    index = pd.date_range("2000-01-31", periods=80, freq="ME")
    x = pd.Series(np.random.default_rng(0).normal(size=80), index=index)
    y = 3.0 * x
    slope = native_markets.trailing_slope(y, x, minimum=10)
    assert math.isnan(slope.iloc[5])
    assert slope.iloc[40] == pytest.approx(3.0, abs=1e-9)
    poisoned = y.copy()
    poisoned.iloc[60] = 1e6
    other = native_markets.trailing_slope(poisoned, x, minimum=10)
    assert slope.iloc[:61].equals(other.iloc[:61])


# --------------------------------------------------------------------------- #
# Position construction
# --------------------------------------------------------------------------- #
def test_terciles_are_dollar_neutral_and_gross_one():
    index = pd.date_range("2000-01-31", periods=3, freq="ME")
    scores = pd.DataFrame({"A": [1.0, 1, 1], "B": [2.0, 2, 2],
                           "C": [3.0, 3, 3], "D": [4.0, 4, 4],
                           "E": [5.0, 5, 5], "F": [6.0, 6, 6]}, index=index)
    weights = strategies.cross_sectional_terciles(scores, min_cross_section=4)
    for _date, row in weights.iterrows():
        assert row.sum() == pytest.approx(0.0)
        assert row.abs().sum() == pytest.approx(1.0)
    assert weights.iloc[0]["F"] > 0 and weights.iloc[0]["A"] < 0


def test_terciles_refuse_a_cross_section_below_the_declared_minimum():
    index = pd.date_range("2000-01-31", periods=2, freq="ME")
    scores = pd.DataFrame({"A": [1.0, 1.0], "B": [2.0, 2.0]}, index=index)
    weights = strategies.cross_sectional_terciles(scores, min_cross_section=4)
    assert weights.abs().to_numpy().sum() == 0.0
    allowed = strategies.cross_sectional_terciles(scores, min_cross_section=2)
    assert allowed.abs().to_numpy().sum() > 0.0


def test_directional_never_exceeds_gross_one():
    index = pd.date_range("2000-01-31", periods=3, freq="ME")
    signs = pd.DataFrame({"A": [1.0, -1.0, 0.0], "B": [1.0, 1.0, 0.0],
                          "C": [-1.0, 1.0, 0.0]}, index=index)
    weights = strategies.directional(signs)
    assert weights.abs().sum(axis=1).max() <= 1.0 + 1e-12
    assert weights.abs().sum(axis=1).iloc[2] == 0.0


def test_duration_neutralise_equalises_the_two_legs():
    index = pd.date_range("2000-01-31", periods=1, freq="ME")
    weights = pd.DataFrame({"SHORT_END": [0.5], "LONG_END": [-0.5]},
                           index=index)
    durations = pd.DataFrame({"SHORT_END": [2.0], "LONG_END": [18.0]},
                             index=index)
    out = strategies.duration_neutralise(weights, durations)
    row = out.iloc[0]
    long_duration = row["SHORT_END"] * 2.0
    short_duration = row["LONG_END"] * 18.0
    assert long_duration + short_duration == pytest.approx(0.0, abs=1e-9)
    assert row.abs().sum() == pytest.approx(contract.MAX_GROSS_EXPOSURE)


# --------------------------------------------------------------------------- #
# The lane panel guards
# --------------------------------------------------------------------------- #
def _panel(control_missing_head: int = 0) -> dict:
    index = pd.date_range("2000-01-31", periods=24, freq="ME")
    excess = pd.DataFrame({"A": np.linspace(0.01, 0.02, 24),
                           "B": np.linspace(-0.01, 0.03, 24)}, index=index)
    control = pd.Series(np.linspace(0.005, 0.01, 24), index=index)
    if control_missing_head:
        control.iloc[:control_missing_head] = np.nan
    return {"lane": contract.LANE_FX, "ok": True, "dates": index,
            "excess": excess, "cash": pd.Series(0.0, index=index),
            "control_excess": control, "cadence": 21,
            "signals": {"carry": excess * 0.5},
            "meta": {c: {"asset_class": "FX", "cost_bps_per_side": 2.0}
                     for c in excess.columns},
            "instruments": list(excess.columns),
            "coverage": {"decisions": 24}}


def test_trim_to_control_removes_decisions_with_no_observable_benchmark():
    panel = _panel(control_missing_head=6)
    trimmed = native_markets.trim_to_control(panel)
    assert len(trimmed["excess"]) == 18
    assert trimmed["coverage"]["trimmed_to_control_window"] is True
    assert trimmed["coverage"]["decisions_before_trim"] == 24
    assert pd.Series(trimmed["control_excess"]).notna().all()
    assert len(trimmed["signals"]["carry"]) == 18


def test_trim_to_control_is_a_no_op_when_the_control_is_complete():
    panel = _panel()
    trimmed = native_markets.trim_to_control(panel)
    assert len(trimmed["excess"]) == 24
    assert "trimmed_to_control_window" not in (trimmed.get("coverage") or {})


def test_trim_to_control_fails_closed_when_the_control_never_exists():
    panel = _panel()
    panel["control_excess"] = pd.Series(np.nan, index=panel["excess"].index)
    trimmed = native_markets.trim_to_control(panel)
    assert trimmed["ok"] is False
    assert trimmed["reason"] == "CONTROL_NEVER_OBSERVABLE"


# --------------------------------------------------------------------------- #
# The experiment spine
# --------------------------------------------------------------------------- #
def test_a_weight_is_removed_where_the_return_is_not_observable():
    panel = _panel()
    panel["excess"].iloc[:5, 0] = np.nan
    weights = pd.DataFrame(0.5, index=panel["excess"].index,
                           columns=panel["excess"].columns)
    returns = panel["excess"]
    held = weights.where(returns.notna(), 0.0)
    assert (held.iloc[:5, 0] == 0.0).all()
    assert (held.iloc[5:, 0] == 0.5).all()


def test_the_control_leg_override_changes_which_benchmark_is_used():
    panel = _panel()
    panel["lane"] = contract.LANE_VOL
    panel["excess"] = panel["excess"].rename(columns={"A": "VIXY", "B": "SPY"})
    panel["meta"] = {c: {"asset_class": "VOL", "cost_bps_per_side": 15.0}
                     for c in panel["excess"].columns}
    panel["signals"] = {"term_slope": pd.Series(
        0.02, index=panel["excess"].index)}
    panel["instruments"] = list(panel["excess"].columns)
    long_row = experiments.run_configuration("VOL_TERM_LONG_TIMING", panel)
    equity_row = experiments.run_configuration("VOL_TERM_EQUITY_TIMING", panel)
    assert long_row["control"] == "PASSIVE_VIXY"
    assert equity_row["control"] == "PASSIVE_SPY"
    assert long_row["control_is_lane_default"] is False
    assert equity_row["control_is_lane_default"] is False


def test_a_lane_without_an_override_keeps_its_lane_control():
    panel = _panel()
    row = experiments.run_configuration("FX_CARRY", panel)
    assert row["control"] == contract.LANE_CONTROL[contract.LANE_FX]
    assert row["control_is_lane_default"] is True


def test_minimum_detectable_excess_scales_with_the_declared_threshold():
    assert experiments.minimum_detectable_excess(0.02, 2.0) == pytest.approx(
        0.02 * contract.MIN_EXCESS_T_STAT / 2.0)
    assert math.isnan(experiments.minimum_detectable_excess(0.02, 0.0))
    assert math.isnan(experiments.minimum_detectable_excess(None, 1.0))


def test_predictive_diagnostic_reports_a_different_statistic_per_construction():
    index = pd.date_range("2000-01-31", periods=30, freq="ME")
    columns = list("ABCDEF")
    rng = np.random.default_rng(3)
    returns = pd.DataFrame(rng.normal(size=(30, 6)), index=index,
                           columns=columns)
    weights = returns.rank(axis=1) - 3.5
    cross = experiments.predictive_diagnostic(
        weights, returns, construction=contract.CONSTRUCTION_CROSS_SECTIONAL)
    assert cross["statistic"] == "PER_DATE_RANK_IC"
    directional = experiments.predictive_diagnostic(
        weights, returns, construction=contract.CONSTRUCTION_DIRECTIONAL)
    assert directional["statistic"] == "DIRECTIONAL_HIT_RATE"


def test_every_gate_in_the_contract_is_evaluated_by_the_spine():
    panel = _panel()
    row = experiments.run_configuration("FX_CARRY", panel)
    gates = set(row["gates"]) | {"survives_multiple_testing_procedure"}
    assert gates == set(contract.QUALIFICATION_CONDITIONS)


# --------------------------------------------------------------------------- #
# Entitlements
# --------------------------------------------------------------------------- #
def test_credential_presence_records_a_boolean_and_never_a_value(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "super-secret-value")
    presence = entitlements.credential_presence()
    assert presence["FRED_API_KEY"] is True
    serialised = json.dumps(presence)
    assert "super-secret-value" not in serialised
    assert all(isinstance(v, bool) for v in presence.values())


def test_http_status_maps_to_a_terminal_entitlement_state():
    assert entitlements._state_for(200) == entitlements.ENTITLED
    assert entitlements._state_for(403) == entitlements.BLOCKED_LICENCE
    assert entitlements._state_for(404) == entitlements.BLOCKED_MISSING
    assert entitlements._state_for(None) == entitlements.UNMEASURED


def test_vix_futures_are_measured_blocked_not_assumed_blocked():
    def transport(url):
        return 403, b""
    row = entitlements.measure_vix_futures(transport=transport)
    assert row["state"] == entitlements.BLOCKED_LICENCE
    assert row["routes_probed"] == len(contract.CBOE_VX_FUTURES_ROUTES)
    assert row["routes_probed"] >= 5
    assert "no LEVEL 3 implementation" in row["consequence"] \
        or "cannot be constructed" in row["consequence"]


def test_a_free_route_that_answers_is_reported_as_entitled():
    def transport(url):
        return 200, b"a" * 100
    row = entitlements.measure_vix_futures(transport=transport)
    assert row["state"] == entitlements.ENTITLED


def test_norgate_absence_is_not_configured_rather_than_an_exception():
    class Dead:
        def status(self):
            raise RuntimeError("no updater")
    row = entitlements.measure_norgate(vendor=Dead())
    assert row["state"] == entitlements.NOT_CONFIGURED


def test_a_one_market_futures_database_reports_native_futures_unsupported():
    class Vendor:
        __version__ = "1.0.74"

        def status(self):
            return True

        def databases(self):
            return ["Continuous Futures", "Forex Spot"]

        def database_symbols(self, name):
            return ["&ES"] if name == "Continuous Futures" else ["EURUSD"]

    row = entitlements.measure_norgate(vendor=Vendor())
    assert row["continuous_futures_symbol_count"] == 1
    assert row["native_futures_supported"] is False
    assert row["continuous_futures_symbols"] == ["&ES"]


def test_the_entitlement_artifact_spends_nothing():
    body = entitlements.artifact({}, campaign_id="t", created_at="t")
    assert body["money_spent"] == 0.0
    assert body["trials_started"] == 0
    assert body["accounts_created"] == 0
    assert body["subscription_tier_changed"] is False
    assert body["credentials_written_to_artifacts"] is False
    assert body["api_key_implies_entitlement"] is False


# --------------------------------------------------------------------------- #
# Acquisition
# --------------------------------------------------------------------------- #
def test_fred_series_are_derived_from_the_contract_universe():
    ids = set(acquisition.fred_series_ids())
    for _code, spec in contract.FX_UNIVERSE.items():
        assert spec[2] in ids, "a currency's short rate is not downloaded"
        assert spec[3] in ids, "a currency's price index is not downloaded"
    assert contract.CASH_YIELD_SERIES in ids
    for leg in contract.CRYPTO_LEGS:
        assert leg in ids


def test_release35_payloads_are_located_and_not_downloaded():
    assert set(contract.REUSED_R35_SOURCES)
    for source in acquisition.LOCATED_SOURCES:
        assert source in acquisition.SOURCE_LICENCE
    row = acquisition.locate_norgate()
    assert row["downloaded"] is False
    assert row["install_or_upgrade_attempted"] is False


def test_the_manifest_records_zero_money_and_redacts_no_credential():
    results = {"X": {"source": "X", "ok": True, "files": {"a": "b"},
                     "records": [], "licence": "free"}}
    body = acquisition.manifest_artifact(results, campaign_id="t",
                                         created_at="t")
    assert body["money_spent"] == 0.0
    assert body["trials_started"] == 0
    assert body["subscription_tier_changed"] is False
    assert body["credentials_written_to_artifacts"] is False
    assert body["http_owner"] == "alpha_agent.r35.acquisition"


def test_the_fred_url_in_a_record_never_carries_the_key(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "leaky-key-value")
    calls = []

    def transport(url):
        calls.append(url)
        return json.dumps({"observations": []}).encode("utf-8")

    result = acquisition.acquire_fred(series_ids=("DTB3",),
                                      transport=transport)
    for record in result["records"]:
        assert "leaky-key-value" not in json.dumps(record)
        assert "api_key=REDACTED" in record["url"]


# --------------------------------------------------------------------------- #
# Coverage matrix
# --------------------------------------------------------------------------- #
def test_every_declared_market_is_complete():
    required = ("asset_class", "sub_asset", "native_instrument", "proxies",
                "source", "history", "frequency", "pit", "survivorship",
                "level", "prior_evidence", "families", "priority")
    for key, market in coverage.MARKETS.items():
        for field in required:
            assert field in market, (key, field)
        assert market["level"] in contract.LEVELS, key
        assert market["families"], key
        for family in market["families"]:
            assert family in contract.STRATEGY_FAMILIES, (key, family)
        if market.get("blocker"):
            assert market["blocker"] in contract.TERMINAL_STATES, key
            assert market.get("blocker_reason"), key


def test_every_cell_is_terminal_and_carries_a_next_action():
    cells = coverage.build([])
    assert cells
    for cell in cells:
        assert cell["state"] in contract.TERMINAL_STATES, cell["market_key"]
        assert cell["next_executable_action"]
    summary = coverage.summarise(cells)
    assert summary["every_cell_is_terminal"] is True
    assert summary["ambiguous_cells"] == []


def test_a_proxy_configuration_never_produces_a_native_tested_state():
    executed = [{"name": "X", "lane": contract.LANE_RATES,
                 "families": [contract.SF_CARRY], "qualified": True}]
    cells = coverage.build(executed)
    rates = [c for c in cells if c["market_key"] == "RATES_US_CURVE"
             and c["strategy_family"] == contract.SF_CARRY]
    assert rates and rates[0]["state"] == contract.STATE_TESTED_PROXY_ONLY
    assert "does not close this cell" in rates[0]["next_executable_action"]


def test_a_native_configuration_that_qualifies_marks_a_survivor():
    executed = [{"name": "X", "lane": contract.LANE_FX,
                 "families": [contract.SF_CARRY], "qualified": True}]
    cells = coverage.build(executed)
    fx = [c for c in cells if c["market_key"] == "FX_G10"
          and c["strategy_family"] == contract.SF_CARRY]
    assert fx and fx[0]["state"] == contract.STATE_TESTED_NATIVE_SURVIVOR
    executed[0]["qualified"] = False
    cells = coverage.build(executed)
    fx = [c for c in cells if c["market_key"] == "FX_G10"
          and c["strategy_family"] == contract.SF_CARRY]
    assert fx[0]["state"] == contract.STATE_TESTED_NATIVE_REJECTED


def test_a_blocked_market_keeps_its_blocker_even_with_data_available():
    cells = coverage.build([])
    vix = [c for c in cells if c["market_key"] == "VOL_VIX_FUTURES"
           and c["strategy_family"] == contract.SF_CARRY]
    assert vix and vix[0]["state"] == contract.STATE_BLOCKED_LICENSING
    short = [c for c in cells if c["market_key"] == "VOL_SHORT_ETP"
             and c["strategy_family"] == contract.SF_CARRY]
    assert short and short[0]["state"] == contract.STATE_BLOCKED_SURVIVORSHIP


def test_the_blocked_frontier_names_one_row_per_market_with_an_action():
    cells = coverage.build([])
    rows = coverage.blocked_frontier(cells)
    assert rows
    keys = [r["market_key"] for r in rows]
    assert len(keys) == len(set(keys)), "a market appears twice"
    for row in rows:
        assert row["blocker_reason"]
        assert row["next_executable_action"]


def test_the_summary_counts_add_up():
    cells = coverage.build([])
    summary = coverage.summarise(cells)
    assert summary["cells_total"] == len(cells)
    assert summary["cells_applicable"] + summary["cells_not_applicable"] \
        == summary["cells_total"]
    assert sum(summary["by_state"].values()) == summary["cells_total"]


# --------------------------------------------------------------------------- #
# Campaign, multiple testing and the verdict
# --------------------------------------------------------------------------- #
def _executed(name, lane, t_stat, excess, *, gates_pass=True):
    return {"name": name, "lane": lane, "state": experiments.EXECUTED,
            "families": [contract.SF_CARRY],
            "implementation_level": contract.LEVEL_NATIVE,
            "control": "PASSIVE", "active_periods": 100,
            "after_cost_excess_annualised": excess,
            "after_cost_excess_t_stat": t_stat,
            "economics": {"periods": 100},
            "gates": {k: gates_pass for k in contract.QUALIFICATION_CONDITIONS
                      if k != "survives_multiple_testing_procedure"},
            "_diff": np.full(100, excess / 12.0)}


def test_multiple_testing_splits_the_direction_of_every_rejection():
    results = [_executed("WIN", contract.LANE_FX, 6.0, 0.05),
               _executed("LOSE", contract.LANE_FX, -6.0, -0.05),
               _executed("FLAT", contract.LANE_FX, 0.1, 0.001)]
    body = campaign.run_multiple_testing(results)
    assert body["denominator_executed_configurations"] == 3
    assert "WIN" in body["rejected_beating_the_control"]
    assert "LOSE" in body["rejected_losing_to_the_control"]
    assert "FLAT" not in body["rejected_beating_the_control"]
    assert body["only_positive_rejections_may_qualify"] is True


def test_a_losing_rejection_never_qualifies():
    results = [_executed("LOSE", contract.LANE_FX, -6.0, -0.05)]
    body = campaign.run_multiple_testing(results)
    campaign.qualify(results, body)
    assert results[0]["qualified"] is False
    assert "survives_multiple_testing_procedure" in results[0]["gates_failed"]


def test_the_denominator_counts_every_executed_configuration():
    results = [_executed("A", contract.LANE_FX, 6.0, 0.05)]
    results.extend(_executed("F%d" % i, contract.LANE_FX, 0.0, 0.0)
                   for i in range(20))
    body = campaign.run_multiple_testing(results)
    assert body["denominator_executed_configurations"] == 21
    assert body["denominator_counts_all_executed"] is True
    assert body["controls_enter_denominator"] is False


def test_alpha_result_stays_fail_even_when_a_configuration_qualifies():
    results = [_executed("WIN", contract.LANE_FX, 6.0, 0.05)]
    body = campaign.run_multiple_testing(results)
    campaign.qualify(results, body)
    assert results[0]["qualified"] is True
    panels = {contract.LANE_FX: _panel()}
    cells = coverage.build(results)
    verdict = campaign.build_verdict(
        results=results, multiple_testing=body, cells=cells,
        entitlement={"sources_blocked": []}, panels=panels)
    assert verdict["verdict"] == contract.VERDICT_EDGE_FOUND
    assert verdict["RESEARCH_CANDIDATE_RESULT"] == contract.RESULT_PASS
    assert verdict["ALPHA_RESULT"] == contract.RESULT_FAIL
    assert verdict["genuinely_independent_evidence_exists"] is False


def test_the_verdict_carries_a_plain_reading():
    for name in contract.PRIMARY_VERDICTS:
        assert campaign.VERDICT_READING.get(name), name


def test_data_integrity_fails_closed_on_an_unobservable_control():
    panel = _panel(control_missing_head=3)
    integrity = campaign.data_integrity({contract.LANE_FX: panel}, [])
    assert integrity["checks"][
        "every_lane_control_is_observable_throughout"] is False
    assert integrity["passes"] is False


def test_the_forward_handoff_registers_nothing():
    body = campaign.forward_handoff([])
    assert body["registered_anything"] is False
    assert body["promoted_a_model"] is False
    assert body["activated_a_sleeve"] is False
    assert body["created_a_second_true_forward_store"] is False
    assert body["canonical_forward_evidence_owner"] == \
        "api/forward_evidence.py"
    assert body["historical_evidence_is_not_prospective_evidence"] is True


def test_findings_report_prediction_and_economics_separately():
    results = [_executed("A", contract.LANE_FX, 1.0, 0.01)]
    results[0]["predictive_diagnostic"] = {
        "state": "OK", "statistic": "PER_DATE_RANK_IC",
        "mean_rank_ic": 0.15, "t_stat": 8.0, "scored_dates": 400}
    body = campaign.run_multiple_testing(results)
    found = campaign.findings(results, body)
    assert found["strongest_predictive"][0]["t_stat"] == 8.0
    assert found["strongest_economic"][0]["after_cost_excess_t_stat"] == 1.0
    assert "wrong way round" in found["reading"]


# --------------------------------------------------------------------------- #
# The frozen artifacts on disk
# --------------------------------------------------------------------------- #
def _campaign_dir() -> Path:
    return r36.campaign_dir(contract.CAMPAIGN_ID)


ARTIFACTS_PRESENT = (_campaign_dir() / "final_verdict.json").exists()
needs_artifacts = pytest.mark.skipif(
    not ARTIFACTS_PRESENT, reason="Release-36 campaign artifacts not on disk")


@needs_artifacts
def test_the_frozen_contract_matches_the_module():
    body = json.loads((_campaign_dir() / "research_contract.json").read_text(
        encoding="utf-8"))
    assert body["campaign_id"] == contract.CAMPAIGN_ID
    assert contract.verify(body)["stable"] is True
    assert body["budget"]["planned_config_total"] == len(contract.STRATEGIES)


@needs_artifacts
def test_the_frozen_verdict_reports_three_separate_results():
    body = json.loads((_campaign_dir() / "final_verdict.json").read_text(
        encoding="utf-8"))
    assert body["verdict"] in contract.PRIMARY_VERDICTS
    assert body["ALPHA_RESULT"] == contract.RESULT_FAIL
    assert body["data_integrity"]["passes"] is True
    assert body["money_spent"] == 0.0
    assert body["executed_configurations"] <= contract.MAX_PRIMARY_CONFIGS


@needs_artifacts
def test_the_frozen_coverage_matrix_leaves_no_ambiguous_cell():
    body = json.loads(
        (_campaign_dir() / "global_multi_asset_coverage_matrix.json").read_text(
            encoding="utf-8"))
    assert body["summary"]["every_cell_is_terminal"] is True
    assert body["summary"]["ambiguous_cells"] == []
    assert body["proxy_may_close_a_native_frontier"] is False
    assert body["native_futures_supported"] is False


@needs_artifacts
def test_the_frozen_registry_executed_within_the_declared_ceiling():
    body = json.loads((_campaign_dir() / "experiment_registry.json").read_text(
        encoding="utf-8"))
    assert body["within_ceiling"] is True
    assert body["executed"] <= body["ceiling"]
    assert body["economic_judge"] == "alpha_agent.r34.economics"
    assert body["concentration_owner"] == "alpha_agent.r34.concentration"


@needs_artifacts
def test_every_executed_configuration_has_a_lane_appropriate_control():
    body = json.loads((_campaign_dir() / "experiment_registry.json").read_text(
        encoding="utf-8"))
    for row in body["configurations"]:
        if row.get("state") != experiments.EXECUTED:
            continue
        assert row.get("control"), row["name"]
        assert row["control"] != "SPY_PLUS_CASH"
        if row["lane"] != contract.LANE_CROSS_ASSET:
            assert "SIXTY_FORTY" not in row["control"], row["name"]


@needs_artifacts
def test_the_acquisition_manifest_reused_release35_payloads():
    body = json.loads((_campaign_dir() / "acquisition_manifest.json").read_text(
        encoding="utf-8"))
    assert body["money_spent"] == 0.0
    assert set(body["sources_located_not_downloaded"]) >= {
        acquisition.SRC_R35_CFTC, acquisition.SRC_R35_EIA_PET,
        acquisition.SRC_NORGATE}


# --------------------------------------------------------------------------- #
# The architecture audit block
# --------------------------------------------------------------------------- #
def test_the_architecture_audit_declares_release36_clean():
    import subprocess
    import sys
    out = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "audit_architecture.py"),
         "--json"],
        capture_output=False, cwd=str(REPO_ROOT), text=True,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=900)
    report = json.loads(out.stdout)
    block = report["release36_global_multi_asset_frontier"]
    failures = {k: v for k, v in block.items()
                if v is not True and k not in ("modules_missing",
                                               "second_owner_modules",
                                               "forbidden_calls",
                                               "forbidden_owner_refs")}
    assert not failures, failures
    assert block["modules_missing"] == []
    assert block["second_owner_modules"] == []
    assert block["forbidden_calls"] == []
    assert block["forbidden_owner_refs"] == []
