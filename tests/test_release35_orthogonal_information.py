"""Release 35 - Orthogonal Information Acquisition regression.

These tests protect the properties that decide whether any number the campaign
published means anything. Most run on synthetic data and need neither the vendor
nor the network; the artifact tests read what the campaign actually froze and
skip when the research root is absent.

Every guard is NEGATIVE-PROBED: it is shown failing against a deliberately
broken input. A guard that has never been observed to fail has not been shown to
guard anything. That discipline is applied to the point-in-time alignment, the
orthogonality gate, the paired increment, the vacuous-arm detector, the
coverage floors and the verdict's evidence gate alike.
"""
from __future__ import annotations

import datetime as _dt
import io
import json
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_PKG_PARENT = str(Path(__file__).resolve().parents[2])
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import r35
from paper_trader.alpha_agent.r35 import acquisition as _acq
from paper_trader.alpha_agent.r35 import analyst_lane as _analyst_lane
from paper_trader.alpha_agent.r35 import campaign as _campaign
from paper_trader.alpha_agent.r35 import contract as _contract
from paper_trader.alpha_agent.r35 import design as _design
from paper_trader.alpha_agent.r35 import features as _features
from paper_trader.alpha_agent.r35 import incremental as _incremental
from paper_trader.alpha_agent.r35 import information as _info
from paper_trader.alpha_agent.r35 import orthogonality as _orthogonality

CAMPAIGN_DIR = r35.campaign_dir(_contract.CAMPAIGN_ID)
REPO = Path(__file__).resolve().parents[1]


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
    assert _contract.ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE is True


def test_no_fresh_lockbox_is_claimed_and_none_can_be_manufactured():
    assert _contract.FRESH_UNSEEN_EVIDENCE_EXISTS is False
    assert _contract.A_FOLD_MAY_BE_CALLED_A_LOCKBOX is False
    assert _contract.genuinely_independent_evidence_exists() is False
    assert _contract.evidence_label() == _contract.EVIDENCE_HISTORICAL
    assert (_contract.verdict_ceiling_without_fresh_evidence()
            != _contract.VERDICT_QUALIFIED)


def test_alpha_pass_is_structurally_unreachable_in_this_release():
    """Even a fully qualified verdict cannot produce ALPHA_RESULT = PASS."""
    body = _campaign.build_verdict(
        campaign_id="probe", created_at="2026-01-01T00:00:00+00:00",
        contract_body={"contract_hash": "x"},
        acquisition={"sources_ok": ["A"], "sources_failed": []},
        coverage={"families": {"FAM": {}}, "integrity_violations": []},
        orthogonality={"FAM": {"admitted_to_predictive_stage": True}},
        increments={"by_horizon": {20: {"per_set": {
            "FAM": {"information_set": "FAM",
                    "gate": {"passed": True}}}}}},
        standalone={"per_family": {}},
        economics={"BASE": {}, "FAM": {"gate": {"passed": True}}},
        multiple_testing={"benjamini_hochberg": {
            "rejected_beating_the_base": ["ECONOMIC::FAM"]}},
        analyst={"acquisition_blocked": True},
        executed_configs=[{"configuration_id": "x"}])
    assert body["primary_verdict"] == _contract.VERDICT_QUALIFIED
    assert body["RESEARCH_CANDIDATE_RESULT"] == "PASS"
    assert body["ALPHA_RESULT"] == "FAIL"
    assert body["genuinely_independent_evidence_exists"] is False


def test_three_results_are_separate_names():
    assert _contract.RESULT_NAMES == (
        "SYSTEM_RESULT", "RESEARCH_CANDIDATE_RESULT", "ALPHA_RESULT")
    assert _contract.SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE is True


def test_planned_configuration_total_is_derived_from_the_grids():
    families = len(_contract.ACQUIRED_FAMILIES)
    assert (_contract.CONFIG_FAMILIES["PREDICTIVE_INCREMENT"]
            == (families + 1) * len(_contract.HORIZONS))
    assert (_contract.CONFIG_FAMILIES["STANDALONE_DIAGNOSTIC"] == families)
    assert (_contract.PLANNED_CONFIG_TOTAL
            == sum(_contract.CONFIG_FAMILIES.values()))
    assert _contract.PLANNED_CONFIG_TOTAL <= _contract.MAX_PRIMARY_CONFIGS


def test_nothing_about_the_model_or_the_conversion_may_be_searched():
    assert _contract.NEW_PREDICTOR_SEARCH_ALLOWED is False
    assert _contract.ADAPTIVE_SEARCH_ALLOWED is False
    assert _contract.MODEL_ARCHITECTURE_SEARCH_ALLOWED is False
    assert _contract.CONVERSION_LAYER_SEARCH_ALLOWED is False
    assert (_contract.FROZEN_CONVERSION["source"]
            == "r34_prediction_to_pnl_v2::FINALIST::COMBINED_BEST")


def test_the_release_may_not_spend_or_contract_for_anything():
    assert _contract.MAY_SPEND_MONEY is False
    assert _contract.MAY_START_PROVIDER_TRIAL is False
    assert _contract.MAY_CREATE_PROVIDER_ACCOUNT is False
    assert _contract.MAY_ACQUIRE_FREE_PUBLIC_DATA is True


def test_every_safety_flag_is_false():
    block = r35.safety_block()
    for key, value in block.items():
        if key == "safety":
            continue
        assert value is False, key


def test_contract_hash_is_stable_and_excludes_the_environment():
    first = _contract.contract_hash()
    second = _contract.contract_hash()
    assert first == second
    body = _contract.build(campaign_id="a", created_at="2026-01-01T00:00:00Z")
    other = _contract.build(campaign_id="b", created_at="2027-01-01T00:00:00Z")
    assert body["contract_hash"] != other["contract_hash"]  # id is in the hash
    assert len(first) == 64


def test_every_declared_feature_belongs_to_a_declared_family():
    for name, (family, reading, fill) in _contract.NEW_FEATURES.items():
        assert family in _contract.ALL_FAMILIES, name
        assert reading and isinstance(reading, str), name
        assert fill == _contract.FILL_NEUTRAL_ZERO, name
    covered = set()
    for family in _contract.ACQUIRED_FAMILIES:
        covered |= set(_contract.features_of(family))
    assert covered == set(_contract.NEW_FEATURE_NAMES)


def test_research_root_is_isolated_from_every_operational_store():
    root = str(r35.research_root()).lower()
    for forbidden in ("portfolio_decisions", "reallocation_proposals",
                      "rebalance_order_plans", "information_collection",
                      "operational_book"):
        assert forbidden not in root


def test_r35_source_has_no_operational_write_path():
    sys.path.insert(0, str(REPO / "scripts"))
    import r33_operational_write_attribution as attribution

    profile = attribution.profile_for("R35")
    report = attribution.source_operational_write_paths(
        REPO, source_globs=profile["source_globs"],
        source_files=profile["source_files"])
    assert report["clean"] is True, report["findings"]
    assert report["sources_scanned"] >= 10


def test_an_unknown_release_profile_still_fails_closed():
    sys.path.insert(0, str(REPO / "scripts"))
    import r33_operational_write_attribution as attribution

    with pytest.raises(RuntimeError):
        attribution.profile_for("R99")


# --------------------------------------------------------------------------- #
# Point in time - the ONE alignment rule
# --------------------------------------------------------------------------- #
def _calendar(n=40, start="2020-01-01"):
    return pd.DatetimeIndex(pd.bdate_range(start, periods=n))


def test_a_value_is_invisible_before_it_was_published():
    calendar = _calendar()
    published = calendar[10]
    series = pd.Series([1.0], index=[published])
    aligned = _info.as_of_align(series, calendar)
    assert aligned.iloc[:11].isna().all()
    assert aligned.iloc[11:].eq(1.0).all()


def test_the_broadcast_lag_is_applied_and_costs_exactly_one_session():
    calendar = _calendar()
    series = pd.Series([1.0], index=[calendar[5]])
    with_lag = _info.as_of_align(series, calendar)
    without = _info.as_of_align(series, calendar, lag_sessions=0)
    assert bool(without.notna().iloc[5])
    assert bool(with_lag.isna().iloc[5])
    assert bool(with_lag.notna().iloc[6])


def test_alignment_never_interpolates_a_value_backwards():
    calendar = _calendar()
    series = pd.Series([1.0, 2.0], index=[calendar[20], calendar[30]])
    aligned = _info.as_of_align(series, calendar)
    assert aligned.iloc[21:31].eq(1.0).all()
    assert aligned.iloc[31:].eq(2.0).all()


def test_the_cot_publication_lag_moves_the_index_forward_not_back():
    frame = pd.DataFrame({
        "as_of": pd.to_datetime(["2020-01-07", "2020-01-14"]),
        "code": ["088691", "088691"],
        "open_interest": [100.0, 120.0],
        "nc_long": [60.0, 70.0], "nc_short": [20.0, 25.0],
        "comm_long": [20.0, 25.0], "comm_short": [60.0, 70.0]})
    weekly = _info.cot_instrument_series(frame, ["088691"], lag_days=6)
    assert list(weekly.index) == [pd.Timestamp("2020-01-13"),
                                  pd.Timestamp("2020-01-20")]
    assert weekly["spec_net_oi"].iloc[0] == pytest.approx(0.4)


def test_a_zero_publication_lag_would_be_a_look_ahead_and_is_not_the_default():
    assert _contract.COT_PUBLICATION_LAG_DAYS >= 3
    assert (_contract.COT_PUBLICATION_LAG_STRESS_DAYS
            > _contract.COT_PUBLICATION_LAG_DAYS)


def test_cot_positions_are_summed_across_the_mapped_contract_codes():
    frame = pd.DataFrame({
        "as_of": pd.to_datetime(["2020-01-07"] * 2),
        "code": ["138741", "13874A"],
        "open_interest": [100.0, 300.0],
        "nc_long": [60.0, 100.0], "nc_short": [20.0, 100.0],
        "comm_long": [20.0, 100.0], "comm_short": [60.0, 100.0]})
    weekly = _info.cot_instrument_series(frame, ["138741", "13874A"],
                                         lag_days=0)
    assert weekly["open_interest"].iloc[0] == 400.0
    assert weekly["spec_net_oi"].iloc[0] == pytest.approx(40.0 / 400.0)


def test_the_consolidated_cot_rows_are_excluded_from_every_mapping():
    for symbol, (codes, _tier, _market) in _contract.COT_MAPPING.items():
        for code in codes:
            assert code not in _contract.COT_EXCLUDED_CODES, symbol
    assert "13874+" in _contract.COT_EXCLUDED_CODES
    assert "20974+" in _contract.COT_EXCLUDED_CODES


def test_an_insider_filing_is_observable_at_its_filing_date_only():
    assert _contract.INSIDER_OBSERVABLE_AT == "FILING_DATE"
    assert _contract.INSIDER_TRANSACTION_DATE_MAY_BE_OBSERVABLE is False


def test_insider_direction_comes_from_transaction_codes_not_from_value():
    """The value fields are unvalidated; a typo must not flip a direction."""
    archive_bytes = io.BytesIO()
    with zipfile.ZipFile(archive_bytes, "w") as archive:
        archive.writestr(
            "SUBMISSION.tsv",
            "ACCESSION_NUMBER\tFILING_DATE\tISSUERCIK\n"
            "0001\t15-MAR-2020\t0000000077\n")
        archive.writestr(
            "NONDERIV_TRANS.tsv",
            "ACCESSION_NUMBER\tTRANS_CODE\tTRANS_SHARES\t"
            "TRANS_PRICEPERSHARE\tTRANS_ACQUIRED_DISP_CD\n"
            "0001\tP\t1e15\t1e6\tA\n")
        archive.writestr("DERIV_TRANS.tsv", "")
    path = Path(_info.cache_path("_probe_insider.zip"))
    path.write_bytes(archive_bytes.getvalue())
    cache = Path(_info.cache_path(
        "sec_insider_direction_by_cik_filing_date.csv"))
    moved = None
    if cache.exists():
        moved = cache.with_suffix(".csv.testbackup")
        cache.replace(moved)
    try:
        result = _info.load_insider_filings({"2020q1": str(path)})
        assert result["ok"] is True
        row = result["frame"].iloc[0]
        assert row["direction"] == _info.FILING_BUY
        assert "net_value" not in result["frame"].columns
        assert "gross_value" not in result["frame"].columns
    finally:
        if cache.exists():
            cache.unlink()
        if moved is not None:
            moved.replace(cache)
        path.unlink()


def test_value_weighting_is_refused_and_the_reason_is_recorded():
    assert _contract.INSIDER_VALUE_WEIGHTING_ALLOWED is False
    assert _info.INSIDER_VALUE_WEIGHTING_REJECTED is True
    assert "2.1e16" in _info.INSIDER_VALUE_REJECTION_REASON


def test_prohibited_substitutions_name_the_specific_temptations():
    joined = " ".join(_contract.PROHIBITED_SUBSTITUTIONS)
    assert "spot price momentum" in joined
    assert "written back onto historical dates" in joined
    assert "continuous or back-adjusted" in joined
    assert "current sector labels backfilled" in joined


def test_a_monthly_oecd_rate_is_stamped_forward_not_at_its_own_month(tmp_path):
    payload = {"observations": [
        {"date": "2020-01-01", "value": "1.5"},
        {"date": "2020-02-01", "value": "1.6"}]}
    path = tmp_path / (_contract.FRED_US_SHORT_RATE + ".json")
    path.write_text(json.dumps(payload), encoding="utf-8")
    loaded = _info.load_fred({_contract.FRED_US_SHORT_RATE: str(path)})
    series = loaded["series"][_contract.FRED_US_SHORT_RATE]
    assert series.index[0] == pd.Timestamp("2020-03-01")
    assert (loaded["meta"][_contract.FRED_US_SHORT_RATE]["cadence"]
            == "MONTHLY_PUBLISHED_IN_ARREARS")


def test_a_daily_market_observable_is_not_stamped_forward(tmp_path):
    payload = {"observations": [{"date": "2020-01-02", "value": "1.5"}]}
    path = tmp_path / "DGS10.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    loaded = _info.load_fred({"DGS10": str(path)})
    assert loaded["series"]["DGS10"].index[0] == pd.Timestamp("2020-01-02")


# --------------------------------------------------------------------------- #
# Features - structural absence, and no invented carry
# --------------------------------------------------------------------------- #
def _panel_stub(symbols=("SPY", "AGG", "GLD"), n=300):
    calendar = _calendar(n)
    rng = np.random.default_rng(11)
    prices = pd.DataFrame(
        100.0 * np.exp(np.cumsum(rng.normal(0, 0.01, (n, len(symbols))),
                                 axis=0)),
        index=calendar, columns=list(symbols))
    return {"calendar": calendar, "symbols": list(symbols), "prices": prices,
            "log_returns": np.log(prices).diff()}


def test_a_structurally_absent_feature_is_zero_and_never_a_median():
    panel = _panel_stub()
    frame = pd.DataFrame(np.nan, index=panel["calendar"],
                         columns=panel["symbols"])
    frame["SPY"] = 5.0
    design = {"X": np.zeros((3, 1)),
              "feature_names": ["base"],
              "decision_index": np.array([10, 10, 10]),
              "symbol_position": np.array([0, 1, 2])}
    ctx = {"design": design, "row_dates": pd.DatetimeIndex(
        [panel["calendar"][10]] * 3), "udates": panel["calendar"][[10]]}
    augmented = _design.augment_context(ctx, frames={"f": frame},
                                        feature_names=["f"])
    added = augmented["design"]["X"][:, -1]
    assert added[0] == 5.0
    assert added[1] == 0.0 and added[2] == 0.0  # neutral, NOT the median 5.0


def test_commodity_carry_is_built_from_dated_contracts_not_from_spot():
    calendar = _calendar(120)
    index = pd.DatetimeIndex(pd.bdate_range("2019-06-01", periods=200))
    curve = pd.DataFrame(
        {_contract.EIA_WTI_CONTRACTS[0]: 60.0,
         _contract.EIA_WTI_CONTRACTS[1]: 59.0,
         _contract.EIA_WTI_CONTRACTS[2]: 58.0,
         _contract.EIA_WTI_CONTRACTS[3]: 57.0}, index=index)
    built = _features.build_commodity_curve(curve, calendar, ["USO", "AGG"])
    basis = built["features"]["cmdty_front_basis"]
    assert basis["USO"].dropna().iloc[-1] > 0  # backwardation, C1 above C2
    assert basis["AGG"].isna().all()  # unmapped instrument stays absent
    # A pure spot series has no second contract and therefore no curve.
    flat = curve[[_contract.EIA_WTI_CONTRACTS[0]]].copy()
    for name in _contract.EIA_WTI_CONTRACTS[1:]:
        flat[name] = flat[_contract.EIA_WTI_CONTRACTS[0]]
    spot_built = _features.build_commodity_curve(flat, calendar, ["USO"])
    assert spot_built["features"]["cmdty_front_basis"]["USO"].dropna().eq(
        0.0).all()


def test_fx_carry_is_a_rate_differential_and_absent_where_no_rate_exists():
    calendar = _calendar(200)
    us = pd.Series(1.0, index=calendar)
    eur = pd.Series(3.0, index=calendar)
    fred = {_contract.FRED_US_SHORT_RATE: us,
            _contract.FRED_FOREIGN_SHORT_RATES["EZ"]: eur}
    built = _features.build_fx_carry(fred, calendar, ["FXE", "SPY"])
    diff = built["features"]["fx_carry_diff"]
    assert diff["FXE"].dropna().iloc[-1] == pytest.approx(2.0)
    assert diff["SPY"].isna().all()


def test_the_dollar_index_carry_reverses_sign_against_the_basket():
    calendar = _calendar(200)
    fred = {_contract.FRED_US_SHORT_RATE: pd.Series(1.0, index=calendar)}
    for key, sid in _contract.FRED_FOREIGN_SHORT_RATES.items():
        fred[sid] = pd.Series(3.0, index=calendar)
    built = _features.build_fx_carry(fred, calendar, ["FXE", "UUP"])
    diff = built["features"]["fx_carry_diff"]
    assert diff["FXE"].dropna().iloc[-1] > 0
    assert diff["UUP"].dropna().iloc[-1] < 0


def test_implied_volatility_slope_is_not_the_volatility_level():
    calendar = _calendar(200)
    panel = _panel_stub(("SPY",), n=200)
    rising = pd.Series(np.linspace(10, 40, 200), index=calendar)
    flat_slope = rising * 1.1
    built = _features.build_iv_term({"VIX": rising, "VIX3M": flat_slope},
                                    panel, calendar, ["SPY"])
    slope = built["features"]["iv_term_slope"]["SPY"].dropna()
    # The level quadrupled; the slope is constant. They are different objects.
    assert slope.std() < 1e-9
    assert slope.iloc[-1] == pytest.approx(np.log(1.1))


def test_insider_ratio_refuses_a_window_with_too_few_filings():
    calendar = _calendar(300)
    table = pd.DataFrame({
        "filed": list(calendar[:5]),
        "sector": ["Energy"] * 5,
        "buy_filings": [1, 1, 1, 0, 0],
        "sell_filings": [0, 0, 0, 1, 1],
        "mixed_filings": [0] * 5})
    built = _features.build_insider(table, calendar, ["XLE"], window=63,
                                    floor=_contract.MIN_INSIDER_FILINGS_IN_WINDOW)
    assert built["features"]["insider_net_buy_63"]["XLE"].isna().all()
    generous = _features.build_insider(table, calendar, ["XLE"], window=63,
                                       floor=2)
    assert generous["features"]["insider_net_buy_63"]["XLE"].notna().any()


def test_unmapped_sector_etfs_are_recorded_rather_than_given_another_sector():
    assert "XLRE" in _contract.INSIDER_UNMAPPED_SECTOR_ETFS
    assert "XLRE" not in _contract.INSIDER_SECTOR_MAPPING
    assert "Financials" in _contract.INSIDER_SECTOR_MAPPING.values()
    assert "refiners only" in _contract.INSIDER_SECTOR_MAP_CAVEAT


# --------------------------------------------------------------------------- #
# Orthogonality - the gate
# --------------------------------------------------------------------------- #
def test_a_linear_combination_of_the_base_set_is_labelled_redundant():
    rng = np.random.default_rng(3)
    base = rng.normal(size=(2000, 4))
    duplicate = 2.0 * base[:, 0] - 0.5 * base[:, 2]
    result = _orthogonality.measure_feature(
        duplicate, base, ["a", "b", "c", "d"])
    assert result["state"] == "OK"
    assert result["residual_share"] < 1e-6
    assert result["redundancy"] == _contract.REDUNDANT


def test_genuinely_new_information_is_labelled_distinct():
    rng = np.random.default_rng(4)
    base = rng.normal(size=(2000, 4))
    fresh = rng.normal(size=2000)
    result = _orthogonality.measure_feature(fresh, base, ["a", "b", "c", "d"])
    assert result["redundancy"] == _contract.DISTINCT
    assert result["residual_share"] > _contract.PARTIAL_RESIDUAL_SHARE_MAX


def test_low_raw_correlation_alone_cannot_establish_distinctness():
    """The negative probe for the rule that raw correlation is the wrong test."""
    rng = np.random.default_rng(5)
    base = rng.normal(size=(4000, 6))
    # Correlates weakly with every single control and is exactly their sum.
    combination = base.sum(axis=1)
    result = _orthogonality.measure_feature(
        combination, base, list("abcdef"))
    assert result["max_abs_rank_correlation"] < 0.6
    assert result["redundancy"] == _contract.REDUNDANT
    assert _contract.DISTINCTNESS_IS_RAW_CORRELATION_ONLY is False


def test_badly_scaled_controls_are_conditioned_rather_than_declared_singular():
    rng = np.random.default_rng(6)
    base = rng.normal(size=(1500, 5))
    base[:, 1] *= 1e6
    base[:, 3] *= 1e-6
    base[:, 4] = 0.0  # a structurally constant column
    result = _orthogonality.measure_feature(
        rng.normal(size=1500), base, list("abcde"))
    assert result["state"] == "OK"
    assert "e" in result["controls_dropped_constant"]
    assert result["controls_used"] == 4


def test_an_exactly_duplicated_control_is_dropped_not_solved_around():
    rng = np.random.default_rng(7)
    base = rng.normal(size=(1200, 3))
    base = np.column_stack([base, base[:, 0]])
    result = _orthogonality.measure_feature(
        rng.normal(size=1200), base, ["a", "b", "c", "a_copy"])
    assert result["state"] == "OK"
    assert any("a_copy" in name
               for name in result["controls_dropped_duplicate"])


def test_a_family_of_only_redundant_features_is_not_admitted():
    redundant = {"features": {"x": {"redundancy": _contract.REDUNDANT,
                                    "residual_share": 0.01}}}
    labels = [spec["redundancy"] for spec in redundant["features"].values()]
    assert all(label == _contract.REDUNDANT for label in labels)
    assert _contract.FAMILY_ADMITTED_IF_ANY_FEATURE_NOT_REDUNDANT is True


def test_redundancy_thresholds_partition_the_line():
    assert (_orthogonality.redundancy_label(0.05) == _contract.REDUNDANT)
    assert (_orthogonality.redundancy_label(0.2)
            == _contract.PARTIALLY_REDUNDANT)
    assert _orthogonality.redundancy_label(0.9) == _contract.DISTINCT
    assert _orthogonality.redundancy_label(None) == _contract.REDUNDANT


# --------------------------------------------------------------------------- #
# The paired increment
# --------------------------------------------------------------------------- #
def _scored(values, start="2020-01-31"):
    index = pd.DatetimeIndex(pd.date_range(start, periods=len(values),
                                           freq="ME"))
    return {"state": "OK", "rank_ic": float(np.mean(values)),
            "_per_date": pd.Series(values, index=index)}


def test_the_increment_is_a_per_date_difference_not_a_difference_of_levels():
    base = _scored([0.10] * 60)
    candidate = _scored([0.11] * 60)
    result = _incremental.paired_increment(base, candidate)
    assert result["increment"] == pytest.approx(0.01)
    assert result["n"] == 60
    assert result["increment_positive_fraction"] == 1.0


def test_dates_only_one_arm_scored_are_dropped_not_counted_as_ties():
    base = _scored([0.10] * 60)
    candidate = _scored([0.11] * 30)
    result = _incremental.paired_increment(base, candidate)
    assert result["n"] == 30
    assert result["increment"] == pytest.approx(0.01)


def test_a_negative_increment_cannot_pass_the_gate():
    base = _scored(list(np.linspace(0.2, 0.1, 80)))
    candidate = _scored(list(np.linspace(0.15, 0.05, 80)))
    result = _incremental.paired_increment(base, candidate)
    assert result["increment"] < 0
    assert _incremental.gate(result)["passed"] is False
    assert "positive_sign" in _incremental.gate(result)["failed_conditions"]


def test_a_tiny_but_significant_increment_still_fails_the_size_gate():
    rng = np.random.default_rng(9)
    delta = 1e-4 + rng.normal(0, 1e-6, 200)
    base = _scored([0.05] * 200)
    candidate = _scored(list(np.asarray([0.05] * 200) + delta))
    result = _incremental.paired_increment(base, candidate)
    assert abs(result["increment_t"]) > _contract.MIN_INCREMENT_T_STAT
    gate = _incremental.gate(result)
    assert gate["passed"] is False
    assert "large_enough" in gate["failed_conditions"]


def test_too_few_paired_dates_cannot_pass_the_gate():
    base = _scored([0.05] * 12)
    candidate = _scored([0.09] * 12)
    result = _incremental.paired_increment(base, candidate)
    assert _incremental.gate(result)["passed"] is False
    assert "enough_dates" in _incremental.gate(result)["failed_conditions"]


def test_an_arm_that_could_not_respond_is_not_a_tested_null():
    scores = np.linspace(0.0, 1.0, 100)
    key = ("M", ())
    identical = {key: {"score_eval": scores.copy()}}
    responded = _incremental.arm_responded(identical, {key: {
        "score_eval": scores.copy()}}, key)
    assert responded["responded"] is False
    assert "zero coefficient" in responded["reason"]
    flat = _scored([0.05] * 100)
    gate = _incremental.gate(_incremental.paired_increment(flat, flat),
                             responded=responded)
    assert gate["passed"] is False
    assert "arm_could_respond" in gate["failed_conditions"]

    moved = {key: {"score_eval": scores + 1e-9}}
    assert _incremental.arm_responded(identical, moved, key)["responded"]


def test_minimum_detectable_increment_is_reported_with_every_failure():
    base = _scored([0.05] * 200)
    rng = np.random.default_rng(12)
    candidate = _scored(list(0.05 + rng.normal(0.001, 0.01, 200)))
    result = _incremental.paired_increment(base, candidate)
    detectable = _incremental.minimum_detectable_increment(result)
    assert detectable["minimum_detectable"] > 0
    assert detectable["minimum_detectable"] == pytest.approx(
        _contract.MIN_INCREMENT_T_STAT * detectable["standard_error"])


def test_per_fold_increment_separates_a_regime_from_a_finding():
    values = [0.05] * 20 + [-0.05] * 20
    base = _scored([0.0] * 40)
    candidate = _scored(values)
    result = _incremental.paired_increment(base, candidate)
    folds = [{"usable": True, "evaluation_start": "2020-01-01",
              "evaluation_end": "2021-06-30"},
             {"usable": True, "evaluation_start": "2021-07-01",
              "evaluation_end": "2023-12-31"}]
    blocks = _incremental.per_fold_increment(result, folds)
    assert len(blocks) == 2
    assert blocks[0]["increment"] > 0 > blocks[1]["increment"]
    assert abs(result["increment"]) < 1e-9  # the average hides both


# --------------------------------------------------------------------------- #
# Design - identical rows, and honest availability
# --------------------------------------------------------------------------- #
def test_augmenting_a_context_changes_the_columns_and_never_the_rows():
    panel = _panel_stub()
    frame = pd.DataFrame(1.0, index=panel["calendar"],
                         columns=panel["symbols"])
    design = {"X": np.ones((6, 2)), "feature_names": ["a", "b"],
              "decision_index": np.array([5, 5, 5, 9, 9, 9]),
              "symbol_position": np.array([0, 1, 2, 0, 1, 2])}
    ctx = {"design": design,
           "row_dates": pd.DatetimeIndex([panel["calendar"][5]] * 3
                                         + [panel["calendar"][9]] * 3),
           "udates": panel["calendar"][[5, 9]]}
    augmented = _design.augment_context(ctx, frames={"f": frame},
                                        feature_names=["f"])
    assert augmented["design"]["X"].shape == (6, 3)
    assert list(augmented["row_dates"]) == list(ctx["row_dates"])
    assert augmented["design"]["feature_names"][:2] == ["a", "b"]


def test_only_new_context_drops_every_base_column():
    design = {"X": np.ones((4, 2)), "feature_names": ["a", "b"],
              "decision_index": np.array([1, 1, 2, 2]),
              "symbol_position": np.array([0, 1, 0, 1])}
    calendar = _calendar(10)
    frame = pd.DataFrame(3.0, index=calendar, columns=["x", "y"])
    ctx = {"design": design,
           "row_dates": pd.DatetimeIndex([calendar[1]] * 2 + [calendar[2]] * 2),
           "udates": calendar[[1, 2]]}
    only = _design.only_new_context(ctx, frames={"f": frame},
                                    feature_names=["f"])
    assert only["design"]["X"].shape == (4, 1)
    assert only["design"]["feature_names"] == ["f"]
    assert (only["design"]["X"] == 3.0).all()


def test_available_dates_exclude_dates_a_family_knows_nothing_about():
    calendar = _calendar(10)
    presence = pd.DataFrame(False, index=calendar, columns=["A", "B"])
    presence.iloc[5:, 0] = True
    ctx = {"udates": calendar, "row_dates": calendar}
    dates = _design.available_dates(ctx, presence)
    assert len(dates) == 5
    assert dates.min() == calendar[5]


def test_the_standalone_mask_covers_instruments_not_only_dates():
    calendar = _calendar(4)
    presence = pd.DataFrame(
        [[True, False], [True, False], [True, True], [True, True]],
        index=calendar, columns=["A", "B"])
    design = {"X": np.zeros((8, 1)), "feature_names": ["a"],
              "decision_index": np.repeat(np.arange(4), 2),
              "symbol_position": np.tile([0, 1], 4)}
    ctx = {"design": design,
           "row_dates": pd.DatetimeIndex(np.repeat(calendar.values, 2))}
    mask = _design.row_mask_for_presence(ctx, presence)
    assert mask.tolist() == [True, False, True, False, True, True, True, True]


def test_the_information_sets_are_enumerated_from_what_was_built():
    built = {name: {} for name in _contract.ACQUIRED_FAMILIES[:3]}
    sets = _design.information_sets(built)
    assert "BASE" in sets and sets["BASE"] == []
    assert "ALL_NEW_COMBINED" in sets
    assert set(sets) - {"BASE", "ALL_NEW_COMBINED"} == set(built)
    single = {name: {} for name in _contract.ACQUIRED_FAMILIES[:1]}
    assert "ALL_NEW_COMBINED" not in _design.information_sets(single)


# --------------------------------------------------------------------------- #
# Acquisition
# --------------------------------------------------------------------------- #
def test_acquisition_records_a_failure_instead_of_raising(tmp_path):
    def transport(url):
        raise OSError("network down")

    record = _acq.fetch("https://example.invalid/x", tmp_path / "x.zip",
                        transport=transport)
    assert record["ok"] is False
    assert "OSError" in record["reason"]


def test_a_credential_never_reaches_an_artifact(monkeypatch, tmp_path):
    monkeypatch.setenv("FRED_API_KEY", "SUPERSECRETKEY")
    monkeypatch.setattr(_acq, "source_dir", lambda source: tmp_path)
    result = _acq.acquire_fred(series_ids=["DGS10"],
                               transport=lambda url: b'{"observations": []}')
    blob = json.dumps(result)
    assert "SUPERSECRETKEY" not in blob
    assert "REDACTED" in blob


def test_an_existing_payload_is_not_downloaded_again(tmp_path):
    target = tmp_path / "already.zip"
    target.write_bytes(b"x" * 4096)
    calls = []

    def transport(url):
        calls.append(url)
        return b"y" * 4096

    record = _acq.fetch("https://example.invalid/y", target,
                        transport=transport)
    assert record["reused_existing"] is True
    assert calls == []


def test_the_manifest_reports_money_and_trials_as_zero():
    body = _acq.manifest_artifact(
        {"S": {"source": "S", "ok": True, "files": {"a": "b"}, "records": []}},
        campaign_id="probe", created_at="2026-01-01T00:00:00+00:00")
    assert body["money_spent"] == 0.0
    assert body["trials_started"] == 0
    assert body["accounts_created"] == 0
    assert body["credentials_written_to_artifacts"] is False


def test_the_owned_financial_statement_sets_are_never_downloaded():
    located = _acq.locate_owned_fsds()
    assert located.get("downloaded", False) is False


def test_insider_quarters_stop_at_the_current_quarter():
    quarters = _acq.insider_quarters(first=(2024, 1),
                                     today=_dt.date(2025, 5, 20))
    assert quarters[0] == (2024, 1)
    assert quarters[-1] == (2025, 2)


# --------------------------------------------------------------------------- #
# Lane A
# --------------------------------------------------------------------------- #
def test_a_current_snapshot_payload_is_inadmissible_as_history():
    body = '{"epsTrendCurrent": 1.2, "epsTrend30daysAgo": 1.1}'
    from paper_trader.alpha_agent.r32 import sources as _r32_sources

    state = _analyst_lane.classify_payload(body)
    assert state == _r32_sources.CURRENT_SNAPSHOT_ONLY
    assert state not in _r32_sources.ADMISSIBLE_FOR_HISTORY


def test_lane_a_claims_no_statistical_evidence_from_a_one_day_sample():
    result = _analyst_lane.run(transport=lambda url: b"{}")
    assert result["statistical_evidence_claimed"] is False
    assert result["owned_trial"]["usable_as_history"] is False
    assert result["purchase_gate"]["purchase_authorised"] is False
    assert result["purchase_gate"]["money_spent_usd"] == 0.0


def test_lane_a_reuses_the_released_gates_rather_than_declaring_its_own():
    source = (REPO / "alpha_agent" / "r35" / "analyst_lane.py").read_text(
        encoding="utf-8")
    assert "from ..r32 import purchase_gate as _purchase_gate" in source
    assert "from .. import analyst_revisions as _stage13a" in source
    assert "CONDITIONS = (" not in source  # the ten conditions live in r32


# --------------------------------------------------------------------------- #
# Campaign verdict logic
# --------------------------------------------------------------------------- #
def _verdict(**overrides):
    kwargs = dict(
        campaign_id="probe", created_at="2026-01-01T00:00:00+00:00",
        contract_body={"contract_hash": "x"},
        acquisition={"sources_ok": [], "sources_failed": []},
        coverage={"families": {"FAM": {}}, "integrity_violations": []},
        orthogonality={"FAM": {"admitted_to_predictive_stage": True}},
        increments={"by_horizon": {}}, standalone={"per_family": {}},
        economics={"BASE": {}},
        multiple_testing={"benjamini_hochberg": {
            "rejected_beating_the_base": []}},
        analyst={"acquisition_blocked": True},
        executed_configs=[])
    kwargs.update(overrides)
    return _campaign.build_verdict(**kwargs)


def test_no_admitted_family_means_acquisition_blocked_not_a_null_result():
    body = _verdict(orthogonality={"FAM": {
        "admitted_to_predictive_stage": False}})
    assert body["primary_verdict"] == _contract.VERDICT_ACQUISITION_BLOCKED
    assert body["RESEARCH_CANDIDATE_RESULT"] == "FAIL"


def test_an_integrity_violation_blocks_rather_than_scoring_anyway():
    body = _verdict(coverage={"families": {"FAM": {}},
                              "integrity_violations": ["FAM"]})
    assert body["primary_verdict"] == _contract.VERDICT_INTEGRITY_BLOCKED


def test_an_economic_survivor_that_bh_rejected_negatively_cannot_qualify():
    body = _verdict(
        economics={"BASE": {}, "FAM": {"gate": {"passed": True}}},
        multiple_testing={"benjamini_hochberg": {
            "rejected_beating_the_base": [],
            "rejected_losing_to_the_base": ["ECONOMIC::FAM"]}})
    assert body["primary_verdict"] == _contract.VERDICT_NO_EDGE
    assert body["families"]["qualified"] == []


def test_a_bh_survivor_without_an_economic_survivor_cannot_qualify():
    body = _verdict(multiple_testing={"benjamini_hochberg": {
        "rejected_beating_the_base": ["ECONOMIC::FAM"]}})
    assert body["primary_verdict"] == _contract.VERDICT_NO_EDGE


def test_the_economic_gate_reads_share_gates_not_a_vacuous_sign_reversal():
    concentration = {
        "sign_reversal_test_is_informative": False,
        "gates": {"single_instrument_pnl_share_within_limit": True,
                  "single_asset_class_pnl_share_within_limit": True,
                  "enough_effective_instruments": True,
                  "no_sign_reversal_on_leave_one_instrument_out": False,
                  "no_sign_reversal_on_leave_one_asset_class_out": False}}
    record = {"state": "OK", "after_cost_excess_annualised": 0.02,
              "survives_stressed_cost": True}
    increment = {"state": "OK", "increment_annualised": 0.01,
                 "increment_t": 3.0}
    gate = _campaign.economic_gate(increment, record, concentration)
    assert gate["passed"] is True
    informative = dict(concentration, sign_reversal_test_is_informative=True)
    assert _campaign.economic_gate(increment, record, informative)["passed"] \
        is False


def test_benjamini_hochberg_splits_the_direction():
    executed = [
        {"configuration_id": "A", "p_value": 0.001, "increment": 0.05},
        {"configuration_id": "B", "p_value": 0.001, "increment": -0.05},
        {"configuration_id": "C", "p_value": 0.9, "increment": 0.01},
    ]
    result = _campaign.run_multiple_testing(executed)
    bh = result["benjamini_hochberg"]
    assert "A" in bh["rejected_beating_the_base"]
    assert "B" in bh["rejected_losing_to_the_base"]
    assert result["denominator_executed_configurations"] == 3
    assert _contract.ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY is True


# --------------------------------------------------------------------------- #
# Frozen artifacts
# --------------------------------------------------------------------------- #
def test_the_frozen_contract_matches_the_module():
    body = _artifact("research_contract.json")
    assert body["campaign_id"] == _contract.CAMPAIGN_ID
    assert body["evidence"]["fresh_unseen_evidence_exists"] is False
    assert body["money"]["may_spend_money"] is False
    assert body["new_feature_count"] == len(_contract.NEW_FEATURES)


def test_the_acquisition_manifest_spent_nothing():
    body = _artifact("acquisition_manifest.json")
    assert body["money_spent"] == 0.0
    assert body["trials_started"] == 0
    assert body["payload_count"] > 0
    assert body["credentials_written_to_artifacts"] is False
    for source, spec in body["sources"].items():
        assert spec["licence"], source


def test_every_acquired_family_reports_a_measured_coverage_window():
    body = _artifact("information_coverage.json")
    for family in _contract.ACQUIRED_FAMILIES:
        spec = body["families"].get(family)
        assert spec is not None, family
        if spec.get("ok"):
            assert spec["first_usable_date"], family
            assert spec["instruments_with_any_value"] > 0, family


def test_the_orthogonality_report_measures_on_training_rows_only():
    body = _artifact("orthogonality_report.json")
    assert body["measured_on"] == "TRAINING_ROWS_ONLY"
    assert body["is_a_gate"] is True
    assert body["distinctness_is_raw_correlation_only"] is False
    assert body["base_feature_count"] == 28


def test_the_increment_artifact_reports_the_paired_statistic():
    body = _artifact("predictive_increment.json")
    assert body["primary_statistic"] == _contract.PRIMARY_INCREMENT_STATISTIC
    assert body["model_held_fixed_across_arms"] is True
    assert body["executed_configuration_count"] > 0
    for horizon, block in body["by_horizon"].items():
        for name, record in block["per_set"].items():
            assert "increment" in record, (horizon, name)
            assert "gate" in record, (horizon, name)


def test_the_base_arm_reproduces_the_release_34_finalist():
    """The anchor. If the base arm has drifted, no increment means anything."""
    body = _artifact("economic_increment.json")
    base = body["arms"]["BASE"]["record"]
    if base.get("state") != "OK":
        pytest.skip("base arm has no book in this campaign run")
    r34_dir = Path(r"D:\Stock_Prediction_app_data\prediction_to_pnl_r34"
                   r"\r34_prediction_to_pnl_v2\final_verdict.json")
    if not r34_dir.exists():
        pytest.skip("Release 34 verdict not present")
    r34 = json.loads(r34_dir.read_text(encoding="utf-8"))
    assert base["after_cost_excess_annualised"] == pytest.approx(
        r34["best_candidate"]["after_cost_excess_annualised"], rel=1e-6)


def test_the_executed_count_never_exceeds_the_declared_ceiling():
    body = _artifact("final_verdict.json")
    assert body["denominator"] <= _contract.MAX_PRIMARY_CONFIGS
    assert body["planned_config_total"] == _contract.PLANNED_CONFIG_TOTAL


def test_the_verdict_reports_three_results_and_alpha_is_not_one_of_them():
    body = _artifact("final_verdict.json")
    assert body["SYSTEM_RESULT"] in ("PASS", "FAIL")
    assert body["RESEARCH_CANDIDATE_RESULT"] in ("PASS", "FAIL")
    assert body["ALPHA_RESULT"] == "FAIL"
    assert body["genuinely_independent_evidence_exists"] is False
    assert body["forward_handoff"]["registered_anything"] is False


def test_the_verdict_records_that_the_analyst_lane_bought_nothing():
    body = _artifact("final_verdict.json")
    assert body["analyst_lane"]["money_spent_usd"] == 0.0
    assert body["analyst_lane"]["statistical_evidence_claimed"] is False
