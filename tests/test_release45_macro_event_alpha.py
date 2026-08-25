"""Release 45 regression - the invariants that make the refutation credible.

Release 45's claim is a negative one: R44's macro-event reversal does not
survive out of sample. A negative claim is only worth anything if the test
that produced it was genuinely the same test, run on genuinely untouched
data, with genuinely honest costs. Every test below pins one of those.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent.r45 import bars as B
from alpha_agent.r45 import burden as BU
from alpha_agent.r45 import causal as CA
from alpha_agent.r45 import closeout as CL
from alpha_agent.r45 import contract as C
from alpha_agent.r45 import eventstudy as ES
from alpha_agent.r45 import frontier as FR
from alpha_agent.r45 import implementable as IM
from alpha_agent.r45 import killer as KI
from alpha_agent.r45 import replication as RE
from alpha_agent.r45 import rv as RV
from alpha_agent.r45 import shell_policy as SP
from alpha_agent.r45 import surprise as SU

REPO = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# The frozen rule is R44's, unchanged
# --------------------------------------------------------------------------- #
def test_frozen_rule_carries_r44_parameters_verbatim():
    r = C.FROZEN_RULE
    assert r["source_release"] == "R44"
    assert r["rule"] == "REVERSAL"
    assert r["entry_delay_min"] == 5
    assert r["hold_min"] == 120
    assert r["shock_window_min"] == 1
    assert r["instrument_of_origin"] == "XAUUSD"


def test_r44_reference_card_is_the_published_one():
    ref = C.R44_ZONE_A_REFERENCE
    assert ref["n_events"] == 386
    assert ref["gross_bps_per_event"] == pytest.approx(6.978859540689271)
    assert ref["gross_t"] == pytest.approx(2.614921750533075)
    assert ref["net_bps_per_event"] == pytest.approx(4.418158203019549)
    assert ref["net_t"] == pytest.approx(1.6572127682028737)
    assert ref["hit_rate"] == pytest.approx(0.5544041450777202)


def test_no_parameter_search_before_replication_is_declared():
    assert C.NO_PARAMETER_SEARCH_BEFORE_FIRST_REPLICATION is True
    assert C.RETUNING_AFTER_A_FAILED_FROZEN_TEST_IS_NOT_A_REPLICATION is True
    assert C.REPLICATION_LANES_FIRST[0] == "L1_GOLD_HOLDOUT"


def test_contract_hash_is_stable_across_calls():
    assert C.frozen_contract()["contract_hash"] == \
        C.frozen_contract()["contract_hash"]


# --------------------------------------------------------------------------- #
# Identity - R45's code must reproduce R44's published number
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(not Path(
    r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41"
    r"\_data_dukascopy\XAUUSD").is_dir(), reason="owned minute bars absent")
def test_r45_reproduces_r44_zone_a_card_exactly():
    got = ES.identity_check()
    assert got["state"] == "IDENTICAL", got
    assert got["worst_relative_difference"] <= C.R44_REFERENCE_TOLERANCE


@pytest.mark.skipif(not Path(
    r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41"
    r"\_data_dukascopy\XAUUSD").is_dir(), reason="owned minute bars absent")
def test_the_holdout_is_disjoint_from_the_search_zone_and_not_empty():
    stamps = ES.release_stamps()
    ev = ES.event_book("XAUUSD", stamps)
    a = ES.slice_zone(ev, "A")
    b = ES.slice_zone(ev, "B")
    c = ES.slice_zone(ev, "C")
    bc = ES.slice_zone(ev, "BC")
    assert len(a) == 386
    assert len(bc) == len(b) + len(c)
    assert len(bc) > 300
    assert len(a) + len(bc) == len(ev)
    ka = set(a["stamp_utc"].astype(str))
    kbc = set(bc["stamp_utc"].astype(str))
    assert not (ka & kbc), "the holdout must share no event with zone A"


# --------------------------------------------------------------------------- #
# Instrument honesty
# --------------------------------------------------------------------------- #
def test_no_proxy_may_stand_in_for_a_futures_hypothesis():
    assert C.NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS is True
    assert C.NO_ETF_PROXY_FOR_A_FUTURES_HYPOTHESIS is True


def test_every_instrument_declares_a_class_and_etfs_are_not_futures():
    for sym, spec in C.LISTED_MINUTE_INSTRUMENTS.items():
        assert spec["class"] == "LISTED_ETF"
        assert B.instrument_class(sym) == "LISTED_ETF"
    for sym, spec in C.NATIVE_FUTURES_INSTRUMENTS.items():
        assert spec["class"] == "NATIVE_FUTURES"
        assert spec["exchange"] in ("CBOT", "CME", "COMEX", "NYMEX")
    for sym, spec in C.OWNED_MINUTE_INSTRUMENTS.items():
        assert spec["class"] in ("OTC_SPOT", "CFD")
        assert spec["class"] != "NATIVE_FUTURES"


def test_listed_lane_states_in_its_payload_that_it_is_not_futures():
    src = (REPO / "alpha_agent/r45/replication.py").read_text(encoding="utf-8")
    assert "this_is_not_a_futures_result" in src
    assert "cfd_symbols_may_not_be_called_a_futures_replication" in src


# --------------------------------------------------------------------------- #
# Cost
# --------------------------------------------------------------------------- #
def test_corwin_schultz_never_prints_free_execution():
    rng = np.random.default_rng(7)
    n = 500
    mid = 100 + np.cumsum(rng.normal(0, 0.01, n))
    high = pd.Series(mid * (1 + rng.uniform(0, 2e-4, n)))
    low = pd.Series(mid * (1 - rng.uniform(0, 2e-4, n)))
    half = B.corwin_schultz_half_bps(high, low, floor_bps=0.25)
    assert half.notna().all()
    assert (half >= 0.25 - 1e-12).all()


def test_corwin_schultz_floors_a_completely_flat_market():
    flat = pd.Series([100.0] * 50)
    half = B.corwin_schultz_half_bps(flat, flat, floor_bps=0.4)
    assert half.to_numpy().tolist() == pytest.approx([0.4] * len(flat))


def test_a_wider_market_estimates_a_wider_spread():
    rng = np.random.default_rng(11)
    n = 800
    mid = 100 + np.cumsum(rng.normal(0, 0.01, n))
    narrow_h = pd.Series(mid * (1 + rng.uniform(0, 1e-4, n)))
    narrow_l = pd.Series(mid * (1 - rng.uniform(0, 1e-4, n)))
    wide_h = pd.Series(mid * (1 + rng.uniform(0, 3e-3, n)))
    wide_l = pd.Series(mid * (1 - rng.uniform(0, 3e-3, n)))
    a = B.corwin_schultz_half_bps(narrow_h, narrow_l, floor_bps=0.01).median()
    b = B.corwin_schultz_half_bps(wide_h, wide_l, floor_bps=0.01).median()
    assert b > a


def test_cost_source_is_labelled_and_the_two_sources_are_distinct():
    assert C.COST_SOURCE_OBSERVED != C.COST_SOURCE_ESTIMATED
    assert "OBSERVED" in C.COST_SOURCE_OBSERVED
    assert "ESTIMATED" in C.COST_SOURCE_ESTIMATED
    assert C.COST_SOURCE_MUST_BE_LABELLED is True


def test_cost_is_charged_on_every_event_including_losers():
    ev = _synthetic_book(n=120, edge_bps=0.0)
    card = ES.score(ev)
    assert card["cost_bps_per_event"] > 0
    assert card["net_bps_per_event"] < card["gross_bps_per_event"]


def test_cost_multiplier_scales_only_the_cost():
    ev = _synthetic_book(n=120, edge_bps=5.0)
    one = ES.score(ev, cost_mult=1.0)
    two = ES.score(ev, cost_mult=2.0)
    assert two["cost_bps_per_event"] == pytest.approx(
        2 * one["cost_bps_per_event"])
    assert two["gross_bps_per_event"] == pytest.approx(
        one["gross_bps_per_event"])


# --------------------------------------------------------------------------- #
# Inference
# --------------------------------------------------------------------------- #
def _synthetic_book(*, n=200, edge_bps=0.0, seed=3, same_day=False):
    rng = np.random.default_rng(seed)
    shock = rng.normal(0, 20e-4, n)
    fwd = -np.sign(shock) * (edge_bps / 1e4) + rng.normal(0, 20e-4, n)
    if same_day:
        dates = pd.to_datetime(["2020-01-02"] * n)
    else:
        dates = pd.bdate_range("2015-01-01", periods=n)
    ev = pd.DataFrame({
        "event": ["CPI"] * n, "date": dates,
        "stamp_utc": pd.DatetimeIndex(dates).tz_localize("UTC"),
        "shock": shock, "forward": fwd,
        "half_in_bps": np.full(n, 1.0), "half_out_bps": np.full(n, 1.0)})
    ev.attrs.update({"symbol": "TEST", "cost_source": C.COST_SOURCE_OBSERVED,
                     "instrument_class": "OTC_SPOT"})
    return ev


def test_clustering_collapses_events_that_share_a_date():
    ev = _synthetic_book(n=200, edge_bps=8.0, same_day=True)
    card = ES.score(ev)
    assert card["n_clusters"] == 1
    assert card["net_t_cluster"] is None or card["n_clusters"] < card[
        "n_events"]


def test_clustering_does_not_inflate_a_spread_out_sample():
    ev = _synthetic_book(n=250, edge_bps=6.0)
    card = ES.score(ev)
    assert card["n_clusters"] == card["n_events"]
    assert card["net_t_cluster"] is not None


def test_cluster_t_is_finite_and_signed_correctly():
    ev = _synthetic_book(n=250, edge_bps=20.0)
    card = ES.score(ev)
    assert card["net_bps_per_event"] > 0
    assert card["net_t_cluster"] > 0


# --------------------------------------------------------------------------- #
# Replication verdicts
# --------------------------------------------------------------------------- #
def test_a_small_sample_can_never_be_called_a_replication():
    card = {"state": "MEASURED", "n_events": 16,
            "net_bps_per_event": 40.0, "net_t_cluster": 3.0}
    assert RE.verdict(card)["replication_state"] == "DATA_INSUFFICIENT"


def test_a_positive_but_weak_result_is_not_a_replication():
    card = {"state": "MEASURED", "n_events": 400,
            "net_bps_per_event": 1.0, "net_t_cluster": 1.4}
    assert RE.verdict(card)["replication_state"] == "DOES_NOT_REPLICATE"


def test_a_negative_result_is_never_a_replication():
    card = {"state": "MEASURED", "n_events": 400,
            "net_bps_per_event": -1.0, "net_t_cluster": 3.0}
    v = RE.verdict(card)
    assert v["replication_state"] == "DOES_NOT_REPLICATE"
    assert v["same_sign_as_r44"] is False


def test_a_strong_positive_result_would_be_allowed_to_replicate():
    card = {"state": "MEASURED", "n_events": 400,
            "net_bps_per_event": 5.0, "net_t_cluster": 2.6}
    assert RE.verdict(card)["replication_state"] == "REPLICATES"


# --------------------------------------------------------------------------- #
# Latency and stress touch only what they claim to
# --------------------------------------------------------------------------- #
def test_latency_delays_the_entry_and_not_the_exit():
    src = (REPO / "alpha_agent/r45/eventstudy.py").read_text(encoding="utf-8")
    assert "minutes=entry_delay + int(extra_latency)" in src
    assert "minutes=entry_delay + hold" in src


def test_stress_multipliers_and_latencies_are_declared():
    assert 2.0 in C.COST_STRESS_MULTIPLIERS
    assert 3.0 in C.COST_STRESS_MULTIPLIERS
    assert 1 in C.LATENCY_STRESS_EXTRA_MINUTES


def test_kill_battery_covers_every_declared_removal():
    src = (REPO / "alpha_agent/r45/killer.py").read_text(encoding="utf-8")
    for fn in ("def cost_stress", "def latency_stress", "def leave_one_out",
               "def remove_extremes", "def bootstrap_by_event_date",
               "def horizon_perturbation"):
        assert fn in src


def test_leave_one_out_actually_removes_the_group():
    ev = _synthetic_book(n=300, edge_bps=5.0)
    ev["date"] = pd.to_datetime(
        ["2015-01-05"] * 150 + ["2019-01-07"] * 150)
    out = KI.leave_one_out(ev, "year")
    assert out["state"] == "MEASURED"
    assert {r["left_out"] for r in out["rows"]} == {"2015", "2019"}
    for r in out["rows"]:
        assert r["n_events"] == 150


def test_bootstrap_resamples_dates_not_events():
    ev = _synthetic_book(n=240, edge_bps=4.0)
    out = KI.bootstrap_by_event_date(ev, draws=200)
    assert out["state"] == "MEASURED"
    assert out["n_clusters"] == 240
    assert out["p025_bps"] < out["mean_bps"] < out["p975_bps"]


# --------------------------------------------------------------------------- #
# Placebos
# --------------------------------------------------------------------------- #
def test_a_shifted_placebo_never_reuses_a_real_release_date():
    stamps = pd.DataFrame({
        "event": ["CPI"] * 5,
        "date": pd.to_datetime(["2020-01-02", "2020-01-09", "2020-01-16",
                                "2020-01-23", "2020-01-30"]),
        "stamp_utc": pd.DatetimeIndex(
            ["2020-01-02", "2020-01-09", "2020-01-16",
             "2020-01-23", "2020-01-30"]).tz_localize("UTC"),
        "declared_time_et": ["08:30"] * 5})
    shifted = CA._shifted_stamps(stamps, 7)
    real = set(stamps["date"].dt.date)
    assert not (set(shifted["date"].dt.date) & real)


def test_the_selection_premium_screen_restates_r44s_grid_exactly():
    assert CA.R44_SCREEN_INSTRUMENTS == ("EURUSD", "USDJPY", "XAUUSD")
    assert CA.R44_SCREEN_DELAYS == (1, 5)
    assert CA.R44_SCREEN_HOLDS == (5, 15, 30, 60, 120)
    assert CA.R44_SCREEN_RULES == ("REVERSAL", "CONTINUATION")
    n = (len(CA.R44_SCREEN_INSTRUMENTS) * len(CA.R44_SCREEN_DELAYS)
         * len(CA.R44_SCREEN_HOLDS) * len(CA.R44_SCREEN_RULES))
    assert n == 60, "R44 screened sixty cells; the diagnostic must match"


def test_the_selection_premium_is_a_diagnostic_that_cannot_qualify():
    src = (REPO / "alpha_agent/r45/causal.py").read_text(encoding="utf-8")
    assert "this_is_a_diagnostic_and_may_not_qualify_anything" in src
    assert "def selection_premium" in src


def test_placebo_seeds_are_declared_constants():
    assert isinstance(CA.PLACEBO_SEED, int)
    assert isinstance(KI.BOOTSTRAP_SEED, int)
    src = "\n".join(
        (REPO / f"alpha_agent/r45/{m}.py").read_text(encoding="utf-8")
        for m in ("causal", "killer", "ml"))
    assert "hash(" not in src, "a hashed seed is not reproducible"


def test_sign_permutation_keeps_the_price_path_and_only_moves_the_sign():
    ev = _synthetic_book(n=300, edge_bps=0.0, seed=21)
    out = CA.placebo_label_permutation(ev, draws=400)
    assert out["state"] == "MEASURED"
    assert 0.0 <= out["p_value_one_sided"] <= 1.0


# --------------------------------------------------------------------------- #
# Relative value
# --------------------------------------------------------------------------- #
def test_hedge_ratio_is_declared_to_be_fitted_on_training_events_only():
    assert C.HEDGE_RATIOS_ARE_FITTED_ON_TRAINING_EVENTS_ONLY is True
    src = (REPO / "alpha_agent/r45/rv.py").read_text(encoding="utf-8")
    assert "beta = _fit_beta(y_shock[in_fit], X_shock[in_fit])" in src


def test_every_rv_expression_states_an_economic_reason():
    for spec in RV.EXPRESSIONS + RV.NATIVE_EXPRESSIONS:
        assert spec["why"].strip()
        assert spec["target"] not in spec["hedges"]


def test_rv_cost_grows_with_the_number_of_legs():
    src = (REPO / "alpha_agent/r45/rv.py").read_text(encoding="utf-8")
    assert "legs = 1.0 + float(np.abs(beta).sum())" in src
    assert "abs(float(bi)) * h[\"half_in_bps\"]" in src


# --------------------------------------------------------------------------- #
# Surprise is PIT
# --------------------------------------------------------------------------- #
def test_surprise_uses_initial_releases_only():
    src = (REPO / "alpha_agent/r45/surprise.py").read_text(encoding="utf-8")
    assert '"output_type": 4' in src
    assert SU.SURPRISE_IS_MODEL_BASED_NOT_CONSENSUS is True


def test_surprise_forecast_window_is_a_declared_constant():
    assert SU.FORECAST_WINDOW == 12
    assert SU.MIN_HISTORY >= 24


def test_no_current_snapshot_may_stand_in_for_a_vintage():
    assert C.NO_CURRENT_SNAPSHOT_AS_HISTORICAL_VINTAGE is True


# --------------------------------------------------------------------------- #
# Burden
# --------------------------------------------------------------------------- #
def test_burden_inherits_r44_and_declares_it_may_not_reset():
    assert C.INHERITED_GLOBAL_BURDEN == 310
    assert C.INHERITED_GLOBAL_BURDEN_CONSERVATIVE == 312
    assert C.BURDEN_MAY_NEVER_BE_RESET is True


def test_a_lowered_inherited_count_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path)
    (tmp_path / BU.LEDGER_NAME).write_text(
        json.dumps({"global_inherited": 3, "candidates": {},
                    "by_family": {}, "evaluations": 0}), encoding="utf-8")
    with pytest.raises(BU.BurdenLaundering):
        BU._load()


def test_the_same_cell_is_charged_once_however_it_is_labelled(tmp_path,
                                                              monkeypatch):
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path)
    spec = {"expression": "RV01", "target": "XAUUSD", "hedges": ["EURUSD"]}
    a = BU.charge(spec, family="EVENT_RELATIVE_VALUE", lane="L8", label="one")
    b = BU.charge(dict(spec), family="EVENT_RELATIVE_VALUE", lane="L8",
                  label="a much nicer name")
    assert a["charged_new_trial"] is True
    assert b["charged_new_trial"] is False
    assert a["candidate_id"] == b["candidate_id"]
    assert BU.summary()["new_r45_effective_trials"] == 1


def test_the_frozen_replication_programme_costs_exactly_one_trial(
        tmp_path, monkeypatch):
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path)
    BU.charge_frozen_replication(["XAUUSD", "ZN=F", "ES=F", "TLT"])
    s = BU.summary()
    assert s["by_family"]["FROZEN_MACRO_REPLICATION"] == 1
    assert s["GLOBAL_SEARCH_BURDEN"] == C.INHERITED_GLOBAL_BURDEN + 1


def test_only_lanes_that_charge_nothing_may_be_served_from_cache():
    src = (REPO / "alpha_agent/r45/campaign.py").read_text(encoding="utf-8")
    # rv / surprise / ml must be unconditional live calls with a charge hook
    assert "rv = RV.run(stamps, charge=BU.charge)" in src
    assert "sur = SU.run(stamps=stamps, charge=BU.charge)" in src
    assert "mlr = ML.run(stamps=stamps, charge=BU.charge)" in src
    for lane in ('pre.get("rv")', 'pre.get("surprise")', 'pre.get("ml")'):
        assert lane not in src, (
            f"{lane} would skip its burden callback and launder the trials")


def test_a_cached_causal_lane_still_charges_its_diagnostic():
    src = (REPO / "alpha_agent/r45/campaign.py").read_text(encoding="utf-8")
    assert 'if pre.get("causal") and causal.get("selection_premium")' in src
    assert 'family="EVENT_FAMILY", lane="L5_CAUSAL"' in src


def test_burden_only_ever_grows(tmp_path, monkeypatch):
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path)
    before = BU.summary()["GLOBAL_SEARCH_BURDEN"]
    BU.charge({"a": 1}, family="EVENT_ML", lane="L11")
    BU.charge({"a": 2}, family="EVENT_ML", lane="L11")
    assert BU.summary()["GLOBAL_SEARCH_BURDEN"] == before + 2


# --------------------------------------------------------------------------- #
# Qualification and freezing
# --------------------------------------------------------------------------- #
def test_qualification_is_stricter_than_a_single_t_above_two():
    assert C.A_SINGLE_T_ABOVE_2_IS_NOT_A_QUALIFICATION is True
    assert C.QUALIFICATION["net_t_cluster_ge"] >= 2.5
    assert C.QUALIFICATION["min_events"] >= 100
    assert C.QUALIFICATION["must_survive_cost_x2"] is True
    assert C.QUALIFICATION["must_survive_leave_one_year_out"] is True


def test_a_negative_card_can_never_be_qualified():
    card = {"state": "MEASURED", "n_events": 500,
            "net_bps_per_event": -2.0, "net_t_cluster": -3.0}
    assert FR.qualification_state(card) in ("REFUTED", "NOT_A_CANDIDATE")


def test_qualification_needs_the_cost_stress_to_pass():
    card = {"state": "MEASURED", "n_events": 500,
            "net_bps_per_event": 5.0, "net_t_cluster": 3.0}
    assert FR.qualification_state(card, {"survives_x2": False}) == \
        "RESEARCH_CANDIDATE"
    assert FR.qualification_state(card, {"survives_x2": True}) == \
        "QUALIFIED_ALPHA"


def test_a_ranked_row_carries_what_qualification_needs_to_read():
    src = (REPO / "alpha_agent/r45/replication.py").read_text(
        encoding="utf-8")
    assert '"state", "symbol"' in src, (
        "qualification_state reads card['state']; if the ranked projection "
        "drops it every row silently becomes DATA_INSUFFICIENT")


def test_a_measured_positive_row_is_not_reported_as_data_insufficient():
    front = FR.build({"ranked": [
        {"state": "MEASURED", "symbol": "SPY", "n_events": 165,
         "gross_bps_per_event": 1.62, "cost_bps_per_event": 1.37,
         "net_bps_per_event": 0.246, "net_t_cluster": 0.071,
         "hit_rate": 0.521, "year_range": [2024, 2026],
         "instrument_class": "LISTED_ETF",
         "replication_state": "DOES_NOT_REPLICATE"}]})
    assert front["best"]["QUALIFICATION_STATE"] == "WEAK_EVIDENCE"


def test_a_tiny_sample_can_never_be_the_best_candidate():
    front = FR.build({"ranked": [
        {"symbol": "CL=F", "state": "MEASURED", "n_events": 16,
         "gross_bps_per_event": 46.2, "cost_bps_per_event": 5.8,
         "net_bps_per_event": 40.4, "net_t_cluster": 1.70,
         "hit_rate": 0.56, "year_range": [2026, 2026],
         "instrument_class": "NATIVE_FUTURES",
         "replication_state": "DATA_INSUFFICIENT"},
        {"symbol": "XAUUSD", "state": "MEASURED", "n_events": 370,
         "gross_bps_per_event": 0.5, "cost_bps_per_event": 2.3,
         "net_bps_per_event": -1.82, "net_t_cluster": -0.63,
         "hit_rate": 0.478, "year_range": [2018, 2026],
         "instrument_class": "OTC_SPOT",
         "replication_state": "DOES_NOT_REPLICATE"}]})
    assert front["best"]["INSTRUMENTS"] == "XAUUSD"
    assert front["n_judgeable"] == 1
    tiny = [r for r in front["rows"] if r["INSTRUMENTS"] == "CL=F"][0]
    assert tiny["JUDGEABLE"] is False
    assert tiny["QUALIFICATION_STATE"] == "DATA_INSUFFICIENT"
    assert tiny["RANK"] > front["best"]["RANK"]


def test_a_frontier_with_nothing_judgeable_has_no_best():
    front = FR.build({"ranked": [
        {"symbol": "ZN=F", "state": "MEASURED", "n_events": 16,
         "gross_bps_per_event": 3.2, "cost_bps_per_event": 1.5,
         "net_bps_per_event": 1.7, "net_t_cluster": 0.47,
         "hit_rate": 0.44, "year_range": [2026, 2026],
         "instrument_class": "NATIVE_FUTURES",
         "replication_state": "DATA_INSUFFICIENT"}]})
    assert front["best"] is None
    assert front["why_no_best"]


def test_no_shadow_is_frozen_without_a_candidate():
    out = FR.freeze_gate({"rows": [
        {"CANDIDATE_ID": "x", "INSTRUMENTS": "X",
         "QUALIFICATION_STATE": "NOT_A_CANDIDATE"},
        {"CANDIDATE_ID": "y", "INSTRUMENTS": "Y",
         "QUALIFICATION_STATE": "REFUTED"}]})
    assert out["FORWARD_SHADOWS_ADDED"] == 0
    assert out["why_none"]


def test_shadows_are_capped_and_never_promotable():
    rows = [{"CANDIDATE_ID": f"c{i}", "INSTRUMENTS": "X",
             "QUALIFICATION_STATE": "RESEARCH_CANDIDATE"} for i in range(9)]
    out = FR.freeze_gate({"rows": rows})
    assert out["FORWARD_SHADOWS_ADDED"] == C.MAX_NEW_SHADOWS
    assert all(s["promotion_allowed"] is False for s in out["shadows"])
    assert all(s["research_shadow_only"] for s in out["shadows"])


def test_prior_shadows_and_prospective_rows_are_protected():
    assert C.PRIOR_SHADOWS_ARE_IMMUTABLE is True
    assert C.NEVER_BACKFILL_PROSPECTIVE_ROWS is True
    assert C.DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_A_SHADOW is True


# --------------------------------------------------------------------------- #
# The economic judge is R43's
# --------------------------------------------------------------------------- #
def test_the_capital_equation_is_imported_not_re_derived():
    assert IM.CAPITAL_EQUATION_OWNER == "alpha_agent.r43.judge"
    src = (REPO / "alpha_agent/r45/implementable.py").read_text(
        encoding="utf-8")
    assert "from ..r43 import judge as J43" in src
    assert "J43.futures_committed_capital" in src


def test_no_release_local_second_owner_module_exists():
    for name in ("judge", "capital", "evidence", "economics", "zones",
                 "panels", "multiple_testing", "deflated_sharpe"):
        assert not (REPO / f"alpha_agent/r45/{name}.py").exists(), name


def test_futures_margin_is_treated_as_remunerated():
    assert IM.COLLATERAL_CLASS == "REMUNERATED_MARGIN"
    src = (REPO / "alpha_agent/r45/implementable.py").read_text(
        encoding="utf-8")
    assert "no further cash rent is charged" in src


def test_an_annual_result_uses_the_committed_margin_denominator():
    card = {"state": "MEASURED", "symbol": "ZN=F", "n_events": 100,
            "gross_bps_per_event": 5.0, "cost_bps_per_event": 1.0,
            "net_bps_per_event": 4.0, "year_range": [2015, 2019]}
    out = IM.score(card, symbol="ZN=F")
    assert out["state"] == "MEASURED"
    assert out["committed_capital_per_leg_unit"] > 0
    assert out["net_annual_excess_return_on_committed_margin"] > \
        out["net_annual_return_on_traded_notional"]
    assert out["primary_capital_model"] == "COMMITTED_MARGIN_X2"


# --------------------------------------------------------------------------- #
# Safety and shell policy
# --------------------------------------------------------------------------- #
def test_release_is_research_only_with_zero_authorized_spend():
    assert C.RESEARCH_ONLY is True
    assert C.PROMOTION_ALLOWED is False
    assert C.AUTHORIZED_SPEND_USD == 0.0


def test_no_r45_module_imports_the_operational_system():
    for p in sorted((REPO / "alpha_agent/r45").glob("*.py")):
        src = p.read_text(encoding="utf-8")
        for tok in ("from api.", "import api.", "from engine.",
                    "import engine.", "operational_book",
                    "portfolio_decision"):
            assert tok not in src, f"{p.name} references {tok}"


def test_shell_policy_is_powershell_only_with_no_waiver():
    assert C.SHELL_POLICY == "WINDOWS_POWERSHELL_ONLY"
    assert C.SHELL_POLICY_WAIVERS_ARE_NOT_AVAILABLE is True
    rec = SP.record()
    assert rec["waivers_available"] is False
    assert rec["SHELL_POLICY_VIOLATION"] in ("YES", "NO")


def test_prior_shell_disclosures_are_inherited_not_erased():
    rel = {d["release"] for d in C.INHERITED_SHELL_DISCLOSURES}
    assert {"R42", "R44"} <= rel
    assert SP.record()["inherited_disclosures_are_never_erased"] is True


def test_no_module_registers_a_scheduled_task():
    for p in sorted((REPO / "alpha_agent/r45").glob("*.py")):
        src = p.read_text(encoding="utf-8")
        for tok in ("schtasks", "Register-ScheduledTask", "crontab"):
            assert tok not in src, f"{p.name} references {tok}"


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
def test_every_declared_terminal_state_is_reachable_from_the_rule():
    assert set(C.TERMINAL_STATES) >= {
        "R45_QUALIFIED_EVENT_ALPHA_FOUND",
        "R45_R44_MACRO_EFFECT_REFUTED_IN_NATIVE_MARKETS",
        "R45_NATIVE_FUTURES_DATA_WALL_BINDING"}


def test_terminal_state_prefers_a_qualified_result():
    assert CL.terminal_state({}, {}, {"n_qualified": 1}) == \
        "R45_QUALIFIED_EVENT_ALPHA_FOUND"


def test_terminal_state_reports_refutation_when_judged_and_negative():
    rep = {"ranked": [{"n_events": 400}],
           "L1_GOLD_HOLDOUT": {"replication_state": "DOES_NOT_REPLICATE"},
           "L3_NATIVE_FUTURES": {"replication_state": "DATA_INSUFFICIENT"}}
    assert CL.terminal_state(rep, {"n_holdout_survivors": 0},
                             {"n_qualified": 0,
                              "n_research_candidates": 0}) == \
        "R45_R44_MACRO_EFFECT_REFUTED_IN_NATIVE_MARKETS"


def test_terminal_state_reports_a_data_wall_when_nothing_is_judgeable():
    rep = {"ranked": [{"n_events": 16}],
           "L1_GOLD_HOLDOUT": {"replication_state": "DATA_INSUFFICIENT"},
           "L3_NATIVE_FUTURES": {"replication_state": "DATA_INSUFFICIENT"}}
    assert CL.terminal_state(rep, {}, {}) == \
        "R45_NATIVE_FUTURES_DATA_WALL_BINDING"


def test_the_verdict_declares_every_required_key():
    body = CL.build({"lanes": {}, "frontier": {}, "shadows": {},
                     "purchase": {}, "burden": {}, "shell_policy": {}})
    missing = [k for k in C.REQUIRED_VERDICT_KEYS if k not in body]
    assert not missing, missing


def test_a_lane_may_not_halt_another_lane():
    assert C.ONE_LANE_MAY_NOT_HALT_ANOTHER is True
    assert C.A_FAILED_LANE_IS_A_ROUTING_EVENT is True
    assert C.NO_BROAD_EXECUTABLE_ZERO_COST_BRANCH_MAY_BE_DEFERRED is True


def test_r45_has_an_operational_write_attribution_profile():
    src = (REPO / "scripts/r33_operational_write_attribution.py").read_text(
        encoding="utf-8")
    assert "R45_MARKERS" in src
    assert '"R45": {"markers": R45_MARKERS' in src
    assert "r45_macro_event_alpha_v1" in src
    assert "macro_event_alpha_r45" in src


def test_the_audit_declares_r45_invariants():
    src = (REPO / "scripts/audit_architecture.py").read_text(encoding="utf-8")
    assert "def check_release45_macro_event_alpha" in src
    assert '"release45_macro_event_alpha":' in src
    assert "R45_SECOND_OWNER_FORBIDDEN" in src


def test_every_blocker_word_is_in_the_declared_vocabulary():
    for tok in ("EXECUTED", "PAYMENT_REQUIRED", "ACCOUNT_REQUIRED",
                "HISTORICAL_DATA_UNAVAILABLE", "FUTURE_TIME_REQUIRED"):
        assert tok in C.BLOCKER_VOCAB
