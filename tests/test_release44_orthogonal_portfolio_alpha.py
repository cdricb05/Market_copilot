"""Release 44 regression - orthogonal information x portfolio alpha.

The tests that matter most here are not the ones that check a number. They
are the ones that check the release cannot cheat:

* the primary combination rule was named before the lockbox
  (:func:`test_primary_rule_is_declared_in_the_frozen_contract`);
* losers are in the stream inventory
  (:func:`test_inventory_includes_streams_r43_killed`);
* flipping a stream's sign does not turn its transaction costs into a credit
  (:func:`test_sign_flip_charges_cost_it_does_not_credit_it`) - this is the
  bug that produced a fake lockbox Sharpe of 1.40 during development;
* two combination rules that produce the same book are ONE trial
  (:func:`test_identical_books_are_one_burden_trial`);
* the inherited search burden of 302 is re-derived from R43's bytes and
  never typed (:func:`test_burden_is_derived_from_r43_bytes`);
* every prior release's shadow registry is byte-identical
  (:func:`test_prior_shadow_registries_are_untouched`).
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent.r44 import (CAMPAIGN_ID, campaign_dir, read_json, sha,
                             sha_file)
from alpha_agent.r44 import burden as B
from alpha_agent.r44 import closeout as CO
from alpha_agent.r44 import combine as CB
from alpha_agent.r44 import contract as C
from alpha_agent.r44 import control as CTL
from alpha_agent.r44 import frontier as FR
from alpha_agent.r44 import intraday as ID
from alpha_agent.r44 import niche as NI
from alpha_agent.r44 import options as OP
from alpha_agent.r44 import portfolio as PF
from alpha_agent.r44 import purchase as PU
from alpha_agent.r44 import streams as ST
from alpha_agent.r43 import judge as J

pytestmark = pytest.mark.usefixtures()

CAMP = campaign_dir(CAMPAIGN_ID)


def _verdict():
    v = read_json(CAMP / "R44_FINAL_VERDICT.json")
    if v is None:
        pytest.skip("R44 campaign has not been run in this environment")
    return v


def _lanes():
    v = read_json(CAMP / "R44_LANE_RESULTS.json")
    if v is None:
        pytest.skip("R44 campaign has not been run in this environment")
    return v


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
def test_contract_is_research_only_and_spends_nothing():
    assert C.RESEARCH_ONLY is True
    assert C.DEFAULT_AUTHORIZED_SPEND_USD == 0.0
    for flag in ("MAY_SPEND_MONEY", "MAY_PURCHASE_DATA",
                 "MAY_START_PROVIDER_TRIAL", "MAY_CREATE_PROVIDER_ACCOUNT",
                 "MAY_SUBMIT_PAYMENT_DETAILS", "MAY_ACCEPT_LICENCE_AGREEMENT",
                 "MAY_SEND_VENDOR_EMAIL", "MAY_CREATE_ORDER",
                 "MAY_CREATE_PAPER_ORDER", "MAY_CONNECT_BROKER",
                 "MAY_CHANGE_HOLDINGS", "MAY_CREATE_CAPITAL_ALLOCATION",
                 "MAY_ACTIVATE_SLEEVE", "MAY_PROMOTE_MODEL",
                 "MAY_RESTART_PRODUCTION", "MAY_MODIFY_PRODUCTION_SCHEDULER",
                 "MAY_MUTATE_OPERATIONAL_STORE",
                 "MAY_MUTATE_PRIOR_RELEASE_ARTIFACT"):
        assert getattr(C, flag) is False, flag


def test_shell_policy_is_windows_powershell_only():
    assert C.WINDOWS_POWERSHELL_ONLY is True
    assert "PowerShell" in C.SHELL_POLICY
    assert C.INHERITED_SHELL_POLICY_DISCLOSURES == 1


def test_shell_policy_events_are_disclosed_not_hidden():
    """A policy that is only reported when it was kept is not a policy."""
    from alpha_agent.r44 import shell_policy as SP
    blk = SP.block()
    assert blk["windows_powershell_only"] is True
    assert len(blk["events"]) == (len(SP.INHERITED_EVENTS)
                                  + len(SP.EVENTS))
    assert len(blk["waiver_token"]) == 16
    for e in blk["events"]:
        assert e["wrote_anything"] is False
        assert e["affected_a_result"] is False
        assert e["state"] == "DISCLOSED_WAIVER_IS_THE_OPERATORS"
    assert blk["r44_violation"] == bool(SP.EVENTS)
    assert blk["any_event_wrote_anything"] is False
    assert blk["any_event_affected_a_result"] is False


def test_verdict_carries_the_shell_policy_record():
    v = _verdict()
    sp = v["shell_policy"]
    assert v["headline"]["SHELL_POLICY_VIOLATION"] == sp["r44_violation"]
    assert v["headline"]["SHELL_POLICY_WAIVER_TOKEN"] == sp["waiver_token"]
    assert sp["any_event_affected_a_result"] is False


def test_primary_rule_is_declared_in_the_frozen_contract():
    assert C.PRIMARY_COMBINATION_RULE in C.COMBINATION_RULES
    assert C.PRIMARY_COMBINATION_RULE == "FAMILY_BALANCED_ERC"
    assert C.WEIGHTS_ARE_FITTED_ON_FIT_ZONES_ONLY is True
    assert C.NO_OPTIMISATION_ON_THE_HOLDOUT is True


def test_dangerous_optimisers_are_forbidden():
    assert C.UNCONSTRAINED_MEAN_VARIANCE_IS_FORBIDDEN is True
    assert C.MAXIMISING_HISTORICAL_SHARPE_IS_FORBIDDEN is True
    assert C.SHORTING_A_STREAM_IS_FORBIDDEN is True
    assert C.SELECTION_ON_MEASURED_PERFORMANCE_IS_FORBIDDEN is True


def test_no_threshold_is_chosen_anywhere():
    assert C.NO_THRESHOLD_IS_CHOSEN is True
    assert C.STREAM_EXPRESSION == "CONTINUOUS"


def test_frozen_body_is_hashable_and_stable():
    a, b = sha(C.frozen_body()), sha(C.frozen_body())
    assert a == b and len(a) == 64


def test_amendment_rule_is_declared_and_bounded():
    assert C.AMENDMENTS_MAY_ONLY_MAKE_AN_UNBUILT_STREAM_BUILDABLE is True
    assert C.AMENDMENTS_AFTER_THE_LOCKBOX_ARE_FORBIDDEN is True
    for a in C.POST_FREEZE_AMENDMENTS:
        assert a["affects_a_measured_result"] is False
        assert "state_before" in a and "admissible_because" in a


def test_sign_selected_run_is_declared_non_qualifying():
    d = C.SIGN_SELECTED_DIAGNOSTIC
    assert d["may_qualify"] is False
    assert d["may_be_frozen_as_a_shadow"] is False
    assert d["sign_chosen_on"] == "FIT_ZONES_ONLY"


# --------------------------------------------------------------------------- #
# Stream inventory
# --------------------------------------------------------------------------- #
def test_every_stream_has_a_role_family_and_expression():
    for s in C.STREAMS:
        assert s["role"] in ("RESIDUAL", "PREMIUM")
        assert s["family"] in C.BURDEN_FAMILIES
        assert s["expression"] in ST.COLLATERAL_BY_EXPRESSION
        assert s["why"] and s["owner"]


def test_stream_ids_are_unique_and_split_into_two_roles():
    ids = [s["id"] for s in C.STREAMS]
    assert len(ids) == len(set(ids))
    assert set(C.RESIDUAL_STREAM_IDS) & set(C.PREMIUM_STREAM_IDS) == set()
    assert set(C.RESIDUAL_STREAM_IDS) | set(C.PREMIUM_STREAM_IDS) == set(ids)


def test_no_two_streams_share_family_and_expression_and_markets():
    seen = {}
    for s in C.STREAMS:
        key = (s["family"], s["expression"], s["build"][0],
               json.dumps(s["build"][1], sort_keys=True))
        assert key not in seen, "duplicate lineage: %s vs %s" % (
            s["id"], seen.get(key))
        seen[key] = s["id"]


def test_inventory_includes_streams_r43_killed():
    """Losers are in, by contract. Excluding them IS the selection bias."""
    assert C.LOSERS_ARE_INCLUDED is True
    inv = ST.build_all()
    built = {k: v for k, v in inv.items() if v.get("state") == "BUILT"}
    if not built:
        pytest.skip("stream inventory not built in this environment")
    frame = ST.excess_frame(inv)
    z = ST.zones(frame)
    fit = pd.DatetimeIndex(z["A"]).union(pd.DatetimeIndex(z["B"]))
    negatives = 0
    for sid in built:
        if sid not in frame.columns:
            continue
        s = frame[sid].reindex(fit).dropna()
        if len(s) > 250 and float(np.nanmean(s)) < 0:
            negatives += 1
    assert negatives > 0, ("an inventory with no losing stream has been "
                           "filtered, which is exactly what the contract "
                           "forbids")


def test_streams_share_one_tz_naive_calendar():
    inv = ST.build_all()
    built = [v for v in inv.values() if v.get("state") == "BUILT"]
    if not built:
        pytest.skip("stream inventory not built")
    for rec in built:
        idx = pd.DatetimeIndex(rec["index"])
        assert idx.tz is None, rec["id"]
        assert idx.is_unique, rec["id"]
        assert idx.is_monotonic_increasing, rec["id"]


def test_collateral_class_is_never_chosen_by_the_builder():
    for s in C.STREAMS:
        assert ST.COLLATERAL_BY_EXPRESSION[s["expression"]] in (
            "REMUNERATED_MARGIN", "FUNDED_LONG_SHORT_EQUITY",
            "UNREMUNERATED_FULLY_FUNDED")
    assert ST.COLLATERAL_BY_EXPRESSION["CRYPTO_CASH_AND_CARRY"] == \
        "UNREMUNERATED_FULLY_FUNDED"
    assert ST.COLLATERAL_BY_EXPRESSION["FUTURES_CROSS_MARKET_RV"] == \
        "REMUNERATED_MARGIN"


def test_conditional_streams_cannot_be_added_off_contract():
    with pytest.raises(ValueError):
        ST.add_conditional("X99_NOT_DECLARED", {"state": "BUILT"})


# --------------------------------------------------------------------------- #
# THE bug: a short position pays the spread too
# --------------------------------------------------------------------------- #
def _toy_stream():
    idx = pd.date_range("2000-01-03", periods=600, freq="B")
    gross = pd.Series(np.linspace(0.001, 0.001, len(idx)), index=idx)
    cost = pd.Series(0.002, index=idx)
    return {"id": "TOY", "state": "BUILT", "gross": gross, "cost": cost,
            "turnover": pd.Series(1.0, index=idx), "index": idx,
            "committed_capital": 1.0,
            "collateral_class": "REMUNERATED_MARGIN",
            "family": "RATES_RV", "asset_class": "RATES", "role": "RESIDUAL"}


def test_sign_flip_charges_cost_it_does_not_credit_it():
    """Flipping a stream must flip GROSS and re-charge COST.

    Multiplying the excess series by -1 turns (gross - cost) into
    (-gross + cost), which pays the researcher the transaction costs. During
    development that error produced a lockbox Sharpe of 1.40 out of nothing.
    """
    rec = _toy_stream()
    inv = {"TOY": rec}
    plus = ST.excess_frame_signed(inv, {"TOY": 1.0})["TOY"]
    minus = ST.excess_frame_signed(inv, {"TOY": -1.0})["TOY"]
    naive = -plus
    # gross +0.001, cost 0.002 -> long nets -0.001, short nets -0.003.
    assert float(plus.mean()) == pytest.approx(-0.001, abs=1e-9)
    assert float(minus.mean()) == pytest.approx(-0.003, abs=1e-9)
    assert float(naive.mean()) == pytest.approx(+0.001, abs=1e-9)
    assert float(minus.mean()) < float(naive.mean()), (
        "the flipped book must be WORSE than the naive negation, because "
        "the naive negation credits the cost")


def test_both_signs_pay_the_same_cost():
    rec = _toy_stream()
    inv = {"TOY": rec}
    a = ST.excess_frame_signed(inv, {"TOY": 1.0})["TOY"]
    b = ST.excess_frame_signed(inv, {"TOY": -1.0})["TOY"]
    # long - short = 2 x gross exactly; the cost cancels because both paid it
    assert float((a - b).mean()) == pytest.approx(2 * 0.001, abs=1e-9)


# --------------------------------------------------------------------------- #
# Combination rules
# --------------------------------------------------------------------------- #
def _toy_panel(n_streams=6, n_days=1500, seed=11):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2000-01-03", periods=n_days, freq="B")
    cols = {}
    meta = {}
    for i in range(n_streams):
        vol = 0.002 * (1 + i)
        cols["S%d" % i] = pd.Series(rng.normal(0.0, vol, n_days), index=idx)
        meta["S%d" % i] = {"family": "F%d" % (i % 3),
                           "asset_class": "A%d" % (i % 2), "role": "RESIDUAL"}
    return pd.DataFrame(cols), meta, idx


@pytest.mark.parametrize("rule", list(C.COMBINATION_RULES))
def test_every_rule_returns_constrained_weights(rule):
    frame, meta, idx = _toy_panel()
    fit = CB.fit_weights(frame, idx, meta, rule)
    assert fit["state"] == "FITTED"
    w = fit["weights"]
    assert set(w) == set(frame.columns)
    assert all(v >= -1e-12 for v in w.values()), "long-only violated"
    assert sum(w.values()) == pytest.approx(1.0, abs=1e-8)
    assert max(w.values()) <= C.PORTFOLIO_CONSTRAINTS[
        "max_single_stream_weight"] + 1e-8


def test_constraints_cap_family_and_asset_class():
    frame, meta, idx = _toy_panel(n_streams=8)
    fit = CB.fit_weights(frame, idx, meta, C.PRIMARY_COMBINATION_RULE)
    w = fit["weights"]
    fam, ac = {}, {}
    for k, v in w.items():
        fam[meta[k]["family"]] = fam.get(meta[k]["family"], 0.0) + v
        ac[meta[k]["asset_class"]] = ac.get(meta[k]["asset_class"], 0.0) + v
    assert max(fam.values()) <= C.PORTFOLIO_CONSTRAINTS[
        "max_family_weight"] + 1e-6
    assert max(ac.values()) <= C.PORTFOLIO_CONSTRAINTS[
        "max_asset_class_weight"] + 1e-6


def test_erc_equalises_risk_contribution_before_caps():
    frame, _, idx = _toy_panel(n_streams=4)
    cov = CB.shrunk_covariance(frame.reindex(idx))
    w = CB._erc_weights(cov.to_numpy(dtype=float))
    rc = w * (cov.to_numpy(dtype=float) @ w)
    rc = rc / rc.sum()
    assert float(np.max(rc) - np.min(rc)) < 1e-6


def test_family_balanced_erc_is_invariant_to_family_size():
    """A family with four streams must not get four times the risk."""
    frame, meta, idx = _toy_panel(n_streams=6)
    for k in list(meta):
        meta[k]["family"] = "BIG" if k != "S0" else "SMALL"
    fit = CB.fit_weights(frame, idx, meta, "FAMILY_BALANCED_ERC")
    w = fit["weights"]
    small = w["S0"]
    assert small > 1.0 / len(w), (
        "the one-stream family should carry more weight per stream than "
        "each member of the five-stream family")


def test_weights_are_fitted_on_fit_dates_only():
    frame, meta, idx = _toy_panel(n_days=2000)
    fit_dates = idx[:1200]
    a = CB.fit_weights(frame, fit_dates, meta, C.PRIMARY_COMBINATION_RULE)
    poisoned = frame.copy()
    poisoned.iloc[1400:] += 10.0          # the holdout becomes absurd
    b = CB.fit_weights(poisoned, fit_dates, meta, C.PRIMARY_COMBINATION_RULE)
    assert a["weights"] == pytest.approx(b["weights"]), (
        "weights changed when only post-fit data changed - the holdout is "
        "leaking into the fit")


def test_portfolio_returns_renormalise_over_live_streams():
    idx = pd.date_range("2000-01-03", periods=300, freq="B")
    frame = pd.DataFrame({"A": pd.Series(0.01, index=idx),
                          "B": pd.Series(0.01, index=idx)})
    frame.loc[frame.index[:150], "B"] = np.nan
    r = CB.portfolio_returns(frame, {"A": 0.5, "B": 0.5},
                             overlay_cost=False)
    early = float(r.iloc[:150].mean())
    late = float(r.iloc[151:].mean())
    assert early == pytest.approx(0.01, abs=1e-9), (
        "a stream that does not exist yet must not dilute the book to 0.005")
    assert late == pytest.approx(0.01, abs=1e-9)


def test_overlay_cost_only_reduces_return():
    frame, meta, idx = _toy_panel()
    w = {c: 1.0 / frame.shape[1] for c in frame.columns}
    with_cost = CB.portfolio_returns(frame, w, overlay_cost=True)
    without = CB.portfolio_returns(frame, w, overlay_cost=False)
    assert float(with_cost.sum()) <= float(without.sum()) + 1e-12


def test_shrunk_covariance_is_symmetric_psd():
    frame, _, idx = _toy_panel()
    cov = CB.shrunk_covariance(frame.reindex(idx)).to_numpy(dtype=float)
    assert np.allclose(cov, cov.T, atol=1e-12)
    assert float(np.min(np.linalg.eigvalsh(cov))) > -1e-12
    assert 0.0 <= float(CB.shrunk_covariance(
        frame.reindex(idx)).attrs["shrinkage"]) <= 1.0


# --------------------------------------------------------------------------- #
# Controls
# --------------------------------------------------------------------------- #
def test_volatility_matched_increment_is_scale_free():
    idx = pd.date_range("2000-01-03", periods=1000, freq="B")
    rng = np.random.default_rng(3)
    cand = pd.Series(rng.normal(0.0002, 0.004, len(idx)), index=idx)
    ctl = pd.Series(rng.normal(0.0001, 0.002, len(idx)), index=idx)
    a = CTL.volatility_matched_increment(cand, ctl, idx)
    b = CTL.volatility_matched_increment(cand, ctl * 7.0, idx)
    assert a["increment_ann"] == pytest.approx(b["increment_ann"], rel=1e-9)
    assert a["increment_t_hac"] == pytest.approx(b["increment_t_hac"],
                                                 rel=1e-9)


def test_premium_control_uses_only_premium_streams():
    inv = ST.build_all()
    frame = ST.excess_frame(inv)
    if frame.empty:
        pytest.skip("streams not built")
    z = ST.zones(frame)
    fit = pd.DatetimeIndex(z["A"]).union(pd.DatetimeIndex(z["B"]))
    meta = PF._weight_meta(inv)
    prem = CTL.premium_portfolio(frame, fit, meta)
    if prem.get("state") != "FITTED":
        pytest.skip("premium control not fitted in this environment")
    assert set(prem["weights"]) <= set(C.PREMIUM_STREAM_IDS)


def test_declared_controls_cover_every_role():
    assert "STRUCTURAL_PREMIUM_PORTFOLIO" in C.CONTROLS
    assert "VOLATILITY_MATCHED_PASSIVE" in C.CONTROLS
    assert C.PRIMARY_PORTFOLIO_CONTROL == "STRUCTURAL_PREMIUM_PORTFOLIO"
    assert C.A_SMOOTHER_PACKAGE_OF_PREMIA_IS_NOT_ALPHA is True


# --------------------------------------------------------------------------- #
# Burden
# --------------------------------------------------------------------------- #
def test_burden_is_derived_from_r43_bytes():
    got = B.verify_inherited()
    assert got["verified"] is True
    assert got["inherited_global_cumulative"] == 302
    assert got["pre_r43_effective_trials"] == 289
    assert got["r43_distinct_zone_b_candidates"] == 13
    assert got["read_only"] is True


def test_burden_refuses_a_contract_that_disagrees_with_the_ledger(
        monkeypatch):
    monkeypatch.setattr(C, "GLOBAL_INHERITED_EFFECTIVE_TRIALS", 1)
    with pytest.raises(B.BurdenLaundering):
        B.verify_inherited()


def test_burden_never_resets_and_is_cumulative():
    s = B.summary()
    assert s["never_reset"] is True
    assert s["r43_ledger_mutated"] is False
    assert s["global_inherited"] == 302
    assert s["global_cumulative"] >= 302
    assert s["global_cumulative"] == 302 + s["r44_distinct_zone_b_candidates"]


def test_portfolio_synthesis_is_a_charged_family():
    assert C.PORTFOLIO_SYNTHESIS_IS_A_SEARCHED_FAMILY is True
    assert "PORTFOLIO_SYNTHESIS" in C.BURDEN_FAMILIES
    assert B.summary()["portfolio_synthesis_is_charged"] is True


def test_identical_books_are_one_burden_trial():
    lanes = _lanes()
    ch = lanes["engine2"]["burden"]
    assert ch["n_distinct_books"] <= ch["n_rules_evaluated"]
    for cid, rules in ch["rules_per_distinct_book"].items():
        assert len(rules) >= 1
    v = _verdict()
    sb = v["search_burden"]
    assert sb["conservative_global_cumulative"] >= sb["global_cumulative"], (
        "the conservative count must never be smaller than the headline")
    assert sb["conservative_note"]
    # every rule that shares a weights hash must share a candidate id
    by_hash = {}
    for row in ch["charged"]:
        by_hash.setdefault(row["weights_hash"], set()).add(
            row["candidate_id"])
    for h, ids in by_hash.items():
        assert len(ids) == 1, "same book, two candidate ids: %s" % h


def test_lane_caps_are_ceilings():
    s = B.summary()
    for lane, used in s["lane_counts"].items():
        assert used <= s["lane_caps"][lane], lane
    assert C.LANE_CAP_IS_A_CEILING_NOT_A_TARGET is True


def test_unknown_family_or_lane_is_refused():
    spec = {"information_family": "X"}
    with pytest.raises(ValueError):
        B.record_zone_b(spec, family="NOT_A_FAMILY", lane="E2_PORTFOLIO_"
                                                          "SYNTHESIS")
    with pytest.raises(ValueError):
        B.record_zone_b(spec, family="PORTFOLIO_SYNTHESIS", lane="NOPE")


# --------------------------------------------------------------------------- #
# The economic judge, inherited from R43 unchanged
# --------------------------------------------------------------------------- #
def test_judge_reproduces_the_r41_convention():
    conv = J.convention("R41_PER_NOTIONAL_ZERO_CONTROL")
    assert conv["K"] == 1.0 and conv["rho"] == 1.0


def test_judge_reproduces_the_r42_convention():
    conv = J.convention("R42_COMMITTED_CAPITAL_CASH_CONTROL")
    assert conv["K"] == pytest.approx(1.35)
    assert conv["rho"] == 0.0


def test_remunerated_margin_is_not_charged_the_risk_free_rate():
    idx = pd.date_range("2005-01-03", periods=500, freq="B")
    g = pd.Series(0.0001, index=idx)
    bk = J.implementable_book(g, pd.Series(1.0, index=idx),
                              committed_capital=0.1,
                              collateral_class="REMUNERATED_MARGIN")
    assert float(bk["benchmark"].abs().sum()) == pytest.approx(0.0, abs=1e-12)


def test_unremunerated_collateral_is_charged_the_risk_free_rate():
    idx = pd.date_range("2021-01-04", periods=500, freq="B")
    g = pd.Series(0.0001, index=idx)
    bk = J.implementable_book(g, pd.Series(1.0, index=idx),
                              committed_capital=1.35,
                              collateral_class="UNREMUNERATED_FULLY_FUNDED")
    assert float(bk["benchmark"].sum()) > 0.0


def test_undeclared_collateral_class_is_refused():
    idx = pd.date_range("2005-01-03", periods=50, freq="B")
    with pytest.raises(ValueError):
        J.implementable_book(pd.Series(0.0, index=idx),
                             pd.Series(1.0, index=idx),
                             committed_capital=1.0,
                             collateral_class="MADE_UP")


def test_option_collateral_classes_are_declared_in_advance():
    for k in ("OPTION_PREMIUM_PAID", "OPTION_MARGINED_SHORT"):
        assert k in C.OPTION_COLLATERAL_CLASSES
    assert C.OPTION_COLLATERAL_CLASSES[
        "OPTION_PREMIUM_PAID"]["collateral_earns_rf"] == 0.0


# --------------------------------------------------------------------------- #
# Kill battery and qualification
# --------------------------------------------------------------------------- #
def test_every_declared_kill_test_ran():
    lanes = _lanes()
    tests = lanes["engine2"]["kill_battery"]
    for name in C.PORTFOLIO_KILL_TESTS:
        if name == "PBO_COMBINATORIAL_SPLIT":
            assert lanes["engine2"]["pbo"]["state"] in ("MEASURED",
                                                        "NOT_RUN")
            continue
        assert name in tests, name


def test_qualification_gate_reports_every_item():
    lanes = _lanes()
    q = lanes["engine2"]["qualification"]
    for k in C.PORTFOLIO_ALPHA_GATE:
        if k == "survives_search_adjustment":
            assert "survives_search_adjustment" in q["gate"]
            continue
        assert k in q["gate"], k
    assert q["qualifies_as_portfolio_alpha"] == (not q["failed_checks"])


def test_pbo_is_a_probability():
    lanes = _lanes()
    p = lanes["engine2"]["pbo"]
    if p.get("state") != "MEASURED":
        pytest.skip("PBO not measured")
    assert 0.0 <= p["pbo"] <= 1.0
    assert p["n_rules"] == len(C.COMBINATION_RULES)


def test_leave_one_out_tests_actually_dropped_something():
    lanes = _lanes()
    t = lanes["engine2"]["kill_battery"]
    for k in ("LEAVE_ONE_STREAM_OUT", "LEAVE_ONE_FAMILY_OUT",
              "LEAVE_ONE_ASSET_CLASS_OUT", "LEAVE_ONE_YEAR_OUT"):
        assert t[k]["n"] > 0, k


def test_search_adjustment_does_not_call_a_negative_a_survivor():
    v = _verdict()
    bh = v["search_adjustment"]
    for row in bh["rows"]:
        if row["is_a_positive_survivor"]:
            assert row["t"] > 0
    assert bh["n_positive_survivors"] == sum(
        1 for r in bh["rows"] if r["is_a_positive_survivor"])


def test_search_adjustment_uses_the_cumulative_denominator():
    v = _verdict()
    bh = v["search_adjustment"]
    assert bh["global_denominator"] >= 302


# --------------------------------------------------------------------------- #
# Intraday event alignment
# --------------------------------------------------------------------------- #
def test_release_stamps_respect_daylight_saving():
    st = ID.release_stamps()
    if st is None:
        pytest.skip("no release calendar")
    st = st.copy()
    st["hour"] = st["stamp_utc"].dt.hour
    st["month"] = pd.to_datetime(st["date"]).dt.month
    winter = st[(st["month"] == 1) & (st["declared_time_et"] == "08:30")]
    summer = st[(st["month"] == 7) & (st["declared_time_et"] == "08:30")]
    if winter.empty or summer.empty:
        pytest.skip("insufficient calendar coverage")
    assert set(winter["hour"]) == {13}
    assert set(summer["hour"]) == {12}


def test_entry_is_never_at_the_print():
    assert min(C.INTRADAY_ENTRY_DELAYS_MIN) >= 1, (
        "the estate has no fill at the release instant")


def test_cfd_instruments_are_excluded_from_futures_hypotheses():
    assert C.NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS is True
    assert set(C.INTRADAY_INSTRUMENTS) & set(C.INTRADAY_EXCLUDED_AS_CFD) \
        == set()
    for s in ("USA500IDXUSD", "BUNDTREUR"):
        assert s in C.INTRADAY_EXCLUDED_AS_CFD


def test_intraday_cost_uses_the_observed_spread_on_both_sides():
    assert C.INTRADAY_COST_MODEL == \
        "OBSERVED_HALF_SPREAD_BOTH_SIDES_PLUS_SLIPPAGE"
    assert C.INTRADAY_SLIPPAGE_BPS_PER_SIDE > 0


def test_placebo_excludes_real_release_dates():
    st = ID.release_stamps()
    if st is None:
        pytest.skip("no release calendar")
    shifted = set((pd.to_datetime(st["date"])
                   + pd.Timedelta(days=7)).dt.date)
    real = set(pd.to_datetime(st["date"]).dt.date)
    assert C.INTRADAY_PLACEBO
    assert len(shifted - real) > 0


def test_release_time_is_declared_not_inferred():
    assert C.MACRO_RELEASE_TIMES_ARE_A_DECLARED_CONSTANT is True
    assert C.MACRO_RELEASE_TIMES_ET["EMPLOYMENT_SITUATION"] == "08:30"
    assert C.MACRO_RELEASE_TIMES_ET["INDUSTRIAL_PRODUCTION"] == "09:15"


def test_intraday_score_charges_cost_on_every_event():
    ev = pd.DataFrame({"event": ["X"] * 4,
                       "date": pd.to_datetime(["2015-01-05"] * 4),
                       "stamp_utc": pd.to_datetime(
                           ["2015-01-05 13:30:00+00:00"] * 4),
                       "shock": [0.001, -0.001, 0.001, -0.001],
                       "forward": [0.0, 0.0, 0.0, 0.0],
                       "cost": [0.0002] * 4,
                       "half_spread_in_bps": [1.0] * 4,
                       "half_spread_out_bps": [1.0] * 4})
    card = ID.score_rule(ev, "REVERSAL")
    assert card["gross_bps_per_event"] == pytest.approx(0.0, abs=1e-9)
    assert card["net_bps_per_event"] == pytest.approx(-2.0, abs=1e-6)


# --------------------------------------------------------------------------- #
# Options
# --------------------------------------------------------------------------- #
def test_option_window_may_diagnose_but_not_qualify():
    assert C.A_SHORT_WINDOW_MAY_DIAGNOSE_AND_MAY_NOT_QUALIFY is True
    assert C.OPTION_MIN_FIT_SESSIONS >= 250
    assert C.OPTION_MIN_JUDGED_SESSIONS >= 250


def test_option_surface_reports_how_much_history_it_lacks():
    st = OP.surface_state()
    if st.get("state") != "MEASURED":
        pytest.skip("no option surface acquired in this environment")
    assert st["sessions_required"] == (C.OPTION_MIN_FIT_SESSIONS
                                       + C.OPTION_MIN_JUDGED_SESSIONS)
    assert st["sessions_short_by"] >= 0
    assert st["may_qualify"] is False
    assert st["additional_months_required"] >= 0


def test_iv_is_inverted_locally_not_bought():
    assert C.OPTION_VENDOR_GREEKS_REQUIRED is False
    assert "BLACK_SCHOLES" in C.OPTION_IV_METHOD


# --------------------------------------------------------------------------- #
# Analyst vintages
# --------------------------------------------------------------------------- #
def test_a_current_snapshot_is_never_a_historical_vintage():
    assert C.NO_CURRENT_SNAPSHOT_AS_HISTORICAL_VINTAGE is True


def test_vendor_backward_strip_is_reconciled_not_trusted():
    from alpha_agent.r44 import acquisition as AQ
    rec = AQ.reconcile_backward_strip()
    if rec.get("state") != "MEASURED":
        pytest.skip("no aligned vintage pairs yet")
    assert 0.0 <= rec["match_rate"] <= 1.0
    assert rec["verdict"] in ("VENDOR_BACKWARD_STRIP_IS_FAITHFUL",
                              "VENDOR_BACKWARD_STRIP_IS_RESTATED")
    assert rec["n_comparisons"] > 0
    assert "competing_explanation" in rec


def test_sample_request_is_prepared_and_not_sent():
    from alpha_agent.r44 import acquisition as AQ
    req = AQ.operator_sample_request(write=False)
    assert req["state"] == "PREPARED_NOT_SENT"
    assert req["may_send_vendor_email"] is False


def test_prospective_ledger_boundary_forbids_backfill():
    from alpha_agent.r44 import acquisition as AQ
    st = AQ.vintage_ledger_state()
    if st.get("state") != "PRESENT":
        pytest.skip("no prospective vintage ledger")
    assert st["backfill_before_floor_allowed"] is False
    assert st["is_a_reconstruction"] is False


# --------------------------------------------------------------------------- #
# Less-efficient markets
# --------------------------------------------------------------------------- #
def test_liquidity_tiers_partition_the_universe():
    t = NI.liquidity_table()
    if t.empty:
        pytest.skip("no futures store")
    tt = NI.tiers(t)
    allm = [m for v in tt["tiers"].values() for m in v]
    assert len(allm) == len(set(allm)) == len(t)
    assert set(tt["tiers"]) == set(NI.TIER_NAMES)


def test_zero_volume_markets_are_excluded():
    t = NI.liquidity_table()
    if t.empty:
        pytest.skip("no futures store")
    assert float(t["adv_usd"].min()) > 0.0


def test_cost_multiplier_is_bounded_and_never_below_one():
    t = NI.liquidity_table()
    if t.empty:
        pytest.skip("no futures store")
    m = NI.cost_multipliers(t)
    assert min(m.values()) >= 1.0
    assert max(m.values()) <= NI.COST_SCALE_CAP + 1e-9


def test_capacity_is_bound_by_the_least_liquid_leg():
    t = NI.liquidity_table()
    if t.empty:
        pytest.skip("no futures store")
    adv = t.set_index("market")["adv_usd"].to_dict()
    mk = list(adv)[:5]
    bk = {"markets": mk, "n_markets": len(mk)}
    cap = NI.capacity(bk, t)
    assert cap["state"] == "MEASURED"
    assert cap["binding_market"] == min(mk, key=lambda m: adv[m])
    assert cap["capacity_usd"] <= cap["capacity_liquidity_weighted_usd"]


def test_fx_conversion_is_for_tiering_only():
    assert C.NICHE_COST_IS_LIQUIDITY_SCALED is True
    assert C.CAPACITY_IS_A_RESULT_NOT_A_FILTER is True
    assert C.LOWER_CAPACITY_IS_ACCEPTABLE is True
    assert C.FANTASY_EXECUTION_IS_NOT is True


def test_advance_bar_is_the_frozen_contract_value():
    assert NI.ADVANCE_T == C.STANDALONE_ALPHA_GATE["t_min_lock"]


# --------------------------------------------------------------------------- #
# Freeze and forward evidence
# --------------------------------------------------------------------------- #
def test_nothing_is_frozen_without_a_surviving_lockbox_result():
    v = _verdict()
    fz = v["freeze"]
    assert fz["promotion_allowed"] is False
    assert fz["n_frozen"] <= C.MAX_NEW_SHADOWS
    if fz["n_frozen"] == 0:
        assert fz["why_none"]
    for r in v["standalone_frontier"]:
        if r.get("QUALIFICATION_STATE") != "RESEARCH_CANDIDATE":
            assert r["CANDIDATE_ID"] not in fz["frozen"]


def test_prior_shadow_registries_are_untouched():
    v = _verdict()
    w = v["witnesses_unchanged"]
    assert w["all_unchanged"] is True, w["changed"]
    assert w["n_witnesses"] >= 10


def test_r40_shadow_registry_is_not_written_by_r44():
    p = Path(r"D:\Stock_Prediction_app_data\prospective_alpha_r40"
             r"\r40_prospective_alpha_acceleration_v1"
             r"\shadow_registry_v2.json")
    if not p.exists():
        pytest.skip("R40 registry not present")
    body = json.loads(p.read_text(encoding="utf-8"))
    blob = json.dumps(body)
    assert "r44" not in blob.lower()
    assert "release44" not in blob.lower()


def test_r44_has_an_operational_write_attribution_profile():
    """A release that writes anywhere must be nameable as the writer."""
    import importlib.util
    p = Path(r"C:\Users\binis\paper_trader\scripts"
             r"\r33_operational_write_attribution.py")
    if not p.exists():
        pytest.skip("attribution owner not present")
    spec = importlib.util.spec_from_file_location("_r33_attr", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    prof = mod.profile_for("R44")
    assert "alpha_agent/r44/*.py" in prof["source_globs"]
    assert "r44_orthogonal_portfolio_alpha_v1" in prof["markers"]
    assert "orthogonal_portfolio_alpha_r44" in prof["markers"]
    with pytest.raises(Exception):
        mod.profile_for("R_NOT_A_RELEASE")


def test_never_backfill_is_declared():
    assert C.NEVER_BACKFILL_PROSPECTIVE_ROWS is True
    assert C.PRIOR_SHADOWS_ARE_IMMUTABLE is True
    assert C.DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_ACTIVITY is True


# --------------------------------------------------------------------------- #
# Purchase gate
# --------------------------------------------------------------------------- #
def test_purchase_gate_spends_nothing():
    g = PU.gate()
    assert g["money_spent_usd"] == 0.0
    assert g["accounts_created"] == 0
    assert g["trials_started"] == 0
    assert g["licences_accepted"] == 0
    assert g["payment_details_submitted"] == 0
    assert g["vendor_emails_sent"] == 0
    assert g["no_purchase_is_made"] is True


def test_every_purchase_candidate_is_fully_specified():
    for row in PU.candidates():
        for field in ("PROVIDER", "EXACT_DATASET", "EXACT_PRICE", "HISTORY",
                      "ASSETS", "FREQUENCY", "PIT_QUALITY",
                      "SURVIVORSHIP_QUALITY", "SAMPLE_QUALITY",
                      "EXACT_HYPOTHESES_UNLOCKED",
                      "WHY_CURRENT_DATA_CANNOT_ANSWER_THEM",
                      "EXPECTED_DECISION_VALUE", "RECOMMEND"):
            assert row.get(field), (row["rank_key"], field)
        assert row["RECOMMEND"] in ("RECOMMEND_BUY", "RECOMMEND_SKIP",
                                    "NEED_SAMPLE")


def test_purchase_ranking_is_by_gain_per_dollar():
    rows = PU.candidates()
    gains = [r["_gain"]["gain_per_1000_usd"] for r in rows]
    assert gains == sorted(gains, reverse=True)


# --------------------------------------------------------------------------- #
# Verdict integrity
# --------------------------------------------------------------------------- #
def test_verdict_reports_every_result_axis_separately():
    v = _verdict()
    for axis in C.RESULT_AXES:
        assert axis in v["result_axes"], axis
    assert C.NEVER_COLLAPSE_RESULT_AXES is True


def test_verdict_answers_all_fifteen_questions():
    v = _verdict()
    assert len(v["fifteen_answers"]) == len(C.FIFTEEN_QUESTIONS)
    for k, a in v["fifteen_answers"].items():
        assert isinstance(a, str) and len(a) > 20, k


def test_headline_reports_the_investment_result_not_the_system_result():
    v = _verdict()
    h = v["headline"]
    for k in ("STANDALONE_ALPHA_RESULT", "PORTFOLIO_ALPHA_RESULT",
              "STRUCTURAL_PREMIUM_RESULT", "BEST_PORTFOLIO",
              "BEST_STRUCTURAL_PREMIUM_CONTROL",
              "BEST_PORTFOLIO_NET_RESIDUAL_ALPHA", "BEST_PORTFOLIO_T_STAT",
              "BEST_PORTFOLIO_SHARPE", "BEST_PORTFOLIO_MAX_DRAWDOWN",
              "PORTFOLIO_INCREMENT_OVER_STRUCTURAL_CONTROL",
              "PORTFOLIO_INCREMENT_T_STAT", "FORWARD_SHADOWS_ADDED",
              "GLOBAL_SEARCH_BURDEN", "NEW_R44_EFFECTIVE_TRIALS",
              "PORTFOLIO_SYNTHESIS_TRIALS", "MONEY_SPENT",
              "SHELL_POLICY_VIOLATION"):
        assert k in h, k
    assert h["MONEY_SPENT"] == 0.0
    assert h["GLOBAL_SEARCH_BURDEN"] >= 302
    assert h["PORTFOLIO_SYNTHESIS_TRIALS"] >= 1
    # The flag must report what the shell-policy record says, whatever that
    # is. Asserting it is False would force a release to hide a violation
    # to keep its own regression green, which is the opposite of the point.
    from alpha_agent.r44 import shell_policy as SP
    assert h["SHELL_POLICY_VIOLATION"] == SP.violated()
    assert SP.block()["any_event_affected_a_result"] is False


def test_modern_window_reaches_the_same_conclusion():
    lanes = _lanes()
    mw = lanes["engine2"].get("modern_window")
    if not mw:
        pytest.skip("modern window not present")
    assert mw["conclusion_unchanged"] is True, (
        "the long-window conclusion must survive a window in which every "
        "declared stream exists at once")


def test_terminal_state_is_declared():
    v = _verdict()
    assert v["terminal_state"] in C.TERMINAL_STATES


def test_no_alpha_terminal_requires_every_zero_cost_branch_executed():
    v = _verdict()
    if v["terminal_state"] != \
            "R44_NO_ALPHA_AFTER_ORTHOGONAL_AND_PORTFOLIO_SYNTHESIS":
        pytest.skip("different terminal state")
    bm = v["branch_matrix"]
    assert bm["every_lane_terminated"] is True, bm["lanes"]
    for lane, state in bm["lanes"].items():
        assert state in C.BLOCKER_VOCAB, (lane, state)


def test_contract_integrity_matches_the_amended_body():
    v = _verdict()
    ci = v["contract_integrity"]
    assert ci["passes"] is True, ci
    assert ci["frozen_contract_hash"]
    assert ci["n_disclosed_amendments"] == len(C.POST_FREEZE_AMENDMENTS)


def test_verdict_is_hashed_and_safety_blocked():
    v = _verdict()
    assert len(v["verdict_hash"]) == 64
    sb = v["safety_block"]
    for k in ("creates_order", "activates_sleeve", "promotes_model",
              "writes_operational_store", "may_spend_money",
              "creates_capital_allocation", "mutates_portfolio"):
        assert sb[k] is False, k
    assert v["money_spent_usd"] == 0.0


def test_readiness_reports_zero_side_effects():
    v = _verdict()
    r = v["readiness"]
    for k in ("operational_writes", "portfolio_mutations", "orders",
              "model_promotions", "scheduler_changes"):
        assert r[k] == 0, k


def test_artifacts_exist_and_are_hashed():
    for name in ("R44_FINAL_VERDICT.json", "R44_LANE_RESULTS.json",
                 "R44_STANDALONE_ALPHA_FRONTIER.json",
                 "R44_PORTFOLIO_ALPHA_FRONTIER.json",
                 "R44_STRUCTURAL_PREMIUM_CONTROL.json",
                 "R44_PURCHASE_GATE.json", "R44_STREAM_INVENTORY.json",
                 "r44_frozen_contract.json", "r44_search_burden_ledger.json"):
        p = CAMP / name
        if not p.exists():
            pytest.skip("campaign artifacts not present: %s" % name)
        assert len(sha_file(p)) == 64


def test_independence_is_measured_not_assumed():
    lanes = _lanes()
    ind = lanes["engine2"]["inventory"]["independence"]
    assert ind["n_pairs"] > 0
    assert ind["threshold"] == C.MAX_CORRELATION_FOR_INDEPENDENCE
    assert ind["mean_abs_correlation"] is not None
    assert 0.0 <= ind["mean_abs_correlation"] <= 1.0


def test_diversification_arithmetic_is_reported():
    lanes = _lanes()
    d = lanes["engine2"]["diversification_arithmetic"]
    assert d["mean_single_stream_vol_ann"] > 0
    assert d["portfolio_vol_ann"] > 0
    assert d["vol_reduction_ratio"] < 1.0, (
        "if diversification did not reduce risk, the portfolio test was not "
        "a portfolio test")
    assert math.isfinite(d["weighted_mean_stream_return_ann"])
