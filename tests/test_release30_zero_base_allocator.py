"""Release 30 - zero-base allocator: intrinsic target, constraints, cash,
transition economics, ownership boundaries and safety.

The central assertion of this suite is negative: the CURRENT PORTFOLIO must be
unable to influence the zero-base target. Several tests therefore vary only the
holdings and require the intrinsic target to come back byte-identical.
"""
from __future__ import annotations

import ast
import json
import random
from pathlib import Path

import pytest

from paper_trader.engine import zero_base_allocator as zk

REPO = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
def _contract(n=40, held=None, nav=100000.0, seed=11, adv=5.0e8):
    rng = random.Random(seed)
    tks = ["T%02d" % i for i in range(n)]
    secs = ["A", "B", "C", "D", "E"]
    cands = [{"ticker": t, "sector": secs[i % 5], "adv_dollar": adv}
             for i, t in enumerate(tks)]
    mu = {t: 0.010 * (1 - 2 * i / float(n - 1)) for i, t in enumerate(tks)}
    sig = {t: 0.09 for t in tks}
    down = {t: mu[t] - 1.645 * 0.09 for t in tks}
    dates = ["d%03d" % i for i in range(80)]
    mkt = [rng.gauss(0, 0.010) for _ in dates]
    series = {t: [mkt[k] + rng.gauss(0, 0.012) for k in range(len(dates))]
              for t in tks}
    current = dict(held) if held is not None else {tks[i + 10]: 0.04
                                                   for i in range(20)}
    return {
        "eligible_market_date": "2026-08-18", "active_book_id": "book_1",
        "nav": nav, "candidates": cands, "mu": mu, "sigma_forecast": sig,
        "downside": down, "current_weights": current,
        "aligned_returns": {"dates": dates, "series": series},
        "forecast_model_spec_hash": "hash_abc",
        "feature_snapshot_hash": "hash_def",
    }


_POL = {"risk_aversion_gamma": 2.0, "uncertainty_aversion_phi": 2.0,
        "max_iterations": 250, "polish_rounds": 150}


def _run(ic, **over):
    pol = dict(_POL)
    pol.update(over)
    return zk.build_allocation(input_contract=ic, policy=pol)


def _weights(block):
    return {r["ticker"]: r["weight"] for r in block["rows"]}


# =========================================================================== #
# ZERO BASE - the intrinsic target
# =========================================================================== #
def test_01_holdings_do_not_change_the_intrinsic_target():
    """The whole point of the release, asserted directly."""
    base = _run(_contract(held={"T00": 0.04}))
    other = _run(_contract(held={"T39": 0.10, "T38": 0.10, "T37": 0.05}))
    none = _run(_contract(held={}))
    a = _weights(base["zero_base_target"])
    assert a == _weights(other["zero_base_target"])
    assert a == _weights(none["zero_base_target"])


def test_02_zero_base_economics_are_independent_of_holdings():
    a = _run(_contract(held={"T00": 0.04}))["zero_base_target"]["economics"]
    b = _run(_contract(held={}))["zero_base_target"]["economics"]
    assert a == b


def test_03_the_candidate_set_is_the_whole_eligible_universe():
    ic = _contract()
    out = _run(ic)
    assert out["universe"]["eligible_candidates"] == len(ic["candidates"])
    chosen = set(_weights(out["zero_base_target"]))
    assert chosen - set(ic["current_weights"]), (
        "the ideal target must be able to hold names that are not held")


def test_04_a_held_name_gets_no_advantage_over_an_unheld_twin():
    """Two names with identical economics must receive identical weight even
    when only one of them is held."""
    ic = _contract()
    ic["mu"]["T05"] = ic["mu"]["T06"]
    ic["aligned_returns"]["series"]["T05"] = list(
        ic["aligned_returns"]["series"]["T06"])
    ic["current_weights"] = {"T05": 0.10}
    z = _weights(_run(ic)["zero_base_target"])
    assert z.get("T05", 0.0) == pytest.approx(z.get("T06", 0.0), abs=5e-3)


def test_05_same_input_gives_the_same_target():
    ic = _contract()
    a, b = _run(ic), _run(ic)
    assert a["allocation_hash"] == b["allocation_hash"]
    assert _weights(a["zero_base_target"]) == _weights(b["zero_base_target"])


def test_06_allocation_hash_ignores_timestamps_but_tracks_weights():
    ic = _contract()
    a = _run(ic)
    b = _run(ic)
    b["generated_at"] = "2099-01-01T00:00:00+00:00"
    assert a["allocation_hash"] == zk.stable_hash(
        {k: v for k, v in a.items() if k != "allocation_hash"})
    c = _run(_contract(seed=99))
    assert c["allocation_hash"] != a["allocation_hash"]


# =========================================================================== #
# CONSTRAINTS
# =========================================================================== #
def test_07_weights_are_long_only_and_within_the_budget():
    for block in ("zero_base_target", "implementable_target"):
        out = _run(_contract())[block]
        w = _weights(out)
        assert all(v >= 0 for v in w.values())
        assert sum(w.values()) <= 1.0 + 1e-9
        assert out["constraints"]["valid"] is True
        assert out["constraints"]["long_only"] is True


def test_08_name_cap_binds():
    out = _run(_contract(), max_name_weight=0.03)
    for v in _weights(out["zero_base_target"]).values():
        assert v <= 0.03 + 1e-9


def test_09_sector_cap_binds():
    out = _run(_contract(), sector_cap_fraction=0.15)
    for sec, v in out["zero_base_target"]["constraints"][
            "sector_weights"].items():
        assert v <= 0.15 + 1e-9, sec


def test_10_liquidity_cap_binds_and_is_a_hard_bound():
    """A position the book could not build in a session is not a target."""
    ic = _contract(adv=1.0e6, nav=100000.0)
    out = _run(ic, max_adv_participation=0.5)
    for r in out["zero_base_target"]["rows"]:
        assert r["dollar_allocation"] <= 0.5 * 1.0e6 + 1.0
        assert r["weight"] <= 0.5 * 1.0e6 / 100000.0 + 1e-9


def test_11_constraints_are_verified_independently_of_the_optimiser():
    caps = {"A": 0.1, "B": 0.1}
    bad = zk.verify_constraints(weights={"A": 0.4, "B": 0.9}, caps=caps,
                                sector_of={"A": "S", "B": "S"},
                                policy=zk.default_policy())
    assert bad["valid"] is False
    codes = {v["code"] for v in bad["violations"]}
    assert "NAME_CAP_BREACH" in codes
    assert "GROSS_EXPOSURE_ABOVE_100_PCT" in codes
    assert "SECTOR_CAP_BREACH" in codes


def test_12_position_count_is_not_fixed_at_the_legacy_25():
    assert zk.LEGACY_TARGET_POSITION_COUNT == 25
    assert zk.POSITION_COUNT_POLICY == "EMERGENT_FROM_WEIGHT_CAPS_NOT_A_FIXED_COUNT"
    # A fixed count would be invariant to the weight cap. An emergent one moves
    # with it, which is exactly what is asserted here.
    tight = _run(_contract(n=60), max_name_weight=0.02,
                 min_position_weight=0.001)["zero_base_target"]["economics"]
    loose = _run(_contract(n=60), max_name_weight=0.20,
                 min_position_weight=0.001)["zero_base_target"]["economics"]
    assert tight["position_count"] != loose["position_count"]
    assert tight["position_count"] > loose["position_count"]
    # It must also respond to the price of risk. A count coinciding with 25 for
    # one configuration is allowed - an emergent number is free to land there -
    # but it may not be the SAME number for every configuration.
    averse = _run(_contract(n=60), max_name_weight=0.02,
                  min_position_weight=0.001, risk_aversion_gamma=40.0)
    counts = {tight["position_count"], loose["position_count"],
              averse["zero_base_target"]["economics"]["position_count"]}
    assert len(counts) >= 2
    assert _run(_contract())["position_count_policy"][
        "legacy_target_position_count"] == 25


def test_13_minimum_position_size_removes_dust_and_reports_it():
    out = _run(_contract(n=60), max_name_weight=0.02, min_position_weight=0.03)
    opt = out["zero_base_target"]["optimiser"]
    assert opt["min_position_weight"] == 0.03
    for v in _weights(out["zero_base_target"]).values():
        assert v >= 0.03 - 1e-9
    assert opt["dust_weight_reallocated_to_cash"] is not None


# =========================================================================== #
# CASH
# =========================================================================== #
def test_14_cash_is_allowed_and_declared_at_zero_return():
    out = _run(_contract())
    assert zk.CASH_RETURN == 0.0
    assert out["cash_policy"]["policy"] == "ZERO_RETURN_PAPER_ASSUMPTION"
    assert "risk-free" in out["cash_policy"]["doc"]
    assert out["zero_base_target"]["economics"]["cash_weight"] >= 0.0


def test_15_an_unattractive_opportunity_set_produces_cash():
    """Cash is a real choice: raise the price of risk and equity must lose."""
    ic = _contract()
    rich = _run(ic, risk_aversion_gamma=0.5)["zero_base_target"]["economics"]
    poor = _run(ic, risk_aversion_gamma=200.0)["zero_base_target"]["economics"]
    assert poor["cash_weight"] > rich["cash_weight"]
    assert poor["cash_weight"] > 0.5


def test_16_all_negative_forecasts_produce_an_all_cash_target():
    ic = _contract()
    ic["mu"] = {k: -0.05 for k in ic["mu"]}
    out = _run(ic)
    assert out["zero_base_target"]["economics"]["cash_weight"] == pytest.approx(1.0)
    assert out["zero_base_target"]["economics"]["position_count"] == 0


def test_17_cash_weight_and_invested_weight_reconcile():
    for block in ("zero_base_target", "implementable_target"):
        e = _run(_contract())[block]["economics"]
        assert e["cash_weight"] + e["invested_weight"] == pytest.approx(1.0, abs=1e-6)


# =========================================================================== #
# TRANSITION ECONOMICS
# =========================================================================== #
def test_18_an_unchanged_portfolio_has_exactly_zero_transition_cost():
    w = {"A": 0.5, "B": 0.5}
    t = zk.transition_economics(current=w, target=dict(w), nav=100000.0,
                                policy=zk.default_policy())
    assert t["one_way_turnover"] == 0.0
    assert t["transaction_cost_weight"] == 0.0
    assert t["transaction_cost_dollars"] == 0.0
    assert t["names_traded"] == 0


def test_19_cost_depends_only_on_the_difference_between_the_two_books():
    pol = zk.default_policy()
    a = zk.transition_economics(current={"A": 0.5, "B": 0.5},
                                target={"A": 0.4, "B": 0.6}, nav=1000.0,
                                policy=pol)
    b = zk.transition_economics(current={"X": 0.5, "Y": 0.5},
                                target={"X": 0.4, "Y": 0.6}, nav=1000.0,
                                policy=pol)
    assert a["transaction_cost_weight"] == b["transaction_cost_weight"]
    assert a["one_way_turnover"] == pytest.approx(0.1)


def test_20_cost_uses_the_canonical_per_side_rate():
    desk = pytest.importorskip("paper_trader.api.paper_trading_desk")
    pol = zk.default_policy()
    assert pol["cost_rate_per_side"] == pytest.approx(desk.COST_RATE_PER_SIDE)
    assert pol["cost_bps_per_side"] == pytest.approx(desk.COST_BPS_PER_SIDE)
    t = zk.transition_economics(current={"A": 0.0}, target={"A": 1.0},
                                nav=100000.0, policy=pol)
    assert t["transaction_cost_dollars"] == pytest.approx(
        100000.0 * desk.COST_RATE_PER_SIDE)


def test_21_zero_cost_makes_the_implementable_target_the_zero_base_optimum():
    out = _run(_contract(), cost_rate_per_side=0.0)
    z = out["zero_base_target"]
    i = out["implementable_target"]
    assert set(_weights(z)) == set(_weights(i))
    assert z["economics"]["expected_net_utility"] == pytest.approx(
        i["economics"]["expected_net_utility"], abs=1e-6)


def test_22_prohibitive_cost_rationally_retains_the_current_portfolio():
    held = {"T%02d" % (i + 10): 0.04 for i in range(20)}
    out = _run(_contract(held=held), cost_rate_per_side=5.0)
    assert out["transition"]["current_to_implementable"]["one_way_turnover"] == 0.0
    assert _weights(out["implementable_target"]) == pytest.approx(held, abs=1e-6)


def test_23_high_cost_reduces_turnover_monotonically():
    ic = _contract()
    low = _run(ic, cost_rate_per_side=0.0)["transition"][
        "current_to_implementable"]["one_way_turnover"]
    mid = _run(ic, cost_rate_per_side=0.01)["transition"][
        "current_to_implementable"]["one_way_turnover"]
    high = _run(ic, cost_rate_per_side=0.10)["transition"][
        "current_to_implementable"]["one_way_turnover"]
    assert low >= mid >= high


def test_24_cost_never_changes_the_zero_base_target():
    """Transaction cost is a property of the transition, never of the ideal."""
    ic = _contract()
    a = _weights(_run(ic, cost_rate_per_side=0.0)["zero_base_target"])
    b = _weights(_run(ic, cost_rate_per_side=5.0)["zero_base_target"])
    assert a == b


def test_25_the_transition_path_is_reported_and_stays_feasible():
    out = _run(_contract())
    path = out["transition"]["path_current_to_zero_base"]
    assert len(path) >= 10
    assert path[0]["fraction"] == 0.0 and path[-1]["fraction"] == 1.0
    assert path[0]["transaction_cost_weight"] == 0.0
    assert all(p["one_way_turnover"] >= 0 for p in path)


def test_26_a_held_name_outside_the_eligible_universe_is_a_mandatory_exit():
    ic = _contract()
    ic["current_weights"]["DELISTED_X"] = 0.05
    out = _run(ic)
    assert "DELISTED_X" in out["transition"]["mandatory_exits"]
    assert "DELISTED_X" not in _weights(out["zero_base_target"])
    assert "DELISTED_X" not in _weights(out["implementable_target"])


def test_27_comparison_separates_retained_removed_and_new():
    out = _run(_contract())
    cmp_ = out["comparison"]["zero_base"]
    assert (cmp_["retained_count"] + cmp_["new_count"]
            == cmp_["position_count"])
    assert set(cmp_["retained"]) <= set(out["comparison"]["current"]["holdings"])
    assert not set(cmp_["new"]) & set(out["comparison"]["current"]["holdings"])


# =========================================================================== #
# OBJECTIVE / RISK
# =========================================================================== #
def test_28_the_objective_is_declared_in_the_payload():
    obj = _run(_contract())["objective"]
    assert obj["objective_version"] == zk.OBJECTIVE_VERSION
    assert "gamma" in obj["formula"] and "Sigma" in obj["formula"]
    for c in ("long only (w >= 0)", "gross exposure <= 100%"):
        assert c in obj["constraints"]
    assert obj["solver"] == "FRANK_WOLFE_LAMINAR_GREEDY_ORACLE"
    assert obj["risk_price_source"] in ("DEFAULT", "WALK_FORWARD_CALIBRATED")


def test_29_risk_terms_are_portfolio_level_not_per_name():
    """Per-name penalties would price risk diversification removes."""
    src = (REPO / "engine" / "zero_base_allocator.py").read_text(encoding="utf-8")
    assert "property of the PORTFOLIO, not a sum of per-name penalties" in src
    e = _run(_contract())["zero_base_target"]["economics"]
    terms = e["utility_terms"]
    assert terms["covariance_risk_penalty"] <= 0
    assert terms["forecast_uncertainty_penalty"] <= 0
    assert terms["downside_shortfall_penalty"] <= 0


def test_30_higher_forecast_uncertainty_reduces_exposure():
    ic = _contract()
    calm = _run(ic)["zero_base_target"]["economics"]["invested_weight"]
    ic2 = _contract()
    ic2["sigma_forecast"] = {k: 1.5 for k in ic2["sigma_forecast"]}
    noisy = _run(ic2)["zero_base_target"]["economics"]["invested_weight"]
    assert noisy < calm


def test_31_covariance_comes_from_the_canonical_risk_owner():
    src = (REPO / "engine" / "zero_base_allocator.py").read_text(encoding="utf-8")
    assert "holding_opportunity_cost" in src
    assert "build_covariance" in src
    hoc = pytest.importorskip("paper_trader.engine.holding_opportunity_cost")
    assert hasattr(hoc, "build_covariance")
    out = _run(_contract())
    assert out["universe"]["covariance_included"] > 0


def test_32_the_covariance_builder_is_shared_with_risk_contributions():
    hoc = pytest.importorskip("paper_trader.engine.holding_opportunity_cost")
    src = (REPO / "engine" / "holding_opportunity_cost.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef)
              and n.name == "compute_risk_contributions")
    assert "build_covariance" in ast.unparse(fn), (
        "a second covariance builder would be a second risk owner")


def test_33_a_missing_forecast_blocks_rather_than_assuming_average():
    ic = _contract()
    ic["mu"] = {}
    out = _run(ic)
    assert out["state"] == zk.STATE_BLOCKED
    assert any(b["code"] == "NO_EXPECTED_RETURNS" for b in out["blockers"])


def test_34_a_candidate_without_a_forecast_is_excluded_by_name():
    ic = _contract()
    ic["mu"].pop("T03")
    out = _run(ic)
    assert "T03" in out["universe"]["excluded_without_forecast"]
    assert "T03" not in _weights(out["zero_base_target"])


def test_35_no_active_book_is_its_own_state():
    ic = _contract()
    ic["active_book_id"] = None
    assert _run(ic)["state"] == zk.STATE_NO_ACTIVE_BOOK


# =========================================================================== #
# OWNERSHIP / SAFETY
# =========================================================================== #
def test_36_the_kernel_is_pure_stdlib_and_does_no_io():
    src = (REPO / "engine" / "zero_base_allocator.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not (imported & {"numpy", "pandas", "requests", "sqlite3", "urllib"})
    for forbidden in ("open(", "requests.", "os.environ", "write_text"):
        assert forbidden not in src, forbidden


def test_37_the_allocator_creates_no_order_signal_or_decision():
    s = _run(_contract())["safety"]
    for key in ("creates_orders", "creates_signals", "creates_decisions",
                "mutates_holdings", "mutates_cash", "promotes_models",
                "automation_enabled"):
        assert s[key] is False, key
    assert s["paper_only"] is True
    assert s["manual_review_required"] is True


def test_38_it_is_not_a_second_proposal_or_decision_owner():
    src = (REPO / "api" / "zero_base_target.py").read_text(encoding="utf-8")
    assert "engine.reallocation_proposal" in src
    assert "api.portfolio_decision" in src
    assert "not a proposal engine" in src
    # Look for CALL-shaped usage, not prose: the docstring legitimately says the
    # module cannot approve anything.
    called = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Call):
            fn = node.func
            called.add(getattr(fn, "attr", getattr(fn, "id", "")))
    for forbidden in ("record_decision", "approve", "confirm_order",
                      "create_order", "build_proposal", "run_proposal",
                      "persist_proposal"):
        assert forbidden not in called, forbidden


def test_39_the_read_surface_never_writes():
    src = (REPO / "api" / "zero_base_target.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    fns = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    for name in ("run_allocation", "load_zero_base_target", "summary",
                 "build_input_contract", "resolve_policy"):
        body = ast.unparse(fns[name])
        for forbidden in ("write_text", "mkdir", "replace(", "unlink"):
            assert forbidden not in body, "%s must not write (%s)" % (name, forbidden)


def test_40_live_canonical_constants_override_the_declared_defaults():
    zbt = pytest.importorskip("paper_trader.api.zero_base_target")
    eng = pytest.importorskip("paper_trader.api.multi_horizon_engine")
    desk = pytest.importorskip("paper_trader.api.paper_trading_desk")
    pol = zbt.resolve_policy()
    assert pol["max_name_weight"] == eng.MAX_INDIVIDUAL_WEIGHT
    assert pol["sector_cap_fraction"] == eng.SECTOR_CAP_FRACTION
    assert pol["min_adv_dollar"] == eng.MIN_ADV_DOLLAR
    assert pol["cost_rate_per_side"] == desk.COST_RATE_PER_SIDE


def test_41_calibrated_risk_prices_are_used_when_the_artifact_supplies_them():
    zbt = pytest.importorskip("paper_trader.api.zero_base_target")
    art = {"horizons": {"20": {"risk_prices": {
        "risk_aversion_gamma": 3.5, "uncertainty_aversion_phi": 3.5,
        "downside_aversion_delta": 0.25, "downside_tail_factor": 1.25}}}}
    pol = zbt.resolve_policy(artifact=art)
    assert pol["risk_aversion_gamma"] == 3.5
    assert pol["downside_tail_factor"] == 1.25
    assert pol["risk_price_source"] == "WALK_FORWARD_CALIBRATED"
    assert zbt.resolve_policy()["risk_price_source"] == "DEFAULT"


def test_42_aligned_returns_have_one_owner():
    pp = pytest.importorskip("paper_trader.api.price_panel")
    assert hasattr(pp, "aligned_returns")
    src = (REPO / "api" / "reallocation_proposal.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "_aligned_returns")
    assert "pp.aligned_returns" in ast.unparse(fn)


def test_43_the_two_targets_are_named_and_never_conflated():
    out = _run(_contract())
    assert out["zero_base_target"]["target_kind"] == zk.TARGET_ZERO_BASE
    assert out["implementable_target"]["target_kind"] == zk.TARGET_IMPLEMENTABLE
    assert "not an input" in out["zero_base_target"]["doc"]
    assert "transaction costs" in out["implementable_target"]["doc"]
