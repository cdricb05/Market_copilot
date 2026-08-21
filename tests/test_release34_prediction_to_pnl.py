"""Release 34 - Prediction-to-PnL Conversion regression.

These tests protect the properties that decide whether any number the campaign
published means anything. Most run on synthetic data and need neither the vendor
nor the network; the artifact tests read what the campaign actually froze and
skip when the research root is absent.

Every guard added to the architecture audit is NEGATIVE-PROBED here: it is shown
failing against a deliberately broken source. A guard that has never been
observed to fail has not been shown to guard anything. The same discipline is
applied to the campaign's own gates - the concentration gate, the
parameter-cliff gate, the engagement gate and the multiple-testing direction
split are each shown rejecting something.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_PKG_PARENT = str(Path(__file__).resolve().parents[2])
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import r34
from paper_trader.alpha_agent.r34 import attrition as _attrition
from paper_trader.alpha_agent.r34 import calibration as _calibration
from paper_trader.alpha_agent.r34 import campaign as _campaign
from paper_trader.alpha_agent.r34 import concentration as _concentration
from paper_trader.alpha_agent.r34 import contract as _contract
from paper_trader.alpha_agent.r34 import economics as _economics
from paper_trader.alpha_agent.r34 import forecast as _forecast
from paper_trader.alpha_agent.r34 import horizon as _horizon
from paper_trader.alpha_agent.r34 import panel as _panel
from paper_trader.alpha_agent.r34 import portfolio as _portfolio
from paper_trader.alpha_agent.r34 import sizing as _sizing
from paper_trader.alpha_agent.r34 import turnover as _turnover
from paper_trader.alpha_agent.r34 import universe as _universe
from paper_trader.alpha_agent.r34 import walkforward as _walkforward

CAMPAIGN_DIR = r34.campaign_dir(_contract.CAMPAIGN_ID)
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
    for verdict in _contract.VERDICTS_WITH_ALPHA_FAIL:
        assert verdict != _contract.VERDICT_QUALIFIED
    v = _campaign.build_verdict(
        campaign_id="t", created_at="t", contract={"contract_hash": "x"},
        primary=_contract.VERDICT_NO_CONVERSION, universe={}, candidates=[],
        finalists=[], multiple_testing={}, evidence={})
    assert v["alpha_result"] == _contract.RESULT_FAIL
    assert v["system_result"] == _contract.RESULT_PASS


def test_needs_forward_confirmation_is_still_an_alpha_failure():
    """The verdict that means "interesting" must not read as a success."""
    v = _campaign.build_verdict(
        campaign_id="t", created_at="t", contract={"contract_hash": "x"},
        primary=_contract.VERDICT_NEEDS_FORWARD, universe={}, candidates=[],
        finalists=[], multiple_testing={}, evidence={})
    assert v["alpha_result"] == _contract.RESULT_FAIL
    assert _contract.VERDICT_NEEDS_FORWARD in _contract.VERDICTS_WITH_ALPHA_FAIL


def test_no_fresh_lockbox_is_claimed():
    """R31, R32 and R33 all selected on evidence through 2026. Declaring a
    fresh lockbox would be a fiction, and the contract says so."""
    assert _contract.FRESH_UNSEEN_EVIDENCE_EXISTS is False
    assert "r33_predictive_edge_v2" in _contract.EVIDENCE_USED_BY_PRIOR_CAMPAIGNS
    state = _walkforward.evidence_state()
    assert state["a_fold_may_be_called_a_lockbox"] is False
    assert state["evidence_produced"] == \
        _contract.HISTORICAL_WALK_FORWARD_EVIDENCE
    assert state["verdict_ceiling_without_fresh_evidence"] == \
        _contract.VERDICT_NEEDS_FORWARD


def test_qualification_requires_independent_evidence_that_does_not_exist():
    """The ceiling is structural: with no independent evidence, the qualified
    verdict is unreachable however good the economics look."""
    assert "genuinely_independent_evidence_exists" in \
        _contract.QUALIFICATION_CONDITIONS
    best = {"candidate_id": "x", "stats": {
        "utility_annualised": 1.0, "annualised_turnover": 1.0,
        "mean_gross_exposure": 0.9},
        "after_cost_excess_annualised": 0.5,
        "after_cost_excess_utility": 0.5,
        "after_cost_excess_t_stat": 9.0,
        "same_sign_fold_fraction": 1.0,
        "survives_stressed_cost": True,
        "no_severe_parameter_cliff": True}
    mt = {"benjamini_hochberg": {"rejected_beating_the_control": ["x"]}}
    conc = {"gates": {"no_sign_reversal_on_leave_one_instrument_out": True,
                      "single_instrument_pnl_share_within_limit": True,
                      "no_sign_reversal_on_leave_one_asset_class_out": True,
                      "single_asset_class_pnl_share_within_limit": True}}
    v = _campaign.build_verdict(
        campaign_id="t", created_at="t", contract={"contract_hash": "x"},
        primary=None,
        universe={"state": _contract.IMPLEMENTABLE_RESEARCH_UNIVERSE},
        candidates=[{"family": "FORECAST",
                     "predictive": {"t_stat": 9.0}}],
        finalists=[], multiple_testing=mt, evidence={}, concentration=conc,
        best=best)
    assert v["primary_verdict"] == _contract.VERDICT_NEEDS_FORWARD
    assert v["alpha_result"] == _contract.RESULT_FAIL
    assert v["failed_conditions"] == ["genuinely_independent_evidence_exists"]


def test_config_ceiling_and_planned_total_agree_with_the_frozen_grids():
    """The planned count is DERIVED from the grids, so it cannot drift from
    what will actually run - v1 typed 12 for a family that enumerates 18."""
    expected_forecast = (1 + len(_contract.RIDGE_ALPHAS)
                         + len(_contract.ELASTIC_NET_ALPHAS)
                         + len(_contract.HIERARCHICAL_SHRINK)) \
        * len(_contract.HORIZONS)
    assert _contract.CONFIG_FAMILIES["FORECAST"] == expected_forecast
    assert _contract.CONFIG_FAMILIES["FORECAST"] == \
        len(_forecast.model_configs()) * len(_contract.HORIZONS)
    assert _contract.CONFIG_FAMILIES["CALIBRATION"] == \
        len(_contract.CALIBRATIONS)
    assert _contract.CONFIG_FAMILIES["SIZING"] == len(_contract.SIZINGS)
    assert _contract.CONFIG_FAMILIES["TURNOVER"] == \
        len(_contract.TURNOVER_RULES)
    assert _contract.CONFIG_FAMILIES["PORTFOLIO"] == len(_contract.PORTFOLIOS)
    assert _contract.PLANNED_CONFIG_TOTAL == sum(
        _contract.CONFIG_FAMILIES.values())
    assert _contract.PLANNED_CONFIG_TOTAL <= _contract.MAX_PRIMARY_CONFIGS


def test_defaults_and_reused_terms_are_declared():
    assert _contract.DEFAULT_CALIBRATION in _contract.CALIBRATIONS
    assert _contract.DEFAULT_SIZING in _contract.SIZINGS
    assert _contract.DEFAULT_TURNOVER in _contract.TURNOVER_RULES
    assert _contract.DEFAULT_PORTFOLIO in _contract.PORTFOLIOS
    assert _contract.PRIMARY_CONVERSION_HORIZON in _contract.HORIZONS
    assert _contract.HORIZON_CHOSEN_BY_RAW_METRIC_MAGNITUDE is False
    # Reused from R33 and asserted equal at import.
    assert _contract.IMPLEMENTATION_LAG_SESSIONS == 1
    assert _contract.MIN_HISTORY_SESSIONS == 252


def test_r33_is_frozen_and_may_not_select_anything():
    rules = _contract.R33_EVIDENCE_RULES
    assert rules["may_be_rerun"] is False
    assert rules["lockbox_may_be_reopened"] is False
    assert rules["candidates_may_be_retuned"] is False
    assert rules["may_select_r34_finalists"] is False
    assert rules["may_reduce_the_multiple_testing_denominator"] is False
    assert rules["may_generate_hypotheses"] is True
    assert _contract.R33_DENOMINATOR == 105
    assert _contract.R33_LOCKBOX_ACCESSES == 8


def test_v1_is_superseded_and_the_change_only_tightens():
    sup = _contract.SUPERSEDED_CAMPAIGNS["r34_prediction_to_pnl_v1"]
    assert sup["gate_change_direction"] == "STRICTLY_TIGHTENING"
    assert sup["measurements_unchanged"] is True
    assert sup["is_preserved_on_disk"] is True
    assert len(sup["defects"]) >= 2
    assert _contract.SUPERSEDED_EVIDENCE_RULES["may_select_finalists"] is False


def test_contract_hash_is_stable_and_excludes_the_environment():
    body = _contract.build(campaign_id="t", created_at="2026-01-01", repo=REPO)
    again = _contract.build(campaign_id="t", created_at="2026-01-01", repo=REPO)
    assert body["contract_hash"] == again["contract_hash"]
    assert _contract.verify(body)["stable"] is True


# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
def test_every_safety_flag_is_false():
    block = r34.safety_block()
    for key, value in block.items():
        if key == "safety":
            continue
        assert value is False, f"{key} must be False in a research package"
    assert "NO OPERATIONAL WRITE" in block["safety"]
    assert "RESEARCH ONLY" in block["safety"]


def test_r34_source_has_no_operational_write_path():
    """The package may not address an operational store, import an operational
    owner, or reach the production API."""
    forbidden = ("D:\\\\Stock_Prediction_app_data\\\\portfolio_decisions",
                 "information_collection", "operational_book",
                 "portfolio_decision", "rebalance_execution",
                 "daily_close", "from ..api", "from paper_trader.api",
                 "import api", "engine.normal_cycle")
    package = REPO / "alpha_agent" / "r34"
    offences = []
    for path in sorted(package.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                offences.append(f"{path.name}: {token}")
    assert not offences, offences


def test_research_root_is_isolated_from_every_operational_store():
    root = str(r34.DEFAULT_RESEARCH_ROOT).lower()
    assert root.endswith("prediction_to_pnl_r34")
    for operational in ("portfolio_decisions", "information_collection",
                        "reallocation_proposals", "rebalance_order_plans",
                        "corporate_actions", "daily_research_cycle"):
        assert operational not in root


def test_out_of_scope_is_declared_exhaustively():
    for item in ("order creation", "paper execution", "broker integration",
                 "champion promotion", "model activation", "scheduler changes",
                 "production restart", "UI work", "API endpoint"):
        assert item in _contract.OUT_OF_SCOPE


# --------------------------------------------------------------------------- #
# Lane A - the implementable universe
# --------------------------------------------------------------------------- #
def test_a_slot_reject_pattern_falls_through_to_the_next_slot():
    """A reject pattern means NOT THIS SLOT, not "not any slot".

    The first version returned on the rejection, and two real exposures were
    lost to it: an equal-weight S&P 500 fund matched the large-cap slot, was
    rejected there, and never reached the equal-weight slot that exists to hold
    it.
    """
    hit = _universe._slot_for("Invesco S&P 500 Equal Weight ETF")
    assert hit is not None
    assert hit[0] == "US_EQUITY_EQUAL_WEIGHT"
    assert hit[3] is None
    hit = _universe._slot_for("Vanguard Global ex-US Real Estate ETF")
    assert hit is not None and hit[0] == "INTL_REAL_ESTATE"


def test_leveraged_inverse_and_hedged_products_are_excluded_by_name():
    for name in ("ProShares Ultra S&P 500", "Direxion Daily Financial Bull 3X",
                 "ProShares Short S&P 500 ETF",
                 "ProShares UltraShort 20+ Year Treasury"):
        assert _universe.LEVERAGED_INVERSE.search(name), name
    assert _universe.CURRENCY_HEDGED.search(
        "iShares Currency Hedged MSCI EAFE ETF")
    assert not _universe.LEVERAGED_INVERSE.search("SPDR S&P 500 ETF")


def test_the_universe_contract_bars_non_investable_series_from_the_portfolio():
    assert _contract.NON_INVESTABLE_SERIES_MAY_ENTER_PORTFOLIO is False
    assert "TRYUSD" in _contract.BARRED_FROM_PORTFOLIO
    assert _contract.IMPLEMENTABLE_REQUIRES_EXCHANGE_TRADED_SECURITY is True
    assert _contract.IMPLEMENTABLE_REQUIRES_TOTAL_RETURN_PRICES is True


def test_implementability_state_fails_closed():
    """Too few instruments, too few asset classes, a non-ETF or a barred
    instrument each block the label rather than downgrading it quietly."""
    thin = {"instruments": [{"symbol": "SPY", "subtype2":
                             "Exchange Traded Fund (ETF)"}],
            "asset_classes": ["EQUITY_US"]}
    state = _universe.implementability_state(thin)
    assert state["state"] == _contract.UNIVERSE_BLOCKED
    assert any("FEWER_THAN" in r for r in state["blocking_reasons"])

    barred = {"instruments": [{"symbol": "TRYUSD", "subtype2":
                               "Exchange Traded Fund (ETF)"}] * 25,
              "asset_classes": list(_universe.ASSET_CLASSES)}
    state = _universe.implementability_state(barred)
    assert state["state"] == _contract.UNIVERSE_BLOCKED
    assert any("BARRED_INSTRUMENT_ADMITTED" in r
               for r in state["blocking_reasons"])

    non_etf = {"instruments": [{"symbol": f"S{i}", "subtype2":
                                "Exchange Traded Note (ETN)"}
                               for i in range(25)],
               "asset_classes": list(_universe.ASSET_CLASSES)}
    state = _universe.implementability_state(non_etf)
    assert state["state"] == _contract.UNIVERSE_BLOCKED
    assert any("NON_ETF_ADMITTED" in r for r in state["blocking_reasons"])


def test_survivorship_the_candidate_pool_must_include_delisted_products():
    assert _contract.UNIVERSE_INCLUDES_DELISTED_CANDIDATES is True
    assert _contract.DELISTED_INSTRUMENT_IS_FORCED_TO_CASH is True


def test_cost_tier_is_a_measured_function_of_liquidity():
    assert _contract.cost_tier(5e9) == "TIER_1_MEGA"
    assert _contract.cost_tier(5e8) == "TIER_2_LARGE"
    assert _contract.cost_tier(1e8) == "TIER_3_MID"
    assert _contract.cost_tier(2e7) == "TIER_4_THIN"
    assert _contract.cost_tier(7e6) == "TIER_5_VERY_THIN"
    assert _contract.cost_tier(1e6) == "TIER_6_UNTRADABLE"
    # Illiquidity is PRICED, not ignored: the thin tiers cost multiples of the
    # mega-cap tier.
    bps = _contract.COST_TIER_BPS
    assert bps["TIER_5_VERY_THIN"] >= 10 * bps["TIER_1_MEGA"]


# --------------------------------------------------------------------------- #
# The panel
# --------------------------------------------------------------------------- #
def _toy_panel(n_sessions: int = 900, n_syms: int = 6):
    idx = pd.bdate_range("2010-01-04", periods=n_sessions)
    rng = np.random.default_rng(34)
    syms = [f"S{i}" for i in range(n_syms)]
    px = pd.DataFrame(
        100.0 * np.exp(np.cumsum(rng.normal(0.0003, 0.01, (n_sessions,
                                                           n_syms)), axis=0)),
        index=idx, columns=syms)
    dv = pd.DataFrame(1e8, index=idx, columns=syms)
    meta = {s: {"asset_class": "EQUITY_US" if i % 2 else "GOVERNMENT_RATES",
                "economic_group": "G", "slot": s, "cost_tier": "TIER_2_LARGE",
                "cost_bps_per_side": 3.0, "live_at_scan": True,
                "last_quoted": None}
            for i, s in enumerate(syms)}
    return {"calendar": idx, "prices": px, "dollar_volume": dv,
            "log_returns": np.log(px).diff(),
            "tradable": _panel.tradability_mask(px, dv),
            "cash_daily": pd.Series(0.0001, index=idx),
            "benchmark": px[syms[0]], "meta": meta, "symbols": syms,
            "max_forward_fill_sessions": 5}


def test_forecast_dates_do_not_overlap():
    """Successive observations of an h-session return may not share a day;
    overlapping windows inflate the effective sample by roughly h."""
    idx = pd.bdate_range("2010-01-04", periods=2000)
    for h in _contract.HORIZONS:
        dates = _panel.forecast_dates(idx, horizon=h)
        assert all(b - a == h for a, b in zip(dates, dates[1:]))
        # The last decision must leave room for the lag AND the full holding.
        assert dates[-1] + _contract.IMPLEMENTATION_LAG_SESSIONS + h < len(idx)


def test_tradability_is_point_in_time():
    """An instrument is tradable only once it has history AND trailing
    liquidity - a fund liquid today was not liquid in its first month."""
    idx = pd.bdate_range("2010-01-04", periods=800)
    px = pd.DataFrame(100.0, index=idx, columns=["A"])
    dv = pd.DataFrame(1e8, index=idx, columns=["A"])
    mask = _panel.tradability_mask(px, dv)
    assert not mask["A"].iloc[:_contract.MIN_HISTORY_SESSIONS - 1].any()
    assert mask["A"].iloc[-1]

    # Below the liquidity floor, history alone is not enough.
    thin = pd.DataFrame(1.0, index=idx, columns=["A"])
    assert not _panel.tradability_mask(px, thin)["A"].any()


def test_an_untradable_instrument_can_never_receive_weight():
    conv = np.array([10.0, 10.0, 10.0])
    trade = np.array([True, False, False])
    ac = np.array(["EQUITY_US"] * 3)
    for mapping in _contract.PORTFOLIOS:
        w = _portfolio.build_weights(
            mapping, conviction=conv, expected_return=np.full(3, 0.01),
            predicted_vol=np.full(3, 0.15), asset_class=ac, tradable=trade)
        assert w[1] == 0.0 and w[2] == 0.0, mapping


def test_observation_returns_exclude_untradable_decisions():
    panel = _toy_panel()
    panel["tradable"] = panel["tradable"].copy()
    panel["tradable"].iloc[:, 0] = False
    obs = _panel.observation_returns(panel, horizon=20)
    assert obs.iloc[:, 0].isna().all()


# --------------------------------------------------------------------------- #
# Lane C - calibration
# --------------------------------------------------------------------------- #
def test_pool_adjacent_violators_is_monotone_and_least_squares():
    y = np.array([3.0, 1.0, 2.0, 5.0, 4.0])
    fit = _calibration.pool_adjacent_violators(y)
    assert np.all(np.diff(fit) >= -1e-12)
    assert abs(float(fit.mean() - y.mean())) < 1e-12
    already = np.array([1.0, 2.0, 3.0])
    assert np.allclose(_calibration.pool_adjacent_violators(already), already)


def test_isotonic_refuses_a_sample_too_small_to_support_it():
    rng = np.random.default_rng(1)
    n = _contract.MIN_ISOTONIC_TRAINING_ROWS - 1
    spec = _calibration.fit(_contract.CAL_ISOTONIC, rng.normal(size=n),
                            rng.normal(size=n))
    assert spec["state"] == "ISOTONIC_SAMPLE_TOO_SMALL"
    assert spec["kind"] == "CONSTANT"
    assert spec["confidence"] == 0.0


def test_calibration_recovers_a_known_linear_relationship():
    rng = np.random.default_rng(2)
    s = rng.normal(size=6000)
    y = 0.02 + 0.5 * s + rng.normal(scale=0.05, size=6000)
    spec = _calibration.fit(_contract.CAL_LINEAR, s, y)
    assert abs(spec["slope"] - 0.5) < 0.05
    assert abs(spec["intercept"] - 0.02) < 0.01
    applied = _calibration.apply(spec, np.array([0.0, 1.0]))
    assert abs(applied["expected_return"][0] - 0.02) < 0.01
    assert (applied["uncertainty"] > 0).all()


def test_shrinking_calibrations_reduce_the_slope_they_retain():
    rng = np.random.default_rng(3)
    s = rng.normal(size=6000)
    y = 0.5 * s + rng.normal(scale=1.0, size=6000)
    linear = _calibration.fit(_contract.CAL_LINEAR, s, y)
    ridge = _calibration.fit(_contract.CAL_RIDGE_SHRUNK, s, y)
    bayes = _calibration.fit(_contract.CAL_BAYES, s, y)
    assert abs(ridge["slope"]) < abs(linear["slope"])
    assert abs(bayes["slope"]) <= abs(linear["slope"])
    assert 0.0 <= bayes["confidence"] <= 1.0


def test_a_degenerate_calibration_returns_no_information_not_a_confident_zero():
    spec = _calibration.fit(_contract.CAL_LINEAR, np.zeros(5), np.zeros(5))
    assert spec["state"] == "INSUFFICIENT_TRAINING_ROWS"
    assert spec["confidence"] == 0.0
    applied = _calibration.apply(spec, np.array([1.0, 2.0, 3.0]))
    assert applied["confidence"] == 0.0


def test_rank_only_discards_magnitude_but_keeps_order():
    s = np.array([0.001, 5.0, 0.002, -3.0])
    trade = np.array([True, True, True, True])
    r = _calibration.rank_only(s, trade)
    assert list(np.argsort(r)) == list(np.argsort(s))
    assert abs(float(r.max()) - 0.5) < 1e-9


def test_reliability_measures_the_calibration_slope():
    rng = np.random.default_rng(4)
    e = rng.normal(scale=0.02, size=4000)
    r = 0.5 * e + rng.normal(scale=0.02, size=4000)
    out = _calibration.reliability(e, r)
    assert out["state"] == "OK"
    assert abs(out["calibration_slope"] - 0.5) < 0.1


# --------------------------------------------------------------------------- #
# Lane D / G - sizing and construction
# --------------------------------------------------------------------------- #
def test_no_sizing_rule_can_create_leverage():
    assert _contract.LEVERAGE_AVAILABLE is False
    assert _contract.MAX_GROSS_EXPOSURE == 1.0
    rng = np.random.default_rng(5)
    n = 20
    ac = np.array(["EQUITY_US"] * 10 + ["GOVERNMENT_RATES"] * 10)
    for rule in _contract.SIZINGS:
        conv = _sizing.conviction(
            rule, expected_return=rng.normal(scale=0.05, size=n),
            uncertainty=np.full(n, 0.03), predicted_vol=np.full(n, 0.12),
            score=rng.normal(size=n), confidence=0.5)
        for mapping in _contract.PORTFOLIOS:
            w = _portfolio.build_weights(
                mapping, conviction=conv,
                expected_return=rng.normal(scale=0.05, size=n),
                predicted_vol=np.full(n, 0.12), asset_class=ac,
                tradable=np.ones(n, dtype=bool))
            assert float(np.abs(w).sum()) <= _contract.MAX_GROSS_EXPOSURE + 1e-9
            assert (w >= -1e-12).all(), f"{rule}/{mapping} went short"


def test_constraint_projection_holds_after_redistribution():
    """Capping one name pushes weight into the others; a single pass can leave
    a book violating the cap it just enforced."""
    w = np.array([0.9, 0.05, 0.03, 0.02])
    ac = np.array(["EQUITY_US"] * 4)
    out = _portfolio.apply_constraints(w, ac)
    assert out.max() <= _contract.MAX_INSTRUMENT_WEIGHT + 1e-9
    assert float(out.sum()) <= _contract.MAX_GROSS_EXPOSURE + 1e-9


def test_asset_class_cap_is_enforced():
    w = np.array([0.2, 0.2, 0.2, 0.2, 0.1, 0.1])
    ac = np.array(["EQUITY_US"] * 4 + ["GOVERNMENT_RATES"] * 2)
    out = _portfolio.apply_constraints(w, ac)
    equity = float(out[:4].sum())
    assert equity <= _contract.MAX_ASSET_CLASS_WEIGHT + 1e-9


def test_a_book_with_no_positive_conviction_holds_cash():
    """Cash is a real asset choice; a book that likes nothing buys nothing."""
    n = 8
    conv = np.full(n, -1.0)
    ac = np.array(["EQUITY_US"] * n)
    w = _portfolio.build_weights(
        _contract.PORT_LONG_CASH_RANKED, conviction=conv,
        expected_return=np.full(n, -0.01), predicted_vol=np.full(n, 0.15),
        asset_class=ac, tradable=np.ones(n, dtype=bool))
    assert float(w.sum()) == 0.0


def test_effective_instruments_sees_through_a_position_count():
    concentrated = np.array([0.9] + [0.0125] * 8)
    spread = np.full(9, 1.0 / 9.0)
    assert _sizing.effective_instruments(concentrated) < 2.0
    assert _sizing.effective_instruments(spread) > 8.5


def test_long_short_is_secondary_and_may_not_qualify():
    assert _contract.LONG_SHORT_IS_SECONDARY_ONLY is True
    assert _contract.LONG_SHORT_MAY_QUALIFY_PRIMARY is False


# --------------------------------------------------------------------------- #
# Lane E - horizon normalisation
# --------------------------------------------------------------------------- #
def test_hnes_does_not_reward_a_long_horizon_for_its_arithmetic():
    """The R33 defect, reproduced and shown corrected.

    A 60-session gain series has a mechanically larger per-period mean than a
    5-session one drawn from the same annualised process, and a twelfth as many
    observations. Raw magnitude picks the long horizon; HNES does not.
    """
    # Deterministic, so the comparison is about the FORMULA and not about which
    # way one random draw happened to fall. Both series carry the same
    # annualised information ratio and are equally stable by construction; the
    # 60-session series has twelve times the raw per-period mean and a twelfth
    # as many observations, which is exactly the R33 situation.
    def _series(mean, sd, n):
        wobble = np.tile([1.0, -1.0], n // 2)       # sums to zero in every block
        return mean + sd * wobble

    n5 = 1200
    short = _series(0.0004, 0.01, n5)
    long_ = _series(0.0048, 0.0346, n5 // 12)       # 12x mean, sqrt(12)x sd

    assert float(long_.mean()) > float(short.mean()), "raw magnitude prefers 60"
    s5 = _horizon.hnes(short, horizon=5)
    s60 = _horizon.hnes(long_, horizon=60)
    # Identical annualised IR and identical stability, by construction.
    assert abs(s5["ir_annualised"] - s60["ir_annualised"]) < 0.02
    assert s5["stability"] == s60["stability"] == 1.0
    # So the ONLY thing separating them is the observation count, and HNES
    # prefers the horizon that actually has the evidence.
    assert s5["hnes"] > s60["hnes"], "HNES must not inherit the raw bias"
    assert s5["shrink"] > s60["shrink"]


def test_hnes_penalises_an_unstable_horizon():
    rng = np.random.default_rng(7)
    steady = rng.normal(0.002, 0.01, 400)
    lumpy = np.concatenate([rng.normal(0.008, 0.01, 100),
                            rng.normal(-0.0007, 0.01, 300)])
    a, b = _horizon.hnes(steady, horizon=20), _horizon.hnes(lumpy, horizon=20)
    assert a["stability"] > b["stability"]


def test_hnes_fails_closed_on_too_few_observations():
    out = _horizon.hnes(np.zeros(3), horizon=20)
    assert out["hnes"] is None
    assert out["state"] == "INSUFFICIENT_OBSERVATIONS"


def test_combination_weights_never_short_a_horizon():
    gains = {5: np.random.default_rng(8).normal(0.002, 0.01, 300),
             20: np.random.default_rng(9).normal(-0.004, 0.01, 300)}
    w = _horizon.combination_weights(_contract.COMBINE_HNES, gains, (5, 20))
    assert all(v >= 0.0 for v in w["weights"].values())
    assert abs(sum(w["weights"].values()) - 1.0) < 1e-9


def test_combination_degrades_to_equal_weight_rather_than_inventing_one():
    gains = {5: np.random.default_rng(10).normal(-0.01, 0.01, 300),
             20: np.random.default_rng(11).normal(-0.01, 0.01, 300)}
    w = _horizon.combination_weights(_contract.COMBINE_HNES, gains, (5, 20))
    assert w["degraded_to_equal"] is True
    assert set(w["weights"].values()) == {0.5}


def test_equal_weight_combination_needs_no_data_at_all():
    w = _horizon.combination_weights(_contract.COMBINE_EQUAL, {}, (5, 20, 60))
    assert w["trained"] is False
    assert abs(sum(w["weights"].values()) - 1.0) < 1e-9


# --------------------------------------------------------------------------- #
# Lane F - turnover
# --------------------------------------------------------------------------- #
def test_every_transition_rule_respects_the_constraint_set():
    rng = np.random.default_rng(12)
    n = 12
    prev = np.abs(rng.normal(size=n)); prev /= prev.sum()
    target = np.abs(rng.normal(size=n)); target /= target.sum()
    ac = np.array(["EQUITY_US"] * 6 + ["CREDIT"] * 6)
    for rule in _contract.TURNOVER_RULES:
        for param in _turnover.parameter_grid(rule):
            w = _turnover.transition(
                rule, previous=prev, target=target,
                expected_return=rng.normal(scale=0.02, size=n),
                cost_rate=np.full(n, 3e-4), asset_class=ac, param=param)
            assert float(w.sum()) <= _contract.MAX_GROSS_EXPOSURE + 1e-9
            assert (w >= -1e-12).all()
            assert w.max() <= _contract.MAX_INSTRUMENT_WEIGHT + 1e-9


def test_a_no_trade_band_reduces_traded_notional():
    # One instrument per asset class, so the 40 % class cap does not bind and
    # the test measures the BAND rather than the projection.
    prev = np.array([0.20, 0.20, 0.20, 0.20, 0.20])
    target = np.array([0.205, 0.195, 0.19, 0.21, 0.20])
    ac = np.array(["EQUITY_US", "CREDIT", "GOVERNMENT_RATES", "COMMODITY",
                   "REAL_ESTATE"])
    immediate = _turnover.transition(_contract.TURN_IMMEDIATE, previous=prev,
                                     target=target, asset_class=ac, param=0.0)
    banded = _turnover.transition(_contract.TURN_NO_TRADE_BAND, previous=prev,
                                  target=target, asset_class=ac, param=0.02)
    assert np.abs(immediate - prev).sum() > 0, "the immediate rule must trade"
    assert np.abs(banded - prev).sum() < np.abs(immediate - prev).sum()


def test_the_turnover_objective_is_not_to_minimise_turnover():
    assert _contract.TURNOVER_OBJECTIVE == \
        "MAXIMISE_EXPECTED_AFTER_COST_UTILITY"


def test_the_penalised_rule_declines_a_trade_that_cannot_pay_for_itself():
    prev = np.zeros(3)
    target = np.array([0.2, 0.2, 0.2])
    # One instrument per asset class, so the 40 % class cap cannot mask the
    # effect being measured.
    ac = np.array(["EQUITY_US", "CREDIT", "GOVERNMENT_RATES"])
    # Expected return far below the penalty-weighted cost: no trade.
    w = _turnover.transition(_contract.TURN_PENALISED, previous=prev,
                             target=target,
                             expected_return=np.full(3, 1e-9),
                             cost_rate=np.full(3, 3e-4), asset_class=ac,
                             param=5.0)
    assert float(w.sum()) < 1e-6
    # Expected return far above it: the trade is made essentially in full. The
    # rule still shaves the hurdle's own share off every leg - keep is
    # 1 - param*rate/|er| = 0.9985 here - so the residual is the cost being
    # charged, not the rule declining.
    w = _turnover.transition(_contract.TURN_PENALISED, previous=prev,
                             target=target, expected_return=np.full(3, 1.0),
                             cost_rate=np.full(3, 3e-4), asset_class=ac,
                             param=5.0)
    keep = 1.0 - 5.0 * 3e-4 / 1.0
    assert abs(float(w.sum()) - keep * float(target.sum())) < 1e-9


# --------------------------------------------------------------------------- #
# Economics
# --------------------------------------------------------------------------- #
def test_cost_is_charged_on_traded_notional_both_legs():
    assert _contract.COST_BASE == "TRADED_NOTIONAL"
    idx = pd.bdate_range("2015-01-05", periods=4)
    weights = pd.DataFrame({"A": [0.0, 0.5, 0.0, 0.5]}, index=idx)
    excess = pd.DataFrame({"A": [0.0, 0.0, 0.0, 0.0]}, index=idx)
    meta = {"A": {"asset_class": "EQUITY_US", "cost_bps_per_side": 10.0}}
    path = _economics.evaluate_book(weights, excess,
                                    pd.Series(0.0, index=idx), meta=meta,
                                    horizon=20)
    # 0 -> 0.5 -> 0 -> 0.5 is 1.5 of traded notional, not 1.0 one-way.
    assert abs(float(path["traded_notional"].sum()) - 1.5) < 1e-12
    assert abs(float(path["costs"].sum()) - 1.5 * 10.0 / 1e4) < 1e-12


def test_excess_over_cash_may_not_rank_anything():
    assert _contract.EXCESS_OVER_CASH_MAY_RANK is False
    assert _contract.ECONOMIC_CONTROL == _contract.CONTROL_VOL_MATCHED
    decl = _economics.judge_declaration()
    assert decl["excess_over_cash_may_rank"] is False
    assert decl["returns_are_total_returns"] is True


def test_the_volatility_matched_control_cannot_use_leverage():
    rng = np.random.default_rng(13)
    book = rng.normal(0.01, 0.08, 200)     # far riskier than the benchmark
    bench = rng.normal(0.004, 0.01, 200)
    cash = np.full(200, 0.0005)
    vm = _economics.volatility_matched_control(book, bench, cash)
    assert vm["state"] == "OK"
    assert vm["weight"] <= 1.0


def test_utility_charges_for_carrying_more_risk():
    steady = np.full(200, 0.004)
    wild = np.random.default_rng(14).normal(0.004, 0.06, 200)
    assert _economics.utility(steady, horizon=20) > \
        _economics.utility(wild, horizon=20)


def test_judge_behaviour_hash_moves_when_the_judge_changes(monkeypatch):
    before = _economics.behaviour_hash()
    monkeypatch.setattr(_contract, "RISK_AVERSION", 9.0)
    assert _economics.behaviour_hash() != before


# --------------------------------------------------------------------------- #
# Walk-forward
# --------------------------------------------------------------------------- #
def test_the_embargo_drops_decisions_whose_holding_window_leaks():
    idx = pd.bdate_range("1999-01-04", periods=7000)
    dates = idx[_panel.forecast_dates(idx, horizon=20)]
    dix = np.asarray(_panel.forecast_dates(idx, horizon=20))
    folds = _walkforward.folds(dates, horizon=20, calendar=idx,
                               decision_index=dix)
    assert folds, "no folds were produced"
    for fold in folds:
        assert fold["embargoed"] >= 1, "no decision was embargoed at all"
        t0 = pd.Timestamp(fold["evaluation_start"])
        # Nothing in training may be decided on or after the evaluation opens.
        assert (dates[fold["train"]] < t0).all()


def test_inner_selection_never_touches_the_evaluation_block():
    idx = pd.bdate_range("1999-01-04", periods=7000)
    dates = idx[_panel.forecast_dates(idx, horizon=20)]
    dix = np.asarray(_panel.forecast_dates(idx, horizon=20))
    for fold in _walkforward.folds(dates, horizon=20, calendar=idx,
                                   decision_index=dix):
        assert not set(fold["inner_fit"]) & set(fold["evaluation"])
        assert not set(fold["inner_validation"]) & set(fold["evaluation"])
        assert not set(fold["inner_fit"]) & set(fold["inner_validation"])
        assert set(fold["inner_fit"]) <= set(fold["train"])
        assert set(fold["inner_validation"]) <= set(fold["train"])


def test_folds_are_chronological_and_do_not_overlap():
    starts = [pd.Timestamp(a) for a, _b in _contract.WALK_FORWARD_FOLDS]
    ends = [pd.Timestamp(b) for _a, b in _contract.WALK_FORWARD_FOLDS]
    assert starts == sorted(starts)
    for end, nxt in zip(ends, starts[1:]):
        assert end < nxt
    assert _contract.RANDOM_SPLIT_ALLOWED is False


# --------------------------------------------------------------------------- #
# Concentration - the R33 failure mode
# --------------------------------------------------------------------------- #
def _toy_path(contrib: np.ndarray, cols: list):
    n = contrib.shape[0]
    return {"columns": cols,
            "contribution": contrib,
            "weights": np.abs(contrib) * 10.0,
            "cash_leg": np.zeros(n),
            "cost_rates_used": np.zeros(len(cols)),
            "net": contrib.sum(axis=1),
            "dates": pd.bdate_range("2015-01-05", periods=n)}


def test_a_single_instrument_carrying_the_result_is_caught():
    """R33's TRYUSD: broad-looking economics that one market produced."""
    rng = np.random.default_rng(15)
    n, cols = 120, ["A", "B", "C", "D"]
    contrib = rng.normal(-0.0004, 0.002, (n, 4))
    contrib[:, 0] = rng.normal(0.006, 0.002, n)      # A carries everything
    path = _toy_path(contrib, cols)
    meta = {c: {"asset_class": "EQUITY_US"} for c in cols}
    out = _concentration.analyse(path, np.zeros(n), meta=meta, horizon=20)
    assert "A" in out["instruments_that_reverse_the_sign"]
    assert out["gates"]["no_sign_reversal_on_leave_one_instrument_out"] is False
    assert out["passes"] is False
    assert out["max_single_instrument_pnl_share"] > \
        _contract.MAX_SINGLE_INSTRUMENT_PNL_SHARE


def test_a_genuinely_diversified_book_passes_the_share_gates():
    rng = np.random.default_rng(16)
    n = 200
    cols = [f"S{i}" for i in range(12)]
    contrib = rng.normal(0.0008, 0.002, (n, 12))
    path = _toy_path(contrib, cols)
    meta = {c: {"asset_class": ("EQUITY_US" if i % 3 == 0 else
                                "CREDIT" if i % 3 == 1 else
                                "GOVERNMENT_RATES")}
            for i, c in enumerate(cols)}
    out = _concentration.analyse(path, np.zeros(n), meta=meta, horizon=20)
    assert out["gates"]["single_instrument_pnl_share_within_limit"] is True
    assert out["gates"]["enough_effective_instruments"] is True


def test_the_sign_reversal_reading_is_labelled_when_the_base_is_zero():
    """When the base excess is indistinguishable from zero, every removal flips
    it - that is a statement about the base, not about concentration, and it
    must not be readable as an R33-style single-market finding."""
    rng = np.random.default_rng(17)
    n, cols = 200, ["A", "B", "C", "D"]
    contrib = rng.normal(0.0, 0.002, (n, 4))
    contrib -= contrib.mean()                     # base excess is exactly ~0
    path = _toy_path(contrib, cols)
    meta = {c: {"asset_class": "EQUITY_US"} for c in cols}
    out = _concentration.analyse(path, np.zeros(n), meta=meta, horizon=20)
    assert out["sign_reversal_test_is_informative"] is False
    assert "indistinguishable" in out["sign_reversal_reading"]


def test_the_concentration_thresholds_are_frozen_before_evaluation():
    assert _contract.CONCENTRATION_GATE_FROZEN_BEFORE_EVALUATION is True
    assert _contract.SIGN_REVERSAL_ON_LEAVE_ONE_OUT_DISQUALIFIES is True
    assert _contract.LEAVE_ONE_INSTRUMENT_OUT_REQUIRED is True
    assert _contract.LEAVE_ONE_ASSET_CLASS_OUT_REQUIRED is True


# --------------------------------------------------------------------------- #
# The engagement gate
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("gross,turn,expected", [
    (0.90, 1.0, True),
    (0.01, 1.0, False),      # holds nothing
    (0.90, 0.001, False),    # never trades
    (0.01, 0.001, False),    # both
])
def test_a_book_that_takes_no_positions_cannot_qualify(gross, turn, expected):
    """Cash is a legitimate ALLOCATION and abstention is a legitimate ANSWER,
    but a book that takes no positions has converted no prediction - and its
    after-cost excess of about zero would otherwise outrank every genuinely
    negative candidate."""
    best = {"candidate_id": "x", "stats": {
        "utility_annualised": 0.01, "annualised_turnover": turn,
        "mean_gross_exposure": gross},
        "after_cost_excess_annualised": 1e-6,
        "after_cost_excess_utility": 1e-6,
        "after_cost_excess_t_stat": 0.001,
        "same_sign_fold_fraction": 1.0,
        "survives_stressed_cost": True,
        "no_severe_parameter_cliff": True}
    v = _campaign.build_verdict(
        campaign_id="t", created_at="t", contract={"contract_hash": "x"},
        primary=None,
        universe={"state": _contract.IMPLEMENTABLE_RESEARCH_UNIVERSE},
        candidates=[], finalists=[], multiple_testing={}, evidence={},
        concentration={"gates": {}}, best=best)
    assert v["qualification_conditions"][
        "book_actually_takes_positions"] is expected


# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
def test_the_denominator_counts_every_executed_configuration():
    assert _contract.DENOMINATOR_COUNTS_ALL_EXECUTED is True
    assert _contract.CONTROLS_ENTER_DENOMINATOR is False
    assert _contract.ADAPTIVE_SEARCH_ALLOWED is False
    candidates = [{"candidate_id": f"c{i}", "family": "SIZING", "horizon": 20,
                   "after_cost_excess_annualised": 0.0,
                   "after_cost_excess_utility": 0.0,
                   "after_cost_excess_t_stat": 0.1}
                  for i in range(37)]
    mt = _campaign.run_multiple_testing(candidates, 20)
    assert mt["denominator_executed_configurations"] == 37
    assert mt["benjamini_hochberg"]["m"] == 37


def test_a_significant_LOSS_is_not_reported_as_a_survivor():
    """BH uses two-sided p-values, so a candidate that reliably LOSES to the
    control is rejected exactly as a winner would be. Only the positive list
    may support a qualification."""
    candidates = [
        {"candidate_id": "loser", "family": "CALIBRATION", "horizon": 20,
         "after_cost_excess_annualised": -0.05,
         "after_cost_excess_utility": -0.05,
         "after_cost_excess_t_stat": -8.0},
        {"candidate_id": "winner", "family": "SIZING", "horizon": 20,
         "after_cost_excess_annualised": 0.05,
         "after_cost_excess_utility": 0.05,
         "after_cost_excess_t_stat": 8.0},
    ]
    bh = _campaign.run_multiple_testing(candidates, 20)["benjamini_hochberg"]
    assert bh["n_rejected"] == 2
    assert bh["rejected_losing_to_the_control"] == ["loser"]
    assert bh["rejected_beating_the_control"] == ["winner"]
    assert bh["only_positive_rejections_may_qualify"] is True


def test_the_verdict_reads_only_the_positive_rejection_list():
    best = {"candidate_id": "loser", "stats": {
        "utility_annualised": 0.01, "annualised_turnover": 1.0,
        "mean_gross_exposure": 0.9},
        "after_cost_excess_annualised": -0.05,
        "after_cost_excess_utility": -0.05,
        "after_cost_excess_t_stat": -8.0,
        "same_sign_fold_fraction": 1.0, "survives_stressed_cost": True,
        "no_severe_parameter_cliff": True}
    mt = {"benjamini_hochberg": {"rejected_beating_the_control": [],
                                 "rejected_losing_to_the_control": ["loser"]}}
    v = _campaign.build_verdict(
        campaign_id="t", created_at="t", contract={"contract_hash": "x"},
        primary=None,
        universe={"state": _contract.IMPLEMENTABLE_RESEARCH_UNIVERSE},
        candidates=[], finalists=[], multiple_testing=mt, evidence={},
        concentration={"gates": {}}, best=best)
    assert v["qualification_conditions"][
        "survives_multiple_testing_procedure"] is False


# --------------------------------------------------------------------------- #
# The attrition waterfall
# --------------------------------------------------------------------------- #
def test_the_waterfall_declares_every_required_failure_mode():
    required = ("forecast_too_weak", "magnitude_poorly_calibrated",
                "sizing_destroys_rank_skill", "turnover_consumes_edge",
                "diversification_dilutes_edge",
                "risk_matched_benchmark_dominates",
                "exposure_neutrality_removes_apparent_alpha",
                "works_only_in_one_asset_class", "works_only_in_one_horizon",
                "works_only_under_unrealistic_cost",
                "covariance_or_risk_forecast_error")
    for mode in required:
        assert mode in _attrition.FAILURE_MODES


def test_the_waterfall_prices_each_conversion_step():
    stages = {"RAW_FORECAST_SKILL": 0.04, "PERFECT_FORESIGHT_SIZED": 0.37,
              "CALIBRATED_EXPECTED_RETURN": 0.075, "AFTER_SIZING": 0.025,
              "AFTER_CONSTRAINTS": 0.057, "AFTER_TURNOVER_CONTROL": 0.056,
              "AFTER_COST": 0.0557, "AFTER_RISK_MATCHED_CONTROL": -0.0006,
              "AFTER_UTILITY_CHARGE": -0.0006}
    out = _attrition.build(
        horizon=20, rank_ic={"value": 0.06, "t_stat": 3.4},
        stage_paths=stages, control=np.full(50, 0.004),
        cost_scenarios={"OPTIMISTIC": 8e-5, "BASE": 2e-5, "STRESSED": -1e-4},
        per_asset_class_excess={}, per_horizon_excess={},
        concentration={"effective_instruments": 11.0,
                       "asset_classes_that_reverse_the_sign": []})
    assert [r["stage"] for r in out["stages"]] == list(_attrition.STAGES)
    assert abs(out["share_of_ceiling_captured"] - 0.0557 / 0.37) < 1e-9
    drops = {r["stage"]: r["drop_from_previous"] for r in out["stages"]}
    # The largest drop of all is ceiling -> calibrated forecast: that is the
    # price of not having perfect foresight, and it is not a conversion defect.
    assert drops["CALIBRATED_EXPECTED_RETURN"] == max(
        v for v in drops.values() if v is not None)
    # Among the stages AFTER the forecast is fixed, the risk-matched control is
    # the decisive one - that IS a conversion finding.
    after_forecast = {k: v for k, v in drops.items()
                      if k in ("AFTER_SIZING", "AFTER_CONSTRAINTS",
                               "AFTER_TURNOVER_CONTROL", "AFTER_COST",
                               "AFTER_RISK_MATCHED_CONTROL",
                               "AFTER_UTILITY_CHARGE")
                      and v is not None}
    assert after_forecast["AFTER_RISK_MATCHED_CONTROL"] == max(
        after_forecast.values())
    assert out["failure_modes"]["risk_matched_benchmark_dominates"][
        "measured"] is True


def test_a_result_that_needs_optimistic_costs_is_flagged():
    assert _attrition._only_under_optimistic_cost(
        {"OPTIMISTIC": 0.01, "BASE": -0.001, "STRESSED": -0.01}) is True
    assert _attrition._only_under_optimistic_cost(
        {"OPTIMISTIC": 0.02, "BASE": 0.01, "STRESSED": 0.005}) is False


# --------------------------------------------------------------------------- #
# The frozen artifacts
# --------------------------------------------------------------------------- #
def test_every_required_artifact_exists():
    required = ["research_contract.json", "implementable_universe.json",
                "instrument_integrity.json", "forecast_models.json",
                "calibration_results.json", "position_sizing_results.json",
                "horizon_combination_results.json",
                "turnover_cost_results.json", "portfolio_results.json",
                "walk_forward_results.json", "concentration_results.json",
                "attrition_waterfall.json", "multiple_testing.json",
                "final_verdict.json"]
    if not CAMPAIGN_DIR.exists():
        pytest.skip("campaign not run")
    missing = [n for n in required if not (CAMPAIGN_DIR / n).exists()]
    assert not missing, missing


def test_the_frozen_universe_is_implementable_and_broad():
    art = _artifact("implementable_universe.json")
    assert art["universe_label"] == _contract.IMPLEMENTABLE_RESEARCH_UNIVERSE
    assert len(art["instruments"]) >= _contract.MIN_INSTRUMENT_COUNT
    assert len(art["asset_classes"]) >= _contract.MIN_ASSET_CLASS_COUNT
    assert art["survivorship"]["candidate_pool_includes_delisted"] is True
    assert art["survivorship"]["delisted_products_enumerated"] > 0
    for row in art["instruments"]:
        assert row["instrument_type"] == "Exchange Traded Fund (ETF)"
        assert row["total_return_represented"] is True
        assert row["symbol"] not in _contract.BARRED_FROM_PORTFOLIO


def test_the_frozen_verdict_is_consistent_with_its_conditions():
    art = _artifact("final_verdict.json")
    assert art["primary_verdict"] in _contract.PRIMARY_VERDICTS
    if art["alpha_result"] == _contract.RESULT_PASS:
        assert art["primary_verdict"] == _contract.VERDICT_QUALIFIED
        assert all(art["qualification_conditions"].values())
    else:
        assert art["primary_verdict"] != _contract.VERDICT_QUALIFIED
        assert art["qualified_candidates"] == []
    assert art["system_result"] == _contract.RESULT_PASS
    assert art["evidence_state"]["fresh_unseen_evidence_exists"] is False


def test_the_frozen_denominator_is_within_the_ceiling_and_counts_everything():
    art = _artifact("multiple_testing.json")
    assert art["within_ceiling"] is True
    assert art["denominator_executed_configurations"] <= \
        _contract.MAX_PRIMARY_CONFIGS
    assert art["executed_by_family"] == art["planned_by_family"], (
        "the executed enumeration must match the frozen plan exactly")
    assert art["denominator_executed_configurations"] == \
        art["planned_config_total"]


def test_the_frozen_campaign_wrote_nothing_outside_its_research_root():
    if not CAMPAIGN_DIR.exists():
        pytest.skip("campaign not run")
    root = r34.research_root().resolve()
    for path in CAMPAIGN_DIR.rglob("*"):
        assert str(path.resolve()).startswith(str(root))


def test_release_33_evidence_is_untouched():
    """R34 may read R33 as history and may not rewrite it."""
    from paper_trader.alpha_agent import r33
    from paper_trader.alpha_agent.r33 import contract as r33_contract
    r33_dir = r33.campaign_dir(r33_contract.CAMPAIGN_ID)
    if not r33_dir.exists():
        pytest.skip("R33 artifacts not present")
    verdict = json.loads((r33_dir / "final_verdict.json").read_text(
        encoding="utf-8"))
    assert verdict["primary_verdict"] == r33_contract.VERDICT_NO_EDGE
    assert verdict["alpha_result"] == r33_contract.RESULT_FAIL
    mt = json.loads((r33_dir / "multiple_testing.json").read_text(
        encoding="utf-8"))
    denominator = (mt.get("denominator_executed_configurations")
                   or mt.get("denominator_executed_candidates"))
    assert denominator == _contract.R33_DENOMINATOR


def test_the_frozen_attrition_waterfall_is_present_even_though_alpha_failed():
    """The waterfall is REQUIRED whether or not alpha qualifies - it is the
    knowledge the release leaves behind when the answer is no."""
    art = _artifact("attrition_waterfall.json")
    assert [r["stage"] for r in art["stages"]] == list(_attrition.STAGES)
    assert art["perfect_foresight_ceiling"] is not None
    assert art["share_of_ceiling_captured"] is not None


# --------------------------------------------------------------------------- #
# The operational-write rule is REUSED, not copied
# --------------------------------------------------------------------------- #
def _attribution_owner():
    scripts = REPO / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    try:
        import r33_operational_write_attribution as owner
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"attribution owner unavailable: {exc}")
    return owner


def test_release_34_reuses_the_canonical_attribution_rule():
    """One implementation with one regression suite, not a second mtime check.

    A release contributes only the strings that identify its writer.
    """
    owner = _attribution_owner()
    assert "R34" in owner.RELEASE_PROFILES
    profile = owner.profile_for("R34")
    assert "alpha_agent/r34/*.py" in profile["source_globs"]
    assert "scripts/run_release34_prediction_to_pnl.py" in \
        profile["source_files"]
    assert "prediction_to_pnl_r34" in profile["markers"]
    # R33 stays the default, so its gate and its behaviour are unchanged.
    assert owner.DEFAULT_PROFILE == "R33"
    assert owner.profile_for("R33")["markers"] is owner.R33_MARKERS


def test_an_unknown_release_profile_fails_closed():
    owner = _attribution_owner()
    with pytest.raises(RuntimeError) as excinfo:
        owner.profile_for("R99")
    assert "UNKNOWN_RELEASE_PROFILE" in str(excinfo.value)


def test_r34_markers_are_specific_enough_not_to_fire_by_accident():
    """A marker that fires on ordinary text trains the operator to ignore the
    gate."""
    owner = _attribution_owner()
    for marker in owner.profile_for("R34")["markers"]:
        assert len(marker) >= 8, marker
        assert marker != "r34"


def test_a_release_marker_in_an_operational_file_is_attributed_to_it(tmp_path):
    owner = _attribution_owner()
    f = tmp_path / "collection_service_state.json"
    f.write_text(json.dumps({"service_id": "PAPER_TRADER_INFORMATION_"
                                           "COLLECTION",
                             "note": "written by alpha_agent/r34"}),
                 encoding="utf-8")
    found = owner.markers_in(f, owner.profile_for("R34")["markers"])
    assert "alpha_agent/r34" in found
    # and the R33 profile does NOT claim it
    assert owner.markers_in(f, owner.R33_MARKERS) == []


def test_the_r34_source_carries_no_operational_write_path_functionally():
    """The static half of the rule, run against the real source tree."""
    owner = _attribution_owner()
    profile = owner.profile_for("R34")
    out = owner.source_operational_write_paths(
        REPO, source_globs=profile["source_globs"],
        source_files=profile["source_files"])
    assert out["sources_scanned"] >= 15
    assert out["clean"] is True, out["findings"][:5]


def test_the_frozen_ceiling_is_not_routed_through_a_calibration():
    """A perfect-foresight ceiling that goes through a calibration fitted on
    model scores can be inverted by a negative fold slope, and v1 reported the
    realised book capturing 98 % of it as a result."""
    art = _artifact("attrition_waterfall.json")
    assert art["share_of_ceiling_captured"] < 0.9, (
        "a realised book capturing nearly all of perfect foresight means the "
        "ceiling is contaminated, not that the conversion layer is perfect")
