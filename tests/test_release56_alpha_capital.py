"""Release 56 - alpha to capital: the deployment ladder, the payback horizon,
the incumbent opportunity cost, the frozen forward paper portfolio challengers,
the alpha opportunity registry, the operator read model and the UI acceptance.

The central assertions of this suite are the ones that would let a bad answer
through if they were missing:

* CASH CAN WIN, and it can also lose. Both are proved on constructed inputs, so
  neither outcome is an artefact of today's live data.
* A frozen shadow portfolio can NEVER be scored on a bar that existed when it
  was frozen. There is no code path to hindsight, and the test tries to find one.
* Transaction cost reduces P&L, and the incumbent gets no privilege beyond it.
* Nothing anywhere creates an order, a fill, a decision, a promotion or a
  sleeve activation, and nothing writes to a production store.
"""
from __future__ import annotations

import json
import random
import re
from pathlib import Path

import pytest

from paper_trader.api import alpha_capital as ac
from paper_trader.api import alpha_opportunity_registry as aor
from paper_trader.api import shadow_portfolio_evidence as spe
from paper_trader.engine import alpha_capital_frontier as kernel
from paper_trader.engine import shadow_portfolio_evidence as skernel
from paper_trader.engine import zero_base_allocator as zba

REPO = Path(__file__).resolve().parents[1]
UI = REPO / "api" / "ui" / "index.html"


# --------------------------------------------------------------------------- #
# Fixtures - constructed, never live
# --------------------------------------------------------------------------- #
def _inputs(n=12, seed=7, mu_scale=0.010):
    rng = random.Random(seed)
    tks = ["T%02d" % i for i in range(n)]
    mu = {t: mu_scale * (1 - 2 * i / float(n - 1)) for i, t in enumerate(tks)}
    sig = {t: 0.05 for t in tks}
    cov_included = list(tks)
    cov_h = {i: {j: (0.0016 if i == j else 0.0004) for j in tks} for i in tks}
    meta = {t: {"sector": "A", "adv_dollar": 5.0e8, "rank": i + 1,
                "destination_kind": kernel.DEST_NEW_EQUITY}
            for i, t in enumerate(tks)}
    rng.random()
    return tks, mu, sig, cov_h, cov_included, meta


def _policy(**over):
    pol = dict(zba.default_policy())
    pol.update({"risk_aversion_gamma": 2.0, "uncertainty_aversion_phi": 2.0,
                "downside_aversion_delta": 0.0, "cost_rate_per_side": 0.00125,
                "cost_bps_per_side": 12.5, "policy_horizon_sessions": 20})
    pol.update(over)
    return pol


def _ladder(*, cost_bps=12.5, mu_scale=0.010, cash=5000.0, nav=100000.0,
            mode=kernel.MODE_CASH_ONLY, n=12):
    tks, mu, sig, cov_h, cov_inc, meta = _inputs(n=n, mu_scale=mu_scale)
    pol = _policy(cost_rate_per_side=cost_bps / 10000.0, cost_bps_per_side=cost_bps)
    cur = {t: 0.06 for t in tks[6:]}                     # 6 names, 36% invested
    impl = {t: 0.08 for t in tks[:6]}                    # the optimiser wants the top 6
    impl.update({t: 0.06 for t in tks[6:]})
    zb = {t: 0.10 for t in tks[:6]}
    zb.update({t: 0.05 for t in tks[6:9]})
    return kernel.build_deployment_ladder(
        current_weights=cur, implementable_weights=impl, zero_base_weights=zb,
        mu=mu, sigma_forecast=sig, cov_h=cov_h, cov_included=cov_inc,
        policy=pol, horizon=20, nav=nav, cash=cash, available_capital=cash,
        candidate_meta=meta, mode=mode), cur, impl, zb, mu, sig, cov_h, cov_inc, pol


# --------------------------------------------------------------------------- #
# The ladder - cash can win AND cash can lose
# --------------------------------------------------------------------------- #
class TestDeploymentLadder:
    def test_ladder_is_ready_and_has_every_declared_rung(self):
        lad = _ladder()[0]
        assert lad["state"] == kernel.STATE_READY
        assert lad["n_rungs"] == len(kernel.DEFAULT_INCREMENTS)
        labels = [r["label"] for r in lad["rungs"]]
        assert labels == [i["label"] for i in kernel.DEFAULT_INCREMENTS]

    def test_cash_loses_when_the_edge_is_real(self):
        """A large expected edge against a small cost: deployment must clear."""
        lad = _ladder(cost_bps=1.0, mu_scale=0.030)[0]
        assert lad["n_rungs_clearing_hurdle"] > 0
        assert lad["all_rungs_retain_cash"] is False
        assert lad["marginal_dollar"]["first_dollar_pays"] is True

    def test_cash_wins_when_the_cost_exceeds_the_edge(self):
        """The SAME portfolios at a punitive cost rate: cash must win every rung.

        This is the assertion that makes the ladder an economic statement rather
        than an advertisement for trading."""
        lad = _ladder(cost_bps=900.0, mu_scale=0.0005)[0]
        assert lad["n_rungs_clearing_hurdle"] == 0
        assert lad["all_rungs_retain_cash"] is True
        assert lad["marginal_dollar"]["first_dollar_pays"] is False
        for r in lad["rungs"]:
            assert r["cash_wins"] is True
            assert r["hurdle_state"] == kernel.HURDLE_FAILS_NET_UTILITY
            assert r["cash_wins_reason"]

    def test_cash_wins_when_there_is_no_cash(self):
        lad = _ladder(cash=0.0)[0]
        assert lad["deployable_capacity_usd"] == 0.0
        for r in lad["rungs"]:
            assert r["deployed_usd"] == 0.0
            assert r["cash_wins"] is True
            assert r["hurdle_state"] == kernel.HURDLE_FAILS_NO_CAPACITY

    def test_cash_only_mode_never_spends_more_than_the_cash_on_hand(self):
        lad = _ladder(cash=5000.0, nav=100000.0, mode=kernel.MODE_CASH_ONLY)[0]
        for r in lad["rungs"]:
            assert r["deployed_usd"] <= 5000.0 + 1e-6
            if r["requested_usd"] > 5000.0:
                assert r["unfunded_usd"] > 0

    def test_cash_only_mode_sells_nothing(self):
        lad = _ladder(mode=kernel.MODE_CASH_ONLY)[0]
        for r in lad["rungs"]:
            assert (r["sold_weight"] or 0.0) == pytest.approx(0.0, abs=1e-9)

    def test_redeployment_mode_may_sell_and_is_funded_by_sales(self):
        lad = _ladder(mode=kernel.MODE_REDEPLOYMENT)[0]
        assert lad["mode"] == kernel.MODE_REDEPLOYMENT
        assert any((r["sold_weight"] or 0.0) > 0 for r in lad["rungs"])

    def test_the_two_modes_are_different_answers(self):
        a = _ladder(mode=kernel.MODE_CASH_ONLY)[0]
        b = _ladder(mode=kernel.MODE_REDEPLOYMENT)[0]
        assert a["deployable_capacity_usd"] != b["deployable_capacity_usd"]

    def test_transaction_cost_reduces_the_net_gain(self):
        cheap = _ladder(cost_bps=1.0)[0]
        dear = _ladder(cost_bps=60.0)[0]
        for c, d in zip(cheap["rungs"], dear["rungs"]):
            assert d["transaction_cost_usd"] > c["transaction_cost_usd"]
            assert d["net_of_cost_gain"] <= c["net_of_cost_gain"]

    def test_every_rung_reports_the_facts_an_allocator_must_weigh(self):
        lad = _ladder()[0]
        need = ("destinations", "expected_utility_gain", "transaction_cost_usd",
                "net_of_cost_gain", "incremental_risk_horizon",
                "incremental_concentration_hhi", "max_adv_participation",
                "one_way_turnover", "hurdle_state", "cash_wins", "payback",
                "funding_source", "cash_weight_after")
        for r in lad["rungs"]:
            for k in need:
                assert k in r, k

    def test_the_ladder_creates_nothing(self):
        lad = _ladder()[0]
        for k in ("creates_orders", "creates_decisions", "creates_proposal",
                  "mutates_holdings", "mutates_cash"):
            assert lad[k] is False


# --------------------------------------------------------------------------- #
# Payback
# --------------------------------------------------------------------------- #
class TestPayback:
    def test_no_cost_means_nothing_to_pay_back(self):
        p = kernel.payback(gain_per_horizon=0.01, cost_weight=0.0,
                           horizon_sessions=20)
        assert p["verdict"] == kernel.PAYBACK_NOT_APPLICABLE

    def test_no_gain_never_pays_back(self):
        p = kernel.payback(gain_per_horizon=-0.001, cost_weight=0.002,
                           horizon_sessions=20)
        assert p["verdict"] == kernel.PAYBACK_NEVER
        assert p["payback_sessions"] is None

    def test_a_gain_larger_than_the_cost_pays_back_inside_one_horizon(self):
        p = kernel.payback(gain_per_horizon=0.004, cost_weight=0.002,
                           horizon_sessions=20)
        assert p["verdict"] == kernel.PAYBACK_WITHIN_ONE_HORIZON
        assert p["payback_horizons"] == pytest.approx(0.5)
        assert p["payback_sessions"] == pytest.approx(10.0)

    def test_a_gain_smaller_than_the_cost_needs_several_horizons(self):
        p = kernel.payback(gain_per_horizon=0.001, cost_weight=0.002,
                           horizon_sessions=20)
        assert p["verdict"] == kernel.PAYBACK_MULTI_HORIZON
        assert p["payback_sessions"] == pytest.approx(40.0)


# --------------------------------------------------------------------------- #
# Incumbent opportunity cost - no privilege beyond the switch cost
# --------------------------------------------------------------------------- #
class TestIncumbentOpportunityCost:
    def _oc(self, cur, cost_bps=12.5):
        tks, mu, sig, cov_h, cov_inc, _m = _inputs()
        pol = _policy(cost_rate_per_side=cost_bps / 10000.0,
                      cost_bps_per_side=cost_bps)
        zb = {t: 0.10 for t in tks[:8]}
        impl = {t: 0.08 for t in tks[:8]}
        impl.update({t: 0.04 for t in tks[8:]})
        return kernel.incumbent_opportunity_cost(
            current_weights=cur, zero_base_weights=zb,
            implementable_weights=impl, mu=mu, sigma_forecast=sig, cov_h=cov_h,
            cov_included=cov_inc, policy=pol, horizon=20, nav=100000.0)

    def test_the_incumbent_is_priced_on_the_same_objective(self):
        tks = _inputs()[0]
        oc = self._oc({t: 0.06 for t in tks[4:]})
        assert oc["incumbency_privilege"] is False
        assert oc["current"]["expected_net_utility"] is not None
        assert set(oc["against"]) == {"zero_base", "implementable"}

    def test_holding_the_target_already_costs_nothing_to_switch_to(self):
        tks = _inputs()[0]
        zb = {t: 0.10 for t in tks[:8]}
        oc = self._oc(zb)
        leg = oc["against"]["zero_base"]
        assert leg["switch_cost_weight"] == pytest.approx(0.0, abs=1e-12)
        assert leg["payback"]["verdict"] == kernel.PAYBACK_NOT_APPLICABLE

    def test_a_higher_cost_rate_lengthens_the_payback(self):
        tks = _inputs()[0]
        cur = {t: 0.06 for t in tks[4:]}
        cheap = self._oc(cur, cost_bps=1.0)["against"]["zero_base"]["payback"]
        dear = self._oc(cur, cost_bps=50.0)["against"]["zero_base"]["payback"]
        if cheap["payback_sessions"] is not None and dear["payback_sessions"] is not None:
            assert dear["payback_sessions"] > cheap["payback_sessions"]

    def test_the_switch_cost_is_the_only_incumbency_effect(self):
        """Two different current books, the same targets: the TARGET economics
        must be identical and only the switch cost may differ."""
        tks = _inputs()[0]
        a = self._oc({t: 0.06 for t in tks[4:]})
        b = self._oc({t: 0.05 for t in tks[2:]})
        for leg in ("zero_base", "implementable"):
            ta = a["against"][leg]["target_concentration"]
            tb = b["against"][leg]["target_concentration"]
            assert ta == tb
        assert (a["against"]["zero_base"]["switch_cost_weight"]
                != b["against"]["zero_base"]["switch_cost_weight"])


# --------------------------------------------------------------------------- #
# Realised excess decomposition
# --------------------------------------------------------------------------- #
class TestExcessDecomposition:
    def test_terms_sum_to_the_excess(self):
        d = kernel.excess_decomposition(
            book_return_pct=-1.6386, benchmark_return_pct=3.4466,
            cash_weight=0.045574, transaction_cost_usd=207.15,
            initial_capital=100000.0)
        assert d["available"] is True
        total = sum(t["pct_points"] for t in d["terms"])
        assert total == pytest.approx(d["excess_pct_points"], abs=1e-3)

    def test_the_residual_is_not_relabelled_as_alpha(self):
        d = kernel.excess_decomposition(
            book_return_pct=-1.0, benchmark_return_pct=3.0, cash_weight=0.05,
            transaction_cost_usd=200.0, initial_capital=100000.0)
        names = [t["term"] for t in d["terms"]]
        assert "UNEXPLAINED_BY_CASH_OR_COST" in names
        assert not any("ALPHA" in n for n in names)
        resid = [t for t in d["terms"] if t["term"] == "UNEXPLAINED_BY_CASH_OR_COST"][0]
        assert resid["caveat"]
        assert d["beta_assumption"] == kernel.BETA_ASSUMPTION

    def test_missing_inputs_degrade_rather_than_guess(self):
        d = kernel.excess_decomposition(
            book_return_pct=None, benchmark_return_pct=3.0, cash_weight=0.05,
            transaction_cost_usd=1.0, initial_capital=1.0)
        assert d["available"] is False


# --------------------------------------------------------------------------- #
# Governed hurdle - an ordering is not an economic proof
# --------------------------------------------------------------------------- #
class TestGovernedHurdle:
    def test_uncalibrated_expected_return_cannot_clear_an_economic_hurdle(self):
        g = kernel.governed_capital_hurdle(
            expected_return_state="NOT_CALIBRATED",
            forecast_lane="RESEARCH_EVIDENCE_ONLY", entry_rank=25,
            eligible_destinations=[{"instrument_id": "AAA", "rank": 1}])
        assert g["hurdle_state"] == kernel.HURDLE_NOT_EVIDENCED
        assert g["economic_proof"] == "ABSENT"
        assert g["automatic_deployment_allowed"] is False
        assert g["manual_review_required"] is True

    def test_calibrated_expected_return_can(self):
        g = kernel.governed_capital_hurdle(
            expected_return_state="CALIBRATED", forecast_lane="OPERATIONAL",
            entry_rank=25, eligible_destinations=[])
        assert g["hurdle_state"] == kernel.HURDLE_CLEARS
        assert g["economic_proof"] == "PRESENT"
        assert g["automatic_deployment_allowed"] is False


# --------------------------------------------------------------------------- #
# The shared covariance helper must not have changed the allocator
# --------------------------------------------------------------------------- #
class TestHorizonCovariance:
    def test_scaling_is_the_daily_matrix_times_the_horizon(self):
        dates = ["d%02d" % i for i in range(60)]
        rng = random.Random(3)
        series = {t: [rng.gauss(0, 0.01) for _ in dates] for t in ("A", "B")}
        out = zba.horizon_covariance(
            tickers=["A", "B"], aligned_returns={"dates": dates, "series": series},
            policy=_policy(), horizon=20)
        for i in out["included_tickers"]:
            for j in out["included_tickers"]:
                assert out["covariance_horizon"][i][j] == pytest.approx(
                    out["covariance_daily"][i][j] * 20.0)
        assert out["matrix_owner"].startswith("engine.holding_opportunity_cost")


# --------------------------------------------------------------------------- #
# Forward paper portfolio challengers - immutability and NO HINDSIGHT
# --------------------------------------------------------------------------- #
def _panel(start="2026-09-01", n=10, tickers=("AAA", "BBB"), step=0.01):
    dates = []
    y, m, d = (int(x) for x in start.split("-"))
    for i in range(n):
        dates.append("%04d-%02d-%02d" % (y, m, d + i))
    series = {}
    for k, tk in enumerate(tickers):
        adj = [100.0 * ((1.0 + step * (k + 1)) ** i) for i in range(n)]
        series[tk] = {"dates": dates, "adj": adj,
                      "ret": [None] + [adj[i] / adj[i - 1] - 1.0 for i in range(1, n)]}
    return {"series": series}, dates


def _record(inception, weights=None, cost_bps=12.5, start=100000.0):
    return skernel.make_inception_record(
        challenger_id="t_ch", label="test", family="TEST",
        strategy_identity={"lane": "TEST"},
        weights=weights if weights is not None else {"AAA": 0.5, "BBB": 0.5},
        inception_session=inception, inception_timestamp="2026-09-03T00:00:00Z",
        starting_capital=start, pit_input_identity={"h": "x"},
        cost_bps_per_side=cost_bps,
        valuation_source=skernel.VALUATION_PRICE_PANEL)


class TestShadowPortfolioKernel:
    def test_the_record_hash_covers_the_body(self):
        a = _record("2026-09-03")
        b = _record("2026-09-03")
        assert a["record_hash"] == b["record_hash"]
        c = _record("2026-09-03", weights={"AAA": 1.0})
        assert c["record_hash"] != a["record_hash"]

    def test_cash_weight_is_the_complement_of_the_invested_weight(self):
        r = _record("2026-09-03", weights={"AAA": 0.4})
        assert r["invested_weight"] == pytest.approx(0.4)
        assert r["cash_weight"] == pytest.approx(0.6)
        assert r["cash_return_policy"] == skernel.CASH_RETURN_POLICY

    def test_no_bar_on_or_before_inception_is_ever_scored(self):
        panel, dates = _panel(start="2026-09-01", n=10)
        rec = _record(dates[4])
        sessions = skernel.forward_sessions(rec, panel["series"])
        assert sessions == dates[5:]
        assert all(s > dates[4] for s in sessions)

    def test_a_record_frozen_on_the_last_available_bar_has_no_evidence(self):
        panel, dates = _panel(n=6)
        rec = _record(dates[-1])
        out = skernel.accrue_forward(record=rec, price_series=panel["series"])
        assert out["sessions_scored"] == 0
        assert out["evidence_state"] == skernel.EVIDENCE_NOT_STARTED
        assert out["net_cumulative_return"] is None

    def test_forward_pnl_accrues_only_after_creation(self):
        panel, dates = _panel(n=10, step=0.01)
        early = skernel.accrue_forward(record=_record(dates[0]),
                                       price_series=panel["series"])
        late = skernel.accrue_forward(record=_record(dates[6]),
                                      price_series=panel["series"])
        assert early["sessions_scored"] > late["sessions_scored"]
        assert early["first_forward_session"] == dates[1]
        assert late["first_forward_session"] == dates[7]

    def test_transaction_cost_reduces_forward_pnl(self):
        panel, dates = _panel(n=10)
        free = skernel.accrue_forward(record=_record(dates[0], cost_bps=0.0),
                                      price_series=panel["series"])
        paid = skernel.accrue_forward(record=_record(dates[0], cost_bps=50.0),
                                      price_series=panel["series"])
        assert paid["entry_cost_usd"] > 0
        assert free["entry_cost_usd"] == 0
        assert paid["net_cumulative_return"] < free["net_cumulative_return"]
        assert paid["gross_cumulative_return"] == pytest.approx(
            free["gross_cumulative_return"])

    def test_an_uncovered_session_is_reported_not_carried_flat(self):
        panel, dates = _panel(n=8)
        panel["series"]["BBB"]["dates"] = dates[:3]
        panel["series"]["BBB"]["adj"] = panel["series"]["BBB"]["adj"][:3]
        out = skernel.accrue_forward(record=_record(dates[0]),
                                     price_series=panel["series"])
        assert out["n_uncovered_sessions"] > 0
        for u in out["uncovered_sessions"]:
            assert u["priced_share"] < skernel.MIN_PRICED_WEIGHT

    def test_sharpe_is_withheld_until_there_is_enough_evidence(self):
        panel, dates = _panel(n=8)
        out = skernel.accrue_forward(record=_record(dates[0]),
                                     price_series=panel["series"])
        assert out["sharpe"] is None
        assert out["sharpe_withheld_reason"]

    def test_comparisons_use_the_common_window_only(self):
        panel, dates = _panel(n=12)
        a = skernel.accrue_forward(record=_record(dates[0]),
                                   price_series=panel["series"])
        b = skernel.accrue_forward(record=_record(dates[5]),
                                   price_series=panel["series"])
        cmp_ = skernel.compare_on_common_window(a, b)
        assert cmp_["comparable"] is True
        assert cmp_["equal_time_window"] is True
        assert cmp_["n_common_sessions"] == len(b["curve"])
        assert cmp_["window_start"] == b["curve"][0]["date"]

    def test_two_books_that_never_overlap_are_refused_a_comparison(self):
        panel, dates = _panel(n=6)
        a = skernel.accrue_forward(record=_record(dates[0]),
                                   price_series=panel["series"])
        b = skernel.accrue_forward(record=_record(dates[-1]),
                                   price_series=panel["series"])
        cmp_ = skernel.compare_on_common_window(a, b)
        assert cmp_["comparable"] is False

    def test_a_frozen_record_has_no_turnover_of_its_own(self):
        panel, dates = _panel(n=8)
        out = skernel.accrue_forward(record=_record(dates[0]),
                                     price_series=panel["series"])
        assert out["turnover_since_inception"] == 0.0
        assert out["turnover_doc"]

    def test_implied_turnover_is_measured_across_successive_records(self):
        r1 = _record("2026-09-01", weights={"AAA": 0.5, "BBB": 0.5})
        r2 = _record("2026-09-02", weights={"AAA": 1.0})
        t = skernel.implied_turnover([r1, r2])
        assert t["n_rebalances"] == 1
        assert t["legs"][0]["one_way_turnover"] == pytest.approx(0.5)

    def test_leaderboard_ranks_maturity_before_measured_edge(self):
        panel, dates = _panel(n=30)
        mature = skernel.accrue_forward(record=_record(dates[0]),
                                        price_series=panel["series"])
        mature["challenger_id"] = "mature"
        young = skernel.accrue_forward(record=_record(dates[-3]),
                                       price_series=panel["series"])
        young["challenger_id"] = "young"
        young["net_cumulative_return"] = 99.0
        lb = skernel.leaderboard([young, mature])
        assert lb[0]["challenger_id"] == "mature"
        assert all(r["promotion_allowed"] is False for r in lb)


class TestShadowPortfolioStore:
    def _ctx(self):
        ps = {"state_hash": "psh", "economic_state_hash": "esh",
              "active_book": {"book_id": "b1"},
              "positions": [{"ticker": "AAA", "portfolio_weight": 0.5},
                            {"ticker": "BBB", "portfolio_weight": 0.4}]}
        sc = {"output_hash": "ush", "primary_model_id": "m1",
              "rankings": [{"ticker": "AAA", "rank": 1, "eligible": True},
                           {"ticker": "BBB", "rank": 2, "eligible": True},
                           {"ticker": "CCC", "rank": 3, "eligible": True}]}
        zb = {"allocation_hash": "ah", "forecast_model_spec_hash": "fh",
              "feature_snapshot_hash": "sh",
              "zero_base_target": {"rows": [{"ticker": "AAA", "weight": 0.6},
                                            {"ticker": "CCC", "weight": 0.4}]},
              "implementable_target": {"rows": [{"ticker": "AAA", "weight": 0.5},
                                                {"ticker": "BBB", "weight": 0.4}]}}
        return ps, sc, zb

    def test_freeze_creates_every_challenger_once(self, tmp_path):
        ps, sc, zb = self._ctx()
        out = spe.freeze_challengers(
            eligible_market_date="2026-09-03", portfolio_state=ps, scoring=sc,
            zero_base=zb, shadow_dir_override=tmp_path, entry_rank=2)
        assert out["n_created"] == 6
        assert out["n_refused"] == 0
        assert out["backfill_allowed"] is False
        assert out["writes_operational_store"] is False

    def test_a_frozen_record_is_never_rewritten(self, tmp_path):
        ps, sc, zb = self._ctx()
        spe.freeze_challengers(eligible_market_date="2026-09-03",
                               portfolio_state=ps, scoring=sc, zero_base=zb,
                               shadow_dir_override=tmp_path)
        before = {p.name: p.read_text(encoding="utf-8")
                  for p in (tmp_path / "records").glob("*.json")}
        zb["zero_base_target"]["rows"] = [{"ticker": "ZZZ", "weight": 1.0}]
        again = spe.freeze_challengers(eligible_market_date="2026-09-03",
                                       portfolio_state=ps, scoring=sc,
                                       zero_base=zb, shadow_dir_override=tmp_path)
        assert again["n_created"] == 0
        assert again["n_already"] == 6
        after = {p.name: p.read_text(encoding="utf-8")
                 for p in (tmp_path / "records").glob("*.json")}
        assert before == after

    def test_freeze_refuses_without_a_session(self, tmp_path):
        ps, sc, zb = self._ctx()
        out = spe.freeze_challengers(eligible_market_date="", portfolio_state=ps,
                                     scoring=sc, zero_base=zb,
                                     shadow_dir_override=tmp_path)
        assert out["state"] == spe.FREEZE_REFUSED
        assert not (tmp_path / "records").exists()

    def test_the_read_model_reports_zero_evidence_on_inception_day(self, tmp_path):
        ps, sc, zb = self._ctx()
        spe.freeze_challengers(eligible_market_date="2026-09-03",
                               portfolio_state=ps, scoring=sc, zero_base=zb,
                               shadow_dir_override=tmp_path)
        panel, _dates = _panel(start="2026-09-01", n=3, tickers=("AAA", "BBB", "CCC"))
        out = spe.load_shadow_portfolio_evidence(
            price_panel=panel, shadow_dir_override=tmp_path,
            desk_curves={"benchmark": [], "book": []})
        assert out["state"] == spe.STATE_READY
        assert out["n_challengers"] == 6
        assert out["n_with_forward_evidence"] == 0
        assert out["evidence_rules"]["backfilled"] is False
        assert out["evidence_rules"]["scored_only_after_inception"] is True

    def test_the_read_model_accrues_once_sessions_exist(self, tmp_path):
        ps, sc, zb = self._ctx()
        spe.freeze_challengers(eligible_market_date="2026-09-01",
                               portfolio_state=ps, scoring=sc, zero_base=zb,
                               shadow_dir_override=tmp_path)
        panel, _d = _panel(start="2026-09-01", n=6, tickers=("AAA", "BBB", "CCC"))
        out = spe.load_shadow_portfolio_evidence(
            price_panel=panel, shadow_dir_override=tmp_path,
            desk_curves={"benchmark": [], "book": []})
        priced = [c for c in out["challengers"]
                  if c["valuation_source"] == skernel.VALUATION_PRICE_PANEL]
        assert priced and all(c["sessions_scored"] == 5 for c in priced)
        assert out["n_with_forward_evidence"] >= len(priced)

    def test_the_read_model_states_which_question_it_answers(self, tmp_path):
        ps, sc, zb = self._ctx()
        spe.freeze_challengers(eligible_market_date="2026-09-03",
                               portfolio_state=ps, scoring=sc, zero_base=zb,
                               shadow_dir_override=tmp_path)
        out = spe.load_shadow_portfolio_evidence(
            price_panel={"series": {}}, shadow_dir_override=tmp_path,
            desk_curves={})
        f = out["comparison_framing"]
        assert "CONSTRUCTED FROM CASH" in f["question"]
        assert "switching-cost" in f["not_the_question"]
        assert "cash_deployment_frontier" in f["not_the_question"]

    def test_no_leader_is_named_before_any_session_is_scored(self, tmp_path):
        ps, sc, zb = self._ctx()
        spe.freeze_challengers(eligible_market_date="2026-09-03",
                               portfolio_state=ps, scoring=sc, zero_base=zb,
                               shadow_dir_override=tmp_path)
        s = spe.summary(spe.load_shadow_portfolio_evidence(
            price_panel={"series": {}}, shadow_dir_override=tmp_path,
            desk_curves={}))
        assert s["leader"] is None
        assert s["leader_withheld_reason"]

    def test_a_leader_is_named_once_a_session_is_scored(self, tmp_path):
        ps, sc, zb = self._ctx()
        spe.freeze_challengers(eligible_market_date="2026-09-01",
                               portfolio_state=ps, scoring=sc, zero_base=zb,
                               shadow_dir_override=tmp_path)
        panel, _d = _panel(start="2026-09-01", n=6, tickers=("AAA", "BBB", "CCC"))
        s = spe.summary(spe.load_shadow_portfolio_evidence(
            price_panel=panel, shadow_dir_override=tmp_path, desk_curves={}))
        assert s["leader"] is not None
        assert s["leader"]["sessions_scored"] > 0
        assert s["leader_withheld_reason"] is None

    def test_no_records_reports_not_started_rather_than_failing(self, tmp_path):
        out = spe.load_shadow_portfolio_evidence(shadow_dir_override=tmp_path,
                                                 desk_curves={})
        assert out["state"] == spe.STATE_NOT_STARTED
        assert out["challengers"] == []

    def test_the_governed_top25_book_is_the_ranking_and_nothing_else(self):
        sc = {"rankings": [{"ticker": "A", "rank": 1, "eligible": True},
                           {"ticker": "B", "rank": 2, "eligible": True},
                           {"ticker": "C", "rank": 3, "eligible": False},
                           {"ticker": "D", "rank": 4, "eligible": True}]}
        w = spe.governed_top25_weights(sc, entry_rank=2)
        assert set(w) == {"A", "B"}
        assert sum(w.values()) == pytest.approx(1.0)

    def test_safety_block_forbids_everything_operational(self, tmp_path):
        out = spe.load_shadow_portfolio_evidence(shadow_dir_override=tmp_path,
                                                 desk_curves={})
        s = out["safety"]
        for k in ("creates_orders", "creates_fills", "creates_signals",
                  "creates_trade_decisions", "mutates_holdings", "mutates_cash",
                  "mutates_operational_book", "writes_operational_store",
                  "promotes_model", "activates_sleeve", "enables_automation",
                  "broker_enabled", "automatic_promotion_allowed"):
            assert s[k] is False, k


# --------------------------------------------------------------------------- #
# The alpha opportunity registry
# --------------------------------------------------------------------------- #
class TestAlphaOpportunityRegistry:
    def test_every_frozen_row_carries_a_verdict_a_document_and_a_reopen_rule(self):
        for row in aor.frozen_catalogue():
            assert row["status"] in aor.STATUS_VOCAB, row["family_id"]
            assert row["capital_state"] in aor.CAPITAL_VOCAB, row["family_id"]
            ev = row["evidence"]
            assert ev.get("release") and ev.get("verdict") and ev.get("doc"), row["family_id"]
            assert row.get("reopen_condition"), row["family_id"]
            assert row.get("pit_integrity"), row["family_id"]

    def test_every_cited_document_exists(self):
        for row in aor.frozen_catalogue():
            doc = row["evidence"]["doc"]
            assert (REPO / doc).exists(), "%s cites a missing %s" % (row["family_id"], doc)

    def test_family_ids_are_unique(self):
        ids = [r["family_id"] for r in aor.frozen_catalogue()]
        assert len(ids) == len(set(ids))

    def test_readiness_vocabulary_is_respected(self):
        for r in aor.asset_class_readiness():
            assert r["readiness"] in aor.READINESS_VOCAB
            if r["readiness"] in (aor.AR_BLOCKED, aor.AR_NOT_READY):
                assert r["next_state_requires"]

    def test_a_blocked_family_is_never_capital_eligible(self):
        for row in aor.frozen_catalogue():
            if row["status"] == aor.ST_BLOCKED:
                assert row["capital_state"] == aor.CAP_BLOCKED, row["family_id"]

    def test_only_the_live_champion_legs_are_active(self):
        active = [r["family_id"] for r in aor.frozen_catalogue()
                  if r["status"] == aor.ST_ACTIVE]
        assert set(active) == {"EQ_XS_FUNDAMENTAL_QUALITY", "EQ_XS_MOMENTUM"}


class TestExperimentQueue:
    def test_rerunning_an_exhausted_family_is_rejected_by_name(self):
        q = aor.experiment_queue()
        rej = {r["experiment_id"]: r["verdict"] for r in q["rejected"]}
        assert rej.get("r56_x7_rerun_r32_sleeves") == \
            "REJECTED_RETESTS_AN_EXHAUSTED_FAMILY_WITH_NO_NEW_INFORMATION"

    def test_a_walled_family_is_rejected_even_with_a_high_score(self):
        q = aor.experiment_queue()
        row = [r for r in q["rejected"]
               if r["experiment_id"] == "r56_x8_crypto_funding_carry_live"][0]
        assert row["expected_information_value"] > aor.EIV_FLOOR
        assert row["verdict"] == "REJECTED_BLOCKED_BY_A_WALL_THE_EXPERIMENT_CANNOT_MOVE"

    def test_queued_experiments_are_ordered_by_information_value(self):
        q = aor.experiment_queue()
        vals = [r["expected_information_value"] for r in q["queued"]]
        assert vals == sorted(vals, reverse=True)
        assert [r["queue_position"] for r in q["queued"]] == list(
            range(1, len(q["queued"]) + 1))

    def test_every_queued_experiment_declares_its_contract(self):
        for r in aor.experiment_queue()["queued"]:
            for k in ("hypothesis", "economic_rationale", "information_source",
                      "target_horizon_sessions", "pit_validity", "cost",
                      "runtime", "success_criterion", "rejection_criterion",
                      "prospective_evidence_now"):
                assert r.get(k) is not None, (r["experiment_id"], k)

    def test_nothing_runs_automatically(self):
        q = aor.experiment_queue()
        assert q["automatic_execution_allowed"] is False
        assert q["all_experiments_require_manual_approval"] is True

    def test_the_floor_is_declared_and_binding(self):
        low = aor.expected_information_value(
            {"orthogonality": 0.1, "evidence_gain": 0.1,
             "prior_plausibility": 0.1, "implementability": 0.1,
             "family_status": aor.ST_CHALLENGER})
        assert low["queued"] is False
        assert low["verdict"] == "REJECTED_EXPECTED_INFORMATION_VALUE_BELOW_FLOOR"

    def test_a_duplicate_idea_scores_below_a_novel_one(self):
        dup = aor.expected_information_value(
            {"orthogonality": 0.05, "evidence_gain": 0.9,
             "prior_plausibility": 0.9, "implementability": 0.9,
             "family_status": aor.ST_CHALLENGER})
        novel = aor.expected_information_value(
            {"orthogonality": 0.95, "evidence_gain": 0.9,
             "prior_plausibility": 0.9, "implementability": 0.9,
             "family_status": aor.ST_CHALLENGER})
        assert novel["expected_information_value"] > dup["expected_information_value"]


# --------------------------------------------------------------------------- #
# The operator read model
# --------------------------------------------------------------------------- #
class TestAlphaCapitalReadModel:
    def _stub(self):
        frontier = {
            "eligible_market_date": "2026-09-03",
            "governed_lane": {"expected_return_state": "NOT_CALIBRATED",
                              "hurdle_state": kernel.HURDLE_NOT_EVIDENCED,
                              "economic_proof": "ABSENT",
                              "eligible_destinations": []},
            "research_lane": {"forecast_state": "NOT_ACTIVATED"},
            "deployment_ladder": {"rungs": [
                {"label": "$1,000", "hurdle_clears": True, "deployed_usd": 1000.0,
                 "net_of_cost_gain_usd": 0.78, "payback": {"payback_sessions": 12.3}}],
                "n_rungs": 1, "n_rungs_clearing_hurdle": 1,
                "marginal_dollar": {"first_dollar_pays": True,
                                    "cost_share_of_marginal_gain": 0.6}},
            "redeployment_ladder": {"rungs": [], "n_rungs_clearing_hurdle": 0},
            "incumbent_opportunity_cost": {
                "against": {"zero_base": {"utility_gap_per_horizon": 0.001,
                                          "utility_gap_dollars_annualised": 1000.0,
                                          "switch_cost_dollars": 190.0,
                                          "payback": {"verdict": kernel.PAYBACK_MULTI_HORIZON,
                                                      "payback_sessions": 29.1}},
                            "implementable": {"payback": {"verdict": kernel.PAYBACK_WITHIN_ONE_HORIZON,
                                                          "payback_sessions": 13.7}}}},
            "targets": {}, "capital": {"cash": 4482.71, "cash_weight": 0.0456},
            "state": "READY"}
        registry = aor.load_alpha_opportunity_registry(
            tournament={}, r32_frontier={}, opportunity_frontier={}, scoring={})
        return frontier, registry

    def test_the_read_model_composes_without_a_live_backend(self):
        frontier, registry = self._stub()
        p = ac.load_alpha_capital(
            cash_frontier=frontier, registry=registry,
            shadow_portfolios={"leaderboard": [], "n_challengers": 0,
                               "inception_sessions": []},
            tournament={}, research_agent={}, forward_evidence={},
            capital_pool={"nav": 98361.4, "starting_nav": 100000.0,
                          "cash": 4482.71, "cash_weight": 0.045574},
            desk_performance={"current_summary": {
                "cumulative_return_pct": -1.6386,
                "benchmark_cumulative_return_pct": 3.4466}})
        assert p["state"] in ac.READ_STATE_VOCAB
        assert p["headline"]
        for k in ("capital_now", "capital_frontier", "cash_decision", "zero_base",
                  "incumbent_opportunity_cost", "alpha_registry",
                  "top_opportunities", "forward_portfolio_challengers",
                  "limiters", "scoreboard", "evidence_maturity",
                  "experiment_queue", "realised_excess_decomposition"):
            assert k in p, k

    def test_the_limiter_ranking_puts_the_measured_loss_first(self):
        frontier, registry = self._stub()
        p = ac.load_alpha_capital(
            cash_frontier=frontier, registry=registry,
            shadow_portfolios={}, tournament={}, research_agent={},
            forward_evidence={},
            capital_pool={"nav": 98361.4, "starting_nav": 100000.0,
                          "cash": 4482.71, "cash_weight": 0.045574},
            desk_performance={"current_summary": {
                "cumulative_return_pct": -1.6386,
                "benchmark_cumulative_return_pct": 3.4466}})
        lim = p["limiters"]
        assert lim["primary_limiter"] == ac.LIM_SIGNAL
        assert lim["limiters"][0]["impact_pct_points"] < 0
        for r in lim["limiters"]:
            assert r["severity"] in ac.SEVERITY_VOCAB
            assert r["measured_by"]
            assert r["what_would_move_it"]

    def test_the_uncalibrated_forecast_is_a_binding_limiter(self):
        frontier, registry = self._stub()
        p = ac.load_alpha_capital(
            cash_frontier=frontier, registry=registry, shadow_portfolios={},
            tournament={}, research_agent={}, forward_evidence={},
            capital_pool={"cash_weight": 0.04, "starting_nav": 100000.0},
            desk_performance={})
        rows = {r["limiter"]: r for r in p["limiters"]["limiters"]}
        assert rows[ac.LIM_CALIBRATION]["severity"] == ac.SEV_BINDING

    def test_the_scoreboard_carries_a_window_on_every_row(self):
        sb = ac.build_scoreboard(
            rolling={"since_inception": {"return_pct": -1.5, "spy_return_pct": 3.4,
                                         "n_daily_returns": 31}},
            capital={"nav": 98361.4, "starting_nav": 100000.0},
            tournament={"shadow_pnl": {"inception": "2026-08-26"},
                        "economic_truth": {}},
            shadow_portfolios={"leaderboard": [
                {"challenger_id": "x", "label": "X", "sessions_scored": 0}],
                "inception_sessions": ["2026-09-03"]})
        assert sb["rows"]
        for r in sb["rows"]:
            assert r.get("window"), r
        assert sb["promotion_allowed"] is False

    def test_cash_is_on_the_scoreboard_as_a_competitor(self):
        sb = ac.build_scoreboard(
            rolling={"since_inception": {"spy_return_pct": 3.4466,
                                         "n_daily_returns": 31}},
            capital={}, tournament={}, shadow_portfolios={})
        cash = [r for r in sb["rows"] if r["entity"] == "CASH"][0]
        assert cash["return_pct"] == 0.0
        assert cash["excess_pct_points"] == pytest.approx(-3.4466)

    def test_safety_block_forbids_everything_operational(self):
        frontier, registry = self._stub()
        p = ac.load_alpha_capital(
            cash_frontier=frontier, registry=registry, shadow_portfolios={},
            tournament={}, research_agent={}, forward_evidence={},
            capital_pool={}, desk_performance={})
        s = p["safety"]
        for k in ("creates_signals", "creates_trade_decisions", "creates_orders",
                  "creates_fills", "creates_proposal", "mutates_holdings",
                  "mutates_cash", "promotes_model", "activates_sleeve",
                  "enables_automation", "broker_enabled",
                  "writes_operational_store", "automatic_promotion_allowed"):
            assert s[k] is False, k
        assert s["proposal_owner"] == "engine.reallocation_proposal"
        assert s["decision_owner"] == "api.portfolio_decision"


# --------------------------------------------------------------------------- #
# Ownership boundaries - no second owner, no operational reach
# --------------------------------------------------------------------------- #
R56_MODULES = ("engine/alpha_capital_frontier.py",
               "engine/shadow_portfolio_evidence.py",
               "api/cash_deployment_frontier.py",
               "api/alpha_opportunity_registry.py",
               "api/shadow_portfolio_evidence.py",
               "api/alpha_capital.py")


class TestOwnershipBoundaries:
    def _src(self, rel):
        return (REPO / rel).read_text(encoding="utf-8")

    def test_no_r56_module_defines_a_second_optimiser_or_covariance(self):
        banned = re.compile(r"def\s+(optimise|optimize|build_covariance|"
                            r"build_allocation|name_caps)\s*\(")
        for rel in R56_MODULES:
            assert not banned.search(self._src(rel)), rel

    def test_the_ladder_imports_the_one_objective_owner(self):
        src = self._src("engine/alpha_capital_frontier.py")
        assert "from . import zero_base_allocator as zba" in src
        assert "zba.portfolio_economics" in src
        assert "zba.transition_economics" in src
        assert kernel.OBJECTIVE_OWNER == zba.CALCULATION_OWNER

    def test_no_r56_module_writes_an_operational_store(self):
        for rel in R56_MODULES:
            src = self._src(rel)
            for term in ("paper_trading_desk._append_ledger", "_append_ledger(",
                         "confirm_order", "settle_", "create_order"):
                assert term not in src, (rel, term)

    def test_no_r56_module_contains_an_execution_call(self):
        banned = ("place_order", "submit_order", "execute_order", "send_order",
                  "broker_execute", "route_order")
        for rel in R56_MODULES:
            src = self._src(rel)
            for term in banned:
                assert not re.search(r"\b%s\s*\(" % term, src), (rel, term)

    def test_only_the_shadow_owner_writes_and_only_to_its_research_root(self):
        writers = [rel for rel in R56_MODULES
                   if "write_text(" in self._src(rel)]
        assert writers == ["api/shadow_portfolio_evidence.py"]
        src = self._src("api/shadow_portfolio_evidence.py")
        assert "r56_shadow_portfolios" in src
        assert spe.SHADOW_DIR_ENV == "PAPER_TRADER_R56_SHADOW_DIR"

    def test_multi_asset_research_never_becomes_an_operational_sleeve(self):
        for rel in R56_MODULES:
            src = self._src(rel)
            assert "activate_sleeve" not in src, rel
            assert "capital_eligible_sleeve_ids\"] =" not in src, rel
        reg = aor.load_alpha_opportunity_registry(
            tournament={}, r32_frontier={}, opportunity_frontier={}, scoring={})
        assert reg["safety"]["activates_sleeve"] is False
        for f in reg["families"]:
            assert f["promotion_allowed"] is False

    def test_a_research_family_cannot_claim_capital_eligibility_alone(self):
        """Capital eligibility comes from the investability registry. With NO
        frontier injected, no family may claim it."""
        reg = aor.load_alpha_opportunity_registry(
            tournament={}, r32_frontier={}, opportunity_frontier={}, scoring={})
        assert reg["capital_eligible_asset_classes"] == []
        assert all(f["capital_eligible_today"] is False for f in reg["families"])


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
class TestRoutes:
    def test_the_four_read_only_routes_are_declared_as_get(self):
        src = (REPO / "api" / "app.py").read_text(encoding="utf-8")
        for path in ("/v1/operations/cash-deployment-frontier",
                     "/v1/operations/alpha-capital",
                     "/v1/research/alpha-opportunity-registry",
                     "/v1/research/shadow-portfolio-evidence"):
            assert '@app.get(\n    "%s"' % path in src, path
            assert '@app.post(\n    "%s"' % path not in src, path

    def test_no_r56_route_is_declared_twice(self):
        src = (REPO / "api" / "app.py").read_text(encoding="utf-8")
        for path in ("/v1/operations/cash-deployment-frontier",
                     "/v1/operations/alpha-capital",
                     "/v1/research/alpha-opportunity-registry",
                     "/v1/research/shadow-portfolio-evidence"):
            assert src.count('"%s"' % path) == 1, path


# --------------------------------------------------------------------------- #
# UI acceptance - the mandatory redesign checklist
# --------------------------------------------------------------------------- #
class TestUiAcceptance:
    @pytest.fixture(scope="class")
    def ui(self):
        return UI.read_text(encoding="utf-8", errors="replace")

    @pytest.fixture(scope="class")
    def view(self, ui):
        # The view carries its OWN end marker. Every adjacent tab pair in this
        # file is already a slice boundary for some other acceptance suite, so a
        # view that relies on "whatever tab comes next" both breaks that suite
        # and breaks itself the next time a tab moves.
        start = ui.index('<div id="tab-alpha-capital"')
        end = ui.index("<!-- end tab-alpha-capital -->")
        return ui[start:end]

    def test_the_view_sits_outside_every_other_tab_slice(self, ui):
        """It lives AFTER the audit tab's end marker, so no other suite's text
        region can accidentally contain it."""
        assert (ui.index('<div id="tab-alpha-capital"')
                > ui.index("<!-- end tab-audit-advanced -->"))
        assert (ui.index("<!-- end tab-alpha-capital -->")
                < ui.index("<!-- end main-content-area -->"))

    def test_the_view_exists_and_is_routed(self, ui):
        assert 'id="tab-alpha-capital"' in ui
        assert "'alpha-capital': 'alpha-capital'" in ui
        assert 'data-route="alpha-capital"' in ui
        assert "navigateToRoute('alpha-capital')" in ui

    def test_the_sidebar_carries_the_entry(self, ui):
        assert 'id="nav-alpha-capital"' in ui
        assert "Alpha &amp; Capital</a>" in ui

    def test_the_layout_is_a_grid_not_a_vertical_stack(self, view):
        assert view.count("grid-template-columns") >= 3
        assert "minmax(0,1fr) 340px" in view

    def test_six_kpi_cards_each_carry_a_real_number(self, ui):
        assert 'id="ak-kpis"' in ui
        block = ui[ui.index("function _akKpis("):ui.index("function _akCash(")]
        assert block.count("{ l: '") == 6
        for token in ("cap.nav", "cap.cash", "book.return_pct",
                      "zb.utility_gap_dollars_annualised",
                      "payback_sessions", "fst.forward_confirmed"):
            assert token in block, token

    def test_every_mandatory_safety_badge_is_present(self, view):
        for badge in ("PREVIEW ONLY", "READ ONLY", "PAPER ONLY", "NO ORDERS",
                      "AUTOMATION OFF", "MANUAL REVIEW",
                      "NO MODEL PROMOTION", "NO LIVE BROKER ORDERS"):
            assert badge in view, badge

    def test_the_platform_orders_disabled_wording_is_never_shown(self, view):
        """Phase 27B.6: paper orders are REAL in this system and exist in the
        operational book under a governed manual workflow. Only live brokerage
        orders are structurally disabled, so the platform badge would be a
        false statement on a Paper Trader surface."""
        assert ">ORDERS DISABLED<" not in view
        from paper_trader.api import alpha_capital as _ac
        assert "ORDERS DISABLED" not in _ac.SAFETY_BADGES
        assert "NO LIVE BROKER ORDERS" in _ac.SAFETY_BADGES

    def test_diagnostics_live_in_audit_advanced(self, view):
        assert "<details" in view
        assert "Audit / Advanced" in view
        assert 'id="ak-audit-body"' in view
        assert view.index('id="ak-kpis"') < view.index("<details")

    def test_no_alert_and_no_confirm(self, view):
        assert "alert(" not in view
        assert "confirm(" not in view

    def test_no_create_orders_and_no_automation_control(self, view):
        low = view.lower()
        assert "create order" not in low
        assert "create orders" not in low
        assert "enable automation" not in low

    def test_no_blank_button(self, view):
        for m in re.finditer(r"<button\b[^>]*>(.*?)</button>", view, re.S):
            label = re.sub(r"<[^>]+>", "", m.group(1)).replace("&hellip;", "")
            assert label.strip(), m.group(0)[:120]

    def test_no_connect_to_load_placeholder(self, view):
        assert "Connect to Load" not in view
        assert "Connect to load" not in view

    def test_the_view_issues_gets_only(self, ui):
        block = ui[ui.index("async function loadAlphaCapital("):
                   ui.index("window.loadAlphaCapital = loadAlphaCapital;")]
        assert "method: 'POST'" not in block
        assert "_mhzGet(" in block
        for path in ("/v1/operations/alpha-capital",
                     "/v1/operations/cash-deployment-frontier",
                     "/v1/research/alpha-opportunity-registry",
                     "/v1/research/shadow-portfolio-evidence"):
            assert path in block, path

    def test_no_r56_route_is_an_orphan_endpoint(self, ui):
        """Every route this release declares is reachable from the view, so the
        audit's orphan-endpoint report stays a real signal."""
        for path in ("/v1/operations/alpha-capital",
                     "/v1/operations/cash-deployment-frontier",
                     "/v1/research/alpha-opportunity-registry",
                     "/v1/research/shadow-portfolio-evidence"):
            assert "'%s'" % path in ui, path

    def test_the_loader_is_lazily_wired_into_the_tab_switch(self, ui):
        assert "if (tabName === 'alpha-capital' && !window._akLoaded)" in ui
        assert "loadAlphaCapital();" in ui

    def test_no_stashed_payload_shadows_a_renderer(self, ui):
        """A top-level `function _akFoo(){}` IS `window._akFoo` in a browser.

        Storing a payload under a name a renderer already occupies replaces the
        function with a plain object, and the next call throws. This is caught
        statically because the failure only appears at runtime, on the second
        render, in a browser nobody is watching."""
        block = ui[ui.index("/* ======================= RELEASE 56"):
                   ui.index("window.loadAlphaCapital = loadAlphaCapital;")]
        funcs = set(re.findall(r"^function (_ak\w+)\s*\(", block, re.M))
        stashed = set(re.findall(r"window\.(_ak\w+)\s*=", block))
        assert funcs, "no renderers found - the slice is wrong"
        assert not (funcs & stashed), sorted(funcs & stashed)


# --------------------------------------------------------------------------- #
# Documentation and inventory
# --------------------------------------------------------------------------- #
class TestDocs:
    def test_the_release_document_exists_and_carries_the_wireframe(self):
        doc = REPO / "docs" / "RELEASE56_ALPHA_TO_CAPITAL_OFFENSIVE.md"
        assert doc.exists()
        text = doc.read_text(encoding="utf-8")
        assert "3.3 PLAN — the wireframe" in text or "wireframe" in text
        assert "1920" in text
        for section in ("SCAN", "REVIEW", "PLAN", "EXECUTE PREVIEW"):
            assert section in text, section

    def test_every_new_module_is_in_the_system_inventory(self):
        inv = json.loads((REPO / "docs" / "architecture" /
                          "system_inventory.json").read_text(encoding="utf-8"))
        listed = {m.get("path", "").replace("\\", "/") for m in inv.get("modules", [])}
        for rel in R56_MODULES:
            assert rel in listed, rel
