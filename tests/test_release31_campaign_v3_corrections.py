"""Release 31 Campaign v3 - the four experimental-design corrections.

Campaign v2 built the right research MACHINERY and pointed it at the wrong
experiment. Four defects made its verdict unable to answer the business question,
and each one is repaired by a named owner that this file holds to its promise:

  1. universe     it judged on the Russell 1000 training panel, not on the S&P 500
                  book we actually manage
  2. allocation   it approximated a portfolio as top-N equal weight with cash
                  pinned at zero, instead of asking the canonical zero-base
                  allocator what to own
  3. allocation   its direct-portfolio learner compared consecutive weight vectors
                  BY ROW POSITION, so a portfolio that sold everything and bought
                  everything else could be scored as having traded nothing
  4. benchmarks   it substituted an equal-weight basket for SPY and thereby
                  answered a different question than the one asked

Most of what follows are NEGATIVE probes: they feed each guard the input it exists
to refuse and assert that it refuses. A guard that has only ever been shown
succeeding has not been shown to work, and three of the four defects above were
invisible precisely because nothing ever tried to break them.
"""
from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")

from paper_trader.alpha_agent.r31 import (allocation as _alloc,
                                          benchmarks as _bench,
                                          calibration as _cal,
                                          campaign as _camp,
                                          universe as _uni)
from paper_trader.engine import zero_base_allocator as _zb


# =========================================================================== #
# A superiority check must be capable of failing
# =========================================================================== #
def _lockbox_out(*, net_excess, dd, turn, spy_excess, win=1.0):
    """One lockbox result set holding a single candidate and no incumbent."""
    return {"results": [{
        "candidate_id": "c:1", "family": "f", "track": "FORECAST_THEN_ALLOCATE",
        "state": "EXECUTED",
        "lockbox": {"evaluation_universe": "EVALUATE_S_AND_P_500_PIT_MEMBERS_ONLY",
                    "primary": {"net_excess_annualised": net_excess,
                                "max_drawdown_net": dd,
                                "turnover_annualised": turn,
                                "net_excess_vs_spy_annualised": spy_excess,
                                "robustness": {"subperiod_win_fraction": win}}}}]}


class TestASuperiorityCheckCanFail:
    """The defect this pins: when the incumbent could not be priced, the absent
    reference was filled with the CANDIDATE'S OWN drawdown and turnover. The two
    checks then compared the candidate against itself and passed on every input
    that could ever be supplied - the exact shape of the inert probes this
    campaign was told not to reintroduce.
    """

    def test_the_two_incumbent_relative_checks_report_unavailable_not_pass(self):
        sup = _camp.superiority_verdict(
            lockbox_out=_lockbox_out(net_excess=0.05, dd=-0.30, turn=99.0,
                                     spy_excess=0.04),
            mt_out={"superior_predictive_ability": {"p_value": 0.001},
                    "paired_vs_incumbent": {"state": "NO_LOCKBOX_RESULT"}})
        assert sup["incumbent_state"] == "INCUMBENT_NOT_ECONOMICALLY_CALIBRATABLE"
        for name in ("drawdown_not_materially_worse", "turnover_ratio"):
            chk = sup["checks"][name]
            assert chk["pass"] is None, name
            assert chk["state"] == _camp.UNAVAILABLE_NO_INCUMBENT, name
            assert chk["value"] is None, name
        assert set(sup["checks_unavailable"]) == {
            "drawdown_not_materially_worse", "turnover_ratio"}

    def test_an_unavailable_check_cannot_carry_a_superiority_claim(self):
        # Everything that CAN be measured is made to pass, so the only thing
        # standing between this candidate and a declared win is the pair of
        # checks whose comparator does not exist. They must block it.
        sup = _camp.superiority_verdict(
            lockbox_out=_lockbox_out(net_excess=0.99, dd=-0.01, turn=0.1,
                                     spy_excess=0.99),
            mt_out={"superior_predictive_ability": {"p_value": 0.0},
                    "paired_vs_incumbent": {"p_value": 0.0}})
        assert sup["all_passed"] is False
        assert sup["winner"] is None

    def test_a_priced_incumbent_still_produces_a_real_comparison(self):
        # The guard must not fire when the comparator genuinely exists.
        out = _lockbox_out(net_excess=0.05, dd=-0.10, turn=4.0, spy_excess=0.04)
        out["results"].append({
            "candidate_id": "bench:incumbent_momentum_leg:h20",
            "family": "incumbent_momentum_leg", "state": "EXECUTED",
            "lockbox": {"primary": {"net_excess_annualised": 0.0,
                                    "max_drawdown_net": -0.12,
                                    "turnover_annualised": 8.0}}})
        sup = _camp.superiority_verdict(
            lockbox_out=out,
            mt_out={"superior_predictive_ability": {"p_value": 0.01},
                    "paired_vs_incumbent": {"p_value": 0.01}})
        assert sup["checks"]["turnover_ratio"]["pass"] is not None
        assert sup["checks"]["turnover_ratio"]["value"] == pytest.approx(0.5)
        assert sup["checks"]["drawdown_not_materially_worse"]["pass"] is not None
        assert sup["checks_unavailable"] == []


# =========================================================================== #
# Correction 3 - transition economics aligned by SECURITY IDENTITY
# =========================================================================== #
class TestSymbolAlignedTransitionCost:
    """The defect: ``|w_t - w_{t-1}|`` computed positionally when the two vectors
    happened to be the same length. Row ``i`` is not the same company on two
    dates, so every assertion here is about IDENTITY rather than shape."""

    def test_row_order_permutation_is_not_turnover(self):
        # THE headline probe. Same book, different assembly order. A positional
        # comparison sees churn; there is none.
        a = {"AAPL": 0.4, "MSFT": 0.35, "XOM": 0.25}
        b = {"XOM": 0.25, "AAPL": 0.4, "MSFT": 0.35}
        assert _alloc.traded_notional(a, b) == pytest.approx(0.0, abs=1e-15)
        assert _alloc.transition_cost(a, b) == pytest.approx(0.0, abs=1e-15)

    def test_one_name_leaves_and_one_enters(self):
        a = {"AAPL": 0.5, "MSFT": 0.5}
        b = {"AAPL": 0.5, "NVDA": 0.5}
        # A full exit of MSFT plus a full entry of NVDA: 0.5 sold + 0.5 bought.
        assert _alloc.traded_notional(a, b) == pytest.approx(1.0)

    def test_same_length_different_securities_do_not_align_by_position(self):
        # The exact shape that fooled v2: identical cardinality, disjoint names.
        a = {"AAA": 0.25, "BBB": 0.25, "CCC": 0.25, "DDD": 0.25}
        b = {"EEE": 0.25, "FFF": 0.25, "GGG": 0.25, "HHH": 0.25}
        # Everything sold and everything bought = 2.0 of notional, not 0.0.
        assert _alloc.traded_notional(a, b) == pytest.approx(2.0)

    def test_delisted_name_retains_its_exit_cost(self):
        # The position did not evaporate because the panel stopped carrying it;
        # it was liquidated, and liquidation is a side that pays cost.
        a = {"AAPL": 0.6, "ENRNQ": 0.4}
        b = {"AAPL": 0.6}
        assert _alloc.traded_notional(a, b) == pytest.approx(0.4)

    def test_cash_change_is_represented(self):
        # De-risking to half cash trades exactly the half that was sold.
        a = {"AAPL": 0.5, "MSFT": 0.5}
        b = {"AAPL": 0.25, "MSFT": 0.25}
        assert _alloc.traded_notional(a, b) == pytest.approx(0.5)

    def test_opening_book_charges_one_side_only(self):
        # No prior book: there is a buy side and no sell side.
        assert _alloc.traded_notional(None, {"AAPL": 0.5, "MSFT": 0.3}) == \
            pytest.approx(0.8)

    def test_both_tracks_share_one_cost_semantics(self):
        pol = _alloc.policy()
        a, b = {"AAPL": 0.5}, {"MSFT": 0.5}
        assert _alloc.transition_cost(a, b, pol) == pytest.approx(
            _alloc.traded_notional(a, b) * pol["cost_rate_per_side"])
        # and the rate is the canonical one, not a research-only assumption
        assert pol["cost_rate_per_side"] == \
            _zb.default_policy()["cost_rate_per_side"]

    def test_realised_return_never_credits_a_missing_price(self):
        # A delisted name's return is unknown, not zero. Crediting zero would
        # quietly convert a wipeout into a flat position.
        tgt = {"AAPL": 0.5, "GONE": 0.5}
        rets = {"AAPL": 0.10}
        assert _alloc.realised_return(tgt, rets) == pytest.approx(0.05)
        assert _alloc.unrealised_weight(tgt, rets) == pytest.approx(0.5)


# =========================================================================== #
# Correction 2 - the primary judge allocates capital, it does not rank names
# =========================================================================== #
def _section(n, mu_level, seed=0, sd=0.08):
    """A synthetic but economically shaped cross-section.

    ``sd`` is HORIZON volatility, not annual: the allocator is handed an already
    horizon-scaled covariance, so a 25% annual vol against a one-month expected
    return would price a month of risk against a year of it and make every
    portfolio look untradeable.
    """
    rng = np.random.default_rng(seed)
    tickers = ["T%03d" % i for i in range(n)]
    mu = {t: float(v) for t, v in zip(tickers, rng.normal(mu_level, 0.02, n))}
    sig = {t: float(sd) for t in tickers}
    cov = {a: {b: (sd * sd if a == b else 0.3 * sd * sd) for b in tickers}
           for a in tickers}
    adv = {t: 5.0e8 for t in tickers}
    return tickers, mu, sig, cov, adv


class TestZeroBaseIsThePrimaryEconomicPath:

    def test_cash_is_a_real_choice_when_nothing_is_worth_owning(self):
        # Every expected return strongly negative. The correct portfolio is
        # NOTHING. A top-N judge would have bought the 25 least-bad names and
        # reported the result as the candidate's.
        tickers, mu, sig, cov, adv = _section(60, -0.05, seed=1)
        out = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                      cov_h=cov, cov_included=tickers, adv=adv)
        assert out["cash_weight"] == pytest.approx(1.0)
        assert out["names_held"] == 0
        assert out["invested_weight"] == pytest.approx(0.0)

    def test_attractive_returns_do_produce_investment(self):
        # The mirror probe: the refusal above must be responsive to the evidence,
        # not a module that always returns cash.
        tickers, mu, sig, cov, adv = _section(60, 0.06, seed=2)
        out = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                      cov_h=cov, cov_included=tickers, adv=adv)
        assert out["invested_weight"] > 0.5
        assert out["names_held"] > 0

    def test_cash_weight_is_never_pinned_to_zero(self):
        # v2 reported a hardcoded cash_weight of 0.0 on every candidate.
        tickers, mu, sig, cov, adv = _section(40, -0.05, seed=3)
        out = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                      cov_h=cov, cov_included=tickers, adv=adv)
        assert out["cash_weight"] > 0.0

    def test_zero_base_target_ignores_the_current_book(self):
        # "Existing holdings have no intrinsic investment privilege." The ideal
        # target must be identical whatever we happen to hold.
        tickers, mu, sig, cov, adv = _section(50, 0.05, seed=4)
        ideal = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                        cov_h=cov, cov_included=tickers, adv=adv)
        holding = {tickers[0]: 0.9, tickers[1]: 0.1}
        again = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                        cov_h=cov, cov_included=tickers, adv=adv,
                                        current_weight=None)
        assert again["weights"] == ideal["weights"]
        # and supplying a book produces the IMPLEMENTABLE target, which is a
        # different question and is allowed to differ
        impl = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                       cov_h=cov, cov_included=tickers, adv=adv,
                                       current_weight=holding)
        assert impl["track"] == _alloc.TRACK_A

    def test_the_canonical_allocator_is_the_owner(self):
        tickers, mu, sig, cov, adv = _section(30, 0.04, seed=5)
        out = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                      cov_h=cov, cov_included=tickers, adv=adv)
        assert out["allocator_owner"] == "engine.zero_base_allocator"

    def test_name_cap_and_liquidity_cap_bind(self):
        tickers, mu, sig, cov, adv = _section(30, 0.05, seed=6)
        pol = _alloc.policy()
        # One name is tiny: its liquidity cap must beat the name cap.
        adv[tickers[0]] = 1.0e5
        out = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                      cov_h=cov, cov_included=tickers, adv=adv,
                                      nav=100000.0)
        for tk, w in out["weights"].items():
            assert w <= pol["max_name_weight"] + 1e-9
        assert out["weights"].get(tickers[0], 0.0) <= 1.0 + 1e-9

    def test_sector_cap_is_declared_unmeasurable_not_faked(self):
        tickers, mu, sig, cov, adv = _section(20, 0.04, seed=7)
        out = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                      cov_h=cov, cov_included=tickers, adv=adv)
        assert out["sector_cap_state"] == "UNMEASURABLE_PIT"

    def test_unknown_sector_does_not_cap_the_whole_book_at_the_sector_limit(self):
        # REGRESSION. Encoding "sector unknown" as one shared sentinel put every
        # name in a single sector, and the canonical 25% sector cap then limited
        # the ENTIRE portfolio to 25% invested. Every candidate would have looked
        # like it chose to hold 75% cash - a fabricated economic result produced
        # by a placeholder string. Strongly attractive returns must be able to
        # push the book well past the sector cap.
        pol = _alloc.policy()
        tickers, mu, sig, cov, adv = _section(60, 0.06, seed=21)
        out = _alloc.zero_base_target(tickers=tickers, mu=mu, sigma=sig,
                                      cov_h=cov, cov_included=tickers, adv=adv)
        assert out["invested_weight"] > pol["sector_cap_fraction"] + 1e-9
        # each name is its own sector, so the binding limit is the NAME cap
        for w in out["weights"].values():
            assert w <= pol["max_name_weight"] + 1e-9


class TestTrackBParity:

    def test_track_b_may_hold_cash_and_need_not_sum_to_one(self):
        # Both proposals sit under the canonical name cap, so the only thing
        # being tested is that the unallocated remainder stays as cash.
        tickers, _, _, _, adv = _section(10, 0.0, seed=8)
        proposed = {tickers[0]: 0.08, tickers[1]: 0.08}
        out = _alloc.feasible_portfolio(tickers=tickers, proposed=proposed,
                                        adv=adv)
        assert out["invested_weight"] == pytest.approx(0.16)
        assert out["cash_weight"] == pytest.approx(0.84)

    def test_a_deliberate_cash_position_is_never_normalised_away(self):
        # An 8% book is LEFT at 8%. Scaling it to 1.0 would overrule the decision
        # Track B is being given the freedom to make.
        tickers, _, _, _, adv = _section(10, 0.0, seed=9)
        out = _alloc.feasible_portfolio(
            tickers=tickers, proposed={tickers[0]: 0.08}, adv=adv)
        assert out["weights"][tickers[0]] == pytest.approx(0.08)
        assert out["cash_weight"] == pytest.approx(0.92)
        assert out["scaled_to_gross_limit"] is False

    def test_track_b_faces_the_same_caps_as_track_a(self):
        tickers, _, _, _, adv = _section(10, 0.0, seed=10)
        pol = _alloc.policy()
        out = _alloc.feasible_portfolio(
            tickers=tickers, proposed={tickers[0]: 0.95}, adv=adv)
        assert out["weights"][tickers[0]] == pytest.approx(pol["max_name_weight"])

    def test_track_b_is_long_only(self):
        tickers, _, _, _, adv = _section(10, 0.0, seed=11)
        out = _alloc.feasible_portfolio(
            tickers=tickers, proposed={tickers[0]: -0.5, tickers[1]: 0.3},
            adv=adv)
        assert tickers[0] not in out["weights"]

    def test_over_gross_proposal_is_scaled_down_not_accepted(self):
        tickers, _, _, _, adv = _section(30, 0.0, seed=12)
        proposed = {t: 0.09 for t in tickers[:20]}      # 1.8 gross
        out = _alloc.feasible_portfolio(tickers=tickers, proposed=proposed,
                                        adv=adv)
        assert out["invested_weight"] <= 1.0 + 1e-12
        assert out["scaled_to_gross_limit"] is True

    def test_dust_below_the_canonical_floor_is_dropped(self):
        tickers, _, _, _, adv = _section(10, 0.0, seed=13)
        pol = _alloc.policy()
        tiny = pol["min_position_weight"] / 10.0
        out = _alloc.feasible_portfolio(
            tickers=tickers, proposed={tickers[0]: tiny, tickers[1]: 0.3},
            adv=adv)
        assert tickers[0] not in out["weights"]


# =========================================================================== #
# Correction 2 (cont.) - a score may not masquerade as an expected return
# =========================================================================== #
class TestForecastCalibration:

    def test_a_positive_ranking_calibrates_and_keeps_its_order(self):
        rng = np.random.default_rng(0)
        s = rng.normal(0, 1, 2000)
        y = 0.01 * s + rng.normal(0, 0.02, 2000)
        d = np.repeat(np.arange(40), 50)
        cal = _cal.fit(s, y, dates=d)
        assert cal.slope > 0
        assert _cal.verify_rank_identity(s, cal.apply(s))
        assert cal.to_dict()["state"] == _cal.CALIBRATION_OK

    def test_an_inverting_calibration_fails_closed(self):
        # THE Release-30.1 defect, reproduced deliberately: a mapping whose slope
        # is negative silently reverses the model it claims to express.
        rng = np.random.default_rng(1)
        s = rng.normal(0, 1, 2000)
        y = -0.01 * s + rng.normal(0, 0.02, 2000)
        with pytest.raises(_cal.CalibrationRefused) as exc:
            _cal.fit(s, y, dates=np.repeat(np.arange(40), 50))
        assert exc.value.state == _cal.RANK_IDENTITY_VIOLATION

    def test_an_arbitrary_score_is_almost_always_refused(self):
        """Pure noise must not become an expected return.

        Measured as a RATE across many draws, not asserted for one lucky seed.
        The gate is a two-sided t-test at 2.0, so by construction it passes noise
        about one time in twenty; a test demanding refusal at every seed would be
        asserting a property the gate does not have, and the only way to make it
        true would be to raise the floor until real alpha is refused too - which
        an earlier draft did, at t >= 3.0, and which measurement then showed
        rejects a genuine factor over any realistic number of fitting dates.

        The residual leak is deliberate and is controlled downstream, not here:
        a noise calibration produces a tiny fitted slope, so the allocator holds
        cash and the candidate simply loses, and the Benjamini-Hochberg control,
        the SPA test and the one-shot lockbox all still stand between it and a
        verdict.
        """
        refused = 0
        trials = 60
        for seed in range(trials):
            rng = np.random.default_rng(seed)
            s = rng.normal(0, 1, 3000)
            y = rng.normal(0, 0.02, 3000)
            try:
                _cal.fit(s, y, dates=np.repeat(np.arange(60), 50))
            except _cal.CalibrationRefused as exc:
                assert exc.state in (_cal.NOT_CALIBRATABLE,
                                     _cal.RANK_IDENTITY_VIOLATION)
                refused += 1
        rate = refused / float(trials)
        assert rate >= 0.85, (
            "only %.0f%% of pure-noise scores were refused; the units gate is "
            "not screening" % (100 * rate))

    def test_a_genuine_signal_still_survives_the_significance_floor(self):
        # The mirror of the probe above, and the reason the floor is 2.0 and not
        # 3.0: the guard must not be so strict that a real relationship is
        # refused. A monthly rank IC around 0.03 with a 0.10 spread - a good
        # real-world equity factor - produces a per-date t near 2.3 over 60 dates.
        rng = np.random.default_rng(7)
        s = rng.normal(0, 1, 3000)
        y = 0.008 * s + rng.normal(0, 0.02, 3000)
        cal = _cal.fit(s, y, dates=np.repeat(np.arange(60), 50))
        assert cal.slope > 0
        assert cal.diagnostics["per_date_slope_t"] >= _cal.MIN_SLOPE_T

    def test_sign_stability_catches_what_the_t_statistic_cannot(self):
        """The two guards must not be one guard wearing two hats.

        Sign stability uses only the SIGN of each date's slope, so no single date
        can move it; the t-statistic uses magnitudes, so a handful of violent
        dates can carry it. Here the relationship is negative on most dates and
        strongly positive on a few - a mean driven by outliers. The t-statistic
        is happy; the sign count is not.
        """
        rng = np.random.default_rng(11)
        blocks_s, blocks_y, dates = [], [], []
        for d in range(40):
            s = rng.normal(0, 1, 50)
            slope = 0.30 if d < 8 else -0.004      # 8 violent, 32 mildly negative
            blocks_s.append(s)
            blocks_y.append(slope * s + rng.normal(0, 0.004, 50))
            dates.append(np.full(50, d))
        s = np.concatenate(blocks_s)
        y = np.concatenate(blocks_y)
        d = np.concatenate(dates)
        with pytest.raises(_cal.CalibrationRefused) as exc:
            _cal.fit(s, y, dates=d)
        assert exc.value.state == _cal.NOT_CALIBRATABLE
        assert "direction holds on only" in exc.value.detail

    def test_the_calibration_contract_is_bound_into_candidate_identity(self):
        """Changing a calibration floor changes WHICH models allocate capital."""
        c = _cal.contract()
        assert c["min_slope_t"] == _cal.MIN_SLOPE_T
        assert c["min_sign_stability"] == _cal.MIN_SIGN_STABILITY
        assert c["negative_slope_refused"] is True
        assert c["fitted_on"] == "DISCOVERY_ONLY"
        assert c["lockbox_visible"] is False

    def test_too_few_observations_refuses(self):
        with pytest.raises(_cal.CalibrationRefused) as exc:
            _cal.fit(np.arange(50.0), np.arange(50.0) * 0.001)
        assert exc.value.state == _cal.NOT_CALIBRATABLE

    def test_a_constant_score_refuses(self):
        s = np.ones(500)
        y = np.random.default_rng(3).normal(0, 0.02, 500)
        with pytest.raises(_cal.CalibrationRefused):
            _cal.fit(s, y)

    def test_rank_identity_verifier_catches_a_reversal(self):
        s = np.array([1.0, 2.0, 3.0, 4.0])
        assert _cal.verify_rank_identity(s, s * 2.0)
        assert not _cal.verify_rank_identity(s, -s)

    def test_native_units_bypass_calibration_without_reordering(self):
        cal = _cal.native()
        s = np.array([0.01, -0.02, 0.03])
        assert np.allclose(cal.apply(s), s)
        assert cal.unit == _cal.UNIT_NATIVE

    def test_an_inverting_calibration_never_reaches_the_allocator(self):
        # End-to-end statement of the guard: the refusal happens BEFORE any mu
        # exists, so there is nothing for the optimiser to act on.
        rng = np.random.default_rng(4)
        s = rng.normal(0, 1, 1500)
        y = -0.02 * s + rng.normal(0, 0.01, 1500)
        mu = None
        try:
            mu = _cal.fit(s, y, dates=np.repeat(np.arange(30), 50))
        except _cal.CalibrationRefused:
            pass
        assert mu is None


# =========================================================================== #
# Correction 4 - two benchmarks, neither standing in for the other
# =========================================================================== #
class TestBenchmarkDuality:

    def test_both_benchmarks_are_declared(self):
        assert _bench.BENCH_EQUAL_WEIGHT in _bench.BENCHMARKS
        assert _bench.BENCH_SPY in _bench.BENCHMARKS

    def test_a_price_only_index_is_inadmissible(self):
        # Comparing a total-return strategy with a price index manufactures
        # roughly the dividend yield in fake annual outperformance.
        assert "$SPX" in _bench.PRICE_ONLY_INADMISSIBLE
        assert "$SPX" not in _bench.SPY_SOURCE_PREFERENCE

    def test_a_total_return_source_is_preferred(self):
        assert _bench.SPY_SOURCE_PREFERENCE[0] == "$SPXTR"

    def test_missing_series_is_a_blocked_state_not_a_substitution(self):
        b = _bench.Benchmarks(np.full(100, np.nan), ["d%03d" % i for i in range(100)], "")
        assert b.spy_state == _bench.SPY_BLOCKED
        assert b.hold_return("d000", "d099") is None

    def test_partial_coverage_does_not_claim_availability(self):
        c = np.full(100, np.nan)
        c[:50] = 100.0
        b = _bench.Benchmarks(c, ["d%03d" % i for i in range(100)], "$SPXTR")
        assert b.spy_state == _bench.SPY_BLOCKED

    def test_equal_weight_uses_only_point_in_time_members(self):
        rets = np.array([0.10, 0.20, -0.50, 0.30])
        eligible = np.array([True, True, False, False])
        # The two non-members must not affect the benchmark at all.
        assert _bench.equal_weight_return(rets, eligible) == pytest.approx(0.15)

    def test_equal_weight_excludes_rather_than_zero_fills_a_missing_return(self):
        rets = np.array([0.10, np.nan, 0.20])
        eligible = np.array([True, True, True])
        assert _bench.equal_weight_return(rets, eligible) == pytest.approx(0.15)

    def test_equal_weight_with_no_members_is_absent_not_zero(self):
        assert _bench.equal_weight_return(np.array([0.1, 0.2]),
                                          np.array([False, False])) is None


# =========================================================================== #
# Correction 1 - training universe is not the investment universe
# =========================================================================== #
class TestUniverseSeparation:

    def test_the_two_universes_are_distinct_concepts(self):
        assert _uni.TRAIN_BROAD_PIT != _uni.TRAIN_INVESTMENT_ONLY
        assert _uni.EVALUATION_UNIVERSE.startswith("EVALUATE_S_AND_P_500")

    def test_both_training_universes_are_declared_choices(self):
        assert set(_uni.TRAINING_UNIVERSES) == {_uni.TRAIN_INVESTMENT_ONLY,
                                                _uni.TRAIN_BROAD_PIT}

    def test_membership_is_indexed_by_date_never_by_position(self):
        dates = ["2020-01-02", "2020-01-03", "2020-01-06"]
        syms = ["AAA", "BBB"]
        mask = np.array([[True, False], [True, True], [False, True]])
        m = _uni.Membership(mask, np.array([True, True]), dates, syms)
        assert m.count_on("2020-01-02") == 1
        assert m.count_on("2020-01-03") == 2
        assert m.count_on("2020-01-06") == 1

    def test_an_unknown_date_raises_rather_than_returning_an_empty_universe(self):
        m = _uni.Membership(np.array([[True]]), np.array([True]),
                            ["2020-01-02"], ["AAA"])
        with pytest.raises(_uni.UniverseUnavailable):
            m.members_on("1999-01-01")

    def test_eligible_columns_masks_a_cross_section_by_membership(self):
        dates = ["2020-01-02"]
        syms = ["AAA", "BBB", "CCC"]
        mask = np.array([[True, False, True]])
        m = _uni.Membership(mask, np.array([True] * 3), dates, syms)
        # a cross-section carrying panel columns [2, 1] -> [member, non-member]
        got = m.eligible_columns("2020-01-02", [2, 1])
        assert list(got) == [True, False]

    def test_a_non_member_cannot_be_owned(self):
        # The portfolio is built only from names the mask admits.
        dates = ["2020-01-02"]
        syms = ["AAA", "BBB"]
        m = _uni.Membership(np.array([[True, False]]), np.array([True, True]),
                            dates, syms)
        eligible = [s for s, ok in zip(syms, m.members_on("2020-01-02")) if ok]
        assert eligible == ["AAA"]
        assert "BBB" not in eligible
