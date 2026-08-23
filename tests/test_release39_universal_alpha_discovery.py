"""Release 39 regression - the Autonomous Universal Alpha Discovery Engine.

Pins the invariants Release 39 must never lose:

* the evidence boundary - three zones, a Zone-C lockbox with the Release-31
  budget, one execution per frozen spec, no revise-and-retry, Zone-B reuse
  counted, Zone C labelled HISTORICAL confirmation and never fresh evidence;
* search honesty - ledgered budgets with hard ceilings, the deflated Sharpe
  penalising trial count, BH/SPA imported from the Release-31 owner;
* PIT discipline - targets strictly forward, pivots usable only after
  confirmation, masks never filled;
* the trade space - costs on traded notional (buys AND sells), every
  expression with a declared control, no degenerate cross-sections;
* predecessor integrity - RESEARCHABLE is not TESTED, untested is not
  rejected, statuses from the frozen vocabulary;
* commercial and production safety - every flag False, models trained and
  never promoted, no purchase or renewal authority anywhere.

Hermetic: every filesystem write goes to a tmp research root via
``PAPER_TRADER_R39_RESEARCH_ROOT``; no network, no Norgate, no estate reads.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from paper_trader.alpha_agent import r39
from paper_trader.alpha_agent.r31 import contract as r31_contract
from paper_trader.alpha_agent.r31 import multiple_testing as r31_mt
from paper_trader.alpha_agent.r39 import burden as B
from paper_trader.alpha_agent.r39 import contract as C
from paper_trader.alpha_agent.r39 import estate as E
from paper_trader.alpha_agent.r39 import integrity as I
from paper_trader.alpha_agent.r39 import judge as J
from paper_trader.alpha_agent.r39 import model_registry as M
from paper_trader.alpha_agent.r39 import representation_factory as R
from paper_trader.alpha_agent.r39 import search_budget as SB
from paper_trader.alpha_agent.r39 import target_factory as TF
from paper_trader.alpha_agent.r39 import trade_space as TS
from paper_trader.alpha_agent.r39 import universal_state as US
from paper_trader.alpha_agent.r39 import zones as Z


@pytest.fixture
def tmp_root(tmp_path, monkeypatch):
    monkeypatch.setenv(r39.RESEARCH_ROOT_ENV, str(tmp_path))
    return tmp_path


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
class TestContract:
    def test_idea_boundary_open_evidence_boundary_closed(self):
        assert C.NO_ARTIFICIAL_ASSET_BOUNDARY
        assert C.NO_ARTIFICIAL_MODEL_FAMILY_BOUNDARY
        assert C.NO_HUMAN_HYPOTHESIS_BOUNDARY
        assert C.SEARCH_CAPACITY_IS_NOT_EVIDENCE
        assert C.NO_TEST_SET_REUSE and C.NO_UNCONTROLLED_MULTIPLE_SEARCH
        assert C.NO_UNAUTHORIZED_SPEND and C.NO_PRODUCTION_MUTATION

    def test_zone_chronology(self):
        a = pd.Timestamp(C.ZONE_A_DISCOVERY_END)
        b0 = pd.Timestamp(C.ZONE_B_VALIDATION_START)
        b1 = pd.Timestamp(C.ZONE_B_VALIDATION_END)
        c0 = pd.Timestamp(C.ZONE_C_CONFIRMATION_START)
        c1 = pd.Timestamp(C.ZONE_C_CONFIRMATION_END)
        assert a < b0 < b1 < c0 < c1

    def test_zone_c_is_never_fresh_evidence(self):
        assert C.ZONE_C_EVIDENCE_LABEL == "HISTORICAL_CONFIRMATION_EVIDENCE"
        assert C.ZONE_C_IS_FRESH_UNSEEN_EVIDENCE is False
        assert C.TRUE_FORWARD_BEGINS_ONLY_AFTER_FREEZE
        assert C.HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE

    def test_lockbox_budget_is_the_r31_budget(self):
        assert C.MAX_LOCKBOX_CANDIDATES \
            == r31_contract.MAX_LOCKBOX_CANDIDATES
        assert C.MAX_LOCKBOX_PER_FAMILY \
            == r31_contract.MAX_LOCKBOX_PER_FAMILY
        assert C.FDR_Q == r31_contract.FDR_Q

    def test_alpha_pass_requires_the_qualified_verdict(self):
        assert C.ALPHA_PASS_REQUIRES_VERDICT \
            == "R39_AUTONOMOUS_ALPHA_DISCOVERED"
        assert C.ALPHA_PASS_REQUIRES_VERDICT in C.SUCCESS_STATES
        assert C.DO_NOT_FORCE_A_SUCCESS_STATE

    def test_five_result_axes(self):
        assert C.RESULT_AXES == ("SYSTEM_RESULT", "DATA_RESULT",
                                 "DISCOVERY_RESULT",
                                 "HISTORICAL_ALPHA_RESULT",
                                 "FORWARD_CANDIDATE_RESULT")

    def test_every_commercial_flag_is_false(self):
        for flag in ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
                     "MAY_CREATE_PROVIDER_ACCOUNT",
                     "MAY_CHANGE_SUBSCRIPTION_TIER",
                     "MAY_RENEW_SUBSCRIPTION",
                     "MAY_ACCEPT_LICENCE_AGREEMENT",
                     "MAY_SUBMIT_PAYMENT_DETAILS",
                     "MAY_PURCHASE_CLOUD_COMPUTE", "MAY_INSTALL_CUDA",
                     "MAY_DOWNLOAD_MODEL_WEIGHTS",
                     "MAY_UPGRADE_NORGATE_PACKAGES", "MAY_PURCHASE_COMPUTE"):
            assert getattr(C, flag) is False, flag
        assert C.MONEY_SPENT_BY_R39_USD == 0.0
        auth = C.purchase_authority()
        assert auth["purchase_authorised"] is False
        assert auth["renewal_authorised"] is False

    def test_untested_is_not_rejected(self):
        assert C.DATA_AVAILABLE_BUT_NOT_TESTED_IS_NOT_A_REJECTED_HYPOTHESIS
        assert "DATA_AVAILABLE_BUT_NOT_TESTED" in C.RESEARCH_STATUSES

    def test_fib_placebo_vocabulary(self):
        assert set(C.FIB_LEVELS).isdisjoint(set(C.PLACEBO_LEVELS))
        assert C.FIB_TIE_WITH_PLACEBO_MEANS == "PULLBACK_STRUCTURE_MAY_MATTER"
        assert C.FIB_REQUIRES_CONFIRMED_PIVOTS

    def test_installed_packages_carry_licences(self):
        for pkg, lic in C.PACKAGES_INSTALLED_FOR_R39:
            assert pkg and lic


class TestSafetyBlock:
    def test_trains_models_and_promotes_nothing(self):
        sb = r39.safety_block()
        assert sb["trains_a_model"] is True
        assert sb["promotes_model"] is False
        for key in ("creates_order", "creates_proposal", "creates_decision",
                    "mutates_holdings", "mutates_cash",
                    "writes_operational_store", "enables_automation",
                    "purchases_data", "renews_subscription",
                    "may_spend_money", "may_mutate_production",
                    "spends_cloud_compute", "installs_cuda"):
            assert sb[key] is False, key
        for badge in ("RESEARCH ONLY", "NO MODEL PROMOTION",
                      "AUTOMATION OFF", "MANUAL REVIEW", "NO PURCHASE"):
            assert badge in sb["safety"]


# --------------------------------------------------------------------------- #
# Zones - lockbox and reuse ledger
# --------------------------------------------------------------------------- #
class TestZones:
    F = [{"candidate_id": "c1", "spec_hash": "h1", "family": "FAM_A",
          "sample": "FUT", "horizon_sessions": 21},
         {"candidate_id": "c2", "spec_hash": "h2", "family": "FAM_A",
          "sample": "FUT", "horizon_sessions": 21},
         {"candidate_id": "c3", "spec_hash": "h3", "family": "FAM_B",
          "sample": "EQ", "horizon_sessions": 21}]

    def test_zone_c_refuses_access_before_freeze(self, tmp_root):
        with pytest.raises(Z.LockboxViolation):
            Z.authorise("h1", campaign_id="t1", family="FAM_A",
                        candidate_id="c1", at="now")

    def test_one_execution_per_spec(self, tmp_root):
        Z.freeze_finalists(self.F, campaign_id="t2", selected_at="now",
                           selection_basis="test")
        Z.authorise("h1", campaign_id="t2", family="FAM_A",
                    candidate_id="c1", at="now")
        with pytest.raises(Z.LockboxViolation):
            Z.authorise("h1", campaign_id="t2", family="FAM_A",
                        candidate_id="c1", at="later")

    def test_per_family_cap_enforced_at_freeze(self, tmp_root):
        too_many = self.F + [{"candidate_id": "c4", "spec_hash": "h4",
                              "family": "FAM_A", "sample": "FUT",
                              "horizon_sessions": 21}]
        with pytest.raises(Z.LockboxViolation):
            Z.freeze_finalists(too_many, campaign_id="t3",
                               selected_at="now", selection_basis="test")

    def test_revised_finalist_set_refused(self, tmp_root):
        Z.freeze_finalists(self.F, campaign_id="t4", selected_at="now",
                           selection_basis="test")
        revised = [dict(f) for f in self.F]
        revised[0]["spec_hash"] = "h1_revised"
        with pytest.raises(Z.LockboxViolation):
            Z.freeze_finalists(revised, campaign_id="t4",
                               selected_at="later", selection_basis="retry")

    def test_unfrozen_spec_refused(self, tmp_root):
        Z.freeze_finalists(self.F, campaign_id="t5", selected_at="now",
                           selection_basis="test")
        with pytest.raises(Z.LockboxViolation):
            Z.authorise("h_unknown", campaign_id="t5", family="FAM_B",
                        candidate_id="cX", at="now")

    def test_reuse_ledger_counts_every_evaluation(self, tmp_root):
        Z.record_zone_b("cA", stage="STAGE2_3", campaign_id="t6")
        Z.record_zone_b("cA", stage="STAGE3", campaign_id="t6")
        Z.record_zone_b("cB", stage="STAGE2_3", campaign_id="t6")
        s = Z.reuse_summary("t6")
        assert s["total_zone_b_evaluations"] == 3
        assert s["distinct_candidates_on_zone_b"] == 2


class TestSearchBudget:
    def test_ceiling_refuses_the_thirteenth_finalist(self, tmp_root):
        led = SB.Ledger("t7")
        for _ in range(C.MAX_LOCKBOX_CANDIDATES):
            led.add("FINALISTS_FROZEN", family="F", stage="FREEZE")
        with pytest.raises(SB.BudgetExceeded):
            led.add("FINALISTS_FROZEN", family="F", stage="FREEZE")

    def test_counters_are_the_public_vocabulary(self, tmp_root):
        led = SB.Ledger("t8")
        assert set(led.counters) == set(C.SEARCH_BUDGET_COUNTERS)


# --------------------------------------------------------------------------- #
# Targets - strictly forward, masked
# --------------------------------------------------------------------------- #
class TestTargets:
    def _panel(self):
        d = pd.to_datetime(["2001-01-31", "2001-01-31", "2001-02-28",
                            "2001-02-28"])
        return pd.DataFrame({
            "decision_date": d,
            "asset_class": ["A", "A", "A", "A"],
            "market_id": ["m1", "m2", "m1", "m2"],
            "fwd_21": [0.02, -0.01, np.nan, 0.03],
            "control_fwd_21": [0.005, 0.005, 0.01, 0.01],
            "fwd_vol_21": [0.1, 0.2, 0.1, 0.2]})

    def test_excess_sign_rank(self):
        p = TF.materialise(self._panel())
        assert p.loc[0, "tgt_excess_21"] == pytest.approx(0.015)
        assert p.loc[1, "tgt_sign_21"] == 0.0
        assert math.isnan(p.loc[2, "tgt_excess_21"])
        assert math.isnan(p.loc[2, "tgt_rank_21"])  # mask, never fill
        assert p.loc[0, "tgt_rank_21"] > p.loc[1, "tgt_rank_21"]

    def test_registry_declares_no_explosion(self, tmp_root):
        body = TF.build_registry("t9")
        assert body["no_combinatorial_explosion"]
        assert body["targets_realised_strictly_after_decision"]


# --------------------------------------------------------------------------- #
# Trade space - traded-notional costs, controls, degeneracy
# --------------------------------------------------------------------------- #
class TestTradeSpace:
    def _mats(self):
        idx = pd.to_datetime(["2001-01-31", "2001-02-28", "2001-03-31"])
        pred = pd.DataFrame({"a": [1.0, 1.0, -1.0], "b": [-1.0, 1.0, 1.0]},
                            index=idx)
        fwd = pd.DataFrame({"a": [0.02, 0.01, -0.01],
                            "b": [-0.01, 0.02, 0.03]}, index=idx)
        cost = pd.Series({"a": 10.0, "b": 10.0})
        return pred, fwd, cost

    def test_costs_charged_on_buys_and_sells(self):
        pred, fwd, cost = self._mats()
        book = TS.ts_outright(pred, fwd, cost)
        # period 0: |dw| = 0.5 + 0.5 = 1.0 -> 10 bps
        assert book["costs"][0] == pytest.approx(1.0 * 10.0 / 1e4)
        # period 1: 'b' flips -0.5 -> +0.5 = 1.0 traded
        assert book["traded_notional"][1] == pytest.approx(1.0)

    def test_cost_stress_doubles_costs(self):
        pred, fwd, cost = self._mats()
        b1 = TS.ts_outright(pred, fwd, cost)
        b2 = TS.ts_outright(pred, fwd, cost, cost_multiplier=2.0)
        assert np.allclose(b2["costs"], 2.0 * b1["costs"])
        assert np.allclose(b2["gross"], b1["gross"])

    def test_xs_long_short_refuses_degenerate_cross_section(self):
        pred, fwd, cost = self._mats()
        book = TS.xs_long_short(pred, fwd, cost)
        # two names cannot fill 2-per-leg terciles -> flat book
        assert np.allclose(book["weights"].to_numpy(), 0.0)

    def test_every_expression_declares_a_control(self):
        for name in C.TRADE_EXPRESSIONS:
            assert name in TS.EXPRESSION_CONTROLS

    def test_passive_control_holds_everything_valid(self):
        pred, fwd, cost = self._mats()
        ctrl = TS.passive_ew_control(fwd, cost)
        assert ctrl["gross"][0] == pytest.approx((0.02 - 0.01) / 2.0)


# --------------------------------------------------------------------------- #
# Judge and burden
# --------------------------------------------------------------------------- #
class TestJudgeAndBurden:
    def test_bh_and_spa_are_the_r31_owners(self):
        assert B.benjamini_hochberg is r31_mt.benjamini_hochberg
        assert B.superior_predictive_ability \
            is r31_mt.superior_predictive_ability

    def test_deflated_sharpe_penalises_search(self):
        rng = np.random.default_rng(7)
        x = rng.normal(0.004, 0.02, 240)
        few = B.deflated_sharpe(x, n_trials=2, trial_sharpe_variance=0.01)
        many = B.deflated_sharpe(x, n_trials=500,
                                 trial_sharpe_variance=0.01)
        assert few["state"] == many["state"] == "OK"
        assert many["dsr"] < few["dsr"]
        assert many["expected_max_sharpe"] > few["expected_max_sharpe"]

    def test_sign_split_robustness(self):
        steady = np.full(120, 0.01)
        r = J.sign_split_robustness(steady)
        assert r["halves_same_sign"] and r["thirds_same_sign"]
        flip = np.concatenate([np.full(60, 0.01), np.full(60, -0.02)])
        assert J.sign_split_robustness(flip)["halves_same_sign"] is False

    def test_vol_matched_control_cannot_leverage(self):
        book = np.random.default_rng(1).normal(0, 0.08, 100)
        bench = np.random.default_rng(2).normal(0, 0.01, 100)
        vm = J.vol_matched(book, bench)
        assert vm["state"] == "OK"
        assert vm["weight"] <= 1.0


# --------------------------------------------------------------------------- #
# Representation PIT - confirmed pivots only
# --------------------------------------------------------------------------- #
class TestPivotConfirmation:
    def test_extremum_is_a_pivot_only_with_a_full_window(self):
        close = np.concatenate([np.linspace(100, 110, 30),
                                [150.0],  # spike at index 30
                                np.linspace(110, 100, 30)])
        piv = R._pivots(close, half=10)
        kinds = {i: k for i, k in piv}
        assert kinds.get(30) == "H"

    def test_unconfirmed_pivot_is_not_usable(self):
        # gate replicated from add_structure_and_fib: usable iff i+10 <= pos
        piv_idx = np.array([30])
        assert piv_idx[piv_idx + 10 <= 35].size == 0
        assert piv_idx[piv_idx + 10 <= 40].size == 1


# --------------------------------------------------------------------------- #
# Predecessor integrity + estate vocabulary
# --------------------------------------------------------------------------- #
class TestIntegrityClassification:
    EXECUTED = {"CMDTY_TS_TREND_12M", "CMDTY_XS_CARRY",
                "VX_TERM_STRUCTURE_CARRY"}

    def _cell(self, key, fam, status="NATIVE_DATA_VERIFIED_RESEARCHABLE"):
        return {"market_key": key, "strategy_family": fam,
                "r38_status": status, "cell_id": key + "::" + fam}

    def test_group_coverage_is_not_direct(self):
        c = I._classify(self._cell("CMDTY_GRAINS", "TREND"), self.EXECUTED)
        assert c["research_status"] == "VALIDLY_REPRESENTED_BY_GROUP_TEST"

    def test_vx_carry_is_direct(self):
        c = I._classify(self._cell("VOL_VIX_FUTURES", "CARRY"),
                        self.EXECUTED)
        assert c["research_status"] == "DIRECTLY_TESTED"

    def test_untested_pattern_is_available_not_rejected(self):
        c = I._classify(self._cell("CMDTY_GRAINS", "RELATIVE_VALUE"),
                        self.EXECUTED)
        assert c["research_status"] == "DATA_AVAILABLE_BUT_NOT_TESTED"

    def test_partial_is_missing_information_leg(self):
        c = I._classify(self._cell("CMDTY_GRAINS",
                                   "FUNDAMENTAL_SUPPLY_DEMAND",
                                   "PARTIALLY_UNLOCKED"), self.EXECUTED)
        assert c["research_status"] == "MISSING_REQUIRED_INFORMATION_LEG"

    def test_blocked_stays_blocked(self):
        c = I._classify(self._cell("CREDIT_SINGLE_NAME", "CARRY",
                                   "STILL_BLOCKED_ENTITLEMENT"),
                        self.EXECUTED)
        assert c["research_status"] == "STILL_BLOCKED"

    def test_no_experiments_manufactured(self):
        assert I.NO_EXPERIMENTS_ARE_MANUFACTURED_HERE


class TestEstateVocabulary:
    def test_every_exclusion_reason_is_named(self):
        for fam in E.families():
            if not fam["admitted"]:
                assert fam["excluded_reason"] in C.EXCLUSION_REASONS, \
                    fam["family"]

    def test_crypto_excluded_on_history_not_taste(self):
        fams = {f["family"]: f for f in E.families()}
        assert fams["CRYPTO_OUTRIGHT_SLEEVE"]["excluded_reason"] \
            == "INSUFFICIENT_HISTORY"

    def test_analyst_lane_excluded_on_survivorship(self):
        fams = {f["family"]: f for f in E.families()}
        assert fams["ANALYST_REVISIONS"]["excluded_reason"] \
            == "SURVIVORSHIP_FAILURE"

    def test_revised_snapshot_macro_is_a_pit_failure(self):
        fams = {f["family"]: f for f in E.families()}
        assert fams["FRED_REVISED_SNAPSHOT"]["excluded_reason"] \
            == "PIT_FAILURE"


class TestZoneOf:
    def test_boundaries(self):
        s = pd.Series(pd.to_datetime([
            "1990-06-15",           # ZONE_A
            C.ZONE_A_DISCOVERY_END,  # last A day
            "2007-03-15",           # embargo gap
            C.ZONE_B_VALIDATION_START, C.ZONE_B_VALIDATION_END,
            "2016-12-01",           # embargo gap
            C.ZONE_C_CONFIRMATION_START, C.ZONE_C_CONFIRMATION_END,
            "2026-08-01"]))         # post sample
        z = US.zone_of(s).tolist()
        assert z == ["ZONE_A", "ZONE_A", "EMBARGO", "ZONE_B", "ZONE_B",
                     "EMBARGO", "ZONE_C", "ZONE_C", "POST_SAMPLE"]


class TestModelRegistry:
    def test_deferred_families_carry_reasons_and_classes(self):
        for entry in M.technology_inventory():
            assert entry["compute_class"] in C.COMPUTE_CLASSES
            if not entry["admitted"]:
                assert entry["reason"]

    def test_adapters_fit_and_predict(self):
        rng = np.random.default_rng(3)
        X = rng.normal(size=(300, 4))
        y = X[:, 0] * 0.1 + rng.normal(0, 0.05, 300)
        for name in ("ridge", "lightgbm", "baseline_hist_mean"):
            m = M.make_adapter(name)
            m.fit(X, y)
            p = m.predict(X)
            assert np.isfinite(p).all() and len(p) == 300

    def test_stage_zoos_are_known_adapters(self):
        for name in M.STAGE1_MODELS + M.STAGE2_MODELS:
            assert M.make_adapter(name) is not None
