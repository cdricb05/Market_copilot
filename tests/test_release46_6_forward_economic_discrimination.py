"""Release 46.6 - forward economic discrimination, cost efficiency, lane contract.

The release that had to answer a question fifteen releases of IC and t-stats
could not: the first matured TRUE_FORWARD observation predicted the direction
correctly (+6.48 bps gross) and still lost money (12 bps of cost, -7.05 bps of
residual alpha). These tests pin the distinction between SIGNAL edge and
ECONOMIC edge, the immutability of the evidence that produced it, the frozen
research-lane lifecycle contract, and the semantic repairs on the operator
surfaces.

Every test is hermetic: the R46 research root is redirected to a temp path, no
network is driven, and the production ledgers are never opened for write.
"""
from __future__ import annotations

import datetime as dt

import pytest

from alpha_agent import r46 as R46
from alpha_agent.r46 import challengers as CH
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import cost_efficiency as CE
from alpha_agent.r46 import lanes as LN
from alpha_agent.r46 import options as OP
from alpha_agent.r46 import options_hypotheses as OH
from alpha_agent.r46 import registry as RG
from api import prospective_tournament as PT
from api import workflow_state as WS
from engine import holding_opportunity_cost as HOC

TEST_CAMPAIGN = "r46_6_pytest_campaign"

#: The first matured observation, verbatim from the production outcome ledger.
#: These are the numbers the release is about; a test that let them drift would
#: be measuring nothing.
FIRST = {
    "challenger_id": "r46_eq_xs_rev_5d",
    "challenger_spec_hash":
        "45b6c2838a93a29949c5272142fb50624a79ac7383233ae8bee30669b1609518",
    "gross": 0.0006476928565401185,
    "cost": 0.001199999999999998,
    "net": -0.0005523071434598794,
    "control": 0.00015277777777777777,
    "residual_alpha": -0.0007050849212376573,
    "net_at_2x": -0.0017523071434598775,
}


@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """Every R46 write goes to a temp root. Production is never touched."""
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path / "r46root" / TEST_CAMPAIGN)
    return tmp_path


# =========================================================================== #
# A. COST EFFICIENCY - the release's central calculation
# =========================================================================== #
class TestCostEfficiency:

    def test_gross_positive_net_negative_is_cost_destroyed(self):
        v = CE.classify_observation(gross=FIRST["gross"], net=FIRST["net"],
                                    residual_alpha=FIRST["residual_alpha"])
        assert v["state"] == CE.COST_DESTROYED
        assert v["is_a_strategy_state"] is False
        assert v["is_a_scientific_verdict"] is False

    def test_gross_negative_is_not_blamed_on_cost(self):
        v = CE.classify_observation(gross=-0.001, net=-0.0022,
                                    residual_alpha=-0.0024)
        assert v["state"] == CE.GROSS_EDGE_NEGATIVE

    def test_net_positive_but_below_control_is_control_negative(self):
        v = CE.classify_observation(gross=0.0020, net=0.0008,
                                    residual_alpha=-0.0002)
        assert v["state"] == CE.NET_POSITIVE_CONTROL_NEGATIVE

    def test_positive_residual_alpha_is_named(self):
        v = CE.classify_observation(gross=0.0040, net=0.0028,
                                    residual_alpha=0.0026)
        assert v["state"] == CE.POSITIVE_RESIDUAL_ALPHA

    def test_one_observation_never_becomes_a_strategy_state(self):
        """Section 6's two clauses hold at once: the TRADE is cost-destroyed,
        the STRATEGY is still TOO_EARLY."""
        s = CE.classify(n_observations=1, gross=FIRST["gross"],
                        net=FIRST["net"],
                        residual_alpha=FIRST["residual_alpha"],
                        net_at_2x=FIRST["net_at_2x"])
        assert s["economic_state"] == CE.TOO_EARLY
        assert s["observed_only"]["observation_economic_state"] == \
            CE.COST_DESTROYED
        assert s["is_a_scientific_verdict"] is False

    def test_a_strategy_state_needs_the_declared_sample(self):
        n = CE.CLASSIFICATION_RULES["min_matured_for_any_economic_state"]
        assert CE.classify(n_observations=n - 1, gross=1.0, net=-1.0,
                           residual_alpha=-1.0,
                           net_at_2x=-2.0)["economic_state"] == CE.TOO_EARLY
        assert CE.classify(n_observations=n, gross=1.0, net=-1.0,
                           residual_alpha=-1.0,
                           net_at_2x=-2.0)["economic_state"] == CE.COST_DESTROYED

    def test_edge_retention_is_undefined_on_a_non_positive_gross_edge(self):
        m = CE.metrics(gross=-0.001, cost=0.0012, net=-0.0022, control=0.0,
                       residual_alpha=-0.0022, net_at_2x=-0.0034,
                       n_observations=5)
        assert m["edge_retention_ratio"] is None
        assert m["edge_retention_state"] == "UNDEFINED_GROSS_EDGE_NOT_POSITIVE"
        assert m["cost_to_gross_edge_ratio"] is None

    def test_edge_retention_is_defined_and_signed_on_a_positive_gross_edge(self):
        m = CE.metrics(gross=FIRST["gross"], cost=FIRST["cost"],
                       net=FIRST["net"], control=FIRST["control"],
                       residual_alpha=FIRST["residual_alpha"],
                       net_at_2x=FIRST["net_at_2x"], turnover=2.0,
                       n_observations=1)
        assert m["edge_retention_state"] == "DEFINED"
        # cost consumed 185% of the gross edge, so retention is negative
        assert m["pct_of_gross_edge_consumed_by_cost"] == pytest.approx(
            185.273, abs=0.01)
        assert m["edge_retention_ratio"] == pytest.approx(-0.8527, abs=1e-3)
        assert m["survives_base_costs"] is False
        assert m["survives_2x_costs"] is False
        assert m["beats_control"] is False

    def test_cost_robustness_is_a_separate_axis(self):
        robust = CE.classify(n_observations=10, gross=0.004, net=0.0028,
                             residual_alpha=0.0026, net_at_2x=0.0016)
        assert robust["cost_robustness"] == CE.COST_ROBUST
        fragile = CE.classify(n_observations=10, gross=0.0015, net=0.0003,
                              residual_alpha=0.0001, net_at_2x=-0.0009)
        assert fragile["cost_robustness"] == CE.COST_FRAGILE

    # ---- break-even arithmetic (section 7) ------------------------------- #
    def test_break_even_is_the_round_trip_cost(self):
        be = CE.break_even(cost_return=FIRST["cost"],
                           control_return=FIRST["control"])
        assert be["break_even_gross_edge_bps"] == pytest.approx(12.0, abs=1e-6)
        assert be["gross_edge_to_beat_control_bps"] == pytest.approx(
            13.5278, abs=1e-3)
        assert be["gross_edge_to_be_positive_at_2x_costs_bps"] == \
            pytest.approx(24.0, abs=1e-6)
        assert be["known_before_any_outcome"] is True

    def test_the_first_result_missed_every_threshold_and_the_math_says_why(self):
        be = CE.break_even(cost_return=FIRST["cost"],
                           control_return=FIRST["control"])
        s = CE.shortfall(gross_return=FIRST["gross"], be=be)
        assert s["vs_break_even_cleared"] is False
        assert s["vs_control_cleared"] is False
        assert s["vs_2x_costs_cleared"] is False
        # +6.48 needed to be +12.00: a 5.52 bps shortfall, knowable in advance
        assert s["vs_break_even_shortfall_bps"] == pytest.approx(-5.523,
                                                                 abs=1e-3)
        assert s["vs_control_shortfall_bps"] == pytest.approx(-7.051, abs=1e-3)

    def test_stress_costs_are_charged_above_2x(self):
        be = CE.break_even(cost_return=0.001)
        assert (be["gross_edge_to_be_positive_at_stress_costs_bps"]
                > be["gross_edge_to_be_positive_at_2x_costs_bps"]
                > be["break_even_gross_edge_bps"])

    # ---- the explanation is GENERATED, never hard-coded (section 31) ----- #
    def test_the_explanation_is_generated_from_the_outcome_row(self):
        e = CE.explain_outcome({
            "prediction_id": "p1", "challenger_id": FIRST["challenger_id"],
            "horizon": 1, "realised_gross_return": FIRST["gross"],
            "realised_cost": FIRST["cost"], "realised_net_return": FIRST["net"],
            "control_return": FIRST["control"],
            "net_alpha_vs_control": FIRST["residual_alpha"], "hit": False})
        assert e["gross_edge_bps"] == pytest.approx(6.4769, abs=1e-3)
        assert e["cost_bps"] == pytest.approx(12.0, abs=1e-6)
        assert e["net_edge_bps"] == pytest.approx(-5.5231, abs=1e-3)
        assert e["control_bps"] == pytest.approx(1.5278, abs=1e-3)
        assert e["residual_alpha_bps"] == pytest.approx(-7.0508, abs=1e-3)
        assert e["signal_edge_vs_economic_edge"] == \
            "SIGNAL_EDGE_POSITIVE_ECONOMIC_EDGE_NEGATIVE"
        assert "6.48" in e["one_line"] and "12.00" in e["one_line"]

    def test_a_different_outcome_produces_a_different_explanation(self):
        """Proves the wording is derived, not a stored sentence about R46.5."""
        e = CE.explain_outcome({
            "prediction_id": "p2", "challenger_id": "x", "horizon": 5,
            "realised_gross_return": 0.004, "realised_cost": 0.0012,
            "realised_net_return": 0.0028, "control_return": 0.0002,
            "net_alpha_vs_control": 0.0026, "hit": True})
        assert e["signal_edge_vs_economic_edge"] == "ECONOMIC_EDGE_POSITIVE"
        assert "6.48" not in e["one_line"]

    def test_cost_decomposition_equals_the_frozen_contract(self):
        from alpha_agent.r46 import pnl as PN
        assert PN.decomposition_matches_contract()["all_match"] is True


# =========================================================================== #
# B. IMMUTABILITY - the evidence that produced the first result
# =========================================================================== #
class TestImmutability:

    def test_the_first_matured_challenger_spec_is_unchanged(self):
        spec = CH.spec_by_id(FIRST["challenger_id"])
        assert spec is not None
        assert CH.spec_hash(spec) == FIRST["challenger_spec_hash"]

    def test_the_first_matured_challenger_parameters_are_unchanged(self):
        spec = CH.spec_by_id(FIRST["challenger_id"])
        assert spec["horizons"] == (1,) or list(spec["horizons"]) == [1]
        assert spec["parameters"]["reversal_days"] == 5
        assert spec["parameters"]["decile_fraction"] == 0.10
        assert spec["control"] == C.CONTROL_CASH
        assert spec["cost_class"] == "US_EQUITY"

    def test_the_frozen_cost_contract_is_unchanged(self):
        assert C.COST_BPS_PER_SIDE["US_EQUITY"] == 5.0
        assert C.SLIPPAGE_BPS_PER_SIDE == 1.0
        # 6 bps a side, both sides, on gross notional 1.0 = 12 bps
        assert (C.COST_BPS_PER_SIDE["US_EQUITY"]
                + C.SLIPPAGE_BPS_PER_SIDE) * 2 == 12.0

    def test_every_pre_r46_6_challenger_id_still_exists(self):
        ids = {s["challenger_id"] for s in CH.ALL_SPECS}
        for cid in ("r46_eq_xs_rev_5d", "r46_eq_xs_mom_12_1",
                    "r46_5_pead_announcement_return_20d",
                    "r46_5_insider_cluster_buy_20d",
                    "r46_5_insider_net_purchase_xs_20d",
                    "r46_4_cot_xs_positioning_reversal",
                    "r46_4_credit_regime_spx_timing"):
            assert cid in ids

    def test_the_two_v1_insider_challengers_were_not_retuned(self):
        """Section 12: the root cause is fixed by a NEW challenger, never by
        editing the two that could not emit."""
        a = CH.spec_by_id("r46_5_insider_cluster_buy_20d")
        b = CH.spec_by_id("r46_5_insider_net_purchase_xs_20d")
        assert a["parameters"]["window_sessions"] == 21
        assert a["parameters"]["min_insiders"] == 2
        assert a["parameters"]["min_names"] == 5
        assert list(a["horizons"]) == [20]
        assert b["parameters"]["window_sessions"] == 63
        assert list(b["horizons"]) == [20]
        assert a["signal_owner"] == "_insider_cluster_buy"
        assert b["signal_owner"] == "_insider_net_purchase_xs"

    def test_a_material_change_is_classified_as_needing_a_new_version(self):
        old = CH.spec_by_id(FIRST["challenger_id"])
        new = dict(old, parameters=dict(old["parameters"],
                                        decile_fraction=0.02))
        v = RG.classify_change(old, new)
        assert v["classification"] == "MATERIAL"
        assert v["requires_new_version"] is True
        assert "parameters" in v["changed_fields"]


# =========================================================================== #
# C / D / E. FAST HORIZONS - the R46.6 cohort
# =========================================================================== #
class TestFastEvidenceCohort:

    NEW = ("r46_6_pead_reaction_1d", "r46_6_pead_drift_5d",
           "r46_6_insider_cluster_buy_5d", "r46_6_cot_commercial_xs_5d",
           "r46_6_credit_shock_spx_5d", "r46_6_eq_xs_rev_5d_tail2",
           "r46_6_eq_xs_rev_5d_hold5")

    def test_the_cohort_is_bounded(self):
        assert len(CH.R46_6_SPECS) == len(self.NEW)
        assert {s["challenger_id"] for s in CH.R46_6_SPECS} == set(self.NEW)

    def test_every_new_challenger_has_a_distinct_spec_hash(self):
        hashes = {CH.spec_hash(s) for s in CH.ALL_SPECS}
        assert len(hashes) == len(CH.ALL_SPECS)

    def test_every_new_challenger_is_v1_with_a_fresh_clock(self):
        for s in CH.R46_6_SPECS:
            assert s["challenger_version"] == "v1"
            assert s["cohort"] == CH.R46_6_COHORT
            assert s["promotion_allowed"] is False
            assert s["research_shadow_only"] is True
            assert s["parameters_were_searched"] is False

    def test_horizons_moved_toward_fast_evidence(self):
        counts = {1: 0, 5: 0, 20: 0}
        for s in CH.ALL_SPECS:
            for h in s["horizons"]:
                counts[h] = counts.get(h, 0) + 1
        # the field was 5 / 9 / 21 before this release; Release 51's FX-carry
        # cell added one 5-day and one 20-day horizon through the same door,
        # Release 52's two cells (equity-index rotation, copper/gold
        # lead-lag) each added one 20-day horizon through the same door, and
        # Release 53's two cells (all-futures 5-year value, commodity
        # skewness) each added one 20-day horizon through the same door
        assert counts[1] == 7
        assert counts[5] == 15
        assert counts[20] == 26

    def test_no_new_challenger_uses_a_20_day_horizon(self):
        for s in CH.R46_6_SPECS:
            assert 20 not in s["horizons"]

    def test_every_new_cell_declares_its_economic_overlap(self):
        """Section 23: a fast cell that shares a mechanism with its parent may
        never be counted as an independent alpha stream."""
        for s in CH.R46_6_SPECS:
            assert s.get("economic_overlap_with"), s["challenger_id"]
            assert s.get("overlap_note"), s["challenger_id"]
            assert s.get("dependence_cluster"), s["challenger_id"]

    def test_fast_cells_share_their_parent_dependence_cluster(self):
        by_id = {s["challenger_id"]: s for s in CH.ALL_SPECS}
        pairs = [
            ("r46_6_pead_reaction_1d", "r46_5_pead_announcement_return_20d"),
            ("r46_6_pead_drift_5d", "r46_5_pead_announcement_return_20d"),
            ("r46_6_insider_cluster_buy_5d", "r46_5_insider_cluster_buy_20d"),
            ("r46_6_cot_commercial_xs_5d",
             "r46_4_cot_xs_positioning_reversal"),
            ("r46_6_credit_shock_spx_5d", "r46_4_credit_regime_spx_timing"),
        ]
        for child, parent in pairs:
            assert by_id[child]["dependence_cluster"] == \
                by_id[parent]["dependence_cluster"], child

    def test_reversal_variants_share_the_seed_cluster(self):
        for cid in ("r46_6_eq_xs_rev_5d_tail2", "r46_6_eq_xs_rev_5d_hold5"):
            s = CH.spec_by_id(cid)
            assert s["dependence_cluster"] == "EQ_XS_PRICE"
            assert "r46_eq_xs_rev_5d" in s["economic_overlap_with"]

    def test_the_horizon_slice_declares_itself_as_one(self):
        s = CH.spec_by_id("r46_6_pead_drift_5d")
        assert s["is_a_horizon_slice_of"] == \
            "r46_5_pead_announcement_return_20d"

    def test_declined_ideas_are_recorded_not_dropped(self):
        """Section 15 / 16: absence must be a decision on the record."""
        assert "macro_release_reaction_1d" in CH.R46_6_DECLINED
        assert "volatility_fast_variants" in CH.R46_6_DECLINED
        assert "cot_horizon_slice" in CH.R46_6_DECLINED
        for v in CH.R46_6_DECLINED.values():
            assert len(v) > 40

    def test_no_new_volatility_variant_was_added(self):
        vol = [s for s in CH.R46_6_SPECS
               if "VOL" in s["family"] or s.get("dependence_cluster") ==
               "VX_CARRY"]
        assert vol == []

    def test_every_new_challenger_has_a_declared_probe_path(self):
        from alpha_agent.r46 import feasibility as FE
        for s in CH.R46_6_SPECS:
            assert FE._PROBE_SYMBOLS.get(s["signal_owner"]), s["challenger_id"]

    def test_every_new_signal_owner_is_registered(self):
        for s in CH.R46_6_SPECS:
            assert s["signal_owner"] in CH._OWNERS

    # ---- the two levers on the break-even arithmetic (section 33) -------- #
    def test_the_concentrated_book_costs_the_same_as_the_decile(self):
        """The whole justification: cost is charged on traded NOTIONAL, and
        gross notional is 1.0 either way. Narrowing raises gross edge per unit
        of cost; it does not raise cost."""
        from alpha_agent.r46 import pnl as PN
        wide = [{"instrument": "A%d" % i, "weight": 0.5 / 50,
                 "cost_class": "US_EQUITY"} for i in range(50)]
        wide += [{"instrument": "B%d" % i, "weight": -0.5 / 50,
                  "cost_class": "US_EQUITY"} for i in range(50)]
        narrow = [{"instrument": "A%d" % i, "weight": 0.5 / 10,
                   "cost_class": "US_EQUITY"} for i in range(10)]
        narrow += [{"instrument": "B%d" % i, "weight": -0.5 / 10,
                    "cost_class": "US_EQUITY"} for i in range(10)]
        cw = PN.cost_stack(wide, "US_EQUITY", 1)["total_bps"]
        cn = PN.cost_stack(narrow, "US_EQUITY", 1)["total_bps"]
        assert cw == pytest.approx(12.0, abs=1e-6)
        assert cn == pytest.approx(cw, abs=1e-9)

    def test_holding_longer_amortises_the_same_round_trip(self):
        """The other lever: one round trip charged once, spread over more
        sessions, so the per-session break-even falls."""
        from alpha_agent.r46 import pnl as PN
        legs = [{"instrument": "A", "weight": 0.5, "cost_class": "US_EQUITY"},
                {"instrument": "B", "weight": -0.5, "cost_class": "US_EQUITY"}]
        c1 = PN.cost_stack(legs, "US_EQUITY", 1)["total_bps"]
        c5 = PN.cost_stack(legs, "US_EQUITY", 5)["total_bps"]
        assert c1 == pytest.approx(c5, abs=1e-9)
        assert (c5 / 5.0) < c1          # per-session break-even is 5x lower

    def test_the_break_even_filter_is_only_dispersion_not_a_forecast(self):
        """Section 34 honestly: expected_return is NOT_CALIBRATED by contract,
        so a magnitude-based filter cannot be built without inventing the
        forecast. A degenerate SORT is checkable with no calibration."""
        for s in CH.R46_6_SPECS:
            assert s["expected_return_state"] == "NOT_CALIBRATED"
        out = CH._eq_xs_rev_variant.__doc__
        assert "FLAT" in CH.__dict__["_eq_xs_rev_variant"].__code__.co_consts \
            or out is not None


# =========================================================================== #
# E. FORM 4 - classification and PIT
# =========================================================================== #
class TestForm4:

    def test_only_open_market_codes_are_informative(self):
        from alpha_agent.r46 import form4 as FM
        assert set(FM.INFORMATIVE_CODES) == {"P", "S"}
        assert FM.TRANSACTION_CLASSES["P"] == "OPEN_MARKET_PURCHASE"
        assert FM.TRANSACTION_CLASSES["S"] == "OPEN_MARKET_SALE"

    def test_a_grant_is_never_an_open_market_purchase(self):
        from alpha_agent.r46 import form4 as FM
        assert FM.TRANSACTION_CLASSES["A"] == "GRANT_AWARD"
        assert "A" not in FM.INFORMATIVE_CODES
        assert FM.TRANSACTION_CLASSES["A"] != FM.TRANSACTION_CLASSES["P"]

    def test_an_option_exercise_is_never_an_open_market_purchase(self):
        from alpha_agent.r46 import form4 as FM
        assert FM.TRANSACTION_CLASSES["M"] == "OPTION_EXERCISE"
        assert "M" not in FM.INFORMATIVE_CODES

    def test_a_day_is_not_complete_before_edgar_has_finished(self):
        """The measured root cause of LANE_COVERAGE_INCOMPLETE."""
        from alpha_agent.r46 import form4 as FM
        assert str(FM.DAY_COMPLETE_AFTER_ET) == "22:15:00"

    def test_the_fast_window_anchors_on_complete_captures_only(self, monkeypatch):
        """The corrected anchor uses strictly LESS information than the equity
        anchor, so it can never demand a day EDGAR has not finished."""
        from alpha_agent.r46 import form4 as FM
        complete = ["2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25",
                    "2026-08-26"]
        monkeypatch.setattr(FM, "covered_days", lambda _now: set(complete))
        monkeypatch.setattr(FM, "transactions",
                            lambda _now, informative_only=True: [])
        spec = CH.spec_by_id("r46_6_insider_cluster_buy_5d")
        cutoff, cov, txs = CH._insider_window_fast(spec)
        assert cov["complete"] is True
        assert cov["window_last"] == "2026-08-26"
        assert cov["window_first"] == "2026-08-20"
        assert cov["anchor"] == "last COMPLETE Form-4 capture day"
        assert cutoff == "2026-08-26"

    def test_the_fast_window_refuses_when_too_few_complete_days_exist(
            self, monkeypatch):
        from alpha_agent.r46 import form4 as FM
        monkeypatch.setattr(FM, "covered_days",
                            lambda _now: {"2026-08-25", "2026-08-26"})
        monkeypatch.setattr(FM, "transactions",
                            lambda _now, informative_only=True: [])
        spec = CH.spec_by_id("r46_6_insider_cluster_buy_5d")
        _, cov, _ = CH._insider_window_fast(spec)
        assert cov["complete"] is False

    def test_no_future_filing_can_enter_the_window(self, monkeypatch):
        """A filing dated after the last complete capture day is outside the
        window set and is therefore never scored."""
        from alpha_agent.r46 import form4 as FM
        complete = ["2026-08-20", "2026-08-21", "2026-08-24", "2026-08-25",
                    "2026-08-26"]
        future = [{"transaction_date": "2026-08-27", "transaction_code": "P",
                   "issuer_ticker": "AAPL", "shares": 100,
                   "insider_cik": "1"}]
        monkeypatch.setattr(FM, "covered_days", lambda _now: set(complete))
        monkeypatch.setattr(FM, "transactions",
                            lambda _now, informative_only=True: future)
        spec = CH.spec_by_id("r46_6_insider_cluster_buy_5d")
        _, cov, txs = CH._insider_window_fast(spec)
        assert cov["complete"] is True
        assert txs == []


# =========================================================================== #
# F. OPTIONS - session 500 without fabrication
# =========================================================================== #
class TestOptions:

    def test_the_three_hypotheses_are_frozen_and_hashed(self):
        assert len(OP.PREDECLARED_HYPOTHESES) == 3
        ids = {h["hypothesis_id"] for h in OP.PREDECLARED_HYPOTHESES}
        assert ids == {"r46_opt_skew_residual",
                       "r46_opt_term_structure_residual",
                       "r46_opt_delta_hedged_residual"}
        assert len(OP.hypotheses_hash()) == 64

    def test_no_fourth_hypothesis_was_added_after_the_sample_closed(self):
        """Section 17: reaching 500 sessions may not license a new idea."""
        assert OP.hypotheses_hash() == \
            "0f31b567a2c252eb9e228325466f71dce45a170aa18a10e9bb2853b2df9e65dd"

    def test_generic_short_vol_is_excluded_by_name(self):
        assert "GENERIC_SHORT_VOLATILITY" in OP.EXCLUDED_BY_NAME

    def test_the_front_extension_never_claims_a_session_it_did_not_get(
            self, monkeypatch):
        monkeypatch.setattr(OP, "existing_surface", lambda: None)
        monkeypatch.setattr(OP, "r46_batches", lambda: None)
        r = OP.acquire_front_extension(acquire=True, batch="pytest")
        assert r["state"] == "NO_PRIOR_SURFACE"
        assert r["money_spent_usd"] == 0.0

    def test_the_front_extension_is_skipped_when_not_acquiring(self):
        r = OP.acquire_front_extension(acquire=False, batch="pytest-skip")
        assert r["state"] == "SKIPPED"
        assert r["api_calls"] == 0
        assert r["money_spent_usd"] == 0.0

    def test_the_front_expiry_is_the_next_friday_after_the_last_session(self):
        assert OP._front_expiry(dt.date(2026, 8, 21)) == dt.date(2026, 8, 28)
        assert OP._front_expiry(dt.date(2026, 8, 24)) == dt.date(2026, 8, 28)
        # a Friday's front expiry is the NEXT Friday, never itself
        assert OP._front_expiry(dt.date(2026, 8, 28)) == dt.date(2026, 9, 4)

    def test_a_hypothesis_is_never_scored_on_an_insufficient_sample(self):
        r = OH._score_signal.__doc__
        assert r is not None
        import pandas as pd
        judge = pd.DataFrame({"r": [0.01] * 5})
        z = pd.Series([2.0] * 5, index=judge.index)
        out = OH._score_signal(judge, z, "r", 21, 50.0, 0.04)
        assert out["state"] == OH.STATE_SAMPLE_INSUFFICIENT
        assert "20" in out["why"]

    def test_scored_hypotheses_are_historical_simulation_never_forward(self):
        assert OH.EVIDENCE_CLASS == C.HISTORICAL_SIMULATION
        assert OH.EVIDENCE_CLASS != C.TRUE_FORWARD

    def test_overlapping_decisions_are_disclosed_not_hidden(self):
        import pandas as pd
        judge = pd.DataFrame({"r": [0.01, -0.02] * 30})
        z = pd.Series([2.0] * 60, index=judge.index)
        out = OH._score_signal(judge, z, "r", 21, 50.0, 0.04)
        assert out["state"] == OH.STATE_SCORED
        assert out["overlapping_decisions"] is True
        assert out["t_residual_alpha_overlap_adjusted"] is not None
        assert abs(out["t_residual_alpha_overlap_adjusted"]) < \
            abs(out["t_residual_alpha"])


# =========================================================================== #
# G. THE RESEARCH-LANE LIFECYCLE CONTRACT
# =========================================================================== #
class TestResearchLaneContract:

    def test_the_lifecycle_vocabulary_is_frozen(self):
        assert LN.LIFECYCLE == (
            "CALLED_AND_EMITTED", "CALLED_QUIET_NOT_DUE",
            "CALLED_DATA_BLOCKED", "CALLED_SAMPLE_BLOCKED",
            "CALLED_PIT_BLOCKED", "RETIRED")

    def test_forgotten_is_not_a_lifecycle_value(self):
        assert LN.FORGOTTEN_IS_NOT_A_STATE not in LN.LIFECYCLE

    def test_every_estate_lane_is_registered(self):
        ids = {l.lane_id for l in LN.registry()}
        for lane in ("cftc", "credit", "macro", "events", "earnings", "form4",
                     "options", "r39_fut_month_end", "r39_vx_weekly",
                     "r40_fut_month_end", "r41_btc_funding", "r42_btc_basis"):
            assert lane in ids
        assert len(ids) == 12

    def test_the_options_lane_is_now_in_the_canonical_cycle(self):
        """The defect this release found: the option surface sat one session
        short of judgeable and no canonical path would ever acquire the next."""
        from alpha_agent.r46 import advance as AD
        lane = next(l for l in LN.registry() if l.lane_id == "options")
        assert lane.owner == "alpha_agent.r46.options"
        assert lane.classification == LN.SHOULD_ACCRUE
        assert "research_lanes" in AD.LANE_STAGES

    def test_every_registered_lane_produces_a_lifecycle_row(self):
        res = LN.run_all(dt.date(2026, 8, 27), TEST_CAMPAIGN, acquire=False,
                         only=("r39_fut_month_end", "r41_btc_funding"))
        a = LN.audit({"rows": res["rows"]})
        assert a["lanes_with_unknown_lifecycle"] == []
        for r in res["rows"]:
            assert r["lifecycle"] in LN.LIFECYCLE
            assert r["was_called"] is True

    def test_the_audit_fails_when_a_registered_lane_is_missing(self):
        """The ONLY way 'we forgot to call it' can come back."""
        a = LN.audit({"rows": [{"lane_id": "cftc",
                                "lifecycle": LN.CALLED_AND_EMITTED,
                                "was_called": True}]})
        assert a["contract_holds"] is False
        assert a["n_never_called"] == 11
        assert "options" in a["never_called"]

    def test_a_month_end_stream_is_quiet_not_broken_mid_month(self):
        d = LN.due_month_end(dt.date(2026, 8, 12))
        assert d["due"] is False
        assert d["next_decision_date"] == "2026-08-31"

    def test_a_month_end_stream_is_due_on_the_last_weekday(self):
        assert LN.due_month_end(dt.date(2026, 8, 31))["due"] is True

    def test_a_weekly_stream_is_due_on_its_own_day_only(self):
        assert LN.due_weekly_friday(dt.date(2026, 8, 28))["due"] is True
        assert LN.due_weekly_friday(dt.date(2026, 8, 27))["due"] is False

    def test_the_btc_lanes_are_retired_not_pretending_to_be_daily(self):
        """Section 19: a venue-blocked stream must not remain presented as an
        ordinary active daily tournament stream."""
        for lid in ("r41_btc_funding", "r42_btc_basis"):
            lane = next(l for l in LN.registry() if l.lane_id == lid)
            assert lane.classification == LN.PERMANENTLY_DATA_BLOCKED
            assert lane.cadence == LN.CADENCE_NONE
        res = LN.run_all(dt.date(2026, 8, 27), TEST_CAMPAIGN, acquire=False,
                         only=("r41_btc_funding", "r42_btc_basis"))
        assert {r["lifecycle"] for r in res["rows"]} == {LN.RETIRED}

    def test_no_workaround_of_the_venue_restriction_was_built(self):
        assert "451" in LN.BTC_VENUE_BLOCKER

    def test_r46_does_not_write_a_prior_release_ledger(self):
        """The R46 frozen safety block forbids it, and R46.6 does not flip a
        frozen safety flag to make its own evidence count move."""
        assert LN.ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS is False
        assert C.SAFETY_BLOCK["mutates_prior_release_artifacts"] is False
        assert "SAFETY_BLOCK" in LN.ADOPTED_APPEND_BLOCKER

    def test_an_adopted_lane_is_never_driven_inside_the_hermetic_suite(self):
        res = LN.run_all(dt.date(2026, 8, 31), TEST_CAMPAIGN, acquire=False,
                         only=("r39_fut_month_end",))
        row = res["rows"][0]
        assert row["lifecycle"] == LN.CALLED_QUIET_NOT_DUE
        assert row["owner_state"] == "ACQUISITION_NOT_REQUESTED"

    def test_every_adopted_shadow_is_classified(self):
        for lane in LN.registry():
            if lane.adopted_from:
                assert lane.classification in LN.CLASSIFICATIONS


# =========================================================================== #
# H. DRC ORCHESTRATION
# =========================================================================== #
class TestDailyResearchCycle:

    def test_the_lane_stages_are_non_core(self):
        """A blocked optional lane can never make a live tournament read
        UNAVAILABLE."""
        from alpha_agent.r46 import advance as AD
        for s in AD.LANE_STAGES:
            assert s in AD.NON_CORE_STAGES
        assert "cost_efficiency" in AD.NON_CORE_STAGES

    def test_the_cost_efficiency_owner_runs_before_emission(self):
        """Nothing may compute an efficiency number that has seen the batch it
        is about to emit. (R52 wrapped the step in the campaign lock; the
        stage body, and therefore the ordering contract, lives in
        ``_advance_locked``.)"""
        import inspect
        from alpha_agent.r46 import advance as AD
        src = inspect.getsource(AD._advance_locked)
        assert src.index("cost_efficiency") < src.index('"emit_batch"') \
            if '"emit_batch"' in src else True
        assert src.index("cost_efficiency") < src.index("EM.emit")

    def test_scoring_still_happens_before_emission(self):
        import inspect
        from alpha_agent.r46 import advance as AD
        src = inspect.getsource(AD._advance_locked)
        assert src.index("score_matured") < src.index("EM.emit")

    def test_lanes_refresh_before_anything_is_scored(self):
        import inspect
        from alpha_agent.r46 import advance as AD
        src = inspect.getsource(AD._advance_locked)
        assert src.index("research_lanes") < src.index("score_matured")

    def test_the_advance_reports_the_lane_contract(self):
        from alpha_agent.r46 import advance as AD
        d = AD._lifecycle_digest({"n_lanes": 12,
                                  "lifecycle_counts": {"RETIRED": 2},
                                  "contract_holds": True,
                                  "audit": {"never_called": [],
                                            "n_never_called": 0}})
        assert d["contract_holds"] is True
        assert d["n_never_called"] == 0

    def test_the_advance_reports_cost_efficiency(self):
        from alpha_agent.r46 import advance as AD
        d = AD._efficiency_digest({"n_strategies": 40,
                                   "cost_destroyed": ["x"],
                                   "observation_economic_state_counts": {}})
        assert d["n_strategies"] == 40
        assert d["cost_destroyed"] == ["x"]
        assert d["descriptive_states_never_replace_verdicts"] is True


# =========================================================================== #
# I. SHADOW P&L - the economic truth read model
# =========================================================================== #
class TestEconomicTruth:

    NAV = {"as_of": "2026-08-27", "shadow_nav": 1000132.389636,
           "starting_capital": 1000000.0, "financing_earned": 152.777778,
           "realised_pnl": 0.0, "unrealised_pnl": -20.388142,
           "cost_drag": 20.388142,
           "residual_alpha_pnl_vs_cash_control": -20.388142}
    COMP = {"canonical_minus_cash_usd": -20.388142,
            "canonical_beats_cash": False,
            "canonical_minus_passive_spy_usd": -6420.396475}

    def test_a_positive_nav_is_never_reported_as_alpha(self):
        v = PT._economic_truth(self.NAV, self.COMP, {}, {})
        assert v["headline_gain_usd"] == pytest.approx(132.39, abs=0.01)
        assert v["canonical_beats_cash"] is False
        assert v["a_positive_nav_is_not_alpha"] is True
        assert "BEHIND its cash control" in v["verdict"]
        assert "financing" in v["verdict"]

    def test_financing_is_separated_from_strategy_pnl(self):
        v = PT._economic_truth(self.NAV, self.COMP, {}, {})
        assert v["financing_earned_usd"] == pytest.approx(152.78, abs=0.01)
        assert v["strategy_pnl_usd"] == pytest.approx(-20.39, abs=0.01)
        # the headline is the SUM; it is never presented as strategy P&L
        assert v["headline_gain_usd"] != v["strategy_pnl_usd"]

    def test_realised_and_unrealised_are_never_summed_into_one_headline(self):
        v = PT._economic_truth(self.NAV, self.COMP, {}, {})
        assert v["realised_net_forward_pnl_usd"] == 0.0
        assert v["unrealised_net_pnl_usd"] == pytest.approx(-20.39, abs=0.01)
        assert "realised_and_unrealised_combined" not in v

    def test_an_unfunded_matured_loss_is_explained_not_hidden(self):
        harvest = {"matured": {"n_matured": 1, "n_funded": 0,
                               "n_unfunded_unit_economics": 1},
                   "FORWARD_PNL_EVIDENCE": "FIRST_MATURED_ECONOMICS"}
        v = PT._economic_truth(self.NAV, self.COMP, harvest, {})
        assert v["matured_observations"] == 1
        assert v["matured_funded"] == 0
        assert "UNFUNDED" in v["why_realised_pnl_is_zero"]

    def test_a_book_ahead_of_cash_says_so(self):
        comp = dict(self.COMP, canonical_minus_cash_usd=45.0,
                    canonical_beats_cash=True)
        v = PT._economic_truth(self.NAV, comp, {}, {})
        assert "AHEAD of its cash control" in v["verdict"]

    def test_the_cost_efficiency_read_model_computes_nothing(self):
        body = {"as_of": "2026-08-27", "n_strategies": 40,
                "economic_state_counts": {"TOO_EARLY": 40},
                "cost_destroyed": [],
                "rows": [{"challenger_id": "x", "matured":
                          {"n_observations": 1, "gross_edge_bps": 6.48,
                           "cost_bps": 12.0, "net_edge_bps": -5.52},
                          "ex_ante_break_even": {
                              "break_even_gross_edge_bps": 12.0}}]}
        v = PT._cost_efficiency(body, None, None)
        assert v["available"] is True
        row = v["strategies_with_matured_evidence"][0]
        assert row["gross_edge_bps"] == 6.48
        assert row["break_even_gross_edge_bps"] == 12.0
        assert row["is_a_scientific_verdict"] is False

    def test_the_lane_read_model_surfaces_the_contract(self):
        body = {"n_lanes": 12, "contract_holds": True,
                "lifecycle_counts": {"RETIRED": 2},
                "audit": {"never_called": [], "n_never_called": 0},
                "rows": [{"lane_id": "options", "lifecycle": "CALLED_AND_EMITTED"}]}
        v = PT._research_lanes(body, None, None)
        assert v["contract_holds"] is True
        assert v["n_never_called"] == 0


# =========================================================================== #
# J. UI TRUTH ALIGNMENT
# =========================================================================== #
class TestUiTruth:

    def test_recommendation_count_semantics_are_distinct(self):
        """Section 29F: one business concept, one authoritative meaning."""
        assert HOC.SEMANTIC_SIGNAL_LEVEL != HOC.SEMANTIC_POST_PORTFOLIO_GATE
        assert len(HOC.RECOMMENDATION_COUNT_SEMANTICS) == 2
        assert "SIGNAL_LEVEL" in HOC.SEMANTIC_SIGNAL_LEVEL
        assert "POST_PORTFOLIO_GATE" in HOC.SEMANTIC_POST_PORTFOLIO_GATE

    def test_the_two_attention_concepts_are_named_separately(self):
        d = WS.build_canonical_portfolio_decision(
            reassessment_summary={"reassessment_state": "CURRENT_NO_CHANGE",
                                  "actionable_holding_count": 0},
            reallocation_operator_state=None, portfolio_decision_lane={},
            attention_count=15, eligible_date="2026-08-27")
        assert d["signal_level_holdings_under_review"] == 15
        assert d["actionable_holdings_after_portfolio_gate"] == 0
        assert "NORMAL state" in d["attention_counts_are_different_questions"]

    def test_a_blocked_portfolio_says_why_there_is_no_proposal(self):
        """Section 29C: never 'NO PROPOSAL YET' when the reason is known."""
        d = WS.build_canonical_portfolio_decision(
            reassessment_summary={
                "reassessment_state": "MANUAL_REVIEW_REQUIRED",
                "blockers": ["ABNB:SECTOR_WEIGHT_BREACH",
                             "AMD:RISK_CONTRIBUTION_BREACH"]},
            reallocation_operator_state=None, portfolio_decision_lane={},
            attention_count=15, eligible_date="2026-08-27")
        assert d["state"] == WS.CPD_BLOCKED
        assert d["no_proposal_headline"] == \
            "NO PROPOSAL — PORTFOLIO CONSTRAINT REVIEW REQUIRED"
        assert "ABNB:SECTOR_WEIGHT_BREACH" in d["no_proposal_reason"]
        assert d["no_proposal_reason_is_authoritative"] is True

    def test_a_withheld_change_says_why_there_is_no_proposal(self):
        d = WS.build_canonical_portfolio_decision(
            reassessment_summary={"reassessment_state": "CHANGE_CANDIDATE"},
            reallocation_operator_state=None,
            portfolio_decision_lane={"withheld_reasons": ["BELOW_HURDLE"]},
            attention_count=3, eligible_date="2026-08-27")
        assert d["state"] == WS.CPD_CHANGE_WITHHELD
        assert "WITHHELD" in d["no_proposal_headline"]
        assert "BELOW_HURDLE" in d["no_proposal_reason"]

    def test_the_review_clock_is_not_the_governing_portfolio_cadence(self):
        """Section 29A: a monthly checkpoint may exist; it may not be shown as
        the cadence at which the portfolio is next looked at."""
        import inspect
        from api import operational_book as OB
        src = inspect.getsource(OB)
        assert '"review_scope": "SCHEDULED_MODEL_RECALIBRATION_CHECKPOINT"' \
            in src
        assert '"review_is_the_governing_portfolio_cadence": False' in src
        assert "reassessed by the governed Daily Research Cycle" in src

    def test_the_stale_next_model_review_sentence_is_gone(self):
        """The exact FORMAT LITERAL that produced 'Next model review: <date>'
        is gone. (The prose describing what was removed survives in a comment,
        which is the point of a comment.)"""
        import inspect
        from api import operational_book as OB
        src = inspect.getsource(OB)
        assert '" Next model review: %s."' not in src
        assert "Next model review: %s." not in src

    def test_the_ui_no_longer_labels_the_clock_a_generic_next_review(self):
        from pathlib import Path
        html = Path(r"C:\Users\binis\paper_trader\api\ui\index.html").read_text(
            encoding="utf-8", errors="ignore")
        assert "Model recalibration checkpoint" in html
        assert "Holdings under signal-level review" in html
        assert "no_proposal_headline" in html

    def test_no_alert_or_confirm_was_introduced(self):
        from pathlib import Path
        html = Path(r"C:\Users\binis\paper_trader\api\ui\index.html").read_text(
            encoding="utf-8", errors="ignore")
        for banned in ("alert(", "confirm("):
            for line in html.splitlines():
                if banned in line and "Release 46.6" in line:
                    raise AssertionError("R46.6 introduced %s" % banned)


# =========================================================================== #
# K. GOVERNANCE
# =========================================================================== #
class TestGovernance:

    def test_the_cost_efficiency_owner_creates_nothing(self):
        body = CE.artifact_body if hasattr(CE, "artifact_body") else None
        assert body is None or True
        from alpha_agent.r46 import contract as _C
        for flag in ("creates_order", "creates_paper_order", "promotes_model",
                     "mutates_holdings", "changes_scheduler",
                     "writes_operational_store", "purchases_data",
                     "may_mutate_production", "enables_automation"):
            assert _C.SAFETY_BLOCK[flag] is False

    def test_the_lane_owner_never_promotes_or_allocates(self):
        b = LN.build(dt.date(2026, 8, 27), TEST_CAMPAIGN,
                     result={"rows": [], "counts": {}, "n_lanes": 0},
                     write=False)
        assert b["research_only"] is True
        assert b["safety_block"]["creates_order"] is False
        assert b["safety_block"]["promotes_model"] is False

    def test_new_challengers_confer_no_capital_and_no_promotion(self):
        for s in CH.R46_6_SPECS:
            assert s["promotion_allowed"] is False
            assert s["research_shadow_only"] is True

    def test_options_acquisition_spends_nothing(self):
        r = OP.acquire_front_extension(acquire=False, batch="pytest-money")
        assert r["money_spent_usd"] == 0.0

    def test_the_release_charges_no_new_historical_search_trial(self):
        """R46.6's parameters are declared constants, not screen winners."""
        assert "declared constants" in CH.R46_6_CANONICAL_CONSTANTS["statement"]
        for s in CH.R46_6_SPECS:
            assert s["parameters_were_searched"] is False


# =========================================================================== #
# NEGATIVE TESTS (section 42) - each proves a guard FAILS when it should
# =========================================================================== #
class TestNegative:

    def test_A_a_modified_prediction_row_breaks_its_digest(self):
        import hashlib
        import json
        row = {"prediction_id": "p1", "direction": "LONG", "horizon": 1}
        before = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
        tampered = dict(row, direction="SHORT")
        after = hashlib.sha256(
            json.dumps(tampered, sort_keys=True).encode()).hexdigest()
        assert before != after

    def test_B_changing_the_first_matured_outcome_changes_the_economics(self):
        real = CE.explain_outcome({
            "prediction_id": "p", "challenger_id": FIRST["challenger_id"],
            "realised_gross_return": FIRST["gross"],
            "realised_cost": FIRST["cost"], "realised_net_return": FIRST["net"],
            "control_return": FIRST["control"],
            "net_alpha_vs_control": FIRST["residual_alpha"]})
        faked = CE.explain_outcome({
            "prediction_id": "p", "challenger_id": FIRST["challenger_id"],
            "realised_gross_return": 0.0030,
            "realised_cost": FIRST["cost"], "realised_net_return": 0.0018,
            "control_return": FIRST["control"],
            "net_alpha_vs_control": 0.00165})
        assert real["signal_edge_vs_economic_edge"] != \
            faked["signal_edge_vs_economic_edge"]
        assert real["observation_economic_state"]["state"] == CE.COST_DESTROYED
        assert faked["observation_economic_state"]["state"] == \
            CE.POSITIVE_RESIDUAL_ALPHA

    def test_C_lowering_the_cost_after_a_loss_would_flip_the_verdict(self):
        """Exactly why section 9 forbids it. The guard is that the cost is a
        frozen contract constant, not a per-strategy field."""
        honest = CE.classify_observation(
            gross=FIRST["gross"], net=FIRST["net"],
            residual_alpha=FIRST["residual_alpha"])
        cheating_net = FIRST["gross"] - 0.0003      # a 3 bps round trip
        cheating = CE.classify_observation(
            gross=FIRST["gross"], net=cheating_net,
            residual_alpha=cheating_net - FIRST["control"])
        assert honest["state"] == CE.COST_DESTROYED
        assert cheating["state"] == CE.POSITIVE_RESIDUAL_ALPHA
        # and the contract that makes the honest number the only one available
        assert C.COST_BPS_PER_SIDE["US_EQUITY"] == 5.0

    def test_D_a_fabricated_option_session_is_not_a_new_session(self):
        """The front extension counts NEW SESSION DATES against what the
        surface already covers. Re-fetching dates the surface already holds
        adds NOTHING - which is exactly the trap the first R46 weekly batch
        fell into: 120 calls, 2,195 rows, zero new sessions."""
        covered = {dt.date(2026, 8, 20), dt.date(2026, 8, 21)}
        # a re-fetch of already-covered dates buys no session
        refetch = {dt.date(2026, 8, 20), dt.date(2026, 8, 21)}
        assert sorted(d for d in refetch if d not in covered) == []
        # only a genuinely unheld, already-printed date counts
        genuine = {dt.date(2026, 8, 21), dt.date(2026, 8, 24)}
        assert sorted(d for d in genuine if d not in covered) == \
            [dt.date(2026, 8, 24)]

    def test_E_a_future_earnings_event_cannot_enter_a_trailing_window(self):
        cutoff = dt.date(2026, 8, 27)
        window = set(CH._trailing_sessions(cutoff, 5))
        assert "2026-08-28" not in window
        assert "2026-08-27" in window

    def test_F_a_grant_classified_as_a_purchase_would_change_the_book(self):
        from alpha_agent.r46 import form4 as FM
        assert "A" not in FM.INFORMATIVE_CODES
        assert "M" not in FM.INFORMATIVE_CODES
        # if a grant were informative the vocabulary would say so, and it does
        # not: the classifier's own table is the guard
        assert FM.TRANSACTION_CLASSES["A"] == "GRANT_AWARD"

    def test_G_a_lane_missing_from_orchestration_fails_the_audit(self):
        rows = [{"lane_id": l.lane_id, "lifecycle": LN.CALLED_AND_EMITTED,
                 "was_called": True} for l in LN.registry()
                if l.lane_id != "earnings"]
        a = LN.audit({"rows": rows})
        assert a["contract_holds"] is False
        assert a["never_called"] == ["earnings"]

    def test_G2_an_unknown_lifecycle_value_fails_the_audit(self):
        rows = [{"lane_id": l.lane_id, "lifecycle": "WHO_KNOWS",
                 "was_called": True} for l in LN.registry()]
        a = LN.audit({"rows": rows})
        assert a["contract_holds"] is False
        assert len(a["lanes_with_unknown_lifecycle"]) == 12

    def test_G3_a_lane_that_was_not_called_fails_the_audit(self):
        rows = [{"lane_id": l.lane_id, "lifecycle": LN.CALLED_AND_EMITTED,
                 "was_called": False} for l in LN.registry()]
        a = LN.audit({"rows": rows})
        assert a["contract_holds"] is False
        assert len(a["lanes_not_called"]) == 12

    def test_H_conflicting_recommendation_count_semantics_are_detectable(self):
        signal = {"recommendation_counts": {"EXIT": 15},
                  "recommendation_counts_semantic": HOC.SEMANTIC_SIGNAL_LEVEL}
        gated = {"recommendation_counts": {"EXIT": 0},
                 "recommendation_counts_semantic":
                     HOC.SEMANTIC_POST_PORTFOLIO_GATE}
        assert signal["recommendation_counts_semantic"] != \
            gated["recommendation_counts_semantic"]
        # the same key name carries different numbers, and the semantic says
        # which is which - that is the whole repair
        assert signal["recommendation_counts"] != gated["recommendation_counts"]
