"""Release 51 - the non-equity promotion offensive.

Covers the two things R51 added to the repository:

1. the frozen FX carry challenger (``r51_fx_xs_carry_cip``) - spec shape,
   canonical constants, sign convention, book construction, isolation, and
   the rule that no earlier cohort was dropped or retuned to admit it;
2. the promotion-distance frontier (``alpha_agent.r51.promotion_frontier``) -
   a pure ranking that never replaces the real gates, never invents a
   PROMOTION_READY, and reports every structural deficit by name.

Hermetic: no Norgate, no network, no research root, no operational store.
"""
from __future__ import annotations

import datetime as _dt
import math

import pandas as pd
import pytest

from alpha_agent.r46 import challengers as CH
from alpha_agent.r46 import contract as C46
from alpha_agent.r46 import feasibility as FE
from alpha_agent.r51 import promotion_frontier as PF


# =========================================================================== #
# 1. The frozen FX carry challenger
# =========================================================================== #
class TestR51FxCarrySpec:

    def spec(self):
        s = CH.spec_by_id("r51_fx_xs_carry_cip")
        assert s is not None
        return s

    def test_spec_is_in_the_union_and_no_prior_cohort_was_dropped(self):
        ids = [s["challenger_id"] for s in CH.ALL_SPECS]
        assert "r51_fx_xs_carry_cip" in ids
        # every earlier cohort still enters through the same door, verbatim
        for probe in ("r46_eq_xs_mom_12_1", "r46_3_eq_xs_max_lottery",
                      "r46_4_cot_xs_positioning_reversal",
                      "r46_5_pead_announcement_return_20d",
                      "r46_6_eq_xs_rev_5d_tail2"):
            assert probe in ids
        # the seed reversal cell keeps its own thesis - never edited
        seed = CH.spec_by_id("r46_eq_xs_rev_5d")
        assert "one-sided pressure" in seed["thesis"]

    def test_spec_shape_and_canonical_constants(self):
        s = self.spec()
        assert s["family"] == "FX_CARRY"
        assert s["asset_class"] == "FX"
        assert s["cohort"] == CH.R51_COHORT
        assert tuple(s["horizons"]) == (5, 20)
        assert s["control"] == C46.CONTROL_CASH
        assert s["cost_class"] == "FX_FUTURES"
        assert s["cost_class"] in C46.COST_BPS_PER_SIDE
        assert s["information_family"] == "FUTURES_CURVE"
        assert s["dependence_cluster"] == "FX_CARRY"
        assert s["promotion_allowed"] is False
        assert s["research_shadow_only"] is True
        assert s["parameters_were_searched"] is False
        assert s["expected_return_state"] == "NOT_CALIBRATED"
        p = s["parameters"]
        assert p["leg_fraction"] == CH.K51["fx_carry_leg_fraction"]
        assert p["min_pairs"] == CH.K51["fx_carry_min_pairs"] == CH.MIN_FX_PAIRS
        assert p["slope"] == CH.K51["fx_carry_slope"]

    def test_history_nominates_but_confers_no_forward_credit(self):
        s = self.spec()
        assert "R36:fx_carry_rank_historical" in s["economic_overlap_with"]
        assert "forward clock starts at zero" in s["overlap_note"]

    def test_spec_hash_is_deterministic(self):
        s = self.spec()
        assert CH.spec_hash(s) == CH.spec_hash(dict(s))
        assert CH.parameters_hash(s)
        assert CH.feature_set_hash(s)

    def test_declined_avenues_are_recorded_decisions(self):
        for key in ("fx_ppp_value_xs", "intl_short_rate_xs_carry",
                    "ml_futures_cross_section",
                    "crypto_funding_or_basis_revival",
                    "vx_fast_or_slow_variants",
                    "micro_yield_futures_challenger"):
            assert key in CH.R51_DECLINED
            assert len(CH.R51_DECLINED[key]) > 40

    def test_universe_excludes_the_dollar_index_basket(self):
        assert "&DX" not in CH.FX_CARRY_MARKETS
        assert set(CH.FX_CARRY_MARKETS) == {
            "&6A", "&6B", "&6C", "&6E", "&6J", "&6M", "&6N", "&6S"}

    def test_owner_probe_and_dispatch_are_wired(self):
        assert CH._OWNERS["_fx_carry_cip"] is CH._fx_carry_cip
        assert "_fx_carry_cip" in FE._PROBE_SYMBOLS
        assert all(sym.startswith("&6")
                   for sym in FE._PROBE_SYMBOLS["_fx_carry_cip"])


class TestR51FxCarryBook:

    CARRIES = {"6A": 0.006, "6B": -0.001, "6C": -0.017, "6E": -0.014,
               "6J": -0.029, "6M": 0.029, "6N": -0.009, "6S": -0.041}

    def _patch(self, monkeypatch, carries=None, closes_missing=()):
        carries = self.CARRIES if carries is None else carries

        def fake_curve(root, ref=None, **kw):
            if root not in carries:
                return {"root": root, "state": "INSUFFICIENT_CURVE"}
            return {"root": root, "state": "OK",
                    "carry_annualised": carries[root],
                    "front": {"symbol": root + "-2026U", "close": 1.0,
                              "last_session": "2026-08-28"},
                    "next": {"symbol": root + "-2026Z", "close": 1.0,
                             "last_session": "2026-08-28"},
                    "months_between": 3}

        def fake_closes(sym, start=None):
            if sym in closes_missing:
                return None
            idx = pd.to_datetime(["2026-08-27", "2026-08-28"])
            return pd.Series([1.0, 1.01], index=idx)

        monkeypatch.setattr(CH.MD, "futures_curve_carry", fake_curve)
        monkeypatch.setattr(CH.MD, "closes", fake_closes)

    def test_book_is_the_textbook_carry_book(self, monkeypatch):
        self._patch(monkeypatch)
        book = CH.build(CH.spec_by_id("r51_fx_xs_carry_cip"))
        assert book["state"] == "OK"
        longs = {l["instrument"] for l in book["legs"] if l["side"] == "LONG"}
        shorts = {l["instrument"] for l in book["legs"]
                  if l["side"] == "SHORT"}
        assert longs == {"&6M", "&6A", "&6B"}      # highest carry
        assert shorts == {"&6S", "&6J", "&6C"}     # lowest carry
        assert book["gross_notional"] == pytest.approx(1.0)
        assert book["net_notional"] == pytest.approx(0.0, abs=1e-12)
        assert all(l["cost_class"] == "FX_FUTURES" for l in book["legs"])
        # cost: 1.0 gross x (1.0 half-spread + 1.0 slippage) per side
        spec = CH.spec_by_id("r51_fx_xs_carry_cip")
        assert CH.expected_cost_bps(book, spec) == pytest.approx(2.0)

    def test_insufficient_pairs_is_a_state_not_a_book(self, monkeypatch):
        few = {k: v for k, v in list(self.CARRIES.items())[:4]}
        self._patch(monkeypatch, carries=few)
        book = CH.build(CH.spec_by_id("r51_fx_xs_carry_cip"))
        assert book["state"] == "INSUFFICIENT_PAIRS"
        assert book["legs"] == []

    def test_missing_continuous_series_is_skipped_not_fatal(self, monkeypatch):
        self._patch(monkeypatch, closes_missing=("&6N",))
        book = CH.build(CH.spec_by_id("r51_fx_xs_carry_cip"))
        assert book["state"] == "OK"
        assert {s["instrument"] for s in book["skipped"]} == {"&6N"}
        assert len(book["legs"]) == 4          # thirds of 7 -> 2 per side

    def test_curves_are_reported_for_audit(self, monkeypatch):
        self._patch(monkeypatch)
        book = CH.build(CH.spec_by_id("r51_fx_xs_carry_cip"))
        assert len(book["curves"]) == 8
        assert {c["instrument"] for c in book["curves"]} == set(
            CH.FX_CARRY_MARKETS)


# =========================================================================== #
# 2. The promotion-distance frontier
# =========================================================================== #
def _lb_row(cid, asset_class, state="FORWARD_PENDING", horizon=20,
            emitted=3, matured=0, eff=0.0, gate="needs more", **kw):
    return dict({"challenger_id": cid, "asset_class": asset_class,
                 "family": "F", "state": state, "horizon": horizon,
                 "forward_predictions_emitted": emitted,
                 "forward_predictions_matured": matured,
                 "effective_independent": eff,
                 "next_evidence_gate": gate,
                 "control": "CASH_COLLATERAL_AT_RISK_FREE"}, **kw)


def _sleeve(sid, blocker="NO_APPROVED_OPERATIONAL_SIGNAL", caps=None):
    return {"sleeve_id": sid,
            "declared_capabilities": caps or {},
            "r50_activation_attempt": {"remaining_blocker": blocker},
            "approval_evidence": {"verdict": "FORWARD_PENDING"}}


FIXTURE_VELOCITY = {
    "projections": {"per_cluster": [
        {"cluster": "VX_CARRY", "projected_effective_per_week": 5.0},
        {"cluster": "RATES_RV", "projected_effective_per_week": 1.0},
    ]},
    "cells": [],
}

FIXTURE_VERDICTS = {"rows": [
    {"challenger_id": "vx1", "dependence_cluster": "VX_CARRY"},
    {"challenger_id": "ra1", "dependence_cluster": "RATES_RV"},
]}


class TestPromotionFrontier:

    def _build(self, rows, sleeves, econ=None, continuation=None):
        return PF.build(
            leaderboard={"rows": rows}, velocity=FIXTURE_VELOCITY,
            verdicts=FIXTURE_VERDICTS, continuation=continuation or {},
            sleeves=sleeves, unit_economics=econ or {},
            nav_usd=100000.0, name_cap_fraction=0.10, as_of="2026-08-30")

    def test_ranking_is_weeks_ascending_and_blocked_ranks_last(self):
        rows = [
            _lb_row("vx1", "VOLATILITY", horizon=5, eff=0.0),
            _lb_row("ra1", "RATES", horizon=5, eff=0.0),
            _lb_row("cr1", "CRYPTO_MARKET_STRUCTURE", state="DATA_BLOCKED",
                    emitted=0, blocked_reason="venue"),
        ]
        out = self._build(rows, [_sleeve("sleeve_volatility_futures"),
                                 _sleeve("sleeve_rates_futures"),
                                 _sleeve("sleeve_crypto_futures")])
        ranked = out["rows"]
        assert [r["sleeve_id"] for r in ranked] == [
            "sleeve_volatility_futures", "sleeve_rates_futures",
            "sleeve_crypto_futures"]
        # 40 effective needed at 5/wk -> 8 weeks; at 1/wk -> 40 weeks
        assert ranked[0]["weeks_to_evidence_floor"] == pytest.approx(8.0)
        assert ranked[1]["weeks_to_evidence_floor"] == pytest.approx(40.0)
        assert ranked[2]["state"] == "BLOCKED"
        assert ranked[2]["weeks_to_evidence_floor"] is None

    def test_no_promotion_ready_without_forward_confirmed(self):
        out = self._build([_lb_row("vx1", "VOLATILITY", horizon=5)],
                          [_sleeve("sleeve_volatility_futures")])
        assert out["promotion_ready"] == []
        assert out["promotion_ready_count"] == 0
        assert out["manual_approval_required"] is True
        assert out["automatic_promotion_performed"] is False

    def test_forward_confirmed_with_no_deficit_is_ready_and_still_manual(self):
        rows = [_lb_row("vx1", "VOLATILITY", state="FORWARD_CONFIRMED",
                        horizon=5, matured=80, eff=45.0, gate=None)]
        econ = {"sleeve_volatility_futures": {
            "smallest_unit_symbol": "&VX",
            "smallest_unit_notional_usd": 9000.0}}
        out = self._build(rows, [_sleeve("sleeve_volatility_futures")],
                          econ=econ)
        assert out["promotion_ready"] == ["sleeve_volatility_futures"]
        assert out["rows"][0]["state"] == "PROMOTION_READY"
        assert out["manual_approval_required"] is True

    def test_granularity_is_reported_and_never_relaxes_a_cap(self):
        rows = [_lb_row("ra1", "RATES", horizon=5)]
        econ = {"sleeve_rates_futures": {
            "smallest_unit_symbol": "&ZF",
            "smallest_unit_notional_usd": 105921.0}}
        out = self._build(rows, [_sleeve("sleeve_rates_futures")], econ=econ)
        d = {x["code"]: x for x in out["rows"][0]["structural_deficits"]}
        assert PF.DEFICIT_GRANULARITY in d
        assert "Minimum NAV for one unit: $1059210" in d[
            PF.DEFICIT_GRANULARITY]["detail"]
        assert out["name_cap_fraction"] == 0.10   # unchanged, by construction

    def test_liquidity_false_capability_is_a_named_deficit(self):
        rows = [_lb_row("fx1", "FX", horizon=20)]
        sl = _sleeve("sleeve_fx_futures",
                     caps={"LIQUIDITY_SUPPORTED": False})
        out = self._build(rows, [sl])
        codes = [x["code"] for x in out["rows"][0]["structural_deficits"]]
        assert PF.DEFICIT_LIQUIDITY in codes

    def test_adopted_shadow_reads_the_continuation_not_the_stale_block(self):
        rows = [_lb_row("shadow_wide_xs", "UNDECLARED", state="DATA_BLOCKED",
                        emitted=0, blocked_reason="stale pre-46.6.1 text")]
        cont = {"lane_results": {"r39_fut_month_end": {
            "lifecycle": "CALLED_QUIET_NOT_DUE",
            "next_decision_date": "2026-08-31"}}}
        out = self._build(rows, [], continuation=cont)
        row = next(r for r in out["rows"]
                   if r["sleeve_id"].startswith("adopted_continuation"))
        assert row["state"] == "CONTINUATION_ARMED"
        codes = [x["code"] for x in row["structural_deficits"]]
        assert PF.DEFICIT_DATA_BLOCKED not in codes
        assert "CONTINUATION_CLOCK" in codes

    def test_the_score_never_replaces_the_gates(self):
        out = self._build([], [])
        assert out["the_score_never_replaces_the_gates"] is True
        assert out["gates_owner"] == \
            "alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES"
        assert PF.GATES is C46.FORWARD_EVIDENCE_GATES

    def test_build_is_pure_and_mutates_no_input(self):
        rows = [_lb_row("vx1", "VOLATILITY", horizon=5)]
        lb = {"rows": rows}
        import copy
        snap = copy.deepcopy(lb)
        sleeves = [_sleeve("sleeve_volatility_futures")]
        snap_s = copy.deepcopy(sleeves)
        PF.build(leaderboard=lb, velocity=FIXTURE_VELOCITY,
                 verdicts=FIXTURE_VERDICTS, continuation={},
                 sleeves=sleeves, unit_economics={}, nav_usd=1e5)
        assert lb == snap and sleeves == snap_s

    def test_prior_evidence_is_citation_only(self):
        for cites in PF.PRIOR_EVIDENCE.values():
            for c in cites:
                assert set(c) == {"release", "finding"}
        out = self._build([_lb_row("fx1", "FX")], [_sleeve("sleeve_fx_futures")])
        assert out["rows"][0][
            "prior_evidence_confers_no_forward_credit"] is True
