r"""Release 47 - CONSTRAINT-RESPECTING ACTIVE REALLOCATION + GOVERNED PAPER EXECUTION
+ PORTFOLIO DECISION OUTCOME TRACKING.

The release in one sentence: a normal portfolio constraint must CHANGE THE SOLUTION,
not freeze the portfolio - and once a reallocation is executed we must be able to say
later whether it actually added value.

Every scenario the release specification names is exercised here:

  A. the ideal target is already feasible          -> it is used unchanged
  B. a sector cap is breached                      -> the target re-optimises, stays feasible
  C. a single-name risk limit is breached          -> the name is reduced, capital redistributed
  D. the turnover budget is breached               -> the best target INSIDE the budget
  E. the top replacement is illiquid               -> the next feasible candidate is used
  F. a feasible alternative is not worth its cost  -> HOLD_CURRENT_BOOK
  G. no feasible portfolio exists                  -> TRUE_BLOCKER
  H. a proposal requires approval                  -> no mutation before approval
  I. one approval                                  -> exactly one paper transition
  J. an approval replay                            -> no duplicate transition
  K. paper execution                               -> holdings / cash / NAV reconcile
  L. decision evidence                             -> both paths frozen PROSPECTIVELY
  M. Release-46 research state                     -> untouched

Every test is hermetic: temp desk / proposal / decision / plan / evidence roots and
injected stores. No live endpoint, no operational ledger, no holding, cash or NAV.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_decision_outcome as pdo
from paper_trader.api import rebalance_execution as rb
from paper_trader.api import reallocation_proposal as arp
from paper_trader.api import workflow_state as ws
from paper_trader.engine import constrained_reallocation as CR
from paper_trader.engine import portfolio_decision_outcome as PDOK
from paper_trader.engine import reallocation_proposal as RP

REPO = Path(__file__).resolve().parents[1]


# =========================================================================== #
# Fixtures for the pure constraint kernel
# =========================================================================== #
def _cand(tk, *, sector="Tech", score=0.5, rank=1, adv=5.0e8):
    return {"ticker": tk, "sector": sector, "score": score, "rank": rank,
            "adv_dollar": adv}


def _pol(**over):
    p = dict(CR.default_policy())
    p.update({"target_position_count": 12, "min_position_weight": 0.01})
    p.update(over)
    return p


# =========================================================================== #
# 0. THE CONSTRAINT INVENTORY - the release's central claim, stated as DATA
# =========================================================================== #
class TestConstraintClassification:

    def test_01_every_normal_portfolio_limit_reshapes_the_solution(self):
        inv = CR.constraint_inventory()
        kinds = {r["code"]: r["kind"] for r in inv["constraints"]}
        for code in (CR.C_SECTOR_CAP, CR.C_NAME_CAP, CR.C_RISK_CONTRIBUTION,
                     CR.C_CONCENTRATION, CR.C_TURNOVER_BUDGET,
                     CR.C_LIQUIDITY_PARTICIPATION, CR.C_LIQUIDITY_FLOOR,
                     CR.C_MIN_POSITION, CR.C_MAX_POSITIONS, CR.C_CASH_BOUNDS):
            assert kinds[code] == CR.KIND_RESHAPES
            assert code not in CR.TRUE_BLOCKER_CODES

    def test_02_every_constraint_declares_what_it_does_to_the_solution(self):
        for row in CR.constraint_inventory()["constraints"]:
            assert row["reshape_action"] and row["owner"]

    def test_03_true_blockers_are_a_closed_declared_set(self):
        inv = CR.constraint_inventory()
        assert set(inv["true_blocker_codes"]) == set(CR.TRUE_BLOCKER_CODES)
        assert all(CR.is_true_blocker(c) for c in CR.TRUE_BLOCKER_CODES)

    def test_04_an_unknown_code_is_not_promoted_to_a_blocker(self):
        # Promoting the unknown is exactly how a normal cap became a freeze.
        assert CR.is_true_blocker("SECTOR_WEIGHT_CAP") is False
        assert CR.is_true_blocker("SOMETHING_NOBODY_DECLARED") is False

    def test_05_a_cap_offered_as_a_blocker_is_refused(self):
        v = CR.decide_outcome(
            solution={"feasible": True, "best_feasible_target": {"AAA": 0.1}},
            economics={"clears_switching_hurdle": True, "reason_codes": []},
            true_blockers=[{"code": CR.C_SECTOR_CAP}])
        assert v["outcome"] == CR.OUTCOME_PROPOSAL_READY
        assert v["misclassified_blockers"] == [CR.C_SECTOR_CAP]
        assert v["true_blockers"] == []

    def test_06_the_three_outcomes_are_exactly_the_declared_vocabulary(self):
        assert CR.OUTCOME_VOCAB == ("PROPOSAL_READY", "HOLD_CURRENT_BOOK",
                                    "TRUE_BLOCKER")


# =========================================================================== #
# SCENARIO A - the ideal target is already feasible
# =========================================================================== #
class TestScenarioAIdealFeasible:

    def test_10_feasible_ideal_is_used_unchanged(self):
        cands = [_cand("AAA", score=0.9), _cand("BBB", sector="Fin", score=0.8),
                 _cand("CCC", sector="Health", score=0.7)]
        ideal = {"AAA": 0.10, "BBB": 0.10, "CCC": 0.05}
        sol = CR.solve_feasible_target(
            current_weight={"AAA": 0.10, "BBB": 0.10, "CCC": 0.05},
            ideal_weight=ideal, candidates=cands, nav=100000.0, policy=_pol())
        assert sol["feasible"] is True
        assert sol["best_feasible_target"] == {k: round(v, 6)
                                               for k, v in ideal.items()}
        assert sol["constraint_adjustments"] == []
        assert sol["constraints_that_reshaped"] == []

    def test_11_a_feasible_target_is_verified_independently_of_the_solver(self):
        caps = {"AAA": 0.10, "BBB": 0.10}
        v = CR.verify_feasibility(weights={"AAA": 0.10, "BBB": 0.10}, caps=caps,
                                  sector_of={"AAA": "Tech", "BBB": "Fin"},
                                  current={}, policy=_pol())
        assert v["valid"] is True and v["violations"] == []

    def test_12_an_inherited_dust_position_is_not_a_blocker(self):
        # The minimum position size governs what a target proposes to ESTABLISH.
        # An untouched inherited dust holding must not fail the whole target -
        # that would manufacture a blocker out of a position nobody proposed.
        caps = {"AAA": 0.10, "DUST": 0.10}
        pol = _pol(min_position_weight=0.01)
        untouched = CR.verify_feasibility(
            weights={"AAA": 0.10, "DUST": 0.002}, caps=caps,
            sector_of={"AAA": "Tech", "DUST": "Fin"},
            current={"AAA": 0.10, "DUST": 0.002}, policy=pol)
        assert untouched["valid"] is True
        # ...but a target that CHANGES a name into dust is still refused.
        changed = CR.verify_feasibility(
            weights={"AAA": 0.10, "DUST": 0.002}, caps=caps,
            sector_of={"AAA": "Tech", "DUST": "Fin"},
            current={"AAA": 0.10, "DUST": 0.05}, policy=pol)
        assert changed["valid"] is False
        assert {v["code"] for v in changed["violations"]} == {CR.C_MIN_POSITION}


# =========================================================================== #
# SCENARIO B - a sector cap is breached
# =========================================================================== #
class TestScenarioBSectorCap:

    def _solve(self):
        cands = [_cand("T1", score=0.99), _cand("T2", score=0.98),
                 _cand("T3", score=0.97), _cand("T4", score=0.96),
                 _cand("F1", sector="Fin", score=0.80),
                 _cand("H1", sector="Health", score=0.79),
                 _cand("E1", sector="Energy", score=0.78)]
        ideal = {"T1": 0.10, "T2": 0.10, "T3": 0.10, "T4": 0.10}   # 0.40 Tech
        return CR.solve_feasible_target(
            current_weight={}, ideal_weight=ideal, candidates=cands,
            nav=1000000.0, policy=_pol(sector_cap_fraction=0.25))

    def test_20_the_sector_is_capped_not_the_portfolio_rejected(self):
        sol = self._solve()
        assert sol["feasible"] is True
        tech = sum(v for k, v in sol["best_feasible_target"].items()
                   if k.startswith("T"))
        assert tech <= 0.25 + 1e-9

    def test_21_the_excess_is_redistributed_to_the_next_best_names(self):
        sol = self._solve()
        assert sol["redistributed_weight"] > 0
        assert set(sol["best_feasible_target"]) & {"F1", "H1", "E1"}
        # nothing is lost: what left Tech arrived somewhere feasible or in cash
        assert sol["released_weight"] == pytest.approx(0.15, abs=1e-6)

    def test_22_the_sector_cap_is_recorded_as_a_reshaping_adjustment(self):
        sol = self._solve()
        assert CR.C_SECTOR_CAP in sol["constraints_that_reshaped"]
        rows = [a for a in sol["constraint_adjustments"]
                if a["constraint"] == CR.C_SECTOR_CAP]
        assert rows and all(r["kind"] == CR.KIND_RESHAPES for r in rows)

    def test_23_the_weakest_name_in_the_sector_gives_up_the_weight(self):
        sol = self._solve()
        tgt = sol["best_feasible_target"]
        # T1 is the strongest Tech idea and keeps its full weight; T4 the weakest.
        assert tgt.get("T1", 0.0) >= tgt.get("T4", 0.0)

    def test_24_a_sector_cap_alone_is_never_a_true_blocker(self):
        sol = self._solve()
        econ = CR.switching_economics(current_weight={},
                                      target_weight=sol["best_feasible_target"],
                                      candidates=[], nav=1000000.0,
                                      score_before=0.10, score_after=0.90)
        v = CR.decide_outcome(solution=sol, economics=econ)
        assert v["outcome"] != CR.OUTCOME_TRUE_BLOCKER


# =========================================================================== #
# SCENARIO C - a single-name risk-contribution limit is breached
# =========================================================================== #
class TestScenarioCRiskContribution:

    def _solve(self):
        cands = [_cand("BIG", score=0.99), _cand("F1", sector="Fin", score=0.80),
                 _cand("H1", sector="Health", score=0.79)]
        return CR.solve_feasible_target(
            current_weight={"BIG": 0.10}, ideal_weight={"BIG": 0.10, "F1": 0.05},
            candidates=cands, nav=1000000.0,
            risk_contributions={"BIG": 0.50, "F1": 0.10},
            policy=_pol(max_name_risk_contribution=0.25))

    def test_30_the_position_is_reduced_to_the_compliant_level(self):
        sol = self._solve()
        # risk share 0.50 against a 0.25 cap -> the weight is halved
        assert sol["best_feasible_target"]["BIG"] == pytest.approx(0.05, abs=1e-6)

    def test_31_the_released_capital_is_redistributed(self):
        sol = self._solve()
        assert sol["redistributed_weight"] == pytest.approx(0.05, abs=1e-6)
        assert sol["best_feasible_target"].get("H1", 0.0) > 0 or \
            sol["best_feasible_target"]["F1"] > 0.05

    def test_32_the_portfolio_is_not_rejected(self):
        sol = self._solve()
        assert sol["feasible"] is True and sol["blockers"] == []
        assert CR.C_RISK_CONTRIBUTION in sol["constraints_that_reshaped"]


# =========================================================================== #
# SCENARIO D - the turnover budget is breached
# =========================================================================== #
class TestScenarioDTurnoverBudget:

    def _solve(self, budget=0.10):
        cands = [_cand("A", score=0.20), _cand("B", sector="Fin", score=0.25),
                 _cand("N1", sector="Health", score=0.99),
                 _cand("N2", sector="Energy", score=0.60)]
        # Every leg here is DISCRETIONARY: the current weights are inside every cap,
        # so the budget is the only thing that binds. (Constraint-mandated legs are
        # exercised separately in test_44.)
        return CR.solve_feasible_target(
            current_weight={"A": 0.25, "B": 0.25},
            ideal_weight={"N1": 0.25, "N2": 0.25},
            candidates=cands, nav=1000000.0,
            policy=_pol(max_one_way_turnover=budget, max_name_weight=0.25))

    def test_40_the_result_fits_inside_the_budget(self):
        sol = self._solve(budget=0.10)
        assert sol["turnover"]["budget_binds"] is True
        assert sol["turnover"]["achieved_one_way_turnover"] <= 0.10 + 1e-9
        assert sol["feasible"] is True

    def test_41_the_best_trades_are_the_ones_that_survive(self):
        sol = self._solve(budget=0.10)
        acc = {t["ticker"] for t in sol["turnover"]["accepted_trades"]}
        dfr = {t["ticker"] for t in sol["turnover"]["deferred_trades"]}
        # N1 (score 0.99) is the highest score improvement per unit of turnover.
        assert "N1" in acc
        assert dfr and "N1" not in dfr

    def test_42_the_ordering_basis_is_declared_and_first_order_only(self):
        sol = self._solve()
        t = sol["turnover"]
        assert t["ordering_basis"] == \
            "SCORE_IMPROVEMENT_PER_UNIT_OF_ONE_WAY_TURNOVER"
        assert t["ordering_is_first_order_only"] is True

    def test_43_a_generous_budget_does_not_bind(self):
        sol = self._solve(budget=1.0)
        assert sol["turnover"]["budget_binds"] is False
        assert sol["turnover"]["achieved_one_way_turnover"] > 0.10

    def test_44_constraint_mandated_exits_outrank_the_budget(self):
        # ZZZ is not in the eligible universe: its exit is a constraint, not a bet.
        sol = CR.solve_feasible_target(
            current_weight={"ZZZ": 0.40, "A": 0.10},
            ideal_weight={"A": 0.10},
            candidates=[_cand("A", score=0.5)], nav=1000000.0,
            policy=_pol(max_one_way_turnover=0.05))
        assert sol["mandatory_exits"] == ["ZZZ"]
        assert sol["turnover"]["budget_subordinated_to_mandatory_constraints"] \
            is True
        assert "ZZZ" not in sol["best_feasible_target"]


# =========================================================================== #
# SCENARIO E - the top replacement is illiquid
# =========================================================================== #
class TestScenarioEIlliquidCandidate:

    def test_50_an_illiquid_top_candidate_is_skipped_for_the_next_feasible_one(self):
        pol = _pol(min_adv_dollar=1.0e7)
        cands = [_cand("ILQ", score=0.99, adv=1.0e5),      # below the ADV floor
                 _cand("NXT", sector="Fin", score=0.90, adv=5.0e8)]
        sol = CR.solve_feasible_target(
            current_weight={}, ideal_weight={"ILQ": 0.10}, candidates=cands,
            nav=1000000.0, policy=pol)
        assert "ILQ" not in sol["best_feasible_target"]
        assert sol["best_feasible_target"].get("NXT", 0.0) > 0
        assert CR.C_LIQUIDITY_FLOOR in sol["constraints_that_reshaped"]
        assert sol["feasible"] is True

    def test_51_a_participation_cap_sizes_a_position_it_does_not_veto_one(self):
        # ADV of $1m against a $10m book: 10% of NAV is $1m -> exactly 1x ADV.
        pol = _pol(max_adv_participation=0.5, min_adv_dollar=1.0e5)
        cands = [_cand("THIN", score=0.99, adv=1.0e6),
                 _cand("DEEP", sector="Fin", score=0.90, adv=5.0e8)]
        sol = CR.solve_feasible_target(
            current_weight={}, ideal_weight={"THIN": 0.10}, candidates=cands,
            nav=10000000.0, policy=pol)
        assert sol["best_feasible_target"]["THIN"] == pytest.approx(0.05, abs=1e-9)
        assert CR.C_LIQUIDITY_PARTICIPATION in sol["constraints_that_reshaped"]


# =========================================================================== #
# SCENARIO F - a feasible alternative that is not worth its cost
# =========================================================================== #
class TestScenarioFHoldCurrentBook:

    def test_60_below_the_hurdle_is_hold_not_blocked(self):
        econ = CR.switching_economics(
            current_weight={"A": 0.5}, target_weight={"B": 0.5}, candidates=[],
            nav=100000.0, score_before=0.50, score_after=0.51,
            score_cost_hurdle=0.02, turnover_one_way=0.5)
        assert econ["clears_switching_hurdle"] is False
        v = CR.decide_outcome(
            solution={"feasible": True, "best_feasible_target": {"B": 0.5}},
            economics=econ)
        assert v["outcome"] == CR.OUTCOME_HOLD_CURRENT_BOOK
        assert v["feasible_target_exists"] is True
        assert v["requires_manual_approval"] is False

    def test_61_above_the_hurdle_is_proposal_ready(self):
        econ = CR.switching_economics(
            current_weight={"A": 0.5}, target_weight={"B": 0.5}, candidates=[],
            nav=100000.0, score_before=0.30, score_after=0.90,
            score_cost_hurdle=0.02, turnover_one_way=0.5)
        assert econ["clears_switching_hurdle"] is True
        v = CR.decide_outcome(
            solution={"feasible": True, "best_feasible_target": {"B": 0.5}},
            economics=econ)
        assert v["outcome"] == CR.OUTCOME_PROPOSAL_READY
        assert v["requires_manual_approval"] is True
        assert v["authorises_execution"] is False

    def test_62_the_hurdle_is_frozen_and_never_tuned_on_outcomes(self):
        econ = CR.switching_economics(current_weight={}, target_weight={},
                                      candidates=[], nav=1.0)
        assert econ["hurdle_frozen"] is True
        assert econ["hurdle_tuned_on_outcomes"] is False
        assert econ["switching_hurdle"] == \
            CR.default_policy()["min_switching_net_improvement"]

    def test_63_a_mandatory_exit_is_not_subject_to_the_economic_hurdle(self):
        econ = CR.switching_economics(
            current_weight={"ZZZ": 0.2, "A": 0.3}, target_weight={"A": 0.3},
            candidates=[], nav=100000.0, mandatory_exits=["ZZZ"],
            score_before=0.5, score_after=0.5)
        assert econ["mandatory_exit_only_change"] is True
        assert econ["clears_switching_hurdle"] is True

    def test_64_no_expected_dollar_return_is_ever_fabricated(self):
        econ = CR.switching_economics(current_weight={}, target_weight={},
                                      candidates=[], nav=1.0)
        assert econ["expected_return_state"] == "NOT_CALIBRATED"
        assert econ["expected_return_before"] is None
        assert econ["expected_return_after"] is None


# =========================================================================== #
# SCENARIO G - no feasible portfolio exists
# =========================================================================== #
class TestScenarioGTrueBlocker:

    def test_70_a_declared_true_blocker_blocks(self):
        v = CR.decide_outcome(
            solution={"feasible": True, "best_feasible_target": {"A": 0.1}},
            economics={"clears_switching_hurdle": True, "reason_codes": []},
            true_blockers=[{"code": CR.B_STALE_MARKET_DATA}])
        assert v["outcome"] == CR.OUTCOME_TRUE_BLOCKER
        assert v["reason_codes"] == [CR.B_STALE_MARKET_DATA]

    def test_71_an_empty_feasible_set_blocks(self):
        v = CR.decide_outcome(
            solution={"feasible": False, "best_feasible_target": {}},
            economics={"clears_switching_hurdle": True})
        assert v["outcome"] == CR.OUTCOME_TRUE_BLOCKER
        assert v["reason_codes"] == [CR.B_NO_FEASIBLE_PORTFOLIO]

    def test_72_impossible_liquidity_blocks(self):
        pol = _pol(min_adv_dollar=1.0e9)
        sol = CR.solve_feasible_target(
            current_weight={}, ideal_weight={"A": 0.1},
            candidates=[_cand("A", adv=1.0e5), _cand("B", adv=2.0e5)],
            nav=1000000.0, policy=pol)
        assert sol["blockers"] and sol["blockers"][0]["code"] == \
            CR.B_IMPOSSIBLE_LIQUIDITY
        v = CR.decide_outcome(solution=sol, economics={
            "clears_switching_hurdle": True})
        assert v["outcome"] == CR.OUTCOME_TRUE_BLOCKER

    def test_73_all_cash_is_a_valid_answer_not_a_blocker(self):
        # Nothing eligible, but the book HOLDS something: exiting to cash is a real,
        # feasible portfolio decision, not a failure to compute one.
        sol = CR.solve_feasible_target(
            current_weight={"OLD": 0.5}, ideal_weight={}, candidates=[],
            nav=100000.0, policy=_pol())
        assert sol["feasible"] is True and sol["blockers"] == []
        assert sol["best_feasible_target"] == {}
        assert sol["mandatory_exits"] == ["OLD"]


# =========================================================================== #
# THE PROPOSAL ENGINE - a breached limit RE-OPTIMISES instead of freezing
# =========================================================================== #
def _rets(n, seed=1):
    return [(((i * 7 + seed * 13) % 21) - 10) / 1000.0 for i in range(n)]


def _aligned(tickers, n=80):
    return {"dates": ["d%03d" % i for i in range(n)],
            "series": {tk: _rets(n, seed=i + 1) for i, tk in enumerate(tickers)}}


def _position(tk, sector, w, mv):
    return {"ticker": tk, "sector": sector, "current_weight": w,
            "market_value": mv, "quantity": 100, "price": mv / 100.0}


def _urow(tk, rank, pct, sector="Tech", adv=5e8, eligible=True):
    return {"ticker": tk, "rank": rank, "percentile": pct, "combined_score": pct,
            "sector": sector, "adv_dollar": adv, "eligible": eligible}


def _review(tk, rec, rank, pct, sector="Tech", repl=None):
    return {"ticker": tk, "recommendation": rec, "current_rank": rank,
            "current_score": pct, "signal_strength": pct,
            "strongest_replacement_ticker": repl, "drawdown_60d": -0.1,
            "liquidity_state": "LIQUID", "switching_cost_usd": 10.0,
            "net_improvement": 0.5, "risk_contribution": 0.1}


def _engine_ic(**over):
    held = ["AAA", "BBB", "CCC", "DDD"]
    cands = ["EEE", "FFF", "GGG", "HHH"]
    ic = {
        "schema_version": RP.INPUT_SCHEMA_VERSION,
        "eligible_market_date": "2026-08-27",
        "active_book_id": "alpha_paper_book_1",
        "nav": 100000.0, "cash": 0.0,
        "portfolio_state_hash": "PSH", "universe_scoring_hash": "USH",
        "universe_input_contract_hash": "UIC",
        "hoc_assessment_hash": "HOC1", "hoc_assessment_state": "READY",
        "hoc_available": True, "hoc_data_gaps": [],
        "positions": [_position("AAA", "Tech", 0.25, 25000.0),
                      _position("BBB", "Tech", 0.25, 25000.0),
                      _position("CCC", "Fin", 0.25, 25000.0),
                      _position("DDD", "Fin", 0.25, 25000.0)],
        "hoc_reviews": [_review("AAA", "HOLD", 5, 0.90),
                        _review("BBB", "EXIT", 90, 0.05),
                        _review("CCC", "EXIT", 95, 0.04, sector="Fin"),
                        _review("DDD", "EXIT", 96, 0.03, sector="Fin")],
        # Every held name stays ELIGIBLE (merely unattractive), so no exit here is a
        # mandatory constraint exit and the turnover budget is the only thing that
        # binds. The conflicting-mandatory-constraint case is test_87.
        "universe_rows": [_urow("AAA", 5, 0.90),
                          _urow("BBB", 40, 0.05),
                          _urow("CCC", 45, 0.04, sector="Fin"),
                          _urow("DDD", 46, 0.03, sector="Fin"),
                          _urow("EEE", 1, 0.99, sector="Health"),
                          _urow("FFF", 2, 0.98, sector="Energy"),
                          _urow("GGG", 3, 0.97, sector="Fin"),
                          _urow("HHH", 4, 0.96, sector="Tech")],
        "aligned_returns": _aligned(held + cands),
    }
    ic.update(over)
    return ic


def _engine_pol(**over):
    p = dict(RP.default_policy())
    p.update({"target_position_count": 5, "max_name_weight": 0.25,
              "sector_cap_fraction": 1.0, "min_covariance_obs": 20,
              "min_volatility_coverage": 0.5, "candidate_rank_max": 50,
              "min_position_weight": 0.01, "max_one_way_turnover": 1.0})
    p.update(over)
    return p


class TestProposalEngineReoptimises:

    def test_80_a_breached_turnover_budget_no_longer_freezes_the_portfolio(self):
        res = RP.build_proposal(input_contract=_engine_ic(),
                                policy=_engine_pol(max_one_way_turnover=0.20))
        reopt = res["constraint_reoptimization"]
        assert reopt["ideal_target_was_feasible"] is False
        assert RP.CT_TURNOVER_BUDGET in reopt["breached_limits"]
        assert reopt["applied"] is True
        # the published target now FITS the budget and is a real, complete target
        assert res["turnover"]["one_way_turnover"] <= 0.20 + 1e-9
        assert res["proposal_state"] != RP.STATE_WITHHELD
        assert res["outcome"] in CR.OUTCOME_VOCAB
        assert res["reallocation_outcome"]["feasible_target_exists"] is True

    def test_81_the_ideal_target_is_preserved_next_to_the_feasible_one(self):
        res = RP.build_proposal(input_contract=_engine_ic(),
                                policy=_engine_pol(max_one_way_turnover=0.20))
        reopt = res["constraint_reoptimization"]
        assert reopt["ideal_target"]                      # what we wanted
        assert reopt["constraint_adjustments"]            # what changed it
        assert reopt["constraints_that_reshaped"]         # and which limits did
        published = {a["ticker"]: a["proposed_weight"] for a in res["allocations"]
                     if a["proposed_weight"] > 0}
        assert published != reopt["ideal_target"]

    def test_82_a_feasible_ideal_target_is_left_alone(self):
        res = RP.build_proposal(input_contract=_engine_ic(),
                                policy=_engine_pol(max_one_way_turnover=1.0))
        reopt = res["constraint_reoptimization"]
        assert reopt["ideal_target_was_feasible"] is True
        assert reopt["applied"] is False
        assert reopt["constraint_adjustments"] == []

    def test_83_the_repaired_target_still_satisfies_every_hard_constraint(self):
        res = RP.build_proposal(input_contract=_engine_ic(),
                                policy=_engine_pol(max_one_way_turnover=0.20))
        assert res["constraints"]["all_ok"] is True
        assert res["complete_target_limits"]["all_ok"] is True

    def test_84_actions_follow_the_repaired_weights_not_the_intention(self):
        res = RP.build_proposal(input_contract=_engine_ic(),
                                policy=_engine_pol(max_one_way_turnover=0.20))
        for row in res["allocations"]:
            pw, cw = row["proposed_weight"], row["current_weight"]
            if row["action"] == "EXIT":
                assert pw == 0.0
            if row["action"] == "ADD":
                assert pw > 0 and cw == 0.0
            if row["action"] == "RETAIN":
                assert abs(pw - cw) <= res["policy"]["material_weight_delta"] + 1e-9

    def test_85_switching_economics_are_delegated_never_recomputed(self):
        res = RP.build_proposal(input_contract=_engine_ic(),
                                policy=_engine_pol(max_one_way_turnover=0.20))
        sw, sig, trn = (res["switching_economics"], res["signal"], res["turnover"])
        assert sw["score_before"] == sig["score_before"]
        assert sw["score_after"] == sig["score_after"]
        assert sw["one_way_turnover"] == trn["one_way_turnover"]
        assert sw["estimated_transaction_cost"] == trn["estimated_transaction_cost"]
        assert all(sw["delegated_inputs"].values())

    def test_86_the_proposal_carries_the_constraint_inventory(self):
        res = RP.build_proposal(input_contract=_engine_ic(),
                                policy=_engine_pol())
        inv = res["constraint_inventory"]
        assert inv["reshaping_count"] >= 10
        assert inv["true_blocker_count"] == len(CR.TRUE_BLOCKER_CODES)

    def test_87_conflicting_mandatory_constraints_still_fail_closed(self):
        # Three held names leave the eligible universe. Their exit is mandatory and
        # alone exceeds a very tight turnover budget: two MANDATORY constraints are
        # in conflict, which is a decision a person owns. The fail-closed WITHHELD
        # path is intact - it is simply no longer the answer to a normal cap.
        ic = _engine_ic(universe_rows=[_urow("AAA", 5, 0.90),
                                       _urow("EEE", 1, 0.99, sector="Health")])
        res = RP.build_proposal(input_contract=ic,
                                policy=_engine_pol(max_one_way_turnover=0.05))
        assert res["proposal_state"] == RP.STATE_WITHHELD
        assert res["approvable"] is False
        assert res["outcome"] == CR.OUTCOME_TRUE_BLOCKER

    def test_88_a_withheld_proposal_is_never_approvable(self):
        assert RP.STATE_WITHHELD not in RP.APPROVABLE_STATES
        assert arp.STATE_WITHHELD not in arp.APPROVABLE_READ_STATES

    def test_89_current_holdings_receive_no_investment_privilege(self):
        sol = CR.solve_feasible_target(
            current_weight={"WEAK": 0.20},
            ideal_weight={"STRONG": 0.10},
            candidates=[_cand("WEAK", score=0.05),
                        _cand("STRONG", sector="Fin", score=0.99)],
            nav=1000000.0, policy=_pol())
        # the held-but-weak name is not carried into the target just because it is held
        assert sol["best_feasible_target"].get("WEAK", 0.0) == 0.0
        assert sol["best_feasible_target"]["STRONG"] > 0
        assert CR.INCUMBENCY_POLICY == \
            "NO_INVESTMENT_PRIVILEGE_ONLY_PRICED_TRANSITION_COST"

    def test_90_incumbency_enters_only_as_a_priced_transition_cost(self):
        econ = CR.switching_economics(current_weight={"A": 0.5},
                                      target_weight={"B": 0.5}, candidates=[],
                                      nav=100000.0, score_before=0.4,
                                      score_after=0.5)
        assert econ["incumbency_advantage_applied"] == "TRANSITION_COST_ONLY"
        assert econ["estimated_transaction_cost"] is not None


# =========================================================================== #
# THE OPERATOR SURFACE - never "blocked" while a feasible target exists
# =========================================================================== #
class TestOperatorTruth:

    def test_100_hold_current_book_is_its_own_state_not_a_blocker(self):
        lane = pdec.derive_decision_state(
            has_active_book=True,
            proposal_summary={"reallocation_proposal_available": True,
                              "reallocation_proposal_hash": "H",
                              "reallocation_action_counts": {"EXIT": 1, "ADD": 1},
                              "reallocation_outcome": CR.OUTCOME_HOLD_CURRENT_BOOK,
                              "reallocation_feasible_target_exists": True,
                              "reallocation_withheld_reasons": []},
            decision_record=None)
        assert lane["portfolio_decision_state"] == pdec.PDS_HOLD_CURRENT_BOOK
        assert lane["approvable"] is False
        assert lane["hold_current_book"] is True
        assert lane["feasible_target_exists"] is True

    def test_101_the_canonical_decision_says_hold_not_withheld(self):
        d = ws.build_canonical_portfolio_decision(
            reassessment_summary={}, reallocation_operator_state=ws.RPS_READY,
            portfolio_decision_lane={
                "portfolio_decision_state": pdec.PDS_HOLD_CURRENT_BOOK,
                "reallocation_outcome": CR.OUTCOME_HOLD_CURRENT_BOOK,
                "hold_current_book": True, "feasible_target_exists": True,
                "constraints_that_reshaped": [CR.C_SECTOR_CAP]},
            attention_count=0, eligible_date="2026-08-27")
        assert d["state"] == ws.CPD_HOLD_CURRENT_BOOK
        assert d["headline"] == "HOLD THE CURRENT BOOK"
        assert "blocked" not in (d["no_proposal_reason"] or "").lower()
        assert d["constraints_that_reshaped"] == [CR.C_SECTOR_CAP]

    def test_102_blocked_while_a_feasible_target_exists_is_a_violation(self):
        v = ws.check_decision_semantics(
            reallocation_operator_state=ws.RPS_READY, reallocation_approvable=True,
            reassessment_state="PROPOSAL_READY", reassessment_proposal_required=True,
            portfolio_decision_state=pdec.PDS_REVIEW_REQUIRED,
            portfolio_decision_requires_review=True,
            portfolio_decision_approvable=True,
            proposal_bound_reassessment_hash=None, current_reassessment_hash=None,
            mandatory_exit_tickers=[], mandatory_exit_obligation="NONE",
            reallocation_outcome=CR.OUTCOME_TRUE_BLOCKER,
            feasible_target_exists=True,
            constraints_that_reshaped=[CR.C_SECTOR_CAP])
        assert "BLOCKED_WHILE_FEASIBLE_TARGET_EXISTS" in {x["code"] for x in v}

    def test_103_hold_current_book_may_never_be_exposed_as_approvable(self):
        v = ws.check_decision_semantics(
            reallocation_operator_state=ws.RPS_READY, reallocation_approvable=True,
            reassessment_state="PROPOSAL_READY", reassessment_proposal_required=True,
            portfolio_decision_state=pdec.PDS_HOLD_CURRENT_BOOK,
            portfolio_decision_requires_review=False,
            portfolio_decision_approvable=True,
            proposal_bound_reassessment_hash=None, current_reassessment_hash=None,
            mandatory_exit_tickers=[], mandatory_exit_obligation="NONE",
            reallocation_outcome=CR.OUTCOME_HOLD_CURRENT_BOOK,
            feasible_target_exists=True, constraints_that_reshaped=[])
        assert "HOLD_CURRENT_BOOK_EXPOSED_AS_APPROVABLE" in {x["code"] for x in v}

    def test_104_recording_a_decision_on_a_hold_outcome_is_refused(self, tmp_path):
        out = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact={"proposal_id": "p1", "identity": {"proposal_hash": "H",
                                                        "active_book_id": "b",
                                                        "eligible_market_date": "d"}},
            proposal_summary={"reallocation_proposal_available": True,
                              "reallocation_proposal_hash": "H",
                              "reallocation_action_counts": {"EXIT": 1},
                              "reallocation_outcome": CR.OUTCOME_HOLD_CURRENT_BOOK,
                              "reallocation_outcome_reason_codes":
                                  ["NET_IMPROVEMENT_BELOW_SWITCHING_HURDLE"]},
            decision_dir=tmp_path / "dec")
        assert out["status"] == pdec.PDS_HOLD_CURRENT_BOOK
        assert out["recorded"] is False
        assert not (tmp_path / "dec").exists()


# =========================================================================== #
# THE LIVE FREEZE - a per-holding cap breach must ASK for a target, not stop
#
# Observed on the real 2026-08-28 book while validating this release in a
# browser: the operator surface read "MANUAL REVIEW REQUIRED - review the
# portfolio constraint breach" over seven per-name breaches (six
# SECTOR_WEIGHT_BREACH, one RISK_CONTRIBUTION_BREACH), and NO proposal was
# produced at all - so the constraint re-optimiser never even ran. That is the
# same defect this release exists to remove, one layer upstream.
# =========================================================================== #
class TestHeldNameBreachAsksForATarget:

    @staticmethod
    def _reassess(breach_codes):
        from paper_trader.api import portfolio_reassessment as PRS
        from paper_trader.engine import portfolio_reassessment as K
        reviews = []
        for i in range(25):
            row = {
                "ticker": "T%02d" % i, "sector": "Tech", "current_quantity": 100,
                "current_weight": 0.04, "market_value": 4000.0,
                "current_rank": i + 1, "previous_rank": i + 1, "rank_change": 0,
                "current_score": 0.80, "score_components": {},
                "signal_strength": 0.80,
                "deterioration_state": K.hoc_kernel.DET_STABLE,
                "deterioration_reason_codes": [], "return_5d": 0.01,
                "return_20d": 0.02, "return_60d": 0.05, "volatility_20d": 0.20,
                "volatility_60d": 0.22, "drawdown_60d": -0.05,
                "risk_contribution_pct": 0.04, "concentration_contribution": 0.04,
                "median_dollar_volume_20d": 5.0e7,
                "estimated_days_to_liquidate": 0.1,
                "liquidity_state": K.hoc_kernel.LIQ_LIQUID,
                "strongest_replacement_ticker": None, "replacement_rank": None,
                "replacement_score": None, "replacement_sector": None,
                "gross_score_improvement": None, "risk_adjusted_improvement": None,
                "switching_cost_bps": 25.0, "switching_cost_usd": 10.0,
                "net_improvement": None, "recommendation": K.REC_HOLD,
                "recommendation_confidence": "HIGH",
                "reason_codes": list(breach_codes) if i < 2 else [],
                "explanation": "seed", "required_data_complete": True}
            reviews.append(row)
        hoc = {
            "schema_version": "holding_opportunity_cost.v1",
            "eligible_market_date": "2026-08-28", "active_book_id": "b1",
            "assessment_state": "READY", "assessment_hash": "h1",
            "policy": {"policy_version": "hoc_decision_policy.v1"},
            "portfolio_summary": {
                "nav": 100000.0, "cash": 0.0, "invested_value": 100000.0,
                "holdings_count": 25, "max_name_weight": 0.04,
                "max_name_ticker": "T00", "max_sector_weight": 1.0,
                "max_sector": "Tech", "sector_weights": {"Tech": 1.0},
                "herfindahl_index": 0.04, "portfolio_variance_daily": 0.0001,
                "risk_contribution_state": "AVAILABLE"},
            "recommendation_counts": {"HOLD": 25, "REDUCE": 0, "EXIT": 0,
                                      "REPLACE": 0, "ADD": 0},
            "holding_reviews": reviews, "addition_candidates": [],
            "diagnostics": {"eligible_universe_size": 503},
            "data_quality": {"data_gaps": []},
            "provenance": {"portfolio_state_hash": "ps1",
                           "economic_state_hash": "e1",
                           "corporate_actions_hash": None,
                           "universe_scoring_hash": "us1",
                           "hoc_assessment_hash": "h1"}}
        ps = {"dates": {"eligible_market_date": "2026-08-28",
                        "valuation_date": "2026-08-28"},
              "active_book": {"book_id": "b1", "book_label": "B", "status": "ACTIVE",
                              "initialized": True, "holdings_count": 25},
              "capital": {"nav": 100000.0, "cash": 0.0},
              "state_hash": "ps1", "economic_state_hash": "e1",
              "corporate_actions": {}}
        sc = {"output_hash": "us1", "input_contract_hash": "uic1",
              "strategy_id": "s", "strategy_version": "v1",
              "primary_model_id": "s", "eligible_market_date": "2026-08-28"}
        fr = {"eligible_market_date": "2026-08-28", "sources": []}
        return PRS.run_reassessment(
            input_contract=PRS.build_input_contract(
                portfolio_state=ps, scoring=sc, hoc_assessment=hoc, freshness=fr,
                recent_change_history=[], corporate_action_stale=None,
                policy=K.default_policy()))["reassessment"]

    def test_105_a_held_name_sector_breach_asks_for_a_target(self):
        from paper_trader.engine import portfolio_reassessment as K
        res = self._reassess(["SECTOR_WEIGHT_BREACH"])
        assert res["reassessment_state"] == K.STATE_PROPOSAL_READY
        assert res["reassessment_state"] != K.STATE_MANUAL_REVIEW
        codes = {b if isinstance(b, str) else b.get("code")
                 for b in (res.get("blockers") or [])}
        assert not any("SECTOR_WEIGHT_BREACH" in str(c) for c in codes)

    def test_106_a_held_name_risk_breach_asks_for_a_target(self):
        from paper_trader.engine import portfolio_reassessment as K
        res = self._reassess(["RISK_CONTRIBUTION_BREACH"])
        assert res["reassessment_state"] == K.STATE_PROPOSAL_READY
        codes = {b if isinstance(b, str) else b.get("code")
                 for b in (res.get("blockers") or [])}
        assert not any("RISK_CONTRIBUTION_BREACH" in str(c) for c in codes)

    def test_107_the_ask_names_why(self):
        from paper_trader.engine import portfolio_reassessment as K
        res = self._reassess(["SECTOR_WEIGHT_BREACH"])
        dec = res["decision"]
        assert K.GATE_HELD_NAME_BREACH_REQUIRES_TARGET in (
            dec.get("reason_codes") or [])
        # the breaching names stay VISIBLE - they are the reason for the ask
        assert dec["held_name_constraint_breaches"]
        assert dec["held_name_constraint_breach_effect"] == \
            K.GATE_HELD_NAME_BREACH_REQUIRES_TARGET
        assert dec["proposal_required"] is True

    def test_108_the_per_name_deferral_is_declared(self):
        from paper_trader.engine import portfolio_reassessment as K
        own = K.constraint_ownership()
        blk = own["per_name_deferred_to_complete_target"]
        assert blk["owner"] == K.CONSTRAINT_OWNER_COMPLETE_TARGET
        assert set(blk["constraints"]) == set(K.HELD_NAME_CONSTRAINT_BREACH_CODES)
        assert blk["authorises_nothing"] is True
        # ...and every one of them is a RESHAPING constraint in the canonical
        # inventory, which is why deferring them is correct rather than lenient.
        reshaping = {r["code"] for r in CR.constraint_inventory()["constraints"]
                     if r["kind"] == CR.KIND_RESHAPES}
        assert {CR.C_NAME_CAP, CR.C_SECTOR_CAP, CR.C_RISK_CONTRIBUTION} <= reshaping

    def test_109_no_clean_book_is_pushed_into_asking(self):
        from paper_trader.engine import portfolio_reassessment as K
        res = self._reassess([])
        assert res["reassessment_state"] != K.STATE_PROPOSAL_READY
        assert res["decision"]["held_name_constraint_breaches"] == []
        assert res["decision"]["held_name_constraint_breach_effect"] is None


# =========================================================================== #
# SCENARIOS H-K - approval, one transition, replay, reconciliation
# =========================================================================== #
COST = desk.COST_RATE_PER_SIDE


def _buy_fill(book_id, tk, qty, price, date, fid):
    gross = qty * price
    cost = gross * COST
    return {"event": "PAPER_FILL", "fill": {
        "fill_id": fid, "order_id": "ord_%s" % fid, "book_id": book_id,
        "ticker": tk, "side": desk.SIDE_BUY, "quantity": qty, "fill_date": date,
        "fill_price": price, "gross_value": round(gross, 2),
        "transaction_cost": round(cost, 4),
        "net_cash_delta": round(-(gross + cost), 4),
        "execution_model": "NEXT_CLOSE", "immutable": True}}


def _write_marks(sdir: Path, series: dict, latest: str):
    desk._atomic_write_json(sdir / desk.MARKS_FILE, {
        "phase": "TEST", "kind": "provider_cache_not_a_ledger", "series": series,
        "latest_completed_date": latest, "updated_at": "2026-01-01T00:00:00+00:00"})


_SERIES_D10 = {
    "AAA": [["2026-01-05", 100.0], ["2026-01-10", 110.0]],
    "BBB": [["2026-01-05", 200.0], ["2026-01-10", 190.0]],
    "CCC": [["2026-01-05", 50.0], ["2026-01-10", 52.0]],
    "SPY": [["2026-01-05", 400.0], ["2026-01-10", 405.0]],
}


def _setup_execution(tmp: Path):
    """Held AAA 40 / BBB 50; approved proposal INCREASE AAA, ADD CCC, EXIT BBB."""
    sdir = tmp / "desk"
    sdir.mkdir(parents=True, exist_ok=True)
    book = {"book_id": "alpha_paper_book_1", "book_number": 1,
            "display_name": "Paper Book #1", "initial_capital": 100000.0,
            "execution_model": "NEXT_CLOSE", "currency": "USD_PAPER",
            "benchmark": "SPY", "status": "OPEN",
            "model_id": "fundamental_momentum_50_50_v1"}
    desk._append_ledger(sdir, desk.BOOKS_FILE,
                        [{"event": "BOOK_CREATED", "book": book}])
    desk._append_ledger(sdir, desk.FILLS_FILE, [
        _buy_fill("alpha_paper_book_1", "AAA", 40, 100.0, "2026-01-05", "f_AAA"),
        _buy_fill("alpha_paper_book_1", "BBB", 50, 200.0, "2026-01-05", "f_BBB")])
    _write_marks(sdir, _SERIES_D10, "2026-01-10")
    nav = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))["nav"]
    allocations = [
        {"ticker": "AAA", "action": "INCREASE", "sector": "Tech",
         "proposed_weight": 0.08, "current_weight": 0.044,
         "current_market_value": 4400.0, "proposed_market_value": 8000.0},
        {"ticker": "CCC", "action": "ADD", "sector": "Health",
         "proposed_weight": 0.06, "current_weight": 0.0,
         "current_market_value": 0.0, "proposed_market_value": 6000.0},
        {"ticker": "BBB", "action": "EXIT", "sector": "Energy",
         "proposed_weight": 0.0, "current_weight": 0.095,
         "current_market_value": 9500.0, "proposed_market_value": 0.0},
    ]
    art = {"proposal_id": "reap_r47", "schema_version": "reallocation_proposal.v1",
           "identity": {"active_book_id": "alpha_paper_book_1",
                        "eligible_market_date": "2026-01-10",
                        "proposal_hash": "HASH_R47", "portfolio_state_hash": "PSH",
                        "hoc_assessment_hash": "HOC",
                        "universe_scoring_hash": "USH",
                        "allocation_policy_version":
                            "reallocation_allocation_policy.v1"},
           "proposal": {"proposal_state": "READY", "portfolio": {"nav": nav},
                        "allocations": allocations, "proposal_hash": "HASH_R47",
                        "outcome": CR.OUTCOME_PROPOSAL_READY,
                        "reallocation_outcome": {
                            "outcome": CR.OUTCOME_PROPOSAL_READY,
                            "feasible_target_exists": True},
                        "switching_economics": {"score_improvement_net_of_cost": 0.2},
                        "constraint_reoptimization": {
                            "applied": True,
                            "constraints_that_reshaped": [CR.C_SECTOR_CAP]},
                        "signal": {"score_after": 0.8},
                        "risk": {}, "constraints": {"all_ok": True},
                        "complete_target_limits": {"all_ok": True}}}
    dec = {"record_id": "pdec_r47", "decision": pdec.DECISION_APPROVE,
           "proposal_id": "reap_r47", "proposal_hash": "HASH_R47",
           "binding": {"active_book_id": "alpha_paper_book_1",
                       "eligible_market_date": "2026-01-10",
                       "proposal_hash": "HASH_R47"}}
    kwargs = dict(desk_dir=sdir, active_book_id="alpha_paper_book_1",
                  eligible_market_date="2026-01-10", artifact=art,
                  decision_record=dec, plan_dir=tmp / "plans",
                  actions_dir=tmp / "ca", outcome_dir=tmp / "evidence")
    return sdir, book, art, dec, kwargs


class TestScenarioHApprovalRequired:

    def test_110_an_unapproved_proposal_mutates_nothing(self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        k = dict(kwargs)
        k["decision_record"] = dict(dec, decision=pdec.DECISION_HOLD)
        res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN, **k)
        assert res["status"] == rb.C_NOT_APPROVED
        assert res["performed_write"] is False
        assert not desk._orders_state(sdir)
        assert not (tmp_path / "evidence").exists()

    def test_111_the_second_confirmation_is_still_mandatory(self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        res = rb.confirm_rebalance_order_plan(confirm="NOT_THE_TOKEN", **kwargs)
        assert res["status"] == rb.C_CONFIRM_REQUIRED
        assert not desk._orders_state(sdir)
        assert not (tmp_path / "evidence").exists()


class TestScenarioIJKExecution:

    def test_120_one_approval_creates_exactly_one_transition(self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                              today="2026-01-10", **kwargs)
        assert res["status"] == rb.C_CREATED and res["n_orders_created"] == 3
        assert len(desk._orders_state(sdir)) == 3

    def test_121_replaying_the_approval_creates_no_duplicate(self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                        today="2026-01-10", **kwargs)
        n = len(desk._orders_state(sdir))
        again = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                                today="2026-01-10", **kwargs)
        assert again["status"] == rb.C_REUSED
        assert again["performed_write"] is False
        assert len(desk._orders_state(sdir)) == n
        # ...and exactly ONE frozen decision record, not two
        assert len(pdo.load_records(outcome_dir=tmp_path / "evidence")) == 1

    def test_122_paper_execution_reconciles_holdings_cash_and_nav(self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        plan = rb.load_rebalance_state(**{k: v for k, v in kwargs.items()
                                          if k != "outcome_dir"})["order_plan"]
        rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                        today="2026-01-10", **kwargs)
        _write_marks(sdir, {
            "AAA": [["2026-01-10", 110.0], ["2026-01-12", 110.0]],
            "BBB": [["2026-01-10", 190.0], ["2026-01-12", 190.0]],
            "CCC": [["2026-01-10", 52.0], ["2026-01-12", 52.0]],
            "SPY": [["2026-01-10", 405.0], ["2026-01-12", 410.0]]}, "2026-01-12")
        desk.settle_due_orders(desk_dir=sdir, today="2026-01-13")
        fills = desk._fills(sdir)
        cash, holdings = desk.book_cash_holdings(book, fills)
        assert "BBB" not in holdings
        po = {o["ticker"]: o for o in plan["orders"]}
        assert holdings["AAA"] == 40 + po["AAA"]["quantity"]
        assert holdings["CCC"] == po["CCC"]["quantity"]
        nav_blk = desk.book_nav(book, fills, desk.read_marks(sdir))
        manual = cash + holdings["AAA"] * 110.0 + holdings["CCC"] * 52.0
        assert nav_blk["nav"] == pytest.approx(round(manual, 2), abs=0.01)
        assert cash == pytest.approx(
            book["initial_capital"] + sum(f["net_cash_delta"] for f in fills),
            abs=0.01)
        assert desk.verify_ledger(sdir, desk.ORDERS_FILE)["intact"] is True

    def test_123_no_broker_and_no_automation_at_the_execution_boundary(
            self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                              today="2026-01-10", **kwargs)
        for k in ("broker_enabled", "live_orders_enabled",
                  "automatic_approval_allowed", "automatic_rebalance_allowed",
                  "promoted_model", "recalibrated_model"):
            assert res[k] is False


# =========================================================================== #
# SCENARIO L - decision evidence, frozen PROSPECTIVELY
# =========================================================================== #
class TestScenarioLDecisionEvidence:

    def test_130_execution_freezes_both_paths(self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        res = rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                              today="2026-01-10", **kwargs)
        ev = res["decision_evidence"]
        assert ev["status"] == pdo.F_CREATED and ev["frozen"] is True
        rec = ev["record"]
        assert set(rec["paths"]) == {PDOK.PATH_EXECUTED, PDOK.PATH_HOLD}
        assert rec["paths"][PDOK.PATH_HOLD]["basket"]          # the book we gave up
        assert rec["paths"][PDOK.PATH_EXECUTED]["basket"]      # the book we bought
        assert rec["paths"][PDOK.PATH_HOLD]["transaction_cost_charged"] == 0.0
        assert rec["paths"][PDOK.PATH_EXECUTED]["transaction_cost_charged"] > 0

    def test_131_the_record_binds_the_whole_decision(self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        rec = rb.confirm_rebalance_order_plan(
            confirm=rb.CONFIRM_TOKEN, today="2026-01-10",
            **kwargs)["decision_evidence"]["record"]
        assert rec["previous_portfolio"] and rec["executed_target"]
        assert rec["proposed_target"]
        assert rec["nav_at_decision"] and rec["transaction_cost"] is not None
        assert rec["eligible_market_date"] == "2026-01-10"
        assert rec["decision_reasons"]["outcome"] == CR.OUTCOME_PROPOSAL_READY
        assert rec["constraints_at_decision"]["constraints"]["all_ok"] is True
        assert rec["model_state"]["universe_scoring_hash"] == "USH"
        assert rec["expected_improvement"]["switching_economics"]

    def test_132_a_counterfactual_is_frozen_before_any_forward_price_exists(
            self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        rec = rb.confirm_rebalance_order_plan(
            confirm=rb.CONFIRM_TOKEN, today="2026-01-10",
            **kwargs)["decision_evidence"]["record"]
        # every reference price is the decision session's own mark
        for path in rec["paths"].values():
            for tk, p in (path["reference_prices"] or {}).items():
                on_decision_day = dict(
                    (d, v) for d, v in _SERIES_D10[tk])["2026-01-10"]
                assert p == pytest.approx(on_decision_day)
        m = pdo.measure_record(record=rec, desk_dir=sdir)
        assert m["state"] == PDOK.M_NOT_YET_MEASURABLE
        assert m["verdict"] == PDOK.V_PENDING

    def test_133_forward_evidence_measures_both_paths_after_a_new_session(
            self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        rec = rb.confirm_rebalance_order_plan(
            confirm=rb.CONFIRM_TOKEN, today="2026-01-10",
            **kwargs)["decision_evidence"]["record"]
        # a genuinely NEW completed session in which the executed book wins
        _write_marks(sdir, {
            "AAA": [["2026-01-10", 110.0], ["2026-01-12", 132.0]],   # +20%
            "BBB": [["2026-01-10", 190.0], ["2026-01-12", 171.0]],   # -10%
            "CCC": [["2026-01-10", 52.0], ["2026-01-12", 57.2]],     # +10%
            "SPY": [["2026-01-10", 405.0], ["2026-01-12", 410.0]]}, "2026-01-12")
        m = pdo.measure_record(record=rec, desk_dir=sdir)
        assert m["state"] == PDOK.M_MEASURED
        assert m["as_of"] == "2026-01-12"
        assert m["incremental_pnl"] > 0
        assert m["verdict"] == PDOK.V_ADDED_VALUE
        assert m["portfolio_decision_alpha"] == m["incremental_return"]
        assert m["holding_period_opportunity_cost"] == 0.0
        assert m["transaction_cost_paid"] > 0

    def test_134_a_losing_decision_is_reported_as_a_losing_decision(
            self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        rec = rb.confirm_rebalance_order_plan(
            confirm=rb.CONFIRM_TOKEN, today="2026-01-10",
            **kwargs)["decision_evidence"]["record"]
        _write_marks(sdir, {
            "AAA": [["2026-01-10", 110.0], ["2026-01-12", 99.0]],    # -10%
            "BBB": [["2026-01-10", 190.0], ["2026-01-12", 228.0]],   # +20%
            "CCC": [["2026-01-10", 52.0], ["2026-01-12", 46.8]],     # -10%
            "SPY": [["2026-01-10", 405.0], ["2026-01-12", 410.0]]}, "2026-01-12")
        m = pdo.measure_record(record=rec, desk_dir=sdir)
        assert m["verdict"] == PDOK.V_DESTROYED_VALUE
        assert m["incremental_pnl"] < 0
        assert m["holding_period_opportunity_cost"] > 0

    def test_135_evidence_on_or_before_the_decision_session_is_refused(self):
        rec = PDOK.freeze_decision_record(
            decision_id="d1", frozen_at="2026-01-10T00:00:00+00:00",
            eligible_market_date="2026-01-10", active_book_id="b",
            previous_portfolio={"A": 0.5}, proposed_target={"B": 0.5},
            executed_target={"B": 0.5},
            reference_prices={"A": 10.0, "B": 20.0}, nav_at_decision=100000.0,
            transaction_cost=100.0)
        pit = PDOK.point_in_time_check(record=rec,
                                       evidence_dates=["2026-01-09", "2026-01-10"])
        assert pit["ok"] is False
        assert pit["state"] == PDOK.M_POINT_IN_TIME_VIOLATION
        # and a measurement using only pre-decision prices measures nothing
        m = PDOK.measure_paths(record=rec, price_history={
            "A": {"2026-01-09": 9.0}, "B": {"2026-01-09": 21.0}})
        assert m["state"] == PDOK.M_NOT_YET_MEASURABLE

    def test_136_the_hold_path_pays_nothing_and_the_executed_path_pays_cost(self):
        rec = PDOK.freeze_decision_record(
            decision_id="d2", frozen_at="2026-01-10T00:00:00+00:00",
            eligible_market_date="2026-01-10", active_book_id="b",
            previous_portfolio={"A": 1.0}, proposed_target={"A": 1.0},
            executed_target={"A": 1.0},
            reference_prices={"A": 100.0}, nav_at_decision=100000.0,
            transaction_cost=250.0)
        m = PDOK.measure_paths(record=rec,
                               price_history={"A": {"2026-01-12": 100.0}})
        # identical baskets, flat market: the whole difference is the cost paid
        assert m["incremental_pnl"] == pytest.approx(-250.0, abs=0.01)
        assert m["incremental_return_gross_of_cost"] == pytest.approx(0.0, abs=1e-9)

    def test_137_an_unpriceable_path_withholds_rather_than_extrapolates(self):
        rec = PDOK.freeze_decision_record(
            decision_id="d3", frozen_at="2026-01-10T00:00:00+00:00",
            eligible_market_date="2026-01-10", active_book_id="b",
            previous_portfolio={"A": 0.5, "B": 0.5}, proposed_target={"A": 1.0},
            executed_target={"A": 1.0}, reference_prices={"A": 10.0, "B": 20.0},
            nav_at_decision=100000.0, transaction_cost=0.0)
        m = PDOK.measure_paths(record=rec,
                               price_history={"A": {"2026-01-12": 11.0}})
        assert m["state"] == PDOK.M_INSUFFICIENT_COVERAGE
        assert m["verdict"] == PDOK.V_PENDING

    def test_138_the_read_contract_is_read_only_and_separate_from_research(
            self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                        today="2026-01-10", **kwargs)
        out = pdo.load_portfolio_decision_outcomes(
            outcome_dir=tmp_path / "evidence", desk_dir=sdir)
        assert out["status"] == "OK" and out["decision_count"] == 1
        assert out["separate_from_research_alpha"] is True
        assert out["counterfactual_frozen_prospectively"] is True
        for k in ("created_orders", "changed_holdings", "changed_cash",
                  "changed_nav", "promoted_model", "hindsight_reconstruction"):
            assert out[k] is False

    def test_139_freezing_refuses_when_nothing_executed(self, tmp_path):
        out = pdo.freeze_executed_decision(
            proposal_hash="H", order_plan_id="p", eligible_market_date="d",
            active_book_id="b", previous_weights={}, proposed_weights={},
            executed_weights={}, reference_prices={}, nav=1.0,
            transaction_cost=0.0, orders_created=0,
            outcome_dir=tmp_path / "evidence")
        assert out["status"] == pdo.F_REFUSED_NOT_EXECUTED
        assert out["frozen"] is False
        assert not (tmp_path / "evidence").exists()


# =========================================================================== #
# SCENARIO M - Release-46 research state is untouched
# =========================================================================== #
class TestScenarioMResearchUntouched:

    R47_MODULES = ("engine/constrained_reallocation.py",
                   "engine/portfolio_decision_outcome.py",
                   "api/portfolio_decision_outcome.py")

    def test_140_no_release47_module_can_address_the_research_tournament(self):
        # Prose may NAME the research lane (the separation has to be explained);
        # what must be absent is any way to reach it - an import, a package path or
        # one of its store names. A module that cannot address the store cannot
        # have written it.
        for rel in self.R47_MODULES:
            src = (REPO / rel).read_text(encoding="utf-8")
            for token in ("import alpha_agent", "from alpha_agent",
                          "alpha_agent.", "alpha_agent/", "alpha_agent\\\\",
                          "prospective_tournament", "prospective_alpha_tournament",
                          "r46_forward_predictions", "r46_forward_outcomes",
                          "r46_challenger_registry"):
                assert token not in src, "%s can address %s" % (rel, token)

    def test_141_no_release47_module_promotes_or_recalibrates_a_model(self):
        for rel in self.R47_MODULES + ("engine/reallocation_proposal.py",):
            src = (REPO / rel).read_text(encoding="utf-8")
            for token in ("def promote_", "def recalibrate_", "promote_model(",
                          "set_champion(", "activate_challenger("):
                assert token not in src

    def test_142_the_decision_alpha_ledger_is_its_own_root(self):
        assert pdo.OUTCOME_DIR_ENV == \
            "PAPER_TRADER_PORTFOLIO_DECISION_OUTCOME_DIR"
        src = (REPO / "api/portfolio_decision_outcome.py").read_text(
            encoding="utf-8")
        assert "portfolio_decision_outcomes" in src
        # never the desk ledger root and never the Stage-18 decision root
        assert "PAPER_TRADER_DESK_DIR" not in src
        assert "PAPER_TRADER_PORTFOLIO_DECISION_DIR" not in src

    def test_143_decision_alpha_and_research_alpha_are_never_summed(
            self, tmp_path):
        sdir, book, art, dec, kwargs = _setup_execution(tmp_path)
        rb.confirm_rebalance_order_plan(confirm=rb.CONFIRM_TOKEN,
                                        today="2026-01-10", **kwargs)
        out = pdo.load_portfolio_decision_outcomes(
            outcome_dir=tmp_path / "evidence", desk_dir=sdir)
        assert "never summed" in out["separation_doc"]


# =========================================================================== #
# SAFETY + ARCHITECTURE
# =========================================================================== #
class TestSafetyAndArchitecture:

    def test_150_the_constraint_kernel_is_pure(self):
        src = (REPO / "engine/constrained_reallocation.py").read_text(
            encoding="utf-8")
        for token in ("import requests", "urllib", "sqlalchemy", "os.environ",
                      "open(", "Path(", "datetime.now", "date.today"):
            assert token not in src

    def test_151_the_outcome_kernel_is_pure(self):
        src = (REPO / "engine/portfolio_decision_outcome.py").read_text(
            encoding="utf-8")
        for token in ("import requests", "urllib", "sqlalchemy", "os.environ",
                      "open(", "Path(", "datetime.now", "date.today"):
            assert token not in src

    def test_152_no_kernel_creates_an_order_or_mutates_a_book(self):
        for rel in ("engine/constrained_reallocation.py",
                    "engine/portfolio_decision_outcome.py"):
            src = (REPO / rel).read_text(encoding="utf-8")
            for token in ("place_order(", "submit_order(", "create_order(",
                          "settle_due_orders(", "_append_ledger("):
                assert token not in src

    def test_153_safety_flags_are_false_everywhere(self):
        for blk in (CR.safety_block(), PDOK.safety_block()):
            for k in ("created_orders", "changed_holdings", "changed_cash",
                      "changed_nav", "promoted_model", "recalibrated_model",
                      "broker_enabled", "live_orders_enabled"):
                assert blk[k] is False
            assert blk["manual_review"] is True

    def test_154_the_new_routes_are_get_only(self):
        from paper_trader.api import app as A
        for path in ("/v1/operations/constrained-reallocation",
                     "/v1/operations/portfolio-decision-outcomes"):
            methods = set()
            for r in A.app.routes:
                if getattr(r, "path", None) == path:
                    methods |= set(getattr(r, "methods", set()))
            assert methods and methods <= {"GET", "HEAD"}

    def test_155_no_create_orders_or_automation_route_was_added(self):
        from paper_trader.api import app as A
        paths = {getattr(r, "path", "") for r in A.app.routes}
        for forbidden in ("/v1/operations/constrained-reallocation/apply",
                          "/v1/operations/constrained-reallocation/execute",
                          "/v1/operations/auto-rebalance",
                          "/v1/operations/create-orders"):
            assert forbidden not in paths

    def test_156_the_new_modules_are_in_the_system_inventory(self):
        inv = json.loads((REPO / "docs/architecture/system_inventory.json")
                         .read_text(encoding="utf-8"))
        listed = {m.get("path", "").replace("\\", "/") for m in inv["modules"]}
        for rel in ("engine/constrained_reallocation.py",
                    "engine/portfolio_decision_outcome.py",
                    "api/portfolio_decision_outcome.py"):
            assert rel in listed

    def test_156b_the_write_attribution_baseline_lane_fails_closed(self, tmp_path):
        # Release 47 is the first OPERATIONAL release to use the attribution gate, so
        # the gate gained a content-based strict-root lane. It must be STRICTLY
        # STRONGER than the mtime rule it supplements: it has to attribute a change
        # the mtime rule would miss entirely (here --since-day is in the future).
        import hashlib
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_r47_attr", REPO / "scripts/r33_operational_write_attribution.py")
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)

        root = tmp_path / "portfolio_decisions"
        root.mkdir(parents=True)
        (root / "a.json").write_text("one", encoding="utf-8")
        base = {"roots": {str(root): {"files": {
            "a.json": {"sha256": hashlib.sha256(b"one").hexdigest()}}}}}
        assert gate.attribute_strict_root(
            root, "2020-01-01", baseline=base)["state"] == gate.ATTRIBUTED
        (root / "a.json").write_text("two", encoding="utf-8")
        changed = gate.attribute_strict_root(root, "2099-01-01", baseline=base)
        assert changed["state"] == gate.R33_ATTRIBUTABLE
        (root / "b.json").write_text("new", encoding="utf-8")
        added = gate.attribute_strict_root(root, "2099-01-01", baseline=base)
        assert "NEW_FILE_IN_A_STORE_WITH_NO_INDEPENDENT_WRITER" in {
            x["reason"] for x in added["r33_attributable"]}
        # and R47 declares its own marker profile rather than borrowing one
        assert "R47" in gate.RELEASE_PROFILES
        assert "portfolio_decision_outcomes" in gate.RELEASE_PROFILES["R47"]["markers"]

    def test_157_strict_architecture_audit_exits_zero(self):
        proc = subprocess.run(
            [sys.executable, str(REPO / "scripts/audit_architecture.py"),
             "--strict", "--json-only"],
            capture_output=True, text=True, cwd=str(REPO))
        assert proc.returncode == 0, proc.stdout[-4000:]
