r"""R62 - PORTFOLIO PROPOSAL DECISION REVIEW.

Risk-based: every test here exists because something could go materially wrong for
an operator standing in front of the Approve gate.

  * the three counterfactuals are actually different objects, and CURRENT /
    FULL_TARGET are the persisted proposal's own numbers rather than a
    re-derivation that could silently disagree with the backend;
  * the MINIMUM REPAIR is a repair and not a second optimiser - it never adds a
    position, never increases one, and releases everything it frees to cash;
  * a repair obligation is only ever something an EXISTING owner declared;
  * a change is FORCED only when a constraint or a governed rule forces it, and a
    constraint that merely limited the SIZE of an increase never promotes that
    increase to forced;
  * the incremental economics measure the step from the repair to the full target,
    and the bar applied to them is the EXISTING frozen hurdle - this release
    introduces no threshold of its own;
  * an obligation is judged satisfied on the RESULTING BOOK, so a breach the full
    target closes through composition counts, and one it leaves open is reported;
  * evidence that has not matured says EVIDENCE_INSUFFICIENT instead of inventing
    confidence, and evidence never changes the verdict;
  * the review is asset-agnostic - a futures / FX / cash book is adjudicated by the
    same code with the same instrument metadata;
  * the explanation is deterministic and no language model is reachable from the
    runtime path;
  * and the whole surface approves nothing, creates nothing and mutates nothing.

Hermetic: every test builds its own proposal artifact in memory. The one test that
touches the live store is explicitly guarded and is READ-ONLY.
"""
from __future__ import annotations

import ast
import copy
import json
from pathlib import Path

import pytest

from paper_trader.api import proposal_decision_review as api
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import holding_opportunity_cost as hoc
from paper_trader.engine import proposal_decision_review as pdr
from paper_trader.engine import reallocation_proposal as rp

NAV = 100_000.0


# --------------------------------------------------------------------------- #
# Fixture world: a small, complete proposal artifact shaped exactly like the one
# api.reallocation_proposal persists.
# --------------------------------------------------------------------------- #
def _alloc(ticker, cw, pw, action, *, score, rank, sector="Information Technology",
           hoc_rec="HOLD", reason_codes=(), asset_class="US_EQUITY",
           sleeve_id="us_equity_fundamental_momentum_50_50_v1", currency="USD",
           instrument_type="CASH_EQUITY", cost_bps=None, capital_usage_ratio=1.0,
           unit_notional=None):
    return {
        "ticker": ticker, "action": action, "sector": sector,
        "current_weight": cw, "proposed_weight": pw, "delta_weight": pw - cw,
        "capital_change": (pw - cw) * NAV, "score": score, "rank": rank,
        "held": cw > 0, "source_hoc_recommendation": hoc_rec,
        "reason_codes": list(reason_codes), "replacement_relationship": None,
        "asset_class": asset_class, "sleeve_id": sleeve_id, "currency": currency,
        "instrument_type": instrument_type, "cost_bps_per_side": cost_bps,
        "capital_usage_ratio": capital_usage_ratio, "multiplier": 1.0,
        "unit_notional_usd": unit_notional, "score_basis": "combined_percentile",
        "combined_score": score, "current_market_value": cw * NAV,
        "proposed_market_value": pw * NAV, "execution_convention": "CASH",
    }


def _review_row(ticker, *, rank, det, codes, rc_pct, rec="HOLD",
                liquidity="LIQUID", weight=0.0, sector="Information Technology"):
    return {
        "ticker": ticker, "current_rank": rank, "previous_rank": rank,
        "rank_change": 0, "deterioration_state": det,
        "deterioration_reason_codes": list(codes), "reason_codes": list(codes),
        "recommendation": rec, "recommendation_confidence": "HIGH",
        "risk_contribution_pct": rc_pct, "liquidity_state": liquidity,
        "estimated_days_to_liquidate": 0.0, "current_weight": weight,
        "sector": sector, "gross_score_improvement": 0.10,
        "net_improvement": 0.07, "switching_cost_usd": 12.5,
        "switching_cost_bps": 25.0, "market_value": weight * NAV,
    }


def _policy():
    pol = dict(cr.default_policy())
    pol.update({
        "cost_rate_per_side": 0.00125, "round_trip_cost_bps": 25.0,
        "score_points_per_cost_bp": 0.001, "max_one_way_turnover": 0.35,
        "min_net_improvement": 0.05, "target_position_count": 25,
        "max_name_weight": 0.10, "sector_cap_fraction": 0.25,
        "covariance_lookback": 60, "min_covariance_obs": 40,
        "covariance_variance_floor": 1e-12, "min_volatility_coverage": 0.8,
        "material_weight_delta": 1e-4, "max_risk_contribution_repair_rounds": 3,
        "risk_contribution_excess_multiple": 3.0, "min_position_weight": 0.005,
    })
    return pol


def _artifact(*, allocations, risk=None, switching=None, turnover=None,
              portfolio=None, outcome=cr.OUTCOME_PROPOSAL_READY, reopt=None,
              rc_policy=None, ctl=None, constraints=None):
    cur = {a["ticker"]: a["current_weight"] for a in allocations
           if a["current_weight"] > 0}
    tgt = {a["ticker"]: a["proposed_weight"] for a in allocations
           if a["proposed_weight"] > 0}
    pol = _policy()
    score_of = {a["ticker"]: a["score"] for a in allocations}
    sb = cr.weighted_score(cur, score_of)
    sa = cr.weighted_score(tgt, score_of)
    two_way = 2.0 * cr.one_way_turnover(cur, tgt)
    cost_pts = two_way * pol["round_trip_cost_bps"] * pol["score_points_per_cost_bp"]
    t = turnover or rp.turnover_and_cost(allocations=allocations, nav=NAV,
                                         policy=pol, proposed_zero={}, selected={})
    proposal = {
        "allocations": allocations,
        "policy": pol,
        "policy_version": "reallocation_allocation_policy.v1",
        "proposal_state": "READY",
        "proposal_hash": "deadbeef" * 8,
        "outcome": outcome,
        "approvable": outcome == cr.OUTCOME_PROPOSAL_READY,
        "blockers": [],
        "reallocation_outcome": {"outcome": outcome, "true_blockers": [],
                                 "headline": "REALLOCATION PROPOSAL READY FOR REVIEW",
                                 "feasible_target_exists": True,
                                 "requires_manual_approval": True,
                                 "authorises_execution": False,
                                 "creates_orders": False},
        "portfolio": portfolio or {
            "nav": NAV, "current_holding_count": len(cur),
            "proposed_holding_count": len(tgt),
            "current_cash_weight": round(1.0 - sum(cur.values()), 6),
            "proposed_cash_weight": round(1.0 - sum(tgt.values()), 6),
            "current_allocation_by_asset_class": {"US_EQUITY": sum(cur.values())},
            "proposed_allocation_by_asset_class": {"US_EQUITY": sum(tgt.values())},
            "current_allocation_by_sleeve": {}, "proposed_allocation_by_sleeve": {},
            "asset_classes_in_target": ["US_EQUITY"],
        },
        "turnover": t,
        "switching_economics": switching or {
            "score_before": sb, "score_after": sa,
            "score_improvement": sa - sb, "score_cost_hurdle": cost_pts,
            "score_improvement_net_of_cost": (sa - sb) - cost_pts,
            "switching_hurdle": pol["min_net_improvement"],
            "clears_switching_hurdle": ((sa - sb) - cost_pts) >= pol["min_net_improvement"],
            "one_way_turnover": t["one_way_turnover"],
            "estimated_transaction_cost": t["estimated_transaction_cost"],
            "expected_return_state": "NOT_CALIBRATED",
            "owner": cr.CALCULATION_OWNER,
        },
        "risk": risk or {
            "portfolio_volatility_before": 0.12, "portfolio_volatility_after": 0.13,
            "volatility_before_state": "AVAILABLE", "volatility_after_state": "AVAILABLE",
            "concentration_before": rp.herfindahl(cur),
            "concentration_after": rp.herfindahl(tgt),
            "largest_position_before": rp.largest_weight(cur),
            "largest_position_after": rp.largest_weight(tgt),
            "sector_concentration_before": sum(cur.values()),
            "sector_concentration_after": sum(tgt.values()),
            "risk_contributions_before": {}, "risk_contributions_after": {},
            "risk_contribution_breaches_before": [],
            "risk_contribution_breaches_after": [],
            "risk_contribution_limit_before": hoc.risk_contribution_limit(
                n_covariance_names=len(cur), policy=pol),
            "risk_contribution_limit_after": hoc.risk_contribution_limit(
                n_covariance_names=len(tgt), policy=pol),
        },
        "risk_contribution_policy": rc_policy or {
            "held_book_breaches": [], "after_target_breaches": [],
            "after_target_gate_evaluated": True,
            "limit_held_book": hoc.risk_contribution_limit(
                n_covariance_names=len(cur), policy=pol),
            "limit_after_target": hoc.risk_contribution_limit(
                n_covariance_names=len(tgt), policy=pol),
            "owner": hoc.RISK_CONTRIBUTION_POLICY_OWNER,
        },
        "complete_target_limits": ctl or {"all_ok": True, "breaches": [],
                                          "owner": rp.CALCULATION_OWNER},
        "constraints": constraints or {"all_ok": True, "violations": []},
        "constraint_inventory": cr.constraint_inventory(pol),
        "constraint_reoptimization": reopt or {
            "applied": True, "constraint_adjustments": [],
            "constraints_that_reshaped": [], "breached_limits": [],
            "mandatory_exits": [],
            "turnover": {"accepted_trades": [], "deferred_trades": [],
                         "deferred_trade_count": 0, "mandatory_turnover": 0.0,
                         "budget_binds": False, "budget": pol["max_one_way_turnover"],
                         "ordering_reference_score": sb},
        },
        "safety": {"preview_only": True, "review_only": True},
        "data_gaps": [],
    }
    return {"proposal": proposal,
            "identity": {"proposal_hash": proposal["proposal_hash"],
                         "eligible_market_date": "2026-09-18",
                         "active_book_id": "book1"},
            "proposal_id": "reap_2026-09-18_book1_deadbeef"}


#: Twelve names at 7.75% across five sectors: inside the 10% name cap, inside the
#: 25% sector cap, inside the 25-position limit, 7% cash. A world with nothing
#: wrong with it, so anything a test finds is something the test put there.
SECTORS = ["Information Technology", "Health Care", "Energy", "Industrials",
           "Consumer Discretionary"]
BOOK = ["N%02d" % i for i in range(1, 13)]
W = 0.0775
SCORES = {t: round(0.99 - 0.05 * i, 4) for i, t in enumerate(BOOK)}


def _sector(ticker):
    return SECTORS[BOOK.index(ticker) % len(SECTORS)]


def _held_rows(overrides=None):
    over = overrides or {}
    rows = []
    for i, t in enumerate(BOOK):
        row = _review_row(t, rank=i + 1, det=hoc.DET_STABLE, codes=["RANK_STABLE"],
                          rc_pct=round(1.0 / len(BOOK), 6), weight=W,
                          sector=_sector(t))
        row.update(over.get(t) or {})
        rows.append(row)
    return {"holding_reviews": rows,
            "policy": {"min_net_improvement": 0.05, "exit_buffer_rank": 30}}


def _clean_world():
    """Nothing to repair, and nothing material proposed."""
    allocs = [_alloc(t, W, W, "RETAIN", score=SCORES[t], rank=i + 1,
                     sector=_sector(t)) for i, t in enumerate(BOOK)]
    return _artifact(allocations=allocs), _held_rows()


def _broken_world():
    """N12 has fallen out of the retention rules. The full target exits it, tilts
    two holdings and adds one new name - a normal mix of forced and discretionary."""
    allocs = []
    for i, t in enumerate(BOOK):
        pw, action, codes, rec = W, "RETAIN", [], "HOLD"
        if t == "N12":
            pw, action, rec = 0.0, "EXIT", "EXIT"
            codes = ["CONSTRAINT_REOPTIMIZED", "HOC_EXIT"]
        elif t in ("N01", "N02"):
            pw, action = 0.09, "INCREASE"
            codes = ["CONSTRAINT_REOPTIMIZED", "HOC_HOLD_RETAINED"]
        allocs.append(_alloc(t, W, pw, action, score=SCORES[t], rank=i + 1,
                             sector=_sector(t), hoc_rec=rec, reason_codes=codes))
    allocs.append(_alloc("NEW", 0.0, 0.05, "ADD", score=0.995, rank=1,
                         sector=SECTORS[1], hoc_rec="ADD",
                         reason_codes=["CONSTRAINT_REOPTIMIZED",
                                       "ELIGIBLE_TOP_CANDIDATE_NOT_HELD"]))
    assessment = _held_rows({"N12": {
        "deterioration_state": hoc.DET_BROKEN, "recommendation": "EXIT",
        "current_rank": 90, "deterioration_reason_codes": ["FELL_BELOW_EXIT_BUFFER"],
        "reason_codes": ["FELL_BELOW_EXIT_BUFFER"]}})
    return _artifact(allocations=allocs), assessment


def _world_tickers(art):
    return sorted({a["ticker"] for a in art["proposal"]["allocations"]})


def _returns(tickers, n=80, seed=7, loud=()):
    """A deterministic owned-return panel in the shape the canonical covariance
    owner reads: {"dates": [...], "series": {ticker: [ret|None, ...]}}.

    ``loud`` names tickers whose returns are scaled up, so a test can create a
    book in which one name genuinely carries too much of the portfolio's risk
    under the CANONICAL limit rather than under a limit the test made up."""
    series = {}
    for k, tk in enumerate(sorted(set(tickers))):
        vals, x = [], (seed + 13 * (k + 1))
        scale = 5.0 if tk in loud else 1.0
        for _ in range(n):
            x = (1103515245 * x + 12345) % (2 ** 31)
            vals.append(((x / (2 ** 31)) - 0.5) * 0.04 * scale)
        series[tk] = vals
    return {"dates": ["2026-%02d-%02d" % (((i // 28) % 12) + 1, (i % 28) + 1)
                      for i in range(n)],
            "series": series}


# --------------------------------------------------------------------------- #
# 1. Three counterfactuals, and where each number comes from
# --------------------------------------------------------------------------- #
def test_three_states_are_built_and_current_and_full_are_artifact_verbatim():
    art, assessment = _clean_world()
    prop = art["proposal"]
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment,
                         read_state="READY", aligned_returns=_returns(BOOK))
    assert list(r["state_order"]) == [pdr.STATE_CURRENT, pdr.STATE_MINIMUM_REPAIR,
                                      pdr.STATE_FULL_TARGET]
    cur = r["states"][pdr.STATE_CURRENT]
    full = r["states"][pdr.STATE_FULL_TARGET]
    assert cur["source"] == "PROPOSAL_ARTIFACT_VERBATIM"
    # every state must spell its open obligations on the SAME key, or a surface
    # reading that key renders the unrepaired book as the clean one
    for st in r["states"].values():
        assert "obligations_remaining" in st["constraint_status"]
        assert "obligations_satisfied" in st["constraint_status"]
    assert full["source"] == "PROPOSAL_ARTIFACT_VERBATIM"
    # verbatim means verbatim - not "close enough"
    se = prop["switching_economics"]
    assert cur["score"] == se["score_before"]
    assert full["score"] == se["score_after"]
    assert full["one_way_turnover"] == prop["turnover"]["one_way_turnover"]
    assert full["estimated_cost"] == prop["turnover"]["estimated_transaction_cost"]
    assert full["portfolio_volatility"] == prop["risk"]["portfolio_volatility_after"]
    assert cur["one_way_turnover"] == 0.0 and cur["estimated_cost"] == 0.0


def test_review_is_idempotent():
    art, assessment = _clean_world()
    ar = _returns(BOOK)
    a = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY", aligned_returns=ar)
    b = pdr.build_review(proposal=copy.deepcopy(art["proposal"]),
                         hoc_assessment=copy.deepcopy(assessment),
                         read_state="READY", aligned_returns=ar)
    assert json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True,
                                                                    default=str)


def test_review_does_not_mutate_the_proposal_it_reads():
    art, assessment = _clean_world()
    before = json.dumps(art["proposal"], sort_keys=True, default=str)
    pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                     read_state="READY",
                     aligned_returns=_returns(BOOK))
    assert json.dumps(art["proposal"], sort_keys=True, default=str) == before


# --------------------------------------------------------------------------- #
# 2. Repair obligations come only from existing owners
# --------------------------------------------------------------------------- #
def test_doing_nothing_leaves_every_obligation_open():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY", aligned_returns=_returns(BOOK + ["NEW"]))
    cs = r["states"][pdr.STATE_CURRENT]["constraint_status"]
    assert cs["valid"] is False
    assert cs["obligations_satisfied"] == 0
    assert len(cs["obligations_remaining"]) == len(r["repair_obligations"])
    assert "stays open" in r["states"][pdr.STATE_CURRENT]["consequences_of_deferring"]


def test_no_obligation_when_the_book_is_clean():
    art, assessment = _clean_world()
    pol = pdr.review_policy(art["proposal"])
    assert pdr.repair_obligations(proposal=art["proposal"],
                                  hoc_assessment=assessment, policy=pol) == []


def test_risk_contribution_breach_is_a_hard_obligation_from_its_owner():
    art, assessment = _clean_world()
    prop = art["proposal"]
    limit = hoc.risk_contribution_limit(n_covariance_names=len(BOOK),
                                        policy=_policy())
    breach = {"ticker": "N01", hoc.RISK_CONTRIBUTION_FIELD: 0.90,
              "limit": limit["limit"], "excess": 0.90 - limit["limit"]}
    prop["risk_contribution_policy"]["held_book_breaches"] = [breach]
    prop["risk"]["risk_contribution_breaches_before"] = [breach]
    pol = pdr.review_policy(prop)
    obs = pdr.repair_obligations(proposal=prop, hoc_assessment=assessment, policy=pol)
    assert len(obs) == 1
    o = obs[0]
    assert o["tier"] == pdr.TIER_HARD
    assert o["primary_reason"] == pdr.REASON_MANDATORY
    assert o["constraint_code"] == cr.C_RISK_CONTRIBUTION
    # the OWNER of that verdict is the risk-contribution contract, not this review
    assert o["owner"] == hoc.RISK_CONTRIBUTION_POLICY_OWNER


def test_retention_and_universe_failures_are_governance_obligations():
    art, assessment = _clean_world()
    rows = {r["ticker"]: r for r in assessment["holding_reviews"]}
    rows["N03"].update(deterioration_state=hoc.DET_BROKEN, recommendation="EXIT",
                       deterioration_reason_codes=["FELL_BELOW_EXIT_BUFFER"],
                       reason_codes=["FELL_BELOW_EXIT_BUFFER"])
    rows["N04"].update(deterioration_state=hoc.DET_BROKEN, recommendation="EXIT",
                       deterioration_reason_codes=["NOT_ELIGIBLE"],
                       reason_codes=["NOT_ELIGIBLE"])
    pol = pdr.review_policy(art["proposal"])
    obs = {o["ticker"]: o for o in pdr.repair_obligations(
        proposal=art["proposal"], hoc_assessment=assessment, policy=pol)}
    assert obs["N03"]["primary_reason"] == pdr.REASON_RETENTION
    assert obs["N04"]["primary_reason"] == pdr.REASON_UNIVERSE
    for o in obs.values():
        assert o["tier"] == pdr.TIER_GOVERNANCE
        assert o["repair_action"] == pdr.REPAIR_ACTION_EXIT
        assert o["owner"] == hoc.CALCULATION_OWNER


def test_illiquid_holding_is_a_liquidity_obligation():
    art, assessment = _clean_world()
    rows = {r["ticker"]: r for r in assessment["holding_reviews"]}
    rows["N02"]["liquidity_state"] = hoc.LIQ_ILLIQUID
    pol = pdr.review_policy(art["proposal"])
    obs = pdr.repair_obligations(proposal=art["proposal"],
                                 hoc_assessment=assessment, policy=pol)
    assert [o["primary_reason"] for o in obs] == [pdr.REASON_LIQUIDITY]
    assert obs[0]["tier"] == pdr.TIER_HARD
    # The owner publishes the liquidity STATE, not the compliant weight, so the
    # repair cannot be sized - and the review fails CLOSED rather than guessing.
    assert obs[0]["repair_action"] == pdr.REPAIR_ACTION_NOT_SIZEABLE
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY", aligned_returns=_returns(BOOK))
    v = r["review_verdict"]
    assert v["verdict"] == pdr.VERDICT_BLOCKED_CONSTRAINT_OR_DATA
    assert "MINIMUM_REPAIR_INCOMPLETE" in v["reason_codes"]


def test_name_cap_breach_is_found_by_the_canonical_verifier():
    allocs = [_alloc(t, 0.30 if t == "N01" else W, W, "REDUCE" if t == "N01" else "RETAIN",
                     score=SCORES[t], rank=i + 1, sector=_sector(t))
              for i, t in enumerate(BOOK)]
    art = _artifact(allocations=allocs)
    assessment = _held_rows({"N01": {"current_weight": 0.30, "market_value": 0.30 * NAV}})
    pol = pdr.review_policy(art["proposal"])
    obs = pdr.repair_obligations(proposal=art["proposal"],
                                 hoc_assessment=assessment, policy=pol)
    cap = [o for o in obs if o["ticker"] == "N01"
           and o["constraint_code"] == cr.C_NAME_CAP]
    assert cap, "the canonical verifier did not flag the name-cap breach"
    assert cap[0]["primary_reason"] == pdr.REASON_CONCENTRATION
    assert cap[0]["tier"] == pdr.TIER_HARD
    assert cap[0]["owner"] == cr.CALCULATION_OWNER
    assert cap[0]["limit"] == pytest.approx(pol["max_name_weight"])
    # and the repair reduces it to exactly the limit, releasing the rest to cash
    repair = pdr.solve_minimum_repair(proposal=art["proposal"], obligations=obs,
                                      policy=pol, aligned_returns=_returns(BOOK))
    assert repair["weights"]["N01"] == pytest.approx(pol["max_name_weight"], abs=1e-9)


# --------------------------------------------------------------------------- #
# 3. The MINIMUM REPAIR is a repair, not an optimiser
# --------------------------------------------------------------------------- #
def test_minimum_repair_never_adds_or_increases_a_position():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    cur = r["states"][pdr.STATE_CURRENT]["weights"]
    rep = r["states"][pdr.STATE_MINIMUM_REPAIR]["weights"]
    assert "NEW" not in rep, "the repair bought a position the book did not hold"
    assert set(rep) <= set(cur), "the repair introduced a position the book did not hold"
    for tk, w in rep.items():
        assert w <= cur[tk] + 1e-12, "the repair INCREASED %s" % tk
    assert r["repair"]["adds_positions"] is False
    assert r["repair"]["increases_positions"] is False
    assert r["repair"]["is_an_optimiser"] is False


def test_minimum_repair_releases_everything_it_frees_to_cash():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    cur = r["states"][pdr.STATE_CURRENT]
    rep = r["states"][pdr.STATE_MINIMUM_REPAIR]
    assert r["repair"]["released_to"] == pdr.RELEASED_CAPITAL_DESTINATION == "CASH"
    freed = sum(cur["weights"].values()) - sum(rep["weights"].values())
    assert rep["cash_weight"] == pytest.approx(cur["cash_weight"] + freed, abs=1e-6)
    for a in r["repair"]["adjustments"]:
        assert a["released_to"] == "CASH"


def test_minimum_repair_closes_the_obligation_and_is_verified_independently():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    rep = r["states"][pdr.STATE_MINIMUM_REPAIR]
    assert "N12" not in rep["weights"]
    assert rep["constraint_status"]["obligations_remaining"] == []
    assert rep["constraint_status"]["valid"] is True
    # verified by the constraint kernel's own independent verifier, not by the solver
    assert rep["constraint_status"]["verification"]["valid"] is True
    assert rep["constraint_status"]["owner"] == cr.CALCULATION_OWNER


def test_risk_contribution_repair_uses_the_canonical_first_order_rule():
    """The reduction must be w * limit/share - the SAME rule the reallocation
    kernel applies - and the result must be re-measured by the canonical risk
    owner, against the canonical 3/N limit rather than a limit made up here."""
    art, assessment = _clean_world()
    prop = art["proposal"]
    # One genuinely loud name, so it really does carry more than 3/N of the risk.
    ar = _returns(BOOK, loud={"N01"})
    pol = pdr.review_policy(prop)
    cur = pdr.current_weights(prop)
    measured = rp.portfolio_volatility(weights=cur, aligned_returns=ar, policy=pol)
    assert measured["state"] == rp.VOL_STATE_AVAILABLE
    shares = measured["contributions"]
    limit_block = hoc.risk_contribution_limit(
        n_covariance_names=len(measured["included_tickers"]), policy=pol)
    breaches = hoc.risk_contribution_breaches(contributions=shares,
                                              limit=limit_block["limit"])
    assert [b["ticker"] for b in breaches] == ["N01"],         "the fixture did not actually breach the canonical limit"
    # Publish that measured breach on the artifact exactly as the pipeline does.
    prop["risk_contribution_policy"]["held_book_breaches"] = breaches
    prop["risk_contribution_policy"]["limit_held_book"] = limit_block
    prop["risk"]["risk_contribution_breaches_before"] = breaches
    prop["risk"]["risk_contribution_limit_before"] = limit_block

    obs = pdr.repair_obligations(proposal=prop, hoc_assessment=assessment, policy=pol)
    assert [o["constraint_code"] for o in obs] == [cr.C_RISK_CONTRIBUTION]
    repair = pdr.solve_minimum_repair(proposal=prop, obligations=obs, policy=pol,
                                      aligned_returns=ar)
    assert repair["risk_measurement_state"] == pdr.RISK_MEASURED
    rc = [a for a in repair["adjustments"] if a["constraint"] == cr.C_RISK_CONTRIBUTION]
    assert rc, "no risk-contribution reduction was applied"
    a = rc[0]
    expected = a["before"] * (a["limit"] / a["risk_contribution"])
    # published at the owners' 8-dp precision, so compare at that precision
    assert a["after"] == pytest.approx(expected, abs=1e-8)
    assert a["released_to"] == "CASH"
    # the book was RE-MEASURED after the reduction, not asserted clean
    assert len(repair["risk_repair_rounds"]) >= 2
    assert repair["risk_contribution_breaches_remaining"] == []
    # and the repair only ever reduced
    for tk, w in repair["weights"].items():
        assert w <= cur[tk] + 1e-12


def test_repair_that_cannot_be_measured_is_refused_not_guessed():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY", aligned_returns=None)
    assert r["states"][pdr.STATE_MINIMUM_REPAIR]["risk_measurement_state"] == \
        pdr.RISK_NOT_MEASURED
    assert r["states"][pdr.STATE_MINIMUM_REPAIR]["portfolio_volatility"] is None
    v = r["review_verdict"]
    assert v["verdict"] == pdr.VERDICT_BLOCKED_CONSTRAINT_OR_DATA
    assert "MINIMUM_REPAIR_NOT_MEASURABLE" in v["reason_codes"]


# --------------------------------------------------------------------------- #
# 4. Mandatory vs discretionary classification
# --------------------------------------------------------------------------- #
def test_every_change_gets_exactly_one_primary_reason_from_the_vocabulary():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    cl = r["change_classification"]
    assert cl["changes"], "no change was classified"
    for c in cl["changes"]:
        assert c["primary_reason"] in pdr.REASON_VOCAB
        assert c["forced_change"] != c["discretionary_change"]
        assert c["forced_change"] == (c["primary_reason"] in pdr.FORCED_REASONS)
    assert (cl["mandatory_change_count"] + cl["discretionary_change_count"]
            == len(cl["changes"]))


def test_retention_exit_is_forced_and_an_addition_is_discretionary():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    by = {c["ticker"]: c for c in r["change_classification"]["changes"]}
    assert by["N12"]["primary_reason"] == pdr.REASON_RETENTION
    assert by["N12"]["forced_change"] is True
    assert by["NEW"]["primary_reason"] == pdr.REASON_OPPORTUNITY
    assert by["NEW"]["forced_change"] is False
    assert by["N01"]["primary_reason"] == pdr.REASON_REOPTIMIZATION
    assert by["N01"]["forced_change"] is False


def test_a_partial_exit_of_a_broken_holding_is_still_forced():
    """The turnover budget can fund only half the exit of a holding the retention
    rule rejects. The trade is still driven by that rule."""
    art, assessment = _broken_world()
    prop = art["proposal"]
    half = [a for a in prop["allocations"] if a["ticker"] == "N12"][0]
    half.update(action="REDUCE", proposed_weight=W / 2, delta_weight=-W / 2,
                capital_change=-(W / 2) * NAV)
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    c = {x["ticker"]: x for x in r["change_classification"]["changes"]}["N12"]
    assert c["primary_reason"] == pdr.REASON_RETENTION
    assert c["forced_change"] is True
    assert c["partially_discharges_obligation"] is True


def test_a_constraint_that_only_limited_the_size_of_an_increase_never_forces_it():
    art, assessment = _broken_world()
    prop = art["proposal"]
    prop["constraint_reoptimization"]["constraint_adjustments"] = [{
        "ticker": "NEW", "constraint": cr.C_RISK_CONTRIBUTION,
        "action": cr.ADJ_CAPPED, "before": 0.08, "after": 0.05, "limit": 0.12,
        "risk_contribution": 0.25, "kind": cr.KIND_RESHAPES,
    }]
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    new = {c["ticker"]: c for c in r["change_classification"]["changes"]}["NEW"]
    assert new["primary_reason"] == pdr.REASON_OPPORTUNITY
    assert new["forced_change"] is False
    assert cr.C_RISK_CONTRIBUTION in new["constraint_limited_size"]


def test_a_reduction_of_a_name_not_in_breach_is_risk_reduction_not_mandatory():
    art, assessment = _clean_world()
    prop = art["proposal"]
    trimmed = [a for a in prop["allocations"] if a["ticker"] == "N12"][0]
    trimmed.update(action="REDUCE", proposed_weight=0.04, delta_weight=0.04 - W,
                   capital_change=(0.04 - W) * NAV)
    prop["constraint_reoptimization"]["constraint_adjustments"] = [{
        "ticker": "N12", "constraint": cr.C_RISK_CONTRIBUTION, "action": cr.ADJ_CAPPED,
        "before": W, "after": 0.04, "limit": 0.12, "risk_contribution": 0.20,
        "kind": cr.KIND_RESHAPES,
    }]
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK))
    d = {c["ticker"]: c for c in r["change_classification"]["changes"]}["N12"]
    # nothing in the HELD book breached the cap, so this is a risk reduction
    assert d["primary_reason"] == pdr.REASON_RISK


# --------------------------------------------------------------------------- #
# 5. Marginal economics and the hurdle
# --------------------------------------------------------------------------- #
def test_incremental_economics_measure_the_step_from_repair_to_full_target():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    incr = r["marginal_economics"]["full_target_vs_minimum_repair"]
    rep = r["states"][pdr.STATE_MINIMUM_REPAIR]["weights"]
    full = r["states"][pdr.STATE_FULL_TARGET]["weights"]
    assert incr["additional_one_way_turnover"] == pytest.approx(
        round(cr.one_way_turnover(rep, full), 6), abs=1e-6)
    assert incr["incremental_score_improvement"] == pytest.approx(
        round(r["states"][pdr.STATE_FULL_TARGET]["score"]
              - r["states"][pdr.STATE_MINIMUM_REPAIR]["score"], 6), abs=1e-6)
    # the plain difference of the two turnovers is published too, and named
    assert "one_way_turnover_difference" in incr
    assert "STEP_FROM_THE_REPAIRED_BOOK" in incr["turnover_basis"]


def test_the_review_introduces_no_threshold_of_its_own():
    art, assessment = _broken_world()
    prop = art["proposal"]
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    contract = r["marginal_economics"]["full_target_vs_minimum_repair"]["hurdle_contract"]
    assert contract["new_threshold_introduced"] is False
    assert contract["tuned_on_this_proposal"] is False
    assert contract["hurdle_source"] == "EXISTING_SWITCHING_HURDLE"
    # and it is NUMERICALLY the hurdle the proposal owner already published
    assert contract["hurdle"] == prop["switching_economics"]["switching_hurdle"]


def test_incremental_cost_uses_the_proposal_owners_own_cost_model():
    art, assessment = _broken_world()
    prop = art["proposal"]
    pol = pdr.review_policy(prop)
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    incr = r["marginal_economics"]["full_target_vs_minimum_repair"]
    rep = r["states"][pdr.STATE_MINIMUM_REPAIR]["weights"]
    full = r["states"][pdr.STATE_FULL_TARGET]["weights"]
    traded = sum(abs((full.get(t) or 0.0) - (rep.get(t) or 0.0))
                 for t in set(rep) | set(full)) * NAV
    assert incr["additional_estimated_cost"] == pytest.approx(
        round(traded * pol["cost_rate_per_side"], 2), abs=0.02)


def test_score_is_never_converted_into_dollars():
    art, assessment = _clean_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK))
    me = r["marginal_economics"]
    assert me["score_converted_to_dollars"] is False
    assert me["expected_return_state"] == "NOT_CALIBRATED"
    for st in r["states"].values():
        assert st["expected_return"] is None
        assert st["expected_return_state"] == "NOT_CALIBRATED"


def test_a_comparison_across_different_invested_capital_is_flagged():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    comp = r["marginal_economics"]["full_target_vs_minimum_repair"]["score_comparability"]
    assert comp["comparable"] is False
    assert comp["code"] == "SCORE_BASIS_EXCLUDES_UNINVESTED_CAPITAL"
    assert comp["band_owner"].startswith("engine.reallocation_proposal")
    assert comp["code"] in r["review_verdict"]["reason_codes"]


# --------------------------------------------------------------------------- #
# 6. Obligation satisfaction is judged on the resulting book
# --------------------------------------------------------------------------- #
def test_full_target_closing_a_breach_by_composition_counts_as_satisfied():
    """The live Sep-18 pattern: a risk-contribution breach the target closes without
    trading that name at all. A trade-matching rule would call it unrepaired."""
    art, assessment = _clean_world()
    prop = art["proposal"]
    breach = {"ticker": "N01", hoc.RISK_CONTRIBUTION_FIELD: 0.40,
              "limit": 0.30, "excess": 0.10}
    prop["risk_contribution_policy"]["held_book_breaches"] = [breach]
    prop["risk"]["risk_contribution_breaches_before"] = [breach]
    prop["risk_contribution_policy"]["after_target_breaches"] = []
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK))
    cs = r["states"][pdr.STATE_FULL_TARGET]["constraint_status"]
    assert cs["obligations_remaining"] == []
    assert cs["obligations_satisfied"] == 1
    # AAA's weight did not change, so no trade discharged it
    assert r["states"][pdr.STATE_FULL_TARGET]["weights"]["N01"] == \
        r["states"][pdr.STATE_CURRENT]["weights"]["N01"]


def test_full_target_leaving_an_obligation_open_is_reported():
    art, assessment = _broken_world()
    prop = art["proposal"]
    # the budget only half-exits the broken holding
    half = [a for a in prop["allocations"] if a["ticker"] == "N12"][0]
    half.update(action="REDUCE", proposed_weight=W / 2, delta_weight=-W / 2,
                capital_change=-(W / 2) * NAV)
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    open_now = r["states"][pdr.STATE_FULL_TARGET]["constraint_status"]["obligations_remaining"]
    assert [o["ticker"] for o in open_now] == ["N12"]
    assert "FULL_TARGET_LEAVES_OBLIGATIONS_OPEN" in r["review_verdict"]["reason_codes"]


# --------------------------------------------------------------------------- #
# 7. The verdict
# --------------------------------------------------------------------------- #
def test_verdict_is_always_in_the_declared_vocabulary_and_the_ladder_is_total():
    worlds = [_clean_world(), _broken_world()]
    for art, assessment in worlds:
        for state in ("READY", "DEGRADED", "BLOCKED", "NOT_RUN",
                      "SUPERSEDED_BY_NEWER_DECISION"):
            r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                                 read_state=state,
                                 aligned_returns=_returns(["AAA", "BBB", "CCC",
                                                           "DDD", "EEE"]))
            assert r["review_verdict"]["verdict"] in pdr.VERDICT_VOCAB


def test_a_non_reviewable_read_state_blocks_rather_than_recommending():
    art, assessment = _broken_world()
    for state in pdr.NON_REVIEWABLE_READ_STATES:
        r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                             read_state=state,
                             aligned_returns=_returns(BOOK + ["NEW"]))
        assert r["review_verdict"]["verdict"] == pdr.VERDICT_BLOCKED_CONSTRAINT_OR_DATA


def test_a_true_blocker_blocks():
    art, assessment = _clean_world()
    prop = art["proposal"]
    prop["outcome"] = cr.OUTCOME_TRUE_BLOCKER
    prop["reallocation_outcome"]["true_blockers"] = [
        {"code": cr.B_NAV_UNRECONCILED}]
    r = pdr.build_review(proposal=prop, hoc_assessment=assessment, read_state="READY",
                         aligned_returns=_returns(BOOK))
    assert r["review_verdict"]["verdict"] == pdr.VERDICT_BLOCKED_CONSTRAINT_OR_DATA
    assert "TRUE_BLOCKER_PRESENT" in r["review_verdict"]["reason_codes"]


def test_no_obligation_and_no_material_change_is_no_change_required():
    art, assessment = _clean_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK))
    assert r["states"][pdr.STATE_FULL_TARGET]["changes"] == 0
    assert r["review_verdict"]["verdict"] == pdr.VERDICT_NO_CHANGE_REQUIRED


def test_a_clearing_increment_is_reviewable_and_a_weak_one_defers():
    """With no repair obligation the repaired book IS the current book, so the
    increment and the whole switch are the same object - and the review's verdict
    must follow the proposal owner's own hurdle verdict rather than a second one."""
    # A strong switch: the worst-scoring name out, the best-scoring name in.
    strong = []
    for i, t in enumerate(BOOK):
        pw = 0.0 if t == "N12" else W
        strong.append(_alloc(t, W, pw, "EXIT" if t == "N12" else "RETAIN",
                             score=(0.01 if t == "N12" else SCORES[t]), rank=i + 1,
                             sector=_sector(t),
                             reason_codes=["CONSTRAINT_REOPTIMIZED"]))
    strong.append(_alloc("NEW", 0.0, W, "ADD", score=0.999, rank=1,
                         sector=SECTORS[1], hoc_rec="ADD",
                         reason_codes=["CONSTRAINT_REOPTIMIZED",
                                       "ELIGIBLE_TOP_CANDIDATE_NOT_HELD"]))
    art = _artifact(allocations=strong)
    assessment = _held_rows({"N12": {"current_rank": 12}})
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY", aligned_returns=_returns(BOOK + ["NEW"]))
    assert r["repair_obligations"] == []
    assert r["states"][pdr.STATE_FULL_TARGET]["clears_switching_hurdle"] is True
    assert r["review_verdict"]["verdict"] == pdr.VERDICT_FULL_TARGET_REVIEWABLE
    # no repair means the repaired book IS the current book
    assert r["states"][pdr.STATE_MINIMUM_REPAIR]["weights"] == \
        r["states"][pdr.STATE_CURRENT]["weights"]

    # A weak one: a half-point tilt between two near-identical names.
    weak = []
    for i, t in enumerate(BOOK):
        pw = W - 0.005 if t == "N01" else (W + 0.005 if t == "N02" else W)
        weak.append(_alloc(t, W, pw, "RETAIN" if pw == W else
                           ("REDUCE" if pw < W else "INCREASE"),
                           score=SCORES[t], rank=i + 1, sector=_sector(t),
                           reason_codes=["CONSTRAINT_REOPTIMIZED"]))
    art2 = _artifact(allocations=weak)
    r2 = pdr.build_review(proposal=art2["proposal"], hoc_assessment=_held_rows(),
                          read_state="READY", aligned_returns=_returns(BOOK))
    assert r2["repair_obligations"] == []
    assert r2["states"][pdr.STATE_FULL_TARGET]["changes"] == 2
    assert r2["states"][pdr.STATE_FULL_TARGET]["clears_switching_hurdle"] is False
    assert r2["review_verdict"]["verdict"] == pdr.VERDICT_DEFER_WEAK_INCREMENTAL_EDGE


def test_an_open_obligation_with_a_weak_increment_prefers_the_repair():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    incr = r["marginal_economics"]["full_target_vs_minimum_repair"]
    expected = (pdr.VERDICT_FULL_TARGET_REVIEWABLE
                if incr["clears_incremental_hurdle"] is True
                else pdr.VERDICT_MINIMAL_REPAIR_PREFERRED)
    assert r["review_verdict"]["verdict"] == expected
    assert "REPAIR_OBLIGATION_OPEN" in r["review_verdict"]["reason_codes"]


# --------------------------------------------------------------------------- #
# 8. Historical evidence
# --------------------------------------------------------------------------- #
def _evidence(*, sufficient=True, rep_wins=3, rep_losses=1):
    total = rep_wins + rep_losses
    return {
        "status": "OK",
        "evidence_state": "HORIZON_ALIGNED_EVIDENCE" if sufficient else "INSUFFICIENT_SAMPLE",
        "evidence_sufficient": sufficient,
        "message": "evidence",
        "scorecard": {
            "primary_horizon": 20,
            "replacement_outcomes": {"observations": total, "measured_spreads": total,
                                     "wins": rep_wins, "losses": rep_losses,
                                     "hit_rate": rep_wins / total, "mean_spread": 0.01},
            "hold_outcomes": {"observations": 10, "measured_spreads": 10, "wins": 5,
                              "losses": 5, "hit_rate": 0.5, "mean_spread": 0.0},
            "exit_outcomes": {"observations": 2, "avoided_loss_count": 1,
                              "missed_upside_count": 1, "basis": "OBSERVED"},
            "evidence": {"matured_observations": 75 if sufficient else 2,
                         "interpretation": "x"},
        },
        "policy_intelligence": {"policy_state": "POLICY_STABLE", "controls": [],
                                "findings": []},
    }


def test_evidence_is_ingested_and_mapped_onto_this_proposals_changes():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         outcome_evidence=_evidence(), read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    ev = r["historical_evidence"]
    assert ev["available"] is True
    classes = {b["evidence_class"] for b in ev["buckets"]}
    assert classes == {"REPLACEMENT_AND_ADDITION", "HOLD", "EXIT"}
    rep = [b for b in ev["buckets"] if b["evidence_class"] == "REPLACEMENT_AND_ADDITION"][0]
    assert rep["applies_to_changes"] > 0


def test_missing_or_immature_evidence_says_so_instead_of_inventing_confidence():
    art, assessment = _broken_world()
    ar = _returns(BOOK + ["NEW"])
    none = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                            outcome_evidence=None, read_state="READY",
                            aligned_returns=ar)["historical_evidence"]
    assert none["available"] is False
    assert none["state"] == pdr.EVIDENCE_INSUFFICIENT

    thin = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                            outcome_evidence=_evidence(sufficient=False),
                            read_state="READY",
                            aligned_returns=ar)["historical_evidence"]
    assert thin["evidence_sufficient"] is False
    assert pdr.EVIDENCE_INSUFFICIENT in {c["code"] for c in thin["cautions"]}


def test_adverse_evidence_is_surfaced_and_never_changes_the_verdict():
    art, assessment = _broken_world()
    ar = _returns(BOOK + ["NEW"])
    good = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                            outcome_evidence=_evidence(rep_wins=6, rep_losses=0),
                            read_state="READY", aligned_returns=ar)
    bad = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                           outcome_evidence=_evidence(rep_wins=0, rep_losses=6),
                           read_state="READY", aligned_returns=ar)
    assert good["review_verdict"]["verdict"] == bad["review_verdict"]["verdict"]
    assert good["review_verdict"]["reason_codes"] == bad["review_verdict"]["reason_codes"]
    codes = {c["code"] for c in bad["historical_evidence"]["cautions"]}
    assert "REPLACEMENT_EVIDENCE_ADVERSE" in codes
    assert bad["historical_evidence"]["changes_verdict"] is False
    assert bad["historical_evidence"]["overrides_proposal"] is False
    assert bad["historical_evidence"]["rewrites_history"] is False


# --------------------------------------------------------------------------- #
# 9. Asset-agnostic
# --------------------------------------------------------------------------- #
def test_the_review_adjudicates_a_multi_asset_book_with_the_same_code():
    """A book of equity, a future, an FX leg and cash. The instrument metadata the
    proposal rows carry must travel into the caps, the groups and the cost model."""
    allocs = [
        _alloc("AAPL", 0.09, 0.09, "RETAIN", score=0.90, rank=1),
        _alloc("ES", 0.08, 0.04, "REDUCE", score=0.60, rank=8, sector="Unknown",
               asset_class="EQUITY_INDEX_FUTURE", sleeve_id="sleeve_equity_index_futures",
               instrument_type="FUTURE", capital_usage_ratio=0.10, cost_bps=2.0,
               unit_notional=5000.0, hoc_rec="HOLD",
               reason_codes=["CONSTRAINT_REOPTIMIZED"]),
        _alloc("EURUSD", 0.06, 0.00, "EXIT", score=0.10, rank=120, sector="Unknown",
               asset_class="FX", sleeve_id="sleeve_fx_carry", currency="EUR",
               instrument_type="FX_SPOT", cost_bps=1.0, hoc_rec="EXIT",
               reason_codes=["CONSTRAINT_REOPTIMIZED", "HOC_EXIT"]),
        _alloc("MSFT", 0.07, 0.09, "INCREASE", score=0.95, rank=2, hoc_rec="HOLD",
               reason_codes=["CONSTRAINT_REOPTIMIZED", "HOC_HOLD_RETAINED"]),
    ]
    art = _artifact(allocations=allocs)
    assessment = {"holding_reviews": [
        _review_row("AAPL", rank=1, det=hoc.DET_STABLE, codes=["RANK_STABLE"],
                    rc_pct=0.3, weight=0.09),
        _review_row("ES", rank=8, det=hoc.DET_STABLE, codes=["RANK_STABLE"],
                    rc_pct=0.2, weight=0.08, sector="Unknown"),
        _review_row("EURUSD", rank=120, det=hoc.DET_BROKEN, rec="EXIT",
                    codes=["FELL_BELOW_EXIT_BUFFER"], rc_pct=0.1, weight=0.06,
                    sector="Unknown"),
        _review_row("MSFT", rank=2, det=hoc.DET_STABLE, codes=["RANK_STABLE"],
                    rc_pct=0.2, weight=0.07)],
        "policy": {"min_net_improvement": 0.05, "exit_buffer_rank": 30}}
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(["AAPL", "ES", "EURUSD", "MSFT"]))
    assert r["review_verdict"]["verdict"] in pdr.VERDICT_VOCAB
    # the FX leg's retention failure is found and repaired, with no equity assumption
    obs = {o["ticker"] for o in r["repair_obligations"]}
    assert "EURUSD" in obs
    assert "EURUSD" not in r["states"][pdr.STATE_MINIMUM_REPAIR]["weights"]
    # instrument metadata survived into the grouped allocation
    rep = r["states"][pdr.STATE_MINIMUM_REPAIR]
    assert "EQUITY_INDEX_FUTURE" in rep["allocation_by_asset_class"]
    assert rep["allocation_by_asset_class"]["CASH"] > 0
    # and the per-instrument cost rate was used, not the equity default
    priced = pdr.price_step(before=pdr.current_weights(art["proposal"]),
                            after=rep["weights"], proposal=art["proposal"],
                            policy=pdr.review_policy(art["proposal"]))
    assert priced["per_instrument_cost_rates_applied"] is True


def test_no_equity_only_vocabulary_is_hard_coded_in_the_kernel():
    src = Path(pdr.__file__).read_text(encoding="utf-8")
    for token in ("US_EQUITY", "S&P", "SPX", "equities only", "stock"):
        assert token not in src, "equity-only assumption %r leaked into the kernel" % token


# --------------------------------------------------------------------------- #
# 10. Determinism, no LLM, and the safety boundary
# --------------------------------------------------------------------------- #
def test_the_explanation_is_deterministic_and_carries_its_facts():
    art, assessment = _broken_world()
    ar = _returns(BOOK + ["NEW"])
    a = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         outcome_evidence=_evidence(), read_state="READY",
                         aligned_returns=ar)["explanation"]
    b = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         outcome_evidence=_evidence(), read_state="READY",
                         aligned_returns=ar)["explanation"]
    assert a["text"] == b["text"]
    assert a["llm_used"] is False and a["prompt_executed"] is False
    assert a["network_call"] is False and a["deterministic"] is True
    assert a["generated_by"] == pdr.CALCULATION_OWNER
    # it must actually explain, not just label
    assert len(a["paragraphs"]) >= 6
    assert "MANUAL REVIEW" in a["text"]
    assert "NOT_CALIBRATED" in a["text"]


#: Any of these appearing as a CODE IDENTIFIER - an import, a name, an attribute -
#: would mean a language model or a network call is reachable from the runtime path.
#: Prose is exempt by construction: the check walks the AST, so a docstring that
#: says "no LLM is called" can never satisfy or trip it.
_FORBIDDEN_IDENTIFIERS = ("anthropic", "openai", "claude", "llm", "gpt", "gemini",
                          "requests", "urllib", "httpx", "aiohttp", "socket",
                          "subprocess", "prompt", "completion")


def _code_identifiers(path: Path) -> set:
    """Every import / name / attribute in a module, ignoring strings and comments."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                names.update(a.name.split("."))
                if a.asname:
                    names.add(a.asname)
        elif isinstance(node, ast.ImportFrom):
            names.update((node.module or "").split("."))
            for a in node.names:
                names.add(a.name)
                if a.asname:
                    names.add(a.asname)
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.keyword) and node.arg:
            # a leak can hide in a KEYWORD name (send(prompt=...)), which is
            # neither a Name nor an Attribute
            names.add(node.arg)
        elif isinstance(node, ast.arg):
            names.add(node.arg)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
    return {n.lower() for n in names if n}


def test_no_llm_or_network_is_reachable_from_the_runtime_path():
    for mod in (pdr, api):
        ids = _code_identifiers(Path(mod.__file__))
        hits = sorted(i for i in ids
                      for t in _FORBIDDEN_IDENTIFIERS if t in i)
        assert not hits, "%s may reach %s" % (mod.__name__, hits)
    assert pdr.build_review.__module__ == "paper_trader.engine.proposal_decision_review"


def test_the_no_llm_guard_actually_catches_a_planted_call(tmp_path):
    """A guard nobody has seen fail is not a guard. Three shapes a real leak would
    take - an httpx post, a client attribute call and a prompt keyword - must all
    be caught, and the prose that DENIES them must not be."""
    prose = tmp_path / "prose.py"
    prose.write_text(
        '"""No LLM, no prompt, no anthropic call is made here."""\n'
        "# openai is never imported\n"
        "X = 1\n", encoding="utf-8")
    assert not [i for i in _code_identifiers(prose)
                for t in _FORBIDDEN_IDENTIFIERS if t in i], \
        "the guard trips on prose that only DENIES a call"
    planted_bodies = (
        'import httpx\nr = httpx.post("u", json={})\n',
        'out = anthropic_client.messages.create(model="x")\n',
        'resp = net.send(prompt=text)\n',
    )
    for body in planted_bodies:
        planted = tmp_path / "planted.py"
        planted.write_text(body, encoding="utf-8")
        assert [i for i in _code_identifiers(planted)
                for t in _FORBIDDEN_IDENTIFIERS if t in i], \
            "the guard MISSED %r" % body


def test_the_review_can_never_approve_execute_or_mutate():
    art, assessment = _broken_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK + ["NEW"]))
    v = r["review_verdict"]
    for flag in ("is_an_approval", "approves_proposal", "creates_order_plan",
                 "confirms_order_plan", "executes", "creates_orders", "creates_fills",
                 "promotes_model", "deploys_capital", "mutates_proposal",
                 "decided_by_llm"):
        assert v[flag] is False, flag
    assert v["manual_approval_still_required"] is True
    assert v["recommends_manual_review_only"] is True
    s = r["safety"]
    for flag in ("approves_proposal", "rejects_proposal", "supersedes_proposal",
                 "mutates_proposal", "regenerates_proposal", "created_order_plan",
                 "confirmed_order_plan", "created_orders", "created_fills",
                 "performed_broker_execution", "promoted_model", "changed_holdings",
                 "changed_cash", "changed_nav", "wrote_to_database", "wrote_to_ledger",
                 "called_llm"):
        assert s[flag] is False, flag
    assert r["runtime_llm_dependency"] == "NONE"
    for badge in ("PREVIEW ONLY", "NO ORDERS", "MANUAL REVIEW", "AUTOMATION OFF"):
        assert badge in s["safety_badges"]


def test_no_write_verb_exists_anywhere_in_either_module():
    for mod in (pdr, api):
        src = Path(mod.__file__).read_text(encoding="utf-8")
        code = "\n".join(ln for ln in src.splitlines()
                         if not ln.strip().startswith("#"))
        for verb in ("open(", "os.replace", "mkdir", "write_text", "json.dump(",
                     "session.add", "commit(", "INSERT", "UPDATE "):
            assert verb not in code, "%s performs a write: %r" % (mod.__name__, verb)


def test_the_review_declares_which_owners_it_reuses_and_forks_none():
    art, assessment = _clean_world()
    r = pdr.build_review(proposal=art["proposal"], hoc_assessment=assessment,
                         read_state="READY",
                         aligned_returns=_returns(BOOK))
    assert r["second_proposal_engine"] is False
    assert r["second_opportunity_cost_engine"] is False
    assert r["second_risk_engine"] is False
    assert r["second_evidence_store"] is False
    assert len(r["reuses_not_rebuilds"]) >= 4
    # the primitives are the owners' own functions, not copies
    assert rp.portfolio_volatility.__module__ == \
        "paper_trader.engine.reallocation_proposal"
    assert cr.weighted_score.__module__ == \
        "paper_trader.engine.constrained_reallocation"


# --------------------------------------------------------------------------- #
# 11. The read owner
# --------------------------------------------------------------------------- #
def test_read_owner_returns_no_proposal_without_inventing_one():
    out = api.load_proposal_decision_review(
        proposal_payload={"state": "NOT_RUN", "artifact": None,
                          "active_book": {"book_id": "b"},
                          "eligible_market_date": "2026-09-18"})
    assert out["status"] == api.STATUS_NO_PROPOSAL
    assert out["review"] is None
    assert "Nothing is fabricated" in out["message"]


def test_read_owner_composes_and_stays_read_only():
    art, assessment = _broken_world()
    payload = {"state": "READY", "approvable": True,
               "active_book": {"book_id": "book1"},
               "eligible_market_date": "2026-09-18",
               "proposal_state": "READY",
               "artifact": {"proposal_id": art["proposal_id"],
                            "identity": art["identity"],
                            "generated_at": "2026-09-18T22:00:00+00:00"}}
    out = api.load_proposal_decision_review(
        proposal_payload=payload, proposal=art["proposal"],
        hoc_assessment=assessment, outcome_evidence=_evidence(),
        aligned_returns=_returns(BOOK + ["NEW"]))
    assert out["status"] == api.STATUS_OK
    assert out["read_only"] is True and out["writes_nothing"] is True
    assert out["mutates_proposal"] is False
    assert out["runtime_llm_dependency"] == "NONE"
    assert out["manual_approval_required"] is True
    assert out["proposal_id"] == art["proposal_id"]
    assert out["review"]["review_verdict"]["verdict"] in pdr.VERDICT_VOCAB


def test_read_owner_summary_matches_the_full_read():
    art, assessment = _broken_world()
    payload = {"state": "READY", "active_book": {"book_id": "book1"},
               "eligible_market_date": "2026-09-18",
               "artifact": {"proposal_id": art["proposal_id"],
                            "identity": art["identity"]}}
    kwargs = dict(proposal_payload=payload, proposal=art["proposal"],
                  hoc_assessment=assessment, outcome_evidence=_evidence(),
                  aligned_returns=_returns(BOOK + ["NEW"]))
    full = api.load_proposal_decision_review(**kwargs)
    summary = api.load_review_summary(**kwargs)
    assert summary["verdict"] == full["review"]["review_verdict"]["verdict"]
    assert summary["mandatory_changes"] == \
        full["review"]["change_classification"]["mandatory_change_count"]
    assert summary["is_an_approval"] is False


def test_read_owner_degrades_rather_than_crashing():
    def _boom(**_kw):
        raise RuntimeError("store is gone")
    out = api.load_proposal_decision_review(proposal_loader=_boom)
    assert out["status"] == api.STATUS_UNAVAILABLE
    assert out["review"] is None


def test_route_is_declared_and_is_a_get_only_surface():
    from paper_trader.api import app as app_mod
    routes = {r.path: r for r in app_mod.app.routes if hasattr(r, "methods")}
    assert api.ROUTE in routes
    assert routes[api.ROUTE].methods == {"GET"}


# --------------------------------------------------------------------------- #
# 12. The live acceptance case - READ-ONLY, and skipped when the store is absent
# --------------------------------------------------------------------------- #
_LIVE_INDEX = Path(r"D:\Stock_Prediction_app_data\reallocation_proposals\index.json")


@pytest.mark.skipif(not _LIVE_INDEX.exists(),
                    reason="the live reallocation-proposal store is not present")
def test_live_standing_proposal_is_reviewable_and_is_left_untouched():
    before = json.loads(_LIVE_INDEX.read_text(encoding="utf-8"))
    out = api.load_proposal_decision_review()
    assert out["status"] in api.STATUS_VOCAB
    if out["status"] == api.STATUS_OK:
        r = out["review"]
        assert r["review_verdict"]["verdict"] in pdr.VERDICT_VOCAB
        assert r["review_verdict"]["is_an_approval"] is False
        assert out["runtime_llm_dependency"] == "NONE"
        # every state must be present and internally consistent
        for name in pdr.STATE_ORDER:
            st = r["states"][name]
            assert st["expected_return_state"] == "NOT_CALIBRATED"
            assert st["positions"] is not None
        # the review must not have changed the artifact index in any way
    assert json.loads(_LIVE_INDEX.read_text(encoding="utf-8")) == before


@pytest.mark.skipif(not _LIVE_INDEX.exists(),
                    reason="the live reallocation-proposal store is not present")
def test_live_review_creates_no_order_and_no_fill():
    """The standing proposal must still be awaiting manual review afterwards."""
    from paper_trader.api import portfolio_decision as pdec
    before = pdec.load_portfolio_decision()
    api.load_proposal_decision_review()
    after = pdec.load_portfolio_decision()
    assert after["portfolio_decision_state"] == before["portfolio_decision_state"]
    assert after["decision"] == before["decision"] is None
    assert after["proposal_id"] == before["proposal_id"]
