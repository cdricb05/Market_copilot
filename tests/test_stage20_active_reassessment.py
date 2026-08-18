"""Stage 20 — Continuous Active Portfolio Reassessment & Proposal Cycle.

Hermetic suite (Workstream O). Every test is offline and deterministic: no provider, no
prediction service, no broker, no database, no live desk, no network. Artifacts are
written only under pytest's ``tmp_path``.

Coverage map (the required 54 checks plus the Stage-20 additions):

  * reassessment identity / idempotency ............. 1-5
  * every-holding assessment ....................... 6-15
  * economic portfolio gate ........................ 16-24
  * proposal integration ........................... 25-30
  * point-in-time / freshness ...................... 31-35
  * workflow ....................................... 36-40
  * safety ......................................... 41-50
  * forward evidence ............................... 51-54
  * explainability, history, attribution, guards ... 55+
"""
from __future__ import annotations

import io
import json
import re
import tokenize
from pathlib import Path

import pytest

from paper_trader.api import portfolio_reassessment as PRS
from paper_trader.engine import portfolio_reassessment as K

BOOK = "alpha_paper_book_1"
DATE = "2026-08-12"
PREV = "2026-08-11"


def _code_only(rel_path: str) -> str:
    """Source with comments and string literals stripped.

    A safety guard must assert on what the module DOES, not on what its documentation
    says it does not do — otherwise "this module never calls a broker" would fail the
    very check it describes.
    """
    src = Path(rel_path).read_text(encoding="utf-8")
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        name = tokenize.tok_name.get(tok.type, "")
        if name in ("COMMENT", "STRING", "FSTRING_MIDDLE", "NL"):
            continue
        out.append(tok.string)
    return " ".join(out)


# =========================================================================== #
# Deterministic fixtures — a synthetic but structurally faithful HOC assessment.
# =========================================================================== #
def _review(ticker, **kw):
    """One Slice-6 holding review row with realistic defaults."""
    base = {
        "ticker": ticker,
        "sector": "Tech",
        "current_quantity": 100,
        "current_weight": 0.04,
        "market_value": 4000.0,
        "current_rank": 10,
        "previous_rank": 10,
        "rank_change": 0,
        "current_score": 0.80,
        "score_components": {},
        "signal_strength": 0.80,
        "deterioration_state": K.hoc_kernel.DET_STABLE,
        "deterioration_reason_codes": [],
        "return_5d": 0.01,
        "return_20d": 0.02,
        "return_60d": 0.05,
        "volatility_20d": 0.20,
        "volatility_60d": 0.22,
        "drawdown_60d": -0.05,
        "risk_contribution_pct": 0.04,
        "concentration_contribution": 0.04,
        "median_dollar_volume_20d": 5.0e7,
        "estimated_days_to_liquidate": 0.1,
        "liquidity_state": K.hoc_kernel.LIQ_LIQUID,
        "strongest_replacement_ticker": None,
        "replacement_rank": None,
        "replacement_score": None,
        "replacement_sector": None,
        "gross_score_improvement": None,
        "risk_adjusted_improvement": None,
        "switching_cost_bps": 25.0,
        "switching_cost_usd": 10.0,
        "net_improvement": None,
        "recommendation": K.REC_HOLD,
        "recommendation_confidence": "HIGH",
        "reason_codes": [],
        "explanation": "seed",
        "required_data_complete": True,
    }
    base.update(kw)
    return base


def _hoc(reviews=None, *, state="READY", gaps=None, candidates=None,
         eligible=DATE, nav=100000.0, cash=0.0, assessment_hash="hoc_hash_1",
         ps_hash="ps_hash_1", ca_hash=None, econ_hash="econ_hash_1"):
    reviews = reviews if reviews is not None else [
        _review("T%02d" % i, current_weight=0.04, current_rank=i + 1) for i in range(25)]
    cands = candidates if candidates is not None else [
        {"ticker": "NEW1", "rank": 3, "score": 0.95, "combined_score": 0.95,
         "sector": "Health", "recommendation": "ADD"},
        {"ticker": "NEW2", "rank": 5, "score": 0.93, "combined_score": 0.93,
         "sector": "Fin", "recommendation": "ADD"},
    ]
    invested = sum((r.get("market_value") or 0.0) for r in reviews)
    return {
        "schema_version": "holding_opportunity_cost.v1",
        "eligible_market_date": eligible,
        "active_book_id": BOOK,
        "assessment_state": state,
        "assessment_hash": assessment_hash,
        "policy": {"policy_version": "hoc_decision_policy.v1"},
        "portfolio_summary": {
            "nav": nav, "cash": cash, "invested_value": invested,
            "holdings_count": len(reviews),
            "max_name_weight": max([r["current_weight"] for r in reviews] or [0]),
            "max_name_ticker": reviews[0]["ticker"] if reviews else None,
            "max_sector_weight": 1.0, "max_sector": "Tech",
            "sector_weights": {"Tech": 1.0},
            "herfindahl_index": sum(r["current_weight"] ** 2 for r in reviews),
            "portfolio_variance_daily": 0.0001,
            "risk_contribution_state": "AVAILABLE",
        },
        "recommendation_counts": {"HOLD": len(reviews), "REDUCE": 0, "EXIT": 0,
                                  "REPLACE": 0, "ADD": len(cands)},
        "holding_reviews": reviews,
        "addition_candidates": cands,
        "diagnostics": {"eligible_universe_size": 503},
        "data_quality": {"data_gaps": list(gaps or [])},
        "provenance": {"portfolio_state_hash": ps_hash,
                       # Stage 21 (Workstream 0E): the ECONOMIC fingerprint is what a
                       # currency check binds to. `portfolio_state_hash` above embeds
                       # this assessment's own output and must never be used for it.
                       "economic_state_hash": econ_hash,
                       "corporate_actions_hash": ca_hash,
                       "universe_scoring_hash": "us_hash_1",
                       "hoc_assessment_hash": assessment_hash},
    }


def _portfolio_state(*, ps_hash="ps_hash_1", nav=100000.0, cash=0.0, ca_fp=None,
                     eligible=DATE, econ_hash="econ_hash_1"):
    return {
        "dates": {"eligible_market_date": eligible, "valuation_date": eligible},
        "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1",
                        "status": "ACTIVE", "initialized": True, "holdings_count": 25},
        "capital": {"nav": nav, "cash": cash},
        "state_hash": ps_hash,
        "economic_state_hash": econ_hash,
        "corporate_actions": ({"registry_fingerprint": ca_fp, "actions": []}
                              if ca_fp is not None else {}),
    }


def _scoring(output_hash="us_hash_1"):
    return {
        "output_hash": output_hash,
        "input_contract_hash": "uic_hash_1",
        "strategy_id": "fundamental_momentum_50_50_v1",
        "strategy_version": "v1",
        "primary_model_id": "fundamental_momentum_50_50_v1",
        "champion_model_id": "composite_sn",
        "model_registry_version": "29",
        "universe_id": "phase8v_combined_eodhd_price_fundamentals_universe",
    }


def _freshness(rows=None, eligible=DATE):
    default = [
        {"source_id": "owned_daily_prices", "status": "FRESH", "as_of_date": eligible,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book", "reason": "current",
         "expected_through_date": eligible},
        {"source_id": "price_score_refresh", "status": "FRESH", "as_of_date": eligible,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine", "reason": "current",
         "expected_through_date": eligible},
        {"source_id": "fundamental_quarterly", "status": "NOT_DUE",
         "as_of_date": "2026-05-22", "cadence": "QUARTERLY",
         "required_for_portfolio_reassessment": False,
         "authoritative_owner": "api.multi_horizon_engine", "reason": "not due",
         "expected_through_date": "2026-05-22"},
    ]
    return {"eligible_market_date": eligible, "source_freshness": rows or default}


def _policy(**kw):
    p = dict(K.default_policy())
    p.update(kw)
    return p


def _run(hoc=None, *, ps=None, sc=None, fr=None, hist=None, policy=None, stale=None):
    return PRS.run_reassessment(
        input_contract=PRS.build_input_contract(
            portfolio_state=ps or _portfolio_state(),
            scoring=sc or _scoring(),
            hoc_assessment=hoc if hoc is not None else _hoc(),
            freshness=fr if fr is not None else _freshness(),
            recent_change_history=hist or [],
            corporate_action_stale=stale,
            policy=_policy(**(policy or {}))),
        policy=policy or {})


def _res(**kw):
    return _run(**kw)["reassessment"]


# =========================================================================== #
# 1-5  REASSESSMENT IDENTITY / IDEMPOTENCY
# =========================================================================== #
def test_01_run_id_and_hash_are_deterministic():
    a = _run()
    b = _run()
    assert a["reassessment"]["reassessment_hash"] == b["reassessment"]["reassessment_hash"]
    ida = PRS.artifact_identity(input_contract=a["input_contract"],
                                result=a["reassessment"])
    idb = PRS.artifact_identity(input_contract=b["input_contract"],
                                result=b["reassessment"])
    assert ida == idb
    assert PRS.artifact_id_for(ida) == PRS.artifact_id_for(idb)
    assert PRS.artifact_id_for(ida).startswith("prs_%s_%s_" % (DATE, BOOK))


def test_02_exact_rerun_reuses_the_artifact_and_appends_no_second_history_row(tmp_path):
    run = _run()
    p1 = PRS.persist_reassessment(result=run["reassessment"],
                                  input_contract=run["input_contract"],
                                  reassessment_dir=tmp_path)
    assert p1["status"] == "CREATED" and p1["history_appended"] is True
    run2 = _run()
    p2 = PRS.persist_reassessment(result=run2["reassessment"],
                                  input_contract=run2["input_contract"],
                                  reassessment_dir=tmp_path)
    assert p2["status"] == "REUSED_EXISTING"
    assert p2["artifact_id"] == p1["artifact_id"]
    assert p2["history_appended"] is False
    # Exactly ONE artifact and ONE history row on disk.
    arts = list((Path(tmp_path) / "artifacts").glob("*.json"))
    assert len(arts) == 1
    assert len(PRS.load_history(reassessment_dir=tmp_path)) == 1


def test_03_changed_rank_snapshot_creates_a_new_assessment(tmp_path):
    a = _res()
    b = _res(sc=_scoring(output_hash="us_hash_CHANGED"),
             hoc=_hoc(assessment_hash="hoc_hash_2"))
    assert a["reassessment_hash"] != b["reassessment_hash"]
    run_a = _run()
    run_b = _run(sc=_scoring(output_hash="us_hash_CHANGED"),
                 hoc=_hoc(assessment_hash="hoc_hash_2"))
    ia = PRS.artifact_identity(input_contract=run_a["input_contract"],
                               result=run_a["reassessment"])
    ib = PRS.artifact_identity(input_contract=run_b["input_contract"],
                               result=run_b["reassessment"])
    assert ia["universe_scoring_hash"] != ib["universe_scoring_hash"]
    PRS.persist_reassessment(result=run_a["reassessment"],
                             input_contract=run_a["input_contract"],
                             reassessment_dir=tmp_path)
    conflict = PRS.persist_reassessment(result=run_b["reassessment"],
                                        input_contract=run_b["input_contract"],
                                        reassessment_dir=tmp_path)
    # An immutable artifact is NEVER overwritten for the same (book, date).
    assert conflict["status"] == "CONFLICT_REJECTED" and conflict["conflict"] is True
    assert conflict["persisted"] is False


def test_04_changed_holdings_create_a_new_assessment():
    base = _res()
    changed = _res(hoc=_hoc(
        reviews=[_review("T%02d" % i, current_weight=0.04, current_rank=i + 1)
                 for i in range(24)] + [_review("XYZ", current_weight=0.04,
                                                current_rank=25)],
        assessment_hash="hoc_hash_holdings"))
    assert base["reassessment_hash"] != changed["reassessment_hash"]
    a = PRS.build_input_contract(portfolio_state=_portfolio_state(), scoring=_scoring(),
                                 hoc_assessment=_hoc(), freshness=_freshness())
    b = PRS.build_input_contract(
        portfolio_state=_portfolio_state(), scoring=_scoring(),
        hoc_assessment=_hoc(reviews=[_review("ZZZ")]), freshness=_freshness())
    assert a["holdings_snapshot_hash"] != b["holdings_snapshot_hash"]


def test_05_changed_policy_version_creates_a_new_assessment():
    a = _res()
    b = _res(policy={"min_portfolio_net_improvement": 0.99})
    assert a["reassessment_hash"] != b["reassessment_hash"]
    # The policy is folded into the hash, so a threshold change can never silently
    # re-label an existing assessment.
    assert a["policy"]["min_portfolio_net_improvement"] != \
        b["policy"]["min_portfolio_net_improvement"]


# =========================================================================== #
# 6-15  EVERY-HOLDING ASSESSMENT
# =========================================================================== #
def test_06_all_current_holdings_are_evaluated():
    r = _res()
    assert len(r["holding_assessments"]) == 25
    assert r["decision"]["holdings_evaluated"] == 25
    assert {a["ticker"] for a in r["holding_assessments"]} == {
        "T%02d" % i for i in range(25)}


def test_07_strongest_eligible_alternative_is_carried_verbatim():
    rv = _review("AAA", recommendation=K.REC_REPLACE,
                 strongest_replacement_ticker="NEW1", replacement_rank=3,
                 replacement_score=0.95, gross_score_improvement=0.15,
                 risk_adjusted_improvement=0.15, net_improvement=0.12)
    r = _res(hoc=_hoc(reviews=[rv] + [_review("T%02d" % i) for i in range(24)]))
    row = next(a for a in r["holding_assessments"] if a["ticker"] == "AAA")
    assert row["strongest_replacement_ticker"] == "NEW1"
    assert row["replacement_rank"] == 3
    assert row["expected_gross_improvement"] == 0.15
    assert row["expected_net_improvement"] == 0.12


def test_08_a_currently_allocated_name_is_never_an_external_replacement():
    # T00 is HELD; it must never appear in strongest_alternatives even if the HOC
    # addition-candidate list is polluted with it.
    r = _res(hoc=_hoc(candidates=[
        {"ticker": "T00", "rank": 1, "score": 0.99, "sector": "Tech"},
        {"ticker": "NEW1", "rank": 3, "score": 0.95, "sector": "Health"}]))
    alts = {a["ticker"] for a in r["strongest_alternatives"]}
    assert "T00" not in alts
    assert "NEW1" in alts
    held = {a["ticker"] for a in r["holding_assessments"]}
    assert not (alts & held)


def test_09_rank_change_semantics_are_preserved():
    rows = [_review("UP", current_rank=5, previous_rank=40, rank_change=35),
            _review("DOWN", current_rank=60, previous_rank=16, rank_change=-44),
            _review("FLAT", current_rank=18, previous_rank=18, rank_change=0)]
    r = _res(hoc=_hoc(reviews=rows))
    by = {a["ticker"]: a for a in r["holding_assessments"]}
    assert by["UP"]["rank_change"] == 35
    assert by["DOWN"]["rank_change"] == -44
    assert by["FLAT"]["rank_change"] == 0


def test_10_missing_prior_rank_stays_explicit_and_is_never_inferred():
    rows = [_review("NOPRIOR", previous_rank=None, rank_change=None)]
    r = _res(hoc=_hoc(reviews=rows))
    row = r["holding_assessments"][0]
    assert row["previous_rank"] is None
    assert row["rank_change"] is None
    assert row["prior_rank_state"] == "PRIOR_RANK_UNAVAILABLE"
    assert "prior rank unavailable" in row["explanation"]


def test_11_deterioration_is_captured():
    rows = [_review("BAD", deterioration_state=K.hoc_kernel.DET_DETERIORATING,
                    deterioration_reason_codes=["RANK_WORSENED"])]
    r = _res(hoc=_hoc(reviews=rows))
    row = r["holding_assessments"][0]
    assert row["deterioration_state"] == "DETERIORATING"
    assert row["deterioration_reason_codes"] == ["RANK_WORSENED"]


def test_12_drawdown_is_captured():
    rows = [_review("DD", drawdown_60d=-0.184)]
    r = _res(hoc=_hoc(reviews=rows))
    assert r["holding_assessments"][0]["drawdown_60d"] == -0.184
    assert r["portfolio_summary"]["worst_holding_drawdown_60d"] == -0.184


def test_13_risk_contribution_is_captured():
    rows = [_review("RC", risk_contribution_pct=0.19)]
    r = _res(hoc=_hoc(reviews=rows))
    assert r["holding_assessments"][0]["risk_contribution_pct"] == 0.19


def test_14_liquidity_is_captured():
    rows = [_review("ILQ", liquidity_state=K.hoc_kernel.LIQ_ILLIQUID,
                    estimated_days_to_liquidate=9.5)]
    r = _res(hoc=_hoc(reviews=rows))
    row = r["holding_assessments"][0]
    assert row["liquidity_state"] == "ILLIQUID"
    assert row["estimated_days_to_liquidate"] == 9.5


def test_15_switching_costs_are_reused_never_recomputed():
    rows = [_review("SC", switching_cost_bps=25.0, switching_cost_usd=1234.56)]
    r = _res(hoc=_hoc(reviews=rows))
    row = r["holding_assessments"][0]
    assert row["switching_cost_bps"] == 25.0
    assert row["switching_cost_usd"] == 1234.56
    # The kernel never re-derives a per-name switching cost.
    src = _code_only("engine/portfolio_reassessment.py")
    assert "def switching_cost" not in src
    assert "def compute_risk_contributions" not in src


# =========================================================================== #
# 16-24  ECONOMIC PORTFOLIO GATE
# =========================================================================== #
def _replace_row(ticker, *, weight, gross, net, rank=60, prev=16):
    return _review(ticker, current_weight=weight, market_value=weight * 100000.0,
                   current_rank=rank, previous_rank=prev, rank_change=prev - rank,
                   recommendation=K.REC_REPLACE,
                   deterioration_state=K.hoc_kernel.DET_DETERIORATING,
                   strongest_replacement_ticker="NEW1", replacement_rank=3,
                   replacement_score=0.95, gross_score_improvement=gross,
                   risk_adjusted_improvement=gross, net_improvement=net,
                   switching_cost_usd=weight * 100000.0 * 0.0025)


def _book(actionable, *, n_hold=None):
    """One 25-name equal-weight book with the supplied actionable rows."""
    n_hold = n_hold if n_hold is not None else (25 - len(actionable))
    fill = [_review("H%02d" % i, current_weight=0.04, market_value=4000.0,
                    current_rank=i + 1) for i in range(n_hold)]
    return actionable + fill


def test_16_small_raw_improvement_negative_after_cost_is_no_change():
    # One 4% position with a tiny raw uplift: 0.04 * 0.01 = 0.0004 gross, while the
    # two-way turnover cost hurdle is 0.08 * 25bps * 0.001 = 0.002 -> NET NEGATIVE.
    rows = _book([_replace_row("SMALL", weight=0.04, gross=0.01, net=0.005)])
    r = _res(hoc=_hoc(reviews=rows))
    d = r["decision"]
    assert d["expected_gross_improvement"] > 0
    assert d["expected_net_improvement"] < 0
    assert r["reassessment_state"] == K.STATE_NO_CHANGE
    assert K.GATE_NET_NON_POSITIVE in d["reason_codes"]
    assert d["proposal_required"] is False


def test_17_meaningful_net_improvement_requires_a_proposal():
    # Four 4% positions each with a large uplift: gross 4 * 0.04 * 0.90 = 0.144,
    # cost 0.32 * 25 * 0.001 = 0.008 -> net 0.136 >> the 0.05 hurdle.
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    r = _res(hoc=_hoc(reviews=rows))
    d = r["decision"]
    assert d["expected_net_improvement"] > d["net_improvement_hurdle"]
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert d["proposal_required"] is True
    assert K.GATE_CLEARED in d["reason_codes"]


def test_18_turnover_budget_is_deferred_to_the_complete_target_owner():
    """Release 29.3 — the turnover BUDGET is a property of the complete target.

    The release-set estimate is an UPPER BOUND: the proposal owner may retain an
    incumbent when no feasible net-positive replacement exists, which lowers the real
    turnover. Judging the budget here would reject plans that are in fact within it, so
    the reassessment publishes the estimate as explicitly non-binding context and
    engine.reallocation_proposal decides it once, on the complete target.
    """
    rows = _book([_replace_row("A%d" % i, weight=0.04, gross=0.90, net=0.85)
                  for i in range(10)])
    r = _res(hoc=_hoc(reviews=rows))
    d = r["decision"]
    assert d["expected_one_way_turnover"] > d["turnover_budget"]
    # The estimate is published, and explicitly NOT binding here.
    assert d["turnover_budget_binding_here"] is False
    assert d["expected_turnover_basis"] == "PRE_PROPOSAL_RELEASE_SET_ESTIMATE"
    assert K.CHURN_TURNOVER_BUDGET not in d["blockers"]
    # Ownership is stated structurally, and the constraint is not duplicated.
    own = d["constraint_ownership"]
    assert own["duplicated"] is False
    assert own["deferred_to_complete_target"]["owner"] == K.TARGET_ENGINE_OWNER
    assert K.CHURN_TURNOVER_BUDGET in own["deferred_to_complete_target"]["constraints"]


def test_19_retained_book_concentration_is_context_never_a_blocker():
    """Release 29.3 — the retained stub must be renormalised to 1.0 to be compared, so
    exiting most of the book ALWAYS 'raises' concentration even though no dollar has
    moved into any surviving name. That is a renormalisation artifact of an incomplete
    portfolio, not economics, so it can never veto the ask."""
    actionable = [_replace_row("X%d" % i, weight=0.03, gross=0.90, net=0.85)
                  for i in range(10)]
    keep = [_review("BIG", current_weight=0.50, market_value=50000.0)]
    r = _res(hoc=_hoc(reviews=actionable + keep))
    d = r["decision"]
    # The arithmetic is still published — honestly labelled for what it is.
    assert d["expected_concentration_change"] > r["policy"]["max_concentration_increase"]
    assert d["concentration_basis"] == "PRE_PROPOSAL_RETAINED_BOOK_RENORMALISED"
    assert r["concentration"]["basis"] == "RETAINED_BOOK_RENORMALISED"
    # ...but it blocks nothing here.
    assert K.GATE_CONCENTRATION not in d["blockers"]
    assert K.GATE_SECTOR_CAP not in d["blockers"]
    own = d["constraint_ownership"]["deferred_to_complete_target"]
    assert K.GATE_CONCENTRATION in own["constraints"]
    assert K.GATE_SECTOR_CAP in own["constraints"]


def test_20_liquidity_violation_blocks_the_proposal():
    rows = _book([_replace_row("ILQ", weight=0.04, gross=0.90, net=0.85)])
    rows[0]["liquidity_state"] = K.hoc_kernel.LIQ_ILLIQUID
    # Enough uplift to clear the hurdle so ONLY liquidity can block it.
    rows[0]["gross_score_improvement"] = 5.0
    rows[0]["risk_adjusted_improvement"] = 5.0
    r = _res(hoc=_hoc(reviews=rows))
    d = r["decision"]
    assert d["expected_net_improvement"] > d["net_improvement_hurdle"]
    assert r["reassessment_state"] == K.STATE_CHANGE_CANDIDATE
    assert K.GATE_LIQUIDITY in d["blockers"]


def test_21_post_change_risk_is_owned_by_the_complete_target_engine():
    """Release 29.3 — post-change portfolio risk cannot be known before the released
    capital is allocated. The reassessment asks; engine.reallocation_proposal answers."""
    actionable = [_replace_row("X%d" % i, weight=0.03, gross=5.0, net=4.9)
                  for i in range(8)]
    keep = [_review("BIG", current_weight=0.60, market_value=60000.0)]
    r = _res(hoc=_hoc(reviews=actionable + keep))
    d = r["decision"]
    assert d["expected_net_improvement"] > d["net_improvement_hurdle"]
    # The economics clear, so the ask is made; risk is judged on the complete target.
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert K.GATE_RISK_DETERIORATION not in d["blockers"]
    assert d["portfolio_volatility_after_state"] == K.VOLATILITY_AFTER_STATE_PRE_PROPOSAL
    assert d["target_tracking_error"] is None
    assert d["target_tracking_error_owner"] == K.TARGET_ENGINE_OWNER
    assert K.GATE_RISK_DETERIORATION in (
        d["constraint_ownership"]["deferred_to_complete_target"]["constraints"])


def test_22_transaction_costs_are_counted_exactly_once():
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85)])
    r = _res(hoc=_hoc(reviews=rows))
    d = r["decision"]
    pol = r["policy"]
    one_way = d["expected_one_way_turnover"]
    assert d["expected_two_way_turnover"] == pytest.approx(2.0 * one_way, abs=1e-9)
    expected_notional = 2.0 * one_way * 100000.0
    assert d["expected_traded_notional"] == pytest.approx(expected_notional, abs=0.01)
    assert d["expected_transaction_cost_usd"] == pytest.approx(
        expected_notional * pol["cost_rate_per_side"], abs=0.01)
    assert d["expected_transaction_cost_score_points"] == pytest.approx(
        2.0 * one_way * pol["round_trip_cost_bps"] * pol["score_points_per_cost_bp"],
        abs=1e-9)
    assert d["transaction_cost_counted_once"] is True


def test_23_churn_cooldown_prevents_an_immediate_reversal():
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    hist = [{"eligible_market_date": PREV, "ticker": t, "direction": "IN"}
            for t in ("A", "B", "C", "D")]
    r = _res(hoc=_hoc(reviews=rows), hist=hist)
    ch = r["churn_control"]
    assert set(ch["protected_tickers"]) == {"A", "B", "C", "D"}
    assert K.CHURN_COOLDOWN in ch["reason_codes"]
    assert K.CHURN_REVERSAL in ch["reason_codes"]      # IN then OUT is a whipsaw
    # No capital moves and no proposal is produced.
    assert r["decision"]["expected_one_way_turnover"] == 0.0
    assert r["decision"]["proposal_required"] is False
    for a in r["holding_assessments"]:
        if a["ticker"] in ("A", "B", "C", "D"):
            assert a["recommendation"] == K.REC_HOLD
            assert a["action_withheld"] is True
            assert a["churn_protected"] is True


def test_24_a_sufficiently_strong_opportunity_clears_the_hurdle():
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    # An unrelated history keeps the churn controls inactive for these names.
    hist = [{"eligible_market_date": PREV, "ticker": "ZZZ", "direction": "OUT"}]
    r = _res(hoc=_hoc(reviews=rows), hist=hist)
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert r["churn_control"]["protected_tickers"] == []


def test_24b_a_mandatory_exit_of_an_ineligible_holding_always_acts():
    rows = _book([_review("BROKEN", current_weight=0.04, market_value=4000.0,
                          recommendation=K.REC_EXIT,
                          deterioration_state=K.hoc_kernel.DET_BROKEN,
                          deterioration_reason_codes=["NOT_ELIGIBLE"],
                          gross_score_improvement=0.0,
                          risk_adjusted_improvement=0.0, net_improvement=0.0)])
    r = _res(hoc=_hoc(reviews=rows))
    assert r["decision"]["mandatory_exit_tickers"] == ["BROKEN"]
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert K.GATE_MANDATORY_EXIT in r["decision"]["reason_codes"]


def test_24b1_a_mandatory_exit_with_NO_replacement_comparison_still_acts():
    """A structurally BROKEN holding has no replacement, so Slice 6 reports no
    improvement for it. That absence must NOT make the portfolio unmeasurable — otherwise
    an ineligible name would be trapped in the book forever."""
    rows = _book([_review("BROKEN", current_weight=0.04, market_value=4000.0,
                          recommendation=K.REC_EXIT,
                          deterioration_state=K.hoc_kernel.DET_BROKEN,
                          deterioration_reason_codes=["NOT_ELIGIBLE"],
                          strongest_replacement_ticker=None,
                          gross_score_improvement=None,
                          risk_adjusted_improvement=None, net_improvement=None)])
    r = _res(hoc=_hoc(reviews=rows))
    d = r["decision"]
    assert K.GATE_IMPROVEMENT_UNMEASURABLE not in d["blockers"]
    assert d["expected_gross_improvement"] is not None
    assert d["mandatory_exit_tickers"] == ["BROKEN"]
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert K.GATE_MANDATORY_EXIT in d["reason_codes"]
    # The missing comparison is reported honestly, not silently absorbed.
    assert "MANDATORY_EXIT_IMPROVEMENT_NOT_APPLICABLE" in r["data_gaps"]
    assert any(a.get("mandatory") for a in d["actionable_holdings"])


def test_24b2_a_hard_feasibility_blocker_still_withholds_a_mandatory_exit():
    """Release 29.3 — the mandatory eligibility-exit override defeats the ECONOMIC gates
    but NEVER a hard feasibility blocker. A turnover-budget breach is no longer a
    reassessment blocker at all (it belongs to the complete-target owner), so the hard
    case is proved with the blocker this kernel genuinely owns: illiquidity."""
    broken = [_review("B%d" % i, current_weight=0.04, market_value=4000.0,
                      recommendation=K.REC_EXIT,
                      deterioration_state=K.hoc_kernel.DET_BROKEN,
                      deterioration_reason_codes=["NOT_ELIGIBLE"],
                      liquidity_state=K.hoc_kernel.LIQ_ILLIQUID,
                      gross_score_improvement=None, risk_adjusted_improvement=None,
                      net_improvement=None) for i in range(10)]
    r = _res(hoc=_hoc(reviews=_book(broken)))
    d = r["decision"]
    assert K.GATE_LIQUIDITY in d["blockers"]
    assert r["reassessment_state"] == K.STATE_CHANGE_CANDIDATE
    assert K.GATE_MANDATORY_EXIT not in d["reason_codes"]
    assert K.GATE_MANDATORY_EXIT_WITHHELD in d["reason_codes"]
    # The operator contract must say REQUIRED-IF, never "must exit now".
    pol = d["mandatory_exit_policy"]
    assert pol["withheld"] is True and pol["override_applied"] is False
    assert pol["obligation"] == "REQUIRED_IF_REALLOCATION_PROCEEDS"
    assert pol["authorizes_order"] is False
    assert pol["authorizes_sell_only_plan"] is False
    assert K.GATE_LIQUIDITY in pol["hard_blockers_present"]


def test_24b3_a_sub_hurdle_improvement_never_traps_an_ineligible_holding():
    """Release 29.3 — the documented policy always said an unmeasurable or sub-hurdle
    improvement must never trap an ineligible name in the book, but the implementation
    tested ``not blockers`` while BELOW_PORTFOLIO_NET_IMPROVEMENT_HURDLE was itself in
    ``blockers``. That is the live 2026-08-17 AIZ / SPG case."""
    weak = [_replace_row("W%d" % i, weight=0.01, gross=0.06, net=0.011)
            for i in range(3)]
    broken = [_review("AIZ", current_weight=0.04, market_value=4000.0,
                      recommendation=K.REC_EXIT,
                      deterioration_state=K.hoc_kernel.DET_BROKEN,
                      deterioration_reason_codes=["FELL_BELOW_EXIT_BUFFER"],
                      gross_score_improvement=None, risk_adjusted_improvement=None,
                      net_improvement=None)]
    r = _res(hoc=_hoc(reviews=_book(weak + broken)))
    d = r["decision"]
    assert "AIZ" in d["mandatory_exit_tickers"]
    assert r["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert K.GATE_MANDATORY_EXIT in d["reason_codes"]
    assert K.GATE_BELOW_NET_HURDLE not in d["blockers"]
    pol = d["mandatory_exit_policy"]
    assert pol["override_applied"] is True and pol["withheld"] is False
    assert pol["policy"] == K.MANDATORY_EXIT_POLICY
    assert K.GATE_BELOW_NET_HURDLE in pol["overrides"]
    # Clearing the ASK is not an authorisation to trade.
    assert pol["authorizes_order"] is False
    assert pol["requires_complete_target"] is True
    assert pol["manual_review_required"] is True


def test_24b3_a_non_mandatory_action_with_no_improvement_is_still_unmeasurable():
    """The exemption is narrow: only a BROKEN-driven EXIT. A REPLACE that somehow lost its
    comparison must still make the aggregate unmeasurable rather than count as zero."""
    rows = _book([_review("NOCMP", current_weight=0.04, market_value=4000.0,
                          recommendation=K.REC_REPLACE,
                          deterioration_state=K.hoc_kernel.DET_DETERIORATING,
                          strongest_replacement_ticker="NEW1",
                          gross_score_improvement=None,
                          risk_adjusted_improvement=None, net_improvement=None)])
    r = _res(hoc=_hoc(reviews=rows))
    assert K.GATE_IMPROVEMENT_UNMEASURABLE in r["decision"]["blockers"]
    assert r["reassessment_state"] == K.STATE_CHANGE_CANDIDATE


def test_24c_a_dust_position_cannot_manufacture_turnover():
    rows = _book([_replace_row("DUST", weight=0.001, gross=5.0, net=4.9)])
    r = _res(hoc=_hoc(reviews=rows))
    row = next(a for a in r["holding_assessments"] if a["ticker"] == "DUST")
    assert row["recommendation"] == K.REC_HOLD
    assert "BELOW_MIN_ACTIONABLE_WEIGHT" in row["withheld_reason_codes"]
    assert r["decision"]["expected_one_way_turnover"] == 0.0
    assert r["reassessment_state"] == K.STATE_NO_CHANGE


def test_24d_no_actionable_holding_is_a_quiet_no_change():
    r = _res()
    assert r["reassessment_state"] == K.STATE_NO_CHANGE
    assert K.GATE_NO_ACTIONABLE in r["decision"]["reason_codes"]
    assert r["attention"]["count"] == 0
    assert r["decision"]["expected_transaction_cost_usd"] == 0.0


def test_24e_thresholds_are_versioned_documented_and_manually_configurable(monkeypatch):
    pol = K.default_policy()
    for k in ("min_portfolio_net_improvement", "max_one_way_turnover_per_reassessment",
              "churn_cooldown_trading_days", "reversal_lookback_reassessments",
              "min_holdings_data_complete_fraction", "max_concentration_increase",
              "min_actionable_weight", "strongest_alternatives_max"):
        assert k in pol, k
    assert pol["policy_version"] == K.REASSESSMENT_POLICY_VERSION
    assert pol["churn_policy_version"] == K.CHURN_POLICY_VERSION
    # Every genuinely-new threshold carries an inline economic rationale.
    src = Path("engine/portfolio_reassessment.py").read_text(encoding="utf-8")
    for k in ("min_portfolio_net_improvement", "max_one_way_turnover_per_reassessment",
              "churn_cooldown_trading_days", "max_concentration_increase"):
        i = src.index('"%s"' % k)
        assert "rationale" in src[max(0, i - 1400):i].lower(), k
    # Manually configurable without a code change.
    monkeypatch.setenv(PRS.POLICY_OVERRIDE_ENV,
                       json.dumps({"min_portfolio_net_improvement": 0.42}))
    assert PRS.resolve_policy()["min_portfolio_net_improvement"] == 0.42
    # Unknown keys are ignored (no silent policy surface expansion).
    monkeypatch.setenv(PRS.POLICY_OVERRIDE_ENV, json.dumps({"bogus_knob": 1}))
    assert "bogus_knob" not in PRS.resolve_policy()


def test_24f_sensitivity_the_hurdle_boundary_flips_the_decision():
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    below = _res(hoc=_hoc(reviews=rows), policy={"min_portfolio_net_improvement": 99.0})
    above = _res(hoc=_hoc(reviews=rows), policy={"min_portfolio_net_improvement": 0.001})
    assert below["reassessment_state"] == K.STATE_CHANGE_CANDIDATE
    assert K.GATE_BELOW_NET_HURDLE in below["decision"]["blockers"]
    assert above["reassessment_state"] == K.STATE_PROPOSAL_READY


def test_24g_the_turnover_budget_no_longer_flips_the_reassessment_decision():
    """Release 29.3 — moving the budget to the complete-target owner means the
    reassessment verdict is INVARIANT to it. The boundary is exercised where it is now
    decided (engine.reallocation_proposal) in
    tests/test_release29_3_decision_integrity.py."""
    rows = _book([_replace_row("A%d" % i, weight=0.04, gross=0.90, net=0.85)
                  for i in range(5)])          # 0.20 one-way
    tight = _res(hoc=_hoc(reviews=rows),
                 policy={"max_one_way_turnover_per_reassessment": 0.10})
    loose = _res(hoc=_hoc(reviews=rows),
                 policy={"max_one_way_turnover_per_reassessment": 0.50})
    assert K.CHURN_TURNOVER_BUDGET not in tight["decision"]["blockers"]
    assert K.CHURN_TURNOVER_BUDGET not in loose["decision"]["blockers"]
    assert tight["reassessment_state"] == loose["reassessment_state"]         == K.STATE_PROPOSAL_READY


def test_24h_sensitivity_the_cooldown_window_boundary_flips_protection():
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85)])
    hist = [{"eligible_market_date": PREV, "ticker": "A", "direction": "OUT"}]
    on = _res(hoc=_hoc(reviews=rows), hist=hist,
              policy={"churn_cooldown_trading_days": 5,
                      "reversal_lookback_reassessments": 0})
    off = _res(hoc=_hoc(reviews=rows), hist=hist,
               policy={"churn_cooldown_trading_days": 0,
                       "reversal_lookback_reassessments": 0})
    assert on["churn_control"]["protected_tickers"] == ["A"]
    assert off["churn_control"]["protected_tickers"] == []


# =========================================================================== #
# 25-30  PROPOSAL INTEGRATION
# =========================================================================== #
def test_25_a_proposal_is_authorised_only_after_the_gate_clears():
    no_change = PRS.should_build_proposal(_res())
    assert no_change["build_proposal"] is False
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    ready = PRS.should_build_proposal(_res(hoc=_hoc(reviews=rows)))
    assert ready["build_proposal"] is True
    # Fail closed on every non-ready state.
    for st in (K.STATE_NOT_READY, K.STATE_NO_CHANGE, K.STATE_CHANGE_CANDIDATE,
               K.STATE_BLOCKED_DATA, K.STATE_BLOCKED_EVIDENCE, K.STATE_MANUAL_REVIEW):
        assert PRS.should_build_proposal({"reassessment_state": st})["build_proposal"] \
            is False
    assert PRS.should_build_proposal(None)["build_proposal"] is False


def test_26_the_proposal_reuses_the_canonical_target_engine():
    # Stage 20 never builds a target: the target engine is named as the owner and the
    # reassessment kernel defines no allocation function.
    v = PRS.should_build_proposal({"reassessment_state": K.STATE_PROPOSAL_READY})
    assert v["proposal_owner"] == "api.reallocation_proposal"
    assert v["target_engine_owner"] == "engine.reallocation_proposal"
    src = _code_only("engine/portfolio_reassessment.py")
    assert "def build_proposal(" not in src
    assert "proposed_weight" not in src
    assert "def _waterfill" not in src
    assert "_apply_joint_caps" not in src


def test_27_a_proposal_is_provenance_bound_to_the_reassessment():
    run = _run()
    b = PRS.proposal_binding(reassessment=run["reassessment"],
                             input_contract=run["input_contract"])
    assert b["reassessment_hash"] == run["reassessment"]["reassessment_hash"]
    for f in ("hoc_assessment_hash", "universe_scoring_hash",
              "universe_input_contract_hash", "portfolio_state_hash",
              "holdings_snapshot_hash", "eligible_market_date", "active_book_id",
              "reassessment_policy_version", "churn_policy_version"):
        assert b[f] is not None, f
    assert b["review_only"] is True and b["creates_orders"] is False


def test_28_an_identical_valid_proposal_is_reused_never_duplicated():
    res = _res()
    prov = res["provenance"]
    art = {"proposal_id": "rp_1",
           "identity": {"eligible_market_date": DATE, "active_book_id": BOOK,
                        "hoc_assessment_hash": prov["hoc_assessment_hash"],
                        "portfolio_state_hash": prov["portfolio_state_hash"],
                        "universe_scoring_hash": prov["universe_scoring_hash"],
                        "corporate_actions_hash": prov["corporate_actions_hash"]},
           "proposal": {"proposal_hash": "ph_1"}}
    ok = PRS.proposal_is_current_for(reassessment=res, proposal_artifact=art)
    assert ok["reusable"] is True and ok["proposal_id"] == "rp_1"
    stale = dict(art)
    stale["identity"] = dict(art["identity"], hoc_assessment_hash="DIFFERENT")
    bad = PRS.proposal_is_current_for(reassessment=res, proposal_artifact=stale)
    assert bad["reusable"] is False
    assert "hoc_assessment_hash" in bad["mismatched_fields"]
    assert PRS.proposal_is_current_for(reassessment=res,
                                       proposal_artifact=None)["reusable"] is False


def test_29_a_proposal_authorisation_creates_no_order():
    v = PRS.should_build_proposal({"reassessment_state": K.STATE_PROPOSAL_READY})
    assert v["creates_orders"] is False
    assert v["approves_proposal"] is False


def test_30_a_proposal_always_requires_manual_review():
    v = PRS.should_build_proposal({"reassessment_state": K.STATE_PROPOSAL_READY})
    assert v["manual_review_required"] is True
    r = _res()
    assert r["safety"]["manual_review"] is True
    assert r["safety"]["approved_proposal"] is False
    assert r["safety"]["confirmed_order_plan"] is False


# =========================================================================== #
# 31-35  POINT-IN-TIME / FRESHNESS
# =========================================================================== #
def test_31_missing_evidence_is_never_fabricated():
    r = _res()
    d = r["decision"]
    assert d["expected_return_improvement"] is None
    assert d["expected_return_state"] == "EXPECTED_RETURN_NOT_CALIBRATED"
    assert d["portfolio_volatility_after_state"] == "NOT_AVAILABLE_PRE_PROPOSAL"
    assert d["target_tracking_error"] is None
    assert d["target_tracking_error_owner"] == "engine.reallocation_proposal"
    for a in r["holding_assessments"]:
        assert a["expected_return_delta"] is None
        assert a["expected_return_delta_state"] == "EXPECTED_RETURN_NOT_CALIBRATED"


def test_32_stale_but_valid_slower_data_is_classified_honestly_and_does_not_block():
    rows = [
        {"source_id": "owned_daily_prices", "status": "FRESH", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True},
        {"source_id": "price_score_refresh", "status": "FRESH", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True},
        {"source_id": "fundamental_quarterly", "status": "STALE",
         "as_of_date": "2026-02-01", "cadence": "QUARTERLY",
         "required_for_portfolio_reassessment": False},
    ]
    r = _res(fr=_freshness(rows))
    q = r["input_quality"]
    byid = {i["source_id"]: i for i in q["inputs"]}
    assert byid["fundamental_quarterly"]["state"] == K.STALE_BUT_VALID
    assert byid["fundamental_quarterly"]["usage"] == K.USAGE_STALE
    assert q["blocking_codes"] == []
    assert "fundamental_quarterly_STALE_BUT_VALID" in q["degraded_codes"]
    assert r["reassessment_state"] != K.STATE_BLOCKED_DATA


def test_33_a_missing_critical_ranking_input_blocks_the_reassessment():
    rows = [
        {"source_id": "price_score_refresh", "status": "MISSING", "as_of_date": None,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True},
    ]
    r = _res(fr=_freshness(rows))
    assert r["reassessment_state"] == K.STATE_BLOCKED_DATA
    assert any("price_score_refresh" in b["code"] for b in r["blockers"])
    assert r["decision"]["proposal_required"] is False
    # A blocked reassessment can never authorise a proposal.
    assert PRS.should_build_proposal(r)["build_proposal"] is False


def test_34_partial_non_critical_evidence_is_handled_per_policy():
    incomplete = [_review("I%02d" % i, required_data_complete=(i < 15))
                  for i in range(25)]
    r = _res(hoc=_hoc(reviews=incomplete))
    # 15/25 = 0.60 < the 0.80 completeness floor -> BLOCKED, never extrapolated.
    assert r["reassessment_state"] == K.STATE_BLOCKED_DATA
    assert any(b["code"] == "INSUFFICIENT_HOLDING_DATA_COMPLETENESS"
               for b in r["blockers"])
    ok = [_review("I%02d" % i, required_data_complete=(i < 21)) for i in range(25)]
    r2 = _res(hoc=_hoc(reviews=ok))    # 21/25 = 0.84 >= 0.80
    assert r2["reassessment_state"] != K.STATE_BLOCKED_DATA


def test_35_the_exact_market_date_is_preserved_end_to_end():
    run = _run()
    assert run["reassessment"]["eligible_market_date"] == DATE
    assert run["input_contract"]["inputs_as_of_eligible_date"] == DATE
    assert run["reassessment"]["provenance"]["eligible_market_date"] == DATE
    # A future-dated input is a point-in-time gap, never silently accepted.
    rows = [{"source_id": "price_score_refresh", "status": "FUTURE_DATED",
             "as_of_date": "2026-08-20", "cadence": "DAILY",
             "required_for_portfolio_reassessment": True}]
    r = _res(fr=_freshness(rows))
    q = r["input_quality"]
    assert q["inputs"][0]["state"] == K.POINT_IN_TIME_GAP
    assert r["reassessment_state"] == K.STATE_BLOCKED_DATA


def test_35b_evidence_bound_to_a_changed_portfolio_blocks():
    stale = _res(stale={"stale": True, "reason": "MNST 2:1 split registered"})
    assert stale["reassessment_state"] == K.STATE_BLOCKED_EVIDENCE
    assert any(b["code"] == "STALE_CORPORATE_ACTION_EVIDENCE"
               for b in stale["blockers"])
    # Stage 21 (Workstream 0E): a REAL economic change (holdings / cash / NAV /
    # corporate actions) still blocks, bound to the ECONOMIC fingerprint.
    mismatch = _res(ps=_portfolio_state(econ_hash="econ_hash_MOVED"))
    assert mismatch["reassessment_state"] == K.STATE_BLOCKED_EVIDENCE
    assert any(b["code"] == "PORTFOLIO_STATE_CHANGED_SINCE_ASSESSMENT"
               for b in mismatch["blockers"])
    # ...while document-wide `state_hash` drift alone does NOT block. That hash embeds
    # the assessment's own output, so a fresh assessment invalidated itself on every run.
    doc_drift_only = _res(ps=_portfolio_state(ps_hash="ps_hash_MOVED"))
    assert doc_drift_only["reassessment_state"] != K.STATE_BLOCKED_EVIDENCE
    wrong_date = _res(hoc=_hoc(eligible="2026-08-11"))
    assert wrong_date["reassessment_state"] == K.STATE_BLOCKED_EVIDENCE


def test_35c_input_usage_is_reported_refreshed_reused_stale_missing_blocked():
    rows = [
        {"source_id": "a", "status": "FRESH", "as_of_date": DATE, "cadence": "DAILY",
         "required_for_portfolio_reassessment": False},
        {"source_id": "b", "status": "NOT_DUE", "as_of_date": "2026-05-01",
         "cadence": "QUARTERLY", "required_for_portfolio_reassessment": False},
        {"source_id": "c", "status": "STALE", "as_of_date": "2026-08-01",
         "cadence": "DAILY", "required_for_portfolio_reassessment": False},
        {"source_id": "d", "status": "MISSING", "as_of_date": None, "cadence": "DAILY",
         "required_for_portfolio_reassessment": False},
        {"source_id": "e", "status": "INCONSISTENT", "as_of_date": DATE,
         "cadence": "DAILY", "required_for_portfolio_reassessment": False},
    ]
    q = _res(fr=_freshness(rows))["input_quality"]
    by = {i["source_id"]: i for i in q["inputs"]}
    assert by["a"]["usage"] == K.USAGE_REFRESHED
    assert by["b"]["usage"] == K.USAGE_REUSED
    assert by["c"]["usage"] == K.USAGE_STALE
    assert by["d"]["usage"] == K.USAGE_MISSING
    assert by["e"]["usage"] == K.USAGE_BLOCKED
    assert q["point_in_time_honest"] is True


# =========================================================================== #
# 36-40  WORKFLOW
# =========================================================================== #
def test_36_no_change_yields_no_operator_action():
    p = PRS.build_presentation(state=K.STATE_NO_CHANGE, reassessment=_res())
    assert p["operator_state"] == "PORTFOLIO_CURRENT"
    assert p["task"] == "No portfolio change is economically justified"
    assert p["next_action"] == "No action required"
    assert p["primary_action"] is None


def test_37_a_proposal_yields_exactly_one_review_action():
    p = PRS.build_presentation(state=K.STATE_PROPOSAL_READY, reassessment=_res())
    assert p["operator_state"] == "MANUAL_REVIEW_REQUIRED"
    assert p["task"] == "Review the proposed portfolio change"
    assert p["next_action"] == "REVIEW PORTFOLIO PROPOSAL"
    assert p["primary_action"] == "REVIEW_PORTFOLIO_PROPOSAL"


def test_38_blocked_states_carry_recovery_or_no_action_semantics():
    for st, expect_action in ((K.STATE_BLOCKED_DATA, None),
                              (K.STATE_BLOCKED_EVIDENCE, None),
                              (K.STATE_CHANGE_CANDIDATE, None),
                              (K.STATE_NOT_READY, None)):
        p = PRS.build_presentation(state=st, reassessment=_res())
        assert p["primary_action"] is expect_action, st
        assert p["task"]
        assert p["next_action"]


def test_39_at_most_one_primary_action_per_state():
    for st in PRS.READ_STATE_VOCAB:
        p = PRS.build_presentation(state=st, reassessment=_res())
        assert isinstance(p.get("primary_action"), (str, type(None)))
    with_action = [st for st in PRS.READ_STATE_VOCAB
                   if PRS.build_presentation(state=st,
                                             reassessment=_res())["primary_action"]]
    assert set(with_action) == {K.STATE_PROPOSAL_READY, K.STATE_MANUAL_REVIEW}


def test_40_stage19_execution_outranks_a_fresh_proposal():
    """THE live-state invariant: while the 29-order rebalance is pending, a new
    reassessment/proposal must not overwrite, obscure or conflict with it."""
    exe = PRS.execution_precedence(
        rebalance_state="ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING",
        pending_orders=29)
    assert exe["execution_active"] is True
    assert exe["reassessment_outranked"] is True
    assert exe["new_proposal_may_supersede_execution"] is False
    p = PRS.build_presentation(state=K.STATE_PROPOSAL_READY, reassessment=_res(),
                               execution=exe)
    assert p["primary_action"] is None          # the reassessment CTA is suppressed
    assert p["execution_precedence"] is True
    assert "pending controlled paper rebalance" in p["next_action"]
    # Pending orders alone are enough, whatever the lifecycle label says.
    assert PRS.execution_precedence(rebalance_state=None,
                                    pending_orders=29)["execution_active"] is True
    # And with nothing in flight the reassessment may drive the action again.
    idle = PRS.execution_precedence(rebalance_state="REBALANCE_NO_PROPOSAL",
                                    pending_orders=0)
    assert idle["execution_active"] is False
    assert PRS.build_presentation(state=K.STATE_PROPOSAL_READY, reassessment=_res(),
                                  execution=idle)["primary_action"] == \
        "REVIEW_PORTFOLIO_PROPOSAL"


def test_40b_the_order_plan_review_state_also_holds_precedence():
    exe = PRS.execution_precedence(
        rebalance_state="PROPOSAL_APPROVED_ORDER_PLAN_REVIEW_REQUIRED",
        pending_orders=0)
    assert exe["execution_active"] is True


# =========================================================================== #
# 41-50  SAFETY
# =========================================================================== #
def test_41_a_read_writes_nothing(tmp_path):
    before = sorted(p.name for p in Path(tmp_path).rglob("*"))
    out = PRS.load_portfolio_reassessment(
        portfolio_state=_portfolio_state(), reassessment_dir=tmp_path,
        rebalance_state={})
    assert out["state"] == PRS.STATE_NOT_RUN
    assert out["review_only"] is True
    after = sorted(p.name for p in Path(tmp_path).rglob("*"))
    assert before == after
    # A summary read is a pure artifact read as well.
    s = PRS.load_reassessment_summary(active_book_id=BOOK, eligible_market_date=DATE,
                                      reassessment_dir=tmp_path)
    assert s["reassessment_available"] is False
    assert sorted(p.name for p in Path(tmp_path).rglob("*")) == after


def test_42_43_44_no_live_provider_prediction_or_broker_in_the_owners():
    owner = _code_only("api/portfolio_reassessment.py")
    kern = _code_only("engine/portfolio_reassessment.py")
    for token in ("requests.", "httpx.", "urlopen(", "eodhd", "yfinance", "polygon",
                  "predict(", "prediction_client", "127.0.0.1:9000", "broker",
                  "alpaca", "ibkr"):
        assert token not in owner, token
        assert token not in kern, token


def test_45_46_no_order_or_fill_creation_anywhere_in_the_owners():
    owner = _code_only("api/portfolio_reassessment.py")
    kern = _code_only("engine/portfolio_reassessment.py")
    for token in ("generate_orders(", "confirm_orders(", "create_order(",
                  "place_order(", "submit_order(", "settle_due_orders(",
                  "run_fill_cycle(", "confirm_rebalance_order_plan(",
                  "record_decision("):
        assert token not in owner, token
        assert token not in kern, token
    r = _res()
    for f in ("created_target", "created_order_plan", "created_orders", "created_fills"):
        assert r["safety"][f] is False, f


def test_47_no_holdings_cash_or_nav_mutation():
    r = _res()
    for f in ("changed_holdings", "changed_cash", "changed_nav"):
        assert r["safety"][f] is False, f
    owner = _code_only("api/portfolio_reassessment.py")
    assert "book_nav(" not in owner


def test_48_49_no_automatic_promotion_retraining_or_recalibration():
    r = _res()
    for f in ("model_promoted", "model_retrained", "model_recalibrated"):
        assert r["safety"][f] is False, f
    ic = PRS.build_input_contract(portfolio_state=_portfolio_state(), scoring=_scoring(),
                                  hoc_assessment=_hoc(), freshness=_freshness())
    assert ic["model_identity"]["automatic_promotion_allowed"] is False
    kern = _code_only("engine/portfolio_reassessment.py").lower()
    owner = _code_only("api/portfolio_reassessment.py").lower()
    for token in ("promote_model", "replace_champion", "retrain", "recalibrat"):
        assert token not in kern, token
        assert token not in owner, token
    # MODEL RECALIBRATION REMAINS A SEPARATE CYCLE: Stage 20 never absorbs the
    # research agent (Slice 8), which owns recalibration governance.
    assert "research_agent" not in kern
    assert "research_agent" not in owner


def test_50_no_automation_or_scheduled_trading_is_enabled():
    r = _res()
    assert r["safety"]["automation_enabled"] is False
    assert r["safety"]["scheduled_trading_enabled"] is False
    owner = _code_only("api/portfolio_reassessment.py").lower()
    kern = _code_only("engine/portfolio_reassessment.py").lower()
    for token in ("schedule", "cron", "apscheduler", "while true", "threading",
                  "subprocess"):
        assert token not in owner, token
        assert token not in kern, token


def test_50b_the_kernel_is_pure_no_io_and_no_clock():
    kern = _code_only("engine/portfolio_reassessment.py")
    for token in ("open(", "requests.", "sqlalchemy", "os.environ", "Path(",
                  "datetime.now(", "time.time("):
        assert token not in kern, token


def test_50c_the_read_contract_never_raises_on_a_broken_source():
    def _boom():
        raise RuntimeError("portfolio state exploded")

    out = PRS.load_portfolio_reassessment(portfolio_state_loader=_boom,
                                          rebalance_state={})
    assert out["state"] == PRS.STATE_UNAVAILABLE
    assert "exploded" in out["message"]


# =========================================================================== #
# 51-54  FORWARD EVIDENCE
# =========================================================================== #
def test_51_recommendation_history_is_append_only(tmp_path):
    run = _run()
    PRS.persist_reassessment(result=run["reassessment"],
                             input_contract=run["input_contract"],
                             reassessment_dir=tmp_path)
    rows = PRS.load_history(reassessment_dir=tmp_path)
    assert len(rows) == 1
    first = json.dumps(rows[0], sort_keys=True)
    # A DIFFERENT session appends; it never rewrites the earlier row.
    run2 = _run(ps=_portfolio_state(eligible="2026-08-13"),
                hoc=_hoc(eligible="2026-08-13", assessment_hash="hoc_hash_d2"),
                fr=_freshness(eligible="2026-08-13"))
    PRS.persist_reassessment(result=run2["reassessment"],
                             input_contract=run2["input_contract"],
                             reassessment_dir=tmp_path)
    rows2 = PRS.load_history(reassessment_dir=tmp_path)
    assert len(rows2) == 2
    assert json.dumps(rows2[0], sort_keys=True) == first
    assert [r["eligible_market_date"] for r in rows2] == [DATE, "2026-08-13"]
    assert all(r["immutable"] is True and r["backfilled"] is False for r in rows2)


def test_52_forward_outcomes_are_measured_only_when_genuinely_available():
    hist = [{"reassessment_id": "prs_1", "eligible_market_date": DATE,
             "decision": K.STATE_PROPOSAL_READY,
             "recommendations": [
                 {"ticker": "AAA", "recommendation": K.REC_REPLACE,
                  "strongest_replacement_ticker": "BBB", "current_weight": 0.04,
                  "expected_net_improvement": 0.2, "action_withheld": False},
                 {"ticker": "CCC", "recommendation": K.REC_HOLD,
                  "strongest_replacement_ticker": None, "current_weight": 0.04,
                  "action_withheld": False}]}]
    panel = {"series": {
        "AAA": {"dates": [DATE, "2026-08-13"], "adjusted_close": [100.0, 90.0]},
        "BBB": {"dates": [DATE, "2026-08-13"], "adjusted_close": [50.0, 60.0]},
        # CCC has NO forward close -> its outcome must stay PENDING.
        "CCC": {"dates": [DATE], "adjusted_close": [10.0]}}}
    att = PRS.build_attribution(history=hist, price_panel=panel, as_of="2026-08-13")
    by = {r["ticker"]: r for r in att["rows"]}
    assert by["AAA"]["outcome_state"] == "MEASURED"
    assert by["AAA"]["incumbent_forward_return"] == pytest.approx(-0.10, abs=1e-9)
    assert by["AAA"]["replacement_forward_return"] == pytest.approx(0.20, abs=1e-9)
    assert by["AAA"]["realized_spread"] == pytest.approx(0.30, abs=1e-9)
    assert by["AAA"]["portfolio_impact"] == pytest.approx(0.012, abs=1e-9)
    assert by["AAA"]["action_taken"] is True
    assert by["CCC"]["outcome_state"] == "PENDING"
    assert by["CCC"]["incumbent_forward_return"] is None
    assert by["CCC"]["realized_spread"] is None
    assert att["measured_count"] == 1 and att["pending_count"] == 1


def test_53_no_hindsight_backfill_and_the_gap_is_documented(tmp_path):
    h = PRS.load_reassessment_history(reassessment_dir=tmp_path)
    assert h["rows"] == [] and h["row_count"] == 0
    assert h["append_only"] is True and h["backfilled"] is False
    assert "not reconstructed" in h["historical_gap_note"].lower()
    assert h["read_only"] is True


def test_54_repeated_evidence_capture_is_idempotent(tmp_path):
    run = _run()
    for _ in range(3):
        PRS.persist_reassessment(result=run["reassessment"],
                                 input_contract=run["input_contract"],
                                 reassessment_dir=tmp_path)
    assert len(PRS.load_history(reassessment_dir=tmp_path)) == 1
    assert len(list((Path(tmp_path) / "artifacts").glob("*.json"))) == 1
    att = PRS.build_attribution(reassessment_dir=tmp_path, price_panel={"series": {}})
    att2 = PRS.build_attribution(reassessment_dir=tmp_path, price_panel={"series": {}})
    assert att["rows"] == att2["rows"]


def test_54b_attribution_changes_nothing():
    att = PRS.build_attribution(history=[], price_panel={"series": {}})
    for f in ("changes_model", "changes_thresholds", "changes_champion",
              "changes_portfolio"):
        assert att[f] is False, f
    assert att["read_only"] is True


def test_54c_the_churn_history_is_derived_from_immutable_evidence(tmp_path):
    run = _run(hoc=_hoc(reviews=_book(
        [_replace_row("A", weight=0.04, gross=0.90, net=0.85),
         _replace_row("B", weight=0.04, gross=0.90, net=0.85),
         _replace_row("C", weight=0.04, gross=0.90, net=0.85),
         _replace_row("D", weight=0.04, gross=0.90, net=0.85)])))
    PRS.persist_reassessment(result=run["reassessment"],
                             input_contract=run["input_contract"],
                             reassessment_dir=tmp_path)
    rows = PRS.recent_change_rows(reassessment_dir=tmp_path, active_book_id=BOOK,
                                  policy=PRS.resolve_policy())
    outs = {r["ticker"] for r in rows if r["direction"] == "OUT"}
    assert {"A", "B", "C", "D"} <= outs
    assert all(r["source"] == "reassessment" for r in rows)


# =========================================================================== #
# 55+  EXPLAINABILITY (Workstream K)
# =========================================================================== #
def test_55_every_holding_has_a_deterministic_generated_explanation():
    rows = [_replace_row("REP", weight=0.04, gross=0.90, net=0.85),
            _review("HLD"),
            _review("EXT", recommendation=K.REC_EXIT,
                    deterioration_state=K.hoc_kernel.DET_BROKEN,
                    deterioration_reason_codes=["NOT_ELIGIBLE"]),
            _review("RED", recommendation=K.REC_REDUCE,
                    reason_codes=["RISK_CONTRIBUTION_BREACH"])]
    a = _res(hoc=_hoc(reviews=rows))
    b = _res(hoc=_hoc(reviews=rows))
    for x, y in zip(a["holding_assessments"], b["holding_assessments"]):
        assert x["explanation"] == y["explanation"]
        assert x["explanation"] and len(x["explanation"]) > 40
        assert x["explanation"].startswith(x["recommendation"])


def test_56_a_hold_explanation_names_the_hurdle_it_failed():
    rows = _book([_replace_row("SMALL", weight=0.04, gross=0.01, net=0.005)])
    r = _res(hoc=_hoc(reviews=rows))
    row = next(a for a in r["holding_assessments"] if a["ticker"] == "SMALL")
    # A per-name REPLACE that survives its own hurdle can still be withheld at the
    # portfolio level; the explanation states the economics, not a guess.
    assert "REPLACE" in row["explanation"] or "HOLD" in row["explanation"]
    quiet = _res()
    hold = quiet["holding_assessments"][0]
    assert hold["explanation"].startswith("HOLD")
    assert "hurdle" in hold["explanation"]


def test_57_a_replace_explanation_names_the_replacement_and_the_cost():
    rows = _book([_replace_row("DOWN", weight=0.04, gross=0.90, net=0.85,
                               rank=60, prev=16)] +
                 [_replace_row("X%d" % i, weight=0.04, gross=0.90, net=0.85)
                  for i in range(3)])
    r = _res(hoc=_hoc(reviews=rows))
    row = next(a for a in r["holding_assessments"] if a["ticker"] == "DOWN")
    assert row["recommendation"] == K.REC_REPLACE
    assert "rank fell 44 places" in row["explanation"]
    assert "NEW1" in row["explanation"]
    assert "bps" in row["explanation"]
    assert "net hurdle" in row["explanation"]


def test_58_explanations_are_derived_only_from_canonical_fields():
    # No model call, no LLM: the kernel builds the sentence from the assessment fields
    # alone (the code, not the prose, is what is scanned).
    src = _code_only("engine/portfolio_reassessment.py").lower()
    for token in ("openai", "anthropic", "llm", "gpt", "claude", "completion",
                  "generate_text"):
        assert token not in src, token
    r = _res()
    assert r["explanation"]
    assert "best risk-adjusted use of capital" in r["explanation"]


def test_59_the_portfolio_explanation_states_the_economics():
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    r = _res(hoc=_hoc(reviews=rows))
    e = r["explanation"]
    assert "economically justified" in e
    assert "turnover" in e and "transaction cost" in e
    assert "nothing is approved or executed" in e.lower()


# =========================================================================== #
# 60+  ARTIFACT SCHEMA + ATTENTION HIERARCHY + GUARDS
# =========================================================================== #
def test_60_the_artifact_carries_the_complete_required_schema(tmp_path):
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    run = _run(hoc=_hoc(reviews=rows))
    p = PRS.persist_reassessment(result=run["reassessment"],
                                 input_contract=run["input_contract"],
                                 reassessment_dir=tmp_path)
    art = json.loads(Path(p["path"]).read_text(encoding="utf-8"))

    # --- run identity ---------------------------------------------------- #
    assert art["reassessment_id"] == p["artifact_id"]
    assert art["generated_at"]
    ident = art["identity"]
    for f in ("active_book_id", "eligible_market_date", "universe_scoring_hash",
              "portfolio_state_hash", "holdings_snapshot_hash", "hoc_assessment_hash",
              "model_identity", "hoc_decision_policy_version",
              "allocation_policy_version", "reassessment_policy_version",
              "churn_policy_version", "reassessment_hash"):
        assert f in ident, f

    res = art["reassessment"]
    # --- portfolio summary ------------------------------------------------ #
    for f in ("nav", "cash", "holdings_count", "gross_exposure", "net_exposure",
              "herfindahl_index", "max_name_weight", "max_sector_weight",
              "sector_weights", "current_portfolio_score", "worst_holding_drawdown_60d"):
        assert f in res["portfolio_summary"], f

    # --- every holding ---------------------------------------------------- #
    for f in ("ticker", "current_weight", "current_rank", "previous_rank", "rank_change",
              "signal_score", "deterioration_state", "return_20d", "drawdown_60d",
              "risk_contribution_pct", "concentration_contribution", "liquidity_state",
              "strongest_replacement_ticker", "replacement_rank",
              "expected_gross_improvement", "switching_cost_usd",
              "expected_net_improvement", "recommendation", "explanation"):
        assert f in res["holding_assessments"][0], f

    # --- strongest non-held candidates ------------------------------------ #
    for f in ("ticker", "rank", "expected_contribution_basis", "displaced_incumbent",
              "expected_net_improvement_vs_incumbent", "reason_not_selected"):
        assert f in res["strongest_alternatives"][0], f

    # --- portfolio-level decision ----------------------------------------- #
    for f in ("decision", "expected_one_way_turnover", "expected_transaction_cost_usd",
              "expected_gross_improvement", "expected_net_improvement",
              "expected_return_improvement", "expected_return_state",
              "expected_concentration_change", "expected_risk_change",
              "target_tracking_error", "strongest_evidence", "blockers",
              "reason_codes"):
        assert f in res["decision"], f
    assert res["decision"]["decision"] in K.REASSESSMENT_STATE_VOCAB


def test_61_the_attention_group_is_exception_first():
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _review("E", recommendation=K.REC_EXIT,
                          deterioration_state=K.hoc_kernel.DET_BROKEN),
                  _review("R", recommendation=K.REC_REDUCE)])
    r = _res(hoc=_hoc(reviews=rows))
    at = r["attention"]
    assert at["count"] == 3
    assert at["replace"] == ["A"] and at["exit"] == ["E"] and at["reduce"] == ["R"]
    # A quiet book surfaces NOTHING for attention even with 25 holdings.
    quiet = _res()
    assert quiet["attention"]["count"] == 0
    assert len(quiet["holding_assessments"]) == 25


def test_62_alternatives_report_why_a_candidate_was_not_selected():
    rows = _book([_review("INC", recommendation=K.REC_HOLD,
                          strongest_replacement_ticker="NEW1", replacement_rank=3,
                          replacement_score=0.95, gross_score_improvement=0.01,
                          risk_adjusted_improvement=0.01, net_improvement=0.005)])
    r = _res(hoc=_hoc(reviews=rows))
    alt = next(a for a in r["strongest_alternatives"] if a["ticker"] == "NEW1")
    assert alt["displaced_incumbent"] == "INC"
    assert alt["selected_as_replacement"] is False
    assert "hurdle" in (alt["reason_not_selected"] or "")
    assert alt["allocation"] is None
    assert alt["allocation_owner"] == "engine.reallocation_proposal"


def test_63_the_summary_read_matches_the_artifact(tmp_path):
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    run = _run(hoc=_hoc(reviews=rows))
    PRS.persist_reassessment(result=run["reassessment"],
                             input_contract=run["input_contract"],
                             reassessment_dir=tmp_path)
    s = PRS.load_reassessment_summary(active_book_id=BOOK, eligible_market_date=DATE,
                                      reassessment_dir=tmp_path)
    assert s["reassessment_available"] is True
    assert s["reassessment_state"] == K.STATE_PROPOSAL_READY
    assert s["proposal_required"] is True
    assert s["reassessment_hash"] == run["reassessment"]["reassessment_hash"]
    assert s["holdings_evaluated"] == run["reassessment"]["decision"]["holdings_evaluated"]
    assert s["expected_net_improvement"] == \
        run["reassessment"]["decision"]["expected_net_improvement"]


def test_64_a_blocked_or_not_ready_run_is_persisted_or_skipped_correctly(tmp_path):
    not_ready = K.build_reassessment(input_contract={})
    p = PRS.persist_reassessment(result=not_ready, input_contract={},
                                 reassessment_dir=tmp_path)
    assert p["status"] == "NOT_PERSISTED" and p["persisted"] is False
    assert not (Path(tmp_path) / "artifacts").exists()
    blocked_run = _run(fr=_freshness([
        {"source_id": "price_score_refresh", "status": "MISSING", "as_of_date": None,
         "cadence": "DAILY", "required_for_portfolio_reassessment": True}]))
    p2 = PRS.persist_reassessment(result=blocked_run["reassessment"],
                                  input_contract=blocked_run["input_contract"],
                                  reassessment_dir=tmp_path)
    # A BLOCKED decision IS durable evidence — the operator must be able to see it.
    assert p2["status"] == "CREATED"


def test_65_the_read_contract_exposes_the_full_operator_payload(tmp_path):
    run = _run()
    PRS.persist_reassessment(result=run["reassessment"],
                             input_contract=run["input_contract"],
                             reassessment_dir=tmp_path)
    out = PRS.load_portfolio_reassessment(portfolio_state=_portfolio_state(),
                                          reassessment_dir=tmp_path,
                                          rebalance_state={})
    assert out["state"] == K.STATE_NO_CHANGE
    for f in ("presentation", "execution_precedence", "proposal_boundary",
              "proposal_binding", "decision", "holding_assessments", "attention",
              "strongest_alternatives", "churn_control", "concentration",
              "input_quality", "explanation", "artifact", "safety", "provenance"):
        assert f in out, f
    assert out["review_only"] is True
    assert out["sole_execution_path"] == "POST /v1/operations/daily-research-cycle/run"
    assert out["proposal_boundary"]["build_proposal"] is False


def test_66_the_drc_gates_the_target_engine_and_orders_the_steps():
    from paper_trader.api import daily_research_cycle as drc
    seq = list(drc.STEP_SEQUENCE)
    assert drc.STEP_REASSESS_PORTFOLIO in seq
    assert seq.index(drc.STEP_HOLDING_OPP_COST) < seq.index(drc.STEP_REASSESS_PORTFOLIO)
    assert seq.index(drc.STEP_REASSESS_PORTFOLIO) < seq.index(drc.STEP_BUILD_REALLOCATION)
    gate = drc._reassessment_gate(None, {"available": False})
    assert gate["build_proposal"] is False          # fail closed
    gate2 = drc._reassessment_gate({"reassessment_state": K.STATE_PROPOSAL_READY}, {})
    assert gate2["build_proposal"] is True
    gate3 = drc._reassessment_gate({"reassessment_state": K.STATE_NO_CHANGE}, {})
    assert gate3["build_proposal"] is False


def test_67_the_workflow_owner_delegates_and_never_recomputes():
    wf = _code_only("api/workflow_state.py")
    assert "load_reassessment_summary" in wf
    assert "execution_precedence" in wf
    # No second economic gate / threshold lives in the workflow owner.
    for token in ("min_portfolio_net_improvement", "max_one_way_turnover_per_reassessment",
                  "def build_reassessment(", "score_points_per_cost_bp"):
        assert token not in wf, token


def test_68_the_ui_renders_the_contract_and_computes_nothing():
    ui = Path("api/ui/index.html").read_text(encoding="utf-8")
    assert ui.count("function loadPortfolioReassessment") == 1
    assert "/v1/operations/portfolio-reassessment" in ui
    assert 'id="reassess-card"' in ui
    assert 'id="reassess-attention-card"' in ui
    assert 'id="reassess-alternatives-card"' in ui
    # No native dialog, no create-orders control, no automation control in the block.
    start = ui.index("function loadPortfolioReassessment")
    end = ui.index("window.renderPortfolioReassessment")
    block = ui[start:end]
    for token in ("alert(", "confirm(", "prompt(", "Create Orders", "automation",
                  "new Date(", "Date.now("):
        assert token not in block, token


def test_69_the_architecture_guard_reports_every_stage20_invariant():
    import sys
    sys.path.insert(0, str(Path("scripts").resolve()))
    import audit_architecture as A          # noqa: E402
    rep = A.check_portfolio_reassessment_ownership(A._iter_source_files())
    assert rep["owners_present"] is True
    assert rep["second_calculation_owner_modules"] == []
    assert rep["second_composition_owner_modules"] == []
    assert rep["second_target_engine_modules"] == []
    assert rep["kernel_forks_neighbouring_owner"] == []
    assert rep["missing_delegation"] == []
    assert rep["owner_forbidden_calls"] == []
    assert rep["kernel_forbidden_calls"] == []
    assert rep["route_methods"] == ["GET"]
    assert rep["forbidden_routes_present"] == []
    assert rep["no_automatic_rebalance"] is True
    assert rep["signal_refresh_linked_to_reassessment"] is True
    assert rep["proposal_gated_by_reassessment"] is True
    assert rep["reassessment_ordered_before_proposal"] is True
    assert rep["workflow_delegates_to_owner"] is True
    assert rep["workflow_honours_execution_precedence"] is True
    assert rep["workflow_second_economic_gate"] == []
    assert rep["recalibration_remains_separate"] is True
    assert rep["history_append_only"] is True
    assert rep["no_hindsight_backfill_declared"] is True
    assert rep["ui_loader_count"] == 1
    assert rep["ui_client_assessment_logic"] == []
    assert rep["automatic_model_promotion_allowed"] is False
    assert rep["automatic_approval_allowed"] is False
    assert rep["cadence_enabled"] is False
    assert A.check_inventory_drift(A._iter_source_files())[
        "on_disk_not_in_inventory"] == []


def test_70_the_routes_are_read_only_and_registered():
    from paper_trader.api.app import app
    paths = {}
    for r in app.routes:
        p = getattr(r, "path", "")
        if "portfolio-reassessment" in p:
            paths.setdefault(p, set()).update(getattr(r, "methods", set()) or set())
    assert set(paths) == {"/v1/operations/portfolio-reassessment",
                          "/v1/operations/portfolio-reassessment/history",
                          "/v1/operations/portfolio-reassessment/attribution"}
    for p, methods in paths.items():
        assert methods <= {"GET", "HEAD"}, (p, methods)


def test_71_no_second_cost_risk_or_nav_model_is_introduced():
    raw = Path("engine/portfolio_reassessment.py").read_text(encoding="utf-8")
    kern = _code_only("engine/portfolio_reassessment.py")
    # The cost rate is READ from the policy (the API owner injects the canonical desk
    # constants), never re-declared inside the decision body.
    body = raw[raw.index("def build_reassessment("):]
    assert "0.00125" not in body
    assert "12.5" not in body
    assert re.search(r'pol\["cost_rate_per_side"\]', body)
    assert re.search(r'pol\["round_trip_cost_bps"\]', body)
    # No NAV or portfolio-state derivation.
    assert "def compute_nav" not in kern and "book_nav" not in kern
    # The covariance primitive is the Slice-6 one; the kernel defines no new risk math.
    assert "def _sample_covariance" not in kern
    assert "def compute_risk_contributions" not in kern


def test_72_the_live_pending_execution_scenario_is_fully_protected():
    """End-to-end guard for the live state: today's 29 SUBMITTED orders."""
    rows = _book([_replace_row("A", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("B", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("C", weight=0.04, gross=0.90, net=0.85),
                  _replace_row("D", weight=0.04, gross=0.90, net=0.85)])
    res = _res(hoc=_hoc(reviews=rows))
    assert res["reassessment_state"] == K.STATE_PROPOSAL_READY
    live_rb = {"rebalance_state": "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING",
               "execution_summary": {"submitted_count": 29, "filled_count": 0}}
    out = PRS.load_portfolio_reassessment(portfolio_state=_portfolio_state(),
                                          artifact={"reassessment_id": "prs_x",
                                                    "reassessment": res},
                                          rebalance_state=live_rb)
    exe = out["execution_precedence"]
    assert exe["execution_active"] is True and exe["reassessment_outranked"] is True
    assert out["presentation"]["primary_action"] is None
    # The reassessment is still fully readable as EVIDENCE.
    assert out["decision"]["proposal_required"] is True
    assert out["explanation"]
    # And it still creates nothing.
    assert out["safety"]["created_orders"] is False
    assert out["proposal_boundary"]["creates_orders"] is False
