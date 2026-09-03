"""Slice 6 (Phase 29G) — Holding Opportunity-Cost Engine tests (Milestone 2).

Deterministic and hermetic: the pure kernel is driven by explicit input-contract
dicts; the api composition/persistence/read owner is driven by injected
portfolio-state / scoring / price-panel fixtures and temporary artifact roots; the
endpoint is exercised through a monkeypatched loader. No DB / provider / prediction /
operational ledger / real cycle is touched. Covers the Workstream-O matrix (84
scenarios): input contract, PIT alignment, rank / deterioration, performance, risk
contribution, concentration, liquidity, transaction-cost delegation, improvement,
replacement eligibility, decision policy + boundaries, determinism, immutable
artifacts, DRC integration, Daily Action Gate compatibility, endpoint auth/schema,
UI, and slice boundaries.
"""
from __future__ import annotations

import importlib
import inspect
import json
import types
from datetime import datetime, timezone
from pathlib import Path

from paper_trader.engine import holding_opportunity_cost as k
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import daily_action_gate as dag
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import paper_trading_desk as desk

ROOT = Path(__file__).resolve().parent.parent
UI = ROOT / "api" / "ui" / "index.html"
KERNEL_SRC = (ROOT / "engine" / "holding_opportunity_cost.py").read_text(encoding="utf-8")
OWNER_SRC = (ROOT / "api" / "holding_opportunity_cost.py").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Deterministic builders
# --------------------------------------------------------------------------- #
def _rets(n, seed=1):
    """Deterministic varying daily returns (no RNG) so vol/covariance are non-zero."""
    return [(((i * 7 + seed * 13) % 21) - 10) / 1000.0 for i in range(n)]


def _adj_from_rets(rets, start=100.0):
    adj = [start]
    for r in rets:
        adj.append(adj[-1] * (1.0 + r))
    return adj


def _trailing(seed=1, n=130):
    rets = _rets(n, seed)
    adj = _adj_from_rets(rets)
    ret = [None] + rets
    dates = ["d%03d" % i for i in range(len(adj))]
    return {"dates": dates, "adj": adj, "ret": ret}


def _urow(ticker, rank, pct, sector="Tech", adv=2e7, eligible=True):
    return {"ticker": ticker, "rank": rank, "combined_score": pct, "percentile": pct,
            "fundamental_score": pct, "fundamental_percentile": pct,
            "momentum_score": pct, "momentum_percentile": pct,
            "sector": sector, "adv_dollar": adv, "eligible": eligible}


def _pos(ticker, sector, weight, mv=4000.0, qty=100):
    return {"ticker": ticker, "sector": sector, "quantity": qty,
            "current_weight": weight, "market_value": mv}


def _ic(**over):
    """A complete, valid kernel input contract (2 held names + a strong non-held CCC)."""
    ic = {
        "schema_version": k.INPUT_SCHEMA_VERSION,
        "eligible_market_date": "2026-08-05",
        "active_book_id": "alpha_paper_book_1",
        "active_book_label": "Alpha Paper Book #1",
        "valuation_date": "2026-08-05",
        "portfolio_state_hash": "PSHASH",
        "universe_scoring_hash": "USHASH",
        "universe_input_contract_hash": "USIN",
        "nav": 100000.0, "cash": 8000.0,
        "positions": [_pos("AAA", "Tech", 0.04), _pos("BBB", "Energy", 0.04)],
        "universe_rows": [_urow("AAA", 10, 0.50, "Tech"),
                          _urow("BBB", 12, 0.45, "Energy"),
                          _urow("CCC", 1, 0.99, "Health")],
        "previous_ranking": {"AAA": 9, "BBB": 11, "CCC": 2},
        "previous_ranking_state": "AVAILABLE",
        "trailing_prices": {"AAA": _trailing(1), "BBB": _trailing(2)},
        "median_dollar_volume": {"AAA": 5e7, "BBB": 5e7},
        "aligned_returns": {"dates": ["d%03d" % i for i in range(60)],
                            "series": {"AAA": _rets(60, 1), "BBB": _rets(60, 2)}},
    }
    ic.update(over)
    return ic


def _run(**over):
    return k.build_assessment(input_contract=_ic(**over))


def _review(res, ticker):
    return next(r for r in res["holding_reviews"] if r["ticker"] == ticker)


# =========================================================================== #
# 1–6. Input contract / alignment / identity / dormant rejection.
# =========================================================================== #
def _ps(book_id="alpha_paper_book_1", eligible="2026-08-05", state_hash="PSHASH",
        dormant=False, positions=None):
    return {
        "active_book": {"book_id": book_id, "book_label": "Alpha Paper Book #1",
                        "status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
                        "holdings_count": len(positions or []),
                        "is_dormant_legacy_book": dormant},
        "dates": {"eligible_market_date": eligible, "valuation_date": eligible},
        "capital": {"nav": 100000.0, "cash": 8000.0},
        "state_hash": state_hash,
        "positions": positions if positions is not None else [
            {"ticker": "AAA", "sector": "Tech", "quantity": 100,
             "portfolio_weight": 0.04, "market_value": 4000.0, "price": 40.0,
             "target_weight": 0.04}],
    }


def _scoring(output_hash="USHASH", ranking_date="2026-08-05", rankings=None):
    return {"output_hash": output_hash, "input_contract_hash": "USIN",
            "ranking_date": ranking_date, "exclusions": {},
            "rankings": rankings if rankings is not None else [
                _urow("AAA", 10, 0.5, "Tech"), _urow("CCC", 1, 0.99, "Health")]}


def test_01_input_contract_validation():
    ic = hoc.build_input_contract(portfolio_state=_ps(), scoring=_scoring())
    for key in ("schema_version", "eligible_market_date", "active_book_id",
                "portfolio_state_hash", "universe_scoring_hash", "positions",
                "universe_rows", "previous_ranking_state", "decision_policy_version",
                "cost_policy_version"):
        assert key in ic, key
    assert ic["schema_version"] == k.INPUT_SCHEMA_VERSION


def test_02_eligible_market_date_alignment():
    ic = hoc.build_input_contract(portfolio_state=_ps(eligible="2026-08-05"),
                                  scoring=_scoring(ranking_date="2026-08-05"))
    assert ic["eligible_market_date"] == "2026-08-05"
    assert ic["inputs_as_of_eligible_date"] is True
    # A scoring ranking date that differs from the eligible session is flagged.
    ic2 = hoc.build_input_contract(portfolio_state=_ps(eligible="2026-08-05"),
                                   scoring=_scoring(ranking_date="2026-08-04"))
    assert ic2["inputs_as_of_eligible_date"] is False


def test_03_portfolio_state_hash_alignment():
    ic = hoc.build_input_contract(portfolio_state=_ps(state_hash="ABC"), scoring=_scoring())
    assert ic["portfolio_state_hash"] == "ABC"


def test_04_universe_scoring_hash_alignment():
    ic = hoc.build_input_contract(portfolio_state=_ps(), scoring=_scoring(output_hash="XYZ"))
    assert ic["universe_scoring_hash"] == "XYZ"


def test_05_active_book_identity():
    ic = hoc.build_input_contract(portfolio_state=_ps(book_id="alpha_paper_book_1"),
                                  scoring=_scoring())
    assert ic["active_book_id"] == "alpha_paper_book_1"


def test_06_dormant_legacy_book_rejected():
    # No active book -> NO_ACTIVE_BOOK (the dormant legacy book is never selected here).
    res = k.build_assessment(input_contract=_ic(active_book_id=None))
    assert res["assessment_state"] == k.STATE_NO_ACTIVE_BOOK
    rd = hoc.load_holding_opportunity_cost(portfolio_state=_ps(book_id=None))
    assert rd["state"] == hoc.STATE_NO_ACTIVE_BOOK


# =========================================================================== #
# 7–13. Holding <-> scoring mapping, rank, previous rank, rank change.
# =========================================================================== #
def test_07_holdings_mapped_to_scoring_rows():
    r = _review(_run(), "AAA")
    assert r["current_rank"] == 10


def test_08_missing_holding_score():
    res = _run(universe_rows=[_urow("BBB", 12, 0.45, "Energy"), _urow("CCC", 1, 0.99, "Health")])
    r = _review(res, "AAA")
    assert r["current_rank"] is None
    assert r["recommendation"] == k.REC_EXIT
    assert any("NOT_IN_ELIGIBLE_UNIVERSE" in c for c in r["deterioration_reason_codes"])


def test_09_current_rank():
    assert _review(_run(), "BBB")["current_rank"] == 12


def test_10_previous_rank():
    assert _review(_run(previous_ranking={"AAA": 5, "BBB": 11}), "AAA")["previous_rank"] == 5


def test_11_rank_improvement():
    res = _run(previous_ranking={"AAA": 20, "BBB": 11})
    r = _review(res, "AAA")
    assert r["rank_change"] == 10               # 20 - 10 (improved)
    assert r["deterioration_state"] == k.DET_IMPROVING


def test_12_rank_deterioration():
    res = _run(universe_rows=[_urow("AAA", 20, 0.5), _urow("BBB", 12, 0.45, "Energy"),
                             _urow("CCC", 1, 0.99, "Health")],
              previous_ranking={"AAA": 5, "BBB": 11})
    r = _review(res, "AAA")
    assert r["rank_change"] == -15
    assert r["deterioration_state"] == k.DET_DETERIORATING
    assert "RANK_WORSENED" in r["deterioration_reason_codes"]


def test_13_missing_prior_rank_honest():
    res = _run(previous_ranking=None, previous_ranking_state="UNAVAILABLE")
    r = _review(res, "AAA")
    assert r["previous_rank"] is None
    assert r["rank_change"] is None
    assert "PRIOR_RANK_UNAVAILABLE" in r["deterioration_reason_codes"]
    assert "PRIOR_RANK_UNAVAILABLE" in res["data_quality"]["data_gaps"]


# =========================================================================== #
# 14–20. Trailing returns / volatility / drawdown.
# =========================================================================== #
def test_14_return_5d():
    rets = [0.0] * 55 + [0.10] + [0.0] * 4   # last 5-day window contains a +10% jump
    adj = _adj_from_rets(rets)
    tp = {"dates": ["d%03d" % i for i in range(len(adj))], "adj": adj, "ret": [None] + rets}
    r = _review(_run(trailing_prices={"AAA": tp, "BBB": _trailing(2)}), "AAA")
    assert r["return_5d"] is not None and abs(r["return_5d"] - 0.10) < 1e-6


def test_15_return_20d():
    assert _review(_run(), "AAA")["return_20d"] is not None


def test_16_return_60d():
    assert _review(_run(), "AAA")["return_60d"] is not None


def test_17_minimum_return_observations():
    short = {"dates": ["d0", "d1", "d2"], "adj": [100.0, 101.0, 102.0], "ret": [None, 0.01, 0.0099]}
    r = _review(_run(trailing_prices={"AAA": short, "BBB": _trailing(2)}), "AAA")
    assert r["return_20d"] is None and r["return_60d"] is None


def test_18_volatility_20d():
    assert _review(_run(), "AAA")["volatility_20d"] is not None


def test_19_volatility_60d():
    assert _review(_run(), "AAA")["volatility_60d"] is not None


def test_20_drawdown_60d():
    r = _review(_run(), "AAA")
    assert r["drawdown_60d"] is not None and r["drawdown_60d"] <= 0.0


# =========================================================================== #
# 21–23. Covariance risk contribution.
# =========================================================================== #
def test_21_date_aligned_covariance():
    res = _run()
    contribs = [res["holding_reviews"][0]["risk_contribution_pct"],
                res["holding_reviews"][1]["risk_contribution_pct"]]
    assert all(c is not None for c in contribs)
    assert abs(sum(contribs) - 1.0) < 1e-6      # contributions sum to 100%


def test_22_risk_contribution_value():
    r = _review(_run(), "AAA")
    assert 0.0 <= r["risk_contribution_pct"] <= 1.0


def test_23_insufficient_covariance_observations():
    res = _run(aligned_returns={"dates": ["d0", "d1"], "series": {"AAA": _rets(5, 1),
                                                                  "BBB": _rets(5, 2)}})
    assert res["holding_reviews"][0]["risk_contribution_pct"] is None
    assert "RISK_CONTRIBUTION_UNAVAILABLE" in res["data_quality"]["data_gaps"]


# =========================================================================== #
# 24–28. Concentration / liquidity.
# =========================================================================== #
def test_24_position_concentration():
    res = _run(positions=[_pos("AAA", "Tech", 0.20), _pos("BBB", "Energy", 0.04)])
    r = _review(res, "AAA")
    assert r["recommendation"] == k.REC_REDUCE
    assert "NAME_WEIGHT_BREACH" in r["reason_codes"]


def test_25_sector_concentration():
    res = _run(positions=[_pos("AAA", "Tech", 0.15), _pos("BBB", "Tech", 0.15)])
    r = _review(res, "AAA")
    assert "SECTOR_WEIGHT_BREACH" in r["reason_codes"]
    assert r["recommendation"] == k.REC_REDUCE


def test_26_median_dollar_volume():
    r = _review(_run(median_dollar_volume={"AAA": 5.0e7, "BBB": 5.0e7}), "AAA")
    assert r["median_dollar_volume_20d"] == 5.0e7


def test_27_days_to_liquidate():
    # mv=4000, participation 0.10 * mdv 40000 = 4000/day -> 1 day.
    r = _review(_run(median_dollar_volume={"AAA": 40000.0, "BBB": 5e7}), "AAA")
    assert abs(r["estimated_days_to_liquidate"] - 1.0) < 1e-6
    assert r["liquidity_state"] == k.LIQ_LIQUID


def test_28_missing_volume_honest():
    res = _run(median_dollar_volume={"AAA": None, "BBB": None})
    r = _review(res, "AAA")
    assert r["estimated_days_to_liquidate"] is None
    assert r["liquidity_state"] == k.LIQ_UNAVAILABLE
    assert "LIQUIDITY_UNAVAILABLE" in res["data_quality"]["data_gaps"]


# =========================================================================== #
# 29–35. Transaction cost delegation + improvement.
# =========================================================================== #
def test_29_transaction_cost_owner_delegation():
    pol = hoc.resolve_policy()
    assert pol["cost_rate_per_side"] == desk.COST_RATE_PER_SIDE
    assert pol["cost_bps_per_side"] == desk.COST_BPS_PER_SIDE
    assert pol["max_name_weight"] == eng.MAX_INDIVIDUAL_WEIGHT
    assert pol["sector_cap_fraction"] == eng.SECTOR_CAP_FRACTION
    assert pol["min_adv_dollar"] == eng.MIN_ADV_DOLLAR


def test_30_switching_cost_dollars():
    r = _review(_run(), "AAA")               # mv 4000 * 2*0.00125 = 10.0
    assert r["switching_cost_usd"] == 10.0


def test_31_switching_cost_basis_points():
    assert _review(_run(), "AAA")["switching_cost_bps"] == 25.0


def test_32_gross_score_improvement():
    r = _review(_run(), "AAA")               # CCC 0.99 - AAA 0.50 = 0.49
    assert abs(r["gross_score_improvement"] - 0.49) < 1e-9


def test_33_risk_adjusted_improvement():
    r = _review(_run(), "AAA")
    assert r["risk_adjusted_improvement"] is not None
    assert r["risk_adjusted_improvement"] >= r["gross_score_improvement"] - 1e-9


def test_34_net_improvement():
    r = _review(_run(), "AAA")               # 0.49 (+risk adj) - 0.025 cost hurdle
    assert r["net_improvement"] is not None
    assert r["net_improvement"] < r["risk_adjusted_improvement"]


def test_35_expected_return_delta_unavailable():
    r = _review(_run(), "AAA")
    assert r["expected_return_delta"] is None
    assert r["expected_return_delta_state"] == "UNAVAILABLE"
    assert "SCORE" in r["improvement_basis"] or "PERCENTILE" in r["improvement_basis"]


# =========================================================================== #
# 36–40. Replacement eligibility.
# =========================================================================== #
def test_36_strongest_eligible_replacement():
    r = _review(_run(), "AAA")
    assert r["strongest_replacement_ticker"] == "CCC"
    assert r["replacement_label"] == k.NON_ALLOCATED_LABEL


def test_37_ineligible_candidate_rejected():
    res = _run(universe_rows=[_urow("AAA", 10, 0.5), _urow("BBB", 12, 0.45, "Energy"),
                             _urow("CCC", 1, 0.99, "Health", eligible=False),
                             _urow("DDD", 2, 0.95, "Utilities")])
    r = _review(res, "AAA")
    assert r["strongest_replacement_ticker"] == "DDD"    # CCC ineligible -> DDD
    reasons = {x["reason"] for x in res["diagnostics"]["rejected_candidate_scan"]}
    assert "NOT_ELIGIBLE" in reasons


def test_38_existing_holding_candidate_rejected():
    r = _review(_run(), "AAA")
    assert r["strongest_replacement_ticker"] not in ("AAA", "BBB")


def test_39_concentration_constrained_candidate_rejected():
    # Health sector already at the cap in the book -> a Health candidate is skipped.
    res = _run(positions=[_pos("AAA", "Health", 0.13), _pos("BBB", "Health", 0.13)],
               universe_rows=[_urow("AAA", 10, 0.5, "Health"), _urow("BBB", 12, 0.45, "Health"),
                             _urow("CCC", 1, 0.99, "Health"), _urow("DDD", 3, 0.9, "Utilities")])
    reasons = {x["reason"] for x in res["diagnostics"]["rejected_candidate_scan"]}
    assert "SECTOR_CONCENTRATION_CONSTRAINED" in reasons
    r = _review(res, "AAA")
    assert r["strongest_replacement_ticker"] == "DDD"


def test_40_liquidity_constrained_candidate_rejected():
    res = _run(universe_rows=[_urow("AAA", 10, 0.5), _urow("BBB", 12, 0.45, "Energy"),
                             _urow("CCC", 1, 0.99, "Health", adv=1e5),  # below floor
                             _urow("DDD", 2, 0.95, "Utilities")])
    r = _review(res, "AAA")
    assert r["strongest_replacement_ticker"] == "DDD"
    reasons = {x["reason"] for x in res["diagnostics"]["rejected_candidate_scan"]}
    assert "LIQUIDITY_FILTER_FAILED" in reasons


# =========================================================================== #
# 41–47. Decision policy + boundaries + determinism.
# =========================================================================== #
def _hold_ic(**over):
    # AAA rank 10, no better eligible candidate (CCC weaker), within caps -> HOLD.
    base = dict(universe_rows=[_urow("AAA", 10, 0.60), _urow("BBB", 12, 0.45, "Energy"),
                              _urow("CCC", 1, 0.61, "Health")])
    base.update(over)
    return base


def test_41_hold_policy():
    r = _review(_run(**_hold_ic()), "AAA")
    assert r["recommendation"] == k.REC_HOLD


def test_42_reduce_policy():
    r = _review(_run(positions=[_pos("AAA", "Tech", 0.25), _pos("BBB", "Energy", 0.04)]), "AAA")
    assert r["recommendation"] == k.REC_REDUCE


def test_43_exit_policy():
    # rank beyond the exit buffer (30) -> EXIT (no replacement required).
    res = _run(universe_rows=[_urow("AAA", 45, 0.5), _urow("BBB", 12, 0.45, "Energy"),
                             _urow("CCC", 1, 0.99, "Health")])
    r = _review(res, "AAA")
    assert r["recommendation"] == k.REC_EXIT
    assert "FELL_BELOW_EXIT_BUFFER" in r["reason_codes"]


def test_44_replace_policy():
    r = _review(_run(), "AAA")               # CCC 0.99 clears net threshold, data complete
    assert r["recommendation"] == k.REC_REPLACE
    assert r["strongest_replacement_ticker"] == "CCC"


def test_45_add_candidate_policy():
    res = _run()
    adds = res["addition_candidates"]
    assert any(a["ticker"] == "CCC" and a["recommendation"] == k.REC_ADD for a in adds)
    assert all(a["allocation"] is None for a in adds)   # candidate-only, no weight


def test_46_policy_boundary_values():
    # net_improvement crosses the REPLACE threshold (0.05). net = gross - 0.025 cost
    # hurdle; gross = repl_pct - hold_pct. Above the threshold -> REPLACE, below -> HOLD.
    above = _run(universe_rows=[_urow("AAA", 10, 0.50), _urow("BBB", 12, 0.45, "Energy"),
                               _urow("CCC", 1, 0.58, "Health")])   # gross 0.08 -> net 0.055
    assert _review(above, "AAA")["recommendation"] == k.REC_REPLACE
    below = _run(universe_rows=[_urow("AAA", 10, 0.50), _urow("BBB", 12, 0.45, "Energy"),
                               _urow("CCC", 1, 0.57, "Health")])   # gross 0.07 -> net 0.045
    assert _review(below, "AAA")["recommendation"] == k.REC_HOLD
    # gross below the minimum gross threshold (0.02) never qualifies for REPLACE.
    tiny = _run(universe_rows=[_urow("AAA", 10, 0.50), _urow("BBB", 12, 0.45, "Energy"),
                              _urow("CCC", 1, 0.51, "Health")])    # gross 0.01 < 0.02
    assert _review(tiny, "AAA")["recommendation"] == k.REC_HOLD
    # name-weight cap boundary: exactly 0.10 -> no breach; 0.101 -> breach.
    edge = _run(**_hold_ic(positions=[_pos("AAA", "Tech", 0.10), _pos("BBB", "Energy", 0.04)]))
    assert "NAME_WEIGHT_BREACH" not in _review(edge, "AAA")["reason_codes"]
    over = _run(**_hold_ic(positions=[_pos("AAA", "Tech", 0.101), _pos("BBB", "Energy", 0.04)]))
    assert _review(over, "AAA")["recommendation"] == k.REC_REDUCE


def test_47_deterministic_recommendation():
    a = _run(); b = _run()
    assert [r["recommendation"] for r in a["holding_reviews"]] == \
           [r["recommendation"] for r in b["holding_reviews"]]


# =========================================================================== #
# 48–49. Assessment hash determinism / generated_at exclusion.
# =========================================================================== #
def test_48_deterministic_assessment_hash():
    assert _run()["assessment_hash"] == _run()["assessment_hash"]


def test_49_generated_at_excluded_from_hash():
    res = _run()
    p1 = hoc.persist_assessment(result=res, input_contract=_ic(),
                                now=datetime(2026, 8, 6, 9, tzinfo=timezone.utc),
                                hoc_dir=None) if False else None
    # generated_at lives on the artifact wrapper, never inside the hashed kernel result.
    assert "generated_at" not in json.dumps(k._strip_volatile(res))
    # two kernel runs (different wall clock is irrelevant — the kernel has no clock).
    assert _run()["assessment_hash"] == _run()["assessment_hash"]


# =========================================================================== #
# 50–53. Immutable artifacts.
# =========================================================================== #
def test_50_atomic_artifact_creation(tmp_path):
    out = hoc.run_and_persist(portfolio_state=_ps(), scoring=_scoring(), price_panel=None,
                              hoc_dir=str(tmp_path))
    assert out["persistence"]["status"] == "CREATED"
    aid = out["persistence"]["artifact_id"]
    assert (tmp_path / "artifacts" / ("%s.json" % aid)).exists()
    assert (tmp_path / "index.json").exists()


def test_51_identical_artifact_reuse(tmp_path):
    hoc.run_and_persist(portfolio_state=_ps(), scoring=_scoring(), hoc_dir=str(tmp_path))
    again = hoc.run_and_persist(portfolio_state=_ps(), scoring=_scoring(), hoc_dir=str(tmp_path))
    assert again["persistence"]["status"] == "REUSED_EXISTING"
    assert again["persistence"]["reused"] is True


def test_52_conflicting_artifact_rejection(tmp_path):
    """Release 54.3 replaced the Slice-6 expectation this test used to pin.

    It previously asserted that a second same-session assessment differing ONLY in
    ``portfolio_state_hash`` was CONFLICT_REJECTED. That document-wide hash embeds
    the assessment's own output (via api.daily_action_gate), so it drifts the moment
    an artifact is written — the Stage-21 trap. Refusing on it is what stranded every
    intraday cycle after the first on an unretrievable in-memory assessment.

    The scenario is unchanged and still pinned; only the correct answer moved. Same
    economic portfolio, same evidence, same conclusion -> REUSED_EXISTING. A GENUINE
    conflict — identical evidence producing a DIFFERENT conclusion — is pinned below
    and still fails closed, so the immutability guarantee is not relaxed.
    """
    hoc.run_and_persist(portfolio_state=_ps(state_hash="H1"), scoring=_scoring(), hoc_dir=str(tmp_path))
    again = hoc.run_and_persist(portfolio_state=_ps(state_hash="H2"), scoring=_scoring(),
                                hoc_dir=str(tmp_path))
    assert again["persistence"]["status"] == "REUSED_EXISTING"
    assert again["persistence"]["conflict"] is False
    # ONE artifact on disk: a drifting document hash creates no version.
    assert len(list((tmp_path / "artifacts").glob("hoc_*.json"))) == 1


def test_52b_identical_evidence_different_conclusion_is_still_rejected(tmp_path):
    """The real conflict — a determinism failure — is never persisted."""
    first = hoc.run_and_persist(portfolio_state=_ps(), scoring=_scoring(),
                                hoc_dir=str(tmp_path))
    run = hoc.run_assessment(portfolio_state=_ps(), scoring=_scoring())
    tampered = dict(run["assessment"])
    tampered["recommendation_counts"] = {**(tampered.get("recommendation_counts") or {}),
                                         "EXIT": 99}
    tampered["assessment_hash"] = "DIFFERENT_ANSWER_SAME_EVIDENCE"
    conflict = hoc.persist_assessment(result=tampered,
                                      input_contract=run["input_contract"],
                                      hoc_dir=str(tmp_path))
    assert conflict["status"] == "CONFLICT_REJECTED"
    assert conflict["conflict"] is True
    assert conflict["persisted"] is False
    # The original artifact is untouched and still the indexed one.
    assert hoc.load_latest_artifact(
        active_book_id="alpha_paper_book_1", eligible_market_date="2026-08-05",
        hoc_dir=str(tmp_path))["artifact_id"] == first["persistence"]["artifact_id"]


def test_53_interrupted_write_recovery(tmp_path):
    hoc.run_and_persist(portfolio_state=_ps(), scoring=_scoring(), hoc_dir=str(tmp_path))
    # Simulate a lost index (interrupted before the index write) + a stray temp file.
    (tmp_path / "index.json").unlink()
    (tmp_path / "artifacts" / "orphan.tmp").write_text("partial", encoding="utf-8")
    again = hoc.run_and_persist(portfolio_state=_ps(), scoring=_scoring(), hoc_dir=str(tmp_path))
    assert again["persistence"]["status"] == "CREATED"        # re-indexed / recovered
    rd = hoc.load_holding_opportunity_cost(portfolio_state=_ps(), hoc_dir=str(tmp_path))
    assert rd["state"] in (hoc.STATE_READY, hoc.STATE_DEGRADED)


# =========================================================================== #
# 54–60. No mutation / no provider / prediction / target / order / fill.
# =========================================================================== #
def test_54_no_operational_ledger_mutation():
    assert ".paper_trader" not in OWNER_SRC and ".paper_trader" not in KERNEL_SRC


def test_55_no_postgresql_write():
    for tok in ("sessionmaker", "create_engine", ".commit(", "SessionLocal"):
        assert tok not in OWNER_SRC and tok not in KERNEL_SRC, tok


def test_56_no_provider_call():
    for tok in ("requests.get(", "requests.post(", "urlopen(", "httpx."):
        assert tok not in OWNER_SRC and tok not in KERNEL_SRC, tok


def test_57_no_prediction_call():
    for tok in ("predict(", ":9000"):
        assert tok not in OWNER_SRC and tok not in KERNEL_SRC, tok


def test_58_no_target_confirmation():
    for tok in ("confirm_target(", "confirm_snapshot(", "operationally_approved = True"):
        assert tok not in OWNER_SRC and tok not in KERNEL_SRC, tok


def test_59_no_order_creation():
    for tok in ("place_order(", "submit_order(", "create_order(", "route_order("):
        assert tok not in OWNER_SRC and tok not in KERNEL_SRC, tok


def test_60_no_fill_creation():
    for tok in ("run_fill_cycle(", "settle_due_orders(", "append_fill("):
        assert tok not in OWNER_SRC and tok not in KERNEL_SRC, tok


# =========================================================================== #
# 61–64. Daily Research Cycle integration + core/optional inputs.
# =========================================================================== #
def _fake_hoc_run(**_):
    return {"assessment": _run(), "persistence": {"status": "CREATED",
                                                  "artifact_id": "hoc_test", "persisted": True}}


def test_61_daily_research_cycle_integration(tmp_path):
    s3 = importlib.import_module("tests.test_slice3_daily_research_cycle")
    rec, _ = s3._run(tmp_path, holding_opp_cost_fn=_fake_hoc_run)
    assert "ASSESS_HOLDING_OPPORTUNITY_COST" in rec["completed_steps"]
    idx_hoc = rec["completed_steps"].index("ASSESS_HOLDING_OPPORTUNITY_COST")
    idx_score = rec["completed_steps"].index("SCORE_UNIVERSE")
    idx_asmt = rec["completed_steps"].index("RUN_PORTFOLIO_ASSESSMENT")
    assert idx_score < idx_hoc < idx_asmt
    assert rec["opportunity_cost_owner"] == "api.holding_opportunity_cost"
    assert rec["opportunity_cost_required"] is True
    assert rec["opportunity_cost_selected"] is True
    assert rec["opportunity_cost_recommendation_counts"] is not None


def test_62_drc_idempotent_reuse(tmp_path):
    s3 = importlib.import_module("tests.test_slice3_daily_research_cycle")
    rec1, _ = s3._run(tmp_path, holding_opp_cost_fn=_fake_hoc_run)
    rec2, _ = s3._run(tmp_path, holding_opp_cost_fn=_fake_hoc_run)
    assert rec2["reused_existing_run"] is True
    assert rec2["opportunity_cost_assessment_hash"] == rec1["opportunity_cost_assessment_hash"]


def test_63_core_missing_input_blocks_safely():
    res = k.build_assessment(input_contract={"active_book_id": "b",
                                             "eligible_market_date": "2026-08-05"})
    assert res["assessment_state"] == k.STATE_BLOCKED
    assert any(b["code"] in ("NO_HOLDINGS", "MISSING_UNIVERSE_ROWS",
                             "MISSING_PORTFOLIO_STATE_HASH", "MISSING_UNIVERSE_SCORING_HASH")
               for b in res["blockers"])


def test_64_optional_data_gap_degrades_safely():
    res = _run(previous_ranking=None, previous_ranking_state="UNAVAILABLE",
               median_dollar_volume={"AAA": None, "BBB": None},
               aligned_returns={"dates": [], "series": {}})
    assert res["assessment_state"] == k.STATE_DEGRADED
    assert len(res["holding_reviews"]) == 2       # still fully populated
    assert set(res["data_quality"]["data_gaps"]) >= {"PRIOR_RANK_UNAVAILABLE",
                                                     "LIQUIDITY_UNAVAILABLE"}


# =========================================================================== #
# 65. Daily Action Gate compatibility.
# =========================================================================== #
def test_65_daily_action_gate_compatibility():
    summary = {"opportunity_cost_available": True, "opportunity_cost_assessment_hash": "H",
               "opportunity_cost_state": "READY",
               "opportunity_cost_recommendation_counts": {"HOLD": 3, "REPLACE": 1, "EXIT": 2,
                                                          "REDUCE": 1, "ADD": 4},
               "opportunity_cost_replacement_count": 1, "opportunity_cost_exit_count": 2,
               "opportunity_cost_reduce_count": 1, "opportunity_cost_hold_count": 3,
               "opportunity_cost_add_count": 4, "opportunity_cost_data_gaps": []}
    r = dag.load_daily_action_gate(today="2026-08-05", current={"status": "MHZ_INPUTS_UNAVAILABLE"},
                                   operational={}, opportunity_cost=summary)
    assert r["opportunity_cost_available"] is True
    assert r["opportunity_cost_exit_count"] == 2
    assert r["opportunity_cost_add_count"] == 4
    assert r["proposal_label"] == dag.PROPOSAL_REVIEW_LABEL
    # Slice 7 (Phase 29H) LANDED: the reallocation engine is implemented, review-only.
    assert "NOT YET IMPLEMENTED" not in r["proposal_label"]
    assert r["reallocation_engine_implemented"] is True
    assert r["proposal_review_only"] is True


# =========================================================================== #
# 66–69. Endpoint auth / GET-only / schema / degraded.
# =========================================================================== #
def _client():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    return TestClient(app, raise_server_exceptions=False)


def _key():
    from paper_trader.config import get_settings
    return get_settings().service_api_key


def test_66_endpoint_requires_auth():
    assert _client().get("/v1/operations/holding-opportunity-cost").status_code in (401, 403)


def test_67_endpoint_get_only():
    c = _client()
    assert c.post("/v1/operations/holding-opportunity-cost",
                  headers={"X-API-Key": _key()}).status_code == 405


def _canned_read(state=hoc.STATE_READY):
    return {"schema_version": hoc.SCHEMA_VERSION, "generated_at": "2026-08-06T00:00:00+00:00",
            "state": state, "eligible_market_date": "2026-08-05",
            "active_book": {"book_id": "alpha_paper_book_1"}, "input_contract": {},
            "policy": k.default_policy(), "portfolio_summary": {},
            "recommendation_counts": {kk: 0 for kk in k.RECOMMENDATION_VOCAB},
            "holding_reviews": [], "addition_candidates": [], "diagnostics": {},
            "data_quality": {}, "artifact": None, "safety": k._safety(),
            "provenance": {}, "assessment_hash": "H"}


def test_68_endpoint_response_schema(monkeypatch):
    monkeypatch.setattr("paper_trader.api.app._hoc.load_holding_opportunity_cost",
                        lambda: _canned_read())
    resp = _client().get("/v1/operations/holding-opportunity-cost", headers={"X-API-Key": _key()})
    assert resp.status_code == 200
    body = resp.json()
    for key in ("schema_version", "generated_at", "state", "eligible_market_date",
                "active_book", "input_contract", "policy", "portfolio_summary",
                "recommendation_counts", "holding_reviews", "addition_candidates",
                "diagnostics", "data_quality", "artifact", "safety", "provenance",
                "assessment_hash"):
        assert key in body, key


def test_69_endpoint_degraded_response(monkeypatch):
    monkeypatch.setattr("paper_trader.api.app._hoc.load_holding_opportunity_cost",
                        lambda: _canned_read(state=hoc.STATE_NOT_RUN))
    resp = _client().get("/v1/operations/holding-opportunity-cost", headers={"X-API-Key": _key()})
    assert resp.status_code == 200
    assert resp.json()["state"] == hoc.STATE_NOT_RUN


# =========================================================================== #
# 70–76. UI.
# =========================================================================== #
def _ui():
    return UI.read_text(encoding="utf-8")


def test_70_one_ui_loader():
    assert _ui().count("function loadHoldingOpportunityCost") == 1


def test_71_no_js_recommendation_calculation():
    ui = _ui()
    start = ui.find("function loadHoldingOpportunityCost")
    end = ui.find("window.renderHoldingOpportunityCost")
    assert start != -1 and end != -1 and end > start
    region = ui[start:end]
    for tok in ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.", "compute"):
        assert tok not in region, tok


def test_72_no_js_cost_calculation():
    ui = _ui()
    start = ui.find("function loadHoldingOpportunityCost")
    end = ui.find("window.renderHoldingOpportunityCost")
    region = ui[start:end]
    for tok in ("cost_rate", "COST_BPS", "* 0.00125", "COST_RATE"):
        assert tok not in region, tok


def test_73_ui_summary():
    assert 'id="hoc-summary"' in _ui()


def test_74_ui_holding_table():
    assert 'id="hoc-table"' in _ui() and 'id="hoc-filters"' in _ui()


def test_75_ui_addition_candidate_section():
    assert 'id="hoc-additions"' in _ui()


def test_76_preliminary_proposal_replaced():
    ui = _ui()
    assert "HOLDING OPPORTUNITY-COST REVIEW" in ui
    assert "PRELIMINARY PROPOSAL — OPPORTUNITY-COST ENGINE NOT YET IMPLEMENTED" not in ui


# =========================================================================== #
# 77–79. Existing surface compatibility.
# =========================================================================== #
def _route_paths():
    from paper_trader.api.app import app
    return {r.path for r in app.routes if hasattr(r, "path")}


def test_77_existing_portfolio_state_compatibility():
    from paper_trader.api import portfolio_state as ps
    # Slice 7 (Phase 29H) LANDED: the banner is review-only manual-review, not "not implemented".
    assert "NOT YET IMPLEMENTED" not in ps.PRELIMINARY_PROPOSAL_LABEL
    assert "REVIEW ONLY" in ps.PRELIMINARY_PROPOSAL_LABEL
    assert "/v1/operations/portfolio-state" in _route_paths()


def test_78_existing_universe_scoring_compatibility():
    assert "/v1/research/universe-scoring" in _route_paths()


def test_79_existing_daily_close_compatibility():
    assert "/v1/operations/daily-close" in _route_paths()


# =========================================================================== #
# 80–84. Architecture audit / inventory / slice boundaries.
# =========================================================================== #
def _audit():
    return importlib.import_module("scripts.audit_architecture")


def test_80_architecture_audit():
    rep = _audit().run_audit()
    ho = rep["holding_opportunity_cost_ownership"]
    assert ho["kernel_present"] and ho["owner_present"]
    assert ho["second_calculation_owner_modules"] == []
    assert ho["second_composition_owner_modules"] == []
    assert ho["missing_delegation"] == []
    assert ho["owner_forbidden_calls"] == []
    assert ho["kernel_impurity"] == []
    assert ho["route_methods"] == ["GET"]
    assert ho["no_separate_manual_execution_endpoint"] is True
    assert ho["drc_delegates_to_owner"] is True
    assert ho["gate_delegates_to_summary"] is True
    assert ho["ui_loader_count"] == 1
    assert ho["ui_recommendation_or_cost_computation"] == []
    # Phase 29G performance repair — the audit proves the ACYCLIC read dependency.
    assert ho["summary_loads_portfolio_state"] == []
    assert ho["gate_supplies_hoc_context"] is True
    assert ho["portfolio_state_composes_gate"] is True
    assert ho["no_circular_read_dependency"] is True


def test_81_inventory_drift_zero():
    d = _audit().run_audit()["inventory_drift"]
    assert d["status"] == "OK"
    assert d["on_disk_not_in_inventory"] == []
    assert d["in_inventory_not_on_disk"] == []


def test_82_slice7_landed():
    # Slice 7 (Phase 29H) is LANDED: the exact owners + GET route exist; the
    # alternative naming and any apply/rebalance route stay absent.
    root = ROOT
    for present in ("engine/reallocation_proposal.py", "api/reallocation_proposal.py"):
        assert (root / present).exists(), present
    assert "/v1/operations/reallocation-proposal" in _route_paths()
    for absent in ("/v1/operations/portfolio-proposal",
                   "/v1/operations/reallocation-proposal/apply"):
        assert absent not in _route_paths(), absent
    # Stage-19 controlled-rebalance route IS present (governed by rebalance_execution.py);
    # it is a controlled APPROVED-decision + second-confirmation bridge, not an auto route.
    assert "/v1/operations/rebalance" in _route_paths()
    assert "/v1/operations/rebalance/confirm-order-plan" in _route_paths()


def test_83_research_agent_landed_no_second_registry():
    # Slice 8 (Persistent Alpha Research Agent, Milestone 4) has LANDED as its two canonical
    # owners; a second/unified model registry (api/model_registry.py) is NOT created — the
    # Research Agent reads the existing champion/challenger registries, it never forks them.
    assert not (ROOT / "api" / "model_registry.py").exists()
    assert (ROOT / "engine" / "research_agent.py").exists()
    assert (ROOT / "api" / "research_agent.py").exists()


def test_84_cadence_remains_disabled():
    ho = _audit().run_audit()["holding_opportunity_cost_ownership"]
    assert ho["cadence_enabled"] is False
    assert ho["automatic_model_promotion_allowed"] is False


# =========================================================================== #
# 85–95. Phase 29G performance repair — the Holding Opportunity-Cost summary is a
# PURE artifact reader and the canonical read graph is a DAG (no
# portfolio_state → daily_action_gate → summary → portfolio_state recomposition).
# Deterministic and fully offline: every leaf loader is stubbed; the real cycle
# participants execute, so a genuine cycle would recurse.
# =========================================================================== #
_STUB_OPS = {
    "operational_book": {"book_id": "alpha_paper_book_1", "initialized": True,
                         "book_label": "Alpha Paper Book #1",
                         "holdings_detail": [], "holdings": {}},
    "canonical_state": {"valuation_date": "2026-08-05", "holdings_detail": [],
                        "holdings_count": 0, "pending_order_count": 0, "fill_count": 0},
}


def _install_offline_cycle_seams(monkeypatch):
    """Stub only the LEAF loaders so the gate + portfolio_state run fully offline
    (no provider / Daily Close / real store) while the REAL cycle participants
    execute — a genuine cycle would recurse."""
    from paper_trader.api import portfolio_state as ps
    monkeypatch.setattr(dag, "_OPERATIONAL_BOOK_LOADER", lambda today=None: dict(_STUB_OPS))
    monkeypatch.setattr(dag, "_ENGINE_CURRENT_LOADER",
                        lambda: {"status": "MHZ_INPUTS_UNAVAILABLE"})
    monkeypatch.setattr(dag, "_FRESHNESS_LOADER",
                        lambda operational=None: {"eligible_market_date": "2026-08-05"})
    monkeypatch.setattr(ps, "_import_operational", lambda: types.SimpleNamespace(
        load_operational_book=lambda **k: dict(_STUB_OPS)))
    monkeypatch.setattr(ps, "_import_freshness", lambda: types.SimpleNamespace(
        load_data_freshness=lambda **k: {"eligible_market_date": "2026-08-05",
                                         "active_book": {"active_book_id": "alpha_paper_book_1"}}))
    monkeypatch.setattr(ps, "_import_desk", lambda: types.SimpleNamespace(
        load_performance=lambda: {"rows": [], "summary": {}}, load_fills=lambda: {"fills": []}))
    monkeypatch.setattr(ps, "_import_fps", lambda: types.SimpleNamespace(
        load_prediction_skill=lambda: {}))
    return ps


def _count_cycle(monkeypatch):
    from paper_trader.api import portfolio_state as ps
    counts: dict[str, int] = {}

    def wrap(mod, name):
        orig = getattr(mod, name)

        def w(*a, **k):
            counts[name] = counts.get(name, 0) + 1
            return orig(*a, **k)

        monkeypatch.setattr(mod, name, w)

    wrap(ps, "load_portfolio_state")
    wrap(dag, "load_daily_action_gate")
    wrap(hoc, "load_assessment_summary")
    wrap(hoc, "_default_portfolio_state_loader")
    return counts


def test_85_summary_context_reads_persisted_artifact(tmp_path):
    # items 1, 6, 8, 9: explicit (book, date) context reads ONLY the immutable artifact.
    out = hoc.run_and_persist(portfolio_state=_ps(), scoring=_scoring(),
                              price_panel=None, hoc_dir=str(tmp_path))
    assert out["persistence"]["persisted"] is True
    summary = hoc.load_assessment_summary(active_book_id="alpha_paper_book_1",
                                          eligible_market_date="2026-08-05",
                                          hoc_dir=str(tmp_path))
    assert summary["opportunity_cost_available"] is True
    assert summary["opportunity_cost_assessment_hash"] == \
        out["assessment"]["assessment_hash"]
    assert summary["opportunity_cost_state"] in (k.STATE_READY, k.STATE_DEGRADED)


def test_86_summary_not_run_when_no_artifact(tmp_path):
    # item 7: NOT_RUN (available=False + zeroed counts) when no artifact exists.
    summary = hoc.load_assessment_summary(active_book_id="alpha_paper_book_1",
                                          eligible_market_date="2099-01-01",
                                          hoc_dir=str(tmp_path))
    assert summary["opportunity_cost_available"] is False
    assert summary["opportunity_cost_state"] == hoc.STATE_NOT_RUN
    assert summary["opportunity_cost_recommendation_counts"] == \
        {kk: 0 for kk in k.RECOMMENDATION_VOCAB}


def test_87_summary_never_loads_portfolio_state(monkeypatch, tmp_path):
    # item 2: the summary must NOT load portfolio state on ANY path (forbidden edge),
    # and its signature no longer even accepts a portfolio_state seam.
    def _boom(*a, **k):
        raise AssertionError("load_assessment_summary must not load portfolio_state")

    monkeypatch.setattr(hoc, "_default_portfolio_state_loader", _boom)
    s = hoc.load_assessment_summary(active_book_id="alpha_paper_book_1",
                                    eligible_market_date="2026-08-05", hoc_dir=str(tmp_path))
    assert s["opportunity_cost_available"] is False  # no artifact, but no ps load / no crash
    params = set(inspect.signature(hoc.load_assessment_summary).parameters)
    assert "portfolio_state" not in params and "portfolio_state_loader" not in params
    assert {"active_book_id", "eligible_market_date"} <= params


def test_88_summary_bounded_single_artifact_read(monkeypatch, tmp_path):
    # item 6: exactly one bounded artifact-index/artifact read per summary call.
    calls = {"n": 0}
    orig = hoc.load_latest_artifact

    def _counting(**kw):
        calls["n"] += 1
        return orig(**kw)

    monkeypatch.setattr(hoc, "load_latest_artifact", _counting)
    hoc.load_assessment_summary(active_book_id="b", eligible_market_date="d",
                                hoc_dir=str(tmp_path))
    assert calls["n"] == 1


def test_89_gate_supplies_context_no_cycle(monkeypatch, tmp_path):
    # items 3, 5, 15: the gate supplies (book, date) context; one summary read; NO
    # recursion; and the opportunity_cost_* compatibility fields are populated.
    monkeypatch.setenv(hoc.HOC_DIR_ENV, str(tmp_path))  # empty HOC dir → NOT_RUN
    _install_offline_cycle_seams(monkeypatch)
    counts = _count_cycle(monkeypatch)
    r = dag.load_daily_action_gate()
    assert counts.get("load_daily_action_gate", 0) == 1
    assert counts.get("load_assessment_summary", 0) == 1
    assert counts.get("load_portfolio_state", 0) == 0
    assert counts.get("_default_portfolio_state_loader", 0) == 0
    for key in ("opportunity_cost_available", "opportunity_cost_state",
                "opportunity_cost_recommendation_counts", "opportunity_cost_exit_count",
                "opportunity_cost_replacement_count", "opportunity_cost_add_count",
                "opportunity_cost_data_gaps"):
        assert key in r
    assert r["opportunity_cost_available"] is False


def test_90_portfolio_state_composes_gate_no_recursion(monkeypatch, tmp_path):
    # items 4, 5: portfolio_state STILL composes the daily action gate, and the whole
    # portfolio_state → gate → summary graph is a DAG (each participant runs once).
    monkeypatch.setenv(hoc.HOC_DIR_ENV, str(tmp_path))
    ps = _install_offline_cycle_seams(monkeypatch)
    counts = _count_cycle(monkeypatch)
    state = ps.load_portfolio_state()
    assert counts.get("load_portfolio_state", 0) == 1
    assert counts.get("load_daily_action_gate", 0) == 1
    assert counts.get("load_assessment_summary", 0) == 1
    assert counts.get("_default_portfolio_state_loader", 0) == 0
    assert state["state"] in ps.PORTFOLIO_STATES


def test_91_dormant_legacy_book_yields_no_summary(tmp_path):
    # item 10: the dormant legacy book is never surfaced — no artifact is keyed on it,
    # so its summary is honestly NOT_RUN (the DRC persists only for the ACTIVE book).
    from paper_trader.api import portfolio_state as ps
    summary = hoc.load_assessment_summary(active_book_id=ps.LEGACY_BOOK_ID,
                                          eligible_market_date="2026-08-05",
                                          hoc_dir=str(tmp_path))
    assert summary["opportunity_cost_available"] is False
    assert summary["opportunity_cost_state"] == hoc.STATE_NOT_RUN


def test_92_calculation_behavior_unchanged():
    # item 13: the pure kernel is untouched — deterministic assessment_hash + the
    # recommendation counts still cover every held name.
    a, b = _run(), _run()
    assert a["assessment_hash"] == b["assessment_hash"]
    assert sum(a["recommendation_counts"][r] for r in ("HOLD", "REDUCE", "EXIT", "REPLACE")) \
        == len(a["holding_reviews"])


def test_93_build_path_still_sources_portfolio_state(monkeypatch, tmp_path):
    # items 13, 14: the BUILD/persist path (DRC) legitimately sources portfolio_state
    # (unchanged) — only the read-summary path dropped that edge.
    used = {"n": 0}

    def _fake_ps():
        used["n"] += 1
        return _ps()

    monkeypatch.setattr(hoc, "_default_portfolio_state_loader", _fake_ps)
    out = hoc.run_and_persist(scoring=_scoring(), price_panel=None, hoc_dir=str(tmp_path))
    assert used["n"] >= 1
    assert out["assessment"]["assessment_hash"]
    assert out["persistence"]["persisted"] is True


def test_94_summary_and_gate_create_no_artifact(monkeypatch, tmp_path):
    # item 16: neither the read summary nor the gate delegation persists an artifact,
    # creates a target / order / fill, or otherwise mutates state.
    monkeypatch.setenv(hoc.HOC_DIR_ENV, str(tmp_path))
    _install_offline_cycle_seams(monkeypatch)
    hoc.load_assessment_summary(active_book_id="alpha_paper_book_1",
                                eligible_market_date="2026-08-05", hoc_dir=str(tmp_path))
    r = dag.load_daily_action_gate()
    assert not (tmp_path / "index.json").exists()
    assert not (tmp_path / "artifacts").exists()
    assert r["performed_write"] is False
    assert r["reallocation_engine_implemented"] is True
    assert r["proposal_review_only"] is True


def test_95_audit_enforces_acyclic_dependency():
    # item 17: the strict architecture audit proves the acyclic read dependency.
    ho = _audit().run_audit()["holding_opportunity_cost_ownership"]
    assert ho["summary_loads_portfolio_state"] == []
    assert ho["gate_supplies_hoc_context"] is True
    assert ho["portfolio_state_composes_gate"] is True
    assert ho["no_circular_read_dependency"] is True
    assert ho["gate_delegates_to_summary"] is True
