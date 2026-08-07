"""Slice 8 (Phase 29I) — Persistent Alpha Research Agent tests (Milestone 4).

Deterministic and hermetic: the pure kernel is driven by explicit research-evidence
input-contract dicts; the api composition/persistence/read owner is driven by injected
scoring / prediction / performance / tournament / HOC-history / reallocation-history
fixtures and temporary artifact roots; the endpoint is exercised through a monkeypatched
loader; the DRC integration reuses the Slice-3 harness with an injected engine seam. No
DB / provider / prediction / operational ledger / real cycle / real research-agent
artifact is touched.

Covers the required matrix:
  A. evidence contract (1-12)     B. champion health (13-23)
  C. HOC/reallocation feedback (24-29)   D. challenger (30-36)
  E. research opportunities (37-44)      F. recalibration (45-50)
  G. persistence / api (51-64)           H. DRC (65-72)
  I. UI (73-81)                          J. architecture (82-89)   + bounded performance.
"""
from __future__ import annotations

import copy
import importlib
import json
from pathlib import Path

from paper_trader.engine import research_agent as K
from paper_trader.api import research_agent as A

ROOT = Path(__file__).resolve().parent.parent
UI = ROOT / "api" / "ui" / "index.html"
KERNEL_SRC = (ROOT / "engine" / "research_agent.py").read_text(encoding="utf-8")
OWNER_SRC = (ROOT / "api" / "research_agent.py").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Deterministic kernel input-contract builder (healthy baseline; no RNG)
# --------------------------------------------------------------------------- #
def _ic(**over):
    ic = {
        "schema_version": K.INPUT_SCHEMA_VERSION,
        "eligible_market_date": "2026-08-05",
        "active_book_id": "alpha_paper_book_1",
        "active_book_label": "Alpha Paper Book #1",
        "champion": {"model_id": "composite_sn",
                     "primary_model_id": "fundamental_momentum_50_50_v1",
                     "strategy_version": "v1", "model_registry_version": "p20",
                     "automatic_promotion_allowed": False},
        "champion_identity_hash": "CIH", "universe_scoring_hash": "USH",
        "portfolio_state_hash": "PSH", "forward_evidence_hash": "2026-08-04",
        "evidence": {"forward_observation_count": 30, "eligible_session_count": 40,
                     "ranking_observation_count": 500, "universe_coverage_ratio": 0.9,
                     "reallocation_observation_count": 5, "hoc_observation_count": 5,
                     "regime_observation_count": 30, "distinct_regime_count": 3,
                     "evidence_start": "2026-06-01", "evidence_end": "2026-08-05"},
        "ranking_quality": {"rank_ic_mean": 0.05, "rank_ic_positive_rate_pct": 60.0,
                            "rank_ic_obs": 30, "rank_ic_recent_mean": 0.05,
                            "rank_ic_prior_mean": 0.05, "decile_spread_pp": 1.2,
                            "decile_spread_obs": 30,
                            "rank_ic_by_horizon": {"1": 0.05, "5": 0.05},
                            "evidence_state": "EV_HORIZON_ALIGNED"},
        "performance": {"cumulative_return_pct": 3.0, "benchmark_ticker": "SPY",
                        "benchmark_cumulative_return_pct": 2.0, "excess_pct_points": 1.0,
                        "max_drawdown_pct": -5.0, "current_drawdown_pct": -3.0,
                        "avg_daily_turnover": 0.2, "total_transaction_cost": 50.0,
                        "eligible_session_count": 40, "rolling": []},
        "sector": {"max_sector_weight": 0.25, "max_sector": "Tech",
                   "sector_weights": {"Tech": 0.25, "Fin": 0.2},
                   "sector_concentration_series": [0.24, 0.25, 0.25]},
        "regime": {"available": True, "current_label": "NEUTRAL", "distinct_count": 3,
                   "drift_flag": False, "observation_count": 30},
        "hoc_history": [{"date": "2026-08-04", "state": "READY",
                         "recommendation_counts": {"HOLD": 20, "REDUCE": 2, "EXIT": 1,
                                                   "REPLACE": 1, "ADD": 1}}],
        "reallocation_history": [{"date": "2026-08-04", "state": "READY",
                                  "action_counts": {"RETAIN": 20},
                                  "one_way_turnover": 0.1,
                                  "score_improvement_net_of_cost": 0.02}],
        "challenger": {"available": True, "signal": "composite_sn_repaired",
                       "book_comparisons": [{"book": "top25", "excess_pp": 1.0,
                                             "observation_count": 60}],
                       "decision": "HOLD"},
        # A recent checkpoint so the HEALTHY baseline is not "checkpoint due".
        "last_checkpoint_forward_observation_count": 25,
        "source_gaps": [],
    }
    ic.update(over)
    return ic


def _run(**over):
    policy = over.pop("policy", None)
    return K.evaluate(input_contract=_ic(**over), policy=policy)


def _comp(res, name):
    return next((c for c in res["champion_health"]["components"] if c["component"] == name), None)


# =========================================================================== #
# A. EVIDENCE CONTRACT (1-12)
# =========================================================================== #
def test_01_pit_evidence_window_reflected():
    r = _run()
    w = r["evidence_window"]
    assert w["evidence_start"] == "2026-06-01" and w["evidence_end"] == "2026-08-05"
    assert w["forward_observation_count"] == 30


def test_02_evidence_windows_carry_counts():
    r = _run()
    w = r["evidence_window"]
    for k in ("forward_observation_count", "eligible_session_count",
              "ranking_observation_count", "reallocation_observation_count",
              "hoc_observation_count", "regime_observation_count"):
        assert k in w


def test_03_observation_counts_are_integers():
    r = _run()
    w = r["evidence_window"]
    assert isinstance(w["forward_observation_count"], int)
    assert isinstance(w["ranking_observation_count"], int)


def test_04_missing_true_forward_marks_gap():
    r = _run(ranking_quality={}, evidence={"forward_observation_count": 0,
             "eligible_session_count": 40, "ranking_observation_count": 500,
             "universe_coverage_ratio": 0.9})
    codes = [g["code"] for g in r["data_gaps"]]
    assert "FORWARD_RANK_IC_UNAVAILABLE" in codes
    assert r["evidence_quality"]["insufficient_forward_evidence"] is True


def test_05_insufficient_forward_observations_not_sufficient():
    r = _run(evidence={"forward_observation_count": 3, "eligible_session_count": 40,
             "ranking_observation_count": 500, "universe_coverage_ratio": 0.9})
    assert r["evidence_quality"]["state"] != K.EV_SUFFICIENT


def test_06_enough_forward_observations_sufficient():
    r = _run()
    assert r["evidence_quality"]["state"] == K.EV_SUFFICIENT
    assert r["evidence_quality"]["forward_observation_state"] == K.EV_SUFFICIENT


def test_07_missing_rank_ic_component_unknown():
    r = _run(ranking_quality={"decile_spread_pp": 1.0, "decile_spread_obs": 30})
    assert _comp(r, "forward_rank_ic")["state"] == K.H_UNKNOWN


def test_08_missing_decile_spread_component_unknown():
    r = _run(ranking_quality={"rank_ic_mean": 0.05, "rank_ic_positive_rate_pct": 60.0,
             "rank_ic_obs": 30})
    assert _comp(r, "decile_spread")["state"] == K.H_UNKNOWN


def test_09_missing_regime_data_marks_gap():
    r = _run(regime={"available": False})
    assert _comp(r, "regime_stability")["state"] == K.H_UNKNOWN
    assert "REGIME_CLASSIFICATION_UNAVAILABLE" in [g["code"] for g in r["data_gaps"]]


def test_10_missing_hoc_history_marks_gap():
    r = _run(hoc_history=[])
    assert _comp(r, "hoc_deterioration")["state"] == K.H_UNKNOWN
    assert "HOC_HISTORY_UNAVAILABLE" in [g["code"] for g in r["data_gaps"]]


def test_11_missing_reallocation_history_marks_gap():
    r = _run(reallocation_history=[])
    assert _comp(r, "reallocation_churn")["state"] == K.H_UNKNOWN
    assert "REALLOCATION_HISTORY_UNAVAILABLE" in [g["code"] for g in r["data_gaps"]]


def test_12_no_hindsight_reconstruction_only_supplied_evidence():
    # The kernel evaluates ONLY the supplied contract; it never fabricates observations.
    r = _run(evidence={"forward_observation_count": 0, "eligible_session_count": 0,
             "ranking_observation_count": 0, "universe_coverage_ratio": 0.0},
             ranking_quality={}, performance={})
    assert r["evidence_window"]["forward_observation_count"] == 0
    assert r["ranking_quality"]["rank_ic_mean"] is None
    assert r["evidence_quality"]["state"] == K.EV_INSUFFICIENT


# =========================================================================== #
# B. CHAMPION HEALTH (13-23)
# =========================================================================== #
def test_13_healthy_evidence_healthy_state():
    r = _run()
    assert r["research_state"] == K.STATE_HEALTHY
    assert r["champion_health"]["weak_components"] == []


def test_14_weak_excess_only_with_sufficient_evidence_investigate():
    # Weak realized excess but rank quality intact + sufficient evidence -> INVESTIGATE,
    # NOT recalibration (performance weakness is a symptom, not signal degradation).
    r = _run(performance={"excess_pct_points": -8.0, "max_drawdown_pct": -5.0,
             "avg_daily_turnover": 0.2, "benchmark_ticker": "SPY",
             "eligible_session_count": 40})
    assert _comp(r, "benchmark_relative")["state"] == K.H_WEAK
    assert r["research_state"] == K.STATE_INVESTIGATE
    assert r["recalibration"]["state"] != K.RECAL_RECOMMENDED


def test_15_rank_ic_degradation_recalibration_due():
    r = _run(ranking_quality={"rank_ic_mean": -0.05, "rank_ic_positive_rate_pct": 30.0,
             "rank_ic_obs": 40, "rank_ic_recent_mean": -0.05, "rank_ic_prior_mean": 0.03,
             "decile_spread_pp": -1.0, "decile_spread_obs": 40})
    assert _comp(r, "forward_rank_ic")["state"] == K.H_WEAK
    assert K.DEG_RANKING in r["degradation"]["categories"]
    assert r["research_state"] == K.STATE_RECALIBRATION_DUE


def test_16_decile_degradation_flagged():
    r = _run(ranking_quality={"rank_ic_mean": 0.01, "rank_ic_positive_rate_pct": 50.0,
             "rank_ic_obs": 30, "decile_spread_pp": -2.0, "decile_spread_obs": 30})
    assert _comp(r, "decile_spread")["state"] == K.H_WEAK


def test_17_drawdown_deterioration_flagged():
    r = _run(performance={"excess_pct_points": 0.5, "max_drawdown_pct": -20.0,
             "avg_daily_turnover": 0.2, "benchmark_ticker": "SPY",
             "eligible_session_count": 40})
    assert _comp(r, "drawdown")["state"] == K.H_WEAK


def test_18_turnover_inefficiency_flagged():
    r = _run(performance={"excess_pct_points": 0.5, "max_drawdown_pct": -5.0,
             "avg_daily_turnover": 0.8, "benchmark_ticker": "SPY",
             "eligible_session_count": 40})
    assert _comp(r, "turnover_efficiency")["state"] == K.H_WEAK
    assert K.DEG_TURNOVER in r["degradation"]["categories"]


def test_19_sector_instability_flagged():
    r = _run(sector={"max_sector_weight": 0.5, "max_sector": "Tech",
             "sector_concentration_series": [0.2, 0.5, 0.2, 0.5]})
    assert _comp(r, "sector_stability")["state"] == K.H_WEAK
    assert K.DEG_SECTOR in r["degradation"]["categories"]


def test_20_regime_drift_flagged():
    r = _run(regime={"available": True, "current_label": "RISK_OFF", "distinct_count": 3,
             "drift_flag": True, "observation_count": 30})
    assert _comp(r, "regime_stability")["state"] == K.H_WATCH
    assert K.DEG_REGIME in r["degradation"]["categories"]


def test_21_data_quality_degradation_multiple_unknown():
    r = _run(ranking_quality={}, performance={}, regime={"available": False},
             hoc_history=[], reallocation_history=[], sector={},
             evidence={"forward_observation_count": 25, "eligible_session_count": 40,
                       "ranking_observation_count": 500, "universe_coverage_ratio": 0.9})
    assert K.DEG_DATA_QUALITY in r["degradation"]["categories"]


def test_22_multiple_degradation_categories():
    r = _run(ranking_quality={"rank_ic_mean": -0.05, "rank_ic_positive_rate_pct": 30.0,
             "rank_ic_obs": 40, "rank_ic_recent_mean": -0.05, "rank_ic_prior_mean": 0.03,
             "decile_spread_pp": -1.0, "decile_spread_obs": 40},
             performance={"excess_pct_points": -8.0, "max_drawdown_pct": -20.0,
             "avg_daily_turnover": 0.8, "benchmark_ticker": "SPY",
             "eligible_session_count": 40})
    cats = r["degradation"]["categories"]
    assert K.DEG_RANKING in cats and K.DEG_PERFORMANCE in cats and K.DEG_TURNOVER in cats


def test_23_insufficient_evidence_prevents_premature_recalibration():
    # Rank IC looks weak but only a handful of observations -> capped, NOT recalibration.
    r = _run(evidence={"forward_observation_count": 3, "eligible_session_count": 5,
             "ranking_observation_count": 500, "universe_coverage_ratio": 0.9},
             ranking_quality={"rank_ic_mean": -0.10, "rank_ic_positive_rate_pct": 20.0,
             "rank_ic_obs": 3, "decile_spread_pp": -2.0, "decile_spread_obs": 3})
    assert r["recalibration"]["state"] != K.RECAL_RECOMMENDED
    assert r["research_state"] in (K.STATE_WATCH, K.STATE_INSUFFICIENT_EVIDENCE)


# =========================================================================== #
# C. HOC / REALLOCATION FEEDBACK (24-29)
# =========================================================================== #
def test_24_repeated_hoc_exit_replace_flagged():
    hist = [{"date": "2026-08-0%d" % i, "state": "READY",
             "recommendation_counts": {"HOLD": 5, "EXIT": 10, "REPLACE": 5}} for i in range(1, 5)]
    r = _run(hoc_history=hist)
    assert _comp(r, "hoc_deterioration")["state"] == K.H_WEAK
    assert "STALE" in r["hoc_diagnostics"]["interpretation"] or \
           r["hoc_diagnostics"]["exit_replace_rate"] > 0.5


def test_25_stable_hoc_history():
    r = _run()
    assert r["hoc_diagnostics"]["stable"] is True
    assert r["hoc_diagnostics"]["interpretation"] == "STABLE_HOC_HISTORY"


def test_26_high_reallocation_churn_flagged():
    hist = [{"date": "2026-08-0%d" % i, "state": "READY", "action_counts": {},
             "one_way_turnover": 0.9, "score_improvement_net_of_cost": 0.01}
            for i in range(1, 5)]
    r = _run(reallocation_history=hist)
    assert _comp(r, "reallocation_churn")["state"] == K.H_WEAK
    assert r["reallocation_diagnostics"]["high_churn"] is True


def test_27_strong_proposal_improvement_interpreted_as_lag_not_failure():
    hist = [{"date": "2026-08-0%d" % i, "state": "READY", "action_counts": {},
             "one_way_turnover": 0.1, "score_improvement_net_of_cost": 0.10}
            for i in range(1, 5)]
    r = _run(reallocation_history=hist)
    assert "PORTFOLIO_LAG" in r["reallocation_diagnostics"]["interpretation"]


def test_28_weak_proposal_improvement_interpreted_as_limited_alpha():
    hist = [{"date": "2026-08-0%d" % i, "state": "READY", "action_counts": {},
             "one_way_turnover": 0.1, "score_improvement_net_of_cost": 0.001}
            for i in range(1, 5)]
    r = _run(reallocation_history=hist)
    assert "LIMITED_CROSS_SECTIONAL_ALPHA" in r["reallocation_diagnostics"]["interpretation"]


def test_29_distinguish_staleness_from_model_weakness():
    # Elevated HOC churn + elevated reallocation churn -> PORTFOLIO_STALENESS, a distinct
    # diagnosis from signal/ranking degradation (which requires weak rank IC).
    hist_hoc = [{"date": "2026-08-0%d" % i, "state": "READY",
                 "recommendation_counts": {"HOLD": 5, "EXIT": 6, "REPLACE": 4}}
                for i in range(1, 5)]
    hist_re = [{"date": "2026-08-0%d" % i, "state": "READY", "action_counts": {},
                "one_way_turnover": 0.6, "score_improvement_net_of_cost": 0.02}
               for i in range(1, 5)]
    r = _run(hoc_history=hist_hoc, reallocation_history=hist_re)
    assert K.DEG_STALENESS in r["degradation"]["categories"]
    assert K.DEG_RANKING not in r["degradation"]["categories"]


# =========================================================================== #
# D. CHALLENGER (30-36)
# =========================================================================== #
def test_30_no_challenger():
    r = _run(challenger={"available": False})
    assert r["challenger"]["state"] == K.CH_NOT_EVALUATED


def test_31_insufficient_challenger_evidence():
    r = _run(challenger={"available": True, "signal": "x",
             "book_comparisons": [{"book": "top25", "excess_pp": 6.0, "observation_count": 5}]})
    assert r["challenger"]["state"] == K.CH_INSUFFICIENT


def test_32_underperforming_challenger():
    r = _run(challenger={"available": True, "signal": "x",
             "book_comparisons": [{"book": "top25", "excess_pp": -3.0, "observation_count": 60}]})
    assert r["challenger"]["state"] == K.CH_UNDERPERFORMING


def test_33_competitive_challenger():
    r = _run(challenger={"available": True, "signal": "x",
             "book_comparisons": [{"book": "top25", "excess_pp": 1.0, "observation_count": 60}]})
    assert r["challenger"]["state"] == K.CH_COMPETITIVE


def test_34_promising_challenger():
    r = _run(challenger={"available": True, "signal": "x",
             "book_comparisons": [{"book": "top25", "excess_pp": 3.0, "observation_count": 60}]})
    assert r["challenger"]["state"] == K.CH_PROMISING
    assert r["research_state"] == K.STATE_CHALLENGER_PROMISING
    assert r["challenger"]["manual_model_review_required"] is True


def test_35_superior_candidate():
    r = _run(challenger={"available": True, "signal": "x",
             "book_comparisons": [{"book": "top25", "excess_pp": 8.0, "observation_count": 60}]})
    assert r["challenger"]["state"] == K.CH_SUPERIOR


def test_36_superior_candidate_never_auto_promotes():
    r = _run(challenger={"available": True, "signal": "x",
             "book_comparisons": [{"book": "top25", "excess_pp": 8.0, "observation_count": 60}]})
    assert r["challenger"]["auto_promotion_allowed"] is False
    assert r["safety"]["promoted_model"] is False
    assert r["recalibration"]["automatic_promotion_allowed"] is False


# =========================================================================== #
# E. RESEARCH OPPORTUNITIES (37-44)
# =========================================================================== #
def test_37_deterministic_ranking():
    a = _run(); b = _run()
    assert [o["opportunity_id"] for o in a["research_opportunities"]] == \
           [o["opportunity_id"] for o in b["research_opportunities"]]
    prios = [o["priority"] for o in a["research_opportunities"]]
    assert prios == sorted(prios, reverse=True)


def test_38_hypothesis_included():
    r = _run(ranking_quality={"rank_ic_mean": -0.05, "rank_ic_positive_rate_pct": 30.0,
             "rank_ic_obs": 40, "rank_ic_recent_mean": -0.05, "rank_ic_prior_mean": 0.03,
             "decile_spread_pp": -1.0, "decile_spread_obs": 40})
    opp = next(o for o in r["research_opportunities"]
               if o["opportunity_id"] == "investigate_rank_ic_degradation")
    assert opp["hypothesis"] and opp["reason"]


def test_39_evidence_included():
    r = _run()
    for o in r["research_opportunities"]:
        assert "supporting_evidence" in o


def test_40_gaps_included():
    r = _run(evidence={"forward_observation_count": 2, "eligible_session_count": 40,
             "ranking_observation_count": 500, "universe_coverage_ratio": 0.9})
    opp = next(o for o in r["research_opportunities"]
               if o["opportunity_id"] == "accumulate_forward_evidence")
    assert opp["evidence_gaps"]


def test_41_bounded_experiment_specification():
    r = _run()
    for o in r["research_opportunities"]:
        exp = o["proposed_experiment"]
        assert exp["experiment_type"] and exp["shadow_only"] is True
        assert exp["changes_champion"] is False
        assert exp["runner"] == "SPECIFICATION_ONLY_NO_AUTOMATIC_EXECUTION"


def test_42_forbidden_actions_present():
    r = _run()
    for o in r["research_opportunities"]:
        for tok in ("AUTO_PROMOTE_MODEL", "AUTO_RECALIBRATE_CHAMPION",
                    "MUTATE_OPERATIONAL_PORTFOLIO", "CREATE_ORDERS", "ENABLE_CADENCE"):
            assert tok in o["forbidden_actions"]


def test_43_manual_approval_requirement():
    r = _run()
    for o in r["research_opportunities"]:
        assert o["required_manual_approval"] is True
        assert o["manual_approval_state"] == K.MANUAL_APPROVAL_PENDING


def test_44_no_operational_side_effect():
    ic = _ic()
    snap = copy.deepcopy(ic)
    K.evaluate(input_contract=ic)
    assert ic == snap  # the kernel never mutates its inputs / produces a side effect.


# =========================================================================== #
# F. RECALIBRATION (45-50)
# =========================================================================== #
def test_45_no_evidence_no_recalibration():
    r = _run(evidence={"forward_observation_count": 0, "eligible_session_count": 0,
             "ranking_observation_count": 0, "universe_coverage_ratio": 0.0},
             ranking_quality={}, performance={})
    assert r["recalibration"]["state"] == K.RECAL_INSUFFICIENT
    assert r["recalibration"]["recommended"] is False


def test_46_short_pnl_weakness_is_watch():
    # Poor benchmark-relative performance but thin forward evidence -> WATCH, not RECAL.
    r = _run(evidence={"forward_observation_count": 3, "eligible_session_count": 4,
             "ranking_observation_count": 500, "universe_coverage_ratio": 0.9},
             ranking_quality={"rank_ic_mean": 0.0, "rank_ic_positive_rate_pct": 45.0,
             "rank_ic_obs": 3, "decile_spread_pp": 0.0, "decile_spread_obs": 3},
             performance={"excess_pct_points": -8.0, "max_drawdown_pct": -18.0,
             "avg_daily_turnover": 0.2, "benchmark_ticker": "SPY",
             "eligible_session_count": 4})
    assert r["research_state"] == K.STATE_WATCH
    assert "INSUFFICIENT_FORWARD_EVIDENCE" in r["state_reason_codes"]
    assert r["recalibration"]["state"] == K.RECAL_INSUFFICIENT


def test_47_sufficient_rank_ic_degradation_recommends_recalibration():
    r = _run(ranking_quality={"rank_ic_mean": -0.05, "rank_ic_positive_rate_pct": 30.0,
             "rank_ic_obs": 40, "rank_ic_recent_mean": -0.05, "rank_ic_prior_mean": 0.03,
             "decile_spread_pp": -1.0, "decile_spread_obs": 40})
    assert r["recalibration"]["state"] == K.RECAL_RECOMMENDED
    assert r["recalibration"]["recommended"] is True


def test_48_checkpoint_due():
    # No prior checkpoint (marker 0) + sufficient forward obs beyond the interval.
    r = _run(last_checkpoint_forward_observation_count=0)
    assert r["recalibration"]["state"] in (K.RECAL_CHECKPOINT_DUE, K.RECAL_RECOMMENDED)
    if r["recalibration"]["state"] == K.RECAL_CHECKPOINT_DUE:
        assert r["research_state"] == K.STATE_INVESTIGATE


def test_49_challenger_evidence_monitoring():
    r = _run(challenger={"available": True, "signal": "x",
             "book_comparisons": [{"book": "top25", "excess_pp": 3.0, "observation_count": 60}]})
    assert r["research_state"] == K.STATE_CHALLENGER_PROMISING


def test_50_no_automatic_retraining():
    r = _run(ranking_quality={"rank_ic_mean": -0.05, "rank_ic_positive_rate_pct": 30.0,
             "rank_ic_obs": 40, "rank_ic_recent_mean": -0.05, "rank_ic_prior_mean": 0.03,
             "decile_spread_pp": -1.0, "decile_spread_obs": 40})
    assert r["recalibration"]["automatic_retraining_allowed"] is False
    assert r["recalibration"]["manual_approval_required"] is True
    assert r["safety"]["retrained_model"] is False


# =========================================================================== #
# G. PERSISTENCE / API (51-64)
# =========================================================================== #
def _ps(date="2026-08-05"):
    return {"active_book": {"book_id": "alpha_paper_book_1", "book_label": "Alpha #1",
                            "status": "ACTIVE"},
            "dates": {"eligible_market_date": date}, "state_hash": "PSHASH",
            "positions": [{"ticker": "AAA", "sector": "Tech", "portfolio_weight": 0.2}]}


def _scoring():
    return {"champion_model_id": "composite_sn",
            "primary_model_id": "fundamental_momentum_50_50_v1", "strategy_version": "v1",
            "model_registry_version": "p20", "automatic_promotion_allowed": False,
            "output_hash": "SCHASH", "coverage_ratio": 0.9,
            "rankings": [{"ticker": "X%d" % i, "rank": i} for i in range(500)]}


def _cells():
    return [{"model_id": "fundamental_momentum_50_50_v1", "horizon_eligible_closes": h,
             "ic_mean": 0.05, "ic_positive_rate_pct": 60.0, "ic_observation_count": 30,
             "matured_observation_count": 30, "top_minus_bottom_pp": 1.2}
            for h in (1, 5, 20, 63)]


def _summ():
    return {"evidence_state": "EV_HORIZON_ALIGNED", "matured_outcome_count": 30,
            "active_model_id": "fundamental_momentum_50_50_v1"}


def _perf():
    return {"summary": {"cumulative_return_pct": 3.0, "benchmark_cumulative_return_pct": 2.0,
            "excess_vs_benchmark_pct_points": 1.0, "max_drawdown_pct": -5.0,
            "total_transaction_cost": 50.0, "benchmark_ticker": "SPY"}, "n_rows": 40}


def _roll():
    return {"inception": {"avg_daily_turnover": 0.2}, "windows": []}


def _rp(tmp=None, **over):
    kw = dict(portfolio_state=_ps(), scoring=_scoring(), prediction_cells=_cells(),
              prediction_summary=_summ(), performance=_perf(), rolling_evidence=_roll(),
              tournament=None, decision_gate=None,
              hoc_history=[{"date": "2026-08-04", "state": "READY",
                            "recommendation_counts": {"HOLD": 20, "EXIT": 1}}],
              reallocation_history=[{"date": "2026-08-04", "state": "READY",
                            "action_counts": {"RETAIN": 20}, "one_way_turnover": 0.1,
                            "score_improvement_net_of_cost": 0.02}],
              regime={"available": False}, research_agent_dir=str(tmp) if tmp else None)
    kw.update(over)
    return A.run_and_persist(**kw)


def test_51_not_run_before_any_run(tmp_path):
    r = A.load_research_agent(portfolio_state=_ps(), research_agent_dir=str(tmp_path))
    assert r["state"] == A.STATE_NOT_RUN


def test_52_ready_after_run(tmp_path):
    _rp(tmp=tmp_path)
    r = A.load_research_agent(portfolio_state=_ps(), research_agent_dir=str(tmp_path))
    assert r["state"] in K.RESEARCH_STATE_VOCAB and r["state"] != K.STATE_BLOCKED
    assert r["research_state"] == r["state"]


def test_53_insufficient_evidence_state_persisted(tmp_path):
    # Thin evidence -> a persistable INSUFFICIENT_EVIDENCE / WATCH assessment.
    out = _rp(tmp=tmp_path, prediction_cells=[], prediction_summary={"matured_outcome_count": 0},
              performance={"summary": {}, "n_rows": 2})
    assert out["assessment"]["research_state"] in (K.STATE_INSUFFICIENT_EVIDENCE, K.STATE_WATCH)
    assert out["persistence"]["persisted"] is True


def test_54_blocked_not_persisted(tmp_path):
    out = A.run_and_persist(portfolio_state={"active_book": {"book_id": None},
                            "dates": {"eligible_market_date": None}},
                            scoring={}, research_agent_dir=str(tmp_path))
    assert out["assessment"]["research_state"] == K.STATE_BLOCKED
    assert out["persistence"]["persisted"] is False


def test_55_immutable_artifact(tmp_path):
    out = _rp(tmp=tmp_path)
    art = A.load_latest_artifact(active_book_id="alpha_paper_book_1",
                                 eligible_market_date="2026-08-05",
                                 research_agent_dir=str(tmp_path))
    assert art["assessment"]["assessment_hash"] == out["assessment"]["assessment_hash"]
    assert art["assessment_id"].startswith("raga_")


def test_56_atomic_persistence_no_partial(tmp_path):
    _rp(tmp=tmp_path)
    assert list(Path(tmp_path).rglob("*.tmp")) == []


def test_57_idempotent_reuse(tmp_path):
    a = _rp(tmp=tmp_path)
    b = _rp(tmp=tmp_path)
    assert b["persistence"]["status"] == "REUSED_EXISTING"
    assert b["persistence"]["assessment_id"] == a["persistence"]["assessment_id"]


def test_58_different_evidence_hash_supersedes_not_reuse(tmp_path):
    a = _rp(tmp=tmp_path)
    b = _rp(tmp=tmp_path, performance={"summary": {"cumulative_return_pct": 1.0,
            "benchmark_cumulative_return_pct": 9.0, "excess_vs_benchmark_pct_points": -8.0,
            "max_drawdown_pct": -18.0, "benchmark_ticker": "SPY"}, "n_rows": 40})
    assert b["persistence"]["status"] == "SUPERSEDED_PRIOR"
    assert b["persistence"]["assessment_id"] != a["persistence"]["assessment_id"]
    r = A.load_research_agent(portfolio_state=_ps(), research_agent_dir=str(tmp_path))
    assert r["artifact"]["assessment_id"] == b["persistence"]["assessment_id"]


def test_59_read_back(tmp_path):
    out = _rp(tmp=tmp_path)
    r = A.load_research_agent(portfolio_state=_ps(), research_agent_dir=str(tmp_path))
    assert r["assessment_hash"] == out["assessment"]["assessment_hash"]


def test_60_endpoint_get_only():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    from paper_trader.config import get_settings
    c = TestClient(app, raise_server_exceptions=False)
    key = get_settings().service_api_key
    assert c.post("/v1/research/research-agent", headers={"X-API-Key": key}).status_code == 405


def test_61_endpoint_requires_auth():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    c = TestClient(app, raise_server_exceptions=False)
    assert c.get("/v1/research/research-agent").status_code in (401, 403)


def test_62_endpoint_response_schema(monkeypatch):
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    from paper_trader.config import get_settings
    canned = A.load_research_agent(portfolio_state=_ps(), research_agent_dir="___nope___")
    monkeypatch.setattr("paper_trader.api.app._research_agent.load_research_agent",
                        lambda: canned)
    c = TestClient(app, raise_server_exceptions=False)
    resp = c.get("/v1/research/research-agent",
                 headers={"X-API-Key": get_settings().service_api_key})
    assert resp.status_code == 200
    body = resp.json()
    for k in ("schema_version", "state", "research_state", "state_vocabulary", "champion",
              "evidence_quality", "champion_health", "degradation", "challenger",
              "recalibration", "research_opportunities", "data_gaps", "safety",
              "sole_execution_path"):
        assert k in body, k


def test_63_no_provider_or_prediction_call_in_read_path(monkeypatch, tmp_path):
    # The READ path never runs the engine and never calls a provider/prediction owner.
    def _boom(*a, **k):
        raise AssertionError("read path must not evaluate the research state")
    monkeypatch.setattr(K, "evaluate", _boom)
    _ = A.load_research_agent(portfolio_state=_ps(), research_agent_dir=str(tmp_path))


def test_64_no_operational_mutation_safety_block(tmp_path):
    out = _rp(tmp=tmp_path)
    s = out["assessment"]["safety"]
    assert s["promoted_model"] is False and s["created_orders"] is False
    assert s["changed_holdings"] is False and s["changed_nav"] is False
    assert s["wrote_champion_pointer"] is False and s["executed_experiment"] is False


# =========================================================================== #
# H. DRC (65-72)
# =========================================================================== #
def _fake_ra_run(**_):
    res = K.evaluate(input_contract=_ic())
    return {"assessment": res, "persistence": {"status": "CREATED",
            "assessment_id": "raga_test", "persisted": True, "superseded_assessment_id": None}}


def _drc_run(tmp, ra_fn=None, **kw):
    s7 = importlib.import_module("tests.test_slice7_reallocation_proposal")
    s3 = importlib.import_module("tests.test_slice3_daily_research_cycle")
    return s3._run(tmp, holding_opp_cost_fn=s7._fake_hoc_run,
                   reallocation_proposal_fn=s7._fake_realloc_run,
                   research_agent_fn=ra_fn or _fake_ra_run, **kw)


def test_65_research_agent_step_selected(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert "RUN_RESEARCH_AGENT" in rec["completed_steps"]


def test_66_ordering_after_reallocation(tmp_path):
    rec, _ = _drc_run(tmp_path)
    steps = rec["completed_steps"]
    assert steps.index("BUILD_REALLOCATION_PROPOSAL") < steps.index("RUN_RESEARCH_AGENT")
    assert steps.index("RUN_RESEARCH_AGENT") < steps.index("RUN_PORTFOLIO_ASSESSMENT")


def test_67_artifact_ref_in_manifest(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert rec["research_agent_owner"] == "api.research_agent"
    assert rec["research_agent_id"] == "raga_test"
    assert rec["research_agent_selected"] is True


def test_68_assessment_hash_in_manifest(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert rec["research_agent_hash"]
    assert rec["research_agent_state"]


def test_69_idempotent_rerun(tmp_path):
    rec1, _ = _drc_run(tmp_path)
    rec2, _ = _drc_run(tmp_path)
    assert rec2["reused_existing_run"] is True
    assert rec2["research_agent_hash"] == rec1["research_agent_hash"]


def test_70_terminal_persistence_preserved(tmp_path):
    rec, _ = _drc_run(tmp_path)
    assert rec["state"] in ("COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP")
    assert rec["terminal"] is True
    assert rec["research_agent_id"] and rec["research_agent_hash"]


def test_71_agent_failure_does_not_fabricate_completion(tmp_path):
    def _failing(**_):
        return {"assessment": {"research_state": "BLOCKED"},
                "persistence": {"status": "NOT_PERSISTED", "persisted": False}}
    rec, _ = _drc_run(tmp_path, ra_fn=_failing)
    step = next(s for s in rec["step_results"] if s["step_id"] == "RUN_RESEARCH_AGENT")
    assert step["status"] == "FAILED"
    assert rec["research_agent_selected"] is False
    assert rec["state"] in ("COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP")  # research never blocks


def test_72_daily_close_independence_preserved(tmp_path):
    # The research-agent step is a research-governance step; it never adds an order/close
    # requirement and leaves the reallocation + HOC references intact.
    rec, _ = _drc_run(tmp_path)
    assert rec["reallocation_proposal_owner"] == "api.reallocation_proposal"
    assert rec["opportunity_cost_owner"] == "api.holding_opportunity_cost"


# =========================================================================== #
# I. UI (73-81)
# =========================================================================== #
UI_TEXT = UI.read_text(encoding="utf-8")


def test_73_research_agent_panel_present():
    assert 'id="rag-panel"' in UI_TEXT


def test_74_one_canonical_loader():
    assert UI_TEXT.count("function loadResearchAgent") == 1


def test_75_no_js_metric_calculation_in_region():
    start = UI_TEXT.find("function loadResearchAgent")
    end = UI_TEXT.find("window.renderResearchAgent")
    assert start != -1 and end != -1 and end > start
    region = UI_TEXT[start:end]
    for pat in ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.", "compute"):
        assert pat not in region, pat


def test_76_state_rendering_present():
    assert 'id="rag-state"' in UI_TEXT and 'id="rag-summary"' in UI_TEXT


def test_77_evidence_rendering_present():
    assert 'id="rag-health"' in UI_TEXT and 'id="rag-degradation"' in UI_TEXT


def test_78_opportunity_queue_present():
    assert 'id="rag-opportunities"' in UI_TEXT


def test_79_challenger_state_present():
    assert 'id="rag-challenger"' in UI_TEXT


def test_80_recalibration_rendered():
    assert "Recalibration" in UI_TEXT and "rag-challenger" in UI_TEXT


def test_81_safety_badges_present():
    for badge in ("RESEARCH ONLY", "NO MODEL PROMOTION", "NO PORTFOLIO MUTATION",
                  "NO ORDERS", "AUTOMATION OFF"):
        assert badge in UI_TEXT, badge


# =========================================================================== #
# J. ARCHITECTURE (82-89)
# =========================================================================== #
def _audit():
    aud = importlib.import_module("scripts.audit_architecture")
    return aud, aud.check_research_agent_ownership(aud._iter_source_files())


def test_82_exact_owners():
    _, ra = _audit()
    assert ra["kernel_present"] and ra["owner_present"]
    assert ra["landed_modules_missing"] == []
    assert ra["second_calculation_owner_modules"] == []
    assert ra["second_composition_owner_modules"] == []


def test_83_no_duplicate_metrics():
    _, ra = _audit()
    assert ra["missing_delegation"] == []
    assert ra["kernel_forks_hoc"] is False
    assert ra["kernel_forks_reallocation"] is False
    assert ra["kernel_forks_scoring"] is False


def test_84_no_promotion_no_retraining():
    _, ra = _audit()
    assert ra["owner_forbidden_calls"] == []
    assert ra["kernel_forbidden_calls"] == []
    assert ra["automatic_model_promotion_allowed"] is False
    assert ra["automatic_model_retraining_allowed"] is False


def test_85_no_operational_mutation_no_second_registry():
    _, ra = _audit()
    assert ra["forbidden_routes_present"] == []
    assert ra["forbidden_route_methods_present"] is False
    assert ra["second_registry_present_modules"] == []
    assert not (ROOT / "api" / "model_registry.py").exists()


def test_86_strict_audit_exit_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    assert aud.main(["--strict", "--json-only"]) == 0


def test_87_strict_audit_covers_research_agent():
    _, ra = _audit()
    assert ra["drc_delegates"] and ra["drc_step_present"]
    assert ra["persist_present"] and ra["atomic_idempotent_persist_present"]
    assert ra["route_get_count"] == 1 and ra["research_agent_route_methods"] == ["GET"]


def test_88_inventory_drift_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    drift = aud.check_inventory_drift(aud._iter_source_files())
    assert drift["on_disk_not_in_inventory"] == []
    assert drift["in_inventory_not_on_disk"] == []


def test_89_cadence_disabled_and_one_ui_loader():
    _, ra = _audit()
    assert ra["cadence_enabled"] is False
    assert ra["ui_loader_count"] == 1
    assert ra["ui_metric_computation"] == []


# =========================================================================== #
# Bounded performance — the warm GET never recomputes the assessment.
# =========================================================================== #
def test_perf_get_never_recomputes(monkeypatch, tmp_path):
    _rp(tmp=tmp_path)

    def _boom(*a, **k):
        raise AssertionError("GET must not recompute the assessment (kernel called).")
    monkeypatch.setattr(K, "evaluate", _boom)
    r = A.load_research_agent(portfolio_state=_ps(), research_agent_dir=str(tmp_path))
    assert r["state"] in K.RESEARCH_STATE_VOCAB and r["state"] != A.STATE_NOT_RUN
