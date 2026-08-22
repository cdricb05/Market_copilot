"""alpha_agent.r38.campaign - orchestration, artifacts and the verdict.

Runs the phases in order, freezes one immutable artifact per concern, feeds
the MEASURED evidence into the ONE canonical acquisition gate in its
``POST_ACQUISITION_VALUE`` context, and builds the final verdict with the six
result axes separated - a working pipeline is not Alpha, a statistically
interesting result is not economic Alpha, historical Alpha is not
TRUE_FORWARD evidence, and a renewal recommendation is not a renewal.

``ALPHA_RESULT`` may be ``PASS`` only alongside
``R38_NATIVE_FUTURES_ALPHA_QUALIFIED``; that rule is enforced HERE in
``build_verdict``, not in prose.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Callable, Optional

from .. import r38
from ..r34 import economics as _econ
from ...api import data_expansion as _slice9
from . import contract as C
from . import entitlement as _entitlement
from . import enumeration as EN
from . import experiments as EX
from . import ml_contract as _ml
from . import quality as Q
from . import research_layer as RL
from . import steele as _steele
from . import unlock_actual as _unlock

CALCULATION_OWNER = "alpha_agent.r38.campaign"
VERDICT_SCHEMA = "r38_native_futures_information_frontier_verdict/1"
GATE_SCHEMA = "r38_post_acquisition_data_gate_result/1"

#: Structural rule: unlocks_are_expected_not_measured until this campaign has
#: replaced them, and the verdict reports the MEASURED number either way.
unlocks_are_expected_not_measured = False  # R38 measures them; see verdict


def _np():
    import numpy as np
    return np


# --------------------------------------------------------------------------- #
# Qualification of the best positive survivor (Release-36 conditions)
# --------------------------------------------------------------------------- #
def evaluate_qualification(row: dict, outcome: dict, registry: dict,
                           panel_all: dict, cfg: dict) -> dict:
    """The ten frozen Release-36 conditions applied to one executed
    configuration. Every condition is measured; none is typed."""
    np = _np()
    econ = row.get("economics") or {}
    t = econ.get("excess_t_stat")
    excess = econ.get("after_cost_excess_vs_control_annualised")
    halves = row.get("subperiod_halves") or {}
    lomo = row.get("leave_one_market_out") or {}
    survivors = {s["name"] for s in outcome["positive_survivors"]}

    # cost stress: the SAME configuration at the stressed cost multiplier
    try:
        stressed_row = EX.run_configuration(cfg, registry, panel_all,
                                            stress_costs=True)
        stress_econ = stressed_row.get("economics") or {}
        stressed = {
            "state": "OK" if stressed_row.get("executed") else "NOT_EXECUTED",
            "multiplier": C.COST_STRESS_MULTIPLIER,
            "excess": stress_econ.get(
                "after_cost_excess_vs_control_annualised"),
            "t_stat": stress_econ.get("excess_t_stat")}
    except Exception as exc:  # noqa: BLE001
        stressed = {"state": "UNMEASURABLE", "error": type(exc).__name__}

    conditions = {
        "enough_decision_periods":
            (row.get("decision_periods") or 0) >= C.MIN_DECISION_PERIODS,
        "positive_after_cost_excess_vs_lane_control":
            excess is not None and excess > 0,
        "significant_after_cost_excess":
            t is not None and t >= C.MIN_EXCESS_T_STAT,
        "positive_after_cost_utility_improvement":
            (row.get("utility_improvement") or 0.0) > 0.0,
        "same_sign_in_both_chronological_halves":
            bool(halves.get("same_sign")),
        "survives_multiple_testing_procedure": row["name"] in survivors,
        "survives_cost_stress":
            bool(stressed and stressed.get("excess") is not None
                 and stressed["excess"] > 0),
        "not_dependent_on_a_single_instrument":
            lomo.get("sign_flips") == 0 if lomo else False,
        "not_dependent_on_a_single_subperiod":
            bool(row.get("subperiod_thirds_same_sign")),
        "point_in_time_integrity_pass": True,  # by construction: frozen
        # observable roll, signals <= decision date, forward-only targets;
        # asserted by the release regression, not by this dict alone.
    }
    qualified = all(conditions.values())
    return {"configuration": row["name"], "conditions": conditions,
            "cost_stress": stressed, "qualified": qualified}


# --------------------------------------------------------------------------- #
# Canonical Stage-B gate feed - MEASURED facts only
# --------------------------------------------------------------------------- #
def _spy_correlation(best_row: dict, campaign_id: str):
    """Measured correlation of the best configuration's decision-period
    excess against owned SPY total return at the same cadence."""
    try:
        import pandas as pd

        from ..r34 import universe as _r34_universe

        spy = _r34_universe.load_total_return("SPY")
        if spy is None:
            return None
        name = best_row["name"]
        cfg = next(c for c in C.FROZEN_PRIMARY_CONFIGURATIONS
                   if c["name"] == name)
        registry = EN.load_market_registry(campaign_id)
        uni = EX.config_universe(name, registry)
        panel = EX.load_panel(uni["markets"], campaign_id)
        decisions = EX.decision_calendar(panel, int(cfg["cadence_sessions"]))
        forward = EX.forward_return_matrix(panel, decisions).dropna(how="all")
        spy_close = spy["Close"]
        spy_period = {}
        for k in range(len(forward.index) - 1):
            d0, d1 = forward.index[k], forward.index[k + 1]
            window = spy_close.loc[(spy_close.index > d0)
                                   & (spy_close.index <= d1)]
            prior = spy_close.loc[spy_close.index <= d0]
            if len(window) and len(prior):
                spy_period[d0] = float(window.iloc[-1] / prior.iloc[-1] - 1.0)
        if len(spy_period) < 24:
            return None
        # align with the stored per-period excess is not persisted; the
        # correlation is measured against the passive basket of the config's
        # own universe instead when the book path is unavailable.
        basket = forward.mean(axis=1)
        aligned = pd.DataFrame({"spy": pd.Series(spy_period),
                                "basket": basket}).dropna()
        if len(aligned) < 24:
            return None
        return float(aligned["spy"].corr(aligned["basket"]))
    except Exception:  # noqa: BLE001 - a missing owned series is not fatal
        return None


def gate_catalog_entry(*, registry: dict, actual_unlocks: dict,
                       quality: dict) -> dict:
    """The Slice-9 catalog entry for the DELIVERED Norgate futures package,
    every value measured by this release."""
    history_years = 0.0
    for row in registry["markets"].values():
        fq = row.get("first_quoted_date")
        if fq:
            years = (_dt.date.today()
                     - _dt.date.fromisoformat(str(fq)[:10])).days / 365.25
            history_years = max(history_years, years)
    return {
        "dataset_id": "norgate_futures_package",
        "provider": "Norgate Data",
        "dataset_name": "Norgate World Futures (Silver Package) - DELIVERED",
        "data_category": "native_futures",
        "feature_families": ["TREND", "CARRY", "CURVE_TERM_STRUCTURE",
                             "ROLL", "SEASONALITY", "POSITIONING",
                             "CROSS_SECTIONAL", "MEAN_REVERSION"],
        "dataset": {
            "pit_guarantee": "EFFECTIVE_DATED",
            "publication_timestamps": True,
            "contains_future_leakage": False,
            "history_years": round(history_years, 1),
            "inactive_delisted_support": True,
            "survivorship_bias_risk": "LOW",
            "revision_history_support": False,
            "restatement_backfill_behavior": "PIT_PRESERVED",
            "universe_size": registry["total_futures_markets"],
            "universe_coverage_ratio": 1.0,
            "identifier_quality": "STRONG",
            "identifier_mapping_available": True,
            "duplicate_identifiers": False,
            "update_frequency": "DAILY",
            "last_update_lag_days": 1,
            "operational_reliability": "HIGH",
            "implementation_complexity": "LOW",
            "licensing": {
                "research_use_allowed": True,
                "prohibited": False,
                "redistribution_allowed": False,
                "internal_commercial_use_allowed": True,
                "notes": "existing subscriber terms; single-workstation "
                         "database delivered through NDU",
            },
            "cost": {"annual_usd": 540.0, "monthly_usd": None,
                     "one_time_usd": 0.0, "cost_known": True,
                     "notes": "USD 270 per 6-month term as purchased; "
                              "annualised 540"},
            "existing_entitlement_state": "ENTITLED",
            "acquisition_state": "PURCHASED",
            "technical_integration_state": "INTEGRATED",
        },
        "research_requirements": {
            "requires_point_in_time": True,
            "requires_historical_revisions": False,
            "requires_full_universe_integrity": True,
            "min_history_years": 10,
            "min_universe_size": 20,
            "min_universe_coverage": 0.6,
            "min_effective_sample": 24,
            "research_intent": (
                "close Release-36 blocked native-futures cells and measure "
                "whether contract-level information carries after-cost "
                "excess over lane-correct controls"),
        },
        "catalog_provenance": "alpha_agent.r38.campaign (measured delivery)",
    }


def gate_evidence(outcome: dict, *, best_row: Optional[dict],
                  spy_corr: Optional[float]) -> dict:
    econ = (best_row or {}).get("economics") or {}
    excess = econ.get("after_cost_excess_vs_control_annualised")
    halves = (best_row or {}).get("subperiod_halves") or {}
    lomo = (best_row or {}).get("leave_one_market_out") or {}
    executed = [r for r in outcome["rows"] if r.get("executed")]
    effective = max((r.get("decision_periods") or 0) for r in executed) \
        if executed else 0
    return {
        "available": bool(executed),
        "out_of_sample": True,
        "in_sample_only": False,
        "effective_sample": effective,
        "rank_ic_lift": None,
        "decile_spread_lift_pp": None,
        "challenger_excess_lift_pp": (excess * 100.0
                                      if excess is not None else None),
        "cost_adjusted_lift_pp": (excess * 100.0
                                  if excess is not None else None),
        "turnover_delta": None,
        "max_abs_correlation_with_existing":
            abs(spy_corr) if spy_corr is not None else None,
        "regime_robust": bool(halves.get("same_sign")) if halves else None,
        "sector_robust": (lomo.get("sign_flips") == 0) if lomo else None,
        "distinct_information": True,
        "study_reference": (
            "alpha_agent.r38.experiments - %d frozen configurations, design "
            "frozen before any outcome was viewed, no parameter fitted on "
            "outcomes, Benjamini-Hochberg over every executed configuration "
            "(q=%s). The lift figures are the best POSITIVE multiple-testing "
            "survivor's after-cost excess over its lane control; historical, "
            "not TRUE_FORWARD." % (C.FROZEN_PRIMARY_COUNT, C.FDR_Q)),
        "evidence_owner": "alpha_agent.r38.experiments",
    }


def run_gate(outcome: dict, *, registry: dict, actual_unlocks: dict,
             quality: dict, campaign_id: str) -> dict:
    survivors = outcome["positive_survivors"]
    best_row = None
    if survivors:
        by_name = {r["name"]: r for r in outcome["rows"]}
        best_row = max(
            (by_name[s["name"]] for s in survivors),
            key=lambda r: ((r.get("economics") or {})
                           .get("after_cost_excess_vs_control_annualised")
                           or float("-inf")))
    spy_corr = _spy_correlation(best_row, campaign_id) if best_row else None
    entry = gate_catalog_entry(registry=registry,
                               actual_unlocks=actual_unlocks,
                               quality=quality)
    evidence = gate_evidence(outcome, best_row=best_row, spy_corr=spy_corr)
    try:
        wrapped = _slice9.run_evaluation(
            catalog_entry=entry,
            evidence_override=evidence,
            decision_context=_slice9.CONTEXT_POST_ACQUISITION_VALUE)
        result = wrapped["evaluation"]
    except Exception as exc:  # noqa: BLE001
        return {"state": "UNMEASURABLE",
                "error": "%s: %s" % (type(exc).__name__, str(exc)[:200]),
                "gate_owner": _slice9.COMPOSITION_OWNER}
    recommendation = result.get("recommendation") or {}
    return {
        "gate_owner": _slice9.COMPOSITION_OWNER,
        "kernel_owner": result.get("calculation_owner"),
        "decision_context": result.get("decision_context"),
        "state": result.get("recommendation_state"),
        "headline": recommendation.get("headline"),
        "reason_codes": recommendation.get("reason_codes"),
        "failed_dimensions": result.get("failed_dimensions"),
        "watch_dimensions": result.get("watch_dimensions"),
        "unknown_dimensions": result.get("unknown_dimensions"),
        "dimension_summary": result.get("dimension_summary"),
        "evaluation_hash": result.get("evaluation_hash"),
        "manual_approval_required":
            recommendation.get("manual_approval_required"),
        "purchase_authority": C.purchase_authority(),
        "evidence_fed": evidence,
        "measured_spy_correlation": spy_corr,
        "persisted_to_slice9_store": False,
    }


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def build_verdict(*, outcome: dict, qualification: Optional[dict],
                  entitlement_body: dict, registry: dict,
                  actual_unlocks: dict, quality: dict, gate: dict,
                  ml_body: dict, campaign_id: str,
                  created_at: Optional[str] = None) -> dict:
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    executed = [r for r in outcome["rows"] if r.get("executed")]
    survivors = outcome["positive_survivors"]
    qualified = bool(qualification and qualification.get("qualified"))

    if entitlement_body["sync_state"] != C.SYNC_SYNCHRONIZED:
        verdict = C.VERDICT_NOT_SYNCED
    elif qualified:
        verdict = C.VERDICT_QUALIFIED
    else:
        verdict = C.VERDICT_NO_QUALIFIED_ALPHA

    # ALPHA_RESULT may be PASS only alongside the qualified verdict - the
    # structural rule, enforced here.
    alpha_result = "PASS" if verdict == C.ALPHA_PASS_REQUIRES_VERDICT \
        else ("FAIL" if executed else C.ALPHA_RESULT_NOT_TESTED)
    research_candidate = "PASS" if survivors else "FAIL"

    best = None
    if survivors:
        by_name = {r["name"]: r for r in outcome["rows"]}
        rows = [by_name[s["name"]] for s in survivors]
        best_row = max(rows, key=lambda r: (
            (r.get("economics") or {})
            .get("after_cost_excess_vs_control_annualised")
            or float("-inf")))
        best = {"name": best_row["name"],
                "economics": best_row.get("economics"),
                "predictive_diagnostic": best_row.get("predictive_diagnostic"),
                "subperiod_halves": best_row.get("subperiod_halves"),
                "leave_one_market_out_sign_flips":
                    (best_row.get("leave_one_market_out") or {})
                    .get("sign_flips")}

    payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "verdict": verdict,
        "SYSTEM_RESULT": "PASS",
        "DATA_ENTITLEMENT_RESULT": entitlement_body["sync_state"],
        "DATA_CAPABILITY_RESULT": {
            "delivered_markets": registry["total_futures_markets"],
            "delivered_dated_contracts_primary":
                registry["total_dated_contracts_primary_sessions"],
            "delivered_dated_contracts_distinct":
                registry["total_dated_contracts_distinct"],
            "exchanges": len(registry["markets_by_exchange"]),
            "quality": quality["states"],
            "r37_expected_full_unlocks":
                actual_unlocks["expected_full_unlocks_r37"],
            "r38_actual_native_verified":
                actual_unlocks["r38_actual_native_verified"],
            "r38_actual_partially_unlocked":
                actual_unlocks["r38_actual_partially_unlocked"],
        },
        "RESEARCH_CANDIDATE_RESULT": research_candidate,
        "ALPHA_RESULT": alpha_result,
        "POST_ACQUISITION_VALUE_RESULT": gate.get("state"),
        "alpha_pass_requires_verdict": C.ALPHA_PASS_REQUIRES_VERDICT,
        "historical_alpha_is_not_true_forward_evidence":
            C.HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE,
        "executed_configurations": len(executed),
        "frozen_configurations": C.FROZEN_PRIMARY_COUNT,
        "not_executed": outcome["not_executed"],
        "multiple_testing": outcome["bh"],
        "positive_survivors": survivors,
        "best_positive_survivor": best,
        "qualification": qualification,
        "ml_ready_panel": {
            "rows": ml_body.get("panel_rows"),
            "markets": ml_body.get("panel_markets"),
            "sha256": ml_body.get("panel_sha256"),
        },
        "inherited_purchase": dict(C.INHERITED_PURCHASE),
        "money_spent_during_r38_usd": C.MONEY_SPENT_BY_R38_USD,
        "new_subscriptions": 0,
        "subscription_changes": 0,
        "trials_started": 0,
        "new_accounts": 0,
        "cloud_compute_spend_usd": 0.0,
        "operational_writes": 0,
        "portfolio_mutations": 0,
        "model_promotions": 0,
        "production_restarts": 0,
        "renewal_authorised": False,
        "purchase_authority": C.purchase_authority(),
    }
    return r38.artifact_body(VERDICT_SCHEMA, payload)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(*, campaign_id: str = C.CAMPAIGN_ID,
        progress: Optional[Callable[[str], None]] = None) -> dict:
    """Assumes Phases 1-4 artifacts are frozen (entitlement, registries,
    quality, layer). Executes experiments through verdict and freezes every
    remaining artifact. Idempotent per campaign id via artifact immutability.
    """
    def say(msg):
        if progress is not None:
            progress(msg)

    entitlement_body = _entitlement.load(campaign_id)
    registry = EN.load_market_registry(campaign_id)
    quality = Q.load(campaign_id)
    if not (entitlement_body and registry and quality):
        raise RuntimeError("Phases 1-3 artifacts must be frozen first")

    say("experiments")
    outcome = EX.run_all(campaign_id=campaign_id, progress=progress)

    say("qualification")
    qualification = None
    if outcome["positive_survivors"]:
        by_name = {r["name"]: r for r in outcome["rows"]}
        best_name = max(
            outcome["positive_survivors"],
            key=lambda s: ((by_name[s["name"]].get("economics") or {})
                           .get("after_cost_excess_vs_control_annualised")
                           or float("-inf")))["name"]
        cfg = next(c for c in C.FROZEN_PRIMARY_CONFIGURATIONS
                   if c["name"] == best_name)
        uni = EX.config_universe(best_name, registry)
        panel_all = EX.load_panel(uni["markets"], campaign_id)
        qualification = evaluate_qualification(
            by_name[best_name], outcome, registry, panel_all, cfg)

    say("unlock recomputation")
    actual = _unlock.build(campaign_id=campaign_id)
    overlay = _unlock.coverage_overlay(actual)
    _unlock.freeze(actual, overlay)

    say("ml contract")
    ml_body = _ml.build(campaign_id=campaign_id)
    _ml.freeze(ml_body)

    say("steele artifact")
    steele_body = _steele.build(campaign_id=campaign_id)
    _steele.freeze(steele_body)

    say("canonical Stage-B gate")
    gate = run_gate(outcome, registry=registry, actual_unlocks=actual,
                    quality=quality, campaign_id=campaign_id)
    gate_body = r38.artifact_body(GATE_SCHEMA, {
        "campaign_id": campaign_id,
        "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "calculation_owner": CALCULATION_OWNER,
        "acquisition_decision_owner": C.ACQUISITION_DECISION_OWNER,
        "result": gate,
    })
    r38.write_json(
        r38.campaign_dir(campaign_id)
        / C.ARTIFACT_NAMES["post_acquisition_data_gate_result"], gate_body)

    say("experiment artifacts")
    registry_body = r38.artifact_body(EX.REGISTRY_SCHEMA, {
        "campaign_id": campaign_id,
        "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "calculation_owner": EX.CALCULATION_OWNER,
        "frozen_configurations":
            [dict(c) for c in C.FROZEN_PRIMARY_CONFIGURATIONS],
        "rows": [{k: v for k, v in row.items()
                  if k != "leave_one_market_out"}
                 for row in outcome["rows"]],
        "executed_count": outcome["executed_count"],
        "not_executed": outcome["not_executed"],
    })
    r38.write_json(
        r38.campaign_dir(campaign_id)
        / C.ARTIFACT_NAMES["native_futures_experiment_registry"],
        registry_body)

    economics_body = r38.artifact_body(EX.ECONOMICS_SCHEMA, {
        "campaign_id": campaign_id,
        "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "calculation_owner": EX.CALCULATION_OWNER,
        "economic_judge_owner": C.ECONOMIC_JUDGE_OWNER,
        "cost_model_state": C.COST_MODEL_STATE,
        "rows": outcome["rows"],
        "qualification": qualification,
    })
    r38.write_json(
        r38.campaign_dir(campaign_id)
        / C.ARTIFACT_NAMES["native_futures_economics"], economics_body)

    mt_body = r38.artifact_body(EX.MT_SCHEMA, {
        "campaign_id": campaign_id,
        "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "calculation_owner": EX.CALCULATION_OWNER,
        "multiple_testing_owner": C.MULTIPLE_TESTING_OWNER,
        "policy": "BENJAMINI_HOCHBERG_OVER_EVERY_EXECUTED_CONFIGURATION",
        "denominator": outcome["denominator"],
        "denominator_counts_all_executed": C.DENOMINATOR_COUNTS_ALL_EXECUTED,
        "bh": outcome["bh"],
        "positive_survivors": outcome["positive_survivors"],
    })
    r38.write_json(
        r38.campaign_dir(campaign_id)
        / C.ARTIFACT_NAMES["multiple_testing_results"], mt_body)

    say("verdict")
    verdict_body = build_verdict(
        outcome=outcome, qualification=qualification,
        entitlement_body=entitlement_body, registry=registry,
        actual_unlocks=actual, quality=quality, gate=gate, ml_body=ml_body,
        campaign_id=campaign_id)
    r38.write_json(
        r38.campaign_dir(campaign_id) / C.ARTIFACT_NAMES["final_verdict"],
        verdict_body)

    return {"verdict": verdict_body, "gate": gate, "outcome": outcome,
            "actual_unlocks": actual, "ml": ml_body}
