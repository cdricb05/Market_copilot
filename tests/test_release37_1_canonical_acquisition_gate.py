"""Release 37.1 — canonical acquisition-gate alignment & ML-readiness correction.

Release 37 hit a semantic gap in the ONE canonical Data Expansion gate: it modelled
only the POST-acquisition question ("did the measured evidence earn continued
purchase?"), so asking it a PRE-acquisition question was circular — you need the data
to measure lift, and the gate needs lift to recommend buying the data. Release 37
handled that honestly by leaving the canonical result at INSUFFICIENT_EVIDENCE and
publishing a separate capability judgement, which was defensible and left the estate
with two apparently competing acquisition truths.

37.1 closes the gap inside the existing owner rather than beside it. These tests prove
the four things that could go wrong with that change:

  A-C. the new context can recommend acquisition WITHOUT measured lift, this does not
       weaken the post-acquisition standard, and every existing caller keeps its
       current semantics because the default is unchanged;
  D-G. the acquisition context is still STRICT — point-in-time, survivorship, licence
       and capability failures all still block, and manual approval is still required;
  H-I. Release 37's recommendation now COMES FROM the canonical gate and nothing in
       the release purchases, subscribes or activates anything;
  J.   ML readiness distinguishes installed software from hardware capability.

Deterministic and hermetic: the pure kernel is driven by explicit input-contract dicts
and the release modules are driven by their own frozen tables. No provider, network,
database, ledger or production artifact is touched.
"""
from __future__ import annotations

import sys
from pathlib import Path

from paper_trader.api import data_expansion as A
from paper_trader.engine import data_expansion_gate as K

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from paper_trader.alpha_agent.r37 import campaign as CAMP  # noqa: E402
from paper_trader.alpha_agent.r37 import contract as CON  # noqa: E402
from paper_trader.alpha_agent.r37 import ml_readiness as ML  # noqa: E402
from paper_trader.alpha_agent.r37 import providers as PROV  # noqa: E402
from paper_trader.alpha_agent.r37 import purchase as PUR  # noqa: E402
from paper_trader.alpha_agent.r37 import scoring as SCO  # noqa: E402
from paper_trader.alpha_agent.r37 import unlock as UNL  # noqa: E402


# --------------------------------------------------------------------------- #
# A dataset that is CREDIBLE in every respect but has NO measured evidence:
# exactly the situation before an acquisition, and the one Stage B cannot judge.
# --------------------------------------------------------------------------- #
_CREDIBLE_DS = {
    "pit_guarantee": "EFFECTIVE_DATED", "publication_timestamps": True,
    "history_years": 40, "inactive_delisted_support": True,
    "survivorship_bias_risk": "LOW", "revision_history_support": True,
    "restatement_backfill_behavior": "PIT_PRESERVED",
    "universe_size": 100, "universe_coverage_ratio": 0.9,
    "identifier_quality": "STRONG", "identifier_mapping_available": True,
    "update_frequency": "DAILY", "operational_reliability": "HIGH",
    "implementation_complexity": "LOW",
    "licensing": {"research_use_allowed": True, "prohibited": False},
    "cost": {"annual_usd": 270.0, "cost_known": True},
    "existing_entitlement_state": "NONE", "acquisition_state": "NOT_ACQUIRED",
    "technical_integration_state": "NOT_INTEGRATED",
}
_REQ = {"requires_point_in_time": True, "requires_full_universe_integrity": True,
        "min_history_years": 15, "min_universe_size": 20,
        "min_universe_coverage": 0.5, "min_effective_sample": 24}
#: No trial has run, so there is no evidence. This is the whole point.
_NO_EVIDENCE = {"available": False}
_ACQ_CASE = {
    "capability_unlocked_count": 53,
    "capability_unlocked_ceiling_count": 68,
    "capability_unlocked_units": "RELEASE36_BLOCKED_CELLS",
    "capability_unlocked_detail": "53 blocked commodity/rates/vol/intl cells",
    "expected_incremental_distinctness": "HIGH",
    "expected_distinctness_basis": "no owned dataset serves dated contracts",
    "owned_substitute_tried": True,
    "bounded_evaluation_declared": True,
}


def _ic(ds=None, req=None, ev=None, acq=None, **top):
    base = {
        "dataset_id": "ds1", "provider": "prov", "data_category": "futures",
        "dataset": dict(_CREDIBLE_DS, **(ds or {})),
        "research_requirements": dict(_REQ, **(req or {})),
        "evidence": dict(_NO_EVIDENCE, **(ev or {})),
        "acquisition_case": dict(_ACQ_CASE, **(acq or {})),
    }
    base.update(top)
    return base


def _acq(**kw):
    return K.evaluate_dataset(input_contract=_ic(**kw),
                              decision_context=K.CONTEXT_RESEARCH_ACQUISITION)


def _acq_state(**kw):
    return _acq(**kw)["recommendation_state"]


def _dims(result):
    return {d["dimension"]: d for d in result["dimensions"]}


# =========================================================================== #
# A. RESEARCH_ACQUISITION can recommend WITHOUT measured lift
# =========================================================================== #
def test_a01_acquisition_recommended_without_any_measured_lift():
    r = _acq()
    assert r["recommendation_state"] == K.REC_RESEARCH_ACQUISITION
    # The point: there is no evidence at all, and that is not a defect here.
    assert r["evidence_facts"]["available"] is False
    assert r["measured_lift_required"] is False
    assert not r["disqualifying_blockers"]
    assert not r["evidence_blockers"]
    assert not r["purchase_blockers"]


def test_a02_absent_lift_is_a_by_design_gap_not_a_failure():
    gaps = {g["code"]: g for g in _acq()["gaps"]}
    gap = gaps["MEASURED_LIFT_NOT_REQUIRED_BEFORE_ACQUISITION"]
    assert gap["by_design"] is True
    assert gap["dimension"] == "measured_research_lift"


def test_a03_acquisition_state_is_not_the_purchase_state():
    # Collapsing the two would let a pre-research judgement read as post-research
    # proof, which is the one misreading this whole correction exists to prevent.
    assert K.REC_RESEARCH_ACQUISITION == "RESEARCH_ACQUISITION_RECOMMENDED"
    assert K.REC_RESEARCH_ACQUISITION != K.REC_PURCHASE
    assert K.REC_RESEARCH_ACQUISITION not in K.RECOMMENDATION_VOCAB
    assert K.REC_PURCHASE not in K.ACQUISITION_RECOMMENDATION_VOCAB


def test_a04_acquisition_recommendation_is_not_alpha_evidence():
    rec = _acq()["recommendation"]
    assert rec["is_research_acquisition_recommendation"] is True
    assert rec["is_alpha_evidence"] is False
    assert rec["is_integration_approval"] is False
    assert rec["is_purchase_recommendation"] is False
    assert K.ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE is False
    assert K.ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL is False


def test_a05_two_extra_dimensions_only_in_the_acquisition_context():
    acq = _acq()
    post = K.evaluate_dataset(input_contract=_ic())
    assert set(acq["dimension_summary"]) - set(post["dimension_summary"]) == {
        K.ACQ_CAPABILITY, K.ACQ_DISTINCTNESS}
    assert len(post["dimensions"]) == len(K.DIMENSIONS) == 16
    assert len(acq["dimensions"]) == len(K.ACQUISITION_DIMENSIONS) == 18


def test_a06_already_entitled_needs_no_acquisition_decision():
    r = _acq(ds={"existing_entitlement_state": "OWNED"})
    assert r["recommendation_state"] == K.REC_ALREADY_ENTITLED


# =========================================================================== #
# B. This does NOT weaken POST_ACQUISITION_VALUE
# =========================================================================== #
def test_b01_post_acquisition_still_demands_evidence_on_the_same_dataset():
    # Same credible dataset, same missing evidence: Stage B still refuses.
    assert K.evaluate_dataset(
        input_contract=_ic())["recommendation_state"] == K.REC_INSUFFICIENT


def test_b02_post_acquisition_still_refuses_in_sample_only_evidence():
    ev = {"available": True, "out_of_sample": False, "effective_sample": 48,
          "rank_ic_lift": 0.9, "decile_spread_lift_pp": 9.0,
          "challenger_excess_lift_pp": 9.0, "cost_adjusted_lift_pp": 9.0,
          "max_abs_correlation_with_existing": 0.1}
    assert K.evaluate_dataset(
        input_contract=_ic(ev=ev))["recommendation_state"] == K.REC_INSUFFICIENT


def test_b03_post_acquisition_still_refuses_a_sample_that_is_too_small():
    ev = {"available": True, "out_of_sample": True, "effective_sample": 3,
          "rank_ic_lift": 0.9, "cost_adjusted_lift_pp": 9.0,
          "regime_robust": True, "sector_robust": True,
          "max_abs_correlation_with_existing": 0.1}
    r = K.evaluate_dataset(input_contract=_ic(ev=ev))
    assert r["recommendation_state"] == K.REC_INSUFFICIENT
    assert "RESEARCH_SAMPLE_TOO_SMALL" in r["state_reason_codes"]


def test_b04_post_acquisition_still_reaches_purchase_on_robust_measured_lift():
    ev = {"available": True, "out_of_sample": True, "effective_sample": 48,
          "rank_ic_lift": 0.02, "decile_spread_lift_pp": 1.0,
          "challenger_excess_lift_pp": 2.0, "cost_adjusted_lift_pp": 1.5,
          "turnover_delta": 0.05, "regime_robust": True, "sector_robust": True,
          "max_abs_correlation_with_existing": 0.2}
    r = K.evaluate_dataset(input_contract=_ic(ev=ev))
    assert r["recommendation_state"] == K.REC_PURCHASE
    assert r["measured_lift_required"] is True


def test_b05_acquisition_context_never_emits_a_post_acquisition_state():
    for state in (K.REC_PURCHASE, K.REC_INTEGRATION, K.REC_RESEARCH_ONLY):
        assert state not in K.ACQUISITION_RECOMMENDATION_VOCAB


# =========================================================================== #
# C. Existing / default callers retain current semantics
# =========================================================================== #
def test_c01_default_context_is_the_legacy_post_acquisition_one():
    assert K.DEFAULT_DECISION_CONTEXT == K.CONTEXT_POST_ACQUISITION_VALUE
    assert K.LEGACY_DECISION_CONTEXT == K.CONTEXT_POST_ACQUISITION_VALUE


def test_c02_omitting_the_context_is_identical_to_asking_for_stage_b():
    silent = K.evaluate_dataset(input_contract=_ic())
    explicit = K.evaluate_dataset(
        input_contract=_ic(), decision_context=K.CONTEXT_POST_ACQUISITION_VALUE)
    assert silent["evaluation_hash"] == explicit["evaluation_hash"]


def test_c03_frozen_post_acquisition_vocabulary_is_untouched():
    # The historical vocabulary is a published contract; extending it would have
    # changed what every existing reader sees.
    assert K.RECOMMENDATION_VOCAB == (
        "REJECT", "INSUFFICIENT_EVIDENCE", "RESEARCH_ONLY", "CANDIDATE",
        "PURCHASE_RECOMMENDED", "INTEGRATION_RECOMMENDED")
    assert K.POST_ACQUISITION_RECOMMENDATION_VOCAB is K.RECOMMENDATION_VOCAB


def test_c04_an_unknown_context_falls_back_to_the_stricter_default():
    assert K.resolve_decision_context(decision_context="NONSENSE") == \
        K.DEFAULT_DECISION_CONTEXT
    assert K.resolve_decision_context() == K.DEFAULT_DECISION_CONTEXT


def test_c05_the_owner_defaults_to_stage_b_and_threads_stage_a():
    entry = {"dataset_id": "d", "provider": "p", "data_category": "futures",
             "dataset": dict(_CREDIBLE_DS), "research_requirements": dict(_REQ),
             "evidence": dict(_NO_EVIDENCE), "acquisition_case": dict(_ACQ_CASE)}
    assert A.run_evaluation(catalog_entry=entry)["evaluation"][
        "recommendation_state"] == K.REC_INSUFFICIENT
    assert A.run_evaluation(
        catalog_entry=entry,
        decision_context=A.CONTEXT_RESEARCH_ACQUISITION)["evaluation"][
            "recommendation_state"] == K.REC_RESEARCH_ACQUISITION


def test_c06_the_two_contexts_do_not_overwrite_each_other_on_disk(tmp_path):
    entry = {"dataset_id": "d", "provider": "p", "data_category": "futures",
             "dataset": dict(_CREDIBLE_DS), "research_requirements": dict(_REQ),
             "evidence": dict(_NO_EVIDENCE), "acquisition_case": dict(_ACQ_CASE)}
    A.run_and_persist(catalog_entry=entry, data_expansion_dir=str(tmp_path))
    A.run_and_persist(catalog_entry=entry, data_expansion_dir=str(tmp_path),
                      decision_context=A.CONTEXT_RESEARCH_ACQUISITION)
    post = A.load_latest_evaluation(dataset_id="d",
                                    data_expansion_dir=str(tmp_path))
    acq = A.load_latest_evaluation(
        dataset_id="d", data_expansion_dir=str(tmp_path),
        decision_context=A.CONTEXT_RESEARCH_ACQUISITION)
    assert post["evaluation"]["recommendation_state"] == K.REC_INSUFFICIENT
    assert acq["evaluation"]["recommendation_state"] == K.REC_RESEARCH_ACQUISITION
    # The legacy index key keeps its bare shape so no existing artifact is orphaned.
    assert A._index_key("d") == "d"
    assert A._index_key("d", A.CONTEXT_RESEARCH_ACQUISITION) != "d"


def test_c07_legacy_evaluation_id_shape_is_preserved():
    assert A.evaluation_id_for({"dataset_id": "d", "evaluation_hash": "a" * 64,
                                "decision_context": None}).startswith("dxev_d_")
    assert A.evaluation_id_for(
        {"dataset_id": "d", "evaluation_hash": "a" * 64,
         "decision_context": A.CONTEXT_RESEARCH_ACQUISITION}).startswith(
             "dxev_acq_d_")


# =========================================================================== #
# D-F. The acquisition context is STILL STRICT
# =========================================================================== #
def test_d01_missing_point_in_time_integrity_still_blocks_acquisition():
    r = _acq(ds={"pit_guarantee": "CURRENT_ONLY"})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "NO_DEFENSIBLE_PIT_HISTORY" in r["disqualifying_blockers"]


def test_d02_unproven_point_in_time_metadata_still_blocks_acquisition():
    r = _acq(ds={"pit_guarantee": None, "publication_timestamps": None})
    assert r["recommendation_state"] == K.REC_INSUFFICIENT
    assert "PIT_METADATA_MISSING" in r["evidence_blockers"]


def test_d03_future_leakage_still_rejects():
    r = _acq(ds={"contains_future_leakage": True})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "FUTURE_LEAKAGE" in r["disqualifying_blockers"]


def test_e01_survivorship_failure_still_blocks_acquisition():
    r = _acq(ds={"inactive_delisted_support": False,
                 "survivorship_bias_risk": "SURVIVORSHIP_ONLY"})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "SURVIVORSHIP_ONLY_HISTORY" in r["disqualifying_blockers"]


def test_e02_insufficient_history_still_blocks_acquisition():
    r = _acq(ds={"history_years": 2})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "INSUFFICIENT_HISTORY" in r["disqualifying_blockers"]


def test_f01_prohibited_licence_still_blocks_acquisition():
    r = _acq(ds={"licensing": {"prohibited": True, "research_use_allowed": True}})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "PROHIBITED_LICENSE" in r["disqualifying_blockers"]


def test_f02_unclear_licence_caps_below_a_recommendation():
    r = _acq(ds={"licensing": {"research_use_allowed": None}})
    assert r["recommendation_state"] == K.REC_CANDIDATE
    assert "UNKNOWN_LICENSE" in r["purchase_blockers"]


def test_f03_unknown_cost_caps_below_a_recommendation():
    r = _acq(ds={"cost": {"annual_usd": None, "cost_known": False}})
    assert r["recommendation_state"] == K.REC_CANDIDATE
    assert "UNKNOWN_COST" in r["purchase_blockers"]


def test_f04_an_acquisition_that_unlocks_nothing_is_rejected():
    r = _acq(acq={"capability_unlocked_count": 0,
                  "capability_unlocked_ceiling_count": 0})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "NO_CAPABILITY_UNLOCKED" in r["disqualifying_blockers"]


def test_f05_a_ceiling_only_unlock_is_limited_not_disqualified():
    # A dataset that reaches the market only at a weaker implementation level is
    # a CANDIDATE, not the same answer as one that unlocks nothing at all.
    r = _acq(acq={"capability_unlocked_count": 0,
                  "capability_unlocked_ceiling_count": 3})
    assert r["recommendation_state"] == K.REC_CANDIDATE
    assert "CAPABILITY_ONLY_AT_CEILING" in r["purchase_blockers"]


def test_f06_undeclared_capability_is_unproven_not_approved():
    r = _acq(acq={"capability_unlocked_count": None,
                  "capability_unlocked_ceiling_count": None})
    assert r["recommendation_state"] == K.REC_INSUFFICIENT
    assert "CAPABILITY_UNLOCKED_NOT_DECLARED" in r["evidence_blockers"]


def test_f07_data_that_duplicates_owned_data_is_rejected():
    r = _acq(acq={"expected_incremental_distinctness": "NONE"})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "NOT_ECONOMICALLY_DISTINCT_FROM_OWNED_DATA" in \
        r["disqualifying_blockers"]


def test_f08_undeclared_distinctness_caps_below_a_recommendation():
    r = _acq(acq={"expected_incremental_distinctness": "UNKNOWN"})
    assert r["recommendation_state"] == K.REC_CANDIDATE
    assert "EXPECTED_DISTINCTNESS_NOT_DECLARED" in r["purchase_blockers"]


def test_f09_owned_data_must_have_been_tried_first():
    r = _acq(acq={"owned_substitute_tried": False})
    assert r["recommendation_state"] == K.REC_CANDIDATE
    assert "OWNED_SUBSTITUTE_NOT_TRIED" in r["purchase_blockers"]


def test_f10_an_acquisition_needs_an_evaluation_that_can_fail():
    r = _acq(acq={"bounded_evaluation_declared": False})
    assert r["recommendation_state"] == K.REC_CANDIDATE
    assert "NO_BOUNDED_EVALUATION_DECLARED" in r["purchase_blockers"]


def test_f11_the_kernel_never_invents_an_acquisition_case():
    # An entirely absent acquisition case must not silently pass.
    r = K.evaluate_dataset(input_contract=_ic(acquisition_case={}),
                           decision_context=K.CONTEXT_RESEARCH_ACQUISITION)
    assert r["recommendation_state"] == K.REC_INSUFFICIENT


# =========================================================================== #
# G. Manual approval remains mandatory
# =========================================================================== #
def test_g01_manual_approval_is_required_and_nothing_is_automatic():
    r = _acq()
    rec, mr, safety = r["recommendation"], r["manual_review"], r["safety"]
    assert rec["manual_approval_required"] is True
    assert rec["auto_purchase_allowed"] is False
    assert rec["auto_acquisition_allowed"] is False
    assert mr["manual_acquisition_approval_required"] is True
    assert mr["automatic_acquisition_allowed"] is False
    assert mr["automatic_purchase_allowed"] is False
    assert safety["acquired_dataset"] is False
    assert safety["purchased_dataset"] is False
    assert K.ACQUISITION_RECOMMENDATION_REQUIRES_MANUAL_APPROVAL is True


def test_g02_every_acquisition_state_leaves_the_estate_unchanged():
    for state in K.ACQUISITION_RECOMMENDATION_VOCAB:
        block = K._recommendation_block(state, [], [], {}, {},
                                        context=K.CONTEXT_RESEARCH_ACQUISITION)
        assert block["auto_purchase_allowed"] is False
        assert block["auto_acquisition_allowed"] is False
        assert block["manual_approval_required"] is True


# =========================================================================== #
# H. Release 37's recommendation COMES FROM the canonical Stage-A gate
# =========================================================================== #
def _gate_results():
    unlock_map = UNL.build()
    scorecard = SCO.build(unlock_map)
    return unlock_map, scorecard, PUR.build(unlock_map, scorecard,
                                            campaign_id="test_r37_1")


def test_h01_norgate_is_recommended_by_the_canonical_gate_not_by_r37():
    _, _, gate = _gate_results()
    row = [r for r in gate["rows"]
           if r["dataset_id"] == "norgate_futures_package"][0]
    assert row["acquisition"]["decision_context"] == K.CONTEXT_RESEARCH_ACQUISITION
    assert row["acquisition"]["kernel_owner"] == K.CALCULATION_OWNER
    assert row["canonical_acquisition_state"] == K.REC_RESEARCH_ACQUISITION
    # It passes on the merits, with nothing outstanding — not by being hard-coded.
    assert row["acquisition"]["failed_dimensions"] == []
    assert row["acquisition"]["disqualifying_blockers"] == []
    assert row["acquisition"]["purchase_blockers"] == []


def test_h02_the_release_recommends_nothing_the_canonical_gate_refused():
    _, _, gate = _gate_results()
    assert gate["recommended_by_r37_but_refused_by_canonical_gate"] == []
    assert gate["every_row_agrees_with_canonical_gate"] is True
    assert set(gate["recommended"]) <= set(gate["canonical_acquisition_recommended"])
    assert PUR.CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE is True
    assert PUR.R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED is False


def test_h03_the_recommendation_artifact_cites_the_canonical_decision():
    unlock_map, scorecard, gate = _gate_results()
    rec = CAMP.recommendation(unlock_map, scorecard, gate,
                              campaign_id="test_r37_1", created_at="t")
    canonical = rec["canonical_acquisition_gate"]
    assert canonical["owner"] == PUR.ACQUISITION_DECISION_OWNER
    assert canonical["decision_context"] == K.CONTEXT_RESEARCH_ACQUISITION
    assert canonical["is_authoritative"] is True
    assert rec["best"]["canonical_acquisition_decision"]["state"] == \
        K.REC_RESEARCH_ACQUISITION
    assert rec["spend_now_recommendation"]["decided_by"] == \
        PUR.ACQUISITION_DECISION_OWNER
    assert rec["spend_now_recommendation"]["is_alpha_evidence"] is False


def test_h04_the_post_acquisition_verdict_is_still_reported_verbatim():
    # The release does not delete the inconvenient half of the pair.
    _, _, gate = _gate_results()
    assert gate["slice9_purchase_recommendations"] == []
    assert set(gate["slice9_states"].values()) <= set(K.RECOMMENDATION_VOCAB)
    assert PUR.SLICE9_RESULT_MAY_BE_OVERRIDDEN is False


def test_h05_release37_defines_no_acquisition_authority_of_its_own():
    assert CON.R37_DEFINES_ITS_OWN_ACQUISITION_AUTHORITY is False
    assert CON.R37_STATES_ARE_TRIAGE_LABELS is True
    assert CON.ACQUISITION_DECISION_OWNER == "engine.data_expansion_gate"
    assert not (ROOT / "alpha_agent" / "r37" / "purchase_gate.py").exists()


def test_h06_the_unlocked_cells_are_expected_not_measured():
    assert CON.EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS is True
    assert CON.UNLOCK_BECOMES_MEASURED_ONLY_AFTER_ENTITLEMENT_ACTIVATION is True
    unlock_map, scorecard, gate = _gate_results()
    rec = CAMP.recommendation(unlock_map, scorecard, gate,
                              campaign_id="test_r37_1", created_at="t")
    assert rec["best"]["unlocks_are_expected_not_measured"] is True
    case = PUR.acquisition_case(
        {"incremental_distinctness": "HIGH"},
        {"cells_unlocked_full": 53, "cells_unlocked_ceiling": 68})
    assert case["capability_is_expected_not_measured"] is True


def test_h07_the_campaign_was_superseded_with_its_defect_named():
    assert CON.CAMPAIGN_ID == "r37_native_market_data_gate_v5"
    v4 = CON.SUPERSEDED_CAMPAIGNS["r37_native_market_data_gate_v4"]
    assert v4["superseded_reason"] == "SUPERSEDED_TWO_COMPETING_ACQUISITION_TRUTHS"
    assert v4["artifacts_retained"] is True


# =========================================================================== #
# I. Nothing in Release 37 purchases, subscribes or activates anything
# =========================================================================== #
def test_i01_no_release37_module_can_spend_money():
    for flag in ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
                 "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_CHANGE_SUBSCRIPTION_TIER",
                 "MAY_ACCEPT_LICENCE_AGREEMENT", "MAY_SUBMIT_PAYMENT_DETAILS",
                 "MAY_PURCHASE_CLOUD_COMPUTE", "MAY_INSTALL_CUDA",
                 "MAY_DOWNLOAD_MODEL_WEIGHTS"):
        assert getattr(CON, flag) is False


def test_i02_no_gate_state_grants_purchase_authority():
    for state in CON.GATE_STATES:
        assert CON.purchase_authority(state)["purchase_authorised"] is False
        assert CON.purchase_authority(state)["money_spent_usd"] == 0.0
    assert CON.PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE is False
    assert CON.ACQUISITION_RECOMMENDATION_IS_PURCHASE_AUTHORITY is False
    assert CON.ACQUISITION_REQUIRES_MANUAL_OPERATOR_APPROVAL is True


def test_i03_the_release_writes_nothing_into_the_slice9_store():
    _, _, gate = _gate_results()
    assert all(r["slice9"]["persisted_to_slice9_store"] is False
               for r in gate["rows"])
    assert all(r["acquisition"]["persisted_to_slice9_store"] is False
               for r in gate["rows"])
    assert gate["information_gate"]["written_to_r32_root"] is False


def test_i04_no_release37_source_file_calls_a_purchase_or_subscribe_api():
    forbidden = ("stripe", "checkout.session", "card_number", "cvv",
                 "pip install", "conda install", "start_trial(",
                 "create_account(", "subscribe(")
    for path in sorted((ROOT / "alpha_agent" / "r37").glob("*.py")):
        text = path.read_text(encoding="utf-8").lower()
        assert [t for t in forbidden if t in text] == [], path.name


# =========================================================================== #
# J. ML readiness distinguishes installed software from hardware feasibility
# =========================================================================== #
_GTX1650 = {"max_vram_gb": 4.0, "total_ram_gb": 68.58, "logical_cpus": 8,
            "gpu_present": True}
_ONLY_NUMPY = {"present": {"numpy": "2.0.0", "pandas": "2.2.0"},
               "absent": ["torch", "xgboost", "scikit-learn", "scipy"]}


def test_j01_readiness_classes_are_the_five_required_ones():
    assert ML.READINESS_CLASSES == (
        "CURRENTLY_INSTALLED_AND_RUNNABLE",
        "HARDWARE_FEASIBLE_AFTER_SOFTWARE_INSTALL",
        "LOCALLY_POSSIBLE_BUT_IMPRACTICAL",
        "EXTERNAL_GPU_RECOMMENDED",
        "NOT_CURRENTLY_FEASIBLE")
    assert set(ML.READINESS_MEANING) == set(ML.READINESS_CLASSES)


def test_j02_hardware_capability_alone_is_not_runnable_today():
    rows = ML.matrix(_GTX1650, _ONLY_NUMPY)
    boosted = [r for r in rows if r["model"] == "GRADIENT_BOOSTED_TREES"][0]
    # The hardware is fine; xgboost is not installed, so it does not run today.
    assert boosted["feasibility"]["hardware_feasible"] is True
    assert boosted["feasibility"]["runnable_today"] is False
    assert boosted["feasibility"]["readiness"] == ML.READY_AFTER_INSTALL
    assert "xgboost" in boosted["feasibility"]["required_libraries_missing"]


def test_j03_only_families_whose_libraries_exist_are_runnable_today():
    summary = ML.summarise(ML.matrix(_GTX1650, _ONLY_NUMPY))
    assert summary["currently_runnable_count"] < summary["hardware_feasible_count"]
    assert summary["currently_runnable"] == ["REGULARISED_LINEAR"]
    for model in summary["currently_runnable"]:
        row = [r for r in ML.matrix(_GTX1650, _ONLY_NUMPY)
               if r["model"] == model][0]
        assert row["feasibility"]["required_libraries_missing"] == []


def test_j04_a_gpu_too_small_recommends_external_capacity():
    rows = ML.matrix(_GTX1650, _ONLY_NUMPY)
    chronos = [r for r in rows
               if r["model"] == "CHRONOS_CLASS_TIME_SERIES_FOUNDATION"][0]
    assert chronos["feasibility"]["readiness"] == ML.READY_EXTERNAL_GPU
    assert chronos["feasibility"]["hardware_feasible"] is False


def test_j05_a_gpu_exactly_at_the_minimum_is_impractical_not_runnable():
    rows = ML.matrix(_GTX1650, _ONLY_NUMPY)
    tabpfn = [r for r in rows
              if r["model"] == "TABPFN_CLASS_TABULAR_FOUNDATION"][0]
    assert tabpfn["feasibility"]["min_vram_gb"] == 4.0
    assert tabpfn["feasibility"]["vram_headroom_gb"] == 0.0
    assert tabpfn["feasibility"]["readiness"] == ML.READY_IMPRACTICAL


def test_j06_every_family_lands_in_exactly_one_readiness_class():
    rows = ML.matrix(_GTX1650, _ONLY_NUMPY)
    summary = ML.summarise(rows)
    assert sum(summary["readiness_counts"].values()) == len(ML.MODELS) == 13
    assert set(summary["readiness_counts"]) == set(ML.READINESS_CLASSES)


def test_j07_every_model_declares_the_libraries_it_needs():
    for model in ML.MODELS:
        assert model["required_libraries"], model["model"]
        assert model["preferred_libraries"], model["model"]


def test_j08_unmeasured_libraries_never_read_as_installed():
    # If nobody measured the environment, no family may be called runnable today.
    rows = ML.matrix(_GTX1650, None)
    assert all(r["feasibility"]["runnable_today"] is False for r in rows)
    assert all(r["feasibility"]["libraries_measured"] is False for r in rows)


def test_j09_readiness_reporting_installs_nothing():
    rows = ML.matrix(_GTX1650, _ONLY_NUMPY)
    body = ML.matrix_artifact(rows, campaign_id="test_r37_1", created_at="t")
    assert body["installed_anything"] is False
    assert body["summary"]["installs_anything"] is False
    assert ML.TRAINS_A_MODEL is False
    assert CON.ML_TRAINING_CAMPAIGN_IN_SCOPE is False


def test_j10_the_long_list_still_validates_after_the_correction():
    assert PROV.validate()["valid"] is True
