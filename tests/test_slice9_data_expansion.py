"""Slice 9 (Phase 29J) — Data Expansion / Purchase-Gate tests (Milestone 5).

Deterministic and hermetic: the pure kernel is driven by explicit dataset-evaluation
input-contract dicts; the api catalog/composition/persistence/read owner is driven by
injected catalog fixtures and temporary artifact roots; the endpoints are exercised through
a TestClient / monkeypatched loader. NO paid provider, DB, ledger, real cycle or real Slice-9
production artifact is touched (fixtures + tmp dirs only).

Covers the required matrix:
  A. PIT / HISTORY (1-9)              B. COVERAGE / SAMPLE (10-14)
  C. INCREMENTAL VALUE (15-24)        D. COST / LICENSING (25-31)
  E. GATE (32-40)                     F. PERSISTENCE / API (41-52)
  G. UI (53-59)                       H. ARCHITECTURE (60-67)   + bounded performance.
"""
from __future__ import annotations

import importlib
import json
from pathlib import Path

from paper_trader.engine import data_expansion_gate as K
from paper_trader.api import data_expansion as A

ROOT = Path(__file__).resolve().parent.parent
UI = ROOT / "api" / "ui" / "index.html"
UI_TEXT = UI.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Deterministic input-contract builder (strong healthy baseline; no RNG).
# --------------------------------------------------------------------------- #
_STRONG_DS = {
    "pit_guarantee": "EFFECTIVE_DATED", "publication_timestamps": True, "history_years": 20,
    "inactive_delisted_support": True, "survivorship_bias_risk": "LOW",
    "revision_history_support": True, "restatement_backfill_behavior": "PIT_PRESERVED",
    "universe_size": 1500, "universe_coverage_ratio": 0.9,
    "identifier_quality": "STRONG", "identifier_mapping_available": True,
    "update_frequency": "DAILY", "operational_reliability": "HIGH",
    "implementation_complexity": "LOW",
    "licensing": {"research_use_allowed": True, "prohibited": False,
                  "redistribution_allowed": True, "internal_commercial_use_allowed": True,
                  "derived_data_allowed": True, "model_training_allowed": True},
    "cost": {"annual_usd": 6000.0, "cost_known": True},
    "existing_entitlement_state": "NONE", "acquisition_state": "NOT_ACQUIRED",
    "technical_integration_state": "NOT_INTEGRATED",
}
_STRONG_REQ = {
    "requires_point_in_time": True, "requires_full_universe_integrity": True,
    "min_history_years": 10, "min_universe_size": 500, "min_universe_coverage": 0.6,
    "min_effective_sample": 24,
}
_STRONG_EV = {
    "available": True, "out_of_sample": True, "effective_sample": 48,
    "rank_ic_lift": 0.02, "decile_spread_lift_pp": 1.0, "challenger_excess_lift_pp": 2.0,
    "cost_adjusted_lift_pp": 1.5, "turnover_delta": 0.05, "regime_robust": True,
    "sector_robust": True, "max_abs_correlation_with_existing": 0.2,
}


def _ic(ds=None, req=None, ev=None, **top):
    base = {
        "dataset_id": "ds1", "provider": "prov", "data_category": "fundamentals",
        "dataset": dict(_STRONG_DS, **(ds or {})),
        "research_requirements": dict(_STRONG_REQ, **(req or {})),
        "evidence": dict(_STRONG_EV, **(ev or {})),
    }
    base.update(top)
    return base


def _eval(**kw):
    return K.evaluate_dataset(input_contract=_ic(**kw))


def _rec(**kw):
    return _eval(**kw)["recommendation_state"]


def _dims(result):
    return {d["dimension"]: d for d in result["dimensions"]}


def _gap_codes(result):
    return {g["code"] for g in result["gaps"]}


# =========================================================================== #
# A. PIT / HISTORY (1-9)
# =========================================================================== #
def test_01_valid_pit_timestamps():
    d = _dims(_eval())["point_in_time_integrity"]
    assert d["state"] == K.D_PASS


def test_02_missing_pit_metadata():
    r = _eval(ds={"pit_guarantee": None})
    assert r["recommendation_state"] == K.REC_INSUFFICIENT
    assert "PIT_METADATA_MISSING" in r["evidence_blockers"]
    assert _dims(r)["point_in_time_integrity"]["state"] == K.D_UNKNOWN


def test_03_future_leakage_rejected():
    r = _eval(ds={"contains_future_leakage": True})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "FUTURE_LEAKAGE" in r["disqualifying_blockers"]


def test_04_survivorship_only_for_integrity_research_rejected():
    r = _eval(ds={"inactive_delisted_support": False,
                  "survivorship_bias_risk": "SURVIVORSHIP_ONLY"})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "SURVIVORSHIP_ONLY_HISTORY" in r["disqualifying_blockers"]


def test_05_inactive_delisted_coverage():
    assert _dims(_eval())["inactive_delisted_coverage"]["state"] == K.D_PASS
    # When integrity is NOT required, missing delisted coverage is a soft WATCH, not fatal.
    r = _eval(ds={"inactive_delisted_support": False, "survivorship_bias_risk": "MEDIUM"},
              req={"requires_full_universe_integrity": False})
    assert _dims(r)["inactive_delisted_coverage"]["state"] == K.D_WATCH
    assert "SURVIVORSHIP_ONLY_HISTORY" not in r["disqualifying_blockers"]


def test_06_insufficient_history():
    r = _eval(ds={"history_years": 3})
    assert _dims(r)["historical_depth"]["state"] == K.D_FAIL
    assert r["recommendation_state"] == K.REC_REJECT
    assert "INSUFFICIENT_HISTORY" in r["disqualifying_blockers"]


def test_07_sufficient_history():
    assert _dims(_eval())["historical_depth"]["state"] == K.D_PASS


def test_08_historical_revisions_pass():
    r = _eval(ds={"revision_history_support": True, "estimates_are_current_consensus": False},
              req={"requires_historical_revisions": True}, data_category="analyst_revisions")
    assert _dims(r)["revision_history"]["state"] == K.D_PASS


def test_09_current_only_estimates_rejected_for_revision_research():
    r = _eval(ds={"revision_history_support": False, "estimates_are_current_consensus": True},
              req={"requires_historical_revisions": True}, data_category="analyst_revisions")
    assert r["recommendation_state"] == K.REC_REJECT
    assert "CURRENT_ONLY_ESTIMATES_FOR_REVISION_RESEARCH" in r["disqualifying_blockers"]


# =========================================================================== #
# B. COVERAGE / SAMPLE (10-14)
# =========================================================================== #
def test_10_universe_breadth():
    assert _dims(_eval())["universe_breadth"]["state"] == K.D_PASS
    r = _eval(ds={"universe_size": 100, "universe_coverage_ratio": 0.1})
    assert _dims(r)["universe_breadth"]["state"] == K.D_FAIL
    assert r["recommendation_state"] == K.REC_REJECT
    assert "INSUFFICIENT_UNIVERSE_BREADTH" in r["disqualifying_blockers"]


def test_11_effective_sample():
    assert _dims(_eval())["effective_sample"]["state"] == K.D_PASS
    r = _eval(ev={"effective_sample": 10})
    assert _dims(r)["effective_sample"]["state"] == K.D_FAIL
    assert r["recommendation_state"] == K.REC_INSUFFICIENT
    assert "RESEARCH_SAMPLE_TOO_SMALL" in r["state_reason_codes"]


def test_12_missing_identifiers():
    r = _eval(ds={"identifier_mapping_available": False})
    assert r["recommendation_state"] == K.REC_REJECT
    assert "NO_RELIABLE_IDENTIFIER_MAPPING" in r["disqualifying_blockers"]


def test_13_duplicate_identifiers():
    r = _eval(ds={"duplicate_identifiers": True})
    assert "DUPLICATE_IDENTIFIERS" in _gap_codes(r)
    assert _dims(r)["identifier_quality"]["state"] == K.D_WATCH


def test_14_coverage_deterioration():
    r = _eval(ds={"coverage_deterioration": True})
    assert "COVERAGE_DETERIORATION" in _gap_codes(r)
    assert _dims(r)["universe_breadth"]["state"] == K.D_WATCH


# =========================================================================== #
# C. INCREMENTAL VALUE (15-24)
# =========================================================================== #
_NO_LIFT = {"rank_ic_lift": 0.0, "decile_spread_lift_pp": 0.0, "challenger_excess_lift_pp": 0.0}


def test_15_redundant_dataset_rejected():
    r = _eval(ev=dict(_NO_LIFT, max_abs_correlation_with_existing=0.95))
    assert r["recommendation_state"] == K.REC_REJECT
    assert "REDUNDANT_NO_LIFT" in r["disqualifying_blockers"]


def test_16_low_correlation_no_alpha_lift_is_research_only():
    r = _eval(ev=dict(_NO_LIFT, max_abs_correlation_with_existing=0.2))
    assert r["recommendation_state"] == K.REC_RESEARCH_ONLY


def test_17_rank_ic_lift_is_material():
    r = _eval(ev={"rank_ic_lift": 0.02, "decile_spread_lift_pp": 0.0,
                  "challenger_excess_lift_pp": 0.0})
    assert r["measured_lift"]["material_lift"] is True
    assert r["recommendation_state"] == K.REC_PURCHASE


def test_18_decile_spread_lift_is_material():
    r = _eval(ev={"rank_ic_lift": 0.0, "decile_spread_lift_pp": 1.0,
                  "challenger_excess_lift_pp": 0.0})
    assert r["recommendation_state"] == K.REC_PURCHASE


def test_19_challenger_lift_is_material():
    r = _eval(ev={"rank_ic_lift": 0.0, "decile_spread_lift_pp": 0.0,
                  "challenger_excess_lift_pp": 2.0})
    assert r["recommendation_state"] == K.REC_PURCHASE


def test_20_turnover_deterioration_caps_to_candidate():
    r = _eval(ev={"turnover_delta": 0.5})
    assert r["recommendation_state"] == K.REC_CANDIDATE
    assert "TURNOVER_DETERIORATION" in _gap_codes(r)


def test_21_cost_adjusted_lift():
    # positive cost-adjusted lift is part of the robust PURCHASE baseline …
    assert _rec() == K.REC_PURCHASE
    # … a negative cost-adjusted lift is not decision-grade -> CANDIDATE.
    r = _eval(ev={"cost_adjusted_lift_pp": -1.0})
    assert r["recommendation_state"] == K.REC_CANDIDATE


def test_22_regime_robustness_required_for_purchase():
    r = _eval(ev={"regime_robust": False})
    assert r["recommendation_state"] == K.REC_CANDIDATE


def test_23_sector_robustness_required_for_purchase():
    r = _eval(ev={"sector_robust": False})
    assert r["recommendation_state"] == K.REC_CANDIDATE


def test_24_out_of_sample_evidence_required():
    r = _eval(ev={"out_of_sample": False, "in_sample_only": True})
    assert r["recommendation_state"] == K.REC_INSUFFICIENT
    assert "OUT_OF_SAMPLE_EVIDENCE_REQUIRED" in r["state_reason_codes"]


# =========================================================================== #
# D. COST / LICENSING (25-31)
# =========================================================================== #
def test_25_known_annual_cost():
    assert _dims(_eval())["acquisition_cost"]["state"] == K.D_PASS


def test_26_unknown_cost_is_explicit_gap():
    r = _eval(ds={"cost": {}})
    assert _dims(r)["acquisition_cost"]["state"] == K.D_UNKNOWN
    assert "UNKNOWN_COST" in _gap_codes(r)
    assert r["recommendation_state"] == K.REC_CANDIDATE


def test_27_acceptable_license():
    assert _dims(_eval())["licensing"]["state"] == K.D_PASS


def test_28_unknown_license_blocks_purchase():
    r = _eval(ds={"licensing": {}})
    assert "UNKNOWN_LICENSE" in r["purchase_blockers"]
    assert r["recommendation_state"] != K.REC_PURCHASE
    assert r["recommendation_state"] == K.REC_CANDIDATE


def test_29_redistribution_restriction_visible():
    r = _eval(ds={"licensing": {"research_use_allowed": True, "prohibited": False,
                                "redistribution_allowed": False}})
    assert "REDISTRIBUTION_RESTRICTED" in _gap_codes(r)
    assert r["recommendation_state"] == K.REC_PURCHASE  # research use fine -> not blocked


def test_30_commercial_use_restriction_visible():
    r = _eval(ds={"licensing": {"research_use_allowed": True, "prohibited": False,
                                "internal_commercial_use_allowed": False}})
    assert "COMMERCIAL_USE_RESTRICTED" in _gap_codes(r)


def test_31_derived_data_restriction_visible():
    r = _eval(ds={"licensing": {"research_use_allowed": True, "prohibited": False,
                                "derived_data_allowed": False}})
    assert "DERIVED_DATA_RESTRICTED" in _gap_codes(r)
    # a prohibited licence, by contrast, is disqualifying.
    r2 = _eval(ds={"licensing": {"prohibited": True}})
    assert r2["recommendation_state"] == K.REC_REJECT
    assert "PROHIBITED_LICENSE" in r2["disqualifying_blockers"]


# =========================================================================== #
# E. GATE (32-40)
# =========================================================================== #
def test_32_reject():
    assert _rec(ds={"contains_future_leakage": True}) == K.REC_REJECT


def test_33_insufficient_evidence():
    assert _rec(ev={"available": False}) == K.REC_INSUFFICIENT


def test_34_research_only():
    assert _rec(ev=dict(_NO_LIFT, max_abs_correlation_with_existing=0.2)) == K.REC_RESEARCH_ONLY


def test_35_candidate():
    assert _rec(ds={"cost": {}}) == K.REC_CANDIDATE


def test_36_purchase_recommended():
    r = _eval()
    assert r["recommendation_state"] == K.REC_PURCHASE
    assert r["recommendation"]["manual_approval_required"] is True
    assert "MANUAL APPROVAL REQUIRED" in r["recommendation"]["headline"]


def test_37_integration_recommended():
    r = _eval(ds={"existing_entitlement_state": "OWNED",
                  "technical_integration_state": "NOT_INTEGRATED"})
    assert r["recommendation_state"] == K.REC_INTEGRATION
    assert r["recommendation"]["manual_approval_required"] is True


def test_38_deterministic_result():
    a = _eval()
    b = _eval()
    assert a["evaluation_hash"] == b["evaluation_hash"]


def test_39_reason_codes():
    r = _eval()
    assert r["recommendation"]["reason_codes"]
    assert r["state_reason_codes"]


def test_40_blockers_structured():
    r = _eval(ds={"contains_future_leakage": True})
    assert r["blockers"]
    b = r["blockers"][0]
    assert set(("code", "tier", "dimension", "detail")).issubset(b.keys())
    assert any(x["tier"] == K.TIER_DISQUALIFYING for x in r["blockers"])


def test_40b_recommendation_vocabulary_frozen():
    assert K.RECOMMENDATION_VOCAB == (
        "REJECT", "INSUFFICIENT_EVIDENCE", "RESEARCH_ONLY", "CANDIDATE",
        "PURCHASE_RECOMMENDED", "INTEGRATION_RECOMMENDED")


def test_40c_no_score_fabricated_when_data_absent():
    # A dimension with no metadata must be UNKNOWN, never a fabricated PASS/FAIL score.
    r = _eval(ds={"operational_reliability": None, "implementation_complexity": None})
    assert _dims(r)["operational_reliability"]["state"] == K.D_UNKNOWN
    assert _dims(r)["implementation_complexity"]["state"] == K.D_UNKNOWN


# =========================================================================== #
# F. PERSISTENCE / API (41-52)
# =========================================================================== #
def _strong_overrides():
    return {"dataset_override": dict(_STRONG_DS), "evidence_override": dict(_STRONG_EV)}


def test_41_not_run_before_evaluation(tmp_path):
    read = A.load_data_expansion(data_expansion_dir=str(tmp_path))
    assert read["state"] == A.STATE_OK
    assert read["evaluated_count"] == 0
    assert all(d["evaluation"]["state"] == A.STATE_NOT_RUN for d in read["datasets"])


def test_42_immutable_artifact(tmp_path):
    out = A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                            data_expansion_dir=str(tmp_path), **_strong_overrides())
    assert out["persistence"]["status"] == "CREATED"
    eid = out["persistence"]["evaluation_id"]
    art = json.loads((tmp_path / "artifacts" / (eid + ".json")).read_text(encoding="utf-8"))
    assert art["evaluation"]["evaluation_hash"] == out["evaluation"]["evaluation_hash"]
    assert out["evaluation"]["evaluation_hash"][:12] in eid  # id embeds the hash (immutable)


def test_43_atomic_write_index_and_artifacts(tmp_path):
    A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                      data_expansion_dir=str(tmp_path), **_strong_overrides())
    idx = json.loads((tmp_path / "index.json").read_text(encoding="utf-8"))
    assert "historical_pit_fundamentals_vendor" in idx
    assert (tmp_path / "artifacts").is_dir()


def test_44_idempotent_reuse(tmp_path):
    a = A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                          data_expansion_dir=str(tmp_path), **_strong_overrides())
    b = A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                          data_expansion_dir=str(tmp_path), **_strong_overrides())
    assert a["persistence"]["status"] == "CREATED"
    assert b["persistence"]["status"] == "REUSED_EXISTING"
    assert a["evaluation"]["evaluation_hash"] == b["evaluation"]["evaluation_hash"]


def test_45_changed_evidence_supersedes(tmp_path):
    A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                      data_expansion_dir=str(tmp_path), **_strong_overrides())
    weak = dict(_STRONG_EV, regime_robust=False)
    out = A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                            dataset_override=dict(_STRONG_DS), evidence_override=weak,
                            data_expansion_dir=str(tmp_path))
    assert out["persistence"]["status"] == "SUPERSEDED_PRIOR"
    assert out["evaluation"]["recommendation_state"] == K.REC_CANDIDATE
    # both immutable artifacts remain on disk.
    assert len(list((tmp_path / "artifacts").glob("*.json"))) == 2


def test_46_get_catalog(tmp_path):
    read = A.load_data_expansion(data_expansion_dir=str(tmp_path))
    assert read["catalog_size"] >= 1
    assert isinstance(read["datasets"], list)
    assert read["recommendation_vocabulary"] == list(K.RECOMMENDATION_VOCAB)
    assert read["safety"]["research_only"] is True


def test_47_get_dataset_detail(tmp_path):
    A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                      data_expansion_dir=str(tmp_path), **_strong_overrides())
    det = A.load_data_expansion_detail("historical_pit_fundamentals_vendor",
                                       data_expansion_dir=str(tmp_path))
    assert det["catalog_entry"]["dataset_id"] == "historical_pit_fundamentals_vendor"
    assert det["evaluation"] is not None
    assert det["evaluation_view"]["recommendation"]["manual_approval_required"] is True
    nf = A.load_data_expansion_detail("nope", data_expansion_dir=str(tmp_path))
    assert nf["state"] == A.STATE_NOT_FOUND


def test_48_endpoint_auth_and_get_only():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    from paper_trader.config import get_settings
    c = TestClient(app, raise_server_exceptions=False)
    key = get_settings().service_api_key
    assert c.get("/v1/research/data-expansion").status_code in (401, 403)
    assert c.post("/v1/research/data-expansion", headers={"X-API-Key": key}).status_code == 405


def test_49_no_post_purchase_route():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    from paper_trader.config import get_settings
    c = TestClient(app, raise_server_exceptions=False)
    key = get_settings().service_api_key
    for path in ("/v1/research/data-expansion/purchase",
                 "/v1/research/data-expansion/subscribe",
                 "/v1/research/data-expansion/activate-provider",
                 "/v1/research/data-expansion/integrate",
                 "/v1/research/data-expansion/enable-paid-data"):
        assert c.post(path, headers={"X-API-Key": key}).status_code in (404, 405)


def test_50_no_provider_activation_safety():
    r = K.evaluate_dataset(input_contract=_ic())
    s = r["safety"]
    assert s["activated_provider"] is False and s["subscribed_provider"] is False
    assert s["purchased_dataset"] is False and s["integrated_dataset"] is False


def test_51_endpoint_response_schema(monkeypatch):
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    from paper_trader.config import get_settings
    canned = A.load_data_expansion(data_expansion_dir="___nope___")
    monkeypatch.setattr("paper_trader.api.app._data_expansion.load_data_expansion",
                        lambda: canned)
    c = TestClient(app, raise_server_exceptions=False)
    resp = c.get("/v1/research/data-expansion",
                 headers={"X-API-Key": get_settings().service_api_key})
    assert resp.status_code == 200
    body = resp.json()
    for k in ("schema_version", "state", "datasets", "recommendation_vocabulary",
              "cadence", "safety", "provenance", "policy"):
        assert k in body, k


def test_52_no_external_api_or_mutation_in_read_path(monkeypatch, tmp_path):
    # The GET read path never runs the gate and never calls a provider/paid API.
    def _boom(*a, **k):
        raise AssertionError("read path must not evaluate the gate")
    monkeypatch.setattr(K, "evaluate_dataset", _boom)
    read = A.load_data_expansion(data_expansion_dir=str(tmp_path))
    assert read["safety"]["called_paid_provider"] is False
    assert read["safety"]["wrote_to_database"] is False and read["safety"]["wrote_to_ledger"] is False
    # a pure read created no artifact / index in a fresh root.
    assert not (tmp_path / "index.json").exists()
    assert not (tmp_path / "artifacts").exists()


# =========================================================================== #
# G. UI (53-59)
# =========================================================================== #
def test_53_nav_and_panel_present():
    assert 'data-rasub="data-expansion"' in UI_TEXT
    assert 'id="dex-panel"' in UI_TEXT


def test_54_one_canonical_loader():
    assert UI_TEXT.count("function loadDataExpansion") == 1


def test_55_no_js_gate_math_in_region():
    start = UI_TEXT.find("function loadDataExpansion")
    end = UI_TEXT.find("window.renderDataExpansion")
    assert start != -1 and end != -1 and end > start
    region = UI_TEXT[start:end]
    for pat in ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.", "compute"):
        assert pat not in region, pat


def test_56_blockers_rendered():
    assert "Blockers:" in UI_TEXT


def test_57_gaps_rendered():
    assert "Gaps:" in UI_TEXT


def test_58_cost_and_licensing_displayed():
    assert "Cost/yr USD" in UI_TEXT and "Licence research-use" in UI_TEXT


def test_59_manual_approval_badges():
    for badge in ("RESEARCH ONLY", "MANUAL PURCHASE APPROVAL", "NO AUTO-PURCHASE",
                  "NO PROVIDER ACTIVATION", "NO PORTFOLIO MUTATION"):
        assert badge in UI_TEXT, badge
    assert "alert(" not in UI_TEXT and "confirm(" not in UI_TEXT


# =========================================================================== #
# H. ARCHITECTURE (60-67)  + bounded performance
# =========================================================================== #
def _audit():
    aud = importlib.import_module("scripts.audit_architecture")
    return aud, aud.check_data_expansion_ownership(aud._iter_source_files())


def test_60_sole_owners():
    _, de = _audit()
    assert de["kernel_present"] and de["owner_present"]
    assert de["landed_modules_missing"] == []
    assert de["second_calculation_owner_modules"] == []
    assert de["second_composition_owner_modules"] == []


def test_61_provider_ownership_not_forked():
    _, de = _audit()
    assert de["missing_reuse"] == []
    assert de["kernel_forks_research_agent"] is False
    assert de["kernel_forks_stage13a"] is False
    assert de["secret_ownership"] == []


def test_62_research_agent_unchanged():
    aud = importlib.import_module("scripts.audit_architecture")
    ra = aud.check_research_agent_ownership(aud._iter_source_files())
    assert ra["kernel_present"] and ra["owner_present"]
    assert ra["landed_modules_missing"] == []
    assert ra["second_calculation_owner_modules"] == []
    assert ra["second_composition_owner_modules"] == []
    assert ra["cadence_enabled"] is False


def test_63_no_purchase_authority():
    _, de = _audit()
    assert de["forbidden_routes_present"] == []
    assert de["forbidden_route_methods_present"] is False
    assert de["owner_forbidden_calls"] == []
    assert de["kernel_forbidden_calls"] == []


def test_64_strict_audit_exit_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    assert aud.main(["--strict", "--json-only"]) == 0


def test_65_inventory_drift_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    drift = aud.check_inventory_drift(aud._iter_source_files())
    assert drift["on_disk_not_in_inventory"] == []
    assert drift["in_inventory_not_on_disk"] == []


def test_66_slice10_remains_future():
    _, de = _audit()
    assert de["slice10_present_modules"] == []
    assert not (ROOT / "api" / "intraday_platform.py").exists()


def test_67_cadence_disabled():
    _, de = _audit()
    assert de["cadence_disabled"] is True
    assert de["drc_daily_job_present"] == []
    assert de["cadence_enabled"] is False
    assert A.CADENCE_ENABLED is False
    assert de["route_get_count"] == 1 and de["detail_route_get_count"] == 1
    assert de["ui_loader_count"] == 1 and de["ui_metric_computation"] == []
    assert de["persist_present"] and de["atomic_idempotent_persist_present"]


def test_perf_get_never_recomputes(monkeypatch, tmp_path):
    A.run_and_persist(dataset_id="historical_pit_fundamentals_vendor",
                      data_expansion_dir=str(tmp_path),
                      dataset_override=dict(_STRONG_DS), evidence_override=dict(_STRONG_EV))

    def _boom(*a, **k):
        raise AssertionError("GET must not recompute the evaluation (kernel called).")
    monkeypatch.setattr(K, "evaluate_dataset", _boom)
    read = A.load_data_expansion(data_expansion_dir=str(tmp_path))
    states = {d["dataset_id"]: d["evaluation"]["state"] for d in read["datasets"]}
    assert states["historical_pit_fundamentals_vendor"] == K.REC_PURCHASE
