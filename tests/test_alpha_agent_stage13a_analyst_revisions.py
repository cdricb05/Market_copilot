"""Deterministic tests for Stage 13A -- analyst-revision data contract, point-in-time
safety, adapter protocol + local importer, frozen 6-hypothesis registry, synthetic
fixtures + evaluator, adequacy gate, power/confidence model, cost/purchase gate,
vendor comparison, read-only endpoints and operational-ledger immutability.

All tests are pure/offline/deterministic: no provider is contacted, no credential is
read, no network call is made, nothing is promoted, and no operational ledger is
mutated.
"""
from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from paper_trader.alpha_agent import analyst_revisions as AR


# --------------------------------------------------------------------------- #
# Workstream A -- schemas / contract validation / immutable append-only store.
# --------------------------------------------------------------------------- #
def test_five_schemas_present_with_required_fields():
    assert set(AR.SCHEMA_NAMES) == {
        "SECURITY_IDENTITY", "ESTIMATE_REVISION_EVENT", "CONSENSUS_SNAPSHOT",
        "ACTUAL_EVENT", "PROVIDER_CAPABILITY"}
    # every schema declares at least one required field
    for name in AR.SCHEMA_NAMES:
        assert AR.required_fields(name), name


def test_validate_record_flags_missing_required_and_unknown_field():
    rec = {"provider": "p", "security_id": "S", "estimate_type": "EPS"}  # many missing
    problems = AR.validate_record("ESTIMATE_REVISION_EVENT", rec)
    assert any("missing required field" in p for p in problems)
    rec2 = {"provider_security_id": "X", "effective_start": "2000-01-01",
            "status": "ACTIVE", "source_provider": "p", "bogus": 1}
    assert any("unknown field bogus" in p for p in AR.validate_record("SECURITY_IDENTITY", rec2))


def test_validate_record_rejects_credential_field():
    rec = {"provider_security_id": "X", "effective_start": "2000-01-01",
           "status": "ACTIVE", "source_provider": "p", "api_key": "leaked"}
    problems = AR.validate_record("SECURITY_IDENTITY", rec)
    assert any("forbidden credential field" in p for p in problems)


def test_validate_record_controlled_vocabulary():
    rec = {"provider": "p", "provider_event_id": "1", "security_id": "S",
           "estimate_type": "PROFIT", "fiscal_period_type": "QUARTERLY",
           "fiscal_period_end": "2019-12-31", "observation_timestamp": "2019-10-01",
           "provider_effective_timestamp": "2019-10-01", "revised_estimate": 1.0,
           "source_availability_timestamp": "2019-10-02", "ingestion_timestamp": "2019-10-02"}
    assert any("bad estimate_type" in p for p in AR.validate_record("ESTIMATE_REVISION_EVENT", rec))


def test_store_is_append_only_and_revision_is_new_record(tmp_path):
    store = AR.RawObservationStore(tmp_path / "raw")
    base = dict(provider="zacks", provider_event_id="e1", security_id="S1",
                estimate_type="EPS", fiscal_period_type="QUARTERLY",
                fiscal_period_end="2019-12-31", observation_timestamp="2019-10-01",
                provider_effective_timestamp="2019-10-01", revised_estimate=1.10,
                source_availability_timestamp="2019-10-02", ingestion_timestamp="2019-10-02")
    base["record_id"] = AR.compute_record_id(base)
    rid = store.append("ESTIMATE_REVISION_EVENT", base)
    # identical re-append is idempotent
    assert store.append("ESTIMATE_REVISION_EVENT", dict(base)) == rid
    assert len(store.all("ESTIMATE_REVISION_EVENT")) == 1
    # a revision (new value) gets a DIFFERENT record_id and is a NEW record
    rev = dict(base, revised_estimate=1.25)
    rev["record_id"] = AR.compute_record_id(rev)
    assert rev["record_id"] != rid
    store.append("ESTIMATE_REVISION_EVENT", rev)
    assert len(store.all("ESTIMATE_REVISION_EVENT")) == 2


def test_store_refuses_overwrite_of_same_record_id_with_different_content(tmp_path):
    store = AR.RawObservationStore(tmp_path / "raw")
    rec = dict(provider="zacks", provider_event_id="e1", security_id="S1",
               estimate_type="EPS", fiscal_period_type="QUARTERLY",
               fiscal_period_end="2019-12-31", observation_timestamp="2019-10-01",
               provider_effective_timestamp="2019-10-01", revised_estimate=1.10,
               source_availability_timestamp="2019-10-02", ingestion_timestamp="2019-10-02",
               record_id="FIXED")
    store.append("ESTIMATE_REVISION_EVENT", rec)
    with pytest.raises(AR.RevisionImmutabilityError):
        store.append("ESTIMATE_REVISION_EVENT", dict(rec, revised_estimate=9.99))


# --------------------------------------------------------------------------- #
# Workstream B -- point-in-time safety contract.
# --------------------------------------------------------------------------- #
def test_pit_missing_source_availability_timestamp():
    rec = {"observation_timestamp": "2019-10-01", "ingestion_timestamp": "2019-10-02",
           "revised_estimate": 1.0}
    assert AR.PIT_MISSING_SOURCE_AVAILABILITY in AR.pit_validate_event(rec)


def test_pit_observation_after_ingestion_without_late_arrival():
    rec = {"source_availability_timestamp": "2019-10-05",
           "observation_timestamp": "2019-10-05", "ingestion_timestamp": "2019-10-02"}
    assert AR.PIT_OBS_AFTER_INGESTION in AR.pit_validate_event(rec)
    ok = dict(rec, late_arrival=True)
    assert AR.PIT_OBS_AFTER_INGESTION not in AR.pit_validate_event(ok)


def test_pit_fiscal_date_used_as_availability():
    rec = {"source_availability_timestamp": "2019-12-31",
           "observation_timestamp": "2019-12-31", "fiscal_period_end": "2019-12-31",
           "ingestion_timestamp": "2020-01-05"}
    assert AR.PIT_FISCAL_AS_AVAILABILITY in AR.pit_validate_event(rec)


def test_pit_actual_before_announcement():
    rec = {"announcement_timestamp": "2020-01-30",
           "source_availability_timestamp": "2020-01-25"}
    assert AR.PIT_ACTUAL_BEFORE_ANNOUNCE in AR.pit_validate_actual(rec)


def test_pit_reconstructed_snapshot_and_backfill():
    snaps = [{"security_id": "S", "estimate_type": "EPS", "fiscal_period_end": "2019-12-31",
              "observation_timestamp": "2019-%02d-15" % m,
              "source_availability_timestamp": "2019-%02d-15" % m,
              "mean": 1.23, "prior_week_consensus": 1.23, "prior_month_consensus": 1.23}
             for m in range(1, 6)]
    codes = AR.pit_scan_consensus_series(snaps)
    assert AR.PIT_RECONSTRUCTED_SNAPSHOT in codes
    assert AR.PIT_CONSENSUS_BACKFILL in codes


def test_pit_duplicate_issuer_from_ticker_change():
    ids = [{"provider_security_id": "SEC0", "issuer_id": "ISS0"},
           {"provider_security_id": "SEC0", "issuer_id": "ISS0_NEW"}]
    assert AR.PIT_TICKER_DUPLICATE_ISSUER in AR.pit_scan_identities(ids)


def test_pit_inactive_delisted_preservation_required():
    active_only = [{"provider_security_id": "S%d" % i, "issuer_id": "I%d" % i,
                    "status": "ACTIVE"} for i in range(5)]
    scan = AR.pit_scan(identities=active_only, require_inactive_retained=True)
    assert scan["violation_counts"][AR.PIT_INACTIVE_IDENTITY_LOST] == 1
    mixed = active_only + [{"provider_security_id": "S9", "issuer_id": "I9",
                            "status": "DELISTED"}]
    scan2 = AR.pit_scan(identities=mixed, require_inactive_retained=True)
    assert scan2["violation_counts"][AR.PIT_INACTIVE_IDENTITY_LOST] == 0


def test_pit_scan_never_repairs():
    scan = AR.pit_scan(events=[{"observation_timestamp": "x", "ingestion_timestamp": "y"}])
    assert scan["no_silent_repair"] is True


# --------------------------------------------------------------------------- #
# Workstream C -- adapter protocol + local importer.
# --------------------------------------------------------------------------- #
def test_adapter_protocol_shapes():
    for prov in ("intrinio_zacks", "nasdaq_zacks"):
        ad = AR.get_adapter(prov)
        cap = ad.capability()
        assert cap["source_provider"] == prov
        assert cap["underlying_estimate_source"] == "zacks"  # not assumed independent
        assert "ESTIMATE_REVISION_EVENT" in ad.field_mapping()
        assert isinstance(ad.pagination_contract(), dict)
        assert isinstance(ad.trial_limits(), dict)
        assert ad.credential_env  # env-var NAME only
        assert ad.classify_error(403) == "ENTITLEMENT_OR_AUTH"
        assert ad.classify_error(429) == "RATE_LIMITED"


def test_intrinio_trial_history_flagged_one_year():
    ad = AR.get_adapter("intrinio_zacks")
    assert ad.trial_limits()["history_years"] == 1


def test_local_importer_maps_csv_without_network(tmp_path):
    p = tmp_path / "trial.csv"
    p.write_text("id,security,type,fiscal_period,fiscal_period_end_date,updated_date,"
                 "effective_date,previous_estimate,estimate,analyst_count,available_date\n"
                 "1,S1,EPS,QUARTERLY,2019-12-31,2019-10-01,2019-10-01,1.0,1.1,7,2019-10-02\n",
                 encoding="utf-8")
    imp = AR.LocalTrialImporter(AR.get_adapter("intrinio_zacks"))
    out = imp.import_file(p, schema="ESTIMATE_REVISION_EVENT")
    assert out["network_used"] is False
    assert out["records_out"] == 1
    rec = out["records"][0]
    assert rec["security_id"] == "S1" and rec["revised_estimate"] == "1.1"
    assert rec["record_id"].startswith("rev_")


def test_local_importer_json(tmp_path):
    p = tmp_path / "trial.json"
    p.write_text(json.dumps({"data": [
        {"id": "9", "security": "S2", "type": "REVENUE", "fiscal_period": "ANNUAL",
         "fiscal_period_end_date": "2019-12-31", "updated_date": "2019-11-01",
         "effective_date": "2019-11-01", "estimate": 1000.0, "available_date": "2019-11-02"}]}),
        encoding="utf-8")
    imp = AR.LocalTrialImporter(AR.get_adapter("intrinio_zacks"))
    out = imp.import_file(p, schema="ESTIMATE_REVISION_EVENT")
    assert out["records_out"] == 1 and out["records"][0]["estimate_type"] == "REVENUE"


# --------------------------------------------------------------------------- #
# Workstream D -- frozen 6-hypothesis registry.
# --------------------------------------------------------------------------- #
def test_registry_has_exactly_six_hypotheses():
    reg = AR.build_revision_registry()
    assert reg["n_hypotheses"] == 6 and reg["family_size"] == 6
    assert len(reg["hypotheses"]) == 6
    assert reg["selected_before_evidence"] is True
    assert AR.validate_revision_registry(reg) == []


def test_registry_expected_hypothesis_ids_and_directions():
    reg = AR.build_revision_registry()
    ids = [h["id"] for h in reg["hypotheses"]]
    assert ids == [
        "rev_eps_consensus_revision_magnitude",
        "rev_revenue_consensus_revision_magnitude",
        "rev_up_minus_down_breadth",
        "rev_joint_eps_revenue_confirmation",
        "rev_revision_acceleration",
        "rev_analyst_dispersion_change"]
    dirs = {h["id"]: h["expected_direction"] for h in reg["hypotheses"]}
    assert dirs["rev_analyst_dispersion_change"] == -1
    assert dirs["rev_eps_consensus_revision_magnitude"] == 1


def test_registry_version_is_content_addressed_and_stable():
    v1 = AR.build_revision_registry()["registry_version"]
    v2 = AR.build_revision_registry()["registry_version"]
    assert v1 == v2 and len(v1) == 16


def test_frozen_registry_first_write_wins_and_immutable(tmp_path):
    path = tmp_path / "reg.json"
    reg = AR.freeze_revision_registry(path)
    assert path.exists()
    # re-freezing identical content is idempotent
    assert AR.freeze_revision_registry(path)["registry_version"] == reg["registry_version"]
    # tampering then re-freezing DIFFERENT content raises
    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["registry_version"] = "deadbeefdeadbeef"
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(AR.RevisionImmutabilityError):
        AR.freeze_revision_registry(path)


def test_committed_frozen_registry_matches_module():
    committed = AR.default_revision_registry_path()
    assert committed.exists(), "frozen registry artifact must be committed"
    on_disk = json.loads(committed.read_text(encoding="utf-8"))
    assert on_disk["registry_version"] == AR.build_revision_registry()["registry_version"]
    assert AR.validate_revision_registry(on_disk) == []


# --------------------------------------------------------------------------- #
# Workstream E -- synthetic fixtures + evaluator.
# --------------------------------------------------------------------------- #
def test_positive_fixture_detected():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_POSITIVE))
    assert v["verdict"] == AR.EV_PASS and v["passed"] is True
    assert v["promoted"] is False


def test_null_fixture_rejected_weak():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_NULL))
    assert v["verdict"] == AR.EV_REJECT_WEAK and v["passed"] is False


def test_wrong_direction_fixture_rejected():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_WRONG_DIRECTION))
    assert v["verdict"] == AR.EV_REJECT_DIRECTION


def test_cost_destroyed_fixture_rejected():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_COST_DESTROYED))
    assert v["verdict"] == AR.EV_REJECT_COST


def test_leakage_fixture_rejected():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_LEAKAGE))
    assert v["verdict"] == AR.EV_REJECT_LEAKAGE


def test_concentration_fixtures_rejected():
    for k in (AR.FIX_ISSUER_CONCENTRATED, AR.FIX_MONTH_CONCENTRATED):
        assert AR.evaluate_signal(AR.make_fixture(k))["verdict"] == AR.EV_REJECT_CONC


def test_active_only_fixture_rejected_survivorship():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_ACTIVE_ONLY))
    assert v["verdict"] == AR.EV_REJECT_SURVIVORSHIP


def test_duplicate_ticker_fixture_rejected():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_DUP_TICKER))
    assert v["verdict"] == AR.EV_REJECT_DUP_TICKER


def test_reconstructed_snapshot_fixture_rejected():
    v = AR.evaluate_signal(AR.make_fixture(AR.FIX_RECONSTRUCTED))
    assert v["verdict"] == AR.EV_REJECT_RECONSTRUCTED


def test_fixtures_are_deterministic():
    a = AR.make_fixture(AR.FIX_POSITIVE, seed=42)
    b = AR.make_fixture(AR.FIX_POSITIVE, seed=42)
    assert a["cohorts"] == b["cohorts"]


def test_self_test_all_expected_over_two_seeds():
    for seed in (20260804, 111, 999):
        st = AR.run_fixture_self_test(seed=seed)
        assert st["all_expected"] is True, (seed, st["results"])
        assert st["positive_fixture_detected"] is True
        assert st["no_automatic_promotion"] is True


def test_fdr_family_size_is_six():
    st = AR.run_fixture_self_test()
    assert st["fdr_family_size"] == 6
    assert st["fdr_applied_over_exactly_six"] is True


# --------------------------------------------------------------------------- #
# Workstream F -- adequacy gate.
# --------------------------------------------------------------------------- #
def test_adequacy_prior_only_when_no_trial():
    a = AR.assess_adequacy(None)
    assert a["status"] == "TRIAL_NOT_STARTED" and a["measured"] is False
    assert "metrics_to_measure" in a and len(a["metrics_to_measure"]) > 20


def _adequate_metrics():
    return {"history_years": 18, "distinct_issuers": 1200, "inactive_issuer_coverage": 0.4,
            "norgate_overlap_fraction": 0.7, "stable_id_mapping_rate": 0.95,
            "timestamp_completeness": 0.98, "exact_time_fraction": 0.7,
            "duplicate_rate": 0.01, "correction_restatement_rate": 0.03,
            "eps_coverage": 0.9, "revenue_coverage": 0.7, "monthly_cohort_count": 180,
            "median_issuers_per_cohort": 300, "missingness": 0.05,
            "total_revision_events": 500000, "step_days": 21}


def test_adequacy_measured_all_pass():
    a = AR.assess_adequacy(_adequate_metrics())
    assert a["measured"] is True and a["adequate"] is True and a["failed_gates"] == []
    assert a["effective_independent_cohort_count"] is not None


def test_adequacy_flags_short_history_as_weakest():
    m = dict(_adequate_metrics(), history_years=1)
    a = AR.assess_adequacy(m)
    assert a["adequate"] is False and "history_years" in a["failed_gates"]


def test_adequacy_not_record_count_only():
    # huge event count but tiny breadth is NOT adequate
    m = dict(_adequate_metrics(), total_revision_events=99999999, distinct_issuers=10,
             monthly_cohort_count=5)
    a = AR.assess_adequacy(m)
    assert a["adequate"] is False


# --------------------------------------------------------------------------- #
# Workstream G -- power & confidence model.
# --------------------------------------------------------------------------- #
def test_power_prior_only_before_trial():
    p = AR.power_model()
    assert p["status"] == "CONFIDENCE_PRIOR_ONLY" and p["measured"] is False
    assert p["confidence_range"] == [0.25, 0.35]
    assert p["confidence_basis"] == "PRIOR_ASSUMPTION_NOT_MEASURED"
    assert p["family_size"] == 6
    assert all("min_detectable_ic" in h for h in p["per_horizon"])


def test_power_measured_update_changes_basis():
    adq = AR.assess_adequacy(_adequate_metrics())
    p = AR.power_model(measured={"adequacy": adq})
    assert p["status"] == "CONFIDENCE_MEASURED_UPDATE" and p["measured"] is True
    assert p["confidence_basis"] == "MEASURED_TRIAL_ADEQUACY_AND_EFFECTIVE_SAMPLE"
    lo, hi = p["confidence_range"]
    assert 0.0 <= lo <= hi <= 0.95


# --------------------------------------------------------------------------- #
# Workstream H -- cost & purchase gate.
# --------------------------------------------------------------------------- #
def test_purchase_terminals_enum():
    assert set(AR.PURCHASE_TERMINALS) == {
        "TRIAL_NOT_STARTED", "TRIAL_DATA_INSUFFICIENT", "DO_NOT_BUY",
        "REQUEST_EXPANDED_TRIAL", "NEGOTIATE_PRICE",
        "PURCHASE_CANDIDATE_PENDING_MANUAL_APPROVAL"}


def test_purchase_trial_not_started():
    d = AR.purchase_decision(trial_started=False, adequacy=AR.assess_adequacy(None))
    assert d["purchase_terminal"] == "TRIAL_NOT_STARTED"
    assert d["manual_approval_required"] is True and d["auto_purchase"] is False


def test_purchase_request_expanded_trial_on_short_history():
    adq = AR.assess_adequacy(dict(_adequate_metrics(), history_years=1))
    d = AR.purchase_decision(trial_started=True, adequacy=adq, power=AR.power_model(),
                             integrity={"clean": True}, cost={})
    assert d["purchase_terminal"] == "REQUEST_EXPANDED_TRIAL"


def test_purchase_do_not_buy_on_pit_violation():
    adq = AR.assess_adequacy(_adequate_metrics())
    d = AR.purchase_decision(trial_started=True, adequacy=adq, power=AR.power_model(),
                             integrity={"clean": False, "total_violations": 2}, cost={})
    assert d["purchase_terminal"] == "DO_NOT_BUY"


def test_purchase_negotiate_when_quote_above_max_rational():
    adq = AR.assess_adequacy(_adequate_metrics())
    d = AR.purchase_decision(trial_started=True, adequacy=adq,
                             power=AR.power_model(measured={"adequacy": adq}),
                             integrity={"clean": True},
                             cost={"one_time_history_usd": 10_000_000})
    assert d["purchase_terminal"] == "NEGOTIATE_PRICE"


def test_purchase_candidate_requires_manual_approval():
    adq = AR.assess_adequacy(_adequate_metrics())
    d = AR.purchase_decision(trial_started=True, adequacy=adq,
                             power=AR.power_model(measured={"adequacy": adq}),
                             integrity={"clean": True}, cost={})
    assert d["purchase_terminal"] == "PURCHASE_CANDIDATE_PENDING_MANUAL_APPROVAL"
    assert d["manual_approval_required"] is True and d["auto_purchase"] is False


def test_purchase_never_invents_price():
    d = AR.purchase_decision(trial_started=False, adequacy=AR.assess_adequacy(None))
    assert d["quoted_one_time_price_usd"] is None
    assert d["max_rational_price"]["is_provider_price"] is False


def test_max_rational_price_uses_explicit_assumptions():
    mrp = AR.max_rational_one_time_price(confidence_range=[0.25, 0.35])
    assert mrp["is_provider_price"] is False
    assert mrp["max_rational_one_time_price_usd"] >= 0.0
    assert "assumptions" in mrp


# --------------------------------------------------------------------------- #
# Workstream I -- vendor comparison.
# --------------------------------------------------------------------------- #
def test_vendor_comparison_preserves_unknown_and_no_independence():
    vc = AR.vendor_comparison()
    assert vc["providers"] == ["intrinio_zacks", "nasdaq_zacks", "unspecified_third"]
    assert vc["assume_independent_sources"] is False
    third = vc["matrix"]["unspecified_third"]
    assert third["underlying_source"] == AR.UNKNOWN
    # both real vendors are zacks-sourced (not independent)
    assert vc["matrix"]["intrinio_zacks"]["underlying_source"] == "zacks"
    assert vc["matrix"]["nasdaq_zacks"]["underlying_source"] == "zacks"


# --------------------------------------------------------------------------- #
# Command center + missing-price handling + safety.
# --------------------------------------------------------------------------- #
def test_command_center_trial_not_started_and_safety_labels():
    cc = AR.build_command_center(cfg={})
    assert cc["purchase_terminal"] == "TRIAL_NOT_STARTED"
    assert cc["prior_confidence_range"] == [0.25, 0.35]
    assert cc["measured_confidence_range"] is None
    for badge in ("RESEARCH ONLY", "NO PURCHASE EXECUTED", "MANUAL APPROVAL REQUIRED",
                  "NO AUTO-PROMOTION"):
        assert badge in cc["safety_badges"]
    assert cc["no_automatic_promotion"] is True


def test_command_center_missing_price_is_none():
    cc = AR.build_command_center(cfg={})
    assert cc["quoted_cost"]["one_time_history_usd"] is None


def test_contract_completeness_counts():
    cc = AR.contract_completeness()
    assert cc["n_schemas"] == 5
    assert cc["n_pit_invariants"] == len(AR.PIT_INVARIANTS)
    assert cc["complete"] is True


# --------------------------------------------------------------------------- #
# Workstream J -- read-only endpoints (route inspection; no DB required).
# --------------------------------------------------------------------------- #
def test_read_only_endpoints_registered_as_get():
    from paper_trader.api.app import app
    want = {
        "/v1/research/analyst-revisions/command-center",
        "/v1/research/analyst-revisions/vendor-comparison",
        "/v1/research/analyst-revisions/self-test"}
    by_path = {getattr(r, "path", None): r for r in app.routes}
    for path in want:
        assert path in by_path, path
        methods = set(by_path[path].methods)
        assert methods == {"GET"}, (path, methods)  # read-only: GET only


# --------------------------------------------------------------------------- #
# Operational-ledger immutability + no-network / snapshot writes isolated.
# --------------------------------------------------------------------------- #
def _ledger_fp():
    from paper_trader.alpha_agent import runtime as RT
    cfg = json.loads(Path("configs/alpha_agent/stage4_runtime.json").read_text(encoding="utf-8-sig"))
    f = RT.fingerprint_ledgers(cfg)
    return hashlib.sha256(json.dumps(f, sort_keys=True).encode()).hexdigest()[:16].upper() + "/" + str(len(f))


def test_write_snapshot_isolated_and_ledger_unchanged(tmp_path):
    before = _ledger_fp()
    cfg = {"research_root": str(tmp_path), "state_subdir": "stage13a",
           "registry_config": str(tmp_path / "reg.json")}
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg), encoding="utf-8")
    out = AR.write_snapshot(str(cfg_path))
    assert out["self_test_all_expected"] is True
    assert out["purchase_terminal"] == "TRIAL_NOT_STARTED"
    # snapshot written ONLY under the isolated research root
    assert Path(out["state_dir"]).is_relative_to(tmp_path)
    assert (Path(out["state_dir"]) / "command_center.json").exists()
    # the real operational ledger is byte-unchanged
    assert _ledger_fp() == before


def test_ledger_baseline_is_expected():
    # The operational-ledger STRUCTURE is the stable release invariant: no phase
    # (Stage 13A, Phase 28C, …) adds or removes ledger files — the count stays 21.
    # The content HASH is NOT pinned here because it legitimately advances every
    # time the operator runs a real Daily Close (each close appends marks /
    # performance / decision rows): the released baseline 403CAFD8B4796FE3/21
    # advanced to 83617F32AE2FBDB6/21 after the real 2026-08-04 operational close,
    # with the file count unchanged at 21. The byte-unchanged guarantee for a
    # single operation is proved by test_write_snapshot_isolated_and_ledger_unchanged.
    fp = _ledger_fp()
    assert fp.endswith("/21"), fp
