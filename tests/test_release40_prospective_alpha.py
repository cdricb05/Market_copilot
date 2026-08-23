"""Release 40 regression (hermetic - temporary research roots for BOTH the
R39 and R40 owners; no research drive, no providers, no network, no
weights). Covers: the contract, R39 ledger inheritance, candidate
immutability, forward-date refusal, duplicate-capture idempotency,
missed-date catch-up, stale-input handling, the family cap, evidence-
velocity dependence accounting, effective-sample calculations, the
always-valid boundaries and confidence sequence, feature-availability
integrity, the NY Fed PIT bridge, model-adapter contracts, the open-weight
licence/provenance contract, contamination classification, and the
economic expressions of the shadow scorer."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import r39  # noqa: E402
from paper_trader.alpha_agent import r40  # noqa: E402
from paper_trader.alpha_agent.r39 import zones  # noqa: E402
from paper_trader.alpha_agent.r40 import availability as AV  # noqa: E402
from paper_trader.alpha_agent.r40 import burden_ledger as BL  # noqa: E402
from paper_trader.alpha_agent.r40 import contract as C  # noqa: E402
from paper_trader.alpha_agent.r40 import evidence_velocity as EV  # noqa: E402
from paper_trader.alpha_agent.r40 import nyfed_bridge as NY  # noqa: E402
from paper_trader.alpha_agent.r40 import open_models as OM  # noqa: E402
from paper_trader.alpha_agent.r40 import research_cycle as RC  # noqa: E402
from paper_trader.alpha_agent.r40 import sequential as SQ  # noqa: E402
from paper_trader.alpha_agent.r40 import shadow_registry as SR  # noqa: E402


@pytest.fixture()
def roots(tmp_path, monkeypatch):
    r39_root = tmp_path / "r39"
    r40_root = tmp_path / "r40"
    r39_root.mkdir()
    r40_root.mkdir()
    monkeypatch.setenv(r39.RESEARCH_ROOT_ENV, str(r39_root))
    monkeypatch.setenv(r40.RESEARCH_ROOT_ENV, str(r40_root))
    # the R40 campaign may already be bound to the real root in-process;
    # rebind it to the temp root for this test
    r39._EXTERNAL_CAMPAIGN_ROOTS.pop(r40.CAMPAIGN_ID, None)
    r40.campaign_dir()
    yield {"r39": r39_root, "r40": r40_root}
    r39._EXTERNAL_CAMPAIGN_ROOTS.pop(r40.CAMPAIGN_ID, None)


def _fake_continuation_ledger(root: Path, n: int) -> None:
    d = root / C.R39_CONTINUATION_CAMPAIGN_ID
    d.mkdir(parents=True, exist_ok=True)
    body = {"contract": zones.REUSE_SCHEMA,
            "campaign_id": C.R39_CONTINUATION_CAMPAIGN_ID,
            "calculation_owner": "test",
            "inherited_distinct_candidates": 107,
            "evaluations": {"c39_%012d" % i: {"count": 1,
                                              "stages": ["STAGE2_3"]}
                            for i in range(n)},
            "total_evaluations": n + 4, "distinct_candidates": n}
    r39.write_json(d / zones.REUSE_NAME, body, immutable=False)


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
def test_contract_declares_the_frozen_rules():
    assert C.MAX_RESEARCH_SHADOW_FAMILY == 5
    assert C.R39_INHERITED_EFFECTIVE_TRIALS_EXPECTED == 194
    assert C.BURDEN_NEVER_RESETS and C.NO_CAMPAIGN_ID_LAUNDERING
    assert C.SLOT_5_SELECTION_RULE["frozen_before_any_r40_evaluation"]
    assert C.SLOT_5_SELECTION_RULE["may_read_zone_c"] is False
    assert C.SLOT_5_SELECTION_RULE["may_read_true_forward"] is False
    assert C.SLOT_5_MIN_ZONE_B_T == 1.5
    assert C.SLOT_5_DUPLICATE_CORRELATION == 0.90
    assert len(C.MODEL_WEIGHT_DOWNLOAD_CONDITIONS) == 10
    assert C.MAY_DOWNLOAD_MODEL_WEIGHTS is True
    assert C.MAY_PURCHASE_COMPUTE is False and C.MAY_SPEND_MONEY is False
    assert C.MAY_ENABLE_SCHEDULED_TASK is False
    assert C.MAY_PROMOTE_MODEL is False and C.MAY_CREATE_ORDER is False
    assert C.NO_HISTORICAL_ROW_IN_TRUE_FORWARD
    assert C.NO_ROW_AT_OR_BEFORE_CANDIDATE_FREEZE
    assert C.NO_OPTIONAL_THRESHOLD_RESET and C.NO_MODEL_SWAP_UNDER_ONE_ID
    assert C.CATCH_UP_MUST_BE_CONTIGUOUS
    assert C.DAILY_MARKS_OF_A_MONTHLY_POSITION_ARE_NOT_INDEPENDENT_TRADES
    assert C.NEVER_REPORT_MARKETS_TIMES_DAYS_AS_INDEPENDENT_SAMPLES
    assert set(C.PRETRAINING_CONTAMINATION_CLASSES) >= {
        "PRETRAINING_DATA_KNOWN_CLEAN", "PRETRAINING_OVERLAP_POSSIBLE",
        "PRETRAINING_OVERLAP_LIKELY", "PRETRAINING_UNKNOWN"}
    assert len(C.contract_hash()) == 64
    assert C.SHELL_POLICY_VIOLATION_REPORTED is True
    assert C.SHELL_POLICY_EVENTS["monitor_tool_invocations"] == 1


def test_safety_block_adds_r40_refusals():
    sb = r40.safety_block()
    assert sb["changes_scheduler"] is False
    assert sb["backdates_forward_rows"] is False
    assert sb["promotes_model"] is False
    assert "NO BACKDATED FORWARD ROW" in sb["safety"]


# --------------------------------------------------------------------------- #
# Burden inheritance (R39 -> R40, never reset)
# --------------------------------------------------------------------------- #
def test_burden_inherits_194_and_accrues(roots):
    _fake_continuation_ledger(roots["r39"], 194)
    s = BL.inherit()
    assert s["R39_INHERITED_EFFECTIVE_TRIALS"] == 194
    assert s["R40_NEW_EFFECTIVE_TRIALS"] == 0
    assert s["CUMULATIVE_R39_R40_EFFECTIVE_TRIALS"] == 194
    BL.record("c40_new_candidate", stage="TEST")
    BL.record("c40_new_candidate", stage="TEST")       # reuse, not new
    BL.record("c39_%012d" % 3, stage="TEST")           # inherited id reuse
    s2 = BL.summary()
    assert s2["R40_NEW_EFFECTIVE_TRIALS"] == 1
    assert s2["CUMULATIVE_R39_R40_EFFECTIVE_TRIALS"] == 195
    assert BL.new_candidate_ids() == ["c40_new_candidate"]
    # the ledger lives under the R40 root, written by the R39 owner
    assert (roots["r40"] / r40.CAMPAIGN_ID / zones.REUSE_NAME).exists()
    assert BL.inherit()["CUMULATIVE_R39_R40_EFFECTIVE_TRIALS"] == 195


def test_burden_refuses_wrong_inherited_count(roots):
    _fake_continuation_ledger(roots["r39"], 150)
    with pytest.raises(ValueError):
        BL.inherit()


def test_campaign_root_registration_refuses_a_second_home(roots):
    with pytest.raises(ValueError):
        r39.register_campaign_root(r40.CAMPAIGN_ID, roots["r39"])
    assert r39.campaign_dir(r40.CAMPAIGN_ID) == \
        roots["r40"] / r40.CAMPAIGN_ID


# --------------------------------------------------------------------------- #
# Candidate immutability
# --------------------------------------------------------------------------- #
def test_immutable_artifact_cannot_be_rewritten(roots):
    p = r40.campaign_dir() / "x.json"
    r39.write_json(p, {"a": 1})
    with pytest.raises(r39.ArtifactImmutable):
        r39.write_json(p, {"a": 2})


def test_family_cap_is_five():
    rows = [{"shadow_id": "s%d" % i} for i in range(5)]
    SR.enforce_cap(rows)
    with pytest.raises(SR.FamilyCapExceeded):
        SR.enforce_cap(rows + [{"shadow_id": "s5"}])
    with pytest.raises(SR.FamilyCapExceeded):
        SR.enforce_cap(rows[:2] + [{"shadow_id": "s0"}])


# --------------------------------------------------------------------------- #
# Forward capture: date refusal, idempotency, catch-up, staleness
# --------------------------------------------------------------------------- #
def _panel(dates, markets=("A", "B", "C", "D", "E", "F", "G")):
    rows = []
    rng = np.random.default_rng(1)
    for d in dates:
        for i, m in enumerate(markets):
            rows.append({"decision_date": pd.Timestamp(d), "market_id": m,
                         "economic_group": "G1" if i < 4 else "G2",
                         "carry_slope_ann": float(rng.normal()),
                         "vol_63": 0.1 + 0.05 * i,
                         "fwd_21": float(rng.normal() * 0.02),
                         "has_cot": 1})
    return pd.DataFrame(rows)


def _shadow(frozen_at="2026-08-23T04:02:47Z", expr="XS_LONG_SHORT"):
    return {"shadow_id": "shadow_test", "candidate_id": "c40_test",
            "origin_release": "release40", "lane": "FUT",
            "model": "rule:carry_slope_ann", "expression": expr,
            "frozen_at": frozen_at, "horizon_sessions": 21,
            "spec_hash": "abc", "coefficient_hash": None,
            "cost_model": {"bps_per_side": {}}}


def test_eligibility_refuses_dates_at_or_before_freeze_and_in_future():
    sh = _shadow()
    panel = _panel(["2026-07-31", "2026-08-22", "2026-08-23",
                    "2026-08-31", "2026-09-30"])
    now = pd.Timestamp("2026-09-05")
    elig = RC.eligible_dates(sh, panel, set(), now)
    assert [str(d.date()) for d in elig] == ["2026-08-31"]
    elig2 = RC.eligible_dates(sh, panel, {"2026-08-31"}, now)
    assert elig2 == []


def test_capture_is_idempotent_and_catches_up_contiguously(roots):
    sh = _shadow(frozen_at="2026-01-31T00:00:00Z")
    registry = {"shadows": [sh]}
    panel = _panel(["2026-01-30", "2026-02-27", "2026-03-31",
                    "2026-04-30"])
    state = {"fut": panel, "macro": pd.DataFrame()}
    now = pd.Timestamp("2026-05-10")
    fresh = RC.input_freshness(state, now)
    cap = RC._capture_r40(registry, state, now, fresh, r40.CAMPAIGN_ID)
    assert cap["appended"] == 3                    # every missed date
    assert cap["verify"]["intact"]
    desk = RC._desk()
    rows = desk._read_ledger(SR.shadow_dir(), SR.SNAPSHOT_LEDGER)
    dates = [r["decision_date"] for r in rows]
    assert dates == ["2026-02-27", "2026-03-31", "2026-04-30"]
    assert all(r["forward_evidence_type"] == "TRUE_FORWARD" for r in rows)
    assert rows[0]["evidence_grade"] == "LATE_CAPTURE_CONTIGUOUS"
    assert rows[0]["capture_lateness_sessions"] > 1
    assert all(r["promotion_allowed"] is False for r in rows)
    # rerun: nothing twice
    cap2 = RC._capture_r40(registry, state, now, fresh, r40.CAMPAIGN_ID)
    assert cap2["appended"] == 0
    assert len(desk._read_ledger(SR.shadow_dir(), SR.SNAPSHOT_LEDGER)) == 3
    # maturation appends once per (shadow, date) and is idempotent too
    mat = RC._mature_r40(registry, state, now, r40.CAMPAIGN_ID)
    assert mat["appended"] == 3 and mat["verify"]["intact"]
    assert RC._mature_r40(registry, state, now, r40.CAMPAIGN_ID)[
        "appended"] == 0
    outs = desk._read_ledger(SR.shadow_dir(), SR.OUTCOME_LEDGER)
    assert all("net_return" in r and "supporting" in r for r in outs)


def test_stale_inputs_are_flagged_not_hidden():
    panel = _panel(["2026-08-21"])
    idx = pd.date_range("2026-07-01", "2026-07-20")
    macro = pd.DataFrame({"vix": np.ones(len(idx)),
                          "DTB3": np.ones(len(idx))}, index=idx)
    fresh = RC.input_freshness({"fut": panel, "macro": macro},
                               pd.Timestamp("2026-08-23"))
    assert fresh["macro_overlay"]["state"] == "STALE"
    assert "macro_overlay" in fresh["stale_sources"]
    assert fresh["latest_market_session"] == "2026-08-21"


def test_status_cycle_runs_without_inputs(roots):
    body = RC.run_cycle(mode="status", build_state=False)
    assert body["FORWARD_CAPTURE_STATE"] == "STATUS_ONLY"
    assert body["ledger_status"]["true_forward_snapshots"] == 0
    assert body["ledger_status"]["all_chains_intact"]
    assert body["automation"].startswith("OFF")
    assert body["scheduler_changed"] is False


def test_capture_cycle_without_eligible_date_waits(roots):
    reg = {"shadows": [_shadow(frozen_at="2026-08-23T04:02:47Z")],
           "n_shadows": 1, "frozen_at": "2026-08-23T04:02:47Z"}
    r39.write_json(r40.campaign_dir() / SR.REGISTRY_NAME, reg)
    panel = _panel(["2026-07-31", "2026-08-21"])
    body = RC.run_cycle(mode="capture",
                        fresh_state={"fut": panel, "macro": pd.DataFrame()},
                        now=pd.Timestamp("2026-08-23T12:00:00"))
    assert body["FORWARD_CAPTURE_STATE"] == "READY_WAITING_FOR_ELIGIBLE_DATE"
    assert body["capture_r40"]["appended"] == 0
    assert "2026-08" in body["eligibility"]["shadow_test"]["next_expected"]


# --------------------------------------------------------------------------- #
# Shadow scorer: economic expressions
# --------------------------------------------------------------------------- #
def test_scorer_xs_weights_are_self_financed_and_unit_gross():
    rows = _panel(["2026-08-31"], markets=tuple("ABCDEFGHI"))
    w = SR.score_at(_shadow(), rows)
    assert abs(sum(w.values())) < 1e-9
    assert abs(sum(abs(v) for v in w.values()) - 1.0) < 1e-9


def test_scorer_group_rv_weights_are_self_financed_per_group():
    sh = _shadow(expr="GROUP_RV")
    rows = _panel(["2026-08-31"])
    w = SR.score_at(sh, rows)
    assert w is not None
    assert abs(sum(w.values())) < 1e-6
    assert sum(abs(v) for v in w.values()) <= 1.0 + 1e-9


def test_scorer_refuses_too_few_markets():
    rows = _panel(["2026-08-31"], markets=("A", "B", "C"))
    assert SR.score_at(_shadow(), rows) is None


# --------------------------------------------------------------------------- #
# Evidence velocity / effective samples
# --------------------------------------------------------------------------- #
def test_ess_ratio_penalises_serial_dependence():
    rng = np.random.default_rng(0)
    iid = rng.normal(size=600)
    ar = np.empty(600)
    ar[0] = 0.0
    for t in range(1, 600):
        ar[t] = 0.6 * ar[t - 1] + rng.normal()
    assert abs(EV.ess_ratio(iid)["ratio"] - 1.0) < 0.25
    assert EV.ess_ratio(ar)["ratio"] < 0.6


def test_effective_markets_below_count_when_correlated():
    rng = np.random.default_rng(0)
    f = rng.normal(size=300)
    X = pd.DataFrame({"m%d" % i: 0.8 * f + 0.6 * rng.normal(size=300)
                      for i in range(10)})
    eff = EV.effective_markets(X)
    assert eff["n_markets"] == 10
    assert eff["effective_number_participation_ratio"] < 4.0
    assert eff["mean_pairwise_correlation"] > 0.4


def test_velocity_never_counts_daily_marks_or_markets_times_days():
    idx = pd.date_range("2007-01-31", periods=120, freq="ME")
    rng = np.random.default_rng(2)
    stream = pd.Series(0.003 + 0.015 * rng.normal(size=120), index=idx)
    v = EV.velocity({"shadow_id": "s", "candidate_id": "c",
                     "cadence": "monthly", "horizon_sessions": 21},
                    stream, {"mean_ic": 0.03, "t_stat": 2.5,
                             "n_dates": 120},
                    {"n_markets": 68,
                     "effective_number_participation_ratio": 9.0})
    daily = v["supporting_channels"]["daily_marks_of_fixed_position"]
    assert daily["mean_information_gain"] == 0.0
    assert daily["counted_as_independent_trades"] is False
    assert v["never_report_markets_times_days"] is True
    assert v["time_to_decision"]["success_years"]["point_estimate"] > 0
    assert v["expected_log_evidence_growth_per_obs"]["under_null"] < 0
    assert 10 < v["primary_economic_evidence_cadence_obs_per_year"] < 14


# --------------------------------------------------------------------------- #
# Always-valid sequential inference
# --------------------------------------------------------------------------- #
def test_e_process_success_boundary_is_rare_under_the_null():
    rng = np.random.default_rng(7)
    crossings = 0
    for _ in range(200):
        x = rng.normal(size=120) * 0.015
        crossed = False
        for n in range(1, 121):
            if SQ.PD.e_process(x[:n], sigma0=0.015)["e_value"] >= \
                    SQ.E_SUCCESS:
                crossed = True
                break
        crossings += crossed
    assert crossings <= 20          # 5% nominal at ANY stopping time


def test_confidence_sequence_contains_the_true_mean():
    rng = np.random.default_rng(3)
    x = 0.004 + 0.015 * rng.normal(size=240)
    cs = SQ.confidence_sequence(x, sigma0=0.015)
    assert cs["lower"] is not None and cs["lower"] <= 0.004 <= cs["upper"]
    assert cs["anytime_valid"]


def test_design_freezes_boundaries_and_growth_is_monotone():
    sh = {"shadow_id": "s", "candidate_id": "c", "control": "RMC",
          "cost_model": {"base": "TRADED_NOTIONAL"}}
    d = SQ.design_for(sh, sigma0=0.015, mu_point=0.003, cadence="monthly")
    assert d["success_boundary_e"] == 20.0
    assert d["minimum_evidence_before_futility"] == 36
    assert d["max_horizon_observations"] == 60
    assert d["thresholds_immutable_after_first_observation"]
    g = d["expected_log_evidence_growth_per_obs"]
    assert g["at_point_estimate"] > g["at_registered_effect"] > g["under_null"]
    assert d["periods_to_success_boundary"]["at_point_estimate"] < \
        d["periods_to_success_boundary"]["at_registered_effect"]
    assert SQ.family_e_value([1.0, 40.0, 2.0]) == pytest.approx(43.0 / 3)


# --------------------------------------------------------------------------- #
# Feature-availability integrity
# --------------------------------------------------------------------------- #
def test_availability_rule_flags_selection_absent_zone_c_live_features():
    dates = pd.date_range("2000-01-31", "2022-12-31", freq="ME")
    rows = []
    for d in dates:
        zone = "ZONE_A" if d <= pd.Timestamp("2007-02-23") else \
            "ZONE_B" if d <= pd.Timestamp("2016-11-01") else "ZONE_C"
        for m in ("A", "B"):
            rows.append({"decision_date": d, "market_id": m, "zone": zone,
                         "asset_class": "X",
                         "good": 1.0,
                         "patchy": (1.0 if zone != "ZONE_B" else np.nan)})
    p = pd.DataFrame(rows)
    cov = AV.coverage_report(p, ["good", "patchy"])
    assert cov["good"]["selection_admissible"]
    assert not cov["patchy"]["selection_admissible"]
    assert cov["patchy"]["zone_c_live_but_selection_absent"]
    assert cov["patchy"]["admissibility_state"] == \
        "INADMISSIBLE_SELECTION_UNAVAILABLE"
    assert cov["patchy"]["by_zone"]["ZONE_B"] == 0.0
    p2, names = AV.add_causal_masks(p, ["patchy"])
    assert names == ["patchy__avail"]
    m = p2.loc[p2["zone"] == "ZONE_B", "patchy__avail"]
    assert (m == 0.0).all()
    assert AV.MIN_SELECTION_COVERAGE == 0.5


# --------------------------------------------------------------------------- #
# NY Fed legacy bridge
# --------------------------------------------------------------------------- #
def _fake_nyfed() -> pd.DataFrame:
    rows = []
    weeks = pd.date_range("1998-01-28", "2026-08-12", freq="W-WED")
    for w in weeks:
        sb = NY._era_of(pd.Series([w]))[0]
        v = 1000.0
        if sb == "SBP2001":
            codes = {"PDPUSGTBNOP": 10 * v, "PDPUSGCS5LNOP": 20 * v,
                     "PDPUSGCS5MNOP": 30 * v, "PDPUSGTIISNOP": 5 * v}
        elif sb == "SBP2013":
            codes = {"PDPUSGTBNOP": 10 * v, "PDPUSGCS3LNOP": 12 * v,
                     "PDPUSGCS36NOP": 8 * v, "PDPUSGCS611NOP": 15 * v,
                     "PDPUSGCSM11NOP": 15 * v, "PDPUSGTIISNOP": 5 * v,
                     "PDPUSGTNOP": 65 * v}
        else:
            codes = {"PDPOSGS-B": 10 * v, "PDPOSGS-BFRN": 1 * v,
                     "PDPOSGSC-L2": 4 * v, "PDPOSGSC-G2L3": 4 * v,
                     "PDPOSGSC-G3L6": 12 * v, "PDPOSGSC-G6L7": 5 * v,
                     "PDPOSGSC-G7L11": 10 * v, "PDPOSTIPS-L2": 1 * v,
                     "PDPOSTIPS-G2": 1 * v, "PDPOSTIPS-G6L11": 2 * v,
                     "PDPOSTIPS-G11": 1 * v}
            if sb == "SBN2013":
                codes.pop("PDPOSGS-BFRN")       # FRNs did not exist yet
                codes["PDPOSGSC-G11"] = 15 * v  # the total stays 65
            elif sb == "SBN2015":
                codes["PDPOSGSC-G11"] = 15 * v
            else:
                codes["PDPOSGSC-G11L21"] = 9 * v
                codes["PDPOSGSC-G21"] = 6 * v
            codes["PDPOSGST-TOT"] = sum(codes.values())
        for k, val in codes.items():
            rows.append({"as_of": w, "series": k, "value": val})
    return pd.DataFrame(rows)


def test_nyfed_bridge_concepts_are_arithmetic_and_pit():
    ny = _fake_nyfed()
    bridged, proofs, wide, era = NY.bridge(ny)
    assert bridged["UST_BILLS_NET"].notna().sum() == len(bridged)
    assert (bridged["UST_COUPONS_NET_TOTAL"].dropna() > 0).all()
    # ex-TIPS is DERIVED as TOTAL - TIPS in every era
    ex = bridged["UST_EX_TIPS_TOTAL_NET"]
    assert np.allclose(ex, bridged["UST_TOTAL_NET"] - bridged["UST_TIPS_NET"],
                       equal_nan=True)
    # the 1998-2001 duration buckets are NOT bridged (blocked semantics)
    first_le6 = bridged["UST_COUPONS_LE_6Y_NET"].dropna().index.min()
    assert first_le6 >= pd.Timestamp("2001-07-01")
    assert NY.BLOCKED["DEALER_FINANCING_REPO"]["state"] == \
        "BLOCKED_IDENTITY_SEMANTICS"
    ident = NY.arithmetic_identities(wide, era)
    assert ident["SBP2013_total_equals_bills_plus_coupons_plus_tiis"][
        "identity_holds"]
    assert ident["SBN2015_total_equals_bills_frn_coupons_tips"][
        "identity_holds"]
    seams = NY.seam_checks(bridged)
    assert all(v.get("continuous_under_rule", True)
               for v in seams["UST_TOTAL_NET"].values())
    f = NY.features(bridged, seams)
    assert f.index.min() == bridged.index.min() + pd.Timedelta(days=9)
    assert "nyfed2_total_z" in f.columns and f["nyfed2_total_z"].notna().any()
    assert set(NY.MARKET_BUCKET) == {"ZT", "ZF", "ZN", "TN", "ZB", "UB"}


# --------------------------------------------------------------------------- #
# Open-weight policy and contamination
# --------------------------------------------------------------------------- #
def test_open_weight_conditions_and_contamination_labels():
    gated = {"hf": {"license": "apache-2.0", "gated": True,
                    "revision": "x"}}
    assert OM.conditions_verdict(gated)["all_conditions_pass"] is False
    for key, e in OM.INVENTORY.items():
        assert e["contamination_class"] in C.PRETRAINING_CONTAMINATION_CLASSES
        assert e["evidence_role"] in C.EVIDENCE_ROLES
        if e["decision"] == "SELECTED":
            assert OM.conditions_verdict(e)["all_conditions_pass"], key
        if e["contamination_class"] != "PRETRAINING_DATA_KNOWN_CLEAN":
            assert e["evidence_role"] != "CLEAN_HISTORICAL_OOS", key
    tab = OM.INVENTORY["TABULAR_FOUNDATION::TabPFN-v2-reg"]
    assert tab["contamination_class"] == "PRETRAINING_DATA_KNOWN_CLEAN"
    chr_ = OM.INVENTORY["TIME_SERIES_FOUNDATION::chronos-bolt-small"]
    assert chr_["contamination_class"] == "PRETRAINING_OVERLAP_LIKELY"
    assert chr_["evidence_role"] == "REPRESENTATION_RESEARCH"
    assert str(OM.R40_LIB_DIR).upper().startswith("D:")
    assert str(OM.HF_HOME).upper().startswith("D:")


# --------------------------------------------------------------------------- #
# Model adapter contracts (torch from the research drive; skip if absent)
# --------------------------------------------------------------------------- #
def _torch_available() -> bool:
    try:
        from paper_trader.alpha_agent.r39 import models_ext as MX
        MX._torch()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _torch_available(), reason="torch CPU not installed")
def test_from_scratch_adapters_fit_and_predict():
    from paper_trader.alpha_agent.r39.models_ext import SEQ_N_LAGS
    from paper_trader.alpha_agent.r40 import model_challenge as MC
    rng = np.random.default_rng(0)
    nf, N = 4, 600
    X = rng.normal(size=(N, nf * (SEQ_N_LAGS + 1)))
    X[rng.random(X.shape) < 0.05] = np.nan
    y = np.nan_to_num(X[:, -1]) * 0.3 + rng.normal(size=N)
    MC.TORCH_EPOCHS = 3
    for cls, cfg in ((MC.SSMLiteSeq, "h16_lr1e-3"),
                     (MC.PatchTSTLiteSeq, "d32_h2")):
        m = cls(nf, cfg).fit(X, y)
        p = m.predict(X[:50])
        assert p.shape == (50,) and np.isfinite(p).all()
    g = MC.GraphMLP().fit(X[:, :8], y)
    assert g.predict(X[:10, :8]).shape == (10,)
    assert len(MC.CONFIGS["ssm_lite_seq"]) <= C.MAX_CONFIGS_PER_MODEL_FAMILY


def test_graph_aggregates_are_causal_features():
    from paper_trader.alpha_agent.r40 import model_challenge as MC
    assert MC.GRAPH_TOP_K == 5 and MC.GRAPH_WINDOW == 60
    assert all(n.startswith("nbr_") for n in MC.NBR_FEATURES)


# --------------------------------------------------------------------------- #
# Closeout import refuses silently-changed facts
# --------------------------------------------------------------------------- #
def test_closeout_import_records_mismatches_instead_of_trusting(roots):
    from paper_trader.alpha_agent.r40 import closeout_import as CI
    _fake_continuation_ledger(roots["r39"], 194)
    body = CI.run(repo_root=roots["r40"], handoff_dir=roots["r40"])
    assert body["state"] == "R39_NOT_VERIFIED"
    checks = {m["check"] for m in body["mismatches"]}
    assert "artifact_manifest_state" in checks
    assert body["r40_contract_hash_frozen_before_any_evaluation"] == \
        C.contract_hash()
    assert body["slot_5_selection_rule_frozen"] == C.SLOT_5_SELECTION_RULE
    # immutable: a second call returns the same artifact
    assert CI.run(repo_root=roots["r40"], handoff_dir=roots["r40"])[
        "closeout_import_hash"] == body["closeout_import_hash"]


def test_runner_script_has_no_scheduler_or_order_surface():
    src = (Path(__file__).resolve().parents[1] / "scripts"
           / "run_r40_research_cycle.py").read_text(encoding="utf-8")
    for bad in ("schtasks", "Register-ScheduledTask", "create_order",
                "place_order", "def promote", "promote_model("):
        assert bad not in src
    assert "AUTOMATION OFF" in src
    r40_json = json.dumps(r40.safety_block())
    assert '"creates_order": false' in r40_json
