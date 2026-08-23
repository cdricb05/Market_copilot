"""Release 39 CONTINUATION regression (hermetic - no research drive, no
providers, no network). Covers the continuation contract, the cumulative
burden inheritance, the Track-F expressions, the Track-G adapters, the
Track-H shadow mechanics and the Track-I sequential design."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import r39  # noqa: E402
from paper_trader.alpha_agent.r39 import continuation as CONT  # noqa: E402
from paper_trader.alpha_agent.r39 import info_expansion as IE  # noqa: E402
from paper_trader.alpha_agent.r39 import models_ext as MX  # noqa: E402
from paper_trader.alpha_agent.r39 import prospective_design as PD  # noqa: E402
from paper_trader.alpha_agent.r39 import research_shadow as RS  # noqa: E402
from paper_trader.alpha_agent.r39 import trade_space_ext as TX  # noqa: E402
from paper_trader.alpha_agent.r39 import wide_prosecution as WP  # noqa: E402
from paper_trader.alpha_agent.r39 import zones  # noqa: E402
from paper_trader.alpha_agent.r39.discovery_director import _cid  # noqa: E402


@pytest.fixture()
def research_root(tmp_path, monkeypatch):
    monkeypatch.setenv(r39.RESEARCH_ROOT_ENV, str(tmp_path))
    return tmp_path


# --------------------------------------------------------------------------- #
# Continuation contract
# --------------------------------------------------------------------------- #
def test_continuation_contract_is_declared():
    assert CONT.CONTINUATION_CAMPAIGN_ID != CONT.V1_CAMPAIGN_ID
    assert CONT.CONTINUATION_CAMPAIGN_ID.startswith("r39_")
    assert CONT.BURDEN_NEVER_RESETS is True
    assert CONT.NO_CAMPAIGN_ID_LAUNDERING is True
    assert CONT.V1_EFFECTIVE_TRIALS_EXPECTED == 107
    assert CONT.ZONE_C_PREGATE_MIN_ZONE_B_T == 3.0
    assert CONT.QUALIFICATION_ADDS_RESIDUAL_ALPHA is True
    assert CONT.RESIDUAL_ALPHA_T_MIN == 2.0
    assert CONT.V1_ARTIFACTS_REMAIN_IMMUTABLE is True
    assert CONT.HISTORICAL_ALPHA_CANDIDATE_IS_NOT_PROVEN_ALPHA is True


def test_no_pretrained_weights_and_named_foundation_blockers():
    assert CONT.MAY_DOWNLOAD_MODEL_WEIGHTS_STILL_FALSE is True
    assert CONT.DEEP_MODELS_TRAINED_FROM_SCRATCH is True
    assert len(CONT.FOUNDATION_MODEL_BLOCKERS) >= 3
    for reason in CONT.FOUNDATION_MODEL_BLOCKERS.values():
        assert len(reason) > 40  # a real reason, not a shrug


def test_shell_policy_audit_records_both_sides():
    audit = CONT.SHELL_POLICY_AUDIT
    assert "operator_assertion" in audit
    assert audit["tool_invocation_matches"] == 0
    assert audit["audit_verdict"] == \
        "NO_BASH_TOOL_INVOCATION_FOUND_IN_TRANSCRIPT"
    assert "DISPUTED_BY_TRANSCRIPT_AUDIT" in audit["v1_violation_state"]
    assert CONT.WINDOWS_POWERSHELL_ONLY is True


# --------------------------------------------------------------------------- #
# Burden inheritance
# --------------------------------------------------------------------------- #
def _fake_v1_ledger(root: Path, n: int) -> None:
    d = root / CONT.V1_CAMPAIGN_ID
    d.mkdir(parents=True, exist_ok=True)
    body = {"contract": zones.REUSE_SCHEMA,
            "campaign_id": CONT.V1_CAMPAIGN_ID,
            "calculation_owner": "test",
            "evaluations": {"c39_%012d" % i: {"count": 1,
                                              "stages": ["STAGE2_3"]}
                            for i in range(n)},
            "total_evaluations": n, "distinct_candidates": n}
    r39.write_json(d / zones.REUSE_NAME, body, immutable=False)


def test_burden_inheritance_and_accrual(research_root):
    _fake_v1_ledger(research_root, 107)
    out = CONT.inherit_reuse_ledger()
    assert out["R39_V1_EFFECTIVE_TRIALS"] == 107
    assert out["R39_CONTINUATION_NEW_TRIALS"] == 0
    assert out["R39_CUMULATIVE_EFFECTIVE_TRIALS"] == 107
    # idempotent
    again = CONT.inherit_reuse_ledger()
    assert again["R39_CUMULATIVE_EFFECTIVE_TRIALS"] == 107
    # continuation evaluations ADD
    zones.record_zone_b("c39_new_candidate", stage="CONT_TEST",
                        campaign_id=CONT.CONTINUATION_CAMPAIGN_ID)
    after = CONT.cumulative_burden()
    assert after["R39_CONTINUATION_NEW_TRIALS"] == 1
    assert after["R39_CUMULATIVE_EFFECTIVE_TRIALS"] == 108


def test_burden_inheritance_refuses_wrong_count(research_root):
    _fake_v1_ledger(research_root, 99)
    with pytest.raises(ValueError):
        CONT.inherit_reuse_ledger()


# --------------------------------------------------------------------------- #
# WIDE identity (pure hash arithmetic; no data)
# --------------------------------------------------------------------------- #
def test_wide_spec_reproduces_the_frozen_candidate_id():
    assert _cid(WP.WIDE_SPEC) == WP.WIDE_ID == "c39_c9233eccaa74"
    assert WP.FROZEN_ZONE_C["after_cost_excess_t_stat"] == \
        pytest.approx(2.4311194784714067)
    assert WP.RECONSTRUCTION_TOLERANCE <= 1e-9


def test_wide_family_blocks_cover_the_wide_bundle_families():
    assert set(WP.WIDE_FAMILY_BLOCKS) == {
        "CLASSICAL", "MACRO", "SPECTRAL", "LATENT", "GRAPH", "MSTRUCT"}


# --------------------------------------------------------------------------- #
# Track F expressions
# --------------------------------------------------------------------------- #
def _rv_fixture():
    dates = pd.date_range("2010-01-31", periods=6, freq="ME")
    cols = ["A", "B", "C", "D"]
    rng = np.random.default_rng(7)
    pred = pd.DataFrame(rng.normal(size=(6, 4)), index=dates, columns=cols)
    fwd = pd.DataFrame(rng.normal(0, 0.02, size=(6, 4)), index=dates,
                       columns=cols)
    vol = pd.DataFrame(0.2, index=dates, columns=cols)
    vol["A"] = 0.1  # low-vol leg gets scaled UP by inverse vol
    cost = pd.Series(5.0, index=cols)
    return pred, fwd, vol, cost


def test_group_rv_is_self_financed_and_costed():
    pred, fwd, vol, cost = _rv_fixture()
    out = TX.vol_scaled_group_rv(pred, fwd, cost,
                                 {"G1": ["A", "B", "C", "D"]}, vol)
    W = out["weights"]
    assert np.allclose(W.sum(axis=1), 0.0, atol=1e-12)      # self-financed
    assert (W.abs().sum(axis=1) <= 1.0 + 1e-9).all()        # no leverage
    assert (np.asarray(out["costs"]) >= 0).all()
    assert out["expression"] == "GROUP_RV"


def test_group_rv_ignores_single_member_groups():
    pred, fwd, vol, cost = _rv_fixture()
    out = TX.vol_scaled_group_rv(pred, fwd, cost,
                                 {"G1": ["A"], "G2": ["B", "C"]}, vol)
    assert out["groups_used"] == ["G2"]
    assert np.allclose(out["weights"]["A"], 0.0)


def test_regime_gate_is_expanding_median_and_observable():
    idx = pd.date_range("2000-01-31", periods=60, freq="ME")
    macro = pd.Series(np.arange(60, dtype=float), index=idx)
    gate = TX.regime_gate(macro, idx, ["A"], "above_median")
    # a rising series is above its expanding median once warmed up
    assert bool(gate["A"].iloc[-1])
    assert not bool(gate["A"].iloc[0])  # warm-up refuses to gate


def test_abstain_overlay_drops_weak_conviction_names():
    dates = pd.date_range("2010-01-31", periods=4, freq="ME")
    cols = list("ABCDEFGH")
    pred = pd.DataFrame(0.0, index=dates, columns=cols)
    pred["A"], pred["H"] = 3.0, -3.0   # only two strong names
    fwd = pd.DataFrame(0.01, index=dates, columns=cols)
    out = TX.xs_abstain(pred, fwd, pd.Series(5.0, index=cols))
    W = out["weights"]
    assert np.allclose(W[list("BCDEFG")].to_numpy(), 0.0)


def test_blocked_structures_are_named():
    assert "CALENDAR_BUTTERFLY" in TX.BLOCKED_STRUCTURES
    for reason in TX.BLOCKED_STRUCTURES.values():
        assert len(reason) > 20
    # sector-neutral was UNBLOCKED by the phase-24 sectors axis and is
    # executed, with the corrected claim recorded rather than rewritten
    assert "SECTOR_NEUTRAL_EQUITY" not in TX.BLOCKED_STRUCTURES
    assert "XS_LS_SECTOR_NEUTRAL" in TX.EXPRESSION_CONTROLS_EXT
    assert "phase-24" in TX.SECTOR_NEUTRAL_UNBLOCKED_BY


# --------------------------------------------------------------------------- #
# Track G adapters
# --------------------------------------------------------------------------- #
def _xy(n=400, f=6, seed=0):
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n, f))
    y = X[:, 0] * 0.1 + rng.normal(0, 1, n)
    return X, y


def test_mlp_and_calibrated_and_quantile_adapters():
    for name in ("mlp", "calibrated_sign", "quantile_blend"):
        m = MX.make_ext_adapter(name)
        X, y = _xy()
        m.fit(X, y)
        p = m.predict(X)
        assert np.asarray(p).shape == (len(X),)
        assert np.isfinite(p).all()


def test_sequence_lags_are_time_ascending_feature_major():
    panel = pd.DataFrame({
        "market_id": ["M"] * 15,
        "decision_date": pd.date_range("2010-01-31", periods=15,
                                       freq="ME"),
        "f1": np.arange(15, dtype=float)})
    out, seq = MX.add_sequence_lags(panel, ["f1"], n_lags=3)
    assert seq == ["f1_lag3", "f1_lag2", "f1_lag1", "f1"]
    last = out.sort_values("decision_date").iloc[-1]
    assert [last[c] for c in seq] == [11.0, 12.0, 13.0, 14.0]


def _torch_available() -> bool:
    try:
        MX._torch()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _torch_available(),
                    reason="CPU torch not present on this machine")
def test_seq_and_ssl_adapters_fit_predict():
    n_feats, steps = 3, MX.SEQ_N_LAGS + 1
    rng = np.random.default_rng(1)
    X = rng.normal(size=(300, n_feats * steps))
    y = rng.normal(size=300)
    for kind in ("tcn_seq", "gru_seq"):
        m = MX.make_ext_adapter(kind, n_feats=n_feats)
        m.fit(X, y)
        p = m.predict(X[:50])
        assert p.shape == (50,) and np.isfinite(p).all()
    ssl = MX.make_ext_adapter("ssl_embed_ridge")
    ssl.fit(X, y)
    p = ssl.predict(X[:20])
    assert p.shape == (20,) and np.isfinite(p).all()


# --------------------------------------------------------------------------- #
# Track E plumbing
# --------------------------------------------------------------------------- #
def test_roll_z_and_market_breadth():
    s = pd.Series(np.arange(60, dtype=float),
                  index=pd.date_range("2010-01-08", periods=60, freq="W"))
    z = IE._roll_z(s, 20, 10)
    assert z.iloc[:8].isna().all()
    assert np.isfinite(z.iloc[-1])
    agg = pd.DataFrame({
        "symbol": ["AAA"] * 40 + ["BBB"] * 40,
        "month": list(pd.date_range("2010-01-31", periods=40,
                                    freq="ME")) * 2,
        "n_P": 3, "n_S": 1, "dollars_P": 10.0, "dollars_S": 5.0})
    breadth = IE.insider_market_series(agg)
    assert breadth.name == "insider_net_breadth_z"


def test_join_weekly_is_as_of_backward_with_lag():
    fut = pd.DataFrame({
        "market_id": ["M"] * 3,
        "decision_date": pd.to_datetime(["2015-01-30", "2015-02-27",
                                         "2015-03-31"])})
    feats = pd.DataFrame(
        {"x": [1.0, 2.0]},
        index=pd.to_datetime(["2015-02-25", "2015-03-25"]))
    out = IE.join_weekly_to_fut(fut, feats, ("x",))
    assert np.isnan(out["x"].iloc[0])          # nothing observable yet
    assert out["x"].iloc[1] == 1.0             # latest available value
    assert out["x"].iloc[2] == 2.0


def test_subsplit_boundary_is_declared():
    assert IE.SUBSPLIT_FIT_END == "2012-12-31"


# --------------------------------------------------------------------------- #
# Track H shadow mechanics (pure functions)
# --------------------------------------------------------------------------- #
def test_shadow_declarations():
    assert RS.PROMOTION_ALLOWED is False
    assert RS.HISTORICAL_QUALIFICATION == "FAIL"
    assert len(RS.SHADOWS) == 3
    ids = {s["candidate_id"] for s in RS.SHADOWS}
    assert ids == {"c39_c9233eccaa74", "c39_8278ddd2d3b9",
                   "c39_0574796699fa"}


def test_apply_frozen_wide_is_the_declared_affine_map():
    frozen = {"impute_median": [0.0, 1.0], "standardise_mu": [0.0, 0.0],
              "standardise_sd": [1.0, 2.0], "coef": [2.0, -1.0],
              "intercept": 0.5}
    X = np.array([[1.0, np.nan]])
    # nan -> median 1.0 -> z = 0.5 ; 1.0 -> z = 1.0
    out = RS.apply_frozen_wide(frozen, X)
    assert out[0] == pytest.approx(2.0 * 1.0 - 1.0 * 0.5 + 0.5)


def test_eligibility_refuses_dates_at_or_before_freeze():
    registry = {"frozen_at": "2026-08-22T00:00:00Z"}
    panel = pd.DataFrame({"decision_date": pd.to_datetime(
        ["2026-07-31", "2026-08-22", "2026-08-31", "2026-09-30"])})
    out = RS.eligible_new_decisions(registry, panel, set(), "FUT")
    assert [str(d.date()) for d in out] == ["2026-08-31", "2026-09-30"]
    out2 = RS.eligible_new_decisions(registry, panel, {"2026-08-31"},
                                     "FUT")
    assert [str(d.date()) for d in out2] == ["2026-09-30"]


def test_target_snapshot_builds_self_financed_terciles():
    sh = {"shadow_id": "shadow_carry_rule_xs",
          "candidate_id": "c39_8278ddd2d3b9",
          "lane": "FUT", "model": "rule:carry_slope_ann",
          "expression": "XS_LONG_SHORT", "horizon_sessions": 21}
    panel = pd.DataFrame({
        "decision_date": [pd.Timestamp("2026-08-31")] * 9,
        "market_id": list("ABCDEFGHI"),
        "carry_slope_ann": np.linspace(-1, 1, 9)})
    snap = RS._target_snapshot(sh, panel, pd.Timestamp("2026-08-31"),
                               None)
    w = pd.Series(snap["weights"])
    assert snap["forward_evidence_type"] == "TRUE_FORWARD"
    assert snap["promotion_allowed"] is False
    assert w.sum() == pytest.approx(0.0, abs=1e-6)   # 8dp-rounded weights
    assert w.abs().sum() == pytest.approx(1.0, abs=1e-6)


# --------------------------------------------------------------------------- #
# Track I sequential design
# --------------------------------------------------------------------------- #
def test_e_process_is_anytime_valid_shaped():
    rng = np.random.default_rng(3)
    null = rng.normal(0.0, 0.015, 240)
    drift = rng.normal(0.01, 0.015, 240)
    e_null = PD.e_process(null, sigma0=0.015)["e_value"]
    e_drift = PD.e_process(drift, sigma0=0.015)["e_value"]
    assert e_drift > 100 * e_null
    assert e_null < PD.E_SUCCESS


def test_decide_boundaries():
    rng = np.random.default_rng(4)
    strong = rng.normal(0.02, 0.01, 120)
    d = PD.decide(strong, sigma0=0.01, shadow_id="shadow_wide_xs")
    assert d["decision_state"] == "SUCCESS_BOUNDARY_CROSSED"
    flatneg = rng.normal(-0.01, 0.01, 60)
    d2 = PD.decide(flatneg, sigma0=0.01, shadow_id="shadow_wide_xs")
    assert d2["decision_state"] in ("FAILURE_BOUNDARY_CROSSED",
                                    "HORIZON_REACHED_WITHOUT_DECISION")
    d3 = PD.decide([0.001] * 5, sigma0=0.01, shadow_id="shadow_wide_xs")
    assert d3["decision_state"] == "ACCUMULATING"


def test_design_freeze_writes_registered_boundaries(research_root):
    body = PD.freeze()
    assert body["registered_before_first_forward_observation"] is True
    rule = body["sequential_rule"]
    assert rule["anytime_valid"] is True
    assert rule["success_boundary_e"] == 20.0
    wide = body["designs"]["shadow_wide_xs"]
    assert wide["expected_effect_registered_annualised"] < \
        wide["expected_effect_point_estimate_annualised"]
    assert wide["minimum_useful_sample_months"][
        "to_detect_point_estimate_80pct_power"] > 100
