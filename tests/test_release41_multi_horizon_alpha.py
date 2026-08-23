"""Release 41 - Multi-Horizon Alpha Breakthrough Campaign: regression.

Targeted, hermetic tests: contract immutability properties, inference
correctness on synthetic data, burden-ledger arithmetic on a temp root,
curve/book construction invariants, acquisition decoding, forward-freeze
honesty, audit-guard wiring. No network, no Norgate, no research-drive
writes (temp roots via the env seam).
"""
from __future__ import annotations

import json
import lzma
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from paper_trader.alpha_agent import r41
from paper_trader.alpha_agent.r41 import contract as C
from paper_trader.alpha_agent.r41 import evidence as EV


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
def test_contract_burden_inheritance_constants():
    assert C.GLOBAL_INHERITED_EFFECTIVE_TRIALS == 230
    assert C.BURDEN_NEVER_RESETS and C.NO_CAMPAIGN_ID_LAUNDERING
    assert C.R40_EXPECTED["cumulative_effective_trials"] == 230
    assert C.R40_EXPECTED["n_research_shadows"] == 5


def test_contract_hash_is_stable_and_covers_gates():
    h1, h2 = C.contract_hash(), C.contract_hash()
    assert h1 == h2 and len(h1) == 64
    assert C.RESEARCH_CANDIDATE_GATE["after_cost_excess_t_hac_min"] == 2.0
    assert C.QUALIFIED_ALPHA_GATE[
        "deflated_sharpe_at_family_burden_min"] == 0.95
    assert C.ZONE_C_PREGATE_T == 2.5


def test_contract_safety_refusals():
    assert not C.MAY_SPEND_MONEY and not C.MAY_PURCHASE_DATA
    assert not C.MAY_CREATE_PROVIDER_ACCOUNT
    assert not C.MAY_SEND_VENDOR_EMAIL
    assert not C.MAY_ENABLE_SCHEDULED_TASK and not C.MAY_RESTART_PRODUCTION
    assert not C.MAY_PROMOTE_MODEL and not C.MAY_CHANGE_HOLDINGS
    assert not C.MAY_PURCHASE_COMPUTE and not C.MAY_INSTALL_CUDA


def test_fib_levels_and_placebo_disjoint():
    named = set(C.FIB_NAMED_LEVELS)
    placebo = set(C.FIB_PLACEBO_LEVELS)
    assert len(named) == len(placebo) == 7
    assert not named & placebo
    assert C.NO_HINDSIGHT_EXTREMA and C.NO_HUMAN_VISUAL_CONFIRMATION


def test_blocker_vocabulary_complete():
    for b in ("PAYMENT_REQUIRED", "ACCOUNT_REQUIRED", "LICENCE_REQUIRED",
              "PIT_INTEGRITY_FAILURE", "SURVIVORSHIP_FAILURE",
              "COMPUTE_REQUIRES_OPERATOR_SPEND", "FUTURE_TIME_REQUIRED"):
        assert b in C.BLOCKER_VOCAB
    assert C.A_FAILED_CANDIDATE_IS_A_ROUTING_EVENT


def test_horizon_contract_no_interpolated_intraday():
    assert C.NO_INTERPOLATED_INTRADAY
    assert C.HORIZON_REQUIRES_NATIVE_SOURCE_FREQUENCY
    assert C.HORIZON_SESSIONS["21s"] == 21
    assert C.HORIZON_MINUTES["4h"] == 240
    assert C.DECISION_CADENCE_IS_A_CANDIDATE_PROPERTY
    assert C.SYSTEM_IS_NOT_MONTHLY


# --------------------------------------------------------------------------- #
# Evidence
# --------------------------------------------------------------------------- #
def test_hac_t_on_iid_noise_is_moderate():
    rng = np.random.default_rng(7)
    ts = [EV.hac_t(rng.normal(0, 1, 500), lags=4)["t"] for _ in range(40)]
    assert np.mean(np.abs(ts)) < 2.0          # no systematic inflation


def test_hac_t_detects_real_mean():
    rng = np.random.default_rng(7)
    r = EV.hac_t(rng.normal(0.5, 1, 500), lags=4)
    assert r["t"] > 5.0


def test_zone_split_proportions_and_embargo():
    dates = pd.bdate_range("2000-01-03", periods=1000)
    z = EV.zone_split(dates, embargo=21)
    assert abs(len(z["A"]) - 500) <= 1
    assert z["A"][-1] < z["B"][0]
    assert (z["B"][0] - z["A"][-1]).days > 21
    assert z["B"][-1] < z["C"][0]


def test_scorecard_cost_stress_monotone_and_annualisation():
    rng = np.random.default_rng(3)
    g = rng.normal(0.001, 0.01, 600)
    k = np.full(600, 0.0002)
    card = EV.scorecard(g, k, np.zeros(600), periods_per_year=252.0,
                        overlap=5)
    cs = card["cost_stress"]
    assert cs["x1"]["excess_ann"] > cs["x2"]["excess_ann"] \
        > cs["x3"]["excess_ann"]
    assert card["vol_ann"] == pytest.approx(0.01 * np.sqrt(252), rel=0.2)
    assert card["hac_lags"] == 5


def test_effective_sample_shrinks_under_autocorrelation():
    rng = np.random.default_rng(5)
    x = rng.normal(0, 1, 800)
    smooth = pd.Series(x).rolling(10, min_periods=1).mean().to_numpy()
    ess = EV.effective_sample(smooth)
    assert ess["ess"] < 800 * 0.35
    iid = EV.effective_sample(x)
    assert iid["ratio"] <= 1.0                # capped


def test_research_candidate_gate_logic():
    good = {"excess_t_hac": 2.5, "same_sign_halves": True,
            "cost_stress": {"x2": {"excess_ann": 0.01}},
            "effective_sample": {"ess": 100}}
    assert EV.research_candidate_gate(good)["passes"]
    bad = dict(good, excess_t_hac=1.0)
    assert not EV.research_candidate_gate(bad)["passes"]
    assert not EV.research_candidate_gate(
        good, residual_t=1.0)["passes"]
    assert not EV.research_candidate_gate(
        good, kill_no_flip=False)["passes"]


def test_factor_residual_removes_beta_not_alpha():
    rng = np.random.default_rng(11)
    f = rng.normal(0, 0.01, 700)
    d = 0.001 + 0.8 * f + rng.normal(0, 0.005, 700)
    res = EV.factor_residual(d, pd.DataFrame({"F": f}))
    assert res["state"] == "OK"
    assert res["betas"]["F"] == pytest.approx(0.8, abs=0.1)
    assert res["alpha_t_hac"] > 3.0


# --------------------------------------------------------------------------- #
# Burden ledger (temp root)
# --------------------------------------------------------------------------- #
def test_burden_ledger_counts_distinct_not_touches(tmp_path, monkeypatch):
    monkeypatch.setenv(r41.RESEARCH_ROOT_ENV, str(tmp_path))
    from paper_trader.alpha_agent.r41 import burden as B
    spec = {"information_family": "RATES_RV", "asset_family": "X",
            "horizon": "5s", "economic_expression": "RV",
            "representation": "R", "model": "M",
            "hyperparameter_budget": 1, "parent_hypotheses": [],
            "validation_touches": 1}
    cid1 = B.record_zone_b(spec, family="RATES_RV",
                           campaign_id="r41_test_campaign")
    cid2 = B.record_zone_b(spec, family="RATES_RV",
                           campaign_id="r41_test_campaign")
    assert cid1 == cid2
    s = B.summary("r41_test_campaign")
    assert s["r41_distinct_zone_b_candidates"] == 1
    assert s["zone_b_evaluations"] == 2
    assert s["global_cumulative"] == 231
    assert s["never_reset"]
    with pytest.raises(ValueError):
        B.record_zone_b(spec, family="NOT_A_FAMILY",
                        campaign_id="r41_test_campaign")


# --------------------------------------------------------------------------- #
# Book construction
# --------------------------------------------------------------------------- #
def _toy_structs(n=300, seed=2):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2015-01-01", periods=n)
    out = {}
    for name in ("S1", "S2"):
        s = pd.Series(rng.normal(0, 0.001, n), index=idx)
        out[name] = {"kind": "SPREAD", "legs": (name,), "tag": name,
                     "spread": s, "gross": pd.Series(2.0, index=idx),
                     "vol": s.rolling(60, min_periods=30).std().shift(1),
                     "carry": pd.Series(0.01, index=idx),
                     "legs_meta": [(name + "a", 1.0, 1.0),
                                   (name + "b", 1.0, 1.0)],
                     "countries": [name]}
    return out, idx


def test_book_stream_gross_notional_and_tranching():
    from paper_trader.alpha_agent.r41.rates_rv_lab import book_stream
    structs, idx = _toy_structs()
    pos = {n: pd.Series(1.0, index=idx) / structs[n]["vol"]
           for n in structs}
    st = book_stream(structs, pos, horizon=21)
    W = st["weights"]
    G = pd.DataFrame({n: structs[n]["gross"] for n in structs})
    gross_notional = (W.abs() * G).sum(axis=1)
    assert gross_notional.dropna().max() <= 1.0 + 1e-9
    st1 = book_stream(structs, pos, horizon=1)
    assert st["turnover"].mean() <= st1["turnover"].mean() + 1e-12
    assert (st["cost"].dropna() >= 0).all()


# --------------------------------------------------------------------------- #
# Acquisition decoding (synthetic bytes, no network)
# --------------------------------------------------------------------------- #
def test_dukascopy_tick_decode_roundtrip():
    from paper_trader.alpha_agent.r41 import sample_acquisition as SA
    raw = np.zeros(3, dtype=SA.TICK_DTYPE)
    raw["ms"] = [100, 60_100, 120_000]
    raw["ask"] = [110150, 110160, 110170]
    raw["bid"] = [110140, 110150, 110160]
    raw["av"] = [1.0, 2.0, 3.0]
    blob = lzma.compress(raw.tobytes())
    ms, ask, bid, av, bv = SA.decode_ticks(blob, 1e5)
    assert ask[0] == pytest.approx(1.10150)
    assert bid[2] == pytest.approx(1.10160)
    import datetime as dt
    bars = SA.ticks_to_minute_bars(dt.date(2024, 1, 2), 10,
                                   (ms, ask, bid, av, bv))
    assert len(bars) == 3                      # minutes 0, 1, 2
    assert (bars["spread_mean"] > 0).all()


def test_dukascopy_candle_dtype_shape():
    from paper_trader.alpha_agent.r41 import sample_acquisition as SA
    assert SA.CANDLE_DTYPE.itemsize == 24
    assert SA.DUKA_SCALE["EURUSD"] == 1e5
    assert SA.DUKA_SCALE["USA500IDXUSD"] == 1e3


# --------------------------------------------------------------------------- #
# Forward freeze honesty (temp root)
# --------------------------------------------------------------------------- #
def test_forward_freeze_registry_shape_without_data(tmp_path, monkeypatch):
    monkeypatch.setenv(r41.RESEARCH_ROOT_ENV, str(tmp_path))
    from paper_trader.alpha_agent.r41 import forward_freeze as FF
    assert FF.MAX_R41_SHADOWS == 3
    spec = dict(FF.FUNDING_SPEC)
    assert spec["decision_cadence"].startswith("DAILY")
    # the registry write path is exercised only when the campaign root has
    # data; here we assert the spec constants and honesty flags
    assert "z_threshold" in spec and spec["z_threshold"] == 0.5


def test_capture_refuses_rows_at_or_before_freeze():
    from paper_trader.alpha_agent.r41 import forward_freeze as FF
    import inspect
    src = inspect.getsource(FF.capture)
    assert "d > frozen_at" in src
    assert "d < today" in src


# --------------------------------------------------------------------------- #
# Campaign / audit wiring
# --------------------------------------------------------------------------- #
def test_result_axes_match_contract():
    from paper_trader.alpha_agent.r41 import campaign as CAM
    import inspect
    src = inspect.getsource(CAM.build_verdict)
    for axis in C.RESULT_AXES:
        assert axis in src


def test_audit_guard_registered():
    audit = Path(__file__).resolve().parents[1] / "scripts" \
        / "audit_architecture.py"
    src = audit.read_text(encoding="utf-8")
    assert "def check_release41_multi_horizon_alpha" in src
    assert '"release41_multi_horizon_alpha":' in src
    assert src.count('("release41_multi_horizon_alpha", ') >= 19


def test_r41_forbidden_second_owners_absent():
    pkg = Path(__file__).resolve().parents[1] / "alpha_agent" / "r41"
    for name in ("economics.py", "multiple_testing.py", "zones.py",
                 "lockbox.py", "ledger.py", "purchase_gate.py",
                 "scheduler.py", "universal_state.py"):
        assert not (pkg / name).exists(), name


def test_safety_block_flags():
    sb = r41.safety_block()
    assert sb["may_spend_money"] is False
    assert sb["purchases_data"] is False
    assert sb["changes_scheduler"] is False
    assert sb["backdates_forward_rows"] is False
    assert sb["trains_a_model"] is True
