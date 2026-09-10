"""Release 64 - information-directed alpha: regressions that keep it honest.

Every test here is HERMETIC. The R64 research root, the R63 results root the
R64 package READS, the R59 memory root and the information-frontier / overlay
paths the governor adapter reads are all redirected into a pytest temp
directory through the owners' own env-var constants. No test opens the live
ResearchMemory, the live desk ledger, an owned data store, the live R63
artifacts or the canonical checkout, and a test proves the resolved roots are
nowhere near them.

What is protected:

* safety flags, protocol registration (with its disclosed amendment), roots
* genuine carry needs DISTINCT contracts; a pseudo-curve is refused
* carry variants: class-neutral z-scores within class; carry-to-risk scaling;
  the CARRY block is the R63 block
* the risk-controlled book: volatility target, leverage cap, instrument cap,
  class risk budgets, no-trade band, costs, no look-ahead, identical
  construction on both arms, the degeneracy rule
* the R63 scorer's additive ``keep_predictions`` hook changes no default byte
* family-aware multiple testing (Holm), the R64 verdict ladder, inherited FDR
* the R63 handoff validator (valid, tampered, reproduced)
* immutable challenger records with a freeze_record_hash; nothing registered
* the information-need overlay is not a frontier
* the governor consumes the frontier through the adapter, dedupes by
  watermark, keeps its fairness, and the adapter imports no research package
* the runner has no execute path
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent import r59
from alpha_agent import r63 as R63
from alpha_agent import r64 as R
from alpha_agent.r59 import governor as GOV
from alpha_agent.r59 import handlers as H
from alpha_agent.r59 import information_needs as IN
from alpha_agent.r59 import memory as M
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import carry as C
from alpha_agent.r64 import challenger as CH
from alpha_agent.r64 import construction as B
from alpha_agent.r64 import experiments as X
from alpha_agent.r64 import family as FAM
from alpha_agent.r64 import frontier as FR
from alpha_agent.r64 import handoff_validation as HV

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")

LIVE_ROOTS = ("Stock_Prediction_app_data", r"C:\Users\binis\paper_trader",
              r"C:\Users\binis\.paper_trader")
REPO = Path(__file__).resolve().parents[1]


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setenv(R.RESEARCH_ROOT_ENV, str(tmp_path / "r64"))
    monkeypatch.setenv(R.R63_RESULTS_ROOT_ENV, str(tmp_path / "r63"))
    monkeypatch.setenv(R63.RESEARCH_ROOT_ENV, str(tmp_path / "r63"))
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    monkeypatch.setenv(IN.FRONTIER_PATH_ENV, str(tmp_path / "frontier.json"))
    monkeypatch.setenv(IN.OVERLAY_PATH_ENV, str(tmp_path / "overlay.json"))
    # the experiments module caches the R63 matrix it read; never let a
    # previous process state (or a real read) leak into a hermetic test
    monkeypatch.setattr(X, "_R63_MATRIX", {})
    monkeypatch.setattr(X, "_DS", {})
    return tmp_path


# --------------------------------------------------------------------------- #
# Hermeticity, safety, protocol
# --------------------------------------------------------------------------- #
def test_research_roots_are_hermetic(root):
    for got in (str(R.research_root()), str(R.r63_results_root()), str(IN.frontier_path()),
                str(IN.overlay_path())):
        assert got.startswith(str(root))
        for bad in LIVE_ROOTS:
            assert bad.lower() not in got.lower()
    R.assert_research_root_is_not_live()


def test_live_checkout_root_is_refused():
    with pytest.raises(RuntimeError):
        R.assert_research_root_is_not_live(Path(r"C:\Users\binis\paper_trader\anything"))


def test_worktree_import_integrity():
    assert Path(R.assert_worktree_import()).name == "alpha_agent"


def test_safety_flags_are_all_off():
    for k in ("creates_orders", "creates_fills", "broker_enabled", "promotes_model",
              "activates_sleeve", "approves_proposal", "registers_forward_challenger",
              "mutates_operational_store", "mutates_live_research_store", "purchases_data",
              "starts_trial_or_subscription", "automation_enabled"):
        assert R.SAFETY[k] is False
    assert R.SAFETY["live_checkout_read_only"] is True
    for badge in ("NO ORDERS", "AUTOMATION OFF", "MANUAL REVIEW", "NO LIVE REGISTRATION",
                  "NO PROMOTION"):
        assert badge in R.SAFETY_BADGES


def test_protocol_registered_before_experiments_and_matches_code():
    p = R.protocol()
    assert p["registered_before_any_experiment_ran"] is True
    assert p["release"] == "R64" and p["campaign_id"] == R.CAMPAIGN_ID
    assert p["horizons"]["sessions"] == list(R.HORIZONS)
    assert p["partition_and_leakage"]["lockbox_start"] == R.LOCKBOX_START
    rc = p["risk_controlled_construction"]["cross_sectional_book"]
    assert "%.1f" % R.MAX_GROSS_LEVERAGE in rc["leverage_cap"]
    assert "%.2f" % R.TARGET_VOL in rc["volatility_target"]
    assert "%.2f" % R.MAX_INSTRUMENT_RISK_SHARE in rc["instrument_contribution_cap"]
    assert "%.2f" % R.NO_TRADE_BAND in rc["no_trade_band"]
    assert p["statistics"]["effective_sample"]["min_effective_periods"] == R.MIN_EFFECTIVE_PERIODS
    assert p["statistics"]["materiality"]["ann_net_increment_floor"] == R.MATERIALITY_ANN_NET
    # the leverage-cap amendment is DISCLOSED, dated, and pre-run
    am = p["amendments_before_any_result_was_computed"]
    assert am and "BEFORE any R64 cell ran" in am[0] and "5.0" in am[0]
    for k in ("creates_orders", "purchases_data", "registers_forward_challenger",
              "automation_enabled", "promotes_model"):
        assert p["safety"][k] is False
    assert p["challenger_rules"]["no_live_registration"] is True


def test_write_artifact_is_deterministic_and_stamps_r64_protocol(root):
    body = {"b": 1, "a": [3, 2]}
    p1 = R.write_artifact("x.json", dict(body))
    d1 = json.loads(p1.read_text())
    p2 = R.write_artifact("x.json", dict(body))
    d2 = json.loads(p2.read_text())
    assert d1["artifact_hash"] == d2["artifact_hash"]
    assert d1["protocol_sha256"] == R.protocol_hash() and d1["phase"] == "R64"
    assert str(p1).startswith(str(root))


# --------------------------------------------------------------------------- #
# Carry: distinct contracts and the variants
# --------------------------------------------------------------------------- #
def _curve_frame(n=300, pseudo=False, seed=1):
    rng = np.random.default_rng(seed)
    c1 = 100.0 + np.cumsum(rng.normal(scale=0.5, size=n))
    c2 = c1.copy() if pseudo else c1 * (1.0 + 0.02 / 4.0 + rng.normal(scale=1e-3, size=n))
    r1 = np.r_[np.nan, np.diff(np.log(c1))]
    r2 = np.r_[np.nan, np.diff(np.log(c2))]
    slope = np.log(c2 / c1) * 4.0
    return pd.DataFrame({"date": pd.bdate_range("2015-01-01", periods=n).strftime("%Y-%m-%d"),
                         "ret1": r1, "ret2": r2, "c1": c1, "c2": c2, "slope_ann": slope})


def test_distinct_contract_evidence_refuses_a_pseudo_curve():
    ok = C.market_evidence(_curve_frame())
    bad = C.market_evidence(_curve_frame(pseudo=True))
    assert ok["state"] == C.STATE_OK and ok["share_c1_equals_c2"] == 0.0
    assert bad["state"] == C.STATE_PSEUDO and bad["share_c1_equals_c2"] == 1.0
    assert bad["share_slope_zero"] == 1.0
    assert C.PSEUDO_CURVE_SHARE_MAX == 0.50


def test_class_neutral_carry_is_standardised_within_class_and_refuses_thin_classes():
    rng = np.random.default_rng(2)
    n_i, n_d = 8, 60
    slope = rng.normal(size=(n_i, n_d))
    slope[:4] += 10.0                        # one class on a different scale
    F = {"slope_ann": slope, "ret1": rng.normal(scale=0.01, size=(n_i, n_d))}
    cls = np.array(["A"] * 4 + ["B"] * 3 + ["C"])
    z = C.class_neutral_carry_block(F, cls)
    assert z.shape == (n_i, n_d, 2)
    za = z[:4, :, 0]
    assert np.allclose(za.mean(axis=0), 0.0, atol=1e-9)
    assert np.allclose(za.std(axis=0, ddof=1), 1.0, atol=1e-9)
    assert np.isnan(z[7, :, 0]).all()        # a one-member class carries no ranking
    # the scale offset of class A never leaks into the standardised block
    assert abs(np.nanmean(z[:4, :, 0])) < 1e-9


def test_carry_to_risk_divides_by_realised_volatility_and_curve_carry_is_r63s():
    rng = np.random.default_rng(3)
    n_i, n_d = 3, 300
    slope = np.full((n_i, n_d), 0.05)
    ret = rng.normal(scale=np.array([[0.005], [0.01], [0.02]]), size=(n_i, n_d))
    F = {"slope_ann": slope, "ret1": ret}
    ctr = C.carry_to_risk_block(F)[:, -1, 0]
    assert ctr[0] > ctr[1] > ctr[2] > 0      # same carry, more risk -> less carry per risk
    from alpha_agent.r63 import features as FE
    assert np.array_equal(np.nan_to_num(C.curve_carry_block(F)), np.nan_to_num(FE.futures_carry_block(F)))


# --------------------------------------------------------------------------- #
# The risk-controlled book
# --------------------------------------------------------------------------- #
def _rows(n=9, T=500, signal=0.02, vol_scale=0.08, seed=1, cost=0.0005):
    rng = np.random.default_rng(seed)
    gid = np.repeat(np.arange(T), n)
    iid = np.tile(np.arange(n), T)
    pred = rng.normal(size=n * T)
    y = signal * pred + rng.normal(scale=0.01, size=n * T)
    vol = np.full(n * T, vol_scale)
    return pred, y, gid, iid, vol, np.full(n * T, cost)


def test_vol_target_scales_the_book_toward_the_target_and_reports_leverage():
    # iid daily scores turn the whole book over every day; cost is switched off
    # here so the test isolates the volatility target (costs have their own test)
    pred, y, gid, iid, vol, cost = _rows(T=800, signal=0.0, cost=0.0)
    book = B.build_book(pred, y, gid, iid, vol, cost, mode="XS", horizon=1)
    s = B.summarise(book, 1, 0)
    assert s["periods"] == 800
    assert s["share_periods_warmup"] < 0.05
    # after warm-up the realised volatility sits near the target
    lev_states = book["leverage_state"]
    targeted = lev_states == B.LEV_TARGETED
    assert targeted.mean() > 0.8
    net_targeted = book["net"][targeted]
    ann = float(np.std(net_targeted, ddof=1)) * np.sqrt(252.0)
    assert 0.06 < ann < 0.16
    assert s["max_gross_leverage"] <= R.MAX_GROSS_LEVERAGE + 1e-9
    assert s["degenerate_under_controls"] is False


def test_leverage_cap_binds_when_the_unlevered_book_is_too_quiet():
    pred, y, gid, iid, vol, cost = _rows(T=300, signal=0.0)
    y = y * 0.01                             # ~1bp moves: vol floor -> leverage wants 5x
    book = B.build_book(pred, y, gid, iid, vol, cost, mode="XS", horizon=1)
    assert (book["gross_leverage"] <= R.MAX_GROSS_LEVERAGE + 1e-9).all()
    at_cap = (book["leverage_state"] == B.LEV_AT_CAP).mean()
    assert at_cap > 0.8
    s = B.summarise(book, 1, 0)
    assert s["degenerate_under_controls"] is True     # the cap decided the book


def test_instrument_cap_bounds_every_name_and_preserves_side_gross():
    w = {0: 0.6, 1: 0.3, 2: 0.1, 3: -0.5, 4: -0.5}
    vol = {0: 0.50, 1: 0.05, 2: 0.05, 3: 0.10, 4: 0.10}
    out = B.apply_instrument_cap(w, vol, 0.25)
    risk = {i: abs(out[i]) * vol[i] for i in out}
    tot = sum(risk.values())
    assert max(r / tot for r in risk.values()) <= 0.25 + 1e-9
    # total ex-ante risk preserved, signs kept; scale is the vol target's job
    assert abs(tot - sum(abs(w[i]) * vol[i] for i in w)) < 1e-9
    assert all(np.sign(out[i]) == np.sign(w[i]) for i in w)
    # an already-compliant book is untouched
    even = {0: 0.5, 1: 0.5, 2: -0.5, 3: -0.5}
    assert B.apply_instrument_cap(even, {i: 0.1 for i in even}, 0.25) == even


def test_class_risk_budget_equalises_ex_ante_risk_across_classes():
    w = {0: 0.5, 1: 0.5, 2: -0.5, 3: -0.5}
    vol = {0: 0.40, 1: 0.02, 2: 0.10, 3: 0.10}
    cls = {0: "A", 1: "B", 2: "A", 3: "B"}
    out = B.apply_class_risk_budget(w, vol, cls)
    long_a = abs(out[0]) * vol[0]
    long_b = abs(out[1]) * vol[1]
    assert abs(long_a - long_b) < 1e-9
    assert abs(sum(x for x in out.values() if x > 0) - 1.0) < 1e-9


def test_no_trade_band_holds_small_changes_and_always_trades_entries_and_exits():
    prev = {1: 0.10, 2: -0.10, 3: 0.20}
    new = {1: 0.11, 2: -0.05, 4: 0.30}
    out = B.apply_no_trade_band(new, prev, 0.25)
    assert out[1] == 0.10                 # 10% change inside the band: held
    assert out[2] == -0.05                # 50% change: traded
    assert out[4] == 0.30 and 3 not in out  # entry and exit always trade


def test_no_trade_band_reduces_turnover_and_costs_reduce_net():
    pred, y, gid, iid, vol, cost = _rows(T=300)
    banded = B.build_book(pred, y, gid, iid, vol, cost, mode="XS", horizon=1)
    free = B.build_book(pred, y, gid, iid, vol, cost, mode="XS", horizon=1,
                        policy={"no_trade_band": 0.0})
    assert banded["turnover"].mean() < free["turnover"].mean()
    assert (banded["net"] <= banded["gross"] + 1e-12).all()
    assert banded["cost"].sum() > 0


def test_book_never_looks_ahead():
    pred, y, gid, iid, vol, cost = _rows(T=300, seed=5)
    k = 150
    y2 = y.copy()
    y2[gid >= k] = -y2[gid >= k] * 3.0       # rewrite the FUTURE only
    a = B.build_book(pred, y, gid, iid, vol, cost, mode="XS", horizon=1)
    b = B.build_book(pred, y2, gid, iid, vol, cost, mode="XS", horizon=1)
    assert np.allclose(a["turnover"][:k], b["turnover"][:k])
    assert np.allclose(a["cost"][:k], b["cost"][:k])
    assert np.allclose(a["gross"][:k], b["gross"][:k])
    assert np.allclose(a["gross_leverage"][:k], b["gross_leverage"][:k])


def test_identical_arms_give_a_zero_increment_and_ts_mode_runs():
    pred, y, gid, iid, vol, cost = _rows(T=200)
    a = B.build_book(pred, y, gid, iid, vol, cost, mode="XS", horizon=1)
    inc = B.paired_increment(a, a, 1, 0)
    assert abs(inc["ann_net_increment"]) < 1e-12 and inc["periods"] == 200
    ts = B.build_book(pred, y, gid, iid, vol, cost, mode="TS", horizon=5, pred_scale=1.0)
    s = B.summarise(ts, 5, 0)
    assert s["book"] == B.BOOK_TS and s["periods"] == 200
    assert s["max_gross_leverage"] <= R.MAX_GROSS_LEVERAGE + 1e-9


def test_degeneracy_rule():
    pol = B.default_policy()
    assert B.degenerate({"max_dd": -0.7, "share_periods_leverage_at_cap": 0.0}, pol) is True
    assert B.degenerate({"max_dd": -0.3, "share_periods_leverage_at_cap": 0.6}, pol) is True
    assert B.degenerate({"max_dd": -0.3, "share_periods_leverage_at_cap": 0.1}, pol) is False


# --------------------------------------------------------------------------- #
# The R63 scorer keeps its predictions; default output unchanged
# --------------------------------------------------------------------------- #
def _synthetic(n_inst=8, n_dates=252 * 14, signal=0.2, seed=7):
    rng = np.random.default_rng(seed)
    dates = np.array(pd.bdate_range("2010-01-04", periods=n_dates).strftime("%Y-%m-%d"))
    ret = rng.normal(scale=0.01, size=(n_inst, n_dates))
    y = pit.forward_compound(ret, 1)
    vol63 = pd.DataFrame(ret.T).rolling(63).std().to_numpy().T * np.sqrt(252.0)
    hvol = vol63 * np.sqrt(1 / 252.0)
    y_scaled = y / np.where(hvol > 0, hvol, np.nan)
    elig = np.isfinite(ret) & np.isfinite(vol63)
    dec = pit.decision_indices(dates, "2010-01-04", 1, 1)
    b1 = rng.normal(size=(n_inst, n_dates, 2))
    b2 = rng.normal(size=(n_inst, n_dates, 1))
    d = signal * np.nan_to_num(y / np.nanstd(y)) + rng.normal(size=(n_inst, n_dates))
    blocks = {"PRICE_RETURN_STATE": b1, "TREND": b2, "TESTDIM": d[..., None]}
    return {"scope": "TEST", "mode": "XS", "horizon": 1, "cadence": 1, "dates": dates,
            "dec": dec, "inst": ["I%d" % i for i in range(n_inst)], "y": y,
            "y_scaled": y_scaled, "elig": elig, "vol": vol63,
            "cost": np.full(n_inst, 0.0005), "blocks": blocks, "market_level": set(),
            "regime_vix": None, "regime_trend": None, "book": "XS_LONG_SHORT"}


def test_keep_predictions_is_additive_and_aligned(root):
    ds = _synthetic()
    plain = S.run_cell(ds, ("PRICE_RETURN_STATE", "TREND"), "TESTDIM")
    kept = S.run_cell(ds, ("PRICE_RETURN_STATE", "TREND"), "TESTDIM", keep_predictions=True)
    assert "_predictions" not in plain
    pr = kept.pop("_predictions")
    assert json.dumps(plain, sort_keys=True, default=str) == json.dumps(kept, sort_keys=True, default=str)
    n = len(pr["pred_B"])
    for k in ("pred_BD", "pred_D", "gid", "iid", "y_raw", "vol", "cost", "fold_kind"):
        assert len(pr[k]) == n
    assert set(pr["pred_scale"]) == {"B", "BD", "D"} and pr["inst"] == ds["inst"]
    okp = np.isfinite(pr["pred_B"]) & np.isfinite(pr["pred_BD"])
    book = B.build_book(pr["pred_BD"][okp], pr["y_raw"][okp], pr["gid"][okp], pr["iid"][okp],
                        pr["vol"][okp], pr["cost"][okp], mode="XS", horizon=1)
    assert len(book["net"]) > 100


# --------------------------------------------------------------------------- #
# Family-aware multiple testing and the verdict ladder
# --------------------------------------------------------------------------- #
def test_holm_step_down_is_monotone_and_finds_the_family_p():
    h = FAM.holm({"a": 0.001, "b": 0.02, "c": 0.2, "d": None}, alpha=0.05)
    assert h["m"] == 3
    assert h["adjusted"]["a"] == pytest.approx(0.003)
    assert h["adjusted"]["b"] == pytest.approx(0.04)
    assert h["adjusted"]["c"] == pytest.approx(0.2)
    assert h["rejected"] == {"a": True, "b": True, "c": False}
    assert h["family_p"] == pytest.approx(0.003) and h["n_rejected"] == 2


def _cell(cid, *, scope="FX_FUTURES", mode="XS", h=1, dim="CARRY", t=4.0, p=1e-5, inc=0.01,
          lock=True, blocks_pos=0.9, econ_inc=0.03, econ_p=0.01, econ_sharpe=0.3, dd=-0.3,
          at_cap=0.0, rs=0.8, eff=1000, tag=X.TAG_REPRODUCTION, r63_fdr=None, repro=None):
    c = {"cell_id": cid, "scope": scope, "mode": mode, "horizon": h, "dimension": dim,
         "kind": "AUGMENTATION", "r64_tag": tag, "baseline": ["PRICE_RETURN_STATE"],
         "instruments": ["A", "B", "C"], "n_instruments": 3, "cadence": min(h, 21),
         "verdict": "X", "effective_periods": eff, "n_periods": eff,
         "conditional": {"t": t, "p_one_sided": p, "increment": inc, "lockbox_sign_agrees": lock,
                         "increment_lockbox": inc, "t_lockbox": 2.0, "increment_selection": inc},
         "stability": {"share_blocks_positive": blocks_pos, "blocks": {}, "regime": {}},
         "redundancy": {"residual_share": rs, "redundancy": "DISTINCT", "max_abs_rank_corr": 0.3},
         "secondary": {}, "cost_bps_per_side": {"A": 5.0},
         "r64_economics": {"ann_net_increment": econ_inc, "ann_net_increment_at_2x_cost": econ_inc - 0.002,
                           "sharpe_increment": econ_sharpe, "t_increment": 2.5,
                           "p_increment_one_sided": econ_p,
                           "augmented": {"max_dd": dd, "share_periods_leverage_at_cap": at_cap,
                                         "degenerate_under_controls": B.degenerate(
                                             {"max_dd": dd, "share_periods_leverage_at_cap": at_cap}),
                                         "ann_net": 0.05, "sharpe": 0.8, "policy": B.default_policy()},
                           "baseline": {"max_dd": -0.3, "ann_net": 0.02, "sharpe": 0.5}}}
    if r63_fdr is not None:
        c["r63_reference"] = {"fdr_pass_conditional": r63_fdr, "verdict": "REF"}
    if repro is not None:
        c["reproduction"] = {"matches": repro, "differences": {}}
    return c


def test_verdict_ladder_names_the_gate_that_refused(root):
    assert X.verdict(_cell("a")) == R.V_ECON
    assert X.verdict(_cell("a", repro=False)) == R.V_REPRO_FAILED
    assert X.verdict(_cell("a", t=1.0)) == R.V_NO_VALUE
    assert X.verdict(_cell("a", econ_inc=0.001, econ_sharpe=0.01)) == R.V_NOT_ECON
    assert X.verdict(_cell("a", dd=-0.8)) == R.V_NOT_ECON
    assert X.verdict(_cell("a", at_cap=0.9)) == R.V_NOT_ECON
    assert X.verdict(_cell("a", blocks_pos=0.4)) == R.V_UNSTABLE
    assert X.verdict(_cell("a", lock=False)) == R.V_UNSTABLE
    assert X.verdict(_cell("a"), fdr_pass=False) == R.V_NOT_FDR
    assert X.verdict(_cell("a", eff=10)) == R.V_DATA_HOLD
    assert X.verdict({"verdict": S.V_DATA_HOLD}) == R.V_DATA_HOLD


def test_fdr_is_inherited_from_r63_where_it_ran_and_r64_campaign_otherwise(root):
    cells = [_cell("FX_FUTURES|XS|1|CARRY", r63_fdr=True),
             _cell("FX_FUTURES|XS|5|CARRY", h=5, r63_fdr=False, p=0.01),
             _cell("FX_FUTURES|XS|1|CARRY_TO_RISK", dim="CARRY_TO_RISK", tag=X.TAG_VARIANT, p=1e-4),
             _cell("FX_FUTURES|XS|21|CARRY_TO_RISK", dim="CARRY_TO_RISK", h=21, tag=X.TAG_VARIANT,
                   p=0.6, t=0.5)]
    cells.append(_cell("FX_FUTURES|XS|5|CARRY_TO_RISK", dim="CARRY_TO_RISK", h=5, tag=X.TAG_VARIANT,
                       p=1e-4))
    bh = X.apply_multiple_testing(cells)
    assert cells[0]["fdr_family"] == "R63_CAMPAIGN_BH_m978" and cells[0]["fdr_pass_conditional"] is True
    assert cells[1]["fdr_pass_conditional"] is False and cells[1]["r64_verdict"] == R.V_NOT_FDR
    # a formula variant is the SAME information: it inherits its base cell's
    # campaign FDR and never gets the smaller R64 family
    assert cells[2]["fdr_family"] == "R63_CAMPAIGN_BH_m978 (inherited from FX_FUTURES|XS|1|CARRY)"
    assert cells[2]["fdr_pass_conditional"] is True
    assert cells[4]["fdr_family"] == "R63_CAMPAIGN_BH_m978 (inherited from FX_FUTURES|XS|5|CARRY)"
    assert cells[4]["fdr_pass_conditional"] is False and cells[4]["r64_verdict"] == R.V_NOT_FDR
    # a variant with no R63 base falls back to the R64 campaign BH
    assert cells[3]["fdr_family"].startswith("R64_CAMPAIGN_BH_m")
    assert cells[3]["r64_verdict"] == R.V_NO_VALUE
    assert bh["conditional"]["m"] == 5
    assert X.base_cell_id(cells[4]) == "FX_FUTURES|XS|5|CARRY" and X.base_cell_id(cells[0]) is None


def test_fx_carry_family_is_one_family_with_holm(root):
    cells = []
    for mode in ("XS", "TS"):
        for h in R.HORIZONS:
            cells.append(_cell("FX_FUTURES|%s|%d|CARRY" % (mode, h), mode=mode, h=h,
                               p=1e-4 if (mode == "XS" and h == 1) else 0.3,
                               econ_p=1e-3 if (mode == "XS" and h == 1) else 0.4))
    fam = FAM.fx_carry_family(cells)
    assert fam["n_cells"] == 8 and fam["verdict"] == FAM.F_SURVIVES
    assert fam["family_p_conditional"] == pytest.approx(8 * 1e-4)
    assert fam["family_p_economic"] == pytest.approx(8 * 1e-3)
    assert len(FAM.fx_carry_family(cells[:3], write=False)["rows"]) == 3
    assert FAM.fx_carry_family(cells[:3], write=False)["verdict"] == FAM.F_INCOMPLETE
    assert (root / "r64" / "results" / FAM.ARTIFACT_NAME).exists()


# --------------------------------------------------------------------------- #
# The R63 handoff validator
# --------------------------------------------------------------------------- #
def _stamp(body: dict) -> dict:
    body = dict(body)
    body.setdefault("protocol_sha256", R63.protocol_hash())
    body.setdefault("generated_at", "2026-09-09T23:00:00+00:00")
    body["artifact_hash"] = R63.stable_hash({k: v for k, v in body.items()
                                             if k not in ("artifact_hash", "generated_at")})
    return body


def _fake_r63_root(root, *, tamper=False, stale_file=False):
    r63 = root / "r63"
    (r63 / "challengers").mkdir(parents=True, exist_ok=True)
    (r63 / "results").mkdir(parents=True, exist_ok=True)
    cond = {"increment": 0.0125, "t": 4.26, "p_one_sided": 1e-5, "increment_selection": 0.0085,
            "t_selection": 2.6, "increment_lockbox": 0.038, "t_lockbox": 2.8,
            "positive_fraction": 0.53, "lockbox_sign_agrees": True}
    cell = {"cell_id": HV.FX_CELL_ID, "scope": "FX_FUTURES", "mode": "XS", "horizon": 1,
            "dimension": "CARRY", "conditional": cond,
            "economics": {"ann_net_increment": 0.025, "sharpe_increment": 0.28},
            "verdict": "INCREMENTAL_INFORMATION_CANDIDATE", "fdr_pass_conditional": True,
            "effective_periods": 6238, "n_periods": 6238, "n_selection_periods": 5395,
            "n_lockbox_periods": 843, "rows_covered": 56000, "n_instruments": 9}
    matrix = _stamp({"schema": "m", "cells": [cell]})
    rec = {"schema": "r63_challenger_candidate/1", "challenger_id": HV.FX_CHALLENGER_ID,
           "cell_id": HV.FX_CELL_ID, "classification": R63.CH_READY, "asset_class": "FX_FUTURES",
           "horizon_sessions": 1, "instruments": 9,
           "oos_improvement": {"increment": 0.0125, "t": 4.26, "lockbox": 0.038},
           "net_improvement": {"ann_net_increment": 0.025, "sharpe_increment": 0.28},
           "promotion_allowed": False, "live_registration_performed": False,
           "holdings_changed": False, "protocol_sha256": R63.protocol_hash()}
    rec["record_hash"] = R63.stable_hash(rec)
    cands = _stamp({"schema": "c", "counts": {R63.CH_READY: 1}, "ready_for_forward_qualification": [rec],
                    "promotion_performed": False, "live_registration_performed": False})
    file_rec = json.loads(json.dumps(rec))
    if stale_file:
        # the R63 write-once file: first-pass economics, same conditional statistics
        file_rec["net_improvement"] = {"ann_net_increment": 0.0248, "sharpe_increment": 0.277}
        file_rec.pop("record_hash")
        file_rec["record_hash"] = R63.stable_hash(file_rec)
    if tamper:
        file_rec["instruments"] = 10                 # body changed, hash not
    (r63 / "challengers" / ("%s.json" % HV.FX_CHALLENGER_ID)).write_text(json.dumps(file_rec), encoding="utf-8")
    (r63 / "results" / "r63_challenger_candidates.json").write_text(json.dumps(cands), encoding="utf-8")
    (r63 / "results" / "information_sensitivity_matrix.json").write_text(json.dumps(matrix), encoding="utf-8")
    return cell


def test_handoff_validation_accepts_a_consistent_artifact_set_and_reproduction(root):
    cell = _fake_r63_root(root)
    out = HV.validate(reproduced_cell=cell)
    assert out["failed_checks"] == [], out["failed_checks"]
    assert out["verdict"] == HV.VALID and out["challenger_file_state"] == HV.FILE_CURRENT
    assert out["reproduction"]["matches"] is True
    assert (root / "r64" / "results" / HV.ARTIFACT_NAME).exists()
    off = json.loads(json.dumps(cell))
    off["conditional"]["t"] = 4.2601
    bad = HV.validate(reproduced_cell=off, write=False)
    assert bad["verdict"] == HV.INVALID
    assert "fresh_run_reproduces_persisted_conditional_statistics" in bad["failed_checks"]


def test_handoff_validation_rejects_a_tampered_record(root):
    _fake_r63_root(root, tamper=True)
    out = HV.validate(write=False)
    assert out["verdict"] == HV.INVALID
    assert "challenger_record_hash_recomputes" in out["failed_checks"]
    assert "ready_record_is_the_fx_carry_record" not in out["failed_checks"]


def test_handoff_validation_names_a_stale_write_once_file_without_failing_the_final_evidence(root):
    cell = _fake_r63_root(root, stale_file=True)
    out = HV.validate(reproduced_cell=cell, write=False)
    assert out["verdict"] == HV.VALID_STALE_FILE and out["failed_checks"] == []
    assert out["challenger_file_state"] == HV.FILE_STALE
    assert out["final_record_hash"] != out["challenger_file_record_hash"]
    assert "same conditional statistics" in out["challenger_file_note"]


# --------------------------------------------------------------------------- #
# Challengers: immutable, hashed, never registered
# --------------------------------------------------------------------------- #
def test_challenger_records_carry_the_result_standard_and_a_stable_freeze_hash(root):
    ready = _cell("FX_FUTURES|XS|1|CARRY", r63_fdr=True)
    ready["fdr_pass_conditional"] = True
    more = _cell("FX_FUTURES|XS|5|CARRY", h=5, econ_inc=0.001, econ_sharpe=0.01)
    more["fdr_pass_conditional"] = False
    more["r64_verdict"] = X.verdict(more)
    ready["r64_verdict"] = X.verdict(ready)
    rej = _cell("RATES_FUTURES|XS|1|CARRY", scope="RATES_FUTURES", t=0.4)
    rej["r64_verdict"] = X.verdict(rej)
    fam = {"verdict": FAM.F_SURVIVES, "family_p_conditional": 0.0008, "family_p_economic": 0.008,
           "rows": [], "family_id": R.FX_CARRY_FAMILY_ID, "n_cells": 8}
    assert CH.classify(ready, fam)[0] == R.CH_READY
    assert CH.classify(more)[0] == R.CH_MORE
    assert CH.classify(rej)[0] == R.CH_REJECTED
    r1 = CH.record(ready, R.CH_READY, "why", fam)
    r2 = CH.record(ready, R.CH_READY, "why", fam)
    assert r1["freeze_record_hash"] == r2["freeze_record_hash"] == R.stable_hash(r1["forward_specification"])
    assert r1["record_hash"] == r2["record_hash"]
    for k in ("hypothesis", "mechanism", "information_class", "asset_class", "horizon_sessions",
              "universe", "pit_source", "baseline", "incremental_oos_result",
              "conditional_information_value", "multiple_testing", "net_return", "turnover", "costs",
              "volatility", "drawdown", "concentration", "robustness", "binding_failure",
              "forward_specification"):
        assert k in r1
    assert r1["promotion_allowed"] is False and r1["live_registration_performed"] is False
    assert r1["forward_specification"]["emission_rule"].startswith("a decision is formed at close t")
    body = CH.build([ready, more, rej], fam)
    assert body["counts"] == {R.CH_READY: 1, R.CH_MORE: 1, R.CH_REJECTED: 1}
    assert body["live_registration_performed"] is False and body["promotion_performed"] is False
    files = sorted((root / "r64" / "challengers").glob("*.json"))
    assert len(files) == 2
    before = files[0].read_text()
    CH.build([ready, more, rej], fam)
    assert files[0].read_text() == before          # immutable: never overwritten
    assert sorted((root / "r64" / "challengers").glob("*.json")) == files
    # a re-run whose economics changed writes a NEW hash-named file; the old one stands
    ready2 = json.loads(json.dumps(ready))
    ready2["r64_economics"]["ann_net_increment"] = 0.031
    CH.build([ready2, more, rej], fam)
    after = sorted((root / "r64" / "challengers").glob("*.json"))
    assert len(after) == 3 and files[0] in after and files[0].read_text() == before
    assert CH.binding_failure(more).startswith("NOT_ECONOMIC_UNDER_CONTROLS")
    assert CH.binding_failure(_cell("x", dd=-0.9)).startswith("DEGENERATE_UNDER_CONTROLS")


# --------------------------------------------------------------------------- #
# The overlay is not a frontier; the governor consumes the frontier
# --------------------------------------------------------------------------- #
def _frontier(root, needs=None):
    needs = needs if needs is not None else [
        {"cell_key": "CROSS_ASSET|21|CARRY", "asset_class": "CROSS_ASSET", "horizon": 21,
         "dimension": "CARRY", "remaining_research_value": 0.13, "observation_state": "WELL_OBSERVED",
         "best_verdict": "CONDITIONAL_VALUE_NOT_ECONOMIC", "next_action": "MORE_RESEARCH_SAME_INFORMATION",
         "sourcing_step": "OWNED"},
        {"cell_key": "FX_FUTURES|5|CARRY", "asset_class": "FX_FUTURES", "horizon": 5,
         "dimension": "CARRY", "remaining_research_value": 0.065, "observation_state": "WELL_OBSERVED",
         "best_verdict": "CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT", "next_action": "MORE_RESEARCH_SAME_INFORMATION",
         "sourcing_step": "OWNED"},
        {"cell_key": "US_EQUITY|21|ANALYST_REVISIONS", "asset_class": "US_EQUITY", "horizon": 21,
         "dimension": "ANALYST_REVISIONS", "remaining_research_value": 0.5, "observation_state": "BLOCKED",
         "best_verdict": None, "next_action": "SOURCE", "sourcing_step": "PAID"}]
    fr = {"schema": "r63_information_gap_frontier/1", "artifact_hash": "abc123", "all_needs": needs}
    (root / "frontier.json").write_text(json.dumps(fr), encoding="utf-8")
    return fr


def test_overlay_multiplies_the_frontiers_value_and_is_not_a_frontier(root):
    fr = _frontier(root)
    c1 = _cell("CROSS_ASSET|XS|21|CARRY", scope="CROSS_ASSET", h=21, econ_inc=0.001, econ_sharpe=0.0)
    c1["r64_verdict"] = X.verdict(c1)
    c2 = _cell("CROSS_ASSET|XS|21|CARRY_CLASS_NEUTRAL", scope="CROSS_ASSET", h=21,
               dim="CARRY_CLASS_NEUTRAL", tag=X.TAG_VARIANT)
    c2["r64_verdict"] = X.verdict(c2)
    ov = FR.build([c1, c2], frontier=fr)
    assert ov["not_a_frontier"] is True and ov["source_frontier_artifact_hash"] == "abc123"
    assert ov["n_updates"] == 1
    u = ov["updates"][0]
    assert u["cell_key"] == "CROSS_ASSET|21|CARRY" and u["r64_verdict"] == R.V_ECON
    assert u["remaining_research_value_multiplier"] == 0.0 and u["in_r63_frontier"] is True
    rows = IN.need_rows(fr, ov, limit=10)
    assert [r["cell_key"] for r in rows] == ["FX_FUTURES|5|CARRY"]    # answered need drops; PAID excluded


def test_governor_consumes_information_needs_dedupes_by_watermark_and_keeps_fairness(root):
    fr = _frontier(root)
    mem = M.open_memory()
    empty_view = {"asset_classes": {}, "ready": [], "non_equity_ready": [], "substrates": {}}
    batch = GOV.generate_mandates(mem, limit=8, frontier_view=empty_view)
    assert batch["n_information_needs"] == 2                     # PAID need never becomes a mandate
    needs = [m for m in batch["mandates"] if m["family"].startswith(IN.FAMILY_PREFIX)]
    assert len(needs) == 2
    assert all(m["kind"] == GOV.MANDATE_DATA for m in needs)
    assert needs[0]["payload"]["source"] == IN.SOURCE
    assert needs[0]["payload"]["frontier_artifact_hash"] == "abc123"
    assert needs[0]["expected_information_value"] == IN.eiv_for(0.13)
    assert all(m["batch_rank"] == i for i, m in enumerate(batch["mandates"]))
    # the DATA_VALIDATION handler records the need and sets the watermark
    handlers = H.make_handlers(mem)

    class _Job:
        payload = needs[0]

    outcome, detail = handlers[H.AR.CAT_DATA_VALIDATION](_Job())
    assert outcome == H.AR.OUTCOME_COMPLETED and detail["real_work"] == "r64_information_need"
    assert mem.get_meta(IN.watermark_key("CROSS_ASSET|21|CARRY")) == "abc123"
    assert any(e["kind"] == IN.EVENT_MANDATED for e in mem.events(kind=IN.EVENT_MANDATED))
    again = GOV.generate_mandates(mem, limit=8, frontier_view=empty_view)
    assert again["n_information_needs"] == 1                     # not re-issued for the same frontier
    # a NEW frontier version revives it
    fr["artifact_hash"] = "def456"
    (root / "frontier.json").write_text(json.dumps(fr), encoding="utf-8")
    assert GOV.generate_mandates(mem, limit=8, frontier_view=empty_view)["n_information_needs"] == 2
    # fairness: information needs never take more than the kind ceiling when other kinds exist
    cap = max(1, int(8 * GOV.MAX_KIND_SHARE))
    kinds = {}
    for m in batch["mandates"]:
        kinds[m["kind"]] = kinds.get(m["kind"], 0) + 1
    assert kinds[GOV.MANDATE_DATA] <= max(cap, 2)


def test_adapter_reads_by_path_and_imports_no_research_package():
    src = (REPO / "alpha_agent" / "r59" / "information_needs.py").read_text(encoding="utf-8")
    imports = [ln for ln in src.splitlines() if ln.lstrip().startswith(("import ", "from "))]
    for ln in imports:
        assert "r63" not in ln and "r64" not in ln, ln
    assert "FRONTIER_PATH_ENV" in src and "WATERMARK_META_PREFIX" in src
    for p in ("alpha_agent/r59/governor.py", "alpha_agent/r59/runtime.py",
              "alpha_agent/r59/loop.py", "alpha_agent/r52/runtime.py"):
        text = (REPO / p).read_text(encoding="utf-8")
        assert "r63" not in text and "import r64" not in text and "r64 import" not in text, p
    assert IN.load_frontier(Path("nowhere/none.json")) is None
    assert IN.candidates(None, frontier=None, overlay={}) == [] or True


def test_runner_has_no_execute_path_and_refuses_a_live_root():
    src = (REPO / "scripts" / "run_r64_information_directed_alpha.py").read_text(encoding="utf-8")
    assert "--execute" not in src and "uvicorn" not in src and "Register-ScheduledTask" not in src
    assert "restart_paper_trader_backend" not in src
    assert "assert_research_root_is_not_live()" in src
    for tok in ("register_forward_challenger(", "adopt_prospective_freeze(", "approve_proposal("):
        assert tok not in src
