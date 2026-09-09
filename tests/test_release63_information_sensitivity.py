"""Release 63 - information sensitivity: regressions that keep it honest.

Every test here is HERMETIC. The R63 research root is redirected into a
pytest temp directory through the owner's own env-var constant; no test opens
the live ResearchMemory, the live desk ledger, an owned data store, or the
canonical checkout, and a test proves that the resolved root is nowhere near
them. The R62.1.2 incident (a pytest run writing real emissions into a live
store) is exactly what these fixtures prevent.

What is protected:

* the ontology is deterministic, unique and complete
* the protocol was registered with the flags and thresholds the code uses
* the as-of join cannot see the future; declared publication lags are applied
* forward windows are NEXT_CLOSE and a missing tail earns zero
* walk-forward folds purge and embargo, and the lockbox is one fold
* the conditional statistic is a PAIRED increment on identical rows; a signal
  dimension is found, a noise dimension is not, a duplicate is REDUNDANT
* Benjamini-Hochberg control and the verdict ladder
* costs reduce net returns; turnover is charged
* the certification carries no UNKNOWN disposition and reads no live memory
* asset x horizon states, gap ranking determinism, sourcing arithmetic
* challenger rules; the handoff executes nothing; no live registration
* the runner has no execute path and no live root
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent import r63 as R
from alpha_agent.r63 import acquire as ACQ
from alpha_agent.r63 import asset_horizon as AH
from alpha_agent.r63 import challengers as CH
from alpha_agent.r63 import gaps as G
from alpha_agent.r63 import handoff as HO
from alpha_agent.r63 import inventory as INV
from alpha_agent.r63 import ontology as ONT
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S
from alpha_agent.r63 import sourcing as SO

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")

LIVE_ROOTS = ("Stock_Prediction_app_data", r"C:\Users\binis\paper_trader",
              r"C:\Users\binis\.paper_trader")


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setenv(R.RESEARCH_ROOT_ENV, str(tmp_path / "r63"))
    # never touch the live research memory from a test
    monkeypatch.setattr(INV, "memory_evidence",
                        lambda: {"state": "RESEARCH_MEMORY_NOT_PRESENT", "families": {}})
    monkeypatch.setattr(SO, "DESK_PERFORMANCE_LEDGER", tmp_path / "no_ledger.json")
    return tmp_path / "r63"


# --------------------------------------------------------------------------- #
# Hermeticity
# --------------------------------------------------------------------------- #
def test_research_root_is_hermetic(root):
    got = str(R.research_root())
    assert got == str(root)
    for bad in LIVE_ROOTS:
        assert bad.lower() not in got.lower()
    R.assert_research_root_is_not_live()


def test_live_checkout_root_is_refused():
    with pytest.raises(RuntimeError):
        R.assert_research_root_is_not_live(Path(r"C:\Users\binis\paper_trader\anything"))


def test_worktree_import_integrity():
    here = Path(R.assert_worktree_import())
    assert here.name == "alpha_agent"


def test_safety_flags_are_all_off():
    for k in ("creates_orders", "creates_fills", "broker_enabled", "promotes_model",
              "activates_sleeve", "approves_proposal", "registers_forward_challenger",
              "mutates_operational_store", "mutates_live_research_store", "purchases_data",
              "starts_trial_or_subscription", "automation_enabled"):
        assert R.SAFETY[k] is False
    assert R.SAFETY["live_checkout_read_only"] is True
    for badge in ("NO ORDERS", "AUTOMATION OFF", "MANUAL REVIEW", "NO PURCHASE"):
        assert badge in R.SAFETY_BADGES


def test_write_artifact_is_deterministic_and_sorted(root):
    body = {"b": 1, "a": [3, 2, {"z": 1, "y": 2}]}
    p1 = R.write_artifact("x.json", dict(body))
    h1 = json.loads(p1.read_text())["artifact_hash"]
    p2 = R.write_artifact("x.json", dict(body))
    h2 = json.loads(p2.read_text())["artifact_hash"]
    assert h1 == h2
    text = p1.read_text()
    assert text.index('"a"') < text.index('"b"')
    assert str(p1).startswith(str(root))


# --------------------------------------------------------------------------- #
# Ontology + protocol
# --------------------------------------------------------------------------- #
def test_ontology_is_deterministic_unique_and_complete():
    ids = ONT.DIMENSION_IDS
    assert len(ids) == len(set(ids)) == 42
    assert ONT.ontology_hash() == ONT.build()["ontology_hash"]
    for d in ONT.DIMENSIONS:
        assert d["information_class"] in ONT.INFORMATION_CLASSES
        assert d["pit_nature"] in ONT.build()["pit_natures"]
        assert d["latent_state"] and d["not_this"]
    for ac, dims in ONT.BASELINE_DIMENSIONS.items():
        for d in dims:
            assert d in ids
    fm = ONT.family_to_dimensions()
    assert "PRICE_STATE" in fm and "INSIDER_FLOW" in fm


def test_protocol_registered_before_experiments_and_matches_code():
    p = R.protocol()
    assert p["registered_before_any_experiment_ran"] is True
    assert p["release"] == "R63" and p["campaign_id"] == R.CAMPAIGN_ID
    assert p["ontology"]["n_dimensions"] == len(ONT.DIMENSIONS)
    assert p["horizons"]["sessions"] == list(R.HORIZONS)
    assert p["partition_and_leakage"]["lockbox_start"] == R.LOCKBOX_START
    assert p["engine"]["multiple_testing"]["method"].startswith("Benjamini-Hochberg at q = %.2f" % R.BH_Q)
    assert p["engine"]["effective_sample"]["min_effective_periods"] == R.MIN_EFFECTIVE_PERIODS
    assert "%.2f" % R.REDUNDANT_RESIDUAL_SHARE_MAX in p["engine"]["redundancy"]["residual_share"]
    assert p["ontology"]["baseline_information"]["US_EQUITY"] == list(ONT.baseline_for(R.AC_US_EQUITY))
    for k in ("creates_orders", "purchases_data", "registers_forward_challenger", "automation_enabled"):
        assert p["safety"][k] is False
    assert p["paid_data_gate"]["no_purchase"] is True


# --------------------------------------------------------------------------- #
# Point-in-time
# --------------------------------------------------------------------------- #
def _cal(n=40, start="2020-01-01"):
    return np.array(pd.bdate_range(start, periods=n).strftime("%Y-%m-%d"))


def test_as_of_never_sees_the_future_and_applies_lags():
    cal = _cal()
    s = pd.Series([1.0, 2.0], index=pd.to_datetime(["2020-01-10", "2020-01-20"]))
    v0 = pit.as_of(s, cal, lag_sessions=0)
    d = pd.to_datetime(cal)
    assert np.isnan(v0[d < pd.Timestamp("2020-01-10")]).all()
    assert (v0[(d >= pd.Timestamp("2020-01-10")) & (d < pd.Timestamp("2020-01-20"))] == 1.0).all()
    assert (v0[d >= pd.Timestamp("2020-01-20")] == 2.0).all()
    v1 = pit.as_of(s, cal, lag_sessions=1)
    assert np.isnan(v1[list(cal).index("2020-01-10")])
    assert v1[list(cal).index("2020-01-13")] == 1.0
    # a 6-day publication lag on a Tuesday report: visible from the next
    # Monday plus the broadcast session, never on the report week
    rep = pd.Series([5.0], index=pd.to_datetime(["2020-01-07"]))
    v6 = pit.as_of(rep, cal, lag_sessions=1, publication_lag_days=6)
    assert np.isnan(v6[list(cal).index("2020-01-10")])
    assert np.isnan(v6[list(cal).index("2020-01-13")])
    assert v6[list(cal).index("2020-01-14")] == 5.0


def test_strictly_after_hides_a_same_day_release():
    cal = _cal()
    s = pd.Series([3.0], index=pd.to_datetime(["2020-01-10"]))
    v = pit.strictly_after(s, cal)
    assert np.isnan(v[list(cal).index("2020-01-10")])
    assert v[list(cal).index("2020-01-13")] == 3.0


def test_forward_compound_is_next_close_and_missing_tail_earns_zero():
    ret = np.array([[0.0, 0.01, 0.02, 0.03, np.nan, np.nan, 0.0, 0.0]])
    f = pit.forward_compound(ret, 2)
    # decision at t=0: window sessions t+2..t+3 -> (1.02)(1.03)-1
    assert abs(f[0, 0] - (1.02 * 1.03 - 1)) < 1e-12
    # decision at t=2: sessions 4,5 are missing -> NaN (nothing observed)
    assert np.isnan(f[0, 2])
    # decision at t=1: sessions 3,4 -> only 3 observed, tail earns zero
    assert abs(f[0, 1] - 0.03) < 1e-12


def test_walk_forward_purges_embargoes_and_keeps_one_lockbox():
    cal = np.array(pd.bdate_range("2010-01-01", periods=252 * 16).strftime("%Y-%m-%d"))
    idx = pit.decision_indices(cal, "2010-01-01", 21, 21)
    folds = pit.walk_forward(cal, idx, horizon=21)
    kinds = [f["kind"] for f in folds]
    assert kinds.count("LOCKBOX") == 1 and kinds[-1] == "LOCKBOX"
    for f in folds:
        t0 = idx[f["test"][0]]
        assert (idx[f["train"]] < t0 - 21 - 1).all()
        if f["kind"] == "SELECTION":
            assert (cal[idx[f["test"]]] < R.LOCKBOX_START).all()
        else:
            assert (cal[idx[f["test"]]] >= R.LOCKBOX_START).all()
    assert all(len(f["train"]) >= 1 for f in folds)


def test_blocked_inner_folds_and_nw_lag():
    folds = pit.blocked_inner_folds(90, 3, gap=2)
    assert len(folds) == 3
    fit, hold = folds[1]
    assert set(fit).isdisjoint(set(hold))
    assert pit.nw_lag(63, 21) == 2 and pit.nw_lag(21, 21) == 0 and pit.nw_lag(1, 1) == 0


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def test_grouped_rank_ic_matches_spearman():
    rng = np.random.default_rng(1)
    gid = np.repeat(np.arange(5), 30)
    p = rng.normal(size=150)
    y = p + rng.normal(size=150)
    ic = S.grouped_rank_ic(p, y, gid)
    for g in range(5):
        m = gid == g
        assert abs(ic.loc[g] - S.spearman(p[m], y[m])) < 1e-12
    small = S.grouped_rank_ic(p[:4], y[:4], np.zeros(4, dtype=int))
    assert np.isnan(small.loc[0])


def test_bh_fdr_and_nw_tstat():
    p = {"a": 0.001, "b": 0.02, "c": 0.5, "d": 0.9}
    out = S.bh_fdr(p, q=0.10)
    assert out["m"] == 4 and out["per_test"]["a"] and not out["per_test"]["c"]
    st = S.nw_tstat(np.ones(50) * 0.01 + np.arange(50) * 1e-6, lag=2)
    assert st["t"] > 10
    assert S.nw_tstat([1.0, 2.0])["t"] is None


def test_capped_weights_never_exceed_the_cap_and_still_sum_to_one():
    w = np.array([0.90, 0.05, 0.03, 0.02])
    c = S._capped(w, 0.25)
    assert abs(c.sum() - 1.0) < 1e-9 and c.max() <= 0.25 + 1e-9
    assert np.allclose(S._capped(np.array([0.5, 0.5]), 0.5), [0.5, 0.5])
    # a degenerate cross-sectional book can never pass the economic gate
    cell = {"effective_periods": 200, "redundancy": {"redundancy": "DISTINCT"},
            "conditional": {"t": 5.0, "increment": 0.01, "lockbox_sign_agrees": True},
            "economics": {"ann_net_increment": 0.10, "sharpe_increment": 1.0,
                          "book": "XS_LONG_SHORT",
                          "augmented": {"max_dd": -0.999, "max_weight": 0.99}},
            "stability": {"share_blocks_positive": 0.9}}
    assert S.verdict(cell, fdr_pass=True) == S.V_NOT_ECON
    cell["economics"]["augmented"] = {"max_dd": -0.3, "max_weight": 0.2}
    assert S.verdict(cell, fdr_pass=True) == S.V_CANDIDATE


def test_residual_share_flags_a_duplicate_as_redundant():
    rng = np.random.default_rng(2)
    B = rng.normal(size=(1000, 3))
    D = np.column_stack([B[:, 0] * 2 + B[:, 1], rng.normal(size=1000)])
    r = S._residual_share(D[:, :1], B)
    assert r["redundancy"] == "REDUNDANT" and r["residual_share"] < 0.05
    r2 = S._residual_share(D[:, 1:], B)
    assert r2["redundancy"] == "DISTINCT"


# --------------------------------------------------------------------------- #
# The engine on a synthetic dataset
# --------------------------------------------------------------------------- #
def _synthetic(n_inst=12, n_dates=252 * 14, signal=0.15, dup=False, seed=7):
    rng = np.random.default_rng(seed)
    cal = np.array(pd.bdate_range("2010-01-01", periods=n_dates).strftime("%Y-%m-%d"))
    b = rng.normal(size=(n_inst, n_dates))
    d = rng.normal(size=(n_inst, n_dates))
    noise = rng.normal(size=(n_inst, n_dates)) * 0.02
    # forward return at t depends on features at t (NEXT_CLOSE window)
    ret = np.zeros((n_inst, n_dates))
    y = 0.003 * b + signal * 0.02 * d + noise
    # build a return series whose 1-session forward compound equals y
    ret[:, 2:] = y[:, :-2]
    dblock = np.stack([b * 0.5 + 0.1 * rng.normal(size=(n_inst, n_dates))], axis=-1) if dup \
        else np.stack([d], axis=-1)
    blocks = {"PRICE_RETURN_STATE": np.stack([b], axis=-1),
              "TREND": np.stack([rng.normal(size=(n_inst, n_dates))], axis=-1),
              "TESTDIM": dblock}
    h, cad = 1, 1
    fwd = pit.forward_compound(ret, h)
    vol = np.full((n_inst, n_dates), 0.3)
    return {"scope": "SYNTH", "mode": "XS", "horizon": h, "cadence": cad, "dates": cal,
            "dec": pit.decision_indices(cal, "2010-01-01", cad, h), "inst": list(range(n_inst)),
            "y": fwd, "y_scaled": fwd / (vol * np.sqrt(h / 252.0)), "elig": np.ones((n_inst, n_dates), bool),
            "vol": vol, "cost": np.full(n_inst, 0.0002), "blocks": blocks, "market_level": set(),
            "regime_vix": None, "regime_trend": None, "book": "XS_LONG_SHORT"}


def test_engine_finds_a_signal_dimension_and_rejects_noise(root):
    ds = _synthetic(signal=0.4)
    cell = S.run_cell(ds, ("PRICE_RETURN_STATE", "TREND"), "TESTDIM")
    assert cell["conditional"]["t"] > 3
    assert cell["conditional"]["increment"] > 0
    assert cell["redundancy"]["redundancy"] == "DISTINCT"
    assert cell["economics"]["ann_net_increment"] > 0
    assert cell["n_lockbox_periods"] > 0 and cell["n_selection_periods"] > 0
    assert cell["verdict"] in (S.V_NOT_FDR, S.V_CANDIDATE, S.V_UNSTABLE)
    assert S.verdict(cell, fdr_pass=True) in (S.V_CANDIDATE, S.V_UNSTABLE)
    noise = S.run_cell(_synthetic(signal=0.0, seed=9), ("PRICE_RETURN_STATE", "TREND"), "TESTDIM")
    assert noise["verdict"] in (S.V_NO_VALUE, S.V_DATA_HOLD, S.V_NOT_ECON, S.V_UNSTABLE, S.V_NOT_FDR)
    assert noise["conditional"]["t"] < 3


def test_engine_flags_a_duplicate_dimension_as_redundant(root):
    cell = S.run_cell(_synthetic(signal=0.0, dup=True, seed=3), ("PRICE_RETURN_STATE", "TREND"), "TESTDIM")
    assert cell["redundancy"]["redundancy"] == "REDUNDANT"
    assert cell["verdict"] == S.V_REDUNDANT


def test_engine_is_paired_on_identical_rows_and_costs_reduce_net(root):
    ds = _synthetic(signal=0.4, seed=11)
    cheap = S.run_cell(ds, ("PRICE_RETURN_STATE", "TREND"), "TESTDIM")
    ds2 = dict(ds)
    ds2["cost"] = np.full(len(ds["inst"]), 0.01)
    dear = S.run_cell(ds2, ("PRICE_RETURN_STATE", "TREND"), "TESTDIM")
    assert cheap["conditional"]["increment"] == dear["conditional"]["increment"]
    assert dear["economics"]["augmented"]["ann_net"] < cheap["economics"]["augmented"]["ann_net"]
    assert dear["economics"]["augmented"]["ann_cost_drag"] > cheap["economics"]["augmented"]["ann_cost_drag"]
    assert dear["economics"]["augmented"]["mean_oneway_turnover"] > 0


def test_data_hold_when_the_dimension_has_no_coverage(root):
    ds = _synthetic(signal=0.4, seed=5)
    ds["blocks"]["TESTDIM"] = np.full_like(ds["blocks"]["TESTDIM"], np.nan)
    cell = S.run_cell(ds, ("PRICE_RETURN_STATE", "TREND"), "TESTDIM")
    assert cell["verdict"] == S.V_DATA_HOLD and cell["coverage"] == 0.0


# --------------------------------------------------------------------------- #
# Certification, map, gaps, sourcing, challengers, handoff
# --------------------------------------------------------------------------- #
def _fake_cells():
    def cell(scope, mode, h, dim, t, econ, rs, kind="AUGMENTATION", blocks=0.8, lock=True):
        return {"cell_id": "%s|%s|%d|%s" % (scope, mode, h, dim), "scope": scope, "mode": mode,
                "horizon": h, "dimension": dim, "kind": kind, "coverage": 0.95,
                "effective_periods": 200, "n_periods": 200, "n_selection_periods": 150,
                "n_lockbox_periods": 50, "baseline": ["PRICE_RETURN_STATE"],
                "conditional": {"increment": 0.01 if t > 0 else -0.001, "t": t,
                                "p_one_sided": 0.5 * (1 - (t / 4.0)) if t < 4 else 1e-5,
                                "lockbox_sign_agrees": lock, "t_selection": t, "t_lockbox": t / 2},
                "secondary": {"partial_t": t, "permutation_drop": 0.001, "oos_r2_increment_mean": 0.0},
                "redundancy": {"residual_share": rs, "redundancy": "DISTINCT" if rs >= 0.35 else "REDUNDANT"},
                "stability": {"blocks": {"a": 1, "b": 1, "c": -1}, "share_blocks_positive": blocks,
                              "regime": {}, "share_instruments_positive": None},
                "economics": {"ann_net_increment": econ, "ann_net_increment_at_2x_cost": econ - 0.005,
                              "sharpe_increment": econ * 10, "t_increment": 2.5 if econ > 0.01 else 0.5,
                              "p_increment_one_sided": 0.01 if econ > 0.01 else 0.3,
                              "augmented": {"mean_oneway_turnover": 0.3, "max_dd": -0.1, "max_weight": 0.1},
                              "baseline": {"max_dd": -0.12}, "book": "XS_LONG_SHORT"},
                "verdict": None}
    cells = [cell("COMMODITY_FUTURES", "XS", 21, "CARRY", 3.5, 0.03, 0.9),
             cell("COMMODITY_FUTURES", "XS", 21, "INVENTORY", 0.4, -0.001, 0.8),
             cell("US_EQUITY", "XS", 21, "INSIDER_BEHAVIOUR", 2.4, 0.005, 0.8),
             cell("US_EQUITY", "XS", 21, "MOMENTUM", 1.0, 0.0, 0.05, kind="ABLATION"),
             cell("FX_FUTURES", "TS", 5, "CARRY", -1.0, -0.01, 0.9)]
    for c in cells:
        c["verdict"] = S.verdict(c)
    return cells


def test_certification_has_no_unknown_and_reads_no_live_memory(root):
    from alpha_agent.r63 import experiments as X
    cells = _fake_cells()
    X.apply_fdr(cells)
    body = INV.certify(cells)
    assert body["unknown_fields"] == []
    assert all(f["disposition"] in R.DISPOSITIONS for f in body["fields"])
    assert body["research_memory"]["state"] == "RESEARCH_MEMORY_NOT_PRESENT"
    assert body["counts"]["USED"] > 0 and body["counts"]["BLOCKED"] > 0
    assert body["n_fields"] >= 40 and body["n_providers"] >= 8
    for f in body["fields"]:
        for k in ("provider", "source", "dataset", "field", "normalized_field", "dimension_id",
                  "asset_classes", "horizons", "history", "cadence", "pit_status",
                  "available_at_semantics", "effective_at_semantics", "canonical_collector",
                  "canonical_normalizer", "consumer", "research_families", "disposition",
                  "disposition_reason", "blocker"):
            assert k in f
        assert f["pit_status"] in R.PIT_STATES
        assert f["dimension_id"] in ONT.DIMENSION_IDS
    carry = [f for f in body["fields"] if f["dimension_id"] == "CARRY" and f["provider"] == "Norgate Data"][0]
    assert carry["disposition"] == R.D_TESTED
    assert (root / "results" / INV.ARTIFACT_NAME).exists()


def test_asset_horizon_states_and_under_informed_ranking(root):
    from alpha_agent.r63 import experiments as X
    cells = _fake_cells()
    X.apply_fdr(cells)
    inv = INV.certify(cells)
    mat = AH.classify(inv, cells)
    states = set()
    for ac in R.ASSET_CLASSES:
        for h in R.HORIZONS:
            for d, v in mat["matrix"][ac][str(h)].items():
                assert v["state"] in R.OBSERVATION_STATES
                states.add(v["state"])
        assert mat["matrix"][ac]["INTRADAY"]["state"] == R.INTRADAY_STATE
    assert mat["matrix"]["COMMODITY_FUTURES"]["21"]["CARRY"]["state"] == R.OBS_WELL
    assert mat["matrix"]["US_EQUITY"]["21"]["ANALYST_REVISIONS"]["state"] == R.OBS_BLOCKED
    assert mat["matrix"]["COMMODITY_FUTURES"]["21"]["SHIPPING_TRANSPORT"]["state"] == R.OBS_BLOCKED
    assert R.OBS_NOT in states or R.OBS_PARTIAL in states
    assert mat["most_under_informed"][0]["blind_share"] >= mat["most_under_informed"][-1]["blind_share"]


def test_gap_frontier_is_deterministic_and_zeroes_tested_negative(root):
    from alpha_agent.r63 import experiments as X
    cells = _fake_cells()
    X.apply_fdr(cells)
    inv = INV.certify(cells)
    mat = AH.classify(inv, cells)
    a = G.build(inv, mat, cells, None)
    b = G.build(inv, mat, cells, None)
    assert [r["cell_key"] for r in a["all_needs"]] == [r["cell_key"] for r in b["all_needs"]]
    assert a["artifact_hash"] == b["artifact_hash"]
    ranks = [r["rank"] for r in a["all_needs"]]
    assert ranks == sorted(ranks)
    inv_row = [r for r in a["all_needs"] if r["cell_key"] == "COMMODITY_FUTURES|21|INVENTORY"][0]
    assert inv_row["remaining_research_value"] == 0.0 and inv_row["next_action"] == "STOP_TESTED_NEGATIVE"
    for r in a["all_needs"]:
        assert set(r["criteria"]) == set(G.CRITERIA)
        assert all(0.0 <= v <= 1.0 for v in r["criteria"].values())


def test_sourcing_break_even_and_no_purchase(root, tmp_path):
    be = SO.break_even(5000.0, 100000.0, 21, 0.02)
    assert abs(be["fee_share_of_nav"] - 0.05) < 1e-12
    assert be["extreme_hurdle"] is True
    assert abs(be["break_even_alpha_annual"] - (0.05 + 0.012 + 0.01 + 0.005)) < 1e-9
    assert SO.break_even(None, 100000.0, 21, 0.0)["state"] == "FEE_OR_NAV_UNKNOWN"
    from alpha_agent.r63 import experiments as X
    cells = _fake_cells()
    X.apply_fdr(cells)
    inv = INV.certify(cells)
    mat = AH.classify(inv, cells)
    fr = G.build(inv, mat, cells, None)
    body = SO.build(fr, inv, mat, cells)
    assert body["purchases"] == 0 and body["money_spent_usd"] == 0.0
    assert body["authoritative_nav"]["state"] == "LEDGER_UNREADABLE"
    assert all(g["purchase_authorised"] is False for g in body["paid_data_gate"])
    assert body["ladder_order"] == list(SO.LADDER)


def test_challenger_rules_and_no_registration(root):
    from alpha_agent.r63 import experiments as X
    cells = _fake_cells()
    X.apply_fdr(cells)
    body = CH.build(cells)
    assert body["promotion_performed"] is False and body["live_registration_performed"] is False
    for rec in body["ready_for_forward_qualification"] + body["more_research_required"]:
        assert rec["promotion_allowed"] is False and rec["holdings_changed"] is False
        assert rec["record_hash"]
    st, why = CH.classify({"verdict": S.V_CANDIDATE, "conditional": {"lockbox_sign_agrees": True},
                           "economics": {"ann_net_increment_at_2x_cost": 0.01}, "redundancy": {}})
    assert st == R.CH_READY
    st2, _ = CH.classify({"verdict": S.V_NOT_ECON, "conditional": {"t": 2.5},
                          "economics": {}, "redundancy": {"residual_share": 0.8}})
    assert st2 == R.CH_MORE
    st3, _ = CH.classify({"verdict": S.V_NO_VALUE, "conditional": {"t": 0.2}, "economics": {},
                          "redundancy": {"residual_share": 0.8}})
    assert st3 == R.CH_REJECTED


def test_handoff_ranks_by_remaining_value_and_executes_nothing(root):
    from alpha_agent.r63 import experiments as X
    cells = _fake_cells()
    X.apply_fdr(cells)
    inv = INV.certify(cells)
    mat = AH.classify(inv, cells)
    fr = G.build(inv, mat, cells, None)
    ranked = HO.next_best_information_research(5, frontier=fr)
    vals = [r["remaining_research_value"] for r in ranked]
    assert vals == sorted(vals, reverse=True) and all(v > 0 for v in vals)
    body = HO.publish(fr, cells)
    assert body["integration_state"] == "INTERFACE_ONLY_NOT_WIRED_INTO_LIVE_RUNTIME"
    assert all(h["executes_nothing"] is True for h in body["mandate_hints"])
    assert body["stop_transforming"]["US_EQUITY"]["flag"] is True


# --------------------------------------------------------------------------- #
# Acquisition parsing (no network)
# --------------------------------------------------------------------------- #
def test_submission_rows_keep_only_declared_forms_and_carry_acceptance():
    obj = {"filings": {"recent": {"form": ["8-K", "4", "424B2", "NT 10-K", "10-Q"],
                                  "filingDate": ["2020-01-02"] * 5,
                                  "acceptanceDateTime": ["2020-01-02T16:00:00.000Z"] * 5}}}
    rows = ACQ._rows_from_submission("0000000123", obj, obj["filings"]["recent"])
    forms = sorted(r["form"] for r in rows)
    assert forms == ["10-Q", "4", "8-K", "NT 10-K"]
    assert all(r["cik"] == "123" and r["acceptance"] for r in rows)


def test_acquire_module_hardcodes_no_contact():
    src = Path(ACQ.__file__).read_text(encoding="utf-8")
    assert "@gmail" not in src and "binisti" not in src
    assert "git config user.email" in ACQ.__doc__


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def test_runner_has_no_execute_path_and_writes_under_the_hermetic_root(root, capsys):
    import importlib.util
    p = R.REPO_ROOT / "scripts" / "run_r63_information_sensitivity.py"
    src = p.read_text(encoding="utf-8")
    assert "--execute" not in src and "Register-ScheduledTask" not in src
    assert "restart_paper_trader_backend" not in src
    spec = importlib.util.spec_from_file_location("r63_runner", str(p))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    rc = mod.main(["--stage", "ontology"])
    assert rc == 0
    out = capsys.readouterr().out.strip().splitlines()
    assert out[-1] == mod.OK
    assert (root / "results" / ONT.ARTIFACT_NAME).exists()
