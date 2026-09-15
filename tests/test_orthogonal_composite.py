"""Executor for ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_V1 - synthetic data only.

Pinned: the contract (kill rule, mechanism id, closed-class mirror), the E1-E10 inventory reading, the
selection rule (one sleeve per mechanism class, asset-class tie-break, stop at 5), the frozen-eligible
drift hold, the 35 % cap with iterative redistribution, the volatility target and 1.5x cap with the
financing spread, the overlay turnover cost, no look-ahead at a rebalance, the session split and history
floors, the unread confirmation after a qualification failure, the owner path adapters, and the Alpha
Agent executor contract.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import alpha_recovery as AR  # noqa: E402
from alpha_agent.alpha_recovery import orthogonal_composite as OC  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

CATALOG = _ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"


def _entry():
    body = json.loads(CATALOG.read_text(encoding="utf-8"))
    return next(m for m in body["mechanisms"] if m["mechanism_id"] == OC.MECHANISM_ID)


def test_01_the_kill_rule_and_closed_classes_are_the_catalogs():
    assert _entry()["pnl_gate"]["KILL_RULE"] == OC.KILL_RULE_FROZEN
    assert tuple(OC.CLOSED_CLASSES) == tuple(MX.CLOSED_CLASSES)


def test_02_a_changed_contract_is_refused():
    entry = _entry()
    with pytest.raises(ValueError):
        OC.run_mechanism(mechanism={**entry, "mechanism_id": "OTHER"})
    with pytest.raises(ValueError):
        OC.run_mechanism(mechanism=dict(entry, pnl_gate=dict(entry["pnl_gate"], KILL_RULE="Kill if nothing.")))


def test_03_the_cap_redistributes_iteratively_and_refuses_an_infeasible_set():
    w = OC.cap_weights([10.0, 1.0, 1.0, 1.0])
    assert w.sum() == pytest.approx(1.0) and w[0] == pytest.approx(0.35)
    assert np.allclose(w[1:], 0.65 / 3)
    w = OC.cap_weights([6.0, 5.0, 1.0, 1.0])                  # two capped after redistribution
    assert w.max() <= 0.35 + 1e-12 and w.sum() == pytest.approx(1.0)
    assert np.allclose(w, [0.35, 0.35, 0.15, 0.15])
    assert np.allclose(OC.cap_weights([1.0, 1.0, 1.0]), 1.0 / 3)
    with pytest.raises(ValueError):
        OC.cap_weights([1.0, 2.0])                              # 2 x 35 % < 100 %


def _sessions(n, start="2000-01-03"):
    return pd.bdate_range(start, periods=n)


def test_04_scale_cap_financing_and_overlay_cost_are_charged_exactly():
    idx = _sessions(300)
    rng = np.random.default_rng(1)
    calm = pd.DataFrame(rng.normal(0.0, 0.001, size=(300, 3)), index=idx, columns=list("ABC"))
    b = OC.build_composite(calm, overlay_bps=2.0)
    first = b.iloc[0]
    assert first["rebalance"] and first["scale"] == pytest.approx(1.5)
    assert first["turnover"] == pytest.approx(0.75)             # 0.5 x |1.5 - 0|
    assert first["overlay_cost"] == pytest.approx(0.75 * 2e-4)
    assert first["financing"] == pytest.approx(0.5 * 0.005 / 252)
    assert np.allclose(b["net"], b["gross"] - b["financing"] - b["overlay_cost"])
    wild = pd.DataFrame(rng.normal(0.0, 0.03, size=(300, 3)), index=idx, columns=list("ABC"))
    bw = OC.build_composite(wild, overlay_bps=4.0)
    assert (bw["scale"] < 1.0).all() and (bw["financing"] == 0.0).all()


def test_05_a_rebalance_uses_only_prior_sessions_and_holds_within_the_month():
    idx = _sessions(400)
    rng = np.random.default_rng(2)
    R = pd.DataFrame(rng.normal(0.0, 0.01, size=(400, 3)), index=idx, columns=list("ABC"))
    b = OC.build_composite(R, overlay_bps=2.0)
    reb = b.index[b["rebalance"]]
    d = reb[2]
    shocked = R.copy()
    shocked.loc[d:, "A"] = shocked.loc[d:, "A"] * 25.0
    b2 = OC.build_composite(shocked, overlay_bps=2.0)
    cols = ["e:A", "e:B", "e:C"]
    assert np.allclose(b.loc[d, cols], b2.loc[d, cols])
    month = b.loc[(b.index.year == d.year) & (b.index.month == d.month), cols]
    assert np.allclose(month.to_numpy(), month.iloc[0].to_numpy())
    assert OC.first_rebalance_position(idx) >= OC.LOOKBACK


def _cand(cid, rank, **kw):
    base = {"candidate_id": cid, "global_rank": rank, "asset_class": "FX", "mechanism_class": "RISK_TRANSFER_PREMIUM",
            "members": [cid.lower()], "disposition": "OPEN", "current_state": "TRUE_FORWARD",
            "evidence_quality": "GOOD_STRATEGY_INCOMPLETE_EVIDENCE", "opportunity_cost_score": 0.5 - rank / 100.0,
            "data_requirement": "owned; no purchase", "next_best_action": {"kind": "ACCRUE_FORWARD_EVIDENCE"}}
    base.update(kw)
    return base


def _catalog(cands, pit="PIT_MARKET_OBSERVABLE"):
    return {"global_reconciliation": {"candidates": [
        {"candidate_id": c["candidate_id"], "historical": {"pit": pit, "t_stat": 2.5}} for c in cands]}}


def test_06_the_inventory_reads_each_criterion_and_the_selection_rule_is_mechanical(monkeypatch):
    avail = {"state": "AVAILABLE", "loader": "load_fx_carry"}
    cands = [
        _cand("S1", 1, asset_class="EQUITY_INDEX", mechanism_class="HEDGER_DEMAND_PRESSURE"),
        _cand("S2", 2),
        _cand("BAD", 3, evidence_quality="BAD_STRATEGY_COMPLETE_EVIDENCE"),
        _cand("PRICE", 4, mechanism_class="PRICE_STATE_TRANSFORMATION"),
        _cand("PAID", 5, data_requirement="PAID: a feed"),
        _cand("GATE", 6, current_state="HUMAN_GATE", next_best_action={"kind": "DATA_PURCHASE_DECISION"}),
        _cand("INC", 7, current_state="LIVE_CANDIDATE"),
        _cand("NOPATH", 8, mechanism_class="LIQUIDITY_PRESSURE"),
        _cand(OC.CANDIDATE_ID, 9, mechanism_class="LIQUIDITY_PRESSURE"),
        _cand("DUP", 10),                                                   # same class as S2: skipped
        _cand("S3", 11, asset_class="FX", mechanism_class="INFORMATION_DIFFUSION_SPEED", opportunity_cost_score=0.30),
        _cand("S4", 12, asset_class="RATES", mechanism_class="FORCED_TRADING_FLOW", opportunity_cost_score=0.295),
        _cand("S5", 13, asset_class="CREDIT", mechanism_class="LIQUIDITY_PRESSURE"),
        _cand("S6", 14, asset_class="COMMODITIES", mechanism_class="DERIVATIVES_LEAD"),
        _cand("S7", 15, asset_class="VOLATILITY", mechanism_class="NONLINEAR_POSITIONING"),
    ]
    paths = {c["candidate_id"]: avail for c in cands if c["candidate_id"] != "NOPATH"}
    monkeypatch.setattr(OC, "RETURN_PATH_OWNERS", paths)
    rows = OC.inventory({"state": "COMPLETE", "candidates": cands}, _catalog(cands))
    fails = {r["candidate_id"]: r["failures"] for r in rows}
    assert fails["S1"] == [] and fails["DUP"] == []
    assert fails["BAD"] == ["E2_CLOSED_NO_EDGE_OR_HOLD"]
    assert fails["PRICE"] == ["E5_CLOSED_MECHANISM_CLASS"]
    assert fails["PAID"] == ["E3_PAID_DATA"]
    assert fails["GATE"] == ["E4_HUMAN_PURCHASE_GATE"]
    assert fails["INC"] == ["E9_INCUMBENT"]
    assert fails["NOPATH"] == ["E7_E8_RETURN_PATH_NOT_DECLARED"]
    assert fails[OC.CANDIDATE_ID] == ["E10_THIS_COMPOSITE"]
    sel = [r["candidate_id"] for r in OC.select(rows)]
    # S3 (FX, already held) and S4 (new asset class, score within 0.02) are consecutive: S4 is taken first
    assert sel == ["S1", "S2", "S4", "S3", "S5"]
    no_pit = OC.inventory({"state": "COMPLETE", "candidates": cands[:1]}, _catalog(cands[:1], pit="METHODOLOGY_BACKFILL"))
    assert no_pit[0]["failures"] == ["E6_NO_PIT_HISTORICAL_OWNER"]


def _frozen_frontier():
    cands = [_cand("GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW", 1, asset_class="EQUITY_INDEX",
                   mechanism_class="HEDGER_DEMAND_PRESSURE", current_state="FORWARD_PENDING"),
             _cand("GC_FX_XS_CARRY_DATED_CONTRACT", 2),
             _cand("GC_XA_R39_R40_MACHINE_SHADOWS", 3, asset_class="CROSS_ASSET",
                   mechanism_class="PRICE_STATE_TRANSFORMATION"),
             _cand("GC_COMMODITY_CURVE_CARRY", 4, asset_class="COMMODITIES",
                   evidence_quality="BAD_STRATEGY_COMPLETE_EVIDENCE")]
    return {"state": "COMPLETE", "candidates": cands}, _catalog(cands)


def test_07_two_frozen_sleeves_hold_at_gate_one_and_drift_is_named():
    fr, cat = _frozen_frontier()
    g = OC.gate_inventory(fr, cat)
    assert g["eligible"] == list(OC.FROZEN_ELIGIBLE)
    assert "SLEEVES_2_BELOW_3" in g["problems"] and "MECHANISM_CLASSES_2_BELOW_3" in g["problems"]
    assert not any(p.startswith("INVENTORY_DRIFT") for p in g["problems"])
    fr["candidates"][1]["evidence_quality"] = "BAD_STRATEGY_COMPLETE_EVIDENCE"
    assert any(p.startswith("INVENTORY_DRIFT") for p in OC.gate_inventory(fr, cat)["problems"])


def _planted(mean_daily, *, n=3150, seed=7, conf_mean=None):
    idx = _sessions(n, "2008-01-02")
    rng = np.random.default_rng(seed)
    X = rng.normal(0.0, 0.01, size=(n, 3)) + mean_daily
    if conf_mean is not None:
        X[int(n * 0.72):] += conf_mean - mean_daily
    R = pd.DataFrame(X, index=idx, columns=["S1", "S2", "S3"])
    frame = pd.DataFrame({"r": rng.normal(0.0003, 0.012, size=n) + 0.0001, "c": 0.0001}, index=idx)
    return R, frame


def test_08_a_planted_diversified_composite_qualifies_and_is_never_capital_eligible():
    R, frame = _planted(0.00095)
    res = OC.evaluate(R, frame)
    assert res["verdict"] == "QUALIFIED", res["why"]
    assert res["untouched_confirmation"] == "CONFIRMED" and res["confirmation"]["state"] == "READ"
    sp = res["split"]
    assert sp["qualification_sessions"] == int(0.7 * sp["portfolio_sessions"])
    assert sp["qualification_sessions"] >= 1500 and sp["confirmation_sessions"] >= 600
    shares = res["qualification"]["contributions"]["abs_share"]
    assert sum(shares.values()) == pytest.approx(1.0)
    assert max(shares.values()) <= OC.MAX_CONTRIBUTION_SHARE


def test_09_a_qualification_failure_leaves_the_confirmation_unread():
    R, frame = _planted(-0.0002)
    res = OC.evaluate(R, frame)
    assert res["verdict"] == "NO_EDGE" and res["gate"].startswith("QUAL_")
    assert res["confirmation"] == {"state": "UNREAD", "why": "read only if every qualification gate passes"}
    assert res["full_history"] == {"state": "UNREAD"} and res["untouched_confirmation"] == "NOT_READ"


def test_10_a_planted_effect_that_dies_in_the_confirmation_fails_there():
    R, frame = _planted(0.00095, conf_mean=-0.0005)
    res = OC.evaluate(R, frame)
    assert res["verdict"] == "NO_EDGE" and res["gate"].startswith("CONF_")
    assert res["untouched_confirmation"] == "FAILED"


def test_11_short_common_history_is_a_data_hold():
    R, frame = _planted(0.00095, n=1800)
    res = OC.evaluate(R, frame)
    assert res["verdict"] == "DATA_HOLD" and "COMMON_HISTORY_1800_BELOW_2268" in res["why"]
    assert "qualification" not in res


def test_12_owner_path_adapters_normalise_without_respecifying():
    idx = _sessions(12, "2024-01-01")
    frame = pd.DataFrame({"r": np.full(12, 0.01), "c": np.full(12, 0.0002)}, index=idx)
    periods = OC.periods_from_positions(idx[[0, 5, 10]], [-1.0, np.nan, 1.0], 1)
    assert periods == [(idx[0], idx[5], -1.0)]                            # NaN position skipped, last row unused
    s = OC.sign_book_daily(periods, frame, cost_bps=1.0)
    assert np.isnan(s.iloc[0]) and np.isnan(s.iloc[6])
    assert s.iloc[1] == pytest.approx(-(0.01 - 0.0002) - 1e-4)
    assert s.iloc[3] == pytest.approx(-(0.01 - 0.0002))
    assert s.iloc[5] == pytest.approx(-(0.01 - 0.0002) - 1e-4)
    sess = pd.DatetimeIndex(["2024-07-03", "2024-07-05", "2024-07-08"])
    fx = pd.Series([0.01, 0.02, 0.03], index=pd.DatetimeIndex(["2024-07-03", "2024-07-04", "2024-07-05"]))
    m = OC.to_sessions(fx, sess)
    assert m.iloc[0] == pytest.approx(0.01) and m.iloc[1] == pytest.approx(1.02 * 1.03 - 1.0)
    assert np.isnan(m.iloc[2])


def test_13_the_executor_contract_holds_without_loading_a_path(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    monkeypatch.setattr(OC, "load_frontier", _frozen_frontier)

    def forbidden(frame):
        raise AssertionError("gate 1 held: no path may be loaded")
    monkeypatch.setattr(OC, "load_fx_carry", forbidden)
    monkeypatch.setattr(OC, "load_spy_skew", forbidden)
    out = OC.run_mechanism(mechanism=_entry())
    assert MX.validate_result(out) == [] and out["verdict"] == "DATA_HOLD" and out["capital_eligible"] is False
    assert out["statistic"]["eligible"] == list(OC.FROZEN_ELIGIBLE)
    art = json.loads(Path(out["artifact"]).read_text(encoding="utf-8"))
    assert art["result"]["paths_loaded"] == [] and art["result"]["confirmation"]["state"] == "UNREAD"

    def boom():
        raise OSError("the estate is unreadable")
    monkeypatch.setattr(OC, "load_frontier", boom)
    held = OC.run_mechanism(mechanism=_entry())
    assert held["verdict"] == "DATA_HOLD" and MX.validate_result(held) == []
