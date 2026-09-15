"""Executor for FX_MONTH_END_EQUITY_HEDGE_REBALANCING_V1 - synthetic data only.

Pinned: the contract (kill rule, mechanism id), the frozen window (signal L[-3], entry at the L[-2]
close, exit at the L[-1] close), the frozen direction (sell the currency of the market that
outperformed the S&P 500 month to date), the stale-index rule, the FXE/FXY data gate, the gate order,
the unread confirmation after a qualification failure, the multiplicity burden and the Alpha Agent
executor contract.
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
from alpha_agent.alpha_recovery import fx_month_end_hedge_rebalancing as FXM  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

CATALOG = _ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"


def _entry():
    body = json.loads(CATALOG.read_text(encoding="utf-8"))
    return next(m for m in body["mechanisms"] if m["mechanism_id"] == FXM.MECHANISM_ID)


def test_01_the_kill_rule_is_the_catalogs_byte_for_byte():
    assert _entry()["pnl_gate"]["KILL_RULE"] == FXM.KILL_RULE_FROZEN


def test_02_a_changed_contract_is_refused():
    entry = _entry()
    with pytest.raises(ValueError):
        FXM.run_mechanism(mechanism={**entry, "mechanism_id": "OTHER"})
    with pytest.raises(ValueError):
        FXM.run_mechanism(mechanism=dict(entry, pnl_gate=dict(entry["pnl_gate"], KILL_RULE="Kill if nothing.")))


def test_03_multiplicity_is_one_cell_plus_four_inherited_nulls():
    assert FXM.multiplicity(0.015)["m"] == 5 and FXM.multiplicity(0.015)["passes"] is True
    assert FXM.multiplicity(0.03)["passes"] is False and FXM.multiplicity(None)["passes"] is False
    assert abs(FXM.multiplicity(0.5)["single_survivor_threshold"] - 0.10 / 5) < 1e-12


def _inputs(effect: float, *, conf_effect=None, seed: int = 11, etf_ok: bool = True):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("1999-01-04", "2026-09-11")
    n = len(idx)
    common = rng.normal(0.0, 0.006, n)
    es = pd.Series(common + rng.normal(0.0003, 0.007, n), index=idx)
    eq = {k: pd.Series(0.6 * common + rng.normal(0.0002, 0.008, n), index=idx) for k in FXM.PAIRS}
    fx = {k: pd.Series(rng.normal(0.0, 0.006, n), index=idx) for k in FXM.PAIRS}
    d0 = FXM.windows(fx, eq, es)
    loc = {dt: i for i, dt in enumerate(idx)}
    for _, row in d0.iterrows():
        eff = effect if (conf_effect is None or row["signal_date"] < pd.Timestamp(FXM.CONFIRMATION[0])) \
            else conf_effect
        for k in FXM.PAIRS:
            if row["w_%s" % k] != 0.0:
                fx[k].iloc[loc[row["exit_date"]]] += -np.sign(row["d_%s" % k]) * eff
    etf = {"EUR": fx["EUR"] + rng.normal(0, 0.0008, n),
           "JPY": (fx["JPY"] + rng.normal(0, 0.0008, n)) if etf_ok else pd.Series(rng.normal(0, 0.006, n), index=idx)}
    return {"fx": fx, "eq": eq, "es": es, "etf": etf, "problems": []}


def test_04_the_window_and_the_frozen_direction():
    idx = pd.bdate_range("2009-09-01", "2010-04-30")                  # 63 sessions of volatility first
    zero = pd.Series(0.0, index=idx)
    fx = {k: pd.Series(np.random.default_rng(1).normal(0, 0.006, len(idx)), index=idx) for k in FXM.PAIRS}
    eq = {k: zero.copy() for k in FXM.PAIRS}
    march = idx[idx.to_period("M") == pd.Period("2010-03")]
    eq["EUR"].loc[march[3]] = 0.05                     # the euro-area market outperforms in March
    d = FXM.windows(fx, eq, zero.copy())
    row = d[d["signal_date"].dt.to_period("M") == pd.Period("2010-03")].iloc[0]
    assert row["signal_date"] == march[-3] and row["entry_date"] == march[-2] and row["exit_date"] == march[-1]
    assert row["d_EUR"] > 0 and row["w_EUR"] < 0     # sell the euro into the fix
    assert row["w_JPY"] == 0.0                       # no relative move: no position


def test_05_a_stale_foreign_index_leaves_its_pair_flat():
    idx = pd.bdate_range("2010-01-01", "2010-04-30")
    fx = {k: pd.Series(np.random.default_rng(2).normal(0, 0.006, len(idx)), index=idx) for k in FXM.PAIRS}
    eq = {k: pd.Series(np.random.default_rng(3).normal(0, 0.01, len(idx)), index=idx) for k in FXM.PAIRS}
    eq["CHF"] = eq["CHF"].loc[:"2010-03-10"]         # no CHF index bar within 5 days of the March signal
    d = FXM.windows(fx, eq, pd.Series(np.random.default_rng(4).normal(0, 0.01, len(idx)), index=idx))
    row = d[d["signal_date"].dt.to_period("M") == pd.Period("2010-03")].iloc[0]
    assert np.isnan(row["d_CHF"]) and row["w_CHF"] == 0.0


def test_06_a_wrong_sign_is_no_edge_and_leaves_the_confirmation_unread():
    res = FXM.run(verbose=False, write=False, inputs=_inputs(-0.004))["result"]
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "WRONG_SIGN"
    assert res["confirmation"]["state"] == "UNREAD" and res["untouched_confirmation"] == "NOT_READ"


def test_07_a_failed_validation_or_a_missing_series_is_a_data_hold():
    res = FXM.run(verbose=False, write=False, inputs=_inputs(0.004, etf_ok=False))["result"]
    assert res["verdict"] == "DATA_HOLD" and "VALIDATION_JPY_FXY" in res["why"]
    inp = _inputs(0.004)
    inp["eq"]["CAD"] = None
    inp["problems"] = ["SERIES_MISSING_&SXF: not found"]
    res = FXM.run(verbose=False, write=False, inputs=inp)["result"]
    assert res["verdict"] == "DATA_HOLD" and "SERIES_MISSING_&SXF" in res["why"]


def test_08_a_planted_effect_that_does_not_reproduce_fails_at_confirmation():
    res = FXM.run(verbose=False, write=False, inputs=_inputs(0.004, conf_effect=-0.004))["result"]
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "CONFIRMATION" and res["untouched_confirmation"] == "FAILED"


def test_09_a_reproduced_planted_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    body = FXM.run(verbose=False, write=True, inputs=_inputs(0.004))
    res = body["result"]
    assert res["verdict"] == "QUALIFIED" and body["capital_eligible"] is False
    assert res["multiplicity"]["m"] == 5 and res["untouched_confirmation"] == "CONFIRMED"
    assert set(res["stability_ann_net"]) == {"H1_2000_2007", "H2_2008_2014", "LOCO_EX_EUR", "LOCO_EX_JPY",
                                             "LOCO_EX_CHF", "LOCO_EX_CAD"}
    assert Path(body["artifact_path"]).resolve().is_relative_to((tmp_path / "research").resolve())
    out = FXM.executor_result(body)
    assert MX.validate_result(out) == [] and out["verdict"] == "QUALIFIED"


def test_10_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    inp = _inputs(-0.001)
    monkeypatch.setattr(FXM, "load_inputs", lambda: inp)
    out = FXM.run_mechanism(mechanism=_entry())
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False

    def boom():
        raise OSError("norgate is not running")
    monkeypatch.setattr(FXM, "load_inputs", boom)
    held = FXM.run_mechanism(mechanism=_entry())
    assert held["verdict"] == "DATA_HOLD" and MX.validate_result(held) == []
