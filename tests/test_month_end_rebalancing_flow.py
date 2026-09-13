"""Executor for MONTH_END_BALANCED_REBALANCING_FLOW_V1 - synthetic data only.

Protects the roll-free leg return, the frozen window and point-in-time entry, the gate order
(including the increment over the unconditional month-end spread), the unread confirmation
after a qualification failure, and the Alpha Agent executor contract.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import alpha_recovery as AR  # noqa: E402
from alpha_agent.alpha_recovery import month_end_rebalancing_flow as ME  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402


def _legs(effect: float, *, conf_effect=None, seed: int = 3, uncond: float = 0.0):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("1997-09-02", "2026-09-11")
    n = len(idx)
    common = rng.normal(0, 0.006, n)
    eq = pd.Series(common + rng.normal(0.0003, 0.008, n), index=idx)
    bd = pd.Series(-0.3 * common + rng.normal(0.0001, 0.003, n), index=idx)
    d0 = ME.windows(eq, bd)
    loc = {dt: i for i, dt in enumerate(idx)}
    eq2, bd2 = eq.copy(), bd.copy()
    for _, row in d0[d0["finite"]].iterrows():
        eff = effect if (conf_effect is None or row["signal_date"] < pd.Timestamp(ME.CONFIRMATION[0])) \
            else conf_effect
        e, x = loc[row["entry_date"]], loc[row["exit_date"]]
        sign = np.sign(row["mtd_rel"])
        for k in range(e + 1, x + 1):
            eq2.iloc[k] += -sign * eff + uncond
            bd2.iloc[k] += sign * eff * 0.4
    etf = {"SPY": eq2 + rng.normal(0, 0.0005, n), "IEF": bd2 + rng.normal(0, 0.0003, n)}
    return eq2, bd2, etf


def test_the_leg_return_removes_the_roll_gap_of_the_unadjusted_series():
    idx = pd.bdate_range("2020-01-01", periods=6)
    unadj = pd.Series([100.0, 101.0, 102.0, 110.0, 111.0, 112.0], index=idx)     # +8 roll jump
    ccb = pd.Series([90.0, 91.0, 92.0, 93.0, 94.0, 95.0], index=idx)             # jump removed
    r = ME.leg_returns(unadj, ccb)
    assert r.iloc[2] == pytest.approx(1.0 / 102.0)
    assert r.max() < 0.011


def test_the_window_is_signal_then_next_close_entry_then_first_session_of_next_month():
    eq, bd, _ = _legs(0.0)
    d = ME.windows(eq, bd)
    row = d[d["finite"]].iloc[10]
    month = row["signal_date"].to_period("M")
    sessions = eq.index[eq.index.to_period("M") == month]
    assert row["signal_date"] == sessions[-5] and row["entry_date"] == sessions[-4]
    assert row["exit_date"].to_period("M") == month + 1 and row["sessions_held"] == 4


def test_a_wrong_sign_closes_the_mechanism_and_leaves_confirmation_unread():
    eq, bd, etf = _legs(-0.0015)
    res = ME.evaluate(eq, bd, etf=etf)
    assert res["verdict"] == "KILLED_WRONG_SIGN" and res["confirmation"]["state"] == "UNREAD"


def test_a_failed_data_validation_is_a_data_hold():
    eq, bd, etf = _legs(0.0015)
    rng = np.random.default_rng(11)
    etf["IEF"] = pd.Series(rng.normal(0, 0.003, len(etf["IEF"])), index=etf["IEF"].index)
    res = ME.evaluate(eq, bd, etf=etf)
    assert res["verdict"] == "DATA_HOLD" and res["gate"] == "DATA"


def test_a_planted_effect_that_does_not_reproduce_is_killed_at_confirmation():
    eq, bd, etf = _legs(0.0015, conf_effect=-0.0015)
    res = ME.evaluate(eq, bd, etf=etf)
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "UNTOUCHED_CONFIRMATION"


def test_a_reproduced_planted_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    eq, bd, etf = _legs(0.0015)
    body = ME.run(verbose=False, write=True, eq=eq, bd=bd, etf=etf, incumbent_daily=None)
    assert body["result"]["verdict"] == "QUALIFIED" and body["capital_eligible"] is False
    assert body["result"]["multiplicity"]["m"] == 1 + len(ME.INHERITED_NULLS)
    assert Path(body["artifact_path"]).resolve().is_relative_to((tmp_path / "research").resolve())


def test_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    eq, bd, etf = _legs(-0.001)
    monkeypatch.setattr(ME, "load_futures", lambda s: eq if s == ME.EQUITY else bd)
    monkeypatch.setattr(ME, "load_etf_total_return", lambda s: etf[s])
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: None)
    out = ME.run_mechanism(mechanism={"mechanism_id": ME.MECHANISM_ID})
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False
    with pytest.raises(ValueError):
        ME.run_mechanism(mechanism={"mechanism_id": "OTHER"})
