"""Executor for SPX_TAIL_HEDGE_DEMAND_SKEW_INDEX_1993_2022_V1 - synthetic data only.

Protects the preregistered construction (point-in-time entry, frozen sign, one weekly
cell), the gate order, the rule that the untouched confirmation window is read only after
every qualification gate passes, and the Alpha Agent executor contract.
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
from alpha_agent.alpha_recovery import skew_index_tail_hedge as SK  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402


def _synthetic(effect_qual: float, effect_conf: float, *, seed: int = 7, nan_share: float = 0.0):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("1993-01-29", "2026-09-11")
    n = len(idx)
    skew = 120.0 + np.cumsum(rng.normal(0, 0.8, n)) * 0.05 + rng.normal(0, 3.0, n)
    skew = pd.Series(skew, index=idx)
    if nan_share:
        skew[rng.random(n) < nan_share] = np.nan
    daily = rng.normal(0.0002, 0.004, n)
    panel0 = SK.build_panel(skew, pd.Series(np.cumprod(1.0 + daily), index=idx))
    d0 = SK.decisions(panel0)
    pos_by_entry = {}
    for _, row in d0.iterrows():
        if np.isfinite(row["pos"]):
            pos_by_entry[row["entry_date"]] = row["pos"]
    loc = {dt: i for i, dt in enumerate(idx)}
    planted = np.zeros(n)
    for entry, pos in pos_by_entry.items():
        e = loc[entry]
        eff = effect_conf if entry >= pd.Timestamp(SK.CONFIRMATION[0]) else effect_qual
        planted[e + 1:e + 1 + SK.HORIZON] += eff * pos
    tr = pd.Series(np.cumprod(1.0 + daily + planted), index=idx)
    return skew, tr


def test_entry_is_the_close_after_the_decision_and_the_sign_is_frozen_negative():
    idx = pd.bdate_range("2000-01-03", periods=120)
    skew = pd.Series(np.linspace(100, 160, 120), index=idx)
    tr = pd.Series(np.linspace(100, 200, 120), index=idx)
    d = SK.decisions(SK.build_panel(skew, tr))
    row = d.iloc[0]
    i = list(idx).index(row["decision_date"])
    assert row["entry_date"] == idx[i + 1] and row["exit_date"] == idx[i + 1 + SK.HORIZON]
    assert row["spy_r"] == pytest.approx(tr.iloc[i + 6] / tr.iloc[i + 1] - 1.0)
    assert row["z"] > 0 and row["pos"] == -1.0
    assert (d["decision_date"].diff().dropna() > pd.Timedelta(0)).all()


def test_the_z_score_never_reads_the_decision_day_or_later():
    idx = pd.bdate_range("2000-01-03", periods=200)
    skew = pd.Series(np.r_[np.full(150, 120.0) + np.sin(np.arange(150)), np.full(50, 500.0)], index=idx)
    tr = pd.Series(np.linspace(100, 110, 200), index=idx)
    z = SK.build_panel(skew, tr)["z"]
    changed = SK.build_panel(skew.where(skew.index < idx[151], 120.0), tr)["z"]
    assert np.allclose(z.iloc[:151].to_numpy(), changed.iloc[:151].to_numpy(), equal_nan=True)


def test_a_wrong_sign_closes_the_mechanism_and_leaves_confirmation_unread():
    skew, tr = _synthetic(-0.002, 0.002)
    res = SK.evaluate(SK.build_panel(skew, tr))
    assert res["verdict"] == "KILLED_WRONG_SIGN"
    assert res["confirmation"]["state"] == "UNREAD"


def test_missing_signal_is_a_data_hold_not_a_result():
    skew, tr = _synthetic(0.002, 0.002, nan_share=0.4)
    res = SK.evaluate(SK.build_panel(skew, tr))
    assert res["verdict"] == "DATA_HOLD" and res["gate"] == "DATA"


def test_a_planted_effect_that_does_not_reproduce_is_killed_at_confirmation():
    skew, tr = _synthetic(0.0015, -0.0015)
    res = SK.evaluate(SK.build_panel(skew, tr))
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "UNTOUCHED_CONFIRMATION"


def test_a_planted_effect_that_reproduces_qualifies_but_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    skew, tr = _synthetic(0.0015, 0.0015)
    body = SK.run(verbose=False, write=True, skew=skew, tr=tr, incumbent_daily=None)
    assert body["result"]["verdict"] == "QUALIFIED"
    assert body["capital_eligible"] is False
    assert Path(body["artifact_path"]).resolve().is_relative_to((tmp_path / "research").resolve())
    assert body["result"]["multiplicity"]["m"] == 1 + len(SK.INHERITED_NULLS)


def test_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    skew, tr = _synthetic(-0.002, 0.0)
    monkeypatch.setattr(SK, "load_skew", lambda path=SK.SKEW_PATH: skew)
    monkeypatch.setattr(SK, "load_spy_total_return", lambda: tr)
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: None)
    out = SK.run_mechanism(mechanism={"mechanism_id": SK.MECHANISM_ID})
    assert MX.validate_result(out) == []
    assert out["verdict"] in MX.EXECUTOR_VERDICTS and out["capital_eligible"] is False
    with pytest.raises(ValueError):
        SK.run_mechanism(mechanism={"mechanism_id": "SOMETHING_ELSE"})
