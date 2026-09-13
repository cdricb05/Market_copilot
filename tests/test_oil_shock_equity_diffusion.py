"""Executor for OIL_SHOCK_EQUITY_SLOW_DIFFUSION_V1 - synthetic data only.

Protects the roll-free oil return, the next-month entry, the frozen sign, the reversal-confound
gate that separates diffusion from monthly index reversal, the unread confirmation after a
qualification failure, and the Alpha Agent executor contract.
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
from alpha_agent.alpha_recovery import oil_shock_equity_diffusion as OI  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402


def _synthetic(*, diffusion: float = 0.0, reversal: float = 0.0, oil_tracks_spy: bool = False,
               conf_sign: float = 1.0, seed: int = 21):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("1992-12-01", "2026-09-11")
    spy_d = rng.normal(0.0003, 0.009, len(idx))
    oil_d = rng.normal(0.0, 0.02, len(idx))
    months = pd.PeriodIndex(idx, freq="M")
    uniq = months.unique()
    for k, m in enumerate(uniq[:-1]):
        cur = months == m
        nxt = months == uniq[k + 1]
        spy_m = float(np.prod(1 + spy_d[cur]) - 1)
        if oil_tracks_spy:
            oil_d[cur] = spy_d[cur] * 2.0 + rng.normal(0, 0.002, cur.sum())
        oil_m = float(np.prod(1 + oil_d[cur]) - 1)
        sign = conf_sign if m.start_time >= pd.Timestamp("2012-12-01") else 1.0
        spy_d[nxt] += sign * (-diffusion * np.sign(oil_m) - reversal * np.sign(spy_m)) / max(1, nxt.sum())
    spy = pd.Series(np.cumprod(1 + spy_d), index=idx)
    oil_r = pd.Series(oil_d, index=idx)
    return oil_r, spy


def test_the_roll_free_oil_return_ignores_the_unadjusted_roll_gap():
    idx = pd.bdate_range("2020-01-01", periods=5)
    u = pd.Series([50.0, 51.0, 60.0, 61.0, 62.0], index=idx)
    c = pd.Series([40.0, 41.0, 42.0, 43.0, 44.0], index=idx)
    r = OI.leg_returns(u, c)
    assert r.iloc[1] == pytest.approx(1.0 / 51.0) and r.max() < 0.03


def test_entry_is_the_first_close_of_the_next_month_and_the_sign_is_negative():
    oil_r, spy = _synthetic()
    d = OI.periods(oil_r, spy)
    row = d.iloc[40]
    assert row["entry_date"].to_period("M") == row["signal_month"].to_period("M") + 1
    assert row["exit_date"].to_period("M") == row["signal_month"].to_period("M") + 2
    assert row["pos"] == -np.sign(row["oil_m"])


def test_a_wrong_sign_closes_the_mechanism_and_leaves_confirmation_unread():
    oil_r, spy = _synthetic(diffusion=-0.03)
    res = OI.evaluate(oil_r, spy)
    assert res["verdict"] == "KILLED_WRONG_SIGN" and res["confirmation"]["state"] == "UNREAD"


def test_an_oil_rule_that_only_harvests_index_reversal_is_killed_by_the_confound_gate():
    oil_r, spy = _synthetic(reversal=0.04, oil_tracks_spy=True)
    res = OI.evaluate(oil_r, spy)
    assert res["verdict"] == "KILLED_NONINCREMENTAL", res["why"]
    assert res["gate"] in ("REVERSAL_CONFOUND", "PASSIVE_LONG_INCREMENT")


def test_a_diffusion_effect_that_does_not_reproduce_is_killed_at_confirmation():
    oil_r, spy = _synthetic(diffusion=0.03, conf_sign=-1.0)
    res = OI.evaluate(oil_r, spy)
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "UNTOUCHED_CONFIRMATION"


def test_a_reproduced_diffusion_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    oil_r, spy = _synthetic(diffusion=0.03)
    body = OI.run(verbose=False, write=True, oil_r=oil_r, spy_tr=spy, incumbent_daily=None)
    assert body["result"]["verdict"] == "QUALIFIED", body["result"]["why"]
    assert body["capital_eligible"] is False
    assert body["result"]["multiplicity"]["m"] == 1 + len(OI.INHERITED_NULLS)


def test_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    oil_r, spy = _synthetic(diffusion=-0.02)
    monkeypatch.setattr(OI, "load_oil_returns", lambda: oil_r)
    monkeypatch.setattr(OI, "load_spy_total_return", lambda: spy)
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: None)
    out = OI.run_mechanism(mechanism={"mechanism_id": OI.MECHANISM_ID})
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False
    with pytest.raises(ValueError):
        OI.run_mechanism(mechanism={"mechanism_id": "OTHER"})
