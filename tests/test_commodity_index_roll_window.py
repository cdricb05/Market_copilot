"""Executor for COMMODITY_INDEX_ROLL_WINDOW_PRESSURE_V1 - synthetic data only.

Protects the frozen window (business days 3..10), the skipped market-month when the held contract
changes, the passive-spread control, leave-one-market-out, the unread confirmation after a
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
from alpha_agent.alpha_recovery import commodity_index_roll_window as CR  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

SYMS = tuple("M%02d" % i for i in range(12))


def _frames(*, effect: float = 0.0, conf_sign: float = 1.0, drift: float = 0.0, only: str = None,
            seed: int = 4):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("1994-01-03", "2026-09-11")
    frames = {}
    for s in SYMS:
        n = len(idx)
        ret = rng.normal(0.0, 0.015, n)
        ret2 = ret + rng.normal(0.0, 0.002, n)
        month = idx.to_period("M")
        held = np.array(["%s-%s" % (s, m) for m in month])       # rolls at each month start
        df = pd.DataFrame({"Date": idx, "ret": ret, "ret2": ret2 + drift, "held": held})
        bd = df.groupby(month).cumcount().to_numpy()
        win = (bd >= CR.ENTRY_INDEX + 1) & (bd <= CR.EXIT_INDEX)
        if effect and (only is None or only == s):
            sign = np.where(idx >= pd.Timestamp("2015-01-01"), conf_sign, 1.0)
            per = effect / CR.WINDOW_SESSIONS
            df.loc[win, "ret2"] += sign[win] * per / 2.0
            df.loc[win, "ret"] -= sign[win] * per / 2.0
        frames[s] = df
    return frames


COSTS = {s: 5.0 for s in SYMS}


def test_the_window_is_business_days_three_to_ten_and_a_roll_inside_skips_the_month():
    f = _frames()["M00"]
    mm, _ = CR.market_months(f)
    row = mm.iloc[5]
    g = f[f["Date"].dt.to_period("M") == row["ym"]]
    assert row["entry_date"] == g["Date"].iloc[2] and row["exit_date"] == g["Date"].iloc[9]
    f2 = f.copy()
    month_rows = f2.index[f2["Date"].dt.to_period("M") == row["ym"]]
    f2.loc[month_rows[6]:, "held"] = "ROLLED"
    mm2, _ = CR.market_months(f2)
    assert row["ym"] not in set(mm2["ym"])


def test_a_wrong_sign_closes_the_mechanism_and_leaves_confirmation_unread():
    res = CR.evaluate(_frames(effect=-0.01), COSTS)
    assert res["verdict"] == "KILLED_WRONG_SIGN" and res["confirmation"]["state"] == "UNREAD"


def test_ordinary_spread_drift_is_not_roll_pressure():
    res = CR.evaluate(_frames(drift=0.0006), COSTS)
    assert res["verdict"] == "KILLED_NONINCREMENTAL", res["why"]


def test_one_market_carrying_the_book_is_killed():
    res = CR.evaluate(_frames(effect=0.25, only="M03"), COSTS)
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "LEAVE_ONE_MARKET_OUT", res["why"]


def test_a_roll_effect_that_does_not_reproduce_is_killed_at_confirmation():
    res = CR.evaluate(_frames(effect=0.01, conf_sign=-1.0), COSTS)
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "UNTOUCHED_CONFIRMATION"


def test_a_reproduced_roll_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    body = CR.run(verbose=False, write=True, frames=_frames(effect=0.01), costs=COSTS, incumbent_daily=None)
    assert body["result"]["verdict"] == "QUALIFIED", body["result"]["why"]
    assert body["capital_eligible"] is False


def test_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    frames = _frames(effect=-0.01)
    monkeypatch.setattr(CR, "load_frames", lambda universe=CR.UNIVERSE: frames)
    monkeypatch.setattr(CR, "load_costs", lambda universe=CR.UNIVERSE: COSTS)
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: None)
    out = CR.run_mechanism(mechanism={"mechanism_id": CR.MECHANISM_ID})
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False
    with pytest.raises(ValueError):
        CR.run_mechanism(mechanism={"mechanism_id": "OTHER"})
