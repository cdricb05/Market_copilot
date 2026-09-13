"""Executor for RELATIVE_INDEX_IMPLIED_VOL_HEDGING_DEMAND_V1 - synthetic data only.

Protects the strictly prior realised volatility and z-score, the next-close entry, the frozen
sign, leg agreement, the data-integrity gate, the unread confirmation after a qualification
failure, and the Alpha Agent executor contract.
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
from alpha_agent.alpha_recovery import relative_index_implied_vol as RI  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402


def _inputs(eff_ndx: float, eff_rut: float, *, conf_sign: float = 1.0, seed: int = 5):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2001-01-02", "2026-09-11")
    n = len(idx)
    mkt = rng.normal(0.0003, 0.009, n)
    rets = {"SPY": mkt, "QQQ": mkt + rng.normal(0, 0.006, n), "IWM": mkt + rng.normal(0, 0.006, n)}
    vix = pd.Series(18 + rng.normal(0, 1.0, n).cumsum() * 0.02 + rng.normal(0, 2.0, n), index=idx).clip(9)
    iv = {"VIXCLS": vix, "VXNCLS": vix + 3 + rng.normal(0, 3.0, n), "RVXCLS": vix + 4 + rng.normal(0, 3.0, n)}
    tr0 = {k: pd.Series(np.cumprod(1 + v), index=idx) for k, v in rets.items()}
    d0 = RI.decisions(RI.build_panel(iv, tr0))
    loc = {dt: i for i, dt in enumerate(idx)}
    for _, row in d0.iterrows():
        e, x = loc[row["entry_date"]], loc[row["exit_date"]]
        sign = conf_sign if row["entry_date"] >= pd.Timestamp(RI.CONFIRMATION[0]) else 1.0
        for leg, eff, etf in (("NDX", eff_ndx, "QQQ"), ("RUT", eff_rut, "IWM")):
            rets[etf][e + 1:x + 1] += sign * eff * row["s_" + leg]
    tr = {k: pd.Series(np.cumprod(1 + v), index=idx) for k, v in rets.items()}
    return iv, tr, vix


def test_realised_volatility_and_the_z_score_only_read_the_past():
    iv, tr, _ = _inputs(0.0, 0.0)
    p1 = RI.build_panel(iv, tr)
    cut = p1.index[3000]
    tr2 = {k: v.where(v.index <= cut, v * 3.0) for k, v in tr.items()}
    iv2 = {k: v.where(v.index <= cut, 80.0) for k, v in iv.items()}
    p2 = RI.build_panel(iv2, tr2)
    for col in ("z_NDX", "z_RUT"):
        assert np.allclose(p1[col].loc[:cut].to_numpy(), p2[col].loc[:cut].to_numpy(), equal_nan=True)


def test_entry_is_the_close_after_the_decision():
    iv, tr, _ = _inputs(0.0, 0.0)
    panel = RI.build_panel(iv, tr)
    d = RI.decisions(panel)
    row = d.iloc[5]
    i = list(panel.index).index(row["decision_date"])
    assert row["entry_date"] == panel.index[i + 1] and row["exit_date"] == panel.index[i + 1 + RI.HORIZON]


def test_a_wrong_sign_closes_the_mechanism_and_leaves_confirmation_unread():
    iv, tr, vix = _inputs(-0.004, -0.004)
    res = RI.evaluate(RI.build_panel(iv, tr), integrity_check=RI.integrity(iv["VIXCLS"], vix))
    assert res["verdict"] == "KILLED_WRONG_SIGN" and res["confirmation"]["state"] == "UNREAD"


def test_one_leg_carrying_the_book_is_killed_for_leg_disagreement():
    iv, tr, vix = _inputs(0.006, -0.001)
    res = RI.evaluate(RI.build_panel(iv, tr), integrity_check=RI.integrity(iv["VIXCLS"], vix))
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "LEG_AGREEMENT"


def test_a_fred_series_that_does_not_match_the_owned_vix_is_a_data_hold():
    iv, tr, vix = _inputs(0.004, 0.004)
    rng = np.random.default_rng(9)
    bad = pd.Series(rng.normal(20, 5, len(vix)), index=vix.index)
    res = RI.evaluate(RI.build_panel(iv, tr), integrity_check=RI.integrity(iv["VIXCLS"], bad))
    assert res["verdict"] == "DATA_HOLD" and res["gate"] == "DATA"


def test_a_planted_effect_that_does_not_reproduce_is_killed_at_confirmation():
    iv, tr, vix = _inputs(0.004, 0.004, conf_sign=-1.0)
    res = RI.evaluate(RI.build_panel(iv, tr), integrity_check=RI.integrity(iv["VIXCLS"], vix))
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "UNTOUCHED_CONFIRMATION"


def test_a_reproduced_planted_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    iv, tr, vix = _inputs(0.004, 0.004)
    body = RI.run(verbose=False, write=True, iv=iv, tr=tr, cboe_vix=vix, incumbent_daily=None)
    assert body["result"]["verdict"] == "QUALIFIED" and body["capital_eligible"] is False
    assert body["result"]["multiplicity"]["m"] == len(RI.LEGS) + len(RI.INHERITED_NULLS)


def test_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    iv, tr, vix = _inputs(-0.002, -0.002)
    monkeypatch.setattr(RI, "load_fred", lambda sid: iv[sid])
    monkeypatch.setattr(RI, "load_total_return", lambda s: tr[s])
    monkeypatch.setattr(RI, "load_cboe_vix", lambda path=RI.CBOE_VIX: vix)
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: None)
    out = RI.run_mechanism(mechanism={"mechanism_id": RI.MECHANISM_ID})
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False
    with pytest.raises(ValueError):
        RI.run_mechanism(mechanism={"mechanism_id": "OTHER"})


def test_a_missing_iv_cache_is_a_data_hold_not_a_download(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "empty_research_root"))
    out = RI.run_mechanism(mechanism={"mechanism_id": RI.MECHANISM_ID})
    assert out["verdict"] == "DATA_HOLD" and out["capital_eligible"] is False
