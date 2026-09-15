"""Executor for DIVIDEND_MONTH_DEMAND_PREMIUM_V1 - synthetic data only.

Pinned: the contract (kill rule, mechanism id), the membership trap (a delisted security's last flag
does not keep it a member), the payer universe and the t-12 prediction, the delisting return, the
target-weight turnover, the gate order, the unread confirmation after a qualification failure (and no
confirmation month in the artifact), the multiplicity burden and the Alpha Agent executor contract.
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
from alpha_agent.alpha_recovery import dividend_month_demand as DM  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

CATALOG = _ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"


def _entry():
    body = json.loads(CATALOG.read_text(encoding="utf-8"))
    return next(m for m in body["mechanisms"] if m["mechanism_id"] == DM.MECHANISM_ID)


def test_01_the_kill_rule_is_the_catalogs_byte_for_byte():
    assert _entry()["pnl_gate"]["KILL_RULE"] == DM.KILL_RULE_FROZEN


def test_02_a_changed_contract_is_refused():
    entry = _entry()
    with pytest.raises(ValueError):
        DM.run_mechanism(mechanism={**entry, "mechanism_id": "OTHER"})
    with pytest.raises(ValueError):
        DM.run_mechanism(mechanism=dict(entry, pnl_gate=dict(entry["pnl_gate"], KILL_RULE="Kill if nothing.")))


def test_03_multiplicity_is_one_cell_plus_three_inherited_nulls():
    assert DM.multiplicity(0.02)["m"] == 4 and DM.multiplicity(0.02)["passes"] is True
    assert DM.multiplicity(0.03)["passes"] is False and DM.multiplicity(None)["passes"] is False
    assert abs(DM.multiplicity(0.5)["single_survivor_threshold"] - 0.025) < 1e-12


def _daily(values, start="2009-01-02", end="2010-06-30"):
    idx = pd.bdate_range(start, end)
    return idx, pd.Series(np.asarray(values(len(idx)), dtype=float), index=idx)


def test_04_a_delisted_security_is_not_a_member_after_its_last_quote():
    idx, px = _daily(lambda n: 100.0 + np.arange(n))
    gone = px.loc[:"2010-03-10"]
    uni = {"GONE": {"tr": gone, "ex": {(2009, 4)}, "flag": pd.Series(1.0, index=gone.index)},
           "LIVE": {"tr": px, "ex": {(2009, 4)}, "flag": pd.Series(1.0, index=idx)}}
    m = DM.monthly_book(uni, idx).set_index("hold")
    assert m.loc["2010-03", "members"] == 2                   # formation 2010-02-26: both quoted
    assert m.loc["2010-04", "members"] == 1                   # formation 2010-03-31: GONE last quoted 21 days earlier
    assert m.loc["2010-03", "unquoted_end_share"] == pytest.approx(0.5)


def test_05_payers_prediction_legs_turnover_and_the_delisting_return():
    idx, base = _daily(lambda n: np.full(n, 100.0))
    px_a = base.copy()
    px_a.loc["2010-04-01":] = 110.0                           # +10 % in April
    px_c = base.loc[:"2010-04-15"].copy()
    px_c.loc["2010-04-14":] = 90.0                            # delists mid-April at -10 %
    flag = pd.Series(1.0, index=idx)
    uni = {"A": {"tr": px_a, "ex": {(2009, 4), (2009, 7)}, "flag": flag},      # ex in April last year: long
           "B": {"tr": base, "ex": {(2009, 7)}, "flag": flag},                  # payer, not due: short
           "C": {"tr": px_c, "ex": {(2009, 10)}, "flag": flag},                 # payer, not due: short
           "N": {"tr": base, "ex": set(), "flag": flag}}                        # never pays: outside the book
    m = DM.monthly_book(uni, idx).set_index("hold")
    apr = m.loc["2010-04"]
    assert (apr["members"], apr["payers"], apr["n_long"], apr["n_short"]) == (4, 3, 1, 2)
    assert apr["gross"] == pytest.approx(0.10 - (0.0 + (-0.10)) / 2.0)
    assert apr["turnover"] == pytest.approx(2.0)                                 # the first formed book
    assert np.isnan(m.loc["2010-01", "gross"])                                  # no payer due yet: no book


def _synthetic(effect: float, *, conf_effect=None, seed: int = 5, n: int = 500):
    rng = np.random.default_rng(seed)
    dates = pd.DatetimeIndex(pd.date_range("1992-01-31", "2026-08-31", freq="ME"))
    uni = {}
    returns = rng.normal(0.008, 0.06, size=(n, len(dates)))
    for i in range(n):
        payer = i % 5 != 0
        cycle = i % 3 + 1
        ex = {(d.year, d.month) for d in dates if payer and (d.month - cycle) % 3 == 0}
        uni["S%03d" % i] = {"ex": ex, "flag": pd.Series(1.0, index=dates), "r": returns[i]}
    for j in range(1, len(dates)):
        t = (dates[j].year, dates[j].month)
        eff = effect if (conf_effect is None or "%04d-%02d" % t < DM.CONFIRMATION[0]) else conf_effect
        lag12 = DM._shift(t[0], t[1], 12)
        for s, rec in uni.items():
            if lag12 in rec["ex"]:
                rec["r"][j] += eff
    for s, rec in uni.items():
        rec["tr"] = pd.Series(100.0 * np.cumprod(1.0 + rec.pop("r")), index=dates)
    return {"universe": uni, "sessions": dates, "spy_tr": None, "n_symbols": n, "n_failed": 0}


def test_06_a_wrong_sign_is_no_edge_and_no_confirmation_month_is_written():
    res = DM.run(verbose=False, write=False, inputs=_synthetic(-0.012))["result"]
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "WRONG_SIGN"
    assert res["confirmation"]["state"] == "UNREAD" and res["untouched_confirmation"] == "NOT_READ"
    assert all(row[0] <= DM.QUALIFICATION[1] for row in res["qualification_monthly_rows"])


def test_07_load_failures_or_thin_legs_are_a_data_hold():
    inp = _synthetic(0.012)
    inp["n_failed"] = 40
    res = DM.run(verbose=False, write=False, inputs=inp)["result"]
    assert res["verdict"] == "DATA_HOLD" and "LOAD_FAILURES_40_OF_500" in res["why"]
    small = _synthetic(0.012, n=300)
    res = DM.run(verbose=False, write=False, inputs=small)["result"]
    assert res["verdict"] == "DATA_HOLD" and "MEMBERS_MEAN" in res["why"]


def test_08_a_planted_effect_that_does_not_reproduce_fails_at_confirmation():
    res = DM.run(verbose=False, write=False, inputs=_synthetic(0.012, conf_effect=-0.012))["result"]
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "CONFIRMATION" and res["untouched_confirmation"] == "FAILED"


def test_09_a_reproduced_planted_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    body = DM.run(verbose=False, write=True, inputs=_synthetic(0.012))
    res = body["result"]
    assert res["verdict"] == "QUALIFIED" and body["capital_eligible"] is False
    assert res["multiplicity"]["m"] == 4 and res["untouched_confirmation"] == "CONFIRMED"
    assert set(res["stability_ann_net"]) == set(DM.HALVES)
    assert Path(body["artifact_path"]).resolve().is_relative_to((tmp_path / "research").resolve())
    out = DM.executor_result(body)
    assert MX.validate_result(out) == [] and out["verdict"] == "QUALIFIED"


def test_10_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    inp = _synthetic(-0.002)
    monkeypatch.setattr(DM, "load_universe", lambda: inp)
    out = DM.run_mechanism(mechanism=_entry())
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False

    def boom():
        raise OSError("norgate is not running")
    monkeypatch.setattr(DM, "load_universe", boom)
    held = DM.run_mechanism(mechanism=_entry())
    assert held["verdict"] == "DATA_HOLD" and MX.validate_result(held) == []
