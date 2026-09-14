r"""The merger-arbitrage executor on SYNTHETIC data only.

No vendor, no network, no real price. What is pinned: the contract (kill rule, mechanism id),
the PIT entry (strictly after the establishing filing date), the fixed-slot book arithmetic
(price change, bill financing, costs on entry and exit notional, delisting exit, the hold cap,
capacity), the untouched confirmation, the gate order and the multiplicity burden.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from paper_trader.alpha_agent.alpha_recovery import merger_arb_events as ME
from paper_trader.alpha_agent.alpha_recovery import merger_arbitrage_cash_deal as MA

CATALOG = Path(__file__).resolve().parents[1] / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"


def _entry():
    body = json.loads(CATALOG.read_text(encoding="utf-8"))
    return next(m for m in body["mechanisms"] if m["mechanism_id"] == MA.MECHANISM_ID)


def test_01_the_kill_rule_is_the_catalogs_byte_for_byte():
    assert _entry()["pnl_gate"]["KILL_RULE"] == MA.KILL_RULE_FROZEN


def test_02_a_changed_contract_is_refused():
    entry = _entry()
    with pytest.raises(ValueError):
        MA.run_mechanism(mechanism={**entry, "mechanism_id": "OTHER"})
    changed = dict(entry, pnl_gate=dict(entry["pnl_gate"], KILL_RULE="Kill if nothing."))
    with pytest.raises(ValueError):
        MA.run_mechanism(mechanism=changed)


def test_03_multiplicity_inherits_six_cells_at_p_one():
    m = MA.multiplicity(0.001)
    assert m["m"] == 7 and m["passes"] is True
    assert abs(m["single_survivor_threshold"] - 0.10 / 7) < 1e-12
    assert MA.multiplicity(0.02)["passes"] is False
    assert MA.multiplicity(None)["passes"] is False


def _cal(n=40, start="2010-01-04"):
    return pd.bdate_range(start, periods=n)


def _loader(series: dict):
    def load(symbols):
        return {s: series[s] for s in symbols if s in series}
    return load


def test_04_entry_is_strictly_after_the_establishing_date():
    cal = _cal()
    assert str(cal[MA.entry_index(cal, str(cal[3].date()))].date()) == str(cal[4].date())
    # a weekend establishing date enters on the next session
    assert MA.entry_index(cal, "2010-01-09") == int(cal.searchsorted(pd.Timestamp("2010-01-11")))


def test_05_slot_book_arithmetic_delisting_costs_and_financing():
    cal = _cal()
    # the target trades at 95, rises to 100 and stops printing (the deal closed)
    px = pd.Series([95.0] * 5 + [96.0, 97.0, 98.0, 99.0, 100.0], index=cal[:10])
    reader = MA.PriceReader(_loader({"TGT-201002": px}))
    rf = np.full(len(cal), 0.0001)
    deal = {"target_cik": "1", "symbol": "TGT-201002", "established_on": str(cal[3].date())}
    b = MA.book([deal], reader, cal, rf, "2010-01-01", "2010-12-31", slots=10, cost_bps=12.5)
    assert b["deals"] == 1
    rec = b["records"][0]
    assert rec["entry"] == str(cal[4].date()) and rec["exit"] == str(cal[9].date())
    assert rec["exit_reason"] == "LAST_QUOTED_SESSION"
    w, c = 0.1, 0.00125
    vals = np.array([95, 96, 97, 98, 99, 100]) / 95.0
    expected = w * (vals[-1] - 1.0) - w * rf[5] * vals[:-1].sum() - w * c - w * vals[-1] * c
    assert abs(b["daily"].sum() - expected) < 1e-12


def test_06_the_hold_cap_binds_and_capacity_refuses_a_full_book():
    cal = _cal(n=200)
    px = pd.Series(np.linspace(50, 60, len(cal)), index=cal)
    reader = MA.PriceReader(_loader({"A": px, "B": px, "C": px}))
    rf = np.zeros(len(cal))
    deals = [{"target_cik": str(i), "symbol": s, "established_on": str(cal[i].date())}
             for i, s in enumerate(("A", "B", "C"))]
    b = MA.book(deals, reader, cal, rf, "2010-01-01", "2011-12-31", slots=2, cost_bps=0.0)
    assert b["deals"] == 2 and b["skipped"]["CAPACITY"] == 1
    assert all(r["held_sessions"] == MA.HOLD_CAP for r in b["records"])


def test_07_a_read_through_bound_is_never_exceeded():
    cal = _cal(n=200)
    px = pd.Series(np.linspace(50, 60, len(cal)), index=cal)
    reader = MA.PriceReader(_loader({"A": px}))
    deal = {"target_cik": "1", "symbol": "A", "established_on": str(cal[2].date())}
    bound = str(cal[20].date())
    MA.book([deal], reader, cal, np.zeros(len(cal)), "2010-01-01", "2010-12-31", read_through=bound)
    assert reader.max_date_read <= bound


def test_08_gates_close_in_the_preregistered_order():
    good = {"ann_net": 0.05, "nw_t": 3.0}
    inc = {"t": 2.5}
    halves = {"H1": {"ann_net": 0.02}, "H2": {"ann_net": 0.03}}
    ok = {"passes": True, "m": 7}
    assert MA.qualification_gate(good, inc, halves, ok) is None
    assert MA.qualification_gate({"ann_net": -0.01, "nw_t": -1}, inc, halves, ok)["verdict"] == MA.V_WRONG_SIGN
    assert MA.qualification_gate({"ann_net": 0.05, "nw_t": 1.9}, inc, halves, ok)["verdict"] == MA.V_NO_EDGE
    assert MA.qualification_gate({"ann_net": 0.01, "nw_t": 3.0}, inc, halves, ok)["verdict"] == MA.V_MATERIALITY
    assert MA.qualification_gate(good, {"t": 1.0}, halves, ok)["verdict"] == MA.V_NONINCREMENTAL
    assert MA.qualification_gate(good, inc, {"H1": {"ann_net": -0.001}, "H2": {"ann_net": 0.1}},
                                 ok)["verdict"] == MA.V_UNSTABLE
    assert MA.qualification_gate(good, inc, halves, {"passes": False, "m": 7})["verdict"] == MA.V_MULTIPLICITY


def _calendar(n_included, start="2004-01-05", unresolved=0):
    cal = []
    days = pd.bdate_range(start, periods=n_included + unresolved)
    for i in range(n_included):
        cal.append({"state": ME.ST_INCLUDED, "target_cik": str(i), "first_filed": str(days[i].date()),
                    "established_on": str(days[i].date()), "symbol": "S%d" % i})
    for j in range(unresolved):
        d = str(days[n_included + j].date())
        cal.append({"state": ME.ST_UNRESOLVED, "target_cik": "u%d" % j, "first_filed": d})
    return cal


def test_09_the_data_gate_holds_before_any_price_is_read():
    inputs = {"calendar": _calendar(10, unresolved=30), "sessions": _cal(n=3000, start="2002-01-01"),
              "rf_daily": np.zeros(3000), "price_loader": _loader({}), "head_coverage": 1.0,
              "audit": {"verified": True, "precision": 0.95}, "spy": object(), "rf": object(),
              "spy_excess": pd.Series(dtype=float)}
    out = MA.evaluate(inputs)
    assert out["executor_result"]["verdict"] == MA.V_DATA_HOLD
    assert out["max_date_read"] is None
    assert out["confirmation"] == "UNTOUCHED"


def test_10_an_unverified_audit_is_a_data_hold():
    inputs = {"calendar": _calendar(400), "sessions": _cal(n=3000, start="2002-01-01"),
              "rf_daily": np.zeros(3000), "price_loader": _loader({}), "head_coverage": 1.0,
              "audit": {"verified": False}, "spy": object(), "rf": object(), "spy_excess": pd.Series(dtype=float)}
    assert MA.evaluate(inputs)["executor_result"]["verdict"] == MA.V_DATA_HOLD


def _full_inputs(monkeypatch, drift: float):
    """A synthetic estate that clears gate 1: 200 qualification and 80 confirmation deals."""
    sessions = pd.bdate_range("2002-01-01", periods=6400)
    rng = np.random.default_rng(11)
    cal, series = [], {}
    q_days = [d for d in sessions if "2003-01-01" <= str(d.date()) <= "2016-10-31"]
    c_days = [d for d in sessions if "2017-01-01" <= str(d.date()) <= "2026-03-31"]
    picks = [q_days[i] for i in np.linspace(0, len(q_days) - 1, 200).astype(int)] + \
            [c_days[i] for i in np.linspace(0, len(c_days) - 1, 80).astype(int)]
    for k, day in enumerate(picks):
        sym = "T%03d" % k
        e = int(sessions.searchsorted(day, side="right"))
        path = 100.0 * (1.0 + drift * np.arange(61) / 60.0 + rng.normal(0, 0.001, 61))
        series[sym] = pd.Series(path, index=sessions[e:e + 61])
        cal.append({"state": ME.ST_INCLUDED, "target_cik": str(k), "first_filed": str(day.date()),
                    "established_on": str(day.date()), "symbol": sym})
    spy_excess = pd.Series(rng.normal(0.0003, 0.01, len(sessions)), index=sessions)
    monkeypatch.setattr(MA, "CALENDAR_HASH", ME.calendar_hash(cal))
    return {"calendar": cal, "calendar_hash": ME.calendar_hash(cal), "sessions": sessions,
            "rf_daily": np.full(len(sessions), 0.00005), "price_loader": _loader(series), "head_coverage": 1.0,
            "audit": {"verified": True, "precision": 0.97}, "spy": object(), "rf": object(),
            "spy_excess": spy_excess}


def test_10b_the_whole_evaluation_runs_and_confirms_only_after_qualification(monkeypatch):
    # 200 deals over 14 years at one 60th of NAV each: a 3 % deal nets about 0.6 %/yr, below the
    # materiality floor (measured by this test), so the profitable fixture carries 30 % per deal.
    good = MA.evaluate(_full_inputs(monkeypatch, drift=0.30))
    res = good["executor_result"]
    assert res["verdict"] in MA.VERDICTS_USED
    assert res["verdict"] == MA.V_QUALIFIED, res["why"]
    assert good["confirmation"] != "UNTOUCHED"
    assert res["multiplicity"]["m"] == 7 and res["capital_eligible"] is False
    bad = MA.evaluate(_full_inputs(monkeypatch, drift=-0.30))
    assert bad["executor_result"]["verdict"] == MA.V_WRONG_SIGN
    assert bad["confirmation"] == "UNTOUCHED"
    assert bad["executor_result"]["statistic"]["lockbox_t"] is None
    assert bad["max_date_read"] <= "2017-06-30"


def test_10c_a_moved_calendar_is_a_data_hold(monkeypatch):
    inputs = _full_inputs(monkeypatch, drift=0.03)
    monkeypatch.setattr(MA, "CALENDAR_HASH", "not-the-frozen-calendar")
    out = MA.evaluate(inputs)
    assert out["executor_result"]["verdict"] == MA.V_DATA_HOLD
    assert "calendar moved" in out["executor_result"]["why"]


def test_11_capital_is_never_eligible():
    res = MA.hold_result("x")
    assert res["capital_eligible"] is False
    src = Path(MA.__file__).read_text(encoding="utf-8")
    for token in ("place_order", "submit_order", "promote_model(", "adopt_prospective_freeze("):
        assert token not in src
