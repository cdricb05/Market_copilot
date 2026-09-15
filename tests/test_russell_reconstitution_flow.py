"""Executor for INDEX_MEMBERSHIP_FORCED_FLOW_V1 - synthetic data only.

Pinned: the contract (kill rule, mechanism id), the event rule read only from membership flags (in-window
session with the most changes, a floor of 100), the tradeable quote rule (stale, next-session and open
traps), the name path (open-to-close entry, flat when unquoted or delisted), the dollar-neutral event
accounting with entry and exit costs, the gate order, the unread confirmation after a qualification
failure, the multiplicity burden and the Alpha Agent executor contract.
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
from alpha_agent.alpha_recovery import russell_reconstitution_flow as RF  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

CATALOG = _ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"
SESSIONS = pd.bdate_range("1990-05-01", "2026-09-10")


def _entry():
    body = json.loads(CATALOG.read_text(encoding="utf-8"))
    return next(m for m in body["mechanisms"] if m["mechanism_id"] == RF.MECHANISM_ID)


def test_01_the_kill_rule_is_the_catalogs_byte_for_byte():
    assert _entry()["pnl_gate"]["KILL_RULE"] == RF.KILL_RULE_FROZEN


def test_02_a_changed_contract_is_refused():
    entry = _entry()
    with pytest.raises(ValueError):
        RF.run_mechanism(mechanism={**entry, "mechanism_id": "OTHER"})
    with pytest.raises(ValueError):
        RF.run_mechanism(mechanism=dict(entry, pnl_gate=dict(entry["pnl_gate"], KILL_RULE="Kill if nothing.")))


def test_03_multiplicity_is_one_cell_plus_two_inherited_nulls():
    assert RF.multiplicity(0.03)["m"] == 3 and RF.multiplicity(0.03)["passes"] is True
    assert RF.multiplicity(0.04)["passes"] is False and RF.multiplicity(None)["passes"] is False
    assert RF.multiplicity(0.5)["single_survivor_threshold"] == pytest.approx(0.1 / 3)


def _flag(dates, switch, before, after):
    return pd.Series(np.where(dates < switch, before, after).astype(float), index=dates)


def test_04_the_event_is_the_in_window_session_with_most_changes_and_needs_one_hundred():
    d = SESSIONS[(SESSIONS >= "2000-06-01") & (SESSIONS <= "2001-01-31")]
    flags = {}
    for i in range(120):
        flags["J%d" % i] = _flag(d, pd.Timestamp("2000-07-03"), 0, 1)
    for i in range(150):
        flags["X%d" % i] = _flag(d, pd.Timestamp("2000-12-18"), 0, 1)      # more changes, out of window
    for i in range(20):
        flags["W%d" % i] = _flag(d, pd.Timestamp("2000-06-20"), 1, 0)       # in window, fewer
    ev = RF.find_events(flags, SESSIONS)
    assert ev == {2000: pd.Timestamp("2000-07-03")}
    small = {k: v for k, v in flags.items() if not k.startswith("J")}
    assert RF.find_events(small, SESSIONS) == {}                              # 20 in-window changes < 100


def _px(dates, level=100.0):
    return pd.DataFrame({"Open": level, "Close": level}, index=pd.DatetimeIndex(dates), dtype=float)


def test_05_the_quote_rule_drops_stale_late_and_openless_names():
    E = pd.Timestamp("2005-06-27")
    ie = SESSIONS.get_loc(E)
    around = SESSIONS[ie - 10: ie + 30]
    flags = {s: _flag(around, E, 0, 1) for s in ("OK", "STALE", "LATE", "NOOPEN")}
    flags["DEL"] = _flag(around, E, 1, 0)
    prices = {"OK": _px(around), "DEL": _px(around),
              "STALE": _px(around[(around < E - pd.Timedelta(days=9)) | (around > E)]),
              "LATE": _px(around[(around < E) | (around > SESSIONS[ie + 2])])}
    no_open = _px(around)
    no_open.loc[SESSIONS[ie + 1], "Open"] = np.nan
    prices["NOOPEN"] = no_open
    m = RF.members(E, flags, prices, SESSIONS)
    assert m["additions"] == ["OK"] and m["deletions"] == ["DEL"] and m["dropped"] == 3
    assert m["E_plus_1"] == SESSIONS[ie + 1]


def test_06_a_name_path_enters_at_the_open_and_is_flat_when_unquoted_or_gone():
    hold = SESSIONS[100:105]
    px = pd.DataFrame({"Open": [100.0, 0, 0, 0, 0], "Close": [110.0, 121.0, np.nan, 133.1, 0]}, index=hold)
    px = px.drop(index=[hold[2], hold[4]])                                   # unquoted on day 3, delisted after day 4
    g = RF.name_growth(px, hold)
    assert np.allclose(g, [1.10, 1.21, 1.21, 1.331, 1.331])


def test_07_event_accounting_charges_entry_and_exit_and_compounds_to_the_event_net():
    E = pd.Timestamp("2010-06-28")
    ie = SESSIONS.get_loc(E)
    hold = SESSIONS[ie + 1: ie + 1 + RF.HOLD]
    mem = {"E": E, "additions": ["A"], "deletions": ["D"]}
    growth = {"D": np.linspace(1.002, 1.04, RF.HOLD), "A": np.linspace(0.999, 0.97, RF.HOLD)}
    b = RF.event_book(mem, {}, SESSIONS, 25.0, growth=growth)
    L, Sh, c = 1.04, 0.97, 25e-4
    assert b["event_net"] == pytest.approx((L - 1) - (Sh - 1) - 2 * c - c * (L + Sh))
    assert np.prod(1.0 + b["net"]) - 1.0 == pytest.approx(b["event_net"])
    assert b["turnover_nav"] == pytest.approx(2.0 + L + Sh) and len(b["sessions"]) == len(hold)


def _synthetic(effect: float, *, conf_effect=None, n_leg: int = 60, n_symbols: int = 5000, seed: int = 3):
    rng = np.random.default_rng(seed)
    flags, prices = {}, {}
    for y in RF.YEARS:
        E = SESSIONS[SESSIONS >= pd.Timestamp(y, 7, 1)][0]
        ie = SESSIONS.get_loc(E)
        span = SESSIONS[ie - 10: ie + RF.HOLD + 5]
        eff = effect if (conf_effect is None or y < RF.CONFIRMATION[0]) else conf_effect
        for side, sign in (("A", -1.0), ("D", 1.0)):
            for i in range(n_leg):
                sym = "%s%d_%d" % (side, y, i)
                flags[sym] = _flag(span, E, 1 if side == "D" else 0, 0 if side == "D" else 1)
                r = rng.normal(0.0, 0.02, len(span))
                r[11:11 + RF.HOLD] += sign * eff / RF.HOLD
                close = 50.0 * np.cumprod(1.0 + r)
                opens = np.r_[close[0], close[:-1]]
                prices[sym] = pd.DataFrame({"Open": opens, "Close": close}, index=span)
    lv = pd.DataFrame(100.0 * np.cumprod(1.0 + rng.normal(0.0003, 0.01, size=(len(SESSIONS), 2)), axis=0),
                      index=SESSIONS, columns=list(RF.CONTROL_SYMBOLS))
    return {"flags": flags, "prices": prices, "r1000": {}, "sessions": SESSIONS, "controls": lv,
            "n_symbols": n_symbols, "n_failed": 0}


def test_08_a_planted_reversal_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    body = RF.run(verbose=False, write=True, inputs=_synthetic(0.04))
    res = body["result"]
    assert res["verdict"] == "QUALIFIED", res["why"]
    assert body["capital_eligible"] is False and res["untouched_confirmation"] == "CONFIRMED"
    assert res["data"]["events"][1991]["additions"] == 60 and len(res["data"]["events"]) == len(RF.YEARS)
    assert set(res["stability_mean_event_net"]) == set(RF.HALVES) and res["multiplicity"]["m"] == 3
    out = RF.executor_result(body)
    assert MX.validate_result(out) == [] and out["verdict"] == "QUALIFIED"


def test_09_a_wrong_sign_is_no_edge_and_no_confirmation_event_is_written():
    res = RF.run(verbose=False, write=False, inputs=_synthetic(-0.04))["result"]
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "WRONG_SIGN"
    assert res["confirmation"]["state"] == "UNREAD" and res["untouched_confirmation"] == "NOT_READ"
    assert all(row[0] <= RF.QUALIFICATION[1] for row in res["qualification_event_rows"])


def test_10_a_planted_effect_that_does_not_reproduce_fails_at_confirmation():
    res = RF.run(verbose=False, write=False, inputs=_synthetic(0.04, conf_effect=-0.04))["result"]
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "CONFIRMATION" and res["untouched_confirmation"] == "FAILED"


def test_11_thin_legs_or_load_failures_are_a_data_hold():
    thin = _synthetic(0.04, n_leg=55)
    for sym in [s for s in thin["flags"] if s.startswith("A2001_")][:10]:
        thin["flags"].pop(sym)
    res = RF.run(verbose=False, write=False, inputs=thin)["result"]
    assert res["verdict"] == "DATA_HOLD" and "THIN_LEGS_IN_[2001]" in res["why"]
    failing = _synthetic(0.04)
    failing["n_failed"] = 80
    res = RF.run(verbose=False, write=False, inputs=failing)["result"]
    assert res["verdict"] == "DATA_HOLD" and "LOAD_FAILURES_80_OF_5000" in res["why"]


def test_12_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    inp = _synthetic(0.0)
    monkeypatch.setattr(RF, "load_inputs", lambda: inp)
    out = RF.run_mechanism(mechanism=_entry())
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False

    def boom():
        raise OSError("norgate is not running")
    monkeypatch.setattr(RF, "load_inputs", boom)
    held = RF.run_mechanism(mechanism=_entry())
    assert held["verdict"] == "DATA_HOLD" and MX.validate_result(held) == []
