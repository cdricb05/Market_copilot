"""Targeted tests for the campaign R56_V2 shared evaluator.

The evaluator adds three capabilities the canonical owners lack (roll cost on
the dated-contract book, a universe eligibility mask on the top-N book, an H5
next-open tranche book). Everything else must be the OWNERS' accounting, so the
first duty of these tests is PARITY with ``alpha_agent.r59.native.run_book``
and ``alpha_agent.r57.engine.run_topn``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pytest

from alpha_agent import r59
from alpha_agent.r57 import engine as K
from alpha_agent.r57 import families
from alpha_agent.r59 import engines as E
from alpha_agent.r59 import native

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "campaign_r56_v2_evaluator",
    ROOT / "research" / "agents" / "campaign_r56_v2" / "evaluator.py")
EV = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(EV)

# The OWNER's eligibility takes a nanmedian over a delisted name's all-NaN
# window; that is its documented behaviour on every real panel.
pytestmark = pytest.mark.filterwarnings(
    "ignore:All-NaN slice encountered:RuntimeWarning")


def _dates(start="2011-01-03", end="2026-07-01"):
    d = np.arange(start, end, dtype="datetime64[D]")
    return d[np.is_busday(d)].astype(str)


def _futures_layer(n_m=16, seed=7):
    rng = np.random.default_rng(seed)
    dates = _dates()
    n_d = len(dates)
    ret = rng.normal(0.0002, 0.01, (n_m, n_d))
    ret[3, 400:460] = np.nan
    roll = np.zeros((n_m, n_d), dtype=np.uint8)
    for i in range(n_m):
        roll[i, (17 + 3 * i)::63] = 1
    nanf = np.full((n_m, n_d), np.nan)
    return {"symbols": ["M%02d" % i for i in range(n_m)], "dates": dates,
            "ret": ret, "ret2": nanf, "slope": nanf, "open_interest": nanf,
            "volume": nanf, "roll": roll,
            "cost_per_side": np.full(n_m, 0.0005)}


def _score(layer):
    ret = layer["ret"]

    def f(t):
        return np.nansum(ret[:, t - 60:t], axis=1)
    return f


@pytest.fixture()
def owner_layer(monkeypatch):
    layer = _futures_layer()
    meta = {s: {"asset_class": "COMMODITY", "economic_group": "G",
                "cost_bps_per_side": 5.0} for s in layer["symbols"]}
    monkeypatch.setattr(native, "_CACHE", {"layer": layer, "meta": meta})
    return layer


# --------------------------------------------------------------------------- #
# Futures book
# --------------------------------------------------------------------------- #
def test_futures_book_matches_owner_run_book_when_rolls_are_not_charged(owner_layer):
    layer = owner_layer
    score = _score(layer)
    mask = np.ones(len(layer["symbols"]), dtype=bool)
    own = native.run_book(score_fn=score, mask=mask, horizon=21, cadence=21,
                          label="x", long_short=True)
    got = EV.run_futures_book(
        layer, lambda t, live: native._xs_weights(score(t), live & mask),
        horizon=21, cadence=21, label="x", charge_rolls=False)
    for name in ("D", "V", "L"):
        for key in ("periods", "effective_observations", "ann_net_excess",
                    "ann_gross_excess", "ann_cost_drag", "t_net_excess",
                    "p_one_sided", "mean_oneway_turnover_per_period", "max_dd"):
            assert got["layers"][name][key] == pytest.approx(
                own["layers"][name][key], rel=1e-12, abs=1e-15), (name, key)
    assert got["layers"]["L"]["periods"] >= 36


def test_roll_cost_only_lowers_the_net_and_never_touches_the_gross(owner_layer):
    layer = owner_layer
    score = _score(layer)
    fn = lambda t, live: EV.rank_weights(score(t), live, min_markets=12)
    a = EV.run_futures_book(layer, fn, horizon=21, cadence=21, label="x",
                            charge_rolls=False)
    b = EV.run_futures_book(layer, fn, horizon=21, cadence=21, label="x")
    assert np.allclose(a["series"]["gross"], b["series"]["gross"])
    assert (b["series"]["net"] <= a["series"]["net"] + 1e-15).all()
    assert b["layers"]["L"]["ann_roll_cost_drag"] > 0
    assert a["layers"]["L"]["ann_roll_cost_drag"] == 0


def test_roll_cost_arithmetic_interior_and_at_entry():
    dates = _dates("2011-06-01", "2011-12-01")
    n_d = len(dates)
    ret = np.zeros((2, n_d))
    roll = np.zeros((2, n_d), dtype=np.uint8)
    t0, t1 = 30, 51
    roll[0, t0 + 5] = 1          # interior of window 0
    roll[1, t1 + 1] = 1          # at the entry of window 1
    roll[0, t1 + 9] = 1          # interior of window 1
    layer = {"dates": dates, "ret": ret, "roll": roll,
             "cost_per_side": np.array([0.001, 0.002])}
    w = np.array([0.5, -0.5])
    out = EV.run_futures_book(layer, lambda t, live: w, horizon=21, cadence=21,
                              label="x", decision_idx=[t0, t1])
    s = out["series"]
    assert s["rebalance_cost"][0] == pytest.approx(0.5 * 0.001 + 0.5 * 0.002)
    assert s["roll_cost"][0] == pytest.approx(2 * 0.001 * 0.5)
    assert s["rebalance_cost"][1] == pytest.approx(0.0)
    # at entry: same sign, 2 x min(0.5, 0.5) x 0.002; interior: 2 x 0.001 x 0.5
    assert s["roll_cost"][1] == pytest.approx(2 * 0.5 * 0.002 + 2 * 0.001 * 0.5)
    doubled = EV.run_futures_book(layer, lambda t, live: w, horizon=21,
                                  cadence=21, label="x", decision_idx=[t0, t1],
                                  cost_mult=2.0)
    assert doubled["series"]["net"] == pytest.approx(2.0 * s["net"])


def test_at_entry_roll_is_free_when_the_position_flips_sign():
    dates = _dates("2011-06-01", "2011-12-01")
    n_d = len(dates)
    roll = np.zeros((1, n_d), dtype=np.uint8)
    roll[0, 52] = 1
    layer = {"dates": dates, "ret": np.zeros((1, n_d)), "roll": roll,
             "cost_per_side": np.array([0.001])}
    ws = {30: np.array([1.0]), 51: np.array([-1.0])}
    out = EV.run_futures_book(layer, lambda t, live: ws[t], horizon=21,
                              cadence=21, label="x", decision_idx=[30, 51])
    assert out["series"]["roll_cost"][1] == pytest.approx(0.0)
    assert out["series"]["rebalance_cost"][1] == pytest.approx(2.0 * 0.001)


def test_futures_daily_series_reproduces_every_period_return(owner_layer):
    layer = owner_layer
    score = _score(layer)
    out = EV.run_futures_book(
        layer, lambda t, live: EV.rank_weights(score(t), live), horizon=21,
        cadence=21, label="x")
    d, s = out["daily"], out["series"]
    for j, t in enumerate(s["decision_idx"]):
        sl = slice(int(t) + 1, int(t) + 22)
        assert d["gross"][sl].sum() == pytest.approx(s["gross"][j], abs=1e-12)
        assert d["net"][sl].sum() == pytest.approx(s["net"][j], abs=1e-12)


def test_closed_market_rules():
    dates = _dates("2011-06-01", "2011-12-01")
    n_d = len(dates)
    ret = np.zeros((2, n_d))
    t = 40
    ret[0, t] = np.nan           # market 0 has no session on the decision date
    ret[0, t + 1] = 0.10         # its first own session afterwards
    ret[0, t + 2] = 0.05
    layer = {"dates": dates, "ret": ret, "roll": np.zeros((2, n_d), np.uint8),
             "cost_per_side": np.zeros(2)}
    w = np.array([1.0, 0.0])
    drop = EV.run_futures_book(layer, lambda t, live: w, horizon=21, cadence=21,
                               label="x", decision_idx=[t])
    assert drop["series"]["gross"][0] == pytest.approx(0.0)
    assert drop["series"]["n_positions"][0] == 0
    nxt = EV.run_futures_book(layer, lambda t, live: w, horizon=21, cadence=21,
                              label="x", decision_idx=[t],
                              closed_rule="ENTER_NEXT_OWN_SETTLE")
    # entered at the settlement of t+1: that session's return is NOT earned
    assert nxt["series"]["gross"][0] == pytest.approx(0.05)


def test_rank_weights_are_flat_below_the_market_floor():
    score = np.arange(11, dtype=float)
    assert not EV.rank_weights(score, np.ones(11, bool), min_markets=12).any()
    w = EV.rank_weights(np.arange(12, dtype=float), np.ones(12, bool))
    assert w.sum() == pytest.approx(0.0, abs=1e-15)
    assert np.abs(w).sum() == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# Equity top-N
# --------------------------------------------------------------------------- #
def _equity_panel(n=80, seed=11, dates=None):
    rng = np.random.default_rng(seed)
    dates = _dates() if dates is None else dates
    n_d = len(dates)
    tr = 50.0 * np.cumprod(1.0 + rng.normal(0.0004, 0.02, (n, n_d)), axis=1)
    op = tr * (1.0 + rng.normal(0.0, 0.004, (n, n_d)))
    mem = np.ones((n, n_d), dtype=np.uint8)
    for i in range(0, n, 9):                      # delisted names
        cut = 1500 + 37 * i
        if cut < n_d:
            tr[i, cut:] = np.nan
            op[i, cut:] = np.nan
            mem[i, cut:] = 0
    mem[5, 900:1300] = 0
    spy = 100.0 * np.cumprod(1.0 + rng.normal(0.0003, 0.01, n_d))
    return {"dates": dates, "tr": tr, "un": tr.copy(),
            "vol": np.full((n, n_d), 1.0e6), "mem": mem, "spy_tr": spy,
            "op_tr": op}


def test_equity_topn_matches_owner_run_topn_under_the_owner_eligibility():
    panel = _equity_panel()
    score = families.mom(126, 21)
    # the r59 engine always passes its own discovery start to the r57 kernel
    own = K.run_topn(panel, score, 21, 21, top_n=10, cost_rate=0.0025,
                     first_date=r59.DISCOVERY_START)
    got = EV.run_equity_topn(panel, score, K.eligibility, label="x", top_n=10)
    assert np.array_equal(own["idx"], got["res"]["idx"])
    for key in ("strat_gross", "strat_net", "bench_gross", "bench_net",
                "turnover_oneway", "n_held"):
        assert np.array_equal(own[key], got["res"][key]), key
    assert got["layers"]["L"] == {**K.layer_stats(own, "L"),
                                  **{k: got["layers"]["L"][k] for k in
                                     ("median_universe", "min_universe",
                                      "median_held")}}


def test_equity_topn_mask_restricts_selection_and_benchmark():
    panel = _equity_panel()
    score = families.mom(126, 21)
    mask = np.zeros(panel["tr"].shape, dtype=bool)
    mask[:30, :] = True
    got = EV.run_equity_topn(panel, score, mask, label="x", top_n=10)
    for _t, held, _w, univ in got["holdings"]:
        assert (held < 30).all() and (univ < 30).all()


def test_equity_daily_series_reproduces_every_period_return():
    panel = _equity_panel()
    got = EV.run_equity_topn(panel, families.mom(126, 21), K.eligibility,
                             label="x", top_n=10)
    d, res = EV.equity_topn_daily(panel, got), got["res"]
    for j, (t, held, _w, _u) in enumerate(got["holdings"]):
        if not len(held):
            continue
        sl = slice(t + 2, t + 23)
        assert d["strat_gross"][sl].sum() == pytest.approx(res["strat_gross"][j], abs=1e-10)
        assert d["bench_gross"][sl].sum() == pytest.approx(res["bench_gross"][j], abs=1e-10)
        assert d["strat_net"][sl].sum() == pytest.approx(res["strat_net"][j], abs=1e-10)


# --------------------------------------------------------------------------- #
# H5 next-open tranche book
# --------------------------------------------------------------------------- #
def _tiny_panel():
    dates = _dates("2011-07-01", "2011-07-14")[:8]
    flat = np.full(8, 100.0)
    a = np.array([100.0, 100.0, 110.0, 121.0, 121.0, 121.0, 121.0, 121.0])
    px = np.vstack([a, flat, flat])
    return {"dates": dates, "tr": px.copy(), "op_tr": px.copy()}


def _prefer_first(panel, t):
    return np.array([3.0, 2.0, 1.0])


def test_tranche_book_hand_example_and_netting():
    panel = _tiny_panel()
    elig = np.ones((3, 8), dtype=bool)
    kw = dict(label="x", hold=2, top_n=1, cost_rate=0.01,
              first_date=str(panel["dates"][1]))
    net = EV.run_equity_daily_tranche_book(panel, _prefer_first, elig, **kw)
    d = net["daily"]
    # the tranche formed at close 1 buys A at the OPEN of session 2 (110) with
    # half the capital and earns open 2 -> open 3
    assert d["strat_gross"][2] == pytest.approx(0.5 * (121.0 - 110.0) / 110.0)
    assert d["strat_traded"][2] == pytest.approx(0.5)
    # session 4: the expiring tranche (worth 0.55) is netted against the new 0.5
    assert d["strat_traded"][4] == pytest.approx(0.05)
    full = EV.run_equity_daily_tranche_book(panel, _prefer_first, elig,
                                            net_overlap=False, **kw)
    assert full["daily"]["strat_traded"][4] == pytest.approx(1.05)
    assert np.allclose(full["daily"]["strat_gross"], d["strat_gross"])
    # a close-t fill would have earned the 100 -> 110 move; the book must not
    assert d["strat_gross"][1] == 0.0


def test_tranche_book_leaves_an_unfillable_open_in_cash():
    panel = _tiny_panel()
    panel["op_tr"][0, 2] = np.nan
    elig = np.ones((3, 8), dtype=bool)
    out = EV.run_equity_daily_tranche_book(
        panel, _prefer_first, elig, label="x", hold=2, top_n=1, cost_rate=0.01,
        first_date=str(panel["dates"][1]))
    d = out["daily"]
    assert d["unfilled_share"][2] == pytest.approx(1.0)
    assert d["strat_traded"][2] == pytest.approx(0.0)
    assert d["strat_gross"][2] == pytest.approx(0.0)


def test_tranche_exit_without_an_open_print_is_marked_at_the_last_close():
    panel = _tiny_panel()
    panel["op_tr"][0, 4] = np.nan        # no open print on the exit session
    panel["tr"][0, 3] = 132.0            # the last total-return close before it
    elig = np.ones((3, 8), dtype=bool)
    out = EV.run_equity_daily_tranche_book(
        panel, _prefer_first, elig, label="x", hold=2, top_n=1, cost_rate=0.01,
        first_date=str(panel["dates"][1]))
    d = out["daily"]
    # tranche 1 (entered at 110) is carried open 3 -> "open" 4 at the 132 mark
    assert d["strat_gross"][3] == pytest.approx(
        0.5 * (132.0 - 121.0) / 110.0 + 0.5 * (132.0 - 121.0) / 121.0)
    # session 4: the new tranche cannot fill A, the old one is sold at the mark
    assert d["unfilled_share"][4] == pytest.approx(1.0)
    assert d["strat_traded"][4] == pytest.approx(0.5 * 132.0 / 110.0)


def _neg_ret5(panel, t):
    tr = panel["tr"]
    with np.errstate(invalid="ignore", divide="ignore"):
        return -(tr[:, t] / tr[:, t - 5] - 1.0)


def test_tranche_selection_never_reads_the_future():
    dates = _dates("2011-06-01", "2012-09-01")
    panel = _equity_panel(n=40, dates=dates)
    elig = panel["mem"].astype(bool)
    kw = dict(label="x", hold=5, top_n=8, first_date="2011-07-01")
    a = EV.run_equity_daily_tranche_book(panel, _neg_ret5, elig, **kw)
    cut = 200
    other = {k: (v.copy() if isinstance(v, np.ndarray) else v)
             for k, v in panel.items()}
    other["tr"][:, cut + 1:] *= 1.5
    other["op_tr"][:, cut + 1:] *= 0.5
    b = EV.run_equity_daily_tranche_book(other, _neg_ret5, elig, **kw)
    for t, names in a["selected"].items():
        if t <= cut:
            assert np.array_equal(names, b["selected"][t]), t
    assert np.allclose(a["daily"]["strat_gross"][:cut - 1],
                       b["daily"]["strat_gross"][:cut - 1])


def test_tranche_layers_feed_the_canonical_gate_as_a_daily_book():
    panel = _equity_panel(n=40)
    elig = panel["mem"].astype(bool)
    out = EV.run_equity_daily_tranche_book(panel, _neg_ret5, elig, label="x",
                                           hold=5, top_n=8)
    L = out["layers"]["L"]
    assert L["days"] > 500 and "effective_observations" not in L
    verdict = E.gate(out, prior_burden=0, family_tests=20)
    assert verdict["lockbox_observations"] == L["days"]
    assert verdict["lockbox_materiality"] == L["ann_net_excess"]
    assert verdict["burden_denominator"] == 20
    # the benchmark trades almost nothing once tranches are netted
    assert L["bench_mean_oneway_turnover_per_day"] < 0.25 * L["mean_oneway_turnover_per_day"]


def test_unnetted_cost_cancels_against_the_benchmark_which_is_why_it_is_forbidden():
    """Director ruling R6: charging BOTH books a full tranche buy and a full
    tranche sell every session makes the cost vanish from the net excess. The
    netted convention is the frozen one; this test only demonstrates why."""
    panel = _equity_panel(n=40)
    elig = panel["mem"].astype(bool)
    kw = dict(label="x", hold=5, top_n=8)
    netted = EV.run_equity_daily_tranche_book(panel, _neg_ret5, elig, **kw)
    full = EV.run_equity_daily_tranche_book(panel, _neg_ret5, elig,
                                            net_overlap=False, **kw)
    assert netted["layers"]["L"]["ann_cost_drag"] > 0.10
    assert abs(full["layers"]["L"]["ann_cost_drag"]) < 0.02
    assert full["layers"]["L"]["ann_gross_excess"] == pytest.approx(
        netted["layers"]["L"]["ann_gross_excess"])


# --------------------------------------------------------------------------- #
# Gate trap
# --------------------------------------------------------------------------- #
def test_no_layer_may_carry_net_sharpe_beside_ann_net_excess(owner_layer):
    with pytest.raises(AssertionError):
        EV.assert_gate_safe({"L": {"net_sharpe": 1.0, "ann_net_excess": 0.02}})
    score = _score(owner_layer)
    fut = EV.run_futures_book(owner_layer,
                              lambda t, live: EV.rank_weights(score(t), live),
                              horizon=21, cadence=21, label="x")
    eq = EV.run_equity_topn(_equity_panel(), families.mom(126, 21),
                            K.eligibility, label="x", top_n=10)
    for out in (fut, eq):
        for row in out["layers"].values():
            assert "net_sharpe" not in row
