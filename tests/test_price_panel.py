"""
tests/test_price_panel.py — Phase 21 owned trailing-price panel (offline, deterministic).

Focuses on the point-in-time guarantees that make the panel safe to backtest with: no look-ahead
(features read only bars <= T; forward returns read only bars > T), feature-window boundaries,
forward-return boundaries, duplicate-date handling, missing / non-positive price handling, and the
correctness of the individual trailing primitives. Pure stdlib, no network, no database.
"""
from __future__ import annotations

import math
from pathlib import Path

import pytest

from paper_trader.api import price_panel as pp

_HEADER = "date,ticker,adjusted_open,adjusted_high,adjusted_low,adjusted_close,volume,benchmark_close,daily_return"


def _weekday_dates(n: int, start_ord: int) -> list[str]:
    from datetime import date, timedelta
    out, d = [], date.fromordinal(start_ord)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _write_price_csv(path: Path, tickers, dates, price_fn, bench_fn) -> None:
    lines = [_HEADER]
    for tk in tickers:
        for i, d in enumerate(dates):
            px = price_fn(tk, i)
            bench = bench_fn(i)
            lines.append("%s,%s,%s,%s,%s,%s,%s,%s,%s" % (
                d, tk, px, px, px, px, 1000000, bench, ""))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture
def simple_panel(tmp_path) -> dict:
    dates = _weekday_dates(400, __import__("datetime").date(2019, 1, 1).toordinal())
    # AAA trends up 0.1%/day, BBB trends down; SPY flat-ish up
    def price_fn(tk, i):
        base = 100.0
        drift = 0.001 if tk == "AAA" else (-0.0008 if tk == "BBB" else 0.0)
        return round(base * math.exp(drift * i) * (1.0 + 0.0), 6)
    def bench_fn(i):
        return round(300.0 * math.exp(0.0003 * i), 6)
    p = tmp_path / "px.csv"
    _write_price_csv(p, ["AAA", "BBB", "SPY"], dates, price_fn, bench_fn)
    panel = pp.load_price_panel(p)
    return {"panel": panel, "dates": dates}


# --------------------------------------------------------------------------- #
# Load + manifest
# --------------------------------------------------------------------------- #
def test_load_shapes_and_manifest(simple_panel):
    panel = simple_panel["panel"]
    assert panel is not None
    assert set(panel["series"].keys()) == {"AAA", "BBB", "SPY"}
    man = panel["manifest"]
    assert man["n_tickers"] == 3
    assert man["date_start"] == simple_panel["dates"][0]
    assert man["date_end"] == simple_panel["dates"][-1]
    assert "survivorship_caveat" in man and "pit_guarantee" in man
    s = panel["series"]["AAA"]
    assert len(s["dates"]) == len(s["adj"]) == len(s["ret"]) == len(s["bret"])
    assert s["ret"][0] is None and s["bret"][0] is None  # first bar has no return


def test_missing_url_returns_none(tmp_path):
    assert pp.load_price_panel(tmp_path / "nope.csv") is None


def test_nonpositive_and_missing_prices_dropped(tmp_path):
    p = tmp_path / "px.csv"
    p.write_text(_HEADER + "\n"
                 + "2020-01-02,ZZZ,1,1,1,10,100,300,\n"
                 + "2020-01-03,ZZZ,1,1,1,,100,300,\n"      # missing close -> dropped
                 + "2020-01-06,ZZZ,1,1,1,-5,100,300,\n"    # non-positive -> dropped
                 + "2020-01-07,ZZZ,1,1,1,11,100,300,\n", encoding="utf-8")
    panel = pp.load_price_panel(p)
    s = panel["series"]["ZZZ"]
    assert s["dates"] == ["2020-01-02", "2020-01-07"]
    assert s["adj"] == [10.0, 11.0]


def test_duplicate_dates_collapse_to_last(tmp_path):
    p = tmp_path / "px.csv"
    p.write_text(_HEADER + "\n"
                 + "2020-01-02,DUP,1,1,1,10,100,300,\n"
                 + "2020-01-02,DUP,1,1,1,12,100,300,\n"    # same date -> keep last (12)
                 + "2020-01-03,DUP,1,1,1,13,100,300,\n", encoding="utf-8")
    s = pp.load_price_panel(p)["series"]["DUP"]
    assert s["dates"] == ["2020-01-02", "2020-01-03"]
    assert s["adj"] == [12.0, 13.0]


# --------------------------------------------------------------------------- #
# PIT as-of index + no look-ahead
# --------------------------------------------------------------------------- #
def test_asof_index_pit():
    dates = ["2020-01-02", "2020-01-06", "2020-01-07", "2020-01-08"]
    assert pp.asof_index(dates, "2020-01-01") == -1          # before first
    assert pp.asof_index(dates, "2020-01-06") == 1           # exact
    assert pp.asof_index(dates, "2020-01-06T23") == 1        # between -> latest <= as_of
    assert pp.asof_index(dates, "2020-12-31") == 3           # after last


def test_forward_return_boundary_is_strictly_future(simple_panel):
    s = simple_panel["panel"]["series"]["AAA"]
    n = len(s["dates"])
    # last bar: every forward horizon is off the end -> None (the forward-return boundary)
    assert pp.forward_returns(s, n - 1) == {5: None, 10: None, 21: None, 63: None}
    # a mid bar: forward uses strictly future bars adj[j+h]/adj[j]-1
    j = 100
    fr = pp.forward_returns(s, j, [5])
    assert fr[5] == pytest.approx(s["adj"][j + 5] / s["adj"][j] - 1.0)


def test_no_lookahead_features_ignore_future_bars(tmp_path):
    """Appending future bars must NOT change a feature computed at an earlier index."""
    dates = _weekday_dates(320, __import__("datetime").date(2019, 1, 1).toordinal())
    def price_fn(tk, i):
        return round(100.0 * math.exp(0.0007 * i), 6)
    def bench_fn(i):
        return round(300.0 * math.exp(0.0002 * i), 6)
    full = tmp_path / "full.csv"
    _write_price_csv(full, ["AAA", "SPY"], dates, price_fn, bench_fn)
    trunc = tmp_path / "trunc.csv"
    _write_price_csv(trunc, ["AAA", "SPY"], dates[:300], price_fn, bench_fn)
    j = 280  # present in both; 19 future bars exist only in `full`
    f_full = pp.compute_features(pp.load_price_panel(full)["series"]["AAA"], j)
    f_trunc = pp.compute_features(pp.load_price_panel(trunc)["series"]["AAA"], j)
    for k in pp.FEATURE_KEYS:
        assert f_full.get(k) == f_trunc.get(k), "feature %s leaked future data" % k


# --------------------------------------------------------------------------- #
# Trailing primitives
# --------------------------------------------------------------------------- #
def test_trailing_return_and_window_boundary():
    adj = [10.0 * (1.01 ** i) for i in range(300)]
    assert pp._tr(adj, 100, 20) == pytest.approx(adj[100] / adj[80] - 1.0)
    assert pp._tr(adj, 10, 63) is None  # not enough trailing history (window boundary)


def test_moving_average_and_maxdd():
    adj = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10.0]
    assert pp._ma(adj, 9, 5) == pytest.approx((6 + 7 + 8 + 9 + 10) / 5.0)
    # monotone up -> zero drawdown; a dip -> negative drawdown
    assert pp._maxdd(adj, 9, 9) == pytest.approx(0.0)
    dip = [10, 9, 8, 12.0]
    assert pp._maxdd(dip, 3, 3) == pytest.approx(8.0 / 10.0 - 1.0)


def test_realized_vol_positive_and_beta_sane():
    import random
    rnd = random.Random(7)
    adj = [100.0]
    bench = [300.0]
    for _ in range(200):
        mr = rnd.gauss(0, 0.01)
        adj.append(adj[-1] * (1 + 1.2 * mr + rnd.gauss(0, 0.003)))  # beta ~1.2 vs market
        bench.append(bench[-1] * (1 + mr))
    ret = [None] + [adj[i] / adj[i - 1] - 1 for i in range(1, len(adj))]
    bret = [None] + [bench[i] / bench[i - 1] - 1 for i in range(1, len(bench))]
    rv = pp._rvol(ret, len(adj) - 1, 126)
    assert rv is not None and rv > 0
    beta = pp._beta(ret, bret, len(adj) - 1, 126)
    assert 0.9 < beta < 1.5  # recovers the ~1.2 construction


def test_feature_bundle_keys_complete(simple_panel):
    s = simple_panel["panel"]["series"]["AAA"]
    f = pp.compute_features(s, 300)
    for k in pp.FEATURE_KEYS:
        assert k in f
