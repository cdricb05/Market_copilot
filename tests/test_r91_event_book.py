"""R91 - the canonical EVENT-TIME book (alpha_agent.agents_v2.event_book).

Proves: no same-day lookahead after the entry cutoff; availability (not the
event instant) decides entry; overlapping events reduce effective N; duplicate
events do not multiply the sample; costs are charged as the frozen cost model
says; D/V/L stay separated with a boundary purge; the lockbox is untouched
until legitimately reached; a pre-registered window cannot change after D is
revealed; the book has no prospective argument; the placebo re-dates within
the layer; the gate reads the layers it produces.
"""
from __future__ import annotations

import inspect

import numpy as np
import pandas as pd
import pytest

from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import books as B
from paper_trader.alpha_agent.agents_v2 import event_book as EVB
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.r59 import engines as E


def _panel(seed: int = 0, nan_after: str = None, zero: bool = False) -> dict:
    dates = pd.bdate_range("2010-01-04", "2024-12-31").values.astype("datetime64[D]")
    rng = np.random.default_rng(seed)
    ret = rng.normal(0.0, 0.01, (3, len(dates)))
    if zero:
        ret[:] = 0.0
    if nan_after is not None:
        ret[:, dates >= np.datetime64(nan_after, "D")] = np.nan
    return {"dates": dates, "symbols": ["AA", "BB", "CC"], "ret": ret,
            "roll": np.zeros(ret.shape, dtype=np.uint8),
            "cost_per_side": np.array([0.0003, 0.0005, 0.0010])}


def _idx(panel, day: str) -> int:
    return int(np.searchsorted(panel["dates"], np.datetime64(day, "D")))


def _ev(eid, avail, *, w=None, cluster=None, ts=None) -> dict:
    return {"event_id": eid, "cluster_id": cluster or eid,
            "event_timestamp": ts or avail, "available_at": avail,
            "weights": w or {"AA": 1.0}}


def _many(prefix, start, end, n, seed=1) -> list:
    days = pd.bdate_range(start, end)
    rng = np.random.default_rng(seed)
    pick = sorted(rng.choice(len(days), size=n, replace=False))
    return [_ev("%s%03d" % (prefix, i), "%sT13:00:00Z" % days[j].date().isoformat())
            for i, j in enumerate(pick)]


RULE = {"entry_rule": EVB.ENTRY_NEXT_CLOSE, "benchmark": EVB.BENCH_UNCONDITIONAL}


# --------------------------------------------------------------------------- #
def test_a_no_same_day_lookahead_when_published_after_the_cutoff():
    p = _panel()
    d = p["dates"]
    # 2015-03-10 is a Tuesday; ET = UTC-4 in March 2015.
    t_after, why = EVB.entry_index(d, _ev("x", "2015-03-10T19:30:00Z"), hold=1)   # 15:30 ET
    t_before, _ = EVB.entry_index(d, _ev("y", "2015-03-10T13:00:00Z"), hold=1)    # 09:00 ET
    t_at, _ = EVB.entry_index(d, _ev("z", "2015-03-10T19:00:00Z"), hold=1)        # 15:00 ET
    assert why == "OK"
    assert str(d[t_before]) == "2015-03-10" and str(d[t_at]) == "2015-03-10"
    assert str(d[t_after]) == "2015-03-11", "after the cutoff -> the NEXT close"
    # A weekend publication enters at Monday's close.
    t_wk, _ = EVB.entry_index(d, _ev("w", "2015-03-14T12:00:00Z"), hold=1)
    assert str(d[t_wk]) == "2015-03-16"


def test_a2_the_cutoff_is_the_earliest_settlement_among_the_legs():
    """R91 skeptic D2: copper settles ~13:00 ET, Nikkei in Asia. An event
    published at 14:00 ET must NOT be entered at a price fixed at 13:00."""
    p = _panel()
    d = p["dates"]
    ev = _ev("hg", "2015-03-10T18:00:00Z", w={"AA": 1.0})            # 14:00 ET
    t_default, _ = EVB.entry_index(d, ev, hold=1)
    t_leg, _ = EVB.entry_index(d, ev, hold=1, leg_cutoff_et={"AA": "13:00"})
    assert str(d[t_default]) == "2015-03-10" and str(d[t_leg]) == "2015-03-11"
    assert EVB.effective_cutoff(ev, leg_cutoff_et={"AA": "13:00", "BB": "01:15"}) == "13:00"
    two = _ev("x", "2015-03-10T18:00:00Z", w={"AA": 0.5, "BB": -0.5})
    assert EVB.effective_cutoff(two, leg_cutoff_et={"AA": "13:00", "BB": "01:15"}) == "01:15"
    assert EVB.effective_cutoff(two, leg_cutoff_et={"AA": "16:30"}) == EVB.DEFAULT_CUTOFF_ET
    rule = {**RULE, "leg_cutoff_et": {"AA": "13:00"}}
    res = EVB.run_event_window_book(p, [ev], stage="D", hold=1, event_rule=rule)
    assert res["series"]["observations"][0]["entry_date"] == "2015-03-11"
    assert res["leg_cutoff_et"] == {"AA": "13:00"}
    spec = {"parameters": {"event_rule": {"leg_cutoff_et": {"AA": "13:00"}}}}
    assert EVB.check_frozen_parameters(spec, {"book_kwargs": {"event_rule": {}}})
    # the same mapping in another key order is the SAME frozen rule
    spec2 = {"parameters": {"event_rule": {"leg_cutoff_et": {"6J": "15:00", "HG": "13:00"}}}}
    plan2 = {"book_kwargs": {"event_rule": {"leg_cutoff_et": {"HG": "13:00", "6J": "15:00"}}}}
    assert EVB.check_frozen_parameters(spec2, plan2) == []


def test_b_the_availability_instant_decides_entry_not_the_event_instant():
    p = _panel()
    ev = _ev("x", "2015-03-12T12:00:00Z", ts="2015-03-09T08:00:00Z")
    t, _ = EVB.entry_index(p["dates"], ev, hold=1)
    assert str(p["dates"][t]) == "2015-03-12"
    res = EVB.run_event_window_book(p, [ev], stage="D", hold=1, event_rule=RULE)
    assert res["series"]["observations"][0]["entry_date"] == "2015-03-12"


def test_c_overlapping_events_reduce_effective_n():
    p = _panel()
    evs = [_ev("a", "2015-03-10T13:00:00Z"), _ev("b", "2015-03-11T13:00:00Z"),
           _ev("c", "2015-03-12T13:00:00Z")]
    res = EVB.run_event_window_book(p, evs, stage="D", hold=5, event_rule=RULE)
    D = res["layers"]["D"]
    assert D["raw_events"] == 3 and D["episodes"] == 3
    assert D["effective_observations"] == 1 and D["periods"] == 1
    assert D["merged_overlapping_episodes"] == 2
    # One cluster id -> one episode even when the windows would not overlap.
    evs2 = [_ev("a", "2015-03-10T13:00:00Z", cluster="storm"),
            _ev("b", "2015-05-10T13:00:00Z", cluster="storm")]
    res2 = EVB.run_event_window_book(p, evs2, stage="D", hold=1, event_rule=RULE)
    assert res2["layers"]["D"]["episodes"] == 1
    assert res2["layers"]["D"]["effective_observations"] == 1


def test_d_duplicate_events_do_not_multiply_the_sample():
    p = _panel()
    ev = _ev("dup", "2015-03-10T13:00:00Z")
    res = EVB.run_event_window_book(p, [ev, dict(ev), dict(ev)], stage="D", hold=1,
                                    event_rule=RULE)
    assert res["n_events_in"] == 3 and res["n_events_resolved"] == 1
    assert res["dropped"]["DUPLICATE_EVENT_ID"] == 2
    assert res["layers"]["D"]["effective_observations"] == 1


def test_e_costs_are_charged_as_the_frozen_cost_model_says():
    p = _panel(zero=True)
    ev = _ev("c", "2015-03-10T13:00:00Z", w={"AA": 1.0})
    res = EVB.run_event_window_book(p, [ev], stage="D", hold=1, event_rule=RULE)
    o = res["series"]["observations"][0]
    assert o["gross"] == 0.0 and o["bench"] == 0.0
    assert o["net"] == pytest.approx(-2 * 0.0003)             # open + close
    assert res["cost_model"] == B.FUT_COST_MODEL
    # An interior roll inside the window costs 2 x cost x |w| (the owner's rule).
    t = _idx(p, "2015-03-10")
    p["roll"][0, t + 3] = 1
    res5 = EVB.run_event_window_book(p, [ev], stage="D", hold=5, event_rule=RULE)
    o5 = res5["series"]["observations"][0]
    assert o5["roll_cost"] == pytest.approx(2 * 0.0003)
    assert o5["net"] == pytest.approx(-(2 * 0.0003 + 2 * 0.0003))
    # Doubled cost doubles the drag; a two-leg book pays each leg's own cost.
    res2 = EVB.run_event_window_book(p, [ev], stage="D", hold=5, event_rule=RULE,
                                     cost_mult=2.0)
    assert res2["series"]["observations"][0]["net"] == pytest.approx(2 * o5["net"])
    ev2 = _ev("two", "2015-03-10T13:00:00Z", w={"AA": 0.5, "CC": -0.5})
    r2 = EVB.run_event_window_book(_panel(zero=True), [ev2], stage="D", hold=1,
                                   event_rule=RULE)
    assert r2["series"]["observations"][0]["net"] == pytest.approx(
        -2 * (0.5 * 0.0003 + 0.5 * 0.0010))
    assert r2["layers"]["D"]["ann_cost_drag"] == pytest.approx(
        r2["layers"]["D"]["ann_gross_excess"] - r2["layers"]["D"]["ann_net_excess"])


def test_f_d_v_l_are_separated_and_a_boundary_crossing_window_is_purged():
    p = _panel()
    evs = [_ev("d", "2015-03-10T13:00:00Z"), _ev("v", "2020-03-10T13:00:00Z"),
           _ev("l", "2024-03-12T13:00:00Z"),
           _ev("purge", "2017-12-27T13:00:00Z")]       # window ends 2018-01-03
    rd = EVB.run_event_window_book(p, evs, stage="D", hold=5, event_rule=RULE)
    assert list(rd["layers"]) == ["D"] and rd["layers"]["D"]["effective_observations"] == 1
    assert rd["dropped"]["BEYOND_STAGE_D"] == 2
    assert rd["dropped"]["BOUNDARY_PURGE_OR_PRE_DISCOVERY"] == 1
    rv = EVB.run_event_window_book(p, evs, stage="V", hold=5, event_rule=RULE)
    assert list(rv["layers"]) == ["D", "V"] and rv["dropped"]["BEYOND_STAGE_V"] == 1
    rl = EVB.run_event_window_book(p, evs, stage="L", hold=5, event_rule=RULE)
    assert list(rl["layers"]) == ["D", "V", "L"]
    assert all(rl["layers"][k]["effective_observations"] == 1 for k in "DVL")
    assert EVB.layer_of_entry(p["dates"], _idx(p, "2010-06-01"), 1) is None  # pre-discovery


def test_g_the_lockbox_is_untouched_until_legitimately_reached():
    full = _panel(seed=3)
    blind = _panel(seed=3, nan_after=r59.VALIDATION_START)   # nothing after D exists
    evs = _many("d", "2012-01-01", "2017-11-30", 25) + _many("l", "2023-02-01", "2024-11-30", 10)
    a = EVB.run_event_window_book(full, evs, stage="D", hold=2, event_rule=RULE)
    b = EVB.run_event_window_book(blind, evs, stage="D", hold=2, event_rule=RULE)
    for k in ("ann_net_excess", "ann_gross_excess", "t_net_excess", "effective_observations"):
        assert a["layers"]["D"][k] == pytest.approx(b["layers"]["D"][k])
    assert np.isfinite(a["layers"]["D"]["t_net_excess"])
    assert "L" not in a["layers"] and a["dropped"]["BEYOND_STAGE_D"] == 10
    assert "later" in a["lockbox_protection"] or "after D" in a["lockbox_protection"]


def test_h_a_preregistered_window_cannot_change_after_d_is_revealed():
    spec = {"parameters": {"holding_window_sessions": 2,
                           "event_rule": {"entry_rule": EVB.ENTRY_NEXT_CLOSE,
                                          "benchmark": EVB.BENCH_UNCONDITIONAL}}}
    ok = {"book_kwargs": {"hold": 2, "event_rule": dict(spec["parameters"]["event_rule"])}}
    assert EVB.check_frozen_parameters(spec, ok) == []
    bad_hold = {"book_kwargs": {"hold": 5, "event_rule": dict(spec["parameters"]["event_rule"])}}
    assert any("holding window" in s for s in EVB.check_frozen_parameters(spec, bad_hold))
    bad_rule = {"book_kwargs": {"hold": 2, "event_rule": {"entry_rule": EVB.ENTRY_BEFORE_EVENT,
                                                          "benchmark": EVB.BENCH_UNCONDITIONAL}}}
    assert any("entry_rule" in s for s in EVB.check_frozen_parameters(spec, bad_rule))
    # Only the pre-registrable windows exist at all.
    assert EVB.ALLOWED_HOLDS == (1, 2, 5)
    with pytest.raises(EVB.EventBookRefusal, match="pre-registrable"):
        EVB.run_event_window_book(_panel(), [_ev("x", "2015-03-10T13:00:00Z")],
                                  stage="D", hold=3, event_rule=RULE)
    src = inspect.getsource(B.run_stage)
    assert "BOOK_EVENT" in src and "run_event_window_book" in src


def test_i_historical_and_prospective_paths_stay_separate():
    sig = inspect.signature(EVB.run_event_window_book)
    for k in P.BACKFILL_KEYS:
        assert k not in sig.parameters
    with pytest.raises(EVB.EventBookRefusal, match="not an argument"):
        EVB.run_event_window_book(_panel(), [_ev("x", "2015-03-10T13:00:00Z")],
                                  stage="D", hold=1, event_rule={**RULE, "as_of": "2030-01-01"})
    p = _panel()
    res = EVB.run_event_window_book(
        p, [_ev("x", "2015-03-10T13:00:00Z"), _ev("future", "2030-01-10T13:00:00Z")],
        stage="D", hold=1, event_rule=RULE)
    assert res["dropped"]["OUT_OF_PANEL"] == 1 and res["n_events_resolved"] == 1


def test_j_the_placebo_redates_episodes_inside_their_own_layer():
    p = _panel(seed=5)
    evs = _many("d", "2012-01-01", "2017-11-30", 30)
    fn = EVB.make_events_fn(evs)
    cand = EVB.run_event_window_book(p, fn, stage="D", hold=2, event_rule=RULE)
    plc_fn = B.permuted_score_fn(fn, seed=7)
    assert getattr(plc_fn, "__placebo_seed__", None) == 7
    plc = EVB.run_event_window_book(p, plc_fn, stage="D", hold=2, event_rule=RULE)
    assert plc["placebo_seed"] == 7 and cand["placebo_seed"] is None
    c_dates = {o["entry_date"] for o in cand["series"]["observations"]}
    p_dates = {o["entry_date"] for o in plc["series"]["observations"]}
    assert c_dates != p_dates
    assert all(o["layer"] == "D" for o in plc["series"]["observations"])
    assert plc["n_observations"] <= 30 and plc["n_observations"] >= 20
    # the candidate's own observations are untouched by the placebo call
    assert cand["layers"]["D"]["ann_net_excess"] != plc["layers"]["D"]["ann_net_excess"]


def test_k_the_book_is_registered_and_runs_through_run_stage():
    assert B.BOOK_EVENT == "FUTURES_EVENT_WINDOW" and B.BOOK_EVENT in B.BOOKS
    assert B.BOOK_EVENT in B.CALLABLE_CONTRACT
    p = _panel(seed=9)
    evs = _many("d", "2012-01-01", "2017-11-30", 40)
    out = B.run_stage(book=B.BOOK_EVENT, stage="D", panel=p,
                      score_fn=EVB.make_events_fn(evs), hold=1, event_rule=RULE)
    st = out["stats"]
    for k in ("ann_net_excess", "ann_gross_excess", "ann_cost_drag", "t_net_excess",
              "p_one_sided", "effective_observations", "mean_oneway_turnover_per_period",
              "strat_max_dd", "halves_ann_net_excess", "hit_rate"):
        assert k in st, k
    assert "net_sharpe" not in st                      # one materiality metric, one floor
    assert st["effective_observations"] == 40
    with pytest.raises(B.BookRefusal):
        B.run_stage(book=B.BOOK_EVENT, stage="D", panel=p,
                    score_fn=EVB.make_events_fn(evs), hold=3, event_rule=RULE)


def test_l_the_canonical_gate_reads_event_layers_unchanged():
    p = _panel(seed=11)
    evs = (_many("d", "2012-01-01", "2017-11-30", 40, seed=1)
           + _many("v", "2018-02-01", "2022-11-30", 40, seed=2)
           + _many("l", "2023-02-01", "2024-11-30", 40, seed=3))
    res = EVB.run_event_window_book(p, evs, stage="L", hold=1, event_rule=RULE)
    g = E.gate({"layers": res["layers"]}, prior_burden=100, family_tests=1)
    assert g["burden_denominator"] == 101
    assert set(g["checks"]) >= {"has_lockbox_observations", "lockbox_material",
                                "validation_same_sign", "burden_corrected_significant"}
    assert g["lockbox_observations"] == res["layers"]["L"]["effective_observations"]
    assert g["materiality_metrics"] == ["ann_net_excess"]
    # Random returns do not qualify.
    assert g["qualified"] is False


def test_m_a_scheduled_release_enters_k_sessions_before_and_needs_the_schedule():
    p = _panel(seed=2)
    rule = {"entry_rule": EVB.ENTRY_BEFORE_EVENT, "pre_event_sessions": 1,
            "benchmark": EVB.BENCH_CASH}
    ev = _ev("wasde", "2014-12-15T12:00:00Z", ts="2015-06-10T16:00:00Z")   # 12:00 ET release
    res = EVB.run_event_window_book(p, [ev], stage="D", hold=1, event_rule=rule)
    o = res["series"]["observations"][0]
    assert o["entry_date"] == "2015-06-09"
    assert o["gross"] == pytest.approx(float(p["ret"][0, _idx(p, "2015-06-10")]))
    assert o["bench"] == 0.0
    late = _ev("late", "2015-06-10T10:00:00Z", ts="2015-06-10T16:00:00Z")
    with pytest.raises(EVB.EventBookRefusal, match="no event resolves"):
        EVB.run_event_window_book(p, [late], stage="D", hold=1, event_rule=rule)
    r2 = EVB.run_event_window_book(p, [ev, late], stage="D", hold=1, event_rule=rule)
    assert r2["dropped"]["SCHEDULE_NOT_YET_AVAILABLE_AT_ENTRY"] == 1
