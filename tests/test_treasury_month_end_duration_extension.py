"""Hermetic regressions for the preregistered bond-index month-end duration-extension executor
(TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_V1).

SYNTHETIC DATA ONLY. No network, no owned store, no real price: the issuance history, the
exchange-like session calendar, the held contracts and every return are generated here. What
these tests make checkable:

* the PIT rule - an issue auctioned on or after the entry session never enters the measure;
* the ranking reads prior months only;
* a held-contract change inside the window costs one extra round trip;
* every gate maps to its verdict, in the preregistered order, and QUALIFIED is a human gate
  with capital_eligible False;
* the confirmation window is never read after a qualification failure;
* the kill rule is refused unless it matches byte for byte;
* the result passes alpha_agent.r59.mechanisms.validate_result.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent import alpha_recovery as AR
from alpha_agent.alpha_recovery import treasury_month_end_duration_extension as E
from alpha_agent.r59 import mechanisms as MX

REPO = Path(__file__).resolve().parents[1]
CONTRACT_KEYS = {"verdict", "why", "kill_rule_fired", "statistic", "economics", "multiplicity",
                 "artifact", "capital_eligible", "scorer", "input_data_identity"}
ENTRY = {"mechanism_id": E.MECHANISM_ID, "pnl_gate": {"KILL_RULE": E.KILL_RULE_FROZEN}}

#: The lead's amended kill rule, verbatim.
LEAD_KILL_RULE = (
    "Kill if the extension-ranked month-end long book has NW t < 2.0 or net below 1.5 %/yr at the "
    "R38 per-market cost (2 bp per side); or its increment over an unconditional month-end long of "
    "the same futures has NW t < 2.0; or it fails BH q=0.10 over m=3 with R32_EVENT_DRIVEN_CALENDAR "
    "and TREASURY_AUCTION_SUPPLY_CONCESSION_V1 inherited at p=1.")


@pytest.fixture()
def root(tmp_path, monkeypatch):
    r = tmp_path / "research_root"
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(r))
    return r


# --------------------------------------------------------------------------- #
# Synthetic substrate
# --------------------------------------------------------------------------- #
def _dates(start="1999-06-01", end="2026-08-21") -> np.ndarray:
    d = pd.bdate_range(start, end)
    holiday = ((d.month == 1) & (d.day == 1)) | ((d.month == 7) & (d.day == 4)) | \
              ((d.month == 12) & (d.day == 25))
    return d[~holiday].strftime("%Y-%m-%d").to_numpy().astype("<U10")


def _months(dates) -> dict:
    out: dict = {}
    for i, d in enumerate(dates):
        out.setdefault(str(d)[:7], []).append(i)
    return out


def _held(dates) -> np.ndarray:
    """Quarterly contracts; in Feb/May/Aug/Nov the layer rolls between the entry session L[-3]
    and L[-2], exactly like the real first-notice months."""
    held = np.empty(len(dates), dtype=object)
    for ym, ii in _months(dates).items():
        y, m = int(ym[:4]), int(ym[5:])
        for pos, i in enumerate(ii):
            k = m // 3 + (1 if (m % 3 == 2 and pos >= len(ii) - 2) else 0)
            held[i] = "ZN-%d-%d" % (y, k)
    return held


def _mat(issue: str, years: int) -> str:
    """A fixed day count, so every synthetic issue of one tenor carries exactly one duration and
    the trailing-median comparisons are exact ties rather than day-of-month noise."""
    from datetime import date, timedelta
    return (date.fromisoformat(issue) + timedelta(days=round(365.25 * years))).isoformat()


def _record(term, auction, issue, maturity, cusip, offering, coupon="4.000000", sec="Note", **over):
    r = {"cusip": cusip, "type": sec, "securityType": sec, "securityTerm": term,
         "originalSecurityTerm": term, "reopening": "No", "tips": "No", "floatingRate": "No",
         "announcementDate": auction + "T00:00:00", "auctionDate": auction + "T00:00:00",
         "issueDate": issue + "T00:00:00", "maturityDate": maturity + "T00:00:00",
         "interestRate": coupon, "offeringAmount": str(int(offering))}
    r.update(over)
    return r


def _records(dates, *, late_amount=500e9) -> list:
    """A 10y every month (heavy in Feb/May/Aug/Nov) plus, in Mar/Jun/Sep/Dec, a huge 7y auctioned
    the session AFTER entry: it enters the full-information measure but never the PIT one."""
    recs = []
    for ym, ii in _months(dates).items():
        if ym < "2000-01" or ym > "2026-07":
            continue
        m = int(ym[5:])
        d10 = str(dates[ii[10]])
        recs.append(_record("10-Year", str(dates[ii[6]]), d10, _mat(d10, 10), "T" + ym,
                            40e9 if m % 3 == 2 else 20e9))
        if m % 3 == 0 and late_amount:
            x = str(dates[ii[-1]])
            recs.append(_record("7-Year", str(dates[ii[-2]]), x, _mat(x, 7), "S" + ym, late_amount))
    return recs


def _synthetic(*, q=0.004, c=None, h1=None, uncond=0.0, noise=0.0001, seed=11):
    """Per-session drift on L[-2] and L[-1]: ``q`` in heavy months up to 2016, ``c`` after
    (default q), ``h1`` overrides 2000-2008, ``uncond`` added in every month."""
    dates = _dates()
    rng = np.random.default_rng(seed)
    ret = rng.normal(0.0, noise, len(dates))
    for ym, ii in _months(dates).items():
        if ym < "2000-01" or ym > "2026-07":
            continue
        e = q if ym <= "2016-12" else (q if c is None else c)
        if h1 is not None and ym <= "2008-12":
            e = h1
        eff = (e if int(ym[5:]) % 3 == 2 else 0.0) + uncond
        ret[ii[-2]] += eff
        ret[ii[-1]] += eff
    bars = {"ZN": {"dates": dates, "ret": ret, "held": _held(dates)}}
    costs = {"r38_bps_per_side": {"ZN": 2.0}, "gate_bps_per_side": {"ZN": 2.0}}
    return _records(dates), bars, costs


def _stub(monkeypatch, recs, bars, costs, incumbent=None):
    monkeypatch.setattr(E, "load_auctions", lambda: {
        "state": "OK", "records": recs, "from_cache": True,
        "manifest": {"sha256": "synthetic", "rows": len(recs), "source": "synthetic",
                     "fetched_at_utc": "synthetic"}})
    monkeypatch.setattr(E, "load_bars", lambda: {"state": "OK", "bars": bars,
                                                 "identity": {"synthetic": True}})
    monkeypatch.setattr(E, "load_costs", lambda: costs)
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: incumbent)


def _spy_windows(monkeypatch) -> list:
    seen: list = []
    real = E.window_rows

    def spy(window, *a, **k):
        seen.append(window)
        return real(window, *a, **k)

    monkeypatch.setattr(E, "window_rows", spy)
    return seen


# --------------------------------------------------------------------------- #
# The contract is the catalog's contract
# --------------------------------------------------------------------------- #
def test_kill_rule_is_the_lead_text_verbatim_and_the_catalog_text():
    assert E.KILL_RULE_FROZEN == LEAD_KILL_RULE
    cat = json.loads((REPO / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json")
                     .read_text(encoding="utf-8"))
    entry = [e for e in cat["mechanisms"] if e["mechanism_id"] == E.MECHANISM_ID][0]
    assert entry["pnl_gate"]["KILL_RULE"] == E.KILL_RULE_FROZEN
    assert entry["horizon_sessions"] == E.HOLD_SESSIONS == 2


def test_frozen_design_constants():
    assert E.MARKET == "ZN" and E.LOOKBACK_MONTHS == 12 and E.ENTRY_FROM_END == 3
    assert E.QUALIFICATION == ("2000-01", "2016-12") and E.CONFIRMATION == ("2017-01", "2026-08")
    assert E.HALVES == {"H1_2000_2008": ("2000-01", "2008-12"), "H2_2009_2016": ("2009-01", "2016-12")}
    assert E.KILL_RULE_COST_BPS == 2.0 and E.STRESS_COST_BPS == 5.0
    assert E.INHERITED_NULLS == ("R32_EVENT_DRIVEN_CALENDAR", "TREASURY_AUCTION_SUPPLY_CONCESSION_V1")
    assert E.T_FLOOR == 2.0 and E.MIN_EFFECTIVE_PERIODS == 36 and E.BH_Q == 0.10
    assert E.MATERIALITY_ANN_NET == 0.015
    assert set(E.VERDICTS_USED) <= set(MX.EXECUTOR_VERDICTS)
    exp = E.expected_months(E.QUALIFICATION)
    assert exp[0] == "2001-01" and exp[-1] == "2016-12" and len(exp) == 192


@pytest.mark.parametrize("mechanism", [
    {"mechanism_id": "OTHER", "pnl_gate": {"KILL_RULE": LEAD_KILL_RULE}},
    {"mechanism_id": E.MECHANISM_ID, "pnl_gate": {"KILL_RULE": "Kill if NW t < 1.0"}},
    {"mechanism_id": E.MECHANISM_ID, "pnl_gate": {"KILL_RULE": LEAD_KILL_RULE + " "}},
    {"mechanism_id": E.MECHANISM_ID, "pnl_gate": {"KILL_RULE": LEAD_KILL_RULE.replace("m=3", "m=1")}},
    {"mechanism_id": E.MECHANISM_ID, "pnl_gate": {}},
])
def test_run_mechanism_refuses_before_touching_data(monkeypatch, mechanism):
    monkeypatch.setattr(E, "load_auctions", lambda: pytest.fail("data touched before the refusal"))
    with pytest.raises(ValueError):
        E.run_mechanism(mechanism=mechanism)


# --------------------------------------------------------------------------- #
# The measure
# --------------------------------------------------------------------------- #
def test_duration_is_a_par_bond_modified_duration_from_coupon_and_dates():
    years = 3652 / 365.25
    assert E.approx_modified_duration(0.0, "2010-01-01", "2020-01-01") == pytest.approx(years)
    assert E.approx_modified_duration(5.0, "2010-01-01", "2020-01-01") == pytest.approx(
        (1.0 - 1.025 ** (-2.0 * years)) / 0.05)
    assert E.approx_modified_duration(4.0, "2020-01-01", "2020-01-01") is None


def test_index_supply_counts_every_nominal_coupon_and_excludes_tips_and_frns():
    base = ("2016-06-08", "2016-06-15")
    recs = [_record("2-Year", *base, "2018-06-15", "A2", 26e9),
            _record("3-Year", *base, "2019-06-15", "A3", 24e9),
            _record("7-Year", *base, "2023-06-15", "A7", 28e9),
            _record("20-Year", *base, "2036-06-15", "B20", 12e9, sec="Bond"),
            _record("10-Year", *base, "2026-06-15", "TIPS", 13e9, tips="Yes"),
            _record("2-Year", *base, "2018-06-15", "FRN", 15e9, floatingRate="Yes"),
            _record("5-Year", *base, "2021-06-15", "NOCOUPON", 34e9, interestRate="")]
    rows = {a["cusip"]: a for a in E.issue_table(recs)}
    for c in ("A2", "A3", "A7", "B20"):
        assert rows[c]["exclusion"] is None and rows[c]["duration_added"] > 0
    assert rows["TIPS"]["exclusion"] == "TIPS" and rows["TIPS"]["duration_added"] is None
    assert rows["FRN"]["exclusion"] == "FRN" and not rows["FRN"]["field_missing"]
    assert rows["NOCOUPON"]["field_missing"] is True and rows["NOCOUPON"]["duration_added"] is None
    assert rows["A7"]["duration_added"] == pytest.approx(28.0 * rows["A7"]["duration"])


def test_pit_excludes_every_auction_on_or_after_the_entry_session():
    dates = _dates("2016-05-02", "2016-09-30")
    months = E.month_table(dates)
    m = months["2016-06"]
    e, x = m["E"], m["X"]
    assert m["entry_date"] == str(dates[e]) and m["exit_date"] == str(dates[x]) and x - e == 2
    issue_x = str(dates[x])
    recs = [_record("10-Year", str(dates[e - 10]), str(dates[e - 5]), "2026-06-15", "EARLY", 20e9),
            _record("7-Year", str(dates[e - 1]), issue_x, "2023-06-28", "BEFORE", 28e9),
            _record("5-Year", str(dates[e]), issue_x, "2021-06-28", "ON_ENTRY", 34e9),
            _record("2-Year", str(dates[e + 1]), issue_x, "2018-06-28", "AFTER", 26e9)]
    issues = E.issue_table(recs)
    da = {a["cusip"]: a["duration_added"] for a in issues}
    meas = E.monthly_measure(issues, months)["2016-06"]
    assert meas["duration_added"] == pytest.approx(da["EARLY"] + da["BEFORE"])
    assert meas["duration_added_full"] == pytest.approx(sum(da.values()))
    assert meas["pit_excluded"] == 2 and sorted(meas["pit_excluded_tenors"]) == [2, 5]


def test_a_late_huge_auction_never_moves_the_pit_ranking():
    recs, bars, _ = _synthetic()
    b = bars["ZN"]
    months = E.month_table(b["dates"], b["held"])
    measure = E.monthly_measure(E.issue_table(recs), months)
    pit = E.rank_months(measure)
    full = E.rank_months(measure, key="duration_added_full")
    assert min(pit) == "2001-01"
    assert all(v["high"] == (int(ym[5:]) % 3 == 2) for ym, v in pit.items())
    assert any(v["high"] and int(ym[5:]) % 3 == 0 for ym, v in full.items())


def _measure(values, start="2000-01") -> dict:
    p = pd.Period(start, "M")
    return {str(p + i): {"duration_added": float(v), "duration_added_full": float(v)}
            for i, v in enumerate(values)}


def test_the_ranking_reads_prior_months_only():
    values = list(range(1, 13)) + [6] + [100] * 6            # index 12 = 2001-01
    ranks = E.rank_months(_measure(values))
    assert "2000-12" not in ranks and ranks["2001-01"]["trailing_median"] == 6.5
    assert ranks["2001-01"]["high"] is False
    future = list(values)
    future[13:] = [0.0] * 6
    assert E.rank_months(_measure(future))["2001-01"] == ranks["2001-01"]
    own = list(values)
    own[12] = 6.4
    assert E.rank_months(_measure(own))["2001-01"]["trailing_median"] == 6.5
    older = [0.0] + list(values)                                # one extra month before 2000-01
    shifted = E.rank_months(_measure(older, start="1999-12"))
    assert shifted["2001-01"] == ranks["2001-01"]
    prior = list(values)
    prior[11] = 1.0                                             # M-1 changes the median
    assert E.rank_months(_measure(prior))["2001-01"]["high"] is True
    gap = _measure(values)
    gap.pop("2000-06")
    g = E.rank_months(gap)
    assert "2001-06" not in g and "2001-07" in g


# --------------------------------------------------------------------------- #
# Calendar, rolls and costs
# --------------------------------------------------------------------------- #
def test_a_roll_inside_the_window_costs_one_extra_round_trip():
    dates = _dates("2016-01-04", "2016-04-29")
    held = _held(dates)
    months = E.month_table(dates, held)
    assert months["2016-02"]["rolls"] == 1 and months["2016-02"]["roll_sessions_after_entry"] == [1]
    assert months["2016-01"]["rolls"] == 0 and months["2016-03"]["rolls"] == 0
    assert months["2016-03"]["complete"] and not months["2016-04"]["complete"]
    rolled = {"gross": 0.01, "rolls": 1, "high": True}
    plain = {"gross": 0.01, "rolls": 0, "high": True}
    flat = {"gross": 0.01, "rolls": 1, "high": False}
    assert E.book([rolled], 2.0)[0] == pytest.approx(0.01 - 4 * 2.0 / 1e4)
    assert E.book([plain], 2.0)[0] == pytest.approx(0.01 - 2 * 2.0 / 1e4)
    assert E.book([flat], 2.0)[0] == 0.0
    assert E.unconditional([flat], 5.0)[0] == pytest.approx(0.01 - 4 * 5.0 / 1e4)


def test_window_returns_are_the_two_sessions_after_the_entry_close():
    dates = _dates("2016-01-04", "2016-04-29")
    months = E.month_table(dates, _held(dates))
    ret = np.zeros(len(dates))
    m = months["2016-02"]
    ret[m["E"]] = 0.5                                           # the entry session itself: not held
    ret[m["E"] + 1], ret[m["X"]] = 0.01, -0.02
    ranks = {"2016-02": {"high": True}}
    monkey = dict(E.WINDOWS)
    try:
        E.WINDOWS["QUALIFICATION"] = ("2016-02", "2016-02")
        rows = E.window_rows("QUALIFICATION", months, ranks, ret)
    finally:
        E.WINDOWS.clear()
        E.WINDOWS.update(monkey)
    assert len(rows) == 1 and rows[0]["gross"] == pytest.approx(1.01 * 0.98 - 1.0)
    assert rows[0]["rolls"] == 1 and rows[0]["window_dates"] == [str(dates[m["E"] + 1]), m["exit_date"]]


def test_increment_is_the_timing_series_against_the_window_share():
    rows = [{"gross": 0.01, "rolls": 0, "high": True}, {"gross": 0.01, "rolls": 0, "high": False},
            {"gross": 0.01, "rolls": 0, "high": False}, {"gross": 0.01, "rolls": 0, "high": False}]
    z, pi = E.increment(rows, 0.0)
    assert pi == 0.25 and z.sum() == pytest.approx(0.0)        # unconditional exposure nets out
    assert list(z) == pytest.approx([0.0075, -0.0025, -0.0025, -0.0025])


def test_census_never_reads_a_return():
    recs, bars, _ = _synthetic()
    b = bars["ZN"]
    months = E.month_table(b["dates"], b["held"])                 # no ret, no finiteness
    cen = E.census(E.issue_table(recs), months, E.monthly_measure(E.issue_table(recs), months))
    q = cen["windows"]["QUALIFICATION"]
    assert cen["reads_returns"] is False
    assert q["issues_pit_excluded"] == 68 and q["months_with_a_pit_exclusion"] == 68
    assert q["lookbacks"]["12"]["high_share"] == pytest.approx(1 / 3)
    assert q["lookbacks"]["12"]["roll_in_window_share_high"] == 1.0
    assert q["lookbacks"]["12"]["roll_in_window_share_low"] == 0.0
    assert q["cost_hurdle_per_unit_zn_notional"]["2bp"]["book_cost_drag_ann"] == pytest.approx(
        12 * (1 / 3) * 8e-4)


# --------------------------------------------------------------------------- #
# Verdict order - every gate maps to its verdict
# --------------------------------------------------------------------------- #
GOOD_Q = {"data_hold": None, "high_months": 100, "low_months": 90, "gross_mean_active": 0.003,
          "net_t": 3.0, "ann_net": 0.03, "increment_t": 2.5,
          "halves_ann_net": {"H1_2000_2008": 0.02, "H2_2009_2016": 0.03}, "fdr_pass": True,
          "stress_ann_net": 0.02, "incumbent": {"state": "DATA_HOLD"}}
GOOD_C = {"high_months": 60, "net_mean": 0.002, "ann_net": 0.02, "t": 2.5, "p": 0.006,
          "increment_mean": 0.0005}


def _q(**over):
    return dict(GOOD_Q, **over)


def _c(**over):
    return dict(GOOD_C, **over)


@pytest.mark.parametrize("g,c,verdict,kill", [
    (_q(), _c(), E.V_QUALIFIED, None),
    (_q(data_hold="ZN missing"), _c(), E.V_DATA_HOLD, None),
    (_q(high_months=35), _c(), E.V_SAMPLE, "INSUFFICIENT_SAMPLE"),
    (_q(low_months=35), _c(), E.V_SAMPLE, "INSUFFICIENT_SAMPLE"),
    (_q(gross_mean_active=0.0), _c(), E.V_WRONG_SIGN, "LONG_DOES_NOT_EARN"),
    (_q(net_t=1.99), _c(), E.V_NO_EDGE, "NW_T_BELOW_2"),
    (_q(net_t=None), _c(), E.V_NO_EDGE, "NW_T_BELOW_2"),
    (_q(ann_net=0.0149), _c(), E.V_MATERIALITY, "NET_BELOW_1.5PCT_PER_YEAR"),
    (_q(increment_t=1.99), _c(), E.V_NONINCREMENTAL, "NO_INCREMENT_OVER_UNCONDITIONAL_MONTH_END"),
    (_q(halves_ann_net={"H1_2000_2008": 0.0, "H2_2009_2016": 0.03}), _c(), E.V_UNSTABLE,
     "HALF_NOT_POSITIVE"),
    (_q(fdr_pass=False), _c(), E.V_MULTIPLICITY, "BH_Q010_FAILS_M3"),
    (_q(stress_ann_net=0.0149), _c(), E.V_MATERIALITY, "NET_BELOW_1.5PCT_PER_YEAR_AT_STRESS_COST"),
    (_q(incumbent={"state": "OK", "positive_incremental_utility_after_costs": False}), _c(),
     E.V_NO_INC_INFO, "NO_EQUAL_RISK_INCREMENT_OVER_INCUMBENT"),
    (_q(incumbent={"state": "OK", "positive_incremental_utility_after_costs": True}), _c(),
     E.V_QUALIFIED, None),
    (_q(), None, E.V_NEED_MORE, None),
    (_q(), {"data_hold": "confirmation gap"}, E.V_DATA_HOLD, None),
    (_q(), _c(high_months=20), E.V_NEED_MORE, None),
    (_q(), _c(net_mean=-0.001), E.V_UNSTABLE, "CONFIRMATION_DID_NOT_REPRODUCE"),
    (_q(), _c(ann_net=0.01), E.V_UNSTABLE, "CONFIRMATION_DID_NOT_REPRODUCE"),
    (_q(), _c(t=1.9), E.V_UNSTABLE, "CONFIRMATION_DID_NOT_REPRODUCE"),
    (_q(), _c(p=0.2), E.V_UNSTABLE, "CONFIRMATION_DID_NOT_REPRODUCE"),
    (_q(), _c(increment_mean=-0.0001), E.V_UNSTABLE, "CONFIRMATION_DID_NOT_REPRODUCE"),
])
def test_each_gate_maps_to_its_verdict(g, c, verdict, kill):
    v = E.verdict_for(g, c)
    assert v["verdict"] == verdict and v["kill_rule_fired"] == kill and v["why"]
    assert v["verdict"] in MX.EXECUTOR_VERDICTS


def test_gate_precedence_follows_the_preregistered_order():
    worst = _q(high_months=1, gross_mean_active=-1.0, net_t=0.0, ann_net=0.0, increment_t=0.0,
               fdr_pass=False, stress_ann_net=0.0)
    assert E.verdict_for(dict(worst, data_hold="gap"), _c())["verdict"] == E.V_DATA_HOLD
    assert E.verdict_for(worst, _c())["verdict"] == E.V_SAMPLE
    assert E.verdict_for(_q(gross_mean_active=-1.0, net_t=0.0), _c())["verdict"] == E.V_WRONG_SIGN
    assert E.verdict_for(_q(net_t=1.0, ann_net=0.0), _c())["verdict"] == E.V_NO_EDGE
    assert E.verdict_for(_q(ann_net=0.0, increment_t=0.0), _c())["verdict"] == E.V_MATERIALITY
    assert E.verdict_for(_q(increment_t=0.0, halves_ann_net={"H1_2000_2008": -1.0}),
                         _c())["verdict"] == E.V_NONINCREMENTAL
    assert E.verdict_for(_q(halves_ann_net={"H1_2000_2008": -1.0}, fdr_pass=False),
                         _c())["verdict"] == E.V_UNSTABLE
    assert E.verdict_for(_q(fdr_pass=False, stress_ann_net=0.0), _c())["verdict"] == E.V_MULTIPLICITY
    assert E.verdict_for(_q(stress_ann_net=0.0, incumbent={"state": "OK"}),
                         _c())["verdict"] == E.V_MATERIALITY


def test_inherited_nulls_make_m_three():
    assert E.multiplicity(0.03)["passes"] is True
    m = E.multiplicity(0.04)
    assert m["passes"] is False and m["m_inherited"] == 3 and m["benjamini_hochberg"]["m"] == 3
    assert E.multiplicity(None)["passes"] is False


# --------------------------------------------------------------------------- #
# End to end on synthetic data
# --------------------------------------------------------------------------- #
def test_a_synthetic_extension_qualifies_only_as_a_human_gate(root, monkeypatch):
    _stub(monkeypatch, *_synthetic())
    seen = _spy_windows(monkeypatch)
    res = E.run_mechanism(mechanism=ENTRY)
    assert set(res) == CONTRACT_KEYS and MX.validate_result(res) == []
    assert res["verdict"] == E.V_QUALIFIED, res["why"]
    assert res["capital_eligible"] is False and "HUMAN" in res["why"]
    assert seen == ["QUALIFICATION", "CONFIRMATION"]
    assert res["statistic"]["lockbox_t"] > 2.0 and res["statistic"]["increment_t"] > 2.0
    assert res["statistic"]["increment_pi"] == pytest.approx(1 / 3)
    p = Path(res["artifact"])
    assert p == root / "results" / E.ARTIFACT_NAME and p.exists()
    body = json.loads(p.read_text(encoding="utf-8"))
    assert body["capital_eligible"] is False and body["executor_result"]["verdict"] == E.V_QUALIFIED
    assert body["safety"]["creates_orders"] is False and body["kill_rule_frozen"] == LEAD_KILL_RULE


@pytest.mark.parametrize("kw,verdict,kill", [
    ({"q": -0.004}, E.V_WRONG_SIGN, "LONG_DOES_NOT_EARN"),
    ({"q": 0.0006}, E.V_MATERIALITY, "NET_BELOW_1.5PCT_PER_YEAR"),
    ({"q": 0.0, "uncond": 0.004}, E.V_NONINCREMENTAL, "NO_INCREMENT_OVER_UNCONDITIONAL_MONTH_END"),
    ({"q": 0.006, "h1": -0.0005}, E.V_UNSTABLE, "HALF_NOT_POSITIVE"),
    ({"q": 0.00255}, E.V_MATERIALITY, "NET_BELOW_1.5PCT_PER_YEAR_AT_STRESS_COST"),
])
def test_a_qualification_failure_never_reads_the_confirmation_window(root, monkeypatch, kw, verdict, kill):
    _stub(monkeypatch, *_synthetic(**kw))
    seen = _spy_windows(monkeypatch)
    body = E.run(verbose=False, write=True)
    res = body["executor_result"]
    assert res["verdict"] == verdict and res["kill_rule_fired"] == kill, res["why"]
    assert seen == ["QUALIFICATION"] and body["confirmation"] == "UNTOUCHED"
    assert res["statistic"]["lockbox_t"] is None and res["statistic"]["confirmation_months"] is None
    assert res["capital_eligible"] is False and MX.validate_result(res) == []


def test_an_extension_that_does_not_reproduce_is_killed_at_confirmation(root, monkeypatch):
    _stub(monkeypatch, *_synthetic(q=0.004, c=-0.004))
    seen = _spy_windows(monkeypatch)
    res = E.run_mechanism(mechanism=ENTRY)
    assert res["verdict"] == E.V_UNSTABLE and res["kill_rule_fired"] == "CONFIRMATION_DID_NOT_REPRODUCE"
    assert seen == ["QUALIFICATION", "CONFIRMATION"] and MX.validate_result(res) == []


def test_a_dominant_incumbent_blocks_before_confirmation(root, monkeypatch):
    rng = np.random.default_rng(3)
    idx = pd.bdate_range("2000-01-03", "2016-12-30")
    incumbent = pd.Series(0.01 + rng.normal(0.0, 0.001, len(idx)), index=idx)
    _stub(monkeypatch, *_synthetic(), incumbent=incumbent)
    seen = _spy_windows(monkeypatch)
    body = E.run(verbose=False, write=True)
    res = body["executor_result"]
    assert res["verdict"] == E.V_NO_INC_INFO, res["why"]
    assert res["economics"]["incumbent_equal_risk"]["state"] == "OK"
    assert seen == ["QUALIFICATION"] and body["confirmation"] == "UNTOUCHED"


def test_too_few_usable_months_is_a_data_hold(root, monkeypatch):
    recs, bars, costs = _synthetic()
    b = bars["ZN"]
    b["ret"] = np.where((b["dates"] >= "2005-01-01") & (b["dates"] <= "2005-12-31"), np.nan, b["ret"])
    _stub(monkeypatch, recs, bars, costs)
    res = E.run_mechanism(mechanism=ENTRY)
    assert res["verdict"] == E.V_DATA_HOLD and "180 of 192" in res["why"]
    assert set(res) == CONTRACT_KEYS and MX.validate_result(res) == []


def test_a_missing_market_is_a_data_hold_naming_the_gap(root, monkeypatch, tmp_path):
    layer = tmp_path / "empty_layer"
    layer.mkdir()
    monkeypatch.setattr(E.N, "NATIVE_LAYER_DIR", layer)
    recs, _bars, _costs = _synthetic()
    monkeypatch.setattr(E, "load_auctions", lambda: {"state": "OK", "records": recs,
                                                     "manifest": {"sha256": "s"}})
    res = E.run(verbose=False, write=True)["executor_result"]
    assert res["verdict"] == E.V_DATA_HOLD and "ZN" in res["why"]
    assert set(res) == CONTRACT_KEYS and res["capital_eligible"] is False
    assert (root / "results" / E.ARTIFACT_NAME).exists()


def test_a_missing_auction_cache_is_a_data_hold_and_never_fetched(root, monkeypatch):
    monkeypatch.setattr(E.TA, "fetch_auctions", lambda *a, **k: pytest.fail("fetched"))
    out = E.load_auctions()
    assert out["state"] == E.V_DATA_HOLD and "fetching is disabled" in out["why"]
    res = E.run(verbose=False, write=False)["executor_result"]
    assert res["verdict"] == E.V_DATA_HOLD and MX.validate_result(res) == []


# --------------------------------------------------------------------------- #
# Hygiene
# --------------------------------------------------------------------------- #
def test_no_api_engine_db_import_no_order_call_and_no_second_scorer():
    src = Path(E.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
        for mod in mods:
            assert not mod.startswith(("api", "engine", "db", "paper_trader", "sqlite3", "urllib")), mod
    called = {n.func.id if isinstance(n.func, ast.Name) else n.func.attr
              for n in ast.walk(tree) if isinstance(n, ast.Call)
              and isinstance(n.func, (ast.Name, ast.Attribute))}
    assert not called & {"submit_order", "place_order", "create_order", "create_orders", "fill",
                         "register_challenger", "adopt_forward", "promote", "promote_model",
                         "allocate", "activate_sleeve", "write_repo_artifact", "fetch_auctions"}
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert not names & {"nw_tstat", "bh_fdr", "benjamini_hochberg", "run_cell", "_max_dd"}
    assert "S.nw_tstat(" in src and "S.bh_fdr(" in src and "S._max_dd(" in src
    assert '"capital_eligible": True' not in src


def test_a_data_hold_result_is_still_the_full_contract():
    res = E.hold_result("synthetic gap")
    assert set(res) == CONTRACT_KEYS and res["capital_eligible"] is False
    assert res["verdict"] == E.V_DATA_HOLD and MX.validate_result(res) == []
