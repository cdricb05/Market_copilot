"""Hermetic regressions for the preregistered Treasury auction supply-concession
executor (TREASURY_AUCTION_SUPPLY_CONCESSION_V1).

SYNTHETIC DATA ONLY. No network, no owned store, no real price: the auction
calendar, the exchange-like session calendar and every return are generated
here. What these tests make checkable:

* the PIT rule - no entry on or before the announcementDate, a window that would
  open earlier is shortened to the next session, never opened early;
* the event-window arithmetic on a calendar with holidays and weekends;
* the frozen legs are 5y/10y/30y: a 2y auction is excluded and never placed;
* each kill rule maps to its verdict, in the preregistered order;
* capital_eligible is always False and the result contract is exact;
* the artifact lands under the (redirected) research root;
* a present auction cache is never re-downloaded.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import numpy as np
import pytest

from alpha_agent import alpha_recovery as AR
from alpha_agent.alpha_recovery import treasury_auction_concession as T
from alpha_agent.r59 import mechanisms as MX

REPO = Path(__file__).resolve().parents[1]
CONTRACT_KEYS = {"verdict", "why", "kill_rule_fired", "statistic", "economics", "multiplicity",
                 "artifact", "capital_eligible", "scorer", "input_data_identity"}
ENTRY = {"mechanism_id": T.MECHANISM_ID, "pnl_gate": {"KILL_RULE": T.KILL_RULE_FROZEN}}

#: The lead's kill rule after the 2y exclusion, verbatim (the catalog carries the same text).
LEAD_KILL_RULE = (
    "Kill if the per-auction DV01-matched short book has NW t < 2.0 or net below 1.5 %/yr at "
    "the R38 per-market cost (2 bp per side); or the concession is already complete between "
    "announcement and entry (announcement-to-entry return carries the whole effect); or the "
    "5y/10y/30y tenor legs disagree in sign; or it fails BH q=0.10 at m=1 with "
    "PRIMARY_DEALER_POSITIONS and RATES_CARRY_CURVE_RV inherited at p=1.")


@pytest.fixture()
def root(tmp_path, monkeypatch):
    r = tmp_path / "research_root"
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(r))
    return r


# --------------------------------------------------------------------------- #
# Synthetic substrate
# --------------------------------------------------------------------------- #
def _dates(start="1999-06-01", end="2026-08-21") -> np.ndarray:
    import pandas as pd
    d = pd.bdate_range(start, end)
    holiday = ((d.month == 1) & (d.day == 1)) | ((d.month == 7) & (d.day == 4)) | \
              ((d.month == 12) & (d.day == 25))
    return d[~holiday].strftime("%Y-%m-%d").to_numpy().astype("<U10")


#: tenor -> (securityType, securityTerm, session-of-month of the auction)
SPEC = {5: ("Note", "5-Year", 16), 10: ("Note", "10-Year", 7), 30: ("Bond", "30-Year", 8)}


def _record(sec, term, ann, auc, cusip, **over) -> dict:
    r = {"cusip": cusip, "type": sec, "securityType": sec, "securityTerm": term,
         "originalSecurityTerm": term, "reopening": "No", "tips": "No", "floatingRate": "No",
         "announcementDate": ann + "T00:00:00", "auctionDate": auc + "T00:00:00"}
    r.update(over)
    return r


def _synthetic(*, concession=0.0008, pre_entry=0.0, ann_lag=6, seed=7) -> tuple:
    dates = _dates()
    n = len(dates)
    rng = np.random.default_rng(seed)
    rets = {m: rng.normal(0.0, 0.0003, n) for m in T.MARKETS}
    held = np.array(["C-%s-Q%d" % (d[:4], (int(d[5:7]) - 1) // 3) for d in dates])
    months: dict = {}
    for i, d in enumerate(dates):
        months.setdefault(d[:7], []).append(i)
    recs = []
    for ym, idx in months.items():
        if ym < "2000-01" or ym > "2026-07":
            continue
        for t, (sec, term, k) in SPEC.items():
            A = idx[k]
            s_ann = A - ann_lag
            m = T.TENOR_MARKET[t]
            E = max(A - T.HOLD_SESSIONS, s_ann + 1)
            rets[m][E + 1:A + 1] -= concession
            if pre_entry:
                rets[m][s_ann + 1:E + 1] -= pre_entry
            recs.append(_record(sec, term, dates[s_ann], dates[A], "%s%d" % (ym, t)))
    bars = {m: {"dates": dates, "ret": rets[m], "held": held} for m in T.MARKETS}
    costs = {"r38_bps_per_side": {m: 2.0 for m in T.MARKETS},
             "gate_bps_per_side": {m: 2.0 for m in T.MARKETS}}
    return recs, bars, costs


def _stub_substrate(monkeypatch, recs, bars, costs):
    monkeypatch.setattr(T, "load_auctions", lambda **kw: {
        "state": "OK", "records": recs, "from_cache": True,
        "manifest": {"sha256": "synthetic", "rows": len(recs), "source": "synthetic",
                     "fetched_at_utc": "synthetic"}})
    monkeypatch.setattr(T, "load_bars", lambda *a, **k: {"state": "OK", "bars": bars,
                                                         "identity": {"synthetic": True}})
    monkeypatch.setattr(T, "load_costs", lambda *a, **k: costs)


# --------------------------------------------------------------------------- #
# The contract is the catalog's contract
# --------------------------------------------------------------------------- #
def test_kill_rule_is_the_lead_text_verbatim():
    assert T.KILL_RULE_FROZEN == LEAD_KILL_RULE
    assert "5y/10y/30y" in T.KILL_RULE_FROZEN and "2y" not in T.KILL_RULE_FROZEN


def test_kill_rule_matches_the_catalog_once_the_lead_has_updated_it():
    cat = json.loads((REPO / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json")
                     .read_text(encoding="utf-8"))
    entry = [e for e in cat["mechanisms"] if e["mechanism_id"] == T.MECHANISM_ID][0]
    assert entry["horizon_sessions"] == T.HOLD_SESSIONS
    if "2y/5y/10y/30y" in entry["pnl_gate"]["KILL_RULE"]:
        pytest.skip("catalog still carries the pre-exclusion kill rule; the lead updates it")
    assert entry["pnl_gate"]["KILL_RULE"] == T.KILL_RULE_FROZEN


def test_frozen_design_constants():
    assert T.TENOR_MARKET == {5: "ZF", 10: "ZN", 30: "ZB"}
    assert T.MARKETS == ("ZF", "ZN", "ZB") and "ZT" not in T.APPROX_DURATION
    assert set(T.EXCLUDED_TENORS) == {2, 3, 7, 20}
    assert T.QUALIFICATION == ("2000-01-01", "2016-12-31")
    assert T.CONFIRMATION == ("2017-01-01", "2026-12-31")
    assert T.INHERITED_NULLS == ("PRIMARY_DEALER_POSITIONS", "RATES_CARRY_CURVE_RV")
    assert T.KILL_RULE_COST_BPS == 1.0 and T.COST_LADDER_BPS == (1.0, 2.0, 5.0)
    assert T.T_FLOOR == 2.0 and T.MIN_EFFECTIVE_PERIODS == 36 and T.BH_Q == 0.10
    assert T.MATERIALITY_ANN_NET == 0.015
    assert set(T.VERDICTS_USED) <= set(MX.EXECUTOR_VERDICTS)


def test_run_mechanism_refuses_another_mechanism_or_a_changed_kill_rule():
    with pytest.raises(ValueError):
        T.run_mechanism(mechanism={"mechanism_id": "OTHER", "pnl_gate": ENTRY["pnl_gate"]})
    with pytest.raises(ValueError):
        T.run_mechanism(mechanism={"mechanism_id": T.MECHANISM_ID,
                                   "pnl_gate": {"KILL_RULE": "Kill if NW t < 1.0"}})
    old = LEAD_KILL_RULE.replace("5y/10y/30y", "2y/5y/10y/30y")
    with pytest.raises(ValueError):
        T.run_mechanism(mechanism={"mechanism_id": T.MECHANISM_ID, "pnl_gate": {"KILL_RULE": old}})


# --------------------------------------------------------------------------- #
# Classification by the auctioned remaining term
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("sec,term,over,tenor,exclusion", [
    ("Note", "9-Year 11-Month", {"reopening": "Yes", "originalSecurityTerm": "10-Year"}, 10, None),
    ("Bond", "29-Year 6-Month", {"reopening": "Yes", "originalSecurityTerm": "30-Year"}, 30, None),
    ("Note", "4-Year 9-Month", {"reopening": "Yes", "originalSecurityTerm": "5-Year"}, 5, None),
    ("Note", "2-Year", {}, None, "EXCLUDED_TENOR_2Y"),
    ("Note", "2-Year", {"reopening": "Yes", "originalSecurityTerm": "5-Year"}, None,
     "EXCLUDED_TENOR_2Y"),
    ("Note", "3-Year", {}, None, "EXCLUDED_TENOR_3Y"),
    ("Note", "7-Year", {}, None, "EXCLUDED_TENOR_7Y"),
    ("Bond", "19-Year 11-Month", {"reopening": "Yes"}, None, "EXCLUDED_TENOR_20Y"),
    ("Note", "10-Year", {"tips": "Yes"}, None, "TIPS"),
    ("Note", "2-Year", {"floatingRate": "Yes"}, None, "FRN"),
    ("Note", "6-Year 4-Month", {"reopening": "Yes"}, None, "OFF_CURVE_REMAINING_TERM_6Y"),
])
def test_classification_uses_the_remaining_term(sec, term, over, tenor, exclusion):
    c = T.classify(_record(sec, term, "2010-05-05", "2010-05-12", "X", **over))
    assert c["tenor"] == tenor and c["exclusion"] == exclusion
    assert c["market"] == (T.TENOR_MARKET[tenor] if tenor else None)


def test_a_two_year_auction_is_excluded_and_never_placed():
    dates = _dates("2016-06-01", "2016-08-31")
    recs = [_record("Note", "2-Year", "2016-06-23", "2016-06-28", "TWO"),
            _record("Note", "5-Year", "2016-06-23", "2016-06-29", "FIVE")]
    auctions = T.auction_table(recs)
    two = [a for a in auctions if a["cusip"] == "TWO"][0]
    assert two["exclusion"] == "EXCLUDED_TENOR_2Y" and two["market"] is None
    cal = {m: {"dates": dates, "held": None} for m in T.MARKETS}
    events = T.build_events(auctions, cal)
    assert [e["cusip"] for e in events] == ["FIVE"]
    cen = T.census(auctions, events)
    assert cen["exclusions_by_window"]["QUALIFICATION"]["EXCLUDED_TENOR_2Y"] == 1
    assert "2Y" not in json.dumps(cen["windows"]["QUALIFICATION"]["tenors"])


# --------------------------------------------------------------------------- #
# PIT and event-window arithmetic on an exchange-like calendar
# --------------------------------------------------------------------------- #
def _placed(ann, auc):
    a = T.classify(_record("Note", "10-Year", ann, auc, "X"))
    return T.place_event(a, _dates("2016-06-01", "2016-08-31"))


def test_entry_is_five_sessions_before_the_auction_across_a_holiday():
    ev = _placed("2016-06-29", "2016-07-08")        # 2016-07-04 is a holiday
    assert ev["status"] == T.ST_OK
    assert ev["entry_date"] == "2016-06-30" and ev["hold_sessions"] == 5
    assert ev["deferred_by_pit"] is False
    assert ev["lead_calendar_days"] == 9 and ev["lead_sessions"] == 6


def test_no_entry_on_or_before_the_announcement_date():
    on_a5 = _placed("2016-06-30", "2016-07-08")     # announced ON the A-5 session
    assert on_a5["entry_date"] == "2016-07-01" and on_a5["hold_sessions"] == 4
    assert on_a5["deferred_by_pit"] is True
    weekend = _placed("2016-07-02", "2016-07-08")   # announced on a Saturday
    assert weekend["entry_date"] == "2016-07-05" and weekend["hold_sessions"] == 3
    for ev in (on_a5, weekend, _placed("2016-06-29", "2016-07-08")):
        assert ev["entry_date"] > ev["announcement_date"]


def test_pit_failures_and_calendar_gaps_have_their_own_status():
    assert _placed("2016-07-07", "2016-07-08")["status"] == T.ST_NO_HOLD
    assert _placed("2016-07-08", "2016-07-08")["status"] == T.ST_ANN_NOT_BEFORE
    assert _placed("2016-06-29", "2016-07-04")["status"] == T.ST_NOT_SESSION
    assert _placed("2016-08-29", "2016-09-07")["status"] == T.ST_BEYOND_DATA
    a = T.classify(_record("Note", "10-Year", "", "2016-07-08", "X", announcementDate=None))
    assert T.place_event(a, _dates("2016-06-01", "2016-08-31"))["status"] == T.ST_NO_ANNOUNCEMENT


def test_window_returns_compound_exactly_the_entry_to_auction_sessions():
    ret = np.array([np.nan, 0.01, -0.02, 0.03, 0.005, -0.001, 0.002])
    assert T._compound(ret, 2, 5) == pytest.approx((1.03) * (1.005) * (0.999) - 1.0)
    assert T._compound(ret, -1, 2) is None                   # a NaN inside the window
    ev = {"market": "ZN", "S_ann": 0, "E": 2, "A": 5}
    assert T.returns_available(ev, {"ZN": {"ret": ret}})
    assert not T.returns_available(dict(ev, S_ann=-1), {"ZN": {"ret": ret}})


def test_a_roll_inside_the_window_costs_one_extra_round_trip():
    dates = _dates("2016-06-01", "2016-08-31")
    held = np.array(["ZF-2016U" if d < "2016-07-06" else "ZF-2016Z" for d in dates])
    a = T.classify(_record("Note", "5-Year", "2016-06-29", "2016-07-08", "X"))
    ev = T.place_event(a, dates, held)
    assert ev["rolls_in_window"] == 1
    bars = {"ZF": {"dates": dates, "ret": np.full(len(dates), -0.001), "held": held}}
    p = T.event_pnl(ev, bars, 2.0, 0.0)
    k = T.dv01_scale("ZF")
    assert p["net_gate"] == pytest.approx(p["gross"] - 4 * 2.0 / 1e4 * k)
    assert p["gross"] == pytest.approx(-((0.999 ** 5) - 1.0) * k)


def test_clusters_chain_overlapping_windows_and_never_share_a_session():
    evs = [{"entry_date": "2016-01-04", "auction_date": "2016-01-08"},
           {"entry_date": "2016-01-07", "auction_date": "2016-01-12"},
           {"entry_date": "2016-01-12", "auction_date": "2016-01-15"},
           {"entry_date": "2016-01-20", "auction_date": "2016-01-25"}]
    assert T.clusters_of(evs) == [[0, 1], [2], [3]]


def test_census_never_reads_a_return():
    recs, bars, _costs = _synthetic()
    cal = {m: {"dates": b["dates"], "held": b["held"]} for m, b in bars.items()}  # no "ret"
    auctions = T.auction_table(recs)
    cen = T.census(auctions, T.build_events(auctions, cal))
    assert cen["reads_returns"] is False
    assert cen["windows"]["QUALIFICATION"]["pit_missing_share"] == 0.0


# --------------------------------------------------------------------------- #
# Verdict order - every kill rule maps to its verdict
# --------------------------------------------------------------------------- #
GOOD_Q = {"pit_missing_share": 0.0, "clusters": 300,
          "tenor_events": {"5Y": 150, "10Y": 150, "30Y": 100},
          "pre_entry_gross_t": 0.5, "gross_t": 4.0, "gross_mean": 0.002,
          "tenor_gross_means": {"5Y": 0.001, "10Y": 0.002, "30Y": 0.001},
          "net_t": 3.5, "ann_net": 0.04, "excess_t": 3.0,
          "fdr_family_pass": True, "fdr_inherited_pass": True}
GOOD_C = {"clusters": 200, "net_mean": 0.001, "lockbox_t": 2.5, "ann_net": 0.03,
          "pit_missing_share": 0.0}


def _q(**over):
    g = dict(GOOD_Q)
    g.update(over)
    return g


def _c(**over):
    c = dict(GOOD_C)
    c.update(over)
    return c


@pytest.mark.parametrize("g,c,verdict,kill", [
    (_q(), _c(), T.V_QUALIFIED, None),
    (_q(data_hold="ZB missing"), _c(), T.V_DATA_HOLD, None),
    (_q(pit_missing_share=0.06), _c(), T.V_PIT, "PIT_UNESTABLISHED"),
    (_q(clusters=35), _c(), T.V_SAMPLE, "INSUFFICIENT_SAMPLE"),
    (_q(tenor_events={"5Y": 150, "10Y": 150, "30Y": 11}), _c(), T.V_SAMPLE,
     "INSUFFICIENT_SAMPLE"),
    (_q(pre_entry_gross_t=3.0, gross_t=1.2), _c(), T.V_PRICED,
     "CONCESSION_COMPLETE_BETWEEN_ANNOUNCEMENT_AND_ENTRY"),
    (_q(pre_entry_gross_t=3.0, gross_t=2.5), _c(), T.V_QUALIFIED, None),
    (_q(gross_mean=-0.0001), _c(), T.V_WRONG_SIGN, "SHORT_DOES_NOT_EARN"),
    (_q(tenor_gross_means={"5Y": 0.001, "10Y": 0.002, "30Y": -0.0001}), _c(),
     T.V_UNSTABLE, "TENOR_LEGS_DISAGREE_IN_SIGN"),
    (_q(net_t=1.99), _c(), T.V_NO_EDGE, "NW_T_BELOW_2"),
    (_q(ann_net=0.0149), _c(), T.V_MATERIALITY, "NET_BELOW_1.5PCT_PER_YEAR"),
    (_q(excess_t=1.0), _c(), T.V_NONINCREMENTAL, "NO_INCREMENT_OVER_PASSIVE_SHORT"),
    (_q(fdr_family_pass=False), _c(), T.V_MULTIPLICITY, "BH_Q010_FAILS_M1_OR_INHERITED_M3"),
    (_q(fdr_inherited_pass=False), _c(), T.V_MULTIPLICITY, "BH_Q010_FAILS_M1_OR_INHERITED_M3"),
    (_q(), None, T.V_NEED_MORE, None),
    (_q(), _c(data_hold="confirmation gap"), T.V_DATA_HOLD, None),
    (_q(), _c(clusters=20), T.V_NEED_MORE, None),
    (_q(), _c(net_mean=-0.001), T.V_UNSTABLE, "CONFIRMATION_SIGN_REVERSED"),
    (_q(), _c(lockbox_t=1.9), T.V_NO_EDGE, "CONFIRMATION_NW_T_BELOW_2"),
    (_q(), _c(ann_net=0.01), T.V_MATERIALITY, "CONFIRMATION_NET_BELOW_1.5PCT_PER_YEAR"),
])
def test_each_gate_maps_to_its_verdict(g, c, verdict, kill):
    v = T.verdict_for(g, c)
    assert v["verdict"] == verdict and v["kill_rule_fired"] == kill and v["why"]


def test_gate_precedence_follows_the_preregistered_order():
    assert T.verdict_for(_q(pit_missing_share=0.5, clusters=1), _c())["verdict"] == T.V_PIT
    assert T.verdict_for(_q(clusters=1, gross_mean=-1.0), _c())["verdict"] == T.V_SAMPLE
    assert T.verdict_for(_q(pre_entry_gross_t=3.0, gross_t=-2.0, gross_mean=-1.0),
                         _c())["verdict"] == T.V_PRICED
    assert T.verdict_for(_q(gross_mean=-1.0, net_t=0.0), _c())["verdict"] == T.V_WRONG_SIGN
    assert T.verdict_for(_q(net_t=1.0, ann_net=0.0), _c())["verdict"] == T.V_NO_EDGE
    assert T.verdict_for(_q(excess_t=0.0, fdr_family_pass=False),
                         _c())["verdict"] == T.V_NONINCREMENTAL


def test_inherited_nulls_only_raise_the_bar():
    m = T.multiplicity(0.05)
    assert m["family_pass"] is True and m["inherited_pass"] is False and m["m_inherited"] == 3
    m = T.multiplicity(0.01)
    assert m["family_pass"] and m["inherited_pass"]
    m = T.multiplicity(None)
    assert not m["family_pass"] and not m["inherited_pass"]


# --------------------------------------------------------------------------- #
# End to end on synthetic data
# --------------------------------------------------------------------------- #
def test_a_synthetic_concession_qualifies_and_the_artifact_lands_under_the_root(root, monkeypatch):
    _stub_substrate(monkeypatch, *_synthetic())
    res = T.run_mechanism(mechanism=ENTRY)
    assert set(res) == CONTRACT_KEYS
    assert res["verdict"] == T.V_QUALIFIED, res["why"]
    assert res["capital_eligible"] is False and MX.validate_result(res) == []
    assert res["statistic"]["lockbox_t"] > 2.0
    assert set(res["statistic"]["tenor_leg_gross_t"]) == {"5Y", "10Y", "30Y"}
    p = Path(res["artifact"])
    assert p == root / "results" / T.ARTIFACT_NAME and p.exists()
    body = json.loads(p.read_text(encoding="utf-8"))
    assert body["capital_eligible"] is False and body["executor_result"]["verdict"] == T.V_QUALIFIED
    assert body["safety"]["creates_orders"] is False


def test_noise_alone_is_not_qualified_and_the_confirmation_stays_untouched(root, monkeypatch):
    _stub_substrate(monkeypatch, *_synthetic(concession=0.0))
    body = T.run(verbose=False, write=True)
    res = body["executor_result"]
    assert res["verdict"] != T.V_QUALIFIED and res["capital_eligible"] is False
    assert res["statistic"]["lockbox_t"] is None and body["confirmation"] == "UNTOUCHED"


def test_a_move_complete_before_entry_is_killed_priced_before_entry(root, monkeypatch):
    _stub_substrate(monkeypatch, *_synthetic(concession=0.0, pre_entry=0.0008, ann_lag=8))
    res = T.run_mechanism(mechanism=ENTRY)
    assert res["verdict"] == T.V_PRICED, res["why"]
    assert res["kill_rule_fired"] == "CONCESSION_COMPLETE_BETWEEN_ANNOUNCEMENT_AND_ENTRY"


def test_a_missing_treasury_market_is_a_data_hold_naming_the_gap(root, monkeypatch, tmp_path):
    layer = tmp_path / "layer"
    layer.mkdir()
    for m in ("ZF", "ZN"):
        (layer / ("%s.csv" % m)).write_text("Date,ret,close,held\n1999-01-04,,100,%s-1999H\n"
                                           "1999-01-05,0.001,100.1,%s-1999H\n" % (m, m),
                                           encoding="utf-8")
    monkeypatch.setattr(T.N, "NATIVE_LAYER_DIR", layer)
    bl = T.load_bars()
    assert bl["state"] == T.V_DATA_HOLD and "ZB" in bl["why"]
    recs, _bars, costs = _synthetic()
    monkeypatch.setattr(T, "load_auctions", lambda **kw: {"state": "OK", "records": recs,
                                                          "manifest": {"sha256": "s"}})
    body = T.run(verbose=False, write=True)
    res = body["executor_result"]
    assert res["verdict"] == T.V_DATA_HOLD and "ZB" in res["why"]
    assert set(res) == CONTRACT_KEYS and res["capital_eligible"] is False
    assert (root / "results" / T.ARTIFACT_NAME).exists()


# --------------------------------------------------------------------------- #
# The cache: fetched once, never re-downloaded
# --------------------------------------------------------------------------- #
def _fake_getter(calls):
    def get(url):
        calls.append(url)
        return json.dumps([_record("Note", "5-Year", "2010-05-20", "2010-05-26", "C%d" % len(calls))]
                          ).encode("utf-8")
    return get


def test_a_present_cache_is_read_and_never_downloaded_again(root, monkeypatch):
    calls: list = []
    man = T.fetch_auctions(years=(2010,), getter=_fake_getter(calls))
    assert len(calls) == 2 and man["rows"] == 2 and len(man["requests"]) == 2
    assert all(r["sha256"] and r["fetched_at_utc"] and r["url"].startswith(T.TA_WS_URL)
               for r in man["requests"])
    assert (root / T.DATA_SUBDIR / T.CACHE_FILE).exists()

    def forbidden(*a, **k):
        raise AssertionError("download attempted although the cache exists")

    monkeypatch.setattr(T, "fetch_auctions", forbidden)
    out = T.load_auctions()
    assert out["state"] == "OK" and out["from_cache"] is True and len(out["records"]) == 2


def test_a_tampered_cache_is_a_data_hold_not_a_refetch(root, monkeypatch):
    T.fetch_auctions(years=(2010,), getter=_fake_getter([]))
    (root / T.DATA_SUBDIR / T.CACHE_FILE).write_bytes(b"[]")
    monkeypatch.setattr(T, "fetch_auctions", lambda *a, **k: pytest.fail("refetched"))
    out = T.load_auctions()
    assert out["state"] == T.V_DATA_HOLD and "manifest" in out["why"]


def test_no_cache_fetches_exactly_once(root, monkeypatch):
    calls: list = []
    real = T.fetch_auctions
    monkeypatch.setattr(T, "fetch_auctions",
                        lambda dest=None, **k: real(dest, years=(2010,), getter=_fake_getter(calls)))
    first = T.load_auctions()
    second = T.load_auctions()
    assert first["from_cache"] is False and second["from_cache"] is True and len(calls) == 2


# --------------------------------------------------------------------------- #
# Hygiene
# --------------------------------------------------------------------------- #
def test_no_api_engine_db_import_no_order_call_and_no_second_scorer():
    src = Path(T.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
        for mod in mods:
            assert not mod.startswith(("api", "engine", "db", "paper_trader", "sqlite3")), mod
    called = {n.func.id if isinstance(n.func, ast.Name) else n.func.attr
              for n in ast.walk(tree) if isinstance(n, ast.Call)
              and isinstance(n.func, (ast.Name, ast.Attribute))}
    assert not called & {"submit_order", "place_order", "create_order", "create_orders", "fill",
                         "register_challenger", "adopt_forward", "promote", "promote_model",
                         "allocate", "activate_sleeve", "write_repo_artifact"}
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert not names & {"nw_tstat", "bh_fdr", "benjamini_hochberg", "run_cell", "_max_dd"}
    assert "S.nw_tstat(" in src and "S.bh_fdr(" in src and "S._max_dd(" in src
    assert '"capital_eligible": True' not in src


def test_a_data_hold_result_is_still_the_full_contract():
    res = T.hold_result("synthetic gap")
    assert set(res) == CONTRACT_KEYS and res["capital_eligible"] is False
    assert res["verdict"] == T.V_DATA_HOLD and MX.validate_result(res) == []
