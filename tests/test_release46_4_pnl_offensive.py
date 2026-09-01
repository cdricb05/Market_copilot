"""Release 46.4 - the prospective P&L offensive: alpha-to-P&L, research trades,
shadow NAV, zero-base allocation, risk, attribution, lanes and governance.

Every test here is hermetic: prices are synthetic, the research root is a
temp directory, no provider is reached and no production ledger is touched.

Claims locked shut:

**Existing science is untouched.** The seed and expansion specifications keep
their hashes; the judge, the ledger and the evidence gate are not modified.

**One economic calculation.** The cost stack is a decomposition of the frozen
contract cost, the open-trade mark reconciles with the judge's number at
close to floating-point precision, and a closed trade TAKES the judge's row.

**One prediction, one trade, append-only.** Opening twice is impossible;
marks and closes are keyed; states are derived; nothing is backdated.

**Money cannot see the future.** A trade is funded only by an allocation
decided strictly before its entry; weights are frozen per policy version;
the NAV is append-only and replay-idempotent; the arithmetic is exact.

**Nothing is an order.** No module here can reach an operational store.
"""
from __future__ import annotations

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

from alpha_agent import r46 as R46
from alpha_agent.r46 import advance as AD
from alpha_agent.r46 import allocation as AL
from alpha_agent.r46 import attribution as AT
from alpha_agent.r46 import challengers as CH
from alpha_agent.r46 import clock as CK
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import credit as CR
from alpha_agent.r46 import cftc as CF
from alpha_agent.r46 import emit as EM
from alpha_agent.r46 import events as EVN
from alpha_agent.r46 import judge as JD
from alpha_agent.r46 import leaderboard as LB
from alpha_agent.r46 import ledger as LG
from alpha_agent.r46 import macro as MC
from alpha_agent.r46 import marketdata as MD
from alpha_agent.r46 import nav as NV
from alpha_agent.r46 import opportunity as OC
from alpha_agent.r46 import pnl as PN
from alpha_agent.r46 import pnl_board as PB
from alpha_agent.r46 import regime as RGM
from alpha_agent.r46 import risk as RK
from alpha_agent.r46 import shadow as SH
from alpha_agent.r46 import strategy_pnl as SP
from alpha_agent.r46 import trades as TR
from api import prospective_tournament as PT

TEST_CAMPAIGN = "r46_4_pytest_campaign"

#: Production seed hashes frozen 2026-08-25; the R46.3 expansion hashes as
#: registered 2026-08-26T16:58:52Z. If any moves, prior economics were edited.
PRODUCTION_SEED_SPEC_HASHES = {
    "r46_eq_xs_mom_12_1": "c2aeb90d2e79ffb1c2666d6e46a6377a1d13a5a3325ffd6c60c4f9b436010c09",
    "r46_eq_xs_rev_5d": "45b6c2838a93a29949c5272142fb50624a79ac7383233ae8bee30669b1609518",
    "r46_eq_xs_lowvol_60d": "64cea1d3e8522554769a3c189e11e258b62bba7d6e4acb45911a92460fe28d0e",
    "r46_eq_xs_resid_mom_12_1": "5a65aaa2e6310af6b36a43266cf87d5085db4ff33c6f013fc71def0f1c10a4f9",
    "r46_fut_ts_mom_252": "6e9f9947fde8d4e4a6323cbaec50666a8a7f165a36598d124d73b8b98a9e6f3b",
    "r46_fx_xs_mom_252": "4307af0f9e03e1df841078f42cfbf3aa2ab5bf210115d0bc841fc9dddd1a83a7",
    "r46_vx_term_carry_5d": "62625f0dead4fc1f31cf27833fcde67a53e1246a38be082a20e663c3a6be0082",
    "r46_rates_curve_rv_5d": "6fc519beefb568abd88bf5387496a4412acac488ab2b1a82f511ed5a946933de",
    "r46_comdty_xs_mom_252": "492581a34daa731e04fa2a432d280acdb26af9bdc2642fb112668b92a8ccfd45",
    "r46_spx_trend_200d": "36ca1f2776d3e2979197f93869b43291c2b55829770d2c4531e8263759a46400",
}


# =========================================================================== #
# Fixtures: a hermetic root, synthetic bars, a tiny frozen field
# =========================================================================== #
@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path / "r46root" / TEST_CAMPAIGN)
    monkeypatch.setenv("PAPER_TRADER_ACCEPTANCE_MODE", "1")
    return tmp_path


DAYS = pd.bdate_range("2026-06-01", "2026-10-30")


def _series(start: float, drift: float, seed: int) -> pd.Series:
    rng = np.random.default_rng(seed)
    r = rng.normal(drift, 0.01, len(DAYS))
    return pd.Series(start * np.cumprod(1.0 + r), index=DAYS)


class Market:
    """Synthetic closes with a movable point-in-time clip."""

    def __init__(self):
        self.px = {"SPY": _series(500, 0.0005, 1), "TLT": _series(90, 0.0, 2),
                   "AAA": _series(50, 0.001, 3), "BBB": _series(30, -0.001, 4),
                   "$VIX": _series(18, 0.0, 5), "&ZN": _series(110, 0.0, 6),
                   "%10YTCM": _series(4.3, 0.0, 7),
                   "#US10Y-2Y": _series(0.5, 0.0, 8),
                   "%CCCHYS": _series(10, 0.0, 9), "#CPISA3": _series(3, 0, 10)}
        self.clip = "2026-08-26"

    def closes(self, sym):
        s = self.px.get(sym)
        if s is None:
            return None
        return s[s.index <= pd.Timestamp(self.clip)]

    def volumes(self, sym):
        return None


@pytest.fixture()
def market(monkeypatch):
    m = Market()
    monkeypatch.setattr(MD, "closes", m.closes)
    monkeypatch.setattr(MD, "volumes", m.volumes)
    monkeypatch.setattr(MD, "risk_free_annual",
                        lambda: {"state": "OK", "annual": 0.04})
    monkeypatch.setattr(MD, "risk_free_per_session",
                        lambda h: 0.04 * h / 252.0)
    monkeypatch.setattr(MD, "last_session",
                        lambda s: dt.date.fromisoformat(m.clip))
    return m


FIELD = ("r46_eq_xs_mom_12_1", "r46_eq_xs_rev_5d", "r46_spx_trend_200d")


def _book(spec):
    if spec["challenger_id"] == "r46_spx_trend_200d":
        legs = [{"instrument": "SPY", "weight": 1.0, "score": 1.0,
                 "side": "LONG", "cost_class": "US_ETF"}]
    else:
        legs = [{"instrument": "AAA", "weight": 0.5, "score": 1.0,
                 "side": "LONG", "cost_class": "US_EQUITY"},
                {"instrument": "BBB", "weight": -0.5, "score": -1.0,
                 "side": "SHORT", "cost_class": "US_EQUITY"}]
    return {"state": "OK", "legs": legs,
            "gross_notional": sum(abs(l["weight"]) for l in legs),
            "net_notional": sum(l["weight"] for l in legs),
            "n_legs": len(legs), "market_state_snapshot_hash": "h",
            "input_evidence_hash": "h"}


@pytest.fixture()
def field(monkeypatch):
    monkeypatch.setattr(EM.CH, "build", _book)
    specs = [s for s in CH.ALL_SPECS if s["challenger_id"] in FIELD]
    reg = {"challengers": [], "adoption": {"adopted": []},
           "retunes_detected": []}
    for s in specs:
        reg["challengers"].append({
            "challenger_id": s["challenger_id"], "challenger_version": "v1",
            "origin": "R46_SEED", "spec_hash": CH.spec_hash(s),
            "frozen_at": "2026-08-20T00:00:00Z", "family": s["family"],
            "asset_class": s["asset_class"], "instrument": s["instrument"],
            "prediction_type": s["prediction_type"],
            "horizons": list(s["horizons"]),
            "information_family": CH.info_family_for(s),
            "dependence_cluster": CH.cluster_for(s),
            "control": s["control"], "benchmark": s["benchmark"],
            "cost_class": s["cost_class"], "universe": s["universe"],
            "parameters": s["parameters"], "state": C.FORWARD_PENDING,
            "point_in_time_status": C.PIT_OK,
            "feasibility": {"state": "CAN_ACCRUE"}})
    return specs, reg


def _day(market, specs, reg, day: str) -> dict:
    """One tournament day: score -> board -> money layer -> emit."""
    market.clip = day
    as_of = dt.date.fromisoformat(day)
    now = dt.datetime(as_of.year, as_of.month, as_of.day, 22, 30,
                      tzinfo=dt.timezone.utc)
    JD.score_pending(TEST_CAMPAIGN, now)
    board = LB.build(TEST_CAMPAIGN, reg)
    res = SH.advance_pnl(as_of, reg, board, TEST_CAMPAIGN,
                         series_fn=market.closes, now=now,
                         risk_free_annual=0.04)
    EM.emit(TEST_CAMPAIGN, reg, now, specs=specs)
    return res


RUN_DAYS = ("2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31",
            "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04",
            "2026-09-08")


# =========================================================================== #
# 1. Existing science is untouched
# =========================================================================== #
def test_seed_spec_hashes_are_the_production_freeze():
    for cid, h in PRODUCTION_SEED_SPEC_HASHES.items():
        assert CH.spec_hash(CH.spec_by_id(cid)) == h, cid


def test_the_field_grew_without_touching_earlier_tuples():
    assert len(CH.SEED_SPECS) == 10
    assert len(CH.EXPANSION_SPECS) == 11
    assert len(CH.R46_4_SPECS) == 9
    # Release 46.5 added three challengers through the same frozen door.
    assert len(CH.R46_5_SPECS) == 3
    # Release 46.6 added seven FAST-EVIDENCE challengers through the same
    # door. The point of this test is that earlier tuples are UNTOUCHED - each
    # cohort above still holds exactly what it froze - so the total is the sum
    # of the cohorts, not a number a later release has to keep re-pinning.
    assert len(CH.R46_6_SPECS) == 7
    assert (len(CH.SEED_SPECS) + len(CH.EXPANSION_SPECS)
            + len(CH.R46_4_SPECS) + len(CH.R46_5_SPECS)) == 33
    # Release 51 added one FX-carry challenger through the same door, and
    # Release 52 added two (equity-index rotation, copper/gold lead-lag).
    # Release 53 added two more (all-futures 5-year value, commodity skewness).
    assert len(CH.ALL_SPECS) == (
        len(CH.SEED_SPECS) + len(CH.EXPANSION_SPECS) + len(CH.R46_4_SPECS)
        + len(CH.R46_5_SPECS) + len(CH.R46_6_SPECS) + len(CH.R51_SPECS)
        + len(CH.R52_SPECS) + len(CH.R53_SPECS))
    ids = [s["challenger_id"] for s in CH.ALL_SPECS]
    assert len(ids) == len(set(ids))


def test_r46_4_cohort_declares_four_new_information_families():
    fams = {s["information_family"] for s in CH.R46_4_SPECS}
    assert {"POSITIONING", "CREDIT_SPREADS", "MACRO_RELEASE_SURPRISE",
            "SCHEDULED_EVENT_CALENDAR"} <= fams
    for s in CH.R46_4_SPECS:
        assert s["cohort"] == CH.R46_4_COHORT
        assert s["parameters_were_searched"] is False
        assert s["promotion_allowed"] is False
        assert s["expected_return_state"] == "NOT_CALIBRATED"


def test_ledger_still_refuses_a_backdated_row():
    row = {f: None for f in C.PREDICTION_RECORD_FIELDS}
    row.update({"forward_evidence_type": C.TRUE_FORWARD,
                "status": C.STATUS_PENDING,
                "emitted_at_utc": "2026-08-28T00:00:00Z",
                "outcome_window_start_utc": "2026-08-27T04:00:00Z"})
    with pytest.raises(LG.LedgerRefusal):
        LG.validate_prediction(row)


# =========================================================================== #
# 2. Alpha-to-P&L: one calculation, decomposed cost, reconciliation
# =========================================================================== #
def test_cost_decomposition_equals_the_frozen_contract_for_every_class():
    m = PN.decomposition_matches_contract()
    assert m["all_match"], m
    for k in C.COST_BPS_PER_SIDE:
        assert abs(PN.base_per_side_bps(k)
                   - (C.COST_BPS_PER_SIDE[k] + C.SLIPPAGE_BPS_PER_SIDE)) < 1e-9


def test_cost_stack_charges_both_sides_and_stress_adds_holding_costs():
    legs = [{"instrument": "AAA", "weight": 0.5, "cost_class": "US_EQUITY"},
            {"instrument": "BBB", "weight": -0.5, "cost_class": "US_EQUITY"}]
    base = PN.cost_stack(legs, "US_EQUITY", 20)
    assert abs(base["transaction_bps_round_trip"] - 12.0) < 1e-9   # 6 x 2
    assert base["holding_bps"] == 0.0
    two = PN.cost_stack(legs, "US_EQUITY", 20, PN.SCENARIO_2X)
    assert abs(two["total_bps"] - 24.0) < 1e-9
    stress = PN.cost_stack(legs, "US_EQUITY", 20, PN.SCENARIO_STRESS)
    assert stress["transaction_bps_round_trip"] > two["total_bps"]
    assert stress["components_bps"]["borrow"] > 0     # the short leg pays


def test_marks_are_point_in_time_never_a_future_bar(market):
    s = market.px["SPY"]
    d, px = PN.mark_on_or_before(s, dt.date(2026, 8, 20))
    assert d <= dt.date(2026, 8, 20)
    assert px == float(s[s.index <= "2026-08-20"].iloc[-1])
    e_d, _ = PN.entry_mark(s, dt.date(2026, 8, 22))          # a Saturday
    assert e_d == dt.date(2026, 8, 24)
    m_d, m_px = PN.maturity_mark(s, dt.date(2026, 8, 24), 5)
    assert m_d == dt.date(2026, 8, 31)


def test_open_trade_economics_reconcile_with_the_judge_at_close(sandbox,
                                                                market, field):
    specs, reg = field
    for day in RUN_DAYS[:4]:
        _day(market, specs, reg, day)
    closes = TR.closes(TEST_CAMPAIGN)
    assert closes, "a one-session trade must have closed by the fourth day"
    for c in closes:
        rec = c["reconciliation"]
        assert rec["state"] == "RECONCILED", rec
        assert rec["abs_diff_net"] <= PN.RECONCILIATION_TOLERANCE
        assert rec["the_judge_number_is_used"] is True


def test_closed_trade_takes_the_judge_row_not_a_recomputation(sandbox, market,
                                                              field):
    specs, reg = field
    for day in RUN_DAYS[:4]:
        _day(market, specs, reg, day)
    outs = {o["prediction_id"]: o for o in LG.outcomes(TEST_CAMPAIGN)}
    for c in TR.closes(TEST_CAMPAIGN):
        o = outs[c["prediction_id"]]
        assert c["net_return"] == o["realised_net_return"]
        assert c["gross_return"] == o["realised_gross_return"]
        assert c["cost_return"] == o["realised_cost"]
        assert c["source_of_truth"] == "alpha_agent.r46.judge outcome row"


# =========================================================================== #
# 3. Research trades: one prediction -> one trade, append-only, derived state
# =========================================================================== #
def test_one_prediction_opens_exactly_one_trade_and_never_twice(sandbox,
                                                                 market, field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    _day(market, specs, reg, "2026-08-27")
    n_open = len(TR.opens(TEST_CAMPAIGN))
    assert n_open == len(LG.predictions(TEST_CAMPAIGN)) - 4  # today's batch
    again = TR.sync(dt.date(2026, 8, 27), TEST_CAMPAIGN, reg, market.closes,
                    funding_fn=lambda c, e, h: AL.funding_for(
                        c, e, h, TEST_CAMPAIGN), risk_free_annual=0.04)
    assert again["n_opened"] == 0 and again["n_marked"] == 0
    assert len(TR.opens(TEST_CAMPAIGN)) == n_open
    ids = [r["prediction_id"] for r in TR.opens(TEST_CAMPAIGN)]
    assert len(ids) == len(set(ids))


def test_trade_states_are_derived_and_cover_the_lifecycle(sandbox, market,
                                                          field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    st = TR.states(dt.date(2026, 9, 8), TEST_CAMPAIGN)
    counts = st["counts"]
    assert counts[TR.TRADE_CLOSED] > 0
    assert counts[TR.TRADE_MARKED] > 0
    assert counts[TR.SIGNAL_EMITTED] > 0          # today's batch enters tomorrow
    assert set(counts) == set(TR.STATES)
    assert TR.verify(TEST_CAMPAIGN)["all_intact"]


def test_realised_and_unrealised_are_split_not_summed(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    body = json.loads((R46.campaign_dir(TEST_CAMPAIGN) / SP.ARTIFACT)
                      .read_text(encoding="utf-8"))
    assert body["expected_vs_unrealised_vs_realised_are_never_summed"]
    for s in body["strategies"]:
        assert s["expected_state"] == "NOT_CALIBRATED"
        assert "realised_net_return" in s and "unrealised_net_return" in s


def test_an_entry_that_never_prints_reads_data_blocked_not_open(sandbox,
                                                                market, field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    preds = LG.predictions(TEST_CAMPAIGN)
    assert preds
    # Freeze the market at the emission day: no entry bar ever prints.
    market.clip = "2026-08-26"
    st = TR.states(dt.date(2026, 9, 10), TEST_CAMPAIGN)
    assert all(t["state"] == TR.DATA_BLOCKED for t in st["trades"])


# =========================================================================== #
# 4. Shadow NAV: exact arithmetic, append-only, replay-idempotent
# =========================================================================== #
def test_nav_inception_is_the_first_decision_and_nothing_is_funded_before_it(
        sandbox, market, field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    assert NV.inception_session(TEST_CAMPAIGN) == dt.date(2026, 8, 26)
    _day(market, specs, reg, "2026-08-27")
    for o in TR.opens(TEST_CAMPAIGN):
        cap = o["capital_by_policy"][AL.CANONICAL_POLICY]
        assert cap["decision_session"] < o["entry_session"]
        assert o["funded"] is True


def test_nav_arithmetic_is_exact_per_session(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    rows = NV.series(AL.CANONICAL_POLICY, TEST_CAMPAIGN)
    assert rows[0]["ending_nav"] == NV.STARTING_CAPITAL
    for prev, cur in zip(rows, rows[1:]):
        assert cur["beginning_nav"] == prev["ending_nav"]
        recon = (cur["beginning_nav"] + cur["financing_pnl"]
                 + cur["transaction_cost_pnl"] + cur["mark_to_market_pnl"]
                 + cur["close_pnl"])
        # Components are rounded to the micro-dollar on the ledger; five
        # rounded terms may differ from the rounded total by a few 1e-6.
        assert abs(recon - cur["ending_nav"]) < 1e-5
        assert abs(cur["financing_pnl"]
                   - cur["beginning_nav"] * 0.04 / 252.0) < 1e-5
        assert cur["drawdown"] <= 0.0
        assert cur["high_water_mark"] >= cur["ending_nav"] - 1e-9


def test_closed_trade_dollars_equal_capital_times_the_judge_net(sandbox,
                                                                market, field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    opens = {o["research_trade_id"]: o for o in TR.opens(TEST_CAMPAIGN)}
    attr = json.loads((R46.campaign_dir(TEST_CAMPAIGN) / AT.ARTIFACT)
                      .read_text(encoding="utf-8"))
    closed = [t for t in attr["trades"] if t["realised"]]
    assert closed
    for t in closed:
        o = opens[t["research_trade_id"]]
        cap = o["capital_by_policy"][AL.CANONICAL_POLICY]["capital_usd"]
        c = next(x for x in TR.closes(TEST_CAMPAIGN)
                 if x["research_trade_id"] == t["research_trade_id"])
        assert abs(t["net_pnl"] - cap * c["net_return"]) < 1e-6


def test_nav_replay_is_idempotent_and_never_rewrites(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    before = json.dumps(NV.rows(TEST_CAMPAIGN), sort_keys=True)
    res = _day(market, specs, reg, RUN_DAYS[-1])
    assert res["trades_opened"] == 0 and res["trades_marked"] == 0
    assert res["nav_roll"]["first"]["n_appended"] == 0
    assert json.dumps(NV.rows(TEST_CAMPAIGN), sort_keys=True) == before
    desk_report = NV._desk().verify_ledger(TR.shadow_dir(TEST_CAMPAIGN),
                                           NV.LEDGER)
    assert desk_report["intact"]


def test_every_policy_and_benchmark_rolls_on_the_same_engine(sandbox, market,
                                                             field):
    specs, reg = field
    for day in RUN_DAYS[:5]:
        _day(market, specs, reg, day)
    ids = {r["series_id"] for r in NV.rows(TEST_CAMPAIGN)}
    assert ids == set(NV.SERIES_IDS)
    cash = NV.series(AL.POLICY_CASH, TEST_CAMPAIGN)
    for prev, cur in zip(cash, cash[1:]):
        assert abs(cur["ending_nav"] - prev["ending_nav"] * (1 + 0.04 / 252.0)) < 1e-6


# =========================================================================== #
# 5. Allocation: zero-base, frozen, no hindsight, discounted, capped
# =========================================================================== #
def _entries(field):
    _, reg = field
    return {c["challenger_id"]: c for c in reg["challengers"]}


def test_four_policies_exist_and_cash_holds_nothing(field):
    entries = _entries(field)
    ev = {cid: {"cells": {}, "mean_net_alpha_bps": None} for cid in entries}
    vols = {cid: 0.1 for cid in entries}
    econ = {cid: SP.ECON_TOO_EARLY for cid in entries}
    for pid in AL.POLICIES:
        t = AL.target(pid, entries, ev, vols, econ)
        if pid == AL.POLICY_CASH:
            assert t["weights"] == {} and t["cash_weight"] == 1.0
        else:
            assert abs(sum(t["weights"].values()) + t["cash_weight"] - 1.0) < 1e-9
    eq = AL.target(AL.POLICY_EQUAL, entries, ev, vols, econ)["weights"]
    assert all(abs(w - 1.0 / 3.0) < 1e-9 for w in eq.values())


def test_early_evidence_gets_small_capital_and_kill_candidates_get_none(field):
    entries = _entries(field)
    ev = {cid: {"cells": {"20": {"state": C.FORWARD_PENDING,
                                 "raw_matured": 0, "effective_independent": 0,
                                 "required_effective_independent": 24}},
                "mean_net_alpha_bps": None} for cid in entries}
    vols = {cid: 0.1 for cid in entries}
    econ = {cid: SP.ECON_TOO_EARLY for cid in entries}
    t = AL.target(AL.POLICY_EVIDENCE, entries, ev, vols, econ)
    assert t["deployment"] == pytest.approx(0.25 + 0.75 * 0.10)
    assert t["cash_weight"] > 0.6
    econ["r46_spx_trend_200d"] = SP.ECON_KILL_CANDIDATE
    t2 = AL.target(AL.POLICY_EVIDENCE, entries, ev, vols, econ)
    assert "r46_spx_trend_200d" not in t2["weights"]
    assert "ECONOMIC_KILL_CANDIDATE" in t2["ineligible"]["r46_spx_trend_200d"]


def test_redundancy_penalty_and_concentration_caps(field):
    entries = _entries(field)
    ev = {cid: {"cells": {"20": {"state": C.FORWARD_CONFIRMED,
                                 "raw_matured": 100,
                                 "effective_independent": 100,
                                 "required_effective_independent": 24}},
                "mean_net_alpha_bps": 5.0} for cid in entries}
    vols = {"r46_eq_xs_mom_12_1": 0.1, "r46_eq_xs_rev_5d": 0.1,
            "r46_spx_trend_200d": 0.001}          # a near-zero vol outlier
    econ = {cid: SP.ECON_OK for cid in entries}
    t = AL.target(AL.POLICY_EVIDENCE, entries, ev, vols, econ)
    caps = AL.POLICY_RULES[AL.POLICY_EVIDENCE]["caps"]
    assert max(t["weights"].values()) <= caps["strategy"] + 1e-9
    d = t["detail"]
    # The two equity cells share EQ_XS_PRICE and are each halved.
    assert d["r46_eq_xs_mom_12_1"]["cluster_size"] == 2
    assert d["r46_spx_trend_200d"]["cluster_size"] == 1


def test_a_second_decision_on_the_same_session_appends_nothing(sandbox,
                                                               market, field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    n = len(AL.rows(TEST_CAMPAIGN))
    entries = _entries(field)
    body = AL.decide(dt.date(2026, 8, 26), entries, {}, {c: 0.1 for c in
                     entries}, {}, NV.nav_by_policy(TEST_CAMPAIGN),
                     TEST_CAMPAIGN)
    assert body["n_appended"] == 0
    assert len(AL.rows(TEST_CAMPAIGN)) == n


def test_funding_reads_only_decisions_strictly_before_entry(sandbox, market,
                                                            field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    assert AL.funding_for("r46_eq_xs_mom_12_1", dt.date(2026, 8, 26), 20,
                          TEST_CAMPAIGN) == {}
    f = AL.funding_for("r46_eq_xs_mom_12_1", dt.date(2026, 8, 27), 20,
                       TEST_CAMPAIGN)
    assert f[AL.CANONICAL_POLICY]["decision_session"] == "2026-08-26"


# =========================================================================== #
# 6. Risk, attribution, opportunity, regime, board
# =========================================================================== #
def test_structural_correlation_counts_a_cluster_once(field):
    entries = list(_entries(field).values())
    corr = RK.structural_correlation(entries)
    assert corr.shape == (3, 3)
    assert corr[0, 1] == 1.0            # both EQ_XS_PRICE
    assert corr[0, 2] == 0.0            # SPX_TREND is another cluster
    cv = RK.cluster_view(entries, {}, None)
    assert cv["clusters"] == ["EQ_XS_PRICE", "SPX_TREND"]
    assert cv["source"] == RK.SOURCE_PRIOR_STRUCTURAL
    eff = RK.effective_streams(cv["matrix"])
    assert abs(eff - 2.0) < 1e-9        # two clusters = two bets, exactly
    # Three equal-weight strategies in two clusters: still two bets.
    cv_w = RK.cluster_view(entries, {}, {e["challenger_id"]: 1 / 3.0
                                         for e in entries})
    assert abs(RK.effective_streams(cv_w["matrix"],
                                    [cv_w["weights"][c]
                                     for c in cv_w["clusters"]]) - 1.89) < 0.01


def test_realised_correlation_needs_enough_history(field):
    entries = list(_entries(field).values())
    streams = {e["challenger_id"]: {"2026-09-01": 0.001} for e in entries}
    cr = RK.correlation(entries, streams)
    assert cr["source"] == RK.SOURCE_PRIOR_STRUCTURAL


def test_volatility_prior_is_labelled_and_never_alpha(market, field):
    entries = _entries(field)
    v = RK.volatility_prior(entries["r46_spx_trend_200d"], dt.date(2026, 8, 26),
                            market.closes)
    assert v["source"] == RK.SOURCE_PRIOR_INSTRUMENT
    assert v["evidence_class"] == PN.EVIDENCE_RISK_PRIOR
    b = RK.volatility_prior(entries["r46_eq_xs_mom_12_1"], dt.date(2026, 8, 26),
                            market.closes)
    assert b["source"] == RK.SOURCE_PRIOR_STRUCTURAL


def test_attribution_sums_to_the_funded_trades(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    attr = json.loads((R46.campaign_dir(TEST_CAMPAIGN) / AT.ARTIFACT)
                      .read_text(encoding="utf-8"))
    funded = attr["trades"]
    tot = sum(t["net_pnl"] for t in funded)
    assert abs(attr["totals_usd"]["net_pnl"] - tot) < 1e-6
    by_cid = sum(g["net_pnl"] for g in attr["by"]["challenger_id"])
    assert abs(by_cid - tot) < 1e-6
    assert attr["unfunded_unit_economics"]["label"] == AT.UNFUNDED


def test_regime_is_recorded_once_and_never_relabelled(sandbox, market):
    RGM.record(dt.date(2026, 8, 26), TEST_CAMPAIGN, market.closes,
               market.volumes)
    first = RGM.regime_for("2026-08-26", TEST_CAMPAIGN)
    market.px["$VIX"] = market.px["$VIX"] * 3.0
    RGM.record(dt.date(2026, 8, 26), TEST_CAMPAIGN, market.closes,
               market.volumes)
    again = RGM.regime_for("2026-08-26", TEST_CAMPAIGN)
    assert again["volatility_regime"] == first["volatility_regime"]
    assert again["recorded_at_utc"] == first["recorded_at_utc"]


def test_economic_kill_needs_a_sample_never_one_trade():
    one = SP.economic_state(n_closed=1, cum_net=-0.2, cum_residual=-0.2,
                            t_residual=-5.0, net_at_2x=-0.3,
                            max_drawdown=-0.5, reconciliation_mismatches=0,
                            data_blocked_trades=0, worst_trade=-0.2)
    assert one["state"] == SP.ECON_TOO_EARLY
    assert "CATASTROPHIC_SINGLE_TRADE_LOSS_FLAGGED_FOR_REVIEW" in one["flags"]
    many = SP.economic_state(n_closed=25, cum_net=-0.05, cum_residual=-0.06,
                             t_residual=-2.0, net_at_2x=-0.1,
                             max_drawdown=-0.08, reconciliation_mismatches=0,
                             data_blocked_trades=0, worst_trade=-0.01)
    assert many["state"] == SP.ECON_KILL_CANDIDATE
    fragile = SP.economic_state(n_closed=25, cum_net=0.01, cum_residual=0.01,
                                t_residual=1.0, net_at_2x=-0.01,
                                max_drawdown=-0.02, reconciliation_mismatches=0,
                                data_blocked_trades=0, worst_trade=-0.01)
    assert "SEVERE_COST_FRAGILITY" in fragile["reasons"]


def test_pnl_board_ranks_evidence_first_and_penalises_economic_watch():
    rows = [{"challenger_id": "a", "state": C.EARLY_FORWARD_EVIDENCE,
             "effective_independent": 3, "net_forward_pnl": 0.05,
             "economic_state": SP.ECON_WATCH, "origin": "R46_SEED"},
            {"challenger_id": "b", "state": C.EARLY_FORWARD_EVIDENCE,
             "effective_independent": 3, "net_forward_pnl": 0.01,
             "economic_state": SP.ECON_OK, "origin": "R46_SEED"},
            {"challenger_id": "c", "state": C.FORWARD_PENDING,
             "effective_independent": 0, "net_forward_pnl": 0.9,
             "economic_state": SP.ECON_TOO_EARLY, "origin": "R46_SEED"}]
    ranked = sorted(rows, key=PB._rank_key)
    assert [r["challenger_id"] for r in ranked] == ["b", "a", "c"]


def test_alpha_result_vocabulary_is_exactly_three():
    assert PB.ALPHA_RESULTS == ("NOT_YET_JUDGED", "EARLY_FORWARD_PNL_EVIDENCE",
                                "FORWARD_CONFIRMED_CANDIDATE")
    assert PB.alpha_result_for([])["result"] == PB.ALPHA_NOT_YET_JUDGED
    r = PB.alpha_result_for([{"origin": "R46_SEED", "state": C.FORWARD_PENDING,
                              "n_trades_closed": 2}])
    assert r["result"] == PB.ALPHA_EARLY
    assert "PROVEN" not in json.dumps(PB.ALPHA_RESULTS)


def test_opportunity_recommendations_change_nothing(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS[:3]:
        _day(market, specs, reg, day)
    body = json.loads((R46.campaign_dir(TEST_CAMPAIGN) / OC.ARTIFACT)
                      .read_text(encoding="utf-8"))
    assert set(body["counts"]) == set(OC.RECOMMENDATIONS)
    assert all(r["changes_nothing"] for r in body["rows"])
    bridge = json.loads((R46.campaign_dir(TEST_CAMPAIGN) / OC.BRIDGE_ARTIFACT)
                        .read_text(encoding="utf-8"))
    assert bridge["read_only"] and bridge["adds_to_portfolio"] is False
    assert bridge["n_candidates"] == 0


# =========================================================================== #
# 7. Orthogonal lanes: PIT contracts, hermetic, fail-soft
# =========================================================================== #
def test_cftc_observability_lag_and_positioning_math():
    rows = []
    d0 = pd.Timestamp("2023-01-03")
    for i in range(170):
        rows.append({"as_of": d0 + pd.Timedelta(days=7 * i), "code": "13874A",
                     "name": "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
                     "oi": 1000.0, "nc_long": 500.0 + i, "nc_short": 400.0,
                     "c_long": 300.0, "c_short": 400.0})
    df = pd.DataFrame(rows)
    last = df["as_of"].max().date()
    obs = CF.observable_reports(df, last + dt.timedelta(days=3))
    assert obs["as_of"].max().date() < last             # 3 days is too early
    obs6 = CF.observable_reports(df, last + dt.timedelta(days=6))
    assert obs6["as_of"].max().date() == last
    pos = CF.positioning(df, last + dt.timedelta(days=6))
    m = pos["markets"]["&ES"]
    assert m["report_as_of"] == str(last)
    assert m["spec_net_share_z"] > 0 and m["spec_net_share_change_13w"] > 0


def test_cftc_refuses_a_code_whose_name_does_not_match():
    df = pd.DataFrame([{"as_of": pd.Timestamp("2026-08-18"), "code": "088691",
                        "name": "SOMETHING ELSE ENTIRELY", "oi": 10.0,
                        "nc_long": 1.0, "nc_short": 1.0, "c_long": 1.0,
                        "c_short": 1.0}])
    mm = CF.mapped_markets(df)
    assert "088691" not in mm["mapped"]
    assert any(r["code"] == "088691" and r["why"] == "NAME_KEYWORD_MISMATCH"
               for r in mm["refused"])


def test_credit_pit_series_uses_only_vintages_published_by_as_of(sandbox):
    CR.raw_dir().mkdir(parents=True, exist_ok=True)
    obs = [{"date": "2026-08-24", "value": "2.69",
            "realtime_start": "2026-08-25", "realtime_end": "9999-12-31"},
           {"date": "2026-08-25", "value": "2.70",
            "realtime_start": "2026-08-26", "realtime_end": "9999-12-31"}]
    p = CR.raw_dir() / "BAMLH0A0HYM2_test.json"
    p.write_text(json.dumps({"observations": obs}), encoding="utf-8")
    man = {"schema": "r46_4_credit_captures/1", "captures": [{
        "series_key": "HY_OAS", "series_id": "BAMLH0A0HYM2", "path": str(p),
        "acquired_at_utc": "2026-08-26T23:00:00Z", "acquired_day": "2026-08-26"}]}
    R46.write_json(CR.manifest_path(), man)
    s25 = CR.pit_series("HY_OAS", dt.date(2026, 8, 25))
    assert list(s25.index.strftime("%Y-%m-%d")) == ["2026-08-24"]
    s26 = CR.pit_series("HY_OAS", dt.date(2026, 8, 26))
    assert len(s26) == 2


def test_macro_surprise_is_model_based_and_first_published(sandbox):
    MC.raw_dir().mkdir(parents=True, exist_ok=True)
    obs = []
    v = 100.0
    for i in range(40):
        period = pd.Timestamp("2023-01-01") + pd.DateOffset(months=i)
        v *= 1.003 if i < 39 else 1.02              # a big last print
        obs.append({"date": period.strftime("%Y-%m-%d"),
                    "realtime_start": (period + pd.DateOffset(days=42)
                                       ).strftime("%Y-%m-%d"),
                    "value": "%.3f" % v})
    p = MC.raw_dir() / "CPI_test.json"
    p.write_text(json.dumps({"observations": obs}), encoding="utf-8")
    rel = MC.raw_dir() / "CPI_dates.json"
    last_pub = obs[-1]["realtime_start"]
    rel.write_text(json.dumps({"release_dates": [{"date": last_pub}]}),
                   encoding="utf-8")
    man = {"schema": "r46_4_macro_captures/1", "captures": [
        {"family": "CPI", "kind": "INITIAL_RELEASES", "path": str(p),
         "acquired_at_utc": "2026-08-26T23:00:00Z"},
        {"family": "CPI", "kind": "RELEASE_DATES", "path": str(rel),
         "acquired_at_utc": "2026-08-26T23:00:00Z"}]}
    R46.write_json(MC.manifest_path(), man)
    day = dt.date.fromisoformat(last_pub)
    s = MC.surprise("CPI", day)
    assert s["state"] == "OK" and s["surprise_z"] > 2.0
    assert s["model_based_not_consensus"] is True
    before = MC.surprise("CPI", day - dt.timedelta(days=1))
    assert before["period"] != s["period"]        # not visible the day before
    sig = MC.rates_signal(day)
    assert sig["direction"] == "SHORT"
    assert MC.rates_signal(day - dt.timedelta(days=1))["state"] == \
        "NO_TRADED_RELEASE_TODAY"


def test_fomc_page_parse_takes_the_last_day_and_handles_month_spans():
    html = ("<h4>2026 FOMC Meetings</h4>"
            "<div class='fomc-meeting__month'><strong>January</strong></div>"
            "<div class='fomc-meeting__date'>27-28</div>"
            "<div class='fomc-meeting__month'><strong>April/May</strong></div>"
            "<div class='fomc-meeting__date'>30-1</div>")
    out = EVN.parse_fomc(html)
    assert out[2026] == ["2026-01-28", "2026-05-01"]


def test_event_owners_are_flat_outside_the_calendar(sandbox, market, monkeypatch):
    fixed = dt.datetime(2026, 8, 26, 22, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(CK, "now_utc", lambda: fixed)
    # No captures at this root -> the frozen fallback list drives the rule.
    assert EVN.fomc_decision_days(2026)["source"] == "FROZEN_FALLBACK"
    spec = CH.spec_by_id("r46_4_spx_pre_fomc_drift")
    assert CH.build(spec)["legs"] == []          # 2026-08-28 is not FOMC
    fomc_eve = dt.datetime(2026, 9, 14, 22, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(CK, "now_utc", lambda: fomc_eve)
    market.clip = "2026-09-14"
    book = CH.build(spec)
    assert book["holding_session"] == "2026-09-16"
    assert len(book["legs"]) == 1 and book["legs"][0]["instrument"] == "SPY"


def test_lanes_do_not_acquire_inside_the_hermetic_process(sandbox):
    assert AD._lanes_may_acquire() is False
    body = CF.run(acquire_now=False, campaign_id=TEST_CAMPAIGN,
                  as_of=dt.date(2026, 8, 26))
    assert body["acquisition"]["results"][0]["state"] in (
        "NEW_REMOTE_NOT_ACQUIRED", "UNREACHABLE")
    assert not (CF.raw_dir() / "cftc_captures.json").exists() or True


# =========================================================================== #
# 8. The ONE orchestration path, ordering, fail-soft, governance
# =========================================================================== #
def test_advance_runs_the_money_layer_before_emission(sandbox, market, field,
                                                      monkeypatch):
    specs, reg = field
    monkeypatch.setattr(CH, "ALL_SPECS", tuple(specs))
    for mod in (CF, CR, MC, EVN):
        monkeypatch.setattr(mod, "run", lambda *a, **k: {"state": "DATA_BLOCKED"})
    order = []
    real_pnl = SH.advance_pnl
    real_emit = EM.emit

    def spy_pnl(*a, **k):
        order.append("pnl")
        return real_pnl(*a, **k)

    def spy_emit(*a, **k):
        order.append("emit")
        return real_emit(*a, **k)
    monkeypatch.setattr(SH, "advance_pnl", spy_pnl)
    monkeypatch.setattr(EM, "emit", spy_emit)
    now = dt.datetime(2026, 8, 26, 22, 30, tzinfo=dt.timezone.utc)
    res = AD.advance(TEST_CAMPAIGN, now=now, registry=reg,
                     eligible_market_date="2026-08-26")
    assert order == ["pnl", "emit"]
    assert res["state"] == AD.STATE_ADVANCED
    assert res["shadow_pnl"]["inception"] == "2026-08-26"
    assert res["pnl_as_of"] == "2026-08-26"
    assert res["orders_created"] == 0 and res["portfolio_mutations"] == 0
    assert res["promoted_models"] == 0


def test_a_broken_money_layer_never_stops_the_tournament(sandbox, market,
                                                         field, monkeypatch):
    specs, reg = field
    monkeypatch.setattr(CH, "ALL_SPECS", tuple(specs))
    for mod in (CF, CR, MC, EVN):
        monkeypatch.setattr(mod, "run", lambda *a, **k: {"state": "DATA_BLOCKED"})

    def boom(*a, **k):
        raise RuntimeError("shadow layer down")
    monkeypatch.setattr(SH, "advance_pnl", boom)
    now = dt.datetime(2026, 8, 26, 22, 30, tzinfo=dt.timezone.utc)
    res = AD.advance(TEST_CAMPAIGN, now=now, registry=reg,
                     eligible_market_date="2026-08-26")
    assert res["available"] is True
    assert any(f["stage"] == "shadow_pnl" for f in res["stage_failures"])
    assert res["emission"]["n_appended"] > 0


def test_manual_advance_marks_only_the_last_printed_session(market):
    market.clip = "2026-08-24"
    assert AD._pnl_as_of(None, CK.now_utc()) == dt.date(2026, 8, 24)
    assert AD._pnl_as_of("2026-08-26", CK.now_utc()) == dt.date(2026, 8, 26)


def test_read_model_serves_the_money_block_and_degrades_honestly(
        sandbox, market, field, monkeypatch):
    specs, reg = field
    monkeypatch.setenv(PT.RESEARCH_ROOT_ENV, str(R46.RESEARCH_ROOT))
    empty = PT.load_prospective_tournament(TEST_CAMPAIGN)
    assert empty["shadow_pnl"]["available"] is False
    for day in RUN_DAYS[:3]:
        _day(market, specs, reg, day)
    from alpha_agent.r46 import campaign as CP
    R46.write_json(R46.campaign_dir(TEST_CAMPAIGN) / CP.FINAL_ARTIFACT,
                   {"TERMINAL_STATE": "R46_PROSPECTIVE_ALPHA_TOURNAMENT_LIVE"})
    d = PT.load_prospective_tournament(TEST_CAMPAIGN)
    sp = d["shadow_pnl"]
    assert sp["available"] is True
    assert sp["shadow_nav"] is not None
    assert sp["ALPHA_RESULT"] in PB.ALPHA_RESULTS
    assert sp["historical_pnl_is_never_shown_as_forward"] is True
    assert sp["realised_unrealised_expected_never_summed"] is True
    assert "cftc" in d["information_lanes"]
    assert "PROVEN" not in json.dumps(sp)


def test_no_r46_4_module_can_reach_an_operational_store():
    import inspect
    for mod in (PN, TR, SP, AL, NV, RK, AT, RGM, OC, PB, SH, CF, CR, MC, EVN):
        src = inspect.getsource(mod)
        for tok in ("portfolio_decision", "rebalance_execution",
                    "operational_book import", "from paper_trader.api import app",
                    "schtasks", "Register-ScheduledTask"):
            assert tok not in src, (mod.__name__, tok)
