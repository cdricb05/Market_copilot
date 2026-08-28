"""Release 46.5 - the forward P&L harvest: matured economics, winner/loser
separation, the frozen realised-correlation blend, and the two free EDGAR
information lanes.

Every test here is hermetic: prices are synthetic, the research root is a
temp directory, no provider is reached and no production ledger is touched.

Claims locked shut:

**Matured economics and marks are different things.** The harvest owner
reports MATURED_FORWARD_EVIDENCE (closed trades, the judge's numbers) and
MARK_TO_MARKET (open trades at their point-in-time mark) separately, never
adds them, and proves every session that the judge, the trade close, the
strategy stream and the NAV's realised booking agree.

**A verdict reads matured trades only.** Thresholds are frozen before any
outcome existed; one outcome never decides; a mark can move a NAV but never a
verdict; a verdict confers no capital.

**The correlation blend was frozen before it was used.** The rule is
versioned, it supersedes v1 explicitly, the structural prior dominates with
little common data, and realised may become primary only from 40 common
sessions.

**The EDGAR lanes are acceptance-stamped and cannot look ahead.** An event is
admissible only if it was accepted before the read instant AND sits in a
capture acquired before it. The synthetic earnings fixture is refused by name.
Form-4 transaction codes are classified, and only open-market purchases and
sales are informative.

**A partial window does not emit.** A challenger whose declared window is not
completely covered by captures refuses, rather than reporting a systematically
undercounted signal as a weak one.
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
from alpha_agent.r46 import challengers as CH
from alpha_agent.r46 import clock as CK
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import cftc as CF
from alpha_agent.r46 import credit as CR
from alpha_agent.r46 import earnings as EA
from alpha_agent.r46 import emit as EM
from alpha_agent.r46 import events as EVN
from alpha_agent.r46 import form4 as FM
from alpha_agent.r46 import harvest as HV
from alpha_agent.r46 import judge as JD
from alpha_agent.r46 import leaderboard as LB
from alpha_agent.r46 import ledger as LG
from alpha_agent.r46 import macro as MC
from alpha_agent.r46 import marketdata as MD
from alpha_agent.r46 import nav as NV
from alpha_agent.r46 import pnl_board as PB
from alpha_agent.r46 import risk as RK
from alpha_agent.r46 import sec as SEC
from alpha_agent.r46 import shadow as SH
from alpha_agent.r46 import strategy_pnl as SP
from alpha_agent.r46 import trades as TR
from alpha_agent.r46 import verdicts as VD
from api import prospective_tournament as PT

TEST_CAMPAIGN = "r46_5_pytest_campaign"

#: The three Release-46.5 specifications as frozen 2026-08-27. If any moves,
#: a challenger was retuned in place rather than versioned.
R46_5_SPEC_HASHES = {
    "r46_5_pead_announcement_return_20d":
        "e5f68e193fc96ad17cbf064730403b58c38c050d42e79e10e15c713953b6e96d",
    "r46_5_insider_cluster_buy_20d":
        "9fc230a7b3c2c9f51b462cb38cecaaafc0dc9abf03b43951f0dd32da0aa90820",
    "r46_5_insider_net_purchase_xs_20d":
        "c28ad01313ba8360f7ed96ad2b3cf89ed9aefaf7ba87d8dbcea77ff454f1193b",
}


# =========================================================================== #
# Fixtures - a hermetic root, synthetic bars, a tiny frozen field
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
    def __init__(self):
        self.px = {"SPY": _series(500, 0.0005, 1), "TLT": _series(90, 0.0, 2),
                   "AAA": _series(50, 0.001, 3), "BBB": _series(30, -0.001, 4),
                   "$VIX": _series(18, 0.0, 5), "&ZN": _series(110, 0.0, 6)}
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
    monkeypatch.setattr(MD, "risk_free_per_session", lambda h: 0.04 * h / 252.0)
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
# 1. The field grew without touching a single earlier tuple
# =========================================================================== #
def test_r46_5_specs_are_frozen_at_their_published_hashes():
    for cid, h in R46_5_SPEC_HASHES.items():
        assert CH.spec_hash(CH.spec_by_id(cid)) == h, cid


def test_the_field_is_thirty_three_and_every_id_is_unique():
    """The R46.5 field is thirty-three and stays thirty-three.

    Release 46.6 added seven FAST-EVIDENCE challengers in their own cohort;
    the four cohorts this release froze are untouched, which is what this test
    exists to prove.
    """
    assert len(CH.SEED_SPECS) == 10
    assert len(CH.EXPANSION_SPECS) == 11
    assert len(CH.R46_4_SPECS) == 9
    assert len(CH.R46_5_SPECS) == 3
    assert (len(CH.SEED_SPECS) + len(CH.EXPANSION_SPECS)
            + len(CH.R46_4_SPECS) + len(CH.R46_5_SPECS)) == 33
    ids = [s["challenger_id"] for s in CH.ALL_SPECS]
    assert len(ids) == len(set(ids))


def test_r46_5_cohort_is_complete_unsearched_and_opens_two_families():
    fams = {s["information_family"] for s in CH.R46_5_SPECS}
    assert fams == {"EARNINGS_EVENTS", "INSIDER_FLOW"}
    for s in CH.R46_5_SPECS:
        assert s["cohort"] == CH.R46_5_COHORT
        assert s["challenger_version"] == "v1"
        assert s["parameters_were_searched"] is False
        assert s["promotion_allowed"] is False
        assert s["research_shadow_only"] is True
        assert s["expected_return_state"] == "NOT_CALIBRATED"
        assert s["signal_owner"] in CH._OWNERS
        assert set(s["horizons"]) <= set(C.HORIZONS)
        for f in ("thesis", "universe", "parameters", "control", "benchmark",
                  "cost_class", "dependence_cluster"):
            assert s[f], "%s missing %s" % (s["challenger_id"], f)


def test_every_r46_5_parameter_is_a_declared_constant():
    K = CH.R46_5_CANONICAL_CONSTANTS
    declared = {v for v in K.values() if isinstance(v, (int, float))}
    for s in CH.R46_5_SPECS:
        for k, v in s["parameters"].items():
            if isinstance(v, (int, float)):
                assert v in declared, "%s: %s=%r was not declared" % (
                    s["challenger_id"], k, v)


def test_the_two_insider_cells_share_one_dependence_cluster():
    assert CH.cluster_for(CH.spec_by_id("r46_5_insider_cluster_buy_20d")) == \
        CH.cluster_for(CH.spec_by_id("r46_5_insider_net_purchase_xs_20d")) == \
        "INSIDER_FLOW"
    assert CH.cluster_for(CH.spec_by_id(
        "r46_5_pead_announcement_return_20d")) == "EARNINGS_DRIFT"


# =========================================================================== #
# 2. The harvest: matured economics and marks are never one number
# =========================================================================== #
def test_harvest_reports_nothing_matured_before_anything_closes(sandbox,
                                                                market, field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    body = HV.build(dt.date(2026, 8, 26), TEST_CAMPAIGN)
    assert body["FORWARD_PNL_EVIDENCE"] == HV.EVIDENCE_STILL_WAITING
    assert body["matured"]["n_matured"] == 0
    assert body["matured"]["usd_funded"]["net"] == 0.0
    assert body["matured_and_mark_to_market_are_never_summed"] is True
    assert body["nothing_here_matures_a_prediction"] is True


def test_harvest_counts_only_closed_trades_as_matured(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    as_of = dt.date.fromisoformat(RUN_DAYS[-1])
    body = HV.build(as_of, TEST_CAMPAIGN)
    closes = [c for c in TR.closes(TEST_CAMPAIGN)
              if str(c["exit_session"]) <= str(as_of)]
    assert body["matured"]["n_matured"] == len(closes) > 0
    assert body["FORWARD_PNL_EVIDENCE"] in HV.EVIDENCE_STATES
    for r in body["matured"]["trades"]:
        assert r["evidence_class"] == HV.MATURED
        assert r["judge_matches_close"] is True
    for r in body["mark_to_market"]["trades"]:
        assert r["evidence_class"] == HV.MTM
        assert r["is_matured_evidence"] is False
    open_ids = {r["research_trade_id"] for r in body["mark_to_market"]["trades"]}
    closed_ids = {r["research_trade_id"] for r in body["matured"]["trades"]}
    assert not (open_ids & closed_ids), "a trade cannot be both"


def test_harvest_matured_takes_the_judge_number_never_a_recomputation(
        sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS[:5]:
        _day(market, specs, reg, day)
    as_of = dt.date.fromisoformat(RUN_DAYS[4])
    rows = HV.matured_trades(as_of, TEST_CAMPAIGN)
    outs = {o["prediction_id"]: o for o in LG.outcomes(TEST_CAMPAIGN)}
    assert rows
    for r in rows:
        o = outs[r["prediction_id"]]
        assert r["net_return"] == o["realised_net_return"]
        assert r["gross_return"] == o["realised_gross_return"]
        assert r["transaction_cost"] == o["realised_cost"]
        assert r["residual_alpha"] == o["net_alpha_vs_control"]


def test_harvest_reconciles_judge_close_stream_and_nav(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    as_of = dt.date.fromisoformat(RUN_DAYS[-1])
    body = HV.build(as_of, TEST_CAMPAIGN)
    rec = body["reconciliation"]
    assert rec["ONE_ECONOMIC_TRUTH"] is True, rec["problems"]
    assert rec["n_matured_checked"] > 0
    assert abs(rec["nav_realised_usd"] - rec["closed_trade_realised_usd"]) < 1e-4
    assert len(rec["checked"]) == 4


def test_harvest_reports_a_reconciliation_break_rather_than_absorbing_it(
        sandbox, market, field, monkeypatch):
    specs, reg = field
    for day in RUN_DAYS[:5]:
        _day(market, specs, reg, day)
    as_of = dt.date.fromisoformat(RUN_DAYS[4])
    real = HV.matured_trades

    def tampered(*a, **k):
        rows = real(*a, **k)
        if rows:
            rows[0] = dict(rows[0], judge_matches_close=False,
                           judge_net_return=rows[0]["net_return"] + 0.5)
        return rows
    monkeypatch.setattr(HV, "matured_trades", tampered)
    body = HV.build(as_of, TEST_CAMPAIGN, write=False)
    assert body["reconciliation"]["ONE_ECONOMIC_TRUTH"] is False
    assert any(p.get("where") == "judge_vs_close"
               for p in body["reconciliation"]["problems"])


def test_mark_to_market_is_conservative_and_carries_its_cost(sandbox, market,
                                                             field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    _day(market, specs, reg, "2026-08-27")
    rows = HV.open_marks(dt.date(2026, 8, 27), TEST_CAMPAIGN)
    assert rows
    for r in rows:
        assert r["cost_drag"] > 0
        assert abs(r["unrealised_net_return"]
                   - (r["unrealised_gross_return"] - r["cost_drag"])) < 1e-9
        assert r["current_drawdown_from_peak_net"] <= 0.0


def test_harvest_evidence_state_vocabulary_is_exactly_three():
    assert HV.EVIDENCE_STATES == ("STILL_WAITING_FOR_REALITY",
                                  "FIRST_MATURED_ECONOMICS",
                                  "MATURED_ECONOMICS_ACCRUING")
    assert HV.evidence_state(0) == HV.EVIDENCE_STILL_WAITING
    assert HV.evidence_state(1) == HV.EVIDENCE_FIRST
    assert HV.evidence_state(HV.FIRST_ECONOMICS_BELOW) == HV.EVIDENCE_ACCRUING


# =========================================================================== #
# 3. Verdicts: frozen, matured-only, and never a false winner
# =========================================================================== #
def _v(**kw):
    base = dict(n_closed=0, residual=0.0, t_residual=None, net_at_2x=0.0,
                max_drawdown=None, hit_rate=None,
                reconciliation_mismatches=0, marginal_diversification=0.1,
                tournament_states=set(), economic_state=SP.ECON_TOO_EARLY)
    base.update(kw)
    return VD.verdict_for(**base)


def test_one_outcome_never_decides_however_large():
    assert _v(n_closed=1, residual=9.9, net_at_2x=9.9,
              t_residual=99.0)["verdict"] == VD.TOO_EARLY
    assert _v(n_closed=2, residual=-9.9, net_at_2x=-9.9,
              t_residual=-99.0)["verdict"] == VD.TOO_EARLY


def test_a_small_matured_sample_is_only_a_sign():
    assert _v(n_closed=4, residual=0.02)["verdict"] == VD.POSITIVE_EARLY
    assert _v(n_closed=4, residual=-0.02)["verdict"] == VD.NEGATIVE_EARLY


def test_scale_requires_every_frozen_condition():
    ok = dict(n_closed=12, residual=0.03, t_residual=1.5, net_at_2x=0.02,
              max_drawdown=-0.02, hit_rate=0.6, marginal_diversification=0.2)
    assert _v(**ok)["verdict"] == VD.SCALE
    # each condition removed in turn must stop the scale verdict
    assert _v(**dict(ok, t_residual=0.5))["verdict"] != VD.SCALE
    assert _v(**dict(ok, hit_rate=0.4))["verdict"] != VD.SCALE
    assert _v(**dict(ok, reconciliation_mismatches=1))["verdict"] != VD.SCALE
    assert _v(**dict(ok, marginal_diversification=-0.1))["verdict"] != VD.SCALE
    assert _v(**dict(ok, net_at_2x=-0.01))["verdict"] == VD.REDUCE


def test_reduce_fires_on_persistent_negative_or_fragility_or_drawdown():
    assert _v(n_closed=12, residual=-0.03,
              t_residual=-1.5)["verdict"] == VD.REDUCE
    assert _v(n_closed=12, residual=0.01, t_residual=1.2,
              net_at_2x=-0.01)["verdict"] == VD.REDUCE
    assert _v(n_closed=12, residual=0.01, t_residual=1.2, net_at_2x=0.005,
              max_drawdown=-0.5)["verdict"] == VD.REDUCE


def test_rejection_comes_from_the_tournament_or_the_frozen_kill_rule():
    assert _v(tournament_states={C.FORWARD_REJECTED})["verdict"] == VD.REJECTED
    assert _v(economic_state=SP.ECON_KILL_CANDIDATE)["verdict"] == VD.REJECTED
    assert _v(n_closed=99, residual=9.0,
              tournament_states={C.FORWARD_CONFIRMED})["verdict"] == VD.CONFIRMED


def test_verdict_rules_are_frozen_and_confer_no_capital():
    R = VD.VERDICT_RULES
    assert R["version"] == "R46_5_VERDICT_RULES_v1"
    assert R["mark_to_market_never_decides"] is True
    assert R["one_outcome_never_decides"] is True
    assert R["a_verdict_confers_no_capital"] is True
    assert R["min_closed_before_any_verdict"] == 3
    assert R["min_closed_for_scale_or_reduce"] == 10
    assert "PROVEN" not in json.dumps(VD.VERDICTS)


def test_verdicts_read_matured_trades_only_not_open_marks(sandbox, market,
                                                          field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    as_of = dt.date.fromisoformat(RUN_DAYS[-1])
    body = VD.build(as_of, TEST_CAMPAIGN, reg)
    matured = SP.matured_summary(as_of, TEST_CAMPAIGN)
    for r in body["rows"]:
        m = matured.get(r["challenger_id"]) or SP.empty_matured(
            r["challenger_id"])
        assert r["matured_observations"] == m["n_closed"]
        assert r["residual_alpha_pnl_unit"] == m["cum_residual_alpha"]
        assert r["mark_to_market_excluded_from_verdict"] is True
        assert r["verdict"] in VD.VERDICTS
    assert set(body["counts"]) == set(VD.VERDICTS)
    assert body["n_strategies"] == len(reg["challengers"])


def test_matured_summary_never_includes_an_open_mark(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS[:4]:
        _day(market, specs, reg, day)
    as_of = dt.date.fromisoformat(RUN_DAYS[3])
    m = SP.matured_summary(as_of, TEST_CAMPAIGN)
    for cid, e in m.items():
        my = [c for c in TR.closes(TEST_CAMPAIGN)
              if c["challenger_id"] == cid
              and str(c["exit_session"]) <= str(as_of)]
        assert e["n_closed"] == len(my)
    # A strategy with open trades but no closes reports an empty matured row.
    empty = SP.empty_matured("nobody")
    assert empty["n_closed"] == 0 and empty["cum_net"] == 0.0


# =========================================================================== #
# 4. The realised-correlation blend: frozen BEFORE it is used
# =========================================================================== #
def test_blend_rule_is_versioned_and_supersedes_v1_before_use():
    R = RK.REALISED_BLEND_RULE
    assert R["version"] == "REALISED_CORRELATION_BLEND_v2"
    assert R["supersedes"]["applied_to_any_forward_observation"] is False
    assert R["frozen_before_any_realised_correlation_was_used"] is True
    assert R["realised_becomes_primary_at"] == RK.MIN_REALISED_SESSIONS == 40


def test_blend_weight_is_monotone_and_capped():
    w = [RK.realised_blend_weight(n) for n in range(0, 130)]
    assert all(b >= a - 1e-12 for a, b in zip(w, w[1:])), "must never fall"
    assert RK.realised_blend_weight(0) == 0.0
    assert RK.realised_blend_weight(9) == 0.0
    assert RK.realised_blend_weight(10) == 0.0
    assert RK.realised_blend_weight(25) == pytest.approx(0.25)
    assert RK.realised_blend_weight(40) == pytest.approx(0.5)
    assert RK.realised_blend_weight(80) == pytest.approx(0.75)
    assert RK.realised_blend_weight(500) == pytest.approx(0.75)
    assert RK.blend_source(0.0) == RK.SOURCE_PRIOR_STRUCTURAL
    assert RK.blend_source(0.25) == RK.SOURCE_BLENDED
    assert RK.blend_source(0.5) == RK.SOURCE_REALISED_PRIMARY


def test_structural_prior_holds_until_the_common_sample_earns_otherwise(field):
    entries = list({c["challenger_id"]: c for c in field[1]["challengers"]}
                   .values())
    rng = np.random.default_rng(7)

    def streams(n):
        keys = ["2026-%02d-%02d" % (m, d) for m in (7, 8, 9, 10)
                for d in range(1, 29)][:n]
        return {e["challenger_id"]: {k: float(rng.normal(0, 0.01))
                                     for k in keys} for e in entries}

    few = RK.correlation(entries, streams(5))
    assert few["source"] == RK.SOURCE_PRIOR_STRUCTURAL
    assert few["realised_weight"] == 0.0
    mid = RK.correlation(entries, streams(25))
    assert mid["source"] == RK.SOURCE_BLENDED
    assert 0.0 < mid["realised_weight"] < 0.5
    many = RK.correlation(entries, streams(60))
    assert many["source"] == RK.SOURCE_REALISED_PRIMARY
    assert many["realised_weight"] >= 0.5
    # The structural prior never vanishes entirely.
    assert many["realised_weight"] <= RK.REALISED_BLEND_RULE[
        "max_realised_weight"]


def test_correlation_state_reports_the_transition_honestly(sandbox, market,
                                                           field):
    entries = list({c["challenger_id"]: c for c in field[1]["challengers"]}
                   .values())
    body = RK.correlation_state(dt.date(2026, 8, 26), entries, {}, None,
                                TEST_CAMPAIGN)
    assert body["source_clusters"] == RK.SOURCE_PRIOR_STRUCTURAL
    assert body["realised_weight_clusters"] == 0.0
    assert body["structural_prior_dominates"] is True
    assert body["realised_is_primary"] is False
    assert body["sessions_until_realised_primary"] == 40
    assert body["effective_streams_structural_prior"] == pytest.approx(
        body["n_clusters"], abs=1e-6)
    assert [r["common_sessions"] for r in body["transition_table"]][0] == 0


# =========================================================================== #
# 5. The earnings lane: acceptance instants, PIT, no synthetic fixture
# =========================================================================== #
def test_announcement_timing_is_classified_on_the_eastern_clock():
    # 08:00 ET Thursday -> before the open, reacts the same session.
    assert EA.classify_timing("2026-08-27T12:00:00Z") == {
        "timing": "BEFORE_OPEN", "reaction_session": "2026-08-27"}
    # 12:00 ET -> intraday, same session.
    assert EA.classify_timing("2026-08-27T16:00:00Z") == {
        "timing": "INTRADAY", "reaction_session": "2026-08-27"}
    # 16:30 ET -> after the close, reacts the NEXT session.
    assert EA.classify_timing("2026-08-27T20:30:00Z") == {
        "timing": "AFTER_CLOSE", "reaction_session": "2026-08-28"}
    # Friday after the close -> reacts on Monday, never on the weekend.
    assert EA.classify_timing("2026-08-28T21:00:00Z")[
        "reaction_session"] == "2026-08-31"
    # A Saturday acceptance also reacts on Monday.
    assert EA.classify_timing("2026-08-29T14:00:00Z")[
        "reaction_session"] == "2026-08-31"


def _earnings_capture(sandbox, *, accepted, acquired, ticker="AAA",
                      cik="0000000001", items="2.02,9.01", amendment=False):
    EA.raw_dir().mkdir(parents=True, exist_ok=True)
    ext = EA.raw_dir() / ("earnings_events_%s.json" % cik)
    R46.write_json(ext, {"events": [{
        "cik": cik, "ticker": ticker, "accession": "acc-%s" % cik,
        "form": "8-K/A" if amendment else "8-K", "filing_date": accepted[:10],
        "report_date": accepted[:10], "accepted_at_utc": accepted,
        "items": items, "is_amendment": amendment}]})
    man = EA._manifest()
    man.setdefault("captures", []).append({
        "cik": cik, "ticker": ticker, "extract_path": str(ext),
        "acquired_day": acquired[:10], "acquired_at_utc": acquired,
        "acquired_at_utc_precise": acquired.replace("Z", ".000000Z")})
    R46.write_json(EA.manifest_path(), man)


def test_an_event_accepted_after_the_read_instant_is_not_admissible(sandbox):
    _earnings_capture(sandbox, accepted="2026-08-27T20:30:00Z",
                      acquired="2026-08-26T23:00:00Z")
    before = EA.events(dt.datetime(2026, 8, 27, 12, 0, tzinfo=dt.timezone.utc))
    assert before == [], "an event accepted later must not be visible"
    after = EA.events(dt.datetime(2026, 8, 28, 12, 0, tzinfo=dt.timezone.utc))
    assert len(after) == 1
    assert after[0]["reaction_session"] == "2026-08-28"


def test_an_event_from_a_capture_taken_after_the_read_instant_is_refused(
        sandbox):
    _earnings_capture(sandbox, accepted="2026-08-20T20:30:00Z",
                      acquired="2026-08-27T23:00:00Z")
    assert EA.events(dt.datetime(2026, 8, 27, 12, 0,
                                 tzinfo=dt.timezone.utc)) == []
    assert len(EA.events(dt.datetime(2026, 8, 28, 12, 0,
                                     tzinfo=dt.timezone.utc))) == 1


def test_amendments_are_excluded_from_the_event_stream(sandbox):
    _earnings_capture(sandbox, accepted="2026-08-20T20:30:00Z",
                      acquired="2026-08-21T23:00:00Z", amendment=True)
    assert EA.events(dt.datetime(2026, 8, 27, 12, 0,
                                 tzinfo=dt.timezone.utc)) == []


def test_the_synthetic_fixture_is_refused_by_name(sandbox):
    assert EA._forbidden("D:/x/earnings_fixture.json") is True
    assert EA._forbidden("D:/x/synthetic_earnings.json") is True
    assert EA._forbidden("D:/x/sample_events.json") is True
    assert EA._forbidden("D:/x/earnings_events_0000320193.json") is False
    EA.raw_dir().mkdir(parents=True, exist_ok=True)
    bad = EA.raw_dir() / "earnings_fixture_events.json"
    R46.write_json(bad, {"events": [{
        "cik": "9", "ticker": "ZZZ", "accession": "a", "form": "8-K",
        "accepted_at_utc": "2026-08-20T20:30:00Z", "is_amendment": False}]})
    man = EA._manifest()
    man.setdefault("captures", []).append({
        "cik": "9", "ticker": "ZZZ", "extract_path": str(bad),
        "acquired_at_utc": "2026-08-21T23:00:00Z",
        "acquired_at_utc_precise": "2026-08-21T23:00:00.000000Z"})
    R46.write_json(EA.manifest_path(), man)
    assert EA.events(dt.datetime(2026, 8, 27, 12, 0,
                                 tzinfo=dt.timezone.utc)) == []


def test_only_item_2_02_filings_become_earnings_events():
    doc = {"filings": {"recent": {
        "form": ["8-K", "8-K", "10-Q", "8-K"],
        "items": ["2.02,9.01", "1.01", "", "5.02"],
        "accessionNumber": ["a", "b", "c", "d"],
        "filingDate": ["2026-08-01"] * 4,
        "reportDate": ["2026-08-01"] * 4,
        "acceptanceDateTime": ["2026-08-01T20:30:00.000Z"] * 4}}}
    ev = EA._extract_events(doc, "1", "AAA")
    assert [e["accession"] for e in ev] == ["a"]


def test_universe_coverage_refuses_a_half_captured_cross_section(sandbox,
                                                                 monkeypatch):
    _earnings_capture(sandbox, accepted="2026-08-20T20:30:00Z",
                      acquired="2026-08-21T23:00:00Z", ticker="AAA",
                      cik="0000000001")
    monkeypatch.setattr(EA, "ticker_to_cik",
                        lambda *a, **k: {"AAA": "0000000001",
                                         "BBB": "0000000002"})
    cov = EA.universe_coverage(["AAA", "BBB"],
                               dt.datetime(2026, 8, 27, tzinfo=dt.timezone.utc))
    assert cov["complete"] is False
    assert cov["n_captured"] == 1 and cov["missing"] == ["BBB"]
    # A name the SEC's own map does not carry is acknowledged, not fatal.
    monkeypatch.setattr(EA, "ticker_to_cik",
                        lambda *a, **k: {"AAA": "0000000001"})
    cov2 = EA.universe_coverage(["AAA", "BBB"],
                                dt.datetime(2026, 8, 27, tzinfo=dt.timezone.utc))
    assert cov2["complete"] is True and cov2["n_unmapped_on_sec_map"] == 1


def test_the_pead_challenger_refuses_a_partially_captured_universe(
        sandbox, market, monkeypatch):
    monkeypatch.setattr(EA, "universe_coverage",
                        lambda *a, **k: {"complete": False, "n_missing": 7})
    monkeypatch.setattr(CH, "_eq_universe", lambda: ("AAA", "BBB"))
    book = CH.build(CH.spec_by_id("r46_5_pead_announcement_return_20d"))
    assert book["state"] == "LANE_COVERAGE_INCOMPLETE"
    assert book["legs"] == []


# =========================================================================== #
# 6. The Form-4 lane: acceptance stamps, classification, no look-ahead
# =========================================================================== #
IDX = """Description:           Daily Index of EDGAR Dissemination Feed
Form Type   Company Name                       CIK       Date Filed  File Name
---------------------------------------------------------------------------
4           A.K.A. BRANDS HOLDING CORP.        1865107   20260825    edgar/data/1865107/0001628280-26-058957.txt
4           A10 Networks, Inc.                 1580808   20260825    edgar/data/1580808/0001931555-26-000007.txt
8-K         SOMETHING ELSE INC                 1000000   20260825    edgar/data/1000000/0000000000-26-000001.txt
"""

SUBMISSION = """<SEC-DOCUMENT>0001628280-26-058957.txt : 20260825
<SEC-HEADER>0001628280-26-058957.hdr.sgml : 20260825
ACCEPTANCE-DATETIME>20260825174327
ACCESSION NUMBER:            0001628280-26-058957
</SEC-HEADER>
<DOCUMENT>
<TYPE>4
<XML>
<ownershipDocument>
  <periodOfReport>2026-08-21</periodOfReport>
  <issuer>
    <issuerCik>0001865107</issuerCik>
    <issuerName>TEST CORP</issuerName>
    <issuerTradingSymbol>AAA</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001200506</rptOwnerCik>
      <rptOwnerName>DOE JANE</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>0</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <officerTitle>Chief Executive Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-08-21</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>1000</value></transactionShares>
        <transactionPricePerShare><value>12.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <postTransactionAmounts>
        <sharesOwnedFollowingTransaction><value>31048</value></sharesOwnedFollowingTransaction>
      </postTransactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <securityTitle><value>Common Stock</value></securityTitle>
      <transactionDate><value>2026-08-21</value></transactionDate>
      <transactionCoding><transactionCode>F</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>200</value></transactionShares>
        <transactionPricePerShare><value>12.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
</XML>
</DOCUMENT>
"""


def test_the_daily_index_yields_form_4_rows_only():
    rows = FM.parse_daily_index(IDX)
    assert len(rows) == 2
    assert all(r["form"] == "4" for r in rows)
    assert rows[0]["file"].endswith("0001628280-26-058957.txt")
    assert rows[0]["cik"] == "1865107"


def test_the_acceptance_stamp_is_read_from_the_sec_header_in_eastern_time():
    p = FM.parse_submission_text(SUBMISSION)
    assert p["parsed"] is True
    # 17:43:27 ET on 2026-08-25 is 21:43:27 UTC (EDT, UTC-4).
    assert p["accepted_at_utc"] == "2026-08-25T21:43:27Z"
    assert p["accession"] == "0001628280-26-058957"
    assert p["issuer_ticker"] == "AAA"
    assert p["period_of_report"] == "2026-08-21"


def test_owners_and_their_relationship_are_parsed():
    p = FM.parse_submission_text(SUBMISSION)
    assert p["n_owners"] == 1
    o = p["owners"][0]
    assert o["is_officer"] is True and o["is_director"] is False
    assert o["officer_title"] == "Chief Executive Officer"
    assert o["cik"] == "0001200506"


def test_not_all_form4s_are_equivalent():
    p = FM.parse_submission_text(SUBMISSION)
    txs = p["transactions"]
    assert len(txs) == 2
    buy, withhold = txs
    assert buy["transaction_code"] == "P"
    assert buy["transaction_class"] == "OPEN_MARKET_PURCHASE"
    assert buy["is_informative"] is True
    assert buy["shares"] == 1000.0 and buy["price_per_share"] == 12.5
    assert buy["direction"] == "BUY" and buy["ownership"] == "D"
    assert withhold["transaction_class"] == "TAX_WITHHOLDING"
    assert withhold["is_informative"] is False
    assert FM.classify_code("S") == "OPEN_MARKET_SALE"
    assert FM.classify_code("A") == "GRANT_AWARD"
    assert FM.classify_code("M") == "OPTION_EXERCISE"
    assert FM.classify_code("G") == "GIFT"
    assert FM.classify_code("??") == FM.CLASS_OTHER
    assert set(FM.INFORMATIVE_CODES) == {"P", "S"}


def _form4_capture(day, acquired, *, complete=True, txs=None):
    FM.raw_dir().mkdir(parents=True, exist_ok=True)
    parsed = FM.raw_dir() / ("form4_rows_%s.json" % day)
    R46.write_json(parsed, {"day": day, "filings": txs if txs is not None else [{
        "accession": "acc-%s" % day, "parsed": True,
        "accepted_at_utc": "%sT21:00:00Z" % day,
        "issuer_cik": "1", "issuer_ticker": "AAA", "issuer_name": "TEST",
        "owners": [{"cik": "9", "name": "DOE", "is_officer": True}],
        "transactions": [{"transaction_date": day, "transaction_code": "P",
                          "transaction_class": "OPEN_MARKET_PURCHASE",
                          "shares": 100.0, "price_per_share": 10.0,
                          "direction": "BUY", "is_informative": True}]}]})
    man = FM._manifest()
    man.setdefault("captures", []).append({
        "day": day, "parsed_path": str(parsed), "complete": complete,
        "acquired_at_utc": acquired,
        "acquired_at_utc_precise": acquired.replace("Z", ".000000Z")})
    R46.write_json(FM.manifest_path(), man)


def test_a_filing_accepted_after_the_read_instant_is_not_visible(sandbox):
    _form4_capture("2026-08-26", "2026-08-25T23:00:00Z")
    assert FM.transactions(dt.datetime(2026, 8, 26, 12, 0,
                                       tzinfo=dt.timezone.utc)) == []
    got = FM.transactions(dt.datetime(2026, 8, 27, 12, 0,
                                      tzinfo=dt.timezone.utc))
    assert len(got) == 1 and got[0]["issuer_ticker"] == "AAA"
    assert got[0]["insider_role"] == "OFFICER"


def test_a_capture_taken_after_the_read_instant_is_refused(sandbox):
    _form4_capture("2026-08-20", "2026-08-27T23:00:00Z")
    assert FM.transactions(dt.datetime(2026, 8, 27, 12, 0,
                                       tzinfo=dt.timezone.utc)) == []


def test_a_partial_day_never_counts_toward_window_coverage(sandbox):
    _form4_capture("2026-08-25", "2026-08-26T23:00:00Z", complete=True)
    _form4_capture("2026-08-26", "2026-08-27T02:00:00Z", complete=False)
    now = dt.datetime(2026, 8, 27, 12, 0, tzinfo=dt.timezone.utc)
    assert FM.covered_days(now) == {"2026-08-25"}
    cov = FM.window_coverage(["2026-08-25", "2026-08-26"], now)
    assert cov["complete"] is False
    assert cov["missing"] == ["2026-08-26"]
    assert FM.window_coverage(["2026-08-25"], now)["complete"] is True


def test_the_insider_challengers_refuse_an_uncovered_window(sandbox, market,
                                                            monkeypatch):
    monkeypatch.setattr(CH, "_eq_universe", lambda: ("AAA", "BBB"))
    for cid in ("r46_5_insider_cluster_buy_20d",
                "r46_5_insider_net_purchase_xs_20d"):
        book = CH.build(CH.spec_by_id(cid))
        assert book["state"] == "LANE_COVERAGE_INCOMPLETE", cid
        assert book["legs"] == []
        assert book["window_coverage"]["complete"] is False


def test_a_covered_window_produces_a_book_from_informative_codes_only(
        sandbox, market, monkeypatch):
    monkeypatch.setattr(CH, "_eq_universe", lambda: ("AAA", "BBB", "SPY"))
    cutoff = dt.date(2026, 8, 26)
    days = FM._trailing_business_days(cutoff, 21)
    filings = []
    for i, d in enumerate(days[:6]):
        for owner in ("o1", "o2"):
            filings.append({
                "accession": "acc-%s-%s" % (d, owner), "parsed": True,
                "accepted_at_utc": "%sT21:00:00Z" % d,
                "issuer_cik": "1", "issuer_ticker": "AAA",
                "owners": [{"cik": owner, "name": owner, "is_officer": True}],
                "transactions": [{"transaction_date": d,
                                  "transaction_code": "P",
                                  "transaction_class": "OPEN_MARKET_PURCHASE",
                                  "shares": 100.0, "price_per_share": 10.0,
                                  "direction": "BUY", "is_informative": True}]})
        # A grant on the same name must never create a cluster.
        filings.append({
            "accession": "grant-%s" % d, "parsed": True,
            "accepted_at_utc": "%sT21:00:00Z" % d,
            "issuer_cik": "2", "issuer_ticker": "BBB",
            "owners": [{"cik": "o3", "name": "o3", "is_director": True}],
            "transactions": [{"transaction_date": d, "transaction_code": "A",
                              "transaction_class": "GRANT_AWARD",
                              "shares": 500.0, "price_per_share": 0.0,
                              "direction": "BUY", "is_informative": False}]})
    for d in days:
        _form4_capture(d, "2026-08-26T23:00:00Z",
                       txs=[f for f in filings if f["accepted_at_utc"][:10] == d])
    now = dt.datetime(2026, 8, 27, 12, 0, tzinfo=dt.timezone.utc)
    txs = FM.transactions(now, informative_only=True)
    assert txs and all(t["transaction_code"] == "P" for t in txs)
    assert {t["issuer_ticker"] for t in txs} == {"AAA"}


def test_form4_window_coverage_is_reported_on_the_lane(sandbox):
    _form4_capture("2026-08-25", "2026-08-26T23:00:00Z")
    body = FM.run(acquire_now=False, campaign_id=TEST_CAMPAIGN,
                  as_of=dt.date(2026, 8, 26))
    wc = body["window_coverage"]
    assert wc["cluster_buy_21_sessions"]["n_sessions"] == 21
    assert wc["net_purchase_ratio_63_sessions"]["n_sessions"] == 63
    assert wc["cluster_buy_21_sessions"]["complete"] is False
    assert body["coverage"]["n_complete_days"] == 1


# =========================================================================== #
# 7. The EDGAR seam: one paced, contact-carrying access point
# =========================================================================== #
def test_the_form4_acquisition_is_bounded_by_a_wall_clock_and_resumes(sandbox,
                                                                      monkeypatch):
    """A daily cycle must never be held hostage by a third-party feed."""
    assert FM.MAX_SECONDS_PER_RUN > 0
    calls = {"n": 0}

    def slow_get(url, **kw):
        calls["n"] += 1
        return {"status": 200, "body": IDX.encode("latin-1"), "error": None}
    monkeypatch.setattr(SEC, "user_agent", lambda: "test-agent contact@example.com")
    monkeypatch.setattr(SEC, "get", slow_get)
    res = FM.acquire(acquire=True, today=dt.date(2026, 8, 27),
                     budget_seconds=0.0, now=dt.datetime(2026, 8, 27, 12, 0,
                                                         tzinfo=dt.timezone.utc))
    assert res["time_budget_exhausted"] is True
    assert res["resumable"] is True
    assert res["n_filings_fetched"] == 0
    assert calls["n"] == 0, "a spent budget must buy nothing at all"
    # The earnings lane is bounded the same way, with the same semantics.
    assert EA.MAX_SECONDS_PER_RUN > 0
    monkeypatch.setattr(EA, "ticker_to_cik", lambda *a, **k: {"AAA": "1"})
    monkeypatch.setattr(CH, "_eq_universe", lambda: ("AAA",))
    ea = EA.acquire(acquire=True, today=dt.date(2026, 8, 27),
                    budget_seconds=0.0,
                    now=dt.datetime(2026, 8, 27, 12, 0, tzinfo=dt.timezone.utc))
    assert ea["time_budget_exhausted"] is True
    assert ea["acquired"] == 0


def test_edgar_access_has_exactly_one_seam_and_declares_a_contact():
    assert SEC.REQUEST_INTERVAL_SECONDS >= 0.1, "SEC allows 10 requests/second"
    assert SEC.PRODUCT.startswith("paper-trader-research/")
    assert SEC.mask("someone@example.com") == "s***@example.com"
    assert SEC.mask(None) is None
    import inspect
    for mod in (EA, FM):
        src = inspect.getsource(mod)
        assert "urlopen" not in src, "%s must go through the seam" % mod.__name__


def test_no_contact_blocks_acquisition_rather_than_guessing(sandbox,
                                                            monkeypatch):
    monkeypatch.setattr(SEC, "user_agent", lambda: None)
    monkeypatch.setattr(SEC, "contact", lambda: None)
    assert SEC.get("https://www.sec.gov/x")["error"] == SEC.BLOCKED_NO_CONTACT
    assert EA.acquire(acquire=True, today=dt.date(2026, 8, 27))["state"] == \
        SEC.BLOCKED_NO_CONTACT
    assert FM.acquire(acquire=True, today=dt.date(2026, 8, 27))["state"] == \
        SEC.BLOCKED_NO_CONTACT


def test_the_lanes_never_acquire_inside_the_hermetic_process(sandbox):
    assert AD._lanes_may_acquire() is False
    ea = EA.run(acquire_now=False, campaign_id=TEST_CAMPAIGN,
                as_of=dt.date(2026, 8, 27))
    assert ea["acquisition"]["state"] == "NOT_ACQUIRED"
    assert ea["state"] in ("FROZEN_PENDING_ACQUISITION", "DATA_BLOCKED")
    fm = FM.run(acquire_now=False, campaign_id=TEST_CAMPAIGN,
                as_of=dt.date(2026, 8, 27))
    assert fm["acquisition"]["state"] == "NOT_ACQUIRED"
    assert fm["money_spent_usd"] == 0.0


# =========================================================================== #
# 8. Policy competition: the question is asked, the answer is not assumed
# =========================================================================== #
def test_the_policy_competition_refuses_to_answer_without_a_sample(sandbox,
                                                                   market,
                                                                   field):
    specs, reg = field
    for day in RUN_DAYS[:4]:
        _day(market, specs, reg, day)
    comp = json.loads((R46.campaign_dir(TEST_CAMPAIGN) / NV.COMPARISON_ARTIFACT)
                      .read_text(encoding="utf-8"))["competition"]
    assert comp["decidable"] is False
    assert comp["answer"] == "NOT_YET_DECIDABLE"
    assert comp["assumes_nothing"] is True
    assert comp["n_forward_sessions"] >= 1
    assert comp["current_leader_by_nav"] in NV.SERIES_IDS


def test_every_policy_reports_the_same_competition_facts(sandbox, market,
                                                         field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    comp = json.loads((R46.campaign_dir(TEST_CAMPAIGN) / NV.COMPARISON_ARTIFACT)
                      .read_text(encoding="utf-8"))
    ids = {r["series_id"] for r in comp["ranked_by_nav"]}
    assert ids == set(NV.SERIES_IDS)
    for r in comp["ranked_by_nav"]:
        for f in ("gross_pnl", "net_pnl", "cost_drag", "realised_pnl",
                  "turnover_usd", "n_sessions"):
            assert f in r
        if r["series_id"] in AL.POLICIES:
            assert r["turnover_usd"] is not None
    cash = next(r for r in comp["ranked_by_nav"]
                if r["series_id"] == AL.POLICY_CASH)
    assert cash["turnover_usd"] == 0.0
    assert cash["cost_drag"] == 0.0


# =========================================================================== #
# 9. The ONE orchestration path carries the harvest
# =========================================================================== #
def test_the_advance_runs_harvest_verdicts_and_correlation_after_the_board(
        sandbox, market, field, monkeypatch):
    specs, reg = field
    monkeypatch.setattr(CH, "ALL_SPECS", tuple(specs))
    for mod in (CF, CR, MC, EVN, EA, FM):
        monkeypatch.setattr(mod, "run", lambda *a, **k: {"state": "DATA_BLOCKED"})
    now = dt.datetime(2026, 8, 26, 22, 30, tzinfo=dt.timezone.utc)
    res = AD.advance(TEST_CAMPAIGN, now=now, registry=reg,
                     eligible_market_date="2026-08-26")
    sp = res["shadow_pnl"]
    assert sp["forward_pnl_evidence"] == HV.EVIDENCE_STILL_WAITING
    assert sp["n_matured_trades"] == 0
    assert sp["one_economic_truth"] is True
    assert sp["verdict_counts"][VD.TOO_EARLY] == len(reg["challengers"])
    assert sp["realised_correlation_source"] == RK.SOURCE_PRIOR_STRUCTURAL
    assert set(res["lanes"]) >= {"cftc", "credit", "macro", "events",
                                 "earnings", "form4"}
    assert res["orders_created"] == 0 and res["portfolio_mutations"] == 0
    assert res["promoted_models"] == 0
    for name in (HV.ARTIFACT, VD.ARTIFACT, RK.CORRELATION_ARTIFACT):
        assert (R46.campaign_dir(TEST_CAMPAIGN) / name).exists(), name


def test_a_broken_harvest_never_stops_the_tournament(sandbox, market, field,
                                                     monkeypatch):
    specs, reg = field
    monkeypatch.setattr(CH, "ALL_SPECS", tuple(specs))
    for mod in (CF, CR, MC, EVN, EA, FM):
        monkeypatch.setattr(mod, "run", lambda *a, **k: {"state": "DATA_BLOCKED"})

    def boom(*a, **k):
        raise RuntimeError("harvest down")
    monkeypatch.setattr(HV, "build", boom)
    now = dt.datetime(2026, 8, 26, 22, 30, tzinfo=dt.timezone.utc)
    res = AD.advance(TEST_CAMPAIGN, now=now, registry=reg,
                     eligible_market_date="2026-08-26")
    assert res["available"] is True
    assert res["emission"]["n_appended"] > 0
    assert any(f["stage"] == "forward_harvest"
               for f in res["shadow_pnl"]["stage_failures"])


def test_a_broken_edgar_lane_never_stops_the_tournament(sandbox, market, field,
                                                        monkeypatch):
    specs, reg = field
    monkeypatch.setattr(CH, "ALL_SPECS", tuple(specs))
    for mod in (CF, CR, MC, EVN):
        monkeypatch.setattr(mod, "run", lambda *a, **k: {"state": "DATA_BLOCKED"})

    def boom(*a, **k):
        raise RuntimeError("EDGAR down")
    monkeypatch.setattr(EA, "run", boom)
    monkeypatch.setattr(FM, "run", boom)
    now = dt.datetime(2026, 8, 26, 22, 30, tzinfo=dt.timezone.utc)
    res = AD.advance(TEST_CAMPAIGN, now=now, registry=reg,
                     eligible_market_date="2026-08-26")
    assert res["available"] is True
    stages = {f["stage"] for f in res["stage_failures"]}
    assert {"lane_earnings", "lane_form4"} <= stages
    assert res["emission"]["n_appended"] > 0


# =========================================================================== #
# 10. Maturity is still the judge's alone
# =========================================================================== #
def test_the_harvest_cannot_mature_a_prediction(sandbox, market, field):
    specs, reg = field
    _day(market, specs, reg, "2026-08-26")
    n_before = len(LG.outcomes(TEST_CAMPAIGN))
    HV.build(dt.date(2026, 9, 30), TEST_CAMPAIGN)      # far-future as_of
    VD.build(dt.date(2026, 9, 30), TEST_CAMPAIGN, reg)
    assert len(LG.outcomes(TEST_CAMPAIGN)) == n_before
    assert len(TR.closes(TEST_CAMPAIGN)) == 0


def test_scoring_a_second_time_appends_nothing(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS[:5]:
        _day(market, specs, reg, day)
    outs = len(LG.outcomes(TEST_CAMPAIGN))
    now = dt.datetime(2026, 9, 1, 22, 30, tzinfo=dt.timezone.utc)
    again = JD.score_pending(TEST_CAMPAIGN, now)
    assert again["n_newly_scored"] == 0
    assert len(LG.outcomes(TEST_CAMPAIGN)) == outs
    body = HV.build(dt.date.fromisoformat(RUN_DAYS[4]), TEST_CAMPAIGN)
    body2 = HV.build(dt.date.fromisoformat(RUN_DAYS[4]), TEST_CAMPAIGN)
    assert body["matured"]["n_matured"] == body2["matured"]["n_matured"]


def test_the_harvest_never_scores_a_trade_that_closes_later(sandbox, market,
                                                            field):
    specs, reg = field
    for day in RUN_DAYS:
        _day(market, specs, reg, day)
    early = HV.build(dt.date(2026, 8, 28), TEST_CAMPAIGN, write=False)
    late = HV.build(dt.date(2026, 9, 8), TEST_CAMPAIGN, write=False)
    assert early["matured"]["n_matured"] <= late["matured"]["n_matured"]
    for r in early["matured"]["trades"]:
        assert str(r["exit_session"]) <= "2026-08-28"


# =========================================================================== #
# 11. The read model serves it, and nothing reads as proven
# =========================================================================== #
def test_the_read_model_serves_harvest_verdicts_and_competition(
        sandbox, market, field, monkeypatch):
    specs, reg = field
    monkeypatch.setenv(PT.RESEARCH_ROOT_ENV, str(R46.RESEARCH_ROOT))
    empty = PT.load_prospective_tournament(TEST_CAMPAIGN)
    assert empty["shadow_pnl"]["available"] is False
    for day in RUN_DAYS[:5]:
        _day(market, specs, reg, day)
    from alpha_agent.r46 import campaign as CP
    R46.write_json(R46.campaign_dir(TEST_CAMPAIGN) / CP.FINAL_ARTIFACT,
                   {"TERMINAL_STATE": "R46_PROSPECTIVE_ALPHA_TOURNAMENT_LIVE"})
    d = PT.load_prospective_tournament(TEST_CAMPAIGN)
    sp = d["shadow_pnl"]
    hv = sp["forward_harvest"]
    assert hv["available"] is True
    assert hv["FORWARD_PNL_EVIDENCE"] in HV.EVIDENCE_STATES
    assert hv["matured_and_mark_to_market_are_never_summed"] is True
    assert hv["mark_to_market"]["is_matured_statistical_evidence"] is False
    vd = sp["strategy_verdicts"]
    assert vd["available"] is True
    assert set(vd["counts"]) == set(VD.VERDICTS)
    assert sp["policy_competition"]["answer"] in (
        "NOT_YET_DECIDABLE", "CANONICAL_LEADS", "SIMPLE_POLICY_LEADS")
    rc = sp["realised_correlation"]
    assert rc["available"] is True
    assert rc["blend_rule_version"] == "REALISED_CORRELATION_BLEND_v2"
    assert "earnings" in d["information_lanes"]
    assert "form4" in d["information_lanes"]
    assert "PROVEN" not in json.dumps(sp)
    assert d["shadow_pnl"]["ALPHA_RESULT"] in PB.ALPHA_RESULTS


def test_the_read_model_degrades_honestly_without_the_new_artifacts(sandbox,
                                                                    monkeypatch):
    monkeypatch.setenv(PT.RESEARCH_ROOT_ENV, str(R46.RESEARCH_ROOT))
    assert PT._harvest(None)["available"] is False
    assert PT._harvest(None)["FORWARD_PNL_EVIDENCE"] == \
        HV.EVIDENCE_STILL_WAITING
    assert PT._verdicts(None)["available"] is False
    assert PT._correlation(None)["available"] is False


# =========================================================================== #
# 12. Governance: research only, always
# =========================================================================== #
def test_no_r46_5_module_can_reach_an_operational_store():
    import inspect
    for mod in (HV, VD, EA, FM, SEC):
        src = inspect.getsource(mod)
        for tok in ("portfolio_decision", "rebalance_execution",
                    "operational_book import", "from paper_trader.api import app",
                    "schtasks", "Register-ScheduledTask", "crontab"):
            assert tok not in src, (mod.__name__, tok)


def test_nothing_in_the_harvest_promotes_or_allocates(sandbox, market, field):
    specs, reg = field
    for day in RUN_DAYS[:4]:
        _day(market, specs, reg, day)
    as_of = dt.date.fromisoformat(RUN_DAYS[3])
    hv = HV.build(as_of, TEST_CAMPAIGN)
    vd = VD.build(as_of, TEST_CAMPAIGN, reg)
    assert hv["research_only"] is True
    assert hv["orders_created"] == 0 and hv["portfolio_mutations"] == 0
    assert vd["research_only"] is True
    assert vd["rules"]["a_verdict_confers_no_capital"] is True
    # A verdict does not touch a shadow weight: the allocator owns those.
    before = json.dumps(AL.rows(TEST_CAMPAIGN), sort_keys=True)
    VD.build(as_of, TEST_CAMPAIGN, reg)
    assert json.dumps(AL.rows(TEST_CAMPAIGN), sort_keys=True) == before


def test_the_lanes_spend_nothing_and_write_no_credential(sandbox):
    for body in (EA.run(acquire_now=False, campaign_id=TEST_CAMPAIGN,
                        as_of=dt.date(2026, 8, 27)),
                 FM.run(acquire_now=False, campaign_id=TEST_CAMPAIGN,
                        as_of=dt.date(2026, 8, 27))):
        assert body["money_spent_usd"] == 0.0
        assert body["credential_written"] is False
        assert body["information_family"] in ("EARNINGS_EVENTS",
                                              "INSIDER_FLOW")
