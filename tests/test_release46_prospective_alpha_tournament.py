"""Release 46 - the prospective alpha tournament.

The tests that matter here are not "does it compute". They are the ones that
make it impossible to quietly turn a losing challenger into a winning one:

* a prediction whose outcome window opened before it was emitted is REFUSED,
  not warned about;
* a prediction row missing any contract field is REFUSED, because a
  half-specified forecast cannot be prosecuted and is therefore not evidence;
* emitting twice creates one row, and the ledger says so;
* editing a registered specification in place is detected as RETUNE_DETECTED
  rather than silently accepted;
* the seven adopted prior-release registries come back byte-identical;
* fifty overlapping twenty-day bets never count as fifty independent ones;
* nothing anywhere can read "proven".
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import hashlib
from pathlib import Path

import pytest

from alpha_agent import r46 as R46
from alpha_agent.r46 import burden as BD
from alpha_agent.r46 import challengers as CH
from alpha_agent.r46 import clock as CK
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import emit as EM
from alpha_agent.r46 import evidence as EV
from alpha_agent.r46 import feasibility as FE
from alpha_agent.r46 import judge as JD
from alpha_agent.r46 import leaderboard as LB
from alpha_agent.r46 import ledger as LG
from alpha_agent.r46 import registry as RG
from alpha_agent.r46 import shell_policy as SP

TEST_CAMPAIGN = "r46_pytest_campaign"


@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """Point every R46 write at a temp root. The real ledger is never touched."""
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path / "r46root" / TEST_CAMPAIGN)
    return tmp_path


# --------------------------------------------------------------------------- #
# 1. Contract
# --------------------------------------------------------------------------- #
def test_contract_hash_is_stable_across_calls():
    assert C.contract_hash() == C.contract_hash()


def test_contract_declares_true_forward_and_historical_separately():
    assert C.TRUE_FORWARD in C.EVIDENCE_CLASSES
    assert C.HISTORICAL_SIMULATION in C.EVIDENCE_CLASSES
    assert C.TRUE_FORWARD != C.HISTORICAL_SIMULATION


def test_proven_alpha_is_not_a_challenger_state():
    assert "PROVEN_ALPHA" not in C.CHALLENGER_STATES
    assert C.PROVEN_ALPHA_IS_NOT_A_STATE is True


def test_forbidden_operations_include_every_backfill_route():
    joined = " | ".join(C.FORBIDDEN_FOREVER).lower()
    for phrase in ("backdating", "reconstructing", "overwriting",
                   "rewriting", "retuning"):
        assert phrase in joined


def test_burden_is_inherited_not_reset():
    assert C.INHERITED_GLOBAL_BURDEN == 353
    assert C.BURDEN_MAY_NEVER_BE_RESET is True
    assert C.PROSPECTIVE_EVIDENCE_IS_NOT_SEARCH_BURDEN is True


def test_safety_block_forbids_every_mutating_action():
    for flag in ("creates_order", "creates_paper_order", "promotes_model",
                 "mutates_holdings", "mutates_cash", "enables_automation",
                 "writes_operational_store", "may_spend_money",
                 "backdates_forward_rows",
                 "mutates_prior_release_artifacts"):
        assert C.SAFETY_BLOCK[flag] is False, flag


# --------------------------------------------------------------------------- #
# 2. The clock and the entry rule
# --------------------------------------------------------------------------- #
def test_entry_is_the_next_weekday_after_the_eastern_date():
    # Tuesday 16:05 ET == 20:05 UTC -> entry Wednesday
    t = dt.datetime(2026, 8, 25, 20, 5, tzinfo=dt.timezone.utc)
    assert CK.entry_session_date(t) == dt.date(2026, 8, 26)


def test_entry_skips_the_weekend():
    friday_evening = dt.datetime(2026, 8, 28, 21, 0, tzinfo=dt.timezone.utc)
    assert CK.entry_session_date(friday_evening) == dt.date(2026, 8, 31)


def test_entry_rule_is_conservative_across_the_utc_date_boundary():
    """23:30 UTC on a Tuesday is still Tuesday evening in New York."""
    t = dt.datetime(2026, 8, 25, 23, 30, tzinfo=dt.timezone.utc)
    assert CK.eastern_date(t) == dt.date(2026, 8, 25)
    assert CK.entry_session_date(t) == dt.date(2026, 8, 26)


def test_emission_is_always_strictly_before_the_outcome_window():
    """Every hour of every day, including the 20:00-24:00 ET evening window.

    Midnight UTC is 8pm Eastern on the PREVIOUS day, so a UTC-anchored outcome
    window would open before an evening emission and the ordering would fail.
    The window is anchored to midnight EASTERN for exactly this reason.
    """
    for day in (24, 25, 28, 29, 30):
        for hour in range(0, 24):
            t = dt.datetime(2026, 8, day, hour, 0, tzinfo=dt.timezone.utc)
            entry = CK.entry_session_date(t)
            assert CK.is_true_forward(t, entry), (day, hour)


def test_outcome_window_opens_at_midnight_eastern_not_midnight_utc():
    start = CK.outcome_window_start_utc(dt.date(2026, 8, 26))
    assert CK.eastern_date(start) == dt.date(2026, 8, 26)
    assert CK.to_eastern(start).hour == 0
    # an 8:30pm ET emission on the 25th is still strictly before it
    evening = dt.datetime(2026, 8, 26, 0, 30, tzinfo=dt.timezone.utc)
    assert CK.eastern_date(evening) == dt.date(2026, 8, 25)
    assert evening < start


def test_maturity_counts_realised_sessions_not_calendar_days():
    sessions = [dt.date(2026, 8, 26), dt.date(2026, 8, 27),
                dt.date(2026, 8, 28), dt.date(2026, 8, 31),
                dt.date(2026, 9, 1)]
    # a holiday on 8/31 simply is not in the list; nothing is interpolated
    assert CK.maturity_session(sessions, dt.date(2026, 8, 26), 1) == \
        dt.date(2026, 8, 27)
    assert CK.maturity_session(sessions, dt.date(2026, 8, 26), 3) == \
        dt.date(2026, 8, 31)
    assert CK.maturity_session(sessions, dt.date(2026, 8, 26), 99) is None


# --------------------------------------------------------------------------- #
# 3. The ledger refuses what it must refuse
# --------------------------------------------------------------------------- #
def _valid_row(**over) -> dict:
    row = {f: None for f in C.PREDICTION_RECORD_FIELDS}
    row.update({
        "prediction_id": "p1", "batch_id": "b1",
        "challenger_id": "c", "challenger_version": "v1",
        "challenger_spec_hash": "h",
        "emitted_at_utc": "2026-08-25T20:00:00Z",
        "outcome_window_start_utc": "2026-08-26T00:00:00Z",
        "data_cutoff_utc": "2026-08-25T20:00:00Z",
        "data_cutoff_session": "2026-08-24",
        "effective_as_of": "2026-08-26",
        "asset_class": "US_EQUITY", "instrument": "BOOK:X",
        "horizon": 5, "horizon_unit": "ELIGIBLE_SESSIONS",
        "point_in_time_status": C.PIT_OK,
        "forward_evidence_type": C.TRUE_FORWARD,
        "status": C.STATUS_PENDING,
    })
    row.update(over)
    return row


def test_ledger_accepts_a_complete_true_forward_row():
    LG.validate_prediction(_valid_row())


def test_ledger_refuses_a_backdated_prediction():
    bad = _valid_row(emitted_at_utc="2026-08-27T12:00:00Z")
    with pytest.raises(LG.LedgerRefusal) as e:
        LG.validate_prediction(bad)
    assert "not TRUE_FORWARD" in str(e.value)


def test_ledger_refuses_a_prediction_emitted_exactly_at_the_window_open():
    bad = _valid_row(emitted_at_utc="2026-08-26T00:00:00Z")
    with pytest.raises(LG.LedgerRefusal):
        LG.validate_prediction(bad)


def test_ledger_refuses_a_historical_simulation_row():
    bad = _valid_row(forward_evidence_type=C.HISTORICAL_SIMULATION)
    with pytest.raises(LG.LedgerRefusal) as e:
        LG.validate_prediction(bad)
    assert "TRUE_FORWARD" in str(e.value)


def test_ledger_refuses_a_row_missing_any_contract_field():
    bad = _valid_row()
    del bad["market_state_snapshot_hash"]
    with pytest.raises(LG.LedgerRefusal) as e:
        LG.validate_prediction(bad)
    assert "market_state_snapshot_hash" in str(e.value)


def test_ledger_refuses_a_data_cutoff_after_emission():
    bad = _valid_row(data_cutoff_utc="2026-08-25T23:00:00Z")
    with pytest.raises(LG.LedgerRefusal):
        LG.validate_prediction(bad)


def test_ledger_refuses_a_pit_violation():
    bad = _valid_row(point_in_time_status=C.PIT_VIOLATION)
    with pytest.raises(LG.LedgerRefusal):
        LG.validate_prediction(bad)


def test_ledger_refuses_a_row_that_does_not_enter_as_pending():
    bad = _valid_row(status=C.STATUS_SCORED)
    with pytest.raises(LG.LedgerRefusal):
        LG.validate_prediction(bad)


# --------------------------------------------------------------------------- #
# 4. Idempotency
# --------------------------------------------------------------------------- #
def test_appending_the_same_prediction_twice_writes_one_row(sandbox):
    row = _valid_row()
    first = LG.append_predictions([row], TEST_CAMPAIGN)
    second = LG.append_predictions([copy.deepcopy(row)], TEST_CAMPAIGN)
    assert first["n_appended"] == 1
    assert second["n_appended"] == 0
    assert second["n_duplicates_skipped"] == 1
    assert len(LG.predictions(TEST_CAMPAIGN)) == 1


def test_identity_key_ignores_prediction_id_and_uses_the_decision(sandbox):
    a = _valid_row(prediction_id="pA")
    b = _valid_row(prediction_id="pB")     # same decision, different id
    LG.append_predictions([a], TEST_CAMPAIGN)
    out = LG.append_predictions([b], TEST_CAMPAIGN)
    assert out["n_appended"] == 0
    assert len(LG.predictions(TEST_CAMPAIGN)) == 1


def test_a_different_horizon_is_a_different_prediction(sandbox):
    LG.append_predictions([_valid_row(prediction_id="p5", horizon=5)],
                          TEST_CAMPAIGN)
    out = LG.append_predictions([_valid_row(prediction_id="p20", horizon=20)],
                                TEST_CAMPAIGN)
    assert out["n_appended"] == 1
    assert len(LG.predictions(TEST_CAMPAIGN)) == 2


def test_a_different_version_is_a_different_prediction(sandbox):
    LG.append_predictions([_valid_row(prediction_id="a")], TEST_CAMPAIGN)
    out = LG.append_predictions(
        [_valid_row(prediction_id="b", challenger_version="v2")],
        TEST_CAMPAIGN)
    assert out["n_appended"] == 1


def test_outcome_appending_is_idempotent(sandbox):
    o = {"prediction_id": "p1", "challenger_id": "c", "horizon": 5,
         "scored_at_utc": "2026-09-02T20:00:00Z",
         "realised_net_return": 0.001,
         "forward_evidence_type": C.TRUE_FORWARD}
    assert LG.append_outcomes([o], TEST_CAMPAIGN)["n_appended"] == 1
    assert LG.append_outcomes([dict(o)], TEST_CAMPAIGN)["n_appended"] == 0
    assert len(LG.outcomes(TEST_CAMPAIGN)) == 1


# --------------------------------------------------------------------------- #
# 5. Immutability - the chain detects a rewrite
# --------------------------------------------------------------------------- #
def test_chain_is_intact_after_normal_appends(sandbox):
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    assert LG.verify(TEST_CAMPAIGN)["all_intact"] is True


def test_editing_a_recorded_forecast_breaks_the_chain(sandbox):
    LG.append_predictions([_valid_row()], TEST_CAMPAIGN)
    path = LG.forward_dir(TEST_CAMPAIGN) / LG.PREDICTION_LEDGER
    body = json.loads(path.read_text(encoding="utf-8"))
    body["rows"][0]["direction"] = "TAMPERED"
    path.write_text(json.dumps(body), encoding="utf-8")
    report = LG.verify(TEST_CAMPAIGN)
    assert report["all_intact"] is False


# --------------------------------------------------------------------------- #
# 6. Challenger specification and versioning
# --------------------------------------------------------------------------- #
def test_every_seed_spec_declares_the_full_economic_contract():
    for spec in CH.SEED_SPECS:
        for field in ("challenger_id", "challenger_version", "family",
                      "asset_class", "horizons", "control", "benchmark",
                      "cost_class", "universe", "thesis", "parameters",
                      "signal_owner"):
            assert spec.get(field) is not None, (spec["challenger_id"], field)
        assert spec["promotion_allowed"] is False
        assert spec["parameters_were_searched"] is False
        assert spec["control"] in C.CONTROLS


def test_seed_challenger_ids_are_unique():
    ids = [s["challenger_id"] for s in CH.SEED_SPECS]
    assert len(ids) == len(set(ids))


def test_spec_hash_changes_when_a_parameter_changes():
    spec = dict(CH.SEED_SPECS[0])
    before = CH.spec_hash(spec)
    spec["parameters"] = dict(spec["parameters"])
    spec["parameters"]["formation_days"] = 999
    assert CH.spec_hash(spec) != before


def test_spec_hash_changes_when_the_horizon_changes():
    spec = dict(CH.SEED_SPECS[0])
    before = CH.spec_hash(spec)
    spec["horizons"] = (7,)
    assert CH.spec_hash(spec) != before


def test_material_change_requires_a_new_version():
    a = dict(CH.SEED_SPECS[0])
    b = dict(a)
    b["parameters"] = dict(a["parameters"])
    b["parameters"]["formation_days"] = 120
    verdict = RG.classify_change(a, b)
    assert verdict["classification"] == "MATERIAL"
    assert verdict["requires_new_version"] is True
    assert "parameters" in verdict["changed_fields"]


def test_non_economic_change_does_not_require_a_new_version():
    a = dict(CH.SEED_SPECS[0])
    b = dict(a)
    b["thesis"] = "reworded, same economics"
    verdict = RG.classify_change(a, b)
    assert verdict["classification"] == "IMPLEMENTATION"
    assert verdict["requires_new_version"] is False


def test_next_version_increments():
    assert RG.next_version(["v1"]) == "v2"
    assert RG.next_version(["v1", "v2", "v7"]) == "v8"
    assert RG.next_version([]) == "v1"


# --------------------------------------------------------------------------- #
# 7. Evidence accounting - overlap is never free
# --------------------------------------------------------------------------- #
def test_overlapping_horizons_are_discounted():
    assert EV.effective_independent(100, 20) == 5
    assert EV.effective_independent(100, 5) == 20
    assert EV.effective_independent(100, 1) == 100


def test_effective_count_never_exceeds_distinct_decision_dates():
    assert EV.effective_independent(100, 1, n_distinct_dates=7) == 7


def test_fifty_overlapping_twenty_day_bets_are_not_fifty_observations():
    outcomes = [{"horizon": 20, "effective_as_of": "2026-%02d-%02d"
                 % (9 + i // 28, 1 + i % 28),
                 "net_alpha_vs_control": 0.001,
                 "realised_net_return": 0.001,
                 "realised_gross_return": 0.002, "hit": True,
                 "turnover": 2.0} for i in range(50)]
    s = EV.summarise(outcomes, 20)
    assert s["raw_matured"] == 50
    assert s["effective_independent"] == 2
    verdict = EV.gate(s)
    assert verdict["state"] != C.FORWARD_CONFIRMED


def test_a_gate_needs_more_than_one_good_t_statistic():
    outcomes = [{"horizon": 1, "effective_as_of": "2026-09-%02d" % (i + 1),
                 "net_alpha_vs_control": 0.01,
                 "realised_net_return": 0.01,
                 "realised_gross_return": 0.01, "hit": True,
                 "turnover": 2.0} for i in range(3)]
    s = EV.summarise(outcomes, 1)
    verdict = EV.gate(s)
    assert verdict["state"] in (C.FORWARD_PENDING, C.EARLY_FORWARD_EVIDENCE)
    assert verdict["all_checks_passed"] is False


def test_pit_violation_rejects_regardless_of_performance():
    outcomes = [{"horizon": 1, "effective_as_of": "2026-09-%02d" % (i + 1),
                 "net_alpha_vs_control": 0.05, "realised_net_return": 0.05,
                 "realised_gross_return": 0.05, "hit": True, "turnover": 2.0}
                for i in range(80)]
    s = EV.summarise(outcomes, 1)
    verdict = EV.gate(s, pit_ok=False)
    assert verdict["state"] == C.FORWARD_REJECTED
    assert "PIT" in verdict["reject_reason"]


def test_retune_rejects_regardless_of_performance():
    outcomes = [{"horizon": 1, "effective_as_of": "2026-09-%02d" % (i + 1),
                 "net_alpha_vs_control": 0.05, "realised_net_return": 0.05,
                 "realised_gross_return": 0.05, "hit": True, "turnover": 2.0}
                for i in range(80)]
    s = EV.summarise(outcomes, 1)
    verdict = EV.gate(s, retune_free=False)
    assert verdict["state"] == C.FORWARD_REJECTED


def test_a_persistently_negative_challenger_is_killed():
    outcomes = [{"horizon": 1, "effective_as_of": "2026-09-%02d" % (i + 1),
                 "net_alpha_vs_control": -0.01, "realised_net_return": -0.01,
                 "realised_gross_return": -0.005, "hit": False,
                 "turnover": 2.0} for i in range(60)]
    s = EV.summarise(outcomes, 1)
    verdict = EV.gate(s)
    assert verdict["state"] == C.FORWARD_REJECTED
    assert verdict["rejected"] is True


def test_summarise_reports_single_day_concentration():
    outcomes = ([{"horizon": 1, "effective_as_of": "2026-09-01",
                  "net_alpha_vs_control": 1.0, "realised_net_return": 1.0,
                  "realised_gross_return": 1.0, "hit": True, "turnover": 2.0}]
                + [{"horizon": 1, "effective_as_of": "2026-09-%02d" % (i + 2),
                    "net_alpha_vs_control": 0.0001,
                    "realised_net_return": 0.0001,
                    "realised_gross_return": 0.0001, "hit": True,
                    "turnover": 2.0} for i in range(20)])
    s = EV.summarise(outcomes, 1)
    assert s["single_day_share_of_pnl"] > 0.9
    assert EV.gate(s)["checks"]["no_single_day_domination"] is False


def test_benjamini_hochberg_controls_the_family():
    out = EV.benjamini_hochberg([0.001, 0.9, 0.8, 0.7], fdr=0.10)
    assert out["n_tests"] == 4
    assert out["n_survivors"] == 1


# --------------------------------------------------------------------------- #
# 8. The judge
# --------------------------------------------------------------------------- #
def _pred_for_judge(**over) -> dict:
    row = {
        "prediction_id": "pj", "challenger_id": "cj",
        "challenger_version": "v1", "challenger_spec_hash": "h",
        "asset_class": "US_EQUITY", "horizon": 2,
        "effective_as_of": "2026-08-26",
        "benchmark": "CASH", "control": C.CONTROL_CASH,
        "cost_class": "US_EQUITY",
        "position_expression": {"legs": [
            {"instrument": "AAA", "weight": 0.5, "score": 1.0,
             "side": "LONG", "cost_class": "US_EQUITY"},
            {"instrument": "BBB", "weight": -0.5, "score": -1.0,
             "side": "SHORT", "cost_class": "US_EQUITY"}]},
    }
    row.update(over)
    return row


def _fake_series(monkeypatch, table):
    import pandas as pd

    def fake(sym):
        data = table.get(sym)
        if data is None:
            return None
        idx = pd.DatetimeIndex([pd.Timestamp(d) for d, _ in data])
        return pd.Series([float(v) for _, v in data], index=idx)

    monkeypatch.setattr(JD, "_series", fake)


def test_judge_reports_not_matured_before_the_horizon_passes(monkeypatch):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 101.0)],
        "BBB": [("2026-08-26", 50.0), ("2026-08-27", 50.0)]})
    out = JD.resolve(_pred_for_judge())
    assert out["state"] == "NOT_MATURED"


def test_judge_scores_a_matured_long_short_book(monkeypatch):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 110.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 90.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    out = JD.resolve(_pred_for_judge())
    assert out["state"] == "SCOREABLE"
    # +10% on the long half, -10% on the short half, each at half weight
    assert out["realised_gross_return"] == pytest.approx(0.10, abs=1e-9)
    # 2 sides x (5 bps + 1 bps slippage) x gross 1.0
    assert out["realised_cost"] == pytest.approx(0.0012, abs=1e-9)
    assert out["realised_net_return"] == pytest.approx(0.0988, abs=1e-9)
    assert out["net_alpha_vs_control"] == pytest.approx(0.0988, abs=1e-9)
    assert out["turnover"] == pytest.approx(2.0)
    assert out["hit"] is True


def test_judge_charges_cost_on_traded_notional_both_sides(monkeypatch):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 100.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 100.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    out = JD.resolve(_pred_for_judge())
    assert out["realised_gross_return"] == pytest.approx(0.0, abs=1e-12)
    assert out["realised_net_return"] < 0        # a flat market still costs
    assert out["realised_cost_entry_side"] == \
        pytest.approx(out["realised_cost_exit_side"])


def test_cash_control_is_remunerated_so_beating_zero_is_not_enough(monkeypatch):
    """R42's lesson: a premium priced below cash is not alpha."""
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 101.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 100.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.01)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    out = JD.resolve(_pred_for_judge())
    assert out["realised_net_return"] > 0
    assert out["net_alpha_vs_control"] < 0
    assert out["hit"] is False


def test_benchmark_control_subtracts_the_benchmarks_own_move(monkeypatch):
    _fake_series(monkeypatch, {
        "SPY": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 105.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    pred = _pred_for_judge(
        benchmark="SPY", control=C.CONTROL_BENCHMARK, cost_class="US_ETF",
        position_expression={"legs": [
            {"instrument": "SPY", "weight": 1.0, "score": 0.0,
             "side": "LONG", "cost_class": "US_ETF"}]})
    out = JD.resolve(pred)
    # holding the benchmark cannot beat the benchmark; it loses exactly cost
    assert out["net_alpha_vs_control"] == pytest.approx(
        -out["realised_cost"], abs=1e-12)


def test_judge_isolates_horizons(monkeypatch):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 110.0),
                ("2026-08-28", 100.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 100.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    h1 = JD.resolve(_pred_for_judge(horizon=1))
    h2 = JD.resolve(_pred_for_judge(horizon=2))
    assert h1["realised_gross_return"] == pytest.approx(0.05, abs=1e-9)
    assert h2["realised_gross_return"] == pytest.approx(0.0, abs=1e-9)


def test_judge_never_revises_a_forecast(monkeypatch, sandbox):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 110.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 90.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    row = _valid_row(prediction_id="pj", challenger_id="cj", horizon=2,
                     effective_as_of="2026-08-26",
                     control=C.CONTROL_CASH, benchmark="CASH")
    row["position_expression"] = _pred_for_judge()["position_expression"]
    LG.append_predictions([row], TEST_CAMPAIGN)
    before = json.dumps(LG.predictions(TEST_CAMPAIGN), sort_keys=True)
    JD.score_pending(TEST_CAMPAIGN)
    after = json.dumps(LG.predictions(TEST_CAMPAIGN), sort_keys=True)
    assert before == after
    assert len(LG.outcomes(TEST_CAMPAIGN)) == 1
    assert LG.verify(TEST_CAMPAIGN)["all_intact"] is True


def test_scoring_twice_scores_once(monkeypatch, sandbox):
    _fake_series(monkeypatch, {
        "AAA": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 110.0)],
        "BBB": [("2026-08-26", 100.0), ("2026-08-27", 100.0),
                ("2026-08-28", 90.0)]})
    monkeypatch.setattr(JD.MD, "risk_free_per_session", lambda h: 0.0)
    monkeypatch.setattr(JD.MD, "risk_free_annual", lambda: {"state": "OK"})
    row = _valid_row(prediction_id="pj", challenger_id="cj", horizon=2,
                     effective_as_of="2026-08-26")
    row["position_expression"] = _pred_for_judge()["position_expression"]
    LG.append_predictions([row], TEST_CAMPAIGN)
    a = JD.score_pending(TEST_CAMPAIGN)
    b = JD.score_pending(TEST_CAMPAIGN)
    assert a["n_newly_scored"] == 1
    assert b["n_newly_scored"] == 0
    assert len(LG.outcomes(TEST_CAMPAIGN)) == 1


# --------------------------------------------------------------------------- #
# 9. Adoption of prior releases
# --------------------------------------------------------------------------- #
def test_adoption_leaves_every_prior_registry_byte_identical():
    out = RG.adopt_prior_shadows()
    assert out["all_sources_unchanged"] is True
    for src in out["sources"]:
        if src["present"]:
            assert src["file_sha256_before"] == src["file_sha256_after"]


def test_adoption_deduplicates_shadows_relisted_by_a_later_release():
    out = RG.adopt_prior_shadows()
    ids = [a["challenger_id"] for a in out["adopted"]]
    assert len(ids) == len(set(ids))
    assert out["n_adopted"] <= out["n_registry_listings"]


def test_r46_never_writes_forward_rows_for_an_adopted_shadow():
    out = RG.adopt_prior_shadows()
    for a in out["adopted"]:
        assert a["r46_writes_forward_rows_for_it"] is False
        assert a["promotion_allowed"] is False
        assert a["forward_rows_owned_by"]


def test_adoption_rules_forbid_mutating_prior_artifacts():
    assert C.ADOPTION_RULES["prior_registries_are_read_only"] is True
    assert C.ADOPTION_RULES[
        "r46_never_writes_a_forward_row_for_an_adopted_shadow"] is True


# --------------------------------------------------------------------------- #
# 10. The leaderboard
# --------------------------------------------------------------------------- #
def test_leaderboard_never_reads_proven(sandbox):
    """No VALUE on the board may claim proof.

    The string ``proven_alpha_is_not_a_state`` appears as a KEY, which is the
    honesty rail itself; what must never appear is a state, verdict or label
    whose value tells an operator something was proven.
    """
    reg = RG.register(TEST_CAMPAIGN)
    board = LB.build(TEST_CAMPAIGN, reg)
    assert board["no_row_may_read_proven"] is True

    def values(node):
        if isinstance(node, dict):
            for v in node.values():
                yield from values(v)
        elif isinstance(node, list):
            for v in node:
                yield from values(v)
        elif isinstance(node, str):
            yield node

    for text in values(board):
        assert "PROVEN" not in text.upper(), text

    for row in board["rows"]:
        assert row["state"] in C.CHALLENGER_STATES
        assert row["state"] != "PROVEN_ALPHA"
        assert row["promotion_allowed"] is False


def test_leaderboard_ranks_evidence_before_edge():
    mature = {"state": C.FORWARD_CANDIDATE, "effective_independent": 500,
              "net_alpha_bps": 1.0}
    lucky = {"state": C.FORWARD_CANDIDATE, "effective_independent": 2,
             "net_alpha_bps": 900.0}
    assert LB._rank_key(mature) < LB._rank_key(lucky)


def test_leaderboard_shows_raw_and_effective_counts_together(sandbox):
    reg = RG.register(TEST_CAMPAIGN)
    board = LB.build(TEST_CAMPAIGN, reg)
    for row in board["rows"]:
        assert "raw_matured" in row
        assert "effective_independent" in row


def test_blocked_adopted_shadows_appear_with_their_reason(sandbox):
    reg = RG.register(TEST_CAMPAIGN)
    board = LB.build(TEST_CAMPAIGN, reg)
    adopted = [r for r in board["rows"]
               if r["origin"] == "ADOPTED_PRIOR_RELEASE"]
    assert adopted, "the orphan shadows must be visible on the one board"
    for row in adopted:
        assert row["forward_predictions_matured"] == 0
        if row["state"] == C.DATA_BLOCKED:
            assert row["blocked_reason"]


# --------------------------------------------------------------------------- #
# 11. Registration and retune detection
# --------------------------------------------------------------------------- #
def test_registration_is_idempotent_and_preserves_the_freeze(sandbox):
    a = RG.register(TEST_CAMPAIGN)
    b = RG.register(TEST_CAMPAIGN)
    fa = {c["challenger_id"]: c["frozen_at"] for c in a["challengers"]}
    fb = {c["challenger_id"]: c["frozen_at"] for c in b["challengers"]}
    assert fa == fb
    assert b["retune_free"] is True


def test_editing_a_registered_spec_in_place_is_detected_as_a_retune(sandbox):
    RG.register(TEST_CAMPAIGN)
    tampered = []
    for spec in CH.SEED_SPECS:
        s = copy.deepcopy(dict(spec))
        if s["challenger_id"] == "r46_eq_xs_mom_12_1":
            s["parameters"] = dict(s["parameters"])
            s["parameters"]["formation_days"] = 60
        tampered.append(s)
    out = RG.register(TEST_CAMPAIGN, specs=tampered)
    assert out["retune_free"] is False
    ids = [r["challenger_id"] for r in out["retunes_detected"]]
    assert "r46_eq_xs_mom_12_1" in ids
    assert out["retunes_detected"][0]["verdict"] == "RETUNE_DETECTED"


def test_registry_records_no_hero_candidate(sandbox):
    reg = RG.register(TEST_CAMPAIGN)
    assert reg["no_hero_candidate"] is True
    for c in reg["challengers"]:
        assert c["historical_qualification_state"] == C.HISTORICAL_ONLY
        assert c["promotion_allowed"] is False


# --------------------------------------------------------------------------- #
# 12. Feasibility
# --------------------------------------------------------------------------- #
def test_feasibility_flags_a_stale_stream(monkeypatch):
    monkeypatch.setattr(FE.MD, "last_session",
                        lambda s: dt.date(2026, 1, 1))
    out = FE.probe(CH.SEED_SPECS[0], reference=dt.date(2026, 8, 25))
    assert out["state"] == FE.DATA_STALE


def test_feasibility_flags_a_missing_stream(monkeypatch):
    monkeypatch.setattr(FE.MD, "last_session", lambda s: None)
    out = FE.probe(CH.SEED_SPECS[0], reference=dt.date(2026, 8, 25))
    assert out["state"] == FE.NO_DATA


def test_the_btc_shadows_are_reported_venue_blocked():
    state = FE.adopted_stream_state(
        {"shadow_id": "shadow_btc_funding_carry_1d", "source_release": "R41"})
    assert state["state"] == FE.VENUE_BLOCKED
    assert state["can_accrue_today"] is False
    assert "451" in state["reason"]


def test_a_blocked_challenger_does_not_block_the_others(sandbox, monkeypatch):
    """One dead stream must not take the tournament down with it.

    Fully stubbed rather than provider-dependent: every symbol is fresh except
    the VIX pair, which returns nothing at all. (R52 fix: freshness derives
    from the canonical clock; the original pinned 2026-08-25, which expired
    once the calendar moved past the feasibility MAX_LAG and blocked EVERY
    challenger instead of only the VIX readers.)
    """
    from alpha_agent.r46 import clock as CK
    fresh = CK.eastern_date(CK.now_utc())

    def selective(sym):
        return None if sym in ("&VX", "$VIX") else fresh

    monkeypatch.setattr(FE.MD, "last_session", selective)
    reg = RG.register(TEST_CAMPAIGN)
    states = {c["challenger_id"]: c["state"] for c in reg["challengers"]}
    # Release 46.3 added a second cell on the SAME VIX data path (the
    # one-session carry clock); a dead stream correctly blocks every
    # challenger that reads it, and only those.
    vx_readers = {"r46_vx_term_carry_5d", "r46_3_vx_term_carry_1d"}
    for cid in vx_readers & set(states):
        assert states[cid] == C.DATA_BLOCKED, cid
    others = {k: v for k, v in states.items() if k not in vx_readers}
    assert others, "there must be other challengers to survive"
    assert all(v == C.FORWARD_PENDING for v in others.values()), others


def test_the_owned_provider_survives_a_strict_warning_filter():
    """A DeprecationWarning inside a vendor import must never decide whether
    the estate can read its own data.

    ``norgatedata`` calls the deprecated ``logging.warn`` when it loads. Under
    pytest's warnings-as-errors configuration that raised, every loader
    returned None, and the feasibility gate reported that a fully entitled,
    locally served, nightly-updated database did not exist.
    """
    import warnings as _w
    with _w.catch_warnings():
        _w.simplefilter("error")
        state = FE.MD.provider_state()
    if state.get("state") == "NOT_CONFIGURED":
        pytest.skip("norgatedata is not installed on this machine")
    assert state["state"] in ("OK", "DEGRADED")
    assert FE.MD.available() is True


# --------------------------------------------------------------------------- #
# 12b. Non-positive prices - refused, never turned into a NaN
# --------------------------------------------------------------------------- #
def test_a_return_across_a_negative_price_is_refused_not_nan():
    """Continuous WTI really did print -37.63 on 2020-04-20.

    A percentage return across that settlement is undefined. It must come back
    as None and be recorded as skipped, because a NaN that reaches a ranking
    silently becomes a position.
    """
    import numpy as np
    import pandas as pd
    from alpha_agent.r46 import marketdata as MD
    idx = pd.date_range("2026-01-01", periods=10, freq="D")
    bad = pd.Series([50.0, 45.0, -37.63, 20.0, 30.0, 35.0, 40.0, 42.0,
                     44.0, 46.0], index=idx)
    assert MD.has_non_positive(bad) is True
    assert MD.realised_vol(bad, 8) is None
    assert MD.total_return(bad, 7) is None
    assert MD.beta_to(bad, bad, 8) is None
    with np.errstate(all="raise"):
        assert MD.realised_vol(bad, 8) is None


def test_a_clean_window_after_a_negative_print_is_still_usable():
    import pandas as pd
    from alpha_agent.r46 import marketdata as MD
    idx = pd.date_range("2026-01-01", periods=12, freq="D")
    s = pd.Series([-5.0, 10.0, 11.0, 12.0, 11.5, 12.5, 13.0, 12.0,
                   13.5, 14.0, 13.0, 14.5], index=idx)
    assert MD.has_non_positive(s) is True          # the whole series
    assert MD.has_non_positive(s, 6) is False      # the recent window
    assert MD.realised_vol(s, 5) is not None


def test_futures_lanes_record_a_refused_market_rather_than_dropping_it(
        monkeypatch):
    import pandas as pd
    from alpha_agent.r46 import marketdata as MD
    idx = pd.date_range("2020-01-01", periods=400, freq="B")
    negative = pd.Series([100.0] * 200 + [-5.0] + [100.0] * 199, index=idx)
    real_closes = CH.MD.closes

    def patched(sym):
        return negative if sym == "&CL" else real_closes(sym)

    monkeypatch.setattr(CH.MD, "closes", patched)
    spec = CH.spec_by_id("r46_comdty_xs_mom_252")
    out = CH._commodity_cross_section(spec)
    reasons = {s["instrument"]: s["why"] for s in out.get("skipped", [])}
    assert reasons.get("&CL") == MD.NON_POSITIVE_PRICE


# --------------------------------------------------------------------------- #
# 13. Burden
# --------------------------------------------------------------------------- #
def test_r46_charges_no_new_historical_trials(sandbox):
    out = BD.historical(TEST_CAMPAIGN)
    assert out["new_r46_effective_trials"] == 0
    assert out["GLOBAL_SEARCH_BURDEN"] == C.INHERITED_GLOBAL_BURDEN
    assert out["burden_may_never_be_reset"] is True


def test_forward_evidence_is_not_search_burden(sandbox):
    out = BD.prospective(TEST_CAMPAIGN)
    assert out["these_are_not_search_trials"] is True


def test_a_choice_made_after_seeing_forward_results_is_recorded(sandbox):
    BD.record_forward_selection(
        {"decision": "promoted a threshold after reading forward results",
         "challenger_id": "x"}, TEST_CAMPAIGN)
    out = BD.forward_selections(TEST_CAMPAIGN)
    assert out["n_forward_selections"] == 1


# --------------------------------------------------------------------------- #
# 14. Emission
# --------------------------------------------------------------------------- #
def test_emission_refuses_when_the_window_is_not_forward(sandbox, monkeypatch):
    reg = RG.register(TEST_CAMPAIGN)
    monkeypatch.setattr(EM.CK, "is_true_forward", lambda a, b: False)
    out = EM.emit(TEST_CAMPAIGN, reg)
    assert out["state"] == "REFUSED_NOT_TRUE_FORWARD"
    assert out["n_appended"] == 0
    assert LG.predictions(TEST_CAMPAIGN) == []


def test_emitted_rows_carry_every_contract_field(sandbox):
    reg = RG.register(TEST_CAMPAIGN)
    batch = EM.build_batch(TEST_CAMPAIGN, reg)
    for row in batch["rows"]:
        for field in C.PREDICTION_RECORD_FIELDS:
            assert field in row, field
        assert row["forward_evidence_type"] == C.TRUE_FORWARD
        assert row["status"] == C.STATUS_PENDING
        assert row["emitted_at_utc"] < row["outcome_window_start_utc"]


def test_expected_return_is_declared_uncalibrated_not_invented(sandbox):
    reg = RG.register(TEST_CAMPAIGN)
    batch = EM.build_batch(TEST_CAMPAIGN, reg)
    for row in batch["rows"]:
        assert row["expected_return"] is None
        assert row["expected_return_state"] == "NOT_CALIBRATED"
        assert row["expected_cost"] is not None and row["expected_cost"] > 0


def test_a_flat_rule_emits_nothing_and_says_why(sandbox, monkeypatch):
    reg = RG.register(TEST_CAMPAIGN)
    monkeypatch.setattr(EM.CH, "build",
                        lambda spec: {"state": "OK", "legs": [],
                                      "n_legs": 0, "gross_notional": 0.0,
                                      "net_notional": 0.0})
    batch = EM.build_batch(TEST_CAMPAIGN, reg)
    assert batch["n_predictions"] == 0
    assert all(s["reason"] == "FLAT_NO_POSITION" for s in batch["skipped"])


# --------------------------------------------------------------------------- #
# 14b. Options lane - the surface paths, and a budget that bought nothing
# --------------------------------------------------------------------------- #
def test_prior_option_surfaces_are_at_the_research_root_not_the_campaign_dir():
    """The acquired surfaces live at each release's RESEARCH ROOT.

    Pointing these at the campaign subdirectory is silent: every loader
    returns None, the combined surface reports zero prior sessions, and the
    dedup that stops R46 re-buying an expiry a prior release already paid for
    never fires. That is exactly what happened on the first attempt.
    """
    from alpha_agent.r46 import options as OP
    for p in (OP.R44_SURFACE, OP.R44_SURFACE_TERM, OP.R45_SURFACE):
        assert "_data_options" in str(p)
        assert "r44_orthogonal_portfolio_alpha_v1" not in str(p)
        assert "r45_macro_event_alpha_v1" not in str(p)
    if not OP.R44_SURFACE.exists():
        pytest.skip("prior option surfaces not present on this machine")
    prev = OP.existing_surface()
    assert prev is not None and len(prev) > 0


def test_expiry_enumeration_includes_third_fridays():
    """Dedup against the DATA, never against an assumption about it.

    Excluding third Fridays by rule - on the grounds that prior releases
    sampled them - made 2026-06-19 and 2026-08-21 permanently unreachable,
    and the prior surface holds neither.
    """
    from alpha_agent.r46 import options as OP
    got = OP._weekly_expiries(dt.date(2026, 6, 1), dt.date(2026, 8, 31))
    assert got
    for d in got:
        assert d.weekday() == 4
    assert dt.date(2026, 6, 19) in got
    assert dt.date(2026, 8, 21) in got


def test_recent_embargo_does_not_hide_the_useful_expiries():
    """Only settlement slack, not a twenty-day blackout.

    The two most recently expired weeklies are the only ones carrying session
    dates the surface does not already hold; a twenty-day embargo put exactly
    those out of reach while the lane reported the gap as free to close.
    """
    from alpha_agent.r46 import options as OP
    assert OP.ENTITLEMENT_RECENT_EMBARGO_DAYS <= 7
    assert OP.ENTITLEMENT_LOOKBACK_DAYS == 700


def test_options_lane_reports_a_budget_that_bought_no_new_sessions(sandbox):
    """A budget can be fully spent, return real data, and buy nothing.

    R46's first weekly batch spent all 120 calls on the OLDEST fourteen
    expiries and added 106 contracts, 2,195 rows and ZERO new session dates,
    because the surface already covered every date they traded on. The lane
    must be able to say so rather than report the row count as progress.

    Release 46.6.1 added the ``sandbox`` fixture. Without it this test called
    the lane owner at the PRODUCTION research root and the owner rewrote the
    production ``R46_OPTIONS_LANE.json`` on every run of the suite - a real
    write with no owner, of exactly the class the write-attribution gate
    exists to catch. It read the same prior surfaces and spent nothing, so no
    science moved; the test now writes where every other test writes.
    """
    from alpha_agent.r46 import options as OP
    if not OP.R44_SURFACE.exists():
        pytest.skip("prior option surfaces not present on this machine")
    body = OP.run(acquire=False, campaign_id=TEST_CAMPAIGN)
    assert "prior_surface_state" in body
    assert body["prior_surface_state"]["readable"] is True
    assert body["prior_surface_state"]["n_prior_sessions"] > 0
    js = body["judgeable_sample"]
    assert js["sessions_required"] == 500
    # sessions_added is derived from the COMBINED surface, never from row count
    assert body["surface"]["sessions_added_by_r46"] == (
        js["usable_sessions_now"] - body["surface"]["sessions_before_r46"])
    assert body["money_spent_usd"] == 0.0


def test_options_hypotheses_are_frozen_and_hashed():
    from alpha_agent.r46 import options as OP
    assert len(OP.PREDECLARED_HYPOTHESES) >= 3
    assert OP.hypotheses_hash() == OP.hypotheses_hash()
    for h in OP.PREDECLARED_HYPOTHESES:
        for f in ("hypothesis_id", "statement", "signal", "position",
                  "horizon_sessions", "control", "cost_model",
                  "fit_window", "judge_window", "why_not_short_vol"):
            assert h.get(f), (h.get("hypothesis_id"), f)
    assert "GENERIC_SHORT_VOLATILITY" in OP.EXCLUDED_BY_NAME


def test_analyst_challenger_is_predeclared_and_refuses_vendor_strips():
    from alpha_agent.r46 import analyst as AN
    spec = AN.PREDECLARED_CHALLENGER
    assert spec["admissible_input"] == "PROSPECTIVELY CAPTURED SNAPSHOTS ONLY"
    assert "backward strip" in spec["inadmissible_input"]
    assert spec["promotion_allowed"] is False
    assert AN.NEVER_BACKFILLED is True
    assert AN.predeclaration_hash() == AN.predeclaration_hash()


# --------------------------------------------------------------------------- #
# 15. Shell policy - disclosed, never waived
# --------------------------------------------------------------------------- #
def test_shell_policy_record_is_honest_about_the_session():
    rec = SP.record()
    assert rec["SHELL_POLICY_VIOLATION"] in ("YES", "NO")
    assert rec["waivers_available"] is False
    if rec["r46_violation"]:
        assert rec["r46_event_count"] == len(rec["r46_events"])
        assert rec["operator_decision_required"] is True
        assert rec["contract_token_if_violation"] == \
            "DO_NOT_COMMIT - R46_SHELL_POLICY_VIOLATION"
        assert rec["repo_source_written_by_prohibited_shell"] is False


def test_prior_release_disclosures_are_never_erased():
    rec = SP.record()
    releases = {d["release"] for d in rec["inherited_disclosures"]}
    assert {"R42", "R44", "R45"}.issubset(releases)
    assert rec["inherited_disclosures_are_never_erased"] is True


# --------------------------------------------------------------------------- #
# 16. Safety - nothing here can move money
# --------------------------------------------------------------------------- #
def test_no_r46_module_creates_an_order_or_promotes_a_model():
    root = Path(__file__).resolve().parents[1] / "alpha_agent" / "r46"
    banned = ("create_order", "submit_order", "place_order", "promote_model",
              "activate_sleeve", "mutate_portfolio", "execute_trade")
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in banned:
            assert ("def " + token) not in text, (path.name, token)


def test_r46_writes_only_under_its_own_research_root():
    assert str(R46.RESEARCH_ROOT).endswith("prospective_alpha_tournament_r46")


def test_portfolio_boundary_is_declared():
    assert C.PORTFOLIO_BOUNDARY["FORWARD_CANDIDATE_is_an_order"] is False
    assert C.PORTFOLIO_BOUNDARY[
        "FORWARD_CONFIRMED_is_an_automatic_holding"] is False
    assert C.PORTFOLIO_BOUNDARY["manual_review_remains_mandatory"] is True


# --------------------------------------------------------------------------- #
# 17. The read model
# --------------------------------------------------------------------------- #
def test_read_model_answers_the_operator_questions(sandbox, monkeypatch):
    from paper_trader.api import prospective_tournament as PT
    monkeypatch.setattr(PT, "DEFAULT_RESEARCH_ROOT", R46.RESEARCH_ROOT)
    monkeypatch.delenv(PT.RESEARCH_ROOT_ENV, raising=False)
    reg = RG.register(TEST_CAMPAIGN)
    LB.build(TEST_CAMPAIGN, reg)
    view = PT.load_prospective_tournament(TEST_CAMPAIGN)
    for key in ("how_many_models_are_competing",
                "how_many_real_forward_predictions_exist",
                "how_many_have_matured", "which_are_winning",
                "which_are_losing", "which_are_too_early_to_judge",
                "which_were_killed", "best_net_alpha_vs_control_bps",
                "forward_evidence_confidence", "next_material_maturity"):
        assert key in view
    assert view["no_historical_only_model_looks_proven"] is True
    assert view["proven_alpha_is_not_a_state"] is True
    assert view["no_live_trading"]["creates_orders"] is False
    assert view["no_live_trading"]["promotes_models"] is False
    assert "NO ORDERS" in view["safety_badges"]


def test_read_model_degrades_without_artifacts(tmp_path, monkeypatch):
    from paper_trader.api import prospective_tournament as PT
    monkeypatch.setattr(PT, "DEFAULT_RESEARCH_ROOT", tmp_path / "absent")
    monkeypatch.delenv(PT.RESEARCH_ROOT_ENV, raising=False)
    view = PT.load_prospective_tournament("nothing_here")
    assert view["state"] == PT.STATE_UNAVAILABLE
    assert view["warnings"]
    assert view["how_many_real_forward_predictions_exist"] == 0


# --------------------------------------------------------------------------- #
# 18. Prior-release compatibility
# --------------------------------------------------------------------------- #
def test_r45_contract_still_imports_and_keeps_its_burden():
    from alpha_agent.r45 import contract as C45
    assert C45.RELEASE == "R45"
    assert C.INHERITED_GLOBAL_BURDEN == 353


def test_r46_reuses_the_canonical_desk_ledger_primitives():
    from paper_trader.api import paper_trading_desk as desk
    assert hasattr(desk, "_append_ledger")
    assert hasattr(desk, "verify_ledger")
    assert LG._desk() is desk


def test_prior_release_shadow_registries_are_not_writable_by_r46():
    src = Path(C.ADOPTED_REGISTRY_SOURCES[0]["path"])
    if not src.exists():
        pytest.skip("R39 registry not present on this machine")
    before = hashlib.sha256(src.read_bytes()).hexdigest()
    RG.adopt_prior_shadows()
    assert hashlib.sha256(src.read_bytes()).hexdigest() == before
