r"""Post-R99 pipeline repair - PF1 cost inputs, one horizon/cadence, frozen source, G7 admission.

Isolation: synthetic panels, ``tmp_path`` stores and a FAKE pipeline. Nothing here
opens the live R59 research memory, a production store or a campaign folder.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import pytest

from alpha_agent import r59
from alpha_agent.agents_v2 import books as B
from alpha_agent.agents_v2 import provenance as PV
from alpha_agent.agents_v2 import runner as RN
from alpha_agent.r59 import native
from alpha_agent.r61 import cost_budget as CB

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "campaign_r56_v2_evaluator_post_r99",
    ROOT / "research" / "agents" / "campaign_r56_v2" / "evaluator.py")
EV = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(EV)

pytestmark = pytest.mark.filterwarnings("ignore:All-NaN slice encountered:RuntimeWarning")

#: R99 R02 (H_d1181435_4f695f30a9e1) lockbox stats as stored in results_w1.json -
#: a ZF/ZB dated-contract book, cadence = horizon = 1, rates-class cost 2.0 bp/side.
R02_LOCKBOX = {"periods": 945, "overlap_factor": 1.0,
               "mean_oneway_turnover_per_period": 0.03878261661194685,
               "ann_rebalance_cost_drag": 0.0039092877544842424,
               "ann_roll_cost_drag": 0.003293553925862946,
               "ann_cost_drag": 0.007202841680347188}


# --------------------------------------------------------------------------- #
# PF1 - dated-contract cost inputs                                             #
# --------------------------------------------------------------------------- #
def test_pf1_r02_reproduces_two_bp_per_side():
    rate, roll = RN.effective_cost_inputs_of(R02_LOCKBOX, horizon_sessions=1)
    assert rate == pytest.approx(0.0002, abs=1e-9)
    assert roll == pytest.approx(0.003293553925862946, abs=1e-15)


def test_pf1_without_the_horizon_the_rate_fails_closed():
    # The horizon is not in the layer stats; guessing it is how a cost goes unpaid.
    assert RN.effective_cost_inputs_of(R02_LOCKBOX) == (None, pytest.approx(0.003293553925862946))
    assert RN.effective_cost_inputs_of(R02_LOCKBOX, horizon_sessions=0)[0] is None


@pytest.mark.parametrize("turnover", [0.0, None])
def test_pf1_zero_or_missing_turnover_fails_closed(turnover):
    s = dict(R02_LOCKBOX)
    if turnover is None:
        s.pop("mean_oneway_turnover_per_period")
    else:
        s["mean_oneway_turnover_per_period"] = turnover
    assert RN.effective_cost_inputs_of(s, horizon_sessions=1)[0] is None


def test_pf1_missing_rebalance_drag_fails_closed():
    s = {k: v for k, v in R02_LOCKBOX.items() if k != "ann_rebalance_cost_drag"}
    assert RN.effective_cost_inputs_of(s, horizon_sessions=1)[0] is None


def test_pf1_event_book_behaviour_is_unchanged():
    # The event book reports BOTH keys; its own annualised turnover wins and the
    # horizon argument is irrelevant to it.
    s = {"ann_rebalance_cost_drag": 0.004, "ann_oneway_turnover": 4.0,
         "mean_oneway_turnover_per_period": 0.5, "ann_roll_cost_drag": 0.001}
    for h in (None, 1, 5):
        assert RN.effective_cost_inputs_of(s, horizon_sessions=h) == (pytest.approx(0.0005), pytest.approx(0.001))


def test_pf1_idempotent_and_does_not_mutate_stats():
    s = dict(R02_LOCKBOX)
    a = RN.effective_cost_inputs_of(s, horizon_sessions=1)
    b = RN.effective_cost_inputs_of(s, horizon_sessions=1)
    assert a == b and s == R02_LOCKBOX and "ann_oneway_turnover" not in s


def _dates(start="2015-01-02", end="2026-07-01"):
    d = np.arange(start, end, dtype="datetime64[D]")
    return d[np.is_busday(d)].astype(str)


@pytest.fixture()
def futures_layer(monkeypatch):
    rng = np.random.default_rng(11)
    dates = _dates()
    n_m, n_d = 12, len(dates)
    nanf = np.full((n_m, n_d), np.nan)
    roll = np.zeros((n_m, n_d), dtype=np.uint8)
    for i in range(n_m):
        roll[i, (11 + 5 * i)::63] = 1
    layer = {"symbols": ["M%02d" % i for i in range(n_m)], "dates": dates,
             "ret": rng.normal(0.0001, 0.01, (n_m, n_d)), "ret2": nanf, "slope": nanf,
             "open_interest": nanf, "volume": nanf, "roll": roll,
             # a per-market cost VECTOR, as R38 futures carry
             "cost_per_side": np.linspace(0.0002, 0.0015, n_m)}
    meta = {s: {"asset_class": "COMMODITY", "economic_group": "G", "cost_bps_per_side": 5.0}
            for s in layer["symbols"]}
    monkeypatch.setattr(native, "_CACHE", {"layer": layer, "meta": meta})
    return layer


@pytest.mark.parametrize("horizon,cadence", [(1, 1), (5, 5), (5, 1), (21, 5)])
def test_pf1_rate_equals_the_notional_weighted_rate_the_book_paid(futures_layer, horizon, cadence):
    ret = futures_layer["ret"]

    def weights(t, live):
        return native._xs_weights(np.nansum(ret[:, max(0, t - 40):t], axis=1), live)

    res = EV.run_futures_book(futures_layer, weights, horizon=horizon, cadence=cadence,
                              label="pf1", charge_rolls=True)
    sel = res["series"]["layer"] == "L"
    exact = (float(res["series"]["rebalance_cost"][sel].mean())
             / (2.0 * float(res["series"]["turnover_oneway"][sel].mean())))
    stats = res["layers"]["L"]
    rate, roll = RN.effective_cost_inputs_of(stats, horizon_sessions=horizon)
    assert rate == pytest.approx(exact, rel=1e-12)
    assert 0.0002 <= rate <= 0.0015
    assert roll == pytest.approx(stats["ann_roll_cost_drag"])
    if cadence != horizon:
        # PF1_PROPOSED_PATCH.md annualised by 252 / cadence; that is wrong on an
        # overlapping book by exactly horizon / cadence.
        wrong = stats["ann_rebalance_cost_drag"] / (
            2.0 * stats["mean_oneway_turnover_per_period"] * 252.0 / cadence)
        assert wrong == pytest.approx(rate * cadence / horizon, rel=1e-12)


def test_pf1_cost_budget_becomes_evaluable_and_reproduces_the_lockbox_drag():
    spec = {"cost_model": {"cost_model_id": "FUTURES_PER_MARKET_R38_PLUS_ROLL_V1",
                           "rate_per_side": "per market, 2-15 bp"},
            "parameters": {"cadence_sessions": 1, "horizon_sessions": 1}}
    before = CB.evaluate_frozen_spec(spec, one_way_turnover=R02_LOCKBOX["mean_oneway_turnover_per_period"],
                                     cost_per_side=None,
                                     additional_ann_cost_drag=R02_LOCKBOX["ann_roll_cost_drag"])
    assert before["state"] == CB.BUDGET_NOT_EVALUABLE and before["passed"] is False
    rate, roll = RN.effective_cost_inputs_of(R02_LOCKBOX, horizon_sessions=1)
    after = CB.evaluate_frozen_spec(spec, one_way_turnover=R02_LOCKBOX["mean_oneway_turnover_per_period"],
                                    cost_per_side=rate, additional_ann_cost_drag=roll)
    assert after["state"] != CB.BUDGET_NOT_EVALUABLE
    # tiled cadence-1 book: the budget's projected drag IS the lockbox's measured drag
    assert after["measured"] == pytest.approx(R02_LOCKBOX["ann_cost_drag"], rel=1e-9)
    assert after["annualized_rebalance_cost_drag"] == pytest.approx(R02_LOCKBOX["ann_rebalance_cost_drag"], rel=1e-9)


# --------------------------------------------------------------------------- #
# PF2 - one horizon, one cadence                                               #
# --------------------------------------------------------------------------- #
def _plan(h=None, c=None):
    kw = {}
    if h is not None:
        kw["horizon"] = h
    if c is not None:
        kw["cadence"] = c
    return {"book": B.BOOK_FUTURES, "book_kwargs": kw}


def test_horizon_equal_to_the_frozen_one_passes():
    got = PV.check_horizon_cadence({"horizon_sessions": 1, "parameters": {"cadence_sessions": 1}},
                                   _plan(1, 1), default_horizon=21, default_cadence=21)
    assert got == {"horizon_sessions": 1, "cadence_sessions": 1}


def test_r99_pf2_shape_is_refused():
    # the cell table said h=21; an import-time override executed h=1
    with pytest.raises(PV.ProvenanceRefusal) as e:
        PV.check_horizon_cadence({"horizon_sessions": 21}, _plan(1, 1), default_horizon=21, default_cadence=21)
    assert e.value.code == "HORIZON_NOT_PREREGISTERED"


def test_book_defaults_count_as_executed_values():
    with pytest.raises(PV.ProvenanceRefusal):
        PV.check_horizon_cadence({"horizon_sessions": 1}, _plan(), default_horizon=21, default_cadence=21)


@pytest.mark.parametrize("frozen", [{}, {"horizon_sessions": 1, "parameters": {"horizon_sessions": 21}},
                                    {"horizon_sessions": 1, "cadence_sessions": 5}])
def test_missing_or_dual_or_divergent_frozen_values_are_refused(frozen):
    with pytest.raises(PV.ProvenanceRefusal):
        PV.check_horizon_cadence(frozen, _plan(1, 1), default_horizon=21, default_cadence=21)


# --------------------------------------------------------------------------- #
# PF4 - frozen, content-addressed source                                       #
# --------------------------------------------------------------------------- #
def _source(tmp_path):
    repo = tmp_path / "repo"
    (repo / "camp").mkdir(parents=True)
    f = repo / "camp" / "cells.py"
    f.write_text("CELLS = {'A': {'h': 1}}\n", encoding="utf-8")
    return repo, f


def test_snapshot_verify_and_materialize_round_trip(tmp_path):
    repo, f = _source(tmp_path)
    store = tmp_path / "store"
    m = PV.snapshot([f], repo_root=repo, store_dir=store)
    assert list(m["files"]) == ["camp/cells.py"]
    assert PV.verify(m, repo_root=repo, store_dir=store) == []
    out = PV.materialize(m, store_dir=store, dest_dir=tmp_path / "rebuilt")
    assert (out / "camp" / "cells.py").read_bytes() == f.read_bytes()
    assert PV.snapshot([f], repo_root=repo, store_dir=store) == m        # idempotent


def test_a_later_wave_edit_fails_closed_and_the_frozen_bytes_survive(tmp_path):
    repo, f = _source(tmp_path)
    store = tmp_path / "store"
    m = PV.snapshot([f], repo_root=repo, store_dir=store)
    original = f.read_bytes()
    f.write_text("CELLS = {'A': {'h': 1}, 'B': {'h': 1}}\n", encoding="utf-8")    # wave 2 appends
    with pytest.raises(PV.ProvenanceRefusal) as e:
        PV.require_frozen_source({"parameters": {"frozen_source": m}}, required=True,
                                 repo_root=repo, store_dir=store)
    assert e.value.code == "FROZEN_SOURCE_MISMATCH"
    rebuilt = PV.materialize(m, store_dir=store, dest_dir=tmp_path / "r")
    assert (rebuilt / "camp" / "cells.py").read_bytes() == original


def test_missing_blob_or_tampered_manifest_or_absent_manifest_refuse(tmp_path):
    repo, f = _source(tmp_path)
    store = tmp_path / "store"
    m = PV.snapshot([f], repo_root=repo, store_dir=store)
    bad = dict(m, files={"camp/cells.py": "0" * 64})
    assert PV.verify(bad, repo_root=repo)
    (store / m["files"]["camp/cells.py"]).unlink()
    with pytest.raises(PV.ProvenanceRefusal):
        PV.require_frozen_source({"parameters": {"frozen_source": m}}, required=False,
                                 repo_root=repo, store_dir=store)
    with pytest.raises(PV.ProvenanceRefusal) as e:
        PV.require_frozen_source({"parameters": {}}, required=True, repo_root=repo, store_dir=store)
    assert e.value.code == "FROZEN_SOURCE_NOT_PREREGISTERED"
    assert PV.require_frozen_source({"parameters": {}}, required=False, repo_root=repo, store_dir=None) is None


# --------------------------------------------------------------------------- #
# C04 - director admission                                                     #
# --------------------------------------------------------------------------- #
def _rulings(tmp_path):
    w1 = tmp_path / "RULING_W1.json"
    w1.write_text(json.dumps({"rulings": {"C04": {"G1": "PASS", "G7": "PASS"},
                                          "C01": {"G1": "FAIL", "G7": "FAIL"}}}), encoding="utf-8")
    w1a = tmp_path / "RULING_W1A.json"
    w1a.write_text(json.dumps({"rulings": {"C04": {"G1": "FAIL", "G7": "FAIL"},
                                           "C01": {"G1": "PASS", "G7": "PASS"}}}), encoding="utf-8")
    return w1, w1a


def test_latest_ruling_wins_in_declared_order(tmp_path):
    w1, w1a = _rulings(tmp_path)
    assert PV.require_admission("C04", [w1])["g7"] == "PASS"
    with pytest.raises(PV.ProvenanceRefusal) as e:
        PV.require_admission("C04", [w1, w1a])
    assert e.value.code == "ADMISSION_NOT_G7_PASS"
    assert PV.require_admission("C01", [w1, w1a])["ruling_file"] == "RULING_W1A.json"
    with pytest.raises(PV.ProvenanceRefusal):
        PV.require_admission("C99", [w1, w1a])                       # never ruled
    with pytest.raises(PV.ProvenanceRefusal):
        PV.require_admission("C04", [])                              # a declared, empty ledger
    assert PV.require_admission("C04", None) is None                 # no ledger declared (pre-R100)


def test_post_measurement_withdrawal_is_a_visible_governance_exception(tmp_path):
    w1, w1a = _rulings(tmp_path)
    exc = PV.admission_exceptions(["C04", "C01"], [w1, w1a])
    assert [x["cell"] for x in exc] == ["C04"]
    assert exc[0]["exception"] == PV.EXC_POST_MEASUREMENT_WITHDRAWAL
    assert [h["g7"] for h in exc[0]["history"]] == ["PASS", "FAIL"]


# --------------------------------------------------------------------------- #
# The runner closes every door BEFORE the first layer is read                  #
# --------------------------------------------------------------------------- #
class _Mem:
    def __init__(self, frozen):
        self._frozen = frozen

    def get(self, eid):
        return {"spec": self._frozen}


class _Pipe:
    """No memory, no writes: any write attempt is a test failure."""

    def __init__(self, frozen):
        self.mem = _Mem(frozen)

    def _latest(self, *a, **k):
        return None

    def perform(self, *a, **k):
        raise AssertionError("a guard must refuse before any reveal/submit")


def _executors(h, c):
    def fn(row):
        # panel None: reaching the book would FAIL, never REFUSE
        return {"book": B.BOOK_FUTURES, "panel": None, "score_fn": lambda t, live: None,
                "book_kwargs": {"horizon": h, "cadence": c}}
    return {"X_H1": fn}


ROW = {"experiment_id": "H_test", "executor": "X_H1", "book": B.BOOK_FUTURES, "cell_id": "C04"}


def test_runner_refuses_an_unregistered_horizon_before_any_layer():
    out = RN.run_experiment(_Pipe({"horizon_sessions": 21, "owning_agent": "a"}), ROW,
                            _executors(1, 1), spec_hash="x", campaign_id="T")
    assert out["state"] == RN.RUN_REFUSED and out["reason"] == "HORIZON_NOT_PREREGISTERED"
    assert out["layers"] == {}


def test_runner_refuses_a_withdrawn_admission_before_any_layer(tmp_path):
    w1, w1a = _rulings(tmp_path)
    frozen = {"horizon_sessions": 1, "parameters": {"cadence_sessions": 1}, "owning_agent": "a"}
    out = RN.run_experiment(_Pipe(frozen), ROW, _executors(1, 1), spec_hash="x", campaign_id="T",
                            provenance={"admission_rulings": [w1, w1a]})
    assert out["state"] == RN.RUN_REFUSED and out["reason"] == "ADMISSION_NOT_G7_PASS"
    # positive control: with only W1 the doors open and the (absent) book is reached
    out = RN.run_experiment(_Pipe(frozen), ROW, _executors(1, 1), spec_hash="x", campaign_id="T",
                            provenance={"admission_rulings": [w1]})
    assert out["state"] == RN.RUN_FAILED


def test_runner_refuses_drifted_frozen_source(tmp_path, monkeypatch):
    repo, f = _source(tmp_path)
    store = tmp_path / "store"
    m = PV.snapshot([f], repo_root=repo, store_dir=store)
    f.write_text("# edited by a later wave\n", encoding="utf-8")
    monkeypatch.setattr(RN, "_REPO_ROOT", repo)
    frozen = {"horizon_sessions": 1, "parameters": {"cadence_sessions": 1, "frozen_source": m},
              "owning_agent": "a"}
    out = RN.run_experiment(_Pipe(frozen), ROW, _executors(1, 1), spec_hash="x", campaign_id="T",
                            provenance={"frozen_source_store": store})
    assert out["state"] == RN.RUN_REFUSED and out["reason"] == "FROZEN_SOURCE_MISMATCH"


# --------------------------------------------------------------------------- #
# Carry-forward -> next-campaign draft is generated, never hand-copied         #
# --------------------------------------------------------------------------- #
_GEN_SPEC = importlib.util.spec_from_file_location("build_next_campaign_draft_post_r99",
                                                   ROOT / "scripts" / "build_next_campaign_draft.py")
GEN = importlib.util.module_from_spec(_GEN_SPEC)
_GEN_SPEC.loader.exec_module(GEN)


def _carry_forward(tmp_path, recorded=4.5):
    cf = {"artifact": "RXX_CARRY_FORWARD", "ranking_update_after_W11": ["WAITING on time: X04"],
          "director_ranked_agenda": [{"rank": 1, "item": "X04"}], "not_reopenable": [],
          "added_evidence": {"X04_RBA_YIB": {"lockbox_merged_obs_now": 17, "floor": 36,
                                             "meetings_per_year_since_2024": 8,
                                             "observed_lockbox_nonzero_share": 0.531,
                                             "expected_years_to_floor_at_70pct_nonzero_SUPERSEDED": 3.4,
                                             "expected_years_to_floor_observed_rate": recorded}}}
    p = tmp_path / "CF.json"
    p.write_text(json.dumps(cf), encoding="utf-8")
    return p, cf


def test_draft_is_a_projection_with_rederived_timelines(tmp_path):
    p, cf = _carry_forward(tmp_path)
    d = GEN.build(p)
    assert d["director_agenda"] == cf["director_ranked_agenda"]
    assert d["derived_checks"]["event_accrual"][0]["derived_years_to_floor"] == pytest.approx(4.47, abs=0.01)
    assert d["derived_checks"]["all_consistent"] is True
    assert d["source_sha256"] == __import__("hashlib").sha256(p.read_bytes()).hexdigest()
    assert GEN.build(p) == d                                          # idempotent


def test_an_inconsistent_record_and_a_stale_hand_draft_are_both_flagged(tmp_path):
    p, cf = _carry_forward(tmp_path, recorded=3.4)
    assert GEN.build(p)["derived_checks"]["all_consistent"] is False
    stale = GEN.stale_facts({"x": "lockbox 17 < 36 (~3.4 years of meetings)"}, cf)
    assert len(stale) == 1 and stale[0]["superseded"]["value"] == 3.4
    assert GEN.stale_facts({"x": "about 4.5 years; 13.4 bp"}, cf) == []


def test_campaign_without_declarations_keeps_legacy_behaviour():
    p = RN.campaign_provenance({"campaign_id": "R99"})
    assert p == {"admission_rulings": None, "require_frozen_source": False, "frozen_source_store": None,
                 "provenance_contract": None}
    assert r59.HORIZON == 21     # the default a plan without kwargs executes
