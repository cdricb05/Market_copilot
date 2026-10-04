r"""R100 research-system repair - real working time, the complete frozen recipe,
automatic source snapshot at pre-registration, and what counts as measured.

Hermetic: synthetic panels, a ``tmp_path`` research memory, research root, frozen-
source store and campaign folder. Nothing here opens the live R59 memory, a
production store or a real campaign folder.
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from alpha_agent import agents_v2 as A
from alpha_agent import r59
from alpha_agent.agents_v2 import books as B
from alpha_agent.agents_v2 import campaign_clock as CK
from alpha_agent.agents_v2 import pipeline as P
from alpha_agent.agents_v2 import provenance as PV
from alpha_agent.agents_v2 import runner as RN
from alpha_agent.r59 import memory as M

pytestmark = pytest.mark.filterwarnings("ignore:All-NaN slice encountered:RuntimeWarning")
THIS = "tests/test_r100_research_repair.py"


# --------------------------------------------------------------------------- #
# 1. The clock counts real working time only                                   #
# --------------------------------------------------------------------------- #
class _Clock:
    def __init__(self):
        self.t = datetime(2026, 10, 5, 9, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.t

    def advance(self, minutes):
        self.t += timedelta(minutes=minutes)


@pytest.fixture()
def clock(monkeypatch):
    c = _Clock()
    monkeypatch.setattr(CK, "_now", c)
    return c


def _art(d: Path, name: str, body: str) -> Path:
    p = d / name
    p.write_text(body, encoding="utf-8")
    return p


def test_no_work_means_zero_minutes_however_much_time_passes(tmp_path, clock):
    assert CK.active_minutes(tmp_path)["active_minutes"] == 0.0
    clock.advance(600)
    assert CK.active_minutes(tmp_path)["active_minutes"] == 0.0


def test_idle_time_between_work_never_completes_a_campaign(tmp_path, clock):
    CK.record_work(tmp_path, activity="screen", artifact=_art(tmp_path, "a.json", "1"), actor="director")
    clock.advance(3)
    CK.record_work(tmp_path, activity="gate", artifact=_art(tmp_path, "b.json", "2"), actor="director")
    clock.advance(240)                           # four idle hours
    CK.record_work(tmp_path, activity="gate", artifact=_art(tmp_path, "c.json", "3"), actor="director")
    got = CK.active_minutes(tmp_path)
    assert got["active_minutes"] == pytest.approx(3 + CK.MAX_GAP_CREDIT_MINUTES)
    assert got["uncredited_idle_minutes_between_events"] == pytest.approx(240 - CK.MAX_GAP_CREDIT_MINUTES)
    # re-reading later (a guard re-run) adds nothing
    clock.advance(1000)
    assert CK.active_minutes(tmp_path)["active_minutes"] == got["active_minutes"]


def test_runner_spans_count_in_full_and_overlaps_are_not_double_counted(tmp_path, clock):
    with CK.span(tmp_path, "measure H1", actor="runner"):
        clock.advance(20)
    clock.advance(2)
    CK.record_work(tmp_path, activity="review", artifact=_art(tmp_path, "r.json", "x"), actor="skeptic")
    assert CK.active_minutes(tmp_path)["active_minutes"] == pytest.approx(22)


def test_a_failed_measurement_span_still_closes(tmp_path, clock):
    with pytest.raises(RuntimeError):
        with CK.span(tmp_path, "measure", actor="runner"):
            clock.advance(7)
            raise RuntimeError("book failed")
    got = CK.active_minutes(tmp_path)
    assert got["active_minutes"] == pytest.approx(7) and got["unterminated_spans"] == 0


def test_work_needs_a_new_existing_artifact(tmp_path, clock):
    with pytest.raises(CK.ClockRefusal) as e:
        CK.record_work(tmp_path, activity="x", artifact=tmp_path / "nope.json", actor="d")
    assert e.value.code == "WORK_ARTIFACT_MISSING"
    a = _art(tmp_path, "a.json", "1")
    CK.record_work(tmp_path, activity="x", artifact=a, actor="d")
    clock.advance(4)
    with pytest.raises(CK.ClockRefusal) as e:
        CK.record_work(tmp_path, activity="x again", artifact=a, actor="d")   # touching the log
    assert e.value.code == "WORK_ARTIFACT_UNCHANGED"
    assert CK.active_minutes(tmp_path)["active_minutes"] == 0.0


def test_no_caller_can_pass_a_timestamp():
    import inspect
    assert "at" not in inspect.signature(CK.record_work).parameters


# --------------------------------------------------------------------------- #
# 2/3. The complete recipe is frozen, and any difference refuses              #
# --------------------------------------------------------------------------- #
def _dates(start="2015-01-02", end="2026-07-01"):
    d = np.arange(start, end, dtype="datetime64[D]")
    return d[np.is_busday(d)].astype(str)


def _panel(seed=11, n_m=12):
    rng = np.random.default_rng(seed)
    dates = _dates()
    nanf = np.full((n_m, len(dates)), np.nan)
    roll = np.zeros((n_m, len(dates)), dtype=np.uint8)
    for i in range(n_m):
        roll[i, (11 + 5 * i)::63] = 1
    return {"symbols": ["M%02d" % i for i in range(n_m)], "dates": dates,
            "ret": rng.normal(0.0001, 0.01, (n_m, len(dates))), "ret2": nanf, "slope": nanf,
            "open_interest": nanf, "volume": nanf, "roll": roll,
            "cost_per_side": np.linspace(0.0002, 0.0015, n_m)}


def _plan(panel, *, h=5, c=5, sign=1, **extra):
    ret = panel["ret"]

    def weights(t, live):
        from alpha_agent.r59 import native
        return native._xs_weights(np.nansum(ret[:, max(0, t - 40):t], axis=1), live)

    kw = {"horizon": h, "cadence": c, "charge_rolls": True, "min_markets": 6}
    kw.update(extra)
    return {"book": B.BOOK_FUTURES, "panel": panel, "score_fn": weights, "book_kwargs": kw,
            "signal_sign": sign}


ROW = {"experiment_id": "H_t", "executor": "X1", "book": B.BOOK_FUTURES, "cell_id": "K01"}
MANIFEST = {"manifest_sha256": "f" * 64, "files": {p: "0" * 64 for p in PV.CANONICAL_SOURCE}}


def _frozen(plan):
    spec = {"expected_sign": 1, "horizon_sessions": 5, "dataset_id": "DS1",
            "cost_model": {"rate_per_side": "per market"},
            "parameters": {"cadence_sessions": 5, "frozen_source": MANIFEST}}
    r = PV.executed_recipe(plan, ROW, spec, MANIFEST, default_horizon=21, default_cadence=21)
    r["recipe_sha256"] = PV.recipe_sha256(r)
    spec["parameters"]["recipe"] = r
    return spec


def _executed(spec, plan, row=ROW):
    return PV.executed_recipe(plan, row, spec, MANIFEST, default_horizon=21, default_cadence=21)


def test_recipe_names_all_eight_parts_and_is_deterministic():
    p = _panel()
    a, b = _executed(_frozen(_plan(p)), _plan(p)), _executed(_frozen(_plan(p)), _plan(_panel()))
    assert set(PV.RECIPE_KEYS) <= set(a) and a == b
    assert a["instruments"] == p["symbols"] and a["holding_period"] == 5
    assert a["rebalance"] == {"cadence_sessions": 5} and a["direction"] == 1
    assert a["data"]["panel_unhashed_keys"] == []


def test_the_registered_recipe_passes_unchanged():
    p = _panel()
    spec = _frozen(_plan(p))
    got = PV.require_recipe(spec, _executed(spec, _plan(p)))
    assert got["matched"] == list(PV.RECIPE_KEYS)


def _mutations():
    def data(p):
        p["ret"][0, 100] += 1e-9
        return _plan(p)

    def instruments(p):
        p["symbols"] = list(p["symbols"])
        p["symbols"][3] = "SWAPPED"
        return _plan(p)

    def costs_vector(p):
        p["cost_per_side"] = p["cost_per_side"] * 0.5
        return _plan(p)

    return {
        "data": data,
        "instruments": instruments,
        "direction": lambda p: _plan(p, sign=-1),
        "holding_period": lambda p: _plan(p, h=21, c=5),
        "rebalance": lambda p: _plan(p, c=1),
        "costs": costs_vector,
        "costs_rolls": lambda p: _plan(p, charge_rolls=False),
        "costs_mult": lambda p: _plan(p, cost_mult=0.5),
        "signal": lambda p: _plan(p, min_markets=3),
    }


@pytest.mark.parametrize("part", sorted(_mutations()))
def test_any_difference_from_the_frozen_recipe_refuses(part):
    spec = _frozen(_plan(_panel()))
    plan = _mutations()[part](_panel())
    with pytest.raises(PV.ProvenanceRefusal) as e:
        PV.require_recipe(spec, _executed(spec, plan))
    assert e.value.code == "RECIPE_MISMATCH"
    assert part.split("_")[0] in str(e.value)


def test_a_different_executor_or_code_snapshot_refuses():
    p = _panel()
    spec = _frozen(_plan(p))
    with pytest.raises(PV.ProvenanceRefusal, match="signal"):
        PV.require_recipe(spec, _executed(spec, _plan(p), row=dict(ROW, executor="X2")))
    other = dict(MANIFEST, manifest_sha256="e" * 64)
    ex = PV.executed_recipe(_plan(p), ROW, spec, other, default_horizon=21, default_cadence=21)
    with pytest.raises(PV.ProvenanceRefusal, match="code_snapshot"):
        PV.require_recipe(spec, ex)


@pytest.mark.parametrize("damage", ["drop_part", "sign", "hash", "no_source", "no_canonical_code"])
def test_an_incomplete_or_inconsistent_frozen_recipe_refuses(damage):
    p = _panel()
    spec = _frozen(_plan(p))
    r = spec["parameters"]["recipe"]
    if damage == "drop_part":
        r.pop("costs")
    elif damage == "sign":
        spec["expected_sign"] = -1
    elif damage == "hash":
        r["holding_period"] = 6
        spec["horizon_sessions"] = 6
    elif damage == "no_source":
        spec["parameters"].pop("frozen_source")
    else:
        m = {"manifest_sha256": r["code_snapshot"], "files": {"only/cells.py": "0" * 64}}
        spec["parameters"]["frozen_source"] = m
    with pytest.raises(PV.ProvenanceRefusal) as e:
        PV.require_recipe(spec, _executed(spec, _plan(p)))
    assert e.value.code == "RECIPE_NOT_PREREGISTERED"


# --------------------------------------------------------------------------- #
# The runner: the contract is mandatory, and refuses before any layer         #
# --------------------------------------------------------------------------- #
def test_a_full_contract_campaign_must_declare_both_doors(tmp_path):
    with pytest.raises(RN.CampaignRefusal):
        RN.campaign_provenance({"provenance_contract": PV.PROVENANCE_CONTRACT_FULL})
    with pytest.raises(RN.CampaignRefusal):
        RN.campaign_provenance({"provenance_contract": "SOMETHING_ELSE"})
    got = RN.campaign_provenance({"provenance_contract": PV.PROVENANCE_CONTRACT_FULL,
                                  "admission_rulings": [str(tmp_path / "R.json")],
                                  "frozen_source_store": str(tmp_path / "store")})
    assert got["require_frozen_source"] is True


class _Mem:
    def __init__(self, frozen):
        self._frozen = frozen

    def get(self, eid):
        return {"spec": self._frozen}


class _Pipe:
    def __init__(self, frozen):
        self.mem = _Mem(frozen)

    def _latest(self, *a, **k):
        return None

    def perform(self, *a, **k):
        raise AssertionError("a provenance door must refuse before any reveal")


def test_runner_refuses_an_unregistered_recipe_before_any_layer(tmp_path, monkeypatch):
    # a legacy pre-registration (no recipe) under a FULL campaign never measures
    monkeypatch.setattr(PV, "require_frozen_source", lambda *a, **k: MANIFEST)
    frozen = {"horizon_sessions": 5, "parameters": {"cadence_sessions": 5}, "owning_agent": "a"}
    p = _panel()
    out = RN.run_experiment(_Pipe(frozen), ROW, {"X1": lambda row: _plan(p)}, spec_hash="x",
                            campaign_id="T", provenance={"provenance_contract": PV.PROVENANCE_CONTRACT_FULL})
    assert out["state"] == RN.RUN_REFUSED and out["reason"] == "RECIPE_NOT_PREREGISTERED"
    assert out["layers"] == {}


def test_runner_refuses_an_executed_recipe_that_drifted(monkeypatch):
    monkeypatch.setattr(PV, "require_frozen_source", lambda *a, **k: MANIFEST)
    p = _panel()
    frozen = dict(_frozen(_plan(p)), owning_agent="a")
    drifted = _panel()
    drifted["ret"][2, 50] = 0.5                         # the data changed after pre-registration
    out = RN.run_experiment(_Pipe(frozen), ROW, {"X1": lambda row: _plan(drifted)}, spec_hash="x",
                            campaign_id="T", provenance={"provenance_contract": PV.PROVENANCE_CONTRACT_FULL})
    assert out["state"] == RN.RUN_REFUSED and out["reason"] == "RECIPE_MISMATCH"
    assert "data" in out["detail"] and out["layers"] == {}


# --------------------------------------------------------------------------- #
# 5. A withdrawn or rejected experiment is never a valid measurement           #
# --------------------------------------------------------------------------- #
def _bound(eid, cell, state="MEASURED", g7="PASS", contract=PV.PROVENANCE_CONTRACT_FULL):
    return {"experiment_id": eid, "cell_id": cell, "state": state, "provenance_contract": contract,
            "recipe_sha256": "r", "frozen_source_manifest_sha256": "m",
            "admission": {"g7": g7}}


def test_only_bound_admitted_unwithdrawn_measurements_count():
    results = [_bound("H1", "C1"), _bound("H2", "C2", state="HALTED"),
               _bound("H3", "C3", state="REFUSED"), _bound("H4", "C4", state="FAILED"),
               _bound("H5", "C5", contract=None), _bound("H6", "C6", g7="FAIL"),
               _bound("H7", "C7")]
    exc = [{"cell": "C7", "exception": PV.EXC_POST_MEASUREMENT_WITHDRAWAL}]
    got = CK.valid_measurements(results, exc)
    assert got["valid"] == ["H1", "H2"] and got["n_valid"] == 2
    why = {x["experiment_id"]: x["reason"] for x in got["excluded"]}
    assert why == {"H3": "RUN_REFUSED", "H4": "RUN_FAILED", "H5": "UNBOUND_LEGACY_MEASUREMENT",
                   "H6": "NOT_ADMITTED_AT_MEASUREMENT", "H7": PV.EXC_POST_MEASUREMENT_WITHDRAWAL}


# --------------------------------------------------------------------------- #
# 6. End to end: automatic snapshot at pre-registration -> bound measurement   #
# --------------------------------------------------------------------------- #
@pytest.fixture()
def pipe(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    mem = M.ResearchMemory(tmp_path / "research_memory.sqlite")
    p = P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")
    p.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id="DS_FX", asset_classes=[r59.AC_FX], pit_status=P.PIT_SAFE,
        availability_rule="stamped when observable", survivorship="expired contracts retained"))
    p.perform(A.UNIVERSE, "define_universe", dict(
        universe_id="U_FX", dataset_id="DS_FX", asset_class=r59.AC_FX,
        execution_representation="LONG_SHORT", short_leg_expressible=True, rules="trailing liquidity"))
    p.perform(A.FEATURES, "publish_features", dict(
        feature_set_id="F_FX", universe_id="U_FX",
        features=[{"name": "f1", "lag": 1, "source": "DS_FX"}], leakage_check="PASS"))
    return p


def _payload():
    return dict(owning_agent=A.TREND_BREADTH, hypothesis="synthetic R100 repair cell",
                asset_class=r59.AC_FX, family="TREND", feature_set_id="F_FX", horizon_sessions=5,
                parameters={"lookback": 40}, long_short=True,
                discovery_sample={"start": r59.DISCOVERY_START, "end": r59.VALIDATION_START},
                evaluation_sample={"validation": r59.VALIDATION_START, "lockbox": r59.LOCKBOX_START},
                cost_model={"rate_per_side": "per market"}, expected_sign=1,
                information_family="PRICE_STATE", instrument_scope=["M00"], venue="RESEARCH")


def _rulings(tmp_path, g7="PASS"):
    f = tmp_path / ("RULING_%s.json" % g7)
    f.write_text(json.dumps({"rulings": {"K01": {"G1": "PASS", "G4": "PASS", "G7": g7}}}), encoding="utf-8")
    return f


def test_preregistration_snapshots_automatically_and_the_measurement_is_bound(pipe, tmp_path):
    p = _panel()
    executors = {"X1": lambda row: _plan(p)}
    store = tmp_path / "store"
    reg = RN.preregister_frozen(pipe, director=A.DIRECTOR, row=dict(ROW), payload=_payload(),
                                executors=executors, store_dir=store, source_paths=[THIS])
    frozen = pipe.mem.get(reg["experiment_id"])["spec"]
    m = frozen["parameters"]["frozen_source"]
    assert set(PV.CANONICAL_SOURCE) | {THIS} == set(m["files"])
    assert all((store / h).exists() for h in m["files"].values())          # stored, not just hashed
    assert frozen["provenance_contract"] == PV.PROVENANCE_CONTRACT_FULL
    assert frozen["parameters"]["recipe"]["recipe_sha256"] == reg["recipe_sha256"]
    assert frozen["parameters"]["cadence_sessions"] == 5

    row = dict(ROW, experiment_id=reg["experiment_id"])
    prov = {"provenance_contract": PV.PROVENANCE_CONTRACT_FULL, "require_frozen_source": True,
            "frozen_source_store": store, "admission_rulings": [_rulings(tmp_path)]}
    out = RN.run_experiment(pipe, row, executors, spec_hash=reg["spec_hash"], campaign_id="T",
                            provenance=prov)
    assert out["state"] in (RN.RUN_MEASURED, RN.RUN_HALTED), out
    assert out["recipe_sha256"] == reg["recipe_sha256"]
    assert out["frozen_source_manifest_sha256"] == m["manifest_sha256"]
    assert CK.valid_measurements([out])["valid"] == [reg["experiment_id"]]


def test_the_pipeline_refuses_a_hand_written_incomplete_full_contract_prereg(pipe, tmp_path):
    body = dict(_payload(), provenance_contract=PV.PROVENANCE_CONTRACT_FULL,
                frozen_source_store=str(tmp_path / "store"))
    with pytest.raises(P.PipelineRefusal) as e:
        pipe.perform(A.DIRECTOR, "preregister", body)
    assert e.value.code == "PREREGISTRATION_PROVENANCE_INCOMPLETE"


def test_the_pipeline_refuses_a_snapshot_whose_blobs_are_gone(pipe, tmp_path):
    p = _panel()
    store = tmp_path / "store"
    executors = {"X1": lambda row: _plan(p)}
    payload = _payload()
    reg = RN.preregister_frozen(pipe, director=A.DIRECTOR, row=dict(ROW), payload=payload,
                                executors=executors, store_dir=store)
    for blob in store.iterdir():
        blob.unlink()
    payload2 = dict(payload, hypothesis="second synthetic cell", parameters={"lookback": 41})
    with pytest.raises(P.PipelineRefusal) as e:
        # a hand-built payload reusing a manifest whose bytes are no longer stored
        frozen = copy.deepcopy(pipe.mem.get(reg["experiment_id"])["spec"])
        body = dict(payload2, parameters=dict(frozen["parameters"], lookback=41),
                    provenance_contract=PV.PROVENANCE_CONTRACT_FULL, frozen_source_store=str(store))
        pipe.perform(A.DIRECTOR, "preregister", body)
    assert e.value.code == "PREREGISTRATION_PROVENANCE_INCOMPLETE"
    assert "absent from the store" in str(e.value)


def test_a_withdrawn_cell_is_refused_and_never_counted(pipe, tmp_path):
    p = _panel()
    executors = {"X1": lambda row: _plan(p)}
    store = tmp_path / "store"
    reg = RN.preregister_frozen(pipe, director=A.DIRECTOR, row=dict(ROW), payload=_payload(),
                                executors=executors, store_dir=store)
    row = dict(ROW, experiment_id=reg["experiment_id"])
    prov = {"provenance_contract": PV.PROVENANCE_CONTRACT_FULL, "require_frozen_source": True,
            "frozen_source_store": store,
            "admission_rulings": [_rulings(tmp_path, "PASS"), _rulings(tmp_path, "FAIL")]}
    out = RN.run_experiment(pipe, row, executors, spec_hash=reg["spec_hash"], campaign_id="T",
                            provenance=prov)
    assert out["state"] == RN.RUN_REFUSED and out["reason"] == "ADMISSION_NOT_G7_PASS"
    assert CK.valid_measurements([out])["n_valid"] == 0
