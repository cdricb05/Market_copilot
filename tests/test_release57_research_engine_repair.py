r"""R57 - the five research-engine repairs, and the local campaign runner.

Hermetic. Every test runs against a research memory inside ``tmp_path`` and a
synthetic panel; none of them reads ``D:\\Stock_Prediction_app_data`` except
the three marked ``owned_data``, which SKIP when the R38 layer is absent.

What is proven here, defect by defect:

    A1 machine prior      the generative charge is reachable for an agent
                          experiment, is DERIVED from the frozen
                          pre-registration, is charged when the family is
                          itself machine space, and never double-counts
    A2 uncertified file   an orphan CSV in the layer directory cannot enter a
                          measured result through the owner, and a MODELLED
                          cost is no longer described as measured
    A3 materiality gate   the floor travels with the metric; a layer carrying
                          BOTH keys can no longer test a Sharpe against 0.015
    A4 placebo            the comparison is cost-neutral BY CONSTRUCTION, and
                          a cost-advantaged placebo is refused, not passed
    A5 sequential reveal  D then V then L; a later layer is refused until the
                          earlier one earned it; a halted experiment settles
                          and its lockbox is NEVER computed; a submitted layer
                          that differs from the revealed one is refused

    runner                exact stage prefixes, idempotence, early stop, one
                          registry, and no order / fill / promotion / adoption
"""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent import agents_v2 as A
from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import books as B
from paper_trader.alpha_agent.agents_v2 import contracts as C
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.agents_v2 import runner as RUN
from paper_trader.alpha_agent.r57 import engine as K
from paper_trader.alpha_agent.r59 import engines as E
from paper_trader.alpha_agent.r59 import handlers as H
from paper_trader.alpha_agent.r59 import memory as M
from paper_trader.alpha_agent.r59 import native

REPO = Path(__file__).resolve().parents[1]
FROZEN_EVALUATOR = (REPO / "research" / "agents" / "campaign_r56_v2"
                    / "evaluator.py")

pytestmark = pytest.mark.filterwarnings(
    "ignore:All-NaN slice encountered:RuntimeWarning")


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def mem(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    return M.ResearchMemory(tmp_path / "research_memory.sqlite")


@pytest.fixture()
def pipe(mem, tmp_path):
    return P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


def _sessions(start="2011-01-03", end="2026-07-01"):
    d = np.arange(start, end, dtype="datetime64[D]")
    return d[np.is_busday(d)].astype(str)


def _equity_panel(n=60, seed=11):
    """A synthetic total-return panel with a REAL cross-sectional signal."""
    rng = np.random.default_rng(seed)
    dates = _sessions()
    n_d = len(dates)
    # a persistent per-name drift the score function can actually find
    drift = rng.normal(0.0, 0.0006, (n, 1))
    ret = rng.normal(0.0003, 0.012, (n, n_d)) + drift
    tr = 100.0 * np.cumprod(1.0 + ret, axis=1)
    return {"dates": dates, "tr": tr, "op_tr": tr * (1.0 + 0.0002),
            "adv": np.full((n, n_d), 5e7), "drift": drift}


def _score_drift(panel, t):
    """Scores from data STRICTLY through t: trailing 63-session return."""
    tr = panel["tr"]
    lo = max(0, t - 63)
    return tr[:, t] / tr[:, lo] - 1.0


def _elig_all(panel, t):
    return np.ones(panel["tr"].shape[0], dtype=bool)


# --------------------------------------------------------------------------- #
# A3 - the materiality gate
# --------------------------------------------------------------------------- #
def _gate(L, V, burden=5):
    return E.gate({"layers": {"L": L, "V": V}}, prior_burden=burden)


def test_a3_a_sharpe_is_never_graded_against_the_return_floor():
    """THE R56 TRAP. A layer carrying both keys used to read materiality from
    ``net_sharpe`` and its FLOOR from ``ann_net_excess``: a Sharpe of 0.30 was
    tested against 0.015 and passed."""
    both = {"ann_net_excess": 0.05, "net_sharpe": 0.30,
            "ann_gross_excess": 0.07, "t_net_excess": 3.0,
            "p_one_sided": 1e-6, "effective_observations": 60}
    g = _gate(both, {"ann_net_excess": 0.02, "net_sharpe": 0.20})
    assert g["qualified"] is False
    assert "lockbox_material" in g["failed_gates"]
    assert g["materiality_floors"] == {"ann_net_excess": 0.015,
                                       "net_sharpe": 0.40}
    # and the reader can see WHICH number met WHICH threshold
    assert g["lockbox_materiality_by_metric"]["net_sharpe"] == 0.30


def test_a3_both_metrics_must_clear_their_own_floor():
    strong_both = {"ann_net_excess": 0.05, "net_sharpe": 0.9,
                   "t_net_excess": 4.0, "p_one_sided": 1e-8,
                   "effective_observations": 60}
    g = _gate(strong_both, {"ann_net_excess": 0.02, "net_sharpe": 0.5})
    assert g["qualified"] is True
    assert sorted(g["materiality_metrics"]) == ["ann_net_excess", "net_sharpe"]


def test_a3_validation_is_read_on_the_same_metrics_as_the_lockbox():
    """A lockbox graded on ann_net_excess and a validation graded on
    net_sharpe is two scales, and two scales is not a comparison."""
    g = _gate({"ann_net_excess": 0.05, "t_net_excess": 4.0,
               "p_one_sided": 1e-8, "effective_observations": 60},
              {"net_sharpe": 5.0})            # no ann_net_excess at all
    assert g["qualified"] is False
    assert "validation_material" in g["failed_gates"]


def test_a3_single_metric_layers_are_graded_exactly_as_before():
    """The estate's 8,000 settled results must not be re-graded by this fix."""
    g = _gate({"ann_net_excess": 0.05, "t_net_excess": 4.0,
               "p_one_sided": 1e-8, "effective_observations": 60},
              {"ann_net_excess": 0.02})
    assert g["qualified"] is True
    weak = _gate({"ann_net_excess": 0.005, "t_net_excess": 1.0,
                  "p_one_sided": 0.2, "effective_observations": 60},
                 {"ann_net_excess": 0.002})
    assert weak["qualified"] is False
    assert "lockbox_material" in weak["failed_gates"]


def test_a3_a_layer_with_no_materiality_metric_is_not_material():
    g = _gate({"t_net_excess": 9.0, "p_one_sided": 1e-12,
               "effective_observations": 99}, {"ann_net_excess": 0.9})
    assert g["qualified"] is False
    assert "lockbox_material" in g["failed_gates"]


# --------------------------------------------------------------------------- #
# A1 - the machine family prior
# --------------------------------------------------------------------------- #
def _settle_machine_rows(mem, n, asset_class=r59.AC_COMMODITY, kind="AUTO"):
    for i in range(n):
        fam = M.family_key(
            economic_family="MACHINE_REPRESENTATION:%s" % kind,
            information_family="PRICE_STATE", asset_class=asset_class,
            model_family="XS_LONG_SHORT")
        hid = mem.register(title="machine %d" % i, release="R59",
                           origin="MACHINE_GENERATED",
                           generation_method="MACHINE",
                           information_family="PRICE_STATE",
                           economic_family="MACHINE_REPRESENTATION:%s" % kind,
                           asset_class=asset_class,
                           model_family="XS_LONG_SHORT",
                           horizon_sessions=21,
                           input_data_identity="synthetic",
                           spec={"i": i})
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                          reason_rejected="synthetic")
    return fam


def test_a1_the_machine_prior_is_reachable_for_an_agent_experiment(mem):
    """In R56 the one caller passed a hard-coded False, so no agent experiment
    could EVER be charged for the estate's generative search."""
    _settle_machine_rows(mem, 12)
    agent_family = M.family_key(economic_family="CARRY",
                                information_family="TERM_STRUCTURE",
                                asset_class=r59.AC_COMMODITY,
                                model_family="XS_LONG_SHORT")
    off = H.search_denominator(mem, family_key=agent_family,
                               asset_class=r59.AC_COMMODITY,
                               machine_generated=False)
    on = H.search_denominator(mem, family_key=agent_family,
                              asset_class=r59.AC_COMMODITY,
                              machine_generated=True)
    assert off["generative_search"] == 0
    assert on["generative_search"] == 12
    assert on["total"] - off["total"] == 12
    assert on["generative_charge_applied"] is True


def test_a1_a_family_in_machine_space_pays_the_prior_without_the_flag(mem):
    """A hand-written hypothesis that lands inside the swept space was still
    selected against that sweep, whoever typed it."""
    fam = _settle_machine_rows(mem, 9)
    den = H.search_denominator(mem, family_key=fam,
                               asset_class=r59.AC_COMMODITY,
                               machine_generated=False)
    assert den["in_machine_representation_space"] is True
    assert den["generative_charge_applied"] is True


def test_a1_the_candidates_own_family_is_never_counted_twice(mem):
    fam = _settle_machine_rows(mem, 9)                 # all in ONE family
    den = H.search_denominator(mem, family_key=fam,
                               asset_class=r59.AC_COMMODITY,
                               machine_generated=True)
    assert den["family"] == 9
    assert den["generative_search"] == 0               # not 9 again
    assert den["total"] == 9


def test_a1_the_prior_is_scoped_to_the_asset_class(mem):
    _settle_machine_rows(mem, 7, asset_class=r59.AC_US_EQUITY)
    fam = M.family_key(economic_family="CARRY",
                       information_family="TERM_STRUCTURE",
                       asset_class=r59.AC_COMMODITY,
                       model_family="XS_LONG_SHORT")
    den = H.search_denominator(mem, family_key=fam,
                               asset_class=r59.AC_COMMODITY,
                               machine_generated=True)
    assert den["generative_search"] == 0


def test_a1_the_flag_is_frozen_at_preregistration_not_chosen_at_review():
    """``generative_search`` is read from the frozen spec by the skeptic, and
    the director is the only role that can set it."""
    src = (REPO / "alpha_agent" / "agents_v2" / "pipeline.py").read_text(
        encoding="utf-8")
    assert 'spec["generative_search"] = bool(spec_in.get("generative_search"' \
        in src
    assert "machine_generated=bool(spec.get(\"generative_search\", False))" \
        in src
    assert "machine_generated=False" not in src


# --------------------------------------------------------------------------- #
# A2 - the uncertified file
# --------------------------------------------------------------------------- #
owned_data = pytest.mark.skipif(
    not (native.NATIVE_LAYER_DIR.exists() and native.LAYER_MANIFEST.exists()),
    reason="R38 native contract layer is not present on this machine")


@owned_data
def test_a2_the_orphan_file_is_excluded_from_the_owner_layer():
    cert = native.layer_certification()
    assert cert["ok"] is True
    assert cert["orphans"], "this machine has no orphan to prove the rule on"
    layer = native.load_layer()
    for orphan in cert["orphans"]:
        assert orphan not in layer["symbols"]
    assert len(layer["symbols"]) == len(cert["certified_markets"])
    assert layer["orphans_excluded"] == cert["orphans"]


@owned_data
def test_a2_the_certification_is_the_load_rule_not_an_optional_overlay():
    """There is no argument that lets an orphan in. ``verify_hashes`` may be
    relaxed for speed; MEMBERSHIP may not be relaxed at all."""
    sig = native.load_layer.__code__.co_varnames[
        :native.load_layer.__code__.co_argcount]
    assert "verify_hashes" in sig
    src = (REPO / "alpha_agent" / "r59" / "native.py").read_text(
        encoding="utf-8")
    # the loader iterates the CERTIFIED list, never the directory
    assert "for market in cert[\"certified_markets\"]:" in src
    assert "for p in sorted(NATIVE_LAYER_DIR.glob(\"*.csv\")):" not in src


def test_a2_a_missing_manifest_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(native, "LAYER_MANIFEST", tmp_path / "nope.json")
    with pytest.raises(native.UncertifiedLayer):
        native.layer_certification()


def test_a2_an_orphan_is_detected_and_a_mismatch_breaks_the_layer(
        tmp_path, monkeypatch):
    d = tmp_path / "layer"
    d.mkdir()
    for name, body in (("AA", "Date,ret\n2020-01-02,0.01\n"),
                       ("BB", "Date,ret\n2020-01-02,0.02\n"),
                       ("ORPHAN", "Date,ret\n2020-01-02,0.03\n")):
        (d / ("%s.csv" % name)).write_text(body, encoding="utf-8")
    manifest = {"markets": {
        n: {"state": "OK",
            "sha256": hashlib.sha256(
                (d / ("%s.csv" % n)).read_bytes()).hexdigest()}
        for n in ("AA", "BB")}}
    (d / "layer_manifest.json").write_text(json.dumps(manifest),
                                           encoding="utf-8")
    monkeypatch.setattr(native, "NATIVE_LAYER_DIR", d)
    monkeypatch.setattr(native, "LAYER_MANIFEST", d / "layer_manifest.json")
    cert = native.layer_certification()
    assert cert["ok"] is True and cert["orphans"] == ["ORPHAN"]
    assert sorted(cert["certified_markets"]) == ["AA", "BB"]

    (d / "AA.csv").write_text("Date,ret\n2020-01-02,0.99\n", encoding="utf-8")
    broken = native.layer_certification()
    assert broken["ok"] is False and broken["mismatched"] == ["AA"]


def test_a2_a_modelled_cost_is_not_described_as_measured():
    prov = native.cost_provenance()
    assert prov["cost_model_state"] == "MODELLED_NOT_OBSERVED"
    assert prov["measured"] is False
    assert "MEASURED per-side cost" not in (native.load_meta.__doc__ or "")
    assert "MODELLED" in (native.load_meta.__doc__ or "")


# --------------------------------------------------------------------------- #
# A4 - the cost-neutral placebo
# --------------------------------------------------------------------------- #
def test_a4_a_cost_advantaged_placebo_comparison_is_refused():
    """THE R56 DEFECT. A permuted signal churns, so it pays more cost. On a
    NET comparison the candidate wins for trading less, not for predicting."""
    candidate = {"ann_net_excess": 0.030, "ann_gross_excess": 0.040}
    churning = {"ann_net_excess": 0.000, "ann_gross_excess": 0.045}
    with pytest.raises(E.StageRefusal, match="not cost-neutral"):
        E.placebo_verdict(candidate, churning,
                          margins={"ann_net_excess": 0.01})


def test_a4_on_equal_cost_the_margin_measures_predictive_content():
    candidate = {"ann_net_excess": 0.030, "ann_gross_excess": 0.040}
    matched = {"ann_net_excess": 0.005, "ann_gross_excess": 0.015}
    v = E.placebo_verdict(candidate, matched,
                          margins={"ann_net_excess": 0.01})
    assert v["placebo_clean"] is True
    assert v["cost_basis"] == "COST_NEUTRAL"
    assert abs(v["cost_drag_gap"]) < 1e-12
    assert abs(v["by_metric"]["ann_net_excess"]["margin"] - 0.025) < 1e-12

    thin = {"ann_net_excess": 0.025, "ann_gross_excess": 0.035}
    assert E.placebo_verdict(candidate, thin,
                             margins={"ann_net_excess": 0.01}
                             )["placebo_clean"] is False


def test_a4_the_simulator_rescales_the_placebo_to_the_candidates_drag():
    """``placebo_cost_neutral`` solves the multiplier; it does not assume one."""
    target = {"ann_net_excess": 0.030, "ann_gross_excess": 0.040}   # drag 0.010

    def run_placebo(seed, mult):
        gross = 0.002 + 0.0005 * (seed % 3)
        return {"ann_net_excess": gross - 0.025 * mult,
                "ann_gross_excess": gross}          # natural drag 0.025

    out = B.placebo_cost_neutral(candidate_stats=target,
                                 run_placebo=run_placebo, seeds=(1, 2, 3))
    assert out["cost_basis"] == "COST_NEUTRAL"
    for row in out["placebo_runs"]:
        assert abs(row["matched_cost_drag"] - 0.010) <= E.PLACEBO_COST_TOLERANCE
        assert abs(row["cost_mult"] - 0.4) < 1e-6      # 0.010 / 0.025
    # the attack is answered by the STRONGEST placebo, not the average one
    assert out["strongest"]["ann_net_excess"] == max(
        r["stats"]["ann_net_excess"] for r in out["placebo_runs"])


def test_a4_a_permutation_destroys_only_the_cross_sectional_assignment():
    base = np.array([3.0, 1.0, np.nan, 2.0, 5.0])
    fn = B.permuted_score_fn(lambda: base, seed=7)
    out = np.asarray(fn())
    assert np.isnan(out[2])                       # the mask is untouched
    assert sorted(out[~np.isnan(out)]) == sorted(base[~np.isnan(base)])


def test_a4_the_schema_declares_the_cost_neutral_rule():
    schema = C.Contracts().gate_schema()
    # 2.1 is the version that introduced the cost-neutral placebo. What this
    # test protects is that the RULE is still declared, not that the schema
    # has stopped evolving - R61 raised it to 2.2 to replace the turnover
    # scalar with an annualised cost budget, and pinning the exact string
    # would have made every later correction look like a regression here.
    version = tuple(int(p) for p in
                    schema["validation_gate_schema_version"].split("."))
    assert version >= (2, 1)
    assert schema["placebo"]["cost_basis"] == "COST_NEUTRAL"
    assert schema["placebo"]["owner"] == \
        "alpha_agent.r59.engines.placebo_verdict"
    assert "placebo_margin" not in schema, "one margin, one home"


# --------------------------------------------------------------------------- #
# A5 - the sequential reveal
# --------------------------------------------------------------------------- #
EQ_COST = {"rate_per_side": 0.00125, "basis": "traded notional"}


def _prereg(pipe, **over):
    pipe.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id="ds", asset_classes=[r59.AC_US_EQUITY],
        pit_status=P.PIT_SAFE, availability_rule="observable at t",
        survivorship="delisted retained"))
    pipe.perform(A.UNIVERSE, "define_universe", dict(
        universe_id="uni", dataset_id="ds", asset_class=r59.AC_US_EQUITY,
        execution_representation="LONG_ONLY", rules="trailing liquidity"))
    pipe.perform(A.FEATURES, "publish_features", dict(
        feature_set_id="fs", universe_id="uni",
        features=[{"name": "f", "lag": 1, "source": "ds"}],
        leakage_check="PASS"))
    payload = dict(
        owning_agent=A.MOMENTUM, hypothesis="synthetic momentum",
        asset_class=r59.AC_US_EQUITY, family="MOMENTUM", feature_set_id="fs",
        horizon_sessions=21, parameters={"lookback": 63},
        discovery_sample={"start": r59.DISCOVERY_START},
        evaluation_sample={"lockbox": r59.LOCKBOX_START},
        cost_model=EQ_COST, expected_sign=1,
        information_family="PRICE_STATE")
    payload.update(over)
    return pipe.perform(A.DIRECTOR, "preregister", payload)


def _layer(x, obs=60):
    return {"ann_net_excess": x, "ann_gross_excess": x + 0.01,
            "t_net_excess": 4.0, "p_one_sided": 1e-8, "periods": obs,
            "effective_observations": obs}


def test_a5_the_lockbox_cannot_be_revealed_first(pipe):
    reg = _prereg(pipe)
    with pytest.raises(P.PipelineRefusal, match="STAGE_OUT_OF_ORDER"):
        pipe.perform(A.MOMENTUM, "reveal_stage", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            stage="L", stats=_layer(0.06)))


def test_a5_validation_cannot_be_revealed_before_discovery(pipe):
    reg = _prereg(pipe)
    with pytest.raises(P.PipelineRefusal, match="STAGE_OUT_OF_ORDER"):
        pipe.perform(A.MOMENTUM, "reveal_stage", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            stage="V", stats=_layer(0.04)))


def test_a5_a_failed_discovery_halts_and_the_lockbox_is_never_computed(pipe):
    reg = _prereg(pipe)
    eid = reg["experiment_id"]
    out = pipe.perform(A.MOMENTUM, "reveal_stage", dict(
        experiment_id=eid, spec_hash=reg["spec_hash"], stage="D",
        stats=_layer(0.0005)))                 # below 0.25 * 0.015
    assert out["advance"] is False and out["halted"] is True
    assert pipe.mem.get(eid)["outcome"] == r59.HO_NO_ALPHA_EVIDENCE
    with pytest.raises(P.PipelineRefusal, match="EXPERIMENT_HALTED"):
        pipe.perform(A.MOMENTUM, "reveal_stage", dict(
            experiment_id=eid, spec_hash=reg["spec_hash"], stage="V",
            stats=_layer(0.04)))
    # the halted experiment still counts to the search burden
    assert pipe.mem.burden()["total"] >= 1


def test_a5_a_failed_validation_halts_before_the_lockbox(pipe):
    reg = _prereg(pipe)
    eid = reg["experiment_id"]
    pipe.perform(A.MOMENTUM, "reveal_stage", dict(
        experiment_id=eid, spec_hash=reg["spec_hash"], stage="D",
        stats=_layer(0.05)))
    out = pipe.perform(A.MOMENTUM, "reveal_stage", dict(
        experiment_id=eid, spec_hash=reg["spec_hash"], stage="V",
        stats=_layer(0.001)))
    assert out["halted"] is True
    stages = [e["detail"]["stage"] for e in pipe._events(P.EV_STAGE, eid)]
    assert stages == ["D", "V"], "the lockbox was never measured"


def test_a5_a_candidate_cannot_be_submitted_without_the_sequential_reveal(
        pipe):
    reg = _prereg(pipe)
    with pytest.raises(P.PipelineRefusal,
                       match="LAYERS_NOT_SEQUENTIALLY_REVEALED"):
        pipe.perform(A.MOMENTUM, "submit_candidate", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            signal_sign=1, cost_model=EQ_COST, turnover=0.2,
            layers={"D": _layer(0.05), "V": _layer(0.04), "L": _layer(0.06)}))


def test_a5_a_submitted_layer_that_differs_from_the_revealed_one_is_refused(
        pipe):
    reg = _prereg(pipe)
    eid, sh = reg["experiment_id"], reg["spec_hash"]
    truth = {"D": _layer(0.05), "V": _layer(0.04), "L": _layer(0.02)}
    for s in r59.STAGES:
        pipe.perform(A.MOMENTUM, "reveal_stage", dict(
            experiment_id=eid, spec_hash=sh, stage=s, stats=truth[s]))
    cooked = dict(truth, L=_layer(0.20))
    with pytest.raises(P.PipelineRefusal,
                       match="SUBMITTED_LAYER_DIFFERS_FROM_REVEALED"):
        pipe.perform(A.MOMENTUM, "submit_candidate", dict(
            experiment_id=eid, spec_hash=sh, signal_sign=1,
            cost_model=EQ_COST, turnover=0.2, layers=cooked))


def test_a5_a_layer_is_revealed_once(pipe):
    reg = _prereg(pipe)
    eid, sh = reg["experiment_id"], reg["spec_hash"]
    pipe.perform(A.MOMENTUM, "reveal_stage", dict(
        experiment_id=eid, spec_hash=sh, stage="D", stats=_layer(0.05)))
    with pytest.raises(P.PipelineRefusal, match="STAGE_ALREADY_REVEALED"):
        pipe.perform(A.MOMENTUM, "reveal_stage", dict(
            experiment_id=eid, spec_hash=sh, stage="D", stats=_layer(0.09)))


def test_a5_a_layer_with_no_observations_cannot_be_judged(pipe):
    reg = _prereg(pipe)
    out = pipe.perform(A.MOMENTUM, "reveal_stage", dict(
        experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
        stage="D", stats={"ann_net_excess": 0.05}))
    assert out["advance"] is False
    assert any("observation" in r for r in out["halt_reasons"])


def test_a5_only_the_owning_agent_may_reveal(pipe):
    reg = _prereg(pipe)
    with pytest.raises(P.PipelineRefusal, match="NOT_THE_OWNING_AGENT"):
        pipe.perform(A.REVERSAL, "reveal_stage", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            stage="D", stats=_layer(0.05)))


def test_a5_the_full_sequence_reaches_the_skeptic(pipe):
    reg = _prereg(pipe)
    eid, sh = reg["experiment_id"], reg["spec_hash"]
    layers = {"D": _layer(0.05), "V": _layer(0.04), "L": _layer(0.06)}
    last = None
    for s in r59.STAGES:
        last = pipe.perform(A.MOMENTUM, "reveal_stage", dict(
            experiment_id=eid, spec_hash=sh, stage=s, stats=layers[s]))
    assert last["next"] == "validation-skeptic-agent"
    sub = pipe.perform(A.MOMENTUM, "submit_candidate", dict(
        experiment_id=eid, spec_hash=sh, signal_sign=1, cost_model=EQ_COST,
        turnover=0.2, layers=layers))
    assert sub["state"] == "CANDIDATE_SUBMITTED"


def test_a5_the_schema_and_the_contract_declare_the_sequence():
    schema = C.Contracts().gate_schema()
    assert schema["sequential_reveal"]["order"] == ["D", "V", "L"]
    assert schema["sequential_reveal"]["owner"] == \
        "alpha_agent.r59.engines.stage_advance"
    assert C.validate() == []
    ac = C.load_contract("agent_contracts.json")
    for a in ac["agents"]:
        if "submit_candidate" in a.get("pipeline_verbs", ()):
            assert "reveal_stage" in a["pipeline_verbs"]


# --------------------------------------------------------------------------- #
# The stage prefix is EXACT
# --------------------------------------------------------------------------- #
def test_the_discovery_prefix_admits_no_validation_decision():
    dates = _sessions()
    for horizon, cadence in ((21, 21), (5, 5), (63, 21)):
        n = B.stage_sessions(dates, "D", horizon)
        idx = K.decision_indices(dates[:n], cadence, r59.DISCOVERY_START,
                                 horizon)
        lab = K.layer_of(dates[:n], idx, cadence, horizon)
        assert set(lab) <= {"D", ""}, (horizon, cadence, sorted(set(lab)))


def test_the_prefix_reproduces_the_full_runs_layer_exactly():
    """A prefix run must give the SAME numbers as the full run for the layer
    it reveals - otherwise staging would be a different experiment."""
    panel = _equity_panel()
    full = B.run_equity_topn(panel, _score_drift, _elig_all, label="full",
                             cadence=21, horizon=21, top_n=15)
    for stage in ("D", "V"):
        n = B.stage_sessions(panel["dates"], stage, 21)
        part = B.run_equity_topn(B.truncate_panel(panel, n), _score_drift,
                                 _elig_all, label=stage, cadence=21,
                                 horizon=21, top_n=15)
        a = full["layers"][stage]
        b = part["layers"][stage]
        assert a["periods"] == b["periods"] > 0
        assert abs(a["ann_net_excess"] - b["ann_net_excess"]) < 1e-12
        assert abs(a["t_net_excess"] - b["t_net_excess"]) < 1e-12
        # and the prefix contains NOTHING of the later layers
        later = r59.STAGES[r59.STAGES.index(stage) + 1:]
        assert all(not part["layers"][x].get("periods") for x in later)


def test_run_stage_measures_one_layer_of_a_real_book():
    panel = _equity_panel()
    out = B.run_stage(book=B.BOOK_EQUITY_TOPN, stage="D", panel=panel,
                      score_fn=_score_drift,
                      elig=_elig_all, top_n=15, cadence=21, horizon=21)
    assert out["stats"]["periods"] > 0
    assert not out["result"]["layers"]["L"].get("periods")


# --------------------------------------------------------------------------- #
# The runner
# --------------------------------------------------------------------------- #
def _campaign(tmp_path, executor_body: str, rows: list) -> dict:
    mod = tmp_path / "executors.py"
    mod.write_text(executor_body, encoding="utf-8")
    spec = {"campaign_id": "TEST_WAVE", "executor_module": str(mod),
            "experiments": rows}
    p = tmp_path / "campaign_spec.json"
    p.write_text(json.dumps(spec), encoding="utf-8")
    return RUN.load_campaign_spec(p)


EXECUTOR_SRC = '''
import numpy as np
from paper_trader.alpha_agent.agents_v2 import books as B

DATES = None


def _panel(seed, n=60):
    import numpy as np
    d = np.arange("2011-01-03", "2026-07-01", dtype="datetime64[D]")
    d = d[np.is_busday(d)].astype(str)
    rng = np.random.default_rng(seed)
    drift = rng.normal(0.0, 0.0006, (n, 1))
    ret = rng.normal(0.0003, 0.012, (n, len(d))) + drift
    tr = 100.0 * np.cumprod(1.0 + ret, axis=1)
    return {"dates": d, "tr": tr, "op_tr": tr, "adv": np.full((n, len(d)), 5e7)}


def _score(panel, t):
    tr = panel["tr"]
    return tr[:, t] / tr[:, max(0, t - 63)] - 1.0


def _noise(panel, t):
    """A signal with no persistence: it churns, pays cost and earns nothing.
    Deterministic in t, so the run is reproducible."""
    rng = np.random.default_rng(1000 + int(t))
    return rng.normal(0.0, 1.0, panel["tr"].shape[0])


def _elig(panel, t):
    return np.ones(panel["tr"].shape[0], dtype=bool)


def momentum(row):
    return {"book": B.BOOK_EQUITY_TOPN, "panel": _panel(11),
            "score_fn": _score, "signal_sign": 1,
            "book_kwargs": {"elig": _elig, "top_n": 15, "cadence": 21,
                            "horizon": 21}}


def dead(row):
    return {"book": B.BOOK_EQUITY_TOPN, "panel": _panel(11),
            "score_fn": _noise, "signal_sign": 1,
            "book_kwargs": {"elig": _elig, "top_n": 15, "cadence": 21,
                            "horizon": 21}}


def broken(row):
    raise ValueError("this executor cannot build its panel")


EXECUTORS = {"momentum": momentum, "dead": dead, "broken": broken}
'''


def test_the_runner_halts_a_dead_signal_before_the_lockbox(pipe, tmp_path):
    reg = _prereg(pipe)
    spec = _campaign(tmp_path, EXECUTOR_SRC,
                     [{"experiment_id": reg["experiment_id"],
                       "executor": "dead", "book": B.BOOK_EQUITY_TOPN}])
    out = RUN.run_campaign(pipe, spec)
    row = out["results"][0]
    assert row["state"] == RUN.RUN_HALTED
    assert row["lockbox_computed"] is False
    assert out["lockboxes_computed"] == 0
    stages = [e["detail"]["stage"] for e in
              pipe._events(P.EV_STAGE, reg["experiment_id"])]
    assert "L" not in stages


def test_a_halted_cell_reads_as_settled_in_the_ledger(pipe, tmp_path):
    """A halted cell HAS an outcome and counts to the burden. Reporting it as
    PREREGISTERED told a reader nine experiments were still pending when nine
    had already answered - the live Wave-2 run surfaced exactly that."""
    reg = _prereg(pipe)
    spec = _campaign(tmp_path, EXECUTOR_SRC,
                     [{"experiment_id": reg["experiment_id"],
                       "executor": "dead", "book": B.BOOK_EQUITY_TOPN}])
    RUN.run_campaign(pipe, spec)
    row = [r for r in pipe.ledger()
           if r["EXPERIMENT_ID"] == reg["experiment_id"]][0]
    assert row["SURVIVOR_STATE"] == "HALTED_AT_D"
    assert row["HALTED_AT"] == "D"
    assert row["STAGES_REVEALED"] == ["D"]
    assert row["LOCKBOX_COMPUTED"] is False


def test_the_runner_is_idempotent(pipe, tmp_path):
    reg = _prereg(pipe)
    spec = _campaign(tmp_path, EXECUTOR_SRC,
                     [{"experiment_id": reg["experiment_id"],
                       "executor": "dead", "book": B.BOOK_EQUITY_TOPN}])
    first = RUN.run_campaign(pipe, spec)["results"][0]
    second = RUN.run_campaign(pipe, spec)["results"][0]
    assert first["state"] == RUN.RUN_HALTED
    assert second["state"] == RUN.RUN_ALREADY
    # and no second set of stage events was written
    stages = [e["detail"]["stage"] for e in
              pipe._events(P.EV_STAGE, reg["experiment_id"])]
    assert stages == ["D"]


def test_a_failing_executor_is_recorded_and_never_stops_the_batch(
        pipe, tmp_path):
    a = _prereg(pipe)
    b = _prereg(pipe, hypothesis="second synthetic", parameters={"lb": 21})
    spec = _campaign(tmp_path, EXECUTOR_SRC, [
        {"experiment_id": a["experiment_id"], "executor": "broken",
         "book": B.BOOK_EQUITY_TOPN},
        {"experiment_id": b["experiment_id"], "executor": "dead",
         "book": B.BOOK_EQUITY_TOPN}])
    out = RUN.run_campaign(pipe, spec)
    assert out["experiments"] == 2
    assert out["by_state"].get(RUN.RUN_FAILED) == 1
    assert out["by_state"].get(RUN.RUN_HALTED) == 1


def test_the_spec_is_validated_before_anything_is_measured(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"campaign_id": "x"}), encoding="utf-8")
    with pytest.raises(RUN.CampaignRefusal, match="missing"):
        RUN.load_campaign_spec(p)
    p.write_text(json.dumps({"campaign_id": "x", "executor_module": "m.py",
                             "experiments": [{"experiment_id": "H",
                                              "executor": "e",
                                              "book": "NOT_A_BOOK"}]}),
                 encoding="utf-8")
    with pytest.raises(RUN.CampaignRefusal, match="unknown book"):
        RUN.load_campaign_spec(p)


def test_the_runner_writes_no_second_registry(pipe, tmp_path):
    """Every durable fact goes through the pipeline into the ONE memory."""
    src = (REPO / "alpha_agent" / "agents_v2" / "runner.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)
    called = {n.func.attr for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    forbidden = {"register", "record_result", "event", "freeze_qualified",
                 "execute", "commit"}
    assert not (called & forbidden), sorted(called & forbidden)
    assert "sqlite3" not in src


def test_the_runner_cannot_order_fill_promote_or_adopt():
    src = (REPO / "alpha_agent" / "agents_v2" / "runner.py").read_text(
        encoding="utf-8")
    low = src.lower()
    for term in ("place_order", "submit_order", "execute_order", "create_fill",
                 "broker", "promote_champion", "prospective_adoption",
                 "adopt_forward", "capital_eligib"):
        assert term not in low, term
    cli = (REPO / "scripts" / "run_agents_v2_campaign.py").read_text(
        encoding="utf-8").lower()
    for term in ("place_order", "submit_order", "create_fill", "broker",
                 "adopt"):
        assert "%s(" % term not in cli, term


def test_the_cli_declares_its_terminal_tokens():
    src = (REPO / "scripts" / "run_agents_v2_campaign.py").read_text(
        encoding="utf-8")
    for token in ("CAMPAIGN_OK", "CAMPAIGN_REFUSED", "CAMPAIGN_FAILED"):
        assert token in src


# --------------------------------------------------------------------------- #
# The frozen evidence is still frozen
# --------------------------------------------------------------------------- #
def test_the_live_book_owner_delegates_and_never_reimplements():
    src = (REPO / "alpha_agent" / "agents_v2" / "books.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)
    defined = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    for book in ("run_futures_book", "run_equity_topn",
                 "run_equity_daily_tranche_book"):
        assert book not in defined, "%s must be re-exported, not rewritten" % book
        assert "%s = EV.%s" % (book, book) in src


def test_the_r56_campaign_evidence_is_unedited():
    """R56's modules are the bound evidence of a settled campaign. R57 repairs
    the OWNERS; it does not rewrite the record of what R56 measured."""
    assert FROZEN_EVALUATOR.exists()
    src = FROZEN_EVALUATOR.read_text(encoding="utf-8")
    # the guard R56 needed because the OWNER was broken is still there, and
    # the owner it guarded against is now fixed (test_a3_* above)
    assert "def assert_gate_safe" in src
    assert B.FROZEN_EVALUATOR.resolve() == FROZEN_EVALUATOR.resolve()
