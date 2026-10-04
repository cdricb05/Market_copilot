"""R89 - power-expanded alpha factory + research-only forward incubation.

Hermetic: every pipeline runs over a research memory inside ``tmp_path``.

What is proven, in the order the release brief asks for it:

    A  the final qualification thresholds are unchanged
    B  FDR / search burden is unchanged (an incubating cell is still charged)
    C  lockbox sequencing is unchanged
    D  an underpowered-but-clean cell is NOT marked historically qualified
    E  an underpowered-but-clean cell CAN enter research-only observation
    F  no backfill is possible
    G  no order / fill / proposal path is reachable from incubation
    H  negative measured evidence cannot escape into incubation
    I  correlated / overlapping observations are not counted as independent
    J  an agent prior alone cannot permanently close a mechanism
    K  existing outcome semantics are backward compatible
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent import agents_v2 as A
from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import contracts as C
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.r59 import engines as E
from paper_trader.alpha_agent.r59 import handlers as H
from paper_trader.alpha_agent.r59 import memory as M
from paper_trader.alpha_agent.r61 import halts as HALT
from paper_trader.alpha_agent.r61 import mechanism_power as MP
from paper_trader.api import capital_eligibility_gate as CEG
from paper_trader.api import prospective_adoption as PA

REPO = Path(__file__).resolve().parents[1]

#: The gate-schema hash frozen into every pre-registration BEFORE R89. If R89
#: had moved a final threshold this constant would no longer match.
GATE_SCHEMA_HASH_BEFORE_R89 = (
    "c73257e7eb0fe913ef204e4338436251c5239e54c9cfc999fc608410a08fedd4")

FUT_COST = {"rate_per_side": 0.0002, "basis": "traded notional"}


@pytest.fixture()
def pipe(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    monkeypatch.setenv(PA.ADOPTION_DIR_ENV, str(tmp_path / "adoption"))
    mem = M.ResearchMemory(tmp_path / "research_memory.sqlite")
    return P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


def _preregister(pipe, *, tag="a", agent=A.TREND_BREADTH, family="TREND",
                 mechanism="frozen mechanism"):
    ds, un, fs = "ds_%s" % tag, "u_%s" % tag, "fs_%s" % tag
    pipe.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id=ds, asset_classes=[r59.AC_CROSS_ASSET],
        pit_status=P.PIT_SAFE,
        availability_rule="value stamped at the instant it became observable",
        survivorship="expired contracts retained"))
    pipe.perform(A.UNIVERSE, "define_universe", dict(
        universe_id=un, dataset_id=ds, asset_class=r59.AC_CROSS_ASSET,
        rules="certified futures", execution_representation="XS_LONG_SHORT"))
    pipe.perform(A.FEATURES, "publish_features", dict(
        feature_set_id=fs, universe_id=un,
        features=[{"name": "macro_mom", "lag": 1, "source": ds,
                   "availability_instant": "release instant"}],
        leakage_check="PASS"))
    return pipe.perform(A.DIRECTOR, "preregister", dict(
        owning_agent=agent, hypothesis="R89 test cell %s" % tag,
        asset_class=r59.AC_CROSS_ASSET, family=family, feature_set_id=fs,
        horizon_sessions=21, mechanism=mechanism,
        parameters={"rule": "xs_rank", "cadence_sessions": 21},
        discovery_sample={"start": "2011-07-01", "end": "2018-01-01"},
        evaluation_sample={"validation": "2018-01-01",
                           "lockbox": "2023-01-01"},
        cost_model=FUT_COST, expected_sign=1, long_short=True,
        instrument_scope=["6A", "6B", "FDAX", "FESX"],
        information_family="MACRO_VINTAGE"))


def _sample(structure=MP.CROSS_SECTIONAL, n=17, rho=0.30, decisions=187,
            lockbox=44, cadence=21, horizon=21):
    return MP.effective_sample(structure, n_instruments=n,
                               n_decisions=decisions,
                               lockbox_decisions=lockbox, cadence=cadence,
                               horizon=horizon, mean_pairwise_correlation=rho)


def _assessment(mde=0.144, prior=0.05, structure=MP.CROSS_SECTIONAL,
                sample=None):
    return MP.assess(structure=structure, sample=sample or _sample(structure),
                     measured_mde_80=mde, measured_artifact="power/x.json",
                     agent_prior_ic=prior, agent_prior_source="director",
                     expression_id="X")


def _power_halt(pipe, reg, agent=A.TREND_BREADTH):
    return pipe.perform(agent, "record_pre_measurement_halt", dict(
        experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
        halt_reason="STATISTICAL_POWER_FLOOR", measured_metric="MDE_80_rho",
        measured_value=0.144, frozen_threshold=0.05,
        frozen_threshold_owner=MP.MECHANISM_POWER_OWNER,
        detail="POWER_WEAK on the certified 17-leg expression"))


def _incubate(pipe, reg, assessment=None, **kw):
    pipe.perform(A.DIRECTOR, "assess_power", dict(
        experiment_id=reg["experiment_id"],
        assessment=assessment or _assessment()))
    return pipe.perform(A.DIRECTOR, "request_forward_observation", dict(
        experiment_id=reg["experiment_id"],
        rationale="clean, PIT-safe, new information family; blocked on "
                  "power alone", frozen_decision_producer="campaign executor",
        **kw))


# --------------------------------------------------------------------------- #
# A. final thresholds unchanged
# --------------------------------------------------------------------------- #
def test_a_the_frozen_gate_schema_hash_is_unchanged():
    assert C.Contracts().gate_schema_hash == GATE_SCHEMA_HASH_BEFORE_R89


def test_a_the_final_gate_constants_are_read_not_written():
    assert r59.BH_Q == 0.10 and r59.OBS_FLOOR == 36
    assert r59.GATE_MATERIALITY_FLOORS == {"ann_net_excess": 0.015,
                                           "net_sharpe": 0.40}
    a = _assessment()
    assert a["final_gate_read_only"]["changed_by_this_module"] is False
    assert a["final_gate_read_only"]["bh_q"] == r59.BH_Q
    # an underpowered lockbox still fails engines.gate exactly as before
    weak = {"layers": {"V": {"net_sharpe": 0.15, "t_net": 1.1, "days": 1200},
                       "L": {"net_sharpe": 0.10, "t_net": 0.6,
                             "p_one_sided": 0.27, "days": 700}}}
    g = E.gate(weak, prior_burden=0, family_tests=1)
    assert not g["qualified"]
    assert "burden_corrected_significant" in g["failed_gates"]


def test_a_a_power_class_never_qualifies_anything():
    for cls in MP.POWER_CLASSES:
        assert MP.PATH_FOR_CLASS[cls] in MP.RESEARCH_PATHS
    assert "QUALIFIED" not in " ".join(MP.RESEARCH_PATHS)


# --------------------------------------------------------------------------- #
# B. burden unchanged: an incubating cell is still charged
# --------------------------------------------------------------------------- #
def test_b_an_incubating_cell_is_charged_to_the_burden(pipe):
    before = pipe.mem.burden(asset_class=r59.AC_CROSS_ASSET)["total"]
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    _incubate(pipe, reg)
    after = pipe.mem.burden(asset_class=r59.AC_CROSS_ASSET)["total"]
    assert after == before + 1
    den = H.search_denominator(pipe.mem, family_key=pipe.mem.get(
        reg["experiment_id"])["family_key"], asset_class=r59.AC_CROSS_ASSET,
        machine_generated=False, campaign_method=A.GENERATION_METHOD,
        within_family_tests=1)
    assert den["total"] >= 1


# --------------------------------------------------------------------------- #
# C. lockbox sequencing unchanged
# --------------------------------------------------------------------------- #
def test_c_incubation_reveals_no_layer_and_the_order_rule_still_holds(pipe):
    reg = _preregister(pipe)
    with pytest.raises(P.PipelineRefusal) as ex:
        pipe.perform(A.TREND_BREADTH, "reveal_stage", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            stage="L", stats={"net_sharpe": 1.0, "t_net": 4.0, "days": 700}))
    assert ex.value.code == "STAGE_OUT_OF_ORDER"
    _power_halt(pipe, reg)
    _incubate(pipe, reg)
    assert pipe._stage_events(reg["experiment_id"]) == []
    row = pipe.ledger()[0]
    assert row["LOCKBOX_COMPUTED"] is False and row["STAGES_REVEALED"] == []


# --------------------------------------------------------------------------- #
# D. not historically qualified
# --------------------------------------------------------------------------- #
def test_d_an_incubating_cell_is_data_hold_never_qualified(pipe):
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    out = _incubate(pipe, reg)
    row = pipe.mem.get(reg["experiment_id"])
    assert row["outcome"] == r59.HO_DATA_HOLD
    assert out["historically_qualified"] is False
    assert out["counts_as_qualified"] is False
    assert pipe.validated_survivors() == [] and pipe.survivors() == []
    assert H.freeze_qualified(pipe.mem, hypothesis_id=reg["experiment_id"])[
        "state"] == "NOT_QUALIFIED"
    led = pipe.ledger()[0]
    assert led["SURVIVOR_STATE"] == "DATA_HOLD"
    assert led["FORWARD_STATE"] == P.OBS_REQUESTED
    assert led["POWER_CLASS"] == MP.POWER_WEAK


# --------------------------------------------------------------------------- #
# E. CAN enter research-only forward observation
# --------------------------------------------------------------------------- #
def test_e_a_clean_underpowered_cell_enters_observation(pipe):
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    out = _incubate(pipe, reg)
    assert out["state"] == "OBSERVATION_REQUESTED"
    assert out["kind"] == "RESEARCH_ONLY_FORWARD_OBSERVATION_REQUEST"
    assert out["forward_state"] == P.OBS_REQUESTED
    assert out["exit_rules"]["vocabulary"] == list(P.OBSERVATION_EXITS)
    assert out["minimum_forward_evidence_owner"] == MP.MECHANISM_POWER_OWNER
    mfe = out["minimum_forward_evidence"]["at_feasible_edge_0_05"]
    assert mfe["effective_decisions_required"] >= r59.OBS_FLOOR
    assert Path(out["artifact"]).exists()
    assert pipe.forward_observation_candidates()[0]["experiment_id"] == \
        reg["experiment_id"]
    # idempotent
    again = pipe.perform(A.DIRECTOR, "request_forward_observation", dict(
        experiment_id=reg["experiment_id"], rationale="again",
        frozen_decision_producer="campaign executor"))
    assert again["state"] == "ALREADY_REQUESTED"
    assert len(pipe.forward_observation_candidates()) == 1


def test_e_an_injected_registrar_is_reached_and_recorded(pipe):
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    seen = {}

    def registrar(*, request, observation_clock_starts):
        seen["request"] = request
        seen["clock"] = observation_clock_starts
        return {"registered": True, "registration_id": "OBS-1"}

    out = _incubate(pipe, reg, register_observation=registrar)
    assert out["forward_state"] == P.OBS_REGISTERED
    # The observation clock is stamped by alpha_agent.r59.now_iso(), which is
    # UTC by design; comparing it with the LOCAL calendar date failed for the
    # hours after 00:00 UTC (R89 continuation, 2026-09-30). Compare against
    # the same authoritative UTC clock, never against local time.
    assert seen["clock"] == r59.now_iso()[:10]
    assert seen["clock"] == _dt.datetime.now(_dt.timezone.utc).date().isoformat()
    assert seen["request"]["capital_eligible"] is False


def test_e_marginal_class_is_admissible_and_unidentifiable_is_not(pipe):
    reg = _preregister(pipe, tag="m")
    _power_halt(pipe, reg)
    out = _incubate(pipe, reg, assessment=_assessment(mde=0.08))
    assert out["power_class"] == MP.POWER_MARGINAL
    reg2 = _preregister(pipe, tag="u")
    _power_halt(pipe, reg2)
    with pytest.raises(P.PipelineRefusal) as ex:
        _incubate(pipe, reg2, assessment=_assessment(mde=0.40))
    assert ex.value.code == "INCUBATION_NOT_ELIGIBLE"
    assert "POWER_UNIDENTIFIABLE" in str(ex.value)


# --------------------------------------------------------------------------- #
# F. no backfill
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("key", P.BACKFILL_KEYS)
def test_f_no_backfill_argument_exists(pipe, key):
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    with pytest.raises(P.PipelineRefusal) as ex:
        _incubate(pipe, reg, **{key: "2020-01-01"})
    assert ex.value.code == "BACKFILL_CANNOT_BE_REQUESTED"
    assert pipe.forward_observation_candidates() == []


def test_f_the_request_carries_zero_observations_and_no_history(pipe):
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    out = _incubate(pipe, reg)
    assert out["forward_observations_at_request"] == 0
    assert out["backfill"] is False and out["synthetic_history"] is False
    assert out["observation_clock"].startswith("DERIVED_BY_")


# --------------------------------------------------------------------------- #
# G. no operational path
# --------------------------------------------------------------------------- #
def test_g_no_operational_verb_and_the_request_is_flagged(pipe):
    for v in P._VERBS:
        for bad in ("order", "fill", "approve", "promote", "execute",
                    "allocate", "capital"):
            assert bad not in v
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    out = _incubate(pipe, reg)
    for k in ("capital_eligible", "promotion_allowed", "may_become_champion",
              "historically_qualified", "counts_as_qualified"):
        assert out[k] is False
    assert out["operational_effect"] == "NONE"
    body = json.loads(Path(out["artifact"]).read_text(encoding="utf-8"))
    assert body["safety"]["creates_orders"] is False
    assert body["safety"]["makes_sleeve_capital_eligible"] is False
    # the capital gate evaluates DECLARED challengers only; none is ours
    assert all(A.RELEASE not in str(c.get("challenger_id"))
               for c in CEG.declared_candidates())
    with pytest.raises(P.PipelineRefusal) as ex:
        pipe.perform(A.DIRECTOR, "request_forward_observation", dict(
            experiment_id=reg["experiment_id"], rationale="x",
            frozen_decision_producer="p", create_order=True))
    assert ex.value.code == "UNKNOWN_REQUEST_FIELD"


def test_g_only_the_director_may_request_and_only_after_assessment(pipe):
    reg = _preregister(pipe)
    _power_halt(pipe, reg)
    with pytest.raises(P.PipelineRefusal) as ex:
        pipe.perform(A.PUBLISHING, "request_forward_observation", dict(
            experiment_id=reg["experiment_id"], rationale="x",
            frozen_decision_producer="p"))
    assert ex.value.code == "VERB_NOT_IN_AGENT_CONTRACT"
    with pytest.raises(P.PipelineRefusal) as ex:
        pipe.perform(A.DIRECTOR, "request_forward_observation", dict(
            experiment_id=reg["experiment_id"], rationale="x",
            frozen_decision_producer="p"))
    assert "8_NO_POWER_ASSESSMENT" in str(ex.value)


# --------------------------------------------------------------------------- #
# H. negative evidence cannot escape
# --------------------------------------------------------------------------- #
def test_h_a_negative_discovery_layer_is_refused(pipe):
    reg = _preregister(pipe)
    pipe.perform(A.TREND_BREADTH, "reveal_stage", dict(
        experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
        stage="D", stats={"net_sharpe": -0.3, "t_net": -1.2, "days": 1600}))
    assert pipe.mem.get(reg["experiment_id"])["outcome"] == \
        r59.HO_NO_ALPHA_EVIDENCE
    with pytest.raises(P.PipelineRefusal) as ex:
        _incubate(pipe, reg)
    assert "10_NEGATIVE_MEASURED_EVIDENCE:NEGATIVE_LAYER_D" in str(ex.value)


def test_h_a_cost_or_turnover_halt_is_not_a_power_halt(pipe):
    reg = _preregister(pipe)
    pipe.perform(A.TREND_BREADTH, "record_pre_measurement_halt", dict(
        experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
        halt_reason="COST_BUDGET_EXCEEDED", measured_metric="ann_cost_drag",
        measured_value=0.05, frozen_threshold=0.03))
    with pytest.raises(P.PipelineRefusal) as ex:
        _incubate(pipe, reg)
    assert "OUTCOME_REJECTED" in str(ex.value)


def test_h_a_skeptic_kill_on_placebo_is_not_underpowered(pipe):
    reg = _preregister(pipe)
    eid, sh = reg["experiment_id"], reg["spec_hash"]
    layers = {"D": {"net_sharpe": 0.5, "t_net": 2.0, "days": 1600},
              "V": {"net_sharpe": 0.3, "t_net": 1.5, "days": 1200},
              "L": {"net_sharpe": 0.45, "t_net": 2.5, "p_one_sided": 0.006,
                    "days": 700}}
    for s in r59.STAGES:
        pipe.perform(A.TREND_BREADTH, "reveal_stage", dict(
            experiment_id=eid, spec_hash=sh, stage=s, stats=layers[s]))
    pipe.perform(A.TREND_BREADTH, "submit_candidate", dict(
        experiment_id=eid, spec_hash=sh, signal_sign=1, cost_model=FUT_COST,
        turnover=0.2, layers=layers, effective_cost_per_side=0.0002))
    checks = {cid: {"passed": True, "measured": 1.0, "evidence": "x"}
              for cid in C.Contracts().required_adversarial_checks()}
    checks["placebo_clean"] = {"passed": False, "measured": -0.1,
                               "evidence": "placebo beat it"}
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(experiment_id=eid,
                                                   checks=checks))
    with pytest.raises(P.PipelineRefusal) as ex:
        _incubate(pipe, reg, assessment=_assessment(mde=0.08))
    # the skeptic REJECTED it (the gate qualified; the placebo did not), and
    # a rejection is negative evidence however underpowered the panel is
    assert "10_NEGATIVE_MEASURED_EVIDENCE:OUTCOME_REJECTED" in str(ex.value)
    assert "8_NOT_SETTLED_BY_A_SAMPLE_INSUFFICIENT_HALT" in str(ex.value)
    assert pipe._negative_measured_evidence(
        eid, pipe.mem.get(eid)) == "OUTCOME_REJECTED"


# --------------------------------------------------------------------------- #
# I. dependence is never counted as independence
# --------------------------------------------------------------------------- #
def test_i_perfectly_correlated_legs_are_one_instrument():
    assert MP.effective_instruments(68, 1.0) == pytest.approx(1.0)
    assert MP.effective_instruments(68, 0.0) == 68
    s = _sample(n=6, rho=0.85)
    assert s["effective_instruments"] < 1.3


def test_i_overlapping_windows_are_divided_out():
    s = _sample(decisions=787, lockbox=188, cadence=5, horizon=21)
    assert s["overlap_factor"] == pytest.approx(21 / 5)
    assert s["effective_decisions"] == pytest.approx(787 / (21 / 5))
    s2 = _sample(decisions=787, lockbox=188, cadence=5, horizon=5)
    assert s2["effective_decisions"] == 787


def test_i_timing_and_event_structures_do_not_multiply_by_width():
    ts = MP.effective_sample(MP.TIME_SERIES, n_instruments=14,
                             n_decisions=187, lockbox_decisions=44,
                             mean_pairwise_correlation=0.6)
    assert ts["independent_decisions"] == 187
    ev = MP.effective_sample(MP.EVENT_DRIVEN, n_events=8 * 6 * 15,
                             n_event_clusters=8 * 15, n_decisions=120,
                             lockbox_decisions=29, cadence=1, horizon=1)
    assert ev["independent_decisions"] == 8 * 15
    assert ev["lockbox_clears_obs_floor"] is False


def test_i_a_lockbox_below_the_obs_floor_is_unidentifiable():
    s = _sample(lockbox=20)
    a = MP.assess(structure=MP.CROSS_SECTIONAL, sample=s, measured_mde_80=0.02,
                  measured_artifact="power/x.json")
    assert a["power_class"] == MP.POWER_UNIDENTIFIABLE


# --------------------------------------------------------------------------- #
# J. an agent prior cannot close a mechanism
# --------------------------------------------------------------------------- #
def test_j_a_tiny_prior_below_the_mde_closes_nothing():
    a = _assessment(mde=0.045, prior=0.001)
    r = MP.g7_ruling(a)
    assert r["closure"] == MP.CLOSURE_NONE
    assert r["research_path"] == MP.PATH_HISTORICAL
    assert r["agent_prior_used_for_closure"] is False
    assert a["agent_prior"]["is_closure_basis"] is False
    assert a["agent_prior"]["use"] == MP.AGENT_PRIOR_USE


def test_j_unidentifiable_closes_the_expression_only_never_permanently():
    r = MP.g7_ruling(_assessment(mde=0.40, prior=0.9))
    assert r["closure"] == MP.CLOSURE_EXPRESSION_ONLY
    assert r["closure_is_permanent"] is False
    assert "WIDTH_OR_DECISIONS_OR_CADENCE" in r["reopen_condition"]
    assert "PERMANENT" not in " ".join(MP.CLOSURE_KINDS)


def test_j_a_prior_unknown_is_a_state_not_a_number():
    a = MP.assess(structure=MP.CROSS_SECTIONAL, sample=_sample(),
                  measured_mde_80=0.06, measured_artifact="power/x.json")
    assert a["agent_prior"]["ic"] is None
    assert a["agent_prior"]["source"] == MP.PRIOR_UNKNOWN
    assert MP.g7_ruling(a)["priority_hint"]["agent_prior"] == MP.PRIOR_UNKNOWN


def test_j_a_hand_typed_assessment_is_refused(pipe):
    reg = _preregister(pipe)
    fake = _assessment()
    fake["power_class"] = MP.POWER_STRONG            # edited after the owner
    with pytest.raises(P.PipelineRefusal) as ex:
        pipe.perform(A.DIRECTOR, "assess_power", dict(
            experiment_id=reg["experiment_id"], assessment=fake))
    assert ex.value.code == "POWER_ASSESSMENT_NOT_FROM_OWNER"
    with pytest.raises(ValueError):
        MP.g7_ruling(fake)


def test_j_r88_cells_reclassify_as_paths_not_closures():
    # The three R88 cells, at their measured MDE_80 and declared priors.
    for mde, prior, expect in ((0.144, 0.05, MP.POWER_WEAK),
                               (0.110, 0.05, MP.POWER_WEAK),
                               (0.090, 0.015, MP.POWER_MARGINAL),
                               (0.256, 0.05, MP.POWER_UNIDENTIFIABLE)):
        sample = _sample(decisions=787, lockbox=188, cadence=5, horizon=5) \
            if mde == 0.090 else _sample()
        a = MP.assess(structure=MP.CROSS_SECTIONAL, sample=sample,
                      measured_mde_80=mde, measured_artifact="power/x.json",
                      agent_prior_ic=prior)
        assert a["power_class"] == expect
        assert MP.g7_ruling(a)["closure_is_permanent"] is False


# --------------------------------------------------------------------------- #
# K. backward compatibility
# --------------------------------------------------------------------------- #
def test_k_existing_halt_reasons_and_outcomes_are_unchanged():
    assert HALT.outcome_for("DATA_COVERAGE_FLOOR") == r59.HO_DATA_HOLD
    assert HALT.outcome_for("COST_BUDGET_EXCEEDED") == r59.HO_REJECTED
    assert HALT.outcome_for("STATISTICAL_POWER_FLOOR") == r59.HO_DATA_HOLD
    with pytest.raises(HALT.HaltRefusal):
        HALT.build(experiment_id="X", halt_reason="STATISTICAL_POWER_FLOOR",
                   measured_metric="m", measured_value=1, frozen_threshold=0,
                   outcome=r59.HO_NO_ALPHA_EVIDENCE)
    assert r59.HO_DATA_HOLD not in r59.TERMINAL_OUTCOMES
    assert set(r59.HYPOTHESIS_OUTCOMES) == {
        "REJECTED", "NO_ALPHA_EVIDENCE", "QUALIFIED", "DATA_HOLD",
        "FORWARD_FROZEN", "NEEDS_MORE_EVIDENCE"}


def test_k_the_verbs_and_contracts_cohere():
    assert set(P._VERBS) == set(
        C.load_contract("agent_contracts.json")["pipeline_verbs"])
    assert C.Contracts().verbs_for(A.DIRECTOR) == (
        "preregister", "director_clear", "assess_power",
        "request_forward_observation")
    assert C.validate() == []


def test_k_a_measured_and_killed_cell_reads_exactly_as_before(pipe):
    reg = _preregister(pipe)
    pipe.perform(A.TREND_BREADTH, "reveal_stage", dict(
        experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
        stage="D", stats={"net_sharpe": 0.0, "t_net": 0.0, "days": 1600}))
    row = pipe.ledger()[0]
    assert row["SURVIVOR_STATE"] == "HALTED_AT_D"
    assert row["FORWARD_STATE"] == "NONE"
    assert row["POWER_CLASS"] == "NOT_ASSESSED"
    for k in ("EXPERIMENT_ID", "AGENT", "HYPOTHESIS", "ASSET_CLASS", "FAMILY",
              "FEATURE_SET", "HORIZON", "PARAMETERS", "DISCOVERY_SAMPLE",
              "EVALUATION_SAMPLE", "PIT_STATUS", "TURNOVER", "COST_MODEL",
              "RESULT", "SKEPTIC_VERDICT", "RISK_VERDICT", "SURVIVOR_STATE",
              "FORWARD_STATE"):
        assert k in row


def test_k_the_analytic_fallback_is_labelled_and_the_measured_one_is_owned():
    a = MP.assess(structure=MP.TIME_SERIES,
                  sample=MP.effective_sample(MP.TIME_SERIES, n_instruments=6,
                                             n_decisions=187,
                                             lockbox_decisions=44,
                                             mean_pairwise_correlation=0.8))
    assert a["mde"]["basis"] == MP.MDE_BASIS_ANALYTIC
    assert a["mc_measurable"] is False
    with pytest.raises(ValueError):
        MP.assess(structure=MP.TIME_SERIES,
                  sample=MP.effective_sample(MP.TIME_SERIES, n_instruments=6,
                                             n_decisions=187,
                                             lockbox_decisions=44),
                  measured_mde_80=0.1, measured_artifact="x")
    b = _assessment()
    assert b["mde"]["basis"] == MP.MDE_BASIS_MEASURED
    assert b["mde"]["owner"] == MP.MEASURED_MDE_OWNER
