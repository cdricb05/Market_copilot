"""Recorded facts move the global multi-asset frontier without a catalog edit.

Two gaps kept the agent from continuing on its own after the global frontier was
built:

* a human gate a person had ANSWERED (authorised, declined, deferred) kept being
  surfaced, because the frontier only knew the declared gate;
* an executor verdict recorded in ResearchMemory (NO_EDGE, DATA_HOLD) left the OPEN
  candidate holding a SETTLED mechanism, which was an owner conflict that BLOCKED
  every mechanism mandate until someone edited the catalog.

These tests prove the derivation that closes both: decisions are recorded inputs
whose effect is recomputed on every build, and executor verdicts close or hold their
own candidate while the agent moves to the next admitted opportunity - including a
passive TRUE_FORWARD wait above it, which never idles the agent.
"""
from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
for p in (str(_ROOT), str(_ROOT / "tests")):
    if p not in sys.path:
        sys.path.insert(0, p)

from alpha_agent import r59  # noqa: E402
from alpha_agent.r59 import global_frontier as GF  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402
from alpha_agent.r59 import memory as M  # noqa: E402

T = importlib.import_module("test_alpha_agent_global_multi_asset_frontier")

WEAK = dict(net_after_cost_pa=None, t_stat=None, increment_t=None,
            untouched_confirmation="NONE", multiplicity="NOT_RUN")


def _decision(cid: str, kind: str, decision: str, **over) -> dict:
    d = {"decision_id": "HD_%s_%s" % (cid, decision), "candidate_id": cid, "gate_kind": kind,
         "subject": "SUBJECT", "decision": decision, "decided_on": "2026-09-14",
         "decided_by": "fixture human operator",
         "reason": "a fixture decision reason a person wrote, at least thirty characters long"}
    d.update(over)
    return d


def _with(cat: dict, decisions: list) -> dict:
    cat[GF.CATALOG_SECTION]["human_decisions"] = decisions
    return cat


@pytest.fixture()
def agent_env(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    cat = tmp_path / "catalog.json"
    monkeypatch.setenv(MX.CATALOG_PATH_ENV, str(cat))
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.setenv(MX.REPO_ROOT_ENV, str(repo))
    return types.SimpleNamespace(tmp=tmp_path, catalog=cat)


# --------------------------------------------------------------------------- #
# Recorded human decisions
# --------------------------------------------------------------------------- #
def test_a_declined_human_gate_closes_its_candidate_and_it_leaves_the_ranking():
    zn = T._open("GC_ZN", [], ac="RATES", source_artifact="fixture_result.json",
                 next_action=T._action(GF.NA_HUMAN_PREREG, research_days=2, information_gain=0.6))
    other = T._open("GC_OTHER", ["o"])
    cat = _with(T._catalog([zn, other]),
                [_decision("GC_ZN", GF.NA_HUMAN_PREREG, GF.HD_DECLINED, subject="ZN_POSTHOC_CONTROL")])
    fr = GF.build(cat, T._estate(r46=[T._lb("o")]))
    assert fr["state"] == GF.ST_COMPLETE
    z = T._by_id(fr)["GC_ZN"]
    assert z["current_state"] == GF.CS_CLOSED and z["global_rank"] is None
    assert z["declared_disposition"] == GF.DISP_OPEN and "HUMAN_DECLINED" in z["derivation"]
    assert z["closed_reason"].startswith("HUMAN_DECLINED on 2026-09-14")
    assert "GC_ZN" not in fr["global_top_ids"]
    assert fr["human_decisions"][0]["effective"] == GF.HDE_CLOSED
    assert fr["human_decisions"][0]["gate_still_waiting_on_a_person"] is False


def test_an_authorised_registration_waits_for_execution_then_becomes_passive_accrual():
    fx = T._open("GC_FX", ["r46_fx", "AR_FX"],
                 next_action=T._action(GF.NA_REGISTER, research_days=0.5, information_gain=0.7))
    build = T._open("GC_BUILD", ["b"], ac="RATES", historical=T._hist(**WEAK),
                    next_action=T._action(GF.NA_BUILD, research_days=3, information_gain=0.9))
    cat = _with(T._catalog([fx, build]),
                [_decision("GC_FX", GF.NA_REGISTER, GF.HD_AUTHORISED, subject="AR_FX")])
    base = dict(r46=[T._lb("r46_fx"), T._lb("b")], ar=[{"challenger_id": "AR_FX"}])

    waiting = GF.build(cat, T._estate(**base))
    assert waiting["state"] == GF.ST_COMPLETE
    assert T._by_id(waiting)["GC_FX"]["next_best_action"]["kind"] == GF.NA_REGISTER
    assert waiting["human_decisions"][0]["effective"] == GF.HDE_AWAITING
    assert waiting["human_decisions"][0]["gate_still_waiting_on_a_person"] is True

    registered = GF.build(cat, T._estate(**base, registry=[
        {"challenger_id": "AR_FX", "identity_hash": "h", "horizon_sessions": 1}]))
    nba = T._by_id(registered)["GC_FX"]["next_best_action"]
    assert nba["kind"] == GF.NA_ACCRUE and nba["requires_human"] is False
    assert registered["human_decisions"][0]["effective"] == GF.HDE_EXECUTED
    assert registered["next_agent_research_action"]["candidate_id"] == "GC_BUILD"


def test_a_deferred_purchase_stops_surfacing_until_it_becomes_globally_justified():
    buy = T._open("GC_BUY", ["buy"], ac="US_EQUITY", historical=T._hist(**WEAK),
                  next_action=T._action(GF.NA_PURCHASE, research_days=12, data_cost_usd=332,
                                        information_gain=0.9))
    strong = T._open("GC_STRONG", ["strong"], ac="RATES",
                     next_action=T._action(GF.NA_BUILD, research_days=2, information_gain=0.9))
    dec = [_decision("GC_BUY", GF.NA_PURCHASE, GF.HD_DEFERRED, resurface_when=GF.V_PURCHASE_JUSTIFIED)]

    fr = GF.build(_with(T._catalog([buy, strong]), dec), T._ranked_estate("buy", "strong"))
    assert fr["human_decisions"][0]["effective"] == GF.HDE_DEFERRED
    assert fr["next_global_action"]["candidate_id"] == "GC_STRONG"
    assert GF.compare_proposal(fr, "GC_BUY")["verdict"] == GF.V_PURCHASE_NOT_JUSTIFIED

    alone = GF.build(_with(T._catalog([buy]), dec), T._ranked_estate("buy"))
    assert alone["human_decisions"][0]["effective"] == GF.HDE_RESURFACED
    assert alone["human_decisions"][0]["gate_still_waiting_on_a_person"] is True


def test_a_human_decision_must_answer_the_gate_its_candidate_declares():
    c = T._open("GC_FX", ["x"], next_action=T._action(GF.NA_REGISTER))
    bad = [_decision("GC_FX", GF.NA_PURCHASE, GF.HD_DECLINED),
           _decision("GC_NOPE", GF.NA_REGISTER, GF.HD_DECLINED),
           _decision("GC_FX", GF.NA_REGISTER, GF.HD_DEFERRED)]
    fr = GF.build(_with(T._catalog([c]), bad), T._estate(r46=[T._lb("x")]))
    assert fr["state"] == GF.ST_INVALID
    probs = fr["reconciliation"]["problems"]
    assert any(p.startswith("HUMAN_DECISION_GATE_MISMATCH") for p in probs)
    assert any(p.startswith("HUMAN_DECISION_UNKNOWN_CANDIDATE") for p in probs)
    assert any(p.startswith("HUMAN_DECISION_DEFERRAL_NEEDS_A_RESURFACE_CONDITION") for p in probs)


# --------------------------------------------------------------------------- #
# Executor verdicts: AUTO_CONTINUES_AFTER_NO_EDGE / AUTO_CONTINUES_AFTER_DATA_HOLD
# --------------------------------------------------------------------------- #
def _two_builds(first_id: str = "M_FIRST"):
    first = T._open("GC_FIRST", [first_id], ac="US_EQUITY",
                    next_action=T._action(GF.NA_BUILD, research_days=2, information_gain=0.9))
    second = T._open("GC_SECOND", ["M_SECOND"], ac="RATES", historical=T._hist(**WEAK),
                     next_action=T._action(GF.NA_BUILD, research_days=6, information_gain=0.9))
    return T._catalog([first, second], mechanisms=[T._mech(first_id), T._mech("M_SECOND")])


def test_an_executed_no_edge_closes_its_candidate_and_the_agent_moves_to_the_next_opportunity():
    cat = _two_builds()
    live = GF.build(cat, T._estate(mechanisms=[
        {"mechanism_id": "M_FIRST", "status": MX.ST_ELIGIBLE, "executor_state": MX.EX_NOT_BUILT},
        {"mechanism_id": "M_SECOND", "status": MX.ST_ELIGIBLE, "executor_state": MX.EX_NOT_BUILT}]))
    assert [q["candidate_id"] for q in live["agent_executable_queue"]] == ["GC_FIRST"]
    assert live["agent_executable_queue"][0]["path"] == GF.PATH_MECHANISM

    settled = GF.build(cat, T._estate(mechanisms=[
        {"mechanism_id": "M_FIRST", "status": MX.ST_SETTLED_CLOSED, "reasons": ["NO_EDGE: signed IC t 0.41"]},
        {"mechanism_id": "M_SECOND", "status": MX.ST_ELIGIBLE, "executor_state": MX.EX_NOT_BUILT}]))
    assert settled["state"] == GF.ST_COMPLETE
    assert settled["reconciliation"]["owner_conflicts"] == []
    first = T._by_id(settled)["GC_FIRST"]
    assert first["current_state"] == GF.CS_CLOSED
    assert first["closed_reason"].startswith("CLOSED_BY_AGENT_EXECUTION") and "NO_EDGE" in first["closed_reason"]
    assert settled["next_agent_research_action"]["candidate_id"] == "GC_SECOND"
    assert [q["candidate_id"] for q in settled["agent_executable_queue"]] == ["GC_SECOND"]


def test_a_data_hold_blocks_only_its_own_candidate_and_is_no_rival():
    cat = _two_builds("M_HELD")
    fr = GF.build(cat, T._estate(mechanisms=[
        {"mechanism_id": "M_HELD", "status": MX.ST_SETTLED_HOLD, "reasons": ["DATA_HOLD: coverage 86 % < 90 %"]},
        {"mechanism_id": "M_SECOND", "status": MX.ST_ELIGIBLE}]))
    assert fr["state"] == GF.ST_COMPLETE
    held = T._by_id(fr)["GC_FIRST"]
    assert held["current_state"] == GF.CS_BLOCKED
    assert held["next_best_action"]["blocked_by_owner_evidence"] is True
    assert held["next_best_action"]["executable_by_agent"] is False
    cmp = GF.compare_proposal(fr, "GC_SECOND")
    assert cmp["admitted"] is True
    assert [c["candidate_id"] for c in cmp["blocked_top5"]] == ["GC_FIRST"]
    assert fr["next_agent_research_action"]["candidate_id"] == "GC_SECOND"
    assert fr["next_global_action"]["candidate_id"] == "GC_SECOND"


def test_a_qualified_verdict_becomes_a_human_registration_gate_never_a_registration():
    cat = _two_builds()
    fr = GF.build(cat, T._estate(mechanisms=[
        {"mechanism_id": "M_FIRST", "status": MX.ST_SETTLED_QUALIFIED, "reasons": ["QUALIFIED"]},
        {"mechanism_id": "M_SECOND", "status": MX.ST_ELIGIBLE}]))
    q = T._by_id(fr)["GC_FIRST"]
    assert q["current_state"] == GF.CS_HUMAN_GATE
    assert q["next_best_action"]["kind"] == GF.NA_REGISTER and q["next_best_action"]["requires_human"]
    assert fr["safety"]["registers_forward_challenger"] is False


# --------------------------------------------------------------------------- #
# TRUE_FORWARD_WAIT_DOES_NOT_IDLE_AGENT
# --------------------------------------------------------------------------- #
def test_passive_true_forward_accrual_above_never_idles_the_agent():
    passive = [T._open("GC_PASSIVE_%d" % i, ["p%d" % i], historical=T._hist(net_after_cost_pa=0.3, t_stat=4.0),
                       next_action=T._action(GF.NA_ACCRUE, information_gain=0.9)) for i in range(5)]
    event = T._open("GC_EVENT", ["evt"], ac="EQUITY_INDEX", historical=T._hist(**WEAK),
                    next_action=T._action(GF.NA_HISTORICAL, research_days=3, information_gain=0.5))
    fr = GF.build(T._catalog(passive + [event]), T._ranked_estate(*["p%d" % i for i in range(5)], "evt"))
    assert set(fr["global_top_ids"][:5]) == {"GC_PASSIVE_%d" % i for i in range(5)}
    assert fr["next_agent_research_action"]["candidate_id"] == "GC_EVENT"
    assert [(q["candidate_id"], q["path"]) for q in fr["agent_executable_queue"]] == [("GC_EVENT", GF.PATH_DECLARE)]


# --------------------------------------------------------------------------- #
# The agent checkpoint
# --------------------------------------------------------------------------- #
def test_the_checkpoint_stops_surfacing_answered_gates_and_names_what_a_build_needs(agent_env, monkeypatch):
    fx = T._open("GC_FX", ["r46_fx"], next_action=T._action(GF.NA_REGISTER, information_gain=0.7))
    buy = T._open("GC_BUY", ["M_BUY"], ac="US_EQUITY", historical=T._hist(**WEAK),
                  next_action=T._action(GF.NA_PURCHASE, research_days=12, data_cost_usd=332, information_gain=0.9))
    build = T._open("GC_BUILD", ["M_BUILD"], ac="RATES",
                    next_action=T._action(GF.NA_BUILD, research_days=2, information_gain=0.9))
    cat = T._catalog([fx, buy, build], mechanisms=[T._mech("M_BUY", requires_purchase=True), T._mech("M_BUILD")])
    cat = _with(cat, [_decision("GC_BUY", GF.NA_PURCHASE, GF.HD_DEFERRED, resurface_when=GF.V_PURCHASE_JUSTIFIED)])
    agent_env.catalog.write_text(json.dumps(cat), encoding="utf-8")
    estate = T._estate(r46=[T._lb("r46_fx")], mechanisms=[
        {"mechanism_id": "M_BUY", "status": MX.ST_HUMAN_GATE, "asset_class": r59.AC_US_EQUITY},
        {"mechanism_id": "M_BUILD", "status": MX.ST_ELIGIBLE, "asset_class": r59.AC_RATES}])
    monkeypatch.setattr(GF, "load_estate", lambda mem=None, catalog=None, paths=None: estate)
    body = MX.checkpoint(M.open_memory())

    pending = body["HUMAN_GATES"]
    assert not any(g.get("mechanism_id") == "M_BUY" for g in pending)
    assert any(g["gate"] == "GLOBAL_FRONTIER_HUMAN_DECISION" and g["candidate_id"] == "GC_FX" for g in pending)
    answered = body["HUMAN_DECISIONS_RECORDED"]["gates_answered"]
    assert [g["mechanism_id"] for g in answered if g["gate"] == "PURCHASE"] == ["M_BUY"]
    assert answered[0]["human_decision"]["effective"] == GF.HDE_DEFERRED
    cur = body["CURRENT_RESEARCH_ACTION"]
    assert (cur["action"], cur["mechanism_id"]) == ("BUILD_EXECUTOR", "M_BUILD")
    assert cur["build_requires"] == MX.BUILD_REQUIRES


# --------------------------------------------------------------------------- #
# The committed decisions
# --------------------------------------------------------------------------- #
def test_the_committed_catalog_records_the_three_human_decisions(monkeypatch):
    monkeypatch.delenv(MX.CATALOG_PATH_ENV, raising=False)
    cat = MX.load_catalog()
    rec = GF.reconciliation(cat)
    assert GF.validate_reconciliation(rec, cat) == []
    got = {d["subject"]: d for d in rec["human_decisions"]}
    assert got["ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7"]["decision"] == GF.HD_AUTHORISED
    assert got["TREASURY_MONTH_END_ZN_POSTHOC_CONTROL"]["decision"] == GF.HD_DECLINED
    options = got["SINGLE_NAME_OPTIONS_PURCHASE"]
    assert options["decision"] == GF.HD_DEFERRED
    assert options["resurface_when"] == GF.V_PURCHASE_JUSTIFIED
