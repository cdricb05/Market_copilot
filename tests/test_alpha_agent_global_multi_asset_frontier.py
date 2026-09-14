"""The ONE global multi-asset alpha frontier (``alpha_agent.r59.global_frontier``).

The mechanism frontier ranked only the mechanisms declared in its catalog and
saw six hand-typed live candidates, while the estate held about 120 candidate
identities across twelve owners in every asset class. A new equity mechanism
could therefore outrank an FX, credit, volatility or rates candidate simply
because the older candidate lived in an owner the ranking never read. These
tests prove the reconciliation layer that closes that error:

* every owner identity is claimed by an opportunity or the frontier is BLOCKED
  (R46, R63/R64, TRUE_FORWARD, FX, credit and volatility identities included);
* all nine asset classes are present, and a class is never EXHAUSTED because
  several of its mechanisms failed;
* the score is blind to asset class, release, declaration date and executor, and
  a good strategy with incomplete evidence outranks a bad one with complete
  evidence;
* every research task and every purchase must beat the global top five, and the
  mechanism frontier offers, executes and checkpoints only what is admitted;
* no second research memory, registry, forward ledger or portfolio path exists;
* the committed reconciliation claims the whole R46 contract, and it reconciles
  the live estate.
"""
from __future__ import annotations

import ast
import hashlib
import json
import re
import sys
import types
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import autonomous_research as AR  # noqa: E402
from alpha_agent import r59  # noqa: E402
from alpha_agent.r51 import promotion_frontier as PF  # noqa: E402
from alpha_agent.r59 import global_frontier as GF  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402
from alpha_agent.r59 import memory as M  # noqa: E402


# --------------------------------------------------------------------------- #
# Fixture builders
# --------------------------------------------------------------------------- #
def _hist(**over) -> dict:
    h = {"summary": "a declared historical summary citing the owner artifact that measured it",
         "evidence": ["fixture"], "net_after_cost_pa": 0.05, "t_stat": 2.5, "increment_pa": 0.03,
         "increment_t": 2.0, "increment_control": "fixture control",
         "untouched_confirmation": "CONFIRMED", "multiplicity": "PASS", "max_drawdown": -0.2,
         "cost_survives_2x": True, "pit": "PIT_TRUE"}
    h.update(over)
    return h


def _judgement(**over) -> dict:
    j = {"diversification": 0.5, "liquidity_capacity": 0.5, "probability_alive": 0.5,
         "rationale": "fixture judgement declared before the ranking was computed"}
    j.update(over)
    return j


def _action(kind: str = GF.NA_ACCRUE, **over) -> dict:
    a = {"kind": kind, "description": "a fixture next action described in at least thirty characters",
         "research_days": 0, "data_cost_usd": 0, "information_gain": 0.5,
         "human_gate": "a human decides this fixture gate" if kind in GF.HUMAN_KINDS else None}
    a.update(over)
    return a


def _open(cid: str, members, ac: str = "FX", **over) -> dict:
    c = {"candidate_id": cid, "title": "fixture %s" % cid, "disposition": GF.DISP_OPEN,
         "asset_class": ac, "information_asset_classes": [ac],
         "mechanism_class": "RISK_TRANSFER_PREMIUM", "information_object": "FIXTURE:%s" % cid,
         "members": list(members), "closed_families": [], "historical": _hist(),
         "judgement": _judgement(), "next_action": _action(),
         "data_requirement": "owned fixture data; no purchase"}
    c.update(over)
    return c


def _closed(cid: str, members, ac: str = "FX") -> dict:
    return {"candidate_id": cid, "title": "fixture %s" % cid, "disposition": GF.DISP_CLOSED,
            "asset_class": ac, "information_asset_classes": [ac],
            "mechanism_class": "RISK_TRANSFER_PREMIUM", "information_object": "FIXTURE:%s" % cid,
            "members": list(members), "closed_families": [],
            "historical": {"summary": "a closed fixture family with a measured negative verdict",
                           "evidence": []},
            "closed_reason": "closed on a measured negative verdict recorded by its owner"}


def _gate() -> dict:
    g = {f: "a declared answer written before any return exists, for field %s" % f
         for f in MX.PNL_GATE_FIELDS}
    g["KILL_RULE"] = "Kill if NW t < 2.0 or net below 1.5 %/yr at 1 bp per side"
    g["WHY_NOT_DUPLICATIVE"] = "no closed family shares this information object at all"
    return g


def _mech(mid: str, **over) -> dict:
    e = {"mechanism_id": mid, "title": "fixture %s" % mid.lower(),
         "domain": "STRUCTURAL_FLOWS_FORCED_TRADING", "mechanism_class": "FORCED_TRADING_FLOW",
         "origin": "MECHANISM_FIRST", "declared_before_returns": True, "declared_at": "2026-09-13",
         "data_version": 1, "asset_class": r59.AC_RATES, "horizon_sessions": 5,
         "model_family": "FIXTURE", "information_objects": ["FIXTURE:%s" % mid],
         "work_conditions": ["TESTS_DISTINCT_PNL_MECHANISM"], "pnl_gate": _gate(),
         "score_inputs": {**{k: 0.5 for k in MX.UNIT_INPUTS}, "implementation_days": 2,
                          "data_cost_usd": 0},
         "requires_purchase": False, "reopen": None, "executor": None}
    e.update(over)
    return e


def _catalog(candidates, *, asset_classes=None, closed_ledger=None, live=None,
             mechanisms=None) -> dict:
    return {"schema": MX.SCHEMA, "declared_before_returns": True, "live_candidates": live or [],
            "closed_mechanisms": closed_ledger or [], "mechanisms": mechanisms or [],
            "data_gaps": [],
            GF.CATALOG_SECTION: {"schema": GF.RECONCILIATION_SCHEMA, "declared_at": "2026-09-14",
                                 "closed_family_asset_classes": {},
                                 "asset_classes": asset_classes or {},
                                 "candidates": list(candidates)}}


def _lb(cid: str, *, ac: str = "FX", horizon: int = 5, emitted: int = 0, matured: int = 0,
        eff: float = 0, t=None, state: str = "FORWARD_PENDING", release: str = "R46") -> dict:
    row = {k: None for k in GF.LEADERBOARD_FIELDS}
    row.update({"challenger_id": cid, "asset_class": ac, "horizon": horizon, "state": state,
                "source_release": release, "forward_predictions_emitted": emitted,
                "forward_predictions_matured": matured, "effective_independent": eff,
                "t_stat": t})
    return row


def _estate(*, r46=(), registry=(), accrual=None, r63=(), r64=(), r58=(), ar=(), memory=(),
            live=(), mechanisms=(), ranked=(), closed_ledger=(), r51=()) -> dict:
    def owner(**kw):
        return {"present": True, "path": "fixture", **kw}
    return {"schema": "fixture", "paths": {}, "owners": {
        GF.OW_R46: owner(rows=[dict(r) for r in r46]),
        GF.OW_R46_ADOPTED: owner(by_adopted={}),
        GF.OW_REGISTRY: owner(rows=[dict(r) for r in registry]),
        GF.OW_ACCRUAL: owner(by_identity=dict(accrual or {})),
        GF.OW_R51: owner(promotion_ready_count=0, rows=[dict(r) for r in r51]),
        GF.OW_R63: owner(records=[dict(r) for r in r63]),
        GF.OW_R64: owner(records=[dict(r) for r in r64]),
        GF.OW_R58: owner(records=[dict(r) for r in r58]),
        GF.OW_AR: owner(records=[dict(r) for r in ar]),
        GF.OW_R59: owner(records=[]),
        GF.OW_MEMORY: owner(records=[dict(r) for r in memory]),
        GF.OW_CATALOG: owner(catalog_hash="fixture", live_candidates=list(live),
                             mechanisms=[dict(m) for m in mechanisms], ranked=list(ranked),
                             closed_mechanisms=list(closed_ledger)),
    }}


def _by_id(frontier: dict) -> dict:
    return {c["candidate_id"]: c for c in frontier["candidates"]}


def _inv(frontier: dict, name: str) -> str:
    return frontier["invariants"][name]["value"]


# --------------------------------------------------------------------------- #
# OLD_FORWARD_CANDIDATES_RECONCILED / R46_CANDIDATES_NOT_SILENTLY_DROPPED
# --------------------------------------------------------------------------- #
def test_an_unclaimed_r46_challenger_blocks_the_frontier_instead_of_disappearing():
    estate = _estate(r46=[_lb("r46_fx_carry"), _lb("r46_credit_spread", ac="CREDIT")])
    blocked = GF.build(_catalog([_open("GC_FX", ["r46_fx_carry"])]), estate)
    assert blocked["state"] == GF.ST_UNRECONCILED
    assert blocked["GLOBAL_MULTI_ASSET_FRONTIER"] == "BLOCKED"
    assert {"owner": GF.OW_R46, "id": "r46_credit_spread"} in blocked["reconciliation"]["unreconciled"]
    assert _inv(blocked, "R46_CANDIDATES_NOT_SILENTLY_DROPPED") == "NO"
    assert _inv(blocked, "OLD_FORWARD_CANDIDATES_RECONCILED") == "NO"
    assert _inv(blocked, "GLOBAL_MULTI_ASSET_FRONTIER") == "NO"

    fixed = GF.build(_catalog([_open("GC_FX", ["r46_fx_carry"]),
                               _open("GC_CREDIT", ["r46_credit_spread"], ac="CREDIT")]), estate)
    assert fixed["state"] == GF.ST_COMPLETE
    assert _inv(fixed, "R46_CANDIDATES_NOT_SILENTLY_DROPPED") == "YES"
    assert _inv(fixed, "OLD_FORWARD_CANDIDATES_RECONCILED") == "YES"
    assert _by_id(fixed)["GC_CREDIT"]["members"][0]["owners"] == [GF.OW_R46]


def test_a_listed_live_candidate_and_a_frozen_memory_hypothesis_must_be_claimed():
    estate = _estate(memory=[{"challenger_id": "R58_FROZEN", "asset_class": "US_EQUITY"}],
                     live=["SKEW_LIVE"])
    fr = GF.build(_catalog([_open("GC_SKEW", ["SKEW_LIVE"], ac="EQUITY_INDEX")]), estate)
    assert {"owner": GF.OW_MEMORY, "id": "R58_FROZEN"} in fr["reconciliation"]["unreconciled"]
    assert _inv(fr, "OLD_FORWARD_CANDIDATES_RECONCILED") == "NO"


# --------------------------------------------------------------------------- #
# R63_R64_CANDIDATES_RECONCILED
# --------------------------------------------------------------------------- #
def test_r63_and_r64_records_are_claimed_and_carry_their_owner_classification():
    r63 = {"challenger_id": "R63_FX_CARRY", "classification": "READY_FOR_FORWARD_QUALIFICATION",
           "asset_class": "FX_FUTURES"}
    r64 = {"challenger_id": "R64_FX_CARRY", "classification": "MORE_RESEARCH_REQUIRED",
           "asset_class": "FX_FUTURES"}
    estate = _estate(r63=[r63], r64=[r64])
    missing = GF.build(_catalog([_open("GC_FX", ["R63_FX_CARRY"])]), estate)
    assert missing["state"] == GF.ST_UNRECONCILED
    assert _inv(missing, "R63_R64_CANDIDATES_RECONCILED") == "NO"
    fr = GF.build(_catalog([_open("GC_FX", ["R63_FX_CARRY", "R64_FX_CARRY"])]), estate)
    assert _inv(fr, "R63_R64_CANDIDATES_RECONCILED") == "YES"
    states = {m["id"]: m["owner_states"] for m in _by_id(fr)["GC_FX"]["members"]}
    assert states["R63_FX_CARRY"][GF.OW_R63] == "READY_FOR_FORWARD_QUALIFICATION"
    assert states["R64_FX_CARRY"][GF.OW_R64] == "MORE_RESEARCH_REQUIRED"


def test_closing_a_candidate_that_holds_a_ready_owner_record_is_an_unresolved_conflict():
    estate = _estate(r63=[{"challenger_id": "R63_READY",
                           "classification": "READY_FOR_FORWARD_QUALIFICATION"}])
    fr = GF.build(_catalog([_closed("GC_CLOSED", ["R63_READY"])]), estate)
    assert fr["state"] == GF.ST_CONFLICT
    assert fr["reconciliation"]["owner_conflicts"][0]["conflict"] == \
        "CLOSED_CANDIDATE_HOLDS_A_READY_OWNER_RECORD"


# --------------------------------------------------------------------------- #
# TRUE_FORWARD_CANDIDATES_RECONCILED
# --------------------------------------------------------------------------- #
def test_registrations_are_reconciled_and_their_counts_are_read_from_the_accrual_owner():
    reg = {"challenger_id": "SKEW_NEXT_OPEN", "identity_hash": "abc", "asset_class": "US_ETF",
           "horizon_sessions": 5, "first_eligible_observation_session": "2026-09-14"}
    accrual = {"abc": {"current_accrual_state": "EMITTED", "predictions_emitted": 3,
                       "matured_observations": 1, "effective_independent_observations": 1},
               "dangling": {"current_accrual_state": "EMITTED", "predictions_emitted": 1}}
    estate = _estate(registry=[reg], accrual=accrual)
    fr = GF.build(_catalog([_open("GC_SKEW", ["SKEW_NEXT_OPEN"], ac="EQUITY_INDEX")]), estate)
    assert {"owner": GF.OW_ACCRUAL, "id": "ACCRUAL_IDENTITY:dangling"} in fr["reconciliation"]["unreconciled"]
    assert _inv(fr, "TRUE_FORWARD_CANDIDATES_RECONCILED") == "NO"

    estate["owners"][GF.OW_ACCRUAL]["by_identity"].pop("dangling")
    fr = GF.build(_catalog([_open("GC_SKEW", ["SKEW_NEXT_OPEN"], ac="EQUITY_INDEX")]), estate)
    skew = _by_id(fr)["GC_SKEW"]
    assert fr["state"] == GF.ST_COMPLETE and _inv(fr, "TRUE_FORWARD_CANDIDATES_RECONCILED") == "YES"
    assert skew["current_state"] == GF.CS_TRUE_FORWARD
    assert skew["forward_observations"]["emitted"] == 3
    assert skew["forward_observations"]["matured"] == 1
    assert skew["forward_observations"]["floor"] == 40


def test_a_registered_clock_that_has_not_emitted_is_forward_pending():
    reg = {"challenger_id": "SKEW", "identity_hash": "h", "asset_class": "US_ETF",
           "horizon_sessions": 5}
    estate = _estate(registry=[reg], accrual={"h": {"current_accrual_state": "NOT_DUE",
                                                    "predictions_emitted": 0}})
    fr = GF.build(_catalog([_open("GC_SKEW", ["SKEW"], ac="EQUITY_INDEX")]), estate)
    assert _by_id(fr)["GC_SKEW"]["current_state"] == GF.CS_FORWARD_PENDING


# --------------------------------------------------------------------------- #
# FX / CREDIT / VOLATILITY _CANDIDATES_RECONCILED
# --------------------------------------------------------------------------- #
def test_every_fx_credit_and_volatility_labelled_identity_must_be_claimed():
    estate = _estate(r46=[_lb("vx_carry", ac="VOLATILITY")],
                     r63=[{"challenger_id": "fx_cell", "asset_class": "FX_FUTURES"}],
                     memory=[{"challenger_id": "credit_momentum", "asset_class": "CREDIT_PROXY"}])
    fr = GF.build(_catalog([_open("GC_FX", ["fx_cell"]),
                            _open("GC_VOL", ["vx_carry"], ac="VOLATILITY")]), estate)
    assert _inv(fr, "FX_CANDIDATES_RECONCILED") == "YES"
    assert _inv(fr, "VOLATILITY_CANDIDATES_RECONCILED") == "YES"
    assert _inv(fr, "CREDIT_CANDIDATES_RECONCILED") == "NO"
    fr = GF.build(_catalog([_open("GC_FX", ["fx_cell"]),
                            _open("GC_VOL", ["vx_carry"], ac="VOLATILITY"),
                            _open("GC_CREDIT", ["credit_momentum"], ac="CREDIT")]), estate)
    assert _inv(fr, "CREDIT_CANDIDATES_RECONCILED") == "YES"
    assert fr["asset_classes"]["CREDIT"]["strongest_candidate"] == "GC_CREDIT"


# --------------------------------------------------------------------------- #
# ALL_REQUIRED_ASSET_CLASSES_PRESENT / ASSET_CLASS_CANNOT_DISAPPEAR
# --------------------------------------------------------------------------- #
def test_all_nine_asset_classes_are_present_even_when_most_have_no_candidate():
    fr = GF.build(_catalog([_open("GC_FX", ["fx"])]), _estate(r46=[_lb("fx")]))
    assert tuple(fr["asset_classes"]) == GF.REQUIRED_ASSET_CLASSES
    for ac, row in fr["asset_classes"].items():
        assert row["state"] in GF.ASSET_CLASS_STATES, ac
        for key in ("strongest_candidate", "tested_mechanisms", "closed_mechanisms",
                    "open_mechanisms", "data_gaps", "next_highest_value_action",
                    "reason_not_currently_ranked_higher"):
            assert key in row, (ac, key)
    assert fr["asset_classes"]["CRYPTO"]["state"] == GF.AS_BLOCKED
    assert _inv(fr, "ALL_REQUIRED_ASSET_CLASSES_PRESENT") == "YES"
    assert _inv(fr, "ASSET_CLASS_CANNOT_DISAPPEAR") == "YES"


def test_a_candidate_outside_the_required_asset_classes_is_refused():
    fr = GF.build(_catalog([_open("GC_OPTIONS", ["o"], ac="OPTIONS")]), _estate(r46=[_lb("o")]))
    assert fr["state"] == GF.ST_INVALID
    assert any(p.startswith("ASSET_CLASS_UNKNOWN") for p in fr["reconciliation"]["problems"])


# --------------------------------------------------------------------------- #
# WHOLE_ASSET_CLASS_NOT_EXHAUSTED_FROM_PARTIAL_FAILURES
# --------------------------------------------------------------------------- #
def test_an_asset_class_of_closed_mechanisms_is_blocked_not_exhausted_without_a_declaration():
    ledger = [{"family_id": "RATES_CARRY", "asset_scope": "RATES_FUTURES", "verdict": "closed"},
              {"family_id": "RATES_RV", "asset_scope": "RATES_FUTURES", "verdict": "closed"}]
    estate = _estate(r46=[_lb("fx"), _lb("rates_a", ac="RATES", state="DATA_BLOCKED")],
                     closed_ledger=ledger)
    cands = [_open("GC_FX", ["fx"]), _closed("GC_RATES_A", ["rates_a"], ac="RATES")]
    fr = GF.build(_catalog(cands, closed_ledger=ledger), estate)
    rates = fr["asset_classes"]["RATES"]
    assert rates["state"] == GF.AS_BLOCKED
    assert {m["id"] for m in rates["closed_mechanisms"]} == {"GC_RATES_A", "RATES_CARRY", "RATES_RV"}
    assert _inv(fr, "WHOLE_ASSET_CLASS_NOT_EXHAUSTED_FROM_PARTIAL_FAILURES") == "YES"

    declared = {"RATES": {"data_gaps": [], "declared_exhaustion": {
        "mechanisms": ["RATES_CARRY", "RATES_RV", "GC_RATES_A"],
        "why": "every declared rates mechanism was measured and closed on untouched evidence"}}}
    fr = GF.build(_catalog(cands, closed_ledger=ledger, asset_classes=declared), estate)
    assert fr["asset_classes"]["RATES"]["state"] == GF.AS_EXHAUSTED
    assert fr["asset_classes"]["RATES"]["declared_exhaustion"]["mechanisms"]

    vague = {"RATES": {"data_gaps": [], "declared_exhaustion": {
        "mechanisms": ["SEVERAL_THINGS_FAILED"], "why": "several rates mechanisms failed so rates is done"}}}
    fr = GF.build(_catalog(cands, closed_ledger=ledger, asset_classes=vague), estate)
    assert fr["state"] == GF.ST_INVALID


def test_an_exhaustion_declaration_cannot_hide_an_open_candidate():
    ledger = [{"family_id": "RATES_CARRY", "asset_scope": "RATES_FUTURES", "verdict": "closed"}]
    declared = {"RATES": {"data_gaps": [], "declared_exhaustion": {
        "mechanisms": ["RATES_CARRY"], "why": "every declared rates mechanism was measured and closed"}}}
    fr = GF.build(_catalog([_open("GC_RATES", ["r"], ac="RATES")], closed_ledger=ledger,
                           asset_classes=declared),
                  _estate(r46=[_lb("r", ac="RATES")], closed_ledger=ledger))
    assert fr["asset_classes"]["RATES"]["state"] != GF.AS_EXHAUSTED


# --------------------------------------------------------------------------- #
# EQUITY / RECENT_MECHANISM / EXECUTOR_AVAILABILITY _HAS_NO_PRIORITY_BONUS
# --------------------------------------------------------------------------- #
def test_the_score_is_blind_to_asset_class_release_declaration_date_and_executor():
    base = dict(historical=_hist(), judgement=_judgement(), next_action=_action(GF.NA_BUILD))
    equity = _open("GC_EQUITY", ["e"], ac="US_EQUITY", declared_at="2026-09-14", release="NEW",
                   executor={"callable": "alpha_agent.x:run", "module_sha256": "pinned"}, **base)
    rates = _open("GC_RATES", ["r"], ac="RATES", declared_at="2019-01-01", release="R32",
                  executor=None, **base)
    fr = GF.build(_catalog([equity, rates]),
                  _estate(r46=[_lb("e", ac="US_EQUITY"), _lb("r", ac="RATES")]))
    got = _by_id(fr)
    assert got["GC_EQUITY"]["opportunity_cost_score"] == got["GC_RATES"]["opportunity_cost_score"]
    assert got["GC_EQUITY"]["score"]["quality"] == got["GC_RATES"]["score"]["quality"]
    assert GF.score_is_blind()
    for name in ("EQUITY_HAS_NO_PRIORITY_BONUS", "RECENT_MECHANISM_HAS_NO_PRIORITY_BONUS",
                 "EXECUTOR_AVAILABILITY_HAS_NO_PRIORITY_BONUS"):
        assert _inv(fr, name) == "YES"
    tree = ast.parse((_ROOT / "alpha_agent" / "r59" / "global_frontier.py").read_text(encoding="utf-8"))
    scoring = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
               and n.name in ("score_candidate", "quality_components", "completeness_components")]
    assert len(scoring) == 3
    for fn in scoring:
        words = {n.value for n in ast.walk(fn) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        words |= {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
        words |= {n.attr for n in ast.walk(fn) if isinstance(n, ast.Attribute)}
        assert not words & {"asset_class", "declared_at", "release", "executor", "executor_state",
                            "domain", "DOMAIN_BONUS"}, fn.name


def test_a_good_strategy_with_incomplete_evidence_outranks_a_bad_one_with_complete_evidence():
    good = _open("GC_GOOD", ["good"], historical=_hist(net_after_cost_pa=0.2, t_stat=3.0),
                 judgement=_judgement(probability_alive=0.6))
    bad = _open("GC_BAD", ["bad"], ac="US_EQUITY",
                historical=_hist(net_after_cost_pa=-0.05, t_stat=-1.0, increment_t=-1.5,
                                 untouched_confirmation="FAILED", multiplicity="FAIL",
                                 max_drawdown=-0.5, cost_survives_2x=False),
                judgement=_judgement(probability_alive=0.1))
    estate = _estate(r46=[_lb("good", emitted=0), _lb("bad", ac="US_EQUITY", emitted=60, matured=50,
                                                      eff=38, t=0.5)])
    fr = GF.build(_catalog([bad, good]), estate)
    got = _by_id(fr)
    assert got["GC_GOOD"]["global_rank"] == 1 and got["GC_BAD"]["global_rank"] == 2
    assert got["GC_BAD"]["score"]["completeness"] > got["GC_GOOD"]["score"]["completeness"]
    assert got["GC_GOOD"]["evidence_quality"] == GF.EQ_GOOD_INCOMPLETE
    assert got["GC_BAD"]["evidence_quality"] == GF.EQ_BAD_COMPLETE


def test_forward_evidence_moves_the_probability_alive_only_once_it_is_decisive():
    cand = [_open("GC_X", ["x"])]
    thin = GF.build(_catalog(cand), _estate(r46=[_lb("x", emitted=6, matured=6, eff=1, t=-5.0)]))
    deep = GF.build(_catalog(cand), _estate(r46=[_lb("x", emitted=30, matured=30, eff=10, t=-5.0)]))
    assert _by_id(thin)["GC_X"]["score"]["quality_components"]["probability_alive"] == 0.5
    assert _by_id(deep)["GC_X"]["score"]["quality_components"]["probability_alive"] == \
        pytest.approx(0.5 * GF.ALIVE_ON_BAD_FORWARD)


# --------------------------------------------------------------------------- #
# GLOBAL_OPPORTUNITY_COST_REQUIRED / PURCHASE_REQUIRES_GLOBAL_COMPARISON
# --------------------------------------------------------------------------- #
def _ranked_estate(*ids: str):
    return _estate(r46=[_lb(i) for i in ids])


def test_a_research_task_must_beat_every_agent_executable_action_in_the_global_top_five():
    cands = [
        _open("GC_HUMAN", ["human"], next_action=_action(GF.NA_REGISTER, information_gain=0.9),
              historical=_hist(net_after_cost_pa=0.3, t_stat=4.0)),
        _open("GC_STRONG_BUILD", ["strong_build"], ac="RATES",
              next_action=_action(GF.NA_BUILD, research_days=3, information_gain=0.9)),
        _open("GC_WEAK_BUILD", ["weak_build"], ac="US_EQUITY",
              historical=_hist(net_after_cost_pa=None, t_stat=None, increment_t=None,
                               untouched_confirmation="NONE", multiplicity="NOT_RUN"),
              judgement=_judgement(probability_alive=0.2),
              next_action=_action(GF.NA_BUILD, research_days=6, information_gain=0.9)),
    ]
    fr = GF.build(_catalog(cands), _estate(r46=[_lb("human", emitted=0),
                                                _lb("strong_build", emitted=0),
                                                _lb("weak_build", emitted=0)]))
    weak = GF.compare_proposal(fr, "GC_WEAK_BUILD")
    assert weak["admitted"] is False and weak["verdict"] == GF.V_LOSES
    assert weak["beaten_by"][0]["candidate_id"] == "GC_STRONG_BUILD"
    assert weak["question"] == GF.OPPORTUNITY_COST_QUESTION and "GC_STRONG_BUILD" in weak["why"]
    strong = GF.compare_proposal(fr, "GC_STRONG_BUILD")
    assert strong["admitted"] is True and strong["verdict"] == GF.V_BEATS
    assert [c["candidate_id"] for c in strong["human_gated_top5"]] == ["GC_HUMAN"]
    assert "GC_HUMAN" in strong["why"]
    assert _inv(fr, "GLOBAL_OPPORTUNITY_COST_REQUIRED") == "YES"
    assert all(c.get("opportunity_cost_comparison") for c in fr["candidates"]
               if c["global_rank"] and c["next_best_action"]["kind"] not in GF.PASSIVE_KINDS)


def test_a_purchase_must_beat_every_research_action_and_human_decision_in_the_top_five():
    purchase = _open("GC_PURCHASE", ["purchase"], ac="US_EQUITY",
                     historical=_hist(net_after_cost_pa=None, t_stat=None, increment_t=None,
                                      untouched_confirmation="NONE", multiplicity="NOT_RUN"),
                     next_action=_action(GF.NA_PURCHASE, research_days=12, data_cost_usd=332,
                                         information_gain=0.9))
    human = _open("GC_FX_REGISTRATION", ["human"],
                  next_action=_action(GF.NA_REGISTER, information_gain=0.7))
    passive = _open("GC_PASSIVE_TOP", ["passive"], ac="EQUITY_INDEX",
                    historical=_hist(net_after_cost_pa=0.3, t_stat=4.0),
                    next_action=_action(GF.NA_ACCRUE, information_gain=0.9))
    fr = GF.build(_catalog([purchase, human, passive]), _ranked_estate("purchase", "human", "passive"))
    cmp = GF.compare_proposal(fr, "GC_PURCHASE")
    assert cmp["is_purchase"] and cmp["verdict"] == GF.V_PURCHASE_NOT_JUSTIFIED
    assert cmp["beaten_by"][0]["candidate_id"] == "GC_FX_REGISTRATION"
    assert [c["proposal"] for c in fr["purchase_comparisons"]] == ["GC_PURCHASE"]
    assert _inv(fr, "PURCHASE_REQUIRES_GLOBAL_COMPARISON") == "YES"
    assert fr["answers"]["FX_VERSUS_THE_SINGLE_NAME_OPTIONS_PURCHASE"] is None

    alone = GF.build(_catalog([purchase, passive]), _ranked_estate("purchase", "passive"))
    justified = GF.compare_proposal(alone, "GC_PURCHASE")
    assert justified["verdict"] == GF.V_PURCHASE_JUSTIFIED
    assert [c["candidate_id"] for c in justified["passive_top5"]] == ["GC_PASSIVE_TOP"]


def test_nothing_is_admitted_while_the_global_frontier_is_incomplete():
    estate = _estate(r46=[_lb("claimed"), _lb("orphaned_owner_id")])
    fr = GF.build(_catalog([_open("GC_BUILD", ["claimed"],
                                  next_action=_action(GF.NA_BUILD, research_days=2))]), estate)
    cmp = GF.compare_proposal(fr, "GC_BUILD")
    assert cmp["admitted"] is False and cmp["verdict"] == GF.V_FRONTIER_INCOMPLETE
    assert GF.mechanism_comparison(fr, "not_a_member")["admitted"] is False


# --------------------------------------------------------------------------- #
# The mechanism frontier resumes on the global frontier
# --------------------------------------------------------------------------- #
@pytest.fixture()
def agent_env(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    cat = tmp_path / "catalog.json"
    monkeypatch.setenv(MX.CATALOG_PATH_ENV, str(cat))
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.setenv(MX.REPO_ROOT_ENV, str(repo))
    return types.SimpleNamespace(tmp=tmp_path, catalog=cat)


def _job(mid: str):
    return types.SimpleNamespace(job_id="job_%s" % mid.lower(), lane=r59.LANE_MECHANISM,
                                 payload={"payload": {"source": MX.SOURCE, "mechanism_id": mid}})


def test_the_agent_offers_executes_and_checkpoints_only_globally_admitted_mechanisms(agent_env, monkeypatch):
    strong = _open("GC_STRONG", ["M_STRONG"], ac="RATES",
                   next_action=_action(GF.NA_BUILD, research_days=2, information_gain=0.9))
    weak = _open("GC_WEAK", ["M_WEAK"], ac="US_EQUITY",
                 historical=_hist(net_after_cost_pa=None, t_stat=None, increment_t=None,
                                  untouched_confirmation="NONE", multiplicity="NOT_RUN"),
                 judgement=_judgement(probability_alive=0.2),
                 next_action=_action(GF.NA_BUILD, research_days=6, information_gain=0.9))
    cat = _catalog([strong, weak], mechanisms=[_mech("M_STRONG"), _mech("M_WEAK")])
    agent_env.catalog.write_text(json.dumps(cat), encoding="utf-8")
    estate = _estate(mechanisms=[
        {"mechanism_id": "M_STRONG", "status": MX.ST_ELIGIBLE, "asset_class": r59.AC_RATES},
        {"mechanism_id": "M_WEAK", "status": MX.ST_ELIGIBLE, "asset_class": r59.AC_US_EQUITY}],
        ranked=["M_STRONG", "M_WEAK"])
    monkeypatch.setattr(GF, "load_estate", lambda mem=None, catalog=None, paths=None: estate)
    mem = M.open_memory()

    rows = MX.candidates(mem)
    assert [r["payload"]["mechanism_id"] for r in rows] == ["M_STRONG"]
    assert rows[0]["payload"]["global_verdict"] == GF.V_BEATS
    assert rows[0]["payload"]["global_candidate_id"] == "GC_STRONG"

    outcome, detail = MX.execute_job(mem, _job("M_WEAK"))
    assert outcome == AR.OUTCOME_BLOCKED_SPECIFIC
    assert detail["disposition"] == MX.ST_DEFERRED_GLOBAL and "GC_STRONG" in detail["reason"]
    assert mem.events(kind="MECHANISM_DEFERRED_BY_GLOBAL_OPPORTUNITY_COST")

    body = MX.checkpoint(mem)
    assert body["CURRENT_RESEARCH_ACTION"]["mechanism_id"] == "M_STRONG"
    assert body["CURRENT_RESEARCH_ACTION"]["global_opportunity_cost"]["verdict"] == GF.V_BEATS
    assert [d["mechanism_id"] for d in body["DEFERRED_BY_GLOBAL_OPPORTUNITY_COST"]] == ["M_WEAK"]
    assert body["GLOBAL_MULTI_ASSET_FRONTIER"]["state"] == GF.ST_COMPLETE

    MX.write_checkpoint(mem)
    written = json.loads(GF.artifact_path().read_text(encoding="utf-8"))
    assert written["schema"] == GF.SCHEMA and written["state"] == GF.ST_COMPLETE


def test_a_purchase_gate_in_the_checkpoint_carries_its_global_comparison(agent_env, monkeypatch):
    human = _open("GC_FX", ["r46_fx"], next_action=_action(GF.NA_REGISTER, information_gain=0.7))
    buy = _open("GC_BUY", ["M_BUY"], ac="US_EQUITY",
                historical=_hist(net_after_cost_pa=None, t_stat=None, increment_t=None,
                                 untouched_confirmation="NONE", multiplicity="NOT_RUN"),
                next_action=_action(GF.NA_PURCHASE, research_days=12, data_cost_usd=332,
                                    information_gain=0.9))
    cat = _catalog([human, buy], mechanisms=[_mech("M_BUY", requires_purchase=True)])
    agent_env.catalog.write_text(json.dumps(cat), encoding="utf-8")
    estate = _estate(r46=[_lb("r46_fx")], mechanisms=[
        {"mechanism_id": "M_BUY", "status": MX.ST_HUMAN_GATE, "asset_class": r59.AC_US_EQUITY}])
    monkeypatch.setattr(GF, "load_estate", lambda mem=None, catalog=None, paths=None: estate)
    mem = M.open_memory()

    body = MX.checkpoint(mem)
    purchase = [g for g in body["HUMAN_GATES"] if g["gate"] == "PURCHASE"]
    assert purchase[0]["global_comparison"]["verdict"] == GF.V_PURCHASE_NOT_JUSTIFIED
    assert any(g["gate"] == "GLOBAL_FRONTIER_HUMAN_DECISION" and g["candidate_id"] == "GC_FX"
               for g in body["HUMAN_GATES"])
    outcome, detail = MX.execute_job(mem, _job("M_BUY"))
    assert outcome == AR.OUTCOME_BLOCKED_SPECIFIC and detail["disposition"] == MX.ST_DEFERRED_GLOBAL


def test_a_catalog_without_the_reconciliation_keeps_the_mechanism_only_behaviour(agent_env, monkeypatch):
    cat = _catalog([], mechanisms=[_mech("ONLY")])
    cat.pop(GF.CATALOG_SECTION)
    agent_env.catalog.write_text(json.dumps(cat), encoding="utf-8")
    monkeypatch.setattr(GF, "load_estate", lambda *a, **k: pytest.fail("estate must not be read"))
    mem = M.open_memory()
    assert [r["payload"]["mechanism_id"] for r in MX.candidates(mem)] == ["ONLY"]
    assert MX.checkpoint(mem)["GLOBAL_MULTI_ASSET_FRONTIER"] == {"state": "NOT_DECLARED"}


# --------------------------------------------------------------------------- #
# NO_SECOND_RESEARCH_MEMORY / NO_SECOND_FORWARD_LEDGER / NO_PORTFOLIO_MUTATION
# --------------------------------------------------------------------------- #
def test_a_declaration_that_carries_forward_state_is_refused_as_a_second_ledger():
    cand = _open("GC_X", ["x"], historical=_hist(predictions_emitted=12))
    fr = GF.build(_catalog([cand]), _estate(r46=[_lb("x")]))
    assert fr["state"] == GF.ST_INVALID
    assert any(p.startswith("STATE_DECLARED") for p in fr["reconciliation"]["problems"])
    assert _inv(fr, "NO_SECOND_FORWARD_LEDGER") == "NO"


def test_the_module_holds_no_memory_registry_ledger_or_portfolio_path():
    src = (_ROOT / "alpha_agent" / "r59" / "global_frontier.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                    else [node.module or ""])
            for mod in mods:
                assert not mod.startswith(("api", "engine", "db", "sqlite3", "paper_trader")), mod
                assert "r63" not in mod and "r64" not in mod, mod
    attrs = {n.func.attr for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not attrs & {"register", "record_result", "freeze_forward", "set_frontier", "set_meta",
                        "event", "set_opportunity", "write_text", "write_bytes", "unlink", "mkdir"}
    writes = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
              and isinstance(n.func, ast.Attribute) and n.func.attr == "write_artifact"]
    assert len(writes) == 1
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert not names & {"run_cell", "bh_fdr", "generate_mandates", "claim_next", "enqueue",
                        "register_forward_challenger", "emit_prospective_prediction", "allocate",
                        "adopt_prospective_freeze"}
    for token in ("CREATE TABLE", "register_forward_challenger(", "_append_ledger", "place_order",
                  "create_order", "open("):
        assert token not in src, token
    fr = GF.build(_catalog([_open("GC_X", ["x"])]), _estate(r46=[_lb("x")]))
    for flag in ("creates_orders", "creates_fills", "promotes_model", "mutates_operational_store",
                 "automation_enabled", "registers_forward_challenger", "purchases_data",
                 "allocates_capital", "writes_owner_store"):
        assert fr["safety"][flag] is False, flag
    assert _inv(fr, "NO_PORTFOLIO_MUTATION") == "YES"
    assert _inv(fr, "NO_SECOND_RESEARCH_MEMORY") == "YES"


def _tree_digest(root: Path) -> dict:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_the_estate_is_read_from_the_owners_by_path_and_nothing_is_written(tmp_path, monkeypatch):
    data = tmp_path / "data"
    r46 = data / "prospective_alpha_tournament_r46" / "r46_prospective_alpha_tournament_v1"
    r46.mkdir(parents=True)
    (r46 / GF.R46_LEADERBOARD).write_text(json.dumps({"built_at_utc": "t", "rows": [
        _lb("r46_fx", emitted=4, matured=2, eff=1),
        _lb("shadow_wide_xs", release="R39", state="DATA_BLOCKED")]}), encoding="utf-8")
    (r46 / GF.R46_CONTINUATION).write_text(json.dumps({
        "lane_results": {"r39_fut_month_end": {"lifecycle": "CALLED_QUIET_NOT_DUE",
                                               "next_decision_date": "2026-09-30"}},
        "summary": {"by_adopted_challenger": {"shadow_wide_xs": {"emitted": 1, "scored": 0}}}}),
        encoding="utf-8")
    reg = data / "forward_challenger_registry" / GF.REGISTRATIONS_SUBDIR
    reg.mkdir(parents=True)
    (reg / "abc.json").write_text(json.dumps({"challenger_id": "REG_X", "asset_class": "US_ETF",
                                              "horizon_sessions": 5,
                                              "identity": {"identity_hash": "abc"}}), encoding="utf-8")
    acc = data / "canonical_forward_accrual"
    acc.mkdir(parents=True)
    (acc / GF.ACCRUAL_PROJECTION).write_text(json.dumps({"by_identity": {
        "abc": {"current_accrual_state": "EMITTED", "predictions_emitted": 2}}}), encoding="utf-8")
    r52 = data / "research_runtime_r52"
    r52.mkdir(parents=True)
    (r52 / GF.R51_FRONTIER_ARTIFACT).write_text(json.dumps({"frontier": {"rows": [
        {"sleeve_id": "incumbent_sleeve", "state": "ALREADY_OPERATIONAL", "challenger_ids": []}]}}),
        encoding="utf-8")
    for leaf, cid in (("r63_information_sensitivity", "R63_X"), ("r64_information_directed_alpha", "R64_X")):
        d = data / leaf / GF.CHALLENGERS_SUBDIR
        d.mkdir(parents=True)
        (d / ("%s_hash.json" % cid)).write_text(json.dumps({"challenger_id": cid,
                                                           "classification": "MORE_RESEARCH_REQUIRED",
                                                           "asset_class": "FX_FUTURES"}), encoding="utf-8")
    r58 = data / "r58_orthogonal_alpha" / "results"
    r58.mkdir(parents=True)
    (r58 / GF.R58_FORWARD[1]).write_text(json.dumps({"frozen": {"R58_X": {"role": "CANDIDATE"}}}),
                                         encoding="utf-8")
    ar = data / "alpha_recovery_offensive" / "results"
    ar.mkdir(parents=True)
    (ar / GF.AR_FORWARD_PACKAGE[1]).write_text(json.dumps({"ready": [], "survivors_not_qualified": [
        {"challenger_id": "AR_X", "classification": "ECONOMIC_UNDER_CONTROLS_NOT_FDR",
         "asset_class": "FX_FUTURES"}]}), encoding="utf-8")
    monkeypatch.setenv(GF.DATA_ROOT_ENV, str(data))
    for env in (GF.REGISTRY_DIR_ENV, GF.ACCRUAL_DIR_ENV, GF.R63_ROOT_ENV, GF.R64_ROOT_ENV,
                GF.R58_ROOT_ENV, GF.ALPHA_RECOVERY_ROOT_ENV):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    mem = M.open_memory()
    before = _tree_digest(data)

    estate = GF.load_estate(mem, catalog={"live_candidates": [], "mechanisms": [],
                                          "closed_mechanisms": []})
    assert _tree_digest(data) == before
    ids = GF.owner_identities(estate)
    assert ids[GF.OW_R46] == ["r46_fx", "shadow_wide_xs"]
    assert set(ids[GF.OW_R46_ADOPTED]) == set(PF.ADOPTED_CONTINUATION_LANES)
    assert ids[GF.OW_REGISTRY] == ["REG_X"] and ids[GF.OW_ACCRUAL] == []
    assert ids[GF.OW_R51] == ["incumbent_sleeve"]
    assert ids[GF.OW_R63] == ["R63_X"] and ids[GF.OW_R64] == ["R64_X"]
    assert ids[GF.OW_R58] == ["R58_X"] and ids[GF.OW_AR] == ["AR_X"]
    assert all(estate["owners"][o]["present"] for o in GF.REQUIRED_OWNERS)
    wide = estate["owners"][GF.OW_R46_ADOPTED]["by_adopted"]["shadow_wide_xs"]
    assert (wide["lifecycle"], wide["emitted"]) == ("CALLED_QUIET_NOT_DUE", 1)


def _literal(path: str, name: str) -> str:
    src = (_ROOT / path).read_text(encoding="utf-8")
    m = re.search(r'^%s\s*=\s*(?:Path\(\s*)?r?"([^"]+)"' % re.escape(name), src, re.M)
    assert m, (path, name)
    return m.group(1)


def test_every_mirrored_owner_name_still_matches_its_owner():
    assert GF.REGISTRY_DIR_ENV == _literal("api/forward_challenger_registry.py", "REGISTRY_DIR_ENV")
    assert GF.REGISTRATIONS_SUBDIR == _literal("api/forward_challenger_registry.py", "_REGISTRATIONS_SUBDIR")
    assert Path(_literal("api/forward_challenger_registry.py", "_DEFAULT_REGISTRY_DIR")) == \
        GF.DEFAULT_DATA_ROOT / "forward_challenger_registry"
    assert GF.ACCRUAL_DIR_ENV == _literal("api/canonical_forward_accrual.py", "STORE_DIR_ENV")
    assert Path(_literal("api/canonical_forward_accrual.py", "_DEFAULT_STORE_DIR")) == \
        GF.DEFAULT_DATA_ROOT / "canonical_forward_accrual"
    assert GF.ACCRUAL_PROJECTION == _literal("api/canonical_forward_accrual.py", "PROJECTION_ARTIFACT")
    assert GF.R51_FRONTIER_ARTIFACT == _literal("alpha_agent/r52/frontier_refresh.py", "ARTIFACT")
    for pkg, env, leaf in (("alpha_agent/r63/__init__.py", GF.R63_ROOT_ENV, "r63_information_sensitivity"),
                           ("alpha_agent/r64/__init__.py", GF.R64_ROOT_ENV, "r64_information_directed_alpha"),
                           ("alpha_agent/r58/__init__.py", GF.R58_ROOT_ENV, "r58_orthogonal_alpha"),
                           ("alpha_agent/alpha_recovery/__init__.py", GF.ALPHA_RECOVERY_ROOT_ENV,
                            "alpha_recovery_offensive")):
        assert env == _literal(pkg, "RESEARCH_ROOT_ENV"), pkg
        if pkg != "alpha_agent/r64/__init__.py":
            assert Path(_literal(pkg, "DEFAULT_RESEARCH_ROOT")) == GF.DEFAULT_DATA_ROOT / leaf, pkg
    r46_src = "\n".join(p.read_text(encoding="utf-8") for p in (_ROOT / "alpha_agent" / "r46").glob("*.py"))
    assert GF.R46_LEADERBOARD in r46_src and GF.R46_CONTINUATION in r46_src
    assert GF.R58_FORWARD[1] in (_ROOT / "alpha_agent" / "r58" / "challengers.py").read_text(encoding="utf-8") \
        or GF.R58_FORWARD[1] in "\n".join(p.read_text(encoding="utf-8")
                                          for p in (_ROOT / "alpha_agent" / "r58").glob("*.py"))
    assert GF.AR_FORWARD_PACKAGE[1] in (_ROOT / "alpha_agent" / "alpha_recovery" /
                                        "forward_package.py").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# The committed reconciliation
# --------------------------------------------------------------------------- #
def _committed(monkeypatch) -> dict:
    monkeypatch.delenv(MX.CATALOG_PATH_ENV, raising=False)
    cat = MX.load_catalog()
    assert GF.declared(cat)
    return cat


def test_the_committed_reconciliation_is_valid_and_claims_the_whole_r46_contract(monkeypatch):
    from alpha_agent.r46 import challengers as CH
    cat = _committed(monkeypatch)
    rec = GF.reconciliation(cat)
    assert GF.validate_reconciliation(rec, cat) == []
    member_of = {m: c["candidate_id"] for c in rec["candidates"] for m in c["members"]}
    by_id = {c["candidate_id"]: c for c in rec["candidates"]}
    for spec in CH.ALL_SPECS:
        assert spec["challenger_id"] in member_of, spec["challenger_id"]
    for sid in PF.ADOPTED_CONTINUATION_LANES:
        assert sid in member_of, sid
    for c in cat["live_candidates"]:
        assert c["candidate_id"] in member_of, c["candidate_id"]
    for m in cat["mechanisms"]:
        assert m["mechanism_id"] in member_of, m["mechanism_id"]
    assert set(rec["asset_classes"]) == set(GF.REQUIRED_ASSET_CLASSES)

    clusters: dict = {}
    for spec in CH.ALL_SPECS:
        clusters.setdefault(CH.cluster_for(spec), []).append(spec["challenger_id"])
    # CREDIT: every credit-regime challenger lives in an OPEN credit opportunity.
    for cid in clusters["CREDIT_REGIME"]:
        c = by_id[member_of[cid]]
        assert c["asset_class"] == "CREDIT" and c["disposition"] == GF.DISP_OPEN, cid
    # VOLATILITY: the prospective VX carry challengers are an OPEN volatility opportunity,
    # distinct from the closed volatility TIMING families it lists.
    vx = {by_id[member_of[cid]]["candidate_id"] for cid in clusters["VX_CARRY"] + ["shadow_vx_carry_ts"]}
    assert len(vx) == 1
    vol = by_id[vx.pop()]
    assert vol["asset_class"] == "VOLATILITY" and vol["disposition"] == GF.DISP_OPEN
    assert {"VX_CONDITIONAL_SHORT_VOL_R41", "VOL_TERM_EQUITY_TIMING_R35_R36_R63"} <= set(vol["closed_families"])
    # FX: the R51 carry challenger shares one OPEN opportunity with every R63/R64/Alpha Recovery carry cell,
    # and the generic closed FX_CARRY ledger family does not close it.
    fx = by_id[member_of["r51_fx_xs_carry_cip"]]
    assert fx["asset_class"] == "FX" and fx["disposition"] == GF.DISP_OPEN
    assert "FX_CARRY" in fx["closed_families"]
    for cid in ("R63_FX_FUTURES_CARRY_H1_E82A5C66", "R64_FX_FUTURES_CARRY_H5_41E918F2",
                "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7"):
        assert member_of[cid] == fx["candidate_id"], cid
    # COT: the prospective positioning challengers are reconciled separately from the closed generic family.
    cot = by_id[member_of["r46_4_cot_xs_positioning_flow"]]
    assert cot["disposition"] == GF.DISP_OPEN and "COT_HEDGING_PRESSURE" in cot["closed_families"]
    # The named forward challengers are explicitly reconciled.
    for cid in ("r46_4_spx_pre_fomc_drift", "r46_4_spx_announcement_day_premium", "r46_3_spx_turn_of_month",
                "r46_5_pead_announcement_return_20d", "r46_5_insider_cluster_buy_20d",
                "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"):
        assert by_id[member_of[cid]]["disposition"] == GF.DISP_OPEN, cid
    # CRYPTO: its only mechanism is closed and the class is not declared exhausted.
    assert rec["asset_classes"]["CRYPTO"]["declared_exhaustion"] is None


_LIVE_ROOT = GF.DEFAULT_DATA_ROOT / "prospective_alpha_tournament_r46"


@pytest.mark.skipif(not _LIVE_ROOT.exists(), reason="the live research estate is not on this machine")
def test_the_committed_frontier_reconciles_the_live_estate_read_only(monkeypatch):
    """Reads the live owners and writes nothing: the global frontier must be COMPLETE."""
    cat = _committed(monkeypatch)
    monkeypatch.delenv(GF.DATA_ROOT_ENV, raising=False)
    monkeypatch.setenv(GF.REGISTRY_DIR_ENV, str(GF.DEFAULT_DATA_ROOT / "forward_challenger_registry"))
    monkeypatch.setenv(GF.ACCRUAL_DIR_ENV, str(GF.DEFAULT_DATA_ROOT / "canonical_forward_accrual"))
    for env in (GF.R63_ROOT_ENV, GF.R64_ROOT_ENV, GF.R58_ROOT_ENV, GF.ALPHA_RECOVERY_ROOT_ENV):
        monkeypatch.delenv(env, raising=False)
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(r59.DEFAULT_RESEARCH_ROOT))
    before = GF.artifact_path().stat().st_mtime if GF.artifact_path().exists() else None
    fr = GF.current(None, catalog=cat)
    assert fr["state"] == GF.ST_COMPLETE, (fr["reconciliation"]["unreconciled"],
                                           fr["reconciliation"]["owner_conflicts"],
                                           fr["reconciliation"]["problems"],
                                           fr["reconciliation"]["missing_owners"])
    assert {k: v["value"] for k, v in fr["invariants"].items()} == {k: "YES" for k in fr["invariants"]}
    assert tuple(fr["asset_classes"]) == GF.REQUIRED_ASSET_CLASSES
    assert fr["asset_classes"]["CRYPTO"]["state"] != GF.AS_EXHAUSTED
    assert fr["purchase_comparisons"], "the priced single-name options purchase must carry a comparison"
    assert (GF.artifact_path().stat().st_mtime if GF.artifact_path().exists() else None) == before
