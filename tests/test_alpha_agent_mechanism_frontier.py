"""Alpha Agent activation - the mechanism-first frontier inside the EXISTING R59 agent.

Alpha Recovery needed a manual prompt for every experiment because the R59
governor could only name Norgate price-state families and generative grammars
(all measured EXHAUSTED), its information-need mandates executed nothing, and no
Alpha Recovery verdict ever reached ResearchMemory. These tests protect the
minimum fix and the operating brief's activation claims:

* the P&L work gate is enforced and dataset tourism is refused;
* a closed family cannot return under a new name, nor a cosmetic variant of a
  live candidate;
* the governor selects the next mechanism itself, the loop executes it and
  continues after a NO_EDGE without a prompt, and measured failures re-rank
  the frontier;
* an unbuilt or re-edited executor blocks only its own mechanism;
* the purchase gate stops for a human, a QUALIFIED verdict registers and
  promotes nothing, and no executor may claim capital eligibility;
* a worker without the mechanism handler cannot claim mechanism work, the
  persistent runtime no longer spins on mandates a session proved unclaimable,
  and no second agent, memory, queue or scorer exists.
"""
from __future__ import annotations

import ast
import json
import sys
import types
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import autonomous_research as AR  # noqa: E402
from alpha_agent import r59  # noqa: E402
from alpha_agent.r46 import runlock as RL  # noqa: E402
from alpha_agent.r59 import blockers as BLK  # noqa: E402
from alpha_agent.r59 import frontier as FR  # noqa: E402
from alpha_agent.r59 import governor as GOV  # noqa: E402
from alpha_agent.r59 import handlers as H  # noqa: E402
from alpha_agent.r59 import information_needs as IN  # noqa: E402
from alpha_agent.r59 import loop as LP  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402
from alpha_agent.r59 import memory as M  # noqa: E402
from alpha_agent.r59 import runtime as RT  # noqa: E402

EXEC_MODULE = "alpha_agent._mechanism_fixture_executor"
EMPTY_VIEW = {"asset_classes": {}, "ready": [], "non_equity_ready": [], "substrates": {}}


def _gate(**over) -> dict:
    g = {f: "a declared answer written before any return exists, for field %s" % f
         for f in MX.PNL_GATE_FIELDS}
    g["KILL_RULE"] = "Kill if NW t < 2.0 or net below 1.5 %/yr at 1 bp per side"
    g["WHY_NOT_DUPLICATIVE"] = "no closed family shares this information object at all"
    g.update(over)
    return g


def _mech(mid: str, *, cls: str = "FORCED_TRADING_FLOW", strength: float = 0.5,
          objects=None, executor=None, **over) -> dict:
    e = {"mechanism_id": mid, "title": "fixture %s" % mid.lower(),
         "domain": "STRUCTURAL_FLOWS_FORCED_TRADING", "mechanism_class": cls,
         "origin": "MECHANISM_FIRST", "declared_before_returns": True,
         "declared_at": "2026-09-13", "data_version": 1,
         "asset_class": r59.AC_RATES, "horizon_sessions": 5, "model_family": "FIXTURE",
         "information_objects": objects or ["FIXTURE:%s" % mid],
         "work_conditions": ["TESTS_DISTINCT_PNL_MECHANISM"], "pnl_gate": _gate(),
         "score_inputs": {**{k: 0.5 for k in MX.UNIT_INPUTS},
                          "mechanism_strength": strength,
                          "implementation_days": 2, "data_cost_usd": 0},
         "requires_purchase": False, "reopen": None, "executor": executor}
    e.update(over)
    return e


def _catalog(mechanisms, closed=None, live=None) -> dict:
    return {"schema": MX.SCHEMA, "declared_before_returns": True,
            "live_candidates": live or [], "closed_mechanisms": closed or [],
            "mechanisms": mechanisms, "data_gaps": []}


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    cat = tmp_path / "catalog.json"
    monkeypatch.setenv(MX.CATALOG_PATH_ENV, str(cat))
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.setenv(MX.REPO_ROOT_ENV, str(repo))
    # The owned-panel frontier and the R63 information needs are not under test.
    monkeypatch.setattr(FR, "measure", lambda mm=None: dict(EMPTY_VIEW))
    monkeypatch.setattr(IN, "candidates", lambda *a, **k: [])
    return types.SimpleNamespace(tmp=tmp_path, catalog=cat, repo=repo)


def _write(env, catalog: dict) -> None:
    env.catalog.write_text(json.dumps(catalog), encoding="utf-8")


def _executor(env, monkeypatch, results: dict, calls: list) -> dict:
    mod = types.ModuleType(EXEC_MODULE)

    def run_mechanism(*, mechanism):
        calls.append(mechanism["mechanism_id"])
        return dict(results[mechanism["mechanism_id"]])

    mod.run_mechanism = run_mechanism
    monkeypatch.setitem(sys.modules, EXEC_MODULE, mod)
    code = env.repo / "exec.py"
    code.write_text("# fixture executor\n", encoding="utf-8")
    pre = env.repo / "prereg.md"
    pre.write_text("# fixture preregistration\n", encoding="utf-8")
    return {"callable": EXEC_MODULE + ":run_mechanism", "module_path": "exec.py",
            "module_sha256": MX.normalised_sha256(code),
            "preregistration_path": "prereg.md",
            "preregistration_sha256": MX.normalised_sha256(pre)}


NO_EDGE = {"verdict": "NO_EDGE", "why": "fixture: signed IC t below the floor",
           "capital_eligible": False, "statistic": {"lockbox_t": 0.4}}


def _mechanism_ids(batch: dict) -> list:
    return [m["payload"]["mechanism_id"] for m in batch["mandates"]
            if m["kind"] == GOV.MANDATE_MECHANISM]


# --------------------------------------------------------------------------- #
# P_AND_L_GATE_ENFORCED / DATASET_TOURISM_BLOCKED
# --------------------------------------------------------------------------- #
def test_the_pnl_work_gate_refuses_unanswered_or_unmeasurable_fields():
    assert MX.pnl_gate(_mech("OK"))["passed"]
    e = _mech("PLACEHOLDER")
    e["pnl_gate"]["KILL_RULE"] = "TBD - to be decided once the backtest results are in"
    assert "PNL_GATE_FIELD_PLACEHOLDER: KILL_RULE" in MX.pnl_gate(e)["failures"]
    e = _mech("MISSING")
    del e["pnl_gate"]["WHY_NOT_DUPLICATIVE"]
    assert "PNL_GATE_FIELD_MISSING: WHY_NOT_DUPLICATIVE" in MX.pnl_gate(e)["failures"]
    e = _mech("VAGUE")
    e["pnl_gate"]["KILL_RULE"] = "we will look at whether it seems good overall"
    assert "KILL_RULE_NOT_MEASURABLE" in MX.pnl_gate(e)["failures"]
    assert any(f.startswith("WORK_CONDITION_UNSATISFIED")
               for f in MX.pnl_gate(_mech("NOWORK", work_conditions=[]))["failures"])
    assert "NOT_DECLARED_BEFORE_RETURNS" in MX.pnl_gate(
        _mech("LATE", declared_before_returns=False))["failures"]
    e = _mech("NOSCORE")
    del e["score_inputs"]["unpriced_at_entry"]
    assert any(f.startswith("SCORE_INPUTS_UNDECLARED") for f in MX.pnl_gate(e)["failures"])


def test_dataset_first_work_is_refused_and_never_mandated(env):
    assert any(f.startswith("DATASET_TOURISM")
               for f in MX.pnl_gate(_mech("TOUR", origin="DATASET_FIRST"))["failures"])
    assert any(f.startswith("NO_ECONOMIC_MECHANISM_CLASS")
               for f in MX.pnl_gate(_mech("NOCLS", cls="AN_UNTESTED_DATASET"))["failures"])
    assert any(f.startswith("CLOSED_MECHANISM_CLASS")
               for f in MX.pnl_gate(_mech("PRICE", cls="PRICE_STATE_TRANSFORMATION"))["failures"])
    _write(env, _catalog([_mech("TOUR", origin="DATASET_FIRST"), _mech("GOOD")]))
    mem = M.open_memory()
    assert _mechanism_ids(GOV.generate_mandates(mem, limit=12, frontier_view=EMPTY_VIEW)) == ["GOOD"]


# --------------------------------------------------------------------------- #
# FAILED_MECHANISMS_NOT_RETESTED
# --------------------------------------------------------------------------- #
CLOSED_FORM4 = {"family_id": "INSIDER_FORM4_ALL", "mechanism_class": "PUBLIC_SLOW_DISCLOSURE",
                "information_objects": ["SEC_FORM4:INSIDER_OPEN_MARKET_PURCHASES"],
                "asset_scope": "US_EQUITY", "verdict": "NO: priced at publication",
                "semantic_tokens": ["insider", "form4", "officer", "director"]}


def test_a_closed_family_cannot_return_under_a_new_name(env):
    closed = MX.closed_index(_catalog([], closed=[CLOSED_FORM4]))
    renamed = _mech("EXECUTIVE_CONVICTION_V1",
                    objects=["SEC_FORM4:INSIDER_OPEN_MARKET_PURCHASES"])
    d = MX.distinctness(renamed, closed=closed, live=[])
    assert not d["distinct"] and d["refusals"][0].startswith("DUPLICATE_OF_CLOSED")

    reworded = _mech("EXECUTIVE_CONVICTION_V2", objects=["SEC_OTHER:EXECUTIVE_TRADES"],
                     title="officer and director conviction purchases")
    d = MX.distinctness(reworded, closed=closed, live=[])
    assert not d["distinct"] and "UNADDRESSED_OVERLAP_WITH_CLOSED" in d["refusals"][0]

    reworded["pnl_gate"]["WHY_NOT_DUPLICATIVE"] = (
        "INSIDER_FORM4_ALL scored the purchases themselves; this scores something else")
    assert MX.distinctness(reworded, closed=closed, live=[])["distinct"]

    # An exact duplicate returns ONLY through contract rule 13 with a measured defect.
    reopened = _mech("FORM4_REOPENED", objects=["SEC_FORM4:INSIDER_OPEN_MARKET_PURCHASES"],
                     reopen={"closed_family": "INSIDER_FORM4_ALL",
                             "reason": "PIT_HISTORY_MATERIALLY_IMPROVED",
                             "measured_binding_failure": "acceptance instants joined for only "
                                                         "40 % of purchase rows in the run"})
    assert MX.distinctness(reopened, closed=closed, live=[])["distinct"]
    sign_flip = dict(reopened, reopen={"closed_family": "INSIDER_FORM4_ALL",
                                       "reason": "WRONG_SIGN_LOOKED_BETTER",
                                       "measured_binding_failure": "x" * 40})
    assert not MX.distinctness(sign_flip, closed=closed, live=[])["distinct"]

    _write(env, _catalog([renamed], closed=[CLOSED_FORM4]))
    mem = M.open_memory()
    MX.seed_closed(mem)
    fr = MX.frontier(mem)
    assert fr["by_status"] == {MX.ST_REFUSED_DUPLICATE: ["EXECUTIVE_CONVICTION_V1"]}
    assert _mechanism_ids(GOV.generate_mandates(mem, limit=12, frontier_view=EMPTY_VIEW)) == []


def test_a_cosmetic_variant_of_the_live_candidate_is_refused():
    live = [{"candidate_id": "LIVE_SKEW", "information_objects": ["SPY_OPRA:SKEW"],
             "semantic_tokens": ["skew", "put", "call"],
             "mechanism_class": "HEDGER_DEMAND_PRESSURE",
             "domain": "DERIVATIVES_IMPLIED_INFORMATION"}]
    variant = _mech("SKEW_TWEAK", objects=["SPY_OPRA:SKEW"])
    assert MX.distinctness(variant, closed=[], live=live)["refusals"] == [
        "COSMETIC_VARIANT_OF_LIVE_CANDIDATE: LIVE_SKEW"]
    near = _mech("SKEW_NEAR", objects=["OTHER:TAIL"], title="put call skew elsewhere")
    assert not MX.distinctness(near, closed=[], live=live)["distinct"]
    near["pnl_gate"]["WHY_NOT_DUPLICATIVE"] = "LIVE_SKEW is a different instrument and window"
    assert MX.distinctness(near, closed=[], live=live)["distinct"]


def test_only_a_historically_evidenced_live_candidate_lifts_its_class_prior():
    evidenced = {"candidate_id": "SURVIVOR", "mechanism_class": "HEDGER_DEMAND_PRESSURE",
                 "information_objects": ["A:B"], "historical_evidence": "confirmed t 2.6"}
    pending = {"candidate_id": "PENDING", "mechanism_class": "FORCED_TRADING_FLOW",
               "information_objects": ["C:D"], "state": "FORWARD_PENDING"}
    cat = _catalog([], live=[evidenced, pending])
    assert MX.evidenced_live_classes(cat) == {"HEDGER_DEMAND_PRESSURE"}
    flow = MX.assess(_mech("FLOW"), catalog=cat)["score"]
    hedge = MX.assess(_mech("HEDGE", cls="HEDGER_DEMAND_PRESSURE"), catalog=cat)["score"]
    assert flow["live_candidate_in_class"] is False
    assert hedge["live_candidate_in_class"] is True and hedge["total"] > flow["total"]


def test_the_closed_ledger_is_seeded_once_into_the_one_memory(env):
    second = dict(CLOSED_FORM4, family_id="FAILS_TO_DELIVER",
                  information_objects=["SEC_FTD:FAILS_BALANCE"], semantic_tokens=["fails"])
    _write(env, _catalog([], closed=[CLOSED_FORM4, second]))
    mem = M.open_memory()
    first = MX.seed_closed(mem)
    stamp = mem.get(MX.CLOSED_PREFIX + "INSIDER_FORM4_ALL")["settled_at"]
    again = MX.seed_closed(mem)
    assert sorted(first["added"]) == ["FAILS_TO_DELIVER", "INSIDER_FORM4_ALL"]
    assert again == {"added": [], "already_present": 2}
    assert mem.get(MX.CLOSED_PREFIX + "INSIDER_FORM4_ALL")["settled_at"] == stamp
    assert mem.burden()["total"] == 2
    assert mem.get(MX.CLOSED_PREFIX + "FAILS_TO_DELIVER")["outcome"] == r59.HO_NO_ALPHA_EVIDENCE


# --------------------------------------------------------------------------- #
# MECHANISM_FIRST_SELECTION / AUTO_SELECTS_NEXT_RESEARCH / AUTO_CONTINUES_AFTER_NO_EDGE
# --------------------------------------------------------------------------- #
def test_the_governor_selects_mechanisms_by_expected_value_without_a_prompt(env):
    _write(env, _catalog([_mech("LOW", strength=0.05), _mech("HIGH", strength=0.95)]))
    mem = M.open_memory()
    batch = GOV.generate_mandates(mem, limit=12, frontier_view=EMPTY_VIEW)
    assert _mechanism_ids(batch) == ["HIGH", "LOW"]
    q = LP.open_queue()
    H.seed_mandates(q, batch["mandates"])
    job = q.claim_next(lane_prefixes=list(r59.LANE_PREFIXES))
    assert job.lane.startswith(r59.LANE_MECHANISM_PREFIX)
    assert job.payload["payload"]["mechanism_id"] == "HIGH"


def test_the_loop_runs_the_next_mechanism_after_a_no_edge(env, monkeypatch):
    calls: list = []
    ex = _executor(env, monkeypatch, {"FIRST": NO_EDGE, "SECOND": NO_EDGE}, calls)
    _write(env, _catalog([_mech("FIRST", strength=0.9, executor=ex),
                          _mech("SECOND", strength=0.2, executor=ex)]))
    mem = M.open_memory()
    s = LP.run_session(batch=12, max_jobs_per_iteration=1, mem=mem, seed_opportunities=False)
    assert calls == ["FIRST", "SECOND"]
    assert s["stop_condition"] == LP.STOP_A
    for mid in ("FIRST", "SECOND"):
        row = mem.get(MX.HYP_PREFIX + mid)
        assert row["outcome"] == r59.HO_NO_ALPHA_EVIDENCE
        assert row["generation_method"] == MX.GENERATION_METHOD
        assert row["evidence_maturity"] == "HISTORICAL"


def test_a_measured_failure_lowers_the_priority_of_its_mechanism_class(env, monkeypatch):
    calls: list = []
    ex = _executor(env, monkeypatch, {"CLASS_A1": NO_EDGE}, calls)
    _write(env, _catalog([_mech("CLASS_A1", strength=0.9, executor=ex), _mech("CLASS_A2")]))
    mem = M.open_memory()
    before = {r["mechanism_id"]: r for r in MX.frontier(mem)["rows"]}["CLASS_A2"]["score"]
    LP.run_session(batch=12, max_jobs_per_iteration=4, mem=mem, seed_opportunities=False)
    after = {r["mechanism_id"]: r for r in MX.frontier(mem)["rows"]}["CLASS_A2"]["score"]
    assert calls == ["CLASS_A1"]
    assert after["class_closed_count"] == before["class_closed_count"] + 1
    assert after["total"] < before["total"]


def test_an_unbuilt_executor_blocks_only_its_own_mechanism(env, monkeypatch):
    calls: list = []
    ex = _executor(env, monkeypatch, {"BUILT": NO_EDGE}, calls)
    _write(env, _catalog([_mech("UNBUILT", strength=0.95), _mech("BUILT", strength=0.1, executor=ex)]))
    mem = M.open_memory()
    q = LP.open_queue()
    s = LP.run_session(batch=12, max_jobs_per_iteration=4, mem=mem, queue=q,
                       seed_opportunities=False)
    assert calls == ["BUILT"]
    assert s["stop_condition"] == LP.STOP_B
    blocked = [j for j in q.blocked_jobs() if j.lane.startswith(r59.LANE_MECHANISM_PREFIX)]
    assert len(blocked) == 1 and "EXECUTOR NOT BUILT" in blocked[0].blocked_reason
    assert BLK.classify_job(blocked[0])["reason_code"] == BLK.DEPENDENCY_BLOCKED
    cp = MX.checkpoint(mem, q)
    assert cp["CURRENT_RESEARCH_ACTION"]["action"] == "BUILD_EXECUTOR"
    assert cp["CURRENT_RESEARCH_ACTION"]["mechanism_id"] == "UNBUILT"


def test_an_executor_edited_after_its_pin_does_not_run(env, monkeypatch):
    calls: list = []
    ex = _executor(env, monkeypatch, {"PINNED": NO_EDGE}, calls)
    (env.repo / "exec.py").write_text("# edited after the pin\n", encoding="utf-8")
    _write(env, _catalog([_mech("PINNED", executor=ex)]))
    mem = M.open_memory()
    q = LP.open_queue()
    LP.run_session(batch=4, max_jobs_per_iteration=4, mem=mem, queue=q, seed_opportunities=False)
    assert calls == []
    assert "EXECUTOR PIN MISMATCH" in q.blocked_jobs()[0].blocked_reason
    assert mem.get(MX.HYP_PREFIX + "PINNED") is None


# --------------------------------------------------------------------------- #
# PURCHASE_GATE_STOPS_FOR_HUMAN / PROMOTION_REMAINS_MANUAL / PORTFOLIO_MUTATION_REMAINS_BLOCKED
# --------------------------------------------------------------------------- #
def test_the_purchase_gate_stops_for_a_human_and_never_calls_the_executor(env, monkeypatch):
    calls: list = []
    ex = _executor(env, monkeypatch, {"PAID": NO_EDGE}, calls)
    _write(env, _catalog([_mech("PAID", executor=ex, requires_purchase=True)]))
    mem = M.open_memory()
    assert _mechanism_ids(GOV.generate_mandates(mem, limit=12, frontier_view=EMPTY_VIEW)) == []
    # Even a mandate forced onto the queue is refused at execution.
    q = LP.open_queue()
    H.seed_mandates(q, [GOV._mandate(GOV.MANDATE_MECHANISM, asset_class=r59.AC_RATES,  # noqa: SLF001
                                     family="MECHANISM:PAID", eiv=0.9, reason="forced",
                                     payload={"source": MX.SOURCE, "mechanism_id": "PAID"})])
    AR.drain_jobs(q, H.make_handlers(mem, q), max_jobs=4, lane_prefixes=list(r59.LANE_PREFIXES))
    assert calls == []
    job = q.blocked_jobs()[0]
    assert job.blocked_reason.startswith("HUMAN GATE")
    assert BLK.classify_job(job)["reason_code"] == BLK.WAITING_FOR_EXTERNAL_ENTITLEMENT
    gates = MX.checkpoint(mem, q)["HUMAN_GATES"]
    assert [g["gate"] for g in gates] == ["PURCHASE"] and gates[0]["mechanism_id"] == "PAID"


def test_a_qualified_mechanism_is_never_registered_or_promoted(env, monkeypatch):
    calls: list = []
    adopted: list = []
    qualified = {"verdict": "QUALIFIED", "why": "fixture: every gate passed",
                 "capital_eligible": False, "statistic": {"lockbox_t": 3.1}}
    ex = _executor(env, monkeypatch, {"WINNER": qualified}, calls)
    _write(env, _catalog([_mech("WINNER", executor=ex)]))
    mem = M.open_memory()
    q = LP.open_queue()
    LP.run_session(batch=4, max_jobs_per_iteration=4, mem=mem, queue=q, seed_opportunities=False,
                   adopt_forward=lambda **kw: adopted.append(kw) or {"adopted": True})
    assert calls == ["WINNER"] and adopted == []
    row = mem.get(MX.HYP_PREFIX + "WINNER")
    assert row["outcome"] == r59.HO_QUALIFIED and row["forward_challenger"] is None
    assert mem.events(kind="MECHANISM_QUALIFIED_AWAITING_HUMAN_GATE")
    assert mem.events(kind="PROSPECTIVE_FREEZE") == []
    cp = MX.checkpoint(mem, q)
    assert [g["gate"] for g in cp["HUMAN_GATES"]] == ["PROSPECTIVE_REGISTRATION"]
    assert cp["safety"]["promotes_model"] is False


def test_an_executor_claiming_capital_eligibility_is_refused(env, monkeypatch):
    calls: list = []
    bad = dict(NO_EDGE, verdict="QUALIFIED", capital_eligible=True)
    ex = _executor(env, monkeypatch, {"GREEDY": bad}, calls)
    _write(env, _catalog([_mech("GREEDY", executor=ex)]))
    mem = M.open_memory()
    q = LP.open_queue()
    LP.run_session(batch=4, max_jobs_per_iteration=4, mem=mem, queue=q, seed_opportunities=False)
    assert calls == ["GREEDY"]
    assert "MALFORMED EXECUTOR RESULT" in q.blocked_jobs()[0].blocked_reason
    assert mem.get(MX.HYP_PREFIX + "GREEDY") is None


# --------------------------------------------------------------------------- #
# PARALLEL_INDEPENDENT_RESEARCH (safely) / NO_IDLE / NO_SECOND_*
# --------------------------------------------------------------------------- #
def test_two_workers_cannot_execute_one_mechanism_at_once(env, monkeypatch):
    calls: list = []
    ex = _executor(env, monkeypatch, {"SHARED": NO_EDGE}, calls)
    _write(env, _catalog([_mech("SHARED", executor=ex)]))
    mem = M.open_memory()
    q = LP.open_queue()
    H.seed_mandates(q, GOV.generate_mandates(mem, limit=4, frontier_view=EMPTY_VIEW)["mandates"])
    RL.acquire_path(MX._lease_path("SHARED"), "another_worker", wait_s=0)  # noqa: SLF001
    job = q.claim_next(lane_prefixes=list(r59.LANE_PREFIXES))
    outcome, detail = MX.execute_job(mem, job, queue=q)
    assert outcome == AR.OUTCOME_RETRYABLE and calls == []
    assert "lease" in detail["reason"]


def test_a_worker_without_the_mechanism_handler_cannot_claim_mechanism_work(env):
    _write(env, _catalog([_mech("ONLY")]))
    mem = M.open_memory()
    q = LP.open_queue()
    H.seed_mandates(q, GOV.generate_mandates(mem, limit=4, frontier_view=EMPTY_VIEW)["mandates"])
    assert q.claim_next(lane_prefixes=[r59.LANE_PREFIX]) is None
    assert q.claim_next(lane_prefixes=list(r59.LANE_PREFIXES)) is not None


def test_the_runtime_sleeps_when_a_session_proved_its_mandates_unclaimable(env, monkeypatch):
    asked: list = []
    slept: list = []
    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_B, "jobs_executed": 0, "hypotheses_measured": 0,
        "research_still_ready": 3})
    monkeypatch.setattr(GOV, "stop_reason", lambda *a, **k: asked.append(1) or {
        "stop": False, "next_mandates": 3})
    monkeypatch.setattr(RT, "_sleep_watching",
                        lambda seconds, **kw: slept.append(seconds) or [])
    out = RT.run_forever(allow_maturation=False, debug_max_cycles=1,
                         sleep_fn=lambda s: None, install_signal_handlers=False)
    cycle = out["cycles"][0]
    assert asked == [], "a STOP_B session already proved the mandates unclaimable"
    assert cycle["sleep_seconds"] > 0 and slept
    assert cycle["sleep_reason"] != "EXECUTABLE_RESEARCH_EXISTS"


def test_the_runtime_still_asks_the_governor_after_a_session_that_did_not_block(env, monkeypatch):
    asked: list = []
    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 1, "hypotheses_measured": 1,
        "research_still_ready": 0})
    monkeypatch.setattr(GOV, "stop_reason", lambda *a, **k: asked.append(1) or {"stop": True})
    monkeypatch.setattr(RT, "_sleep_watching", lambda seconds, **kw: [])
    RT.run_forever(allow_maturation=False, debug_max_cycles=1, sleep_fn=lambda s: None,
                   install_signal_handlers=False)
    assert asked == [1]


def test_no_second_agent_memory_queue_governor_or_scorer_was_created():
    src = (_ROOT / "alpha_agent" / "r59" / "mechanisms.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert not names & {"run_cell", "bh_fdr", "nw_tstat", "benjamini_hochberg", "gate",
                        "claim_next", "enqueue", "generate_mandates", "run_session",
                        "run_forever", "register_challenger", "allocate"}
    assert "CREATE TABLE" not in src and "import sqlite3" not in src
    for node in ast.walk(tree):
        mods = ([a.name for a in node.names] if isinstance(node, ast.Import)
                else [node.module or ""] if isinstance(node, ast.ImportFrom) else [])
        for mod in mods:
            assert not mod.startswith(("api", "engine", "db", "sqlite3")), mod
            assert "sensitivity" not in mod and "multiple_testing" not in mod, mod
    pkg = _ROOT / "alpha_agent" / "r59"
    for p in pkg.glob("*.py"):
        for node in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ClassDef):
                assert not any(w in node.name for w in ("Agent", "Governor", "Queue", "Scorer")), \
                    (p.name, node.name)


def test_the_checkpoint_is_rewritten_every_iteration_with_the_brief_fields(env, monkeypatch):
    calls: list = []
    ex = _executor(env, monkeypatch, {"ONE": NO_EDGE}, calls)
    _write(env, _catalog([_mech("ONE", executor=ex), _mech("TWO")]))
    mem = M.open_memory()
    LP.run_session(batch=4, max_jobs_per_iteration=4, mem=mem, seed_opportunities=False,
                   checkpoint_context=lambda: {"commits": [{"commit": "abc"}], "forward": []})
    body = json.loads((r59.research_root() / MX.CHECKPOINT_SUBDIR / MX.CHECKPOINT_NAME)
                      .read_text(encoding="utf-8"))
    for key in ("ACTIVE_CANDIDATES", "TRUE_FORWARD_CANDIDATES", "CLOSED_MECHANISMS",
                "CURRENT_RESEARCH_ACTION", "NEXT_RANKED_OPPORTUNITIES", "HUMAN_GATES",
                "DATA_GAPS", "LATEST_RESEARCH_COMMITS", "CURRENT_INFORMATION_FRONTIER",
                "CURRENT_BEST_ESTIMATE_OF_WHERE_ALPHA_IS_MOST_LIKELY"):
        assert key in body, key
    assert body["LATEST_RESEARCH_COMMITS"] == [{"commit": "abc"}]
    assert [c["family_id"] for c in body["CLOSED_MECHANISMS"]] == ["ONE"]
    assert body["CURRENT_RESEARCH_ACTION"]["mechanism_id"] == "TWO"


def test_reopen_reasons_are_contract_rule_13():
    from alpha_agent.alpha_recovery import program as PR
    assert tuple(PR.REOPEN_REASONS) == MX.REOPEN_REASONS


# --------------------------------------------------------------------------- #
# The committed catalog
# --------------------------------------------------------------------------- #
def test_the_committed_catalog_passes_its_own_gate_and_holds_the_closed_ledger(tmp_path, monkeypatch):
    monkeypatch.delenv(MX.CATALOG_PATH_ENV, raising=False)
    monkeypatch.delenv(MX.REPO_ROOT_ENV, raising=False)
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    cat = MX.load_catalog()
    assert cat and cat["declared_before_returns"] is True
    fr = MX.frontier(None, catalog=cat)
    refused = [(r["mechanism_id"], r["reasons"]) for r in fr["rows"]
               if r["status"] in MX.REFUSED_STATUSES]
    assert refused == []
    # The graveyard-family overlap only exists once the ledger is IN memory, which is
    # how the agent actually assesses; an overlap that appeared only after seeding
    # (a catalog text sharing three tokens with a closed family name) must be caught here.
    mem = M.open_memory()
    MX.seed_closed(mem, cat)
    seeded = MX.frontier(mem, catalog=cat)
    refused_seeded = [(r["mechanism_id"], r["reasons"]) for r in seeded["rows"]
                      if r["status"] in MX.REFUSED_STATUSES]
    assert refused_seeded == []
    closed = {c["family_id"] for c in cat["closed_mechanisms"]}
    for fid in ("INSIDER_FORM4_ALL", "FAILS_TO_DELIVER_AND_SHORT_INTEREST", "OWNERSHIP_BREADTH_13F",
                "CONTROL_BLOCK_13DG", "EVENT_8K_ITEM_CODES", "ANALYST_REVISIONS_SALES_SURPRISE",
                "INTRADAY_ETF_FAMILIES", "FUTURES_INTRADAY_8_FAMILIES", "MICROSTRUCTURE_ORDER_FLOW",
                "FIBONACCI_NAMED_TECHNICAL_LEVELS", "PRICE_STATE_EQUITY_FACTORS"):
        assert fid in closed, fid
    live = {c["candidate_id"]: c for c in cat["live_candidates"]}
    skew = live["REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"]
    assert skew["identity_hash"].startswith("6606c9a4")
    assert skew["agent_may"].startswith("read only")
    for e in cat["mechanisms"]:
        assert MX.executor_state(e)["state"] in (MX.EX_NOT_BUILT, MX.EX_READY), e["mechanism_id"]
        assert not e.get("requires_purchase") or \
            {r["mechanism_id"]: r for r in fr["rows"]}[e["mechanism_id"]]["status"] == MX.ST_HUMAN_GATE
