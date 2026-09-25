r"""R74 - THE AUTONOMOUS RESEARCH ROUTE, WALKED END TO END ON A SCRATCH ESTATE.

What this suite is for
----------------------
The live R59 queue holds seventeen blocked jobs and no runnable work, and that
is the CORRECT state: three are TERMINAL on the director's durable record and
fourteen need information the estate does not own. An empty runnable queue is
therefore not evidence that the route is broken - but it is also not evidence
that the route WORKS. The two are indistinguishable from the live estate alone,
and that is exactly the condition in which a broken seam hides: R72 found a
director's ruling that reached no reader, and the loop slept on it for a week
while every surface reported itself healthy.

So this suite proves the route on a SCRATCH estate, where an admissible task can
exist without anybody pretending one exists live:

    director ruling
      -> durable canonical memory
      -> blocker reconciliation
      -> research queue
      -> spawn plan
      -> assigned agent
      -> result artifact
      -> research state

and then asserts the properties the operator actually has to trust: restart
recovery, lease safety, bounded concurrency, task identity, durable results and
cost control.

WHAT THIS SUITE REFUSES TO DO. It does not mutate the live hypothesis registry,
the live queue, the live rulings, the operational book or any evidence store. It
does not manufacture a runnable live job to make the route look exercised - the
whole point is that an admissible task is CONSTRUCTED here, in ``tmp_path``, and
that the live estate's emptiness is left alone as the honest answer it is.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from paper_trader.alpha_agent.autonomous_research import CAT_EXPERIMENT as CAT
from paper_trader.alpha_agent.r59 import blockers as B
from paper_trader.alpha_agent.r59 import loop as LOOP
from paper_trader.alpha_agent.r59 import memory as M

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = os.path.join(REPO, ".venv-win", "Scripts", "python.exe")
BRIEF = os.path.join(REPO, "scripts", "agents_v2_brief.py")

#: An ADMISSIBLE task: a mechanism the scratch estate has never prosecuted and
#: no director has ruled on. Deliberately NOT one of the live estate's dead
#: families, so a pass here can never be mistaken for reopening settled work.
ADMISSIBLE = {
    "asset_class": "US_EQUITY",
    "family": "EVENT_OVERREACTION",
    "information_family": "NASDAQ_TRADING_HALTS",
    "model_family": "LONG_ONLY",
    "kind": "EVENT",
    "mandate_id": "M_R74_ADMISSIBLE",
    "expected_information_value": 0.42,
}

#: A task that a director HAS terminally refused, carried alongside so every
#: assertion about the admissible one is a contrast rather than a hope.
REFUSED_FAMILY = "CROSS_ASSET_RELATIVE_VALUE"


@pytest.fixture()
def estate(tmp_path):
    """A scratch memory + scratch queue. Neither live store is opened."""
    mem = M.open_memory(tmp_path / "research_memory.sqlite")
    q = LOOP.open_queue(tmp_path / "r59_autonomy.sqlite")
    assert str(tmp_path) in str(mem.db_path)
    assert str(tmp_path) in str(q.db_path)
    return mem, q


def _rule_terminally(mem):
    return mem.record_director_ruling(
        asset_class="CROSS_ASSET", economic_family=REFUSED_FAMILY,
        verdict="REFUSED", blocker_reason="WAITING_FOR_EXTERNAL_ENTITLEMENT",
        rationale="relative value is a PRICE_STATE relabel; only non-price "
                  "terms-of-trade vintages would reopen it, and they are not "
                  "owned. Do not re-propose.",
        reopen_condition="OWNED_PIT_NON_PRICE_TERMS_OF_TRADE_VINTAGES",
        campaign_id="R74_ROUTE_PROOF", decided_by="quant-research-director")


def _later(*, hours: int) -> str:
    """An ISO instant `hours` in the future, in the queue's own clock format."""
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc)
            + timedelta(hours=hours)).isoformat().replace("+00:00", "Z")


def _job(payload, *, reason="engine returned NO_MEMBERS"):
    return {"job_id": "j_" + str(payload.get("mandate_id")),
            "lane": "r59.%s.probe" % payload["asset_class"].lower(),
            "state": "BLOCKED", "attempts": 3, "blocked_reason": reason,
            "payload": payload}


# --------------------------------------------------------------------------- #
# LINK 1-2. A RULING REACHES DURABLE CANONICAL MEMORY.
# --------------------------------------------------------------------------- #
def test_a_ruling_is_durable_and_readable_by_the_governor(estate):
    mem, _q = estate
    out = _rule_terminally(mem)
    assert out["recorded"] is True
    reread = M.ResearchMemory(mem.db_path, read_only=True).director_ruling(
        asset_class="CROSS_ASSET", economic_family=REFUSED_FAMILY)
    assert reread["blocker_reason"] == "WAITING_FOR_EXTERNAL_ENTITLEMENT"
    assert reread["decided_by"] == "quant-research-director"


def test_the_ruling_is_recorded_as_an_event_not_only_as_a_row(estate):
    """The durable record must be auditable, not merely current."""
    mem, _q = estate
    _rule_terminally(mem)
    kinds = [e["kind"] for e in mem.events(limit=50)]
    assert "DIRECTOR_RULING_RECORDED" in kinds


# --------------------------------------------------------------------------- #
# LINK 3. BLOCKER RECONCILIATION READS THE RULING - AND ONLY THE RULING.
# --------------------------------------------------------------------------- #
def test_reconciliation_turns_a_ruled_job_terminal(estate):
    mem, _q = estate
    _rule_terminally(mem)
    row = B.classify_job(_job({"asset_class": "CROSS_ASSET",
                               "family": REFUSED_FAMILY, "kind": "CROSS_ASSET",
                               "mandate_id": "M_REFUSED"}), mem=mem)
    assert row["recorded_reason_code"] == "DEPENDENCY_BLOCKED"
    assert row["clears_on"] == "TERMINAL"
    assert row["is_authoritative"] is True


def test_reconciliation_leaves_an_unruled_admissible_task_alone(estate):
    """An admissible task must NOT be swept terminal by a neighbour's ruling."""
    mem, _q = estate
    _rule_terminally(mem)
    row = B.classify_job(_job(ADMISSIBLE), mem=mem)
    assert row["is_authoritative"] is False
    assert row["director_ruling"] is None
    assert row["reason_code"] == row["recorded_reason_code"]


# --------------------------------------------------------------------------- #
# LINK 4. THE QUEUE. AN ADMISSIBLE TASK IS RUNNABLE; A REFUSED ONE IS NOT.
# --------------------------------------------------------------------------- #
def test_an_admissible_task_is_claimable_and_a_blocked_one_is_not(estate):
    _mem, q = estate
    admissible = q.enqueue(CAT, lane="r59.us_equity.probe",
                           payload=ADMISSIBLE, dedupe_key="R74_ADMISSIBLE")
    blocked = q.enqueue(CAT, lane="r59.cross_asset.probe",
                        payload={"asset_class": "CROSS_ASSET",
                                 "family": REFUSED_FAMILY},
                        dedupe_key="R74_REFUSED")
    q.block_specific(blocked, "engine returned NO_MEMBERS")
    assert q.runnable_depth() == 1
    job = q.claim_next()
    assert job is not None and job.job_id == admissible
    assert q.claim_next() is None, "a BLOCKED job must never be claimable"


def test_an_empty_runnable_queue_is_reported_as_itself(estate):
    """The live condition. Depth and RUNNABLE depth are different facts."""
    _mem, q = estate
    blocked = q.enqueue(CAT, lane="r59.cross_asset.probe",
                        payload={"asset_class": "CROSS_ASSET",
                                 "family": REFUSED_FAMILY},
                        dedupe_key="R74_ONLY_BLOCKED")
    q.block_specific(blocked, "engine returned NO_MEMBERS")
    assert q.depth() >= 1
    assert q.runnable_depth() == 0
    assert q.claim_next() is None
    assert q.counts_by_state().get("BLOCKED_SPECIFIC") == 1


# --------------------------------------------------------------------------- #
# LINK 5. THE SPAWN PLAN. IT MUST DISPATCH ON STATE, NOT ON HOPE.
# --------------------------------------------------------------------------- #
def _spawn_plan(tmp_path, spec: dict, mem) -> dict:
    p = tmp_path / "spec.json"
    p.write_text(json.dumps(spec), encoding="utf-8")
    proc = subprocess.run(
        [PY, BRIEF, "--spawn-plan", "--spec", str(p),
         "--memory", str(mem.db_path)],
        capture_output=True, text=True, cwd=REPO, timeout=600)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    body = proc.stdout[proc.stdout.index("{"):proc.stdout.rindex("}") + 1]
    return json.loads(body)


@pytest.mark.skipif(not os.path.exists(PY), reason="venv absent")
def test_the_spawn_plan_dispatches_on_state_not_on_a_fixed_roster(
        tmp_path, estate):
    """The director always runs; no signal agent runs without an experiment.

    On this EMPTY scratch estate the three foundation roles are correctly
    SPAWNed - nothing is certified yet - which is the point: the plan is a
    function of state. The live estate, whose substrate IS certified, skips
    them. Asserting a fixed roster here would assert the opposite property.
    """
    mem, _q = estate
    plan = _spawn_plan(tmp_path, {"campaign_id": "R74_ROUTE_PROOF",
                                  "experiments": []}, mem)
    assert "quant-research-director" in plan["spawn"]
    assert plan["roles_available"] == 12
    assert plan["agent_may_spawn_agent"] is False
    assert plan["spawned_by"] == "SESSION_ORCHESTRATOR_ONLY"
    signal_agents = {"momentum-signal-agent", "reversal-signal-agent",
                     "trend-breadth-signal-agent", "volatility-liquidity-agent"}
    assert not (signal_agents & set(plan["spawn"])), \
        "a signal agent was dispatched with zero pre-registered experiments"
    assert "signal-publishing-agent" not in plan["spawn"]
    assert plan["agent_invocations_planned"] == len(plan["spawn"])


@pytest.mark.skipif(not os.path.exists(PY), reason="venv absent")
def test_every_skipped_role_carries_a_reason_code(tmp_path, estate):
    """A skip without a reason is indistinguishable from work being hidden."""
    mem, _q = estate
    plan = _spawn_plan(tmp_path, {"campaign_id": "R74_ROUTE_PROOF",
                                  "experiments": []}, mem)
    skipped = [r for r in plan["roles"] if r["decision"] == "SKIP"]
    assert skipped, "nothing was skipped, so nothing proves a reason is given"
    assert len(skipped) + len(plan["spawn"]) == plan["roles_available"]
    for role in skipped:
        assert role["reason"], role["role"]
        assert role["reason"] == role["reason"].upper()


@pytest.mark.skipif(not os.path.exists(PY), reason="venv absent")
def test_the_spawn_plan_routes_a_model_and_effort_per_role(tmp_path, estate):
    """Routing is owned by the contract, not chosen at call time."""
    mem, _q = estate
    plan = _spawn_plan(tmp_path, {"campaign_id": "R74_ROUTE_PROOF",
                                  "experiments": []}, mem)
    assert plan["routing_owner"] == "alpha_agent.agents_v2.routing"
    for role in plan["roles"]:
        assert role["model"] in {"opus", "sonnet", "haiku"}
        assert role["effort"] in {"low", "medium", "high"}


# --------------------------------------------------------------------------- #
# LINK 6-7. AN ASSIGNED AGENT PRODUCES A DURABLE RESULT ARTIFACT.
# --------------------------------------------------------------------------- #
def test_a_claimed_task_completes_with_a_durable_result(estate):
    _mem, q = estate
    jid = q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
                    dedupe_key="R74_RESULT")
    job = q.claim_next()
    q.complete(job.job_id, result={"verdict": "MEASURED",
                                   "mandate_id": ADMISSIBLE["mandate_id"]})
    row = q.get(jid)
    assert row.state == "COMPLETED"
    assert (row.result or {})["verdict"] == "MEASURED"


def test_the_result_survives_reopening_the_queue(estate, tmp_path):
    """Durable means on disk, not in the worker that wrote it."""
    _mem, q = estate
    jid = q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
                    dedupe_key="R74_DURABLE")
    job = q.claim_next()
    q.complete(job.job_id, result={"verdict": "MEASURED"})
    reopened = LOOP.open_queue(tmp_path / "r59_autonomy.sqlite")
    assert reopened.get(jid).state == "COMPLETED"


# --------------------------------------------------------------------------- #
# LINK 8. RESEARCH STATE. A MEASURED MECHANISM BECOMES UNREPEATABLE.
# --------------------------------------------------------------------------- #
def test_a_settled_result_closes_the_mechanism_to_a_later_proposal(estate):
    """The route's terminus: the next director cannot re-propose it."""
    mem, _q = estate
    hid = mem.register(
        title="R74 admissible probe", release="R74", origin="ROUTE_PROOF",
        generation_method="TEST", asset_class=ADMISSIBLE["asset_class"],
        economic_family=ADMISSIBLE["family"],
        information_family=ADMISSIBLE["information_family"],
        model_family=ADMISSIBLE["model_family"], spec={"v": 1})
    mem.record_result(hid, outcome="NO_ALPHA_EVIDENCE",
                      statistic={"lockbox_t": 0.31})
    st = mem.mechanism_state(
        asset_class=ADMISSIBLE["asset_class"],
        economic_family=ADMISSIBLE["family"],
        information_family=ADMISSIBLE["information_family"],
        model_family=ADMISSIBLE["model_family"])
    assert st["query_valid"] is True
    assert st["mechanism_is_settled"] is True
    assert st["n_settled"] == 1


def test_renaming_the_mechanism_does_not_reopen_it_through_the_family(estate):
    """STAGE 1 is defeated by a rename; the mechanism key is what holds."""
    mem, _q = estate
    hid = mem.register(
        title="R74 admissible probe", release="R74", origin="ROUTE_PROOF",
        generation_method="TEST", asset_class=ADMISSIBLE["asset_class"],
        economic_family=ADMISSIBLE["family"],
        information_family=ADMISSIBLE["information_family"],
        model_family=ADMISSIBLE["model_family"], spec={"v": 1})
    mem.record_result(hid, outcome="NO_ALPHA_EVIDENCE",
                      statistic={"lockbox_t": 0.31})
    # The same information and model, relabelled into a new economic family.
    renamed = mem.mechanism_state(
        asset_class=ADMISSIBLE["asset_class"],
        economic_family="HALT_OVERREACTION_V2",
        information_family=ADMISSIBLE["information_family"],
        model_family=ADMISSIBLE["model_family"])
    assert renamed["n_matching"] == 0, "a rename must not be silently merged"
    # ...but the information family alone still shows the settled sibling, so a
    # director who asks the right question cannot be told it is untested.
    by_info = mem.mechanism_state(
        asset_class=ADMISSIBLE["asset_class"],
        information_family=ADMISSIBLE["information_family"])
    assert by_info["mechanism_is_settled"] is True


# --------------------------------------------------------------------------- #
# THE PROPERTIES THE OPERATOR HAS TO TRUST.
# --------------------------------------------------------------------------- #
def test_task_identity_is_deduplicated(estate):
    """The same task enqueued twice is ONE task, not two executions."""
    _mem, q = estate
    a = q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
                  dedupe_key="R74_IDENTITY")
    b = q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
                  dedupe_key="R74_IDENTITY")
    assert a == b
    assert q.runnable_depth() == 1


def test_bounded_concurrency_one_job_is_claimed_once(estate):
    """Two overlapping workers cannot both hold the same task."""
    _mem, q = estate
    tmpdir = os.path.dirname(str(q.db_path))
    q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
              dedupe_key="R74_CONCURRENCY")
    second_worker = LOOP.open_queue(q.db_path)
    first = q.claim_next()
    again = second_worker.claim_next()
    assert first is not None
    assert again is None, "a second worker claimed a RUNNING job"
    assert q.get(first.job_id).attempts == 1
    assert tmpdir


def test_attempts_counts_real_executions_only(estate):
    """``attempts`` is the auditable execution count, not a retry guess."""
    _mem, q = estate
    never = q.enqueue(CAT, lane="r59.us_equity.probe",
                      payload=dict(ADMISSIBLE, mandate_id="M_NEVER"),
                      dedupe_key="R74_NEVER")
    ran = q.enqueue(CAT, lane="r59.us_equity.probe",
                    payload=dict(ADMISSIBLE, mandate_id="M_RAN"),
                    dedupe_key="R74_RAN", priority=10)
    job = q.claim_next()
    assert job.job_id == ran
    q.complete(job.job_id, result={"verdict": "MEASURED"})
    assert q.get(never).attempts == 0
    assert q.get(ran).attempts == 1


def test_restart_recovery_a_stale_lease_is_requeued_not_lost(estate):
    """A worker that dies mid-task must not strand the task for ever."""
    _mem, q = estate
    jid = q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
                    dedupe_key="R74_RESTART")
    q.claim_next()
    assert q.get(jid).state == "RUNNING"
    assert q.runnable_depth() == 0
    # Nothing is stale yet, so a healthy worker's lease is NOT stolen.
    assert q.requeue_stale(stale_seconds=1800) == 0
    assert q.get(jid).state == "RUNNING"
    # Past the lease horizon it is recovered, and it is the SAME task. The
    # clock is advanced rather than the threshold zeroed, because "the worker
    # died two hours ago" is the condition being tested, and a zero threshold
    # compares started_at against the very instant it was written.
    assert q.requeue_stale(now=_later(hours=2)) == 1
    assert q.get(jid).state in {"QUEUED", "RETRYABLE"}
    recovered = q.claim_next()
    assert recovered.job_id == jid


def test_a_live_lease_is_never_stolen_by_the_recovery_sweep(estate):
    """Lease safety, stated as its own assertion."""
    _mem, q = estate
    q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
              dedupe_key="R74_LEASE")
    job = q.claim_next()
    assert q.stale_running(stale_seconds=1800) == []
    assert q.requeue_stale(stale_seconds=1800) == 0
    assert q.get(job.job_id).state == "RUNNING"


def test_cost_control_a_task_cannot_retry_for_ever(estate):
    """Bounded attempts is the cost control on a failing task."""
    _mem, q = estate
    jid = q.enqueue(CAT, lane="r59.us_equity.probe", payload=ADMISSIBLE,
                    dedupe_key="R74_COST", max_attempts=2)
    for _ in range(4):
        job = q.claim_next()
        if job is None:
            break
        q.mark_retryable(job.job_id, "transient")
        q.requeue_stale(stale_seconds=0)
    row = q.get(jid)
    assert row.attempts <= 2
    assert row.state != "RUNNING"


def test_the_route_touches_no_live_store(estate, tmp_path):
    """The guard that makes every assertion above safe to have run."""
    mem, q = estate
    for path in (str(mem.db_path), str(q.db_path)):
        assert str(tmp_path) in path
        assert "Stock_Prediction_app_data" not in path
