"""Release 59 continuation - the PERSISTENT autonomous research runtime.

R59 restored the autonomous loop; its lifetime was still bound to an
interactive session, so the researcher died whenever the person did. These
tests protect the properties that make it outlive that session WITHOUT
letting it become something more dangerous than a researcher:

* exactly ONE canonical runtime and ONE logical worker, failing closed
  against a healthy lease holder and recovering from an abandoned one;
* a heartbeat, because a lease that only records an acquisition time evicts
  the healthy long-running worker it exists to protect;
* resume, never reset: the queue, memory, graveyard and burden are the
  same SQLite state a previous process left behind;
* no production iteration limit, and every cap reported as an operator
  override;
* an unbounded mathematical grammar cannot monopolise the machine, and the
  other lanes keep exploring while it runs;
* sleeping only when solely future data can advance the state - READY work
  always beats idling;
* forward evidence is advanced through the CANONICAL owner and is REFUSED
  outright from an uncommitted development source;
* the task scripts are idempotent, status is read-only, installation needs
  an explicit switch, and no test touches the live scheduler or the live
  C: checkout;
* nothing here can promote, allocate, approve, order or fill.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import autonomous_research as AR  # noqa: E402
from alpha_agent import r59  # noqa: E402
from alpha_agent.r46 import runlock as RL  # noqa: E402
from alpha_agent.r59 import governor as GOV  # noqa: E402
from alpha_agent.r59 import loop as LP  # noqa: E402
from alpha_agent.r59 import memory as M  # noqa: E402
from alpha_agent.r59 import runtime as RT  # noqa: E402

_PS = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File"]
_INSTALLER = _ROOT / "scripts" / "install_research_runtime_task.ps1"
_VALIDATOR = _ROOT / "scripts" / "validate_research_runtime_task.ps1"
_MANAGER = _ROOT / "scripts" / "manage_research_runtime.ps1"
_ENTRYPOINT = _ROOT / "scripts" / "run_research_runtime.py"

LIVE_TASK = "PaperTrader-ResearchRuntime"
LIVE_CHECKOUT = Path(r"C:\Users\binis\paper_trader")


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    return tmp_path


@pytest.fixture()
def mem(root):
    return M.open_memory()


def _register(mem, **kw):
    base = dict(title="t", release="RTEST", origin="TEST",
                generation_method="TEST", information_family="PRICE_STATE",
                economic_family="FAM_A", asset_class=r59.AC_COMMODITY,
                model_family="LINEAR")
    base.update(kw)
    return mem.register(**base)


def _ps(script: Path, *args) -> str:
    out = subprocess.run(_PS + [str(script), *args], capture_output=True,
                         text=True, timeout=180)
    return (out.stdout or "") + (out.stderr or "")


def _probe_json(out: str) -> dict:
    """The decision document a hermetic probe prints, or {} if it printed none."""
    try:
        return json.loads(out[out.index("{"):out.rindex("}") + 1])
    except (ValueError, IndexError):
        return {}


def _live_task_snapshot() -> str:
    """Read-only fingerprint of the installed task. Touches nothing."""
    ps = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
          "-Command",
          "$t = Get-ScheduledTask -TaskName '%s' -ErrorAction SilentlyContinue;"
          " if ($null -eq $t) { 'ABSENT' } else {"
          " $t.Actions[0].Execute + '|' + $t.Actions[0].Arguments + '|' +"
          " [string]$t.Settings.ExecutionTimeLimit + '|' +"
          " [string]$t.Principal.LogonType + '|' + [string]$t.Settings.Enabled"
          " + '|' + ((@($t.Triggers | ForEach-Object"
          " { $_.CimClass.CimClassName }) | Sort-Object) -join ',') }"
          % LIVE_TASK]
    try:
        out = subprocess.run(ps, capture_output=True, text=True, timeout=120)
        return (out.stdout or "").strip()
    except (OSError, subprocess.SubprocessError):        # pragma: no cover
        return ""


@pytest.fixture(scope="module")
def live_task_before() -> str:
    return _live_task_snapshot()


# --------------------------------------------------------------------------- #
# ONE runtime, ONE worker
# --------------------------------------------------------------------------- #
def test_exactly_one_persistent_runtime_owner_exists():
    """A second run_forever anywhere would be a second research lifetime."""
    hits = []
    for path in sorted((_ROOT / "alpha_agent").rglob("*.py")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "def run_forever(" in text:
            hits.append(str(path.relative_to(_ROOT)).replace("\\", "/"))
    assert hits == ["alpha_agent/r59/runtime.py"], hits


def test_the_runtime_owns_lifecycle_and_not_research():
    """Process lifecycle only: no kernel, no gate, no queue semantics here."""
    src = (_ROOT / "alpha_agent" / "r59" / "runtime.py").read_text(
        encoding="utf-8")
    for forbidden in ("def nw_tstat", "def bh_fdr", "def claim_next",
                      "def apply_outcome", "def evaluate", "def run_topn",
                      "def generate_mandates", "def freeze"):
        assert forbidden not in src, forbidden
    # It must DELEGATE to the canonical owners rather than reimplement them.
    assert "LP.run_session" in src
    assert "research_runtime_cycle" in src
    assert "RL.acquire_path" in src


def test_a_second_worker_is_refused_while_a_healthy_lease_is_held(root):
    holder = "r59_research_worker:first"
    RL.acquire_path(RT.lease_path(), holder, wait_s=0,
                    stale_after_s=RT.LEASE_STALE_SECONDS,
                    extra={"instance_id": "first"})
    try:
        body = RT.run_forever(debug_max_cycles=1,
                              install_signal_handlers=False)
        assert body["worker_state"] == RT.W_REFUSED
        assert body["existing_lease"]["holder"] == holder
        # It must not have researched anything on the way to refusing.
        assert "cycles" not in body
    finally:
        RL.release_path(RT.lease_path(), holder)


def test_an_abandoned_lease_is_recovered_after_the_stale_threshold(root,
                                                                  monkeypatch):
    """A dead worker must not lock research out forever."""
    dead = "r59_research_worker:dead"
    RL.acquire_path(RT.lease_path(), dead, wait_s=0, stale_after_s=1e9,
                    extra={"instance_id": "dead"})
    # Age the lease past the threshold without touching the clock.
    p = RT.lease_path()
    old = os.stat(p).st_mtime - (RT.LEASE_STALE_SECONDS + 60)
    os.utime(p, (old, old))

    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 0,
        "hypotheses_measured": 0, "research_still_ready": 0})
    body = RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                          sleep_fn=lambda s: None)
    assert body["worker_state"] != RT.W_REFUSED
    assert body["n_cycles"] == 1


def test_the_heartbeat_keeps_a_live_worker_from_being_declared_stale(root):
    """The defect this closes: a healthy long hold aging out of its own lease."""
    holder = "r59_research_worker:live"
    RL.acquire_path(RT.lease_path(), holder, wait_s=0,
                    stale_after_s=RT.LEASE_STALE_SECONDS,
                    extra={"instance_id": "live"})
    p = RT.lease_path()
    old = os.stat(p).st_mtime - (RT.LEASE_STALE_SECONDS + 60)
    os.utime(p, (old, old))
    assert RL.state_path(p)["age_seconds"] > RT.LEASE_STALE_SECONDS

    assert RL.heartbeat_path(p, holder, {"last_lane": "r59.research"}) is True
    assert RL.state_path(p)["age_seconds"] < 5
    assert RL.state_path(p)["identity"]["last_lane"] == "r59.research"
    RL.release_path(p, holder)


def test_a_heartbeat_is_refused_for_someone_elses_lease(root):
    RL.acquire_path(RT.lease_path(), "worker:a", wait_s=0,
                    stale_after_s=RT.LEASE_STALE_SECONDS)
    assert RL.heartbeat_path(RT.lease_path(), "worker:b") is False
    RL.release_path(RT.lease_path(), "worker:a")


def test_a_lost_lease_stops_the_worker_rather_than_recreating_it(root,
                                                                monkeypatch):
    """Recreating a lost lease is how two workers each believe they are alone."""
    calls = {"n": 0}

    def _session(**kw):
        calls["n"] += 1
        cb = kw.get("on_progress")
        if cb is not None:
            RT.lease_path().unlink()          # the lease disappears mid-run
            assert cb({"iteration": 1, "jobs_completed": 1,
                       "hypotheses_measured": 3}) is False
        return {"stop_condition": LP.STOP_D, "jobs_executed": 1,
                "hypotheses_measured": 3, "research_still_ready": 5}

    monkeypatch.setattr(LP, "run_session", _session)
    body = RT.run_forever(install_signal_handlers=False,
                          sleep_fn=lambda s: None)
    assert body["worker_state"] == RT.W_LEASE_LOST
    assert calls["n"] == 1


# --------------------------------------------------------------------------- #
# Resume, never reset
# --------------------------------------------------------------------------- #
def test_the_runtime_resumes_persisted_state_and_resets_nothing(root,
                                                               monkeypatch):
    mem = M.open_memory()
    hid = _register(mem, title="prior work")
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                      reason_rejected="prior verdict")
    burden_before = mem.burden()["total"]
    graveyard_before = len(mem.graveyard())

    queue = LP.open_queue()
    queue.enqueue(AR.CAT_EXPERIMENT, lane="r59.economic",
                  payload={"mandate_id": "M_PRIOR"})
    depth_before = queue.depth()

    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 0,
        "hypotheses_measured": 0, "research_still_ready": 0})
    RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                   sleep_fn=lambda s: None)

    mem2 = M.open_memory()
    assert mem2.burden()["total"] == burden_before
    assert len(mem2.graveyard()) == graveyard_before
    assert mem2.get(hid)["outcome"] == r59.HO_NO_ALPHA_EVIDENCE
    assert LP.open_queue().depth() == depth_before


def test_a_second_invocation_resumes_instead_of_starting_over(root,
                                                              monkeypatch):
    seen = []

    def _session(**kw):
        seen.append(len(seen) + 1)
        return {"stop_condition": LP.STOP_A, "jobs_executed": 0,
                "hypotheses_measured": 0, "research_still_ready": 0}

    monkeypatch.setattr(LP, "run_session", _session)
    RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                   sleep_fn=lambda s: None)
    first = RT.read_status()
    RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                   sleep_fn=lambda s: None)
    second = RT.read_status()

    assert seen == [1, 2]
    # A NEW worker instance, over the SAME research state.
    assert (first["worker_identity"]["instance_id"]
            != second["worker_identity"]["instance_id"])
    assert second["worker_state"] == RT.W_STOPPED


# --------------------------------------------------------------------------- #
# No production cap
# --------------------------------------------------------------------------- #
def test_the_persistent_runtime_has_no_production_iteration_limit():
    import inspect
    sig = inspect.signature(RT.run_forever)
    assert sig.parameters["debug_max_cycles"].default is None
    assert sig.parameters["debug_max_seconds"].default is None


def test_a_debug_cap_is_reported_as_an_operator_override(root, monkeypatch):
    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 0,
        "hypotheses_measured": 0, "research_still_ready": 7})
    body = RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                          sleep_fn=lambda s: None)
    assert body["operator_override"] is True
    assert body["production_iteration_limit"] is None
    assert "DEBUG" in str(body["stopped_because"])


def test_the_research_loop_is_called_without_any_cap(root, monkeypatch):
    captured = {}

    def _session(**kw):
        captured.update(kw)
        return {"stop_condition": LP.STOP_A, "jobs_executed": 0,
                "hypotheses_measured": 0, "research_still_ready": 0}

    monkeypatch.setattr(LP, "run_session", _session)
    RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                   sleep_fn=lambda s: None)
    assert "max_iterations" not in captured or captured["max_iterations"] is None
    assert "budget_seconds" not in captured or captured["budget_seconds"] is None
    assert captured.get("on_progress") is not None


def test_a_supervisor_stop_is_external_and_never_a_research_conclusion(root):
    """The counter this release deleted was self-imposed; a signal is not."""
    mem = M.open_memory()
    queue = LP.open_queue()
    stops = {"n": 0}

    def _progress(_row):
        stops["n"] += 1
        return False

    out = LP.run_session(mem=mem, queue=queue, batch=1,
                         max_jobs_per_iteration=1, on_progress=_progress,
                         seed_opportunities=False)
    assert out["stop_condition"] == LP.STOP_D
    assert out["stop_detail"]["limit"] == LP.SUPERVISOR_STOP
    assert out["stop_detail"]["external"] is True
    assert out["stop_detail"]["operator_override"] is False
    assert out["resumable"] is True


# --------------------------------------------------------------------------- #
# Sleep / wake
# --------------------------------------------------------------------------- #
def test_ready_work_always_beats_sleeping():
    plan = RT.plan_sleep(ready_work=3, conditions=[])
    assert plan["sleep_seconds"] == 0.0
    assert plan["state"] == RT.W_RESEARCHING
    assert plan["reason"] == "EXECUTABLE_RESEARCH_EXISTS"


def test_it_sleeps_only_when_solely_future_data_can_advance_the_state():
    plan = RT.plan_sleep(ready_work=0, conditions=[
        {"name": "BLOCKED_SOURCES", "watermark": "0"}])
    assert plan["sleep_seconds"] >= RT.MIN_SLEEP_SECONDS
    assert plan["reason"] == "ONLY_FUTURE_DATA_CAN_ADVANCE_THE_STATE"


def test_a_blocked_external_source_backs_off_without_abandoning_the_lane():
    plan = RT.plan_sleep(ready_work=0, conditions=[
        {"name": "BLOCKED_SOURCES", "watermark": "4"}])
    assert plan["reason"] == "WAITING_ON_A_BLOCKED_EXTERNAL_SOURCE"
    assert plan["sleep_seconds"] == RT.MIN_SLEEP_SECONDS


def test_wake_conditions_cover_every_declared_arrival(root):
    mem = M.open_memory()
    queue = LP.open_queue()
    names = {c["name"] for c in RT.wake_conditions(mem, queue)}
    for required in ("QUEUE_RUNNABLE", "BLOCKED_SOURCES", "MARKET_DATA_PANEL",
                     "EODHD_COLLECTION", "INGESTION_STORE", "FORM4_HISTORY",
                     "FORWARD_EVIDENCE", "STEELE_ANALYST_SAMPLE",
                     "DATA_OPPORTUNITY_FRONTIER"):
        assert required in names, required


def test_new_data_changes_a_watermark_and_is_detected_as_arrival(root):
    mem = M.open_memory()
    queue = LP.open_queue()
    before = RT.wake_conditions(mem, queue)
    mem.set_opportunity("OPP_NEW", title="a new source", state=
                        r59.DO_FREE_AVAILABLE)
    after = RT.wake_conditions(mem, queue)
    delta = RT.wake_delta(before, after)
    assert delta["any_change"] is True
    assert "DATA_OPPORTUNITY_FRONTIER" in delta["changed"]


def test_an_unchanged_world_is_not_reported_as_an_arrival(root):
    mem = M.open_memory()
    queue = LP.open_queue()
    a = RT.wake_conditions(mem, queue)
    b = RT.wake_conditions(mem, queue)
    assert RT.wake_delta(a, b)["any_change"] is False


# --------------------------------------------------------------------------- #
# Resource governance
# --------------------------------------------------------------------------- #
def _fill_machine_history(mem, n=120, notable=4, prefix="m"):
    for i in range(n):
        hid = _register(mem, title="%s%d" % (prefix, i),
                        generation_method="AUTO_TRANSFORM_GRAMMAR",
                        economic_family="MACHINE_%d" % (i % 7),
                        asset_class=r59.AC_US_EQUITY)
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                          statistic={"lockbox_t": 3.1 if i < notable else 0.4},
                          reason_rejected="measured")


def _fill_economic_history(mem, n=60, notable=18):
    for i in range(n):
        hid = _register(mem, title="e%d" % i,
                        generation_method="GOVERNOR_ECONOMIC_MANDATE",
                        economic_family="ECON_%d" % (i % 5),
                        asset_class=r59.AC_RATES)
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                          statistic={"lockbox_t": 3.4 if i < notable else 0.3},
                          reason_rejected="measured")


def test_an_unbounded_grammar_cannot_monopolise_the_machine(mem):
    _fill_economic_history(mem)
    _fill_machine_history(mem)
    cap = GOV.capacity_allocation(mem)
    assert cap["generative_share_recent"] > GOV.MACHINE_SHARE_CEILING
    assert cap["crowding_multiplier"] < 1.0
    assert cap["multiplier"] < 1.0
    assert cap["machine_batch"] < GOV.MACHINE_BATCH
    assert cap["reasons"]


def test_a_deteriorating_marginal_yield_reduces_allocation(mem):
    _fill_economic_history(mem, n=60, notable=18)      # 30% notable
    _fill_machine_history(mem, n=120, notable=2)       # ~1.7% notable
    cap = GOV.capacity_allocation(mem)
    assert cap["marginal_yield_multiplier"] < 0.5
    assert cap["alternative_notable_rate"] > cap["generative_notable_rate"]


def test_mathematical_discovery_is_throttled_and_never_killed(mem):
    _fill_economic_history(mem, n=60, notable=60)      # a perfect alternative
    _fill_machine_history(mem, n=400, notable=0)       # a worthless grammar
    cap = GOV.capacity_allocation(mem)
    assert cap["multiplier"] >= GOV.MIN_CAPACITY_MULTIPLIER
    assert cap["machine_batch"] >= 1
    assert cap["mathematical_discovery_disabled"] is False


def test_new_information_can_reopen_allocation(mem):
    _fill_economic_history(mem)
    _fill_machine_history(mem)
    baseline = GOV.capacity_allocation(mem)
    throttled = GOV.capacity_allocation(mem)["multiplier"]
    mem.set_opportunity("OPP_FRESH", title="a source that did not exist",
                        state=r59.DO_ALREADY_OWNED_UNUSED)
    reopened = GOV.capacity_allocation(mem)
    assert throttled < 1.0
    assert reopened["multiplier"] == 1.0
    assert reopened["reopened_by"] == "INFORMATION_SET_CHANGED"
    assert reopened["information_set_fingerprint"] != \
        baseline["information_set_fingerprint"]


def test_re_seeding_the_same_opportunities_does_not_reopen_allocation(mem):
    """Every session re-seeds; a timestamp rule would reopen on every call."""
    _fill_economic_history(mem)
    _fill_machine_history(mem)
    mem.set_opportunity("OPP_A", title="a known source",
                        state=r59.DO_FREE_AVAILABLE)
    GOV.capacity_allocation(mem)                    # observe the baseline
    throttled = GOV.capacity_allocation(mem)
    # Re-seed the identical row, exactly as opportunities.seed() does.
    mem.set_opportunity("OPP_A", title="a known source",
                        state=r59.DO_FREE_AVAILABLE)
    after = GOV.capacity_allocation(mem)
    assert throttled["multiplier"] < 1.0
    assert after["multiplier"] == throttled["multiplier"]
    assert after["reopened_by"] is None


def test_a_reopened_allocation_expires_once_the_grace_tests_are_spent(mem):
    _fill_economic_history(mem)
    _fill_machine_history(mem)
    GOV.capacity_allocation(mem)
    mem.set_opportunity("OPP_NEW2", title="new", state=r59.DO_FREE_AVAILABLE)
    assert GOV.capacity_allocation(mem)["multiplier"] == 1.0
    # The grammar spends its restored allocation on the new information.
    _fill_machine_history(mem, n=GOV.REOPEN_GRACE_TESTS + 5, notable=1,
                          prefix="spent")
    spent = GOV.capacity_allocation(mem)
    assert spent["multiplier"] < 1.0
    assert spent["reopened_by"] is None


def test_every_declared_exploration_lane_is_preserved(mem):
    cap = GOV.capacity_allocation(mem)
    for lane in (r59.AC_US_EQUITY, r59.AC_RATES, r59.AC_COMMODITY, r59.AC_FX,
                 r59.AC_CROSS_ASSET, "EVENTS", "FUNDAMENTALS", "POSITIONING",
                 "MACRO", "DATA_OPPORTUNITY"):
        assert lane in cap["protected_lanes"], lane


def test_throttling_the_machine_leaves_the_batch_diverse():
    machine = [{"kind": GOV.MANDATE_MATHEMATICAL, "asset_class":
                r59.AC_US_EQUITY, "expected_information_value": 0.9}
               for _ in range(20)]
    others = [{"kind": GOV.MANDATE_ECONOMIC, "asset_class": r59.AC_RATES,
               "expected_information_value": 0.4} for _ in range(20)]
    picked = GOV._apply_fairness(machine + others, limit=10,
                                 machine_multiplier=0.25)
    n_machine = sum(1 for m in picked if m["kind"] == GOV.MANDATE_MATHEMATICAL)
    assert len(picked) == 10                    # never costs throughput
    assert 1 <= n_machine <= 3                  # alive, but not the batch


def test_cross_asset_fairness_survives_the_capacity_change(mem):
    """The non-equity floor is not undone by throttling one kind."""
    eq = [{"kind": GOV.MANDATE_MATHEMATICAL, "asset_class": r59.AC_US_EQUITY,
           "expected_information_value": 0.95} for _ in range(20)]
    non_eq = [{"kind": GOV.MANDATE_ECONOMIC, "asset_class": r59.AC_COMMODITY,
               "expected_information_value": 0.2} for _ in range(20)]
    picked = GOV._apply_fairness(eq + non_eq, limit=10, machine_multiplier=0.25)
    n_non_eq = sum(1 for m in picked
                   if m["asset_class"] != r59.AC_US_EQUITY)
    assert n_non_eq >= int(round(10 * r59.NON_EQUITY_RESERVATION))


# --------------------------------------------------------------------------- #
# Forward evidence
# --------------------------------------------------------------------------- #
def test_maturation_is_refused_from_this_development_worktree():
    """Prospective rows record what was known; uncommitted code may not write them."""
    from api import runtime_identity as rid
    ident = RT.worker_identity(reader=rid.read_source_identity)
    assert ident["source"]["dirty"] is True, "this worktree should be dirty"
    policy = RT.maturation_policy(ident)
    assert policy["allowed"] is False
    assert policy["reason"] == "SOURCE_HAS_UNCOMMITTED_CHANGES"


def test_an_unresolved_revision_fails_closed_rather_than_open():
    """No reader means no answer, and no answer must never mean yes."""
    ident = RT.worker_identity()                 # no reader injected
    assert ident["source"]["commit"] is None
    policy = RT.maturation_policy(ident)
    assert policy["allowed"] is False
    assert policy["reason"] == "SOURCE_REVISION_UNRESOLVED"


def test_maturation_requires_a_known_clean_commit():
    dirty = {"source": {"commit": "abc", "dirty": True}}
    clean = {"source": {"commit": "abc", "dirty": False}}
    unknown = {"source": {"commit": "abc", "dirty": None}}
    pinned_elsewhere = {"source": {"commit": "abc", "dirty": False,
                                   "is_deployed_source": False,
                                   "repo_root": "D:\\somewhere",
                                   "deployment_pin": "C:\\elsewhere"}}
    assert RT.maturation_policy(dirty)["allowed"] is False
    assert RT.maturation_policy(unknown)["allowed"] is False
    assert RT.maturation_policy(pinned_elsewhere)["allowed"] is False
    assert RT.maturation_policy(clean)["allowed"] is True


def test_the_research_package_never_imports_the_application_api():
    """The reader is INJECTED; importing it here would couple research to the app."""
    import ast
    tree = ast.parse((_ROOT / "alpha_agent" / "r59" / "runtime.py")
                     .read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            base = "." * (node.level or 0) + (node.module or "")
            assert not base.startswith(("api", "engine", "db")), base
        elif isinstance(node, ast.Import):
            for a in node.names:
                assert not a.name.startswith(("api", "engine", "db")), a.name
    import inspect
    assert "identity_reader" in inspect.signature(RT.run_forever).parameters
    assert "reader" in inspect.signature(RT.worker_identity).parameters


def test_the_entrypoint_injects_the_canonical_revision_reader():
    src = _ENTRYPOINT.read_text(encoding="utf-8")
    assert "runtime_identity" in src
    assert "read_source_identity" in src
    assert "identity_reader=reader" in src


def test_the_runtime_skips_maturation_when_it_is_not_allowed(root,
                                                             monkeypatch):
    called = {"n": 0}
    monkeypatch.setattr(RT, "_mature_forward_evidence",
                        lambda: called.__setitem__("n", called["n"] + 1))
    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 0,
        "hypotheses_measured": 0, "research_still_ready": 0})
    body = RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                          sleep_fn=lambda s: None)
    assert called["n"] == 0
    assert body["maturation"]["allowed"] is False
    assert body["cycles"][0]["maturation"] == "SKIPPED"


def test_maturation_delegates_to_the_canonical_r52_owner():
    src = (_ROOT / "alpha_agent" / "r59" / "runtime.py").read_text(
        encoding="utf-8")
    assert "from ..r52 import runtime as R52" in src
    assert "R52.research_runtime_cycle(" in src
    # It must not grow its own scoring, emission or timing rule.
    for forbidden in ("def score", "def emit", "INVOCATION_PLAN",
                      "def build_batch"):
        assert forbidden not in src, forbidden


def test_a_forward_confirmation_asks_for_review_and_promotes_nothing(root,
                                                                     monkeypatch):
    from alpha_agent.r52 import runtime as R52
    monkeypatch.setattr(R52, "research_runtime_cycle", lambda **kw: {
        "state": "RUN_COMPLETED", "run_id": "r52run_x",
        "promotion_ready_count": 2,
        "advance": {"tournament_outcomes_scored": 3}})
    out = RT._mature_forward_evidence()
    assert out["challenger_review"]["signal"] == RT.CHALLENGER_REVIEW
    assert out["promotes_model"] is False
    assert out["mutates_holdings"] is False


# --------------------------------------------------------------------------- #
# Observability, on D:
# --------------------------------------------------------------------------- #
def test_status_reports_every_declared_field(root, monkeypatch):
    monkeypatch.setattr(LP, "run_session", lambda **kw: {
        "stop_condition": LP.STOP_A, "jobs_executed": 2,
        "hypotheses_measured": 9, "research_still_ready": 4})
    RT.run_forever(debug_max_cycles=1, install_signal_handlers=False,
                   sleep_fn=lambda s: None)
    st = RT.status()
    for field in ("worker_state", "worker_identity", "source_identity",
                  "started_at", "last_heartbeat", "current_lane",
                  "queue_ready", "queue_running", "queue_blocked",
                  "cumulative_hypotheses", "cumulative_search_burden",
                  "last_completed_experiment", "stop_or_sleep_reason",
                  "next_planned_wake", "latest_error", "safety"):
        assert field in st, field
    assert st["safety"]["creates_orders"] is False


def test_runtime_state_and_logs_live_under_the_research_root(root):
    assert RT.runtime_dir().is_relative_to(r59.research_root())
    assert RT.lease_path().parent == RT.runtime_dir()
    src = (_ROOT / "alpha_agent" / "r59" / "runtime.py").read_text(
        encoding="utf-8")
    # No state path may be hard-coded onto the C: system drive.
    assert "C:\\\\Users\\\\binis\\\\paper_trader\\\\logs" not in src
    assert 'r59.research_root()' in src


def test_the_default_research_root_is_on_the_data_drive():
    assert str(r59.DEFAULT_RESEARCH_ROOT).upper().startswith("D:")


# --------------------------------------------------------------------------- #
# Scheduled task: idempotent, explicit, and untouched by these tests
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(not _INSTALLER.exists(), reason="installer absent")
def test_task_install_decision_is_idempotent(tmp_path):
    """An identical persistent definition installs as UNCHANGED."""
    snapshot = {
        "Action": {
            "Execute": "C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe",
            "Arguments": ("\"C:\\Users\\binis\\paper_trader\\scripts\\"
                          "run_research_runtime.py\" --mode persistent "
                          "--trigger SCHEDULED"),
            "WorkingDirectory": "C:\\Users\\binis\\paper_trader"},
        "Triggers": [
            {"Type": "MSFT_TaskDailyTrigger",
             "StartBoundary": "2026-08-31T08:15:00", "Enabled": True},
            {"Type": "MSFT_TaskDailyTrigger",
             "StartBoundary": "2026-08-31T17:45:00", "Enabled": True},
            {"Type": "MSFT_TaskDailyTrigger",
             "StartBoundary": "2026-08-31T19:45:00", "Enabled": True},
            {"Type": "MSFT_TaskDailyTrigger",
             "StartBoundary": "2026-08-31T21:45:00", "Enabled": True},
            {"Type": "MSFT_TaskLogonTrigger",
             "StartBoundary": "2026-08-31T00:00:00", "Enabled": True}],
        "Enabled": True,
        "Principal": {"UserId": os.environ.get("USERNAME", "binis"),
                      "LogonType": "S4U"},
        "Settings": {"StartWhenAvailable": True,
                     "MultipleInstances": "IgnoreNew",
                     "ExecutionTimeLimit": "PT0S", "RestartCount": 2,
                     "RestartInterval": "PT10M", "WakeToRun": True},
    }
    probe = tmp_path / "snap.json"
    probe.write_text(json.dumps(snapshot), encoding="utf-8")
    out = _ps(_INSTALLER, "-Mode", "Persistent", "-DecisionProbe", str(probe))
    body = _probe_json(out)
    assert body.get("decision") == "UNCHANGED", out


@pytest.mark.skipif(not _INSTALLER.exists(), reason="installer absent")
def test_a_cycle_task_is_not_silently_treated_as_a_persistent_one(tmp_path):
    """The live task today is a CYCLE task; installing persistent must MIGRATE."""
    snapshot = {
        "Action": {
            "Execute": "C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe",
            "Arguments": ("\"C:\\Users\\binis\\paper_trader\\scripts\\"
                          "run_research_runtime.py\" --trigger SCHEDULED"),
            "WorkingDirectory": "C:\\Users\\binis\\paper_trader"},
        "Triggers": [{"Type": "MSFT_TaskDailyTrigger",
                      "StartBoundary": "2026-08-31T08:15:00", "Enabled": True}],
        "Enabled": True,
        "Principal": {"UserId": os.environ.get("USERNAME", "binis"),
                      "LogonType": "S4U"},
        "Settings": {"StartWhenAvailable": True,
                     "MultipleInstances": "IgnoreNew",
                     "ExecutionTimeLimit": "PT2H", "RestartCount": 2,
                     "RestartInterval": "PT10M", "WakeToRun": True},
    }
    probe = tmp_path / "snap.json"
    probe.write_text(json.dumps(snapshot), encoding="utf-8")
    out = _ps(_INSTALLER, "-Mode", "Persistent", "-DecisionProbe", str(probe))
    body = _probe_json(out)
    assert body.get("decision") == "BLOCKED_DEFINITION", out
    joined = " ".join(body["mismatches"])
    assert "--mode persistent" in joined
    assert "logon trigger" in joined or "PT0S" in joined


@pytest.mark.skipif(not _MANAGER.exists(), reason="manager absent")
def test_every_mutating_action_requires_an_explicit_execute_switch():
    for action in ("Install", "Start", "Stop", "Restart", "Uninstall"):
        out = _ps(_MANAGER, "-Action", action)
        assert "RESEARCH_WORKER_BLOCKED" in out, (action, out)
        assert "requires -Execute" in out, (action, out)


@pytest.mark.skipif(not _MANAGER.exists(), reason="manager absent")
def test_a_development_worktree_may_never_be_promoted_into_a_service():
    for action in ("Install", "Start", "Restart", "Uninstall"):
        out = _ps(_MANAGER, "-RepoRoot", str(_ROOT), "-Action", action,
                  "-Execute")
        assert "RESEARCH_WORKER_BLOCKED" in out, (action, out)
        assert "not the deployed checkout" in out, (action, out)


@pytest.mark.skipif(not _MANAGER.exists(), reason="manager absent")
def test_status_is_read_only_and_needs_no_execute_switch():
    src = _MANAGER.read_text(encoding="utf-8")
    status_block = src[src.index("'Status' {"):src.index("'Validate' {")]
    for mutator in ("Register-ScheduledTask", "Start-ScheduledTask",
                    "Stop-ScheduledTask", "Stop-Process",
                    "Unregister-ScheduledTask", "Out-File", "Set-Content"):
        assert mutator not in status_block, mutator


def test_the_manager_delegates_registration_to_the_one_registrar():
    """One task, one registrar. A second Register-ScheduledTask is a second owner."""
    registrars = []
    for p in sorted((_ROOT / "scripts").glob("*.ps1")):
        text = p.read_text(encoding="utf-8", errors="ignore")
        if "Register-ScheduledTask" in text and LIVE_TASK in text:
            registrars.append(p.name)
    assert registrars == ["install_research_runtime_task.ps1"], registrars
    assert "Register-ScheduledTask" not in _MANAGER.read_text(encoding="utf-8")


def test_no_test_in_this_module_mutated_the_live_scheduled_task(
        live_task_before):
    """Measured, not promised: the live task is byte-identical afterwards.

    Everything above drives the installer's hermetic -DecisionProbe mode and
    the manager's refusal paths, so the real PowerShell decision logic is
    exercised while the scheduler is never touched. This asserts that rather
    than asserting the absence of a token, which an added test could quietly
    make untrue.
    """
    if not live_task_before:
        pytest.skip("the scheduler could not be read on this host")
    assert _live_task_snapshot() == live_task_before


def test_the_live_checkout_is_never_named_by_the_research_package():
    """A deployment path is a deployment fact, not a research constant."""
    for path in sorted((_ROOT / "alpha_agent" / "r59").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        assert str(LIVE_CHECKOUT) not in text, path.name
        assert "binis\\paper_trader" not in text, path.name
    src = (_ROOT / "alpha_agent" / "r59" / "runtime.py").read_text(
        encoding="utf-8")
    assert "DEPLOYED_ROOT_ENV" in src


# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
def test_the_runtime_cannot_reach_an_operational_owner():
    """Checked on the IMPORT GRAPH, not on the prose.

    A raw-text scan for these words flags the module's own safety docstring,
    which is how a banned-token test ends up being weakened until it proves
    nothing. What matters is what the module can actually reach.
    """
    import ast
    src = (_ROOT / "alpha_agent" / "r59" / "runtime.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)

    imported: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = "." * (node.level or 0) + (node.module or "")
            imported.add(base)
            imported.update("%s.%s" % (base, a.name) for a in node.names)

    banned = ("portfolio_decision", "rebalance_execution", "daily_close",
              "normal_cycle", "operational_book", "broker", "requests",
              "urllib", "http")
    for name in sorted(imported):
        for token in banned:
            assert token not in name, (name, token)

    # No network endpoint may appear as a literal either.
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert "127.0.0.1" not in node.value
            assert "http://" not in node.value


def test_the_entrypoint_owns_no_loop_and_no_clock_rule():
    src = _ENTRYPOINT.read_text(encoding="utf-8")
    assert "while True" not in src
    assert "def research_runtime_cycle" not in src
    assert "research_runtime_cycle" in src
    assert "run_forever" in src


def test_persistent_mode_is_opt_in_and_cycle_remains_the_default():
    src = _ENTRYPOINT.read_text(encoding="utf-8")
    assert 'default="cycle"' in src
    assert '"--mode"' in src


def test_the_safety_contract_is_unchanged_by_persistence():
    assert r59.SAFETY["creates_orders"] is False
    assert r59.SAFETY["promotes_model"] is False
    assert r59.SAFETY["activates_sleeve"] is False
    assert r59.SAFETY["approves_proposal"] is False
    assert r59.SAFETY["mutates_operational_store"] is False
    assert r59.SAFETY["live_checkout_read_only"] is True
