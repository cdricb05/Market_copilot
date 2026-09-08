"""alpha_agent.r59.loop - the autonomous session runner that does not idle.

This is the behaviour R59 exists to restore. One session is:

    measure the frontier
    -> ask the governor for mandates
    -> seed them onto the CANONICAL queue
    -> drain the r59.* lanes
    -> record every result in research memory
    -> RE-measure, RE-generate, RE-seed
    -> continue

A failed hypothesis is an INPUT to the next iteration, not an exit. The only
things that end a session are the four conditions in section O:

    A  the governor cannot generate a valid new mandate without new information
    B  a genuine external blocker prevents every remaining high-value path
    C  a safety constraint requires human action
    D  the execution environment imposes a hard resource or runtime limit

"the batch drained" and "a survivor was frozen" are explicitly NOT stop
conditions, and the runner asserts that: after every batch it asks the governor
for more work before it will even consider stopping.

Resumability is inherited, not invented: the queue is the Stage-8 durable
SQLite queue and the memory is a SQLite database, so a killed process resumes
exactly where it stopped - an interrupted job is left RUNNING and recovered by
``requeue_stale`` on the next pass.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, Optional

from .. import autonomous_research as AR
from .. import r59
from . import frontier as FR
from . import governor as GOV
from . import handlers as H
from . import memory as M
from . import opportunities as OPP

CALCULATION_OWNER = "alpha_agent.r59.loop"
QUEUE_NAME = "r59_autonomy.sqlite"

STOP_A = "A_NO_INDEPENDENT_EXECUTABLE_RESEARCH_REMAINS"
STOP_B = "B_EXTERNAL_BLOCKER"
STOP_C = "C_SAFETY_CONSTRAINT_REQUIRES_HUMAN"
STOP_D = "D_ENVIRONMENT_LIMIT"

#: The supervisor asked this session to wind down (SIGINT/SIGTERM, a service
#: stop, a lost lease). Unlike the iteration counter this release deleted, it
#: is genuinely EXTERNAL: the operating system or the operator asked, and the
#: session stops at the next clean iteration boundary with its state persisted.
SUPERVISOR_STOP = "supervisor_stop_requested"


def queue_db_path(db_path: Optional[Path] = None) -> Path:
    """Where the R59 queue LIVES, without creating anything (R60)."""
    return Path(db_path) if db_path else (r59.research_root() / QUEUE_NAME)


def queue_present(db_path: Optional[Path] = None) -> bool:
    return queue_db_path(db_path).exists()


def open_queue(db_path: Optional[Path] = None, *,
               read_only: bool = False) -> AR.ResearchQueue:
    """Open the canonical Stage-8 queue for the R59 session.

    A dedicated database file, NOT a dedicated queue implementation: this is
    ``alpha_agent.autonomous_research.ResearchQueue`` with all of its atomic
    claim/settle, backoff and dead-worker recovery. Using a separate file keeps
    an R59 research session from competing for write locks with the live
    collection queue while remaining the same code path - and
    ``handlers.route_r59`` supports sharing one file when that is wanted.

    ``read_only`` (R60) returns an observer handle: no directory is created,
    no schema script runs and every transition is refused, so a read model can
    count the queue without writing to it.
    """
    path = queue_db_path(db_path)
    if read_only:
        return AR.ResearchQueue(path, read_only=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    return AR.ResearchQueue(path)


def run_session(*, max_iterations: Optional[int] = None, batch: int = 12,
                max_jobs_per_iteration: int = 8,
                budget_seconds: Optional[float] = None,
                mem: Optional[M.ResearchMemory] = None,
                queue: Optional[AR.ResearchQueue] = None,
                base_handlers: Optional[dict] = None,
                seed_opportunities: bool = True,
                adopt_forward: Optional[Callable] = None,
                on_progress: Optional[Callable[[dict], bool]] = None) -> dict:
    """Run autonomous research until it is genuinely exhausted.

    The DEFAULT is ``RUN_UNTIL_RESEARCH_EXHAUSTED_OR_REAL_EXTERNAL_BLOCKER``:
    both ``max_iterations`` and ``budget_seconds`` default to None, so the
    session ends on condition A or B, never on a counter the runner invented.

    An earlier R59 run stopped at ``D_ENVIRONMENT_LIMIT - iteration limit``
    with nineteen executable mandates still READY. That was the runner
    manufacturing its own interruption and reporting it as if the environment
    had imposed it; a self-imposed counter is not condition D. Both caps remain
    available as EXPLICIT operator or debugging overrides and are reported as
    such when they bind.

    ``on_progress`` is how a persistent supervisor stays alive around this
    function WITHOUT reintroducing a cap. It is called once per completed
    iteration with that iteration's record; returning False asks the session
    to wind down at the next clean boundary. The supervisor uses it to
    heartbeat its lease and to honour SIGINT/SIGTERM - so the loop still runs
    until the research itself is finished, and the only thing that can cut it
    short from outside is something genuinely outside.
    """
    started = time.monotonic()
    mem = mem or M.open_memory()
    queue = queue or open_queue()
    if seed_opportunities:
        OPP.seed(mem)

    # R61 - the governed prospective-adoption owner, INJECTED. The research
    # package may not import the application layer, so a freeze reaches its
    # forward-evidence owner the same way the revision reader reaches the
    # runtime: from the entrypoint that composes them.
    r59_handlers = H.make_handlers(mem, queue, adopt_forward=adopt_forward)
    handlers = (H.route_r59(base_handlers, r59_handlers) if base_handlers
                else r59_handlers)

    iterations: list = []
    total_executed = 0
    total_measured = 0
    stop = None
    stop_detail: dict = {}

    i = -1
    while True:
        i += 1
        if max_iterations is not None and i >= int(max_iterations):
            stop, stop_detail = STOP_D, {
                "limit": "max_iterations", "value": max_iterations,
                "operator_override": True,
                "note": "an EXPLICIT operator/debug iteration cap bound; this "
                        "is not a research conclusion and work remains READY"}
            break
        if budget_seconds is not None and \
                (time.monotonic() - started) >= float(budget_seconds):
            stop, stop_detail = STOP_D, {
                "limit": "budget_seconds", "value": budget_seconds,
                "operator_override": True,
                "note": "an EXPLICIT operator/debug wall-clock cap bound; the "
                        "queue and memory hold the state"}
            break

        view = FR.measure(mem)
        batch_doc = GOV.generate_mandates(mem, limit=batch, frontier_view=view)
        seeded = H.seed_mandates(queue, batch_doc["mandates"])

        # Dead-worker recovery before claiming: an interrupted previous run
        # left its job RUNNING, and it must come back rather than be lost.
        requeued = queue.requeue_stale()

        report = AR.drain_jobs(
            queue, handlers, max_jobs=max_jobs_per_iteration,
            lane_prefixes=[r59.LANE_PREFIX],
            budget_seconds=(None if budget_seconds is None
                            else max(1.0, float(budget_seconds)
                                     - (time.monotonic() - started))))

        measured = 0
        for h in report.get("handled") or []:
            job = queue.get(h["job_id"])
            res = (job.result or {}) if job else {}
            measured += int(res.get("hypotheses_measured") or 0)
        total_executed += int(report.get("jobs_claimed") or 0)
        total_measured += measured

        iterations.append({
            "iteration": i + 1,
            "frontier_ready": view["ready"],
            "non_equity_ready": view["non_equity_ready"],
            "mandates_generated": len(batch_doc["mandates"]),
            "mandates_enqueued": seeded["enqueued"],
            "already_live": seeded["already_live"],
            "requeued_stale": requeued,
            "jobs_claimed": report.get("jobs_claimed"),
            "jobs_completed": report.get("jobs_completed"),
            "jobs_blocked": report.get("jobs_blocked"),
            "jobs_retryable": report.get("jobs_retryable"),
            "handler_errors": report.get("handler_errors"),
            "hypotheses_measured": measured,
            "queue_depth_after": report.get("queue_depth_after"),
        })

        # The supervisor gets the floor between iterations: it heartbeats its
        # lease here, and may ask for a clean wind-down. Everything measured
        # in this iteration is already persisted, so stopping here loses
        # nothing and the next invocation resumes from the queue.
        if on_progress is not None and on_progress(iterations[-1]) is False:
            stop, stop_detail = STOP_D, {
                "limit": SUPERVISOR_STOP,
                "operator_override": False,
                "external": True,
                "note": "the supervising runtime asked this session to wind "
                        "down (service stop, signal, or a lost worker lease); "
                        "READY work remains and the next start resumes it"}
            break

        # THE INVARIANT: a drained batch is never terminal. Ask the governor
        # for more work; only its refusal can stop the session.
        if not report.get("jobs_claimed"):
            decision = GOV.stop_reason(mem)
            if decision.get("stop"):
                stop, stop_detail = STOP_A, decision
                break
            if seeded["enqueued"] == 0 and seeded["already_live"] == 0:
                stop, stop_detail = STOP_B, {
                    "reason": "the governor reports available mandates but "
                              "none could be enqueued or claimed; an external "
                              "blocker is holding every remaining path",
                    "governor": decision}
                break
            # Mandates exist and are live but nothing was claimable this pass
            # (every live job is blocked or backing off). That is condition B,
            # not a reason to spin.
            if queue.runnable_depth() == 0:
                stop, stop_detail = STOP_B, {
                    "reason": "no job is claimable: every outstanding job is "
                              "blocked on a named external condition",
                    "blocked": [{"lane": j.lane, "reason": j.blocked_reason}
                                for j in queue.blocked_jobs(limit=20)],
                    "governor": decision}
                break

    final_view = FR.measure(mem)
    still_ready = GOV.generate_mandates(mem, limit=50,
                                        frontier_view=final_view)
    summary = {
        "calculation_owner": CALCULATION_OWNER,
        "started_at": r59.now_iso(),
        "elapsed_seconds": round(time.monotonic() - started, 1),
        "iterations": iterations,
        "n_iterations": len(iterations),
        "jobs_executed": total_executed,
        "hypotheses_measured": total_measured,
        "stop_condition": stop,
        "stop_detail": stop_detail,
        "research_still_ready": len(still_ready["mandates"]),
        "still_ready_by_asset_class": count_by_asset_class(still_ready["mandates"]),
        "queue_depth": queue.depth(),
        "queue_path": str(queue.db_path),
        "memory_path": str(mem.db_path),
        "resumable": True,
        "memory_summary": mem.summary(),
        "frontier": {ac: r["state"]
                     for ac, r in final_view["asset_classes"].items()},
        "safety": dict(r59.SAFETY),
    }
    mem.event("SESSION_COMPLETE", subject=CALCULATION_OWNER,
              detail={"stop": stop, "jobs": total_executed,
                      "measured": total_measured,
                      "still_ready": summary["research_still_ready"]})
    return summary


def count_by_asset_class(mandates: list) -> dict:
    """Public: mandates per asset class. Read by the report owner."""
    out: dict = {}
    for m in mandates:
        out[m["asset_class"]] = out.get(m["asset_class"], 0) + 1
    return out


def session_results(mem: Optional[M.ResearchMemory] = None) -> dict:
    """Everything R59 measured in this estate, for the report."""
    mem = mem or M.open_memory()
    r59_rows = [h for h in mem.list_hypotheses(limit=20000)
                if h.get("release") == r59.RELEASE]
    qualified = [h for h in r59_rows if h.get("outcome") == r59.HO_QUALIFIED]

    def _t(h):
        st = h.get("statistic") or {}
        return st.get("lockbox_t")

    ranked = sorted((h for h in r59_rows if _t(h) is not None),
                    key=lambda h: -float(_t(h)))
    machine = [h for h in ranked if h.get("origin") == "MACHINE_GENERATED"]
    econ = [h for h in ranked if h.get("origin") == "R59_GOVERNOR"]
    non_eq = [h for h in ranked if h.get("asset_class") != r59.AC_US_EQUITY]

    return {
        "calculation_owner": CALCULATION_OWNER,
        "r59_hypotheses_measured": len(r59_rows),
        "qualified": [h["hypothesis_id"] for h in qualified],
        "n_qualified": len(qualified),
        "strongest_overall": _brief(ranked[:5]),
        "strongest_machine": _brief(machine[:5]),
        "strongest_economic": _brief(econ[:5]),
        "strongest_non_equity": _brief(non_eq[:5]),
        "by_asset_class": _by_asset_rows(r59_rows),
        "by_generation_method": _by_method(r59_rows),
        "search_burden_after": mem.burden(),
    }


def _brief(rows: list) -> list:
    out = []
    for h in rows:
        st = h.get("statistic") or {}
        ec = h.get("economics") or {}
        out.append({
            "hypothesis_id": h["hypothesis_id"],
            "title": h.get("title"),
            "asset_class": h.get("asset_class"),
            "economic_family": h.get("economic_family"),
            "generation_method": h.get("generation_method"),
            "outcome": h.get("outcome"),
            "lockbox_t": st.get("lockbox_t"),
            "burden_corrected_p": st.get("burden_corrected_p"),
            "lockbox_materiality": ec.get("lockbox_materiality"),
            "validation_materiality": ec.get("validation_materiality"),
            "reason_rejected": h.get("reason_rejected"),
            "spec": h.get("spec"),
        })
    return out


def _by_asset_rows(rows: list) -> dict:
    out: dict = {}
    for h in rows:
        out[h.get("asset_class")] = out.get(h.get("asset_class"), 0) + 1
    return out


def _by_method(rows: list) -> dict:
    out: dict = {}
    for h in rows:
        k = h.get("generation_method")
        out[k] = out.get(k, 0) + 1
    return out
