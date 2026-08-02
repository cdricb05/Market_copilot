"""
alpha_agent.drain_fairness — Stage 9.5 DETERMINISTIC DRAIN FAIRNESS.

The bounded collect drain executes AT MOST ONE queued job per cycle, in three
passes: PASS A (approved Stage-9 tournament-continuation allowlist), PASS B (the
SEC Form 4 / 8-K campaign self-continuation) and PASS C (the SEC companyfacts
campaign self-continuation). PASS B/C run ONLY when the earlier passes claimed
nothing, so at most one job ever executes.

THE DEFECT this module fixes: the Stage 9.4 tournament revalidation loop
registers one computable-feature revalidation into PASS A every cycle, so PASS A
holds a claimable job on every cycle and consumes the single per-cycle slot. The
companyfacts continuation (origin ``campaign-continuation``, claimable ONLY by
PASS C when A and B are idle) can therefore wait an UNBOUNDED number of cycles.
"Run many cycles until the revalidation catalogue is exhausted" is not an
acceptable scheduling guarantee.

THE FIX (deterministic, durable, WITHOUT relaxing any of the hard invariants):

  * ``max_jobs_per_cycle`` stays 1 — exactly one job still executes per cycle.
  * the exact companyfacts origin/lane/category scope is UNCHANGED.
  * Stage 9.4 revalidation stays operational.
  * the campaign's OWN bounded stop conditions (daily completed-batch cap +
    consecutive-no-progress limit, both derived from the durable append-only
    batch log) ALWAYS win over fairness — a stopped/capped campaign is never
    promoted, so PASS A keeps the slot.

Mechanism: a durable per-scheduler-name counter records how many completed drain
cycles have passed since the companyfacts continuation last executed. Once that
idle count reaches a configured bound (``fairness_max_idle_cycles``, default 2),
the NEXT cycle PROMOTES the companyfacts continuation ahead of PASS A — running
PASS C first for that one cycle (still exactly one job) — provided an eligible
continuation is actually queued and the campaign is not stopped. Executing the
continuation resets the counter, so the promotion fires at most once per
``bound + 1`` cycles in steady state: the companyfacts continuation is guaranteed
to run within the bound, and the tournament experiments are never permanently
starved either (they keep ``bound`` of every ``bound + 1`` cycles).

The counter lives in its OWN stdlib sqlite file co-located with the queue and
campaign stores under the Stage 8 autonomy root on the data drive — NEVER under
an operational-ledger root — so it is restart-safe and NEVER changes the
operational-ledger fingerprint. Nothing here touches an operational ledger,
order, fill, signal, trade decision, model promotion, paper book, PostgreSQL, the
prediction service or a Daily Close.
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

SCHEMA_VERSION = "1.0.0"

# Default fairness bound: the companyfacts continuation must execute after no
# more than this many completed collect cycles without companyfacts progress.
DEFAULT_MAX_IDLE_CYCLES = 2

# Canonical fairness-state key for the SEC companyfacts continuation.
COMPANYFACTS_KEY = "companyfacts_continuation"

# The exact companyfacts lane the fairness counter watches. Detection of "the
# continuation executed this cycle" is by this lane prefix on the drain report's
# handled entries, so it is uniform across the bootstrap (PASS A) and the
# self-continuation (PASS C / promoted) jobs.
_COMPANYFACTS_LANE = "acq.sec_companyfacts"

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scheduler_fairness (
    name TEXT PRIMARY KEY,
    idle_cycles INTEGER NOT NULL DEFAULT 0,
    total_executions INTEGER NOT NULL DEFAULT 0,
    total_cycles INTEGER NOT NULL DEFAULT 0,
    last_execution_cursor INTEGER,
    last_execution_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

_FOLD_COUNTERS = ("jobs_claimed", "jobs_completed", "jobs_retryable",
                  "jobs_blocked", "jobs_failed", "jobs_rejected",
                  "handler_errors")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------- #
# Pure fairness rule (unit-testable in isolation).
# --------------------------------------------------------------------------- #
def should_promote_companyfacts(*, idle_cycles: int, max_idle_cycles: int,
                                continuation_queued: bool,
                                campaign_stopped: bool) -> bool:
    """Whether the companyfacts continuation should be PROMOTED ahead of PASS A
    for one cycle. Promote iff it has been idle for at least ``max_idle_cycles``
    completed cycles AND an eligible continuation is actually queued AND the
    campaign's own bounded stop conditions do not apply.

    The stop conditions ALWAYS win: a stopped/capped/complete campaign is never
    promoted (PASS A keeps the slot), so the daily / no-progress campaign limits
    dominate fairness. With ``max_idle_cycles >= 1`` the promotion can fire at
    most once per ``max_idle_cycles + 1`` cycles in steady state, so it can never
    monopolise every cycle."""
    if campaign_stopped:
        return False
    if not continuation_queued:
        return False
    if int(max_idle_cycles) < 1:
        # A bound below 1 would promote every eligible cycle (monopoly); treat as
        # "no promotion" — the caller clamps the configured bound to >= 1.
        return False
    return int(idle_cycles) >= int(max_idle_cycles)


# --------------------------------------------------------------------------- #
# Durable fairness state.
# --------------------------------------------------------------------------- #
class FairnessStore:
    """Durable, restart-safe per-scheduler-name fairness counter. ``clock`` is an
    injectable ``() -> iso`` for deterministic tests. One stdlib sqlite file under
    the Stage 8 autonomy root on the data drive — never an operational-ledger
    file, so it never changes the operational-ledger fingerprint."""

    def __init__(self, db_path, *, clock: Optional[Callable[[], str]] = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or _utc_now_iso
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(_SCHEMA_SQL)
                conn.commit()
            finally:
                conn.close()

    def state(self, name: str) -> dict:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM scheduler_fairness WHERE name=?",
                (name,)).fetchone()
        finally:
            conn.close()
        if row is None:
            return {"name": name, "exists": False, "idle_cycles": 0,
                    "total_executions": 0, "total_cycles": 0,
                    "last_execution_cursor": None, "last_execution_at": None}
        return {"name": row["name"], "exists": True,
                "idle_cycles": int(row["idle_cycles"]),
                "total_executions": int(row["total_executions"]),
                "total_cycles": int(row["total_cycles"]),
                "last_execution_cursor": row["last_execution_cursor"],
                "last_execution_at": row["last_execution_at"]}

    def idle_cycles(self, name: str) -> int:
        return int(self.state(name)["idle_cycles"])

    def is_due(self, name: str, *, max_idle_cycles: int) -> bool:
        return self.idle_cycles(name) >= int(max_idle_cycles)

    def record_cycle(self, name: str, *, executed: bool,
                     cursor: Optional[int] = None,
                     now: Optional[str] = None) -> dict:
        """Record exactly ONE completed drain cycle for ``name``.

        ``executed`` = the companyfacts continuation was claimed and run this
        cycle (in ANY pass): reset the idle counter to 0 and stamp the execution.
        Otherwise increment the idle counter by exactly 1. Durable and atomic per
        call (single transaction); returns the new state."""
        now = now or self._clock()
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT idle_cycles, total_executions, total_cycles "
                    "FROM scheduler_fairness WHERE name=?", (name,)).fetchone()
                if row is None:
                    idle, texe, tcyc = 0, 0, 0
                    conn.execute(
                        "INSERT INTO scheduler_fairness "
                        "(name, idle_cycles, total_executions, total_cycles, "
                        " last_execution_cursor, last_execution_at, created_at, "
                        " updated_at) VALUES (?,?,?,?,?,?,?,?)",
                        (name, 0, 0, 0, None, None, now, now))
                else:
                    idle = int(row["idle_cycles"])
                    texe = int(row["total_executions"])
                    tcyc = int(row["total_cycles"])
                tcyc += 1
                if executed:
                    conn.execute(
                        "UPDATE scheduler_fairness SET idle_cycles=0, "
                        "total_executions=?, total_cycles=?, "
                        "last_execution_cursor=?, last_execution_at=?, "
                        "updated_at=? WHERE name=?",
                        (texe + 1, tcyc,
                         (int(cursor) if cursor is not None else None),
                         now, now, name))
                else:
                    conn.execute(
                        "UPDATE scheduler_fairness SET idle_cycles=?, "
                        "total_cycles=?, updated_at=? WHERE name=?",
                        (idle + 1, tcyc, now, name))
                conn.commit()
            finally:
                conn.close()
        return self.state(name)


# --------------------------------------------------------------------------- #
# Fairness-aware bounded drain (the single source of truth for pass ordering).
# --------------------------------------------------------------------------- #
def _continuation_queued(queue, cf_cont: dict) -> bool:
    """Whether an eligible companyfacts continuation is QUEUED right now — a job
    whose origin AND lane-prefix AND category all match the exact companyfacts
    continuation triple (the same ANDed scope PASS C claims by)."""
    from . import autonomous_research as _ar
    orgs = set(cf_cont.get("allowed_origins") or [])
    lanes = tuple(cf_cont.get("allowed_lane_prefixes") or [])
    cats = set(cf_cont.get("allowed_categories") or [])
    if not (orgs and lanes and cats):
        return False
    for j in queue.list_jobs(state=_ar.STATE_QUEUED, limit=1000):
        if (j.origin in orgs and str(j.lane).startswith(lanes)
                and j.category in cats):
            return True
    return False


def _fold(report: dict, sub: dict) -> None:
    """Fold a later-pass report into the top-level counters, mirroring the drain
    step's existing single-execution accounting."""
    for k in _FOLD_COUNTERS:
        report[k] = int(report.get(k, 0)) + int(sub.get(k, 0))
    report["job_ids"] = list(report.get("job_ids") or []) + \
        list(sub.get("job_ids") or [])
    report["handled"] = list(report.get("handled") or []) + \
        list(sub.get("handled") or [])
    report["queue_depth_after"] = sub.get(
        "queue_depth_after", report.get("queue_depth_after"))


def _drain_companyfacts_pass(queue, handlers, cf_cont, budget_seconds):
    from . import autonomous_research as _ar
    return _ar.drain_jobs(
        queue, handlers, max_jobs=1,
        categories=cf_cont.get("allowed_categories"),
        origins=cf_cont.get("allowed_origins"),
        lane_prefixes=cf_cont.get("allowed_lane_prefixes"),
        budget_seconds=budget_seconds)


def run_fair_drain(queue, handlers, *, drain_cfg: dict, fair_store=None,
                   campaign_stopped: bool = False, cursor_after=None,
                   name: str = COMPANYFACTS_KEY,
                   budget_seconds: Optional[float] = None) -> dict:
    """Execute ONE bounded, fairness-aware drain cycle (AT MOST ONE job) and
    return the drain report augmented with a ``companyfacts_fairness`` block.

    This is the SINGLE source of truth for the drain's pass ordering. Both the
    runtime and the tests call it, so the fairness guarantee is exercised by the
    real code, not a re-implementation.

    Ordering:
      * If the companyfacts continuation is DUE (durable idle count >= bound) AND
        an eligible continuation is queued AND the campaign is not stopped, run
        the companyfacts PASS C FIRST and SKIP PASS A / PASS B this cycle
        (still exactly one job).
      * Otherwise run PASS A, then PASS B iff A claimed nothing, then PASS C iff
        A and B both claimed nothing — the unchanged Stage 9.2/9.3/9.5 order.

    After execution the durable counter is updated: reset to 0 iff the
    companyfacts continuation executed this cycle (any pass), else +1.

    ``campaign_stopped`` and ``cursor_after`` are supplied by the caller because
    they require the campaign store; ``fair_store`` may be ``None`` (fairness off,
    e.g. offline/acceptance configs) in which case the classic order runs and no
    state file is created."""
    from . import autonomous_research as _ar
    allowed_cats = drain_cfg.get("allowed_categories")
    if allowed_cats is None:
        allowed_cats = drain_cfg.get("categories")
    max_jobs = int(drain_cfg.get("max_jobs_per_cycle", 1))
    sec_cont = drain_cfg.get("sec_continuation") or {}
    cf_cont = drain_cfg.get("companyfacts_continuation") or {}
    cf_enabled = bool(cf_cont.get("enabled"))
    max_idle = max(1, int(cf_cont.get("fairness_max_idle_cycles",
                                      DEFAULT_MAX_IDLE_CYCLES)
                          or DEFAULT_MAX_IDLE_CYCLES))
    fairness_on = (cf_enabled and fair_store is not None
                   and bool(cf_cont.get("fairness_enabled", True)))

    continuation_queued = (_continuation_queued(queue, cf_cont)
                           if cf_enabled else False)
    idle_before = fair_store.idle_cycles(name) if fairness_on else 0
    promote = fairness_on and should_promote_companyfacts(
        idle_cycles=idle_before, max_idle_cycles=max_idle,
        continuation_queued=continuation_queued,
        campaign_stopped=bool(campaign_stopped))

    promoted = False
    if promote:
        # PROMOTED PASS C — run the companyfacts continuation ahead of PASS A for
        # this one cycle. Still max_jobs=1; PASS A / PASS B are skipped so exactly
        # one job executes.
        report = _drain_companyfacts_pass(queue, handlers, cf_cont,
                                          budget_seconds)
        report["sec_continuation_enabled"] = bool(sec_cont.get("enabled"))
        report["companyfacts_continuation_enabled"] = True
        report["companyfacts_continuation_pass"] = dict(report)
        promoted = True
    else:
        # PASS A — unchanged approved Stage-9 tournament-continuation allowlist.
        report = _ar.drain_jobs(
            queue, handlers, max_jobs=max_jobs, categories=allowed_cats,
            origins=drain_cfg.get("allowed_origins"),
            lane_prefixes=drain_cfg.get("allowed_lane_prefixes"),
            budget_seconds=budget_seconds)
        # PASS B — SEC Form 4 / 8-K continuation; only when A claimed nothing.
        report["sec_continuation_enabled"] = bool(sec_cont.get("enabled"))
        if sec_cont.get("enabled") and report.get("jobs_claimed", 0) == 0:
            reportB = _ar.drain_jobs(
                queue, handlers, max_jobs=1,
                categories=sec_cont.get("allowed_categories"),
                origins=sec_cont.get("allowed_origins"),
                lane_prefixes=sec_cont.get("allowed_lane_prefixes"),
                budget_seconds=budget_seconds)
            _fold(report, reportB)
            report["sec_continuation_pass"] = reportB
        # PASS C — SEC companyfacts continuation; only when A and B both idle.
        report["companyfacts_continuation_enabled"] = cf_enabled
        if cf_enabled and report.get("jobs_claimed", 0) == 0:
            reportC = _drain_companyfacts_pass(queue, handlers, cf_cont,
                                               budget_seconds)
            _fold(report, reportC)
            report["companyfacts_continuation_pass"] = reportC

    report["companyfacts_fairness_promoted"] = promoted

    if fairness_on:
        cf_exec = any(
            str(h.get("lane", "")).startswith(_COMPANYFACTS_LANE)
            for h in (report.get("handled") or []))
        cur: Optional[int] = None
        if callable(cursor_after):
            try:
                cur = cursor_after()
            except Exception:  # noqa: BLE001 - bookkeeping only, never fatal
                cur = None
        elif cursor_after is not None:
            cur = cursor_after
        st = fair_store.record_cycle(name, executed=cf_exec, cursor=cur)
        report["companyfacts_fairness"] = {
            "enabled": True, "promoted": promoted, "executed": bool(cf_exec),
            "idle_cycles_before": int(idle_before),
            "idle_cycles_after": int(st["idle_cycles"]),
            "max_idle_cycles": int(max_idle),
            "continuation_queued": bool(continuation_queued),
            "campaign_stopped": bool(campaign_stopped),
            "cursor_after": cur,
            "total_executions": int(st["total_executions"])}
    else:
        report["companyfacts_fairness"] = {"enabled": False}
    return report
