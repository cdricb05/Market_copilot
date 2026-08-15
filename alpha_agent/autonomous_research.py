"""
alpha_agent.autonomous_research — Stage 8 durable, never-idle research queue.

A crash-safe, resumable, idempotent work queue that keeps the Alpha Agent
research loop *never idle*: when it is about to run dry it replenishes itself
from the outstanding accessible-source coverage, failed mappings, unexplored
feature families and refinement backlog, so at least one useful next action
always exists. One blocked lane never stops the others.

Design (deliberately narrow + safe):

  * Persistence is a single stdlib ``sqlite3`` database under the EXISTING
    research-state root on ``D:`` (``<stage8_root>/autonomy.sqlite``). NO new
    PostgreSQL dependency; NO write to the operational trading ledgers; NO
    network, LLM, subprocess or prediction call happens in this module. The
    handlers that DO real work are INJECTED, so this file is pure orchestration
    and is fully unit-testable with fakes.
  * The queue is the single source of truth for outstanding research work and
    survives process restart, Windows reboot, provider outage, partial
    experiment failure and Telegram-worker restart (WAL + bounded busy-timeout;
    every transition is one committed transaction).
  * "Never stop" is a scheduling policy, not a busy-loop: cycles are bounded
    (``max_jobs``), respect per-lane backoff, and the process may sleep between
    scheduled ticks. The invariant is only that the durable queue is never left
    empty while useful work remains possible.

Job categories (Stage 8 contract): SOURCE_DISCOVERY, ENTITLEMENT_PROBE,
DATA_ACQUISITION, COVERAGE_REPAIR, DATA_VALIDATION, HYPOTHESIS_GENERATION,
EXPERIMENT, ROBUSTNESS_TEST, SIGNAL_COMBINATION, PROSPECTIVE_SNAPSHOT, REPORT,
TELEGRAM_REQUEST.

Job states (Stage 8 contract): QUEUED, RUNNING, RETRYABLE, BLOCKED_SPECIFIC,
COMPLETED, REJECTED, FAILED_PERMANENT. A ``BLOCKED_SPECIFIC`` job is skipped by
the scheduler but never blocks an unrelated job.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

STAGE = "8"
SCHEMA_VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# Job categories.
# --------------------------------------------------------------------------- #
CAT_SOURCE_DISCOVERY = "SOURCE_DISCOVERY"
CAT_ENTITLEMENT_PROBE = "ENTITLEMENT_PROBE"
CAT_DATA_ACQUISITION = "DATA_ACQUISITION"
CAT_COVERAGE_REPAIR = "COVERAGE_REPAIR"
CAT_DATA_VALIDATION = "DATA_VALIDATION"
CAT_HYPOTHESIS_GENERATION = "HYPOTHESIS_GENERATION"
CAT_EXPERIMENT = "EXPERIMENT"
CAT_ROBUSTNESS_TEST = "ROBUSTNESS_TEST"
CAT_SIGNAL_COMBINATION = "SIGNAL_COMBINATION"
CAT_PROSPECTIVE_SNAPSHOT = "PROSPECTIVE_SNAPSHOT"
CAT_REPORT = "REPORT"
CAT_TELEGRAM_REQUEST = "TELEGRAM_REQUEST"

JOB_CATEGORIES = (
    CAT_SOURCE_DISCOVERY, CAT_ENTITLEMENT_PROBE, CAT_DATA_ACQUISITION,
    CAT_COVERAGE_REPAIR, CAT_DATA_VALIDATION, CAT_HYPOTHESIS_GENERATION,
    CAT_EXPERIMENT, CAT_ROBUSTNESS_TEST, CAT_SIGNAL_COMBINATION,
    CAT_PROSPECTIVE_SNAPSHOT, CAT_REPORT, CAT_TELEGRAM_REQUEST,
)

# --------------------------------------------------------------------------- #
# Job states.
# --------------------------------------------------------------------------- #
STATE_QUEUED = "QUEUED"
STATE_RUNNING = "RUNNING"
STATE_RETRYABLE = "RETRYABLE"
STATE_BLOCKED_SPECIFIC = "BLOCKED_SPECIFIC"
STATE_COMPLETED = "COMPLETED"
STATE_REJECTED = "REJECTED"
STATE_FAILED_PERMANENT = "FAILED_PERMANENT"

JOB_STATES = (
    STATE_QUEUED, STATE_RUNNING, STATE_RETRYABLE, STATE_BLOCKED_SPECIFIC,
    STATE_COMPLETED, STATE_REJECTED, STATE_FAILED_PERMANENT,
)

# Non-terminal states hold outstanding work; terminal states are settled.
NON_TERMINAL_STATES = frozenset({
    STATE_QUEUED, STATE_RUNNING, STATE_RETRYABLE, STATE_BLOCKED_SPECIFIC})
TERMINAL_STATES = frozenset({
    STATE_COMPLETED, STATE_REJECTED, STATE_FAILED_PERMANENT})
# States a scheduler is willing to claim and run right now (RETRYABLE only once
# its backoff has elapsed; BLOCKED_SPECIFIC is deliberately excluded).
CLAIMABLE_STATES = frozenset({STATE_QUEUED, STATE_RETRYABLE})

# Handler outcome tokens (a handler returns one of these + optional detail).
OUTCOME_COMPLETED = STATE_COMPLETED
OUTCOME_REJECTED = STATE_REJECTED
OUTCOME_RETRYABLE = STATE_RETRYABLE
OUTCOME_BLOCKED_SPECIFIC = STATE_BLOCKED_SPECIFIC
OUTCOME_FAILED_PERMANENT = STATE_FAILED_PERMANENT
HANDLER_OUTCOMES = frozenset({
    OUTCOME_COMPLETED, OUTCOME_REJECTED, OUTCOME_RETRYABLE,
    OUTCOME_BLOCKED_SPECIFIC, OUTCOME_FAILED_PERMANENT})

_DEFAULT_MAX_ATTEMPTS = 3
_DEFAULT_RETRY_BACKOFF_SECONDS = 300
_DEFAULT_STALE_SECONDS = 1800
_DEFAULT_QUEUE_FLOOR = 1

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS autonomy_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    dedupe_key TEXT NOT NULL,
    category TEXT NOT NULL,
    lane TEXT NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT,
    blocked_reason TEXT,
    origin TEXT NOT NULL DEFAULT 'seed',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    not_before TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
-- Idempotency: at most one LIVE (non-terminal) job per dedupe_key.
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_live_dedupe
    ON jobs (dedupe_key) WHERE state IN
        ('QUEUED','RUNNING','RETRYABLE','BLOCKED_SPECIFIC');
CREATE INDEX IF NOT EXISTS ix_jobs_state ON jobs (state);
CREATE INDEX IF NOT EXISTS ix_jobs_category ON jobs (category);
CREATE INDEX IF NOT EXISTS ix_jobs_lane ON jobs (lane);

CREATE TABLE IF NOT EXISTS autonomy_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    kind TEXT NOT NULL,
    job_id TEXT,
    detail_json TEXT
);
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _digest(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def make_dedupe_key(category: str, lane: str, payload: Optional[dict]) -> str:
    """Deterministic identity for a unit of work. Two enqueue calls with the
    same (category, lane, payload) collapse to one live job (idempotent)."""
    return "dk_" + _digest(category, lane, canonical_json(payload or {}))[:24]


def make_job_id(dedupe_key: str, created_at: str) -> str:
    return "job_" + _digest(dedupe_key, created_at)[:20]


# One ``created_at`` second can legitimately hold MANY jobs for the same
# dedupe_key: the live-dedupe index below only covers non-terminal states, so a
# SETTLED key is re-enqueueable, and the never-idle floor re-adds its constant
# specs every time the queue drains. Identity therefore needs a deterministic
# collision sequence; 0 is the primary id and 1 is the legacy fallback id, both
# left byte-identical so existing stores reproduce exactly.
_JOB_ID_SEQUENCE_START = 2
# A ceiling on CONSECUTIVE probes, not on re-adds: the search is seeded from the
# rows already stored for the (dedupe_key, second), so it normally succeeds on
# the first probe however many times that key has already been re-added.
_MAX_JOB_ID_PROBES = 10_000


def make_fallback_job_id(dedupe_key: str, created_at: str,
                         priority: int) -> str:
    """Second identity for one ``(dedupe_key, created_at)`` pair - unchanged
    since the queue shipped, so historical rows stay reproducible."""
    return "job_" + _digest(dedupe_key, created_at, str(priority))[:20]


def make_sequenced_job_id(dedupe_key: str, created_at: str, priority: int,
                          sequence: int) -> str:
    """Third and later identity for one ``(dedupe_key, created_at)`` pair.

    Deterministic by construction: no randomness, no UUID, no clock sleep. The
    same database state always yields the same id, so the audit trail stays
    reproducible."""
    return "job_" + _digest(dedupe_key, created_at, str(priority),
                            "seq=%d" % int(sequence))[:20]


@dataclass
class Job:
    job_id: str
    dedupe_key: str
    category: str
    lane: str
    state: str
    priority: int = 0
    payload: dict = field(default_factory=dict)
    result: Optional[dict] = None
    blocked_reason: Optional[str] = None
    origin: str = "seed"
    attempts: int = 0
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS
    not_before: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        return cls(
            job_id=row["job_id"], dedupe_key=row["dedupe_key"],
            category=row["category"], lane=row["lane"], state=row["state"],
            priority=int(row["priority"]),
            payload=json.loads(row["payload_json"] or "{}"),
            result=(json.loads(row["result_json"]) if row["result_json"]
                    else None),
            blocked_reason=row["blocked_reason"], origin=row["origin"],
            attempts=int(row["attempts"]), max_attempts=int(row["max_attempts"]),
            not_before=row["not_before"], created_at=row["created_at"],
            updated_at=row["updated_at"], started_at=row["started_at"],
            finished_at=row["finished_at"])

    def as_dict(self) -> dict:
        return {
            "job_id": self.job_id, "dedupe_key": self.dedupe_key,
            "category": self.category, "lane": self.lane, "state": self.state,
            "priority": self.priority, "payload": self.payload,
            "result": self.result, "blocked_reason": self.blocked_reason,
            "origin": self.origin, "attempts": self.attempts,
            "max_attempts": self.max_attempts, "not_before": self.not_before,
            "created_at": self.created_at, "updated_at": self.updated_at,
            "started_at": self.started_at, "finished_at": self.finished_at,
        }


class ResearchQueue:
    """Durable, resumable, idempotent Stage 8 research work queue.

    ``clock`` is an injectable ``() -> iso-string`` so tests are deterministic;
    production uses UTC wall-clock. The queue never raises on a missing job and
    never deletes a settled row (full audit trail)."""

    def __init__(self, db_path: str | Path, *,
                 clock: Optional[Callable[[], str]] = None,
                 max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
                 retry_backoff_seconds: int = _DEFAULT_RETRY_BACKOFF_SECONDS,
                 stale_seconds: int = _DEFAULT_STALE_SECONDS,
                 queue_floor: int = _DEFAULT_QUEUE_FLOOR):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or _utc_now_iso
        self.max_attempts = int(max_attempts)
        self.retry_backoff_seconds = int(retry_backoff_seconds)
        self.stale_seconds = int(stale_seconds)
        self.queue_floor = int(queue_floor)
        self._lock = threading.Lock()
        self._init_schema()

    # -- low-level ---------------------------------------------------------- #
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA_SQL)
            conn.execute(
                "INSERT OR IGNORE INTO autonomy_meta(key,value) VALUES(?,?)",
                ("schema_version", SCHEMA_VERSION))
        finally:
            conn.close()

    def _event(self, conn: sqlite3.Connection, kind: str, job_id: Optional[str],
               detail: Optional[dict]) -> None:
        conn.execute(
            "INSERT INTO autonomy_events(recorded_at,kind,job_id,detail_json)"
            " VALUES(?,?,?,?)",
            (self._clock(), kind, job_id,
             canonical_json(detail) if detail is not None else None))

    # -- job identity allocation -------------------------------------------- #
    @staticmethod
    def _job_id_taken(conn: sqlite3.Connection, job_id: str) -> bool:
        return conn.execute("SELECT 1 FROM jobs WHERE job_id=?",
                            (job_id,)).fetchone() is not None

    def _allocate_job_id(self, conn: sqlite3.Connection, dedupe_key: str,
                         created_at: str, priority: int) -> str:
        """Return an unused ``jobs.job_id`` for one same-second (re-)add.

        Ordered so historical identities never move:

          1. ``make_job_id(dk, now)``            - the original primary id;
          2. ``make_fallback_job_id(...)``       - the original fallback id;
          3. ``make_sequenced_job_id(..., seq)`` - deterministic extension.

        Steps 1-2 are byte-identical to what this queue has always written.
        Step 3 exists because ``created_at`` has one-second resolution while the
        live-dedupe index deliberately permits a SETTLED dedupe_key to be
        re-enqueued: with only two identities the THIRD same-second re-add hit
        the ``jobs.job_id`` primary key and raised ``sqlite3.IntegrityError``
        out of enqueue -> replenish -> ensure_never_idle -> run_cycle, crashing
        the autonomy cycle. The collision is prevented, never swallowed.

        Runs on the caller's connection inside the caller's ``self._lock``, so
        it opens no connection, starts no transaction and takes no extra lock.
        """
        job_id = make_job_id(dedupe_key, created_at)
        if not self._job_id_taken(conn, job_id):
            return job_id
        job_id = make_fallback_job_id(dedupe_key, created_at, priority)
        if not self._job_id_taken(conn, job_id):
            return job_id
        # Seed the sequence from the rows already stored for this
        # (dedupe_key, second) so the search is O(1) in practice, then still
        # probe: the two identities above can have been consumed under
        # different ``priority`` values, which leaves gaps in the sequence.
        used = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE dedupe_key=? AND created_at=?",
            (dedupe_key, created_at)).fetchone()["n"]
        seq = max(_JOB_ID_SEQUENCE_START, int(used))
        for _ in range(_MAX_JOB_ID_PROBES):
            job_id = make_sequenced_job_id(dedupe_key, created_at, priority, seq)
            if not self._job_id_taken(conn, job_id):
                return job_id
            seq += 1
        raise RuntimeError(
            "exhausted job_id collision probes for dedupe_key=%s at %s"
            % (dedupe_key, created_at))

    # -- enqueue (idempotent) ---------------------------------------------- #
    def enqueue(self, category: str, *, lane: str, payload: Optional[dict] = None,
                priority: int = 0, max_attempts: Optional[int] = None,
                dedupe_key: Optional[str] = None, origin: str = "seed",
                not_before: Optional[str] = None) -> str:
        """Insert a job unless an identical LIVE job already exists (idempotent).

        Returns the job_id of the live job (existing or new). Duplicate
        Telegram updates / duplicate seed passes therefore never create
        duplicate work."""
        if category not in JOB_CATEGORIES:
            raise ValueError("unknown job category: %s" % category)
        payload = payload or {}
        dk = dedupe_key or make_dedupe_key(category, lane, payload)
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                existing = conn.execute(
                    "SELECT job_id FROM jobs WHERE dedupe_key=? AND state IN"
                    " ('QUEUED','RUNNING','RETRYABLE','BLOCKED_SPECIFIC')"
                    " LIMIT 1", (dk,)).fetchone()
                if existing:
                    return existing["job_id"]
                # Not live: this is legitimate new work for a SETTLED key, so
                # allocate an identity that cannot collide on the primary key.
                job_id = self._allocate_job_id(conn, dk, now, int(priority))
                conn.execute(
                    "INSERT INTO jobs(job_id,dedupe_key,category,lane,state,"
                    "priority,payload_json,origin,attempts,max_attempts,"
                    "not_before,created_at,updated_at) VALUES"
                    "(?,?,?,?,?,?,?,?,0,?,?,?,?)",
                    (job_id, dk, category, lane, STATE_QUEUED, int(priority),
                     canonical_json(payload), origin,
                     int(max_attempts if max_attempts is not None
                         else self.max_attempts),
                     not_before, now, now))
                self._event(conn, "ENQUEUE", job_id,
                            {"category": category, "lane": lane,
                             "origin": origin})
                return job_id
            finally:
                conn.close()

    # -- claim (atomic) ----------------------------------------------------- #
    def claim_next(self, *, categories: Optional[Iterable[str]] = None,
                   origins: Optional[Iterable[str]] = None,
                   lane_prefixes: Optional[Iterable[str]] = None,
                   now: Optional[str] = None) -> Optional[Job]:
        """Atomically claim the highest-priority claimable job whose backoff has
        elapsed, transitioning it QUEUED/RETRYABLE -> RUNNING and incrementing
        ``attempts`` by EXACTLY ONE in the same committed transaction.

        ``attempts`` is therefore the single, auditable count of real
        executions: a job that is never claimed keeps ``attempts==0``; every
        COMPLETED / BLOCKED_SPECIFIC / RETRYABLE / FAILED_PERMANENT job that
        actually ran shows ``attempts>=1``. Because the increment is atomic with
        the QUEUED/RETRYABLE->RUNNING transition under ``BEGIN IMMEDIATE``, two
        overlapping workers can never claim - or increment - the same job twice.

        An optional allowlist restricts *which* jobs are eligible: by
        ``categories``, by ``origins`` and/or by ``lane_prefixes`` (prefix match
        via GLOB, so ``_`` is a literal not a wildcard). The three dimensions are
        ANDed, and an EXPLICITLY EMPTY collection on any dimension matches
        nothing (an empty allowlist claims nothing). Ineligible jobs are never
        transitioned, so unrelated queue work is left completely untouched.
        Returns None when nothing eligible is runnable right now (which does NOT
        mean the queue is empty — see ``depth``)."""
        now = now or self._clock()
        cats = None if categories is None else tuple(categories)
        orgs = None if origins is None else tuple(origins)
        lpfx = None if lane_prefixes is None else tuple(lane_prefixes)
        # An explicitly-empty allowlist dimension matches nothing (so an empty
        # allowlist executes nothing) - claim and increment neither.
        if (cats is not None and not cats) or (orgs is not None and not orgs) \
                or (lpfx is not None and not lpfx):
            return None
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                sql = ("SELECT * FROM jobs WHERE state IN ('QUEUED','RETRYABLE')"
                       " AND (not_before IS NULL OR not_before<=?)")
                params: list[Any] = [now]
                if cats:
                    sql += " AND category IN (%s)" % ",".join("?" * len(cats))
                    params.extend(cats)
                if orgs:
                    sql += " AND origin IN (%s)" % ",".join("?" * len(orgs))
                    params.extend(orgs)
                if lpfx:
                    sql += " AND (%s)" % " OR ".join(["lane GLOB ?"] * len(lpfx))
                    params.extend(p + "*" for p in lpfx)
                sql += " ORDER BY priority DESC, created_at ASC, rowid ASC LIMIT 1"
                row = conn.execute(sql, params).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None
                new_attempts = int(row["attempts"]) + 1
                cur = conn.execute(
                    "UPDATE jobs SET state=?, attempts=?, started_at=?,"
                    " updated_at=? WHERE job_id=? AND state IN"
                    " ('QUEUED','RETRYABLE')",
                    (STATE_RUNNING, new_attempts, now, now, row["job_id"]))
                if cur.rowcount != 1:
                    # Lost the race for this row under an overlapping writer
                    # (should not occur inside BEGIN IMMEDIATE): claim nothing
                    # and, critically, increment nothing.
                    conn.execute("ROLLBACK")
                    return None
                self._event(conn, "CLAIM", row["job_id"],
                            {"attempts": new_attempts})
                conn.execute("COMMIT")
                fresh = conn.execute("SELECT * FROM jobs WHERE job_id=?",
                                     (row["job_id"],)).fetchone()
                return Job.from_row(fresh)
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    # -- transitions -------------------------------------------------------- #
    def _settle(self, job_id: str, state: str, *, result: Optional[dict],
                blocked_reason: Optional[str]) -> None:
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE jobs SET state=?, result_json=?, blocked_reason=?,"
                    " updated_at=?, finished_at=? WHERE job_id=?",
                    (state, canonical_json(result) if result is not None
                     else None, blocked_reason, now, now, job_id))
                self._event(conn, state, job_id,
                            {"reason": blocked_reason} if blocked_reason
                            else None)
            finally:
                conn.close()

    def complete(self, job_id: str, *, result: Optional[dict] = None) -> None:
        self._settle(job_id, STATE_COMPLETED, result=result,
                     blocked_reason=None)

    def reject(self, job_id: str, reason: str, *,
               result: Optional[dict] = None) -> None:
        self._settle(job_id, STATE_REJECTED, result=result,
                     blocked_reason=reason)

    def fail_permanent(self, job_id: str, reason: str) -> None:
        self._settle(job_id, STATE_FAILED_PERMANENT, result=None,
                     blocked_reason=reason)

    def block_specific(self, job_id: str, reason: str) -> None:
        """Mark ONE job blocked on a specific missing input. It is skipped by
        the scheduler but never blocks an unrelated job or lane."""
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE jobs SET state=?, blocked_reason=?, updated_at=?"
                    " WHERE job_id=?",
                    (STATE_BLOCKED_SPECIFIC, reason, now, job_id))
                self._event(conn, STATE_BLOCKED_SPECIFIC, job_id,
                            {"reason": reason})
            finally:
                conn.close()

    def mark_retryable(self, job_id: str, reason: str, *,
                       backoff_seconds: Optional[int] = None) -> str:
        """Schedule a bounded retry, or escalate to FAILED_PERMANENT once the
        execution budget is spent. ``attempts`` is NOT incremented here: it is
        incremented exactly once per execution at CLAIM time (so applying an
        outcome can never double-count an attempt). This reads the
        already-incremented count and, when it has reached ``max_attempts``,
        settles the job FAILED_PERMANENT; otherwise it schedules a backoff so a
        later claim consumes the next attempt. Returns the resulting state."""
        backoff = self.retry_backoff_seconds if backoff_seconds is None \
            else int(backoff_seconds)
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute("SELECT attempts,max_attempts FROM jobs"
                                   " WHERE job_id=?", (job_id,)).fetchone()
                if row is None:
                    return STATE_FAILED_PERMANENT
                attempts = int(row["attempts"])  # already counted at claim
                if attempts >= int(row["max_attempts"]):
                    conn.execute(
                        "UPDATE jobs SET state=?, blocked_reason=?,"
                        " updated_at=?, finished_at=? WHERE job_id=?",
                        (STATE_FAILED_PERMANENT,
                         "retry budget exhausted: %s" % reason, now, now,
                         job_id))
                    self._event(conn, STATE_FAILED_PERMANENT, job_id,
                                {"reason": reason, "attempts": attempts})
                    return STATE_FAILED_PERMANENT
                not_before = _iso_plus_seconds(now, backoff)
                conn.execute(
                    "UPDATE jobs SET state=?, blocked_reason=?,"
                    " not_before=?, updated_at=? WHERE job_id=?",
                    (STATE_RETRYABLE, reason, not_before, now,
                     job_id))
                self._event(conn, STATE_RETRYABLE, job_id,
                            {"reason": reason, "attempts": attempts,
                             "not_before": not_before})
                return STATE_RETRYABLE
            finally:
                conn.close()

    def apply_outcome(self, job_id: str, outcome: str, *,
                      result: Optional[dict] = None,
                      reason: str = "") -> str:
        """Route a handler outcome token to the matching transition. Returns the
        resulting durable state."""
        if outcome == OUTCOME_COMPLETED:
            self.complete(job_id, result=result)
            return STATE_COMPLETED
        if outcome == OUTCOME_REJECTED:
            self.reject(job_id, reason or "rejected", result=result)
            return STATE_REJECTED
        if outcome == OUTCOME_BLOCKED_SPECIFIC:
            self.block_specific(job_id, reason or "blocked on specific input")
            return STATE_BLOCKED_SPECIFIC
        if outcome == OUTCOME_FAILED_PERMANENT:
            self.fail_permanent(job_id, reason or "failed permanently")
            return STATE_FAILED_PERMANENT
        # default / OUTCOME_RETRYABLE
        return self.mark_retryable(job_id, reason or "retryable failure")

    # -- introspection ------------------------------------------------------ #
    def get(self, job_id: str) -> Optional[Job]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM jobs WHERE job_id=?",
                               (job_id,)).fetchone()
            return Job.from_row(row) if row else None
        finally:
            conn.close()

    def depth(self) -> int:
        """Number of outstanding (non-terminal) jobs. The never-idle invariant
        keeps this >= 1 while useful work remains possible."""
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE state IN"
                " ('QUEUED','RUNNING','RETRYABLE','BLOCKED_SPECIFIC')"
            ).fetchone()[0])
        finally:
            conn.close()

    def runnable_depth(self, *, now: Optional[str] = None) -> int:
        """Jobs the scheduler could run right now (QUEUED, or RETRYABLE past its
        backoff). Excludes RUNNING and BLOCKED_SPECIFIC."""
        now = now or self._clock()
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE state IN ('QUEUED','RETRYABLE')"
                " AND (not_before IS NULL OR not_before<=?)", (now,)
            ).fetchone()[0])
        finally:
            conn.close()

    def counts_by_state(self) -> dict:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT state, COUNT(*) c FROM jobs GROUP BY state").fetchall()
            out = {s: 0 for s in JOB_STATES}
            for r in rows:
                out[r["state"]] = int(r["c"])
            return out
        finally:
            conn.close()

    def counts_by_category(self, *, non_terminal_only: bool = True) -> dict:
        conn = self._connect()
        try:
            sql = "SELECT category, COUNT(*) c FROM jobs"
            if non_terminal_only:
                sql += (" WHERE state IN ('QUEUED','RUNNING','RETRYABLE',"
                        "'BLOCKED_SPECIFIC')")
            sql += " GROUP BY category"
            rows = conn.execute(sql).fetchall()
            return {r["category"]: int(r["c"]) for r in rows}
        finally:
            conn.close()

    def list_jobs(self, *, state: Optional[str] = None,
                  category: Optional[str] = None, limit: int = 100) -> list:
        conn = self._connect()
        try:
            sql = "SELECT * FROM jobs"
            clauses, params = [], []
            if state:
                clauses.append("state=?")
                params.append(state)
            if category:
                clauses.append("category=?")
                params.append(category)
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY priority DESC, created_at ASC LIMIT ?"
            params.append(int(limit))
            return [Job.from_row(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def blocked_jobs(self, *, limit: int = 100) -> list:
        return self.list_jobs(state=STATE_BLOCKED_SPECIFIC, limit=limit)

    def stale_running(self, *, now: Optional[str] = None,
                      stale_seconds: Optional[int] = None) -> list:
        """RUNNING jobs whose ``started_at`` is older than the stale threshold —
        a proxy for a dead worker / crashed process."""
        now = now or self._clock()
        thresh = self.stale_seconds if stale_seconds is None else stale_seconds
        cutoff = _iso_plus_seconds(now, -int(thresh))
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE state='RUNNING' AND started_at IS NOT"
                " NULL AND started_at<?", (cutoff,)).fetchall()
            return [Job.from_row(r) for r in rows]
        finally:
            conn.close()

    def last_progress_at(self) -> Optional[str]:
        """Timestamp of the most recent settled transition — the watchdog's
        'is the agent actually advancing?' signal."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT MAX(recorded_at) m FROM autonomy_events WHERE kind IN"
                " ('COMPLETED','REJECTED','FAILED_PERMANENT','BLOCKED_SPECIFIC')"
            ).fetchone()
            return row["m"] if row and row["m"] else None
        finally:
            conn.close()

    def requeue_stale(self, *, now: Optional[str] = None,
                      stale_seconds: Optional[int] = None) -> int:
        """Safely return stale RUNNING jobs to QUEUED (bounded by max_attempts)
        so a dead worker never strands work. Returns the count requeued."""
        now = now or self._clock()
        stale = self.stale_running(now=now, stale_seconds=stale_seconds)
        n = 0
        for job in stale:
            with self._lock:
                conn = self._connect()
                try:
                    # PRESERVE attempts: the crashed execution already counted
                    # its attempt at CLAIM time. Requeueing neither erases nor
                    # re-increments it (a later re-claim consumes the next
                    # attempt), so a dead worker never inflates or loses the
                    # audit count.
                    attempts = job.attempts
                    if attempts >= job.max_attempts:
                        conn.execute(
                            "UPDATE jobs SET state=?, attempts=?,"
                            " blocked_reason=?, updated_at=?, finished_at=?"
                            " WHERE job_id=? AND state='RUNNING'",
                            (STATE_FAILED_PERMANENT, attempts,
                             "stale worker; retry budget exhausted", now, now,
                             job.job_id))
                        self._event(conn, "STALE_FAILED", job.job_id, None)
                    else:
                        conn.execute(
                            "UPDATE jobs SET state=?, attempts=?, started_at=?,"
                            " blocked_reason=?, updated_at=? WHERE job_id=? AND"
                            " state='RUNNING'",
                            (STATE_QUEUED, attempts, None,
                             "requeued after stale RUNNING", now, job.job_id))
                        self._event(conn, "STALE_REQUEUE", job.job_id, None)
                    if conn.total_changes:
                        n += 1
                finally:
                    conn.close()
        return n


def _iso_plus_seconds(iso: str, seconds: int) -> str:
    """Add ``seconds`` (may be negative) to an ISO-8601 timestamp string."""
    try:
        base = datetime.fromisoformat(iso)
    except ValueError:
        base = datetime.now(timezone.utc)
    from datetime import timedelta
    return (base + timedelta(seconds=int(seconds))).replace(
        microsecond=0).isoformat()


# --------------------------------------------------------------------------- #
# Never-idle replenishment.
# --------------------------------------------------------------------------- #
# A "planner" is a callable returning a list of seed-job specs. Each spec is a
# dict: {"category","lane","payload"?,"priority"?,"origin"?}. The default
# planner is injected by the runtime (it consults the source registry); this
# module ships a minimal, dependency-free fallback so the queue is NEVER left
# empty even if no richer planner is wired.
SeedSpec = dict
Planner = Callable[["ResearchQueue"], "list[SeedSpec]"]


def minimal_fallback_planner(queue: "ResearchQueue") -> "list[SeedSpec]":
    """A dependency-free planner of last resort. It always yields at least one
    useful, safe next action (re-probe entitlements + re-scan sources) so the
    queue can never become terminally empty. Real planners (source registry,
    experiment refinements) are injected on top of this."""
    return [
        {"category": CAT_SOURCE_DISCOVERY, "lane": "registry.rescan",
         "payload": {"reason": "never-idle floor"}, "origin": "replenish"},
        {"category": CAT_ENTITLEMENT_PROBE, "lane": "registry.entitlements",
         "payload": {"reason": "never-idle floor"}, "origin": "replenish"},
    ]


def replenish(queue: "ResearchQueue", *, planner: Optional[Planner] = None,
              floor: Optional[int] = None) -> int:
    """If the outstanding queue depth is below ``floor``, enqueue fresh work
    from ``planner`` (idempotent enqueue means already-live specs are no-ops).
    Returns the number of NEW jobs added. Guarantees the queue is never left
    below the floor while the planner can propose work."""
    floor = queue.queue_floor if floor is None else int(floor)
    if queue.depth() >= floor:
        return 0
    planner = planner or minimal_fallback_planner
    specs = list(planner(queue) or [])
    if not specs:
        # Last-resort guarantee: never leave the queue empty.
        specs = minimal_fallback_planner(queue)
    added = 0
    live_before = set()
    for spec in specs:
        cat = spec.get("category")
        lane = spec.get("lane") or "unspecified"
        payload = spec.get("payload") or {}
        dk = make_dedupe_key(cat, lane, payload)
        if dk in live_before:
            continue
        live_before.add(dk)
        before = queue.get(make_job_id(dk, "")) is not None
        job_id = queue.enqueue(cat, lane=lane, payload=payload,
                               priority=int(spec.get("priority", 0)),
                               dedupe_key=dk,
                               origin=spec.get("origin", "replenish"))
        job = queue.get(job_id)
        # Count only genuinely-new QUEUED jobs (idempotent re-adds are no-ops).
        if job and job.origin == spec.get("origin", "replenish") and not before:
            added += 1
    return added


def ensure_never_idle(queue: "ResearchQueue", *,
                      planner: Optional[Planner] = None,
                      floor: Optional[int] = None) -> dict:
    """Enforce the never-idle invariant: after this call the queue holds at
    least ``floor`` outstanding jobs whenever any useful work is possible.
    Returns a small report."""
    floor = queue.queue_floor if floor is None else int(floor)
    added = replenish(queue, planner=planner, floor=floor)
    depth = queue.depth()
    return {"floor": floor, "added": added, "depth": depth,
            "never_idle": depth >= min(floor, 1)}


# --------------------------------------------------------------------------- #
# Bounded cycle runner.
# --------------------------------------------------------------------------- #
# Handlers map a category -> Callable[[Job], (outcome, detail_dict)]. Handlers
# do the real (read-only / enqueue-only) work and are INJECTED so this module
# stays pure. A handler that raises is treated as a bounded RETRYABLE failure
# (never crashes the loop, never stops unrelated lanes).
Handler = Callable[[Job], "tuple[str, dict]"]
HandlerMap = "dict[str, Handler]"


def run_cycle(queue: "ResearchQueue", handlers: HandlerMap, *,
              planner: Optional[Planner] = None, max_jobs: int = 8,
              floor: Optional[int] = None,
              now: Optional[str] = None) -> dict:
    """Run one bounded, never-idle autonomy cycle:

      1. Requeue stale RUNNING jobs (dead-worker recovery).
      2. Ensure the queue is not empty (never-idle floor).
      3. Drain up to ``max_jobs`` claimable jobs, dispatching each to its
         handler and applying the durable outcome. One blocked/failed job never
         stops the rest.
      4. Ensure never-idle AGAIN afterward — a sent report or a drained queue is
         never treated as terminal 'research complete'.

    Returns a deterministic per-cycle summary. Never raises for a handler error.
    """
    now = now or queue._clock()  # noqa: SLF001 - deliberate shared clock
    summary = {
        "requeued_stale": queue.requeue_stale(now=now),
        "replenished_before": ensure_never_idle(queue, planner=planner,
                                                floor=floor)["added"],
        "processed": 0, "outcomes": {}, "handled": [],
        "missing_handler": [], "handler_errors": 0,
    }
    for _ in range(max(0, int(max_jobs))):
        job = queue.claim_next(now=now)
        if job is None:
            break
        handler = handlers.get(job.category)
        if handler is None:
            # No handler for this category yet: park it specifically (does not
            # block other lanes) instead of failing the whole cycle.
            queue.block_specific(
                job.job_id, "no handler registered for %s" % job.category)
            summary["missing_handler"].append(job.category)
            _bump(summary["outcomes"], STATE_BLOCKED_SPECIFIC)
            continue
        try:
            outcome, detail = handler(job)
            if outcome not in HANDLER_OUTCOMES:
                outcome, detail = OUTCOME_RETRYABLE, {
                    "reason": "handler returned unknown outcome %r" % outcome}
        except Exception as exc:  # noqa: BLE001 - loop must never crash
            summary["handler_errors"] += 1
            outcome, detail = OUTCOME_RETRYABLE, {
                "reason": "handler raised: %s" % type(exc).__name__}
        detail = detail if isinstance(detail, dict) else {"detail": detail}
        result = detail if outcome in (OUTCOME_COMPLETED, OUTCOME_REJECTED) \
            else None
        state = queue.apply_outcome(job.job_id, outcome,
                                    result=result,
                                    reason=str(detail.get("reason", "")))
        summary["processed"] += 1
        summary["handled"].append({"job_id": job.job_id,
                                   "category": job.category,
                                   "lane": job.lane, "state": state})
        _bump(summary["outcomes"], state)
    summary["replenished_after"] = ensure_never_idle(
        queue, planner=planner, floor=floor)["added"]
    summary["depth"] = queue.depth()
    summary["runnable_depth"] = queue.runnable_depth(now=now)
    summary["last_progress_at"] = queue.last_progress_at()
    return summary


def _bump(d: dict, key: str) -> None:
    d[key] = int(d.get(key, 0)) + 1


# --------------------------------------------------------------------------- #
# Bounded queue DRAIN (Stage 9.2). A minimal primitive for INTEGRATING execution
# into an already-running cycle (e.g. the scheduled AlphaAgent-Collect). Unlike
# run_cycle it does NOT replenish/seed - it only claims and executes up to
# ``max_jobs`` already-queued jobs, applying each durable outcome via the SAME
# atomic claim/settle primitives. It is therefore a strict subset of run_cycle's
# behaviour (no new framework), and inherits its safety: a claimed job is RUNNING
# and can never be double-claimed by an overlapping cycle; a handler crash is a
# bounded RETRYABLE that never stops the cycle; one blocked job never blocks the
# rest; a restart resumes leased/retryable work through requeue_stale + re-claim.
# --------------------------------------------------------------------------- #
def drain_jobs(queue: "ResearchQueue", handlers: HandlerMap, *,
               max_jobs: int = 1, categories: Optional[Iterable[str]] = None,
               origins: Optional[Iterable[str]] = None,
               lane_prefixes: Optional[Iterable[str]] = None,
               budget_seconds: Optional[float] = None,
               now: Optional[str] = None,
               monotonic: Optional[Callable[[], float]] = None) -> dict:
    """Claim and execute up to ``max_jobs`` claimable jobs, honouring the queue's
    own priority ordering (claim_next orders by priority DESC, created_at ASC),
    and return a deterministic report.

    Scope (WS3): the drain is restricted to eligible jobs by ``categories``,
    ``origins`` and ``lane_prefixes`` (prefix match). The three dimensions are
    ANDed and pushed into ``claim_next`` so ineligible/unrelated jobs are NEVER
    claimed, transitioned or counted. An explicitly-empty allowlist on any
    dimension claims nothing (an empty allowlist executes nothing). ``None`` on a
    dimension leaves it unrestricted.

    Execution containment (WS1/WS2 - SAFE TIMEOUT). Each handler runs INLINE, on
    THIS thread - there is NO worker/daemon thread and nothing is ever abandoned.
    The queue lease is therefore settled (``apply_outcome``) strictly AFTER the
    handler has returned control, so a handler can never still be executing after
    its job is settled: the specific defect that a timed-out daemon thread kept
    writing canonical state after the lease was released is eliminated BY
    CONSTRUCTION. The drain only admits internally-bounded handlers (WS3
    allowlist); each such handler self-bounds its own wall-clock work and returns
    RETRYABLE on its own bounded timeout (e.g. the SEC acquisition handler stops
    at ``collect_time_budget_seconds`` between bounded network requests and
    returns RETRYABLE) - a cooperative bound the handler enforces inline, never a
    thread we abandon. ``budget_seconds`` is a cooperative per-cycle wall-clock
    bound checked BEFORE each claim (a NEW job is never STARTED once the budget is
    spent; a job already running is never interrupted). Neither is a hard process
    kill - the hard, OS-enforced ceilings are the AlphaAgent-Collect task
    ``ExecutionTimeLimit`` (PT20M) and ``MultipleInstancesPolicy=IgnoreNew`` (no
    overlapping cycle), plus the runtime collect lock. If the OS kills the
    process mid-handler the job stays RUNNING (never settled) and is recovered by
    ``requeue_stale`` on a later cycle. Never raises for a handler error.
    """
    now = now or queue._clock()  # noqa: SLF001 - deliberate shared clock
    monotonic = monotonic or time.monotonic
    cats = None if categories is None else tuple(categories)
    orgs = None if origins is None else tuple(origins)
    lpfx = None if lane_prefixes is None else tuple(lane_prefixes)
    started = monotonic()
    report = {
        "enabled": True, "max_jobs": int(max_jobs),
        "budget_seconds": budget_seconds,
        "allowed_categories": list(cats) if cats is not None else None,
        "allowed_origins": list(orgs) if orgs is not None else None,
        "allowed_lane_prefixes": list(lpfx) if lpfx is not None else None,
        "queue_depth_before": queue.depth(),
        "jobs_claimed": 0, "jobs_completed": 0, "jobs_retryable": 0,
        "jobs_blocked": 0, "jobs_failed": 0, "jobs_rejected": 0,
        "handler_errors": 0, "budget_exhausted": False,
        "job_ids": [], "handled": [],
    }
    _COUNTER = {STATE_COMPLETED: "jobs_completed",
                STATE_RETRYABLE: "jobs_retryable",
                STATE_BLOCKED_SPECIFIC: "jobs_blocked",
                STATE_FAILED_PERMANENT: "jobs_failed",
                STATE_REJECTED: "jobs_rejected"}
    for _ in range(max(0, int(max_jobs))):
        if budget_seconds is not None and \
                (monotonic() - started) >= float(budget_seconds):
            report["budget_exhausted"] = True
            break
        job = queue.claim_next(categories=cats, origins=orgs,
                               lane_prefixes=lpfx, now=now)
        if job is None:
            break
        report["jobs_claimed"] += 1
        report["job_ids"].append(job.job_id)
        handler = handlers.get(job.category)
        if handler is None:
            queue.block_specific(
                job.job_id, "no handler registered for %s" % job.category)
            report["jobs_blocked"] += 1
            report["handled"].append(
                {"job_id": job.job_id, "category": job.category,
                 "lane": job.lane, "state": STATE_BLOCKED_SPECIFIC})
            continue
        # INLINE execution: the handler runs on this thread and MUST return
        # before the lease below is settled. No thread is ever abandoned.
        try:
            ho = handler(job)
            if ho is None:
                outcome, detail = OUTCOME_RETRYABLE, {
                    "reason": "handler produced no outcome"}
            else:
                outcome, detail = ho
                if outcome not in HANDLER_OUTCOMES:
                    outcome, detail = OUTCOME_RETRYABLE, {
                        "reason": "handler returned unknown outcome %r"
                        % outcome}
        except Exception as exc:  # noqa: BLE001 - the cycle must never crash
            report["handler_errors"] += 1
            outcome, detail = OUTCOME_RETRYABLE, {
                "reason": "handler raised: %s" % type(exc).__name__}
        detail = detail if isinstance(detail, dict) else {"detail": detail}
        result = detail if outcome in (OUTCOME_COMPLETED, OUTCOME_REJECTED) \
            else None
        state = queue.apply_outcome(job.job_id, outcome, result=result,
                                    reason=str(detail.get("reason", "")))
        key = _COUNTER.get(state)
        if key:
            report[key] += 1
        report["handled"].append(
            {"job_id": job.job_id, "category": job.category, "lane": job.lane,
             "state": state, "real_work": detail.get("real_work"),
             "candidate_id": detail.get("candidate_id"),
             "disposition": detail.get("disposition")})
    report["queue_depth_after"] = queue.depth()
    return report


# --------------------------------------------------------------------------- #
# Watchdog (WS11).
# --------------------------------------------------------------------------- #
def watchdog_scan(queue: "ResearchQueue", *, planner: Optional[Planner] = None,
                  now: Optional[str] = None, floor: Optional[int] = None,
                  stale_seconds: Optional[int] = None) -> dict:
    """Continuous-operation supervisor. Detects stale RUNNING jobs, an empty
    queue and lanes that have stopped advancing; safely requeues stale work and
    replenishes; and classifies whether a genuine GLOBAL hard blocker exists (a
    hard blocker is when NO runnable work can be produced at all — every
    remaining job is FAILED_PERMANENT and the planner yields nothing). It never
    mutates operational trading state."""
    now = now or queue._clock()  # noqa: SLF001
    floor = queue.queue_floor if floor is None else int(floor)
    stale = queue.stale_running(now=now, stale_seconds=stale_seconds)
    requeued = queue.requeue_stale(now=now, stale_seconds=stale_seconds)
    replen = ensure_never_idle(queue, planner=planner, floor=floor)
    states = queue.counts_by_state()
    runnable = queue.runnable_depth(now=now)
    depth = queue.depth()
    # A hard global blocker: nothing outstanding AND nothing could be produced.
    hard_blocker = (depth == 0 and replen["added"] == 0)
    report = {
        "checked_at": now,
        "stale_running_detected": len(stale),
        "stale_requeued": requeued,
        "replenished": replen["added"],
        "queue_depth": depth,
        "runnable_depth": runnable,
        "state_counts": states,
        "blocked_specific": states.get(STATE_BLOCKED_SPECIFIC, 0),
        "failed_permanent": states.get(STATE_FAILED_PERMANENT, 0),
        "last_progress_at": queue.last_progress_at(),
        "never_idle": depth >= min(floor, 1) or not hard_blocker,
        "hard_blocker": bool(hard_blocker),
        "hard_blocker_reason": (
            "no outstanding work and no replenishable next action"
            if hard_blocker else None),
    }
    return report
