"""Alpha Agent Stage 9 - persistent Autonomous Alpha Tournament.

A disciplined candidate-selection system that continuously generates, evaluates,
ranks, rejects, combines and forward-monitors alpha candidates. It is a thin
orchestration layer that REUSES the existing research infrastructure rather than
a second competing framework:

  * Stage 5 experiment engine  -> the canonical evaluation battery
        (rank-IC, decile spread, cost grid, robustness, signal combination).
  * Stage 8 durable never-idle queue (``autonomous_research``) -> autonomous
        generation of the next highest-value experiments, deduplicated by spec.
  * Phase 28B ``forward_prediction_skill`` immutable first-write-wins snapshots
        -> read-only research shadow books with true-forward, never-backfilled
        evidence.
  * ``evidence_observatory`` / ``report_renderer`` / ``telegram_control``
        -> read-only surfacing.

SAFETY. RESEARCH ONLY. There is NO automatic PROMOTED lifecycle state and
``READY_FOR_MANUAL_REVIEW`` never modifies the operating model. This module
never writes an operational trading ledger, never creates an order/fill/signal/
trade-decision/model-promotion, never mutates the Alpha Paper Book, never
triggers a Daily Close, never writes PostgreSQL and never runs the prediction
service. The durable tournament registry and the shadow books live under the
approved Stage 8 research-state root on ``D:`` - off the operational ledger.

A candidate is NEVER ranked highly merely because one isolated metric or one
test period looks good: the combined tournament score is decomposed into six
weighted sub-scores and hard cost/stability caps prevent a strong-gross,
poor-cost or unstable candidate from leading.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from . import orthogonality as _orth


# --------------------------------------------------------------------------- #
# Lifecycle states (WS2). There is deliberately NO PROMOTED state.
# --------------------------------------------------------------------------- #
PROPOSED = "PROPOSED"
DATA_HOLD = "DATA_HOLD"
TESTING = "TESTING"
REJECTED = "REJECTED"
KEEP_FOR_RESEARCH = "KEEP_FOR_RESEARCH"
SHADOW_BOOK_ACTIVE = "SHADOW_BOOK_ACTIVE"
READY_FOR_MANUAL_REVIEW = "READY_FOR_MANUAL_REVIEW"
ARCHIVED = "ARCHIVED"

LIFECYCLE_STATES = (
    PROPOSED, DATA_HOLD, TESTING, REJECTED, KEEP_FOR_RESEARCH,
    SHADOW_BOOK_ACTIVE, READY_FOR_MANUAL_REVIEW, ARCHIVED,
)

# Allowed lifecycle transitions. A disallowed transition raises so no state is
# reached by an unaudited path. Note a REJECTED/ARCHIVED candidate can only be
# re-opened to TESTING when genuinely NEW data/spec evidence arrives.
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    PROPOSED: frozenset({DATA_HOLD, TESTING, ARCHIVED}),
    DATA_HOLD: frozenset({DATA_HOLD, TESTING, ARCHIVED}),
    TESTING: frozenset({TESTING, DATA_HOLD, REJECTED, KEEP_FOR_RESEARCH, ARCHIVED}),
    KEEP_FOR_RESEARCH: frozenset(
        {KEEP_FOR_RESEARCH, TESTING, SHADOW_BOOK_ACTIVE, REJECTED, ARCHIVED}),
    SHADOW_BOOK_ACTIVE: frozenset(
        {SHADOW_BOOK_ACTIVE, KEEP_FOR_RESEARCH, READY_FOR_MANUAL_REVIEW,
         REJECTED, ARCHIVED}),
    READY_FOR_MANUAL_REVIEW: frozenset(
        {READY_FOR_MANUAL_REVIEW, SHADOW_BOOK_ACTIVE, KEEP_FOR_RESEARCH, ARCHIVED}),
    REJECTED: frozenset({REJECTED, TESTING, ARCHIVED}),
    ARCHIVED: frozenset({ARCHIVED, TESTING}),
}

# The states a candidate can be REJECTED from require COMPLETE evidence; this is
# enforced by ``classify_evidence`` (a candidate is only REJECTED on
# complete-and-weak evidence, never on missing data - that is DATA_HOLD).

# Evidence-status vocabulary (WS4/WS5).
EVIDENCE_UNTESTED = "UNTESTED"
EVIDENCE_INCOMPLETE = "INCOMPLETE"          # cannot be evaluated honestly yet
EVIDENCE_COMPLETE_WEAK = "COMPLETE_WEAK"    # complete and fails a gate
EVIDENCE_COMPLETE_STRONG = "COMPLETE_STRONG"  # complete and passes all gates
EVIDENCE_FORWARD_MATURING = "FORWARD_MATURING"

# Alpha families (WS3).
FAM_PRICE_MOMENTUM = "PRICE_MOMENTUM"
FAM_FUNDAMENTAL = "FUNDAMENTAL"
FAM_ANALYST_EARNINGS = "ANALYST_EARNINGS"
FAM_EVENT_INSIDER = "EVENT_INSIDER"
FAM_COMPOSITE = "COMPOSITE"

ALL_FAMILIES = (
    FAM_PRICE_MOMENTUM, FAM_FUNDAMENTAL, FAM_ANALYST_EARNINGS,
    FAM_EVENT_INSIDER, FAM_COMPOSITE,
)

# Gate/blocker diagnostics (specific, never a silent reconciles=false).
BLOCK_COVERAGE = "DATA_HOLD_INSUFFICIENT_COVERAGE"
BLOCK_OBSERVATIONS = "DATA_HOLD_INSUFFICIENT_OBSERVATIONS"
BLOCK_UNIVERSE = "DATA_HOLD_INSUFFICIENT_UNIVERSE"
BLOCK_PIT = "DATA_HOLD_POINT_IN_TIME_UNAVAILABLE"
BLOCK_LOOKAHEAD = "DATA_HOLD_LOOKAHEAD_CONTAMINATION"
BLOCK_NO_DATA_FAMILY = "DATA_HOLD_DATA_FAMILY_NOT_ACCESSIBLE"
# Stage 9.4: specific blockers for pre-registered price factors whose formula
# needs data the owned close panel does not carry (never approximated).
BLOCK_REQUIRES_VOLUME = "DATA_HOLD_REQUIRES_VOLUME_TURNOVER_DATA"
BLOCK_REQUIRES_INTRADAY = "DATA_HOLD_REQUIRES_INTRADAY_OPEN_DATA"
BLOCK_REQUIRES_EVENT_CAL = "DATA_HOLD_REQUIRES_EVENT_CALENDAR_DATA"
# Keyed by the spec ``data_requirement`` values in price_factor_catalogue.
_PRICE_REQ_BLOCKER = {
    "owned_volume_turnover": BLOCK_REQUIRES_VOLUME,
    "intraday_open": BLOCK_REQUIRES_INTRADAY,
    "point_in_time_gics": BLOCK_PIT,
    "event_calendar": BLOCK_REQUIRES_EVENT_CAL,
}

REJECT_WEAK_RANK_IC = "REJECT_WEAK_RANK_IC"
REJECT_WEAK_RANK_IC_T = "REJECT_STATISTICALLY_WEAK_RANK_IC_T"
REJECT_WEAK_SPREAD_T = "REJECT_WEAK_SPREAD_T"
REJECT_LOW_HIT_RATE = "REJECT_LOW_POSITIVE_IC_HIT_RATE"
REJECT_NOT_COST_ROBUST = "REJECT_NOT_COST_ROBUST_NET25_NONPOSITIVE"
REJECT_UNSTABLE_SUBPERIOD = "REJECT_UNSTABLE_SUBPERIOD"
REJECT_UNSTABLE_REGIME = "REJECT_UNSTABLE_REGIME"
REJECT_SEVERE_DRAWDOWN = "REJECT_SEVERE_DRAWDOWN"
REJECT_EXCESSIVE_TURNOVER = "REJECT_EXCESSIVE_TURNOVER"
REJECT_SECTOR_CONCENTRATION = "REJECT_FATAL_SECTOR_CONCENTRATION"

CODE_VERSION = "stage9-tournament-1.0.0"


# --------------------------------------------------------------------------- #
# Small pure helpers.
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _clamp01(x: float) -> float:
    if x != x:  # NaN
        return 0.0
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else x)


def canonical_json(obj: Any) -> str:
    """Deterministic JSON for hashing/dedup (sorted keys, no whitespace drift)."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, default=str)


def spec_hash(spec: dict) -> str:
    """Stable content hash of a candidate/experiment specification.

    Used both as the candidate identity seed and as the deterministic
    deduplication key for autonomously generated experiments (WS7).
    """
    return hashlib.sha256(canonical_json(spec).encode("utf-8")).hexdigest()[:16]


def candidate_id_for(family: str, spec: dict) -> str:
    """Deterministic candidate id: family tag + spec hash (idempotent seeding)."""
    fam = "".join(ch for ch in str(family).lower() if ch.isalnum())[:12] or "cand"
    return "c9_%s_%s" % (fam, spec_hash(spec)[:10])


# --------------------------------------------------------------------------- #
# Configuration (WS5 - versioned thresholds; nothing invented at runtime).
# --------------------------------------------------------------------------- #
class ConfigError(ValueError):
    """Raised when the Stage 9 config is missing/unreadable."""


def load_config(path: str | Path) -> dict:
    p = Path(path)
    if not p.exists():
        raise ConfigError("stage9 config not found: %s" % p)
    try:
        cfg = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:  # pragma: no cover - defensive
        raise ConfigError("stage9 config unreadable: %s" % exc) from exc
    if str(cfg.get("stage")) != "9":
        raise ConfigError("not a stage9 config: %s" % p)
    return cfg


# --------------------------------------------------------------------------- #
# Durable candidate registry (WS2) - stdlib sqlite3, off the operational ledger.
# --------------------------------------------------------------------------- #
_SCHEMA = """
CREATE TABLE IF NOT EXISTS candidates (
    candidate_id        TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    family              TEXT NOT NULL,
    spec_json           TEXT NOT NULL,
    spec_hash           TEXT NOT NULL,
    spec_version        TEXT NOT NULL,
    code_hash           TEXT NOT NULL,
    data_dependencies   TEXT NOT NULL,
    universe            TEXT NOT NULL,
    pit_status          TEXT NOT NULL,
    inception_ts        TEXT NOT NULL,
    experiment_ids      TEXT NOT NULL,
    latest_evidence_date TEXT,
    evidence_status     TEXT NOT NULL,
    lifecycle_state     TEXT NOT NULL,
    blocker             TEXT,
    parent_candidates   TEXT NOT NULL,
    component_signals   TEXT NOT NULL,
    active_shadow_book_id TEXT,
    combined_score      REAL,
    scores_json         TEXT,
    metrics_json        TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_candidates_family_spec
    ON candidates(family, spec_hash);
CREATE INDEX IF NOT EXISTS ix_candidates_state ON candidates(lifecycle_state);

CREATE TABLE IF NOT EXISTS evaluations (
    eval_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id        TEXT NOT NULL,
    evaluated_at        TEXT NOT NULL,
    experiment_id       TEXT,
    metrics_json        TEXT NOT NULL,
    scores_json         TEXT NOT NULL,
    gate_json           TEXT NOT NULL,
    pit_json            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_eval_candidate ON evaluations(candidate_id);

CREATE TABLE IF NOT EXISTS changes (
    change_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at         TEXT NOT NULL,
    kind                TEXT NOT NULL,
    candidate_id        TEXT,
    detail_json         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_changes_recorded ON changes(recorded_at);

CREATE TABLE IF NOT EXISTS shadow_books (
    shadow_book_id      TEXT PRIMARY KEY,
    candidate_id        TEXT NOT NULL,
    inception_date      TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    status              TEXT NOT NULL,
    meta_json           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_shadow_candidate ON shadow_books(candidate_id);

CREATE TABLE IF NOT EXISTS generated_experiments (
    gen_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    spec_hash           TEXT NOT NULL UNIQUE,
    candidate_id        TEXT,
    strategy            TEXT NOT NULL,
    spec_json           TEXT NOT NULL,
    created_at          TEXT NOT NULL
);

-- WS4: idempotent ingestion of completed Stage 8 experiment results. A result is
-- keyed by a stable content hash so the same completed result is never
-- re-evaluated on a later 30-minute cycle; a genuinely new result is a new hash.
CREATE TABLE IF NOT EXISTS processed_experiments (
    result_hash         TEXT PRIMARY KEY,
    candidate_id        TEXT,
    source              TEXT NOT NULL,
    job_id              TEXT,
    feature             TEXT,
    evidence_date       TEXT,
    processed_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_processed_candidate
    ON processed_experiments(candidate_id);

-- Small durable key/value store for the resumable production tick (e.g. the
-- last calendar date the bounded owned-price campaign was run, so the heavy
-- Stage 5 evaluation runs at most once per evidence date - not every cycle).
CREATE TABLE IF NOT EXISTS meta (
    key                 TEXT PRIMARY KEY,
    value               TEXT,
    updated_at          TEXT NOT NULL
);

-- Stage 9.2: weakest-gate DATA_VALIDATION coverage measurements. A completed
-- tournament follow-up job records a durable coverage artifact here and advances
-- the candidate's latest_evidence_date / experiment_ids WITHOUT changing its
-- lifecycle_state, blocker, metrics or combined_score. Coverage is evidence of
-- PROGRESS toward resolving a DATA_HOLD; it never promotes a candidate - only a
-- full canonical statistical evidence contract (classify_evidence) can move a
-- candidate out of DATA_HOLD.
CREATE TABLE IF NOT EXISTS data_coverage (
    coverage_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id        TEXT NOT NULL,
    measured_at         TEXT NOT NULL,
    evidence_date       TEXT,
    job_id              TEXT,
    data_dependency     TEXT,
    sufficient          INTEGER NOT NULL DEFAULT 0,
    next_action         TEXT,
    coverage_json       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_coverage_candidate ON data_coverage(candidate_id);
"""

_CANDIDATE_COLUMNS = (
    "candidate_id", "name", "family", "spec_json", "spec_hash", "spec_version",
    "code_hash", "data_dependencies", "universe", "pit_status", "inception_ts",
    "experiment_ids", "latest_evidence_date", "evidence_status",
    "lifecycle_state", "blocker", "parent_candidates", "component_signals",
    "active_shadow_book_id", "combined_score", "scores_json", "metrics_json",
    "created_at", "updated_at",
)

_JSON_FIELDS = ("data_dependencies", "experiment_ids", "parent_candidates",
                "component_signals", "spec", "scores", "metrics")


class TransitionError(ValueError):
    """Raised on a disallowed lifecycle transition."""


class CandidateRegistry:
    """Durable, resumable candidate registry (WS2).

    All state lives in one sqlite file under the Stage 8 research root. The
    store never touches an operational ledger. A monotonic injectable clock
    keeps timestamps deterministic in tests.
    """

    def __init__(self, db_path: str | Path, *,
                 clock: Optional[Callable[[], str]] = None) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or _wall_clock
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    # -- lifecycle -------------------------------------------------------- #
    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:  # pragma: no cover
            pass

    def now(self) -> str:
        return self._clock()

    # -- candidate CRUD --------------------------------------------------- #
    def seed_candidate(self, *, name: str, family: str, spec: dict,
                       data_dependencies: Iterable[str], universe: str,
                       pit_status: str, component_signals: Iterable[str] = (),
                       parent_candidates: Iterable[str] = (),
                       spec_version: str = "1.0.0",
                       lifecycle_state: str = PROPOSED,
                       blocker: Optional[str] = None) -> str:
        """Idempotently register a candidate.

        Deduplicated by (family, spec_hash): re-seeding the identical spec
        returns the existing candidate_id and does NOT reset its lifecycle or
        evidence (WS13 spec-dedup). Returns the candidate_id.
        """
        sh = spec_hash(spec)
        cid = candidate_id_for(family, spec)
        existing = self.get(cid) or self._get_by_family_spec(family, sh)
        if existing is not None:
            return existing["candidate_id"]
        ts = self.now()
        row = {
            "candidate_id": cid, "name": name, "family": family,
            "spec_json": canonical_json(spec), "spec_hash": sh,
            "spec_version": spec_version, "code_hash": CODE_VERSION,
            "data_dependencies": canonical_json(list(data_dependencies)),
            "universe": universe, "pit_status": pit_status, "inception_ts": ts,
            "experiment_ids": canonical_json([]),
            "latest_evidence_date": None, "evidence_status": EVIDENCE_UNTESTED,
            "lifecycle_state": lifecycle_state, "blocker": blocker,
            "parent_candidates": canonical_json(list(parent_candidates)),
            "component_signals": canonical_json(list(component_signals)),
            "active_shadow_book_id": None, "combined_score": None,
            "scores_json": None, "metrics_json": None,
            "created_at": ts, "updated_at": ts,
        }
        cols = ",".join(_CANDIDATE_COLUMNS)
        ph = ",".join("?" for _ in _CANDIDATE_COLUMNS)
        self._conn.execute(
            "INSERT INTO candidates (%s) VALUES (%s)" % (cols, ph),
            tuple(row[c] for c in _CANDIDATE_COLUMNS))
        self._conn.commit()
        self._record_change("CANDIDATE_SEEDED", cid,
                            {"name": name, "family": family, "state": lifecycle_state})
        return cid

    def _get_by_family_spec(self, family: str, sh: str) -> Optional[dict]:
        cur = self._conn.execute(
            "SELECT * FROM candidates WHERE family=? AND spec_hash=?", (family, sh))
        r = cur.fetchone()
        return _decode_candidate(r) if r else None

    def get(self, candidate_id: str) -> Optional[dict]:
        cur = self._conn.execute(
            "SELECT * FROM candidates WHERE candidate_id=?", (candidate_id,))
        r = cur.fetchone()
        return _decode_candidate(r) if r else None

    def list(self, *, state: Optional[str] = None,
             family: Optional[str] = None) -> list[dict]:
        q = "SELECT * FROM candidates"
        where, args = [], []
        if state:
            where.append("lifecycle_state=?")
            args.append(state)
        if family:
            where.append("family=?")
            args.append(family)
        if where:
            q += " WHERE " + " AND ".join(where)
        q += " ORDER BY (combined_score IS NULL), combined_score DESC, candidate_id"
        return [_decode_candidate(r) for r in self._conn.execute(q, args)]

    def counts_by_state(self) -> dict[str, int]:
        out = {s: 0 for s in LIFECYCLE_STATES}
        for r in self._conn.execute(
                "SELECT lifecycle_state, COUNT(*) n FROM candidates "
                "GROUP BY lifecycle_state"):
            out[r["lifecycle_state"]] = int(r["n"])
        return out

    def counts_by_family(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in self._conn.execute(
                "SELECT family, COUNT(*) n FROM candidates GROUP BY family"):
            out[r["family"]] = int(r["n"])
        return out

    # -- transitions ------------------------------------------------------ #
    def transition(self, candidate_id: str, new_state: str, *,
                   blocker: Optional[str] = None,
                   reason: Optional[str] = None) -> dict:
        cand = self.get(candidate_id)
        if cand is None:
            raise TransitionError("unknown candidate: %s" % candidate_id)
        cur_state = cand["lifecycle_state"]
        if new_state not in LIFECYCLE_STATES:
            raise TransitionError("unknown state: %s" % new_state)
        if new_state not in ALLOWED_TRANSITIONS.get(cur_state, frozenset()):
            raise TransitionError(
                "illegal transition %s -> %s (candidate %s)"
                % (cur_state, new_state, candidate_id))
        ts = self.now()
        self._conn.execute(
            "UPDATE candidates SET lifecycle_state=?, blocker=?, updated_at=? "
            "WHERE candidate_id=?", (new_state, blocker, ts, candidate_id))
        self._conn.commit()
        if new_state != cur_state:
            self._record_change("LIFECYCLE_TRANSITION", candidate_id,
                                {"from": cur_state, "to": new_state,
                                 "blocker": blocker, "reason": reason})
        return self.get(candidate_id)  # type: ignore[return-value]

    # -- evaluations ------------------------------------------------------ #
    def record_evaluation(self, candidate_id: str, *, metrics: dict,
                          scores: dict, gate: dict, pit: dict,
                          experiment_id: Optional[str] = None,
                          evidence_date: Optional[str] = None) -> int:
        ts = self.now()
        cur = self._conn.execute(
            "INSERT INTO evaluations "
            "(candidate_id, evaluated_at, experiment_id, metrics_json, "
            " scores_json, gate_json, pit_json) VALUES (?,?,?,?,?,?,?)",
            (candidate_id, ts, experiment_id, canonical_json(metrics),
             canonical_json(scores), canonical_json(gate), canonical_json(pit)))
        combined = _f(scores.get("combined_score"))
        self._conn.execute(
            "UPDATE candidates SET latest_evidence_date=?, combined_score=?, "
            "scores_json=?, metrics_json=?, evidence_status=?, updated_at=? "
            "WHERE candidate_id=?",
            (evidence_date or ts, combined, canonical_json(scores),
             canonical_json(metrics), gate.get("evidence_status", EVIDENCE_UNTESTED),
             ts, candidate_id))
        if experiment_id:
            cand = self.get(candidate_id)
            ids = list(cand["experiment_ids"]) if cand else []
            if experiment_id not in ids:
                ids.append(experiment_id)
                self._conn.execute(
                    "UPDATE candidates SET experiment_ids=? WHERE candidate_id=?",
                    (canonical_json(ids), candidate_id))
        self._conn.commit()
        return int(cur.lastrowid)

    def latest_evaluation(self, candidate_id: str) -> Optional[dict]:
        cur = self._conn.execute(
            "SELECT * FROM evaluations WHERE candidate_id=? "
            "ORDER BY eval_id DESC LIMIT 1", (candidate_id,))
        r = cur.fetchone()
        if not r:
            return None
        return {"eval_id": r["eval_id"], "candidate_id": r["candidate_id"],
                "evaluated_at": r["evaluated_at"], "experiment_id": r["experiment_id"],
                "metrics": json.loads(r["metrics_json"]),
                "scores": json.loads(r["scores_json"]),
                "gate": json.loads(r["gate_json"]),
                "pit": json.loads(r["pit_json"])}

    # -- weakest-gate coverage evidence (Stage 9.2) ----------------------- #
    def record_data_coverage(self, candidate_id: str, *, coverage: dict,
                             evidence_date: Optional[str] = None,
                             data_dependency: Optional[str] = None,
                             job_id: Optional[str] = None,
                             sufficient: bool = False,
                             next_action: Optional[str] = None) -> int:
        """Durably attach a weakest-gate DATA_VALIDATION coverage measurement to a
        candidate as evidence of PROGRESS toward resolving its DATA_HOLD.

        Advances ONLY ``latest_evidence_date`` and appends the executing job to
        ``experiment_ids``; it never touches ``lifecycle_state``, ``blocker``,
        ``metrics`` or ``combined_score`` - so a coverage measurement can never
        promote a candidate or spuriously flip its lifecycle on a later tick
        (only a full canonical statistical evidence contract, via
        ``record_evaluation`` + ``classify_evidence``, can move a candidate out of
        DATA_HOLD). Returns the coverage row id.
        """
        ts = self.now()
        cur = self._conn.execute(
            "INSERT INTO data_coverage (candidate_id, measured_at, evidence_date,"
            " job_id, data_dependency, sufficient, next_action, coverage_json)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (candidate_id, ts, evidence_date, job_id, data_dependency,
             1 if sufficient else 0, next_action, canonical_json(coverage)))
        cand = self.get(candidate_id)
        if cand is not None:
            ids = list(cand.get("experiment_ids") or [])
            if job_id and job_id not in ids:
                ids.append(job_id)
            self._conn.execute(
                "UPDATE candidates SET latest_evidence_date=?, experiment_ids=?,"
                " updated_at=? WHERE candidate_id=?",
                (evidence_date or ts, canonical_json(ids), ts, candidate_id))
        self._conn.commit()
        self._record_change("DATA_COVERAGE_MEASURED", candidate_id, {
            "evidence_date": evidence_date, "job_id": job_id,
            "data_dependency": data_dependency, "sufficient": bool(sufficient),
            "next_action": next_action, "coverage": coverage})
        return int(cur.lastrowid)

    def latest_data_coverage(self, candidate_id: str) -> Optional[dict]:
        r = self._conn.execute(
            "SELECT * FROM data_coverage WHERE candidate_id=? "
            "ORDER BY coverage_id DESC LIMIT 1", (candidate_id,)).fetchone()
        if not r:
            return None
        return {"coverage_id": r["coverage_id"], "candidate_id": r["candidate_id"],
                "measured_at": r["measured_at"], "evidence_date": r["evidence_date"],
                "job_id": r["job_id"], "data_dependency": r["data_dependency"],
                "sufficient": bool(r["sufficient"]),
                "next_action": r["next_action"],
                "coverage": json.loads(r["coverage_json"])}

    # -- shadow books ----------------------------------------------------- #
    def create_shadow_book(self, candidate_id: str, shadow_book_id: str, *,
                           inception_date: str, meta: dict) -> None:
        ts = self.now()
        self._conn.execute(
            "INSERT OR IGNORE INTO shadow_books "
            "(shadow_book_id, candidate_id, inception_date, created_at, "
            " status, meta_json) VALUES (?,?,?,?,?,?)",
            (shadow_book_id, candidate_id, inception_date, ts, "ACTIVE",
             canonical_json(meta)))
        self._conn.execute(
            "UPDATE candidates SET active_shadow_book_id=?, updated_at=? "
            "WHERE candidate_id=?", (shadow_book_id, ts, candidate_id))
        self._conn.commit()
        self._record_change("SHADOW_BOOK_ACTIVATED", candidate_id,
                            {"shadow_book_id": shadow_book_id,
                             "inception_date": inception_date})

    def list_shadow_books(self) -> list[dict]:
        return [{"shadow_book_id": r["shadow_book_id"],
                 "candidate_id": r["candidate_id"],
                 "inception_date": r["inception_date"],
                 "created_at": r["created_at"], "status": r["status"],
                 "meta": json.loads(r["meta_json"])}
                for r in self._conn.execute(
                    "SELECT * FROM shadow_books ORDER BY created_at DESC")]

    # -- generated-experiment dedup (WS7) --------------------------------- #
    def try_register_generated(self, *, strategy: str, spec: dict,
                               candidate_id: Optional[str]) -> Optional[str]:
        """Register an autonomously generated experiment spec.

        Returns the spec_hash if newly registered, or ``None`` if this exact
        spec was already generated (deterministic dedup - no endless duplicate
        experiments).
        """
        sh = spec_hash(spec)
        try:
            self._conn.execute(
                "INSERT INTO generated_experiments "
                "(spec_hash, candidate_id, strategy, spec_json, created_at) "
                "VALUES (?,?,?,?,?)",
                (sh, candidate_id, strategy, canonical_json(spec), self.now()))
            self._conn.commit()
            return sh
        except sqlite3.IntegrityError:
            return None

    def generated_count(self) -> int:
        r = self._conn.execute(
            "SELECT COUNT(*) n FROM generated_experiments").fetchone()
        return int(r["n"])

    # -- durable key/value meta (WS3 resumable campaign cadence) ----------- #
    def get_meta(self, key: str) -> Optional[str]:
        r = self._conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

    def set_meta(self, key: str, value: Optional[str]) -> None:
        self._conn.execute(
            "INSERT INTO meta (key, value, updated_at) VALUES (?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
            "updated_at=excluded.updated_at", (key, value, self.now()))
        self._conn.commit()

    # -- processed-experiment ledger (WS4 idempotent ingestion) ----------- #
    def is_processed(self, result_hash: str) -> bool:
        r = self._conn.execute(
            "SELECT 1 FROM processed_experiments WHERE result_hash=?",
            (result_hash,)).fetchone()
        return r is not None

    def mark_processed(self, *, result_hash: str,
                       candidate_id: Optional[str], source: str,
                       job_id: Optional[str] = None,
                       feature: Optional[str] = None,
                       evidence_date: Optional[str] = None) -> bool:
        """Record that a completed experiment result was ingested. Returns True
        if newly recorded, False if this exact result was already processed
        (deterministic idempotency - the same result is never re-ingested)."""
        try:
            self._conn.execute(
                "INSERT INTO processed_experiments "
                "(result_hash, candidate_id, source, job_id, feature, "
                " evidence_date, processed_at) VALUES (?,?,?,?,?,?,?)",
                (result_hash, candidate_id, source, job_id, feature,
                 evidence_date, self.now()))
            self._conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def processed_count(self) -> int:
        r = self._conn.execute(
            "SELECT COUNT(*) n FROM processed_experiments").fetchone()
        return int(r["n"])

    def list_processed(self, *, candidate_id: Optional[str] = None) -> list[dict]:
        q = "SELECT * FROM processed_experiments"
        args: list = []
        if candidate_id:
            q += " WHERE candidate_id=?"
            args.append(candidate_id)
        q += " ORDER BY processed_at DESC"
        return [{"result_hash": r["result_hash"],
                 "candidate_id": r["candidate_id"], "source": r["source"],
                 "job_id": r["job_id"], "feature": r["feature"],
                 "evidence_date": r["evidence_date"],
                 "processed_at": r["processed_at"]}
                for r in self._conn.execute(q, args)]

    # -- changes / meaningful-change reporting (WS10) --------------------- #
    def _record_change(self, kind: str, candidate_id: Optional[str],
                       detail: dict) -> None:
        self._conn.execute(
            "INSERT INTO changes (recorded_at, kind, candidate_id, detail_json) "
            "VALUES (?,?,?,?)", (self.now(), kind, candidate_id,
                                 canonical_json(detail)))
        self._conn.commit()

    def record_change(self, kind: str, candidate_id: Optional[str],
                      detail: dict) -> None:
        self._record_change(kind, candidate_id, detail)

    def recent_changes(self, *, limit: int = 20) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM changes ORDER BY change_id DESC LIMIT ?", (limit,))
        return [{"change_id": r["change_id"], "recorded_at": r["recorded_at"],
                 "kind": r["kind"], "candidate_id": r["candidate_id"],
                 "detail": json.loads(r["detail_json"])} for r in rows]


def _wall_clock() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())


def _decode_candidate(row: sqlite3.Row) -> dict:
    d = dict(row)
    for k in ("data_dependencies", "experiment_ids", "parent_candidates",
              "component_signals"):
        d[k] = json.loads(d[k]) if d.get(k) else []
    d["spec"] = json.loads(d["spec_json"]) if d.get("spec_json") else {}
    d["scores"] = json.loads(d["scores_json"]) if d.get("scores_json") else None
    d["metrics"] = json.loads(d["metrics_json"]) if d.get("metrics_json") else None
    return d


# --------------------------------------------------------------------------- #
# Evidence gates (WS5). Complete-and-weak -> REJECTED; incomplete -> DATA_HOLD;
# complete-and-strong -> KEEP_FOR_RESEARCH. Nothing is invented at runtime; all
# thresholds come from the versioned config.
# --------------------------------------------------------------------------- #
def classify_evidence(metrics: dict, cfg: dict) -> dict:
    """Return {target_state, evidence_status, blocker, failed_gates, complete}.

    ``metrics`` is the canonical evaluation contract (WS4). A candidate can only
    be REJECTED when its evidence is COMPLETE and it fails a strength gate; when
    coverage/observations/PIT are insufficient it is DATA_HOLD (honest), and a
    known look-ahead contamination is DATA_HOLD (cannot be evaluated honestly).
    """
    ec = cfg.get("evidence_completeness", {})
    gates = cfg.get("gates", {})

    coverage = _f(metrics.get("coverage_pct"))
    periods = _f(metrics.get("scored_periods"))
    names = _f(metrics.get("min_names_per_period"))
    pit_valid = bool(metrics.get("point_in_time_valid", True))
    survivorship_safe = bool(metrics.get("survivorship_safe", True))
    lookahead = bool(metrics.get("lookahead_contamination", False))

    # Incompleteness / honesty blockers -> DATA_HOLD.
    if ec.get("require_point_in_time_valid", True) and not pit_valid:
        return _hold(BLOCK_PIT)
    if lookahead and gates.get("forbid_known_lookahead", True):
        return _hold(BLOCK_LOOKAHEAD)
    if names is not None and names < _f(ec.get("min_universe_names_per_period")):
        return _hold(BLOCK_UNIVERSE)
    if coverage is None or periods is None:
        return _hold(BLOCK_OBSERVATIONS)
    if coverage < _f(ec.get("min_coverage_pct")):
        return _hold(BLOCK_COVERAGE)
    if periods < _f(ec.get("min_scored_periods")):
        return _hold(BLOCK_OBSERVATIONS)
    if ec.get("require_survivorship_safe", True) and not survivorship_safe:
        return _hold(BLOCK_PIT)

    # Evidence is COMPLETE -> apply strength gates.
    failed: list[str] = []
    rank_ic = _f(metrics.get("rank_ic"))
    rank_ic_t = _f(metrics.get("rank_ic_t"))
    hit = _f(metrics.get("positive_ic_hit_rate"))
    spread_t = _f(metrics.get("spread_t"))
    net25 = _f(metrics.get("net25_spread"))
    subp = _f(metrics.get("subperiod_consistency"))
    regime = _f(metrics.get("regime_consistency"))
    dd = _f(metrics.get("max_drawdown_pct"))
    turn = _f(metrics.get("turnover_per_rebalance"))
    sector = _f(metrics.get("sector_concentration_pct"))
    worst = _f(metrics.get("worst_period_return_pct"))

    def _lt(v, key):
        thr = _f(gates.get(key))
        return v is not None and thr is not None and v < thr

    def _gt(v, key):
        thr = _f(gates.get(key))
        return v is not None and thr is not None and v > thr

    if _lt(rank_ic, "keep_min_rank_ic"):
        failed.append(REJECT_WEAK_RANK_IC)
    if _lt(rank_ic_t, "keep_min_rank_ic_t"):
        failed.append(REJECT_WEAK_RANK_IC_T)
    if _lt(hit, "keep_min_positive_ic_hit_rate"):
        failed.append(REJECT_LOW_HIT_RATE)
    if _lt(spread_t, "keep_min_spread_t"):
        failed.append(REJECT_WEAK_SPREAD_T)
    if _lt(net25, "keep_min_net25_spread"):
        failed.append(REJECT_NOT_COST_ROBUST)
    if _lt(subp, "keep_min_subperiod_consistency"):
        failed.append(REJECT_UNSTABLE_SUBPERIOD)
    if _lt(regime, "keep_min_regime_consistency"):
        failed.append(REJECT_UNSTABLE_REGIME)
    if dd is not None and _f(gates.get("keep_max_drawdown_pct")) is not None \
            and dd < _f(gates.get("keep_max_drawdown_pct")):
        failed.append(REJECT_SEVERE_DRAWDOWN)
    if _gt(turn, "keep_max_turnover_per_rebalance"):
        failed.append(REJECT_EXCESSIVE_TURNOVER)
    if _gt(sector, "keep_max_sector_concentration_pct"):
        failed.append(REJECT_SECTOR_CONCENTRATION)
    if worst is not None and _f(gates.get("keep_max_worst_period_return_pct")) is not None \
            and worst < _f(gates.get("keep_max_worst_period_return_pct")):
        failed.append(REJECT_SEVERE_DRAWDOWN)

    if failed:
        return {"target_state": REJECTED, "evidence_status": EVIDENCE_COMPLETE_WEAK,
                "blocker": failed[0], "failed_gates": failed, "complete": True}
    return {"target_state": KEEP_FOR_RESEARCH,
            "evidence_status": EVIDENCE_COMPLETE_STRONG,
            "blocker": None, "failed_gates": [], "complete": True}


def _hold(blocker: str) -> dict:
    return {"target_state": DATA_HOLD, "evidence_status": EVIDENCE_INCOMPLETE,
            "blocker": blocker, "failed_gates": [], "complete": False}


def coverage_gate_allows_evaluation(coverage_row: Optional[dict]) -> bool:
    """Stage 9.3 family-evaluation GUARD (Part 7). A family evaluation adapter
    may run the canonical Stage 9 evidence contract for a candidate ONLY when its
    latest measured data coverage is sufficient. Raw coverage never itself
    changes a lifecycle state; this only permits an evaluation to be ATTEMPTED,
    whose gate result (via ``classify_evidence``) may then move DATA_HOLD ->
    REJECTED / KEEP_FOR_RESEARCH. Missing or insufficient coverage -> False, so
    the candidate stays DATA_HOLD."""
    if not coverage_row:
        return False
    return bool(coverage_row.get("sufficient"))


# --------------------------------------------------------------------------- #
# Tournament scoring (WS6) - reproducible, decomposable into six sub-scores.
# Cost and stability caps guarantee a strong-gross but poor-cost/unstable
# candidate cannot lead the tournament.
# --------------------------------------------------------------------------- #
def score_candidate(metrics: dict, cfg: dict, *,
                    corr_champion: Optional[float] = None,
                    corr_retained: Optional[float] = None,
                    forward: Optional[dict] = None) -> dict:
    sc = cfg.get("scoring", {})
    w = sc.get("weights", {})

    hist = _score_historical(metrics, sc.get("historical_evidence", {}))
    robust = _score_robustness(metrics, sc.get("robustness", {}))
    cost = _score_cost(metrics, sc.get("cost", {}))
    stab = _score_stability(metrics, sc.get("stability", {}))
    divers = _score_diversification(corr_champion, corr_retained,
                                    sc.get("diversification", {}))
    fwd = _score_forward(forward or {}, sc.get("forward_evidence", {}))

    # Stage 9.4 Track B: INDEPENDENT_INFORMATION sub-score. It rewards a candidate
    # that is orthogonal to the champion/retained set and carries partial rank-IC
    # / incremental net return beyond them; a candidate that merely duplicates the
    # champion is driven toward zero. It NEVER weakens a gate - it is only folded
    # into the combined score when a config weight is present (default absent -> 0
    # -> the six-subscore combined score is unchanged for existing candidates).
    indep = _orth.independent_information_score(
        corr_champion=corr_champion, corr_retained=corr_retained,
        partial_rank_ic=_f(metrics.get("partial_rank_ic")),
        incremental_net_return=_f(metrics.get("incremental_net_return")),
        cfg=sc.get("independent_information", {}))
    indep_score = _f(indep.get("independent_information_score")) or 0.0

    combined = (
        hist * _f(w.get("historical_evidence") or 0.0)
        + robust * _f(w.get("robustness") or 0.0)
        + cost * _f(w.get("cost") or 0.0)
        + stab * _f(w.get("stability") or 0.0)
        + divers * _f(w.get("diversification") or 0.0)
        + fwd * _f(w.get("forward_evidence") or 0.0)
        + indep_score * _f(w.get("independent_information") or 0.0)
    )
    combined = _clamp01(combined)

    # Hard caps: strong gross cannot hide poor cost or instability (WS6/WS13).
    caps: list[str] = []
    net25 = _f(metrics.get("net25_spread"))
    if net25 is not None and net25 <= 0.0:
        cap = _f(sc.get("cost", {}).get("net25_nonpositive_caps_combined_at"))
        if cap is not None and combined > cap:
            combined = cap
            caps.append("NET25_NONPOSITIVE_CAP")
    if stab <= 0.25:
        cap = _f(sc.get("stability", {}).get("instability_caps_combined_at"))
        if cap is not None and combined > cap:
            combined = cap
            caps.append("INSTABILITY_CAP")

    return {
        "combined_score": round(_clamp01(combined), 6),
        "historical_evidence_score": round(hist, 6),
        "robustness_score": round(robust, 6),
        "cost_score": round(cost, 6),
        "stability_score": round(stab, 6),
        "diversification_score": round(divers, 6),
        "forward_evidence_score": round(fwd, 6),
        "independent_information_score": round(indep_score, 6),
        "independent_information": indep,
        "caps_applied": caps,
    }


def _score_historical(m: dict, c: dict) -> float:
    t_ic = _f(m.get("rank_ic_t")) or 0.0
    t_sp = _f(m.get("spread_t")) or 0.0
    fic = _f(c.get("rank_ic_t_full_credit")) or 4.0
    fsp = _f(c.get("spread_t_full_credit")) or 4.0
    wic = _f(c.get("rank_ic_t_weight")) or 0.5
    wsp = _f(c.get("spread_t_weight")) or 0.5
    return _clamp01(_clamp01(t_ic / fic) * wic + _clamp01(t_sp / fsp) * wsp)


def _score_robustness(m: dict, c: dict) -> float:
    sub = _clamp01((_f(m.get("subperiod_consistency")) or 0.0)
                   / (_f(c.get("subperiod_full_credit")) or 1.0))
    reg = _clamp01((_f(m.get("regime_consistency")) or 0.0)
                   / (_f(c.get("regime_full_credit")) or 1.0))
    boot = _clamp01((_f(m.get("bootstrap_positive_fraction")) or 0.0)
                    / (_f(c.get("bootstrap_full_credit")) or 0.99))
    return _clamp01(sub * (_f(c.get("subperiod_weight")) or 0.4)
                    + reg * (_f(c.get("regime_weight")) or 0.35)
                    + boot * (_f(c.get("bootstrap_weight")) or 0.25))


def _score_cost(m: dict, c: dict) -> float:
    net25 = _f(m.get("net25_spread"))
    if net25 is not None and net25 <= 0.0:
        return 0.0
    erosion = _f(m.get("cost_erosion"))
    zero = _f(c.get("cost_erosion_zero_credit")) or 0.75
    if erosion is None:
        return 0.5
    return _clamp01(1.0 - (erosion / zero))


def _score_stability(m: dict, c: dict) -> float:
    dd = _f(m.get("max_drawdown_pct"))
    dz = _f(c.get("drawdown_zero_credit_pct")) or -35.0
    dd_s = 1.0 if dd is None else _clamp01(1.0 - (dd / dz)) if dz != 0 else 0.0
    turn = _f(m.get("turnover_per_rebalance"))
    tz = _f(c.get("turnover_zero_credit")) or 2.0
    turn_s = 1.0 if turn is None else _clamp01(1.0 - (turn / tz)) if tz != 0 else 0.0
    hit = _f(m.get("positive_ic_hit_rate"))
    hz = _f(c.get("hit_rate_full_credit")) or 0.6
    hit_s = 0.0 if hit is None else _clamp01(hit / hz) if hz != 0 else 0.0
    return _clamp01(dd_s * (_f(c.get("drawdown_weight")) or 0.4)
                    + turn_s * (_f(c.get("turnover_weight")) or 0.3)
                    + hit_s * (_f(c.get("hit_rate_weight")) or 0.3))


def _score_diversification(corr_champ: Optional[float],
                           corr_ret: Optional[float], c: dict) -> float:
    vals = [abs(x) for x in (corr_champ, corr_ret) if x is not None]
    if not vals:
        return 0.5  # unknown correlation -> neutral, never full credit
    return _clamp01(1.0 - max(vals))


def _score_forward(fwd: dict, c: dict) -> float:
    obs = _f(fwd.get("observations")) or 0.0
    mat = _f(c.get("maturity_observations")) or 63.0
    frac = _clamp01(obs / mat) if mat else 0.0
    out = _f(fwd.get("net_outperformance_pct_points"))
    full = _f(c.get("outperformance_full_credit_pct_points")) or 3.0
    out_s = 0.5 if out is None else _clamp01(0.5 + 0.5 * (out / full))
    return _clamp01(frac * out_s)


def null_scores() -> dict:
    """Scores for a candidate whose evidence is INCOMPLETE (DATA_HOLD) or that
    has not been evaluated. It carries NO combined score, so it is never ranked
    on the leaderboard - it only appears in the DATA_HOLD / untested views."""
    return {"combined_score": None, "historical_evidence_score": None,
            "robustness_score": None, "cost_score": None,
            "stability_score": None, "diversification_score": None,
            "forward_evidence_score": None,
            "independent_information_score": None, "caps_applied": []}


# --------------------------------------------------------------------------- #
# READY_FOR_MANUAL_REVIEW eligibility (WS5). Never modifies the operating model.
# --------------------------------------------------------------------------- #
def qualifies_for_manual_review(*, metrics: dict, scores: dict, cfg: dict,
                                gate: dict, corr_champion: Optional[float],
                                corr_retained: Optional[float],
                                incremental_net25: Optional[float],
                                forward_observations: int,
                                caveats_displayed: bool) -> dict:
    mr = cfg.get("manual_review_gate", {})
    missing: list[str] = []
    if mr.get("require_historical_gates_pass", True) and not gate.get("complete"):
        missing.append("HISTORICAL_GATES_NOT_COMPLETE")
    if gate.get("target_state") not in (KEEP_FOR_RESEARCH, SHADOW_BOOK_ACTIVE,
                                        READY_FOR_MANUAL_REVIEW):
        missing.append("NOT_RETAINED")
    if mr.get("require_robustness_pass", True) and \
            (_f(scores.get("robustness_score")) or 0.0) < 0.5:
        missing.append("ROBUSTNESS_INSUFFICIENT")
    max_champ = _f(mr.get("max_abs_corr_vs_champion"))
    if corr_champion is not None and max_champ is not None \
            and abs(corr_champion) > max_champ:
        missing.append("TOO_CORRELATED_WITH_CHAMPION")
    max_ret = _f(mr.get("max_abs_corr_vs_retained"))
    if corr_retained is not None and max_ret is not None \
            and abs(corr_retained) > max_ret:
        missing.append("TOO_CORRELATED_WITH_RETAINED")
    if mr.get("require_adds_value_vs_champion", True):
        min_inc = _f(mr.get("min_incremental_net25_spread")) or 0.0
        if incremental_net25 is None or incremental_net25 < min_inc:
            missing.append("NO_INCREMENTAL_VALUE_VS_CHAMPION")
    min_obs = int(_f(mr.get("min_forward_observations")) or 20)
    if forward_observations < min_obs:
        missing.append("INSUFFICIENT_FORWARD_OBSERVATIONS")
    if mr.get("require_all_caveats_displayed", True) and not caveats_displayed:
        missing.append("CAVEATS_NOT_DISPLAYED")
    return {"eligible": not missing, "missing": missing}


# --------------------------------------------------------------------------- #
# Alpha families (WS3). Each candidate is a real factor spec. The owned Stage 5
# price campaign scores the price/momentum family for real; fundamental /
# analyst / event families resolve to an HONEST DATA_HOLD because the owned data
# for those families is snapshot-only, prospective-without-history, or sparse -
# exactly the true state recorded across the source-exhaustion registry. Nothing
# is fabricated; DATA_HOLD carries the specific missing-data blocker.
# --------------------------------------------------------------------------- #
# Owned price features the Stage 5 campaign scores for real.
SCORED_PRICE_FEATURES = ("residual_momentum", "momentum_accel",
                         "vol_scaled_momentum", "short_term_reversal",
                         "low_volatility", "combo_mom_lowvol")
# Features the Stage 5 campaign HOLDS honestly (owned PIT data unavailable).
HELD_PRICE_FEATURES = ("sector_neutral_momentum", "insider_event", "news_event")

# PIT-status vocabulary recorded on every candidate.
PIT_OWNED_PRICE = "PIT_SAFE_OWNED_PRICE"
PIT_SNAPSHOT_ONLY = "SNAPSHOT_ONLY_NOT_PIT"
PIT_PROSPECTIVE = "PROSPECTIVE_NO_OWNED_HISTORY"
PIT_SPARSE = "SPARSE_OWNED_COVERAGE"
PIT_NEEDS_GICS = "NEEDS_POINT_IN_TIME_GICS_SECTOR"

# Non-price data families and the honest DATA_HOLD blocker each maps to.
_DATA_FAMILY_BLOCKER = {
    PIT_SNAPSHOT_ONLY: BLOCK_PIT,
    PIT_PROSPECTIVE: BLOCK_OBSERVATIONS,
    PIT_SPARSE: BLOCK_OBSERVATIONS,
    PIT_NEEDS_GICS: BLOCK_PIT,
}


def default_candidate_specs() -> list[dict]:
    """The seed catalogue across all five families. Deterministic and idempotent
    (each candidate id is a hash of its spec, so re-seeding never duplicates)."""
    price = "S&P 500 Current & Past (Norgate survivorship-safe)"
    specs: list[dict] = []

    # ---- PRICE / MOMENTUM (owned Stage 5 campaign scores these for real) ----
    for feature, name, hz in (
            ("residual_momentum", "63-day residual momentum", 63),
            ("residual_momentum", "21-day residual momentum", 21),
            ("momentum_accel", "Momentum acceleration", 21),
            ("vol_scaled_momentum", "Volatility-scaled momentum", 21),
            ("short_term_reversal", "Short-term reversal", 21),
            ("low_volatility", "Low volatility", 21)):
        specs.append({
            "name": name, "family": FAM_PRICE_MOMENTUM,
            "spec": {"feature": feature, "horizon_days": hz,
                     "rebalance": "monthly", "template": "campaign_price_factor"},
            "data_dependencies": ["owned_norgate_daily_bars"],
            "universe": price, "pit_status": PIT_OWNED_PRICE,
            "component_signals": [feature]})
    specs.append({
        "name": "Sector-relative momentum", "family": FAM_PRICE_MOMENTUM,
        "spec": {"feature": "sector_neutral_momentum", "horizon_days": 21,
                 "rebalance": "monthly", "template": "sector_relative_ranking"},
        "data_dependencies": ["owned_norgate_daily_bars", "point_in_time_gics"],
        "universe": price, "pit_status": PIT_NEEDS_GICS,
        "component_signals": ["sector_neutral_momentum"]})

    # ---- FUNDAMENTAL (owned fundamentals are snapshot-only -> DATA_HOLD) -----
    for feature, name in (
            ("gross_profitability", "Profitability / quality"),
            ("balance_sheet_quality", "Balance-sheet quality"),
            ("earnings_quality_accruals", "Earnings quality (accruals)"),
            ("valuation_composite", "Valuation"),
            ("asset_growth", "Investment / asset growth"),
            ("quality_value", "Quality-value combination")):
        specs.append({
            "name": name, "family": FAM_FUNDAMENTAL,
            "spec": {"feature": feature, "horizon_days": 63,
                     "rebalance": "quarterly", "template": "fundamental_momentum_rank",
                     "requires_data_family": "fundamentals"},
            "data_dependencies": ["owned_eodhd_fundamentals",
                                  "point_in_time_fundamentals"],
            "universe": price, "pit_status": PIT_SNAPSHOT_ONLY,
            "component_signals": [feature]})

    # ---- ANALYST / EARNINGS (prospective collection active, no history yet) --
    for feature, name in (
            ("earnings_estimate_revisions", "Earnings-estimate revisions"),
            ("revision_breadth", "Revision breadth"),
            ("earnings_surprise", "Earnings surprise"),
            ("price_target_revision", "Price-target revision")):
        specs.append({
            "name": name, "family": FAM_ANALYST_EARNINGS,
            "spec": {"feature": feature, "horizon_days": 21,
                     "rebalance": "monthly", "template": "earnings_filing_event_drift",
                     "requires_data_family": "analyst"},
            "data_dependencies": ["eodhd_analyst_vintages"],
            "universe": price, "pit_status": PIT_PROSPECTIVE,
            "component_signals": [feature]})

    # ---- EVENT / INSIDER (sparse owned Form-4 / 8-K coverage -> DATA_HOLD) ---
    for feature, name in (
            ("net_insider_buys", "Open-market insider purchases"),
            ("insider_cluster", "Clustered insider purchases"),
            ("earnings_8k_drift", "8-K earnings-release drift")):
        specs.append({
            "name": name, "family": FAM_EVENT_INSIDER,
            "spec": {"feature": feature, "horizon_days": 21,
                     "rebalance": "monthly", "template": "insider_activity_event_study",
                     "requires_data_family": "event"},
            "data_dependencies": ["sec_form4", "sec_8k"],
            "universe": price, "pit_status": PIT_SPARSE,
            "component_signals": [feature]})

    # ---- COMPOSITES (mom+lowvol is scored; quality legs are DATA_HOLD) -------
    specs.append({
        "name": "Momentum + low volatility", "family": FAM_COMPOSITE,
        "spec": {"feature": "combo_mom_lowvol", "horizon_days": 21,
                 "rebalance": "monthly", "template": "combined_factor_ranking"},
        "data_dependencies": ["owned_norgate_daily_bars"],
        "universe": price, "pit_status": PIT_OWNED_PRICE,
        "component_signals": ["residual_momentum", "low_volatility"]})
    specs.append({
        "name": "Residual momentum + quality", "family": FAM_COMPOSITE,
        "spec": {"feature": "combo_mom_quality", "horizon_days": 63,
                 "rebalance": "quarterly", "template": "combined_factor_ranking",
                 "requires_data_family": "fundamentals"},
        "data_dependencies": ["owned_norgate_daily_bars",
                              "point_in_time_fundamentals"],
        "universe": price, "pit_status": PIT_SNAPSHOT_ONLY,
        "component_signals": ["residual_momentum", "gross_profitability"]})
    return specs


def seed_families(registry: "CandidateRegistry",
                  specs: Optional[list[dict]] = None) -> list[str]:
    """Idempotently register the seed catalogue. Returns the candidate ids."""
    out = []
    for s in (specs if specs is not None else default_candidate_specs()):
        out.append(registry.seed_candidate(
            name=s["name"], family=s["family"], spec=s["spec"],
            data_dependencies=s.get("data_dependencies", []),
            universe=s.get("universe", ""), pit_status=s.get("pit_status", ""),
            component_signals=s.get("component_signals", []),
            parent_candidates=s.get("parent_candidates", [])))
    return out


def seed_stage9_4_catalogue(registry: "CandidateRegistry") -> list[str]:
    """Idempotently register the Stage 9.4 pre-registered price-factor catalogue
    (Track A). Deterministic and deduplicated against the existing catalogue by
    (family, spec_hash); re-seeding never duplicates or resets a candidate."""
    from . import price_factor_catalogue as _cat
    return seed_families(registry, _cat.catalogue_specs())


# --------------------------------------------------------------------------- #
# Evaluation adapter (WS4) - map a Stage 5 score_experiment result row into the
# canonical evaluation contract used by the gates and the scorer. Reuses the
# existing engine; never recomputes rank-IC/spread here.
# --------------------------------------------------------------------------- #
def build_owned_price_campaign(stage8_cfg: dict, *,
                               cost_grid: Optional[list] = None,
                               champion_returns: Optional[list] = None,
                               spy_series: Optional[list] = None,
                               new_factors: bool = False,
                               stage9_4_cfg: Optional[dict] = None) -> dict:
    """Run the bounded owned-price Stage 5 campaign ONCE and return its result.

    Reuses the EXACT panel the Stage 8 production experiment handler uses
    (``experiment_runner.NormalizedStore`` over the owned normalized root). This
    is read-only research: it reads owned normalized price data and writes
    nothing.
    """
    from . import experiment_runner as _er
    ph = (stage8_cfg.get("production_handlers") or {}) \
        if isinstance(stage8_cfg, dict) else {}
    panel_start = ph.get("panel_date_start", "2016-01-01")
    repo_root = Path(__file__).resolve().parents[1]
    s6_path = ph.get("stage6_backfill_config")
    cfg6: dict = {}
    if s6_path:
        p = Path(s6_path)
        if not p.is_absolute():
            p = repo_root / s6_path
        if p.exists():
            cfg6 = json.loads(p.read_text(encoding="utf-8-sig"))
    ingestion_root = cfg6.get("ingestion_root") or \
        str(Path(stage8_cfg.get("stage8_root", ".")) / "ingestion")
    store = _er.NormalizedStore(ingestion_root, max_files=6000, max_symbols=300)
    panel = store.price_panel(panel_start, None)
    result = _er.run_price_factor_campaign(
        panel, cost_grid=cost_grid or [5, 25, 50, 75, 100],
        champion_returns=champion_returns, spy_series=spy_series)
    if new_factors:
        # Stage 9.4: the AGENT's tick also evaluates the pre-registered NEW
        # price-factor catalogue on the SAME owned Norgate panel and enriches
        # each result with orthogonality / regime / walk-forward / selection-bias
        # evidence, so the canonical tournament classifies it in this same pass.
        result = _merge_stage9_4_new_factors(
            result, panel, champion_returns=champion_returns,
            stage9_4_cfg=stage9_4_cfg or {})
    return result


def _merge_stage9_4_new_factors(result: dict, panel: dict, *,
                                champion_returns=None,
                                stage9_4_cfg: Optional[dict] = None) -> dict:
    """Compute the Stage 9.4 pre-registered NEW price factors on the owned panel
    and merge their enriched Stage 5 rows into the campaign result (so the
    existing tick scores them with no change to its evaluation logic). Failure
    isolated: a computation error never breaks the base campaign."""
    try:
        from . import price_factors as _pf
        from . import price_factor_catalogue as _cat
        from . import stage9_4 as _s94
    except Exception:  # pragma: no cover - defensive import guard
        return result
    try:
        # Only the features the pre-registered catalogue actually enables.
        wanted = sorted({c["spec"]["feature"] for c in _cat.computable_specs()})
        nf = _pf.run_new_price_factor_campaign(
            panel, features=wanted, champion_returns=champion_returns)
        fam_size = int((stage9_4_cfg or {}).get("search_family_size_default", 2))
        enriched = []
        for row in nf.get("results", []):
            if row.get("rank_ic_t") is not None:
                row = _s94.enrich_price_factor_row(
                    row, family_size=fam_size, champion_returns=champion_returns,
                    cfg=stage9_4_cfg or {})
            enriched.append(row)
        result.setdefault("results", []).extend(enriched)
        result["stage9_4_new_factor_features"] = wanted
    except Exception as exc:  # noqa: BLE001 - isolate; base campaign stands
        result["stage9_4_new_factor_error"] = type(exc).__name__
    return result


def _cost_net_at(row: dict, cost_bps: int) -> Optional[float]:
    grid = ((row.get("cost_sensitivity") or {}).get("grid") or [])
    for r in grid:
        if r.get("cost_bps") == cost_bps:
            return _f(r.get("net_annualized_return"))
    return _f(row.get("net_annualized_return"))


def row_to_contract_metrics(row: dict, *, survivorship_safe: bool = True) -> dict:
    """Stage 5 result row -> canonical WS4 evaluation-contract metrics."""
    champ_comp = _f(row.get("champion_complementarity"))
    corr_champ = None if champ_comp is None else round(1.0 - champ_comp, 6)
    dd = _f(row.get("max_drawdown"))
    metrics = {
        "rank_ic": _f(row.get("rank_ic_mean")),
        "rank_ic_t": _f(row.get("rank_ic_t")),
        "positive_ic_hit_rate": _f(row.get("rank_ic_positive_ratio")),
        "decile_spread": _f(row.get("decile_spread_mean")),
        "spread_t": _f(row.get("spread_t")),
        "oos_ic_mean": _f(row.get("oos_ic_mean")),
        "gross_return": _f(row.get("gross_annualized_return")),
        "net_return": _f(row.get("net_annualized_return")),
        "net25_spread": _cost_net_at(row, 25),
        "net50_spread": _cost_net_at(row, 50),
        "turnover_per_rebalance": _f(row.get("turnover")),
        "max_drawdown_pct": (dd * 100.0) if dd is not None else None,
        "worst_period_return_pct": None,
        "subperiod_consistency": _f(row.get("subperiod_consistency")),
        "regime_consistency": _f(row.get("regime_consistency")),
        "bootstrap_positive_fraction": _f(row.get("rank_ic_positive_ratio")),
        "cost_erosion": _f(row.get("cost_erosion_ratio")),
        "cost_flips_sign": bool(row.get("cost_flips_sign")),
        "corr_vs_champion": corr_champ,
        "scored_periods": _f(row.get("periods")),
        "min_names_per_period": _f(row.get("universe")),
        "coverage_pct": 100.0,
        "sector_concentration_pct": None,
        "point_in_time_valid": (not bool(row.get("leakage_warning"))),
        "survivorship_safe": bool(survivorship_safe),
        "lookahead_contamination": bool(row.get("leakage_warning")),
        "spy_excess_annualized": _f(row.get("spy_excess_annualized")),
        "beats_null_control": bool(row.get("beats_null_control")),
        "sharpe": _f(row.get("sharpe")),
        "annualized_vol": _f(row.get("annualized_vol")),
        "data_boundaries": {"universe": _f(row.get("universe")),
                            "scored_periods": _f(row.get("periods")),
                            "point_in_time_valid":
                                not bool(row.get("leakage_warning")),
                            "survivorship_safe": bool(survivorship_safe)},
    }
    # Stage 9.4 (additive, never a gate input): pass through orthogonality /
    # regime / walk-forward / bootstrap / selection-bias evidence when the row
    # was enriched. The scorer reads partial_rank_ic / incremental_net_return for
    # the INDEPENDENT_INFORMATION sub-score; the nested dicts are surfaced only.
    for k in ("partial_rank_ic", "incremental_net_return",
              "independent_information_score", "stage9_4_regime",
              "stage9_4_bootstrap", "stage9_4_walk_forward",
              "stage9_4_deflated_sharpe", "stage9_4_selection",
              "stage9_4_independent_information"):
        if k in row:
            metrics[k] = row[k]
    if row.get("corr_vs_champion") is not None:
        metrics["corr_vs_champion"] = row["corr_vs_champion"]
    return metrics


def _index_campaign(campaign_result: Optional[dict]) -> tuple[dict, set]:
    feature_metrics: dict[str, dict] = {}
    for row in (campaign_result or {}).get("results", []):
        f = row.get("feature")
        if f and row.get("rank_ic_t") is not None:
            feature_metrics[f] = row
    held = {h.get("feature") for h in (campaign_result or {}).get("held", [])}
    return feature_metrics, held


def _data_hold_metrics(reason_blocker: str) -> dict:
    """Metrics dict for a candidate that cannot be evaluated honestly yet."""
    return {"coverage_pct": None, "scored_periods": None,
            "min_names_per_period": None, "point_in_time_valid": True,
            "survivorship_safe": True, "lookahead_contamination": False,
            "data_hold_blocker": reason_blocker}


# --------------------------------------------------------------------------- #
# WS4 - completed-experiment ingestion. A Stage 5 result row (from the owned
# price campaign OR a completed Stage 8 queue job) is imported at most once,
# keyed by a stable content hash, and associated to its candidate by feature.
# --------------------------------------------------------------------------- #
def result_hash_for(*, source: str, feature: Optional[str], row: dict,
                    evidence_date: Optional[str] = None,
                    job_id: Optional[str] = None,
                    candidate_id: Optional[str] = None) -> str:
    """Deterministic content hash of one completed experiment result AS APPLIED
    TO ONE CANDIDATE. The same result for the same candidate on the same
    evidence date hashes identically (idempotent - never re-evaluated); a
    changed metric, a new evidence date, or a DIFFERENT candidate that happens
    to share the feature hashes differently (so every candidate is classified,
    and two candidates sharing a feature are both evaluated)."""
    payload = {
        "source": source, "feature": feature, "evidence_date": evidence_date,
        "job_id": job_id, "candidate_id": candidate_id,
        "rank_ic_t": round(_f(row.get("rank_ic_t")) or 0.0, 6),
        "spread_t": round(_f(row.get("spread_t")) or 0.0, 6),
        "rank_ic_mean": round(_f(row.get("rank_ic_mean")) or 0.0, 6),
        "periods": _f(row.get("periods")),
        "universe": _f(row.get("universe")),
        "net_annualized_return": round(_f(row.get("net_annualized_return"))
                                       or 0.0, 6),
        "decision": row.get("decision"),
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _candidate_for_feature(registry: "CandidateRegistry",
                           feature: str) -> Optional[dict]:
    """The first registry candidate whose spec targets ``feature`` (catalogue is
    small and bounded)."""
    if not feature:
        return None
    for c in registry.list():
        if (c.get("spec") or {}).get("feature") == feature:
            return c
    return None


# States a completed follow-up result may legally advance (WS4): never re-opens a
# REJECTED / KEEP / SHADOW candidate automatically - only PROPOSED/TESTING/DATA_HOLD.
_INGEST_ADVANCE_FROM = frozenset({PROPOSED, TESTING, DATA_HOLD})


def ingest_completed_experiments(registry: "CandidateRegistry", cfg: dict, *,
                                 completed: Iterable[dict],
                                 source: str = "stage8_queue_job",
                                 evidence_date: Optional[str] = None) -> dict:
    """Idempotently import completed Stage 8 experiment jobs / results.

    Each item is a dict carrying a ``feature`` (or a ``spec``/``payload`` that
    contains one), a Stage 5 result ``row`` (or ``result``/``metrics``) and an
    optional ``job_id``. Already-processed results are skipped; a genuinely new
    completed result updates the matching candidate (records the evaluation,
    populates ``experiment_ids`` + ``latest_evidence_date`` and, for an
    unresolved candidate, applies the gate transition). A malformed result
    produces a specific diagnostic and never stops the ingestion. Read-only
    w.r.t. every operational trading ledger."""
    imported: list[dict] = []
    skipped: list[dict] = []
    malformed: list[dict] = []
    for item in (completed or []):
        job_id = None
        try:
            if not isinstance(item, dict):
                malformed.append({"job_id": None, "reason": "not_a_dict"})
                registry.record_change("EXPERIMENT_INGEST_MALFORMED", None,
                                       {"reason": "not_a_dict"})
                continue
            job_id = item.get("job_id")
            spec = item.get("spec") or (item.get("payload") or {}).get("spec") \
                or {}
            feature = item.get("feature") or spec.get("feature")
            row = item.get("row") or item.get("result") or item.get("metrics") \
                or {}
            if not feature or not isinstance(row, dict) \
                    or row.get("rank_ic_t") is None:
                malformed.append({"job_id": job_id,
                                  "reason": "missing_feature_or_metrics"})
                registry.record_change("EXPERIMENT_INGEST_MALFORMED", None,
                                       {"job_id": job_id,
                                        "reason": "missing_feature_or_metrics"})
                continue
            cand = _candidate_for_feature(registry, feature)
            cand_id = cand["candidate_id"] if cand else None
            rhash = result_hash_for(source=source, feature=feature, row=row,
                                    evidence_date=evidence_date, job_id=job_id,
                                    candidate_id=cand_id)
            if registry.is_processed(rhash):
                skipped.append({"job_id": job_id, "feature": feature,
                                "reason": "already_processed"})
                continue
            if cand is None:
                # nothing to associate; still mark processed so a repeated
                # orphan job is not rescanned every cycle.
                registry.mark_processed(result_hash=rhash, candidate_id=None,
                                        source=source, job_id=job_id,
                                        feature=feature,
                                        evidence_date=evidence_date)
                skipped.append({"job_id": job_id, "feature": feature,
                                "reason": "no_candidate_for_feature"})
                continue
            cid = cand["candidate_id"]
            metrics = row_to_contract_metrics(row)
            gate = classify_evidence(metrics, cfg)
            if gate.get("complete"):
                scores = score_candidate(
                    metrics, cfg,
                    corr_champion=_f(metrics.get("corr_vs_champion")))
            else:
                scores = null_scores()
            pit = metrics.get("data_boundaries") or {}
            registry.record_evaluation(cid, metrics=metrics, scores=scores,
                                       gate=gate, pit=pit,
                                       experiment_id=(job_id or rhash),
                                       evidence_date=evidence_date)
            prev_state = cand["lifecycle_state"]
            moved = False
            if prev_state in _INGEST_ADVANCE_FROM:
                _apply_gate_transition(registry, cid, prev_state, gate)
                moved = registry.get(cid)["lifecycle_state"] != prev_state
            registry.mark_processed(result_hash=rhash, candidate_id=cid,
                                    source=source, job_id=job_id,
                                    feature=feature, evidence_date=evidence_date)
            imported.append({"job_id": job_id, "candidate_id": cid,
                             "feature": feature, "from": prev_state,
                             "to": registry.get(cid)["lifecycle_state"],
                             "moved": moved})
        except Exception as exc:  # noqa: BLE001 - one bad result never stops
            malformed.append({"job_id": job_id, "reason": type(exc).__name__})
            registry.record_change("EXPERIMENT_INGEST_ERROR", None,
                                   {"job_id": job_id,
                                    "error": type(exc).__name__})
    return {"imported": len(imported), "skipped": len(skipped),
            "malformed": len(malformed), "items": imported}


# --------------------------------------------------------------------------- #
# Shadow books (WS8) - read-only research shadow books built on immutable,
# first-write-wins, NO-RETROACTIVE snapshots (the forward_prediction_skill
# discipline). They live under the Stage 8 shadow-book root and can NEVER touch
# the operating portfolio.
# --------------------------------------------------------------------------- #
SHADOW_LABEL = "RESEARCH SHADOW BOOK - NOT THE OPERATING PORTFOLIO"


class RetroactiveError(ValueError):
    """Raised when a shadow book is asked to record a non-forward (retroactive)
    snapshot or mark. Forward history is never backfilled (WS8/WS13)."""


class ShadowBook:
    """One immutable, read-only research shadow book on disk.

    Stores a single first-write-wins inception snapshot plus an append-only,
    strictly-monotonic series of daily valuation marks. A recorded mark is never
    rewritten and a date at or before the latest recorded date is refused. The
    book only ever writes under ``root/<shadow_book_id>`` - it has no code path
    to the operational ledger, an order, a fill, or any cash/holding.
    """

    def __init__(self, root: str | Path, shadow_book_id: str) -> None:
        self.dir = Path(root) / shadow_book_id
        self.shadow_book_id = shadow_book_id
        self.file = self.dir / "shadow_book.json"

    def _load(self) -> dict:
        if self.file.exists():
            return json.loads(self.file.read_text(encoding="utf-8"))
        return {}

    def _write(self, doc: dict) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        tmp = self.file.with_suffix(".json.tmp")
        tmp.write_text(canonical_json(doc), encoding="utf-8")
        tmp.replace(self.file)

    def inception(self, *, candidate_id: str, inception_date: str,
                  membership: list, benchmark: str, cost_bps: float,
                  spec: dict, notional: float = 100000.0) -> dict:
        """Create the immutable inception snapshot (first-write-wins)."""
        doc = self._load()
        if doc.get("inception"):
            return doc  # first write wins; never restated
        doc = {
            "label": SHADOW_LABEL, "shadow_book_id": self.shadow_book_id,
            "candidate_id": candidate_id, "read_only": True,
            "operating_portfolio": False,
            "inception": {"date": inception_date, "membership": list(membership),
                          "benchmark": benchmark, "cost_bps": cost_bps,
                          "spec": spec, "notional": notional},
            "marks": [],
        }
        self._write(doc)
        return doc

    def record_mark(self, *, date: str, nav: float,
                    benchmark_close: Optional[float] = None,
                    turnover: Optional[float] = None) -> dict:
        """Append one immutable daily mark. Refuses a retroactive date."""
        doc = self._load()
        if not doc.get("inception"):
            raise RetroactiveError("shadow book has no inception snapshot")
        marks = doc.get("marks", [])
        if date <= doc["inception"]["date"]:
            raise RetroactiveError(
                "mark date %s is not after inception %s"
                % (date, doc["inception"]["date"]))
        if marks and date <= marks[-1]["date"]:
            raise RetroactiveError(
                "mark date %s is not after latest recorded mark %s"
                % (date, marks[-1]["date"]))
        marks.append({"date": date, "nav": _f(nav),
                      "benchmark_close": _f(benchmark_close),
                      "turnover": _f(turnover)})
        doc["marks"] = marks
        self._write(doc)
        return doc

    def replay(self) -> dict:
        """Deterministic read-only P&L/drawdown/turnover from stored marks."""
        doc = self._load()
        if not doc.get("inception"):
            return {"status": "EMPTY"}
        notional = _f(doc["inception"].get("notional")) or 100000.0
        marks = doc.get("marks", [])
        navs = [notional] + [m["nav"] for m in marks if m.get("nav") is not None]
        peak = navs[0]
        max_dd = 0.0
        for v in navs:
            peak = max(peak, v)
            if peak:
                max_dd = min(max_dd, (v - peak) / peak)
        last = navs[-1]
        turns = [m["turnover"] for m in marks if m.get("turnover") is not None]
        return {
            "status": "ACTIVE", "label": SHADOW_LABEL,
            "inception_date": doc["inception"]["date"],
            "forward_observations": len(marks),
            "cumulative_pnl": round(last - notional, 4),
            "cumulative_return_pct": round(100.0 * (last / notional - 1.0), 4)
            if notional else None,
            "max_drawdown_pct": round(100.0 * max_dd, 4),
            "avg_turnover": round(sum(turns) / len(turns), 6) if turns else None,
            "benchmark": doc["inception"].get("benchmark"),
        }


def maybe_activate_shadow_books(registry: "CandidateRegistry", cfg: dict, *,
                                inception_provider: Optional[Callable] = None,
                                evidence_date: Optional[str] = None) -> list[dict]:
    """Activate a read-only shadow book for the strongest KEEP_FOR_RESEARCH
    candidates that pass the shadow-score floor and do not already have one."""
    sb = cfg.get("shadow_books", {})
    root = sb.get("shadow_book_root") or cfg.get("shadow_book_root")
    if not root:
        return []
    floor = _f(sb.get("min_combined_score_for_shadow")) or 0.55
    max_active = int(_f(sb.get("max_active_shadow_books")) or 3)
    active = [s for s in registry.list_shadow_books() if s["status"] == "ACTIVE"]
    if len(active) >= max_active:
        return []
    out = []
    keeps = sorted(registry.list(state=KEEP_FOR_RESEARCH),
                   key=lambda c: -(_f(c.get("combined_score")) or 0.0))
    for cand in keeps:
        if len(active) + len(out) >= max_active:
            break
        if (_f(cand.get("combined_score")) or 0.0) < floor:
            continue
        if cand.get("active_shadow_book_id"):
            continue
        sbid = "sb_%s" % cand["candidate_id"]
        inception_date = evidence_date or registry.now()[:10]
        membership: list = []
        spec = cand.get("spec", {})
        if inception_provider is not None:
            info = inception_provider(cand) or {}
            membership = info.get("membership", [])
            spec = info.get("spec", spec)
        book = ShadowBook(root, sbid)
        book.inception(candidate_id=cand["candidate_id"],
                       inception_date=inception_date, membership=membership,
                       benchmark=sb.get("benchmark", "SPY"),
                       cost_bps=_f(sb.get("cost_bps_round_trip")) or 50.0,
                       spec=spec)
        registry.create_shadow_book(
            cand["candidate_id"], sbid, inception_date=inception_date,
            meta={"label": SHADOW_LABEL, "combined_score":
                  cand.get("combined_score"), "family": cand.get("family")})
        registry.transition(cand["candidate_id"], SHADOW_BOOK_ACTIVE)
        out.append({"shadow_book_id": sbid, "candidate_id": cand["candidate_id"],
                    "inception_date": inception_date})
    return out


# Shadow-book daily-mark diagnostics (WS5).
SHADOW_MARK_MISSING = "SHADOW_MARK_COVERAGE_MISSING"


def advance_shadow_books(registry: "CandidateRegistry", cfg: dict, *,
                         mark_provider: Callable, evidence_date: str) -> list[dict]:
    """Advance every ACTIVE research shadow book by ONE immutable daily mark for
    ``evidence_date`` using the completed-close/snapshot rules (WS5).

    ``mark_provider(candidate_id, date) -> {nav, benchmark_close, turnover}`` or
    ``None`` when no immutable completed-close mark exists for that date (a
    non-market day, a holiday or a genuine coverage gap). Guarantees:
      * no retroactive history and no duplicate same-date mark (first-write-wins);
      * a non-market day / missing price creates NO false observation - it yields
        an exact ``SHADOW_MARK_COVERAGE_MISSING`` diagnostic instead;
      * one book's failure never stops the others;
      * never mutates any operational holding / cash / order / fill.
    """
    sb = cfg.get("shadow_books", {}) if isinstance(cfg, dict) else {}
    root = sb.get("shadow_book_root") or cfg.get("shadow_book_root")
    out: list[dict] = []
    if not root or evidence_date is None:
        return out
    for rec in registry.list_shadow_books():
        if rec.get("status") != "ACTIVE":
            continue
        sbid = rec["shadow_book_id"]
        cand_id = rec["candidate_id"]
        try:
            book = ShadowBook(root, sbid)
            doc = book._load()
            if not doc.get("inception"):
                out.append({"shadow_book_id": sbid, "status": "NO_INCEPTION"})
                continue
            marks = doc.get("marks", [])
            last_date = marks[-1]["date"] if marks else doc["inception"]["date"]
            if evidence_date <= last_date:
                out.append({"shadow_book_id": sbid, "status": "NO_ADVANCE",
                            "reason": "not_after_last_mark",
                            "last_date": last_date})
                continue
            try:
                mark = mark_provider(cand_id, evidence_date)
            except Exception as exc:  # noqa: BLE001 - provider failure isolated
                out.append({"shadow_book_id": sbid, "status": "PROVIDER_ERROR",
                            "error": type(exc).__name__})
                continue
            if not mark or _f(mark.get("nav")) is None:
                out.append({"shadow_book_id": sbid, "status": "DATA_HOLD",
                            "blocker": SHADOW_MARK_MISSING, "date": evidence_date})
                registry.record_change(SHADOW_MARK_MISSING, cand_id,
                                       {"shadow_book_id": sbid,
                                        "date": evidence_date})
                continue
            try:
                book.record_mark(date=evidence_date, nav=_f(mark.get("nav")),
                                 benchmark_close=_f(mark.get("benchmark_close")),
                                 turnover=_f(mark.get("turnover")))
            except RetroactiveError as exc:
                out.append({"shadow_book_id": sbid,
                            "status": "NO_ADVANCE_RETROACTIVE",
                            "reason": str(exc)[:120]})
                continue
            registry.record_change("SHADOW_MARK_RECORDED", cand_id,
                                   {"shadow_book_id": sbid,
                                    "date": evidence_date})
            out.append({"shadow_book_id": sbid, "status": "ADVANCED",
                        "date": evidence_date})
        except Exception as exc:  # noqa: BLE001 - one book never stops the rest
            out.append({"shadow_book_id": sbid, "status": "ERROR",
                        "error": type(exc).__name__})
    return out


# --------------------------------------------------------------------------- #
# Automatic next-experiment generation (WS7) - bounded, deduplicated, enqueued
# into the SHARED Stage 8 durable queue (never a second queue).
# --------------------------------------------------------------------------- #
def generate_next_experiments(registry: "CandidateRegistry", cfg: dict, *,
                              queue=None, now: Optional[str] = None) -> dict:
    """For retained candidates, enqueue the next highest-value experiments.

    Deterministically deduplicated by spec hash (both in the registry's
    generated-experiments table and, when a queue is given, by the queue's own
    idempotent ``dedupe_key``). Bounded per cycle. Records the variant so
    per-family variant counts are auditable. Never optimizes endlessly against
    one sample: each strategy poses a distinct next question.
    """
    from . import autonomous_research as _ar
    ax = cfg.get("auto_experiments", {})
    budget = int(_f(ax.get("max_new_experiments_per_cycle")) or 6)
    max_neighbors = int(_f(ax.get("max_parameter_neighbors_per_candidate")) or 2)
    horizons = ax.get("horizons_trading_days", [21, 63, 126])
    rebals = ax.get("rebalance_frequencies_trading_days", [21, 63])
    cost_grid = ax.get("cost_grid_bps", [5, 25, 50, 75, 100])

    _CATEGORY = {
        "cost_sensitivity": _ar.CAT_ROBUSTNESS_TEST,
        "low_correlation_combination": _ar.CAT_SIGNAL_COMBINATION,
    }
    generated: list[dict] = []
    retained = registry.list(state=KEEP_FOR_RESEARCH) + \
        registry.list(state=SHADOW_BOOK_ACTIVE)
    for cand in retained:
        if len(generated) >= budget:
            break
        feature = cand.get("spec", {}).get("feature")
        base_hz = int(_f(cand.get("spec", {}).get("horizon_days")) or 21)
        plans = _variant_plans(cand, feature, base_hz, horizons, rebals,
                               cost_grid, max_neighbors)
        for strat, vspec in plans:
            if len(generated) >= budget:
                break
            sh = registry.try_register_generated(
                strategy=strat, spec=vspec, candidate_id=cand["candidate_id"])
            if sh is None:
                continue  # already generated this exact spec (dedup)
            job_id = None
            if queue is not None:
                cat = _CATEGORY.get(strat, _ar.CAT_EXPERIMENT)
                job_id = queue.enqueue(
                    cat, lane="tournament.%s" % strat,
                    payload={"tournament": True,
                             "candidate_id": cand["candidate_id"],
                             "strategy": strat, "spec": vspec},
                    priority=3, origin="stage9-tournament")
            generated.append({"strategy": strat,
                              "candidate_id": cand["candidate_id"],
                              "spec_hash": sh, "job_id": job_id,
                              "family": cand.get("family")})

    # Weakest-gate follow-ups for UNRESOLVED (DATA_HOLD) candidates so the queue
    # always holds useful future work while unresolved candidates remain (WS11
    # step 8). Deduplicated by the blocked data dependency, so exactly one
    # acquisition/validation follow-up is queued per blocked family - never an
    # endless re-optimisation and never a duplicate.
    if len(generated) < budget:
        seen_dep: set[str] = set()
        for cand in registry.list(state=DATA_HOLD):
            if len(generated) >= budget:
                break
            deps = cand.get("data_dependencies") or ["owned_data"]
            dep = deps[-1]  # the most-specific (blocking) dependency
            if dep in seen_dep:
                continue
            seen_dep.add(dep)
            vspec = {"address": "weakest_gate", "data_dependency": dep,
                     "blocker": cand.get("blocker")}
            sh = registry.try_register_generated(
                strategy="address_weakest_gate", spec=vspec,
                candidate_id=cand["candidate_id"])
            if sh is None:
                continue
            job_id = None
            if queue is not None:
                job_id = queue.enqueue(
                    _ar.CAT_DATA_VALIDATION,
                    lane="tournament.address_weakest_gate",
                    payload={"tournament": True,
                             "candidate_id": cand["candidate_id"],
                             "strategy": "address_weakest_gate", "spec": vspec},
                    priority=2, origin="stage9-tournament")
            generated.append({"strategy": "address_weakest_gate",
                              "candidate_id": cand["candidate_id"],
                              "spec_hash": sh, "job_id": job_id,
                              "family": cand.get("family")})
    return {"generated": generated, "count": len(generated),
            "queue_used": queue is not None}


def generate_stage9_4_followups(registry: "CandidateRegistry", cfg: dict, *,
                                queue=None) -> dict:
    """Stage 9.4 release closure: the AGENT autonomously enqueues a BOUNDED,
    pre-registered price-factor REVALIDATION experiment onto the SHARED durable
    queue so the generate -> claim -> execute -> persist -> ingest loop closes
    through the production CAT_EXPERIMENT handler even when no candidate is
    retained.

    It is a genuine canonical queue experiment (category EXPERIMENT, origin
    ``stage9-tournament``, lane ``tournament.stage9_4_revalidation``) - never a
    second queue, handler or worker. Deterministic, spec-deduplicated and capped
    at ``stage9_4.followups.max_per_cycle`` NEW jobs per cycle; it targets only
    computable OWNED-price catalogue candidates that already carry COMPLETE
    evidence (state REJECTED), re-running the candidate's OWN registered horizon
    (a reproduction, not a parameter search). Config-gated (disabled by default in
    offline/acceptance configs). Never lowers a gate, never forces a KEEP, never
    mutates any operational trading state. When disabled or nothing is eligible it
    returns an empty, honest result."""
    from . import autonomous_research as _ar
    from . import price_factors as _pf
    s94 = (cfg.get("stage9_4") or {}) if isinstance(cfg, dict) else {}
    fu = s94.get("followups") or {}
    if not fu.get("enabled"):
        return {"generated": [], "count": 0, "enabled": False}
    max_fu = max(0, int(_f(fu.get("max_per_cycle")) or 1))
    computable = set(_pf.COMPUTABLE_FEATURES)
    generated: list[dict] = []
    # Deterministic order: the distinct computable features, each mapped to the
    # canonical registry candidate ingestion will associate the result with.
    for feature in sorted(computable):
        if len(generated) >= max_fu:
            break
        cand = _candidate_for_feature(registry, feature)
        if cand is None or cand.get("lifecycle_state") != REJECTED:
            continue
        spec0 = cand.get("spec") or {}
        vspec = {"feature": feature,
                 "horizon_days": int(_f(spec0.get("horizon_days")) or 21),
                 "rebalance": spec0.get("rebalance", "monthly"),
                 "template": "campaign_price_factor",
                 "study_kind": "stage9_4_revalidation",
                 "revalidation_of": cand["candidate_id"]}
        sh = registry.try_register_generated(
            strategy="stage9_4_revalidation", spec=vspec,
            candidate_id=cand["candidate_id"])
        if sh is None:
            continue  # already generated this exact revalidation (dedup)
        job_id = None
        if queue is not None:
            job_id = queue.enqueue(
                _ar.CAT_EXPERIMENT, lane="tournament.stage9_4_revalidation",
                payload={"tournament": True,
                         "candidate_id": cand["candidate_id"],
                         "strategy": "stage9_4_revalidation",
                         "feature": feature, "spec": vspec},
                priority=3, origin="stage9-tournament")
        generated.append({"strategy": "stage9_4_revalidation",
                          "candidate_id": cand["candidate_id"],
                          "feature": feature, "spec_hash": sh,
                          "job_id": job_id})
    return {"generated": generated, "count": len(generated), "enabled": True}


def generate_stage9_5_fundamental_followups(registry: "CandidateRegistry",
                                            cfg: dict, *, queue=None) -> dict:
    """Stage 9.5: the AGENT autonomously enqueues a BOUNDED, pre-registered PIT
    FUNDAMENTAL experiment onto the shared durable queue - but ONLY for a
    fundamental candidate whose owned PIT companyfacts coverage has been MEASURED
    sufficient (the weakest-gate recorded ``sufficient=True`` on the candidate).

    It is a genuine canonical queue experiment (category EXPERIMENT, origin
    ``stage9-tournament``, lane ``tournament.stage9_5_fundamental``) that the
    production handler evaluates against a real cross-sectional Stage 9 evidence
    contract over owned PIT fundamentals + owned survivorship-safe prices, imported
    exactly once. Deterministic, spec-deduplicated and capped at
    ``stage9_5.fundamental_experiments.max_per_cycle``. Coverage-gated: while owned
    coverage is insufficient (the honest live state) NOTHING is generated and the
    candidate stays DATA_HOLD - coverage alone never promotes it. Config-gated;
    never lowers a gate, never forces a KEEP, never mutates operational state."""
    from . import autonomous_research as _ar
    from . import fundamental_readiness as _fr
    s95 = (cfg.get("stage9_5") or {}) if isinstance(cfg, dict) else {}
    fx = s95.get("fundamental_experiments") or {}
    if not s95.get("enabled") or not fx.get("enabled"):
        return {"generated": [], "count": 0, "enabled": False}
    signals = ((s95.get("fundamental_mvp") or {}).get("signals")
               or ["gross_profitability", "asset_growth",
                   "balance_sheet_quality"])
    max_fu = max(0, int(_f(fx.get("max_per_cycle")) or 1))
    hz = int(_f(fx.get("horizon_days")) or 63)
    rb = fx.get("rebalance", "quarterly")
    # Stage 9.5B SAFETY SWITCH (Blocker 7) / Stage 10.2 MEASURED-AUTO unlock. A
    # HISTORICAL fundamental experiment is generated ONLY when a survivorship-safe
    # historical ticker->CIK mapping AND the measured per-rebalance PIT readiness
    # both pass - never a hand-set flag; below the measured coverage threshold the
    # gate stays refused honestly, and once the contract genuinely passes it
    # unlocks AUTOMATICALLY here. Eligibility is measured PER SIGNAL: a single
    # signal whose owned historical PIT coverage is below threshold (e.g.
    # gross_profitability) can NEVER veto the signals that genuinely pass (e.g.
    # asset_growth), and no threshold is ever lowered. When a measured identity
    # store exists the per-signal readiness is read from the durable snapshot the
    # fundamentals.coverage_measure job persisted (cheap; no PIT rebuild in the
    # tick), else computed live off the owned companyfacts PIT store built from
    # the resolved historical universe. When no measured identity store is
    # configured the exact legacy config-flag behaviour is preserved.
    _id_store = _fr.open_identity_store(cfg)
    eligible: dict = {}
    if _id_store is not None:
        activation_permits = (_fr.activation_mode(fx)
                              == _fr.ACTIVATION_MEASURED_AUTO)
        map_avail = False
        snap = {}
        try:
            raw = _id_store.get_meta("stage102_readiness_snapshot")
            snap = json.loads(raw) if raw else {}
        except Exception:  # noqa: BLE001 - snapshot read never fatal
            snap = {}
        if snap:
            for sig in signals:
                d = snap.get(sig) or {}
                map_avail = map_avail or bool(d.get("mapping_available"))
                eligible[sig] = bool(activation_permits
                                     and d.get("mapping_available")
                                     and d.get("sufficient"))
        else:
            # No durable snapshot yet: compute the per-signal gate live off the
            # owned companyfacts PIT store (built once and reused).
            ps = _fr.open_companyfacts_pit_store(cfg, store=_id_store)
            for sig in signals:
                g = _fr.historical_fundamental_experiment_allowed(
                    cfg, readiness=None, store=_id_store, signal=sig,
                    pit_store=ps)
                map_avail = map_avail or bool(g["mapping_available"])
                eligible[sig] = bool(g["allowed"])
        if not any(eligible.values()):
            # Honest blocker (no extra heavy PIT rebuild): every configured
            # signal's measured survivorship-safe historical PIT readiness is
            # below threshold, so ZERO jobs are generated and the switch stays
            # closed - never flipped by a below-threshold signal.
            return {"generated": [], "count": 0, "enabled": True,
                    "blocker": _fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY,
                    "historical_evaluation_enabled": bool(
                        fx.get("historical_evaluation_enabled")),
                    "historical_mapping_available": map_avail,
                    "reason": ("measured per-rebalance PIT readiness not yet "
                               "sufficient for any configured signal on the "
                               "survivorship-safe historical universe")}
    else:
        gate = _fr.historical_fundamental_experiment_allowed(
            cfg, readiness=None, store=None)
        if not gate["allowed"]:
            return {"generated": [], "count": 0, "enabled": True,
                    "blocker": gate["diagnostic"],
                    "historical_evaluation_enabled": gate[
                        "historical_evaluation_enabled"],
                    "historical_mapping_available": gate["mapping_available"],
                    "reason": gate["reason"]}
    generated: list[dict] = []
    for feature in sorted(signals):
        if len(generated) >= max_fu:
            break
        cand = _candidate_for_feature(registry, feature)
        if cand is None:
            continue
        if cand.get("lifecycle_state") not in (DATA_HOLD, PROPOSED, TESTING):
            continue
        if _id_store is not None:
            # Measured per-signal survivorship-safe historical PIT readiness IS
            # the eligibility (coverage alone still never promotes lifecycle).
            if not eligible.get(feature):
                continue
        else:
            # Legacy/test path: the weakest-gate-persisted owned coverage.
            cov = registry.latest_data_coverage(cand["candidate_id"]) or {}
            if not cov.get("sufficient"):
                continue
        vspec = {"feature": feature, "horizon_days": hz, "rebalance": rb,
                 "template": "fundamental_momentum_rank",
                 "study_kind": "stage9_5_fundamental",
                 "fundamental_of": cand["candidate_id"]}
        sh = registry.try_register_generated(
            strategy="stage9_5_fundamental", spec=vspec,
            candidate_id=cand["candidate_id"])
        if sh is None:
            continue  # already generated this exact fundamental experiment
        job_id = None
        if queue is not None:
            job_id = queue.enqueue(
                _ar.CAT_EXPERIMENT, lane="tournament.stage9_5_fundamental",
                payload={"tournament": True,
                         "candidate_id": cand["candidate_id"],
                         "strategy": "stage9_5_fundamental",
                         "feature": feature, "spec": vspec},
                priority=3, origin="stage9-tournament")
        generated.append({"strategy": "stage9_5_fundamental",
                          "candidate_id": cand["candidate_id"],
                          "feature": feature, "spec_hash": sh, "job_id": job_id})
    return {"generated": generated, "count": len(generated), "enabled": True}


def _variant_plans(cand: dict, feature: Optional[str], base_hz: int,
                   horizons, rebals, cost_grid, max_neighbors) -> list[tuple]:
    """Deterministic ordered (strategy, spec) plans for one candidate."""
    plans: list[tuple] = []
    if not feature:
        return plans
    # 1. additional horizons (parameter neighbors, bounded)
    n = 0
    for hz in horizons:
        if hz == base_hz or n >= max_neighbors:
            continue
        plans.append(("additional_horizon",
                      {"feature": feature, "horizon_days": hz,
                       "rebalance": "monthly"}))
        n += 1
    # 2. alternative rebalance frequency
    for rb in rebals:
        plans.append(("alternative_rebalance_frequency",
                      {"feature": feature, "horizon_days": base_hz,
                       "rebalance_days": rb}))
        break
    # 3. cost sensitivity
    plans.append(("cost_sensitivity",
                  {"feature": feature, "horizon_days": base_hz,
                   "cost_grid_bps": list(cost_grid)}))
    # 4. sector-neutral variant
    plans.append(("sector_neutral_variant",
                  {"feature": feature, "horizon_days": base_hz,
                   "sector_neutral": True}))
    # 5. ex-financials universe variant
    plans.append(("ex_financials_universe_variant",
                  {"feature": feature, "horizon_days": base_hz,
                   "universe_variant": "ex_financials"}))
    # 6. low-correlation combination with the candidate's own component legs
    legs = cand.get("component_signals") or []
    if len(legs) >= 2:
        plans.append(("low_correlation_combination",
                      {"features": sorted(legs), "horizon_days": base_hz}))
    return plans


def variants_by_family(registry: "CandidateRegistry") -> dict:
    out: dict[str, int] = {}
    for r in registry._conn.execute(
            "SELECT c.family f, COUNT(*) n FROM generated_experiments g "
            "JOIN candidates c ON c.candidate_id=g.candidate_id "
            "GROUP BY c.family"):
        out[r["f"]] = int(r["n"])
    return out


# --------------------------------------------------------------------------- #
# Leaderboard (WS6) - transparent, reproducible, decomposable.
# --------------------------------------------------------------------------- #
_SUBSCORE_LABEL = {
    "historical_evidence_score": "historical evidence",
    "robustness_score": "robustness", "cost_score": "cost efficiency",
    "stability_score": "stability", "diversification_score": "diversification",
    "forward_evidence_score": "forward evidence",
}


def _strongest_weakest(scores: dict) -> tuple[str, str]:
    items = [(k, _f(v)) for k, v in scores.items()
             if k in _SUBSCORE_LABEL and v is not None]
    if not items:
        return "not yet evaluated", "not yet evaluated"
    strong = max(items, key=lambda kv: kv[1])
    weak = min(items, key=lambda kv: kv[1])
    return _SUBSCORE_LABEL[strong[0]], _SUBSCORE_LABEL[weak[0]]


def _next_required(cand: dict) -> str:
    state = cand.get("lifecycle_state")
    if state == DATA_HOLD:
        return "acquire data: %s" % (cand.get("blocker") or "insufficient data")
    if state == REJECTED:
        return "rejected: %s" % (cand.get("blocker") or "complete-and-weak")
    if state == KEEP_FOR_RESEARCH:
        return "activate shadow book / accrue true-forward observations"
    if state == SHADOW_BOOK_ACTIVE:
        return "mature forward observations toward manual-review threshold"
    if state == READY_FOR_MANUAL_REVIEW:
        return "awaiting human manual review (never auto-promoted)"
    if state in (PROPOSED, TESTING):
        return "run historical evaluation"
    return "n/a"


def build_leaderboard(registry: "CandidateRegistry", cfg: dict, *,
                      limit: Optional[int] = None) -> dict:
    ranked_rows: list[dict] = []
    unranked_rows: list[dict] = []
    for c in registry.list():
        scores = c.get("scores") or {}
        strong, weak = _strongest_weakest(scores)
        row = {
            "candidate_id": c["candidate_id"], "name": c["name"],
            "family": c["family"], "lifecycle_state": c["lifecycle_state"],
            "historical_evidence_score": scores.get("historical_evidence_score"),
            "robustness_score": scores.get("robustness_score"),
            "cost_score": scores.get("cost_score"),
            "stability_score": scores.get("stability_score"),
            "diversification_score": scores.get("diversification_score"),
            "forward_evidence_score": scores.get("forward_evidence_score"),
            "combined_score": c.get("combined_score"),
            "strongest_evidence": strong, "largest_weakness": weak,
            "blocker": c.get("blocker"), "next_required_evidence": _next_required(c),
            "last_evaluated": c.get("latest_evidence_date"),
        }
        (ranked_rows if c.get("combined_score") is not None
         else unranked_rows).append(row)
    tb = cfg.get("leaderboard", {}).get("tie_breakers", [])
    ranked_rows.sort(key=lambda r: (
        -(_f(r["combined_score"]) or 0.0),
        -(_f(r["robustness_score"]) or 0.0),
        -(_f(r["cost_score"]) or 0.0),
        -(_f(r["historical_evidence_score"]) or 0.0),
        r["candidate_id"]))
    for i, r in enumerate(ranked_rows, start=1):
        r["overall_rank"] = i
    for r in unranked_rows:
        r["overall_rank"] = None
    rows = ranked_rows + unranked_rows
    if limit:
        rows = rows[:limit]
    return {"rows": rows, "ranked": len(ranked_rows),
            "tie_breakers": tb or ["robustness", "cost", "rank_ic_t"]}


# --------------------------------------------------------------------------- #
# The bounded tournament tick (WS11). Resumable; leaves useful work queued.
# --------------------------------------------------------------------------- #
def run_tournament_cycle(registry: "CandidateRegistry", cfg: dict, *,
                         queue=None, campaign_result: Optional[dict] = None,
                         survivorship_safe: bool = True,
                         evidence_date: Optional[str] = None,
                         max_candidates: Optional[int] = None,
                         seed: bool = True, activate_shadows: bool = True,
                         inception_provider: Optional[Callable] = None,
                         completed_experiments: Optional[Iterable[dict]] = None,
                         mark_provider: Optional[Callable] = None) -> dict:
    """One bounded, resumable tournament tick.

    (1) seed the family catalogue (idempotent); (2) idempotently ingest any
    completed Stage 8 experiment jobs; (3) evaluate a bounded number of
    unresolved candidates against the owned Stage 5 evidence; (4) recompute the
    leaderboard; (5) generate the next experiments into the shared durable queue;
    (6) activate shadow books for the strongest retained candidates and advance
    active ones by one immutable daily mark; (7) record meaningful changes.
    Never mutates any operational trading state.
    """
    if seed:
        seed_families(registry)
        # Stage 9.4: the AGENT autonomously seeds the pre-registered price-factor
        # catalogue (Track A) when the operator has enabled it in config. Idempotent
        # and deduplicated; a disabled flag leaves the catalogue unseeded.
        if (cfg.get("stage9_4") or {}).get("seed_price_catalogue"):
            try:
                seed_stage9_4_catalogue(registry)
            except Exception as exc:  # noqa: BLE001 - never break the tick
                registry.record_change("STAGE94_SEED_ERROR", None,
                                       {"error": type(exc).__name__})
    ingested = ({"imported": 0, "skipped": 0, "malformed": 0}
                if completed_experiments is None
                else ingest_completed_experiments(
                    registry, cfg, completed=completed_experiments,
                    evidence_date=evidence_date))
    cap = int(max_candidates if max_candidates is not None
              else _f(cfg.get("cycle", {}).get("max_candidates_evaluated_per_cycle"))
              or 4)
    feature_metrics, held = _index_campaign(campaign_result)

    evaluated: list[dict] = []
    # Prefer never-evaluated candidates, then TESTING, then DATA_HOLD re-checks.
    order = (registry.list(state=PROPOSED) + registry.list(state=TESTING)
             + registry.list(state=DATA_HOLD))
    for cand in order:
        if len(evaluated) >= cap:
            break
        try:
            res = _evaluate_one(registry, cfg, cand, feature_metrics, held,
                                survivorship_safe, evidence_date)
        except Exception as exc:  # noqa: BLE001 - one bad candidate never stops
            registry.record_change("EVALUATION_ERROR", cand["candidate_id"],
                                   {"error": type(exc).__name__})
            res = {"candidate_id": cand["candidate_id"],
                   "error": type(exc).__name__}
        evaluated.append(res)

    leaderboard = build_leaderboard(registry, cfg)
    generated = generate_next_experiments(registry, cfg, queue=queue)
    # Stage 9.4 release closure: bounded, config-gated canonical experiment
    # generation so the queued generate->execute->ingest loop closes even with no
    # retained candidate. Failure-isolated; never breaks the tick.
    try:
        followups = generate_stage9_4_followups(registry, cfg, queue=queue)
    except Exception as exc:  # noqa: BLE001 - never break the tick
        registry.record_change("STAGE94_FOLLOWUP_ERROR", None,
                               {"error": type(exc).__name__})
        followups = {"generated": [], "count": 0, "enabled": False}
    # Stage 9.5 release: bounded, config-gated, coverage-GATED canonical PIT
    # fundamental experiment generation. Emits nothing until owned coverage is
    # measured sufficient (honest all-DATA_HOLD while coverage is thin).
    try:
        fund_followups = generate_stage9_5_fundamental_followups(
            registry, cfg, queue=queue)
    except Exception as exc:  # noqa: BLE001 - never break the tick
        registry.record_change("STAGE95_FOLLOWUP_ERROR", None,
                               {"error": type(exc).__name__})
        fund_followups = {"generated": [], "count": 0, "enabled": False}
    shadows = (maybe_activate_shadow_books(
        registry, cfg, inception_provider=inception_provider,
        evidence_date=evidence_date) if activate_shadows else [])
    # WS5: advance every ACTIVE shadow book by one immutable daily mark. With no
    # retained candidate there are zero active books and this is a no-op.
    shadow_advances = (advance_shadow_books(
        registry, cfg, mark_provider=mark_provider, evidence_date=evidence_date)
        if (mark_provider is not None and evidence_date is not None) else [])
    advanced = [a for a in shadow_advances if a.get("status") == "ADVANCED"]

    counts_state = registry.counts_by_state()
    counts_family = registry.counts_by_family()
    unresolved = counts_state.get(PROPOSED, 0) + counts_state.get(TESTING, 0) \
        + counts_state.get(DATA_HOLD, 0)
    registry.record_change("TOURNAMENT_CYCLE", None, {
        "evaluated": len(evaluated), "generated": generated["count"],
        "stage9_4_followups": followups.get("count", 0),
        "stage9_5_fundamental_followups": fund_followups.get("count", 0),
        "shadow_books_activated": len(shadows),
        "shadow_books_advanced": len(advanced),
        "experiments_ingested": ingested.get("imported", 0),
        "unresolved_candidates": unresolved})
    return {
        "status": "OK", "evaluated": evaluated,
        "experiments_ingested": ingested,
        "leaderboard_size": leaderboard["ranked"],
        "generated_experiments": generated["count"],
        "stage9_4_followups": followups,
        "stage9_4_followups_generated": followups.get("count", 0),
        "stage9_5_fundamental_followups": fund_followups,
        "stage9_5_fundamental_followups_generated": fund_followups.get("count", 0),
        "shadow_books_activated": shadows,
        "shadow_book_advances": shadow_advances,
        "shadow_books_advanced": len(advanced),
        "counts_by_state": counts_state, "counts_by_family": counts_family,
        "variants_by_family": variants_by_family(registry),
        "unresolved_candidates": unresolved,
        "leave_work_queued": (unresolved > 0 or generated["count"] > 0
                              or fund_followups.get("count", 0) > 0),
    }


def _evaluate_one(registry: "CandidateRegistry", cfg: dict, cand: dict,
                  feature_metrics: dict, held: set, survivorship_safe: bool,
                  evidence_date: Optional[str]) -> dict:
    cid = cand["candidate_id"]
    spec = cand.get("spec", {})
    feature = spec.get("feature")
    requires = spec.get("requires_data_family")
    prev_state = cand["lifecycle_state"]
    rhash: Optional[str] = None  # set only on a completed owned-price result

    # Stage 9.4: pre-registered price factors whose formula needs data absent
    # from the owned close panel resolve to a SPECIFIC, honest DATA_HOLD.
    data_req = spec.get("data_requirement")
    if data_req in _PRICE_REQ_BLOCKER:
        blocker = _PRICE_REQ_BLOCKER[data_req]
        metrics = _data_hold_metrics(blocker)
        gate = {"target_state": DATA_HOLD, "evidence_status": EVIDENCE_INCOMPLETE,
                "blocker": blocker, "failed_gates": [], "complete": False}
    # Non-price families whose owned data cannot be evaluated honestly yet.
    elif requires in ("fundamentals", "analyst", "event"):
        blocker = _DATA_FAMILY_BLOCKER.get(cand.get("pit_status"),
                                           BLOCK_NO_DATA_FAMILY)
        metrics = _data_hold_metrics(blocker)
        gate = {"target_state": DATA_HOLD, "evidence_status": EVIDENCE_INCOMPLETE,
                "blocker": blocker, "failed_gates": [], "complete": False}
    elif feature in held:
        metrics = _data_hold_metrics(BLOCK_PIT)
        gate = {"target_state": DATA_HOLD, "evidence_status": EVIDENCE_INCOMPLETE,
                "blocker": BLOCK_PIT, "failed_gates": [], "complete": False}
    elif feature in feature_metrics:
        row = feature_metrics[feature]
        # WS4 idempotency: the same completed result FOR THIS CANDIDATE is
        # ingested at most once, so it is never re-evaluated on a later
        # 30-minute cycle - while a second candidate sharing the feature is
        # still classified against the same evidence.
        rhash = result_hash_for(source="owned_price_campaign", feature=feature,
                                row=row, evidence_date=evidence_date,
                                candidate_id=cid)
        if registry.is_processed(rhash):
            return {"candidate_id": cid, "from": prev_state, "to": prev_state,
                    "skipped": "already_processed",
                    "combined_score": _f(cand.get("combined_score"))}
        metrics = row_to_contract_metrics(row,
                                          survivorship_safe=survivorship_safe)
        gate = classify_evidence(metrics, cfg)
    else:
        metrics = _data_hold_metrics(BLOCK_OBSERVATIONS)
        gate = {"target_state": DATA_HOLD, "evidence_status": EVIDENCE_INCOMPLETE,
                "blocker": BLOCK_OBSERVATIONS, "failed_gates": [],
                "complete": False}

    # DATA_HOLD idempotency: a candidate already parked on the same blocker with
    # no new completed evidence is not re-recorded every cycle (no churn).
    if (not gate.get("complete") and prev_state == DATA_HOLD
            and cand.get("blocker") == gate.get("blocker")):
        return {"candidate_id": cid, "from": prev_state, "to": prev_state,
                "skipped": "data_hold_unchanged", "blocker": gate.get("blocker"),
                "combined_score": None}

    if gate.get("complete"):
        scores = score_candidate(metrics, cfg,
                                 corr_champion=_f(metrics.get("corr_vs_champion")),
                                 corr_retained=None, forward=None)
    else:
        scores = null_scores()  # DATA_HOLD candidates are never ranked
    pit = metrics.get("data_boundaries") or {
        "point_in_time_valid": metrics.get("point_in_time_valid"),
        "survivorship_safe": metrics.get("survivorship_safe")}
    registry.record_evaluation(cid, metrics=metrics, scores=scores, gate=gate,
                               pit=pit, experiment_id=rhash,
                               evidence_date=evidence_date)
    _apply_gate_transition(registry, cid, prev_state, gate)
    if rhash is not None:
        registry.mark_processed(result_hash=rhash, candidate_id=cid,
                                source="owned_price_campaign", feature=feature,
                                evidence_date=evidence_date)
    new_state = gate["target_state"]
    if new_state == KEEP_FOR_RESEARCH and prev_state != KEEP_FOR_RESEARCH:
        registry.record_change("NEW_RETAINED_CANDIDATE", cid,
                               {"name": cand["name"], "family": cand["family"],
                                "combined_score": scores.get("combined_score")})
    elif new_state == REJECTED and prev_state != REJECTED:
        registry.record_change("CANDIDATE_REJECTED", cid,
                               {"name": cand["name"], "family": cand["family"],
                                "reason": gate.get("blocker")})
    # Stage 9.4 Track H: record the full lifecycle evidence artifact for a
    # pre-registered catalogue candidate (hypothesis, family, search-family size,
    # raw + selection-adjusted stats, orthogonality/regime/walk-forward evidence,
    # failed gates, next required evidence). Never changes the decision.
    if (spec.get("catalogue") or metrics.get("stage9_4_selection")):
        try:
            from . import stage9_4 as _s94
            artifact = _s94.build_decision_record(
                cand, metrics, scores, gate,
                hypothesis=spec.get("formula", ""),
                family=spec.get("factor_family", ""),
                search_family_size=int(spec.get("search_family_size", 1) or 1))
            registry.record_change("STAGE94_DECISION", cid, artifact)
        except Exception as exc:  # noqa: BLE001 - artifact is best-effort
            registry.record_change("STAGE94_DECISION_ERROR", cid,
                                   {"error": type(exc).__name__})
    return {"candidate_id": cid, "from": prev_state, "to": new_state,
            "blocker": gate.get("blocker"),
            "combined_score": scores.get("combined_score")}


def _apply_gate_transition(registry: "CandidateRegistry", cid: str,
                           current: str, gate: dict) -> None:
    target = gate["target_state"]
    blocker = gate.get("blocker")
    if target == DATA_HOLD:
        if current in (PROPOSED, TESTING, DATA_HOLD):
            registry.transition(cid, DATA_HOLD, blocker=blocker)
        return
    # Complete evidence (REJECTED or KEEP_FOR_RESEARCH): mark tested first.
    if current == PROPOSED:
        registry.transition(cid, TESTING)
        current = TESTING
    elif current == DATA_HOLD:
        registry.transition(cid, TESTING)
        current = TESTING
    registry.transition(cid, target, blocker=blocker,
                        reason=",".join(gate.get("failed_gates", []) or []))


# --------------------------------------------------------------------------- #
# Read-only payloads (WS10) - API / observatory / Telegram all consume these.
# --------------------------------------------------------------------------- #
_SAFETY_BADGES = ["RESEARCH ONLY", "READ-ONLY EVIDENCE", "NO ORDERS",
                  "NO AUTOMATIC PROMOTION", "NOT APPROVED FOR LIVE TRADING"]


def _open_registry_readonly(config_path=None, *, db_path=None):
    if db_path is None:
        if config_path is None:
            return None, None
        try:
            cfg = load_config(config_path)
        except ConfigError:
            return None, None
        db_path = cfg.get("tournament_db")
    else:
        cfg = {}
        if config_path is not None:
            try:
                cfg = load_config(config_path)
            except ConfigError:
                cfg = {}
    if not db_path or not Path(db_path).exists():
        return None, cfg
    return CandidateRegistry(db_path), cfg


def load_tournament(config_path=None, *, db_path=None,
                    leaderboard_limit: int = 20) -> dict:
    """The one canonical read-only tournament payload. Degrades to a controlled
    UNAVAILABLE dict (never raises) so an HTTP endpoint always returns 200."""
    reg, cfg = _open_registry_readonly(config_path, db_path=db_path)
    if reg is None:
        return {"status": "UNAVAILABLE", "reason":
                "tournament registry not initialized", "safety": _SAFETY_BADGES,
                "read_only": True}
    try:
        counts_state = reg.counts_by_state()
        counts_family = reg.counts_by_family()
        lb = build_leaderboard(reg, cfg or {}, limit=leaderboard_limit)
        shadows = reg.list_shadow_books()
        changes = reg.recent_changes(limit=15)
        blockers = sorted({r["blocker"] for r in lb["rows"]
                           if r.get("blocker")})
        return {
            "status": "OK", "read_only": True, "safety": _SAFETY_BADGES,
            "no_automatic_promotion": True,
            "counts_by_state": counts_state, "counts_by_family": counts_family,
            "leaderboard": lb["rows"], "ranked": lb["ranked"],
            "shadow_books": [{"shadow_book_id": s["shadow_book_id"],
                              "candidate_id": s["candidate_id"],
                              "inception_date": s["inception_date"],
                              "status": s["status"]} for s in shadows],
            "recent_changes": changes, "blockers": blockers,
            "variants_by_family": variants_by_family(reg),
            "views": (cfg or {}).get("leaderboard", {}).get("views", []),
        }
    finally:
        reg.close()


def load_candidate(config_path=None, *, db_path=None,
                   candidate_id: str) -> Optional[dict]:
    """Full read-only candidate record + latest evaluation, matched by exact id,
    id suffix, or case-insensitive name substring. Returns None when unknown."""
    reg, _cfg = _open_registry_readonly(config_path, db_path=db_path)
    if reg is None:
        return None
    try:
        cand = reg.get(candidate_id)
        if cand is None:
            key = (candidate_id or "").strip().lower()
            for c in reg.list():
                if (c["candidate_id"].lower().endswith(key)
                        or (key and key in c["name"].lower())):
                    cand = c
                    break
        if cand is None:
            return None
        return {"candidate": cand,
                "evaluation": reg.latest_evaluation(cand["candidate_id"])}
    finally:
        reg.close()


def tournament_snapshot(config_path=None, *, db_path=None) -> dict:
    """Compact block for the evidence observatory (mirrors autonomy_snapshot)."""
    payload = load_tournament(config_path, db_path=db_path, leaderboard_limit=5)
    if payload.get("status") != "OK":
        return {"status": payload.get("status", "UNAVAILABLE"),
                "read_only": True, "safety_badges": _SAFETY_BADGES}
    top = payload["leaderboard"][:5]
    recent = payload["recent_changes"][0] if payload["recent_changes"] else None
    return {
        "status": "OK", "mode": "read_only_research",
        "safety_badges": _SAFETY_BADGES, "no_automatic_promotion": True,
        "counts_by_state": payload["counts_by_state"],
        "counts_by_family": payload["counts_by_family"],
        "top_candidates": [{"rank": r.get("overall_rank"), "name": r["name"],
                            "family": r["family"], "state": r["lifecycle_state"],
                            "combined_score": r["combined_score"],
                            "strongest_evidence": r["strongest_evidence"],
                            "largest_weakness": r["largest_weakness"]}
                           for r in top],
        "shadow_book_count": len(payload["shadow_books"]),
        "blockers": payload["blockers"], "latest_change": recent,
    }


# --------------------------------------------------------------------------- #
# Meaningful-change reporting (WS10) - flag ONLY material tournament changes.
# --------------------------------------------------------------------------- #
_MEANINGFUL_KINDS = {
    "NEW_RETAINED_CANDIDATE": "New retained candidate: %(name)s (%(family)s)",
    "CANDIDATE_REJECTED": "Candidate rejected: %(name)s (%(reason)s)",
    "SHADOW_BOOK_ACTIVATED": "New research shadow book: %(shadow_book_id)s",
}


def meaningful_tournament_changes(config_path=None, *, db_path=None,
                                  limit: int = 30) -> list[str]:
    """Human lines for the report - only material changes, else one sentence."""
    reg, _cfg = _open_registry_readonly(config_path, db_path=db_path)
    if reg is None:
        return ["No tournament activity to report."]
    try:
        lines: list[str] = []
        for ch in reg.recent_changes(limit=limit):
            tmpl = _MEANINGFUL_KINDS.get(ch["kind"])
            if not tmpl:
                continue
            d = dict(ch.get("detail") or {})
            d.setdefault("name", ch.get("candidate_id") or "?")
            d.setdefault("family", "?")
            d.setdefault("reason", "complete-and-weak evidence")
            d.setdefault("shadow_book_id", "?")
            try:
                lines.append(tmpl % d)
            except (KeyError, TypeError):
                continue
        if not lines:
            return ["No meaningful tournament change since the previous report."]
        return lines
    finally:
        reg.close()


__all__ = [
    # states
    "LIFECYCLE_STATES", "PROPOSED", "DATA_HOLD", "TESTING", "REJECTED",
    "KEEP_FOR_RESEARCH", "SHADOW_BOOK_ACTIVE", "READY_FOR_MANUAL_REVIEW",
    "ARCHIVED", "ALLOWED_TRANSITIONS", "ALL_FAMILIES",
    # config
    "load_config", "ConfigError",
    # registry
    "CandidateRegistry", "TransitionError", "spec_hash", "candidate_id_for",
    "canonical_json",
    # evaluation/scoring/gates
    "classify_evidence", "score_candidate", "qualifies_for_manual_review",
    "row_to_contract_metrics", "build_owned_price_campaign", "CODE_VERSION",
    # families
    "default_candidate_specs", "seed_families",
    # shadow books
    "ShadowBook", "RetroactiveError", "SHADOW_LABEL",
    "maybe_activate_shadow_books", "advance_shadow_books", "SHADOW_MARK_MISSING",
    # completed-experiment ingestion (WS4)
    "ingest_completed_experiments", "result_hash_for",
    # generation / leaderboard / tick
    "generate_next_experiments", "generate_stage9_4_followups",
    "generate_stage9_5_fundamental_followups",
    "variants_by_family", "build_leaderboard", "run_tournament_cycle",
    "seed_stage9_4_catalogue",
    # read-only payloads
    "load_tournament", "load_candidate", "tournament_snapshot",
    "meaningful_tournament_changes",
]
