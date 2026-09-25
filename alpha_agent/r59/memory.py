"""alpha_agent.r59.memory - the ONE persistent research memory.

Every release from R31 to R58 wrote its conclusions into its own JSON silo and
carried the ONLY cross-release fact - how much searching had already happened -
as a hand-copied integer (``PRIOR_SEARCH_BURDEN = 302`` in R58). That is why a
campaign could end and the system had nothing to consult before choosing what
to do next: there was no memory, only a pile of reports.

This module owns that memory. One SQLite database under the R59 research root:

    hypotheses          one row per hypothesis IDENTITY, with its information /
                        economic / asset / horizon / model coordinates, its
                        result, its statistics, its economics, its robustness,
                        the reason it was rejected and the condition that would
                        REOPEN it.
    burden              search-burden accounting per (economic family, asset
                        class): tests actually run, counted, never copied.
    frontier            per-asset-class research state.
    opportunities       the data-opportunity frontier.
    provider_usage      what each paid/free provider actually unlocked.
    events              an append-only journal of everything the loop decided.

Design rules enforced here:

* IDENTITY IS ECONOMIC. Two hypotheses that differ only in a parameter belong
  to one family and count as ONE test against the multiple-testing denominator;
  ``family_key`` is what the burden ledger counts, ``hypothesis_id`` is what
  novelty de-duplicates.
* A HISTORICAL RESULT IS NEVER FORWARD EVIDENCE. ``record_result`` refuses to
  write a forward-evidence field, and ``freeze_forward`` is the only path that
  creates a prospective row - it stores an inception instant and never a score.
* NOTHING IS DELETED. A rejected hypothesis stays, because it is the
  denominator of every later claim.

Pure stdlib. Opens no socket, imports no engine, touches no operational store.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, Optional

from .. import r59

SCHEMA_VERSION = "r59_research_memory/1"
DB_NAME = "research_memory.sqlite"

#: R72.1. The wildcard a director ruling uses for a mechanism component it does
#: NOT bind, meaning "every mechanism in this family". It is deliberately a
#: character no family name may contain, so it can never be confused with one.
RULING_ANY = "*"

#: How far a recorded ruling reaches. Derived from the two mechanism components
#: rather than stored, so a row and its scope can never disagree.
RULING_SCOPE_FAMILY = "ECONOMIC_FAMILY"
RULING_SCOPE_INFORMATION = "INFORMATION_FAMILY"
RULING_SCOPE_MODEL = "MODEL_FAMILY"
RULING_SCOPE_MECHANISM = "MECHANISM"
RULING_SCOPES = (RULING_SCOPE_MECHANISM, RULING_SCOPE_INFORMATION,
                 RULING_SCOPE_MODEL, RULING_SCOPE_FAMILY)

#: The migration that gave this table its mechanism identity. Recorded in
#: ``memory_meta`` so the rebuild is provably once-only.
DIRECTOR_RULING_IDENTITY_MIGRATION = "r72_1_director_ruling_mechanism_identity"

#: R72.1. Why a mechanism query was refused rather than answered. One code,
#: because there is exactly one way to ask an unanswerable question here: name
#: no part of the mechanism at all.
MECHANISM_QUERY_UNBOUND = "INVALID_QUERY_NO_MECHANISM_COMPONENT_BOUND"


def _ruling_table_has_mechanism_identity(conn) -> bool:
    """Is this connection's ``director_rulings`` already in the R72.1 shape?

    A READ-ONLY handle can never migrate - :meth:`ResearchMemory._init_schema`
    is not run for one - so a reader may legitimately meet the legacy table
    while a writer has not yet opened the store. Asking the table rather than
    assuming is what keeps the blocker reconciliation, which runs read-only
    against the LIVE store, from losing every ruling to a missing column.
    """
    try:
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(director_rulings)").fetchall()}
    except sqlite3.Error:
        return False
    return "information_family" in cols


def _none_if_unbound(value: Any) -> Optional[Any]:
    """``None`` for anything that does not actually name a component.

    A filter is bound only when it carries a non-blank string. ``None``, ``""``
    and whitespace are the same statement - "I did not say" - and a query
    predicate must not treat them as three different ones.
    """
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _ruling_component(value: Any) -> str:
    """One mechanism component of a ruling key, normalised.

    ``None``, an empty string and whitespace all mean THE SAME THING - the
    director did not bind this component - and all become :data:`RULING_ANY`.
    Treating a blank as a distinct literal would create a third scope nobody
    declared, reachable only by a caller that passed an empty string.
    """
    text = "" if value is None else str(value).strip()
    return text or RULING_ANY


def ruling_scope(information_family: Any, model_family: Any) -> str:
    """How far a ruling with these two components reaches."""
    info_bound = _ruling_component(information_family) != RULING_ANY
    model_bound = _ruling_component(model_family) != RULING_ANY
    if info_bound and model_bound:
        return RULING_SCOPE_MECHANISM
    if info_bound:
        return RULING_SCOPE_INFORMATION
    if model_bound:
        return RULING_SCOPE_MODEL
    return RULING_SCOPE_FAMILY

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS memory_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hypotheses (
    hypothesis_id       TEXT PRIMARY KEY,
    family_key          TEXT NOT NULL,
    title               TEXT NOT NULL,
    release             TEXT NOT NULL,
    origin              TEXT NOT NULL,
    generation_method   TEXT NOT NULL,
    information_family  TEXT NOT NULL,
    economic_family     TEXT NOT NULL,
    asset_class         TEXT NOT NULL,
    horizon_sessions    INTEGER,
    model_family        TEXT NOT NULL,
    input_data_identity TEXT NOT NULL,
    outcome             TEXT,
    evidence_maturity   TEXT,
    statistic_json      TEXT,
    economics_json      TEXT,
    turnover_cost_json  TEXT,
    robustness_json     TEXT,
    reason_rejected     TEXT,
    counts_to_burden    INTEGER NOT NULL DEFAULT 1,
    reopen_condition    TEXT,
    forward_challenger  TEXT,
    spec_json           TEXT,
    created_at          TEXT NOT NULL,
    settled_at          TEXT,
    invalidated_reason  TEXT
);
CREATE INDEX IF NOT EXISTS ix_hyp_family   ON hypotheses (family_key);
CREATE INDEX IF NOT EXISTS ix_hyp_invalid   ON hypotheses (invalidated_reason);
CREATE INDEX IF NOT EXISTS ix_hyp_asset    ON hypotheses (asset_class);
CREATE INDEX IF NOT EXISTS ix_hyp_outcome  ON hypotheses (outcome);
CREATE INDEX IF NOT EXISTS ix_hyp_econ     ON hypotheses (economic_family);

CREATE TABLE IF NOT EXISTS frontier (
    asset_class   TEXT PRIMARY KEY,
    state         TEXT NOT NULL,
    reason        TEXT,
    detail_json   TEXT,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS opportunities (
    opportunity_id   TEXT PRIMARY KEY,
    title            TEXT NOT NULL,
    state            TEXT NOT NULL,
    asset_class      TEXT,
    information_need TEXT,
    unlocks_json     TEXT,
    owned_but_unused INTEGER NOT NULL DEFAULT 0,
    free_proxy       TEXT,
    provider         TEXT,
    pit_integrity    TEXT,
    effective_sample TEXT,
    expected_value   TEXT,
    cost_usd_year    REAL,
    gate_verdict     TEXT,
    detail_json      TEXT,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_usage (
    provider        TEXT NOT NULL,
    data_class      TEXT NOT NULL,
    coverage_json   TEXT,
    detail_json     TEXT,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (provider, data_class)
);

-- R72. THE DIRECTOR'S RULING ON AN ECONOMIC FAMILY, made durable.
--
-- A campaign director rules on families the autonomous loop is blocked on, and
-- until now that ruling lived only in a campaign JSON file. The governor reads
-- THIS database, so a family the director had terminally closed kept the
-- classification its engine's free-text symptom implied - typically
-- DEPENDENCY_BLOCKED, which clears on INFORMATION - and the runtime went on
-- sleeping on it. alpha_agent.r59.blockers names that failure in its own
-- docstring: "sleeping on a TERMINAL one and calling it research is the
-- failure mode this taxonomy exists to make visible."
--
-- A ruling decides nothing about scheduling and creates no hypothesis. It
-- records WHAT WAS RULED, BY WHOM, WHEN, and WHAT WOULD REOPEN IT, so the
-- reconciliation can be provenance-carrying rather than an assertion. The
-- blocker code is drawn from the EXISTING taxonomy - a ruling makes a code
-- authoritative, it does not add a vocabulary.
--
-- R72.1 - THE KEY IS THE MECHANISM, NOT THE FAMILY.
--
-- The first version of this table was keyed (asset_class, economic_family).
-- That key cannot tell two DIFFERENT mechanisms apart when they happen to share
-- an economic family, and the estate has many: US_EQUITY|EVENT_OVERREACTION
-- holds both the SEC comment-letter cell and a NASDAQ trading-halt cell, which
-- read different information through different models and were settled by
-- different evidence. Under the old key, ruling on the first TERMINALLY closed
-- the second - a refusal nobody wrote, arriving through a primary key.
--
-- The key is therefore the same four-part mechanism identity every other reader
-- in this estate already uses (economic_family | information_family |
-- asset_class | model_family), and the two mechanism components accept the
-- wildcard ``*`` meaning "every mechanism in this family". A family-wide ruling
-- is (family, '*', '*') and keeps exactly the reach it always had; a
-- mechanism-scoped ruling binds its own information and model family and
-- reaches nothing else. Resolution is MOST-SPECIFIC-FIRST, and a caller that
-- names no mechanism can only ever match the family-wide row - so a
-- mechanism-scoped ruling can never leak onto a job whose mechanism is unknown.
CREATE TABLE IF NOT EXISTS director_rulings (
    asset_class        TEXT NOT NULL,
    economic_family    TEXT NOT NULL,
    information_family TEXT NOT NULL DEFAULT '*',
    model_family       TEXT NOT NULL DEFAULT '*',
    verdict            TEXT NOT NULL,
    blocker_reason     TEXT NOT NULL,
    rationale          TEXT NOT NULL,
    reopen_condition   TEXT,
    campaign_id        TEXT,
    decided_by         TEXT,
    decision_date      TEXT,
    source_artifact    TEXT,
    detail_json        TEXT,
    updated_at         TEXT NOT NULL,
    PRIMARY KEY (asset_class, economic_family, information_family,
                 model_family)
);

CREATE TABLE IF NOT EXISTS generator_yield (
    asset_class   TEXT NOT NULL,
    kind          TEXT NOT NULL,
    generated     INTEGER NOT NULL DEFAULT 0,
    novel         INTEGER NOT NULL DEFAULT 0,
    duplicates    INTEGER NOT NULL DEFAULT 0,
    batches       INTEGER NOT NULL DEFAULT 0,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (asset_class, kind)
);

CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at  TEXT NOT NULL,
    kind         TEXT NOT NULL,
    subject      TEXT,
    detail_json  TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_kind ON events (kind);
"""


def _j(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    return json.dumps(obj, sort_keys=True, default=str)


def _unj(text: Optional[str]) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return None


def family_key(*, economic_family: str, information_family: str,
               asset_class: str, model_family: str = "LINEAR") -> str:
    """The multiple-testing unit.

    Horizon is deliberately EXCLUDED: section M of the release brief requires
    that horizon variants of the same economic signal count as ONE search
    family, because scanning 1/5/21/63 sessions of one idea is one idea tested
    four ways, not four ideas.
    """
    return "%s|%s|%s|%s" % (economic_family, information_family, asset_class,
                            model_family)


def hypothesis_id(*, family: str, spec: Any) -> str:
    """A stable identity for one concrete hypothesis inside a family."""
    return "H_%s_%s" % (r59.short_hash(family, 8), r59.short_hash(spec, 12))


class ReadOnlyMemory(RuntimeError):
    """A write was attempted on a memory opened read-only.

    Raised rather than ignored. A read model that reaches a mutation has a
    defect, and swallowing it would leave the defect invisible behind a
    plausible-looking response.
    """


def memory_db_path(db_path: Optional[Path] = None) -> Path:
    """Where the memory LIVES, without creating anything.

    Asking the question must not answer it: constructing a
    :class:`ResearchMemory` used to be the only way to learn the path, and
    that call created the directory and ran the schema script.
    """
    return Path(db_path) if db_path else (r59.research_root() / DB_NAME)


class ResearchMemory:
    """The persistent memory. One writer process at a time; WAL + busy timeout
    make a concurrent reader safe.

    ``read_only`` (R60) opens an OBSERVER handle for a read model. It creates
    no directory, runs no schema script, writes no ``memory_meta`` row and
    refuses every mutating method. Before R60 the only way to READ this
    memory was to construct a writer, so merely asking what research had
    concluded wrote to the store the question was about - which is exactly
    what a read-only projection may not do.
    """

    def __init__(self, db_path: Optional[Path] = None, *,
                 busy_timeout_ms: int = 10000, read_only: bool = False):
        self.db_path = memory_db_path(db_path)
        self.read_only = bool(read_only)
        self._busy = int(busy_timeout_ms)
        self._lock = threading.RLock()
        if self.read_only:
            if not self.db_path.exists():
                raise FileNotFoundError(
                    "research memory not present: %s" % self.db_path)
        else:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._init_schema()

    # -- plumbing ----------------------------------------------------------- #
    def _guard_write(self) -> None:
        if self.read_only:
            raise ReadOnlyMemory(
                "this ResearchMemory handle is read-only: %s" % self.db_path)

    def _connect(self) -> sqlite3.Connection:
        if self.read_only:
            return self._connect_read_only()
        conn = sqlite3.connect(str(self.db_path), timeout=self._busy / 1000.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=%d" % self._busy)
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _connect_read_only(self) -> sqlite3.Connection:
        """A handle that cannot write, even by accident.

        ``mode=ro`` is preferred and touches nothing at all. It fails when the
        database is in WAL mode and its ``-shm`` companion is absent (a
        cleanly-closed store), because SQLite would have to CREATE that file
        in order to read. The fallback opens normally and sets ``query_only``,
        which the engine itself enforces: the shared-memory file may be
        recreated, but no row, page or schema object can change.
        ``immutable=1`` is deliberately NOT used - it would return torn reads
        while the live worker is mid-transaction.
        """
        uri = "file:%s?mode=ro" % self.db_path.as_posix()
        try:
            conn = sqlite3.connect(uri, uri=True,
                                   timeout=self._busy / 1000.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=%d" % self._busy)
            conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            return conn
        except sqlite3.Error:
            conn = sqlite3.connect(str(self.db_path),
                                   timeout=self._busy / 1000.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=%d" % self._busy)
            conn.execute("PRAGMA query_only=ON")
            return conn

    #: The columns a pre-R72.1 ``director_rulings`` row carries, in the order
    #: the rebuild copies them. Named explicitly rather than with ``SELECT *``
    #: so a future column cannot silently change what the migration moves.
    _RULING_LEGACY_COLUMNS = (
        "asset_class", "economic_family", "verdict", "blocker_reason",
        "rationale", "reopen_condition", "campaign_id", "decided_by",
        "decision_date", "source_artifact", "detail_json", "updated_at")

    def _migrate_director_ruling_identity(self, conn) -> Optional[dict]:
        """R72.1 - rekey ``director_rulings`` onto the mechanism identity.

        A PRIMARY KEY cannot be altered in place in SQLite, so the table is
        rebuilt. This is the ONLY table in this memory that is ever rebuilt,
        and it is rebuilt exactly once: every pre-existing ruling is copied
        with :data:`RULING_ANY` in both mechanism components, which is the
        shape that PRESERVES ITS MEANING EXACTLY. A ruling recorded under the
        old key said "this economic family, in this asset class" and nothing
        about which mechanism inside it, so family-wide is neither a widening
        nor a narrowing - it is a faithful restatement in the new vocabulary.
        Inventing a mechanism for it from its rationale text would be the
        narrowing this migration exists to avoid.

        Idempotent: once the column is present the table is already in the new
        shape and this returns ``None`` without touching a row. Returns the
        migration report (mapping and collisions) the one time it runs, and
        records the same report in ``memory_meta`` so it stays auditable.

        Runs INSIDE the caller's connection and transaction, before the schema
        script, so a fresh database never sees the legacy shape at all.
        """
        cols = {r[1] for r in conn.execute(
            "PRAGMA table_info(director_rulings)").fetchall()}
        if not cols:
            return None                      # fresh database: schema builds it
        if "information_family" in cols:
            return None                      # already migrated
        legacy = [c for c in self._RULING_LEGACY_COLUMNS if c in cols]
        rows = [dict(r) for r in conn.execute(
            "SELECT %s FROM director_rulings" % ", ".join(legacy)).fetchall()]
        mapping, seen, collisions = [], {}, []
        for r in rows:
            key = (str(r.get("asset_class")), str(r.get("economic_family")),
                   RULING_ANY, RULING_ANY)
            entry = {
                "from": {"asset_class": r.get("asset_class"),
                         "economic_family": r.get("economic_family")},
                "to": {"asset_class": key[0], "economic_family": key[1],
                       "information_family": key[2], "model_family": key[3]},
                "scope_before": RULING_SCOPE_FAMILY,
                "scope_after": ruling_scope(key[2], key[3]),
                "verdict": r.get("verdict"),
                "blocker_reason": r.get("blocker_reason"),
                "reopen_condition": r.get("reopen_condition"),
                "campaign_id": r.get("campaign_id"),
                "decided_by": r.get("decided_by"),
                "narrowed": False, "broadened": False,
            }
            if key in seen:
                # Unreachable through the old PRIMARY KEY, which already made
                # (asset_class, economic_family) unique. Detected and reported
                # anyway: a migration that cannot say it lost nothing has not
                # shown that it lost nothing.
                entry["collided_with"] = seen[key]
                collisions.append(entry)
            else:
                seen[key] = entry["from"]
            mapping.append(entry)
        # A scratch table that survives here is, BY DEFINITION, from a rebuild
        # that never reached its RENAME - so it holds no ruling the original
        # does not still hold, and the original is still the live table. Left
        # in place it would fail every subsequent open with "table already
        # exists", turning one interrupted migration into a store nobody can
        # open. CREATE TABLE does not roll back in SQLite's autocommit mode,
        # so this is reachable, and it is the recovery.
        conn.execute("DROP TABLE IF EXISTS director_rulings__r72_1")
        conn.execute("""
            CREATE TABLE director_rulings__r72_1 (
                asset_class        TEXT NOT NULL,
                economic_family    TEXT NOT NULL,
                information_family TEXT NOT NULL DEFAULT '*',
                model_family       TEXT NOT NULL DEFAULT '*',
                verdict            TEXT NOT NULL,
                blocker_reason     TEXT NOT NULL,
                rationale          TEXT NOT NULL,
                reopen_condition   TEXT,
                campaign_id        TEXT,
                decided_by         TEXT,
                decision_date      TEXT,
                source_artifact    TEXT,
                detail_json        TEXT,
                updated_at         TEXT NOT NULL,
                PRIMARY KEY (asset_class, economic_family,
                             information_family, model_family)
            )""")
        carried = [c for c in self._RULING_LEGACY_COLUMNS if c in cols]
        conn.execute(
            "INSERT INTO director_rulings__r72_1"
            " (information_family, model_family, %s)"
            " SELECT ?, ?, %s FROM director_rulings"
            % (", ".join(carried), ", ".join(carried)),
            (RULING_ANY, RULING_ANY))
        moved = conn.execute(
            "SELECT COUNT(*) FROM director_rulings__r72_1").fetchone()[0]
        if int(moved) != len(rows):
            raise RuntimeError(
                "director_rulings migration would lose rulings: read %d, "
                "wrote %d; refusing to drop the original table"
                % (len(rows), int(moved)))
        conn.execute("DROP TABLE director_rulings")
        conn.execute("ALTER TABLE director_rulings__r72_1"
                     " RENAME TO director_rulings")
        report = {
            "migration": DIRECTOR_RULING_IDENTITY_MIGRATION,
            "owner": "alpha_agent.r59.memory.ResearchMemory",
            "key_before": ["asset_class", "economic_family"],
            "key_after": ["asset_class", "economic_family",
                          "information_family", "model_family"],
            "rulings_before": len(rows), "rulings_after": int(moved),
            "n_collisions": len(collisions), "collisions": collisions,
            "n_narrowed": 0, "n_broadened": 0,
            "every_ruling_preserved": int(moved) == len(rows),
            "rule": ("a pre-R72.1 ruling bound no mechanism, so it migrates to "
                     "(information_family='*', model_family='*') - the same "
                     "reach it already had, restated in the new key"),
            "mapping": mapping,
            "migrated_at": r59.now_iso(),
        }
        conn.execute(
            "INSERT OR REPLACE INTO memory_meta(key,value) VALUES(?,?)",
            (DIRECTOR_RULING_IDENTITY_MIGRATION, _j(report)))
        return report

    def director_ruling_migration_report(self) -> Optional[dict]:
        """The R72.1 migration's mapping and collision report, or ``None``.

        ``None`` means this database was created at or after R72.1 and never
        held a family-keyed ruling, which is a different fact from "the
        migration lost something" and is reported as itself.
        """
        conn = self._connect_read_only()
        try:
            row = conn.execute(
                "SELECT value FROM memory_meta WHERE key=?",
                (DIRECTOR_RULING_IDENTITY_MIGRATION,)).fetchone()
        except sqlite3.Error:
            return None
        finally:
            conn.close()
        return _unj(row["value"]) if row else None

    def _init_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                # Additive migration BEFORE the schema script, so an existing
                # database from an earlier R59 invocation gains the column
                # without being rebuilt. Nothing in this memory is ever dropped
                # or recreated - the graveyard is the denominator of every later
                # claim and must survive every schema change.
                have = {r[1] for r in conn.execute(
                    "PRAGMA table_info(hypotheses)").fetchall()}
                if have and "invalidated_reason" not in have:
                    conn.execute("ALTER TABLE hypotheses"
                                 " ADD COLUMN invalidated_reason TEXT")
                self._migrate_director_ruling_identity(conn)
                conn.executescript(_SCHEMA_SQL)
                conn.execute(
                    "INSERT OR REPLACE INTO memory_meta(key,value) VALUES(?,?)",
                    ("schema_version", SCHEMA_VERSION))
                conn.commit()
            finally:
                conn.close()

    def close(self) -> None:  # symmetry with the Stage-8 queue
        return None

    # -- events ------------------------------------------------------------- #
    def event(self, kind: str, *, subject: Optional[str] = None,
              detail: Optional[dict] = None) -> None:
        self._guard_write()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO events(recorded_at,kind,subject,detail_json)"
                    " VALUES(?,?,?,?)",
                    (r59.now_iso(), kind, subject, _j(detail)))
                conn.commit()
            finally:
                conn.close()

    def events(self, *, kind: Optional[str] = None, limit: int = 200) -> list:
        conn = self._connect()
        try:
            if kind:
                rows = conn.execute(
                    "SELECT * FROM events WHERE kind=? ORDER BY id DESC LIMIT ?",
                    (kind, int(limit))).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM events ORDER BY id DESC LIMIT ?",
                    (int(limit),)).fetchall()
            return [{"recorded_at": r["recorded_at"], "kind": r["kind"],
                     "subject": r["subject"],
                     "detail": _unj(r["detail_json"])} for r in rows]
        finally:
            conn.close()

    # -- hypotheses --------------------------------------------------------- #
    def register(self, *, title: str, release: str, origin: str,
                 generation_method: str, information_family: str,
                 economic_family: str, asset_class: str,
                 model_family: str = "LINEAR",
                 horizon_sessions: Optional[int] = None,
                 input_data_identity: str = "",
                 spec: Any = None, counts_to_burden: bool = True,
                 hyp_id: Optional[str] = None) -> str:
        """Register a hypothesis identity. Idempotent: re-registering the same
        identity returns it untouched (novelty is a property of the identity,
        not of how many times something proposed it).

        ``asset_class`` is validated against the R59 vocabulary and fails
        closed. An unrecognised label would create a scope the frontier has no
        substrate for and the fairness reservation would then under-count
        non-equity work without saying so.
        """
        self._guard_write()
        if asset_class not in r59.ASSET_CLASSES:
            raise ValueError(
                "unknown asset class %r; map it onto the R59 vocabulary "
                "before registering" % (asset_class,))
        fam = family_key(economic_family=economic_family,
                         information_family=information_family,
                         asset_class=asset_class, model_family=model_family)
        hid = hyp_id or hypothesis_id(family=fam, spec=spec if spec is not None
                                      else title)
        with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(
                    "SELECT hypothesis_id FROM hypotheses WHERE hypothesis_id=?",
                    (hid,)).fetchone()
                if cur:
                    return hid
                conn.execute(
                    "INSERT INTO hypotheses(hypothesis_id,family_key,title,"
                    "release,origin,generation_method,information_family,"
                    "economic_family,asset_class,horizon_sessions,model_family,"
                    "input_data_identity,counts_to_burden,spec_json,created_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (hid, fam, title, release, origin, generation_method,
                     information_family, economic_family, asset_class,
                     horizon_sessions, model_family, input_data_identity,
                     1 if counts_to_burden else 0, _j(spec), r59.now_iso()))
                conn.commit()
                return hid
            finally:
                conn.close()

    def record_result(self, hyp_id: str, *, outcome: str,
                      evidence_maturity: str = "HISTORICAL",
                      statistic: Optional[dict] = None,
                      economics: Optional[dict] = None,
                      turnover_cost: Optional[dict] = None,
                      robustness: Optional[dict] = None,
                      reason_rejected: Optional[str] = None,
                      reopen_condition: Optional[str] = None) -> None:
        """Settle a hypothesis with a measured result.

        ``evidence_maturity`` says what KIND of evidence this is. A historical
        backtest is HISTORICAL and can never be relabelled: converting one into
        forward evidence is the exact fraud R46 was built to prevent, so it is
        refused here rather than left to a caller's discipline.
        """
        self._guard_write()
        if outcome not in r59.HYPOTHESIS_OUTCOMES:
            raise ValueError("unknown outcome: %s" % outcome)
        if evidence_maturity == "FORWARD_CONFIRMED":
            raise ValueError(
                "record_result cannot mint forward evidence; forward "
                "confirmation is owned by the R46/R52 prospective runtime")
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE hypotheses SET outcome=?, evidence_maturity=?,"
                    " statistic_json=?, economics_json=?, turnover_cost_json=?,"
                    " robustness_json=?, reason_rejected=?, reopen_condition=?,"
                    " settled_at=? WHERE hypothesis_id=?",
                    (outcome, evidence_maturity, _j(statistic), _j(economics),
                     _j(turnover_cost), _j(robustness), reason_rejected,
                     reopen_condition, r59.now_iso(), hyp_id))
                conn.commit()
            finally:
                conn.close()

    def freeze_forward(self, hyp_id: str, *, challenger_id: str,
                       inception: str, record_hash: str = "") -> None:
        """Bind a hypothesis to a PROSPECTIVE challenger identity.

        Stores the inception instant and the frozen record's hash - never a
        score. Whatever that challenger goes on to earn is the R46/R52
        runtime's to measure, forward, from this instant on.
        """
        self._guard_write()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE hypotheses SET outcome=?, evidence_maturity=?,"
                    " forward_challenger=?, settled_at=? WHERE hypothesis_id=?",
                    (r59.HO_FORWARD_FROZEN, "PROSPECTIVE_INCEPTION",
                     json.dumps({"challenger_id": challenger_id,
                                 "inception": inception,
                                 "record_hash": record_hash,
                                 "forward_observations_at_freeze": 0},
                                sort_keys=True),
                     r59.now_iso(), hyp_id))
                conn.commit()
            finally:
                conn.close()

    def invalidate(self, *, economic_families: Iterable[str], reason: str,
                   release: Optional[str] = None) -> dict:
        """Mark settled hypotheses as resting on an invalid input.

        The rows are NOT deleted and NOT removed from the search burden: the
        search effort was really spent, and discounting it would flatter every
        later claim. What changes is that the evidence can no longer be read as
        a statement about the economics its family name implies - a result
        computed from a meaningless feature is not a negative finding about
        that economics, it is no finding at all.
        """
        self._guard_write()
        fams = tuple(economic_families)
        if not fams:
            return {"invalidated": 0}
        q = ("UPDATE hypotheses SET invalidated_reason=? WHERE economic_family"
             " IN (%s) AND outcome IS NOT NULL" % ",".join("?" * len(fams)))
        params: list = [reason, *fams]
        if release:
            q += " AND release=?"
            params.append(release)
        with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(q, params)
                n = cur.rowcount
                conn.commit()
            finally:
                conn.close()
        self.event("HYPOTHESES_INVALIDATED", subject=",".join(fams),
                   detail={"reason": reason, "rows": int(n),
                           "release": release})
        return {"invalidated": int(n), "families": list(fams),
                "reason": reason}

    def invalidated(self) -> list:
        conn = self._connect()
        try:
            return [self._row(r) for r in conn.execute(
                "SELECT * FROM hypotheses WHERE invalidated_reason IS NOT NULL"
            ).fetchall()]
        finally:
            conn.close()

    def get(self, hyp_id: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM hypotheses WHERE hypothesis_id=?",
                               (hyp_id,)).fetchone()
            return self._row(row) if row else None
        finally:
            conn.close()

    @staticmethod
    def _row(row: sqlite3.Row) -> dict:
        d = dict(row)
        for k in ("statistic_json", "economics_json", "turnover_cost_json",
                  "robustness_json", "spec_json", "forward_challenger"):
            if k in d:
                d[k.replace("_json", "")] = _unj(d.pop(k))
        d["counts_to_burden"] = bool(d.get("counts_to_burden"))
        return d

    def list_hypotheses(self, *, asset_class: Optional[str] = None,
                        economic_family: Optional[str] = None,
                        outcome: Optional[str] = None,
                        unsettled_only: bool = False,
                        limit: int = 5000) -> list:
        sql = "SELECT * FROM hypotheses WHERE 1=1"
        params: list = []
        if asset_class:
            sql += " AND asset_class=?"
            params.append(asset_class)
        if economic_family:
            sql += " AND economic_family=?"
            params.append(economic_family)
        if outcome:
            sql += " AND outcome=?"
            params.append(outcome)
        if unsettled_only:
            sql += " AND outcome IS NULL"
        sql += " ORDER BY created_at ASC LIMIT ?"
        params.append(int(limit))
        conn = self._connect()
        try:
            return [self._row(r) for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def get_meta(self, key: str, default: Any = None) -> Any:
        conn = self._connect()
        try:
            row = conn.execute("SELECT value FROM memory_meta WHERE key=?",
                               (key,)).fetchone()
            return _unj(row[0]) if row else default
        finally:
            conn.close()

    def set_meta(self, key: str, value: Any) -> None:
        self._guard_write()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO memory_meta(key,value)"
                    " VALUES(?,?)", (key, _j(value)))
                conn.commit()
            finally:
                conn.close()

    def count_settled(self, *, methods: Optional[Iterable[str]] = None) -> int:
        sql = "SELECT COUNT(*) FROM hypotheses WHERE outcome IS NOT NULL"
        params: list = []
        ms = [str(m) for m in (methods or ())]
        if ms:
            sql += " AND generation_method IN (%s)" % ",".join("?" for _ in ms)
            params.extend(ms)
        conn = self._connect()
        try:
            return int(conn.execute(sql, params).fetchone()[0])
        finally:
            conn.close()

    def recent_settled(self, *, limit: int = 600,
                       exclude_methods: Optional[Iterable[str]] = None) -> list:
        """The most recently SETTLED hypotheses, newest first.

        Deliberately a small projection rather than ``list_hypotheses``: the
        capacity allocator reads this on every mandate generation, and pulling
        four thousand fully hydrated rows to answer "what has the machine been
        spending itself on lately" would make the governor the most expensive
        thing in the loop.
        """
        excl = [str(m) for m in (exclude_methods or ())]
        sql = ("SELECT asset_class, generation_method, economic_family,"
               " outcome, statistic_json, settled_at FROM hypotheses"
               " WHERE outcome IS NOT NULL")
        params: list = []
        if excl:
            sql += " AND generation_method NOT IN (%s)" % ",".join(
                "?" for _ in excl)
            params.extend(excl)
        sql += " ORDER BY settled_at DESC, rowid DESC LIMIT ?"
        params.append(int(limit))
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
            return [{"asset_class": r[0], "generation_method": r[1],
                     "economic_family": r[2], "outcome": r[3],
                     "statistic": _unj(r[4]) or {}, "settled_at": r[5]}
                    for r in rows]
        finally:
            conn.close()

    def settled_between(self, *, since: Optional[str] = None,
                        until: Optional[str] = None,
                        limit: int = 50000) -> list:
        """Compact rows for everything SETTLED in a time window (R60).

        ``settled_at`` is the instant this MEMORY recorded a verdict, which is
        the import instant for a result a prior release originally measured.
        ``release`` therefore travels with every row so a caller can separate
        what the machine measured in the window from what was carried into it
        - reporting an import as a day's research output would be a lie the
        counts themselves cannot detect.
        """
        sql = ("SELECT hypothesis_id, release, origin, generation_method,"
               " economic_family, information_family, asset_class,"
               " horizon_sessions, outcome, reason_rejected, settled_at,"
               " invalidated_reason FROM hypotheses WHERE outcome IS NOT NULL")
        params: list = []
        if since:
            sql += " AND settled_at >= ?"
            params.append(str(since))
        if until:
            sql += " AND settled_at < ?"
            params.append(str(until))
        sql += " ORDER BY settled_at DESC, rowid DESC LIMIT ?"
        params.append(int(limit))
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def strongest_unqualified(self, *, limit: int = 8,
                              min_t: Optional[float] = None) -> list:
        """The settled hypotheses with the highest persisted lockbox t (R60).

        These are the "statistically interesting but not qualified" rows an
        operator asks about: what came CLOSEST, and which gate refused it.
        Ordering is by the SIGNED statistic, because a large negative t is not
        a near-miss for a long book - it is the opposite finding.

        Nothing is recomputed. The statistic, the economics, the robustness
        checks and the refusing gates are read back exactly as the evaluation
        kernel recorded them; invalidated rows are excluded because a result
        computed from a meaningless feature is not a near-miss either.
        """
        base = ("SELECT hypothesis_id, release, economic_family,"
                " information_family, asset_class, horizon_sessions,"
                " model_family, outcome, reason_rejected, settled_at,"
                " statistic_json, economics_json, robustness_json,"
                " reopen_condition FROM hypotheses"
                " WHERE outcome IS NOT NULL AND invalidated_reason IS NULL"
                " AND outcome <> ? AND statistic_json IS NOT NULL")
        conn = self._connect()
        try:
            rows = []
            try:
                sql = (base.replace("SELECT ", "SELECT"
                                    " json_extract(statistic_json,"
                                    " '$.lockbox_t') AS lockbox_t, ")
                       + " AND json_extract(statistic_json, '$.lockbox_t')"
                         " IS NOT NULL")
                if min_t is not None:
                    sql += " AND json_extract(statistic_json,"\
                           " '$.lockbox_t') >= %f" % float(min_t)
                sql += " ORDER BY lockbox_t DESC LIMIT ?"
                rows = conn.execute(
                    sql, (r59.HO_FORWARD_FROZEN, int(limit))).fetchall()
            except sqlite3.OperationalError:
                # A SQLite build without JSON1. Rank in Python over the same
                # rows rather than degrade the answer.
                rows = conn.execute(
                    base, (r59.HO_FORWARD_FROZEN,)).fetchall()
                scored = []
                for r in rows:
                    st = _unj(r["statistic_json"]) or {}
                    try:
                        t = float(st.get("lockbox_t"))
                    except (TypeError, ValueError):
                        continue
                    if min_t is not None and t < float(min_t):
                        continue
                    scored.append((t, r))
                scored.sort(key=lambda x: -x[0])
                rows = [r for _, r in scored[:int(limit)]]
            out = []
            for r in rows:
                d = dict(r)
                st = _unj(d.pop("statistic_json")) or {}
                d["statistic"] = st
                d["economics"] = _unj(d.pop("economics_json"))
                d["robustness"] = _unj(d.pop("robustness_json"))
                d["lockbox_t"] = d.get("lockbox_t", st.get("lockbox_t"))
                out.append(d)
            return out
        finally:
            conn.close()

    def count_unqualified_above_t(self, min_t: float = 2.0) -> int:
        """How many settled, non-invalidated rows recorded a lockbox t at or
        above ``min_t`` and were still refused (R60).

        A COUNT rather than a page of rows: the operator's question is "how
        much is interesting but unqualified", and hydrating hundreds of rows
        to answer it would make the read model the most expensive thing on
        the page.
        """
        conn = self._connect()
        try:
            try:
                return int(conn.execute(
                    "SELECT COUNT(*) FROM hypotheses WHERE outcome IS NOT NULL"
                    " AND invalidated_reason IS NULL AND outcome NOT IN (?,?)"
                    " AND statistic_json IS NOT NULL"
                    " AND json_extract(statistic_json, '$.lockbox_t') >= ?",
                    (r59.HO_QUALIFIED, r59.HO_FORWARD_FROZEN, float(min_t))
                ).fetchone()[0])
            except sqlite3.OperationalError:
                n = 0
                for row in conn.execute(
                        "SELECT statistic_json FROM hypotheses"
                        " WHERE outcome IS NOT NULL"
                        " AND invalidated_reason IS NULL"
                        " AND outcome NOT IN (?,?)"
                        " AND statistic_json IS NOT NULL",
                        (r59.HO_QUALIFIED, r59.HO_FORWARD_FROZEN)):
                    st = _unj(row[0]) or {}
                    try:
                        if float(st.get("lockbox_t")) >= float(min_t):
                            n += 1
                    except (TypeError, ValueError):
                        continue
                return n
        finally:
            conn.close()

    # -- novelty ------------------------------------------------------------ #
    def mechanism_state(self, *, asset_class: Optional[str] = None,
                        economic_family: Optional[str] = None,
                        information_family: Optional[str] = None,
                        model_family: Optional[str] = None) -> dict:
        """What the estate already knows about a MECHANISM, not one hypothesis.

        R68. :meth:`is_novel` answers "has this exact (family, spec) pair been
        tested", which is a different and much narrower question than "has this
        economic mechanism been prosecuted". MEASURED: ``is_novel`` returned
        ``novel: True`` for ``FUNDAMENTAL_MOMENTUM|DIVIDEND_DECLARATION_EVENTS|
        US_EQUITY|LONG_ONLY`` while that mechanism already carried a settled row,
        because the proposed spec hashed differently. A novelty check that can be
        passed by changing a parameter is not a duplicate check.

        Every filter is optional, so a caller can ask about a whole economic
        family or narrow to the exact four-part key. Nothing is settled or
        reopened here; this is a pure read.

        R72.1 - AN UNBOUND QUERY IS REFUSED, NOT ANSWERED.

        With every component unbound the predicate below is vacuously true, so
        the query matched EVERY row in research memory, found settled ones
        among them (there are thousands) and returned
        ``mechanism_is_settled: True``. The novelty probe turned that into
        ``SETTLED_DO_NOT_REPEAT`` - a governed refusal, indistinguishable from
        a real one, produced by a payload that named no mechanism at all. A
        malformed question must fail as malformed; the one thing it must never
        do is come back wearing the estate's most authoritative answer.

        ``None``, ``""`` and whitespace are all UNBOUND and all behave
        identically, so a blank component narrows nothing rather than filtering
        to rows whose family is literally empty - which would be the mirror
        defect, a false ``NO_ROW_IN_MEMORY`` read as "novel".

        A PARTIALLY bound query is valid and unchanged: asking about a whole
        asset class or a whole economic family is a real question with a real
        answer, and callers that ask it keep exactly what they always got.
        """
        asset_class = _none_if_unbound(asset_class)
        economic_family = _none_if_unbound(economic_family)
        information_family = _none_if_unbound(information_family)
        model_family = _none_if_unbound(model_family)
        bound = {k: v for k, v in (
            ("asset_class", asset_class),
            ("economic_family", economic_family),
            ("information_family", information_family),
            ("model_family", model_family)) if v is not None}
        if not bound:
            return {
                "asset_class": None, "economic_family": None,
                "information_family": None, "model_family": None,
                "query_valid": False,
                "invalid_query_reason": MECHANISM_QUERY_UNBOUND,
                "invalid_query_detail": (
                    "a mechanism check must bind at least one of "
                    "economic_family, information_family, asset_class or "
                    "model_family; an unbound query matches every row in "
                    "research memory and would report the whole estate's "
                    "settled history as this mechanism's own"),
                "bound_components": [], "n_bound_components": 0,
                # Deliberately the SAFE values. A caller that ignores
                # query_valid must still not be able to read a refusal here.
                "n_matching": 0, "n_settled": 0, "n_unsettled": 0,
                "outcomes": {}, "best_lockbox_t": None,
                "mechanism_is_settled": False,
                "hypothesis_ids": [], "reopen_conditions": [],
                "family_keys": [],
            }
        rows = self.list_hypotheses(limit=1000000)

        def _m(r):
            return ((asset_class is None or r.get("asset_class") == asset_class)
                    and (economic_family is None
                         or r.get("economic_family") == economic_family)
                    and (information_family is None
                         or r.get("information_family") == information_family)
                    and (model_family is None
                         or r.get("model_family") == model_family))

        hits = [r for r in rows if _m(r)]
        settled = [r for r in hits if r.get("outcome")]
        outcomes: dict = {}
        best_t = None
        for r in settled:
            oc = str(r.get("outcome"))
            outcomes[oc] = outcomes.get(oc, 0) + 1
            t = (r.get("statistic") or {}).get("lockbox_t")
            if isinstance(t, (int, float)) and (best_t is None or t > best_t):
                best_t = float(t)
        return {
            "asset_class": asset_class, "economic_family": economic_family,
            "information_family": information_family,
            "model_family": model_family,
            "query_valid": True, "invalid_query_reason": None,
            "bound_components": sorted(bound), "n_bound_components": len(bound),
            "n_matching": len(hits), "n_settled": len(settled),
            "n_unsettled": len(hits) - len(settled),
            "outcomes": outcomes, "best_lockbox_t": best_t,
            "mechanism_is_settled": bool(settled),
            "hypothesis_ids": [r.get("hypothesis_id") for r in hits][:50],
            "reopen_conditions": sorted({str(r.get("reopen_condition"))
                                         for r in settled
                                         if r.get("reopen_condition")}),
            "family_keys": sorted({str(r.get("family_key")) for r in hits
                                   if r.get("family_key")})[:25],
        }

    def is_novel(self, *, family: str, spec: Any,
                 asset_class: Optional[str] = None,
                 economic_family: Optional[str] = None,
                 information_family: Optional[str] = None,
                 model_family: Optional[str] = None) -> dict:
        """Has this exact hypothesis been tested before, and may it be retried?

        A settled hypothesis is NOT novel. It becomes eligible again only
        through an explicit reopen condition, which a caller must satisfy and
        declare - never by simply asking a second time.

        R68 - AND THE MECHANISM IS REPORTED BESIDE THE ANSWER.

        This is an identity check on ``(family, spec)``. It cannot see a sibling
        that tests the SAME economic mechanism with a different parameter, which
        is how a settled idea gets re-proposed under a new name. Passing the
        mechanism coordinates attaches :meth:`mechanism_state` to the result, so
        a caller physically cannot read ``novel: True`` without also seeing that
        the mechanism carries settled rows. The verdict itself is unchanged -
        callers that pass nothing get exactly what they always got - because
        whether a settled sibling BLOCKS a hypothesis is a director ruling and
        not this method's to make.
        """
        hid = hypothesis_id(family=family, spec=spec)
        row = self.get(hid)
        mech = None
        if any(v is not None for v in (asset_class, economic_family,
                                       information_family, model_family)):
            mech = self.mechanism_state(
                asset_class=asset_class, economic_family=economic_family,
                information_family=information_family,
                model_family=model_family)
        if row is None:
            out = {"novel": True, "hypothesis_id": hid, "prior": None}
        else:
            settled = row.get("outcome") is not None
            out = {"novel": not settled, "hypothesis_id": hid, "prior": row,
                   "reopen_condition": row.get("reopen_condition"),
                   "reason": ("already settled as %s" % row.get("outcome"))
                   if settled else "registered but never settled"}
        if mech is not None:
            out["mechanism"] = mech
            out["novel_but_the_mechanism_is_settled"] = bool(
                out["novel"] and mech["mechanism_is_settled"])
            if out["novel_but_the_mechanism_is_settled"]:
                out["mechanism_warning"] = (
                    "this exact (family, spec) pair is new, and the mechanism it "
                    "belongs to already carries %d settled row(s): %s. A novelty "
                    "check that can be passed by changing a parameter is not a "
                    "duplicate check - rule on the mechanism, not on this flag."
                    % (mech["n_settled"], mech["outcomes"]))
        return out

    # -- graveyard ---------------------------------------------------------- #
    def graveyard(self, *, asset_class: Optional[str] = None,
                  limit: int = 2000) -> list:
        """Everything that has been prosecuted to a negative verdict, with the
        condition that would justify reopening it."""
        sql = ("SELECT * FROM hypotheses WHERE outcome IN (?,?)")
        params: list = [r59.HO_REJECTED, r59.HO_NO_ALPHA_EVIDENCE]
        if asset_class:
            sql += " AND asset_class=?"
            params.append(asset_class)
        sql += " ORDER BY settled_at DESC LIMIT ?"
        params.append(int(limit))
        conn = self._connect()
        try:
            return [self._row(r) for r in conn.execute(sql, params).fetchall()]
        finally:
            conn.close()

    def reopenable(self, *, satisfied: Iterable[str]) -> list:
        """Graveyard rows whose declared reopen condition is in ``satisfied``.

        The caller supplies what has actually changed (a new information family
        landed, a coverage floor was cleared). Nothing reopens on its own.
        """
        sat = {s.strip().upper() for s in satisfied if s}
        if not sat:
            return []
        out = []
        for row in self.graveyard():
            cond = (row.get("reopen_condition") or "").strip().upper()
            if cond and cond in sat:
                out.append(row)
        return out

    # -- search burden ------------------------------------------------------ #
    def burden(self, *, asset_class: Optional[str] = None) -> dict:
        """Search burden, COUNTED from the memory rather than copied.

        ``total`` is every hypothesis that has ever been prosecuted and declared
        itself countable. ``by_family`` is the per-family denominator a
        Benjamini-Hochberg correction should use when a family is re-entered.
        """
        conn = self._connect()
        try:
            sql = ("SELECT family_key, economic_family, asset_class,"
                   " COUNT(*) AS n FROM hypotheses"
                   " WHERE counts_to_burden=1 AND outcome IS NOT NULL")
            params: list = []
            if asset_class:
                sql += " AND asset_class=?"
                params.append(asset_class)
            sql += " GROUP BY family_key"
            rows = conn.execute(sql, params).fetchall()
            by_family = {r["family_key"]: int(r["n"]) for r in rows}
            by_asset: dict = {}
            for r in rows:
                by_asset[r["asset_class"]] = \
                    by_asset.get(r["asset_class"], 0) + int(r["n"])
            return {"total": sum(by_family.values()),
                    "distinct_families": len(by_family),
                    "by_family": by_family, "by_asset_class": by_asset}
        finally:
            conn.close()

    # -- generator yield ---------------------------------------------------- #
    def record_generator_yield(self, *, asset_class: str, kind: str,
                               generated: int, novel: int,
                               duplicates: int) -> None:
        """Accumulate how productive a generative family still is in a scope.

        A generative search space is not spent by one verdict, but it IS spent
        when the generator stops producing books the estate has not already
        measured. Recording the yield is what lets exhaustion be MEASURED
        rather than assumed - and it is the only thing that lets the loop reach
        a genuine terminal state instead of running until a clock stops it.
        """
        self._guard_write()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO generator_yield(asset_class,kind,generated,"
                    "novel,duplicates,batches,updated_at) VALUES(?,?,?,?,?,1,?)"
                    " ON CONFLICT(asset_class,kind) DO UPDATE SET"
                    " generated=generated+excluded.generated,"
                    " novel=novel+excluded.novel,"
                    " duplicates=duplicates+excluded.duplicates,"
                    " batches=batches+1, updated_at=excluded.updated_at",
                    (asset_class, kind, int(generated), int(novel),
                     int(duplicates), r59.now_iso()))
                conn.commit()
            finally:
                conn.close()

    def generator_yield(self, *, asset_class: Optional[str] = None) -> list:
        conn = self._connect()
        try:
            if asset_class:
                rows = conn.execute(
                    "SELECT * FROM generator_yield WHERE asset_class=?",
                    (asset_class,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM generator_yield").fetchall()
            out = []
            for r in rows:
                d = dict(r)
                g = max(1, int(d["generated"]))
                d["novelty_yield"] = round(int(d["novel"]) / g, 4)
                out.append(d)
            return out
        finally:
            conn.close()

    # -- frontier ----------------------------------------------------------- #
    def set_frontier(self, asset_class: str, *, state: str,
                     reason: str = "", detail: Optional[dict] = None) -> None:
        self._guard_write()
        if state not in r59.FRONTIER_STATES:
            raise ValueError("unknown frontier state: %s" % state)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO frontier(asset_class,state,reason,detail_json,"
                    "updated_at) VALUES(?,?,?,?,?)"
                    " ON CONFLICT(asset_class) DO UPDATE SET state=excluded.state,"
                    " reason=excluded.reason, detail_json=excluded.detail_json,"
                    " updated_at=excluded.updated_at",
                    (asset_class, state, reason, _j(detail), r59.now_iso()))
                conn.commit()
            finally:
                conn.close()

    def get_frontier(self) -> dict:
        conn = self._connect()
        try:
            rows = conn.execute("SELECT * FROM frontier").fetchall()
            return {r["asset_class"]: {"state": r["state"],
                                       "reason": r["reason"],
                                       "detail": _unj(r["detail_json"]),
                                       "updated_at": r["updated_at"]}
                    for r in rows}
        finally:
            conn.close()

    # -- data opportunities ------------------------------------------------- #
    def set_opportunity(self, opportunity_id: str, *, title: str, state: str,
                        asset_class: Optional[str] = None,
                        information_need: str = "",
                        unlocks: Optional[list] = None,
                        owned_but_unused: bool = False,
                        free_proxy: Optional[str] = None,
                        provider: Optional[str] = None,
                        pit_integrity: Optional[str] = None,
                        effective_sample: Optional[str] = None,
                        expected_value: Optional[str] = None,
                        cost_usd_year: Optional[float] = None,
                        gate_verdict: Optional[str] = None,
                        detail: Optional[dict] = None) -> None:
        self._guard_write()
        if state not in r59.DATA_OPPORTUNITY_STATES:
            raise ValueError("unknown opportunity state: %s" % state)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO opportunities(opportunity_id,title,state,"
                    "asset_class,information_need,unlocks_json,owned_but_unused,"
                    "free_proxy,provider,pit_integrity,effective_sample,"
                    "expected_value,cost_usd_year,gate_verdict,detail_json,"
                    "updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(opportunity_id) DO UPDATE SET"
                    " title=excluded.title, state=excluded.state,"
                    " asset_class=excluded.asset_class,"
                    " information_need=excluded.information_need,"
                    " unlocks_json=excluded.unlocks_json,"
                    " owned_but_unused=excluded.owned_but_unused,"
                    " free_proxy=excluded.free_proxy, provider=excluded.provider,"
                    " pit_integrity=excluded.pit_integrity,"
                    " effective_sample=excluded.effective_sample,"
                    " expected_value=excluded.expected_value,"
                    " cost_usd_year=excluded.cost_usd_year,"
                    " gate_verdict=excluded.gate_verdict,"
                    " detail_json=excluded.detail_json,"
                    " updated_at=excluded.updated_at",
                    (opportunity_id, title, state, asset_class,
                     information_need, _j(unlocks),
                     1 if owned_but_unused else 0, free_proxy, provider,
                     pit_integrity, effective_sample, expected_value,
                     cost_usd_year, gate_verdict, _j(detail), r59.now_iso()))
                conn.commit()
            finally:
                conn.close()

    def opportunities(self, *, state: Optional[str] = None) -> list:
        conn = self._connect()
        try:
            if state:
                rows = conn.execute(
                    "SELECT * FROM opportunities WHERE state=?"
                    " ORDER BY opportunity_id", (state,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM opportunities ORDER BY opportunity_id"
                ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["unlocks"] = _unj(d.pop("unlocks_json"))
                d["detail"] = _unj(d.pop("detail_json"))
                d["owned_but_unused"] = bool(d["owned_but_unused"])
                out.append(d)
            return out
        finally:
            conn.close()

    # -- director rulings (R72) --------------------------------------------- #
    def record_director_ruling(self, *, asset_class: str,
                               economic_family: str, verdict: str,
                               blocker_reason: str, rationale: str,
                               information_family: Optional[str] = None,
                               model_family: Optional[str] = None,
                               reopen_condition: Optional[str] = None,
                               campaign_id: Optional[str] = None,
                               decided_by: Optional[str] = None,
                               decision_date: Optional[str] = None,
                               source_artifact: Optional[str] = None,
                               detail: Optional[dict] = None) -> dict:
        """Make ONE director ruling on a MECHANISM durably readable.

        The governor and :mod:`alpha_agent.r59.blockers` read this database; a
        campaign JSON file is not durable research state, and a ruling that
        never reaches the reader is a ruling nobody made. This is the seam, and
        it is the same shape as every other defect this estate has found on
        one: a fact that is real, knowable, and unreachable by its reader.

        ``blocker_reason`` MUST already be in the canonical taxonomy. A ruling
        makes an existing code AUTHORITATIVE for a family; it may not invent a
        twelfth reason, because a second vocabulary is a second owner.

        R72.1 - ``information_family`` and ``model_family`` SCOPE the ruling.
        Omitting them (the pre-R72.1 call, and every existing caller) records a
        FAMILY-WIDE ruling that reaches every mechanism in the family, exactly
        as before. Naming them binds the ruling to that one mechanism, so a
        refusal aimed at SEC comment letters cannot terminate a trading-halt
        cell that merely shares an economic family.

        Writes no hypothesis, spends no burden, enqueues nothing and schedules
        nothing. Re-recording the same MECHANISM replaces its ruling, so the
        newest director's word stands - and the previous one is kept in the
        event log rather than silently dropped. A mechanism-scoped ruling never
        replaces the family-wide one; they are different rows and both stand,
        with the more specific winning at read time.
        """
        self._guard_write()
        from . import blockers as _B
        if blocker_reason not in _B.BLOCKER_REASONS:
            raise ValueError(
                "blocker_reason %r is not in the canonical taxonomy %s; a "
                "ruling makes an existing code authoritative and may not add "
                "a new one" % (blocker_reason, list(_B.BLOCKER_REASONS)))
        if not asset_class or not economic_family:
            raise ValueError("a ruling must name an asset class and a family")
        if RULING_ANY in (str(asset_class), str(economic_family)):
            raise ValueError(
                "a ruling must name a real asset class and economic family; "
                "%r is the mechanism wildcard and may only scope "
                "information_family or model_family" % RULING_ANY)
        if not str(rationale or "").strip():
            raise ValueError(
                "a ruling without a rationale cannot be audited later; the "
                "director's reason is the whole point of recording it")
        info = _ruling_component(information_family)
        model = _ruling_component(model_family)
        scope = ruling_scope(info, model)
        # The row this write REPLACES is the one with this exact key, never
        # whatever a read would have resolved: replacing the family-wide ruling
        # because a mechanism-scoped write found it would be the silent
        # broadening this release exists to prevent.
        prior = self._director_ruling_exact(
            asset_class=asset_class, economic_family=economic_family,
            information_family=info, model_family=model)
        row = (str(asset_class), str(economic_family), info, model,
               str(verdict), str(blocker_reason), str(rationale),
               (str(reopen_condition) if reopen_condition else None),
               (str(campaign_id) if campaign_id else None),
               (str(decided_by) if decided_by else None),
               (str(decision_date) if decision_date else None),
               (str(source_artifact) if source_artifact else None),
               _j(detail or {}), r59.now_iso())
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO director_rulings (asset_class,"
                    " economic_family, information_family, model_family,"
                    " verdict, blocker_reason, rationale,"
                    " reopen_condition, campaign_id, decided_by,"
                    " decision_date, source_artifact, detail_json, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(asset_class, economic_family,"
                    " information_family, model_family) DO UPDATE SET"
                    " verdict=excluded.verdict,"
                    " blocker_reason=excluded.blocker_reason,"
                    " rationale=excluded.rationale,"
                    " reopen_condition=excluded.reopen_condition,"
                    " campaign_id=excluded.campaign_id,"
                    " decided_by=excluded.decided_by,"
                    " decision_date=excluded.decision_date,"
                    " source_artifact=excluded.source_artifact,"
                    " detail_json=excluded.detail_json,"
                    " updated_at=excluded.updated_at", row)
                conn.commit()
            finally:
                conn.close()
        self.event("DIRECTOR_RULING_RECORDED",
                   subject="%s|%s|%s|%s" % (economic_family, info,
                                            asset_class, model),
                   detail={"verdict": verdict, "blocker_reason": blocker_reason,
                           "campaign_id": campaign_id,
                           "decided_by": decided_by,
                           "information_family": info, "model_family": model,
                           "ruling_scope": scope,
                           "reopen_condition": reopen_condition,
                           "replaced": prior or None})
        return {"recorded": True, "asset_class": asset_class,
                "economic_family": economic_family,
                "information_family": info, "model_family": model,
                "ruling_scope": scope, "verdict": verdict,
                "blocker_reason": blocker_reason, "replaced": prior or None}

    @staticmethod
    def _ruling_row(row) -> dict:
        d = dict(row)
        d["detail"] = _unj(d.pop("detail_json", None))
        d["ruling_scope"] = ruling_scope(d.get("information_family"),
                                         d.get("model_family"))
        return d

    def _director_ruling_exact(self, *, asset_class: str,
                               economic_family: str,
                               information_family: str,
                               model_family: str) -> Optional[dict]:
        """The ruling stored under EXACTLY this key, with no fallback."""
        conn = self._connect()
        try:
            if _ruling_table_has_mechanism_identity(conn):
                row = conn.execute(
                    "SELECT * FROM director_rulings WHERE asset_class=?"
                    " AND economic_family=? AND information_family=?"
                    " AND model_family=?",
                    (str(asset_class), str(economic_family),
                     str(information_family), str(model_family))).fetchone()
            elif (str(information_family), str(model_family)) == (RULING_ANY,
                                                                  RULING_ANY):
                # Legacy table: every row in it IS family-wide, so only the
                # family-wide key can have an exact match.
                row = conn.execute(
                    "SELECT * FROM director_rulings WHERE asset_class=?"
                    " AND economic_family=?",
                    (str(asset_class), str(economic_family))).fetchone()
            else:
                row = None
        finally:
            conn.close()
        return self._ruling_row(row) if row else None

    def director_ruling(self, *, asset_class: str, economic_family: str,
                        information_family: Optional[str] = None,
                        model_family: Optional[str] = None
                        ) -> Optional[dict]:
        """The ruling that governs ONE mechanism, or ``None``.

        ``None`` is a real answer and is never an implied clearance: a
        mechanism nobody has ruled on keeps whatever its own engine recorded.

        R72.1 - resolution is MOST-SPECIFIC-FIRST within the family:

          1. the exact mechanism ``(information_family, model_family)``
          2. its information family, any model  ``(info, '*')``
          3. any information, its model family  ``(  '*', model)``
          4. the family-wide ruling             ``(  '*',   '*')``

        A CALLER THAT NAMES NO MECHANISM CAN ONLY MATCH STEP 4. This is the
        whole point of the release: the R59 queue's blocked jobs carry only
        ``(asset_class, family)``, so if an unnamed mechanism could match a
        mechanism-scoped ruling, a refusal written about SEC comment letters
        would silently terminate every other cell in EVENT_OVERREACTION. A
        ruling reaches a job only when the job is demonstrably inside it.

        The returned row carries ``ruling_scope`` and ``matched_on`` so a
        reader can always see WHICH of the four steps answered, and therefore
        audit the reach of the ruling rather than trust it.
        """
        info = _ruling_component(information_family)
        model = _ruling_component(model_family)
        candidates = [(info, model)]
        if info != RULING_ANY:
            candidates.append((info, RULING_ANY))
        if model != RULING_ANY:
            candidates.append((RULING_ANY, model))
        if (RULING_ANY, RULING_ANY) not in candidates:
            candidates.append((RULING_ANY, RULING_ANY))
        conn = self._connect()
        try:
            if not _ruling_table_has_mechanism_identity(conn):
                # Legacy table met by a read-only handle. Every row in it is
                # family-wide, so resolution collapses to step 4 - which is
                # exactly what this store meant before R72.1. The answer is
                # reported with its real reach, not with the mechanism the
                # caller asked about.
                candidates = [(RULING_ANY, RULING_ANY)]
                legacy_sql = ("SELECT * FROM director_rulings WHERE"
                              " asset_class=? AND economic_family=?")
            else:
                legacy_sql = None
            for cand_info, cand_model in candidates:
                if legacy_sql:
                    row = conn.execute(
                        legacy_sql,
                        (str(asset_class), str(economic_family))).fetchone()
                else:
                    row = conn.execute(
                        "SELECT * FROM director_rulings WHERE asset_class=?"
                        " AND economic_family=? AND information_family=?"
                        " AND model_family=?",
                        (str(asset_class), str(economic_family),
                         cand_info, cand_model)).fetchone()
                if row:
                    d = self._ruling_row(row)
                    d["matched_on"] = {"information_family": cand_info,
                                       "model_family": cand_model}
                    d["asked"] = {"asset_class": str(asset_class),
                                  "economic_family": str(economic_family),
                                  "information_family": info,
                                  "model_family": model}
                    d["matched_exactly"] = (cand_info, cand_model) == (info,
                                                                       model)
                    return d
        finally:
            conn.close()
        return None

    def director_rulings(self, *, asset_class: Optional[str] = None,
                         economic_family: Optional[str] = None) -> list:
        """Every recorded ruling, newest first."""
        sql = "SELECT * FROM director_rulings WHERE 1=1"
        params: list = []
        if asset_class:
            sql += " AND asset_class=?"
            params.append(str(asset_class))
        if economic_family:
            sql += " AND economic_family=?"
            params.append(str(economic_family))
        sql += " ORDER BY updated_at DESC"
        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
        return [self._ruling_row(r) for r in rows]

    # -- provider utilisation ----------------------------------------------- #
    def set_provider_usage(self, provider: str, data_class: str, *,
                           coverage: Optional[dict] = None,
                           detail: Optional[dict] = None) -> None:
        self._guard_write()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO provider_usage(provider,data_class,"
                    "coverage_json,detail_json,updated_at) VALUES(?,?,?,?,?)"
                    " ON CONFLICT(provider,data_class) DO UPDATE SET"
                    " coverage_json=excluded.coverage_json,"
                    " detail_json=excluded.detail_json,"
                    " updated_at=excluded.updated_at",
                    (provider, data_class, _j(coverage), _j(detail),
                     r59.now_iso()))
                conn.commit()
            finally:
                conn.close()

    def provider_usage(self, provider: Optional[str] = None) -> list:
        conn = self._connect()
        try:
            if provider:
                rows = conn.execute(
                    "SELECT * FROM provider_usage WHERE provider=?"
                    " ORDER BY data_class", (provider,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM provider_usage ORDER BY provider,data_class"
                ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["coverage"] = _unj(d.pop("coverage_json"))
                d["detail"] = _unj(d.pop("detail_json"))
                out.append(d)
            return out
        finally:
            conn.close()

    # -- summary ------------------------------------------------------------ #
    def summary(self) -> dict:
        conn = self._connect()
        try:
            total = conn.execute("SELECT COUNT(*) c FROM hypotheses").fetchone()["c"]
            settled = conn.execute(
                "SELECT COUNT(*) c FROM hypotheses WHERE outcome IS NOT NULL"
            ).fetchone()["c"]
            by_outcome = {r["outcome"]: r["c"] for r in conn.execute(
                "SELECT outcome, COUNT(*) c FROM hypotheses"
                " WHERE outcome IS NOT NULL GROUP BY outcome").fetchall()}
            by_asset = {r["asset_class"]: r["c"] for r in conn.execute(
                "SELECT asset_class, COUNT(*) c FROM hypotheses"
                " GROUP BY asset_class").fetchall()}
            by_release = {r["release"]: r["c"] for r in conn.execute(
                "SELECT release, COUNT(*) c FROM hypotheses"
                " GROUP BY release").fetchall()}
            by_method = {r["generation_method"]: r["c"] for r in conn.execute(
                "SELECT generation_method, COUNT(*) c FROM hypotheses"
                " GROUP BY generation_method").fetchall()}
        finally:
            conn.close()
        return {
            "schema_version": SCHEMA_VERSION,
            "db_path": str(self.db_path),
            "hypotheses_total": int(total),
            "hypotheses_settled": int(settled),
            "hypotheses_open": int(total) - int(settled),
            "by_outcome": by_outcome,
            "by_asset_class": by_asset,
            "by_release": by_release,
            "by_generation_method": by_method,
            "search_burden": self.burden(),
            "frontier": self.get_frontier(),
            "data_opportunities": len(self.opportunities()),
        }


def open_memory(db_path: Optional[Path] = None) -> ResearchMemory:
    return ResearchMemory(db_path)


def memory_present(db_path: Optional[Path] = None) -> bool:
    """Does the persistent memory exist? Answered WITHOUT creating it."""
    return memory_db_path(db_path).exists()


def open_memory_readonly(db_path: Optional[Path] = None) -> ResearchMemory:
    """Open the memory for reading only.

    The ONE way a read model may reach this store. It creates nothing, runs
    no schema script and refuses every mutating method; a missing database
    raises ``FileNotFoundError`` so a caller reports absence rather than
    silently reporting an empty research record as a measured zero.
    """
    return ResearchMemory(db_path, read_only=True)
