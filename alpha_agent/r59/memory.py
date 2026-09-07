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


class ResearchMemory:
    """The persistent memory. One writer process at a time; WAL + busy timeout
    make a concurrent reader safe."""

    def __init__(self, db_path: Optional[Path] = None, *,
                 busy_timeout_ms: int = 10000):
        self.db_path = Path(db_path) if db_path else \
            (r59.research_root() / DB_NAME)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._busy = int(busy_timeout_ms)
        self._lock = threading.RLock()
        self._init_schema()

    # -- plumbing ----------------------------------------------------------- #
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=self._busy / 1000.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=%d" % self._busy)
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

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

    # -- novelty ------------------------------------------------------------ #
    def is_novel(self, *, family: str, spec: Any) -> dict:
        """Has this exact hypothesis been tested before, and may it be retried?

        A settled hypothesis is NOT novel. It becomes eligible again only
        through an explicit reopen condition, which a caller must satisfy and
        declare - never by simply asking a second time.
        """
        hid = hypothesis_id(family=family, spec=spec)
        row = self.get(hid)
        if row is None:
            return {"novel": True, "hypothesis_id": hid, "prior": None}
        settled = row.get("outcome") is not None
        return {"novel": not settled, "hypothesis_id": hid, "prior": row,
                "reopen_condition": row.get("reopen_condition"),
                "reason": ("already settled as %s" % row.get("outcome"))
                if settled else "registered but never settled"}

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

    # -- provider utilisation ----------------------------------------------- #
    def set_provider_usage(self, provider: str, data_class: str, *,
                           coverage: Optional[dict] = None,
                           detail: Optional[dict] = None) -> None:
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
