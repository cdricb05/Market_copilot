"""
alpha_agent.sec_companyfacts_index — Stage 10.2 SEC COMPANYFACTS BULK INDEX.

The missing reusable BULK-INGESTION + INDEXING capability that turns the released
historical security->CIK bridge (Stage 10.1) into a survivorship-safe historical
PIT FUNDAMENTAL dataset. Stage 10.1 resolved the mapped historical universe; the
current Stage 9.5 companyfacts campaign fetches facts one CIK at a time from the
network and is scoped to CURRENT companies, so historical PIT depth over the
mapped delisted universe stayed shallow. This module ingests the COMPLETE free
SEC companyfacts XBRL corpus — by STREAMING the official ``companyfacts.zip`` bulk
archive — into a durable, queryable fact index, WITHOUT loading the archive into
memory and WITHOUT re-fetching per CIK over the network.

Design invariants (mirrors ``sec_issuer_index.SecIssuerIndex``):
  * COMPLETE, not sampled — every primary companyfacts member in the archive is
    visited and CATALOGUED (a lightweight per-CIK row), so "indexed CIK count" is
    the whole archive; the full XBRL facts are materialised only for the resolved
    mapped-CIK allowlist that the historical evaluation actually needs (Part 4:
    "materialize PIT observations only for resolved historical CIKs"). Bounded per
    step, restart-safe via a durable member cursor + wall-clock budget.
  * DURABLE / APPEND-ONLY / IDEMPOTENT / RESTART-SAFE / CONTENT-HASH VERSIONED —
    replaying the same archive adds zero duplicate fact rows (UNIQUE per
    cik/tag/unit/period/accession preserves amendments as DISTINCT rows); a changed
    archive OR a changed allowlist records a new revision and re-streams.
  * QUERYABLE by canonical zero-padded CIK and by concept — exactly what the PIT
    materialiser + per-rebalance readiness need.
  * The historical knowledge boundary is the SEC ``filed`` date on each fact; a
    later amendment/restatement is a SEPARATE row and never rewrites the earlier
    as-of state (leakage-safe when consumed through ``pit_fundamentals``).
  * OUTSIDE operational-ledger roots — one stdlib sqlite file under the research
    root; no order / fill / signal / decision / model touch anywhere. Missing
    facts stay missing (never zero-filled); unsupported units recorded explicitly.

Pure stdlib. The zip path is injected so the module is unit-testable with a tiny
in-memory companyfacts archive and no real network.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from .historical_identity import canonical_json, content_hash, norm_cik

STAGE = "10.2"
INDEX_SCHEMA_VERSION = "sec-companyfacts-index-1.0.0"
PARSER_VERSION = "sec-companyfacts-parser-1.0.0"

# A primary per-CIK companyfacts member: ``CIK##########.json`` (10-digit). The
# archive is a flat set of these; ``member.split('/')[-1]`` tolerates a subdir.
_PRIMARY_RE = re.compile(r"^CIK(\d{10})\.json$")

# Only USD monetary facts are treated as monetary values (mirrors
# ``sec_companyfacts.normalize_unit``); other units are recorded on the row but
# never used as a monetary concept value.
_MONETARY_UNITS = {"USD"}

# The taxonomies whose facts are indexed (us-gaap carries every candidate concept).
_TAXONOMIES = ("us-gaap",)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _is_safe_member(name: str) -> bool:
    """Reject zip path traversal / absolute / drive-escaping member names before
    they are ever opened (defence in depth even though members are streamed into
    memory, never extracted to disk)."""
    if not name or name != name.strip():
        return False
    if name.startswith("/") or name.startswith("\\"):
        return False
    if ".." in name.replace("\\", "/").split("/"):
        return False
    if ":" in name:  # drive-letter escape on Windows
        return False
    return True


def default_target_tags() -> frozenset:
    """The us-gaap tags the three registered candidate families (gross
    profitability, asset growth, balance-sheet quality) + their deterministic
    fallbacks need — the exact scope ``pit_fundamentals`` can map."""
    from . import sec_companyfacts as _cf
    return frozenset(_cf.TARGET_TAGS)


# --------------------------------------------------------------------------- #
# Companyfacts document parsing (fact-level, filed-date bounded, amendments
# preserved). Deterministic; read-only. Returns FULL fact rows (taxonomy,
# concept, unit, value, start, end, filed, accession, form, fy, fp, frame).
# --------------------------------------------------------------------------- #
def parse_companyfacts_facts(doc: dict, *, cik: Optional[str] = None,
                             target_tags: Optional[Iterable[str]] = None
                             ) -> list[dict]:
    """Parse an SEC companyfacts document into normalised fact rows for the target
    us-gaap tags. Accession-level dedup within the document ``(accn, tag, unit,
    end, fy, fp)``; a fact missing a ``filed`` date / ``val`` / ``accn`` is
    dropped (no availability boundary). Only monetary-USD facts are emitted as
    values. Deterministic, sorted."""
    tags = set(target_tags) if target_tags is not None else set(
        default_target_tags())
    facts = ((doc or {}).get("facts") or {})
    doc_cik = cik or doc.get("cik")
    c = norm_cik(doc_cik) if doc_cik is not None else None
    out: list[dict] = []
    seen: set = set()
    for taxonomy in _TAXONOMIES:
        tax = facts.get(taxonomy) or {}
        for tag, node in tax.items():
            if tag not in tags:
                continue
            label = (node or {}).get("label")
            desc = (node or {}).get("description")
            units = (node or {}).get("units") or {}
            for unit, arr in units.items():
                u = str(unit or "").strip() or "UNKNOWN"
                if u not in _MONETARY_UNITS:
                    continue
                for f in (arr or []):
                    accn = f.get("accn")
                    end = f.get("end")
                    fy, fp = f.get("fy"), f.get("fp")
                    filed = f.get("filed")
                    val = f.get("val")
                    if filed is None or val is None or accn is None:
                        continue
                    key = (accn, tag, u, end, fy, fp)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append({
                        "cik": c, "taxonomy": taxonomy, "concept": tag,
                        "label": label, "description": desc, "unit": u,
                        "value": val, "period_start": f.get("start"),
                        "period_end": end, "fy": fy, "fp": fp,
                        "frame": f.get("frame"), "form": f.get("form"),
                        "filed": str(filed)[:10], "accession": accn})
    out.sort(key=lambda p: (str(p.get("concept")), str(p.get("filed")),
                            str(p.get("period_end")), str(p.get("accession"))))
    return out


def _entity_summary(doc: dict, facts: list[dict]) -> dict:
    """Cheap per-member summary for the catalogue row (entity name, taxonomies,
    target-fact count, first/last filed over the target facts)."""
    taxos = sorted((doc.get("facts") or {}).keys()) if isinstance(doc, dict) \
        else []
    filed = [f["filed"] for f in facts if f.get("filed")]
    return {"entity_name": (doc.get("entityName") if isinstance(doc, dict)
                            else None),
            "taxonomies": taxos, "target_facts": len(facts),
            "first_filed": min(filed) if filed else None,
            "last_filed": max(filed) if filed else None}


_INDEX_SCHEMA = """
CREATE TABLE IF NOT EXISTS cf_index_meta (
    key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cf_member (
    cik TEXT PRIMARY KEY,
    entity_name TEXT, member TEXT,
    taxonomies_json TEXT NOT NULL DEFAULT '[]',
    target_facts INTEGER NOT NULL DEFAULT 0,
    first_filed TEXT, last_filed TEXT,
    materialized INTEGER NOT NULL DEFAULT 0,
    malformed INTEGER NOT NULL DEFAULT 0,
    source_archive_hash TEXT, content_hash TEXT,
    cataloged_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cf_fact (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cik TEXT NOT NULL, taxonomy TEXT NOT NULL, concept_tag TEXT NOT NULL,
    label TEXT, unit TEXT NOT NULL, value REAL,
    period_start TEXT, period_end TEXT, filed TEXT NOT NULL,
    accession TEXT NOT NULL, form TEXT, fy TEXT, fp TEXT, frame TEXT,
    source_archive_hash TEXT, source_member TEXT, parser_version TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(cik, concept_tag, unit, period_end, fy, fp, accession));
CREATE INDEX IF NOT EXISTS ix_cf_fact_cik ON cf_fact(cik);
CREATE INDEX IF NOT EXISTS ix_cf_fact_cik_concept ON cf_fact(cik, concept_tag);
CREATE TABLE IF NOT EXISTS cf_archive_cursor (
    archive_name TEXT PRIMARY KEY,
    archive_hash TEXT, allowlist_hash TEXT,
    total_members INTEGER NOT NULL DEFAULT 0,
    members_done INTEGER NOT NULL DEFAULT 0,
    members_cataloged INTEGER NOT NULL DEFAULT 0,
    ciks_materialized INTEGER NOT NULL DEFAULT 0,
    facts_indexed INTEGER NOT NULL DEFAULT 0,
    skipped_members INTEGER NOT NULL DEFAULT 0,
    malformed_members INTEGER NOT NULL DEFAULT 0,
    complete INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS cf_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL, cik TEXT, before_json TEXT, after_json TEXT,
    created_at TEXT NOT NULL);
"""


class SecCompanyFactsIndex:
    """Durable, append-only, versioned SEC companyfacts fact index (one stdlib
    sqlite file under the research root, NEVER an operational-ledger root).
    Idempotent replay, restart-safe streaming, deterministic hashes, exact
    provenance. ``clock`` is an injectable ``() -> iso`` for deterministic tests."""

    def __init__(self, db_path: str | Path, *,
                 clock: Optional[Callable[[], str]] = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or _utc_now_iso
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=60.0,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=60000")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(_INDEX_SCHEMA)
                conn.execute(
                    "INSERT OR IGNORE INTO cf_index_meta(key,value) VALUES(?,?)",
                    ("schema_version", INDEX_SCHEMA_VERSION))
            finally:
                conn.close()

    # -- meta --------------------------------------------------------------- #
    def get_meta(self, key: str) -> Optional[str]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT value FROM cf_index_meta WHERE key=?",
                               (key,)).fetchone()
            return row["value"] if row else None
        finally:
            conn.close()

    # -- streaming companyfacts archive indexer (restart-safe) -------------- #
    def index_companyfacts_archive(self, zip_path: str | Path, *,
                                   archive_hash: Optional[str] = None,
                                   allowlist_ciks: Optional[Iterable[str]] = None,
                                   target_tags: Optional[Iterable[str]] = None,
                                   archive_name: str = "companyfacts",
                                   member_step: int = 5000,
                                   time_budget_seconds: float = 1500.0,
                                   max_members: Optional[int] = None) -> dict:
        """Stream the SEC companyfacts bulk archive and (a) CATALOGUE every valid
        primary member (a lightweight per-CIK row — the complete, not-sampled
        archive), and (b) MATERIALISE the full target-concept facts for every CIK
        in ``allowlist_ciks`` (the resolved mapped historical universe). Resumes
        from the durable member cursor; bounded by a per-step member count AND a
        wall-clock budget so ONE queue job makes large restart-safe progress
        without loading the archive into memory. A changed ``archive_hash`` OR a
        changed allowlist resets the cursor and records a revision. Path-traversal
        / malformed members are isolated and counted, never fatal. Idempotent:
        fact rows are UNIQUE per (cik, tag, unit, period, accession) so amendments
        are preserved as distinct rows and a re-stream adds none."""
        zip_path = Path(zip_path)
        if not zip_path.exists():
            return {"ok": False, "reason": "archive not present: %s" % zip_path,
                    "complete": False}
        allow = {norm_cik(c) for c in (allowlist_ciks or []) if norm_cik(c)}
        allow_hash = content_hash(sorted(allow)) if allow else "none"
        tags = frozenset(target_tags) if target_tags is not None else \
            default_target_tags()
        started = time.monotonic()
        now0 = self._clock()
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            total = len(names)
            cur = self._archive_cursor(archive_name)
            reset = False
            if cur is None:
                reset = True
            elif (archive_hash and cur.get("archive_hash")
                  and cur["archive_hash"] != archive_hash):
                self._record_revision(archive_name, cur.get("archive_hash"),
                                      archive_hash, "ARCHIVE_REVISION", now0)
                reset = True
            elif cur.get("allowlist_hash") != allow_hash:
                self._record_revision(archive_name, cur.get("allowlist_hash"),
                                      allow_hash, "ALLOWLIST_REVISION", now0)
                reset = True
            if reset:
                self._reset_cursor(archive_name, archive_hash, allow_hash, total,
                                   now0)
                cur = self._archive_cursor(archive_name)
            done = int(cur["members_done"])
            cataloged = int(cur["members_cataloged"])
            materialized = int(cur["ciks_materialized"])
            facts = int(cur["facts_indexed"])
            skipped = int(cur["skipped_members"])
            malformed = int(cur["malformed_members"])
            if max_members is not None:
                total = min(total, int(max_members))
            while done < total and (time.monotonic() - started) < \
                    float(time_budget_seconds):
                hi = min(done + max(1, int(member_step)), total)
                now = self._clock()
                with self._lock:
                    conn = self._connect()
                    try:
                        conn.execute("BEGIN IMMEDIATE")
                        for idx in range(done, hi):
                            member = names[idx]
                            if not _is_safe_member(member):
                                skipped += 1
                                continue
                            m = _PRIMARY_RE.match(member.split("/")[-1])
                            if m is None:
                                skipped += 1
                                continue
                            cik = norm_cik(m.group(1))
                            cataloged += 1
                            in_allow = cik in allow
                            self._catalog_stub(conn, cik, member, archive_hash,
                                               now)
                            if not in_allow:
                                continue
                            try:
                                with zf.open(member) as fh:
                                    doc = json.loads(fh.read().decode(
                                        "utf-8", "replace"))
                                frows = parse_companyfacts_facts(
                                    doc, cik=cik, target_tags=tags)
                            except Exception:  # noqa: BLE001 - isolate bad member
                                malformed += 1
                                conn.execute(
                                    "UPDATE cf_member SET malformed=1,updated_at=?"
                                    " WHERE cik=?", (now, cik))
                                continue
                            added = self._materialize_conn(
                                conn, cik, member, archive_hash, doc, frows, now)
                            facts += added
                            materialized += 1
                        done = hi
                        conn.execute(
                            "UPDATE cf_archive_cursor SET members_done=?,"
                            "members_cataloged=?,ciks_materialized=?,"
                            "facts_indexed=?,skipped_members=?,malformed_members=?,"
                            "complete=?,updated_at=? WHERE archive_name=?",
                            (done, cataloged, materialized, facts, skipped,
                             malformed, 1 if done >= total else 0, now,
                             archive_name))
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                    finally:
                        conn.close()
        complete = done >= total
        return {"ok": True, "archive_name": archive_name,
                "archive_hash": archive_hash, "allowlist_size": len(allow),
                "total_members": total, "members_done": done,
                "members_cataloged": cataloged, "ciks_materialized": materialized,
                "facts_indexed": facts, "skipped_members": skipped,
                "malformed_members": malformed, "complete": complete,
                "elapsed_seconds": round(time.monotonic() - started, 2)}

    def _catalog_stub(self, conn: sqlite3.Connection, cik: str, member: str,
                      ahash: Optional[str], now: str) -> None:
        conn.execute(
            "INSERT INTO cf_member(cik,member,source_archive_hash,cataloged_at,"
            "updated_at) VALUES(?,?,?,?,?) ON CONFLICT(cik) DO UPDATE SET "
            "member=excluded.member,updated_at=excluded.updated_at",
            (cik, member, ahash, now, now))

    def _materialize_conn(self, conn: sqlite3.Connection, cik: str, member: str,
                          ahash: Optional[str], doc: dict, frows: list[dict],
                          now: str) -> int:
        summ = _entity_summary(doc, frows)
        body = {"entity_name": summ["entity_name"],
                "taxonomies": summ["taxonomies"],
                "target_facts": summ["target_facts"],
                "first_filed": summ["first_filed"],
                "last_filed": summ["last_filed"]}
        conn.execute(
            "UPDATE cf_member SET entity_name=?,taxonomies_json=?,target_facts=?,"
            "first_filed=?,last_filed=?,materialized=1,malformed=0,"
            "content_hash=?,updated_at=? WHERE cik=?",
            (summ["entity_name"], canonical_json(summ["taxonomies"]),
             summ["target_facts"], summ["first_filed"], summ["last_filed"],
             content_hash(body), now, cik))
        added = 0
        for f in frows:
            cur = conn.execute(
                "INSERT OR IGNORE INTO cf_fact(cik,taxonomy,concept_tag,label,"
                "unit,value,period_start,period_end,filed,accession,form,fy,fp,"
                "frame,source_archive_hash,source_member,parser_version,"
                "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (f["cik"], f["taxonomy"], f["concept"], f.get("label"),
                 f["unit"], _as_real(f.get("value")), f.get("period_start"),
                 f.get("period_end"), f["filed"], f["accession"], f.get("form"),
                 _s(f.get("fy")), _s(f.get("fp")), f.get("frame"), ahash, member,
                 PARSER_VERSION, now))
            added += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        return added

    def _archive_cursor(self, archive_name: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM cf_archive_cursor WHERE archive_name=?",
                (archive_name,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def archive_status(self, archive_name: str = "companyfacts") -> dict:
        cur = self._archive_cursor(archive_name)
        return cur or {"archive_name": archive_name, "members_done": 0,
                       "total_members": 0, "complete": 0, "facts_indexed": 0,
                       "ciks_materialized": 0}

    def _reset_cursor(self, archive_name: str, archive_hash: Optional[str],
                      allow_hash: str, total: int, now: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO cf_archive_cursor(archive_name,archive_hash,"
                    "allowlist_hash,total_members,members_done,members_cataloged,"
                    "ciks_materialized,facts_indexed,skipped_members,"
                    "malformed_members,complete,updated_at) VALUES(?,?,?,?,0,0,0,"
                    "0,0,0,0,?) ON CONFLICT(archive_name) DO UPDATE SET "
                    "archive_hash=?,allowlist_hash=?,total_members=?,"
                    "members_done=0,members_cataloged=0,ciks_materialized=0,"
                    "facts_indexed=0,skipped_members=0,malformed_members=0,"
                    "complete=0,updated_at=?",
                    (archive_name, archive_hash, allow_hash, total, now,
                     archive_hash, allow_hash, total, now))
            finally:
                conn.close()

    def _record_revision(self, archive_name: str, before: Optional[str],
                         after: Optional[str], kind: str, now: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO cf_revisions(kind,cik,before_json,after_json,"
                    "created_at) VALUES(?,?,?,?,?)",
                    (kind, None,
                     canonical_json({"archive": archive_name, "v": before}),
                     canonical_json({"archive": archive_name, "v": after}), now))
            finally:
                conn.close()

    # -- queries (PIT materialisation feed) --------------------------------- #
    def member(self, cik: Any) -> Optional[dict]:
        c = norm_cik(cik)
        if c is None:
            return None
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM cf_member WHERE cik=?",
                               (c,)).fetchone()
            if row is None:
                return None
            out = dict(row)
            out["taxonomies"] = json.loads(out.pop("taxonomies_json") or "[]")
            return out
        finally:
            conn.close()

    def facts_for_cik(self, cik: Any, *,
                      concepts: Optional[Iterable[str]] = None) -> list[dict]:
        """Every indexed fact row for ``cik`` (optionally restricted to a set of
        us-gaap tags), in the payload shape ``pit_fundamentals.add_fact`` consumes:
        ``{cik, concept, unit, value, period_end, fy, fp, form, filed,
        accession}``. Amendments are distinct rows."""
        c = norm_cik(cik)
        if c is None:
            return []
        conn = self._connect()
        try:
            if concepts is not None:
                cs = sorted(set(concepts))
                if not cs:
                    return []
                q = ("SELECT * FROM cf_fact WHERE cik=? AND concept_tag IN (%s) "
                     "ORDER BY concept_tag,filed,period_end,accession"
                     % ",".join("?" * len(cs)))
                rows = conn.execute(q, (c, *cs)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM cf_fact WHERE cik=? ORDER BY concept_tag,filed,"
                    "period_end,accession", (c,)).fetchall()
            return [self._fact_payload(r) for r in rows]
        finally:
            conn.close()

    def pit_payloads_for_ciks(self, ciks: Iterable[str], *,
                              concepts: Optional[Iterable[str]] = None
                              ) -> list[dict]:
        """PIT-store payloads for every fact of the given CIKs (bounded read).
        Feed to ``PitFundamentalsStore.add_records`` (each wrapped as an
        ``XBRL_FACT`` record) or ``add_fact`` directly."""
        out: list[dict] = []
        for c in {norm_cik(x) for x in ciks if norm_cik(x)}:
            out.extend(self.facts_for_cik(c, concepts=concepts))
        return out

    @staticmethod
    def _fact_payload(r: sqlite3.Row) -> dict:
        return {"cik": r["cik"], "concept": r["concept_tag"], "unit": r["unit"],
                "value": r["value"], "period_start": r["period_start"],
                "period_end": r["period_end"], "fy": r["fy"], "fp": r["fp"],
                "frame": r["frame"], "form": r["form"], "filed": r["filed"],
                "accession": r["accession"]}

    # -- counts / digest ---------------------------------------------------- #
    def counts(self) -> dict:
        conn = self._connect()
        try:
            def n(sql):
                return int(conn.execute(sql).fetchone()[0])
            return {
                "members": n("SELECT COUNT(*) FROM cf_member"),
                "members_materialized":
                    n("SELECT COUNT(*) FROM cf_member WHERE materialized=1"),
                "members_malformed":
                    n("SELECT COUNT(*) FROM cf_member WHERE malformed=1"),
                "facts": n("SELECT COUNT(*) FROM cf_fact"),
                "distinct_fact_ciks":
                    n("SELECT COUNT(DISTINCT cik) FROM cf_fact"),
                "distinct_concepts":
                    n("SELECT COUNT(DISTINCT concept_tag) FROM cf_fact"),
                "revisions": n("SELECT COUNT(*) FROM cf_revisions"),
            }
        finally:
            conn.close()

    def concept_coverage(self) -> dict:
        conn = self._connect()
        try:
            return {r["concept_tag"]: int(r["n"]) for r in conn.execute(
                "SELECT concept_tag, COUNT(DISTINCT cik) AS n FROM cf_fact "
                "GROUP BY concept_tag")}
        finally:
            conn.close()

    def revisions(self, *, limit: int = 200) -> list[dict]:
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM cf_revisions ORDER BY id DESC LIMIT ?",
                (int(limit),))]
        finally:
            conn.close()

    def index_fingerprint(self) -> str:
        """A CHEAP, stable content fingerprint (counts + archive cursor) for the
        companyfacts index version hash — avoids hashing every fact row."""
        c = self.counts()
        cur = self._archive_cursor("companyfacts") or {}
        return hashlib.sha256(canonical_json({
            "members": c["members"], "facts": c["facts"],
            "fact_ciks": c["distinct_fact_ciks"],
            "archive_hash": cur.get("archive_hash"),
            "allowlist_hash": cur.get("allowlist_hash"),
            "members_done": cur.get("members_done")}).encode()).hexdigest()

    def digest(self) -> str:
        """Deterministic content digest (fact rows + archive cursor) for restart /
        idempotency verification."""
        conn = self._connect()
        try:
            facts = [tuple(r) for r in conn.execute(
                "SELECT cik,concept_tag,unit,period_end,fy,fp,accession,value "
                "FROM cf_fact ORDER BY cik,concept_tag,unit,period_end,fy,fp,"
                "accession")]
            cur = [tuple(r) for r in conn.execute(
                "SELECT archive_name,archive_hash,allowlist_hash,members_done,"
                "complete FROM cf_archive_cursor ORDER BY archive_name")]
            return hashlib.sha256(
                canonical_json({"f": facts, "c": cur}).encode()).hexdigest()
        finally:
            conn.close()


def _s(v) -> Optional[str]:
    return None if v is None else str(v)


def _as_real(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


__all__ = ["SecCompanyFactsIndex", "parse_companyfacts_facts",
           "default_target_tags", "INDEX_SCHEMA_VERSION", "PARSER_VERSION"]
