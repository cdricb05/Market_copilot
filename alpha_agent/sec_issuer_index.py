"""
alpha_agent.sec_issuer_index — Stage 10.1 SEC ISSUER-HISTORY INDEX.

The missing reusable BULK-INGESTION + INDEXING capability that closes the
historical security->CIK bridge. Stage 10 built the survivorship-safe identity
model + the deterministic matching contract, but only the free CURRENT-only
``company_tickers.json`` fed it, so every DELISTED name had NO SEC candidate at
all and stayed UNRESOLVED. This module ingests the COMPLETE free SEC issuer
history — by STREAMING the official ``submissions.zip`` bulk archive — and builds
a durable, queryable index of every issuer's legal name, formerNames (with
effective from/to dates), tickers, exchanges, SIC, entity type, state of
incorporation and first/last filing date. That index gives every historical
Norgate security a real candidate pool for the Stage 10 Tier-3 (filing-date
overlapping ticker) and Tier-4 (legal-name / formerName + date overlap) contract.

Design invariants (all preserved here):
  * COMPLETE, not sampled — every valid primary issuer document in the archive is
    processed (bounded per step, restart-safe via a durable member cursor).
  * DURABLE / APPEND-ONLY / IDEMPOTENT / RESTART-SAFE / CONTENT-HASH VERSIONED —
    replaying the same archive adds zero duplicate issuer-history records; a
    changed archive (new bytes) records a new revision and re-streams.
  * QUERYABLE by CIK, normalized legal name, former name and ticker — the
    candidate lookup the matching contract needs, WITHOUT ever passing the whole
    ~800k-issuer index into the O(n) contract (per-security candidate pre-filter).
  * OUTSIDE operational-ledger roots — one stdlib sqlite file under the research
    root; no order / fill / signal / decision / model touch anywhere.
  * Never treats a CURRENT SEC ticker as historical unless dated filing evidence
    supports it; never fabricates a CIK; ambiguous stays ambiguous.

Pure stdlib. The HTTP transport (for the small current-ticker JSON maps) and the
zip path are injected so the whole module is unit-testable with no real network.
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

from .historical_identity import (canonical_json, content_hash, norm_cik,
                                   _norm_name as norm_name, _iso)

STAGE = "10.1"
INDEX_SCHEMA_VERSION = "sec-issuer-index-1.0.0"
PARSER_VERSION = "sec-submissions-parser-1.0.0"

# The SEC EDGAR electronic-filing floor. Older paper filings are not in EDGAR, so
# an issuer whose submissions archive references older shard files has a true
# first-filing date at or after this floor — used as a CONSERVATIVE lower bound
# for the filing-life interval (never an invented precise date).
_EDGAR_FLOOR = "1993-01-01"

# A primary per-CIK submissions member: ``CIK##########.json`` (10-digit). The
# ``CIK##########-submissions-###.json`` shards (older-filing pages) are skipped
# for the issuer-identity index — the primary member carries name / formerNames /
# tickers / exchanges / sic / the recent-filing dates.
_PRIMARY_RE = re.compile(r"^CIK(\d{10})\.json$")

# Exact, evidence-based unresolved reason codes (Part 4). Never a generic
# "data not owned" when a more specific reason is known.
REASON_NO_SEC_CANDIDATE = "NO_SEC_CANDIDATE"
REASON_MULTIPLE_CIK_CANDIDATES = "MULTIPLE_CIK_CANDIDATES"
REASON_TICKER_REUSE_CONFLICT = "TICKER_REUSE_CONFLICT"
REASON_NAME_COLLISION = "NAME_COLLISION"
REASON_DATE_INTERVAL_MISMATCH = "DATE_INTERVAL_MISMATCH"
REASON_SHARE_CLASS_AMBIGUITY = "SHARE_CLASS_AMBIGUITY"
REASON_MERGER_OR_SUCCESSOR_AMBIGUITY = "MERGER_OR_SUCCESSOR_AMBIGUITY"
REASON_MISSING_SEC_HISTORY = "MISSING_SEC_HISTORY"
REASON_MISSING_NORGATE_IDENTITY = "MISSING_NORGATE_IDENTITY"
REASON_INSUFFICIENT_CORROBORATION = "INSUFFICIENT_CORROBORATION"

UNRESOLVED_REASON_CODES = (
    REASON_NO_SEC_CANDIDATE, REASON_MULTIPLE_CIK_CANDIDATES,
    REASON_TICKER_REUSE_CONFLICT, REASON_NAME_COLLISION,
    REASON_DATE_INTERVAL_MISMATCH, REASON_SHARE_CLASS_AMBIGUITY,
    REASON_MERGER_OR_SUCCESSOR_AMBIGUITY, REASON_MISSING_SEC_HISTORY,
    REASON_MISSING_NORGATE_IDENTITY, REASON_INSUFFICIENT_CORROBORATION)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _norm_ticker(t: Any) -> str:
    return str(t or "").strip().upper()


def _is_safe_member(name: str) -> bool:
    """Reject zip path traversal / absolute / drive-escaping member names before
    they are ever touched (defence in depth even though members are read into
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


# --------------------------------------------------------------------------- #
# Submissions document parsing (issuer identity + conservative filing life).
# --------------------------------------------------------------------------- #
def parse_submissions_document(doc: dict, *, source_member: str,
                               archive_hash: Optional[str]) -> Optional[dict]:
    """Parse ONE SEC submissions primary JSON document into a durable issuer
    identity record. Deterministic; read-only. Returns None when no CIK is
    present. The filing life ``[first_filing, last_filing]`` is a CONSERVATIVE
    bound derived from the owned recent-filing dates (and widened to the EDGAR
    electronic floor when older shard files exist); a still-active filer (current
    tickers present) keeps an OPEN ``last_filing`` so a later-renamed successor's
    formerName still overlaps its historical era."""
    if not isinstance(doc, dict):
        return None
    cik = norm_cik(doc.get("cik"))
    if cik is None:
        return None
    name = str(doc.get("name")).strip() if doc.get("name") else None
    former = []
    for fn in (doc.get("formerNames") or []):
        if isinstance(fn, dict) and fn.get("name"):
            former.append({"name": str(fn["name"]).strip(),
                           "from": _iso(fn.get("from")),
                           "to": _iso(fn.get("to"))})
    tickers = sorted({_norm_ticker(t) for t in (doc.get("tickers") or []) if t})
    exchanges = [str(e) for e in (doc.get("exchanges") or []) if e]
    recent = ((doc.get("filings") or {}).get("recent") or {})
    fdates = [d for d in (recent.get("filingDate") or []) if d]
    first_recent = min(fdates)[:10] if fdates else None
    last_recent = max(fdates)[:10] if fdates else None
    has_older = bool((doc.get("filings") or {}).get("files"))
    first_filing = _EDGAR_FLOOR if has_older else first_recent
    # Still active => keep last_filing OPEN so former-name era overlap works.
    still_active = bool(tickers)
    last_filing = None if still_active else last_recent
    rec = {
        "cik": cik, "name": name, "former_names": former,
        "tickers": tickers, "exchanges": exchanges,
        "sic": (str(doc.get("sic")) if doc.get("sic") else None),
        "sic_description": doc.get("sicDescription"),
        "entity_type": doc.get("entityType"),
        "state_of_incorp": doc.get("stateOfIncorporation"),
        "first_filing": first_filing, "last_filing": last_filing,
        "has_older_filings": has_older, "still_active": still_active,
        "source_member": source_member, "source_archive_hash": archive_hash,
        "parser_version": PARSER_VERSION,
    }
    return rec


_INDEX_SCHEMA = """
CREATE TABLE IF NOT EXISTS index_meta (
    key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS issuer (
    cik TEXT PRIMARY KEY,
    name TEXT, norm_name TEXT,
    tickers_json TEXT NOT NULL DEFAULT '[]',
    exchanges_json TEXT NOT NULL DEFAULT '[]',
    sic TEXT, sic_description TEXT, entity_type TEXT, state_of_incorp TEXT,
    first_filing TEXT, last_filing TEXT,
    has_older_filings INTEGER NOT NULL DEFAULT 0,
    still_active INTEGER NOT NULL DEFAULT 0,
    source_member TEXT, source_archive_hash TEXT, parser_version TEXT,
    content_hash TEXT NOT NULL, indexed_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_issuer_norm ON issuer(norm_name);
CREATE TABLE IF NOT EXISTS former_name (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cik TEXT NOT NULL, name TEXT NOT NULL, norm_name TEXT NOT NULL,
    from_date TEXT, to_date TEXT, created_at TEXT NOT NULL,
    UNIQUE(cik, norm_name, from_date));
CREATE INDEX IF NOT EXISTS ix_former_norm ON former_name(norm_name);
CREATE TABLE IF NOT EXISTS name_lookup (
    norm_name TEXT NOT NULL, cik TEXT NOT NULL, kind TEXT NOT NULL,
    PRIMARY KEY(norm_name, cik, kind));
CREATE INDEX IF NOT EXISTS ix_namelookup_norm ON name_lookup(norm_name);
CREATE TABLE IF NOT EXISTS ticker_current (
    ticker TEXT NOT NULL, cik TEXT NOT NULL, source TEXT NOT NULL,
    exchange TEXT, PRIMARY KEY(ticker, cik, source));
CREATE INDEX IF NOT EXISTS ix_tickcur_ticker ON ticker_current(ticker);
CREATE TABLE IF NOT EXISTS ticker_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cik TEXT NOT NULL, ticker TEXT NOT NULL, observed_date TEXT,
    form TEXT, accession TEXT, source_hash TEXT, created_at TEXT NOT NULL,
    UNIQUE(cik, ticker, observed_date, form));
CREATE INDEX IF NOT EXISTS ix_tickobs_ticker ON ticker_observation(ticker);
CREATE TABLE IF NOT EXISTS archive_cursor (
    archive_name TEXT PRIMARY KEY,
    archive_hash TEXT, total_members INTEGER NOT NULL DEFAULT 0,
    members_done INTEGER NOT NULL DEFAULT 0,
    issuers_indexed INTEGER NOT NULL DEFAULT 0,
    skipped_members INTEGER NOT NULL DEFAULT 0,
    malformed_members INTEGER NOT NULL DEFAULT 0,
    complete INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS successor_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    predecessor_security_id TEXT NOT NULL, predecessor_cik TEXT,
    successor_cik TEXT, successor_security_id TEXT,
    relationship TEXT NOT NULL, effective_date TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}', confidence REAL NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'UNRESOLVED', created_at TEXT NOT NULL,
    UNIQUE(predecessor_security_id, successor_cik, relationship));
CREATE TABLE IF NOT EXISTS index_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL, cik TEXT, before_json TEXT, after_json TEXT,
    created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS processed_source (
    source_key TEXT PRIMARY KEY, kind TEXT, recorded_at TEXT NOT NULL);
"""


class SecIssuerIndex:
    """Durable, append-only, versioned SEC issuer-history index (one stdlib sqlite
    file under the research root, NEVER an operational-ledger root). Idempotent
    replay, restart-safe streaming, deterministic hashes, exact provenance. The
    ``clock`` is an injectable ``() -> iso`` for deterministic tests."""

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
                    "INSERT OR IGNORE INTO index_meta(key,value) VALUES(?,?)",
                    ("schema_version", INDEX_SCHEMA_VERSION))
            finally:
                conn.close()

    # -- meta / idempotency ------------------------------------------------- #
    def get_meta(self, key: str) -> Optional[str]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT value FROM index_meta WHERE key=?",
                               (key,)).fetchone()
            return row["value"] if row else None
        finally:
            conn.close()

    def already_processed_source(self, source_key: str) -> bool:
        conn = self._connect()
        try:
            return conn.execute(
                "SELECT 1 FROM processed_source WHERE source_key=?",
                (source_key,)).fetchone() is not None
        finally:
            conn.close()

    # -- issuer upsert (idempotent, append-only history) -------------------- #
    def _upsert_issuer_conn(self, conn: sqlite3.Connection, rec: dict,
                            now: str) -> str:
        """Upsert ONE issuer inside an open transaction. Returns 'created',
        'changed' or 'unchanged'. Append-only formerName / name_lookup /
        ticker_current rows are idempotent (UNIQUE / OR IGNORE)."""
        cik = rec["cik"]
        nm = rec.get("name")
        nnm = norm_name(nm) if nm else ""
        body = {k: rec.get(k) for k in (
            "name", "former_names", "tickers", "exchanges", "sic",
            "sic_description", "entity_type", "state_of_incorp", "first_filing",
            "last_filing", "has_older_filings", "still_active")}
        ch = content_hash(body)
        row = conn.execute("SELECT content_hash FROM issuer WHERE cik=?",
                           (cik,)).fetchone()
        outcome = "unchanged"
        if row is None:
            conn.execute(
                "INSERT INTO issuer(cik,name,norm_name,tickers_json,"
                "exchanges_json,sic,sic_description,entity_type,state_of_incorp,"
                "first_filing,last_filing,has_older_filings,still_active,"
                "source_member,source_archive_hash,parser_version,content_hash,"
                "indexed_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (cik, nm, nnm, canonical_json(rec.get("tickers") or []),
                 canonical_json(rec.get("exchanges") or []), rec.get("sic"),
                 rec.get("sic_description"), rec.get("entity_type"),
                 rec.get("state_of_incorp"), rec.get("first_filing"),
                 rec.get("last_filing"), 1 if rec.get("has_older_filings") else 0,
                 1 if rec.get("still_active") else 0, rec.get("source_member"),
                 rec.get("source_archive_hash"),
                 rec.get("parser_version", PARSER_VERSION), ch, now, now))
            outcome = "created"
            conn.execute(
                "INSERT INTO index_revisions(kind,cik,before_json,after_json,"
                "created_at) VALUES(?,?,?,?,?)",
                ("ISSUER_CREATED", cik, None, canonical_json(body), now))
        elif row["content_hash"] != ch:
            conn.execute(
                "UPDATE issuer SET name=?,norm_name=?,tickers_json=?,"
                "exchanges_json=?,sic=?,sic_description=?,entity_type=?,"
                "state_of_incorp=?,first_filing=?,last_filing=?,"
                "has_older_filings=?,still_active=?,source_member=?,"
                "source_archive_hash=?,parser_version=?,content_hash=?,updated_at=?"
                " WHERE cik=?",
                (nm, nnm, canonical_json(rec.get("tickers") or []),
                 canonical_json(rec.get("exchanges") or []), rec.get("sic"),
                 rec.get("sic_description"), rec.get("entity_type"),
                 rec.get("state_of_incorp"), rec.get("first_filing"),
                 rec.get("last_filing"), 1 if rec.get("has_older_filings") else 0,
                 1 if rec.get("still_active") else 0, rec.get("source_member"),
                 rec.get("source_archive_hash"),
                 rec.get("parser_version", PARSER_VERSION), ch, now, cik))
            outcome = "changed"
            conn.execute(
                "INSERT INTO index_revisions(kind,cik,before_json,after_json,"
                "created_at) VALUES(?,?,?,?,?)",
                ("ISSUER_REVISED", cik, None, canonical_json(body), now))
        # Append-only lookup rows (idempotent).
        if nnm:
            conn.execute("INSERT OR IGNORE INTO name_lookup(norm_name,cik,kind) "
                         "VALUES(?,?,?)", (nnm, cik, "current"))
        for t in (rec.get("tickers") or []):
            conn.execute("INSERT OR IGNORE INTO ticker_current(ticker,cik,source,"
                         "exchange) VALUES(?,?,?,?)",
                         (_norm_ticker(t), cik, "sec_submissions", None))
        for fn in (rec.get("former_names") or []):
            fnm = fn.get("name")
            if not fnm:
                continue
            fnnm = norm_name(fnm)
            conn.execute(
                "INSERT OR IGNORE INTO former_name(cik,name,norm_name,from_date,"
                "to_date,created_at) VALUES(?,?,?,?,?,?)",
                (cik, fnm, fnnm, fn.get("from"), fn.get("to"), now))
            if fnnm:
                conn.execute("INSERT OR IGNORE INTO name_lookup(norm_name,cik,"
                             "kind) VALUES(?,?,?)", (fnnm, cik, "former"))
        return outcome

    def upsert_issuer(self, rec: dict) -> str:
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                out = self._upsert_issuer_conn(conn, rec, now)
                conn.execute("COMMIT")
                return out
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    # -- streaming submissions archive indexer (restart-safe) --------------- #
    def index_submissions_archive(self, zip_path: str | Path, *,
                                  archive_hash: Optional[str] = None,
                                  archive_name: str = "submissions",
                                  member_step: int = 2000,
                                  time_budget_seconds: float = 1500.0,
                                  max_members: Optional[int] = None) -> dict:
        """Stream the SEC submissions bulk archive and index every valid PRIMARY
        issuer document, resuming from the durable member cursor. Bounded by a
        per-step member count (checkpointed) AND a wall-clock budget so ONE queue
        job makes large restart-safe progress on the one-time backfill without
        loading the archive into memory. A changed ``archive_hash`` resets the
        cursor and records an ARCHIVE_REVISION. Path-traversal / malformed members
        are isolated and counted, never fatal. Idempotent: a fully-streamed
        archive re-run does zero work; unchanged issuers are content-hash no-ops."""
        zip_path = Path(zip_path)
        if not zip_path.exists():
            return {"ok": False, "reason": "archive not present: %s" % zip_path,
                    "complete": False}
        started = time.monotonic()
        now0 = self._clock()
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            total = len(names)
            # Cursor bootstrap / archive-version reset.
            cur = self._archive_cursor(archive_name)
            if cur is None or (archive_hash and cur.get("archive_hash") and
                               cur["archive_hash"] != archive_hash):
                if cur is not None and archive_hash and \
                        cur.get("archive_hash") != archive_hash:
                    self._record_archive_revision(archive_name,
                                                  cur.get("archive_hash"),
                                                  archive_hash, now0)
                self._reset_cursor(archive_name, archive_hash, total, now0)
                cur = self._archive_cursor(archive_name)
            done = int(cur["members_done"])
            issuers = int(cur["issuers_indexed"])
            skipped = int(cur["skipped_members"])
            malformed = int(cur["malformed_members"])
            if max_members is not None:
                total = min(total, int(max_members))
            created = changed = 0
            # Process in checkpointed steps until archive end or time budget.
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
                            try:
                                with zf.open(member) as fh:
                                    doc = json.loads(fh.read().decode(
                                        "utf-8", "replace"))
                                rec = parse_submissions_document(
                                    doc, source_member=member,
                                    archive_hash=archive_hash)
                            except Exception:  # noqa: BLE001 - isolate bad member
                                malformed += 1
                                continue
                            if rec is None:
                                skipped += 1
                                continue
                            out = self._upsert_issuer_conn(conn, rec, now)
                            issuers += 1 if out == "created" else 0
                            created += 1 if out == "created" else 0
                            changed += 1 if out == "changed" else 0
                        done = hi
                        conn.execute(
                            "UPDATE archive_cursor SET members_done=?,"
                            "issuers_indexed=?,skipped_members=?,"
                            "malformed_members=?,complete=?,updated_at=? WHERE "
                            "archive_name=?",
                            (done, issuers, skipped, malformed,
                             1 if done >= total else 0, now, archive_name))
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                    finally:
                        conn.close()
        complete = done >= total
        return {"ok": True, "archive_name": archive_name,
                "archive_hash": archive_hash, "total_members": total,
                "members_done": done, "issuers_indexed": issuers,
                "created_this_call": created, "changed_this_call": changed,
                "skipped_members": skipped, "malformed_members": malformed,
                "complete": complete,
                "elapsed_seconds": round(time.monotonic() - started, 2)}

    def _archive_cursor(self, archive_name: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM archive_cursor WHERE archive_name=?",
                               (archive_name,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def archive_status(self, archive_name: str = "submissions") -> dict:
        cur = self._archive_cursor(archive_name)
        return cur or {"archive_name": archive_name, "members_done": 0,
                       "total_members": 0, "complete": 0, "issuers_indexed": 0}

    def _reset_cursor(self, archive_name: str, archive_hash: Optional[str],
                      total: int, now: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO archive_cursor(archive_name,archive_hash,"
                    "total_members,members_done,issuers_indexed,skipped_members,"
                    "malformed_members,complete,updated_at) VALUES(?,?,?,0,0,0,0,"
                    "0,?) ON CONFLICT(archive_name) DO UPDATE SET archive_hash=?,"
                    "total_members=?,members_done=0,complete=0,updated_at=?",
                    (archive_name, archive_hash, total, now, archive_hash, total,
                     now))
            finally:
                conn.close()

    def _record_archive_revision(self, archive_name: str, before: Optional[str],
                                 after: Optional[str], now: str) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO index_revisions(kind,cik,before_json,after_json,"
                    "created_at) VALUES(?,?,?,?,?)",
                    ("ARCHIVE_REVISION", None,
                     canonical_json({"archive": archive_name, "hash": before}),
                     canonical_json({"archive": archive_name, "hash": after}),
                     now))
            finally:
                conn.close()

    # -- current ticker maps (company_tickers[_exchange]) ------------------- #
    def index_company_tickers(self, doc: Any, *, kind: str = "company_tickers",
                              source_key: Optional[str] = None) -> dict:
        """Index the CURRENT SEC ticker->CIK map (``company_tickers.json`` dict
        form, or ``company_tickers_exchange.json`` {fields,data} form). Adds
        ticker_current rows (source-tagged so they are never mislabeled as
        historical) and a lightweight name_lookup/current-name issuer stub. These
        are explicitly CURRENT-listing evidence (used only for current securities
        or as a Tier-3 candidate, never as historical proof). Idempotent per
        source document content. Returns counts."""
        rows = _iter_company_tickers(doc, kind)
        sk = source_key or (kind + ":" + content_hash(rows)[:16])
        if self.already_processed_source(sk):
            return {"kind": kind, "idempotent_skip": True, "tickers": 0}
        now = self._clock()
        added = 0
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                for cik, ticker, title, exch in rows:
                    c = norm_cik(cik)
                    tk = _norm_ticker(ticker)
                    if not c or not tk:
                        continue
                    conn.execute("INSERT OR IGNORE INTO ticker_current(ticker,"
                                 "cik,source,exchange) VALUES(?,?,?,?)",
                                 (tk, c, kind, exch))
                    nnm = norm_name(title) if title else ""
                    if nnm:
                        conn.execute("INSERT OR IGNORE INTO name_lookup(norm_name,"
                                     "cik,kind) VALUES(?,?,?)", (nnm, c, "current"))
                    added += 1
                conn.execute("INSERT OR IGNORE INTO processed_source(source_key,"
                             "kind,recorded_at) VALUES(?,?,?)", (sk, kind, now))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        return {"kind": kind, "tickers_indexed": added, "source_key": sk}

    # -- owned filing-derived ticker observations --------------------------- #
    def index_filing_evidence(self, records: Iterable[dict], *,
                              source: str = "owned_normalized",
                              limit: Optional[int] = None) -> dict:
        """Index owned, DATED filing-derived ticker observations from normalized
        FILING_EVENT / INSIDER_FILING / FUNDAMENTAL_FACT records: (cik, ticker,
        filing/available date, form, accession). A current SEC ticker is NEVER
        described as historical unless a dated filing observation supports it —
        that is exactly what these rows are. Idempotent (UNIQUE per
        cik/ticker/date/form). Returns counts."""
        now = self._clock()
        added = seen = 0
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                for r in records:
                    if limit is not None and seen >= int(limit):
                        break
                    seen += 1
                    p = r.get("normalized_payload") or {}
                    cik = norm_cik(p.get("cik") or r.get("company_id"))
                    tk = _norm_ticker(r.get("ticker") or p.get("ticker"))
                    if not cik or not tk:
                        continue
                    obs = (r.get("available_at") or p.get("filed")
                           or r.get("effective_at"))
                    obs = str(obs)[:10] if obs else None
                    form = p.get("form") or r.get("event_type")
                    acc = p.get("accession") or p.get("accessionNumber")
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO ticker_observation(cik,ticker,"
                        "observed_date,form,accession,source_hash,created_at) "
                        "VALUES(?,?,?,?,?,?,?)",
                        (cik, tk, obs, str(form) if form else None,
                         str(acc) if acc else None, source, now))
                    added += cur.rowcount if cur.rowcount and cur.rowcount > 0 \
                        else 0
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        return {"source": source, "records_seen": seen,
                "observations_added": added}

    # -- queries (candidate generation) ------------------------------------- #
    def issuer(self, cik: Any) -> Optional[dict]:
        c = norm_cik(cik)
        if c is None:
            return None
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM issuer WHERE cik=?", (c,)).fetchone()
            if row is None:
                return None
            out = dict(row)
            out["tickers"] = json.loads(out.pop("tickers_json") or "[]")
            out["exchanges"] = json.loads(out.pop("exchanges_json") or "[]")
            out["former_names"] = [dict(r) for r in conn.execute(
                "SELECT name,from_date AS 'from',to_date AS 'to' FROM former_name "
                "WHERE cik=? ORDER BY id", (c,))]
            return out
        finally:
            conn.close()

    def submissions_record(self, cik: Any) -> Optional[dict]:
        """The issuer record in the exact shape the Stage 10 matching contract
        consumes: name, former_names[{name,from,to}], tickers, exchanges,
        first_filing, last_filing, sic."""
        iss = self.issuer(cik)
        if iss is None:
            return None
        return {"name": iss.get("name"),
                "former_names": [{"name": f["name"], "from": f.get("from"),
                                  "to": f.get("to")}
                                 for f in iss.get("former_names") or []],
                "tickers": iss.get("tickers") or [],
                "exchanges": iss.get("exchanges") or [],
                "first_filing": iss.get("first_filing"),
                "last_filing": iss.get("last_filing"),
                "sic": iss.get("sic"),
                "sic_description": iss.get("sic_description")}

    def candidates_by_name(self, nnm: str, *, limit: int = 500) -> "set[str]":
        if not nnm:
            return set()
        conn = self._connect()
        try:
            return {r["cik"] for r in conn.execute(
                "SELECT DISTINCT cik FROM name_lookup WHERE norm_name=? LIMIT ?",
                (nnm, int(limit)))}
        finally:
            conn.close()

    def candidates_by_ticker(self, ticker: str, *, limit: int = 500) -> "set[str]":
        tk = _norm_ticker(ticker)
        if not tk:
            return set()
        conn = self._connect()
        try:
            cur = {r["cik"] for r in conn.execute(
                "SELECT DISTINCT cik FROM ticker_current WHERE ticker=? LIMIT ?",
                (tk, int(limit)))}
            obs = {r["cik"] for r in conn.execute(
                "SELECT DISTINCT cik FROM ticker_observation WHERE ticker=? "
                "LIMIT ?", (tk, int(limit)))}
            return cur | obs
        finally:
            conn.close()

    def candidate_evidence_for(self, security: dict, *,
                               max_candidates: int = 300) -> tuple:
        """Build the SMALL per-security evidence dicts the Stage 10 contract needs:
        (ticker_cik_index, submissions_by_cik, meta). Candidate CIKs come from the
        indexed ticker + normalized legal-name / formerName lookups — so the O(n)
        contract sees only the handful of real candidates, never the whole index.
        ``meta`` carries the candidate pools + an overflow flag for exact reason
        coding. Nothing is decided here — the contract still resolves."""
        ticker = _norm_ticker(security.get("ticker"))
        names = [security.get("issuer_name")] + [
            h.get("name") for h in (security.get("name_history") or [])]
        names = [n for n in names if n]
        tcands = self.candidates_by_ticker(ticker) if ticker else set()
        ncands: set = set()
        for n in names:
            ncands |= self.candidates_by_name(norm_name(n))
        overflow = False
        cand = set(tcands) | set(ncands)
        if len(cand) > max_candidates:
            overflow = True
            cand = set(sorted(cand)[:max_candidates])
        sub_by_cik = {}
        for c in cand:
            rec = self.submissions_record(c)
            if rec is not None:
                sub_by_cik[c] = rec
        tk_index = {ticker: sorted(tcands)} if (ticker and tcands) else None
        meta = {"ticker_candidates": sorted(tcands),
                "name_candidates": sorted(ncands), "overflow": overflow}
        return tk_index, sub_by_cik, meta

    # -- successor evidence (preserve; never auto-collapse) ----------------- #
    def record_successor_evidence(self, *, predecessor_security_id: str,
                                  predecessor_cik: Optional[str],
                                  successor_cik: Optional[str],
                                  successor_security_id: Optional[str],
                                  relationship: str,
                                  effective_date: Optional[str],
                                  evidence: dict, confidence: float) -> bool:
        """Persist a predecessor/successor RELATIONSHIP as supporting evidence
        ONLY. It never maps a security or rewrites a predecessor's historical
        identity (status stays UNRESOLVED). Idempotent. Returns True if inserted."""
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO successor_evidence(predecessor_"
                    "security_id,predecessor_cik,successor_cik,successor_security"
                    "_id,relationship,effective_date,evidence_json,confidence,"
                    "status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (predecessor_security_id, norm_cik(predecessor_cik),
                     norm_cik(successor_cik), successor_security_id, relationship,
                     effective_date, canonical_json(evidence), float(confidence),
                     "UNRESOLVED", now))
                return bool(cur.rowcount and cur.rowcount > 0)
            finally:
                conn.close()

    def successor_evidence(self, *, limit: int = 500) -> "list[dict]":
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM successor_evidence ORDER BY id DESC LIMIT ?",
                (int(limit),))]
        finally:
            conn.close()

    # -- counts / digest ---------------------------------------------------- #
    def counts(self) -> dict:
        conn = self._connect()
        try:
            def n(sql):
                return int(conn.execute(sql).fetchone()[0])
            return {
                "issuers": n("SELECT COUNT(*) FROM issuer"),
                "issuers_with_former_names":
                    n("SELECT COUNT(DISTINCT cik) FROM former_name"),
                "former_name_rows": n("SELECT COUNT(*) FROM former_name"),
                "name_lookup_rows": n("SELECT COUNT(*) FROM name_lookup"),
                "ticker_current_rows": n("SELECT COUNT(*) FROM ticker_current"),
                "ticker_observation_rows":
                    n("SELECT COUNT(*) FROM ticker_observation"),
                "successor_evidence_rows":
                    n("SELECT COUNT(*) FROM successor_evidence"),
                "revisions": n("SELECT COUNT(*) FROM index_revisions"),
            }
        finally:
            conn.close()

    def revisions(self, *, limit: int = 500) -> "list[dict]":
        conn = self._connect()
        try:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM index_revisions ORDER BY id DESC LIMIT ?",
                (int(limit),))]
        finally:
            conn.close()

    def index_fingerprint(self) -> str:
        """A CHEAP, stable content fingerprint (counts + archive cursor) for the
        mapping-version hash — avoids hashing every issuer row on ~1M-issuer
        indexes. Changes whenever the index genuinely grows."""
        c = self.counts()
        cur = self._archive_cursor("submissions") or {}
        return hashlib.sha256(canonical_json({
            "issuers": c["issuers"], "former": c["former_name_rows"],
            "tickers": c["ticker_current_rows"],
            "observations": c["ticker_observation_rows"],
            "archive_hash": cur.get("archive_hash"),
            "members_done": cur.get("members_done")}).encode()).hexdigest()

    def digest(self) -> str:
        """Deterministic content digest of the issuer index (issuer content hashes
        + archive cursor state) for restart / idempotency verification."""
        conn = self._connect()
        try:
            iss = [tuple(r) for r in conn.execute(
                "SELECT cik,content_hash FROM issuer ORDER BY cik")]
            cur = [tuple(r) for r in conn.execute(
                "SELECT archive_name,archive_hash,members_done,complete FROM "
                "archive_cursor ORDER BY archive_name")]
            return hashlib.sha256(
                canonical_json({"i": iss, "c": cur}).encode()).hexdigest()
        finally:
            conn.close()


def _iter_company_tickers(doc: Any, kind: str):
    """Yield (cik, ticker, title, exchange) tuples from either
    ``company_tickers.json`` ({idx: {cik_str,ticker,title}}) or
    ``company_tickers_exchange.json`` ({fields:[...], data:[[...],...]})."""
    if isinstance(doc, dict) and "data" in doc and "fields" in doc:
        fields = [str(f).lower() for f in (doc.get("fields") or [])]
        try:
            ci = fields.index("cik")
            ti = fields.index("ticker")
        except ValueError:
            return
        ni = fields.index("name") if "name" in fields else None
        ei = fields.index("exchange") if "exchange" in fields else None
        for row in (doc.get("data") or []):
            try:
                yield (row[ci], row[ti],
                       row[ni] if ni is not None else None,
                       row[ei] if ei is not None else None)
            except (IndexError, TypeError):
                continue
    elif isinstance(doc, dict):
        for _, v in doc.items():
            if isinstance(v, dict):
                yield (v.get("cik_str") or v.get("cik"), v.get("ticker"),
                       v.get("title") or v.get("name"), v.get("exchange"))


__all__ = [
    "SecIssuerIndex", "parse_submissions_document", "INDEX_SCHEMA_VERSION",
    "PARSER_VERSION", "UNRESOLVED_REASON_CODES",
    "REASON_NO_SEC_CANDIDATE", "REASON_MULTIPLE_CIK_CANDIDATES",
    "REASON_TICKER_REUSE_CONFLICT", "REASON_NAME_COLLISION",
    "REASON_DATE_INTERVAL_MISMATCH", "REASON_SHARE_CLASS_AMBIGUITY",
    "REASON_MERGER_OR_SUCCESSOR_AMBIGUITY", "REASON_MISSING_SEC_HISTORY",
    "REASON_MISSING_NORGATE_IDENTITY", "REASON_INSUFFICIENT_CORROBORATION",
]
