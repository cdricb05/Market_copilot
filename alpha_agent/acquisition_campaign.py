"""
alpha_agent.acquisition_campaign — Stage 8 durable, resumable, sharded
FULL-UNIVERSE acquisition campaigns (WS3/WS4/WS5/WS7).

A source collector that only proves a six-symbol acceptance batch is a proven
PARSER, not exhausted DATA. This module turns each real collector into a
campaign that enumerates the COMPLETE eligible universe, shards it into bounded
per-job batches, persists a durable cursor, retries failed symbols, identifies
permanent symbol-specific failures, reconciles complete/remaining coverage, and
keeps going until every accessible target is complete or has an exact genuine
blocker. A per-job batch size is permitted; a permanent total-universe cap is
not.

Persistence is one stdlib sqlite database under the Stage 8 research-state root
on ``D:`` (``<stage8>/campaigns.sqlite``). NO PostgreSQL, NO operational-ledger
write, NO network here — the campaign store is pure durable bookkeeping; the
runtime injects the real collector that acquires the batch.

Universe growth is absorbed, never destructive: re-resolving a larger universe
(e.g. a new S&P 500 constituent) APPENDS new PENDING symbols after the current
cursor and never drops, resets or re-does an already COMPLETED symbol.
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

STAGE = "8"
SCHEMA_VERSION = "1.0.0"

# Per-symbol acquisition states.
S_PENDING = "PENDING"
S_COMPLETED = "COMPLETED"
S_FAILED = "FAILED"          # retryable while attempts < max_attempts
SYMBOL_STATES = (S_PENDING, S_COMPLETED, S_FAILED)

_DEFAULT_MAX_SYMBOL_ATTEMPTS = 4

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS campaign (
    campaign_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    run_mode TEXT NOT NULL DEFAULT 'production',
    universe_source TEXT,
    universe_fingerprint TEXT,
    batch_size INTEGER NOT NULL DEFAULT 25,
    max_symbol_attempts INTEGER NOT NULL DEFAULT 4,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS campaign_symbol (
    campaign_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    position INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, symbol)
);
CREATE INDEX IF NOT EXISTS ix_campsym_status
    ON campaign_symbol (campaign_id, status, position);
CREATE TABLE IF NOT EXISTS campaign_batch (
    batch_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL,
    origin TEXT,
    run_date TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    stop_reason TEXT,
    ciks_attempted INTEGER NOT NULL DEFAULT 0,
    ciks_completed INTEGER NOT NULL DEFAULT 0,
    records_added INTEGER NOT NULL DEFAULT 0,
    records_deduped INTEGER NOT NULL DEFAULT 0,
    issuers_covered INTEGER,
    form4_filings INTEGER,
    open_market_purchases INTEGER,
    sales INTEGER,
    eightk_2_02_events INTEGER,
    remaining_backlog INTEGER,
    detail_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_campbatch_date
    ON campaign_batch (campaign_id, run_date);
CREATE INDEX IF NOT EXISTS ix_campbatch_seq
    ON campaign_batch (campaign_id, batch_seq);
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _norm(sym: str) -> str:
    return str(sym).strip().upper()


def _int_or_none(v) -> Optional[int]:
    try:
        return None if v is None else int(v)
    except (TypeError, ValueError):
        return None


class CampaignStore:
    """Durable per-campaign symbol cursor. ``clock`` is an injectable
    ``() -> iso`` for deterministic tests."""

    def __init__(self, db_path: str | Path, *,
                 clock: Optional[Callable[[], str]] = None):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or _utc_now_iso
        self._lock = threading.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA_SQL)
            self._migrate_revision_columns(conn)
        finally:
            conn.close()

    # -- append-only revision / supersession migration (Stage 9.5 Blocker 6) --- #
    # Scope changes (e.g. survivorship -> current universe) are handled by an
    # AUDITED, APPEND-ONLY supersession: the prior campaign is flagged SUPERSEDED
    # (its symbol + batch history is never deleted and stays queryable) and a new
    # linked revision becomes ACTIVE. These columns are added idempotently so an
    # existing live campaign DB is migrated in place without rewriting any row.
    _REVISION_COLUMNS = (
        ("base_campaign_id", "TEXT"),
        ("revision", "INTEGER NOT NULL DEFAULT 1"),
        ("status", "TEXT NOT NULL DEFAULT 'ACTIVE'"),
        ("superseded_by", "TEXT"),
        ("supersedes", "TEXT"),
        ("superseded_at", "TEXT"),
    )

    def _migrate_revision_columns(self, conn: sqlite3.Connection) -> None:
        have = {r["name"] for r in conn.execute("PRAGMA table_info(campaign)")}
        for name, decl in self._REVISION_COLUMNS:
            if name not in have:
                conn.execute("ALTER TABLE campaign ADD COLUMN %s %s"
                             % (name, decl))
        # Backfill: a pre-existing campaign is its own revision-1 base (never a
        # row rewrite of symbol/batch state - only the new metadata columns).
        conn.execute("UPDATE campaign SET base_campaign_id=campaign_id "
                     "WHERE base_campaign_id IS NULL")

    # -- campaign lifecycle ------------------------------------------------- #
    def ensure_campaign(self, campaign_id: str, *, kind: str,
                        universe: Iterable[str], universe_source: str,
                        batch_size: int = 25, run_mode: str = "production",
                        universe_fingerprint: Optional[str] = None,
                        max_symbol_attempts: int = _DEFAULT_MAX_SYMBOL_ATTEMPTS
                        ) -> dict:
        """Create the campaign if new, else RECONCILE its universe: append any
        newly-eligible symbols as PENDING after the current cursor, never
        dropping / resetting / re-doing a COMPLETED symbol. Idempotent. Returns
        ``{created, added_symbols, total_symbols}``."""
        now = self._clock()
        ordered = _dedupe_ordered(universe)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute("SELECT campaign_id FROM campaign WHERE"
                                   " campaign_id=?", (campaign_id,)).fetchone()
                created = row is None
                if created:
                    conn.execute(
                        "INSERT INTO campaign(campaign_id,kind,run_mode,"
                        "universe_source,universe_fingerprint,batch_size,"
                        "max_symbol_attempts,created_at,updated_at,"
                        "base_campaign_id,revision,status) VALUES"
                        "(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (campaign_id, kind, run_mode, universe_source,
                         universe_fingerprint, int(batch_size),
                         int(max_symbol_attempts), now, now,
                         campaign_id, 1, "ACTIVE"))
                else:
                    conn.execute(
                        "UPDATE campaign SET run_mode=?, universe_source=?,"
                        " universe_fingerprint=?, batch_size=?,"
                        " max_symbol_attempts=?, updated_at=? WHERE campaign_id=?",
                        (run_mode, universe_source, universe_fingerprint,
                         int(batch_size), int(max_symbol_attempts), now,
                         campaign_id))
                existing = {r["symbol"] for r in conn.execute(
                    "SELECT symbol FROM campaign_symbol WHERE campaign_id=?",
                    (campaign_id,))}
                maxpos = conn.execute(
                    "SELECT COALESCE(MAX(position),-1) m FROM campaign_symbol"
                    " WHERE campaign_id=?", (campaign_id,)).fetchone()["m"]
                pos = int(maxpos) + 1
                added = 0
                for sym in ordered:
                    if sym in existing:
                        continue
                    conn.execute(
                        "INSERT INTO campaign_symbol(campaign_id,symbol,position,"
                        "status,attempts,updated_at) VALUES(?,?,?,?,0,?)",
                        (campaign_id, sym, pos, S_PENDING, now))
                    pos += 1
                    added += 1
                total = conn.execute(
                    "SELECT COUNT(*) c FROM campaign_symbol WHERE campaign_id=?",
                    (campaign_id,)).fetchone()["c"]
                conn.execute("COMMIT")
                return {"created": created, "added_symbols": added,
                        "total_symbols": int(total)}
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    def _meta(self, conn: sqlite3.Connection, campaign_id: str
              ) -> Optional[sqlite3.Row]:
        return conn.execute("SELECT * FROM campaign WHERE campaign_id=?",
                            (campaign_id,)).fetchone()

    # -- batch selection (read-only) --------------------------------------- #
    def next_batch(self, campaign_id: str, *,
                   batch_size: Optional[int] = None) -> "list[str]":
        """The next claimable batch: PENDING symbols first (forward progress, by
        position), then retryable FAILED symbols (repair, attempts < max). Never
        includes COMPLETED or permanently-failed symbols. Read-only — the caller
        records the outcome with ``record_results``."""
        conn = self._connect()
        try:
            meta = self._meta(conn, campaign_id)
            if meta is None:
                return []
            size = int(batch_size if batch_size is not None
                       else meta["batch_size"])
            maxa = int(meta["max_symbol_attempts"])
            rows = conn.execute(
                "SELECT symbol FROM campaign_symbol WHERE campaign_id=? AND ("
                " status=? OR (status=? AND attempts<?)) "
                "ORDER BY CASE status WHEN ? THEN 0 ELSE 1 END, position ASC"
                " LIMIT ?",
                (campaign_id, S_PENDING, S_FAILED, maxa, S_PENDING, size)
            ).fetchall()
            return [r["symbol"] for r in rows]
        finally:
            conn.close()

    # -- outcome recording -------------------------------------------------- #
    def record_results(self, campaign_id: str, *,
                       succeeded: Optional[Iterable[str]] = None,
                       failed: Optional[Iterable] = None,
                       skipped: Optional[Iterable[str]] = None) -> dict:
        """Record a batch outcome durably. ``succeeded`` and ``skipped``
        (idempotent same-day no-op) → COMPLETED; ``failed`` (iterable of ``sym``
        or ``(sym, error)``) → FAILED with ``attempts+1``. Returns fresh
        coverage."""
        now = self._clock()
        succ = [_norm(s) for s in (succeeded or [])]
        skip = [_norm(s) for s in (skipped or [])]
        fails = []
        for item in (failed or []):
            if isinstance(item, (list, tuple)):
                fails.append((_norm(item[0]),
                              str(item[1])[:400] if len(item) > 1 else None))
            else:
                fails.append((_norm(item), None))
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                for sym in succ + skip:
                    conn.execute(
                        "UPDATE campaign_symbol SET status=?, last_error=NULL,"
                        " completed_at=?, updated_at=? WHERE campaign_id=? AND"
                        " symbol=?",
                        (S_COMPLETED, now, now, campaign_id, sym))
                for sym, err in fails:
                    conn.execute(
                        "UPDATE campaign_symbol SET status=?, attempts=attempts+1,"
                        " last_error=?, updated_at=? WHERE campaign_id=? AND"
                        " symbol=? AND status!=?",
                        (S_FAILED, err, now, campaign_id, sym, S_COMPLETED))
                conn.execute("UPDATE campaign SET updated_at=? WHERE"
                             " campaign_id=?", (now, campaign_id))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        return self.coverage(campaign_id)

    # -- coverage reconciliation ------------------------------------------- #
    def coverage(self, campaign_id: str) -> dict:
        """Fully reconciled campaign coverage — the WS3/WS8 counters that must
        add up: target = completed + pending + failed_retryable + permanent."""
        conn = self._connect()
        try:
            meta = self._meta(conn, campaign_id)
            if meta is None:
                return {"exists": False, "campaign_id": campaign_id}
            maxa = int(meta["max_symbol_attempts"])
            total = conn.execute(
                "SELECT COUNT(*) c FROM campaign_symbol WHERE campaign_id=?",
                (campaign_id,)).fetchone()["c"]
            completed = conn.execute(
                "SELECT COUNT(*) c FROM campaign_symbol WHERE campaign_id=? AND"
                " status=?", (campaign_id, S_COMPLETED)).fetchone()["c"]
            pending = conn.execute(
                "SELECT COUNT(*) c FROM campaign_symbol WHERE campaign_id=? AND"
                " status=?", (campaign_id, S_PENDING)).fetchone()["c"]
            fail_retry = conn.execute(
                "SELECT COUNT(*) c FROM campaign_symbol WHERE campaign_id=? AND"
                " status=? AND attempts<?",
                (campaign_id, S_FAILED, maxa)).fetchone()["c"]
            fail_perm = conn.execute(
                "SELECT COUNT(*) c FROM campaign_symbol WHERE campaign_id=? AND"
                " status=? AND attempts>=?",
                (campaign_id, S_FAILED, maxa)).fetchone()["c"]
            remaining = int(pending) + int(fail_retry)
            return {
                "exists": True, "campaign_id": campaign_id,
                "kind": meta["kind"], "run_mode": meta["run_mode"],
                "universe_source": meta["universe_source"],
                "universe_fingerprint": meta["universe_fingerprint"],
                "base_campaign_id": (meta["base_campaign_id"]
                                     if meta["base_campaign_id"]
                                     else campaign_id),
                "revision": _int_or_none(meta["revision"]) or 1,
                "status": meta["status"] or "ACTIVE",
                "superseded_by": meta["superseded_by"],
                "per_job_symbol_batch_size": int(meta["batch_size"]),
                "full_universe_target_count": int(total),
                "completed_symbol_count": int(completed),
                "pending_symbol_count": int(pending),
                "repair_backlog_count": int(fail_retry),
                "permanent_failed_count": int(fail_perm),
                "remaining_symbol_count": remaining,
                # Monotone progress cursor: symbols resolved so far.
                "acquisition_cursor": int(completed),
                "is_complete": remaining == 0,
                "reconciles": (int(completed) + int(pending) + int(fail_retry)
                               + int(fail_perm)) == int(total),
            }
        finally:
            conn.close()

    def is_complete(self, campaign_id: str) -> bool:
        cov = self.coverage(campaign_id)
        return bool(cov.get("exists") and cov.get("is_complete"))

    def repair_backlog(self, campaign_id: str, *, limit: int = 500
                       ) -> "list[dict]":
        conn = self._connect()
        try:
            meta = self._meta(conn, campaign_id)
            if meta is None:
                return []
            maxa = int(meta["max_symbol_attempts"])
            rows = conn.execute(
                "SELECT symbol,attempts,last_error FROM campaign_symbol WHERE"
                " campaign_id=? AND status=? AND attempts<? ORDER BY position"
                " LIMIT ?", (campaign_id, S_FAILED, maxa, int(limit))).fetchall()
            return [{"symbol": r["symbol"], "attempts": int(r["attempts"]),
                     "last_error": r["last_error"]} for r in rows]
        finally:
            conn.close()

    def permanent_failures(self, campaign_id: str, *, limit: int = 500
                           ) -> "list[dict]":
        conn = self._connect()
        try:
            meta = self._meta(conn, campaign_id)
            if meta is None:
                return []
            maxa = int(meta["max_symbol_attempts"])
            rows = conn.execute(
                "SELECT symbol,attempts,last_error FROM campaign_symbol WHERE"
                " campaign_id=? AND status=? AND attempts>=? ORDER BY position"
                " LIMIT ?", (campaign_id, S_FAILED, maxa, int(limit))).fetchall()
            return [{"symbol": r["symbol"], "attempts": int(r["attempts"]),
                     "last_error": r["last_error"]} for r in rows]
        finally:
            conn.close()

    def list_campaigns(self) -> "list[dict]":
        conn = self._connect()
        try:
            return [{"campaign_id": r["campaign_id"], "kind": r["kind"],
                     "run_mode": r["run_mode"],
                     "universe_source": r["universe_source"]}
                    for r in conn.execute(
                        "SELECT * FROM campaign ORDER BY campaign_id")]
        finally:
            conn.close()

    # -- append-only revision / supersession (Stage 9.5 Blocker 6) ----------- #
    def active_revision(self, base_id: str) -> Optional[str]:
        """The campaign_id of the ACTIVE revision for ``base_id`` (deterministic:
        the highest-revision row still flagged ACTIVE). Restart-safe - selection
        is a pure query over durable rows. Falls back to a legacy same-id campaign
        when the revision columns were never populated."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT campaign_id FROM campaign WHERE base_campaign_id=? AND"
                " status='ACTIVE' ORDER BY revision DESC LIMIT 1",
                (base_id,)).fetchone()
            if row:
                return row["campaign_id"]
            row = conn.execute("SELECT campaign_id FROM campaign WHERE"
                               " campaign_id=?", (base_id,)).fetchone()
            return row["campaign_id"] if row else None
        finally:
            conn.close()

    def campaign_revisions(self, base_id: str) -> "list[dict]":
        """The full APPEND-ONLY revision history for ``base_id`` (active +
        superseded), oldest revision first. Every prior revision remains queryable
        - supersession never deletes symbol or batch rows."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT campaign_id, revision, status, universe_source,"
                " universe_fingerprint, superseded_by, supersedes, superseded_at"
                " FROM campaign WHERE base_campaign_id=? OR campaign_id=?"
                " ORDER BY revision ASC, campaign_id ASC",
                (base_id, base_id)).fetchall()
            seen = set()
            out = []
            for r in rows:
                if r["campaign_id"] in seen:
                    continue
                seen.add(r["campaign_id"])
                out.append({"campaign_id": r["campaign_id"],
                            "revision": _int_or_none(r["revision"]) or 1,
                            "status": r["status"] or "ACTIVE",
                            "universe_source": r["universe_source"],
                            "universe_fingerprint": r["universe_fingerprint"],
                            "superseded_by": r["superseded_by"],
                            "supersedes": r["supersedes"],
                            "superseded_at": r["superseded_at"]})
            return out
        finally:
            conn.close()

    def ensure_campaign_revision(self, base_id: str, *, kind: str,
                                 universe: Iterable[str], universe_source: str,
                                 batch_size: int = 25,
                                 run_mode: str = "production",
                                 universe_fingerprint: Optional[str] = None,
                                 max_symbol_attempts:
                                 int = _DEFAULT_MAX_SYMBOL_ATTEMPTS) -> dict:
        """Scope-aware, APPEND-ONLY campaign resolution (Blocker 6).

        * No campaign yet -> create revision 1 (campaign_id == base_id).
        * Active revision with the SAME universe fingerprint (or an unknown/None
          fingerprint) -> reconcile append-only on the active revision (the normal
          growth path; never resets a COMPLETED symbol).
        * Active revision with a DIFFERENT fingerprint (a genuine scope change) ->
          SUPERSEDE it (flag it SUPERSEDED, never delete its rows) and create a
          new linked revision as the ACTIVE campaign over the new universe.

        Returns ``{campaign_id (active), base_campaign_id, revision, superseded,
        created, reconciled}``. This REPLACES manual deletion of mis-seeded
        campaign rows: prior state is always preserved and queryable."""
        active = self.active_revision(base_id)
        if active is None:
            res = self.ensure_campaign(
                base_id, kind=kind, universe=universe,
                universe_source=universe_source, batch_size=batch_size,
                run_mode=run_mode, universe_fingerprint=universe_fingerprint,
                max_symbol_attempts=max_symbol_attempts)
            return {"campaign_id": base_id, "base_campaign_id": base_id,
                    "revision": 1, "superseded": None, "created": True,
                    "reconciled": False, "added_symbols": res["added_symbols"]}
        conn = self._connect()
        try:
            meta = self._meta(conn, active)
        finally:
            conn.close()
        cur_fp = meta["universe_fingerprint"] if meta is not None else None
        cur_rev = (_int_or_none(meta["revision"]) or 1) if meta is not None else 1
        if universe_fingerprint is None or cur_fp is None \
                or universe_fingerprint == cur_fp:
            res = self.ensure_campaign(
                active, kind=kind, universe=universe,
                universe_source=universe_source, batch_size=batch_size,
                run_mode=run_mode, universe_fingerprint=universe_fingerprint,
                max_symbol_attempts=max_symbol_attempts)
            return {"campaign_id": active, "base_campaign_id": base_id,
                    "revision": cur_rev, "superseded": None, "created": False,
                    "reconciled": True, "added_symbols": res["added_symbols"]}
        # Genuine scope change -> append-only supersession.
        new_rev = cur_rev + 1
        new_id = "%s#r%d" % (base_id, new_rev)
        self.ensure_campaign(
            new_id, kind=kind, universe=universe,
            universe_source=universe_source, batch_size=batch_size,
            run_mode=run_mode, universe_fingerprint=universe_fingerprint,
            max_symbol_attempts=max_symbol_attempts)
        now = self._clock()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE campaign SET status='SUPERSEDED', superseded_by=?,"
                    " superseded_at=?, updated_at=? WHERE campaign_id=?",
                    (new_id, now, now, active))
                conn.execute(
                    "UPDATE campaign SET base_campaign_id=?, revision=?,"
                    " status='ACTIVE', supersedes=?, updated_at=? WHERE"
                    " campaign_id=?", (base_id, new_rev, active, now, new_id))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        return {"campaign_id": new_id, "base_campaign_id": base_id,
                "revision": new_rev, "superseded": active, "created": True,
                "reconciled": False}

    # -- append-only batch history (Stage 9.3) ----------------------------- #
    # The batch log is the durable, restart-safe source of truth for the
    # controlled-continuation caps: the per-calendar-day completed-batch count
    # and the trailing consecutive-no-progress run are DERIVED from it, so there
    # is no mutable counter to corrupt on a crash or a duplicate run. One row is
    # appended per fully-executed batch (a batch aborted mid-flight by the
    # cooperative deadline records NO row - "partial batch not recorded").
    _BATCH_COLS = (
        "campaign_id", "origin", "run_date", "started_at", "finished_at",
        "progress", "stop_reason", "ciks_attempted", "ciks_completed",
        "records_added", "records_deduped", "issuers_covered", "form4_filings",
        "open_market_purchases", "sales", "eightk_2_02_events",
        "remaining_backlog", "detail_json")

    def record_batch(self, campaign_id: str, *, run_date: str,
                     progress: bool, origin: Optional[str] = None,
                     stop_reason: Optional[str] = None,
                     started_at: Optional[str] = None,
                     metrics: Optional[dict] = None) -> dict:
        """Append ONE immutable batch-history row. ``run_date`` is the UTC
        calendar day the batch executed (the daily-cap key); ``progress`` is True
        iff the batch advanced the cursor or added records. ``metrics`` supplies
        the per-batch acquisition counts. Returns the persisted row as a dict."""
        import json as _json
        now = self._clock()
        m = dict(metrics or {})
        detail = m.pop("detail", None)
        row = {
            "campaign_id": campaign_id, "origin": origin,
            "run_date": str(run_date), "started_at": started_at or now,
            "finished_at": now, "progress": 1 if progress else 0,
            "stop_reason": stop_reason,
            "ciks_attempted": int(m.get("ciks_attempted", 0) or 0),
            "ciks_completed": int(m.get("ciks_completed", 0) or 0),
            "records_added": int(m.get("records_added", 0) or 0),
            "records_deduped": int(m.get("records_deduped", 0) or 0),
            "issuers_covered": _int_or_none(m.get("issuers_covered")),
            "form4_filings": _int_or_none(m.get("form4_filings")),
            "open_market_purchases": _int_or_none(m.get("open_market_purchases")),
            "sales": _int_or_none(m.get("sales")),
            "eightk_2_02_events": _int_or_none(m.get("eightk_2_02_events")),
            "remaining_backlog": _int_or_none(m.get("remaining_backlog")),
            "detail_json": _json.dumps(detail, sort_keys=True) if detail
            else None}
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                cols = ",".join(self._BATCH_COLS)
                conn.execute(
                    "INSERT INTO campaign_batch(%s) VALUES(%s)"
                    % (cols, ",".join("?" * len(self._BATCH_COLS))),
                    tuple(row[c] for c in self._BATCH_COLS))
                seq = conn.execute("SELECT last_insert_rowid() s").fetchone()["s"]
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()
        row["batch_seq"] = int(seq)
        return row

    def batches_on_date(self, campaign_id: str, run_date: str) -> int:
        """Count of fully-executed batches recorded for ``run_date`` (the daily
        cap counter). Every appended batch row counts - a deadline-aborted batch
        never records a row, so it is correctly excluded."""
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(*) c FROM campaign_batch WHERE campaign_id=? AND"
                " run_date=?", (campaign_id, str(run_date))).fetchone()["c"])
        finally:
            conn.close()

    def consecutive_no_progress(self, campaign_id: str) -> int:
        """Length of the trailing run of no-progress batches (most recent first).
        A single progress batch anywhere resets it to 0."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT progress FROM campaign_batch WHERE campaign_id=?"
                " ORDER BY batch_seq DESC", (campaign_id,)).fetchall()
        finally:
            conn.close()
        n = 0
        for r in rows:
            if int(r["progress"]) == 0:
                n += 1
            else:
                break
        return n

    def batch_count(self, campaign_id: str) -> int:
        conn = self._connect()
        try:
            return int(conn.execute(
                "SELECT COUNT(*) c FROM campaign_batch WHERE campaign_id=?",
                (campaign_id,)).fetchone()["c"])
        finally:
            conn.close()

    def batch_history(self, campaign_id: str, *, limit: int = 50) -> "list[dict]":
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM campaign_batch WHERE campaign_id=? ORDER BY"
                " batch_seq DESC LIMIT ?", (campaign_id, int(limit))).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def last_batch(self, campaign_id: str) -> Optional[dict]:
        h = self.batch_history(campaign_id, limit=1)
        return h[0] if h else None


def _dedupe_ordered(universe: Iterable[str]) -> "list[str]":
    """Deterministic, sorted, de-duplicated symbol list (stable cursor order)."""
    return sorted({_norm(s) for s in universe if str(s).strip()})
