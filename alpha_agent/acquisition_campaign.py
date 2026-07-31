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
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _norm(sym: str) -> str:
    return str(sym).strip().upper()


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
        finally:
            conn.close()

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
                        "max_symbol_attempts,created_at,updated_at) VALUES"
                        "(?,?,?,?,?,?,?,?,?)",
                        (campaign_id, kind, run_mode, universe_source,
                         universe_fingerprint, int(batch_size),
                         int(max_symbol_attempts), now, now))
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


def _dedupe_ordered(universe: Iterable[str]) -> "list[str]":
    """Deterministic, sorted, de-duplicated symbol list (stable cursor order)."""
    return sorted({_norm(s) for s in universe if str(s).strip()})
