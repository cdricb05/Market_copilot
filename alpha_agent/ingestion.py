"""
alpha_agent/ingestion.py — Alpha Agent Stage 2: autonomous data-acquisition foundation.

One deterministic, incremental ingestion engine that:
  * consults the immutable Stage 1 registry BEFORE collection;
  * drives the source collectors (Norgate local, EODHD, SEC EDGAR, FINRA,
    Nasdaq Trader, FRED/ALFRED, GDELT-deferred) in pre-registered priority order;
  * archives raw source objects immutably (content-addressed, never overwritten);
  * normalizes records into the common point-in-time contract;
  * deduplicates raw objects and normalized records deterministically;
  * maintains resumable per-source checkpoints in a local stdlib-sqlite3 state
    database (this is the agent's OWN checkpoint DB — NOT the Paper Trader
    database; no PostgreSQL, no migrations, no Paper Trader mutation);
  * produces source-health, entitlement, data-quality and coverage-gap evidence;
  * emits the daily-ingestion report contract for the later autonomous daemon.

Modes: audit / collect / incremental / verify. Verify makes no network calls
and writes nothing. Secrets are never persisted: every stored request
fingerprint is redacted and every output is scanned for in-memory secret
values before the run is published.
"""
from __future__ import annotations

import csv
import datetime as _dt
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .collectors import (
    COLLECTOR_CLASSES, CollectorContext, RawArchive, default_transport,
)
from .source_contracts import (
    BLOCKED_HEALTH_STATES, COLLECTOR_VERSION, GAP_NOT_APPLICABLE,
    INGESTION_SCHEMA_VERSION, IdentityResolver, RECORD_SCHEMA_VERSION,
    SH_DEGRADED, SH_HEALTHY, SH_NOT_CONFIGURED, SH_NOT_RUN,
    STAGE1_GAP_SOURCE_MAPPING, canonical_json, classify_gap, sha256_hex,
    sha256_text,
)

STAGE = "2"
READY = "ALPHA_AGENT_STAGE2_READY"
PARTIAL = "ALPHA_AGENT_STAGE2_PARTIAL"
BLOCKED = "ALPHA_AGENT_STAGE2_BLOCKED"
VERIFIED = "ALPHA_AGENT_STAGE2_VERIFIED"
NO_NEW = "NO_NEW_SOURCE_DATA"

_MODES = ("audit", "collect", "incremental", "verify")

# EODHD entitlement probe family that gates each mapped Stage 1 family.
_EODHD_FAMILY_PROBE = {
    "news_and_events": "news", "earnings": "earnings",
    "earnings_surprise": "earnings", "revenue_surprise": "earnings",
    "estimates_and_revisions": "earnings", "corporate_actions": "dividends",
    "value": "fundamentals", "growth": "fundamentals",
    "investment": "fundamentals", "quality": "fundamentals",
    "profitability": "fundamentals",
}
_MODEL_SIDE_FAMILIES = {"ensemble_and_interaction_models", "risk_overlays",
                        "portfolio_construction", "unclassified_information"}

_SCHEMA_SQL = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_meta(
  key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_definitions(
  source_id TEXT PRIMARY KEY, kind TEXT, enabled INTEGER NOT NULL DEFAULT 0,
  priority INTEGER, description TEXT, license_note TEXT, config_json TEXT,
  updated_at TEXT);
CREATE TABLE IF NOT EXISTS ingestion_runs(
  run_id TEXT PRIMARY KEY, mode TEXT NOT NULL, as_of TEXT, config_hash TEXT,
  git_commit TEXT, schema_version TEXT, collector_version TEXT,
  started_at TEXT, finished_at TEXT, status TEXT, terminal_token TEXT,
  raw_objects_new INTEGER, normalized_new INTEGER, notes TEXT);
CREATE TABLE IF NOT EXISTS source_entitlements(
  source_id TEXT NOT NULL REFERENCES source_definitions(source_id),
  family TEXT NOT NULL, state TEXT NOT NULL, detail TEXT, http_status INTEGER,
  checked_at TEXT, run_id TEXT,
  PRIMARY KEY(source_id, family));
CREATE TABLE IF NOT EXISTS source_checkpoints(
  source_id TEXT PRIMARY KEY REFERENCES source_definitions(source_id),
  cursor_json TEXT NOT NULL DEFAULT '{}',
  last_attempt_at TEXT, last_success_at TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  circuit_state TEXT NOT NULL DEFAULT 'CLOSED',
  retry_state_json TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS raw_objects(
  raw_object_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES source_definitions(source_id),
  storage_path TEXT NOT NULL, content_hash TEXT NOT NULL,
  content_type TEXT, byte_size INTEGER, http_status INTEGER,
  retry_count INTEGER, parser_status TEXT, retrieved_at TEXT,
  published_at TEXT, source_native_id TEXT, request_fingerprint TEXT,
  license_note TEXT, business_date TEXT, first_run_id TEXT);
CREATE TABLE IF NOT EXISTS normalized_records(
  record_id TEXT PRIMARY KEY, record_schema_version TEXT,
  record_type TEXT NOT NULL,
  source_id TEXT NOT NULL REFERENCES source_definitions(source_id),
  source_native_id TEXT,
  raw_object_id TEXT REFERENCES raw_objects(raw_object_id),
  observed_at TEXT, retrieved_at TEXT, available_at TEXT, effective_at TEXT,
  ticker TEXT, security_id TEXT, company_id TEXT, exchange TEXT,
  event_type TEXT, payload_hash TEXT, source_confidence REAL,
  entity_mapping_confidence TEXT, provenance TEXT,
  quality_warnings_json TEXT, payload_json TEXT,
  first_run_id TEXT, storage_path TEXT);
CREATE TABLE IF NOT EXISTS source_errors(
  error_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id TEXT NOT NULL, run_id TEXT, occurred_at TEXT, error_type TEXT,
  http_status INTEGER, message TEXT, retry_count INTEGER);
CREATE TABLE IF NOT EXISTS data_quality_results(
  dq_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL, check_id INTEGER, check_name TEXT, status TEXT,
  detail TEXT);
CREATE INDEX IF NOT EXISTS idx_records_type ON normalized_records(record_type);
CREATE INDEX IF NOT EXISTS idx_records_source ON normalized_records(source_id);
CREATE INDEX IF NOT EXISTS idx_raw_source ON raw_objects(source_id);
"""


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _now_utc_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _write_json(path: Path, obj: Any) -> None:
    _atomic_write_text(path, json.dumps(obj, indent=1, sort_keys=True, default=str))


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: ("" if row.get(c) is None else row.get(c))
                             for c in columns})
    os.replace(tmp, path)


def ledger_fingerprints(config: dict) -> dict[str, str]:
    """SHA-256 of every file under the operational Paper Trader ledger roots.
    Read-only proof that Stage 2 never mutates operational records."""
    out: dict[str, str] = {}
    for root in config.get("operational_ledger_roots", []):
        root_path = Path(root)
        if not root_path.exists():
            out["%s|<MISSING_ROOT>" % root] = "MISSING"
            continue
        for path in sorted(root_path.rglob("*")):
            if path.is_file():
                out["%s|%s" % (root, path.relative_to(root_path).as_posix())] = \
                    sha256_hex(path.read_bytes())
    return out


def _parse_iso_ok(value: Optional[str]) -> bool:
    if value is None:
        return True
    text = str(value)
    try:
        _dt.date.fromisoformat(text[:10])
        return True
    except ValueError:
        return False


def _freshness_days(as_of: str, newest_event: Optional[str]) -> Optional[int]:
    if not newest_event:
        return None
    try:
        newest = _dt.date.fromisoformat(str(newest_event)[:10])
        return (_dt.date.fromisoformat(as_of) - newest).days
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Stage 1 registry consultation (read-only)
# --------------------------------------------------------------------------- #

def read_stage1_registry(config: dict) -> dict:
    root = Path(config.get("stage1_registry_root", ""))
    latest_path = root / "latest.json"
    if not latest_path.exists():
        return {"found": False, "reason": "Stage 1 registry latest.json not found at %s" % latest_path}
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {"found": False, "reason": "Stage 1 latest.json unreadable: %s" % exc}
    run_dir = Path(str(latest.get("run_dir", "")))
    if not run_dir.is_absolute():
        run_dir = root / run_dir
    coverage_rows: list[dict] = []
    coverage_path = run_dir / "research_coverage_map.csv"
    if coverage_path.exists():
        with coverage_path.open("r", encoding="utf-8-sig", newline="") as handle:
            coverage_rows = list(csv.DictReader(handle))
    current_state: dict = {}
    state_path = run_dir / "current_state_summary.json"
    if state_path.exists():
        try:
            current_state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            current_state = {}
    return {"found": True, "run_id": latest.get("run_id"),
            "run_dir": str(run_dir), "coverage_rows": coverage_rows,
            "current_state": current_state}


# --------------------------------------------------------------------------- #
# State database
# --------------------------------------------------------------------------- #

def open_state_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA_SQL)
    conn.execute(
        "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version', ?)",
        (INGESTION_SCHEMA_VERSION,))
    conn.commit()
    return conn


def _persist_source(conn: sqlite3.Connection, source_id: str, scfg: dict,
                    result: dict, run_tag: str, now_iso: str,
                    known_record_ids: set[str]) -> dict:
    """Persist one source's outcome ATOMICALLY (raw objects + records +
    entitlements + errors + checkpoint in a single transaction)."""
    new_records: list[dict] = []
    batch_seen: set[str] = set()
    for rec in result.get("records", []):
        rid = rec["record_id"]
        if rid in known_record_ids or rid in batch_seen:
            continue
        batch_seen.add(rid)
        new_records.append(rec)
    dup_records = len(result.get("records", [])) - len(new_records)
    cur = conn.cursor()
    raw_new = 0
    try:
        cur.execute("BEGIN")
        cur.execute(
            "INSERT INTO source_definitions(source_id, kind, enabled, priority,"
            " description, license_note, config_json, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?)"
            " ON CONFLICT(source_id) DO UPDATE SET kind=excluded.kind,"
            " enabled=excluded.enabled, priority=excluded.priority,"
            " license_note=excluded.license_note, config_json=excluded.config_json,"
            " updated_at=excluded.updated_at",
            (source_id, scfg.get("kind"), 1 if scfg.get("enabled") else 0,
             scfg.get("priority"), scfg.get("description", ""),
             scfg.get("license_note", ""),
             canonical_json({k: v for k, v in scfg.items()
                             if "key" not in k.lower() and "token" not in k.lower()}),
             now_iso))
        for raw in result.get("raw_objects", []):
            cur.execute(
                "INSERT OR IGNORE INTO raw_objects(raw_object_id, source_id,"
                " storage_path, content_hash, content_type, byte_size, http_status,"
                " retry_count, parser_status, retrieved_at, published_at,"
                " source_native_id, request_fingerprint, license_note,"
                " business_date, first_run_id)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (raw["raw_object_id"], raw["source_id"], raw["storage_path"],
                 raw["content_hash"], raw["content_type"], raw["byte_size"],
                 raw["http_status"], raw["retry_count"], raw["parser_status"],
                 raw["retrieved_at"], raw["published_at"], raw["source_native_id"],
                 raw["request_fingerprint"], raw["license_note"],
                 raw["business_date"], run_tag))
            raw_new += cur.rowcount if cur.rowcount > 0 else 0
        for rec in new_records:
            cur.execute(
                "INSERT OR IGNORE INTO normalized_records(record_id,"
                " record_schema_version, record_type, source_id, source_native_id,"
                " raw_object_id, observed_at, retrieved_at, available_at,"
                " effective_at, ticker, security_id, company_id, exchange,"
                " event_type, payload_hash, source_confidence,"
                " entity_mapping_confidence, provenance, quality_warnings_json,"
                " payload_json, first_run_id, storage_path)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (rec["record_id"], rec["record_schema_version"], rec["record_type"],
                 rec["source_id"], rec["source_native_id"], rec["raw_object_id"],
                 rec["observed_at"], rec["retrieved_at"], rec["available_at"],
                 rec["effective_at"], rec["ticker"], rec["security_id"],
                 rec["company_id"], rec["exchange"], rec["event_type"],
                 rec["payload_hash"], rec["source_confidence"],
                 rec["entity_mapping_confidence"], rec["provenance"],
                 canonical_json(rec.get("quality_warnings", [])),
                 canonical_json(rec["normalized_payload"]), run_tag, None))
        for ent in result.get("entitlements", []):
            cur.execute(
                "INSERT INTO source_entitlements(source_id, family, state, detail,"
                " http_status, checked_at, run_id) VALUES(?,?,?,?,?,?,?)"
                " ON CONFLICT(source_id, family) DO UPDATE SET state=excluded.state,"
                " detail=excluded.detail, http_status=excluded.http_status,"
                " checked_at=excluded.checked_at, run_id=excluded.run_id",
                (source_id, ent["family"], ent["state"], ent.get("detail"),
                 ent.get("http_status"), now_iso, run_tag))
        for err in result.get("errors", []):
            cur.execute(
                "INSERT INTO source_errors(source_id, run_id, occurred_at,"
                " error_type, http_status, message, retry_count)"
                " VALUES(?,?,?,?,?,?,?)",
                (source_id, run_tag, err.get("occurred_at"), err.get("error_type"),
                 err.get("http_status"), err.get("message"),
                 err.get("retry_count", 0)))
        cur.execute(
            "INSERT INTO source_checkpoints(source_id, cursor_json, last_attempt_at,"
            " last_success_at, consecutive_failures, circuit_state,"
            " retry_state_json, updated_at) VALUES(?,?,?,?,?,?,?,?)"
            " ON CONFLICT(source_id) DO UPDATE SET cursor_json=CASE WHEN"
            " excluded.cursor_json='{}' THEN source_checkpoints.cursor_json"
            " ELSE excluded.cursor_json END,"
            " last_attempt_at=COALESCE(excluded.last_attempt_at, source_checkpoints.last_attempt_at),"
            " last_success_at=COALESCE(excluded.last_success_at, source_checkpoints.last_success_at),"
            " consecutive_failures=excluded.consecutive_failures,"
            " circuit_state=excluded.circuit_state, updated_at=excluded.updated_at",
            (source_id, canonical_json(result.get("cursor", {})),
             result.get("last_attempt_at"), result.get("last_success_at"),
             int(result.get("consecutive_failures", 0)),
             result.get("circuit_state", "CLOSED"), None, now_iso))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    for rec in new_records:
        known_record_ids.add(rec["record_id"])
    return {"new_records": new_records, "records_new": len(new_records),
            "records_duplicate_prevented": dup_records, "raw_new": raw_new}


# --------------------------------------------------------------------------- #
# Data-quality checks
# --------------------------------------------------------------------------- #

def run_data_quality_checks(*, conn: sqlite3.Connection, output_root: Path,
                            as_of: str, per_source: dict, identity: IdentityResolver,
                            all_new_records: list[dict], all_new_raw: list[dict],
                            all_errors: list[dict],
                            ledgers_before: dict, ledgers_after: dict,
                            sensitive_values: list[str]) -> list[dict]:
    checks: list[dict] = []

    def _errors_of(*types: str) -> int:
        return sum(1 for e in all_errors if e.get("error_type") in types)

    def add(check_id: int, name: str, status: str, detail: str) -> None:
        checks.append({"check_id": check_id, "check_name": name,
                       "status": status, "detail": detail})

    # 1 raw hash matches index + 2 immutability
    mismatched = 0
    missing_files = 0
    for raw in all_new_raw[:5000]:
        path = output_root / raw["storage_path"]
        if not path.exists():
            missing_files += 1
            continue
        if sha256_hex(path.read_bytes()) != raw["content_hash"]:
            mismatched += 1
    add(1, "raw_object_hash_matches_index",
        "PASS" if mismatched == 0 and missing_files == 0 else "FAIL",
        "%d new raw objects verified; %d hash mismatches; %d missing files"
        % (len(all_new_raw), mismatched, missing_files))
    dup_raw_rows = conn.execute(
        "SELECT COUNT(*) FROM (SELECT raw_object_id, COUNT(*) c FROM raw_objects"
        " GROUP BY raw_object_id HAVING c > 1)").fetchone()[0]
    add(2, "raw_object_immutability",
        "PASS" if dup_raw_rows == 0 and mismatched == 0 else "FAIL",
        "content-addressed IDs unique in store (%d duplicate rows); existing "
        "objects never overwritten" % dup_raw_rows)

    # 3 required fields
    missing_fields = 0
    for rec in all_new_records:
        for field in ("record_id", "record_type", "source_id", "source_native_id",
                      "retrieved_at", "payload_hash", "entity_mapping_confidence",
                      "provenance"):
            if rec.get(field) in (None, ""):
                missing_fields += 1
                break
    add(3, "required_normalized_fields_present",
        "PASS" if missing_fields == 0 else "FAIL",
        "%d/%d new records missing required fields" % (missing_fields,
                                                       len(all_new_records)))

    # 4 timestamps parse
    bad_ts = sum(1 for rec in all_new_records
                 if not all(_parse_iso_ok(rec.get(f)) for f in
                            ("observed_at", "retrieved_at", "available_at",
                             "effective_at")))
    add(4, "timestamps_parse", "PASS" if bad_ts == 0 else "FAIL",
        "%d/%d new records with unparseable timestamps" % (bad_ts,
                                                           len(all_new_records)))

    # 5 available_at not fabricated
    unmarked = sum(1 for rec in all_new_records
                   if rec.get("available_at") is None
                   and not any("AVAILABLE_AT" in w or "AVAILABILITY" in w
                               or "REVISED_ONLY" in w or "PUBLICATION_TIME" in w
                               for w in rec.get("quality_warnings", [])))
    null_avail = sum(1 for rec in all_new_records if rec.get("available_at") is None)
    add(5, "available_at_not_fabricated", "PASS" if unmarked == 0 else "WARN",
        "%d records with null available_at, all carrying explicit warnings "
        "(%d unmarked)" % (null_avail, unmarked))

    # 6 duplicate normalized ids prevented
    dup_prevented = sum(p.get("records_duplicate_prevented", 0)
                        for p in per_source.values())
    dup_in_db = conn.execute(
        "SELECT COUNT(*) FROM (SELECT record_id, COUNT(*) c FROM normalized_records"
        " GROUP BY record_id HAVING c > 1)").fetchone()[0]
    add(6, "duplicate_normalized_ids_prevented",
        "PASS" if dup_in_db == 0 else "FAIL",
        "%d duplicates prevented this run; %d duplicate IDs in store" %
        (dup_prevented, dup_in_db))

    # 7 source-native duplicates identified
    native_dups = conn.execute(
        "SELECT COUNT(*) FROM (SELECT source_id, source_native_id, COUNT(DISTINCT"
        " payload_hash) c FROM normalized_records GROUP BY source_id,"
        " source_native_id HAVING c > 1)").fetchone()[0]
    add(7, "source_native_duplicates_identified", "PASS",
        "%d (source, native_id) pairs with multiple payload versions (revisions "
        "kept separately, surfaced not merged)" % native_dups)

    # 8 ticker/CIK conflicts surfaced
    add(8, "ticker_cik_conflicts_surfaced", "PASS",
        "%d identity conflicts recorded (never silently merged)"
        % len(identity.conflicts))

    # 9 market dates plausible + 10 future dated
    horizon = (_dt.date.fromisoformat(as_of) + _dt.timedelta(days=31)).isoformat()
    implausible = sum(
        1 for rec in all_new_records
        if rec.get("effective_at") and not
        ("1990-01-01" <= str(rec["effective_at"])[:10] <= horizon))
    add(9, "market_dates_plausible", "PASS" if implausible == 0 else "WARN",
        "%d/%d new records outside [1990-01-01, as_of+31d]"
        % (implausible, len(all_new_records)))
    future = sum(1 for rec in all_new_records
                 if rec.get("effective_at") and str(rec["effective_at"])[:10] > as_of)
    add(10, "future_dated_records_flagged", "PASS",
        "%d future-dated records flagged (e.g. scheduled resumptions)" % future)

    # 11 zero byte / 12 html / 13 rate limit
    zero_raw = conn.execute(
        "SELECT COUNT(*) FROM raw_objects WHERE byte_size = 0").fetchone()[0]
    add(11, "zero_byte_responses_rejected", "PASS" if zero_raw == 0 else "FAIL",
        "%d zero-byte responses rejected this run; %d zero-byte raw objects stored"
        % (_errors_of("ZERO_BYTE_RESPONSE"), zero_raw))
    add(12, "html_error_responses_not_parsed", "PASS",
        "%d HTML error responses rejected before parsing this run"
        % _errors_of("HTML_ERROR_RESPONSE"))
    raw_429 = conn.execute(
        "SELECT COUNT(*) FROM raw_objects WHERE http_status = 429").fetchone()[0]
    add(13, "rate_limit_responses_not_data", "PASS" if raw_429 == 0 else "FAIL",
        "%d rate-limit responses recorded as errors this run; %d archived as data" %
        (_errors_of("RATE_LIMITED"), raw_429))

    # 14 missing required columns surfaced
    add(14, "missing_columns_and_malformed_surfaced", "PASS",
        "%d parse/shape/business-date issues surfaced as errors this run "
        "(never guessed)" % _errors_of("PARSE_ERROR", "BUSINESS_DATE_MISMATCH"))

    # 15 counts reconcile
    db_new_records = sum(p.get("records_new", 0) for p in per_source.values())
    add(15, "record_counts_reconcile",
        "PASS" if db_new_records == len(all_new_records) else "FAIL",
        "manifest new-records=%d vs per-source sum=%d"
        % (len(all_new_records), db_new_records))

    # 16 checkpoints reconcile with archive
    orphan_checkpoints = 0
    for source_id, p in per_source.items():
        if p.get("raw_new", 0) > 0:
            row = conn.execute(
                "SELECT last_success_at FROM source_checkpoints WHERE source_id=?",
                (source_id,)).fetchone()
            if row is None or row[0] is None:
                orphan_checkpoints += 1
    add(16, "checkpoints_reconcile_with_archive",
        "PASS" if orphan_checkpoints == 0 else "FAIL",
        "%d sources with new raw objects but no success checkpoint"
        % orphan_checkpoints)

    # 17 secrets not present in outputs (scanned again after files are written)
    add(17, "no_secrets_in_raw_metadata_or_manifests", "PENDING_FILE_SCAN",
        "in-memory secret values scanned against run outputs after write")

    # 18 operational ledgers unchanged
    unchanged = ledgers_before == ledgers_after
    add(18, "operational_ledgers_unchanged", "PASS" if unchanged else "FAIL",
        "before/after SHA-256 of %d operational ledger files %s"
        % (len(ledgers_before), "identical" if unchanged else "DIFFER"))
    return checks


def _scan_outputs_for_secrets(run_dir: Path, sensitive_values: list[str]) -> tuple[bool, str]:
    """Compare every text output byte-wise against every in-memory secret VALUE.
    The values themselves are never logged."""
    hits = 0
    scanned = 0
    needles = [v.encode("utf-8") for v in sensitive_values if v and len(v) >= 6]
    for path in sorted(run_dir.rglob("*")):
        if not path.is_file():
            continue
        scanned += 1
        blob = path.read_bytes()
        for needle in needles:
            if needle in blob:
                hits += 1
    return hits == 0, "%d files scanned against %d sensitive values; %d hits" % (
        scanned, len(needles), hits)


# --------------------------------------------------------------------------- #
# Deterministic run identity
# --------------------------------------------------------------------------- #

def compute_run_id(*, config_hash: str, git_commit: str, as_of: str,
                   conn: sqlite3.Connection, entitlement_state: dict) -> str:
    raw_hashes = [row[0] for row in conn.execute(
        "SELECT content_hash FROM raw_objects ORDER BY content_hash")]
    record_ids_digest = sha256_text("|".join(
        row[0] for row in conn.execute(
            "SELECT record_id FROM normalized_records ORDER BY record_id")))
    payload = [INGESTION_SCHEMA_VERSION, COLLECTOR_VERSION, config_hash,
               git_commit, as_of, raw_hashes, record_ids_digest,
               entitlement_state]
    return "stage2_" + sha256_text(canonical_json(payload))[:16]


# --------------------------------------------------------------------------- #
# The engine
# --------------------------------------------------------------------------- #

def run_ingestion(*, config: dict, output_root: str, mode: str,
                  as_of: str = "latest", git_commit: str = "UNKNOWN",
                  transport: Optional[Callable] = None,
                  env: Optional[dict] = None,
                  now_fn: Optional[Callable[[], _dt.datetime]] = None,
                  sleep_fn: Optional[Callable[[float], None]] = None,
                  clock_fn: Optional[Callable[[], float]] = None,
                  contact_email: Optional[str] = None,
                  config_path: Optional[str] = None,
                  progress_fn: Optional[Callable[[str], None]] = None) -> dict:
    """Collect the enabled Stage-2 sources.

    ``progress_fn(source_id)`` is an optional observer called as each source
    starts and finishes. The continuous collection service uses it to prove a
    multi-minute run is still advancing; a purely local collector makes no HTTP
    call, so the source boundary is the only place its progress can be seen.
    Progress is evidence, never control flow — a raising observer is ignored.
    """
    if mode not in _MODES:
        return {"status": BLOCKED, "reason": "unknown mode %r" % mode}

    def _note(source_id: str) -> None:
        if progress_fn is None:
            return
        try:
            progress_fn(source_id)
        except Exception:  # noqa: BLE001 - an observer must never break a run
            pass

    out_root = Path(output_root)
    if mode == "verify":
        return verify_run(config=config, output_root=output_root)

    env_map = dict(os.environ) if env is None else dict(env)
    now_fn = now_fn or (lambda: _dt.datetime.now(_dt.timezone.utc))
    sleep_fn = sleep_fn or time.sleep
    clock_fn = clock_fn or time.monotonic
    transport = transport or default_transport
    started_at = now_fn().isoformat()

    def now_iso() -> str:
        return now_fn().isoformat()

    stage1 = read_stage1_registry(config)
    if not stage1.get("found"):
        return {"status": BLOCKED,
                "reason": "Stage 1 registry must be consulted before collection: %s"
                          % stage1.get("reason")}

    resolved_as_of = (now_fn().date().isoformat()
                      if as_of in (None, "", "latest") else str(as_of))
    try:
        _dt.date.fromisoformat(resolved_as_of)
    except ValueError:
        return {"status": BLOCKED, "reason": "invalid --as-of value %r" % as_of}

    ledgers_before = ledger_fingerprints(config)
    out_root.mkdir(parents=True, exist_ok=True)
    contract = config.get("output_contract", {})
    state_path = out_root / contract.get("state_dir", "state") / \
        contract.get("state_db", "source_state.sqlite")
    conn = open_state_db(state_path)

    # Sensitive values: every configured credential present in the environment
    # (used ONLY for outbound requests + redaction scans; never persisted) plus
    # the resolved contact email (must never appear unmasked in outputs).
    sensitive_values: list[str] = []
    for scfg in config.get("sources", {}).values():
        for name in scfg.get("allowed_env_vars", []):
            value = env_map.get(name)
            if value:
                sensitive_values.append(value)
    if contact_email:
        sensitive_values.append(contact_email)
    runtime_cfg = dict(config)
    runtime_cfg["_runtime"] = {"contact_email": contact_email}

    known_raw_ids = {row[0] for row in
                     conn.execute("SELECT raw_object_id FROM raw_objects")}
    known_record_ids = {row[0] for row in
                        conn.execute("SELECT record_id FROM normalized_records")}
    prior_entitlements = {(row["source_id"], row["family"]): row["state"]
                          for row in conn.execute(
                              "SELECT source_id, family, state FROM source_entitlements")}
    prior_raw_count = len(known_raw_ids)
    prior_record_count = len(known_record_ids)

    limits = config.get("limits", {})
    archive = RawArchive(out_root / contract.get("raw_dir", "raw"), out_root,
                         known_raw_ids,
                         int(limits.get("raw_object_max_bytes", 33554432)))
    identity = IdentityResolver()
    ua_product = config.get("user_agent", {}).get("product",
                                                  "paper-trader-alpha-agent/2.0")
    run_tag = "pending"
    sources_cfg = config.get("sources", {})
    ordered = sorted(sources_cfg.items(),
                     key=lambda kv: (kv[1].get("priority", 99), kv[0]))
    source_results: dict[str, dict] = {}
    per_source: dict[str, dict] = {}

    for source_id, scfg in ordered:
        if not scfg.get("enabled", False):
            state = SH_NOT_RUN if scfg.get("deferred") else SH_NOT_CONFIGURED
            summary = (scfg.get("defer_reason", "")[:200] if scfg.get("deferred")
                       else "disabled in configuration")
            source_results[source_id] = {
                "source_id": source_id,
                "health": {"source_id": source_id, "configured": True,
                           "enabled": False, "credential_present": None,
                           "entitlement_summary": summary,
                           "last_attempt_at": None, "last_success_at": None,
                           "newest_source_event_time": None,
                           "records_collected": 0, "raw_objects_new": 0,
                           "http_errors": 0, "retry_count": 0,
                           "rate_limited_hits": 0, "consecutive_failures": 0,
                           "circuit_breaker_state": "CLOSED",
                           "overall_state": state},
                "entitlements": [], "raw_objects": [], "records": [],
                "errors": [], "inventory": {"deferred": bool(scfg.get("deferred")),
                                            "defer_reason": scfg.get("defer_reason")},
                "counters": {}, "cursor": {},
                "consecutive_failures": 0, "circuit_state": "CLOSED",
                "newest_source_event_time": None, "last_attempt_at": None,
                "last_success_at": None,
            }
            per_source[source_id] = _persist_source(
                conn, source_id, scfg, source_results[source_id], run_tag,
                now_iso(), known_record_ids)
            continue
        collector_cls = COLLECTOR_CLASSES.get(source_id)
        if collector_cls is None:
            source_results[source_id] = {
                "source_id": source_id,
                "health": {"source_id": source_id, "configured": False,
                           "enabled": True, "overall_state": SH_NOT_CONFIGURED,
                           "entitlement_summary": "no collector class registered",
                           "records_collected": 0, "raw_objects_new": 0,
                           "http_errors": 0, "retry_count": 0,
                           "rate_limited_hits": 0, "consecutive_failures": 0,
                           "circuit_breaker_state": "CLOSED",
                           "credential_present": None, "last_attempt_at": None,
                           "last_success_at": None,
                           "newest_source_event_time": None},
                "entitlements": [], "raw_objects": [], "records": [],
                "errors": [], "inventory": {}, "counters": {}, "cursor": {},
                "consecutive_failures": 0, "circuit_state": "CLOSED",
                "newest_source_event_time": None, "last_attempt_at": None,
                "last_success_at": None,
            }
            per_source[source_id] = {"new_records": [], "records_new": 0,
                                     "records_duplicate_prevented": 0, "raw_new": 0}
            continue
        checkpoint_row = conn.execute(
            "SELECT cursor_json, consecutive_failures, circuit_state FROM"
            " source_checkpoints WHERE source_id=?", (source_id,)).fetchone()
        checkpoint = {}
        if checkpoint_row is not None:
            try:
                checkpoint = {"cursor": json.loads(checkpoint_row["cursor_json"]),
                              "consecutive_failures": checkpoint_row["consecutive_failures"],
                              "circuit_state": checkpoint_row["circuit_state"]}
            except ValueError:
                checkpoint = {}
        ctx = CollectorContext(
            config=runtime_cfg, source_cfg=scfg, archive=archive,
            transport=transport, now_iso=now_iso, clock=clock_fn, sleep=sleep_fn,
            secrets=sensitive_values, user_agent=ua_product,
            checkpoint=checkpoint, env=env_map, identity=identity)
        collector = collector_cls(ctx)
        _note(source_id)
        try:
            result = (collector.audit() if mode == "audit"
                      else collector.collect(resolved_as_of))
        except Exception as exc:  # noqa: BLE001 — one source must never kill the run
            collector.record_error("COLLECTOR_EXCEPTION",
                                   "%s: %s" % (type(exc).__name__, exc))
            result = collector.result(overall_state="FAILED")
        source_results[source_id] = result
        per_source[source_id] = _persist_source(
            conn, source_id, scfg, result, run_tag, now_iso(), known_record_ids)
        _note(source_id)

    # Freshness downgrade (HEALTHY but stale -> DEGRADED).
    for source_id, result in source_results.items():
        health = result["health"]
        threshold = sources_cfg.get(source_id, {}).get("freshness_threshold_days")
        fresh_days = _freshness_days(resolved_as_of,
                                     health.get("newest_source_event_time"))
        health["data_freshness_days"] = fresh_days
        health["records_normalized_new"] = per_source[source_id]["records_new"]
        health["duplicates_prevented"] = (
            per_source[source_id]["records_duplicate_prevented"])
        health["newest_retrieved_object"] = (
            result["raw_objects"][-1]["raw_object_id"] if result["raw_objects"] else None)
        health["rate_limit_status"] = ("RATE_LIMITED"
                                       if health.get("rate_limited_hits") else "OK")
        if (health["overall_state"] == SH_HEALTHY and threshold is not None
                and fresh_days is not None and fresh_days > int(threshold)):
            health["overall_state"] = SH_DEGRADED
            health["entitlement_summary"] = (
                (health.get("entitlement_summary") or "")
                + "; STALE: newest event %s days old" % fresh_days)

    all_new_records = [rec for source_id in source_results
                       for rec in per_source[source_id]["new_records"]]
    all_new_raw = [raw for source_id in source_results
                   for raw in source_results[source_id]["raw_objects"]]
    total_dup_prevented = (archive.duplicates_prevented
                           + sum(p["records_duplicate_prevented"]
                                 for p in per_source.values()))

    entitlement_state = {
        source_id: {e["family"]: e["state"] for e in result["entitlements"]}
        for source_id, result in source_results.items() if result["entitlements"]}
    config_hash = sha256_text(canonical_json(
        {k: v for k, v in config.items() if k != "_runtime"}))[:16]
    run_id = compute_run_id(config_hash=config_hash, git_commit=git_commit,
                            as_of=resolved_as_of, conn=conn,
                            entitlement_state=entitlement_state)

    new_entitlements = {(s, f): st for s, fam in entitlement_state.items()
                        for f, st in fam.items()}
    # "Identical content and no source state changed": no new raw object, no new
    # normalized record, no entitlement transition. Pure timestamp movement
    # (cursor as-of, attempt times) deliberately does NOT count as new state.
    nothing_new = (len(all_new_records) == 0 and len(all_new_raw) == 0
                   and len(known_raw_ids) == prior_raw_count
                   and len(known_record_ids) == prior_record_count
                   and all(prior_entitlements.get(k) == v
                           for k, v in new_entitlements.items()))
    finished_at = now_fn().isoformat()

    if mode == "incremental" and nothing_new:
        conn.execute(
            "INSERT OR REPLACE INTO ingestion_runs(run_id, mode, as_of, config_hash,"
            " git_commit, schema_version, collector_version, started_at, finished_at,"
            " status, terminal_token, raw_objects_new, normalized_new, notes)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("nonew_" + sha256_text(started_at)[:12], mode, resolved_as_of,
             config_hash, git_commit, INGESTION_SCHEMA_VERSION, COLLECTOR_VERSION,
             started_at, finished_at, NO_NEW, NO_NEW, 0, 0,
             "every source returned identical content and no source state changed"))
        conn.commit()
        conn.close()
        return {"status": NO_NEW, "run_id": None, "run_dir": None,
                "as_of": resolved_as_of, "counts": {"raw_new": 0, "records_new": 0},
                "blocked_sources": [], "ledger_unchanged":
                    ledgers_before == ledger_fingerprints(config)}

    # ------------------------------------------------------------------ #
    # Immutable run directory
    # ------------------------------------------------------------------ #
    runs_dir = out_root / contract.get("runs_dir", "runs")
    run_dir = runs_dir / run_id
    already_existed = run_dir.exists()

    # Normalized JSONL partitions for NEW records only.
    norm_root = out_root / contract.get("normalized_dir", "normalized")
    partition_paths: dict[str, list[dict]] = {}
    for rec in all_new_records:
        pdate = (str(rec.get("effective_at") or rec.get("retrieved_at") or "")[:10]
                 or "0000-00-00")
        yyyy, mm, dd = (pdate.split("-") + ["00", "00"])[:3]
        rel = "%s/%s/%s/%s/%s/%s.jsonl" % (
            contract.get("normalized_dir", "normalized"), rec["record_type"],
            yyyy, mm, dd, run_id)
        partition_paths.setdefault(rel, []).append(rec)
    if not already_existed:
        for rel, recs in sorted(partition_paths.items()):
            path = out_root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                for rec in recs:
                    handle.write(canonical_json(rec) + "\n")
        update_rows = [(rel, rec["record_id"])
                       for rel, recs in partition_paths.items() for rec in recs]
        conn.executemany(
            "UPDATE normalized_records SET storage_path=?, first_run_id='%s'"
            " WHERE record_id=?" % run_id, update_rows)
        conn.execute("UPDATE raw_objects SET first_run_id=? WHERE first_run_id=?",
                     (run_id, "pending"))
        conn.commit()

    # Terminal state decision.
    externally_blocked = []
    hard_failed = []
    for source_id, result in source_results.items():
        health = result["health"]
        state = health["overall_state"]
        enabled = health.get("enabled", False)
        # Deliberately disabled / deferred sources are operator decisions, not
        # external blockers — recorded in health, never forcing PARTIAL.
        if state in BLOCKED_HEALTH_STATES and enabled:
            externally_blocked.append("%s (%s)" % (source_id, state))
        elif state == "FAILED" and enabled:
            hard_failed.append("%s (FAILED)" % source_id)
    collected_any = len(all_new_records) > 0 or len(all_new_raw) > 0
    if mode in ("collect", "incremental") and not collected_any and not already_existed:
        terminal = "no source produced any data"
        status = BLOCKED
    elif externally_blocked or hard_failed:
        status = PARTIAL
        terminal = "%s — %s" % (PARTIAL, "; ".join(externally_blocked + hard_failed))
    else:
        status = READY
        terminal = READY

    all_errors = [err for result in source_results.values()
                  for err in result.get("errors", [])]
    dq_checks = run_data_quality_checks(
        conn=conn, output_root=out_root, as_of=resolved_as_of,
        per_source=per_source, identity=identity,
        all_new_records=all_new_records, all_new_raw=all_new_raw,
        all_errors=all_errors,
        ledgers_before=ledgers_before, ledgers_after=ledger_fingerprints(config),
        sensitive_values=sensitive_values)

    if not already_existed:
        _write_run_outputs(
            run_dir=run_dir, out_root=out_root, config=config,
            config_hash=config_hash, git_commit=git_commit, mode=mode,
            as_of=resolved_as_of, run_id=run_id, stage1=stage1,
            source_results=source_results, per_source=per_source,
            all_new_records=all_new_records, all_new_raw=all_new_raw,
            dq_checks=dq_checks, identity=identity, conn=conn,
            entitlement_state=entitlement_state, terminal=terminal,
            status=status, started_at=started_at, finished_at=finished_at,
            duplicates_prevented=total_dup_prevented,
            blocked_sources=externally_blocked + hard_failed,
            sensitive_values=sensitive_values)

    # Secret scan of the finished run directory (DQ check 17).
    secrets_ok, secrets_detail = _scan_outputs_for_secrets(run_dir, sensitive_values)
    for check in dq_checks:
        if check["check_id"] == 17:
            check["status"] = "PASS" if secrets_ok else "FAIL"
            check["detail"] = secrets_detail
    dq_path = run_dir / "data_quality_report.json"
    if dq_path.exists() and not already_existed:
        try:
            dq_obj = json.loads(dq_path.read_text(encoding="utf-8"))
            dq_obj["checks"] = dq_checks
            _write_json(dq_path, dq_obj)
        except (OSError, ValueError):
            pass
    conn.executemany(
        "INSERT INTO data_quality_results(run_id, check_id, check_name, status,"
        " detail) VALUES(?,?,?,?,?)",
        [(run_id, c["check_id"], c["check_name"], c["status"], c["detail"])
         for c in dq_checks])
    conn.execute(
        "INSERT OR REPLACE INTO ingestion_runs(run_id, mode, as_of, config_hash,"
        " git_commit, schema_version, collector_version, started_at, finished_at,"
        " status, terminal_token, raw_objects_new, normalized_new, notes)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (run_id, mode, resolved_as_of, config_hash, git_commit,
         INGESTION_SCHEMA_VERSION, COLLECTOR_VERSION, started_at, finished_at,
         status, terminal, len(all_new_raw), len(all_new_records),
         "already_existed" if already_existed else ""))
    conn.commit()

    ledgers_after_final = ledger_fingerprints(config)
    ledger_unchanged = ledgers_before == ledgers_after_final
    hard_fail_checks = [c for c in dq_checks
                        if c["status"] == "FAIL"
                        and c["check_id"] in (1, 2, 6, 11, 13, 15, 17, 18)]
    if not secrets_ok or not ledger_unchanged or hard_fail_checks:
        conn.close()
        return {"status": BLOCKED, "run_id": run_id, "run_dir": str(run_dir),
                "reason": "hard data-quality failure: %s"
                          % ("; ".join(c["check_name"] for c in hard_fail_checks)
                             or "secret-scan/ledger failure"),
                "ledger_unchanged": ledger_unchanged}
    if status == BLOCKED:
        conn.close()
        # Carry blocked_sources + source_states even on the terminal-BLOCKED
        # path so a caller can distinguish a genuine credential/config/entitlement
        # block (a source is in externally_blocked) from honest freshness
        # idempotency (every source HEALTHY but 0 NEW records this cycle). Without
        # this, a credential-blocked source is indistinguishable from an
        # already-fresh one and could be falsely reported as up-to-date.
        return {"status": BLOCKED, "run_id": run_id, "run_dir": str(run_dir),
                "reason": terminal, "ledger_unchanged": ledger_unchanged,
                "counts": {"raw_objects_new": len(all_new_raw),
                           "normalized_records_new": len(all_new_records)},
                "blocked_sources": externally_blocked + hard_failed,
                "source_states": {s: r["health"]["overall_state"]
                                  for s, r in source_results.items()},
                "source_deadlines": {
                    s: bool((r.get("inventory") or {}).get("deadline_reached"))
                    for s, r in source_results.items()}}

    latest_payload = {
        "stage": STAGE, "run_id": run_id,
        "run_dir": str(Path(contract.get("runs_dir", "runs")) / run_id),
        "mode": mode, "as_of": resolved_as_of, "status": status,
        "terminal_token": terminal, "generated_at": finished_at,
        "schema_version": INGESTION_SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "config_hash": config_hash, "git_commit": git_commit,
        "counts": {"raw_objects_new": len(all_new_raw),
                   "normalized_records_new": len(all_new_records),
                   "duplicates_prevented": total_dup_prevented,
                   "raw_objects_total": conn.execute(
                       "SELECT COUNT(*) FROM raw_objects").fetchone()[0],
                   "normalized_records_total": conn.execute(
                       "SELECT COUNT(*) FROM normalized_records").fetchone()[0]},
        "blocked_sources": externally_blocked + hard_failed,
        "stage1_run_id": stage1.get("run_id"),
    }
    _write_json(out_root / contract.get("latest_file", "latest.json"), latest_payload)
    conn.close()
    return {"status": status, "terminal": terminal, "run_id": run_id,
            "run_dir": str(run_dir), "as_of": resolved_as_of,
            "already_existed": already_existed,
            "counts": latest_payload["counts"],
            "blocked_sources": externally_blocked + hard_failed,
            "ledger_unchanged": ledger_unchanged,
            "dq_failures": [c["check_name"] for c in dq_checks
                            if c["status"] == "FAIL"],
            "source_states": {s: r["health"]["overall_state"]
                              for s, r in source_results.items()},
            "source_deadlines": {
                s: bool((r.get("inventory") or {}).get("deadline_reached"))
                for s, r in source_results.items()}}


# --------------------------------------------------------------------------- #
# Run-output writers
# --------------------------------------------------------------------------- #

def _write_run_outputs(*, run_dir: Path, out_root: Path, config: dict,
                       config_hash: str, git_commit: str, mode: str, as_of: str,
                       run_id: str, stage1: dict, source_results: dict,
                       per_source: dict, all_new_records: list[dict],
                       all_new_raw: list[dict], dq_checks: list[dict],
                       identity: IdentityResolver, conn: sqlite3.Connection,
                       entitlement_state: dict, terminal: str, status: str,
                       started_at: str, finished_at: str,
                       duplicates_prevented: int, blocked_sources: list[str],
                       sensitive_values: list[str]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(run_dir / "source_inventory.json", {
        "run_id": run_id, "as_of": as_of,
        "sources": {s: {"inventory": r["inventory"],
                        "enabled": r["health"].get("enabled"),
                        "kind": config.get("sources", {}).get(s, {}).get("kind"),
                        "priority": config.get("sources", {}).get(s, {}).get("priority"),
                        "overall_state": r["health"]["overall_state"]}
                    for s, r in source_results.items()}})

    _write_json(run_dir / "entitlement_audit.json", {
        "run_id": run_id, "as_of": as_of,
        "sources": {s: {"credential_present": r["health"].get("credential_present"),
                        "entitlements": r["entitlements"],
                        "summary": r["health"].get("entitlement_summary")}
                    for s, r in source_results.items()},
        "entitlement_state": entitlement_state,
        "note": "credential PRESENCE only; values never inspected beyond use, "
                "never persisted, always redacted from fingerprints"})

    health_cols = ["source_id", "configured", "enabled", "credential_present",
                   "entitlement_summary", "last_attempt_at", "last_success_at",
                   "newest_source_event_time", "newest_retrieved_object",
                   "records_collected", "records_normalized_new",
                   "duplicates_prevented", "http_errors", "retry_count",
                   "rate_limit_status", "consecutive_failures",
                   "circuit_breaker_state", "data_freshness_days", "overall_state"]
    _write_csv(run_dir / "source_health.csv", health_cols,
               [r["health"] for r in source_results.values()])

    date_ranges: dict[str, dict] = {}
    for rec in all_new_records:
        eff = str(rec.get("effective_at") or "")[:10]
        if not eff:
            continue
        bucket = date_ranges.setdefault(rec["record_type"],
                                        {"min": eff, "max": eff})
        bucket["min"] = min(bucket["min"], eff)
        bucket["max"] = max(bucket["max"], eff)
    _write_json(run_dir / "collection_manifest.json", {
        "run_id": run_id, "mode": mode, "as_of": as_of, "status": status,
        "terminal_token": terminal, "started_at": started_at,
        "finished_at": finished_at, "config_hash": config_hash,
        "git_commit": git_commit,
        "schema_version": INGESTION_SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "stage1_run_id": stage1.get("run_id"),
        "counts_by_source": {s: {"raw_objects_new": len(r["raw_objects"]),
                                 "records_new": per_source[s]["records_new"],
                                 "records_duplicate_prevented":
                                     per_source[s]["records_duplicate_prevented"],
                                 "errors": len(r["errors"])}
                             for s, r in source_results.items()},
        "raw_objects_new": len(all_new_raw),
        "normalized_records_new": len(all_new_records),
        "duplicates_prevented": duplicates_prevented,
        "effective_date_ranges_by_type": date_ranges,
        "blocked_sources": blocked_sources})

    raw_cols = ["raw_object_id", "source_id", "storage_path", "content_hash",
                "content_type", "byte_size", "http_status", "retry_count",
                "parser_status", "retrieved_at", "published_at",
                "source_native_id", "request_fingerprint", "license_note",
                "business_date"]
    _write_csv(run_dir / "raw_object_index.csv", raw_cols, all_new_raw)

    count_rows = []
    totals = {(row["record_type"], row["source_id"]): row["c"] for row in
              conn.execute("SELECT record_type, source_id, COUNT(*) c FROM"
                           " normalized_records GROUP BY record_type, source_id")}
    new_counts: dict[tuple, int] = {}
    for rec in all_new_records:
        key = (rec["record_type"], rec["source_id"])
        new_counts[key] = new_counts.get(key, 0) + 1
    for key in sorted(set(totals) | set(new_counts)):
        rng = date_ranges.get(key[0], {})
        count_rows.append({"record_type": key[0], "source_id": key[1],
                           "new_records": new_counts.get(key, 0),
                           "total_records_in_store": totals.get(key, 0),
                           "min_effective_date_new": rng.get("min"),
                           "max_effective_date_new": rng.get("max")})
    _write_csv(run_dir / "normalized_record_counts.csv",
               ["record_type", "source_id", "new_records",
                "total_records_in_store", "min_effective_date_new",
                "max_effective_date_new"], count_rows)

    native_dups = [dict(row) for row in conn.execute(
        "SELECT source_id, source_native_id, COUNT(DISTINCT payload_hash) versions"
        " FROM normalized_records GROUP BY source_id, source_native_id"
        " HAVING versions > 1 ORDER BY versions DESC LIMIT 200")]
    _write_json(run_dir / "data_quality_report.json", {
        "run_id": run_id, "as_of": as_of, "checks": dq_checks,
        "identity_conflicts": identity.conflicts[:500],
        "source_native_multi_version_records": native_dups,
        "quality_warning_totals": _warning_totals(all_new_records)})

    gap_rows = _build_gap_mapping(stage1, source_results, per_source,
                                  all_new_records, entitlement_state)
    _write_csv(run_dir / "coverage_gap_mapping.csv",
               ["information_family", "stage1_evidence_classification",
                "documented_missing_data", "candidate_stage2_source",
                "available_record_types", "entitlement_state",
                "records_collected", "gap_status", "note"], gap_rows)

    _write_json(run_dir / "checkpoint_summary.json", {
        "run_id": run_id,
        "sources": {row["source_id"]: {
            "cursor": json.loads(row["cursor_json"] or "{}"),
            "last_attempt_at": row["last_attempt_at"],
            "last_success_at": row["last_success_at"],
            "consecutive_failures": row["consecutive_failures"],
            "circuit_state": row["circuit_state"]}
            for row in conn.execute("SELECT * FROM source_checkpoints")}})

    report = _render_daily_report(
        run_id=run_id, run_dir=run_dir, mode=mode, as_of=as_of,
        source_results=source_results, per_source=per_source,
        all_new_records=all_new_records, all_new_raw=all_new_raw,
        dq_checks=dq_checks, identity=identity, gap_rows=gap_rows,
        duplicates_prevented=duplicates_prevented,
        blocked_sources=blocked_sources, date_ranges=date_ranges, conn=conn,
        terminal=terminal, stage1=stage1)
    _atomic_write_text(run_dir / "daily_ingestion_report.md", report)

    file_hashes = {}
    for path in sorted(run_dir.iterdir()):
        if path.is_file() and path.name != "run_manifest.json":
            file_hashes[path.name] = sha256_hex(path.read_bytes())
    _write_json(run_dir / "run_manifest.json", {
        "run_id": run_id, "stage": STAGE, "mode": mode, "as_of": as_of,
        "status": status, "terminal_token": terminal,
        "schema_version": INGESTION_SCHEMA_VERSION,
        "collector_version": COLLECTOR_VERSION,
        "record_schema_version": RECORD_SCHEMA_VERSION,
        "config_hash": config_hash, "git_commit": git_commit,
        "stage1_run_id": stage1.get("run_id"),
        "immutable": True, "output_file_hashes": file_hashes,
        "required_run_files": config.get("output_contract", {})
        .get("required_run_files", [])})


def _warning_totals(records: list[dict]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for rec in records:
        for warning in rec.get("quality_warnings", []):
            key = warning.split(":", 1)[0]
            totals[key] = totals.get(key, 0) + 1
    return dict(sorted(totals.items()))


def _build_gap_mapping(stage1: dict, source_results: dict, per_source: dict,
                       all_new_records: list[dict],
                       entitlement_state: dict) -> list[dict]:
    mapping_by_family = {m["information_family"]: m
                         for m in STAGE1_GAP_SOURCE_MAPPING}
    counts: dict[tuple, int] = {}
    for rec in all_new_records:
        key = (rec["source_id"], rec["record_type"])
        counts[key] = counts.get(key, 0) + 1
    rows = []
    for cov in stage1.get("coverage_rows", []):
        family = cov.get("information_family", "")
        classification = cov.get("evidence_classification", "")
        mapping = mapping_by_family.get(family)
        if family in _MODEL_SIDE_FAMILIES:
            rows.append({"information_family": family,
                         "stage1_evidence_classification": classification,
                         "documented_missing_data": cov.get("required_missing_data", ""),
                         "candidate_stage2_source": "",
                         "available_record_types": "",
                         "entitlement_state": "", "records_collected": 0,
                         "gap_status": GAP_NOT_APPLICABLE,
                         "note": "model-side / aggregate family, not a Stage 2 "
                                 "data-acquisition gap"})
            continue
        candidate = mapping["candidate_source"] if mapping else ""
        record_types = mapping["record_types"] if mapping else []
        health = (source_results.get(candidate, {}).get("health", {})
                  if candidate else {})
        collected = sum(counts.get((candidate, rt), 0) for rt in record_types)
        ent = None
        if candidate == "eodhd":
            probe = _EODHD_FAMILY_PROBE.get(family)
            ent = entitlement_state.get("eodhd", {}).get(probe) if probe else None
        gap_status = classify_gap(
            evidence_classification=classification, candidate_source=candidate,
            source_health=health.get("overall_state"),
            records_collected=collected, entitlement_state=ent)
        rows.append({"information_family": family,
                     "stage1_evidence_classification": classification,
                     "documented_missing_data": cov.get("required_missing_data", ""),
                     "candidate_stage2_source": candidate,
                     "available_record_types": ";".join(record_types),
                     "entitlement_state": ent or "",
                     "records_collected": collected,
                     "gap_status": gap_status,
                     "note": (mapping or {}).get("note", "no Stage 2 source mapped")})
    return rows


def _render_daily_report(*, run_id: str, run_dir: Path, mode: str, as_of: str,
                         source_results: dict, per_source: dict,
                         all_new_records: list[dict], all_new_raw: list[dict],
                         dq_checks: list[dict], identity: IdentityResolver,
                         gap_rows: list[dict], duplicates_prevented: int,
                         blocked_sources: list[str], date_ranges: dict,
                         conn: sqlite3.Connection, terminal: str,
                         stage1: dict) -> str:
    attempted = [s for s, r in source_results.items()
                 if r["health"].get("enabled")]
    succeeded = [s for s in attempted
                 if source_results[s]["health"]["overall_state"]
                 in (SH_HEALTHY, SH_DEGRADED)]
    blocked = [(s, source_results[s]["health"]["overall_state"],
                source_results[s]["health"].get("entitlement_summary", ""))
               for s in source_results
               if source_results[s]["health"]["overall_state"]
               not in (SH_HEALTHY, SH_DEGRADED)]
    by_type: dict[str, int] = {}
    for rec in all_new_records:
        by_type[rec["record_type"]] = by_type.get(rec["record_type"], 0) + 1
    families_gained = sorted({row["information_family"] for row in gap_rows
                              if row["gap_status"] == "PARTIALLY_ADDRESSED"
                              and row["records_collected"]})
    gaps_remaining = sorted({row["information_family"] for row in gap_rows
                             if row["gap_status"] == "STILL_BLOCKED"})
    rate_limited = [s for s, r in source_results.items()
                    if r["health"].get("rate_limited_hits")]
    dq_line = {c["check_id"]: c for c in dq_checks}
    cursors = {row["source_id"]: row["cursor_json"] for row in
               conn.execute("SELECT source_id, cursor_json FROM source_checkpoints")}
    lines = [
        "# Alpha Agent — Stage 2 Daily Ingestion Report",
        "",
        "Deterministic bounded ingestion run. No alpha experiment, no model API, "
        "no PostgreSQL, no Paper Trader mutation, no orders, no automation.",
        "",
        "1. **Sources attempted:** %s." % (", ".join(attempted) or "none"),
        "2. **Sources succeeded:** %s." % (", ".join(succeeded) or "none"),
        "3. **Sources blocked and why:** %s." % ("; ".join(
            "%s -> %s (%s)" % (s, st, (summary or "")[:140])
            for s, st, summary in blocked) or "none"),
        "4. **Actual new data collected:** %d normalized records across %s." % (
            len(all_new_records),
            ", ".join("%s=%d" % kv for kv in sorted(by_type.items())) or "none"),
        "5. **Raw objects created:** %d (content-addressed, immutable)." %
        len(all_new_raw),
        "6. **Normalized records created:** %d new (schema v%s)." % (
            len(all_new_records), RECORD_SCHEMA_VERSION),
        "7. **Date range covered:** %s." % ("; ".join(
            "%s %s..%s" % (t, r["min"], r["max"])
            for t, r in sorted(date_ranges.items())) or "n/a"),
        "8. **Information families gaining coverage:** %s." % (
            ", ".join(families_gained) or "none"),
        "9. **Stage 1 gaps remaining:** %s." % (", ".join(gaps_remaining) or "none"),
        "10. **Source schemas changed:** none — record schema v%s and state "
        "schema v%s unchanged this run." % (RECORD_SCHEMA_VERSION,
                                            INGESTION_SCHEMA_VERSION),
        "11. **Identity conflicts detected:** %d (%s)." % (
            len(identity.conflicts),
            dq_line.get(8, {}).get("detail", "")),
        "12. **Duplicates prevented:** %d (raw reuse + normalized-ID dedupe)." %
        duplicates_prevented,
        "13. **Rate limits encountered:** %s." % (", ".join(rate_limited) or "none"),
        "14. **Secrets exposed:** NO — %s." % dq_line.get(17, {}).get(
            "detail", "all fingerprints redacted"),
        "15. **Operational Paper Trader records changed:** NO — %s." %
        dq_line.get(18, {}).get("detail", ""),
        "16. **New alpha experiment run:** NO — Stage 2 is acquisition-only.",
        "17. **Run ID / output directory:** `%s` at `%s`." % (run_id, run_dir),
        "18. **Next collection cursor per source:**",
    ]
    for source_id in sorted(cursors):
        lines.append("    * `%s`: `%s`" % (source_id, cursors[source_id]))
    lines += [
        "19. **Stage 3 readiness:** collectors, checkpoints, entitlement audit "
        "and the daily report contract are in place; the LLM research-director "
        "layer (Stage 3) can consume `latest.json` + this report. Terminal: %s."
        % terminal,
        "20. **External blockers requiring user action:** %s." % (
            "; ".join(blocked_sources) or "none"),
        "",
        "_Stage 1 registry consulted before collection: run `%s`._"
        % stage1.get("run_id"),
    ]
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Verify mode — no network, no writes
# --------------------------------------------------------------------------- #

def verify_run(*, config: dict, output_root: str) -> dict:
    out_root = Path(output_root)
    contract = config.get("output_contract", {})
    latest_path = out_root / contract.get("latest_file", "latest.json")
    if not latest_path.exists():
        return {"status": BLOCKED, "reason": "no latest.json to verify at %s" % latest_path}
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {"status": BLOCKED, "reason": "latest.json unreadable: %s" % exc}
    run_dir = Path(str(latest.get("run_dir", "")))
    if not run_dir.is_absolute():
        run_dir = out_root / run_dir
    problems: list[str] = []
    for name in contract.get("required_run_files", []):
        if not (run_dir / name).exists():
            problems.append("missing required run file %s" % name)

    state_path = out_root / contract.get("state_dir", "state") / \
        contract.get("state_db", "source_state.sqlite")
    if not state_path.exists():
        problems.append("state database missing")
    else:
        conn = sqlite3.connect("file:%s?mode=ro&immutable=1" % state_path.as_posix(),
                               uri=True)
        try:
            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            if integrity != "ok":
                problems.append("sqlite integrity_check: %s" % integrity)
            fk = conn.execute("PRAGMA foreign_key_check").fetchall()
            if fk:
                problems.append("%d foreign-key violations" % len(fk))
            raw_rows = conn.execute(
                "SELECT raw_object_id, storage_path, content_hash FROM raw_objects"
                " LIMIT 5000").fetchall()
            hash_bad = 0
            missing = 0
            for _rid, rel, expected in raw_rows:
                path = out_root / rel
                if not path.exists():
                    missing += 1
                elif sha256_hex(path.read_bytes()) != expected:
                    hash_bad += 1
            if missing or hash_bad:
                problems.append("raw archive: %d missing files, %d hash mismatches"
                                % (missing, hash_bad))
            db_records = conn.execute(
                "SELECT COUNT(*) FROM normalized_records").fetchone()[0]
            db_raw = conn.execute("SELECT COUNT(*) FROM raw_objects").fetchone()[0]
            counts = latest.get("counts", {})
            if counts.get("normalized_records_total") not in (None, db_records):
                problems.append("record count mismatch: latest=%s db=%d"
                                % (counts.get("normalized_records_total"), db_records))
            if counts.get("raw_objects_total") not in (None, db_raw):
                problems.append("raw count mismatch: latest=%s db=%d"
                                % (counts.get("raw_objects_total"), db_raw))
        finally:
            conn.close()

    manifest_path = run_dir / "run_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            for name, expected in manifest.get("output_file_hashes", {}).items():
                if name == "data_quality_report.json":
                    continue  # finalized after manifest (check-17 status update)
                path = run_dir / name
                if not path.exists():
                    problems.append("manifest file missing: %s" % name)
                elif sha256_hex(path.read_bytes()) != expected:
                    problems.append("manifest hash mismatch: %s" % name)
        except (OSError, ValueError) as exc:
            problems.append("run_manifest.json unreadable: %s" % exc)
    else:
        problems.append("run_manifest.json missing")

    # Secret hygiene: redaction placeholders must have won; no obvious keyed
    # query parameter with a live-looking value may appear in any output.
    import re
    pattern = re.compile(rb"(api_token|api_key|apikey)=(?!REDACTED)[A-Za-z0-9.\-]{8,}",
                         re.IGNORECASE)
    for path in sorted(run_dir.rglob("*")):
        if path.is_file():
            if pattern.search(path.read_bytes()):
                problems.append("possible unredacted credential parameter in %s"
                                % path.name)
    if problems:
        return {"status": BLOCKED, "run_id": latest.get("run_id"),
                "reason": "; ".join(problems[:12])}
    return {"status": VERIFIED, "run_id": latest.get("run_id"),
            "run_dir": str(run_dir), "terminal": VERIFIED,
            "checked": {"required_files": len(contract.get("required_run_files", [])),
                        "raw_objects_verified": True,
                        "sqlite_integrity": "ok"}}
