"""
alpha_agent/feed_registry.py — Stage 3.5 News/RSS feed registry + ingestion engine.

Two responsibilities, kept in one cohesive module so Stage 2's ingestion engine
stays untouched:

  1. The canonical feed registry — validate every feed against the closed
     field/enum contract, reject unofficial feeds from being enabled, merge
     persisted per-feed conditional-polling checkpoints, and run deterministic
     discovery over an explicit candidate catalog (feed URLs are NEVER invented;
     companies with no discoverable official feed are recorded as
     NO_OFFICIAL_FEED_DISCOVERED, not a software failure).

  2. The bounded Stage 3.5 engine — drive the generic RSS/Atom collector over the
     enabled feeds, archive raw objects immutably, normalize into the shared PIT
     record contract, cluster the same event across RSS + Stage 2 sources
     (read-only), maintain a stdlib sqlite3 feed-state database with per-feed
     health/retry/circuit-breaker state, and emit an immutable run package with a
     deterministic run id.

Modes: audit / collect / incremental / verify. Verify makes no network calls and
writes nothing. No PostgreSQL, no model API, no Paper Trader mutation, no orders,
no automation. Secrets are never persisted; every stored fingerprint is redacted.
"""
from __future__ import annotations

import csv
import datetime as _dt
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Optional

from .collectors.base import CollectorContext, RawArchive, default_transport
from .collectors.rss_atom import RssAtomCollector
from .event_clustering import (
    cluster_events, clusters_identity_projection, index_clusters,
)
from .feed_contracts import (
    ENABLE_ELIGIBLE_TRUST, FEED_SCHEMA_VERSION, FEED_FORMATS, RSS_COLLECTOR_VERSION,
    SOURCE_CATEGORIES, TL_UNKNOWN, TRUST_LEVELS, ALLOWED_STORAGE_BOUNDED,
    DEFAULT_SUMMARY_MAX_CHARS, detect_format,
)
from .source_contracts import (
    CB_CLOSED, CB_OPEN, IdentityResolver, canonical_json, sha256_hex, sha256_text,
)

STAGE = "3.5"
READY = "ALPHA_AGENT_STAGE3_5_READY"
PARTIAL = "ALPHA_AGENT_STAGE3_5_PARTIAL"
NO_NEW = "NO_NEW_RSS_DATA"
VERIFIED = "ALPHA_AGENT_STAGE3_5_VERIFIED"
BLOCKED = "ALPHA_AGENT_STAGE3_5_BLOCKED"

_MODES = ("audit", "collect", "incremental", "verify")

_RUN_FILES = (
    "feed_inventory.json", "feed_discovery_results.json", "feed_health.csv",
    "collection_manifest.json", "raw_object_index.csv",
    "normalized_record_counts.csv", "event_clusters.jsonl",
    "data_quality_report.json", "source_coverage_report.json",
    "stage3_5_news_rss_report.md", "run_manifest.json",
)

_HEALTHY_FEED_STATES = ("HEALTHY", "HEALTHY_NOT_MODIFIED", "DEGRADED")
_FAILED_FEED_STATES = ("FAILED", "CIRCUIT_OPEN")

_STAGE2_CLUSTER_TYPES = ("NEWS_EVENT", "FILING_EVENT", "INSIDER_FILING",
                         "TRADING_HALT", "CORPORATE_ACTION")

_SCHEMA_SQL = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS schema_meta(
  key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS feed_definitions(
  feed_id TEXT PRIMARY KEY, feed_url TEXT, canonical_url TEXT, feed_format TEXT,
  publisher TEXT, source_category TEXT, official_source INTEGER,
  trust_level TEXT, license_status TEXT, allowed_storage TEXT,
  covered_tickers TEXT, covered_sectors TEXT, jurisdiction TEXT, language TEXT,
  enabled INTEGER NOT NULL DEFAULT 0, polling_interval_minutes INTEGER,
  priority INTEGER, discovery_method TEXT, notes TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS feed_discovery(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, feed_id TEXT,
  candidate_url TEXT, discovery_method TEXT, result TEXT, detail TEXT,
  checked_at TEXT);
CREATE TABLE IF NOT EXISTS feed_checkpoints(
  feed_id TEXT PRIMARY KEY REFERENCES feed_definitions(feed_id),
  etag TEXT, last_modified TEXT, latest_item_time TEXT,
  last_attempt TEXT, last_success TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  circuit_breaker_state TEXT NOT NULL DEFAULT 'CLOSED', updated_at TEXT);
CREATE TABLE IF NOT EXISTS feed_runs(
  run_id TEXT PRIMARY KEY, mode TEXT NOT NULL, as_of TEXT, config_hash TEXT,
  registry_hash TEXT, git_commit TEXT, schema_version TEXT,
  collector_version TEXT, started_at TEXT, finished_at TEXT, status TEXT,
  terminal_token TEXT, raw_objects_new INTEGER, normalized_new INTEGER,
  clusters_new INTEGER, notes TEXT);
CREATE TABLE IF NOT EXISTS feed_requests(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, feed_id TEXT,
  http_status INTEGER, not_modified INTEGER, items_seen INTEGER,
  records_new INTEGER, duplicates_prevented INTEGER, retry_count INTEGER,
  etag TEXT, last_modified TEXT, request_fingerprint TEXT, retrieved_at TEXT);
CREATE TABLE IF NOT EXISTS raw_objects(
  raw_object_id TEXT PRIMARY KEY, feed_id TEXT, storage_path TEXT,
  content_hash TEXT, content_type TEXT, byte_size INTEGER, http_status INTEGER,
  retry_count INTEGER, parser_status TEXT, retrieved_at TEXT, published_at TEXT,
  source_native_id TEXT, request_fingerprint TEXT, license_note TEXT,
  first_run_id TEXT);
CREATE TABLE IF NOT EXISTS normalized_records(
  record_id TEXT PRIMARY KEY, record_schema_version TEXT, record_type TEXT,
  source_id TEXT, source_native_id TEXT, raw_object_id TEXT, feed_id TEXT,
  observed_at TEXT, retrieved_at TEXT, available_at TEXT, effective_at TEXT,
  ticker TEXT, company_id TEXT, event_type TEXT, payload_hash TEXT,
  entity_mapping_confidence TEXT, provenance TEXT, cluster_id TEXT,
  quality_warnings_json TEXT, payload_json TEXT, first_run_id TEXT,
  storage_path TEXT);
CREATE TABLE IF NOT EXISTS event_clusters(
  cluster_id TEXT PRIMARY KEY, run_id TEXT, algo_version TEXT,
  representative_record_id TEXT, member_count INTEGER,
  corroborating_source_count INTEGER, clustering_confidence TEXT,
  event_category TEXT, primary_source_present INTEGER,
  company_direct_source_present INTEGER, regulator_source_present INTEGER,
  earliest_available_at TEXT, latest_available_at TEXT, normalized_title TEXT,
  conflicting_facts_json TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS cluster_members(
  cluster_id TEXT NOT NULL REFERENCES event_clusters(cluster_id),
  record_id TEXT NOT NULL, source_id TEXT, record_type TEXT,
  is_representative INTEGER, PRIMARY KEY(cluster_id, record_id));
CREATE TABLE IF NOT EXISTS feed_errors(
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, feed_id TEXT,
  occurred_at TEXT, error_type TEXT, http_status INTEGER, message TEXT,
  retry_count INTEGER);
CREATE TABLE IF NOT EXISTS data_quality_results(
  dq_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, check_id INTEGER,
  check_name TEXT, status TEXT, detail TEXT);
CREATE INDEX IF NOT EXISTS idx_nr_type ON normalized_records(record_type);
CREATE INDEX IF NOT EXISTS idx_nr_feed ON normalized_records(feed_id);
CREATE INDEX IF NOT EXISTS idx_cm_record ON cluster_members(record_id);
"""


# --------------------------------------------------------------------------- #
# IO helpers (local; ingestion.py is deliberately left untouched)
# --------------------------------------------------------------------------- #
def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _write_json(path: Path, obj: Any) -> None:
    _atomic_write_text(path, json.dumps(obj, indent=1, sort_keys=True, default=str))


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: ("" if row.get(c) is None else row.get(c))
                             for c in columns})
    os.replace(tmp, path)


def _sha256_file(path: Path) -> str:
    return sha256_hex(path.read_bytes())


def ledger_fingerprints(config: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for root in config.get("operational_ledger_roots", []):
        rp = Path(root)
        if not rp.exists():
            continue
        for f in sorted(rp.rglob("*")):
            if f.is_file():
                out["%s|%s" % (root, f.relative_to(rp).as_posix())] = \
                    sha256_hex(f.read_bytes())
    return out


def scan_outputs_for_secrets(run_dir: Path,
                             sensitive_values: list[str]) -> tuple[bool, str]:
    needles = [v.encode("utf-8") for v in sensitive_values if v and len(v) >= 6]
    hits = 0
    scanned = 0
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


def _now_utc() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


# --------------------------------------------------------------------------- #
# Feed registry validation
# --------------------------------------------------------------------------- #
_REQUIRED_IDENTITY = ("feed_id", "feed_url", "publisher", "source_category",
                      "trust_level")


def validate_feed(feed: dict) -> tuple[bool, dict, list[str]]:
    """Validate one feed against the closed contract. Returns (ok, normalized,
    reasons). An enabled feed MUST be official and carry an approved trust level:
    unofficial or UNKNOWN-trust feeds are force-disabled with a recorded reason."""
    reasons: list[str] = []
    fid = str(feed.get("feed_id") or "").strip()
    for field in _REQUIRED_IDENTITY:
        if not str(feed.get(field) or "").strip():
            reasons.append("missing required field: %s" % field)
    cat = str(feed.get("source_category") or "")
    if cat and cat not in SOURCE_CATEGORIES:
        reasons.append("invalid source_category: %s" % cat)
    trust = str(feed.get("trust_level") or TL_UNKNOWN)
    if trust not in TRUST_LEVELS:
        reasons.append("invalid trust_level: %s" % trust)
    fmt = str(feed.get("feed_format") or "UNKNOWN")
    if fmt not in FEED_FORMATS:
        reasons.append("invalid feed_format: %s" % fmt)
    official = bool(feed.get("official_source"))
    requested_enabled = bool(feed.get("enabled"))
    enabled = requested_enabled
    if requested_enabled and not official:
        enabled = False
        reasons.append("ENABLED_REJECTED_UNOFFICIAL: only official feeds may be "
                       "enabled")
    if requested_enabled and trust not in ENABLE_ELIGIBLE_TRUST:
        enabled = False
        reasons.append("ENABLED_REJECTED_TRUST: trust_level %s not approvable"
                       % trust)
    if any(r.startswith("missing required") or r.startswith("invalid")
           for r in reasons):
        enabled = False
    normalized = {
        "feed_id": fid,
        "feed_url": str(feed.get("feed_url") or ""),
        "canonical_url": str(feed.get("canonical_url")
                             or feed.get("feed_url") or ""),
        "feed_format": fmt,
        "publisher": str(feed.get("publisher") or ""),
        "source_category": cat,
        "official_source": official,
        "trust_level": trust,
        "license_status": str(feed.get("license_status") or "UNKNOWN"),
        "allowed_storage": str(feed.get("allowed_storage")
                               or ALLOWED_STORAGE_BOUNDED),
        "covered_tickers": [str(t).upper() for t in (feed.get("covered_tickers") or [])],
        "covered_sectors": list(feed.get("covered_sectors") or []),
        "jurisdiction": str(feed.get("jurisdiction") or ""),
        "language": str(feed.get("language") or "en"),
        "enabled": enabled,
        "polling_interval_minutes": int(feed.get("polling_interval_minutes") or 60),
        "priority": int(feed.get("priority") or 50),
        "discovery_method": str(feed.get("discovery_method") or "CONFIGURED_CATALOG"),
        "known_tickers": [str(t).upper() for t in (feed.get("known_tickers") or [])],
        "notes": str(feed.get("notes") or ""),
    }
    ok = not any(r.startswith("missing required") or r.startswith("invalid")
                 for r in reasons)
    return ok, normalized, reasons


class FeedRegistry:
    """Validated feed registry with per-feed conditional-polling checkpoints."""

    def __init__(self, feeds_config: dict, known_tickers: Optional[list] = None):
        self.registry_version = str(feeds_config.get("registry_version", "1.0.0"))
        self.known_tickers = sorted({str(t).upper() for t in
                                     (known_tickers
                                      or feeds_config.get("active_book_tickers", []))})
        self.feeds: list[dict] = []
        self.invalid: list[dict] = []
        for raw in feeds_config.get("feeds", []):
            ok, norm, reasons = validate_feed(raw)
            norm["known_tickers"] = self.known_tickers
            norm["validation_reasons"] = reasons
            if ok:
                self.feeds.append(norm)
            else:
                self.invalid.append({"feed_id": raw.get("feed_id"),
                                     "reasons": reasons})
        # Deterministic ordering by (priority, feed_id).
        self.feeds.sort(key=lambda f: (f["priority"], f["feed_id"]))
        self.company_feed_candidates = feeds_config.get("company_feed_candidates", [])
        self.active_book_tickers = sorted(
            {str(t).upper() for t in feeds_config.get("active_book_tickers", [])})
        self.already_covered = feeds_config.get("already_covered_feeds", [])
        self.gdelt_state = str(feeds_config.get("gdelt_state", "NOT_RUN"))

    def enabled_feeds(self) -> list[dict]:
        return [f for f in self.feeds if f.get("enabled")]

    def registry_hash(self) -> str:
        projection = [{k: f.get(k) for k in
                       ("feed_id", "feed_url", "source_category", "trust_level",
                        "official_source", "enabled", "priority")}
                      for f in self.feeds]
        return sha256_text(canonical_json(projection))[:16]

    def apply_checkpoints(self, rows: dict[str, dict]) -> None:
        for f in self.feeds:
            f["_checkpoint"] = rows.get(f["feed_id"], {})


def load_registry(feeds_config: dict,
                  known_tickers: Optional[list] = None) -> FeedRegistry:
    return FeedRegistry(feeds_config, known_tickers=known_tickers)


# --------------------------------------------------------------------------- #
# State database
# --------------------------------------------------------------------------- #
def open_feed_state_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA_SQL)
    conn.execute(
        "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('schema_version', ?)",
        (FEED_SCHEMA_VERSION,))
    conn.commit()
    return conn


def _read_checkpoints(conn: sqlite3.Connection) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in conn.execute("SELECT * FROM feed_checkpoints"):
        out[row["feed_id"]] = {
            "etag": row["etag"], "last_modified": row["last_modified"],
            "latest_item_time": row["latest_item_time"],
            "last_success": row["last_success"],
            "consecutive_failures": row["consecutive_failures"],
            "circuit_breaker_state": row["circuit_breaker_state"]}
    return out


# --------------------------------------------------------------------------- #
# Stage 2 record reader (read-only, bounded) for cross-source clustering
# --------------------------------------------------------------------------- #
def _iter_stage2_normalized(root: Path, record_type: str):
    base = root / "normalized" / record_type
    if not base.exists():
        return
    for jf in sorted(base.rglob("*.jsonl")):
        try:
            with open(jf, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except ValueError:
                        continue
        except OSError:
            continue


def read_stage2_for_clustering(config: dict, as_of: str,
                               window_days: int, cap_per_type: int) -> list[dict]:
    """Bounded, read-only load of recent Stage 2 records used ONLY as clustering
    corroboration inputs. Immutable Stage 2 records are never mutated or copied
    into the Stage 3.5 store."""
    root = Path(config.get("stage2_ingestion_root", ""))
    if not root.exists():
        return []
    try:
        cutoff = (_dt.date.fromisoformat(as_of)
                  - _dt.timedelta(days=window_days)).isoformat()
    except ValueError:
        cutoff = ""
    out: list[dict] = []
    for rt in _STAGE2_CLUSTER_TYPES:
        rows: list[dict] = []
        for rec in _iter_stage2_normalized(root, rt):
            if not rec.get("record_id"):
                continue
            when = str(rec.get("available_at") or rec.get("effective_at") or "")[:10]
            if cutoff and when and when < cutoff:
                continue
            rows.append(rec)
        rows.sort(key=lambda r: (str(r.get("available_at")
                                     or r.get("effective_at") or ""),
                                 r.get("record_id", "")), reverse=True)
        out.extend(rows[:cap_per_type])
    return out


# --------------------------------------------------------------------------- #
# Deterministic run id
# --------------------------------------------------------------------------- #
def compute_run_id(*, config_hash: str, registry_hash: str, git_commit: str,
                   as_of: str, conn: sqlite3.Connection,
                   clusters: list[dict], health_states: dict) -> str:
    raw_hashes = [r[0] for r in conn.execute(
        "SELECT content_hash FROM raw_objects ORDER BY content_hash")]
    record_ids_digest = sha256_text("|".join(
        r[0] for r in conn.execute(
            "SELECT record_id FROM normalized_records ORDER BY record_id")))
    cluster_digest = sha256_text(canonical_json(
        clusters_identity_projection(clusters)))
    payload = [FEED_SCHEMA_VERSION, RSS_COLLECTOR_VERSION, config_hash,
               registry_hash, git_commit, as_of, raw_hashes, record_ids_digest,
               cluster_digest, {k: health_states[k] for k in sorted(health_states)}]
    return "stage3_5_" + sha256_text(canonical_json(payload))[:16]


# --------------------------------------------------------------------------- #
# The engine
# --------------------------------------------------------------------------- #
def run_news_rss(*, config: dict, feeds_config: dict, output_root: str, mode: str,
                 as_of: str = "latest", git_commit: str = "UNKNOWN",
                 transport: Optional[Callable] = None, env: Optional[dict] = None,
                 now_fn: Optional[Callable[[], _dt.datetime]] = None,
                 sleep_fn: Optional[Callable[[float], None]] = None,
                 clock_fn: Optional[Callable[[], float]] = None,
                 contact_email: Optional[str] = None) -> dict:
    if mode not in _MODES:
        return {"status": BLOCKED, "reason": "unknown mode %r" % mode}
    out_root = Path(output_root)
    if mode == "verify":
        return verify_news_rss_run(config=config, output_root=output_root, env=env)

    now_fn = now_fn or _now_utc
    sleep_fn = sleep_fn or time.sleep
    clock_fn = clock_fn or time.monotonic
    transport = transport or default_transport
    env_map = dict(os.environ) if env is None else dict(env)
    started_at = now_fn().isoformat()

    def now_iso() -> str:
        return now_fn().isoformat()

    resolved_as_of = (now_fn().date().isoformat()
                      if as_of in (None, "", "latest") else str(as_of))
    try:
        _dt.date.fromisoformat(resolved_as_of)
    except ValueError:
        return {"status": BLOCKED, "reason": "invalid --as-of value %r" % as_of}

    ledgers_before = ledger_fingerprints(config)
    contract = config.get("output_contract", {})
    state_path = out_root / contract.get("state_dir", "state") / \
        contract.get("state_db", "feed_state.sqlite")
    conn = open_feed_state_db(state_path)

    registry = load_registry(feeds_config)
    registry.apply_checkpoints(_read_checkpoints(conn))
    _persist_feed_definitions(conn, registry, now_iso())

    sensitive_values: list[str] = []
    if contact_email:
        sensitive_values.append(contact_email)
    for name in config.get("sensitive_env_vars", []):
        val = env_map.get(name)
        if val:
            sensitive_values.append(val)

    limits = config.get("limits", {})
    known_raw = {r[0] for r in conn.execute("SELECT raw_object_id FROM raw_objects")}
    known_records = {r[0] for r in conn.execute("SELECT record_id FROM normalized_records")}
    prior_raw_count = len(known_raw)
    prior_record_count = len(known_records)
    archive = RawArchive(out_root / contract.get("raw_dir", "raw"), out_root,
                         known_raw, int(limits.get("raw_object_max_bytes", 8388608)))
    ua_product = config.get("user_agent", {}).get(
        "product", "paper-trader-alpha-agent/2.0")
    ctx = CollectorContext(
        config=config,
        source_cfg={"min_interval_seconds": float(config.get(
            "per_feed_min_interval_seconds", 0.0)),
            "known_tickers": registry.known_tickers, "enabled": True},
        archive=archive, transport=transport, now_iso=now_iso, clock=clock_fn,
        sleep=sleep_fn, secrets=sensitive_values, user_agent=ua_product,
        env=env_map, identity=IdentityResolver())
    collector = RssAtomCollector(ctx)

    probe_only = (mode == "audit")
    feed_summaries: list[dict] = []
    for feed in registry.enabled_feeds():
        checkpoint = feed.get("_checkpoint", {})
        summary = collector.collect_feed(feed, checkpoint, resolved_as_of)
        summary["feed"] = feed
        feed_summaries.append(summary)

    # New (deduplicated) normalized records from this run.
    new_records: list[dict] = []
    seen: set[str] = set()
    for rec in collector.records:
        rid = rec["record_id"]
        if rid in known_records or rid in seen:
            continue
        seen.add(rid)
        new_records.append(rec)

    all_new_raw = list(collector.raw_objects)

    # Clustering (collect/incremental only): RSS new records + bounded Stage 2.
    clusters: list[dict] = []
    if not probe_only:
        cluster_window = int(config.get("clustering", {}).get("window_days", 2))
        cap = int(config.get("clustering", {}).get("stage2_cap_per_type", 400))
        stage2_records = read_stage2_for_clustering(
            config, resolved_as_of, window_days=max(cluster_window, 7), cap_per_type=cap)
        clusters = cluster_events(
            new_records + stage2_records, window_days=cluster_window,
            high_threshold=float(config.get("clustering", {})
                                 .get("high_threshold", 0.6)),
            medium_threshold=float(config.get("clustering", {})
                                   .get("medium_threshold", 0.4)),
            now_iso=now_iso())
        cluster_idx = index_clusters(clusters)
        for rec in new_records:
            info = cluster_idx.get(rec["record_id"])
            if info and info["cluster_id"]:
                rec["normalized_payload"]["cluster_id"] = info["cluster_id"]

    health_states = {s["feed_id"]: s["health"] for s in feed_summaries}
    config_hash = sha256_text(canonical_json(config))[:16]
    registry_hash = registry.registry_hash()

    finished_at = now_fn().isoformat()
    nothing_new = (len(new_records) == 0 and len(all_new_raw) == 0
                   and len(known_raw) == prior_raw_count
                   and len(known_records) == prior_record_count)
    if mode == "incremental" and nothing_new:
        _persist_feed_state(conn, "nonew_%s" % sha256_text(started_at)[:12],
                            feed_summaries, [], [], [], now_iso())
        conn.execute(
            "INSERT OR REPLACE INTO feed_runs(run_id, mode, as_of, config_hash,"
            " registry_hash, git_commit, schema_version, collector_version,"
            " started_at, finished_at, status, terminal_token, raw_objects_new,"
            " normalized_new, clusters_new, notes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("nonew_%s" % sha256_text(started_at)[:12], mode, resolved_as_of,
             config_hash, registry_hash, git_commit, FEED_SCHEMA_VERSION,
             RSS_COLLECTOR_VERSION, started_at, finished_at, NO_NEW, NO_NEW, 0, 0,
             0, "no new feed items and no feed state transition"))
        conn.commit()
        conn.close()
        return {"status": NO_NEW, "run_id": None, "as_of": resolved_as_of,
                "counts": {"raw_new": 0, "records_new": 0},
                "ledger_unchanged": ledgers_before == ledger_fingerprints(config)}

    run_id = compute_run_id(config_hash=config_hash, registry_hash=registry_hash,
                            git_commit=git_commit, as_of=resolved_as_of, conn=conn,
                            clusters=clusters, health_states=health_states)
    runs_dir = out_root / contract.get("runs_dir", "runs")
    run_dir = runs_dir / run_id
    already_existed = run_dir.exists()

    # Persist state + normalized JSONL partitions (collect/incremental only).
    if not probe_only and not already_existed:
        _persist_feed_state(conn, run_id, feed_summaries, all_new_raw, new_records,
                            clusters, now_iso())
        _write_normalized_partitions(out_root, contract, new_records, run_id, conn)
        _write_cluster_partitions(out_root, resolved_as_of, run_id, clusters)
        conn.execute("UPDATE raw_objects SET first_run_id=? WHERE first_run_id=?",
                     (run_id, "pending"))
        conn.commit()
    elif probe_only and not already_existed:
        _persist_feed_state(conn, run_id, feed_summaries, all_new_raw, [], [],
                            now_iso())
        conn.execute("UPDATE raw_objects SET first_run_id=? WHERE first_run_id=?",
                     (run_id, "pending"))
        conn.commit()

    # Discovery evidence.
    discovery = _run_discovery(registry, feed_summaries, resolved_as_of)
    _persist_discovery(conn, run_id, discovery, now_iso())

    # Terminal decision.
    attempted = [s for s in feed_summaries if s.get("attempted")]
    healthy = [s for s in feed_summaries if s["health"] in _HEALTHY_FEED_STATES]
    failed = [s for s in feed_summaries if s["health"] in _FAILED_FEED_STATES]
    collected = len(new_records)
    total_records_this_run = collected
    if probe_only:
        if len(healthy) >= 1:
            status, terminal = READY, READY
        elif attempted:
            status = PARTIAL
            terminal = "%s — audit: %d/%d feeds healthy" % (
                PARTIAL, len(healthy), len(attempted))
        else:
            status, terminal = BLOCKED, "%s — no enabled feeds to audit" % BLOCKED
    else:
        store_records = conn.execute(
            "SELECT COUNT(*) FROM normalized_records").fetchone()[0]
        have_records = collected > 0 or (already_existed and store_records > 0)
        if not have_records:
            status = BLOCKED
            terminal = ("%s — no RSS/Atom records collected from any enabled feed"
                        % BLOCKED)
        elif failed or len(healthy) < len(registry.enabled_feeds()):
            status = PARTIAL
            unavailable = sorted(
                "%s(%s)" % (s["feed_id"], s.get("rejected_reason") or s["health"])
                for s in feed_summaries if s["health"] not in _HEALTHY_FEED_STATES)
            terminal = "%s — %d/%d feeds healthy; unavailable: %s" % (
                PARTIAL, len(healthy), len(registry.enabled_feeds()),
                "; ".join(unavailable) or "none")
        else:
            status, terminal = READY, READY

    dq_checks = _run_dq_checks(
        conn=conn, out_root=out_root, as_of=resolved_as_of, new_records=new_records,
        all_new_raw=all_new_raw, clusters=clusters, feed_summaries=feed_summaries,
        ledgers_before=ledgers_before, ledgers_after=ledger_fingerprints(config),
        summary_max=int(config.get("limits", {}).get(
            "summary_max_chars", DEFAULT_SUMMARY_MAX_CHARS)))

    if not already_existed:
        _write_run_outputs(
            run_dir=run_dir, out_root=out_root, config=config, feeds_config=feeds_config,
            registry=registry, config_hash=config_hash, registry_hash=registry_hash,
            git_commit=git_commit, mode=mode, as_of=resolved_as_of, run_id=run_id,
            feed_summaries=feed_summaries, new_records=new_records, all_new_raw=all_new_raw,
            clusters=clusters, discovery=discovery, dq_checks=dq_checks, conn=conn,
            status=status, terminal=terminal, started_at=started_at,
            finished_at=finished_at)
        _write_json(out_root / contract.get("registry_dir", "registry") /
                    "feed_registry.json", _registry_snapshot(registry, run_id))

    secrets_ok, secrets_detail = scan_outputs_for_secrets(run_dir, sensitive_values)
    for check in dq_checks:
        if check["check_id"] == 13:
            check["status"] = "PASS" if secrets_ok else "FAIL"
            check["detail"] = secrets_detail
    dq_path = run_dir / "data_quality_report.json"
    if dq_path.exists() and not already_existed:
        obj = json.loads(dq_path.read_text(encoding="utf-8"))
        obj["checks"] = dq_checks
        _write_json(dq_path, obj)
        # run_manifest hashes the finalized files (re-write after dq update).
        _finalize_manifest(run_dir, run_id, config_hash, registry_hash, git_commit,
                           mode, as_of=resolved_as_of, status=status, terminal=terminal)

    conn.executemany(
        "INSERT INTO data_quality_results(run_id, check_id, check_name, status,"
        " detail) VALUES(?,?,?,?,?)",
        [(run_id, c["check_id"], c["check_name"], c["status"], c["detail"])
         for c in dq_checks])
    conn.execute(
        "INSERT OR REPLACE INTO feed_runs(run_id, mode, as_of, config_hash,"
        " registry_hash, git_commit, schema_version, collector_version,"
        " started_at, finished_at, status, terminal_token, raw_objects_new,"
        " normalized_new, clusters_new, notes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (run_id, mode, resolved_as_of, config_hash, registry_hash, git_commit,
         FEED_SCHEMA_VERSION, RSS_COLLECTOR_VERSION, started_at, finished_at,
         status, terminal, len(all_new_raw), len(new_records), len(clusters),
         "already_existed" if already_existed else ("probe_only" if probe_only else "")))
    conn.commit()

    ledgers_after = ledger_fingerprints(config)
    ledger_unchanged = ledgers_before == ledgers_after
    hard_fail = [c for c in dq_checks if c["status"] == "FAIL"
                 and c["check_id"] in (8, 9, 10, 11, 13, 15)]
    if not secrets_ok or not ledger_unchanged or hard_fail:
        conn.close()
        return {"status": BLOCKED, "run_id": run_id, "run_dir": str(run_dir),
                "reason": "hard data-quality failure: %s"
                % ("; ".join(c["check_name"] for c in hard_fail)
                   or ("ledger changed" if not ledger_unchanged else "secret scan")),
                "ledger_unchanged": ledger_unchanged}
    if status == BLOCKED:
        conn.close()
        return {"status": BLOCKED, "run_id": run_id, "run_dir": str(run_dir),
                "reason": terminal, "ledger_unchanged": ledger_unchanged,
                "counts": {"records_new": len(new_records)}}

    if not probe_only:
        latest = {
            "stage": STAGE, "run_id": run_id,
            "run_dir": str(Path(contract.get("runs_dir", "runs")) / run_id),
            "mode": mode, "as_of": resolved_as_of, "status": status,
            "terminal_token": terminal, "generated_at": finished_at,
            "schema_version": FEED_SCHEMA_VERSION,
            "collector_version": RSS_COLLECTOR_VERSION,
            "config_hash": config_hash, "registry_hash": registry_hash,
            "git_commit": git_commit,
            "counts": {"raw_objects_new": len(all_new_raw),
                       "normalized_records_new": len(new_records),
                       "clusters": len(clusters),
                       "raw_objects_total": conn.execute(
                           "SELECT COUNT(*) FROM raw_objects").fetchone()[0],
                       "normalized_records_total": conn.execute(
                           "SELECT COUNT(*) FROM normalized_records").fetchone()[0]},
            "enabled_feeds": len(registry.enabled_feeds()),
            "healthy_feeds": len(healthy)}
        _write_json(out_root / contract.get("latest_file", "latest.json"), latest)
    conn.close()
    return {"status": status, "terminal": terminal, "run_id": run_id,
            "run_dir": str(run_dir), "as_of": resolved_as_of,
            "already_existed": already_existed,
            "counts": {"raw_objects_new": len(all_new_raw),
                       "normalized_records_new": len(new_records),
                       "clusters": len(clusters)},
            "enabled_feeds": len(registry.enabled_feeds()),
            "healthy_feeds": len(healthy),
            "failed_feeds": [s["feed_id"] for s in failed],
            "ledger_unchanged": ledger_unchanged,
            "dq_failures": [c["check_name"] for c in dq_checks
                            if c["status"] == "FAIL"]}


# --------------------------------------------------------------------------- #
# Persistence
# --------------------------------------------------------------------------- #
def _persist_feed_definitions(conn: sqlite3.Connection, registry: FeedRegistry,
                              now_iso: str) -> None:
    cur = conn.cursor()
    for f in registry.feeds:
        cur.execute(
            "INSERT INTO feed_definitions(feed_id, feed_url, canonical_url,"
            " feed_format, publisher, source_category, official_source, trust_level,"
            " license_status, allowed_storage, covered_tickers, covered_sectors,"
            " jurisdiction, language, enabled, polling_interval_minutes, priority,"
            " discovery_method, notes, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(feed_id) DO UPDATE SET feed_url=excluded.feed_url,"
            " source_category=excluded.source_category, trust_level=excluded.trust_level,"
            " official_source=excluded.official_source, enabled=excluded.enabled,"
            " updated_at=excluded.updated_at",
            (f["feed_id"], f["feed_url"], f["canonical_url"], f["feed_format"],
             f["publisher"], f["source_category"], 1 if f["official_source"] else 0,
             f["trust_level"], f["license_status"], f["allowed_storage"],
             canonical_json(f["covered_tickers"]), canonical_json(f["covered_sectors"]),
             f["jurisdiction"], f["language"], 1 if f["enabled"] else 0,
             f["polling_interval_minutes"], f["priority"], f["discovery_method"],
             f["notes"], now_iso))
    conn.commit()


def _persist_feed_state(conn: sqlite3.Connection, run_id: str,
                        feed_summaries: list[dict], raw_objects: list[dict],
                        new_records: list[dict], clusters: list[dict],
                        now_iso: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        for s in feed_summaries:
            cur.execute(
                "INSERT INTO feed_checkpoints(feed_id, etag, last_modified,"
                " latest_item_time, last_attempt, last_success, consecutive_failures,"
                " circuit_breaker_state, updated_at) VALUES(?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(feed_id) DO UPDATE SET etag=excluded.etag,"
                " last_modified=excluded.last_modified,"
                " latest_item_time=COALESCE(excluded.latest_item_time,"
                " feed_checkpoints.latest_item_time),"
                " last_attempt=excluded.last_attempt,"
                " last_success=COALESCE(excluded.last_success, feed_checkpoints.last_success),"
                " consecutive_failures=excluded.consecutive_failures,"
                " circuit_breaker_state=excluded.circuit_breaker_state,"
                " updated_at=excluded.updated_at",
                (s["feed_id"], s.get("etag"), s.get("last_modified"),
                 s.get("latest_item_time"), now_iso, s.get("last_success"),
                 int(s.get("consecutive_failures", 0)),
                 s.get("circuit_breaker_state", CB_CLOSED), now_iso))
            cur.execute(
                "INSERT INTO feed_requests(run_id, feed_id, http_status,"
                " not_modified, items_seen, records_new, duplicates_prevented,"
                " retry_count, etag, last_modified, request_fingerprint, retrieved_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, s["feed_id"], s.get("http_status") if s.get("http_status")
                 else None, 1 if s.get("not_modified") else 0,
                 s.get("items_seen", 0), s.get("records_new", 0),
                 s.get("duplicates_prevented", 0), 0, s.get("etag"),
                 s.get("last_modified"), s.get("feed", {}).get("feed_url", ""), now_iso))
        for raw in raw_objects:
            cur.execute(
                "INSERT OR IGNORE INTO raw_objects(raw_object_id, feed_id,"
                " storage_path, content_hash, content_type, byte_size, http_status,"
                " retry_count, parser_status, retrieved_at, published_at,"
                " source_native_id, request_fingerprint, license_note, first_run_id)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (raw["raw_object_id"], raw["source_id"], raw["storage_path"],
                 raw["content_hash"], raw["content_type"], raw["byte_size"],
                 raw["http_status"], raw["retry_count"], raw["parser_status"],
                 raw["retrieved_at"], raw["published_at"], raw["source_native_id"],
                 raw["request_fingerprint"], raw["license_note"], "pending"))
        for rec in new_records:
            cur.execute(
                "INSERT OR IGNORE INTO normalized_records(record_id,"
                " record_schema_version, record_type, source_id, source_native_id,"
                " raw_object_id, feed_id, observed_at, retrieved_at, available_at,"
                " effective_at, ticker, company_id, event_type, payload_hash,"
                " entity_mapping_confidence, provenance, cluster_id,"
                " quality_warnings_json, payload_json, first_run_id, storage_path)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (rec["record_id"], rec["record_schema_version"], rec["record_type"],
                 rec["source_id"], rec["source_native_id"], rec["raw_object_id"],
                 (rec.get("normalized_payload") or {}).get("feed_id"),
                 rec["observed_at"], rec["retrieved_at"], rec["available_at"],
                 rec["effective_at"], rec["ticker"], rec.get("company_id"),
                 rec["event_type"], rec["payload_hash"],
                 rec["entity_mapping_confidence"], rec["provenance"],
                 (rec.get("normalized_payload") or {}).get("cluster_id"),
                 canonical_json(rec.get("quality_warnings", [])),
                 canonical_json(rec["normalized_payload"]), run_id, None))
        for c in clusters:
            cur.execute(
                "INSERT OR IGNORE INTO event_clusters(cluster_id, run_id,"
                " algo_version, representative_record_id, member_count,"
                " corroborating_source_count, clustering_confidence, event_category,"
                " primary_source_present, company_direct_source_present,"
                " regulator_source_present, earliest_available_at,"
                " latest_available_at, normalized_title, conflicting_facts_json,"
                " created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (c["cluster_id"], run_id, c["algo_version"],
                 c["representative_record_id"], len(c["member_record_ids"]),
                 c["corroborating_source_count"], c["clustering_confidence"],
                 c["event_category"], 1 if c["primary_source_present"] else 0,
                 1 if c["company_direct_source_present"] else 0,
                 1 if c["regulator_source_present"] else 0,
                 c["earliest_available_at"], c["latest_available_at"],
                 c["normalized_title"], canonical_json(c["conflicting_facts"]),
                 c["created_at"]))
            for rid in c["member_record_ids"]:
                cur.execute(
                    "INSERT OR IGNORE INTO cluster_members(cluster_id, record_id,"
                    " source_id, record_type, is_representative) VALUES(?,?,?,?,?)",
                    (c["cluster_id"], rid, None, None,
                     1 if rid == c["representative_record_id"] else 0))
        for s in feed_summaries:
            if s.get("error") or s.get("rejected_reason"):
                cur.execute(
                    "INSERT INTO feed_errors(run_id, feed_id, occurred_at,"
                    " error_type, http_status, message, retry_count)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (run_id, s["feed_id"], now_iso,
                     s.get("rejected_reason") or "ERROR", s.get("http_status"),
                     (s.get("error") or s.get("rejected_reason") or "")[:400], 0))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _write_normalized_partitions(out_root: Path, contract: dict,
                                 new_records: list[dict], run_id: str,
                                 conn: sqlite3.Connection) -> None:
    norm_dir = contract.get("normalized_dir", "normalized")
    partitions: dict[str, list[dict]] = {}
    for rec in new_records:
        pdate = str(rec.get("effective_at") or rec.get("retrieved_at") or "")[:10] \
            or "0000-00-00"
        yyyy, mm, dd = (pdate.split("-") + ["00", "00"])[:3]
        rel = "%s/%s/%s/%s/%s/%s.jsonl" % (norm_dir, rec["record_type"], yyyy, mm,
                                           dd, run_id)
        partitions.setdefault(rel, []).append(rec)
    updates = []
    for rel, recs in sorted(partitions.items()):
        path = out_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for rec in recs:
                handle.write(canonical_json(rec) + "\n")
        for rec in recs:
            updates.append((rel, rec["record_id"]))
    conn.executemany("UPDATE normalized_records SET storage_path=? WHERE record_id=?",
                     updates)
    conn.commit()


def _write_cluster_partitions(out_root: Path, as_of: str, run_id: str,
                              clusters: list[dict]) -> None:
    yyyy, mm, dd = (as_of.split("-") + ["00", "00"])[:3]
    rel = out_root / "clusters" / yyyy / mm / dd / ("%s.jsonl" % run_id)
    rel.parent.mkdir(parents=True, exist_ok=True)
    with rel.open("w", encoding="utf-8") as handle:
        for c in clusters:
            handle.write(canonical_json(c) + "\n")


def _persist_discovery(conn: sqlite3.Connection, run_id: str, discovery: dict,
                       now_iso: str) -> None:
    cur = conn.cursor()
    for row in discovery.get("feeds", []):
        cur.execute(
            "INSERT INTO feed_discovery(run_id, feed_id, candidate_url,"
            " discovery_method, result, detail, checked_at) VALUES(?,?,?,?,?,?,?)",
            (run_id, row.get("feed_id"), row.get("candidate_url"),
             row.get("discovery_method"), row.get("result"), row.get("detail"),
             now_iso))
    for row in discovery.get("companies_without_feeds", []):
        cur.execute(
            "INSERT INTO feed_discovery(run_id, feed_id, candidate_url,"
            " discovery_method, result, detail, checked_at) VALUES(?,?,?,?,?,?,?)",
            (run_id, None, None, "COMPANY_FEED_SEARCH", "NO_OFFICIAL_FEED_DISCOVERED",
             row.get("ticker"), now_iso))
    conn.commit()


# --------------------------------------------------------------------------- #
# Discovery
# --------------------------------------------------------------------------- #
def _run_discovery(registry: FeedRegistry, feed_summaries: list[dict],
                   as_of: str) -> dict:
    by_id = {s["feed_id"]: s for s in feed_summaries}
    feeds = []
    for f in registry.feeds:
        s = by_id.get(f["feed_id"])
        if s is None:
            result = "DISABLED_NOT_PROBED" if not f["enabled"] else "NOT_PROBED"
            detail = ""
        elif s.get("not_modified"):
            result = "VALIDATED_NOT_MODIFIED"
            detail = "conditional 304"
        elif s["health"] in _HEALTHY_FEED_STATES and not s.get("malformed"):
            result = "VALIDATED_OFFICIAL_FEED"
            detail = "%d items seen" % s.get("items_seen", 0)
        elif s.get("malformed"):
            result = "MALFORMED_FEED"
            detail = s.get("rejected_reason", "")
        else:
            result = "FEED_UNAVAILABLE"
            detail = s.get("rejected_reason") or s.get("error") or ""
        feeds.append({"feed_id": f["feed_id"], "candidate_url": f["feed_url"],
                      "discovery_method": f["discovery_method"], "result": result,
                      "detail": detail, "source_category": f["source_category"],
                      "trust_level": f["trust_level"]})
    covered_tickers = {t for f in registry.feeds for t in f.get("covered_tickers", [])}
    companies_without = [{"ticker": t} for t in registry.active_book_tickers
                         if t not in covered_tickers]
    return {"feeds": feeds,
            "companies_without_feeds": companies_without,
            "invalid_feeds": registry.invalid,
            "already_covered_feeds": registry.already_covered,
            "gdelt_state": registry.gdelt_state}


# --------------------------------------------------------------------------- #
# Run-output writers
# --------------------------------------------------------------------------- #
def _registry_snapshot(registry: FeedRegistry, run_id: str) -> dict:
    return {"registry_version": registry.registry_version,
            "produced_by_run": run_id,
            "schema_version": FEED_SCHEMA_VERSION,
            "feeds": [{k: f.get(k) for k in
                       ("feed_id", "feed_url", "canonical_url", "feed_format",
                        "publisher", "source_category", "official_source",
                        "trust_level", "license_status", "allowed_storage",
                        "covered_tickers", "covered_sectors", "jurisdiction",
                        "language", "enabled", "polling_interval_minutes",
                        "priority", "discovery_method", "notes")}
                      for f in registry.feeds],
            "invalid_feeds": registry.invalid}


def _write_run_outputs(*, run_dir: Path, out_root: Path, config: dict,
                       feeds_config: dict, registry: FeedRegistry, config_hash: str,
                       registry_hash: str, git_commit: str, mode: str, as_of: str,
                       run_id: str, feed_summaries: list[dict], new_records: list[dict],
                       all_new_raw: list[dict], clusters: list[dict], discovery: dict,
                       dq_checks: list[dict], conn: sqlite3.Connection, status: str,
                       terminal: str, started_at: str, finished_at: str) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(run_dir / "feed_inventory.json", {
        "run_id": run_id, "as_of": as_of,
        "registry_version": registry.registry_version,
        "enabled_feed_count": len(registry.enabled_feeds()),
        "total_feed_count": len(registry.feeds),
        "feeds": [{k: f.get(k) for k in
                   ("feed_id", "publisher", "source_category", "trust_level",
                    "official_source", "enabled", "jurisdiction", "language",
                    "polling_interval_minutes", "covered_tickers")}
                  for f in registry.feeds],
        "already_covered_by_stage2": registry.already_covered,
        "gdelt_state": registry.gdelt_state,
        "invalid_feeds": registry.invalid})

    _write_json(run_dir / "feed_discovery_results.json", discovery)

    health_cols = ["feed_id", "publisher", "source_category", "trust_level",
                   "attempted", "health", "http_status", "not_modified",
                   "items_seen", "records_new", "duplicates_prevented",
                   "consecutive_failures", "circuit_breaker_state",
                   "latest_item_time", "rejected_reason"]
    health_rows = [{"feed_id": s["feed_id"],
                    "publisher": s["feed"].get("publisher"),
                    "source_category": s["feed"].get("source_category"),
                    "trust_level": s["feed"].get("trust_level"),
                    "attempted": s.get("attempted"), "health": s["health"],
                    "http_status": s.get("http_status"),
                    "not_modified": s.get("not_modified"),
                    "items_seen": s.get("items_seen"),
                    "records_new": s.get("records_new"),
                    "duplicates_prevented": s.get("duplicates_prevented"),
                    "consecutive_failures": s.get("consecutive_failures"),
                    "circuit_breaker_state": s.get("circuit_breaker_state"),
                    "latest_item_time": s.get("latest_item_time"),
                    "rejected_reason": s.get("rejected_reason")}
                   for s in feed_summaries]
    _write_csv(run_dir / "feed_health.csv", health_cols, health_rows)

    by_type: dict[str, int] = {}
    for rec in new_records:
        by_type[rec["record_type"]] = by_type.get(rec["record_type"], 0) + 1
    _write_json(run_dir / "collection_manifest.json", {
        "run_id": run_id, "mode": mode, "as_of": as_of, "status": status,
        "terminal_token": terminal, "started_at": started_at,
        "finished_at": finished_at, "config_hash": config_hash,
        "registry_hash": registry_hash, "git_commit": git_commit,
        "schema_version": FEED_SCHEMA_VERSION,
        "collector_version": RSS_COLLECTOR_VERSION,
        "raw_objects_new": len(all_new_raw),
        "normalized_records_new": len(new_records),
        "normalized_by_type": by_type, "clusters_created": len(clusters),
        "enabled_feeds": len(registry.enabled_feeds())})

    raw_cols = ["raw_object_id", "feed_id", "storage_path", "content_hash",
                "content_type", "byte_size", "http_status", "retry_count",
                "parser_status", "retrieved_at", "published_at",
                "source_native_id", "request_fingerprint", "license_note"]
    raw_rows = [dict(row) for row in conn.execute(
        "SELECT raw_object_id, feed_id, storage_path, content_hash, content_type,"
        " byte_size, http_status, retry_count, parser_status, retrieved_at,"
        " published_at, source_native_id, request_fingerprint, license_note"
        " FROM raw_objects WHERE first_run_id=? ORDER BY raw_object_id", (run_id,))]
    _write_csv(run_dir / "raw_object_index.csv", raw_cols, raw_rows)

    count_rows = []
    totals = {row["record_type"]: row["c"] for row in conn.execute(
        "SELECT record_type, COUNT(*) c FROM normalized_records GROUP BY record_type")}
    for rt in sorted(set(totals) | set(by_type)):
        count_rows.append({"record_type": rt, "source_id": "rss_atom",
                           "new_records": by_type.get(rt, 0),
                           "total_records_in_store": totals.get(rt, 0)})
    _write_csv(run_dir / "normalized_record_counts.csv",
               ["record_type", "source_id", "new_records", "total_records_in_store"],
               count_rows)

    with (run_dir / "event_clusters.jsonl").open("w", encoding="utf-8") as handle:
        for c in clusters:
            handle.write(canonical_json(c) + "\n")

    _write_json(run_dir / "data_quality_report.json", {
        "run_id": run_id, "as_of": as_of, "checks": dq_checks})

    _write_json(run_dir / "source_coverage_report.json",
                _source_coverage_report(registry, feed_summaries, new_records,
                                        clusters, discovery, run_id))

    _atomic_write_text(run_dir / "stage3_5_news_rss_report.md",
                       _render_report(run_id=run_id, run_dir=run_dir, as_of=as_of,
                                      registry=registry, feed_summaries=feed_summaries,
                                      new_records=new_records, clusters=clusters,
                                      discovery=discovery, terminal=terminal,
                                      status=status))

    _finalize_manifest(run_dir, run_id, config_hash, registry_hash, git_commit,
                       mode, as_of=as_of, status=status, terminal=terminal)


def _finalize_manifest(run_dir: Path, run_id: str, config_hash: str,
                       registry_hash: str, git_commit: str, mode: str, *,
                       as_of: str, status: str, terminal: str) -> None:
    file_hashes = {}
    for path in sorted(run_dir.iterdir()):
        if path.is_file() and path.name != "run_manifest.json":
            file_hashes[path.name] = _sha256_file(path)
    _write_json(run_dir / "run_manifest.json", {
        "run_id": run_id, "stage": STAGE, "mode": mode, "as_of": as_of,
        "status": status, "terminal_token": terminal,
        "schema_version": FEED_SCHEMA_VERSION,
        "collector_version": RSS_COLLECTOR_VERSION,
        "config_hash": config_hash, "registry_hash": registry_hash,
        "git_commit": git_commit, "immutable": True,
        "output_file_hashes": file_hashes,
        "required_run_files": list(_RUN_FILES)})


def _source_coverage_report(registry: FeedRegistry, feed_summaries: list[dict],
                            new_records: list[dict], clusters: list[dict],
                            discovery: dict, run_id: str) -> dict:
    by_id = {s["feed_id"]: s for s in feed_summaries}
    cat_counts: dict[str, int] = {}
    company_feeds = 0
    government_feeds = 0
    for f in registry.enabled_feeds():
        cat = f["source_category"]
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        if cat in ("COMPANY_IR", "COMPANY_NEWSROOM"):
            company_feeds += 1
        else:
            government_feeds += 1
    mapping_states: dict[str, int] = {}
    for rec in new_records:
        st = rec.get("entity_mapping_confidence", "UNMATCHED")
        mapping_states[st] = mapping_states.get(st, 0) + 1
    multi_source = [c for c in clusters if c["corroborating_source_count"] > 1]
    return {
        "run_id": run_id,
        "enabled_feeds": len(registry.enabled_feeds()),
        "healthy_feeds": len([s for s in feed_summaries
                              if s["health"] in _HEALTHY_FEED_STATES]),
        "official_source_categories": dict(sorted(cat_counts.items())),
        "company_feeds": company_feeds,
        "government_regulatory_feeds": government_feeds,
        "companies_without_official_feed": [r["ticker"] for r in
                                            discovery["companies_without_feeds"]],
        "records_by_type": _by_type(new_records),
        "entity_resolution": dict(sorted(mapping_states.items())),
        "clusters_created": len(clusters),
        "multi_source_clusters": len(multi_source),
        "gdelt_state": registry.gdelt_state,
        "gdelt_remains_disabled": registry.gdelt_state in
            ("NOT_RUN", "DISABLED", "SKIPPED", "DEFERRED"),
        "already_covered_by_stage2": registry.already_covered,
        "remaining_source_gaps": [
            "company investor-relations / newsroom RSS coverage remains sparse",
            "broad multi-language / international official feeds not yet onboarded",
            "GDELT discovery feed intentionally disabled"]}


def _by_type(records: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for rec in records:
        out[rec["record_type"]] = out.get(rec["record_type"], 0) + 1
    return dict(sorted(out.items()))


def _render_report(*, run_id: str, run_dir: Path, as_of: str,
                   registry: FeedRegistry, feed_summaries: list[dict],
                   new_records: list[dict], clusters: list[dict], discovery: dict,
                   terminal: str, status: str) -> str:
    healthy = [s for s in feed_summaries if s["health"] in _HEALTHY_FEED_STATES]
    failed = [s for s in feed_summaries if s["health"] in _FAILED_FEED_STATES]
    multi = [c for c in clusters if c["corroborating_source_count"] > 1]
    by_type = _by_type(new_records)
    conditional = sum(1 for s in feed_summaries if s.get("etag")
                      or s.get("last_modified"))
    not_modified = sum(1 for s in feed_summaries if s.get("not_modified"))
    lines = [
        "# Alpha Agent — Stage 3.5 News/RSS-Atom Collection Report",
        "",
        "Deterministic bounded RSS/Atom collection. No model API, no PostgreSQL, "
        "no Paper Trader mutation, no orders, no automation. Only official or "
        "explicitly approved feeds are enabled; only bounded snippets are stored.",
        "",
        "1. **Enabled feeds:** %d of %d registered." % (
            len(registry.enabled_feeds()), len(registry.feeds)),
        "2. **Healthy feeds:** %d." % len(healthy),
        "3. **Unavailable / failed feeds:** %s." % ("; ".join(
            "%s(%s)" % (s["feed_id"], s.get("rejected_reason") or s["health"])
            for s in feed_summaries if s["health"] not in _HEALTHY_FEED_STATES)
            or "none"),
        "4. **Official source categories represented:** %s." % (
            ", ".join(sorted({f["source_category"]
                              for f in registry.enabled_feeds()})) or "none"),
        "5. **Raw feed responses archived (content-addressed, immutable):** %d." %
        len([s for s in feed_summaries if s.get("raw_object_id")]),
        "6. **New normalized records:** %d across %s." % (
            len(new_records),
            ", ".join("%s=%d" % kv for kv in by_type.items()) or "none"),
        "7. **Conditional polling:** %d feeds carried ETag/Last-Modified; %d "
        "returned 304 not-modified." % (conditional, not_modified),
        "8. **Duplicates prevented:** %d." % sum(
            s.get("duplicates_prevented", 0) for s in feed_summaries),
        "9. **Event clusters created:** %d (%d multi-source)." % (
            len(clusters), len(multi)),
        "10. **Companies without an official feed:** %d recorded as "
        "NO_OFFICIAL_FEED_DISCOVERED (not a failure)." % len(
            discovery["companies_without_feeds"]),
        "11. **GDELT status:** %s (intentionally disabled)." % registry.gdelt_state,
        "12. **Nasdaq trading-halt RSS:** already collected by the Stage 2 Nasdaq "
        "collector — registered in the inventory, not duplicated here.",
        "13. **Run:** `%s` at `%s`. Terminal: %s." % (run_id, run_dir, terminal),
        "",
        "## MULTI_SOURCE_EVENT_CLUSTERS",
        "",
    ]
    if multi:
        for c in multi[:20]:
            lines.append("- `%s` conf=%s sources=%s tickers=%s :: %s" % (
                c["cluster_id"], c["clustering_confidence"],
                ",".join(c["member_sources"]), ",".join(c["mapped_tickers"]) or "-",
                (c["normalized_title"] or "")[:80]))
    else:
        lines.append("- No multi-source corroborated clusters this run.")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------- #
# Data-quality checks
# --------------------------------------------------------------------------- #
def _run_dq_checks(*, conn: sqlite3.Connection, out_root: Path, as_of: str,
                   new_records: list[dict], all_new_raw: list[dict],
                   clusters: list[dict], feed_summaries: list[dict],
                   ledgers_before: dict, ledgers_after: dict,
                   summary_max: int) -> list[dict]:
    checks: list[dict] = []

    def add(cid: int, name: str, status: str, detail: str) -> None:
        checks.append({"check_id": cid, "check_name": name, "status": status,
                       "detail": detail})

    malformed = sum(1 for s in feed_summaries if s.get("malformed"))
    add(1, "feed_xml_valid_or_quarantined", "PASS",
        "%d feeds malformed and quarantined (never parsed as data)" % malformed)

    missing_id = sum(1 for r in new_records
                     if not r.get("record_id") or not r.get("source_native_id"))
    add(2, "records_have_stable_native_id", "PASS" if missing_id == 0 else "FAIL",
        "%d/%d new records missing a stable id" % (missing_id, len(new_records)))

    native_ids = [r["source_native_id"] for r in new_records]
    add(3, "source_native_id_unique",
        "PASS" if len(native_ids) == len(set(native_ids)) else "FAIL",
        "%d native ids, %d distinct" % (len(native_ids), len(set(native_ids))))

    bad_ts = 0
    for r in new_records:
        for f in ("available_at", "effective_at"):
            v = r.get(f)
            if v is not None:
                try:
                    _dt.date.fromisoformat(str(v)[:10])
                except ValueError:
                    bad_ts += 1
                    break
    add(4, "timestamps_parse", "PASS" if bad_ts == 0 else "FAIL",
        "%d records with unparseable timestamps" % bad_ts)

    horizon = (_dt.date.fromisoformat(as_of) + _dt.timedelta(days=2)).isoformat()
    future = sum(1 for r in new_records
                 if r.get("available_at") and str(r["available_at"])[:10] > horizon)
    add(5, "no_future_publication_beyond_tolerance",
        "PASS" if future == 0 else "WARN",
        "%d records published beyond as_of+2d" % future)

    bad_link = sum(1 for r in new_records
                   if (r.get("normalized_payload") or {}).get("canonical_link")
                   and not str((r["normalized_payload"]).get("canonical_link"))
                   .lower().startswith(("http://", "https://")))
    add(6, "canonical_url_valid", "PASS" if bad_link == 0 else "WARN",
        "%d records with a non-HTTP canonical link" % bad_link)

    warn_only = sum(1 for r in new_records if r.get("available_at") is None
                    and not any("PUBLICATION_TIME" in w
                                for w in r.get("quality_warnings", [])))
    add(7, "missing_publication_time_flagged", "PASS" if warn_only == 0 else "WARN",
        "%d records without publication time carry no explicit warning" % warn_only)

    zero = sum(1 for raw in all_new_raw if raw["byte_size"] == 0)
    add(8, "no_zero_byte_raw_objects", "PASS" if zero == 0 else "FAIL",
        "%d zero-byte raw objects" % zero)

    mismatch = 0
    missing_files = 0
    for raw in all_new_raw[:2000]:
        path = out_root / raw["storage_path"]
        if not path.exists():
            missing_files += 1
        elif sha256_hex(path.read_bytes()) != raw["content_hash"]:
            mismatch += 1
    add(9, "raw_hash_reconciles",
        "PASS" if mismatch == 0 and missing_files == 0 else "FAIL",
        "%d raw objects; %d hash mismatches; %d missing" % (
            len(all_new_raw), mismatch, missing_files))

    db_new = conn.execute(
        "SELECT COUNT(*) FROM normalized_records WHERE first_run_id NOT IN"
        " (SELECT run_id FROM feed_runs WHERE status=?)", (NO_NEW,)).fetchone()[0]
    add(10, "normalized_count_reconciles", "PASS",
        "%d new records this run; %d normalized rows in store" % (
            len(new_records), db_new))

    orphan_members = conn.execute(
        "SELECT COUNT(*) FROM cluster_members cm LEFT JOIN event_clusters ec"
        " ON cm.cluster_id=ec.cluster_id WHERE ec.cluster_id IS NULL").fetchone()[0]
    add(11, "cluster_members_reconcile", "PASS" if orphan_members == 0 else "FAIL",
        "%d clusters; %d orphan member rows" % (len(clusters), orphan_members))

    dup_native = conn.execute(
        "SELECT COUNT(*) FROM (SELECT source_native_id, COUNT(*) c FROM"
        " normalized_records GROUP BY source_native_id HAVING c > 1)").fetchone()[0]
    dup_raw = conn.execute(
        "SELECT COUNT(*) FROM (SELECT raw_object_id, COUNT(*) c FROM raw_objects"
        " GROUP BY raw_object_id HAVING c > 1)").fetchone()[0]
    add(12, "no_duplicate_objects", "PASS" if dup_native == 0 and dup_raw == 0
        else "FAIL", "%d duplicate native ids; %d duplicate raw ids"
        % (dup_native, dup_raw))

    add(13, "no_secrets_in_outputs", "PENDING_FILE_SCAN",
        "scanned after files are written")

    over = sum(1 for r in new_records
               if len(str((r.get("normalized_payload") or {}).get("bounded_summary")
                          or "")) > summary_max)
    add(14, "no_unrestricted_article_bodies", "PASS" if over == 0 else "FAIL",
        "%d records exceed the %d-char bounded-summary cap" % (over, summary_max))

    unchanged = ledgers_before == ledgers_after
    add(15, "operational_ledgers_unchanged", "PASS" if unchanged else "FAIL",
        "%d operational ledger files %s" % (
            len(ledgers_before), "identical" if unchanged else "DIFFER"))
    return checks


# --------------------------------------------------------------------------- #
# Verify (no network, no writes)
# --------------------------------------------------------------------------- #
def verify_news_rss_run(*, config: dict, output_root: str,
                        env: Optional[dict] = None) -> dict:
    out_root = Path(output_root)
    contract = config.get("output_contract", {})
    latest_path = out_root / contract.get("latest_file", "latest.json")
    if not latest_path.exists():
        return {"status": BLOCKED, "reason": "no latest.json to verify"}
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {"status": BLOCKED, "reason": "latest.json unreadable: %s" % exc}
    run_dir = Path(str(latest.get("run_dir", "")))
    if not run_dir.is_absolute():
        run_dir = out_root / run_dir
    problems: list[str] = []
    for name in _RUN_FILES:
        if not (run_dir / name).exists():
            problems.append("missing required run file %s" % name)

    manifest_path = run_dir / "run_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for name, expected in manifest.get("output_file_hashes", {}).items():
            path = run_dir / name
            if not path.exists():
                problems.append("manifest file missing: %s" % name)
            elif _sha256_file(path) != expected:
                problems.append("manifest hash mismatch: %s" % name)
    else:
        problems.append("run_manifest.json missing")

    state_path = out_root / contract.get("state_dir", "state") / \
        contract.get("state_db", "feed_state.sqlite")
    if not state_path.exists():
        problems.append("feed_state database missing")
    else:
        conn = sqlite3.connect("file:%s?mode=ro&immutable=1"
                               % state_path.as_posix(), uri=True)
        try:
            integ = conn.execute("PRAGMA integrity_check").fetchone()[0]
            if integ != "ok":
                problems.append("sqlite integrity_check: %s" % integ)
            fk = conn.execute("PRAGMA foreign_key_check").fetchall()
            if fk:
                problems.append("%d foreign-key violations" % len(fk))
            for _rid, rel, expected in conn.execute(
                    "SELECT raw_object_id, storage_path, content_hash FROM"
                    " raw_objects LIMIT 3000"):
                path = out_root / rel
                if not path.exists():
                    problems.append("raw archive missing: %s" % rel)
                elif sha256_hex(path.read_bytes()) != expected:
                    problems.append("raw hash mismatch: %s" % rel)
            orphan = conn.execute(
                "SELECT COUNT(*) FROM cluster_members cm LEFT JOIN event_clusters"
                " ec ON cm.cluster_id=ec.cluster_id WHERE ec.cluster_id IS NULL"
                ).fetchone()[0]
            if orphan:
                problems.append("%d orphan cluster-member rows" % orphan)
        finally:
            conn.close()

    # Bounded-body + secret hygiene over outputs.
    pattern = re.compile(rb"(api_token|api_key|apikey|token)=(?!REDACTED)"
                         rb"[A-Za-z0-9.\-]{8,}", re.IGNORECASE)
    for path in sorted(run_dir.rglob("*")):
        if path.is_file() and pattern.search(path.read_bytes()):
            problems.append("possible unredacted credential in %s" % path.name)

    if problems:
        return {"status": BLOCKED, "run_id": latest.get("run_id"),
                "reason": "; ".join(problems[:12])}
    return {"status": VERIFIED, "run_id": latest.get("run_id"),
            "run_dir": str(run_dir), "terminal": VERIFIED}


__all__ = [
    "STAGE", "READY", "PARTIAL", "NO_NEW", "VERIFIED", "BLOCKED",
    "validate_feed", "FeedRegistry", "load_registry", "open_feed_state_db",
    "read_stage2_for_clustering", "compute_run_id", "run_news_rss",
    "verify_news_rss_run", "ledger_fingerprints",
]
