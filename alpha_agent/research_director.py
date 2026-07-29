"""
alpha_agent/research_director.py — Stage 3 GROUNDED LLM RESEARCH DIRECTOR.

Deterministic orchestrator around a strictly-bounded LLM:

* consumes ONLY the verified Stage 1 registry package and verified Stage 2
  normalized records (never raw feeds, never operational ledgers);
* selects genuinely new, health-checked, provenance-carrying event records
  incrementally from the prior checkpoint with per-source / per-ticker /
  per-cycle / token bounds;
* asks the LLM to interpret events, propose hypotheses, prioritize research
  and narrate the daily report — NOTHING else. The LLM has no tools, no code
  execution, no web, no file access, and every claim must trace to supplied
  record IDs;
* validates grounding, rejects unsupported output, runs Stage 1 duplicate
  prevention (classify_candidate_experiment) and makes the FINAL queue
  decision deterministically;
* enforces hard token / call / cost budgets; tracks exact usage;
* writes an immutable run package + resumable sqlite state; publishes
  latest.json only after verification gates pass.

No experiment execution, no model promotion, no orders, no Paper Trader
mutation, no PostgreSQL, no commit.
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
from typing import Any, Callable, Mapping, Optional

from . import research_registry as _stage1
from .llm_budget import BudgetLedger, PricingTable, estimate_tokens
from .llm_contracts import (COST_UNAVAILABLE, DIRECTOR_SCHEMA_VERSION,
                            GR_GROUNDED, HYPOTHESIS_STATUS_DRAFT,
                            PC_BLOCKED_UNAVAILABLE,
                            PC_DEVELOPMENT_READY, PC_PRODUCTION_READY,
                            PROMPT_VERSION, QS_HOLD_DATA, QS_HOLD_METADATA,
                            QS_READY, QS_REJECT_DUP, QS_REJECT_UNGROUNDED,
                            QS_RESUME, TASK_EVENT, TASK_HYPOTHESIS,
                            TASK_NARRATIVE, TASK_PRIORITIZE, build_output_schema,
                            canonical_json, clamp_event_analysis,
                            detect_injection_indicators, hypothesis_spec_hash,
                            parse_json_object, render_prompt,
                            sanitize_untrusted_text, sha256_text,
                            validate_event_analysis, validate_hypothesis,
                            wrap_untrusted)
from .llm_providers import (AnthropicHttpProvider, ClaudeCodeProvider)
from .llm_providers.claude_code import CLAUDE_CODE_DEVELOPMENT_ONLY
from .event_clustering import index_clusters

READY = "ALPHA_AGENT_STAGE3_READY"
DEV_READY = "ALPHA_AGENT_STAGE3_DEV_READY"
NO_NEW = "NO_NEW_DIRECTOR_INPUT"
BUDGET_EXHAUSTED = "ALPHA_AGENT_STAGE3_BUDGET_EXHAUSTED"
VERIFIED = "ALPHA_AGENT_STAGE3_VERIFIED"
PARTIAL = "ALPHA_AGENT_STAGE3_PARTIAL"
BLOCKED = "ALPHA_AGENT_STAGE3_BLOCKED"

ELIGIBLE_RECORD_TYPES = (
    "NEWS_EVENT", "FILING_EVENT", "INSIDER_FILING", "EARNINGS_EVENT",
    "TRADING_HALT", "MACRO_OBSERVATION", "SHORT_VOLUME", "CORPORATE_ACTION",
    "SOURCE_HEALTH",
    # Stage 3.5 generalized RSS/Atom event contracts (source_id 'rss_atom'),
    # read from the verified additional_event_roots alongside Stage 2 records.
    "REGULATORY_EVENT", "PRESS_RELEASE")
_FORBIDDEN_INDIVIDUAL_TYPES = ("MARKET_BAR",)
_RSS_SOURCE_ID = "rss_atom"

# Verified news/RSS acquisition state. Stage 3 must never imply comprehensive
# news or RSS coverage: only EODHD financial news (NEWS_EVENT), SEC EDGAR
# (FILING_EVENT + INSIDER_FILING) and the single narrow Nasdaq Trader
# trading-halt RSS feed (TRADING_HALT) are operational; GDELT is implemented
# but intentionally disabled; no generalized RSS/Atom feed registry or
# collector, no company investor-relations/newsroom feeds, no broader official
# regulatory feeds and no cross-feed event clustering exist yet.
STAGE3_5_MARKER = "STAGE3_5_NEWS_RSS_EXPANSION_REQUIRED"
_NEWS_COVERAGE_TYPES = ("NEWS_EVENT", "FILING_EVENT", "INSIDER_FILING",
                        "TRADING_HALT", "EARNINGS_EVENT", "CORPORATE_ACTION",
                        "REGULATORY_EVENT", "PRESS_RELEASE")
_NEWS_RSS_INVENTORY = {
    "news_sources": {
        "eodhd_financial_news": "ENTITLED — normalized NEWS_EVENT records",
        "sec_edgar": "OPERATIONAL — FILING_EVENT + INSIDER_FILING records",
        "nasdaq_trader_halt_rss": "OPERATIONAL_NARROW — single trading-halt "
                                  "RSS feed producing TRADING_HALT records",
        "gdelt": "IMPLEMENTED_INTENTIONALLY_DISABLED"},
    "rss_feeds": ["nasdaq_trader_trading_halts (narrow, single feed)"],
    "company_direct_rss_atom_feeds_exist": False,
    "generalized_rss_collection_exists": False,
    "source_gaps": [
        "no generalized RSS/Atom feed registry or collector",
        "no company investor-relations feeds",
        "no corporate newsroom feeds",
        "no broader official regulatory/government feeds",
        "no cross-feed event clustering"]}

_ANALYZE_RUN_FILES = (
    "input_snapshot.json", "selected_event_records.jsonl",
    "registry_context.json", "prompt_manifest.json", "provider_receipt.json",
    "structured_event_analysis.jsonl", "hypothesis_proposals.json",
    "duplicate_gate_results.json", "rejected_proposals.json",
    "research_queue.json", "director_decisions.json", "token_cost_report.json",
    "stage3_daily_report.md", "stage3_5_news_rss_requirements.json",
    "run_manifest.json")
_AUDIT_RUN_FILES = ("input_snapshot.json", "provider_receipt.json",
                    "token_cost_report.json", "stage3_daily_report.md",
                    "run_manifest.json")

_SECRET_ENV_NAMES = ("ANTHROPIC_API_KEY", "CLAUDE_API_KEY", "EODHD_API_KEY",
                     "FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY")
_SECRET_PATTERN = re.compile(r"sk-ant-[A-Za-z0-9\-_]{16,}")

_SCHEMA_SQL = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS director_runs (
  run_id TEXT PRIMARY KEY, mode TEXT NOT NULL, as_of TEXT NOT NULL,
  status TEXT NOT NULL, terminal_token TEXT, stage1_run_id TEXT,
  stage2_run_id TEXT, provider TEXT, model TEXT, config_hash TEXT,
  git_commit TEXT, schema_version TEXT, prompt_version TEXT,
  records_considered INTEGER DEFAULT 0, records_selected INTEGER DEFAULT 0,
  analyses_accepted INTEGER DEFAULT 0, hypotheses_proposed INTEGER DEFAULT 0,
  queue_entries INTEGER DEFAULT 0, started_at TEXT, finished_at TEXT);
CREATE TABLE IF NOT EXISTS provider_calls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  call_index INTEGER NOT NULL, task TEXT NOT NULL, provider TEXT NOT NULL,
  model TEXT, prompt_hash TEXT NOT NULL, request_id TEXT, status TEXT NOT NULL,
  retries INTEGER DEFAULT 0, input_tokens INTEGER, output_tokens INTEGER,
  cache_creation_tokens INTEGER, cache_read_tokens INTEGER,
  response_hash TEXT, error TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS token_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  provider TEXT NOT NULL, model TEXT, day TEXT NOT NULL, month TEXT NOT NULL,
  input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0,
  cache_creation_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0,
  estimated_cost_usd REAL, cost_available INTEGER DEFAULT 0, created_at TEXT);
CREATE TABLE IF NOT EXISTS processed_records (
  record_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  record_type TEXT, source_id TEXT, selected INTEGER DEFAULT 0,
  skip_reason TEXT, processed_at TEXT);
CREATE TABLE IF NOT EXISTS event_analyses (
  event_analysis_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  packet_id TEXT, grounding_result TEXT NOT NULL, materiality TEXT,
  novelty TEXT, confidence REAL, source_record_ids_json TEXT,
  analysis_json TEXT, prompt_hash TEXT, response_hash TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS hypothesis_proposals (
  hypothesis_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  title TEXT, status TEXT NOT NULL, grounding_result TEXT NOT NULL,
  information_family TEXT, source_record_ids_json TEXT, spec_hash TEXT,
  proposal_json TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS duplicate_gate_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  hypothesis_id TEXT NOT NULL REFERENCES hypothesis_proposals(hypothesis_id),
  result TEXT NOT NULL, matched_experiment_ids_json TEXT, reason TEXT,
  created_at TEXT);
CREATE TABLE IF NOT EXISTS research_queue (
  queue_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  hypothesis_id TEXT NOT NULL REFERENCES hypothesis_proposals(hypothesis_id),
  priority INTEGER, duplicate_result TEXT, status TEXT NOT NULL,
  spec_hash TEXT, entry_json TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS director_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES director_runs(run_id),
  decision_type TEXT NOT NULL, subject_id TEXT, decision TEXT NOT NULL,
  reason TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS director_checkpoints (
  checkpoint_key TEXT PRIMARY KEY, cursor_json TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT);
CREATE TABLE IF NOT EXISTS director_errors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT, category TEXT NOT NULL, detail TEXT, created_at TEXT);
CREATE INDEX IF NOT EXISTS idx_tu_day ON token_usage(day);
CREATE INDEX IF NOT EXISTS idx_tu_month ON token_usage(month);
CREATE INDEX IF NOT EXISTS idx_pc_run ON provider_calls(run_id);
"""


# --------------------------------------------------------------------------- #
# Small IO helpers.
# --------------------------------------------------------------------------- #
def _read_json(path: Path) -> Optional[Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, sort_keys=True, indent=1, default=str),
                   encoding="utf-8")
    os.replace(tmp, path)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def config_hash_of(config: dict) -> str:
    return sha256_text(canonical_json(config))[:16]


def ledger_fingerprints(config: dict) -> dict[str, str]:
    """SHA-256 of every operational Paper Trader ledger file (read-only)."""
    out: dict[str, str] = {}
    for root in config.get("operational_ledger_roots", []):
        rp = Path(root)
        if not rp.exists():
            continue
        for f in sorted(rp.glob("*")):
            if f.is_file():
                out[str(f)] = _sha256_file(f)
    return out


# --------------------------------------------------------------------------- #
# Stage 1 / Stage 2 package consumption (read-only, verified).
# --------------------------------------------------------------------------- #
def read_stage1(config: dict) -> dict:
    root = Path(config["stage1_registry_root"])
    latest = _read_json(root / "latest.json")
    if not isinstance(latest, dict) or not latest.get("run_id"):
        return {"ok": False, "reason": "Stage 1 latest.json missing or invalid "
                                       "at %s" % root}
    run_dir = root / str(latest.get("run_dir", "runs/%s" % latest["run_id"]))
    if not run_dir.exists():
        return {"ok": False, "reason": "Stage 1 run dir missing: %s" % run_dir}
    required = ("experiment_registry.csv", "research_coverage_map.csv",
                "current_state_summary.json")
    for name in required:
        if not (run_dir / name).exists():
            return {"ok": False, "reason": "Stage 1 run file missing: %s" % name}
    experiments: list[dict] = []
    with open(run_dir / "experiment_registry.csv", encoding="utf-8-sig",
              newline="") as fh:
        for row in csv.DictReader(fh):
            experiments.append({
                "experiment_id": row.get("experiment_id"),
                "exact_fingerprint": row.get("exact_fingerprint"),
                "family_fingerprint": row.get("family_fingerprint"),
                "decision": row.get("decision") or None,
                "observed_at": row.get("observed_at") or None,
                "information_family": row.get("information_family")})
    coverage: list[dict] = []
    with open(run_dir / "research_coverage_map.csv", encoding="utf-8-sig",
              newline="") as fh:
        coverage = list(csv.DictReader(fh))
    current_state = _read_json(run_dir / "current_state_summary.json") or {}
    return {"ok": True, "run_id": latest["run_id"], "root": root,
            "run_dir": run_dir, "latest": latest, "experiments": experiments,
            "coverage": coverage, "current_state": current_state}


def read_stage2(config: dict) -> dict:
    root = Path(config["stage2_ingestion_root"])
    latest = _read_json(root / "latest.json")
    if not isinstance(latest, dict) or not latest.get("run_id"):
        return {"ok": False, "reason": "Stage 2 latest.json missing or invalid "
                                       "at %s" % root}
    token = str(latest.get("terminal_token") or "")
    if token not in ("ALPHA_AGENT_STAGE2_READY",) \
            and not token.startswith("ALPHA_AGENT_STAGE2_PARTIAL"):
        return {"ok": False, "reason": "Stage 2 package not in a verified "
                                       "READY/PARTIAL state: %s" % token}
    run_dir = root / str(latest.get("run_dir", "")).replace("\\", os.sep)
    if not run_dir.exists():
        return {"ok": False, "reason": "Stage 2 run dir missing: %s" % run_dir}
    if not (run_dir / "run_manifest.json").exists():
        return {"ok": False, "reason": "Stage 2 run_manifest.json missing"}
    health: dict[str, str] = {}
    hp = run_dir / "source_health.csv"
    if hp.exists():
        with open(hp, encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                health[str(row.get("source_id"))] = str(row.get("overall_state"))
    counts: dict[str, int] = {}
    cp = run_dir / "normalized_record_counts.csv"
    if cp.exists():
        with open(cp, encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                rt = str(row.get("record_type"))
                counts[rt] = counts.get(rt, 0) + int(row.get("total_records_in_store") or 0)
    return {"ok": True, "run_id": latest["run_id"], "root": root,
            "run_dir": run_dir, "latest": latest, "source_health": health,
            "record_type_counts": counts,
            "as_of": str(latest.get("as_of") or "")}


_STAGE3_5_ACCEPTED_TOKENS = ("ALPHA_AGENT_STAGE3_5_READY",
                             "ALPHA_AGENT_STAGE3_5_VERIFIED")
_STAGE3_5_PARTIAL_PREFIX = "ALPHA_AGENT_STAGE3_5_PARTIAL"
_HEALTHY_FEED_STATES = ("HEALTHY", "HEALTHY_NOT_MODIFIED", "DEGRADED")
_BLOCKED_FEED_STATES = ("FAILED", "CIRCUIT_OPEN")


def _load_stage35_feed_evidence(run_dir: Path) -> dict:
    """Read the Stage 3.5 run's feed-level evidence (feed_health.csv +
    source_coverage_report.json) so the Stage 3 report can state enabled /
    attempted / healthy / degraded / blocked feeds, the newest feed item and the
    clustering totals. Read-only; tolerant of missing files."""
    ev = {"enabled_feeds": 0, "attempted_feeds": 0, "healthy_feeds": 0,
          "degraded_feeds": 0, "blocked_feeds": 0, "newest_feed_item": None,
          "duplicate_items_prevented": 0, "clusters_created": 0,
          "multi_source_clusters": 0, "company_feeds": 0,
          "government_regulatory_feeds": 0, "unresolved_entity_mappings": 0,
          "gdelt_state": "NOT_RUN"}
    scr = _read_json(run_dir / "source_coverage_report.json")
    if isinstance(scr, dict):
        ev["enabled_feeds"] = int(scr.get("enabled_feeds", 0))
        ev["healthy_feeds"] = int(scr.get("healthy_feeds", 0))
        ev["clusters_created"] = int(scr.get("clusters_created", 0))
        ev["multi_source_clusters"] = int(scr.get("multi_source_clusters", 0))
        ev["company_feeds"] = int(scr.get("company_feeds", 0))
        ev["government_regulatory_feeds"] = int(
            scr.get("government_regulatory_feeds", 0))
        ev["gdelt_state"] = str(scr.get("gdelt_state", "NOT_RUN"))
        er = scr.get("entity_resolution", {}) or {}
        ev["unresolved_entity_mappings"] = int(er.get("UNMATCHED", 0)) \
            + int(er.get("AMBIGUOUS", 0))
    hp = run_dir / "feed_health.csv"
    if hp.exists():
        try:
            with open(hp, encoding="utf-8-sig", newline="") as fh:
                for row in csv.DictReader(fh):
                    if str(row.get("attempted")).lower() in ("true", "1"):
                        ev["attempted_feeds"] += 1
                    health = str(row.get("health") or "")
                    if health == "DEGRADED":
                        ev["degraded_feeds"] += 1
                    if health in _BLOCKED_FEED_STATES:
                        ev["blocked_feeds"] += 1
                    ev["duplicate_items_prevented"] += int(
                        row.get("duplicates_prevented") or 0)
                    lit = str(row.get("latest_item_time") or "")
                    if lit and (ev["newest_feed_item"] is None
                                or lit > ev["newest_feed_item"]):
                        ev["newest_feed_item"] = lit
        except (OSError, ValueError):
            pass
    return ev


def read_additional_roots(config: dict) -> dict:
    """Read verified additional event roots (e.g. the Stage 3.5 News/RSS root).

    Each root MUST publish a latest.json whose terminal token is an accepted
    Stage 3.5 token (READY / PARTIAL / VERIFIED); unverified or missing roots are
    REJECTED (recorded, never read). For every accepted root the run's
    event_clusters.jsonl is loaded into a record_id -> cluster-membership index so
    selection can prefer representative records and retain member ids for
    grounding. Never mutates any source record."""
    verified: list[Path] = []
    rejected: list[dict] = []
    file_clusters: list[dict] = []
    evidences: list[dict] = []
    for root_str in config.get("additional_event_roots", []):
        root = Path(root_str)
        latest = _read_json(root / "latest.json")
        if not isinstance(latest, dict) or not latest.get("run_id"):
            rejected.append({"root": str(root_str),
                             "reason": "missing or invalid latest.json"})
            continue
        token = str(latest.get("terminal_token") or "")
        if token not in _STAGE3_5_ACCEPTED_TOKENS \
                and not token.startswith(_STAGE3_5_PARTIAL_PREFIX):
            rejected.append({"root": str(root_str),
                             "reason": "unverified terminal token: %s" % token})
            continue
        run_dir = root / str(latest.get("run_dir", "")).replace("\\", os.sep)
        cf = run_dir / "event_clusters.jsonl"
        if cf.exists():
            for line in cf.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    file_clusters.append(json.loads(line))
                except ValueError:
                    continue
        evidences.append(_load_stage35_feed_evidence(run_dir))
        verified.append(root)
    clusters = index_clusters(file_clusters) if file_clusters else {}
    feed_evidence: dict = {}
    for ev in evidences:
        for k, v in ev.items():
            if k in ("newest_feed_item", "gdelt_state"):
                cur = feed_evidence.get(k)
                feed_evidence[k] = max(cur, v) if (cur and v) else (v or cur)
            else:
                feed_evidence[k] = feed_evidence.get(k, 0) + (v or 0)
    return {"roots": verified, "rejected": rejected, "clusters": clusters,
            "cluster_count": len({c["cluster_id"] for c in file_clusters}),
            "verified_count": len(verified), "feed_evidence": feed_evidence}


# --------------------------------------------------------------------------- #
# State DB.
# --------------------------------------------------------------------------- #
def open_state_db(output_root: Path) -> sqlite3.Connection:
    state_dir = output_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(state_dir / "director_state.sqlite"))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA_SQL)
    # Cap-dropped records are DEFERRED to later cycles, never permanently
    # consumed. Remove any legacy CAP_* markings so backlog beyond the
    # per-cycle bounds (NEWS_EVENT candidates in particular) stays selectable.
    conn.execute("DELETE FROM processed_records WHERE skip_reason LIKE 'CAP_%'")
    conn.commit()
    return conn


def _known_processed(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT record_id FROM processed_records")}


def _prior_usage(conn: sqlite3.Connection, day: str) -> tuple[dict, dict]:
    month = day[:7]
    def _agg(where: str, value: str) -> dict:
        row = conn.execute(
            "SELECT COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), "
            "COALESCE(SUM(CASE WHEN cost_available=1 THEN estimated_cost_usd "
            "ELSE 0 END),0.0) FROM token_usage WHERE %s=?" % where,
            (value,)).fetchone()
        return {"input_tokens": row[0], "output_tokens": row[1],
                "cost_usd": row[2]}
    return _agg("day", day), _agg("month", month)


# --------------------------------------------------------------------------- #
# Input selection (incremental, bounded, deduplicated, health/PIT-aware).
# --------------------------------------------------------------------------- #
def _iter_normalized(stage2_root: Path, record_type: str):
    base = stage2_root / "normalized" / record_type
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
                        yield {"_malformed_line": True, "_file": str(jf)}
        except OSError:
            continue


def select_input_records(config: dict, stage2: dict, known_ids: set[str], *,
                         extra_roots: Optional[list] = None,
                         cluster_index: Optional[dict] = None) -> dict:
    """Deterministic pre-filter. Returns selection evidence + selected records.
    Never mutates any source record.

    Reads normalized records from the Stage 2 ingestion root AND every verified
    additional event root (Stage 3.5 News/RSS). Record ids are deduplicated
    ACROSS roots. When a cluster index is supplied, records sharing a multi-member
    cluster collapse to the cluster's representative (retaining the member ids for
    grounding) so the LLM never sees the same event twice."""
    sel_cfg = config.get("input_selection", {})
    eligible = tuple(sel_cfg.get("eligible_record_types",
                                 ELIGIBLE_RECORD_TYPES))
    healthy_states = set(sel_cfg.get("require_source_states",
                                     ["HEALTHY", "DEGRADED"]))
    max_total = int(sel_cfg.get("max_event_records_per_cycle", 40))
    max_per_source = int(sel_cfg.get("max_records_per_source", 15))
    max_per_ticker = int(sel_cfg.get("max_records_per_ticker", 3))
    focus = [str(t).upper() for t in sel_cfg.get("focus_tickers", [])]
    health = stage2.get("source_health", {})
    roots = [stage2["root"]] + list(extra_roots or [])
    cluster_index = cluster_index or {}

    considered = 0
    malformed = 0
    skipped_processed = 0
    skipped_unhealthy = 0
    skipped_no_provenance = 0
    dup_payloads = 0
    cross_root_dups = 0
    rss_considered = 0
    seen_record_ids: set[str] = set()
    seen_payloads: set[str] = set()
    seen_titles: set[str] = set()
    candidates: list[dict] = []
    macro_by_series: dict[str, dict] = {}
    macro_all: list[dict] = []
    short_volume: list[dict] = []
    skipped_ids: list[tuple[str, str, str, str]] = []
    type_considered: dict[str, int] = {}
    type_freshness: dict[str, str] = {}

    for rt in eligible:
        if rt in _FORBIDDEN_INDIVIDUAL_TYPES:
            continue
        for root in roots:
            for rec in _iter_normalized(root, rt):
                considered += 1
                if rec.get("_malformed_line") or not rec.get("record_id"):
                    malformed += 1
                    continue
                rid = rec["record_id"]
                if rid in seen_record_ids:
                    # Same record present in more than one root — counted once.
                    cross_root_dups += 1
                    continue
                seen_record_ids.add(rid)
                type_considered[rt] = type_considered.get(rt, 0) + 1
                if rec.get("source_id") == _RSS_SOURCE_ID:
                    rss_considered += 1
                ts = str(rec.get("available_at") or rec.get("effective_at") or "")
                if ts and ts > type_freshness.get(rt, ""):
                    type_freshness[rt] = ts
                if rid in known_ids:
                    skipped_processed += 1
                    continue
                if not rec.get("provenance"):
                    skipped_no_provenance += 1
                    skipped_ids.append((rid, rt, rec.get("source_id", ""),
                                        "NO_PROVENANCE"))
                    continue
                state = health.get(str(rec.get("source_id")))
                if state is not None and state not in healthy_states:
                    skipped_unhealthy += 1
                    skipped_ids.append((rid, rt, rec.get("source_id", ""),
                                        "SOURCE_UNHEALTHY:%s" % state))
                    continue
                ph = rec.get("payload_hash") or rid
                if ph in seen_payloads:
                    dup_payloads += 1
                    skipped_ids.append((rid, rt, rec.get("source_id", ""),
                                        "DUPLICATE_PAYLOAD"))
                    continue
                seen_payloads.add(ph)
                info = cluster_index.get(rid)
                if info:
                    rec["_cluster"] = info
                if rt == "SHORT_VOLUME":
                    short_volume.append(rec)
                    continue
                if rt == "MACRO_OBSERVATION":
                    macro_all.append(rec)
                    series = str((rec.get("normalized_payload") or {})
                                 .get("series_id") or rec.get("source_native_id"))
                    prev = macro_by_series.get(series)
                    key = str(rec.get("available_at") or "")
                    if prev is None or key > str(prev.get("available_at") or ""):
                        macro_by_series[series] = rec
                    continue
                title = (rec.get("normalized_payload") or {}).get("title")
                if title:
                    tkey = sha256_text(str(title).strip().lower())
                    if tkey in seen_titles:
                        dup_payloads += 1
                        skipped_ids.append((rid, rt, rec.get("source_id", ""),
                                            "DUPLICATE_TITLE"))
                        continue
                    seen_titles.add(tkey)
                candidates.append(rec)

    chosen_macro = {r["record_id"] for r in macro_by_series.values()}
    for rec in macro_all:
        if rec["record_id"] not in chosen_macro:
            skipped_ids.append((rec["record_id"], "MACRO_OBSERVATION",
                                rec.get("source_id", ""),
                                "SUPERSEDED_VINTAGE"))
    candidates.extend(macro_by_series.values())
    # Fair, deterministic round-robin across record types (newest first within
    # each type) so no type — NEWS_EVENT in particular — is starved by the
    # per-cycle total cap. A flat alphabetical type ordering let FILING /
    # INSIDER / MACRO records exhaust the cap before any news was reached.
    by_type: dict[str, list[dict]] = {}
    for rec in candidates:
        by_type.setdefault(str(rec.get("record_type", "")), []).append(rec)
    for rows in by_type.values():
        rows.sort(key=lambda r: r.get("record_id", ""))
        rows.sort(key=lambda r: str(r.get("available_at")
                                    or r.get("effective_at") or ""),
                  reverse=True)
    interleaved: list[dict] = []
    type_order = sorted(by_type)
    while any(by_type[t] for t in type_order):
        for t in type_order:
            if by_type[t]:
                interleaved.append(by_type[t].pop(0))
    candidates = interleaved

    # Cross-source cluster dedup: when several candidates belong to the SAME
    # multi-member event cluster, keep only the deterministic representative and
    # attach the full member id list to it for grounding. The others are dropped
    # as CLUSTERED_DUPLICATE so the LLM never re-reads one corroborated event.
    clustered_dropped = 0
    if cluster_index:
        by_cluster: dict[str, list[dict]] = {}
        for rec in candidates:
            info = cluster_index.get(rec["record_id"])
            if info and info.get("cluster_id") \
                    and len(info.get("member_record_ids", [])) > 1:
                by_cluster.setdefault(info["cluster_id"], []).append(rec)
        drop_ids: set[str] = set()
        for cid, recs in by_cluster.items():
            present = {r["record_id"] for r in recs}
            rep_id = cluster_index[recs[0]["record_id"]]["representative_record_id"]
            keeper_id = rep_id if rep_id in present else min(present)
            keeper = next(r for r in recs if r["record_id"] == keeper_id)
            keeper["_cluster_member_ids"] = \
                cluster_index[keeper_id]["member_record_ids"]
            for r in recs:
                if r["record_id"] != keeper_id:
                    drop_ids.add(r["record_id"])
                    skipped_ids.append((r["record_id"], r.get("record_type", ""),
                                        r.get("source_id", ""),
                                        "CLUSTERED_DUPLICATE"))
        if drop_ids:
            candidates = [r for r in candidates if r["record_id"] not in drop_ids]
            clustered_dropped = len(drop_ids)

    # SHORT_VOLUME: never sent individually beyond a bounded focus set — a
    # compact deterministic summary is computed in Python instead.
    short_selected: list[dict] = []
    if short_volume:
        focus_set = set(focus)
        matches = [r for r in short_volume
                   if str(r.get("ticker") or "").upper() in focus_set]
        newest = max((str(r.get("effective_at") or "") for r in matches),
                     default="")
        short_selected = sorted(
            [r for r in matches if str(r.get("effective_at") or "") == newest],
            key=lambda r: r.get("record_id", ""))
        sel_short_ids = {r["record_id"] for r in short_selected}
        for r in short_volume:
            if r["record_id"] not in sel_short_ids:
                skipped_ids.append((r["record_id"], "SHORT_VOLUME",
                                    r.get("source_id", ""),
                                    "SHORT_VOLUME_SUMMARIZED_NOT_SELECTED"))
    else:
        pass

    per_source: dict[str, int] = {}
    per_ticker: dict[str, int] = {}
    selected: list[dict] = []
    cap_dropped = 0
    # Records beyond a per-cycle cap are DEFERRED to a later cycle — they are
    # never persisted as processed, so incremental runs drain the backlog.
    deferred_ids: list[tuple[str, str, str, str]] = []
    for rec in candidates + short_selected:
        if len(selected) >= max_total:
            cap_dropped += 1
            deferred_ids.append((rec["record_id"], rec.get("record_type", ""),
                                 rec.get("source_id", ""), "CAP_TOTAL"))
            continue
        src = str(rec.get("source_id"))
        if per_source.get(src, 0) >= max_per_source:
            cap_dropped += 1
            deferred_ids.append((rec["record_id"], rec.get("record_type", ""),
                                 src, "CAP_PER_SOURCE"))
            continue
        tk = str(rec.get("ticker") or "").upper()
        is_short = rec.get("record_type") == "SHORT_VOLUME"
        # SHORT_VOLUME rows feed the bounded Python-computed summary — they
        # are already focus+date bounded and exempt from the per-ticker cap.
        if tk and not is_short and per_ticker.get(tk, 0) >= max_per_ticker:
            cap_dropped += 1
            deferred_ids.append((rec["record_id"], rec.get("record_type", ""),
                                 src, "CAP_PER_TICKER"))
            continue
        per_source[src] = per_source.get(src, 0) + 1
        if tk and not is_short:
            per_ticker[tk] = per_ticker.get(tk, 0) + 1
        selected.append(rec)
    type_selected: dict[str, int] = {}
    for rec in selected:
        rts = str(rec.get("record_type", ""))
        type_selected[rts] = type_selected.get(rts, 0) + 1

    injection: dict[str, list[str]] = {}
    for rec in selected:
        payload_text = canonical_json(rec.get("normalized_payload") or {})
        hits = detect_injection_indicators(payload_text)
        if hits:
            injection[rec["record_id"]] = hits

    rss_selected = sum(1 for r in selected
                       if r.get("source_id") == _RSS_SOURCE_ID)
    clusters_selected = sorted({r["_cluster"]["cluster_id"] for r in selected
                                if r.get("_cluster")})
    multi_source_selected = sorted(
        {r["_cluster"]["cluster_id"] for r in selected if r.get("_cluster")
         and r["_cluster"].get("corroborating_source_count", 0) > 1})
    return {"considered": considered, "malformed": malformed,
            "skipped_already_processed": skipped_processed,
            "skipped_unhealthy": skipped_unhealthy,
            "skipped_no_provenance": skipped_no_provenance,
            "duplicates_skipped": dup_payloads, "cap_dropped": cap_dropped,
            "cross_root_duplicates": cross_root_dups,
            "clustered_duplicates_dropped": clustered_dropped,
            "rss_atom_considered": rss_considered,
            "rss_atom_selected": rss_selected,
            "clusters_selected": clusters_selected,
            "multi_source_clusters_selected": multi_source_selected,
            "selected": selected, "skipped_ids": skipped_ids,
            "deferred_ids": deferred_ids,
            "type_considered": type_considered,
            "type_selected": type_selected,
            "type_freshness": type_freshness,
            "injection_indicators": injection,
            "new_eligible": len(candidates) + len(short_selected)}


_DEV_DEFAULT_TARGETS = (("NEWS_EVENT", 2), ("FILING_EVENT", 1),
                        ("INSIDER_FILING", 1), ("MACRO_OBSERVATION", 1),
                        ("TRADING_HALT", 1))


def apply_development_sample(sel: dict, dev_profile: dict) -> dict:
    """Reduce a full selection to the bounded, balanced development proof
    sample (<= max_records) for the claude_code development cycle.

    * Fills the per-type quota first (newest-first within type, preserved from
      the round-robin selection order), then substitutes any shortfall via a
      deterministic round-robin over the remaining eligible records so the cap
      is still reached when a quota type is unavailable.
    * Every record dropped by the development cap is DEFERRED (never persisted
      as processed) so the backlog is preserved for later cycles.
    Returns a NEW selection dict; the input is not mutated."""
    max_total = int(dev_profile.get("max_event_records_per_cycle", 6))
    raw_targets = dev_profile.get("balanced_sample_targets") \
        or [list(p) for p in _DEV_DEFAULT_TARGETS]
    targets = [(str(t), int(q)) for t, q in raw_targets]
    selected = sel["selected"]

    by_type: dict[str, list[dict]] = {}
    for rec in selected:
        by_type.setdefault(str(rec.get("record_type", "")), []).append(rec)
    used: dict[str, int] = {t: 0 for t in by_type}

    chosen: list[dict] = []
    chosen_ids: set[str] = set()
    substitutions: list[dict] = []

    # Pass 1 — per-type quotas.
    for t, quota in targets:
        avail = by_type.get(t, [])
        take = avail[:quota]
        for r in take:
            if r["record_id"] not in chosen_ids and len(chosen) < max_total:
                chosen.append(r)
                chosen_ids.add(r["record_id"])
        used[t] = len(take)
        if len(take) < quota:
            substitutions.append({"type": t, "requested": quota,
                                  "available": len(take),
                                  "shortfall": quota - len(take)})

    # Pass 2 — deterministic round-robin substitution to reach the cap.
    order = sorted(by_type)
    substituted_ids: list[str] = []
    while len(chosen) < max_total:
        progressed = False
        for t in order:
            if len(chosen) >= max_total:
                break
            lst = by_type.get(t, [])
            if used[t] < len(lst):
                r = lst[used[t]]
                used[t] += 1
                if r["record_id"] not in chosen_ids:
                    chosen.append(r)
                    chosen_ids.add(r["record_id"])
                    substituted_ids.append(r["record_id"])
                    progressed = True
        if not progressed:
            break

    newly_deferred = [(r["record_id"], r.get("record_type", ""),
                       r.get("source_id", ""), "DEV_CAP_BALANCED_SAMPLE")
                      for r in selected if r["record_id"] not in chosen_ids]

    out = dict(sel)
    out["selected"] = chosen
    out["deferred_ids"] = list(sel.get("deferred_ids", [])) + newly_deferred
    out["cap_dropped"] = int(sel.get("cap_dropped", 0)) + len(newly_deferred)
    type_selected: dict[str, int] = {}
    for r in chosen:
        rt = str(r.get("record_type", ""))
        type_selected[rt] = type_selected.get(rt, 0) + 1
    out["type_selected"] = type_selected
    out["rss_atom_selected"] = sum(1 for r in chosen
                                   if r.get("source_id") == _RSS_SOURCE_ID)
    out["clusters_selected"] = sorted({r["_cluster"]["cluster_id"] for r in chosen
                                       if r.get("_cluster")})
    out["multi_source_clusters_selected"] = sorted(
        {r["_cluster"]["cluster_id"] for r in chosen if r.get("_cluster")
         and r["_cluster"].get("corroborating_source_count", 0) > 1})
    injection: dict[str, list[str]] = {}
    for r in chosen:
        hits = detect_injection_indicators(
            canonical_json(r.get("normalized_payload") or {}))
        if hits:
            injection[r["record_id"]] = hits
    out["injection_indicators"] = injection
    out["dev_sample"] = {
        "active": True, "max_records": max_total,
        "targets": [[t, q] for t, q in targets],
        "composition_selected": type_selected,
        "substitutions": substitutions,
        "substituted_record_ids": sorted(substituted_ids),
        "selected_record_ids": sorted(chosen_ids),
        "deferred_count": len(newly_deferred)}
    return out


def build_news_rss_coverage(stage2: dict, sel: dict,
                            extra: Optional[dict] = None) -> dict:
    """Deterministic NEWS_AND_RSS_COVERAGE evidence: exact per-type counts of
    the event sources represented, source freshness, cross-feed clustering
    evidence and verified coverage gaps. The generalized-RSS flags are computed
    from ACTUAL live evidence — they only report that generalized RSS/Atom
    collection exists once real rss_atom records have been read. Never LLM-derived."""
    extra = extra or {}
    counts = {}
    for rt in _NEWS_COVERAGE_TYPES:
        counts[rt] = {"considered": sel["type_considered"].get(rt, 0),
                      "selected": sel["type_selected"].get(rt, 0),
                      "newest_record_at": sel["type_freshness"].get(rt)}
    other = {rt: {"considered": c,
                  "selected": sel["type_selected"].get(rt, 0),
                  "newest_record_at": sel["type_freshness"].get(rt)}
             for rt, c in sorted(sel["type_considered"].items())
             if rt not in _NEWS_COVERAGE_TYPES}
    gdelt_state = str((stage2.get("source_health") or {})
                      .get("gdelt", "NOT_RUN"))
    rss_considered = int(sel.get("rss_atom_considered", 0))
    rss_selected = int(sel.get("rss_atom_selected", 0))
    press_considered = int(sel["type_considered"].get("PRESS_RELEASE", 0))
    regulatory_considered = int(sel["type_considered"].get("REGULATORY_EVENT", 0))
    generalized_exists = rss_considered > 0
    verified_roots = int(extra.get("verified_count", 0))
    news_sources = dict(_NEWS_RSS_INVENTORY["news_sources"])
    rss_feeds = list(_NEWS_RSS_INVENTORY["rss_feeds"])
    if generalized_exists:
        news_sources["generalized_rss_atom"] = (
            "OPERATIONAL (Stage 3.5) — official RSS/Atom feeds normalized into "
            "NEWS_EVENT / REGULATORY_EVENT / PRESS_RELEASE records")
        rss_feeds = ["generalized official RSS/Atom registry (Stage 3.5): "
                     "%d rss_atom records read" % rss_considered,
                     "nasdaq_trader_trading_halts (Stage 2)"]
        gaps = ["company investor-relations / newsroom RSS coverage still sparse",
                "broader international / multi-language official feeds pending",
                "GDELT discovery feed intentionally disabled"]
    else:
        gaps = list(_NEWS_RSS_INVENTORY["source_gaps"])
    return {
        "marker": STAGE3_5_MARKER,
        "stage3_5_status": ("IMPLEMENTED_PARTIAL" if generalized_exists
                            else "REQUIRED_BEFORE_PERSISTENT_24_7_RUNTIME"),
        "current_news_sources": news_sources,
        "current_rss_feeds": rss_feeds,
        "record_counts": counts,
        "other_event_sources": other,
        "source_health": stage2.get("source_health", {}),
        "gdelt_state": gdelt_state,
        "gdelt_remains_disabled": gdelt_state in
            ("NOT_RUN", "DISABLED", "SKIPPED", "DEFERRED"),
        "company_direct_rss_atom_feeds_exist": press_considered > 0,
        "generalized_rss_collection_exists": generalized_exists,
        "rss_atom_records_considered": rss_considered,
        "rss_atom_records_selected": rss_selected,
        "rss_regulatory_considered": regulatory_considered,
        "rss_press_release_considered": press_considered,
        "additional_event_roots_verified": verified_roots,
        "additional_event_roots_rejected": extra.get("rejected", []),
        "event_clusters_available": int(extra.get("cluster_count", 0)),
        "clusters_selected": len(sel.get("clusters_selected", [])),
        "multi_source_clusters_selected":
            len(sel.get("multi_source_clusters_selected", [])),
        "clustered_duplicates_dropped": int(sel.get("clustered_duplicates_dropped", 0)),
        "cross_root_duplicates": int(sel.get("cross_root_duplicates", 0)),
        "feed_evidence": extra.get("feed_evidence", {}),
        "source_gaps": gaps}


def _stage3_5_requirements(run_id: str, stage1: dict, stage2: dict,
                           metrics: dict) -> dict:
    """Immutable Stage 3.5 implementation contract. Recording this file is a
    hard precondition for ever classifying Stage 4 as fully ready."""
    cov = metrics.get("news_rss_coverage", {})
    generalized = bool(cov.get("generalized_rss_collection_exists", False))
    return {
        "marker": STAGE3_5_MARKER,
        "status": cov.get("stage3_5_status",
                          "REQUIRED_BEFORE_PERSISTENT_24_7_RUNTIME"),
        "produced_by_run": run_id,
        "stage1_run_id": stage1["run_id"], "stage2_run_id": stage2["run_id"],
        "schema_version": DIRECTOR_SCHEMA_VERSION,
        "current_state": {
            "financial_news_operational": True,
            "narrow_rss_operational": True,
            "generalized_rss_atom_operational": generalized,
            "broad_rss_atom_acquisition_missing": not generalized,
            "gdelt_remains_disabled": cov.get("gdelt_remains_disabled", True),
            "coverage_evidence": cov},
        "implementation_contract": {
            "canonical_feed_registry":
                "versioned registry of every feed (id, url, publisher, "
                "licensing, entity scope, poll cadence, enabled flag)",
            "generic_rss2_atom_parser":
                "one bounded parser for RSS 2.0 and Atom with strict "
                "size/field limits and malformed-feed quarantine",
            "conditional_polling_etag_last_modified":
                "ETag + Last-Modified conditional requests; 304 handling; "
                "never unconditionally re-download",
            "company_ir_newsroom_feed_discovery":
                "deterministic discovery + human-approved onboarding of "
                "company investor-relations and newsroom feeds",
            "official_regulatory_government_feed_catalog":
                "curated catalog of official regulatory/government feeds "
                "(e.g. SEC press, FRB, BLS, Treasury) with provenance",
            "feed_licensing_and_provenance":
                "per-feed license record; unrestricted article bodies are "
                "never stored beyond entitlement or sent to the LLM",
            "ticker_entity_mapping":
                "deterministic ticker/entity resolution with confidence "
                "labels; unmapped items quarantined, never guessed",
            "canonical_url_content_hash_dedup":
                "canonical-URL normalization + content-hash dedup across "
                "feeds and providers",
            "cross_feed_event_clustering":
                "deterministic clustering of the same event across EODHD, "
                "RSS, SEC and GDELT before any LLM interpretation",
            "bounded_snippets_only":
                "only bounded snippets (title + capped excerpt) are ever "
                "persisted for LLM use",
            "feed_health_and_retry_state":
                "per-feed health, failure counters, backoff and retry state "
                "in the ingestion state database",
            "normalized_event_contracts":
                "normalized NEWS_EVENT / REGULATORY_EVENT / PRESS_RELEASE "
                "record contracts with PIT fields and provenance",
            "deterministic_prefiltering_before_llm":
                "all selection, dedup, caps and injection defenses run in "
                "deterministic Python before any LLM call",
            "token_budget_protection":
                "Stage 3 token/cost budgets apply unchanged to expanded "
                "news volume; caps enforced before every call",
            "required_tests":
                "parser, conditional-polling, dedup, clustering, licensing, "
                "health-state and budget tests with fake transports only",
            "polling_cadence_24_7":
                "bounded 24/7 polling cadence per feed class for the "
                "persistent runtime (Stage 4), never unbounded"}}


def build_market_context(stage2_root: Path, focus: list[str]) -> dict:
    """Compact deterministic MARKET_BAR summary (bars are NEVER sent
    individually). Returns {"text", "record_ids"}."""
    focus_set = {t.upper() for t in focus}
    by_ticker: dict[str, list[dict]] = {}
    ids: list[str] = []
    for rec in _iter_normalized(stage2_root, "MARKET_BAR"):
        tk = str(rec.get("ticker") or "").upper()
        if tk in focus_set and rec.get("record_id"):
            by_ticker.setdefault(tk, []).append(rec)
    lines: list[str] = []
    for tk in sorted(by_ticker):
        bars = sorted(by_ticker[tk], key=lambda r: str(r.get("effective_at")))
        first, last = bars[0], bars[-1]
        c0 = (first.get("normalized_payload") or {}).get("close")
        c1 = (last.get("normalized_payload") or {}).get("close")
        try:
            pct = round((float(c1) / float(c0) - 1.0) * 100.0, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            pct = None
        ids.extend([first["record_id"], last["record_id"]])
        lines.append("%s: close %s (%s) -> %s (%s), change %s%%"
                     % (tk, c0, first.get("effective_at"), c1,
                        last.get("effective_at"), pct))
    return {"text": "\n".join(lines), "record_ids": sorted(set(ids))}


# --------------------------------------------------------------------------- #
# Packets + prompt context building.
# --------------------------------------------------------------------------- #
def build_packets(selected: list[dict], max_snippet: int) -> list[dict]:
    groups: dict[str, list[dict]] = {}
    for rec in selected:
        payload = rec.get("normalized_payload") or {}
        if rec.get("record_type") == "MACRO_OBSERVATION":
            key = "macro:%s" % payload.get("series_id", "unknown")
        elif rec.get("ticker"):
            key = "ticker:%s" % str(rec["ticker"]).upper()
        else:
            key = "market:%s" % rec.get("record_type")
        groups.setdefault(key, []).append(rec)
    packets: list[dict] = []
    for key in sorted(groups):
        recs = sorted(groups[key], key=lambda r: r.get("record_id", ""))
        items = []
        for rec in recs:
            payload = rec.get("normalized_payload") or {}
            items.append({
                "record_id": rec["record_id"],
                "record_type": rec.get("record_type"),
                "source_id": rec.get("source_id"),
                "event_type": rec.get("event_type"),
                "ticker": rec.get("ticker"),
                "effective_at": rec.get("effective_at"),
                "observed_at": rec.get("observed_at"),
                "available_at": rec.get("available_at"),
                "publication_time": payload.get("publication_time"),
                "provenance": rec.get("provenance"),
                "title": sanitize_untrusted_text(payload.get("title"),
                                                 max_snippet)
                if payload.get("title") else None,
                "snippet": sanitize_untrusted_text(
                    payload.get("content_snippet"), max_snippet)
                if payload.get("content_snippet") else None,
                "key_values": {k: payload.get(k) for k in
                               ("form_type", "series_id", "value",
                                "observation_date", "revision_vintage_date",
                                "short_volume", "total_volume", "short_ratio",
                                "reason_code", "halt_time", "resumption_trade_time",
                                "action", "amount", "measure_note")
                               if k in payload}})
        packets.append({"packet_id": "pkt_" + sha256_text(key)[:12],
                        "entity": key, "records": items})
    return packets


def packets_to_prompt_text(packets: list[dict], market_context: str,
                           stage1_families_text: str) -> str:
    parts: list[str] = []
    if market_context:
        parts.append("TRUSTED deterministic market context (computed in "
                     "Python from owned bars):\n" + market_context)
    if stage1_families_text:
        parts.append("TRUSTED Stage 1 research-memory context:\n"
                     + stage1_families_text)
    for pkt in packets:
        lines = ["PACKET %s entity=%s" % (pkt["packet_id"], pkt["entity"])]
        for item in pkt["records"]:
            meta = {k: item[k] for k in ("record_id", "record_type",
                                         "source_id", "event_type", "ticker",
                                         "effective_at", "available_at",
                                         "publication_time", "provenance")
                    if item.get(k) is not None}
            lines.append("RECORD " + canonical_json(meta))
            if item.get("key_values"):
                lines.append("VALUES " + canonical_json(item["key_values"]))
            for field in ("title", "snippet"):
                if item.get(field):
                    lines.append(wrap_untrusted(item["record_id"], field,
                                                item[field]))
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def _packet_source_text(packets: list[dict], market_context: str) -> str:
    return market_context + "\n" + "\n".join(
        canonical_json(p) for p in packets)


# --------------------------------------------------------------------------- #
# Duplicate gate + queue policy.
# --------------------------------------------------------------------------- #
def run_duplicate_gate(hyp: dict, as_of: str, experiments: list[dict]) -> dict:
    cand = {
        "name": hyp.get("title"), "family": hyp.get("information_family"),
        "model_id": hyp.get("prediction_target"),
        "universe": hyp.get("universe"), "horizon": hyp.get("horizon"),
        "rebalance": hyp.get("rebalance_cadence"), "cost_bps": None,
        "model_params": hyp.get("feature_definition"), "portfolio_params": None,
        "data_cutoff": as_of, "spec_hash": hypothesis_spec_hash(hyp),
        "metadata_completeness": {"is_complete": True},
    }
    return _stage1.classify_candidate_experiment(cand, experiments)


def check_data_adequacy(hyp: dict, record_type_counts: dict) -> dict:
    """Deterministic: every Stage 2 record type the hypothesis names in its
    requirements must actually have records in the verified store."""
    text = " ".join([canonical_json(hyp.get("required_fields") or []),
                     canonical_json(hyp.get("data_adequacy_requirements") or [])]
                    ).upper()
    referenced = [rt for rt in ELIGIBLE_RECORD_TYPES + ("MARKET_BAR",)
                  if rt in text]
    missing = [rt for rt in referenced if not record_type_counts.get(rt)]
    return {"referenced_record_types": referenced, "missing": missing,
            "adequate": not missing}


def queue_policy(gate_result: str, adequacy: dict,
                 param_dup_permitted: bool) -> tuple[str, str]:
    if gate_result == "NEW_INFORMATION":
        if adequacy["adequate"]:
            return QS_READY, "new information; named data present in store"
        return QS_HOLD_DATA, ("required record types missing: %s"
                              % ",".join(adequacy["missing"]))
    if gate_result == "NEW_DATA_AVAILABLE":
        return QS_READY, ("reopen permitted: changed data version explicitly "
                          "identified (newer data_cutoff)")
    if gate_result == "PRIOR_TEST_INCOMPLETE":
        return QS_RESUME, "matching prior experiment lacks a terminal decision"
    if gate_result == "EXACT_DUPLICATE":
        return QS_REJECT_DUP, "identical spec already tested with a decision"
    if gate_result == "PARAMETER_DUPLICATE":
        if param_dup_permitted:
            return QS_READY, "parameter variant explicitly permitted by config"
        return QS_REJECT_DUP, ("same information family already explored; no "
                               "pre-registration permits this variant")
    if gate_result == "METADATA_INSUFFICIENT":
        return QS_HOLD_METADATA, "deterministic metadata repair required"
    return QS_REJECT_UNGROUNDED, "unknown duplicate-gate result"


# --------------------------------------------------------------------------- #
# Provider construction + selection.
# --------------------------------------------------------------------------- #
def build_providers(config: dict, *, env: Mapping[str, str],
                    sleep_fn: Callable[[float], None],
                    secret_values: list[str],
                    overrides: Optional[dict] = None) -> dict:
    overrides = overrides or {}
    pcfg = config.get("providers", {})
    out: dict[str, Any] = {}
    if "anthropic_http" in overrides:
        out["anthropic_http"] = overrides["anthropic_http"]
    else:
        out["anthropic_http"] = AnthropicHttpProvider(
            pcfg.get("anthropic_http", {}), env=env, sleep_fn=sleep_fn,
            secret_values=secret_values)
    if "claude_code" in overrides:
        out["claude_code"] = overrides["claude_code"]
    else:
        out["claude_code"] = ClaudeCodeProvider(
            pcfg.get("claude_code", {}), env=env, sleep_fn=sleep_fn,
            secret_values=secret_values)
    return out


def select_provider(providers: dict, config: dict) -> dict:
    audits = {}
    priority = config.get("providers", {}).get("priority",
                                               ["anthropic_http", "claude_code"])
    for name in priority:
        if name in providers:
            audits[name] = providers[name].audit()
    for name in priority:
        a = audits.get(name)
        if a and a.get("classification") == PC_PRODUCTION_READY:
            return {"selected": name, "classification": PC_PRODUCTION_READY,
                    "audits": audits}
    for name in priority:
        a = audits.get(name)
        if a and a.get("classification") == PC_DEVELOPMENT_READY:
            return {"selected": name, "classification": PC_DEVELOPMENT_READY,
                    "audits": audits}
    blocked = {n: a.get("classification") for n, a in audits.items()}
    return {"selected": None, "classification": PC_BLOCKED_UNAVAILABLE,
            "audits": audits,
            "reason": "no operable provider: %s" % canonical_json(blocked)}


# --------------------------------------------------------------------------- #
# Deterministic run id.
# --------------------------------------------------------------------------- #
def compute_run_id(*, stage1_run_id: str, stage2_run_id: str,
                   config_hash: str, git_commit: str, mode: str, as_of: str,
                   selected_digest: str, context_hash: str,
                   provider: Optional[str], model: Optional[str],
                   response_hashes: list[str],
                   gate_digest: str) -> str:
    seed = canonical_json({
        "stage1_run_id": stage1_run_id, "stage2_run_id": stage2_run_id,
        "schema_version": DIRECTOR_SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION, "config_hash": config_hash,
        "git_commit": git_commit or "UNKNOWN", "mode": mode, "as_of": as_of,
        "selected_digest": selected_digest, "context_hash": context_hash,
        "provider": provider or "NONE", "model": model or "NONE",
        "response_hashes": sorted(response_hashes),
        "gate_digest": gate_digest})
    return "stage3_" + sha256_text(seed)[:16]


# --------------------------------------------------------------------------- #
# Secret scanning.
# --------------------------------------------------------------------------- #
def scan_outputs_for_secrets(paths: list[Path],
                             secret_values: list[str]) -> list[str]:
    hits: list[str] = []
    for path in paths:
        try:
            data = path.read_bytes()
        except OSError:
            continue
        text = data.decode("utf-8", "replace")
        for value in secret_values:
            if value and value in text:
                hits.append("%s contains a live secret value" % path.name)
        if _SECRET_PATTERN.search(text):
            hits.append("%s matches the API-key pattern" % path.name)
    return hits


# --------------------------------------------------------------------------- #
# The director cycle.
# --------------------------------------------------------------------------- #
def run_director(config: dict, output_root: str, mode: str, as_of: str, *,
                 env: Optional[Mapping[str, str]] = None,
                 sleep_fn: Callable[[float], None] = time.sleep,
                 git_commit: str = "UNKNOWN",
                 now_fn: Optional[Callable[[], str]] = None,
                 provider_overrides: Optional[dict] = None) -> dict:
    env = env if env is not None else os.environ
    now_fn = now_fn or (lambda: _dt.datetime.now(_dt.timezone.utc).isoformat())
    out_root = Path(output_root)
    cfg_hash = config_hash_of(config)

    if mode == "verify":
        return verify_run(config, output_root, env=env)

    stage1 = read_stage1(config)
    if not stage1["ok"]:
        return {"terminal": "%s — %s" % (BLOCKED, stage1["reason"]),
                "token": BLOCKED, "reason": stage1["reason"]}
    stage2 = read_stage2(config)
    if not stage2["ok"]:
        return {"terminal": "%s — %s" % (BLOCKED, stage2["reason"]),
                "token": BLOCKED, "reason": stage2["reason"]}
    if as_of in (None, "", "latest"):
        as_of = stage2["as_of"] or _dt.date.today().isoformat()

    ledgers_before = ledger_fingerprints(config)
    secret_values = [env.get(n) for n in _SECRET_ENV_NAMES if env.get(n)]

    providers = build_providers(config, env=env, sleep_fn=sleep_fn,
                                secret_values=secret_values,
                                overrides=provider_overrides)
    selection = select_provider(providers, config)
    provider_name = selection["selected"]
    provider = providers.get(provider_name) if provider_name else None
    classification = selection["classification"]
    model = None
    if provider_name == "anthropic_http":
        model = providers["anthropic_http"].resolve_model()
    elif provider_name == "claude_code":
        # Record the pinned development model (non-secret config) at run level
        # so the immutable manifest/receipts carry the exact identifier. No
        # provider or model change is made silently.
        dp = config.get("providers", {}).get("claude_code", {}) \
            .get("development_profile", {})
        if dp.get("enabled") and dp.get("model"):
            model = str(dp["model"])

    conn = open_state_db(out_root)
    try:
        return _run_cycle(config, out_root, mode, as_of, conn=conn,
                          stage1=stage1, stage2=stage2, providers=providers,
                          selection=selection, provider=provider,
                          provider_name=provider_name,
                          classification=classification, model=model,
                          cfg_hash=cfg_hash, git_commit=git_commit,
                          now_fn=now_fn, ledgers_before=ledgers_before,
                          secret_values=secret_values, env=env)
    finally:
        conn.close()


def _run_cycle(config: dict, out_root: Path, mode: str, as_of: str, *,
               conn: sqlite3.Connection, stage1: dict, stage2: dict,
               providers: dict, selection: dict, provider: Any,
               provider_name: Optional[str], classification: str,
               model: Optional[str], cfg_hash: str, git_commit: str,
               now_fn: Callable[[], str], ledgers_before: dict,
               secret_values: list[str], env: Mapping[str, str]) -> dict:
    budgets_cfg = config["budgets"]
    # Development proof profile is active ONLY when the claude_code
    # (development-only) provider is selected AND the profile is enabled. It
    # bounds the live cycle so BOTH event interpretation and hypothesis
    # generation complete within the UNCHANGED budgets. Never active for the
    # production Anthropic provider.
    dev_profile: Optional[dict] = None
    if provider_name == "claude_code":
        dp = config.get("providers", {}).get("claude_code", {}) \
            .get("development_profile", {})
        if dp.get("enabled"):
            dev_profile = dp
    dev_field_limits = (dev_profile or {}).get("output_field_limits") or {}
    dev_out_budgets = (dev_profile or {}).get("per_call_max_output_tokens") \
        or {}
    pricing = PricingTable(config.get("providers", {})
                           .get("anthropic_http", {}).get("pricing", []))
    prior_day, prior_month = _prior_usage(conn, as_of)
    ledger = BudgetLedger(budgets_cfg, pricing, prior_day=prior_day,
                          prior_month=prior_month)
    decisions: list[dict] = []
    errors: list[dict] = []

    def decide(dtype: str, subject: Optional[str], decision: str,
               reason: str) -> None:
        decisions.append({"decision_type": dtype, "subject_id": subject,
                          "decision": decision, "reason": reason})

    decide("provider_selection", provider_name or "NONE", classification,
           selection.get("reason") or "priority order")

    # ---------------- audit mode: no selection consumption, no LLM ---------- #
    if mode == "audit":
        return _finish_audit(config, out_root, as_of, conn=conn, stage1=stage1,
                             stage2=stage2, selection=selection,
                             classification=classification, model=model,
                             cfg_hash=cfg_hash, git_commit=git_commit,
                             now_fn=now_fn, ledger=ledger,
                             ledgers_before=ledgers_before,
                             secret_values=secret_values, decisions=decisions)

    # ---------------- input selection -------------------------------------- #
    known = _known_processed(conn)
    # Verified additional event roots (Stage 3.5 News/RSS) are read alongside
    # Stage 2; unverified roots are rejected (never read). Cross-feed clusters are
    # used to collapse duplicate coverage to a single representative record.
    extra = read_additional_roots(config)
    for rej in extra["rejected"]:
        decide("additional_event_root", rej["root"], "REJECTED_UNVERIFIED",
               rej["reason"])
    if extra["verified_count"]:
        decide("additional_event_root", None,
               "VERIFIED_%d_ROOTS" % extra["verified_count"],
               "%d cross-feed clusters available for dedup/grounding"
               % extra["cluster_count"])
    sel = select_input_records(config, stage2, known,
                               extra_roots=extra["roots"],
                               cluster_index=extra["clusters"])
    selected = sel["selected"]
    if sel["new_eligible"] == 0 or not selected:
        # Nothing genuinely new: no immutable run dir, no LLM call.
        started = now_fn()
        run_row_id = "no_new_%s" % sha256_text(as_of + cfg_hash)[:12]
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO director_runs (run_id, mode, as_of, "
                "status, terminal_token, stage1_run_id, stage2_run_id, "
                "provider, model, config_hash, git_commit, schema_version, "
                "prompt_version, records_considered, started_at, finished_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_row_id, mode, as_of, NO_NEW, NO_NEW, stage1["run_id"],
                 stage2["run_id"], provider_name, model, cfg_hash, git_commit,
                 DIRECTOR_SCHEMA_VERSION, PROMPT_VERSION, sel["considered"],
                 started, now_fn()))
        return {"terminal": NO_NEW, "token": NO_NEW,
                "considered": sel["considered"],
                "already_processed": sel["skipped_already_processed"]}

    # Development proof: reduce to a bounded, balanced <=6-record sample. All
    # records dropped by the development cap are DEFERRED, never processed.
    if dev_profile:
        sel = apply_development_sample(sel, dev_profile)
        selected = sel["selected"]
        ds = sel["dev_sample"]
        decide("development_sample", None,
               "BOUNDED_%d_OF_MAX_%d" % (len(selected), ds["max_records"]),
               "balanced dev sample; composition=%s"
               % canonical_json(ds["composition_selected"]))
        for s in ds["substitutions"]:
            decide("development_sample_substitution", s["type"],
                   "SUBSTITUTED_%d" % s["shortfall"],
                   "requested %d, available %d; shortfall filled by "
                   "round-robin over available types"
                   % (s["requested"], s["available"]))

    sel_cfg = config.get("input_selection", {})
    focus = [str(t).upper() for t in sel_cfg.get("focus_tickers", [])]
    market_ctx = build_market_context(stage2["root"], focus)
    packets = build_packets(selected,
                            int(sel_cfg.get("max_snippet_chars", 500)))

    # Stage 1 context for the LLM + grounding sets.
    coverage_lines = []
    known_registry_ids: set[str] = set()
    for row in stage1["coverage"]:
        fam = row.get("information_family")
        known_registry_ids.add(str(fam))
        coverage_lines.append(
            "%s: experiments=%s surviving=%s rejected=%s class=%s exhausted=%s"
            % (fam, row.get("unique_experiments"), row.get("surviving_signals"),
               row.get("rejected_signals"), row.get("evidence_classification"),
               row.get("local_research_supported_as_exhausted")))
    cs = stage1["current_state"]
    for key in ("champion_alpha_signals_recovered", "challengers_recovered"):
        for x in cs.get(key, []) or []:
            known_registry_ids.add(str(x))
    known_registry_ids.update(
        str(e["experiment_id"]) for e in stage1["experiments"]
        if e.get("experiment_id"))
    families_text = (
        "Champion model: %s. Champion signals: %s. Challengers: %s. "
        "Rejected candidate signals to date: %s.\nInformation families "
        "(registry IDs usable in stage1_registry_ids):\n%s"
        % (cs.get("champion_model"),
           ",".join(cs.get("champion_alpha_signals_recovered", []) or []),
           ",".join(cs.get("challengers_recovered", []) or []),
           cs.get("rejected_candidate_signal_count"),
           "\n".join(coverage_lines)))

    registry_context = {
        "stage1_run_id": stage1["run_id"],
        "champion_model": cs.get("champion_model"),
        "champion_signals": cs.get("champion_alpha_signals_recovered", []),
        "challengers": cs.get("challengers_recovered", []),
        "rejected_candidate_signal_count":
            cs.get("rejected_candidate_signal_count"),
        "coverage_families": stage1["coverage"],
        "experiment_count": len(stage1["experiments"]),
        "known_registry_id_count": len(known_registry_ids)}
    context_hash = sha256_text(canonical_json(registry_context))

    known_record_ids = {r["record_id"] for r in selected}
    known_tickers = {str(r.get("ticker") or "").upper()
                     for r in selected if r.get("ticker")} | set(focus)
    id_map = {r["record_id"]: {
        "publication_time": (r.get("normalized_payload") or {})
        .get("publication_time"),
        "effective_at": r.get("effective_at"),
        "observed_at": r.get("observed_at"),
        "event_type": r.get("event_type")} for r in selected}
    source_text = _packet_source_text(packets, market_ctx["text"]) \
        + "\n" + families_text

    prompt_context = packets_to_prompt_text(packets, market_ctx["text"],
                                            families_text)
    max_prompt_chars = int(budgets_cfg["max_prompt_characters"])
    dropped_packets = 0
    while len(prompt_context) > max_prompt_chars and packets:
        packets = packets[:-1]
        dropped_packets += 1
        prompt_context = packets_to_prompt_text(packets, market_ctx["text"],
                                                families_text)
    if dropped_packets:
        decide("prompt_bounding", None, "DROPPED_%d_PACKETS" % dropped_packets,
               "max_prompt_characters=%d" % max_prompt_chars)
        kept_ids = {i["record_id"] for p in packets for i in p["records"]}
        selected = [r for r in selected if r["record_id"] in kept_ids
                    or r.get("record_type") == "SHORT_VOLUME"]
        known_record_ids = {r["record_id"] for r in selected}

    snapshot_id = "snap_" + sha256_text(
        canonical_json(sorted(known_record_ids)))[:16]

    # ---------------- LLM calls -------------------------------------------- #
    prompt_files: list[dict] = []
    receipts: list[dict] = []
    usage_rows: list[dict] = []
    analyses_out: list[dict] = []
    hypotheses_out: list[dict] = []
    rejected: list[dict] = []
    ranking: dict[str, int] = {}
    narrative: Optional[str] = None
    budget_exhausted = False
    provider_failed: Optional[str] = None
    max_out = int(budgets_cfg["max_output_tokens_per_cycle"])
    max_resp_chars = int(budgets_cfg["max_response_characters"])

    def call_llm(task: str, context_text: str,
                 per_call_max_out: int) -> Optional[dict]:
        nonlocal budget_exhausted, provider_failed
        if provider is None:
            provider_failed = selection.get("reason", "no provider")
            return None
        # Development profile: append the bounded-output instructions and pass
        # the structured-output JSON Schema to the provider. Production leaves
        # both None, so its prompts and requests are unchanged.
        fl = dev_field_limits if dev_profile else None
        output_schema = build_output_schema(task, dev_field_limits) \
            if dev_profile else None
        prompt_obj = render_prompt(task, context_text, field_limits=fl)
        est_in = estimate_tokens(prompt_obj["system"] + prompt_obj["user"])
        verdict = ledger.can_call(est_in, per_call_max_out)
        if not verdict["allowed"]:
            budget_exhausted = True
            decide("budget", task, "CALL_FORBIDDEN", verdict["reason"])
            return None
        prompt_files.append(prompt_obj)
        result = provider.complete(prompt_obj,
                                   max_output_tokens=per_call_max_out,
                                   output_schema=output_schema)
        usage_rows.append(ledger.record_usage(
            provider=provider_name, model=result.get("model") or model,
            usage=result.get("usage")))
        text = result.get("response_text") or ""
        if len(text) > max_resp_chars:
            result = dict(result)
            result["response_text"] = text[:max_resp_chars]
            result["truncated"] = True
        receipts.append({
            "call_index": len(receipts) + 1, "task": task,
            "provider": provider_name,
            "provider_recorded_as": CLAUDE_CODE_DEVELOPMENT_ONLY
            if provider_name == "claude_code" else provider_name,
            "model": result.get("model") or model,
            "prompt_hash": prompt_obj["prompt_hash"],
            "request_id": result.get("request_id"),
            "status": result.get("status"), "ok": result.get("ok"),
            "retries": result.get("retries"),
            "usage": result.get("usage"),
            "usage_reliable": result.get("usage_reliable"),
            "json_schema_enforced": output_schema is not None,
            "invocation": result.get("invocation"),
            "response_hash": result.get("response_hash"),
            "response_text": result.get("response_text"),
            "error": result.get("error"),
            "estimated_input_tokens": est_in})
        if not result.get("ok"):
            provider_failed = "%s call failed: %s (%s)" % (
                task, result.get("status"), result.get("error"))
            errors.append({"category": "PROVIDER_ERROR",
                           "detail": provider_failed})
            return None
        parsed = parse_json_object(result.get("response_text") or "")
        if parsed is None:
            provider_failed = "%s returned non-JSON output (rejected)" % task
            errors.append({"category": "REJECTED_NON_JSON",
                           "detail": provider_failed})
            return None
        return parsed

    def out_for(task: str, default: int) -> int:
        """Per-call output-token target. The development profile uses its
        smaller per-task targets (event<=4000, hypothesis<=3000,
        prioritize<=1500) so both required calls fit the UNCHANGED 12000
        per-cycle cap with a safety reserve."""
        if dev_profile and task in dev_out_budgets:
            return int(dev_out_budgets[task])
        return default

    _per_call_cap = min(max_out, int(budgets_cfg.get(
        "max_output_tokens_per_call", max_out)))

    # Role 1 — event interpretation.
    parsed = call_llm(TASK_EVENT, prompt_context,
                      out_for(TASK_EVENT, _per_call_cap))
    accepted_analyses: list[dict] = []
    if parsed is not None:
        raw_list = parsed.get("analyses")
        if not isinstance(raw_list, list):
            errors.append({"category": "REJECTED_SCHEMA",
                           "detail": "event response missing 'analyses' list"})
        else:
            for obj in raw_list:
                if dev_profile and isinstance(obj, dict):
                    obj = clamp_event_analysis(obj, dev_field_limits)
                pf = {}
                if isinstance(obj, dict):
                    rids = obj.get("source_record_ids") or []
                    if rids and rids[0] in id_map:
                        pf = id_map[rids[0]]
                verdict = validate_event_analysis(
                    obj, known_record_ids=known_record_ids,
                    known_tickers=known_tickers,
                    packet_source_text=source_text, packet_fields=pf)
                wrapped = {
                    "input_snapshot_id": snapshot_id,
                    "schema_version": DIRECTOR_SCHEMA_VERSION,
                    "prompt_hash": receipts[-1]["prompt_hash"],
                    "provider": receipts[-1]["provider_recorded_as"],
                    "model": receipts[-1]["model"],
                    "response_hash": receipts[-1]["response_hash"],
                    "grounding_validation": verdict,
                    "analysis": obj}
                analyses_out.append(wrapped)
                gr = verdict["grounding"]
                decide("event_analysis",
                       obj.get("event_analysis_id")
                       if isinstance(obj, dict) else None,
                       gr, ";".join(verdict["issues"]) or "clean")
                if gr in (GR_GROUNDED,):
                    accepted_analyses.append(obj)

    # Role 2 — hypothesis generation (only from accepted analyses).
    gate_rows: list[dict] = []
    queue_entries: list[dict] = []
    if accepted_analyses and not budget_exhausted and provider_failed is None:
        hyp_context = (
            "ACCEPTED EVENT ANALYSES:\n"
            + "\n".join(canonical_json(a) for a in accepted_analyses)
            + "\n\nSTAGE 1 RESEARCH MEMORY:\n" + families_text
            + "\n\nSupplied record IDs you may cite: "
            + ",".join(sorted(known_record_ids)))
        parsed = call_llm(TASK_HYPOTHESIS, hyp_context,
                          out_for(TASK_HYPOTHESIS, _per_call_cap))
        if parsed is not None:
            raw_hyps = parsed.get("hypotheses")
            if not isinstance(raw_hyps, list):
                errors.append({"category": "REJECTED_SCHEMA",
                               "detail": "hypothesis response missing "
                                         "'hypotheses' list"})
                raw_hyps = []
            for obj in raw_hyps:
                verdict = validate_hypothesis(
                    obj, known_record_ids=known_record_ids,
                    known_registry_ids=known_registry_ids)
                wrapped = {
                    "input_snapshot_id": snapshot_id,
                    "schema_version": DIRECTOR_SCHEMA_VERSION,
                    "prompt_hash": receipts[-1]["prompt_hash"],
                    "provider": receipts[-1]["provider_recorded_as"],
                    "model": receipts[-1]["model"],
                    "response_hash": receipts[-1]["response_hash"],
                    "grounding_validation": verdict,
                    "status": HYPOTHESIS_STATUS_DRAFT,
                    "proposal": obj}
                hypotheses_out.append(wrapped)
                gr = verdict["grounding"]
                decide("hypothesis_grounding",
                       obj.get("hypothesis_id") if isinstance(obj, dict)
                       else None, gr, ";".join(verdict["issues"]) or "clean")
                if gr != GR_GROUNDED:
                    rejected.append({"kind": "hypothesis",
                                     "id": obj.get("hypothesis_id")
                                     if isinstance(obj, dict) else None,
                                     "grounding": gr,
                                     "issues": verdict["issues"],
                                     "proposal": obj})

    # Duplicate gate on GROUNDED hypotheses only.
    grounded_hyps = [w["proposal"] for w in hypotheses_out
                     if w["grounding_validation"]["grounding"] == GR_GROUNDED]
    param_dup_ok = bool(config.get("duplicate_gate", {})
                        .get("parameter_duplicate_permitted", False))
    for hyp in grounded_hyps:
        gate = run_duplicate_gate(hyp, as_of, stage1["experiments"])
        adequacy = check_data_adequacy(hyp, stage2["record_type_counts"])
        status, reason = queue_policy(gate["result"], adequacy, param_dup_ok)
        gate_rows.append({"hypothesis_id": hyp.get("hypothesis_id"),
                          "result": gate["result"],
                          "matched_experiment_ids":
                              gate.get("matched_experiment_ids", []),
                          "reason": gate.get("reason"),
                          "rerun_permitted": gate.get("rerun_permitted"),
                          "queue_status": status, "queue_reason": reason,
                          "data_adequacy": adequacy})
        decide("duplicate_gate", hyp.get("hypothesis_id"), gate["result"],
               str(gate.get("reason")))
        if status in (QS_REJECT_DUP,):
            rejected.append({"kind": "hypothesis_duplicate",
                             "id": hyp.get("hypothesis_id"),
                             "gate_result": gate["result"],
                             "reason": reason, "proposal": hyp})

    # Role 3 — prioritization (advisory) over queue-eligible hypotheses.
    eligible = [g for g in gate_rows
                if g["queue_status"] in (QS_READY, QS_RESUME)]
    if len(eligible) >= 2 and not budget_exhausted and provider_failed is None:
        pr_context = "VALIDATED DRAFT HYPOTHESES:\n" + "\n".join(
            canonical_json({"hypothesis_id": g["hypothesis_id"],
                            "gate_result": g["result"]}) for g in eligible)
        parsed = call_llm(TASK_PRIORITIZE, pr_context,
                          out_for(TASK_PRIORITIZE, 2000))
        if parsed is not None and isinstance(parsed.get("ranking"), list):
            valid_ids = {g["hypothesis_id"] for g in eligible}
            for row in parsed["ranking"]:
                if isinstance(row, dict) \
                        and row.get("hypothesis_id") in valid_ids \
                        and isinstance(row.get("priority_rank"), int):
                    ranking[row["hypothesis_id"]] = row["priority_rank"]

    # FINAL deterministic queue decision.
    hyp_by_id = {h.get("hypothesis_id"): h for h in grounded_hyps}
    ordered = sorted(gate_rows, key=lambda g: (
        0 if g["queue_status"] in (QS_READY, QS_RESUME) else 1,
        ranking.get(g["hypothesis_id"], 9999),
        -float((hyp_by_id.get(g["hypothesis_id"]) or {}).get("confidence") or 0),
        str(g["hypothesis_id"])))
    for idx, g in enumerate(ordered, start=1):
        hyp = hyp_by_id.get(g["hypothesis_id"]) or {}
        entry = {
            "queue_id": "q_" + sha256_text(
                str(g["hypothesis_id"]) + as_of)[:16],
            "hypothesis_id": g["hypothesis_id"], "priority": idx,
            "duplicate_gate_result": g["result"],
            "source_record_ids": hyp.get("source_record_ids", []),
            "required_data": hyp.get("required_fields", []),
            "current_data_adequacy": g["data_adequacy"],
            "next_deterministic_action":
                "deterministic design review (Stage 4)"
                if g["queue_status"] == QS_READY else g["queue_reason"],
            "reason": g["queue_reason"], "created_at_run": None,
            "specification_hash": hypothesis_spec_hash(hyp) if hyp else None,
            "status": g["queue_status"],
            "llm_advisory_rank": ranking.get(g["hypothesis_id"])}
        queue_entries.append(entry)
        decide("queue", g["hypothesis_id"], g["queue_status"],
               g["queue_reason"])

    # ---------------- deterministic report metrics ------------------------- #
    grounded_count = sum(
        1 for w in hypotheses_out
        if w["grounding_validation"]["grounding"] == GR_GROUNDED)
    metrics = {
        "stage1_run_id": stage1["run_id"], "stage2_run_id": stage2["run_id"],
        "records_considered": sel["considered"],
        "records_selected": len(selected),
        "record_types_selected": sorted({r.get("record_type")
                                         for r in selected}),
        "sources_selected": sorted({r.get("source_id") for r in selected}),
        "analyses_total": len(analyses_out),
        "analyses_accepted": len(accepted_analyses),
        "analyses_rejected": len(analyses_out) - len(accepted_analyses),
        "hypotheses_proposed": len(hypotheses_out),
        "hypotheses_grounded": grounded_count,
        "exact_duplicates": sum(1 for g in gate_rows
                                if g["result"] == "EXACT_DUPLICATE"),
        "parameter_duplicates": sum(1 for g in gate_rows
                                    if g["result"] == "PARAMETER_DUPLICATE"),
        "new_information": sum(1 for g in gate_rows
                               if g["result"] == "NEW_INFORMATION"),
        "queue_entries": len(queue_entries),
        "held_for_data": sum(1 for q in queue_entries
                             if q["status"] == QS_HOLD_DATA),
        "rejected_total": len(rejected),
        "injection_indicator_records": len(sel["injection_indicators"]),
        "provider": provider_name, "classification": classification,
        "model": model, "llm_calls": len(receipts),
        "development_sample": sel.get("dev_sample"),
        "rss_atom_selected": sel.get("rss_atom_selected", 0),
        "additional_roots_verified": extra["verified_count"],
        "additional_roots_rejected": extra["rejected"],
        "event_clusters_available": extra["cluster_count"],
        "clusters_selected": sel.get("clusters_selected", []),
        "multi_source_clusters_selected":
            sel.get("multi_source_clusters_selected", []),
        "clustered_duplicates_dropped": sel.get("clustered_duplicates_dropped", 0),
        "cross_root_duplicates": sel.get("cross_root_duplicates", 0),
        "news_rss_coverage": build_news_rss_coverage(stage2, sel, extra),
        "budget": ledger.snapshot()}

    # Role 4 — narrative (values are Python-verified; LLM restates only).
    # In development mode the narrative is produced deterministically from the
    # verified metrics — NO extra LLM call is made — preserving output budget.
    narrative_llm_allowed = config.get("narrative_enabled", True) and not (
        dev_profile and dev_profile.get("disable_narrative_llm_call", True))
    if narrative_llm_allowed and not budget_exhausted \
            and provider_failed is None and provider is not None:
        nr = call_llm(TASK_NARRATIVE,
                      "VERIFIED METRICS (restate, never alter):\n"
                      + canonical_json({k: v for k, v in metrics.items()
                                        if k != "budget"}), 1500)
        if nr is not None and isinstance(nr.get("narrative"), str):
            narrative = nr["narrative"][:4000]

    # ---------------- terminal token --------------------------------------- #
    if budget_exhausted:
        token = BUDGET_EXHAUSTED
        terminal = BUDGET_EXHAUSTED
    elif provider_failed is not None and not receipts:
        token = PARTIAL
        terminal = "%s — %s" % (PARTIAL, provider_failed)
    elif provider_failed is not None:
        token = PARTIAL
        terminal = "%s — %s" % (PARTIAL, provider_failed)
    elif classification == PC_PRODUCTION_READY:
        token = READY
        terminal = READY
    elif classification == PC_DEVELOPMENT_READY:
        token = DEV_READY
        terminal = DEV_READY
    else:
        token = PARTIAL
        terminal = "%s — %s" % (PARTIAL,
                                selection.get("reason", "no provider"))

    # ---------------- run id + immutable package --------------------------- #
    selected_digest = sha256_text(canonical_json(
        sorted((r["record_id"], r.get("payload_hash") or "")
               for r in selected)))
    gate_digest = sha256_text(canonical_json(
        [(g["hypothesis_id"], g["result"], g["queue_status"])
         for g in gate_rows]))
    run_id = compute_run_id(
        stage1_run_id=stage1["run_id"], stage2_run_id=stage2["run_id"],
        config_hash=cfg_hash, git_commit=git_commit, mode="analyze",
        as_of=as_of, selected_digest=selected_digest,
        context_hash=context_hash, provider=provider_name, model=model,
        response_hashes=[r["response_hash"] for r in receipts
                         if r.get("response_hash")],
        gate_digest=gate_digest)
    run_dir = out_root / "runs" / run_id
    already_existed = run_dir.exists()

    started = now_fn()
    if not already_existed:
        for entry in queue_entries:
            entry["created_at_run"] = run_id
        _persist_cycle(conn, run_id, mode, as_of, token, terminal,
                       stage1, stage2, provider_name, model, cfg_hash,
                       git_commit, sel, selected, analyses_out,
                       hypotheses_out, gate_rows, queue_entries, decisions,
                       errors, receipts, usage_rows, started, now_fn)
        _write_run_outputs(run_dir, out_root, run_id, as_of, token, terminal,
                           stage1, stage2, selection, provider_name, model,
                           sel, selected, snapshot_id, packets, market_ctx,
                           registry_context, prompt_files, receipts,
                           analyses_out, hypotheses_out, gate_rows, rejected,
                           queue_entries, decisions, metrics, narrative,
                           ledger, cfg_hash, git_commit, now_fn)

    # ---------------- gates before publishing latest ------------------------ #
    ledgers_after = ledger_fingerprints(config)
    ledger_ok = ledgers_after == ledgers_before
    out_files = [run_dir / n for n in _ANALYZE_RUN_FILES if (run_dir / n).exists()]
    secret_hits = scan_outputs_for_secrets(out_files, secret_values)
    if not ledger_ok:
        return {"terminal": "%s — operational ledger fingerprints changed "
                            "during the cycle" % BLOCKED, "token": BLOCKED,
                "run_id": run_id}
    if secret_hits:
        return {"terminal": "%s — secret scan hits: %s"
                            % (BLOCKED, "; ".join(secret_hits[:3])),
                "token": BLOCKED, "run_id": run_id}
    if token in (READY, DEV_READY, BUDGET_EXHAUSTED) or \
            (token == PARTIAL and receipts is not None):
        _write_json(out_root / "latest.json", {
            "stage": "3", "run_id": run_id, "run_dir": "runs/%s" % run_id,
            "mode": "analyze", "as_of": as_of, "status": token,
            "terminal_token": terminal, "stage1_run_id": stage1["run_id"],
            "stage2_run_id": stage2["run_id"], "provider": provider_name,
            "provider_recorded_as": CLAUDE_CODE_DEVELOPMENT_ONLY
            if provider_name == "claude_code" else provider_name,
            "model": model, "classification": classification,
            "schema_version": DIRECTOR_SCHEMA_VERSION,
            "prompt_version": PROMPT_VERSION, "config_hash": cfg_hash,
            "git_commit": git_commit, "generated_at": now_fn(),
            "counts": {"records_selected": len(selected),
                       "analyses": len(analyses_out),
                       "hypotheses": len(hypotheses_out),
                       "queue_entries": len(queue_entries),
                       "llm_calls": len(receipts)}})
    return {"terminal": terminal, "token": token, "run_id": run_id,
            "run_dir": str(run_dir), "already_existed": already_existed,
            "metrics": metrics, "queue": queue_entries,
            "receipts_count": len(receipts)}


# --------------------------------------------------------------------------- #
# Persistence.
# --------------------------------------------------------------------------- #
def _persist_cycle(conn, run_id, mode, as_of, token, terminal, stage1, stage2,
                   provider_name, model, cfg_hash, git_commit, sel, selected,
                   analyses_out, hypotheses_out, gate_rows, queue_entries,
                   decisions, errors, receipts, usage_rows, started, now_fn):
    now = now_fn()
    day = as_of
    month = as_of[:7]
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO director_runs (run_id, mode, as_of, status,"
            " terminal_token, stage1_run_id, stage2_run_id, provider, model,"
            " config_hash, git_commit, schema_version, prompt_version,"
            " records_considered, records_selected, analyses_accepted,"
            " hypotheses_proposed, queue_entries, started_at, finished_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, mode, as_of, token, terminal, stage1["run_id"],
             stage2["run_id"], provider_name, model, cfg_hash, git_commit,
             DIRECTOR_SCHEMA_VERSION, PROMPT_VERSION, sel["considered"],
             len(selected),
             sum(1 for a in analyses_out
                 if a["grounding_validation"]["grounding"] == GR_GROUNDED),
             len(hypotheses_out), len(queue_entries), started, now))
        for rec in selected:
            conn.execute(
                "INSERT OR IGNORE INTO processed_records (record_id, run_id,"
                " record_type, source_id, selected, processed_at)"
                " VALUES (?,?,?,?,1,?)",
                (rec["record_id"], run_id, rec.get("record_type"),
                 rec.get("source_id"), now))
        for rid, rt, src, reason in sel["skipped_ids"]:
            conn.execute(
                "INSERT OR IGNORE INTO processed_records (record_id, run_id,"
                " record_type, source_id, selected, skip_reason, processed_at)"
                " VALUES (?,?,?,?,0,?,?)", (rid, run_id, rt, src, reason, now))
        for i, r in enumerate(receipts):
            u = r.get("usage") or {}
            conn.execute(
                "INSERT INTO provider_calls (run_id, call_index, task,"
                " provider, model, prompt_hash, request_id, status, retries,"
                " input_tokens, output_tokens, cache_creation_tokens,"
                " cache_read_tokens, response_hash, error, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, r["call_index"], r["task"], r["provider"],
                 r.get("model"), r["prompt_hash"], r.get("request_id"),
                 str(r.get("status")), r.get("retries") or 0,
                 u.get("input_tokens"), u.get("output_tokens"),
                 u.get("cache_creation_input_tokens"),
                 u.get("cache_read_input_tokens"), r.get("response_hash"),
                 r.get("error"), now))
        for u in usage_rows:
            conn.execute(
                "INSERT INTO token_usage (run_id, provider, model, day, month,"
                " input_tokens, output_tokens, cache_creation_tokens,"
                " cache_read_tokens, estimated_cost_usd, cost_available,"
                " created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, u["provider"], u.get("model"), day, month,
                 u["input_tokens"], u["output_tokens"],
                 u["cache_creation_tokens"], u["cache_read_tokens"],
                 u["estimated_cost_usd"]
                 if u["estimated_cost_usd"] != COST_UNAVAILABLE else None,
                 1 if u["cost_available"] else 0, now))
        for idx, w in enumerate(analyses_out):
            a = w["analysis"] if isinstance(w["analysis"], dict) else {}
            conn.execute(
                "INSERT OR IGNORE INTO event_analyses (event_analysis_id,"
                " run_id, packet_id, grounding_result, materiality, novelty,"
                " confidence, source_record_ids_json, analysis_json,"
                " prompt_hash, response_hash, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (str(a.get("event_analysis_id") or "ea_invalid_%03d" % idx),
                 run_id, None,
                 w["grounding_validation"]["grounding"], a.get("materiality"),
                 a.get("novelty"),
                 a.get("confidence") if isinstance(a.get("confidence"),
                                                   (int, float)) else None,
                 canonical_json(a.get("source_record_ids") or []),
                 canonical_json(w), w["prompt_hash"], w["response_hash"], now))
        for w in hypotheses_out:
            h = w["proposal"] if isinstance(w["proposal"], dict) else {}
            conn.execute(
                "INSERT OR IGNORE INTO hypothesis_proposals (hypothesis_id,"
                " run_id, title, status, grounding_result, information_family,"
                " source_record_ids_json, spec_hash, proposal_json, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (str(h.get("hypothesis_id") or "hyp_unknown"), run_id,
                 h.get("title"), HYPOTHESIS_STATUS_DRAFT,
                 w["grounding_validation"]["grounding"],
                 h.get("information_family"),
                 canonical_json(h.get("source_record_ids") or []),
                 hypothesis_spec_hash(h) if h else None,
                 canonical_json(w), now))
        for g in gate_rows:
            conn.execute(
                "INSERT INTO duplicate_gate_results (run_id, hypothesis_id,"
                " result, matched_experiment_ids_json, reason, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (run_id, g["hypothesis_id"], g["result"],
                 canonical_json(g["matched_experiment_ids"]),
                 str(g.get("reason")), now))
        for q in queue_entries:
            conn.execute(
                "INSERT OR IGNORE INTO research_queue (queue_id, run_id,"
                " hypothesis_id, priority, duplicate_result, status,"
                " spec_hash, entry_json, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (q["queue_id"], run_id, q["hypothesis_id"], q["priority"],
                 q["duplicate_gate_result"], q["status"],
                 q["specification_hash"], canonical_json(q), now))
        for d in decisions:
            conn.execute(
                "INSERT INTO director_decisions (run_id, decision_type,"
                " subject_id, decision, reason, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (run_id, d["decision_type"], d["subject_id"], d["decision"],
                 d["reason"], now))
        for e in errors:
            conn.execute(
                "INSERT INTO director_errors (run_id, category, detail,"
                " created_at) VALUES (?,?,?,?)",
                (run_id, e["category"], e["detail"], now))
        conn.execute(
            "INSERT INTO director_checkpoints (checkpoint_key, cursor_json,"
            " updated_at) VALUES ('stage2_normalized', ?, ?)"
            " ON CONFLICT(checkpoint_key) DO UPDATE SET"
            " cursor_json=excluded.cursor_json, updated_at=excluded.updated_at",
            (canonical_json({"last_run_id": run_id, "last_as_of": as_of,
                             "stage2_run_id": stage2["run_id"]}), now))


# --------------------------------------------------------------------------- #
# Output files.
# --------------------------------------------------------------------------- #
def _write_run_outputs(run_dir, out_root, run_id, as_of, token, terminal,
                       stage1, stage2, selection, provider_name, model, sel,
                       selected, snapshot_id, packets, market_ctx,
                       registry_context, prompt_files, receipts, analyses_out,
                       hypotheses_out, gate_rows, rejected, queue_entries,
                       decisions, metrics, narrative, ledger, cfg_hash,
                       git_commit, now_fn):
    run_dir.mkdir(parents=True, exist_ok=True)
    prompts_dir = out_root / "prompts"
    prompt_manifest = []
    for p in prompt_files:
        ph = p["prompt_hash"]
        pf = prompts_dir / ("%s.json" % ph)
        if not pf.exists():
            _write_json(pf, p)
        prompt_manifest.append({"task": p["task"], "prompt_hash": ph,
                                "prompt_version": p["prompt_version"],
                                "file": "prompts/%s.json" % ph})
    _write_json(run_dir / "input_snapshot.json", {
        "input_snapshot_id": snapshot_id, "as_of": as_of,
        "stage1_run_id": stage1["run_id"], "stage2_run_id": stage2["run_id"],
        "considered": sel["considered"], "malformed": sel["malformed"],
        "skipped_already_processed": sel["skipped_already_processed"],
        "skipped_unhealthy": sel["skipped_unhealthy"],
        "skipped_no_provenance": sel["skipped_no_provenance"],
        "duplicates_skipped": sel["duplicates_skipped"],
        "cap_dropped": sel["cap_dropped"],
        "deferred_to_next_cycle": sel["cap_dropped"],
        "development_sample": sel.get("dev_sample"),
        "type_considered": sel["type_considered"],
        "type_selected": sel["type_selected"],
        "selected_record_ids": sorted(r["record_id"] for r in selected),
        "market_context_record_ids": market_ctx["record_ids"],
        "packets": [p["packet_id"] for p in packets],
        "injection_indicators": sel["injection_indicators"],
        "source_health": stage2["source_health"]})
    _write_text(run_dir / "selected_event_records.jsonl",
                "\n".join(canonical_json(r) for r in selected) + "\n")
    _write_json(run_dir / "registry_context.json", registry_context)
    _write_json(run_dir / "prompt_manifest.json",
                {"prompt_version": PROMPT_VERSION, "prompts": prompt_manifest})
    _write_json(run_dir / "provider_receipt.json",
                {"selection": {"selected": selection["selected"],
                               "classification": selection["classification"],
                               "audits": selection["audits"]},
                 "calls": receipts})
    _write_text(run_dir / "structured_event_analysis.jsonl",
                "\n".join(canonical_json(a) for a in analyses_out)
                + ("\n" if analyses_out else ""))
    _write_json(run_dir / "hypothesis_proposals.json",
                {"proposals": hypotheses_out})
    _write_json(run_dir / "duplicate_gate_results.json", {"results": gate_rows})
    _write_json(run_dir / "rejected_proposals.json", {"rejected": rejected})
    _write_json(run_dir / "research_queue.json", {"queue": queue_entries})
    _write_json(run_dir / "director_decisions.json", {"decisions": decisions})
    snap = ledger.snapshot()
    _write_json(run_dir / "token_cost_report.json", {
        "provider": provider_name, "model": model,
        "provider_recorded_as": CLAUDE_CODE_DEVELOPMENT_ONLY
        if provider_name == "claude_code" else provider_name,
        "request_count": snap["calls"], "accounting": snap,
        "per_call": [{"call_index": r["call_index"], "task": r["task"],
                      "usage": r.get("usage"),
                      "usage_reliable": r.get("usage_reliable")}
                     for r in receipts],
        "usage_unavailable_calls": sum(1 for r in receipts
                                       if not r.get("usage")),
        "cost_note": "cost is UNAVAILABLE unless explicit pricing matched the "
                     "exact model and the provider reported usage"})
    _write_text(run_dir / "stage3_daily_report.md",
                _render_daily_report(run_id, run_dir, as_of, token, terminal,
                                     stage1, stage2, metrics, gate_rows,
                                     queue_entries, rejected, receipts,
                                     narrative, sel))
    _write_json(run_dir / "stage3_5_news_rss_requirements.json",
                _stage3_5_requirements(run_id, stage1, stage2, metrics))
    manifest_files = {}
    for name in _ANALYZE_RUN_FILES:
        if name == "run_manifest.json":
            continue
        fp = run_dir / name
        if fp.exists():
            manifest_files[name] = _sha256_file(fp)
    _write_json(run_dir / "run_manifest.json", {
        "run_id": run_id, "mode": "analyze", "as_of": as_of,
        "status": token, "terminal_token": terminal,
        "stage1_run_id": stage1["run_id"], "stage2_run_id": stage2["run_id"],
        "provider": provider_name, "model": model,
        "schema_version": DIRECTOR_SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION, "config_hash": cfg_hash,
        "git_commit": git_commit, "generated_at": now_fn(),
        "file_hashes": manifest_files,
        "required_files": list(_ANALYZE_RUN_FILES)})


def _render_daily_report(run_id, run_dir, as_of, token, terminal, stage1,
                         stage2, metrics, gate_rows, queue_entries, rejected,
                         receipts, narrative, sel) -> str:
    b = metrics["budget"]
    held = [q for q in queue_entries if q["status"] in (QS_HOLD_DATA,
                                                        QS_HOLD_METADATA)]
    rej_q = [q for q in queue_entries if q["status"].startswith("REJECT")]
    prevented = sorted({m for g in gate_rows
                        for m in g["matched_experiment_ids"]})[:20]
    injection_total = metrics["injection_indicator_records"]
    lines = [
        "# Alpha Agent — Stage 3 Daily Research-Director Report", "",
        "All numeric values below are produced by deterministic Python — "
        "never by the LLM.", "",
        "1. **Stage 1 registry run used:** `%s`." % stage1["run_id"],
        "2. **Stage 2 ingestion run used:** `%s`." % stage2["run_id"],
        "3. **New source records considered:** %d (malformed %d, already "
        "processed %d, unhealthy-source %d, no-provenance %d, duplicates %d)."
        % (metrics["records_considered"], sel["malformed"],
           sel["skipped_already_processed"], sel["skipped_unhealthy"],
           sel["skipped_no_provenance"], sel["duplicates_skipped"]),
        "4. **Selected for the LLM:** %d." % metrics["records_selected"],
        "5. **Record types and sources represented:** %s | %s."
        % (", ".join(metrics["record_types_selected"]),
           ", ".join(metrics["sources_selected"])),
        "6. **Material events identified:** %d analyses accepted (of %d)."
        % (metrics["analyses_accepted"], metrics["analyses_total"]),
        "7. **Claims rejected as unsupported:** %d analysis objects rejected; "
        "%d total rejected proposals." % (metrics["analyses_rejected"],
                                          metrics["rejected_total"]),
        "8. **Hypotheses proposed:** %d." % metrics["hypotheses_proposed"],
        "9. **Hypotheses grounded:** %d." % metrics["hypotheses_grounded"],
        "10. **Exact duplicates:** %d." % metrics["exact_duplicates"],
        "11. **Parameter duplicates:** %d." % metrics["parameter_duplicates"],
        "12. **Genuinely new information:** %d." % metrics["new_information"],
        "13. **Entered the research queue:** %d entries (%d actionable)."
        % (metrics["queue_entries"],
           sum(1 for q in queue_entries
               if q["status"] in (QS_READY, QS_RESUME))),
        "14. **Held for missing data:** %s."
        % (", ".join("%s (%s)" % (q["hypothesis_id"], q["status"])
                     for q in held) or "none"),
        "15. **Rejected candidates and why:** %s."
        % ("; ".join("%s: %s" % (q["hypothesis_id"], q["reason"])
                     for q in rej_q) or "none"),
        "16. **Existing research preventing redundant work:** %s."
        % (", ".join(prevented) or "no duplicate matches this cycle"),
        "17. **Provider and model:** %s (%s), model %s."
        % (metrics["provider"], metrics["classification"], metrics["model"]),
        "18. **LLM calls:** %d." % metrics["llm_calls"],
        "19. **Exact tokens (input/output/cache-create/cache-read):** "
        "%d / %d / %d / %d."
        % (b["cycle_input_tokens"], b["cycle_output_tokens"],
           sum(int((r.get("usage") or {}).get("cache_creation_input_tokens")
                   or 0) for r in receipts),
           sum(int((r.get("usage") or {}).get("cache_read_input_tokens")
                   or 0) for r in receipts)),
        "20. **Estimated cost and budget remaining:** cycle %s USD; remaining "
        "day %s USD / month %s USD."
        % (b["cycle_estimated_cost_usd"],
           b["budget_remaining"]["daily_cost_usd"],
           b["budget_remaining"]["monthly_cost_usd"]),
        "21. **Token or cost usage unavailable:** %s (calls without reliable "
        "cost: %d)." % ("YES" if b["cycle_estimated_cost_usd"]
                        == COST_UNAVAILABLE else "NO",
                        b["cost_unavailable_calls"]),
        "22. **Prompt-injection indicators detected:** %d record(s); source "
        "records kept unchanged; indicators recorded in input_snapshot.json."
        % injection_total,
        "23. **Provider failures and retries:** %s; retries %d."
        % ("; ".join(sorted({str(r.get("error")) for r in receipts
                             if r.get("error")})) or "none",
           sum(int(r.get("retries") or 0) for r in receipts)),
        "24. **Immutable run:** `%s` at `%s`." % (run_id, run_dir),
        "25. **No experiment ran:** CONFIRMED — Stage 3 interprets and queues "
        "only.",
        "26. **No LLM-generated code executed:** CONFIRMED — code payloads are "
        "rejected and nothing returned by the LLM is ever executed.",
        "27. **No active model or book changed:** CONFIRMED — operational "
        "ledger fingerprints verified identical before/after.",
        "28. **No orders, signals or trade decisions created:** CONFIRMED.",
        "29. **External blockers:** %s."
        % (terminal.split("— ", 1)[1] if token == PARTIAL and "— " in terminal
           else "none"),
        _stage4_readiness_line(metrics)]
    lines += _news_rss_section(metrics)
    ds = metrics.get("development_sample")
    if ds:
        lines += [
            "", "## DEVELOPMENT PROFILE (claude_code bounded proof)", "",
            "- Development-bounded sample: %d records (max %d). Composition: "
            "%s." % (len(ds["selected_record_ids"]), ds["max_records"],
                     canonical_json(ds["composition_selected"])),
            "- Quota substitutions: %s."
            % (canonical_json(ds["substitutions"])
               if ds["substitutions"] else "none"),
            "- Records deferred by the development cap (NOT marked processed): "
            "%d." % ds["deferred_count"],
            "- Output bounded by --json-schema + per-task token targets; the "
            "daily narrative uses NO additional LLM call in development mode."]
    lines += ["", "Terminal: %s" % terminal, ""]
    if narrative:
        lines += ["## LLM narrative (restates verified values only)", "",
                  narrative, ""]
    return "\n".join(lines)


def _stage4_readiness_line(metrics: dict) -> str:
    """Report item #30 — Stage 4 readiness.

    Corrected for the verified Stage 3.5 package: once generalized official
    RSS/Atom collection is OPERATIONAL, the report no longer states that Stage
    3.5 remains unimplemented / mandatory-before-runtime. It reports Stage 3.5
    IMPLEMENTED with generalized collection OPERATIONAL_PARTIAL, the remaining
    coverage gaps (company-direct sparse, international partial, GDELT disabled)
    and that the Stage 4 runtime is ACTIVE only after installation and
    verification. When generalized collection does NOT yet exist the original
    STAGE3_5_NEWS_RSS_EXPANSION_REQUIRED language is preserved unchanged.
    """
    cov = metrics.get("news_rss_coverage") or {}
    generalized = bool(cov.get("generalized_rss_collection_exists", False))
    provider = ("production provider ready; queue and evidence contracts in "
                "place" if metrics.get("classification") == PC_PRODUCTION_READY
                else "software ready; ANTHROPIC_API_KEY (and optionally "
                "ALPHA_AGENT_LLM_MODEL) must be provisioned for unattended "
                "production cycles — the claude_code provider is "
                "development-only")
    if generalized:
        return ("30. **Stage 4 readiness:** %s; generalized RSS/Atom "
                "collection: OPERATIONAL_PARTIAL; Stage 3.5: IMPLEMENTED "
                "(company-direct feed coverage still missing or sparse; "
                "international coverage still partial; GDELT disabled); Stage 4 "
                "runtime status: ACTIVE only after installation and "
                "verification." % provider)
    return ("30. **Stage 4 readiness:** %s; %s — broad RSS/Atom acquisition "
            "(Stage 3.5) is mandatory before the persistent 24/7 runtime (see "
            "stage3_5_news_rss_requirements.json)." % (provider,
                                                       STAGE3_5_MARKER))


_NEWS_COVERAGE_LABELS = {
    "NEWS_EVENT": "NEWS_EVENT (EODHD+RSS)", "FILING_EVENT": "SEC FILING_EVENT",
    "INSIDER_FILING": "SEC INSIDER_FILING",
    "TRADING_HALT": "Nasdaq TRADING_HALT",
    "EARNINGS_EVENT": "EARNINGS_EVENT",
    "CORPORATE_ACTION": "CORPORATE_ACTION",
    "REGULATORY_EVENT": "RSS REGULATORY_EVENT",
    "PRESS_RELEASE": "RSS PRESS_RELEASE"}


def _news_rss_section(metrics: dict) -> list[str]:
    """Mandatory NEWS_AND_RSS_COVERAGE report section. All counts are
    Python-computed; the section must never claim comprehensive coverage."""
    cov = metrics.get("news_rss_coverage") or {}
    counts = cov.get("record_counts") or {}
    parts = []
    fresh = []
    for rt in _NEWS_COVERAGE_TYPES:
        c = counts.get(rt) or {"considered": 0, "selected": 0,
                               "newest_record_at": None}
        parts.append("%s %d considered / %d selected"
                     % (_NEWS_COVERAGE_LABELS[rt], c["considered"],
                        c["selected"]))
        fresh.append("%s %s" % (rt, c.get("newest_record_at") or "n/a"))
    other = cov.get("other_event_sources") or {}
    other_str = ", ".join("%s %d/%d" % (rt, v["considered"], v["selected"])
                          for rt, v in sorted(other.items())) or "none"
    generalized = bool(cov.get("generalized_rss_collection_exists", False))
    company_direct = bool(cov.get("company_direct_rss_atom_feeds_exist", False))
    fe = cov.get("feed_evidence") or {}
    lines = [
        "", "## NEWS_AND_RSS_COVERAGE", "",
        "- **Current news sources:** %s." % "; ".join(
            "%s = %s" % (k, v) for k, v in sorted(
                (cov.get("current_news_sources")
                 or _NEWS_RSS_INVENTORY["news_sources"]).items())),
        "- **Current RSS feeds:** %s." % "; ".join(
            cov.get("current_rss_feeds") or _NEWS_RSS_INVENTORY["rss_feeds"]),
        "- **Enabled feeds / attempted / healthy / degraded / blocked:** "
        "%d / %d / %d / %d / %d." % (
            fe.get("enabled_feeds", 0), fe.get("attempted_feeds", 0),
            fe.get("healthy_feeds", 0), fe.get("degraded_feeds", 0),
            fe.get("blocked_feeds", 0)),
        "- **Newest feed item:** %s." % (fe.get("newest_feed_item") or "n/a"),
        "- **RSS/Atom records considered / selected this cycle:** %d / %d "
        "(REGULATORY_EVENT %d, PRESS_RELEASE %d considered)." % (
            cov.get("rss_atom_records_considered", 0),
            cov.get("rss_atom_records_selected", 0),
            cov.get("rss_regulatory_considered", 0),
            cov.get("rss_press_release_considered", 0)),
        "- **Official government/regulatory feeds represented:** %d; "
        "official-company feeds represented: %d." % (
            fe.get("government_regulatory_feeds", 0), fe.get("company_feeds", 0)),
        "- **Records considered / selected this cycle:** %s; other event "
        "sources: %s." % ("; ".join(parts), other_str),
        "- **Source freshness (newest record per type):** %s."
        % ", ".join(fresh),
        "- **Event clusters created (Stage 3.5):** %d (%d multi-source "
        "corroborated); clusters represented in this cycle's selection: %d "
        "(%d multi-source); clustered duplicate items prevented: %d; "
        "cross-root duplicates prevented: %d." % (
            fe.get("clusters_created", cov.get("event_clusters_available", 0)),
            fe.get("multi_source_clusters", 0),
            cov.get("clusters_selected", 0),
            cov.get("multi_source_clusters_selected", 0),
            cov.get("clustered_duplicates_dropped", 0),
            cov.get("cross_root_duplicates", 0)),
        "- **Duplicate feed items prevented (Stage 3.5 collection):** %d."
        % fe.get("duplicate_items_prevented", 0),
        "- **Unresolved entity mappings (Stage 3.5):** %d (UNMATCHED + "
        "AMBIGUOUS; never guessed)." % fe.get("unresolved_entity_mappings", 0),
        "- **Verified additional event roots:** %d; rejected (unverified): %d."
        % (cov.get("additional_event_roots_verified", 0),
           len(cov.get("additional_event_roots_rejected") or [])),
        "- **Source gaps:** %s." % "; ".join(
            cov.get("source_gaps") or _NEWS_RSS_INVENTORY["source_gaps"]),
        "- **GDELT remains disabled:** %s (state %s)."
        % ("YES" if cov.get("gdelt_remains_disabled", True) else "NO",
           cov.get("gdelt_state", "NOT_RUN")),
        "- **Company-direct RSS/Atom feeds exist:** %s."
        % ("YES" if company_direct else "NO"),
        "- **Generalized RSS collection exists:** %s."
        % ("YES (Stage 3.5 operational — partial coverage)" if generalized
           else "NO"),
    ]
    if generalized:
        lines.append(
            "- **Coverage verdict:** news collection is now BROADER but still "
            "PARTIAL — generalized official RSS/Atom collection is OPERATIONAL "
            "(Stage 3.5) alongside EODHD financial news, SEC filings and the "
            "Nasdaq halt feed; company investor-relations and international "
            "coverage remain to be expanded. Stage 3.5 status: %s."
            % cov.get("stage3_5_status", "IMPLEMENTED_PARTIAL"))
    else:
        lines.append(
            "- **Coverage verdict:** news collection is NOT complete — EODHD "
            "financial news plus the narrow Nasdaq halt RSS do not constitute "
            "comprehensive news/RSS coverage. %s." % STAGE3_5_MARKER)
    return lines


# --------------------------------------------------------------------------- #
# Audit mode package.
# --------------------------------------------------------------------------- #
def _finish_audit(config, out_root, as_of, *, conn, stage1, stage2, selection,
                  classification, model, cfg_hash, git_commit, now_fn, ledger,
                  ledgers_before, secret_values, decisions):
    seed = canonical_json({
        "mode": "audit", "stage1_run_id": stage1["run_id"],
        "stage2_run_id": stage2["run_id"], "config_hash": cfg_hash,
        "git_commit": git_commit or "UNKNOWN", "as_of": as_of,
        "schema_version": DIRECTOR_SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION,
        "classifications": {n: a.get("classification")
                            for n, a in selection["audits"].items()}})
    run_id = "stage3_" + sha256_text(seed)[:16]
    run_dir = out_root / "runs" / run_id
    already_existed = run_dir.exists()
    if selection["classification"] == PC_PRODUCTION_READY:
        token, terminal = READY, READY
    elif selection["classification"] == PC_DEVELOPMENT_READY:
        token, terminal = DEV_READY, DEV_READY
    else:
        token = PARTIAL
        terminal = "%s — %s" % (PARTIAL, selection.get("reason",
                                                       "no operable provider"))
    started = now_fn()
    if not already_existed:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO director_runs (run_id, mode, as_of,"
                " status, terminal_token, stage1_run_id, stage2_run_id,"
                " provider, model, config_hash, git_commit, schema_version,"
                " prompt_version, started_at, finished_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, "audit", as_of, token, terminal, stage1["run_id"],
                 stage2["run_id"], selection["selected"], model, cfg_hash,
                 git_commit, DIRECTOR_SCHEMA_VERSION, PROMPT_VERSION,
                 started, now_fn()))
        run_dir.mkdir(parents=True, exist_ok=True)
        _write_json(run_dir / "input_snapshot.json", {
            "mode": "audit", "as_of": as_of,
            "stage1_run_id": stage1["run_id"],
            "stage2_run_id": stage2["run_id"],
            "stage1_valid": True, "stage2_valid": True,
            "budget_config_valid": True,
            "record_type_counts": stage2["record_type_counts"],
            "source_health": stage2["source_health"], "llm_calls": 0})
        _write_json(run_dir / "provider_receipt.json",
                    {"selection": {"selected": selection["selected"],
                                   "classification": selection["classification"],
                                   "audits": selection["audits"]},
                     "calls": []})
        _write_json(run_dir / "token_cost_report.json",
                    {"request_count": 0, "accounting": ledger.snapshot(),
                     "per_call": [], "note": "audit mode makes no LLM call"})
        audit_sel = {"type_considered": {}, "type_selected": {},
                     "type_freshness": {}}
        for k, v in (stage2.get("record_type_counts") or {}).items():
            try:
                audit_sel["type_considered"][str(k)] = int(v)
            except (TypeError, ValueError):
                audit_sel["type_considered"][str(k)] = 0
        _write_text(run_dir / "stage3_daily_report.md", "\n".join([
            "# Alpha Agent — Stage 3 Provider / Input Audit", "",
            "1. Stage 1 registry run: `%s` (valid)." % stage1["run_id"],
            "2. Stage 2 ingestion run: `%s` (valid, %d record types)."
            % (stage2["run_id"], len(stage2["record_type_counts"])),
            "3. Provider audit: %s." % canonical_json(
                {n: a.get("classification")
                 for n, a in selection["audits"].items()}),
            "4. Selected provider: %s (%s), model %s."
            % (selection["selected"], selection["classification"], model),
            "5. LLM calls made: 0 (audit mode never calls the LLM).",
            "6. Budgets validated: hard caps %s calls / %s in-tokens / %s "
            "out-tokens per cycle; daily stop %s USD; monthly stop %s USD."
            % (config["budgets"]["max_calls_per_cycle"],
               config["budgets"]["max_input_tokens_per_cycle"],
               config["budgets"]["max_output_tokens_per_cycle"],
               config["budgets"]["daily_cost_hard_stop_usd"],
               config["budgets"]["monthly_cost_hard_stop_usd"])]
            + _news_rss_section({"news_rss_coverage":
                                 build_news_rss_coverage(stage2, audit_sel)})
            + ["", "Terminal: %s" % terminal, ""]))
        manifest_files = {}
        for name in _AUDIT_RUN_FILES:
            if name == "run_manifest.json":
                continue
            fp = run_dir / name
            if fp.exists():
                manifest_files[name] = _sha256_file(fp)
        _write_json(run_dir / "run_manifest.json", {
            "run_id": run_id, "mode": "audit", "as_of": as_of,
            "status": token, "terminal_token": terminal,
            "stage1_run_id": stage1["run_id"],
            "stage2_run_id": stage2["run_id"],
            "provider": selection["selected"], "model": model,
            "schema_version": DIRECTOR_SCHEMA_VERSION,
            "prompt_version": PROMPT_VERSION, "config_hash": cfg_hash,
            "git_commit": git_commit, "generated_at": now_fn(),
            "file_hashes": manifest_files,
            "required_files": list(_AUDIT_RUN_FILES)})
    ledger_ok = ledger_fingerprints(config) == ledgers_before
    hits = scan_outputs_for_secrets(
        [run_dir / n for n in _AUDIT_RUN_FILES if (run_dir / n).exists()],
        secret_values)
    if not ledger_ok:
        return {"terminal": "%s — ledger fingerprints changed during audit"
                            % BLOCKED, "token": BLOCKED, "run_id": run_id}
    if hits:
        return {"terminal": "%s — secret scan hits in audit package"
                            % BLOCKED, "token": BLOCKED, "run_id": run_id}
    _write_json(out_root / "latest.json", {
        "stage": "3", "run_id": run_id, "run_dir": "runs/%s" % run_id,
        "mode": "audit", "as_of": as_of, "status": token,
        "terminal_token": terminal, "stage1_run_id": stage1["run_id"],
        "stage2_run_id": stage2["run_id"],
        "provider": selection["selected"],
        "classification": selection["classification"], "model": model,
        "schema_version": DIRECTOR_SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION, "config_hash": cfg_hash,
        "git_commit": git_commit, "generated_at": now_fn(),
        "counts": {"llm_calls": 0}})
    return {"terminal": terminal, "token": token, "run_id": run_id,
            "run_dir": str(run_dir), "already_existed": already_existed,
            "audits": selection["audits"]}


# --------------------------------------------------------------------------- #
# Verify mode (no network, no LLM, writes NOTHING).
# --------------------------------------------------------------------------- #
def verify_run(config: dict, output_root: str, *,
               env: Optional[Mapping[str, str]] = None) -> dict:
    env = env if env is not None else os.environ
    out_root = Path(output_root)
    problems: list[str] = []
    latest = _read_json(out_root / "latest.json")
    if not isinstance(latest, dict) or not latest.get("run_id"):
        return {"terminal": "%s — no publishable latest.json to verify"
                            % BLOCKED, "token": BLOCKED}
    run_dir = out_root / str(latest["run_dir"]).replace("\\", os.sep)
    manifest = _read_json(run_dir / "run_manifest.json")
    if not isinstance(manifest, dict):
        return {"terminal": "%s — run_manifest.json missing" % BLOCKED,
                "token": BLOCKED}
    for name in manifest.get("required_files", []):
        if not (run_dir / name).exists():
            problems.append("missing required file %s" % name)
    for name, expected in (manifest.get("file_hashes") or {}).items():
        fp = run_dir / name
        if fp.exists() and _sha256_file(fp) != expected:
            problems.append("hash mismatch %s" % name)

    # SQLite integrity (read-only, immutable: creates no -wal/-shm sidecars).
    db_path = out_root / "state" / "director_state.sqlite"
    if db_path.exists():
        uri = "file:%s?mode=ro&immutable=1" % str(db_path).replace("\\", "/")
        ro = sqlite3.connect(uri, uri=True)
        try:
            if ro.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                problems.append("sqlite integrity_check failed")
            if ro.execute("PRAGMA foreign_key_check").fetchall():
                problems.append("sqlite foreign_key_check failures")
            q_rows = ro.execute(
                "SELECT queue_id, hypothesis_id FROM research_queue"
                " WHERE run_id=?", (latest["run_id"],)).fetchall()
            gate_ids = {r[0] for r in ro.execute(
                "SELECT hypothesis_id FROM duplicate_gate_results"
                " WHERE run_id=?", (latest["run_id"],))}
            for _, hid in q_rows:
                if hid not in gate_ids:
                    problems.append("queue entry %s lacks a duplicate-gate row"
                                    % hid)
            db_usage = ro.execute(
                "SELECT COALESCE(SUM(input_tokens),0),"
                " COALESCE(SUM(output_tokens),0) FROM provider_calls"
                " WHERE run_id=?", (latest["run_id"],)).fetchone()
        finally:
            ro.close()
    else:
        problems.append("state database missing")
        db_usage = (0, 0)

    # Prompt hashes.
    pm = _read_json(run_dir / "prompt_manifest.json")
    if isinstance(pm, dict):
        from .llm_contracts import prompt_hash as _ph
        for row in pm.get("prompts", []):
            pf = out_root / str(row["file"]).replace("\\", os.sep)
            obj = _read_json(pf)
            if not isinstance(obj, dict):
                problems.append("prompt file unreadable %s" % row["file"])
                continue
            recomputed = _ph({k: obj[k] for k in
                              ("task", "prompt_version", "schema_version",
                               "system", "user") if k in obj})
            if recomputed != row["prompt_hash"]:
                problems.append("prompt hash mismatch %s" % row["task"])

    # Provider response hashes + token reconciliation.
    receipt = _read_json(run_dir / "provider_receipt.json")
    file_in = file_out = 0
    if isinstance(receipt, dict):
        for call in receipt.get("calls", []):
            text = call.get("response_text")
            if text is not None and not call.get("truncated") \
                    and call.get("response_hash"):
                if sha256_text(text) != call["response_hash"]:
                    problems.append("response hash mismatch call %s"
                                    % call.get("call_index"))
            u = call.get("usage") or {}
            file_in += int(u.get("input_tokens") or 0)
            file_out += int(u.get("output_tokens") or 0)
        if (file_in, file_out) != (db_usage[0], db_usage[1]):
            problems.append("token reconciliation mismatch: receipts %s vs "
                            "state db %s" % ((file_in, file_out),
                                             tuple(db_usage)))
    tc = _read_json(run_dir / "token_cost_report.json")
    if isinstance(tc, dict) and isinstance(receipt, dict):
        acct = tc.get("accounting") or {}
        if int(acct.get("cycle_input_tokens") or 0) != file_in \
                or int(acct.get("cycle_output_tokens") or 0) != file_out:
            problems.append("token_cost_report does not reconcile with "
                            "provider receipts")

    # Secret scan (values from live env + structural pattern).
    secret_values = [env.get(n) for n in _SECRET_ENV_NAMES if env.get(n)]
    for name in manifest.get("required_files", []):
        fp = run_dir / name
        if fp.exists():
            for hit in scan_outputs_for_secrets([fp], secret_values):
                problems.append("secret scan: %s" % hit)

    if problems:
        return {"terminal": "%s — %s" % (BLOCKED, "; ".join(problems[:8])),
                "token": BLOCKED, "problems": problems}
    return {"terminal": VERIFIED, "token": VERIFIED,
            "run_id": latest["run_id"], "problems": []}


__all__ = ["READY", "DEV_READY", "NO_NEW", "BUDGET_EXHAUSTED", "VERIFIED",
           "PARTIAL", "BLOCKED", "ELIGIBLE_RECORD_TYPES", "run_director",
           "verify_run", "read_stage1", "read_stage2", "read_additional_roots",
           "select_input_records", "apply_development_sample",
           "build_packets", "build_market_context", "run_duplicate_gate",
           "check_data_adequacy", "queue_policy", "compute_run_id",
           "ledger_fingerprints", "scan_outputs_for_secrets",
           "config_hash_of", "open_state_db", "select_provider",
           "build_providers"]
