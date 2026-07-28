"""
alpha_agent/research_importers.py — Stage 1 deterministic importers + normalizers.

This module is the read-only parsing / fingerprinting / normalization layer for the
Alpha Research Registry. It is intentionally pure and standard-library only:

    - Content fingerprinting: full SHA-256 at or below a size threshold; a labelled
      deterministic sampled fingerprint above it (never presented as a full hash).
    - Deterministic format importers (JSON / JSONL / CSV / Markdown / TXT / TOML /
      YAML / SQLite / Parquet-metadata). Unsupported or malformed files get an
      explicit parser status — they are never silently skipped and never guessed.
    - A field-alias normalizer that maps heterogeneous source records onto a stable
      canonical field set without fabricating anything (missing -> null + a
      completeness marker).
    - Deterministic record-type and information-family classifiers (explicit keyword
      rules, no LLM, no fuzzy text similarity).
    - Two deterministic experiment fingerprints (EXACT + INFORMATION_FAMILY) built on
      canonical-JSON serialization and SHA-256, plus the pairwise comparison rules.

Nothing here opens a network socket, a database connection, or a model API, and
nothing here executes source payloads (pickles are hashed as opaque bytes, never
unpickled).
"""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

# Bump this when the parsing / normalization / classification / fingerprint logic
# changes in a way that should invalidate a previously-built registry version.
IMPORTER_VERSION = "1.0.0"

# Allow very large CSV fields (some research panels embed wide rows) without the
# module-global affecting the host process unpredictably; keep it deterministic.
csv.field_size_limit(min(2**31 - 1, 50_000_000))

# --------------------------------------------------------------------------- #
# Canonical serialization + hashing helpers.
# --------------------------------------------------------------------------- #
def canonical_json(obj: Any) -> str:
    """Deterministic JSON serialization: sorted keys, compact separators, UTF-8 safe."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def h16(text: str) -> str:
    """Short deterministic 16-hex-char id fragment."""
    return sha256_text(text)[:16]


# Fingerprint type labels.
FP_FULL = "FULL_CONTENT_SHA256"
FP_PARTIAL = "PARTIAL_CONTENT_FINGERPRINT"

# Parser status vocabulary.
PS_OK = "OK"
PS_OK_ROWS_SKIPPED = "OK_ROW_COUNT_SKIPPED_LARGE_FILE"
PS_EMPTY = "EMPTY_FILE"
PS_PARSE_ERROR = "PARSE_ERROR"
PS_UNSUPPORTED = "UNSUPPORTED_FORMAT"
PS_NO_LIBRARY = "UNSUPPORTED_NO_LIBRARY_AVAILABLE"

# Default parsing thresholds (overridable via the config's "parsing"/"hashing" blocks).
DEFAULT_FULL_HASH_MAX_BYTES = 268_435_456          # 256 MiB
DEFAULT_SAMPLE_EDGE_BYTES = 1_048_576              # 1 MiB per edge for sampled fp
DEFAULT_READ_CHUNK_BYTES = 4_194_304               # 4 MiB streaming chunk
DEFAULT_ROW_COUNT_MAX_BYTES = 134_217_728          # 128 MiB
CANON_EXTRACT_MAX_BYTES = 16_777_216               # 16 MiB: parse into memory for canon extraction


def _hcfg(config: Optional[dict], section: str, key: str, default: Any) -> Any:
    try:
        return (config or {}).get(section, {}).get(key, default)
    except AttributeError:
        return default


def fingerprint_file(path: Path, config: Optional[dict] = None) -> dict[str, Any]:
    """Return {fingerprint_type, fingerprint, size_bytes}.

    Files at or below the threshold get a full streaming SHA-256. Larger files get a
    deterministic sampled fingerprint over (size, head, middle, tail) byte ranges,
    explicitly labelled PARTIAL_CONTENT_FINGERPRINT so it is never confused with a
    complete content hash.
    """
    threshold = int(_hcfg(config, "hashing", "full_hash_max_bytes", DEFAULT_FULL_HASH_MAX_BYTES))
    edge = int(_hcfg(config, "hashing", "sampled_fingerprint_edge_bytes", DEFAULT_SAMPLE_EDGE_BYTES))
    chunk = int(_hcfg(config, "hashing", "read_chunk_bytes", DEFAULT_READ_CHUNK_BYTES))
    size = path.stat().st_size

    if size <= threshold:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            while True:
                block = fh.read(chunk)
                if not block:
                    break
                h.update(block)
        return {"fingerprint_type": FP_FULL, "fingerprint": h.hexdigest(), "size_bytes": size}

    # Sampled fingerprint: hash size + three fixed byte windows. Deterministic.
    h = hashlib.sha256()
    h.update(("size=%d;edge=%d;" % (size, edge)).encode("ascii"))
    with open(path, "rb") as fh:
        fh.seek(0)
        h.update(fh.read(edge))
        mid = max(0, (size // 2) - (edge // 2))
        fh.seek(mid)
        h.update(fh.read(edge))
        tail = max(0, size - edge)
        fh.seek(tail)
        h.update(fh.read(edge))
    return {"fingerprint_type": FP_PARTIAL, "fingerprint": h.hexdigest(), "size_bytes": size}


# --------------------------------------------------------------------------- #
# Format detection.
# --------------------------------------------------------------------------- #
STRUCTURED_EXTS = {".json", ".jsonl", ".ndjson", ".csv", ".tsv", ".md", ".markdown",
                   ".txt", ".toml", ".yaml", ".yml", ".sqlite", ".sqlite3", ".db", ".parquet"}
CODE_EXTS = {".py"}
BINARY_UNSUPPORTED_EXTS = {".pkl", ".pickle", ".npz", ".npy", ".zip", ".gz", ".tar",
                           ".pyc", ".pyo", ".html", ".htm", ".png", ".jpg", ".jpeg",
                           ".gif", ".pdf", ".xlsx", ".xls", ".parq", ".feather", ".bin"}


def category_for_ext(ext: str) -> str:
    ext = ext.lower()
    if ext in (".json", ".jsonl", ".ndjson"):
        return "structured_json"
    if ext in (".csv", ".tsv"):
        return "tabular_csv"
    if ext in (".md", ".markdown", ".txt"):
        return "document_text"
    if ext == ".toml":
        return "config_toml"
    if ext in (".yaml", ".yml"):
        return "config_yaml"
    if ext in (".sqlite", ".sqlite3", ".db"):
        return "database_sqlite"
    if ext == ".parquet":
        return "columnar_parquet"
    if ext in CODE_EXTS:
        return "code"
    if ext in BINARY_UNSUPPORTED_EXTS:
        return "binary_unsupported"
    return "other"


# --------------------------------------------------------------------------- #
# Individual importers. Each returns a metadata dict (never raises); the caller
# supplies the raw parsed object separately when small enough for extraction.
# --------------------------------------------------------------------------- #
def _blank_result() -> dict[str, Any]:
    return {"parser": None, "parser_status": PS_OK, "row_count": None, "columns": None,
            "structure": None, "warnings": [], "obj": None}


def _keys_sorted(d: dict, cap: int = 400) -> list:
    ks = sorted(str(k) for k in d.keys())
    return ks[:cap]


def _import_json(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "json"
    try:
        text = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeDecodeError) as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("decode: %s" % exc)
        return r
    if not text.strip():
        r["parser_status"] = PS_EMPTY
        return r
    try:
        obj = json.loads(text)
    except ValueError as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("json: %s" % exc)
        return r
    if isinstance(obj, dict):
        r["structure"] = "object"
        r["columns"] = _keys_sorted(obj)
        r["row_count"] = len(obj)
    elif isinstance(obj, list):
        r["structure"] = "array"
        r["row_count"] = len(obj)
        if obj and isinstance(obj[0], dict):
            r["columns"] = _keys_sorted(obj[0])
    else:
        r["structure"] = "scalar"
    if size <= CANON_EXTRACT_MAX_BYTES:
        r["obj"] = obj
    return r


def _import_jsonl(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "jsonl"
    r["structure"] = "records"
    row_cap = int(_hcfg(config, "parsing", "row_count_max_bytes", DEFAULT_ROW_COUNT_MAX_BYTES))
    count = 0
    first_keys = None
    bad = 0
    records: list = []
    keep = size <= CANON_EXTRACT_MAX_BYTES
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                count += 1
                try:
                    o = json.loads(s)
                except ValueError:
                    bad += 1
                    continue
                if first_keys is None and isinstance(o, dict):
                    first_keys = _keys_sorted(o)
                if keep and isinstance(o, dict):
                    records.append(o)
    except (OSError, UnicodeDecodeError) as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("read: %s" % exc)
        return r
    if count == 0:
        r["parser_status"] = PS_EMPTY
        return r
    r["row_count"] = count
    r["columns"] = first_keys
    if bad:
        r["warnings"].append("%d unparseable line(s)" % bad)
    if size > row_cap:
        r["warnings"].append("large file; row_count is a line count")
    if keep:
        r["obj"] = records
    return r


def _import_delimited(path: Path, size: int, config: Optional[dict], delim: str) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "csv" if delim == "," else "tsv"
    r["structure"] = "table"
    row_cap = int(_hcfg(config, "parsing", "row_count_max_bytes", DEFAULT_ROW_COUNT_MAX_BYTES))
    keep = size <= CANON_EXTRACT_MAX_BYTES
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.reader(fh, delimiter=delim)
            try:
                header = next(reader)
            except StopIteration:
                r["parser_status"] = PS_EMPTY
                return r
            r["columns"] = [str(c) for c in header]
            if size > row_cap:
                r["parser_status"] = PS_OK_ROWS_SKIPPED
                r["warnings"].append("row count skipped: file > row_count_max_bytes")
                return r
            rows: list = []
            n = 0
            for rec in reader:
                n += 1
                if keep:
                    rows.append(dict(zip(r["columns"], rec)))
            r["row_count"] = n
            if keep:
                r["obj"] = rows
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("csv: %s" % exc)
    return r


def _import_markdown(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "markdown"
    r["structure"] = "document"
    try:
        text = path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("read: %s" % exc)
        return r
    lines = text.splitlines()
    r["row_count"] = len(lines)
    headings = [ln.strip() for ln in lines if ln.lstrip().startswith("#")]
    r["columns"] = headings[:40] if headings else None
    if size <= CANON_EXTRACT_MAX_BYTES:
        r["obj"] = {"text": text, "headings": headings}
    if not text.strip():
        r["parser_status"] = PS_EMPTY
    return r


def _import_text(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "text"
    r["structure"] = "document"
    try:
        text = path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("read: %s" % exc)
        return r
    r["row_count"] = len(text.splitlines())
    if size <= CANON_EXTRACT_MAX_BYTES:
        r["obj"] = {"text": text}
    if not text.strip():
        r["parser_status"] = PS_EMPTY
    return r


def _import_toml(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "toml"
    try:
        import tomllib  # py311+ stdlib
    except ImportError:
        r["parser_status"] = PS_NO_LIBRARY
        r["warnings"].append("tomllib unavailable")
        return r
    try:
        with open(path, "rb") as fh:
            obj = tomllib.load(fh)
    except (OSError, ValueError) as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("toml: %s" % exc)
        return r
    r["structure"] = "object"
    if isinstance(obj, dict):
        r["columns"] = _keys_sorted(obj)
        r["row_count"] = len(obj)
    if size <= CANON_EXTRACT_MAX_BYTES:
        r["obj"] = obj
    return r


def _import_yaml(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "yaml"
    try:
        import yaml  # optional; only if already installed
    except ImportError:
        r["parser_status"] = PS_NO_LIBRARY
        r["warnings"].append("PyYAML not installed; recorded without parse")
        return r
    try:
        text = path.read_text(encoding="utf-8-sig")
        obj = yaml.safe_load(text)  # safe_load never constructs arbitrary objects
    except (OSError, UnicodeDecodeError, Exception) as exc:  # noqa: BLE001
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("yaml: %s" % exc)
        return r
    if isinstance(obj, dict):
        r["structure"] = "object"
        r["columns"] = _keys_sorted(obj)
        r["row_count"] = len(obj)
    elif isinstance(obj, list):
        r["structure"] = "array"
        r["row_count"] = len(obj)
    else:
        r["structure"] = "scalar"
    if size <= CANON_EXTRACT_MAX_BYTES:
        r["obj"] = obj
    return r


def _import_sqlite(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "sqlite"
    r["structure"] = "database"
    import sqlite3
    uri = "file:%s?mode=ro&immutable=1" % path.as_posix()
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=5)
    except sqlite3.Error as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("open: %s" % exc)
        return r
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cur.fetchall()]
        r["columns"] = tables
        counts = {}
        for t in tables:
            try:
                cur.execute('SELECT COUNT(*) FROM "%s"' % t.replace('"', '""'))
                counts[t] = cur.fetchone()[0]
            except sqlite3.Error:
                counts[t] = None
        r["row_count"] = sum(v for v in counts.values() if isinstance(v, int))
        if size <= CANON_EXTRACT_MAX_BYTES:
            r["obj"] = {"tables": tables, "table_row_counts": counts}
    except sqlite3.Error as exc:
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("query: %s" % exc)
    finally:
        conn.close()
    return r


def _import_parquet(path: Path, size: int, config: Optional[dict]) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = "parquet"
    r["structure"] = "columnar"
    try:
        import pyarrow.parquet as pq  # optional
    except ImportError:
        r["parser_status"] = PS_NO_LIBRARY
        r["warnings"].append("pyarrow not installed; metadata not read")
        return r
    try:
        md = pq.read_metadata(str(path))
        r["row_count"] = md.num_rows
        r["columns"] = [md.schema.column(i).name for i in range(md.num_columns)]
        if size <= CANON_EXTRACT_MAX_BYTES:
            r["obj"] = {"num_rows": md.num_rows, "num_columns": md.num_columns}
    except Exception as exc:  # noqa: BLE001
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("parquet: %s" % exc)
    return r


def _import_unsupported(path: Path, size: int, category: str) -> dict[str, Any]:
    r = _blank_result()
    r["parser"] = None
    r["parser_status"] = PS_UNSUPPORTED
    r["structure"] = category
    r["warnings"].append("no importer for this format; recorded as opaque artifact (not parsed, not executed)")
    return r


def parse_artifact(path: Path, ext: str, size: int, category: str,
                   config: Optional[dict] = None) -> dict[str, Any]:
    """Dispatch to the right importer by extension. Never raises."""
    ext = ext.lower()
    try:
        if ext in (".json",):
            return _import_json(path, size, config)
        if ext in (".jsonl", ".ndjson"):
            return _import_jsonl(path, size, config)
        if ext == ".csv":
            return _import_delimited(path, size, config, ",")
        if ext == ".tsv":
            return _import_delimited(path, size, config, "\t")
        if ext in (".md", ".markdown"):
            return _import_markdown(path, size, config)
        if ext in (".txt",):
            return _import_text(path, size, config)
        if ext == ".toml":
            return _import_toml(path, size, config)
        if ext in (".yaml", ".yml"):
            return _import_yaml(path, size, config)
        if ext in (".sqlite", ".sqlite3", ".db"):
            return _import_sqlite(path, size, config)
        if ext == ".parquet":
            return _import_parquet(path, size, config)
        if ext == ".py":
            return _import_text(path, size, config)  # code recorded as text metadata
        return _import_unsupported(path, size, category)
    except Exception as exc:  # noqa: BLE001 — importers should not raise, but be safe
        r = _blank_result()
        r["parser_status"] = PS_PARSE_ERROR
        r["warnings"].append("importer crash: %s: %s" % (type(exc).__name__, exc))
        return r


# --------------------------------------------------------------------------- #
# Field-alias normalization (no fabrication; missing -> None + completeness).
# --------------------------------------------------------------------------- #
FIELD_ALIASES: dict[str, list[str]] = {
    "native_id": ["experiment_id", "exp_id", "candidate_id", "trade_idea_id", "signal_id",
                  "hypothesis_id", "wave_id", "id", "candidate_signal_id", "run_id"],
    "name": ["name", "display_name", "candidate_id", "signal_id"],
    "family": ["family", "candidate_family", "signal_family", "data_family",
               "information_family", "factor_family"],
    "hypothesis_ref": ["hypothesis_id", "parent_hypothesis", "hypothesis_bank_id"],
    "universe": ["universe"],
    "survivorship": ["survivorship", "survivorship_treatment"],
    "point_in_time": ["point_in_time", "pit", "pit_treatment"],
    "horizon": ["horizon", "evaluation_horizons", "horizon_eligible_closes", "horizons"],
    "rebalance": ["rebalance", "rebalance_cadence", "cadence"],
    "cost_bps": ["cost_bps_per_side", "cost_bps", "primary_cost_bps_per_side",
                 "transaction_cost_bps_per_side"],
    "decision": ["decision", "terminal_decision", "final_state", "promotion_status",
                 "validation_status", "signal_status", "action", "alpha_promotion"],
    "status": ["status", "state", "stage"],
    "reject_reason": ["reject_reason", "rejection_reason", "reason", "reasons",
                      "stop_reason", "current_blocker", "note"],
    "ic": ["ic", "mean_ic", "rank_ic_mean", "ic_mean"],
    "ic_t": ["ic_t", "ic_t_stat", "rank_ic_t", "rank_ic_nw_t"],
    "net25": ["net25", "ev_after_25bps", "net_spy_excess_ann"],
    "spread": ["spread", "top_minus_bottom_spread_monthly", "lift_vs_control"],
    "turnover": ["turnover", "turnover_monthly_oneside", "mean_turnover"],
    "drawdown": ["drawdown", "max_drawdown"],
    "coverage_pct": ["coverage_pct", "coverage_fraction"],
    "benchmark": ["benchmark", "benchmark_ticker"],
    "model_id": ["model_id", "baseline_model", "candidate_family", "target_book"],
    "spec_hash": ["spec_hash"],
    "code_commit": ["code_commit", "git_commit", "commit"],
    "data_cutoff": ["data_cutoff", "data_version", "as_of"],
    "model_params": ["model_params"],
    "portfolio_params": ["portfolio_params", "portfolio_rules"],
    "provider_dependency": ["provider_dependency", "needs_provider", "required_data_families",
                            "data_required"],
}

# Canonical fields that constitute "core" experiment metadata for completeness scoring.
CORE_EXPERIMENT_FIELDS = ["family", "universe", "horizon", "model_params", "cost_bps"]


def normalize_record(raw: dict) -> dict[str, Any]:
    """Map a raw source dict onto canonical fields via the alias table.

    Never fabricates: an absent canonical field maps to None. Returns the normalized
    dict plus a metadata_completeness block listing which core fields were present.
    """
    lower = {str(k).lower(): v for k, v in raw.items()} if isinstance(raw, dict) else {}
    norm: dict[str, Any] = {}
    for canon, aliases in FIELD_ALIASES.items():
        val = None
        for a in aliases:
            if a in lower and lower[a] not in (None, ""):
                val = lower[a]
                break
        norm[canon] = val
    present_core = [f for f in CORE_EXPERIMENT_FIELDS if norm.get(f) not in (None, "")]
    norm["metadata_completeness"] = {
        "present_core_fields": present_core,
        "core_fields_total": len(CORE_EXPERIMENT_FIELDS),
        "core_fields_present": len(present_core),
        "is_complete": len(present_core) == len(CORE_EXPERIMENT_FIELDS),
    }
    return norm


# --------------------------------------------------------------------------- #
# Record-type classification (deterministic ordered rules).
# --------------------------------------------------------------------------- #
REC_EXPERIMENT = "experiment"
REC_HYPOTHESIS = "hypothesis"
REC_SIGNAL = "signal"
REC_DECISION = "decision"
REC_DATASET = "dataset"
REC_FEATURE = "feature"
REC_EVIDENCE = "evidence"
REC_UNCLASSIFIED = "unclassified"

_TYPE_RULES: list[tuple[str, str]] = [
    ("graveyard", REC_DECISION),
    ("rejection_report", REC_DECISION),
    ("rejected_hypothesis", REC_DECISION),
    ("provider_blocker", REC_DATASET),
    ("missing_data_family", REC_DATASET),
    ("trailing_price_panel_manifest", REC_DATASET),
    ("manifest", REC_DATASET),
    ("hypothes", REC_HYPOTHESIS),
    ("signal_promotion", REC_SIGNAL),
    ("promotion_log", REC_SIGNAL),
    ("candidate_signal", REC_SIGNAL),
    ("candidate_report", REC_SIGNAL),
    ("trade_idea", REC_SIGNAL),
    ("leaderboard", REC_SIGNAL),
    ("price_alpha_registry", REC_SIGNAL),
    ("alpha_registry", REC_SIGNAL),
    ("wave_registry", REC_EXPERIMENT),
    ("experiment_results", REC_EXPERIMENT),
    ("experiment_index", REC_EXPERIMENT),
    ("experiment_queue", REC_EXPERIMENT),
    ("feature_campaign", REC_FEATURE),
    ("feature", REC_FEATURE),
    ("gate_results", REC_EVIDENCE),
    ("metrics", REC_EVIDENCE),
    ("diagnostics", REC_EVIDENCE),
    ("correlation", REC_EVIDENCE),
    ("next_action", REC_DECISION),
    ("daemon_state", REC_DECISION),
    ("factory_state", REC_DECISION),
    ("run_state", REC_DECISION),
    ("research_memory", REC_DECISION),
    ("latest_run", REC_DECISION),
    ("executive_recommendation", REC_DECISION),
    ("executive_report", REC_DECISION),
    ("campaign_report", REC_DECISION),
    ("final_report", REC_DECISION),
    ("decision", REC_DECISION),
    ("status", REC_DECISION),
    ("campaign", REC_DECISION),
    ("panel", REC_DATASET),
    ("_inputs", REC_DATASET),
    ("provenance", REC_DATASET),
]


def classify_record_type(relpath: str, filename: str) -> str:
    """Deterministic best-effort record-type tag from path + filename."""
    fn = filename.lower()
    rp = relpath.lower().replace("\\", "/")
    # Experiment directories (research_agent campaigns): .../experiments/exp_*/<file>.
    if "/experiments/exp" in rp or rp.startswith("experiments/exp"):
        return REC_EXPERIMENT
    for token, rtype in _TYPE_RULES:
        if token in fn or token in rp:
            return rtype
    return REC_UNCLASSIFIED


# --------------------------------------------------------------------------- #
# Information-family classification (explicit keyword rules, no LLM).
# --------------------------------------------------------------------------- #
INFORMATION_FAMILIES = [
    "price_momentum", "short_term_reversal", "long_term_reversal", "volatility",
    "liquidity", "volume", "market_beta", "sector_exposure", "quality", "profitability",
    "investment", "value", "growth", "earnings", "estimates_and_revisions",
    "earnings_surprise", "revenue_surprise", "insider_activity", "news_and_events",
    "filings_and_textual", "macro_and_regime", "options", "short_activity",
    "corporate_actions", "alternative_data", "ensemble_and_interaction_models",
    "portfolio_construction", "risk_overlays", "unclassified_information",
]

# Ordered (keyword_substrings, family). First hit wins — specific before generic.
_FAMILY_RULES: list[tuple[tuple[str, ...], str]] = [
    (("revenue surprise", "revenue_surprise", "sales surprise"), "revenue_surprise"),
    (("earnings surprise", "earnings_surprise", "surprise", "sue", "post-earnings", "pead"), "earnings_surprise"),
    (("revision", "estimate", "analyst"), "estimates_and_revisions"),
    (("insider",), "insider_activity"),
    (("short interest", "short_interest", "si_", "days_to_cover", "short activity"), "short_activity"),
    (("option", "implied vol", "implied_vol", "_iv", "iv_", "skew", "gamma"), "options"),
    (("news", "sentiment", "headline", "event"), "news_and_events"),
    (("filing", "10-k", "10k", "8-k", "edgar", "textual", "mda", "10-q"), "filings_and_textual"),
    (("macro", "regime", "rates", "cross-asset", "cross_asset", "yield"), "macro_and_regime"),
    (("corporate action", "split", "buyback", "dividend"), "corporate_actions"),
    (("alternative data", "alt_data", "alt-data", "satellite", "cardspend"), "alternative_data"),
    (("accrual", "earnings quality", "earnings_quality"), "quality"),
    (("profitab", "gross_profit", "gross profit", "fcf", "free cash", "roa", "roe"), "profitability"),
    (("asset growth", "asset_growth", "investment", "capex"), "investment"),
    (("book_to", "book-to", "earnings yield", "value", "cheap", "b/p", "e/p"), "value"),
    (("growth",), "growth"),
    (("quality",), "quality"),
    (("earnings",), "earnings"),
    (("reversal", "revert", "mean_reversion", "mean reversion"), "short_term_reversal"),
    (("momentum", "mom_6", "mom_12", "mom_", "trend", "relative strength", "relative_strength"), "price_momentum"),
    (("beta",), "market_beta"),
    (("volatilit", "low_vol", "low vol", "realized_vol", "idiosyncratic"), "volatility"),
    (("liquidit", "amihud", "adv", "turnover_dollar"), "liquidity"),
    (("volume",), "volume"),
    (("sector",), "sector_exposure"),
    (("ensemble", "blend", "interaction", "composite", "combination", "multi-leg",
      "fundamental_momentum", "fund_momentum"), "ensemble_and_interaction_models"),
    (("hedge", "overlay", "drawdown", "risk control", "risk_control", "beta-controlled",
      "vol target", "vol_target"), "risk_overlays"),
    (("portfolio", "construction", "weighting", "top_n", "top25", "top50", "turnover_penalty"),
     "portfolio_construction"),
]


def classify_information_family(*texts: Optional[str]) -> str:
    """Map free-text family/name/thesis onto the coverage taxonomy deterministically."""
    hay = " ".join(t for t in texts if t).lower()
    if not hay.strip():
        return "unclassified_information"
    for keywords, fam in _FAMILY_RULES:
        for kw in keywords:
            if kw in hay:
                return fam
    return "unclassified_information"


# --------------------------------------------------------------------------- #
# Experiment fingerprints + duplicate comparison.
# --------------------------------------------------------------------------- #
def _norm_scalar(v: Any) -> Any:
    if isinstance(v, str):
        return v.strip().lower()
    if isinstance(v, list):
        return sorted(_norm_scalar(x) for x in v)
    if isinstance(v, dict):
        return {str(k).lower(): _norm_scalar(x) for k, x in sorted(v.items())}
    return v


def exact_experiment_fingerprint(norm: dict) -> str:
    """Deterministic EXACT fingerprint over the full normalized experiment spec.

    If a source-native spec_hash is present it is authoritative and used verbatim so
    two records that the source itself deemed identical always collide. Otherwise the
    fingerprint is computed from the normalized specification fields.
    """
    if norm.get("spec_hash"):
        return "spec:" + str(norm["spec_hash"])
    payload = {
        "model_id": _norm_scalar(norm.get("model_id")),
        "family": _norm_scalar(norm.get("family")),
        "universe": _norm_scalar(norm.get("universe")),
        "horizon": _norm_scalar(norm.get("horizon")),
        "rebalance": _norm_scalar(norm.get("rebalance")),
        "cost_bps": _norm_scalar(norm.get("cost_bps")),
        "model_params": _norm_scalar(norm.get("model_params")),
        "portfolio_params": _norm_scalar(norm.get("portfolio_params")),
        "data_cutoff": _norm_scalar(norm.get("data_cutoff")),
    }
    return "calc:" + sha256_text(canonical_json(payload))


def information_family_fingerprint(norm: dict) -> str:
    """Deterministic coarse fingerprint over the economic-information family."""
    fam = classify_information_family(
        str(norm.get("family") or ""), str(norm.get("name") or ""),
        str(norm.get("model_id") or ""))
    payload = {
        "information_family": fam,
        "feature_family": _norm_scalar(norm.get("family")),
        "prediction_target_family": _norm_scalar(norm.get("model_id")),
        "horizon_family": _norm_scalar(norm.get("horizon")),
        "universe_family": _norm_scalar(norm.get("universe")),
    }
    return sha256_text(canonical_json(payload))


# Comparison verdicts.
CMP_EXACT_DUPLICATE = "EXACT_DUPLICATE"
CMP_PARAMETER_VARIANT = "PARAMETER_VARIANT"
CMP_SAME_INFO_FAMILY = "SAME_INFORMATION_FAMILY"
CMP_DISTINCT = "DISTINCT_INFORMATION"
CMP_INCOMPLETE = "INCOMPLETE_METADATA"
CMP_CONFLICTING = "CONFLICTING_METADATA"


def compare_experiments(a: dict, b: dict) -> str:
    """Classify the relationship between two normalized experiment records."""
    ac = a.get("metadata_completeness", {}).get("is_complete", False) or bool(a.get("spec_hash"))
    bc = b.get("metadata_completeness", {}).get("is_complete", False) or bool(b.get("spec_hash"))
    if not ac or not bc:
        return CMP_INCOMPLETE
    if exact_experiment_fingerprint(a) == exact_experiment_fingerprint(b):
        return CMP_EXACT_DUPLICATE
    if information_family_fingerprint(a) == information_family_fingerprint(b):
        # Same information family + same coarse target: differ only in parameters?
        same_family = _norm_scalar(a.get("family")) == _norm_scalar(b.get("family"))
        same_universe = _norm_scalar(a.get("universe")) == _norm_scalar(b.get("universe"))
        if same_family and same_universe:
            return CMP_PARAMETER_VARIANT
        return CMP_SAME_INFO_FAMILY
    return CMP_DISTINCT


__all__ = [
    "IMPORTER_VERSION",
    "canonical_json", "sha256_hex", "sha256_text", "h16",
    "FP_FULL", "FP_PARTIAL", "fingerprint_file",
    "PS_OK", "PS_OK_ROWS_SKIPPED", "PS_EMPTY", "PS_PARSE_ERROR", "PS_UNSUPPORTED", "PS_NO_LIBRARY",
    "category_for_ext", "parse_artifact",
    "FIELD_ALIASES", "CORE_EXPERIMENT_FIELDS", "normalize_record",
    "REC_EXPERIMENT", "REC_HYPOTHESIS", "REC_SIGNAL", "REC_DECISION", "REC_DATASET",
    "REC_FEATURE", "REC_EVIDENCE", "REC_UNCLASSIFIED", "classify_record_type",
    "INFORMATION_FAMILIES", "classify_information_family",
    "exact_experiment_fingerprint", "information_family_fingerprint",
    "CMP_EXACT_DUPLICATE", "CMP_PARAMETER_VARIANT", "CMP_SAME_INFO_FAMILY",
    "CMP_DISTINCT", "CMP_INCOMPLETE", "CMP_CONFLICTING", "compare_experiments",
]
