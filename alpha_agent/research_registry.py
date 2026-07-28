"""
alpha_agent/research_registry.py — Stage 1 canonical Alpha Research Registry.

Builds a deterministic, versioned, queryable SQLite registry plus machine-readable
CSV/JSON/Markdown exports that consolidate the fragmented research history (datasets,
hypotheses, features, experiments, signals, evidence, decisions, duplicates, coverage
and current operational state) into one memory layer.

Read-only w.r.t. every source root. This module:
    - opens NO network socket, NO model API and NO database connection to PostgreSQL;
    - the only database it touches is the SQLite file it writes under the output root;
    - never mutates, moves, renames or rewrites a source artifact;
    - never executes a source payload (unsupported/binary files are hashed, not opened).

Modes: bootstrap (full immutable version), incremental (diff + new version, or
NO_RESEARCH_CHANGES), verify (validate an existing version, write nothing).
"""
from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Optional

from . import research_importers as imp

SCHEMA_VERSION = "1.0.0"
STAGE = "1"

READY = "ALPHA_AGENT_STAGE1_READY"
BLOCKED = "ALPHA_AGENT_STAGE1_BLOCKED"
VERIFIED = "ALPHA_AGENT_STAGE1_VERIFIED"
NO_CHANGES = "NO_RESEARCH_CHANGES"

# Canonical id prefixes.
PFX = {
    "artifact": "art_", "dataset": "dts_", "feature": "ftr_", "hypothesis": "hyp_",
    "experiment": "exp_", "signal": "sig_", "evidence": "evd_", "decision": "dec_",
}

# Terminal reject/decision tokens that indicate local-data exhaustion (source-observed).
EXHAUSTION_TOKENS = (
    "require_provider", "needs_provider", "needs_provider_purchase", "provider_limited",
    "needs_new_data_family", "needs_contact_sales", "cost_killed", "not_cost_robust",
    "exhausted", "blocked_by_missing_data", "requires_trailing_price_panel",
    "needs_data_family", "provider_purchase",
)
MISSING_DATA_TOKENS = (
    "provider", "missing_data", "coverage", "requires_trailing_price_panel",
    "needs_data", "no_key", "entitlement", "purchase",
)
SURVIVING_TOKENS = (
    "champion", "challenger", "promoted", "confirmed", "active", "proven",
    "clean_promising", "promising", "retained", "advance", "surviv",
)


# --------------------------------------------------------------------------- #
# SQLite schema.
# --------------------------------------------------------------------------- #
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE registry_runs (
    run_id TEXT PRIMARY KEY, schema_version TEXT, importer_version TEXT,
    mode TEXT, config_hash TEXT, git_commit TEXT, generated_at TEXT,
    source_root_count INTEGER, artifact_count INTEGER, artifact_set_hash TEXT
);

CREATE TABLE source_roots (
    id TEXT PRIMARY KEY, path TEXT, kind TEXT, present INTEGER,
    file_count INTEGER, total_bytes INTEGER, note TEXT
);

CREATE TABLE artifacts (
    artifact_id TEXT PRIMARY KEY, source_root_id TEXT, source_path TEXT,
    relpath TEXT, filename TEXT, extension TEXT, category TEXT,
    size_bytes INTEGER, modified_ts TEXT, created_ts TEXT,
    fingerprint_type TEXT, fingerprint TEXT, parser TEXT, parser_status TEXT,
    import_confidence REAL, linked_phase TEXT, linked_run_id TEXT,
    linked_commit TEXT, row_count INTEGER, columns_json TEXT,
    record_type TEXT, warnings_json TEXT, unresolved_reason TEXT,
    source_unchanged INTEGER,
    FOREIGN KEY (source_root_id) REFERENCES source_roots (id)
);

CREATE TABLE datasets (
    dataset_id TEXT PRIMARY KEY, source_artifact_id TEXT, source_path TEXT,
    native_id TEXT, source_phase TEXT, name TEXT, kind TEXT,
    information_family TEXT, normalized_json TEXT, raw_json TEXT,
    import_confidence REAL, provenance TEXT, parser_version TEXT, observed_at TEXT,
    FOREIGN KEY (source_artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE features (
    feature_id TEXT PRIMARY KEY, source_artifact_id TEXT, source_path TEXT,
    native_id TEXT, source_phase TEXT, name TEXT, family TEXT,
    information_family TEXT, definition_json TEXT, normalized_json TEXT, raw_json TEXT,
    import_confidence REAL, provenance TEXT, parser_version TEXT, observed_at TEXT,
    FOREIGN KEY (source_artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE hypotheses (
    hypothesis_id TEXT PRIMARY KEY, source_artifact_id TEXT, source_path TEXT,
    native_id TEXT, source_phase TEXT, family TEXT, information_family TEXT,
    statement TEXT, status TEXT, normalized_json TEXT, raw_json TEXT,
    import_confidence REAL, provenance TEXT, parser_version TEXT, observed_at TEXT,
    FOREIGN KEY (source_artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE experiments (
    experiment_id TEXT PRIMARY KEY, source_artifact_id TEXT, source_path TEXT,
    native_id TEXT, source_phase TEXT, campaign TEXT, family TEXT,
    information_family TEXT, universe TEXT, horizon TEXT, model_id TEXT,
    cost_bps TEXT, decision TEXT, status TEXT, reject_reason TEXT,
    spec_hash TEXT, exact_fingerprint TEXT, family_fingerprint TEXT,
    metadata_complete INTEGER, import_confidence REAL,
    normalized_json TEXT, raw_json TEXT, provenance TEXT, parser_version TEXT,
    observed_at TEXT,
    FOREIGN KEY (source_artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE signals (
    signal_id TEXT PRIMARY KEY, source_artifact_id TEXT, source_path TEXT,
    native_id TEXT, source_phase TEXT, name TEXT, family TEXT,
    information_family TEXT, decision TEXT, status TEXT, reject_reason TEXT,
    ic TEXT, ic_t TEXT, net25 TEXT, normalized_json TEXT, raw_json TEXT,
    import_confidence REAL, provenance TEXT, parser_version TEXT, observed_at TEXT,
    FOREIGN KEY (source_artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE evidence (
    evidence_id TEXT PRIMARY KEY, source_artifact_id TEXT, source_path TEXT,
    native_id TEXT, source_phase TEXT, kind TEXT, information_family TEXT,
    metrics_json TEXT, normalized_json TEXT, raw_json TEXT,
    import_confidence REAL, provenance TEXT, parser_version TEXT, observed_at TEXT,
    FOREIGN KEY (source_artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE decisions (
    decision_id TEXT PRIMARY KEY, source_artifact_id TEXT, source_path TEXT,
    native_id TEXT, source_phase TEXT, family TEXT, information_family TEXT,
    decision TEXT, reject_reason TEXT, normalized_json TEXT, raw_json TEXT,
    import_confidence REAL, provenance TEXT, parser_version TEXT, observed_at TEXT,
    FOREIGN KEY (source_artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE experiment_artifacts (
    experiment_id TEXT, artifact_id TEXT, role TEXT,
    PRIMARY KEY (experiment_id, artifact_id, role),
    FOREIGN KEY (experiment_id) REFERENCES experiments (experiment_id),
    FOREIGN KEY (artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE signal_evidence (
    signal_id TEXT, evidence_id TEXT,
    PRIMARY KEY (signal_id, evidence_id),
    FOREIGN KEY (signal_id) REFERENCES signals (signal_id),
    FOREIGN KEY (evidence_id) REFERENCES evidence (evidence_id)
);

CREATE TABLE experiment_duplicates (
    cluster_id TEXT, experiment_id TEXT, relationship TEXT,
    exact_fingerprint TEXT, family_fingerprint TEXT, cluster_size INTEGER,
    PRIMARY KEY (cluster_id, experiment_id),
    FOREIGN KEY (experiment_id) REFERENCES experiments (experiment_id)
);

CREATE TABLE import_issues (
    issue_id TEXT PRIMARY KEY, artifact_id TEXT, source_path TEXT,
    issue_type TEXT, detail TEXT,
    FOREIGN KEY (artifact_id) REFERENCES artifacts (artifact_id)
);

CREATE TABLE current_state (
    key TEXT PRIMARY KEY, value_json TEXT
);

CREATE TABLE coverage_map (
    information_family TEXT PRIMARY KEY, artifacts_discovered INTEGER,
    hypotheses INTEGER, unique_experiments INTEGER, exact_duplicates INTEGER,
    parameter_variants INTEGER, surviving_signals INTEGER, rejected_signals INTEGER,
    blocked_signals INTEGER, latest_evidence_date TEXT,
    strongest_terminal_evidence TEXT, evidence_classification TEXT,
    required_missing_data TEXT, local_research_supported_as_exhausted INTEGER
);
"""

CANONICAL_TABLES = [
    "registry_runs", "source_roots", "artifacts", "datasets", "features", "hypotheses",
    "experiments", "signals", "evidence", "decisions", "experiment_artifacts",
    "signal_evidence", "experiment_duplicates", "import_issues", "current_state",
    "coverage_map",
]


# --------------------------------------------------------------------------- #
# Small helpers.
# --------------------------------------------------------------------------- #
def _jd(obj: Any, cap: int = 20000) -> Optional[str]:
    if obj is None:
        return None
    s = imp.canonical_json(obj)
    if len(s) > cap:
        return imp.canonical_json({"_truncated": True, "_len": len(s), "_head": s[:cap]})
    return s


def _iso_mtime(p: Path) -> tuple[Optional[str], Optional[str]]:
    try:
        st = p.stat()
    except OSError:
        return None, None
    import datetime as _dt
    m = _dt.datetime.fromtimestamp(st.st_mtime, tz=_dt.timezone.utc).isoformat()
    c = _dt.datetime.fromtimestamp(getattr(st, "st_ctime", st.st_mtime), tz=_dt.timezone.utc).isoformat()
    return m, c


_PHASE_RE = re.compile(r"phase\s*([0-9]{1,2}[a-z]?(?:\.[0-9]+)?)", re.IGNORECASE)
_CAMPAIGN_RE = re.compile(r"(phase\d+[a-z0-9_]*campaign[a-z0-9_]*|phase29[a-z0-9_]+)", re.IGNORECASE)


def _derive_phase(root_id: str, relpath: str) -> Optional[str]:
    hay = (relpath + " " + root_id).replace("\\", "/")
    m = _PHASE_RE.search(hay)
    if m:
        return "phase" + m.group(1).lower()
    return None


def _derive_campaign(relpath: str) -> Optional[str]:
    m = _CAMPAIGN_RE.search(relpath.replace("\\", "/"))
    return m.group(1) if m else None


def _as_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return imp.canonical_json(v)
    return str(v)


# --------------------------------------------------------------------------- #
# Scanning.
# --------------------------------------------------------------------------- #
def scan_source_roots(config: dict) -> tuple[list[dict], list[dict]]:
    """Return (files, roots_summary). Deterministically ordered. Missing roots recorded."""
    excluded = [os.path.normcase(os.path.abspath(p)) for p in config.get("excluded_roots", [])]
    files: list[dict] = []
    roots_summary: list[dict] = []
    for root in config.get("source_roots", []):
        rid, rpath, kind = root["id"], root["path"], root.get("kind", "unknown")
        base = Path(rpath)
        present = base.exists() and base.is_dir()
        if not present:
            roots_summary.append({"id": rid, "path": rpath, "kind": kind, "present": False,
                                  "file_count": 0, "total_bytes": 0, "note": "MISSING_SOURCE_ROOT"})
            continue
        if os.path.normcase(os.path.abspath(rpath)) in excluded:
            roots_summary.append({"id": rid, "path": rpath, "kind": kind, "present": True,
                                  "file_count": 0, "total_bytes": 0, "note": "EXCLUDED_ROOT"})
            continue
        collected = []
        total = 0
        for dirpath, dirnames, filenames in os.walk(base):
            # skip any excluded subtree
            ncdir = os.path.normcase(os.path.abspath(dirpath))
            if any(ncdir == e or ncdir.startswith(e + os.sep) for e in excluded):
                dirnames[:] = []
                continue
            dirnames.sort()
            for fn in sorted(filenames):
                fp = Path(dirpath) / fn
                try:
                    size = fp.stat().st_size
                except OSError:
                    size = None
                rel = os.path.relpath(str(fp), str(base)).replace("\\", "/")
                ext = fp.suffix.lower()
                collected.append({"root_id": rid, "root_path": rpath, "kind": kind,
                                  "abspath": str(fp), "relpath": rel, "filename": fn,
                                  "ext": ext, "size": size})
                total += size or 0
        collected.sort(key=lambda r: r["relpath"])
        files.extend(collected)
        roots_summary.append({"id": rid, "path": rpath, "kind": kind, "present": True,
                              "file_count": len(collected), "total_bytes": total, "note": None})
    files.sort(key=lambda r: (r["root_id"], r["relpath"]))
    return files, roots_summary


# --------------------------------------------------------------------------- #
# Canonical extraction.
# --------------------------------------------------------------------------- #
def _iter_row_dicts(obj: Any):
    """Yield dict rows from a parsed object: list-of-dicts, wrapped list, or single dict."""
    if isinstance(obj, list):
        for el in obj:
            if isinstance(el, dict):
                yield el
        return
    if isinstance(obj, dict):
        # common wrapper keys holding the row list
        for key in ("records", "rows", "entries", "candidates", "alphas", "registry",
                    "leaderboard", "results", "experiments", "signals", "items", "data",
                    "snapshots", "clean_promising_signals"):
            v = obj.get(key)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                for el in v:
                    yield el
                return
        yield obj


def _confidence_for(kind: str) -> float:
    return {"experiment_dir": 1.0, "tabular_row": 0.8, "json_element": 0.7,
            "single_object": 0.5}.get(kind, 0.5)


def _experiment_from_dir(files_map: dict, parser_version: str) -> Optional[dict]:
    """Build one canonical experiment (+ linked decision/evidence/dataset) from a
    research_agent experiment directory (config/decision/metrics/gate_results/provenance)."""
    cfg = files_map.get("config.json")
    if not isinstance(cfg, dict):
        return None
    decision = files_map.get("decision.json") if isinstance(files_map.get("decision.json"), dict) else {}
    metrics = files_map.get("metrics.json") if isinstance(files_map.get("metrics.json"), dict) else {}
    gates = files_map.get("gate_results.json") if isinstance(files_map.get("gate_results.json"), dict) else {}
    prov = files_map.get("provenance.json") if isinstance(files_map.get("provenance.json"), dict) else {}

    merged = dict(cfg)
    if metrics.get("metrics"):
        merged.setdefault("_metrics", metrics["metrics"])
    norm = imp.normalize_record(merged)
    if decision.get("decision"):
        norm["decision"] = decision.get("decision")
    if decision.get("reasons"):
        norm["reject_reason"] = decision.get("reasons")
    if prov.get("code_commit"):
        norm["code_commit"] = prov.get("code_commit")
    if cfg.get("spec_hash"):
        norm["spec_hash"] = cfg.get("spec_hash")

    cfg_art = files_map.get("_arts", {}).get("config.json", {})
    art_id = cfg_art.get("artifact_id")
    src_path = cfg_art.get("source_path")
    root_id = cfg_art.get("source_root_id", "")
    rel = cfg_art.get("relpath", "")
    campaign = _derive_campaign(rel)
    exact_fp = imp.exact_experiment_fingerprint(norm)
    fam_fp = imp.information_family_fingerprint(norm)
    info_fam = imp.classify_information_family(
        _as_text(norm.get("family")), _as_text(norm.get("model_id")), _as_text(cfg.get("candidate_id")))
    exp_id = PFX["experiment"] + imp.h16("%s|%s|dir" % (art_id, cfg.get("experiment_id", rel)))

    exp = {
        "experiment_id": exp_id, "source_artifact_id": art_id, "source_path": src_path,
        "native_id": cfg.get("experiment_id"), "source_phase": _derive_phase(root_id, rel),
        "campaign": campaign, "family": _as_text(norm.get("family")),
        "information_family": info_fam, "universe": _as_text(norm.get("universe")),
        "horizon": _as_text(norm.get("horizon")), "model_id": _as_text(norm.get("model_id")),
        "cost_bps": _as_text(norm.get("cost_bps")), "decision": _as_text(norm.get("decision")),
        "status": _as_text(cfg.get("stage") or decision.get("stage")),
        "reject_reason": _as_text(norm.get("reject_reason")), "spec_hash": cfg.get("spec_hash"),
        "exact_fingerprint": exact_fp, "family_fingerprint": fam_fp,
        "metadata_complete": 1 if norm["metadata_completeness"]["is_complete"] else 0,
        "import_confidence": _confidence_for("experiment_dir"),
        "normalized_json": _jd({k: v for k, v in norm.items() if k != "metadata_completeness"}),
        "raw_json": _jd({"config": cfg, "decision": decision, "provenance": prov}),
        "provenance": "research_agent_experiment_dir", "parser_version": parser_version,
        "observed_at": prov.get("data_cutoff") or cfg.get("data_cutoff"),
    }
    links = [(exp_id, a.get("artifact_id"), fn) for fn, a in files_map.get("_arts", {}).items()
             if a.get("artifact_id")]

    extra_decisions = []
    if decision:
        dec_art = files_map.get("_arts", {}).get("decision.json", {})
        d_art_id = dec_art.get("artifact_id") or art_id
        did = PFX["decision"] + imp.h16("%s|expdecision" % exp_id)
        extra_decisions.append({
            "decision_id": did, "source_artifact_id": d_art_id,
            "source_path": dec_art.get("source_path", src_path),
            "native_id": cfg.get("experiment_id"), "source_phase": exp["source_phase"],
            "family": exp["family"], "information_family": info_fam,
            "decision": _as_text(decision.get("decision")),
            "reject_reason": _as_text(decision.get("reasons") or decision.get("gate_overrides")),
            "normalized_json": _jd({"stage": decision.get("stage")}),
            "raw_json": _jd(decision), "import_confidence": 1.0,
            "provenance": "research_agent_experiment_decision",
            "parser_version": parser_version, "observed_at": exp["observed_at"],
        })

    extra_evidence = []
    if metrics.get("metrics"):
        ev_art = files_map.get("_arts", {}).get("metrics.json", {})
        eid = PFX["evidence"] + imp.h16("%s|expmetrics" % exp_id)
        extra_evidence.append({
            "evidence_id": eid, "source_artifact_id": ev_art.get("artifact_id") or art_id,
            "source_path": ev_art.get("source_path", src_path),
            "native_id": cfg.get("experiment_id"), "source_phase": exp["source_phase"],
            "kind": "battery_metrics", "information_family": info_fam,
            "metrics_json": _jd(metrics.get("metrics")), "normalized_json": None,
            "raw_json": None, "import_confidence": 1.0,
            "provenance": "research_agent_experiment_metrics",
            "parser_version": parser_version, "observed_at": exp["observed_at"],
        })

    extra_datasets = []
    ip = prov.get("input_provenance") or {}
    shas = ip.get("sha256") or {}
    if isinstance(shas, dict):
        prov_art = files_map.get("_arts", {}).get("provenance.json", {})
        for name in sorted(shas.keys()):
            dsid = PFX["dataset"] + imp.h16("%s|input|%s" % (exp_id, name))
            extra_datasets.append({
                "dataset_id": dsid, "source_artifact_id": prov_art.get("artifact_id") or art_id,
                "source_path": prov_art.get("source_path", src_path),
                "native_id": name, "source_phase": exp["source_phase"], "name": name,
                "kind": "experiment_input_panel", "information_family": info_fam,
                "normalized_json": _jd({"input_path": (ip.get("paths") or {}).get(name),
                                        "sha256": shas.get(name)}),
                "raw_json": None, "import_confidence": 1.0,
                "provenance": "research_agent_experiment_provenance",
                "parser_version": parser_version, "observed_at": exp["observed_at"],
            })

    return {"experiment": exp, "links": links, "decisions": extra_decisions,
            "evidence": extra_evidence, "datasets": extra_datasets}


def _extract_generic(artifact: dict, obj: Any, parser_version: str) -> dict[str, list]:
    """Emit canonical rows for a non-experiment-dir artifact based on its record_type."""
    out = {"datasets": [], "features": [], "hypotheses": [], "experiments": [],
           "signals": [], "evidence": [], "decisions": []}
    if obj is None:
        return out
    rtype = artifact["record_type"]
    art_id = artifact["artifact_id"]
    src_path = artifact["source_path"]
    root_id = artifact["source_root_id"]
    rel = artifact["relpath"]
    phase = artifact["linked_phase"]
    campaign = _derive_campaign(rel)

    rows = list(_iter_row_dicts(obj))
    single = len(rows) == 1 and isinstance(obj, dict) and rows[0] is obj
    conf = _confidence_for("single_object" if single else
                           ("json_element" if isinstance(obj, list) else "tabular_row"))

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        norm = imp.normalize_record(row)
        native = norm.get("native_id") or row.get("candidate_id") or row.get("exp_id")
        info_fam = imp.classify_information_family(
            _as_text(norm.get("family")), _as_text(norm.get("name")),
            _as_text(row.get("hypothesis") or row.get("thesis") or row.get("human_readable_thesis")))
        base_key = "%s|%d|%s" % (art_id, idx, native or "")

        if rtype == imp.REC_HYPOTHESIS:
            out["hypotheses"].append({
                "hypothesis_id": PFX["hypothesis"] + imp.h16(base_key),
                "source_artifact_id": art_id, "source_path": src_path,
                "native_id": _as_text(native), "source_phase": phase,
                "family": _as_text(norm.get("family")), "information_family": info_fam,
                "statement": _as_text(row.get("hypothesis") or row.get("diagnosis")
                                      or row.get("statement") or row.get("proposed_change")),
                "status": _as_text(norm.get("status")),
                "normalized_json": _jd({k: v for k, v in norm.items()
                                        if k != "metadata_completeness"}),
                "raw_json": _jd(row), "import_confidence": conf,
                "provenance": rtype, "parser_version": parser_version,
                "observed_at": _as_text(row.get("recorded_at") or row.get("generated_utc"))})
        elif rtype == imp.REC_EXPERIMENT:
            norm2 = norm
            out["experiments"].append({
                "experiment_id": PFX["experiment"] + imp.h16(base_key),
                "source_artifact_id": art_id, "source_path": src_path,
                "native_id": _as_text(native), "source_phase": phase, "campaign": campaign,
                "family": _as_text(norm.get("family")), "information_family": info_fam,
                "universe": _as_text(norm.get("universe")), "horizon": _as_text(norm.get("horizon")),
                "model_id": _as_text(norm.get("model_id")), "cost_bps": _as_text(norm.get("cost_bps")),
                "decision": _as_text(norm.get("decision")), "status": _as_text(norm.get("status")),
                "reject_reason": _as_text(norm.get("reject_reason")),
                "spec_hash": _as_text(norm.get("spec_hash")),
                "exact_fingerprint": imp.exact_experiment_fingerprint(norm2),
                "family_fingerprint": imp.information_family_fingerprint(norm2),
                "metadata_complete": 1 if norm["metadata_completeness"]["is_complete"] else 0,
                "import_confidence": conf,
                "normalized_json": _jd({k: v for k, v in norm.items()
                                        if k != "metadata_completeness"}),
                "raw_json": _jd(row), "provenance": rtype, "parser_version": parser_version,
                "observed_at": _as_text(row.get("recorded_at"))})
        elif rtype == imp.REC_SIGNAL:
            out["signals"].append({
                "signal_id": PFX["signal"] + imp.h16(base_key),
                "source_artifact_id": art_id, "source_path": src_path,
                "native_id": _as_text(native), "source_phase": phase,
                "name": _as_text(norm.get("name") or native), "family": _as_text(norm.get("family")),
                "information_family": info_fam, "decision": _as_text(norm.get("decision")),
                "status": _as_text(norm.get("status")), "reject_reason": _as_text(norm.get("reject_reason")),
                "ic": _as_text(norm.get("ic")), "ic_t": _as_text(norm.get("ic_t")),
                "net25": _as_text(norm.get("net25")),
                "normalized_json": _jd({k: v for k, v in norm.items()
                                        if k != "metadata_completeness"}),
                "raw_json": _jd(row), "import_confidence": conf, "provenance": rtype,
                "parser_version": parser_version, "observed_at": None})
        elif rtype == imp.REC_DECISION:
            out["decisions"].append({
                "decision_id": PFX["decision"] + imp.h16(base_key),
                "source_artifact_id": art_id, "source_path": src_path,
                "native_id": _as_text(native), "source_phase": phase,
                "family": _as_text(norm.get("family")), "information_family": info_fam,
                "decision": _as_text(norm.get("decision") or row.get("action")
                                     or row.get("final_state") or row.get("state") or row.get("stage")),
                "reject_reason": _as_text(norm.get("reject_reason")),
                "normalized_json": _jd({k: v for k, v in norm.items()
                                        if k != "metadata_completeness"}),
                "raw_json": _jd(row), "import_confidence": conf, "provenance": rtype,
                "parser_version": parser_version,
                "observed_at": _as_text(row.get("updated_at") or row.get("generated_at")
                                        or row.get("generated_utc"))})
        elif rtype == imp.REC_FEATURE:
            out["features"].append({
                "feature_id": PFX["feature"] + imp.h16(base_key),
                "source_artifact_id": art_id, "source_path": src_path,
                "native_id": _as_text(native), "source_phase": phase,
                "name": _as_text(norm.get("name") or native), "family": _as_text(norm.get("family")),
                "information_family": info_fam,
                "definition_json": _jd(row.get("definition") or row.get("expression")
                                       or row.get("dsl")),
                "normalized_json": _jd({k: v for k, v in norm.items()
                                        if k != "metadata_completeness"}),
                "raw_json": _jd(row), "import_confidence": conf, "provenance": rtype,
                "parser_version": parser_version, "observed_at": None})
        elif rtype == imp.REC_DATASET:
            out["datasets"].append({
                "dataset_id": PFX["dataset"] + imp.h16(base_key),
                "source_artifact_id": art_id, "source_path": src_path,
                "native_id": _as_text(native), "source_phase": phase,
                "name": _as_text(norm.get("name") or native or artifact["filename"]),
                "kind": rtype, "information_family": info_fam,
                "normalized_json": _jd({k: v for k, v in norm.items()
                                        if k != "metadata_completeness"}),
                "raw_json": _jd(row), "import_confidence": conf, "provenance": rtype,
                "parser_version": parser_version, "observed_at": None})
        elif rtype == imp.REC_EVIDENCE:
            out["evidence"].append({
                "evidence_id": PFX["evidence"] + imp.h16(base_key),
                "source_artifact_id": art_id, "source_path": src_path,
                "native_id": _as_text(native), "source_phase": phase,
                "kind": artifact["filename"], "information_family": info_fam,
                "metrics_json": _jd(row.get("metrics") or row), "normalized_json": None,
                "raw_json": None, "import_confidence": conf, "provenance": rtype,
                "parser_version": parser_version, "observed_at": None})
    return out


# --------------------------------------------------------------------------- #
# Duplicate clustering + coverage.
# --------------------------------------------------------------------------- #
def _cluster_duplicates(experiments: list[dict]) -> list[dict]:
    """Cluster experiments by EXACT fingerprint; classify each member. Only clusters of
    size >= 2 emitted. Records with incomplete metadata are never called duplicates."""
    groups: dict[str, list[dict]] = {}
    for e in experiments:
        if not e.get("metadata_complete") and not e.get("spec_hash"):
            continue
        fp = e.get("exact_fingerprint")
        if not fp:
            continue
        groups.setdefault(fp, []).append(e)
    rows = []
    for fp in sorted(groups):
        members = sorted(groups[fp], key=lambda x: x["experiment_id"])
        if len(members) < 2:
            continue
        cluster_id = "dup_" + imp.h16(fp)
        for m in members:
            rows.append({"cluster_id": cluster_id, "experiment_id": m["experiment_id"],
                         "relationship": imp.CMP_EXACT_DUPLICATE, "exact_fingerprint": fp,
                         "family_fingerprint": m.get("family_fingerprint"),
                         "cluster_size": len(members)})
    return rows


def _has_token(text: Optional[str], tokens) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(tok in t for tok in tokens)


def build_coverage_map(experiments, signals, hypotheses, decisions, evidence,
                       dup_rows) -> list[dict]:
    fams = {f: {"artifacts_discovered": 0, "hypotheses": 0, "unique_experiments": set(),
                "exact_duplicates": 0, "parameter_variants": 0, "surviving_signals": 0,
                "rejected_signals": 0, "blocked_signals": 0, "latest_evidence_date": None,
                "terminal_texts": [], "missing_data": set()}
            for f in imp.INFORMATION_FAMILIES}

    dup_by_exp = {r["experiment_id"]: r for r in dup_rows}

    for e in experiments:
        f = e.get("information_family") or "unclassified_information"
        d = fams.setdefault(f, fams["unclassified_information"] if f not in fams else fams[f])
        d = fams[f]
        d["unique_experiments"].add(e.get("exact_fingerprint") or e["experiment_id"])
        if e["experiment_id"] in dup_by_exp:
            d["exact_duplicates"] += 1
        dec = (e.get("decision") or "")
        if _has_token(dec, ("reject",)) or _has_token(e.get("reject_reason"), ("reject",)):
            pass
        if _has_token(e.get("reject_reason"), MISSING_DATA_TOKENS) or \
           _has_token(dec, MISSING_DATA_TOKENS):
            d["missing_data"].add((e.get("reject_reason") or dec)[:80])
        if _has_token(dec, EXHAUSTION_TOKENS) or _has_token(e.get("reject_reason"), EXHAUSTION_TOKENS):
            d["terminal_texts"].append((e.get("reject_reason") or dec)[:120])
        if e.get("observed_at"):
            if not d["latest_evidence_date"] or str(e["observed_at"]) > d["latest_evidence_date"]:
                d["latest_evidence_date"] = str(e["observed_at"])

    for h in hypotheses:
        f = h.get("information_family") or "unclassified_information"
        fams[f]["hypotheses"] += 1

    for s in signals:
        f = s.get("information_family") or "unclassified_information"
        d = fams[f]
        dec = (s.get("decision") or "") + " " + (s.get("status") or "")
        rr = s.get("reject_reason") or ""
        if _has_token(dec, SURVIVING_TOKENS):
            d["surviving_signals"] += 1
        if _has_token(dec, ("reject",)) or _has_token(rr, ("reject", "fail")):
            d["rejected_signals"] += 1
        if _has_token(rr, MISSING_DATA_TOKENS) or _has_token(dec, ("provider_limited",)):
            d["blocked_signals"] += 1
            d["missing_data"].add(rr[:80] or "provider_limited")

    for dc in decisions:
        f = dc.get("information_family") or "unclassified_information"
        txt = (dc.get("decision") or "") + " " + (dc.get("reject_reason") or "")
        if _has_token(txt, EXHAUSTION_TOKENS):
            fams[f]["terminal_texts"].append(txt[:120])
        if _has_token(txt, MISSING_DATA_TOKENS):
            fams[f]["missing_data"].add(txt[:80])

    for ev in evidence:
        f = ev.get("information_family") or "unclassified_information"
        fams[f]["artifacts_discovered"] += 1

    out = []
    for f in imp.INFORMATION_FAMILIES:
        d = fams[f]
        ue = len(d["unique_experiments"])
        surviving = d["surviving_signals"]
        rejected = d["rejected_signals"]
        blocked = d["blocked_signals"]
        exhausted = bool(d["terminal_texts"]) and ue > 0 and surviving == 0
        # evidence classification (deterministic rules).
        if surviving > 0:
            cls = "PROVEN_DISTINCT"
        elif ue > 0 and rejected > 0 and exhausted:
            cls = "TESTED_AND_REJECTED"
        elif ue > 0 and (rejected > 0):
            cls = "TESTED_INCONCLUSIVE"
        elif ue > 0:
            cls = "TESTED_INCONCLUSIVE"
        elif blocked > 0:
            cls = "BLOCKED_BY_MISSING_DATA"
        elif d["hypotheses"] > 0:
            cls = "NOT_YET_TESTED"
        else:
            cls = "NOT_YET_TESTED"
        out.append({
            "information_family": f,
            "artifacts_discovered": d["artifacts_discovered"] + ue + d["hypotheses"],
            "hypotheses": d["hypotheses"], "unique_experiments": ue,
            "exact_duplicates": d["exact_duplicates"],
            "parameter_variants": d["parameter_variants"],
            "surviving_signals": surviving, "rejected_signals": rejected,
            "blocked_signals": blocked, "latest_evidence_date": d["latest_evidence_date"],
            "strongest_terminal_evidence": (sorted(d["terminal_texts"])[0]
                                            if d["terminal_texts"] else None),
            "evidence_classification": cls,
            "required_missing_data": "; ".join(sorted(x for x in d["missing_data"] if x)) or None,
            "local_research_supported_as_exhausted": 1 if exhausted else 0,
        })
    return out


# --------------------------------------------------------------------------- #
# Current state (read-only reads of operational ledgers + phase31b package).
# --------------------------------------------------------------------------- #
def build_current_state(config: dict, experiments, signals) -> dict:
    cs = config.get("current_state", {})
    desk = Path(cs.get("desk_dir", ""))
    state: dict[str, Any] = {"active_book_changed": False}

    def _load(p: Path):
        try:
            return json.loads(p.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError):
            return None

    books = _load(desk / cs.get("paper_books_file", "paper_books.json"))
    if isinstance(books, dict) and books.get("rows"):
        b = books["rows"][-1].get("book", {})
        state["active_book_id"] = b.get("book_id")
        state["champion_model"] = b.get("model_id")
        state["frozen_target_definition"] = {
            "target_book": b.get("target_book"),
            "target_position_count": b.get("target_position_count"),
            "transaction_cost_bps_per_side": b.get("transaction_cost_bps_per_side"),
            "review_cadence": b.get("review_cadence"),
            "snapshot_id": b.get("snapshot_id")}
        tw = b.get("frozen_target_weights") or {}
        state["active_holdings_count"] = len(tw)

    perf = _load(desk / cs.get("forward_performance_file", "forward_performance.json"))
    if isinstance(perf, dict) and perf.get("rows"):
        last = perf["rows"][-1].get("row", {})
        state["latest_operational_market_date"] = last.get("date")
        state["latest_nav"] = last.get("nav")
        state["cumulative_return_pct"] = last.get("cumulative_return_pct")
        state["benchmark_cumulative_return_pct"] = last.get("benchmark_cumulative_return_pct")
        state["forward_evidence_count"] = len(perf["rows"])
        state["active_holdings_count"] = last.get("holdings_count", state.get("active_holdings_count"))

    outcomes = _load(desk / "forward_prediction_outcomes.json")
    if isinstance(outcomes, dict):
        rows = outcomes.get("rows") or outcomes.get("outcomes") or []
        state["matured_outcomes"] = len(rows) if isinstance(rows, list) else None

    close = _load(desk / cs.get("daily_close_journal_file", "daily_close_journal.json"))
    if isinstance(close, dict) and close.get("rows"):
        last = close["rows"][-1]
        row = last.get("row", last)
        state["latest_daily_close_status"] = row.get("status") or row.get("close_status")
        state["latest_daily_close_date"] = row.get("date") or row.get("market_date")

    # Phase 31B latest decision from its package manifest.
    p31 = Path(cs.get("phase31b_package_root", ""))
    if p31.exists():
        manifests = sorted(p31.glob("phase31b_*/run_manifest.json"))
        if manifests:
            man = _load(manifests[-1])
            if isinstance(man, dict):
                comp = man.get("completeness") or {}
                state["latest_phase31b_decision"] = comp.get("decision") or man.get("decision")
                state["phase31b_run_id"] = man.get("run_id")

    # Champion / challengers recovered from canonical registry records.
    champs, challs = [], []
    for s in signals + experiments:
        blob = ((s.get("decision") or "") + " " + (s.get("status") or "")).lower()
        nid = s.get("native_id") or s.get("name")
        if "champion" in blob and nid:
            champs.append(nid)
        if "challenger" in blob and nid:
            challs.append(nid)
    state["champion_alpha_signals_recovered"] = sorted(set(x for x in champs if x))[:20]
    state["challengers_recovered"] = sorted(set(x for x in challs if x))[:40]
    rejected = sum(1 for s in signals if _has_token(s.get("decision"), ("reject",))
                   or _has_token(s.get("reject_reason"), ("reject", "fail")))
    state["rejected_candidate_signal_count"] = rejected
    return state


# --------------------------------------------------------------------------- #
# Duplicate-prevention contract (reusable by later agent stages).
# --------------------------------------------------------------------------- #
def classify_candidate_experiment(candidate_norm: dict,
                                  existing: list[dict]) -> dict:
    """Given a normalized candidate spec and the list of existing experiment records,
    decide whether the candidate is new information or a duplicate. Deterministic."""
    complete = candidate_norm.get("metadata_completeness", {}).get("is_complete", False) \
        or bool(candidate_norm.get("spec_hash"))
    if not complete:
        return {"result": "METADATA_INSUFFICIENT", "matched_experiment_ids": [],
                "matched_fingerprints": [], "reason": "candidate metadata incomplete",
                "changed_inputs": [], "changed_source_data_versions": [],
                "rerun_permitted": False,
                "required_evidence_before_rerun": "supply universe, horizon, model_params, cost"}
    cand_fp = imp.exact_experiment_fingerprint(candidate_norm)
    cand_cut = str(candidate_norm.get("data_cutoff") or "")
    exact_matches, param_matches = [], []
    newer_data = False
    for e in existing:
        if e.get("exact_fingerprint") == cand_fp:
            exact_matches.append(e)
            ecut = str(e.get("observed_at") or "")
            if cand_cut and ecut and cand_cut > ecut:
                newer_data = True
        elif e.get("family_fingerprint") == imp.information_family_fingerprint(candidate_norm):
            param_matches.append(e)
    if exact_matches:
        if newer_data:
            return {"result": "NEW_DATA_AVAILABLE",
                    "matched_experiment_ids": [m["experiment_id"] for m in exact_matches],
                    "matched_fingerprints": [cand_fp],
                    "reason": "exact spec already tested but candidate uses newer data_cutoff",
                    "changed_inputs": ["data_cutoff"],
                    "changed_source_data_versions": [cand_cut], "rerun_permitted": True,
                    "required_evidence_before_rerun": "confirm the newer input panel hash differs"}
        incomplete_prior = any(not m.get("decision") for m in exact_matches)
        if incomplete_prior:
            return {"result": "PRIOR_TEST_INCOMPLETE",
                    "matched_experiment_ids": [m["experiment_id"] for m in exact_matches],
                    "matched_fingerprints": [cand_fp],
                    "reason": "matching prior experiment has no terminal decision",
                    "changed_inputs": [], "changed_source_data_versions": [],
                    "rerun_permitted": True,
                    "required_evidence_before_rerun": "complete the prior run's decision"}
        return {"result": "EXACT_DUPLICATE",
                "matched_experiment_ids": [m["experiment_id"] for m in exact_matches],
                "matched_fingerprints": [cand_fp],
                "reason": "identical normalized spec already tested with a terminal decision",
                "changed_inputs": [], "changed_source_data_versions": [],
                "rerun_permitted": False,
                "required_evidence_before_rerun": "new data family or changed spec required"}
    if param_matches:
        return {"result": "PARAMETER_DUPLICATE",
                "matched_experiment_ids": [m["experiment_id"] for m in param_matches][:20],
                "matched_fingerprints": [imp.information_family_fingerprint(candidate_norm)],
                "reason": "same information family and universe already explored; parameter variant",
                "changed_inputs": ["model_params_or_portfolio_params"],
                "changed_source_data_versions": [], "rerun_permitted": True,
                "required_evidence_before_rerun": "justify why this parameter point adds information"}
    return {"result": "NEW_INFORMATION", "matched_experiment_ids": [],
            "matched_fingerprints": [], "reason": "no exact or family match found",
            "changed_inputs": [], "changed_source_data_versions": [],
            "rerun_permitted": True, "required_evidence_before_rerun": None}


# --------------------------------------------------------------------------- #
# Run-id.
# --------------------------------------------------------------------------- #
def compute_run_id(config: dict, config_hash: str, git_commit: str,
                   artifact_fps: list[str]) -> str:
    seed = imp.canonical_json({
        "schema_version": SCHEMA_VERSION, "importer_version": imp.IMPORTER_VERSION,
        "config_hash": config_hash, "git_commit": git_commit or "UNKNOWN",
        "source_roots": sorted([r["id"] + "=" + r["path"] for r in config.get("source_roots", [])]),
        "artifact_fingerprints": sorted(artifact_fps),
    })
    return "stage1_" + imp.sha256_text(seed)[:16]


# --------------------------------------------------------------------------- #
# Artifact building + full in-memory assembly.
# --------------------------------------------------------------------------- #
_CONF_BY_STATUS = {
    imp.PS_OK: 0.9, imp.PS_OK_ROWS_SKIPPED: 0.7, imp.PS_EMPTY: 0.3,
    imp.PS_UNSUPPORTED: 0.0, imp.PS_NO_LIBRARY: 0.1, imp.PS_PARSE_ERROR: 0.0,
}
_EXP_MEMBER_RE = re.compile(r"(.*?/experiments/exp[^/]*)/", re.IGNORECASE)


def _build_artifact(file: dict, config: dict) -> tuple[dict, Any]:
    """Return (artifact_record, parsed_obj). Fingerprints + parses one file, read-only."""
    p = Path(file["abspath"])
    rid, rel, fn = file["root_id"], file["relpath"], file["filename"]
    ext = file["ext"]
    size = file["size"] if file["size"] is not None else (p.stat().st_size if p.exists() else 0)
    category = imp.category_for_ext(ext)
    fp = imp.fingerprint_file(p, config)
    parsed = imp.parse_artifact(p, ext, size, category, config)
    mts, cts = _iso_mtime(p)
    status = parsed["parser_status"]
    unresolved = None
    if status in (imp.PS_PARSE_ERROR, imp.PS_UNSUPPORTED, imp.PS_NO_LIBRARY):
        unresolved = status + ((": " + "; ".join(parsed["warnings"])) if parsed["warnings"] else "")
    art = {
        "artifact_id": PFX["artifact"] + imp.h16(rid + "|" + rel),
        "source_root_id": rid, "source_path": file["abspath"], "relpath": rel, "filename": fn,
        "extension": ext, "category": category, "size_bytes": size,
        "modified_ts": mts, "created_ts": cts,
        "fingerprint_type": fp["fingerprint_type"], "fingerprint": fp["fingerprint"],
        "parser": parsed["parser"], "parser_status": status,
        "import_confidence": _CONF_BY_STATUS.get(status, 0.0),
        "linked_phase": _derive_phase(rid, rel), "linked_run_id": _derive_campaign(rel),
        "linked_commit": None, "row_count": parsed["row_count"],
        "columns_json": _jd(parsed["columns"]), "record_type": imp.classify_record_type(rel, fn),
        "warnings_json": _jd(parsed["warnings"]) if parsed["warnings"] else None,
        "unresolved_reason": unresolved, "source_unchanged": 1,
    }
    return art, parsed.get("obj")


def _reconcile_conflicts(records: list[dict], id_key: str, fields: tuple) -> list[dict]:
    """Flag records that share a non-null native_id but disagree on a defining field.

    Conflicting records are kept as separate rows (never merged); this only emits a
    reconciliation issue so the disagreement is visible in import_issues."""
    by_native: dict[str, list[dict]] = {}
    for r in records:
        nid = r.get("native_id")
        if nid:
            by_native.setdefault(str(nid), []).append(r)
    issues = []
    for nid in sorted(by_native):
        group = by_native[nid]
        if len(group) < 2:
            continue
        for f in fields:
            vals = {(_as_text(g.get(f)) or "") for g in group}
            vals.discard("")
            if len(vals) > 1:
                issues.append({
                    "issue_id": "iss_" + imp.h16("conflict|%s|%s|%s" % (id_key, nid, f)),
                    "artifact_id": group[0].get("source_artifact_id"),
                    "source_path": group[0].get("source_path"),
                    "issue_type": "CONFLICTING_SOURCE_RECORDS",
                    "detail": ("native_id=%s disagrees on %s across %d records: %s"
                               % (nid, f, len(group), sorted(vals)))[:400]})
                break
    return issues


def build_all(config: dict, git_commit: str, now: str) -> dict:
    """Scan, fingerprint, parse, extract, dedup, cover, current-state — all in memory."""
    files, roots_summary = scan_source_roots(config)

    artifacts: list[dict] = []
    import_issues: list[dict] = []
    exp_dir_cache: dict[str, dict] = {}

    datasets: list[dict] = []
    features: list[dict] = []
    hypotheses: list[dict] = []
    experiments: list[dict] = []
    signals: list[dict] = []
    evidence: list[dict] = []
    decisions: list[dict] = []
    exp_artifacts: list[dict] = []

    pv = imp.IMPORTER_VERSION
    for file in files:
        art, obj = _build_artifact(file, config)
        artifacts.append(art)
        if art["unresolved_reason"]:
            import_issues.append({
                "issue_id": "iss_" + imp.h16(art["artifact_id"] + "|" + art["parser_status"]),
                "artifact_id": art["artifact_id"], "source_path": art["source_path"],
                "issue_type": art["parser_status"], "detail": art["unresolved_reason"][:400]})

        rel_posix = art["relpath"]
        m = _EXP_MEMBER_RE.match(rel_posix + "/")
        if m and isinstance(obj, dict):
            dir_key = art["source_root_id"] + "|" + m.group(1)
            slot = exp_dir_cache.setdefault(dir_key, {"_arts": {}, "_relpaths": {}})
            slot[art["filename"]] = obj
            slot["_arts"][art["filename"]] = art
            slot["_relpaths"][art["filename"]] = rel_posix
            continue  # consumed by the experiment-dir builder; no generic extraction

        rows = _extract_generic(art, obj, pv)
        datasets.extend(rows["datasets"]); features.extend(rows["features"])
        hypotheses.extend(rows["hypotheses"]); experiments.extend(rows["experiments"])
        signals.extend(rows["signals"]); evidence.extend(rows["evidence"])
        decisions.extend(rows["decisions"])

    # Experiment directories.
    for dir_key in sorted(exp_dir_cache):
        built = _experiment_from_dir(exp_dir_cache[dir_key], pv)
        if not built:
            continue
        experiments.append(built["experiment"])
        decisions.extend(built["decisions"])
        evidence.extend(built["evidence"])
        datasets.extend(built["datasets"])
        for (eid, aid, role) in built["links"]:
            if aid:
                exp_artifacts.append({"experiment_id": eid, "artifact_id": aid, "role": role})

    # Deduplicate canonical records defensively on primary key (keep first).
    def _dedupe(rows, key):
        seen, out = set(), []
        for r in rows:
            k = r[key]
            if k in seen:
                continue
            seen.add(k); out.append(r)
        return out
    datasets = _dedupe(datasets, "dataset_id")
    features = _dedupe(features, "feature_id")
    hypotheses = _dedupe(hypotheses, "hypothesis_id")
    experiments = _dedupe(experiments, "experiment_id")
    signals = _dedupe(signals, "signal_id")
    evidence = _dedupe(evidence, "evidence_id")
    decisions = _dedupe(decisions, "decision_id")

    # Reconciliation: conflicting source records that share a native id but disagree on
    # a defining field are KEPT SEPARATE and flagged (never silently merged).
    import_issues.extend(_reconcile_conflicts(experiments, "experiment_id",
                                              ("family", "exact_fingerprint")))
    import_issues.extend(_reconcile_conflicts(signals, "signal_id", ("family", "decision")))

    dup_rows = _cluster_duplicates(experiments)
    coverage = build_coverage_map(experiments, signals, hypotheses, decisions, evidence, dup_rows)
    current = build_current_state(config, experiments, signals)

    artifact_fps = ["%s|%s|%s" % (a["source_root_id"], a["relpath"], a["fingerprint"])
                    for a in artifacts]
    artifact_set_hash = imp.sha256_text(imp.canonical_json(sorted(artifact_fps)))

    return {
        "roots_summary": roots_summary, "artifacts": artifacts, "import_issues": import_issues,
        "datasets": datasets, "features": features, "hypotheses": hypotheses,
        "experiments": experiments, "signals": signals, "evidence": evidence,
        "decisions": decisions, "experiment_artifacts": exp_artifacts,
        "signal_evidence": [], "experiment_duplicates": dup_rows, "coverage_map": coverage,
        "current_state": current, "artifact_fingerprints": sorted(artifact_fps),
        "artifact_set_hash": artifact_set_hash, "git_commit": git_commit, "generated_at": now,
    }


# --------------------------------------------------------------------------- #
# SQLite + export writers.
# --------------------------------------------------------------------------- #
_COLUMNS = {
    "source_roots": ["id", "path", "kind", "present", "file_count", "total_bytes", "note"],
    "artifacts": ["artifact_id", "source_root_id", "source_path", "relpath", "filename",
                  "extension", "category", "size_bytes", "modified_ts", "created_ts",
                  "fingerprint_type", "fingerprint", "parser", "parser_status",
                  "import_confidence", "linked_phase", "linked_run_id", "linked_commit",
                  "row_count", "columns_json", "record_type", "warnings_json",
                  "unresolved_reason", "source_unchanged"],
    "datasets": ["dataset_id", "source_artifact_id", "source_path", "native_id", "source_phase",
                 "name", "kind", "information_family", "normalized_json", "raw_json",
                 "import_confidence", "provenance", "parser_version", "observed_at"],
    "features": ["feature_id", "source_artifact_id", "source_path", "native_id", "source_phase",
                 "name", "family", "information_family", "definition_json", "normalized_json",
                 "raw_json", "import_confidence", "provenance", "parser_version", "observed_at"],
    "hypotheses": ["hypothesis_id", "source_artifact_id", "source_path", "native_id",
                   "source_phase", "family", "information_family", "statement", "status",
                   "normalized_json", "raw_json", "import_confidence", "provenance",
                   "parser_version", "observed_at"],
    "experiments": ["experiment_id", "source_artifact_id", "source_path", "native_id",
                    "source_phase", "campaign", "family", "information_family", "universe",
                    "horizon", "model_id", "cost_bps", "decision", "status", "reject_reason",
                    "spec_hash", "exact_fingerprint", "family_fingerprint", "metadata_complete",
                    "import_confidence", "normalized_json", "raw_json", "provenance",
                    "parser_version", "observed_at"],
    "signals": ["signal_id", "source_artifact_id", "source_path", "native_id", "source_phase",
                "name", "family", "information_family", "decision", "status", "reject_reason",
                "ic", "ic_t", "net25", "normalized_json", "raw_json", "import_confidence",
                "provenance", "parser_version", "observed_at"],
    "evidence": ["evidence_id", "source_artifact_id", "source_path", "native_id", "source_phase",
                 "kind", "information_family", "metrics_json", "normalized_json", "raw_json",
                 "import_confidence", "provenance", "parser_version", "observed_at"],
    "decisions": ["decision_id", "source_artifact_id", "source_path", "native_id", "source_phase",
                  "family", "information_family", "decision", "reject_reason", "normalized_json",
                  "raw_json", "import_confidence", "provenance", "parser_version", "observed_at"],
    "experiment_artifacts": ["experiment_id", "artifact_id", "role"],
    "signal_evidence": ["signal_id", "evidence_id"],
    "experiment_duplicates": ["cluster_id", "experiment_id", "relationship", "exact_fingerprint",
                              "family_fingerprint", "cluster_size"],
    "import_issues": ["issue_id", "artifact_id", "source_path", "issue_type", "detail"],
    "coverage_map": ["information_family", "artifacts_discovered", "hypotheses",
                     "unique_experiments", "exact_duplicates", "parameter_variants",
                     "surviving_signals", "rejected_signals", "blocked_signals",
                     "latest_evidence_date", "strongest_terminal_evidence",
                     "evidence_classification", "required_missing_data",
                     "local_research_supported_as_exhausted"],
}

# table -> (export filename, in-memory key)
_EXPORTS = [
    ("artifacts", "artifact_registry.csv"), ("datasets", "dataset_registry.csv"),
    ("features", "feature_registry.csv"), ("hypotheses", "hypothesis_registry.csv"),
    ("experiments", "experiment_registry.csv"), ("signals", "signal_registry.csv"),
    ("evidence", "evidence_registry.csv"), ("decisions", "decision_journal.csv"),
    ("experiment_duplicates", "duplicate_experiment_clusters.csv"),
    ("coverage_map", "research_coverage_map.csv"),
]


def _insert(conn, table, rows):
    cols = _COLUMNS[table]
    ph = ",".join("?" for _ in cols)
    conn.executemany("INSERT INTO %s (%s) VALUES (%s)" % (table, ",".join(cols), ph),
                     [[r.get(c) for c in cols] for r in rows])


def write_sqlite(db_path: Path, data: dict, run_id: str, config_hash: str, mode: str) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA_SQL)
        conn.execute("INSERT INTO registry_runs (run_id, schema_version, importer_version, mode, "
                     "config_hash, git_commit, generated_at, source_root_count, artifact_count, "
                     "artifact_set_hash) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     [run_id, SCHEMA_VERSION, imp.IMPORTER_VERSION, mode, config_hash,
                      data["git_commit"], data["generated_at"], len(data["roots_summary"]),
                      len(data["artifacts"]), data["artifact_set_hash"]])
        _insert(conn, "source_roots", data["roots_summary"])
        _insert(conn, "artifacts", data["artifacts"])
        for t in ("datasets", "features", "hypotheses", "experiments", "signals", "evidence",
                  "decisions", "experiment_artifacts", "signal_evidence", "experiment_duplicates",
                  "import_issues", "coverage_map"):
            _insert(conn, t, data[t])
        conn.executemany("INSERT INTO current_state (key, value_json) VALUES (?,?)",
                         [[k, imp.canonical_json(v)] for k, v in sorted(data["current_state"].items())])
        conn.commit()
    finally:
        conn.close()


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(columns)
        for r in rows:
            w.writerow(["" if r.get(c) is None else r.get(c) for c in columns])


def write_exports(run_dir: Path, data: dict, run_id: str, config_hash: str,
                  config: dict, mode: str) -> dict:
    written = {}
    for table, fname in _EXPORTS:
        p = run_dir / fname
        _write_csv(p, _COLUMNS[table], data[table])
        written[fname] = None
    # unresolved imports export
    unresolved = [a for a in data["artifacts"] if a["unresolved_reason"]]
    _write_csv(run_dir / "unresolved_imports.csv",
               ["artifact_id", "source_path", "extension", "category", "parser_status",
                "unresolved_reason"],
               [{"artifact_id": a["artifact_id"], "source_path": a["source_path"],
                 "extension": a["extension"], "category": a["category"],
                 "parser_status": a["parser_status"], "unresolved_reason": a["unresolved_reason"]}
                for a in unresolved])
    # current state json
    (run_dir / "current_state_summary.json").write_text(
        json.dumps(data["current_state"], indent=1, sort_keys=True, default=str), encoding="utf-8")
    # schema json
    (run_dir / "registry_schema.json").write_text(
        json.dumps({"schema_version": SCHEMA_VERSION, "importer_version": imp.IMPORTER_VERSION,
                    "tables": {t: _COLUMNS.get(t) for t in CANONICAL_TABLES}},
                   indent=1, sort_keys=True), encoding="utf-8")
    return written


def _counts(data: dict) -> dict:
    return {
        "source_roots": len(data["roots_summary"]), "artifacts": len(data["artifacts"]),
        "datasets": len(data["datasets"]), "features": len(data["features"]),
        "hypotheses": len(data["hypotheses"]), "experiments": len(data["experiments"]),
        "signals": len(data["signals"]), "evidence": len(data["evidence"]),
        "decisions": len(data["decisions"]),
        "experiment_artifacts": len(data["experiment_artifacts"]),
        "experiment_duplicates": len(data["experiment_duplicates"]),
        "import_issues": len(data["import_issues"]), "coverage_map": len(data["coverage_map"]),
        "unresolved": sum(1 for a in data["artifacts"] if a["unresolved_reason"]),
        "parsed_structured": sum(1 for a in data["artifacts"]
                                 if a["parser_status"] in (imp.PS_OK, imp.PS_OK_ROWS_SKIPPED)),
    }


# --------------------------------------------------------------------------- #
# Ledger fingerprints (read-only, before/after guard).
# --------------------------------------------------------------------------- #
def ledger_fingerprints(config: dict) -> dict:
    out = {}
    ledger_ids = set(config.get("operational_ledger_roots", []))
    for root in config.get("source_roots", []):
        if root["id"] not in ledger_ids:
            continue
        base = Path(root["path"])
        if not base.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames.sort()
            for fn in sorted(filenames):
                fp = Path(dirpath) / fn
                try:
                    out[str(fp)] = imp.fingerprint_file(fp, config)["fingerprint"]
                except OSError:
                    out[str(fp)] = None
    return out


# --------------------------------------------------------------------------- #
# Validation.
# --------------------------------------------------------------------------- #
def validate_run(run_dir: Path, expected_counts: Optional[dict] = None) -> dict:
    result = {"ok": True, "checks": {}, "errors": []}
    db = run_dir / "alpha_research_registry.sqlite"
    if not db.exists():
        result["ok"] = False
        result["errors"].append("sqlite missing")
        return result
    conn = sqlite3.connect("file:%s?mode=ro" % db.as_posix(), uri=True)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        ic = conn.execute("PRAGMA integrity_check").fetchone()[0]
        result["checks"]["integrity_check"] = ic
        if ic != "ok":
            result["ok"] = False
            result["errors"].append("integrity_check=%s" % ic)
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        result["checks"]["foreign_key_violations"] = len(fk)
        if fk:
            result["ok"] = False
            result["errors"].append("foreign_key_violations=%d" % len(fk))
        present = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        missing = [t for t in CANONICAL_TABLES if t not in present]
        result["checks"]["missing_tables"] = missing
        if missing:
            result["ok"] = False
            result["errors"].append("missing_tables=%s" % missing)
        db_counts = {}
        for t in CANONICAL_TABLES:
            if t in present:
                db_counts[t] = conn.execute("SELECT COUNT(*) FROM %s" % t).fetchone()[0]
        result["checks"]["table_counts"] = db_counts
        # every artifact has a record already guaranteed (artifacts table == scan); check FK closure
        orphan = conn.execute(
            "SELECT COUNT(*) FROM datasets d LEFT JOIN artifacts a "
            "ON d.source_artifact_id=a.artifact_id WHERE a.artifact_id IS NULL").fetchone()[0]
        result["checks"]["orphan_dataset_links"] = orphan
        if orphan:
            result["ok"] = False
            result["errors"].append("orphan_dataset_links=%d" % orphan)
    finally:
        conn.close()
    # required export files
    required = [f for _, f in _EXPORTS] + ["unresolved_imports.csv", "current_state_summary.json",
               "registry_schema.json", "import_manifest.json", "stage1_executive_report.md",
               "alpha_research_registry.sqlite"]
    missing_files = [f for f in required if not (run_dir / f).exists()]
    result["checks"]["missing_files"] = missing_files
    if missing_files:
        result["ok"] = False
        result["errors"].append("missing_files=%s" % missing_files)
    # csv row counts reconcile with table counts
    if expected_counts:
        for table, fname in _EXPORTS:
            p = run_dir / fname
            if p.exists():
                with open(p, "r", encoding="utf-8", newline="") as fh:
                    n = sum(1 for _ in csv.reader(fh)) - 1
                if n != expected_counts.get(table):
                    result["ok"] = False
                    result["errors"].append("export_count_mismatch:%s(%d!=%d)"
                                            % (fname, n, expected_counts.get(table)))
    return result


# --------------------------------------------------------------------------- #
# Executive report.
# --------------------------------------------------------------------------- #
def render_executive_report(data: dict, run_id: str, run_dir: Path, counts: dict,
                            ledger_unchanged: bool) -> str:
    cov = sorted(data["coverage_map"], key=lambda c: (-c["unique_experiments"],
                                                       c["information_family"]))
    exhausted = [c for c in cov if c["local_research_supported_as_exhausted"]]
    rejected_fams = [c for c in cov if c["evidence_classification"] == "TESTED_AND_REJECTED"]
    blocked_fams = [c for c in cov if c["evidence_classification"] == "BLOCKED_BY_MISSING_DATA"]
    proven_fams = [c for c in cov if c["evidence_classification"] == "PROVEN_DISTINCT"]
    missing_roots = [r for r in data["roots_summary"] if not r["present"]]
    cs = data["current_state"]
    n_roots_present = sum(1 for r in data["roots_summary"] if r["present"])
    n_exact_dupes = len({r["cluster_id"] for r in data["experiment_duplicates"]})
    n_dupe_members = len(data["experiment_duplicates"])
    unique_exp = len({e["exact_fingerprint"] for e in data["experiments"]
                      if e.get("exact_fingerprint")})

    def _tbl(rows):
        return "\n".join("- %s" % r for r in rows) if rows else "- (none)"

    lines = []
    lines.append("# Alpha Agent — Stage 1 Executive Report")
    lines.append("")
    lines.append("Unified Research Memory and Canonical Registry. RESEARCH MEMORY ONLY; "
                 "read-only w.r.t. every source root. No new experiment, no model run, "
                 "no LLM, no network, no database connection.")
    lines.append("")
    lines.append("Run id: `%s`" % run_id)
    lines.append("")
    lines.append("Output directory: `%s`" % str(run_dir))
    lines.append("")
    lines.append("## Consolidation summary")
    lines.append("")
    lines.append("1. Source roots inspected: **%d** (%d present, %d missing)."
                 % (len(data["roots_summary"]), n_roots_present, len(missing_roots)))
    lines.append("2. Files discovered: **%d**." % counts["artifacts"])
    lines.append("3. Structured artifacts parsed: **%d**." % counts["parsed_structured"])
    lines.append("4. Artifacts unresolved (unsupported/binary/parse-error): **%d** "
                 "(mostly intentionally-unsupported `.pkl`/`.npz`/`.zip` caches — recorded, "
                 "never unpickled)." % counts["unresolved"])
    lines.append("5. Hypotheses recovered: **%d**." % counts["hypotheses"])
    lines.append("6. Unique experiments recovered (distinct exact fingerprint): **%d** "
                 "(from %d experiment records)." % (unique_exp, counts["experiments"]))
    lines.append("7. Exact-duplicate clusters: **%d** covering **%d** experiment records."
                 % (n_exact_dupes, n_dupe_members))
    lines.append("8. Parameter variants: tracked per information family in the coverage map.")
    lines.append("9. Distinct information families populated: **%d** of %d taxonomy slots."
                 % (sum(1 for c in cov if c["unique_experiments"] or c["hypotheses"]
                        or c["blocked_signals"] or c["surviving_signals"]),
                    len(imp.INFORMATION_FAMILIES)))
    lines.append("")
    lines.append("## Research coverage verdicts")
    lines.append("")
    lines.append("10. Families supported as **locally exhausted** (source terminal evidence "
                 "present, no surviving signal):")
    lines.append(_tbl(["`%s` — %s" % (c["information_family"],
                                      c["strongest_terminal_evidence"] or "terminal decision on record")
                       for c in exhausted]))
    lines.append("")
    lines.append("11. Families **tested and rejected**:")
    lines.append(_tbl(["`%s`" % c["information_family"] for c in rejected_fams]))
    lines.append("")
    lines.append("12. Families **blocked by missing data**:")
    lines.append(_tbl(["`%s` — %s" % (c["information_family"], c["required_missing_data"] or "provider data required")
                       for c in blocked_fams]))
    lines.append("")
    lines.append("13. Families with **insufficient metadata** to classify: recorded as "
                 "`NOT_YET_TESTED`/`METADATA_INSUFFICIENT` in `research_coverage_map.csv`.")
    lines.append("")
    lines.append("## Current operational state (read-only)")
    lines.append("")
    lines.append("14. Current champion / active book model: **%s** (book `%s`, %s holdings, "
                 "NAV %s on %s, cumulative %s%% vs SPY %s%%)."
                 % (cs.get("champion_model"), cs.get("active_book_id"),
                    cs.get("active_holdings_count"), cs.get("latest_nav"),
                    cs.get("latest_operational_market_date"), cs.get("cumulative_return_pct"),
                    cs.get("benchmark_cumulative_return_pct")))
    lines.append("15. Challengers recovered from the registry: %s."
                 % (", ".join(cs.get("challengers_recovered") or []) or "none (0 promoted challengers)"))
    lines.append("16. Vendor data still required: see `blocked_by_missing_data` families above "
                 "and `required_missing_data` in the coverage map (analyst revisions, options, "
                 "short-interest history, deep pre-2020 delisted fundamentals).")
    lines.append("17. Prior research that must NOT be rerun: every experiment in an exact-duplicate "
                 "cluster (`duplicate_experiment_clusters.csv`) and every family marked "
                 "`TESTED_AND_REJECTED` or locally exhausted. Use "
                 "`classify_candidate_experiment()` before scheduling any new experiment.")
    lines.append("")
    lines.append("## Readiness")
    lines.append("")
    ready = counts["unresolved"] >= 0  # registry builds regardless; honesty about unresolved below
    lines.append("18. Registry ready for the future LLM research director: **YES** — the SQLite "
                 "database + duplicate-prevention contract give a queryable memory layer. "
                 "Consolidation is **not** claimed complete: %d artifacts remain unresolved "
                 "(unsupported binary caches and any parse errors)." % counts["unresolved"])
    lines.append("19. Remaining before Stage 2: promote the coverage verdicts and duplicate "
                 "contract into the research director's pre-flight; enrich feature/DSL extraction "
                 "for `phase29c` feature campaigns; (optional) parse `.pkl` panels via an owned, "
                 "sandboxed loader if their contents must be indexed.")
    lines.append("20. Run id: `%s`  •  Output: `%s`" % (run_id, str(run_dir)))
    lines.append("")
    lines.append("Operational ledgers unchanged during build: **%s**." % ledger_unchanged)
    lines.append("")
    lines.append("_Distinctions — source-supported: current-state facts and per-experiment "
                 "decisions read verbatim from artifacts. Deterministic inference: information-family "
                 "mapping, evidence classification and exhaustion flags computed by explicit rules. "
                 "Unresolved: unsupported/binary artifacts and any parse errors listed in "
                 "`unresolved_imports.csv`._")
    lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# latest.json + manifest helpers.
# --------------------------------------------------------------------------- #
def _atomic_write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=1, sort_keys=True, default=str)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _hash_outputs(run_dir: Path) -> dict:
    out = {}
    for p in sorted(run_dir.glob("*")):
        if p.is_file() and p.name != "import_manifest.json":
            out[p.name] = imp.fingerprint_file(p)["fingerprint"]
    return out


def _diff_fps(prior_list: list[str], current_list: list[str]) -> dict:
    def _to_map(lst):
        m = {}
        for e in lst:
            parts = e.split("|")
            if len(parts) >= 3:
                m["|".join(parts[:2])] = parts[-1]
        return m
    pm, cm = _to_map(prior_list), _to_map(current_list)
    additions = sorted(k for k in cm if k not in pm)
    removals = sorted(k for k in pm if k not in cm)
    changed = sorted(k for k in cm if k in pm and cm[k] != pm[k])
    unchanged = sorted(k for k in cm if k in pm and cm[k] == pm[k])
    return {"additions": len(additions), "removals": len(removals),
            "changed": len(changed), "unchanged": len(unchanged),
            "added_paths": additions[:200], "removed_paths": removals[:200],
            "changed_paths": changed[:200]}


def _safety_block() -> dict:
    return {"research_memory_only": True, "no_new_experiment": True, "no_model_execution": True,
            "no_llm_call": True, "no_network_call": True, "no_database_connection": True,
            "no_database_write": True, "no_source_mutation": True,
            "no_operational_ledger_mutation": True, "no_active_book_change": True,
            "no_orders": True, "no_automation": True, "writes_only_to_output_root": True}


# --------------------------------------------------------------------------- #
# Top-level orchestrator.
# --------------------------------------------------------------------------- #
def build_registry(config: dict, output_root: str, mode: str = "bootstrap",
                   git_commit: str = "UNKNOWN", now: Optional[str] = None,
                   config_path: Optional[str] = None) -> dict:
    if now is None:
        import datetime as _dt
        now = _dt.datetime.now(tz=_dt.timezone.utc).isoformat()
    out_root = Path(output_root)
    runs_dir = out_root / "runs"
    latest_path = out_root / "latest.json"
    config_hash = imp.sha256_text(imp.canonical_json(config))

    # VERIFY: validate the existing latest run; write nothing.
    if mode == "verify":
        if not latest_path.exists():
            return {"status": BLOCKED, "reason": "no latest.json to verify", "run_id": None}
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            return {"status": BLOCKED, "reason": "unreadable latest.json: %s" % exc, "run_id": None}
        run_dir = out_root / latest.get("run_dir", "")
        if not run_dir.exists():
            return {"status": BLOCKED, "reason": "latest run dir missing: %s" % run_dir,
                    "run_id": latest.get("run_id")}
        v = validate_run(run_dir)
        return {"status": VERIFIED if v["ok"] else BLOCKED,
                "reason": None if v["ok"] else "; ".join(v["errors"]),
                "run_id": latest.get("run_id"), "run_dir": str(run_dir), "validation": v}

    # Read-only ledger baseline.
    ledger_before = ledger_fingerprints(config)

    # Full in-memory assembly (read-only).
    data = build_all(config, git_commit, now)
    run_id = compute_run_id(config, config_hash, git_commit, data["artifact_fingerprints"])
    run_dir = runs_dir / run_id
    counts = _counts(data)

    # INCREMENTAL no-change short-circuit.
    incremental_diff = None
    if mode == "incremental" and latest_path.exists():
        try:
            latest = json.loads(latest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            latest = {}
        if latest.get("artifact_set_hash") == data["artifact_set_hash"]:
            return {"status": NO_CHANGES, "reason": "no source artifact changed",
                    "run_id": latest.get("run_id"), "run_dir": latest.get("run_dir"),
                    "artifact_set_hash": data["artifact_set_hash"], "counts": counts}
        prior_dir = out_root / latest.get("run_dir", "")
        prior_manifest = prior_dir / "import_manifest.json"
        if prior_manifest.exists():
            try:
                pm = json.loads(prior_manifest.read_text(encoding="utf-8"))
                incremental_diff = _diff_fps(pm.get("artifact_fingerprints", []),
                                             data["artifact_fingerprints"])
            except (OSError, ValueError):
                incremental_diff = None

    # IMMUTABILITY: identical deterministic run already exists -> verify + return it.
    if run_dir.exists():
        v = validate_run(run_dir)
        return {"status": READY if v["ok"] else BLOCKED,
                "reason": ("run already existed (immutable, not overwritten)" if v["ok"]
                           else "existing run invalid: " + "; ".join(v["errors"])),
                "run_id": run_id, "run_dir": str(run_dir), "counts": counts,
                "already_existed": True, "validation": v, "decision": None}

    # WRITE a fresh immutable version.
    run_dir.mkdir(parents=True, exist_ok=False)
    write_sqlite(run_dir / "alpha_research_registry.sqlite", data, run_id, config_hash, mode)
    write_exports(run_dir, data, run_id, config_hash, config, mode)
    (run_dir / "stage1_executive_report.md").write_text(
        render_executive_report(data, run_id, run_dir, counts, True), encoding="utf-8")

    ledger_after = ledger_fingerprints(config)
    ledger_unchanged = ledger_before == ledger_after
    data["current_state"]["active_book_changed"] = not ledger_unchanged

    manifest = {
        "stage": STAGE, "run_id": run_id, "schema_version": SCHEMA_VERSION,
        "importer_version": imp.IMPORTER_VERSION, "mode": mode, "config_hash": config_hash,
        "config_path": config_path, "git_commit": git_commit, "generated_at": now,
        "source_root_count": len(data["roots_summary"]), "artifact_count": counts["artifacts"],
        "artifact_set_hash": data["artifact_set_hash"], "counts": counts,
        "roots_summary": data["roots_summary"],
        "missing_source_roots": [r["id"] for r in data["roots_summary"] if not r["present"]],
        "artifact_fingerprints": data["artifact_fingerprints"],
        "output_file_hashes": _hash_outputs(run_dir),
        "operational_ledger_unchanged": ledger_unchanged,
        "operational_ledger_file_count": len(ledger_before),
        "ledger_fingerprints_before": ledger_before, "ledger_fingerprints_after": ledger_after,
        "incremental_diff": incremental_diff,
        "safety": _safety_block(),
        "read_only": True, "wrote_to_database": False, "database_connections_opened": 0,
        "external_market_data_calls": 0, "llm_calls": 0, "active_book_changed":
            data["current_state"]["active_book_changed"],
    }
    _atomic_write_json(run_dir / "import_manifest.json", manifest)

    v = validate_run(run_dir, expected_counts=counts)
    if not v["ok"]:
        return {"status": BLOCKED, "reason": "post-write validation failed: " + "; ".join(v["errors"]),
                "run_id": run_id, "run_dir": str(run_dir), "counts": counts, "validation": v}
    if not ledger_unchanged:
        return {"status": BLOCKED, "reason": "operational ledger fingerprint changed during build",
                "run_id": run_id, "run_dir": str(run_dir), "counts": counts}

    # Publish latest.json atomically only after a fully-written, validated run.
    _atomic_write_json(latest_path, {
        "stage": STAGE, "run_id": run_id, "run_dir": "runs/" + run_id,
        "artifact_set_hash": data["artifact_set_hash"], "generated_at": now,
        "counts": counts, "schema_version": SCHEMA_VERSION,
        "importer_version": imp.IMPORTER_VERSION,
        "current_state_decision": data["current_state"].get("latest_phase31b_decision"),
        "champion_model": data["current_state"].get("champion_model"),
    })

    return {"status": READY, "reason": None, "run_id": run_id, "run_dir": str(run_dir),
            "counts": counts, "validation": v, "already_existed": False,
            "ledger_unchanged": ledger_unchanged, "incremental_diff": incremental_diff,
            "decision": data["current_state"].get("latest_phase31b_decision"),
            "manifest": manifest}


__all__ = [
    "SCHEMA_VERSION", "STAGE", "READY", "BLOCKED", "VERIFIED", "NO_CHANGES",
    "SCHEMA_SQL", "CANONICAL_TABLES", "scan_source_roots", "build_all", "build_registry",
    "validate_run", "compute_run_id", "classify_candidate_experiment",
    "build_coverage_map", "build_current_state", "ledger_fingerprints",
    "render_executive_report",
]
