"""
alpha_agent.experiment_factory — Stage 5 autonomous experiment & evidence engine.

Reads verified Stage 1-3.5 packages, discovers grounded + accepted Stage 3
hypotheses, deduplicates them against prior experiment history, converts each to
a deterministic allowlisted experiment specification, checks historical-data
coverage, runs bounded historical experiments (via ``experiment_runner``),
applies the deterministic evidence gates and writes an immutable experiment
package plus persistent state — while NEVER touching any operational trading
state, model, portfolio, database, prediction service or LLM.

Hard safety invariants (structural):
  * RESEARCH-ONLY. No order/fill/signal/trade-decision/Daily-Close/model or
    portfolio change; no PostgreSQL, no prediction call, no network, no LLM, no
    code generation or dynamic import of LLM output.
  * Experiment specifications are built ONLY from the allowlisted deterministic
    templates in ``experiment_contracts``; a hypothesis that cannot map safely
    becomes a structured data gap, never generated code.
  * Every identifier is deterministic; completed cycles are idempotent; the same
    hypothesis/spec/data-version never produces a duplicate experiment.
  * Operational ledgers are fingerprinted before and after and must be unchanged.
"""
from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from . import experiment_contracts as ec
from . import experiment_runner as er
from . import runtime_contracts as rc

_ACCEPTED_QUEUE_STATUSES = frozenset({
    "READY_FOR_DETERMINISTIC_DESIGN_REVIEW"})
_GROUNDED = "GROUNDED"

# Cycle statuses persisted in stage5_cycles.status.
CYCLE_COMPLETE = "COMPLETE"
CYCLE_RUNNING = "RUNNING"


# --------------------------------------------------------------------------- #
# Small deterministic IO helpers.
# --------------------------------------------------------------------------- #
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def _read_jsonl(path: Path) -> list:
    out: list = []
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    import os
    os.replace(tmp, path)


def _write_json_atomic(path: Path, obj: Any) -> None:
    _write_text_atomic(path, json.dumps(obj, indent=1, sort_keys=True,
                                        default=str))


def _write_jsonl(path: Path, rows: list) -> None:
    body = "".join(rc.canonical_json(r) + "\n" for r in rows)
    _write_text_atomic(path, body)


def _write_csv(path: Path, fieldnames: list, rows: list) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n",
                            extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: _csv_cell(r.get(k)) for k in fieldnames})
    _write_text_atomic(path, buf.getvalue())


def _csv_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, float):
        return "%.10g" % v
    if isinstance(v, (list, tuple, dict)):
        return rc.canonical_json(v)
    return v


def _sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- #
# Persistent experiment-state database.
# --------------------------------------------------------------------------- #
def state_db_path(root: Path) -> Path:
    return root / "state" / "experiment_state.sqlite"


def open_state_db(root: Path, *, create: bool = True) -> sqlite3.Connection:
    path = state_db_path(root)
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
    else:
        if not path.exists():
            raise FileNotFoundError("stage5 state db missing: %s" % path)
        conn = sqlite3.connect("file:%s?immutable=1" % path.as_posix(),
                               uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if create:
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.DatabaseError:
            pass
        conn.executescript(ec.SCHEMA_SQL)
        conn.execute(
            "INSERT OR IGNORE INTO stage5_meta (key, value) VALUES (?, ?)",
            ("schema_version", ec.STAGE5_SCHEMA_VERSION))
        conn.commit()
    return conn


# --------------------------------------------------------------------------- #
# Source-package reading + verification + fingerprint.
# --------------------------------------------------------------------------- #
_STAGE_LATEST = {
    "stage1": "stage1_registry_root",
    "stage2": "stage2_ingestion_root",
    "stage3": "stage3_director_root",
    "stage3_5": "stage3_5_news_rss_root",
}


def read_source_packages(cfg: dict) -> dict:
    pkgs: dict[str, Any] = {}
    for name, key in _STAGE_LATEST.items():
        root = cfg.get(key)
        latest = _read_json(Path(root) / "latest.json") if root else None
        pkgs[name] = latest or {}
    pkgs["champion_model"] = (pkgs.get("stage1") or {}).get("champion_model")
    pkgs["source_fingerprint"] = _source_fingerprint(pkgs)
    return pkgs


def _source_fingerprint(pkgs: dict) -> str:
    keyed = {
        "stage1": (pkgs.get("stage1") or {}).get("run_id"),
        "stage1_hash": (pkgs.get("stage1") or {}).get("artifact_set_hash"),
        "stage2": (pkgs.get("stage2") or {}).get("run_id"),
        "stage3": (pkgs.get("stage3") or {}).get("run_id"),
        "stage3_5": (pkgs.get("stage3_5") or {}).get("run_id"),
    }
    return ec.sha("src", rc.canonical_json(keyed))[:16]


def verify_source_packages(cfg: dict, pkgs: dict) -> tuple:
    """Return (ok, problems). Requires Stage 1-3 latest packages to resolve to a
    real run directory (Stage 3.5 is operational-partial and only warned)."""
    problems: list[str] = []
    for name in ("stage1", "stage2", "stage3"):
        latest = pkgs.get(name) or {}
        root = Path(cfg.get(_STAGE_LATEST[name]) or "")
        if not latest.get("run_id"):
            problems.append("%s latest.json missing or has no run_id" % name)
            continue
        run_dir = latest.get("run_dir")
        if run_dir:
            rp = Path(run_dir) if Path(run_dir).is_absolute() \
                else (root / run_dir)
            if not rp.exists():
                problems.append("%s run dir missing: %s" % (name, rp))
    return (not problems), problems


# --------------------------------------------------------------------------- #
# Hypothesis intake (grounded + accepted Stage 3 proposals).
# --------------------------------------------------------------------------- #
def read_grounded_hypotheses(cfg: dict) -> list[dict]:
    stage3 = _read_json(Path(cfg["stage3_director_root"]) / "latest.json") or {}
    run_dir = stage3.get("run_dir")
    if not run_dir:
        return []
    root = Path(cfg["stage3_director_root"])
    rp = Path(run_dir) if Path(run_dir).is_absolute() else (root / run_dir)
    proposals = _read_json(rp / "hypothesis_proposals.json") or {}
    queue = _read_json(rp / "research_queue.json") or {}
    prop_list = proposals.get("proposals") or proposals.get("hypotheses") or []
    queue_list = queue.get("queue") or []
    queue_by_id = {q.get("hypothesis_id"): q for q in queue_list
                   if isinstance(q, dict)}
    out: list[dict] = []
    for obj in prop_list:
        if not isinstance(obj, dict):
            continue
        grounding = ((obj.get("grounding_validation") or {}).get("grounding")
                     or obj.get("grounding_result"))
        proposal = obj.get("proposal") if isinstance(obj.get("proposal"), dict) \
            else obj
        hid = proposal.get("hypothesis_id") or obj.get("hypothesis_id")
        if not hid:
            continue
        q = queue_by_id.get(hid) or {}
        q_status = q.get("status")
        if grounding != _GROUNDED or q_status not in _ACCEPTED_QUEUE_STATUSES:
            continue
        out.append(_normalize_intake(proposal, q))
    return out


def _normalize_intake(proposal: dict, queue: dict) -> dict:
    reqs = list(proposal.get("required_fields") or [])
    reqs += list(proposal.get("data_adequacy_requirements") or [])
    reqs += list(queue.get("required_data") or [])
    text = (proposal.get("economic_rationale") or proposal.get("title")
            or proposal.get("hypothesis_statement") or "")
    return {
        "hypothesis_id": proposal.get("hypothesis_id"),
        "title": proposal.get("title") or "",
        "text": text,
        "information_family": proposal.get("information_family") or "",
        "expected_direction": proposal.get("expected_direction") or "",
        "rebalance_cadence": proposal.get("rebalance_cadence") or "",
        "horizon": proposal.get("horizon") or "",
        "universe": proposal.get("universe") or "",
        "required_fields": [str(r) for r in reqs],
        "feature_definition": proposal.get("feature_definition") or "",
        "source_record_ids": list(proposal.get("source_record_ids") or []),
        "priority": queue.get("priority"),
        "queue_status": queue.get("status"),
        "grounding": _GROUNDED,
    }


# --------------------------------------------------------------------------- #
# Duplicate prevention + template mapping + spec generation.
# --------------------------------------------------------------------------- #
def is_duplicate(conn: sqlite3.Connection, hypothesis_id: str,
                 template: str) -> bool:
    key = ec.hypothesis_seen_key(hypothesis_id, template)
    row = conn.execute(
        "SELECT 1 FROM hypotheses_seen WHERE hypothesis_key=?",
        (key,)).fetchone()
    return row is not None


def detect_unsupported(hyp: dict) -> Optional[str]:
    """Return a short reason if the hypothesis needs data no allowlisted,
    owned-data template can safely express; else None."""
    haystack = " ".join([
        hyp.get("text", ""), hyp.get("feature_definition", ""),
        hyp.get("title", ""), " ".join(hyp.get("required_fields", [])),
    ]).lower()
    for tok in ec.UNSUPPORTED_REQUIREMENT_TOKENS:
        if tok in haystack:
            return "requires unsupported data: '%s'" % tok
    return None


def map_to_templates(hyp: dict, enabled: list[str]) -> list[str]:
    """Deterministically rank enabled templates for a hypothesis by
    family/keyword score. Returns candidate template names (may be empty)."""
    fam = (hyp.get("information_family") or "").lower()
    text = " ".join([hyp.get("text", ""), hyp.get("title", ""),
                     hyp.get("feature_definition", "")]).lower()
    scored: list[tuple] = []
    for name in enabled:
        tpl = ec.TEMPLATES.get(name)
        if not tpl:
            continue
        score = 0
        for kw in tpl["keywords"]:
            if kw in text:
                score += 2
            if kw in fam:
                score += 3
        # Family-name affinity.
        if fam and any(fam in kw or kw in fam for kw in tpl["keywords"]):
            score += 2
        if score > 0:
            scored.append((score, name))
    # Deterministic: highest score first, then registry order for ties.
    order = {n: i for i, n in enumerate(ec.SUPPORTED_TEMPLATES)}
    scored.sort(key=lambda s: (-s[0], order.get(s[1], 999)))
    return [name for _score, name in scored]


def build_spec(hyp: dict, template: str, pkgs: dict, *, coverage: dict,
               cfg: dict) -> dict:
    tpl = ec.TEMPLATES[template]
    data_version = coverage.get("data_version") or ec.data_version_id(
        pkgs.get("source_fingerprint", ""), coverage.get("date_start", ""),
        coverage.get("date_end", ""))
    cost_grid = list(cfg.get("cost_bps") or [10, 25, 50])
    primary_cost = cost_grid[len(cost_grid) // 2] if cost_grid else 25
    spec = {
        "template": template,
        "template_version": tpl["version"],
        "study_kind": tpl["study_kind"],
        "hypothesis_id": hyp["hypothesis_id"],
        "hypothesis_text": hyp.get("text", "")[:600],
        "feature": tpl["default_feature"],
        "features_allowed": list(tpl["features"]),
        "required_datasets": list(tpl["required_datasets"]),
        "universe": hyp.get("universe") or "owned_scoreable_universe",
        "date_start": coverage.get("date_start"),
        "date_end": coverage.get("date_end"),
        "rebalance": tpl["rebalance"],
        "horizon_days": tpl["horizon_days"],
        "benchmark": (cfg.get("benchmark") or {}).get("name",
                                                      "equal_weight_universe"),
        "transaction_cost_bps": primary_cost,
        "cost_grid_bps": cost_grid,
        "min_universe": tpl["min_universe"],
        "min_periods": tpl["min_periods"],
        "leakage_control": tpl["leakage_control"],
        "expected_direction": tpl["expected_direction"],
        "data_version": data_version,
        "creation_source": "stage5_allowlisted_template",
        "lineage": {
            "stage1_run_id": (pkgs.get("stage1") or {}).get("run_id"),
            "stage2_run_id": (pkgs.get("stage2") or {}).get("run_id"),
            "stage3_run_id": (pkgs.get("stage3") or {}).get("run_id"),
        },
    }
    spec["spec_fingerprint"] = ec.spec_fingerprint(spec)
    spec["experiment_id"] = ec.experiment_id(
        hyp["hypothesis_id"], template, spec["spec_fingerprint"], data_version)
    return spec


# --------------------------------------------------------------------------- #
# Stage 5 cycle orchestration.
# --------------------------------------------------------------------------- #
def run_stage5_cycle(cfg: dict, *, output_root: Optional[str] = None,
                     as_of: str = "latest", now_iso: Optional[str] = None,
                     hypotheses: Optional[list] = None,
                     source_packages: Optional[dict] = None,
                     store: Optional[Any] = None,
                     ledger_fingerprint: Optional[Callable[[], dict]] = None
                     ) -> dict:
    """Execute one bounded, deterministic Stage 5 experiment & evidence cycle.

    All external inputs (hypotheses, source packages, data store, clock) are
    injectable so the whole engine runs deterministically with fakes in tests.
    Returns a fully-computed result dict (also written as an immutable package).
    """
    now = now_iso or _now_utc_iso()
    root = Path(output_root or cfg["experiments_root"])
    root.mkdir(parents=True, exist_ok=True)
    (root / "runs").mkdir(parents=True, exist_ok=True)
    (root / "state").mkdir(parents=True, exist_ok=True)

    ledgers_before = ledger_fingerprint() if ledger_fingerprint else None

    pkgs = source_packages if source_packages is not None \
        else read_source_packages(cfg)
    pkgs.setdefault("source_fingerprint", _source_fingerprint(pkgs))
    ok, problems = verify_source_packages(cfg, pkgs)
    cfg_hash = ec.config_hash(cfg)
    run_id = ec.stage5_run_id(as_of, cfg_hash, pkgs["source_fingerprint"])

    conn = open_state_db(root)
    try:
        if not ok:
            return _finish_blocked(conn, root, run_id, now, cfg, pkgs,
                                   problems, ledgers_before, ledger_fingerprint)

        # Idempotency: a COMPLETE cycle with identical inputs is not redone.
        existing = conn.execute(
            "SELECT status FROM stage5_cycles WHERE run_id=?",
            (run_id,)).fetchone()
        run_dir = root / "runs" / run_id
        if existing and existing["status"] == CYCLE_COMPLETE \
                and (run_dir / "run_manifest.json").exists():
            cached = _read_json(run_dir / "run_manifest.json") or {}
            cached["idempotent_replay"] = True
            return _load_result(root, run_id, cached)

        conn.execute(
            "INSERT OR REPLACE INTO stage5_cycles (run_id, as_of, config_hash,"
            " source_fingerprint, status, started_at) VALUES (?,?,?,?,?,?)",
            (run_id, as_of, cfg_hash, pkgs["source_fingerprint"],
             CYCLE_RUNNING, now))
        conn.commit()

        raw_hyps = hypotheses if hypotheses is not None \
            else read_grounded_hypotheses(cfg)
        grounded_count = len(raw_hyps)

        enabled = ec.enabled_templates(cfg)
        gates = ec.resolved_gates(cfg)
        max_new = int(ec.bound(cfg, "max_new_hypotheses", 3))
        max_specs = int(ec.bound(cfg, "max_specs_per_hypothesis", 2))
        max_exp = int(ec.bound(cfg, "max_experiments", 6))

        runner = er.ExperimentRunner(cfg, store=store)

        intake_rows: list[dict] = []
        duplicates: list[dict] = []
        specs: list[dict] = []
        data_gaps: list[dict] = []
        results: list[dict] = []
        decisions: list[dict] = []
        failures: list[dict] = []
        deferred = 0

        considered = 0
        for hyp in raw_hyps:
            hid = hyp.get("hypothesis_id")
            if considered >= max_new:
                deferred += 1
                continue
            considered += 1
            unsupported = detect_unsupported(hyp)
            # Candidate templates (bounded).
            cand = [] if unsupported else map_to_templates(hyp, enabled)
            intake_rows.append({
                "hypothesis_id": hid, "title": hyp.get("title"),
                "information_family": hyp.get("information_family"),
                "candidate_templates": cand[:max_specs],
                "unsupported": unsupported,
                "grounding": hyp.get("grounding"),
                "queue_status": hyp.get("queue_status"),
            })
            if unsupported or not cand:
                reason = ec.DATA_HOLD_UNSUPPORTED_TEMPLATE
                detail = unsupported or "no allowlisted template matched"
                data_gaps.append({"hypothesis_id": hid, "template": None,
                                  "gap_reason": reason, "detail": detail})
                _record_gap(conn, run_id, hid, None, reason, detail, now)
                _record_seen(conn, run_id, hyp, None, reason, now)
                continue

            hyp_specs = 0
            for template in cand:
                if hyp_specs >= max_specs:
                    break
                if len(specs) + len(data_gaps) >= 10 * max_exp:
                    break
                if is_duplicate(conn, hid, template):
                    duplicates.append({"hypothesis_id": hid,
                                       "template": template,
                                       "reason": "already experimented"})
                    continue
                coverage = runner.check_coverage(hyp, template)
                if coverage.get("data_gap"):
                    reason = coverage["data_gap"]
                    detail = coverage.get("detail", "")
                    data_gaps.append({"hypothesis_id": hid,
                                      "template": template,
                                      "gap_reason": reason, "detail": detail})
                    _record_gap(conn, run_id, hid, template, reason, detail,
                                now)
                    _record_seen(conn, run_id, hyp, template, reason, now)
                    hyp_specs += 1
                    continue
                spec = build_spec(hyp, template, pkgs, coverage=coverage,
                                  cfg=cfg)
                if len([s for s in specs]) >= max_exp:
                    deferred += 1
                    break
                specs.append(spec)
                _record_spec(conn, run_id, hyp, spec, now)
                _record_seen(conn, run_id, hyp, template,
                             "EXPERIMENTED", now)
                hyp_specs += 1

        # Run bounded experiments.
        for spec in specs[:max_exp]:
            try:
                metrics = runner.run_experiment(spec, gates=gates,
                                                champion=pkgs.get(
                                                    "champion_model"))
            except Exception as exc:  # noqa: BLE001
                metrics = {"experiment_failed": True,
                           "failure_reason": "%s: %s" % (type(exc).__name__,
                                                         rc.redact(str(exc)))}
            # A run-time data gap (e.g. a template whose signal field is absent
            # from owned data) is recorded as a structured gap, never scored as
            # a completed experiment.
            if metrics.get("data_gap") and metrics.get("rank_ic_mean") is None:
                reason = metrics["data_gap"]
                detail = metrics.get("detail", "")
                data_gaps.append({"hypothesis_id": spec["hypothesis_id"],
                                  "template": spec["template"],
                                  "gap_reason": reason, "detail": detail})
                _record_gap(conn, run_id, spec["hypothesis_id"],
                            spec["template"], reason, detail, now)
                continue
            decision = ec.evaluate_evidence(metrics, gates)
            metrics_row = {"experiment_id": spec["experiment_id"],
                           "hypothesis_id": spec["hypothesis_id"],
                           "template": spec["template"], **metrics}
            results.append(metrics_row)
            decisions.append({"experiment_id": spec["experiment_id"],
                              "hypothesis_id": spec["hypothesis_id"],
                              "template": spec["template"],
                              "decision": decision["decision"],
                              "reasons": decision["reasons"]})
            _record_result(conn, run_id, spec, metrics, decision, now)
            if metrics.get("experiment_failed"):
                failures.append({"experiment_id": spec["experiment_id"],
                                 "hypothesis_id": spec["hypothesis_id"],
                                 "failure_reason": metrics.get(
                                     "failure_reason")})
                _record_failure(conn, run_id, spec, metrics, now)

        deferred += max(0, len(specs) - max_exp)

        control = _maybe_control_health_check(cfg, pkgs, results)

        terminal, status = _terminal(grounded_count, considered,
                                      len(intake_rows), specs, results,
                                      failures, data_gaps, duplicates,
                                      deferred)

        # Ledger immutability check.
        ledgers_after = ledger_fingerprint() if ledger_fingerprint else None
        ledgers_ok = (ledgers_before is None) or (ledgers_before ==
                                                  ledgers_after)
        if not ledgers_ok:
            terminal, status = ec.BLOCKED, "LEDGER_MUTATION_DETECTED"

        result = {
            "stage": ec.STAGE5_STAGE,
            "schema_version": ec.STAGE5_SCHEMA_VERSION,
            "engine_version": ec.STAGE5_ENGINE_VERSION,
            "run_id": run_id, "as_of": as_of, "generated_at": now,
            "config_hash": cfg_hash,
            "source_fingerprint": pkgs["source_fingerprint"],
            "champion_model": pkgs.get("champion_model"),
            "terminal": terminal, "status": status,
            "ledgers_unchanged": ledgers_ok,
            "counts": {
                "grounded_hypotheses": grounded_count,
                "hypotheses_considered": considered,
                "specs_generated": len(specs),
                "experiments_completed": len(results),
                "experiments_failed": len(failures),
                "data_gaps": len(data_gaps),
                "duplicates_rejected": len(duplicates),
                "keep_for_research": sum(
                    1 for d in decisions
                    if d["decision"] == ec.KEEP_FOR_RESEARCH),
                "deferred_to_next_cycle": deferred,
            },
            "intake": intake_rows,
            "specs": specs,
            "results": results,
            "decisions": decisions,
            "data_gaps": data_gaps,
            "duplicates": duplicates,
            "failures": failures,
            "control_health_check": control,
            "source_packages": {
                k: (pkgs.get(k) or {}).get("run_id")
                for k in ("stage1", "stage2", "stage3", "stage3_5")},
        }

        run_dir = _write_run_package(root, run_id, cfg, pkgs, result)
        result["run_dir"] = str(run_dir)

        conn.execute(
            "UPDATE stage5_cycles SET status=?, terminal=?,"
            " hypotheses_considered=?, experiments_completed=?,"
            " experiments_failed=?, finished_at=? WHERE run_id=?",
            (CYCLE_COMPLETE, terminal, considered, len(results),
             len(failures), now, run_id))
        conn.commit()

        _write_json_atomic(root / "latest.json", {
            "run_id": run_id, "as_of": as_of, "generated_at": now,
            "terminal": terminal, "status": status,
            "run_dir": "runs/%s" % run_id,
            "counts": result["counts"],
            "champion_model": pkgs.get("champion_model")})
        return result
    finally:
        conn.close()


def _terminal(grounded_count, considered, intake_n, specs, results, failures,
              data_gaps, duplicates, deferred):
    if grounded_count == 0:
        return ec.NO_EXPERIMENTABLE_HYPOTHESES, "NO_GROUNDED_HYPOTHESES"
    if not specs and not results:
        # Grounded hypotheses existed but none produced an experiment.
        if data_gaps:
            return ec.DATA_HOLD, "ALL_HYPOTHESES_DATA_HELD"
        if duplicates and not data_gaps:
            return ec.NO_EXPERIMENTABLE_HYPOTHESES, "ALL_DUPLICATES"
        return ec.NO_EXPERIMENTABLE_HYPOTHESES, "NO_EXPERIMENTABLE_HYPOTHESES"
    if results:
        if failures or deferred or data_gaps:
            return ec.PARTIAL, "EXPERIMENTS_RUN_WITH_REMAINDER"
        return ec.READY, "EXPERIMENTS_COMPLETE"
    # Specs generated but not run (bounded out) — partial.
    if data_gaps:
        return ec.DATA_HOLD, "SPECS_HELD_FOR_DATA"
    return ec.PARTIAL, "SPECS_DEFERRED"


def _maybe_control_health_check(cfg, pkgs, results) -> Optional[dict]:
    if results:
        return None
    if not (cfg.get("control") or {}).get("enable_champion_health_check"):
        return None
    return {
        "kind": "champion_identity_health_check",
        "champion_model": pkgs.get("champion_model"),
        "stage1_run_id": (pkgs.get("stage1") or {}).get("run_id"),
        "note": "No new experiment this cycle; recorded champion identity only."
                " No metric fabricated, no model or portfolio touched.",
    }


# --------------------------------------------------------------------------- #
# State persistence helpers.
# --------------------------------------------------------------------------- #
def _record_seen(conn, run_id, hyp, template, disposition, now):
    hid = hyp.get("hypothesis_id")
    key = ec.hypothesis_seen_key(hid, template)
    conn.execute(
        "INSERT OR IGNORE INTO hypotheses_seen (hypothesis_key, hypothesis_id,"
        " template, hypothesis_sha, first_seen_run_id, first_seen_at,"
        " disposition) VALUES (?,?,?,?,?,?,?)",
        (key, hid, template, ec.hypothesis_sha(hyp.get("text", "")), run_id,
         now, disposition))
    conn.commit()


def _record_spec(conn, run_id, hyp, spec, now):
    conn.execute(
        "INSERT OR IGNORE INTO experiment_specs (experiment_id, hypothesis_id,"
        " hypothesis_key, template, template_version, spec_fingerprint,"
        " data_version, spec_json, created_run_id, created_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (spec["experiment_id"], spec["hypothesis_id"],
         ec.hypothesis_seen_key(spec["hypothesis_id"], spec["template"]),
         spec["template"], spec["template_version"], spec["spec_fingerprint"],
         spec["data_version"], rc.canonical_json(spec), run_id, now))
    conn.execute(
        "INSERT OR IGNORE INTO experiment_runs (experiment_id, run_id, status,"
        " started_at) VALUES (?,?,?,?)",
        (spec["experiment_id"], run_id, "SPEC_CREATED", now))
    conn.commit()


def _record_result(conn, run_id, spec, metrics, decision, now):
    conn.execute(
        "INSERT OR REPLACE INTO experiment_results (experiment_id, run_id,"
        " metrics_json, benchmark_json, regime_json, cost_json, recorded_at)"
        " VALUES (?,?,?,?,?,?,?)",
        (spec["experiment_id"], run_id, rc.canonical_json(metrics),
         rc.canonical_json(metrics.get("benchmark") or {}),
         rc.canonical_json(metrics.get("regime") or {}),
         rc.canonical_json(metrics.get("cost_sensitivity") or {}), now))
    conn.execute(
        "INSERT OR REPLACE INTO evidence_decisions (experiment_id, run_id,"
        " hypothesis_id, decision, reasons_json, recorded_at)"
        " VALUES (?,?,?,?,?,?)",
        (spec["experiment_id"], run_id, spec["hypothesis_id"],
         decision["decision"], rc.canonical_json(decision["reasons"]), now))
    conn.execute(
        "UPDATE experiment_runs SET status=?, finished_at=? WHERE"
        " experiment_id=? AND run_id=?",
        ("COMPLETE", now, spec["experiment_id"], run_id))
    conn.commit()


def _record_gap(conn, run_id, hid, template, reason, detail, now):
    conn.execute(
        "INSERT INTO data_gaps (run_id, hypothesis_id, template, gap_reason,"
        " detail, recorded_at) VALUES (?,?,?,?,?,?)",
        (run_id, hid, template, reason, str(detail)[:500], now))
    conn.commit()


def _record_failure(conn, run_id, spec, metrics, now):
    conn.execute(
        "INSERT INTO failed_experiments (run_id, experiment_id, hypothesis_id,"
        " failure_reason, detail, recorded_at) VALUES (?,?,?,?,?,?)",
        (run_id, spec["experiment_id"], spec["hypothesis_id"],
         str(metrics.get("failure_reason"))[:200], None, now))
    conn.commit()


def _finish_blocked(conn, root, run_id, now, cfg, pkgs, problems,
                    ledgers_before, ledger_fingerprint):
    result = {
        "stage": ec.STAGE5_STAGE, "run_id": run_id, "generated_at": now,
        "terminal": ec.BLOCKED,
        "status": "MISSING_SOURCE_ARTIFACTS",
        "problems": problems,
        "counts": {"grounded_hypotheses": 0, "experiments_completed": 0},
    }
    conn.execute(
        "INSERT OR REPLACE INTO stage5_cycles (run_id, as_of, config_hash,"
        " source_fingerprint, status, terminal, started_at, finished_at)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (run_id, "latest", ec.config_hash(cfg),
         pkgs.get("source_fingerprint", ""), "BLOCKED", ec.BLOCKED, now, now))
    conn.commit()
    return result


# --------------------------------------------------------------------------- #
# Immutable run-package writer.
# --------------------------------------------------------------------------- #
def _write_run_package(root: Path, run_id: str, cfg: dict, pkgs: dict,
                       result: dict) -> Path:
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json_atomic(run_dir / "stage5_input.json", {
        "run_id": run_id, "as_of": result.get("as_of"),
        "generated_at": result.get("generated_at"),
        "config_hash": result.get("config_hash"),
        "bounds": cfg.get("bounds"), "gates": ec.resolved_gates(cfg),
        "templates_enabled": ec.enabled_templates(cfg)})
    _write_json_atomic(run_dir / "source_packages.json", {
        "source_fingerprint": pkgs.get("source_fingerprint"),
        "champion_model": pkgs.get("champion_model"),
        "packages": {k: pkgs.get(k) for k in
                     ("stage1", "stage2", "stage3", "stage3_5")}})
    _write_jsonl(run_dir / "hypothesis_intake.jsonl", result["intake"])
    _write_jsonl(run_dir / "experiment_specs.jsonl", result["specs"])
    _write_jsonl(run_dir / "experiment_results.jsonl", result["results"])
    _write_jsonl(run_dir / "evidence_decisions.jsonl", result["decisions"])
    _write_jsonl(run_dir / "data_gaps.jsonl", result["data_gaps"])
    _write_jsonl(run_dir / "rejected_duplicates.jsonl", result["duplicates"])

    _write_csv(run_dir / "data_coverage.csv",
               ["experiment_id", "hypothesis_id", "template",
                "required_datasets", "date_start", "date_end", "universe",
                "periods", "coverage_ok"],
               [_coverage_row(s, result) for s in result["specs"]])
    _write_csv(run_dir / "evidence_metrics.csv",
               ["experiment_id", "hypothesis_id", "template", "observations",
                "universe", "periods", "rank_ic_mean", "rank_ic_t",
                "rank_ic_positive_ratio", "decile_spread_mean", "spread_t",
                "gross_annualized_return", "net_annualized_return",
                "annualized_vol", "sharpe", "max_drawdown", "turnover",
                "hit_rate", "missing_data_rate", "decision"],
               [_metrics_row(r, result) for r in result["results"]])
    _write_csv(run_dir / "benchmark_comparison.csv",
               ["experiment_id", "benchmark", "benchmark_excess_annualized",
                "champion_model", "champion_complementarity"],
               [_benchmark_row(r, result) for r in result["results"]])
    _write_csv(run_dir / "regime_results.csv",
               ["experiment_id", "regime_consistency",
                "subperiod_consistency", "up_regime_excess",
                "down_regime_excess"],
               [_regime_row(r) for r in result["results"]])
    _write_csv(run_dir / "cost_sensitivity.csv",
               ["experiment_id", "cost_bps", "net_annualized_return",
                "cost_flips_sign"],
               [row for r in result["results"] for row in _cost_rows(r)])

    report_md = _render_report_md(result)
    _write_text_atomic(run_dir / "stage5_experiment_report.md", report_md)

    file_hashes = {}
    for name in ec.REQUIRED_RUN_FILES:
        if name == "run_manifest.json":
            continue
        p = run_dir / name
        if p.exists():
            file_hashes[name] = _sha256_file(p)
    manifest = {
        "stage": ec.STAGE5_STAGE,
        "schema_version": ec.STAGE5_SCHEMA_VERSION,
        "engine_version": ec.STAGE5_ENGINE_VERSION,
        "template_registry_version": ec.TEMPLATE_REGISTRY_VERSION,
        "run_id": run_id, "generated_at": result.get("generated_at"),
        "terminal": result.get("terminal"), "status": result.get("status"),
        "counts": result.get("counts"),
        "source_fingerprint": pkgs.get("source_fingerprint"),
        "required_files": list(ec.REQUIRED_RUN_FILES),
        "file_hashes": file_hashes,
    }
    _write_json_atomic(run_dir / "run_manifest.json", manifest)
    return run_dir


def _coverage_row(spec, result):
    return {"experiment_id": spec["experiment_id"],
            "hypothesis_id": spec["hypothesis_id"],
            "template": spec["template"],
            "required_datasets": spec.get("required_datasets"),
            "date_start": spec.get("date_start"),
            "date_end": spec.get("date_end"),
            "universe": spec.get("universe"),
            "periods": next((r.get("periods") for r in result["results"]
                             if r["experiment_id"] == spec["experiment_id"]),
                            None),
            "coverage_ok": True}


def _decision_for(result, experiment_id):
    for d in result["decisions"]:
        if d["experiment_id"] == experiment_id:
            return d["decision"]
    return None


def _metrics_row(r, result):
    row = {k: r.get(k) for k in (
        "experiment_id", "hypothesis_id", "template", "observations",
        "universe", "periods", "rank_ic_mean", "rank_ic_t",
        "rank_ic_positive_ratio", "decile_spread_mean", "spread_t",
        "gross_annualized_return", "net_annualized_return", "annualized_vol",
        "sharpe", "max_drawdown", "turnover", "hit_rate", "missing_data_rate")}
    row["decision"] = _decision_for(result, r["experiment_id"])
    return row


def _benchmark_row(r, result):
    return {"experiment_id": r["experiment_id"],
            "benchmark": r.get("benchmark_name"),
            "benchmark_excess_annualized": r.get("benchmark_excess_annualized"),
            "champion_model": result.get("champion_model"),
            "champion_complementarity": r.get("champion_complementarity")}


def _regime_row(r):
    reg = r.get("regime") or {}
    return {"experiment_id": r["experiment_id"],
            "regime_consistency": r.get("regime_consistency"),
            "subperiod_consistency": r.get("subperiod_consistency"),
            "up_regime_excess": reg.get("up_regime_excess"),
            "down_regime_excess": reg.get("down_regime_excess")}


def _cost_rows(r):
    grid = (r.get("cost_sensitivity") or {}).get("grid") or []
    if not grid:
        return [{"experiment_id": r["experiment_id"], "cost_bps": None,
                 "net_annualized_return": r.get("net_annualized_return"),
                 "cost_flips_sign": r.get("cost_flips_sign")}]
    return [{"experiment_id": r["experiment_id"], "cost_bps": g.get("cost_bps"),
             "net_annualized_return": g.get("net_annualized_return"),
             "cost_flips_sign": g.get("flips_sign")} for g in grid]


def _render_report_md(result: dict) -> str:
    c = result.get("counts", {})
    lines = ["# Alpha Agent Stage 5 experiment & evidence report", "",
             "- Run ID: %s" % result.get("run_id"),
             "- Terminal: %s (%s)" % (result.get("terminal"),
                                      result.get("status")),
             "- Generated: %s" % result.get("generated_at"),
             "- Champion: %s" % result.get("champion_model"), "",
             "## Summary", "",
             "- Grounded hypotheses: %s" % c.get("grounded_hypotheses"),
             "- Considered this cycle: %s" % c.get("hypotheses_considered"),
             "- Specs generated: %s" % c.get("specs_generated"),
             "- Experiments completed: %s" % c.get("experiments_completed"),
             "- Experiments failed: %s" % c.get("experiments_failed"),
             "- Data gaps: %s" % c.get("data_gaps"),
             "- Duplicates rejected: %s" % c.get("duplicates_rejected"),
             "- KEEP_FOR_RESEARCH: %s" % c.get("keep_for_research"),
             "- Deferred to next cycle: %s" %
             c.get("deferred_to_next_cycle"), ""]
    if result.get("decisions"):
        lines += ["## Evidence decisions", ""]
        for d in result["decisions"]:
            lines.append("- `%s` [%s] → **%s** — %s" % (
                d.get("hypothesis_id"), d.get("template"), d.get("decision"),
                "; ".join(d.get("reasons", []))[:240]))
        lines.append("")
    if result.get("data_gaps"):
        lines += ["## Data gaps", ""]
        for g in result["data_gaps"]:
            lines.append("- `%s` [%s] → %s: %s" % (
                g.get("hypothesis_id"), g.get("template"), g.get("gap_reason"),
                str(g.get("detail"))[:200]))
        lines.append("")
    lines += ["## Safety", "",
              "- Research-only: no order/fill/signal/decision/model/portfolio "
              "change; no DB, no prediction service, no LLM code execution.",
              "- Experiment specs from allowlisted deterministic templates "
              "only.",
              "- Operational ledgers unchanged: %s." %
              result.get("ledgers_unchanged"), ""]
    return "\n".join(lines)


def _load_result(root: Path, run_id: str, manifest: dict) -> dict:
    run_dir = root / "runs" / run_id
    intake = _read_jsonl(run_dir / "hypothesis_intake.jsonl")
    specs = _read_jsonl(run_dir / "experiment_specs.jsonl")
    results = _read_jsonl(run_dir / "experiment_results.jsonl")
    decisions = _read_jsonl(run_dir / "evidence_decisions.jsonl")
    gaps = _read_jsonl(run_dir / "data_gaps.jsonl")
    dups = _read_jsonl(run_dir / "rejected_duplicates.jsonl")
    return {
        "run_id": run_id, "run_dir": str(run_dir),
        "terminal": manifest.get("terminal"), "status": manifest.get("status"),
        "counts": manifest.get("counts") or {},
        "generated_at": manifest.get("generated_at"),
        "idempotent_replay": True,
        "intake": intake, "specs": specs, "results": results,
        "decisions": decisions, "data_gaps": gaps, "duplicates": dups,
    }


# --------------------------------------------------------------------------- #
# Verify (no writes, no network).
# --------------------------------------------------------------------------- #
def verify_cycle(cfg: dict, *, output_root: Optional[str] = None,
                 run_id: Optional[str] = None,
                 ledger_fingerprint: Optional[Callable[[], dict]] = None
                 ) -> dict:
    """Read-only verification of the latest (or given) Stage 5 package."""
    root = Path(output_root or cfg["experiments_root"])
    problems: list[str] = []
    checks: dict[str, Any] = {}

    latest = _read_json(root / "latest.json") or {}
    rid = run_id or latest.get("run_id")
    if not rid:
        return {"terminal": ec.BLOCKED, "status": "NO_LATEST_PACKAGE",
                "problems": ["no latest.json / run_id"]}
    run_dir = root / "runs" / rid
    manifest = _read_json(run_dir / "run_manifest.json") or {}
    if not manifest:
        problems.append("run_manifest.json missing for %s" % rid)

    # Required files present + hashes reconcile.
    for name in ec.REQUIRED_RUN_FILES:
        p = run_dir / name
        if not p.exists():
            problems.append("missing required file: %s" % name)
    for name, want in (manifest.get("file_hashes") or {}).items():
        p = run_dir / name
        if p.exists() and _sha256_file(p) != want:
            problems.append("hash mismatch: %s" % name)

    # State DB present + required tables + integrity (read-only).
    try:
        conn = open_state_db(root, create=False)
        ic = conn.execute("PRAGMA integrity_check").fetchone()
        checks["integrity_check"] = ic[0] if ic else "unknown"
        if checks["integrity_check"] != "ok":
            problems.append("state db integrity: %s" % checks["integrity_check"])
        have = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        for t in ec.REQUIRED_TABLES:
            if t not in have:
                problems.append("missing table: %s" % t)
        # Result reconciliation: every decision has a spec + result.
        drow = conn.execute(
            "SELECT COUNT(*) FROM evidence_decisions d WHERE NOT EXISTS"
            " (SELECT 1 FROM experiment_specs s WHERE"
            " s.experiment_id=d.experiment_id)").fetchone()
        if drow and drow[0]:
            problems.append("%d decisions without a spec" % drow[0])
        conn.close()
    except (FileNotFoundError, sqlite3.DatabaseError) as exc:
        problems.append("state db unreadable: %s" % exc)

    # Evidence-decision domain check from the written package.
    for d in _read_jsonl(run_dir / "evidence_decisions.jsonl"):
        if d.get("decision") not in ec.EVIDENCE_DECISIONS:
            problems.append("invalid decision token: %s" % d.get("decision"))

    if ledger_fingerprint is not None:
        checks["ledger_files"] = len(ledger_fingerprint())

    checks["problems"] = problems
    checks["run_id"] = rid
    if problems:
        return {"terminal": ec.BLOCKED, "status": "VERIFY_FAILED",
                "run_id": rid, "problems": problems, "checks": checks}
    return {"terminal": ec.VERIFIED, "status": "VERIFY_OK", "run_id": rid,
            "checks": checks}


# --------------------------------------------------------------------------- #
# Report model for the Stage 4 "Experiment & Evidence" section.
# --------------------------------------------------------------------------- #
def experiment_report_model(result: dict) -> dict:
    """Deterministic model for the report renderer. All numbers from Python."""
    c = result.get("counts", {})
    results = result.get("results", [])
    decisions = result.get("decisions", [])

    def _rank_key(r):
        v = r.get("rank_ic_t")
        return v if isinstance(v, (int, float)) else float("-inf")

    ranked = sorted([r for r in results
                     if isinstance(r.get("rank_ic_t"), (int, float))],
                    key=_rank_key, reverse=True)
    strongest = _one_line(ranked[0]) if ranked else None
    weakest = _one_line(ranked[-1]) if ranked else None
    dec_counts: dict[str, int] = {}
    for d in decisions:
        dec_counts[d["decision"]] = dec_counts.get(d["decision"], 0) + 1
    next_action = _next_action(result)
    bench = None
    if ranked:
        top = ranked[0]
        bench = {"benchmark": top.get("benchmark_name"),
                 "excess_annualized": top.get("benchmark_excess_annualized"),
                 "champion_complementarity": top.get(
                     "champion_complementarity")}
    cost = None
    if ranked:
        cs = ranked[0].get("cost_sensitivity") or {}
        cost = {"flips_sign": ranked[0].get("cost_flips_sign"),
                "erosion_ratio": ranked[0].get("cost_erosion_ratio"),
                "grid": cs.get("grid")}
    return {
        "run_id": result.get("run_id"),
        "terminal": result.get("terminal"),
        "status": result.get("status"),
        "hypotheses_considered": c.get("hypotheses_considered", 0),
        "grounded_hypotheses": c.get("grounded_hypotheses", 0),
        "experiments_started": c.get("specs_generated", 0),
        "experiments_completed": c.get("experiments_completed", 0),
        "experiments_failed": c.get("experiments_failed", 0),
        "data_gaps": c.get("data_gaps", 0),
        "duplicates_rejected": c.get("duplicates_rejected", 0),
        "keep_for_research": c.get("keep_for_research", 0),
        "decision_counts": dec_counts,
        "strongest": strongest,
        "weakest": weakest,
        "benchmark_comparison": bench,
        "cost_sensitivity": cost,
        "next_action": next_action,
        "evidence_paths": [
            {"label": "Stage 5 run dir", "path": result.get("run_dir")},
        ],
        "champion_model": result.get("champion_model"),
    }


def _one_line(r: dict) -> dict:
    return {"hypothesis_id": r.get("hypothesis_id"),
            "template": r.get("template"),
            "rank_ic_t": r.get("rank_ic_t"),
            "rank_ic_mean": r.get("rank_ic_mean"),
            "net_annualized_return": r.get("net_annualized_return")}


def _next_action(result: dict) -> str:
    c = result.get("counts", {})
    if result.get("terminal") == ec.BLOCKED:
        return "Resolve source-package / integrity blocker before next cycle."
    if c.get("keep_for_research"):
        return ("Human review of %d KEEP_FOR_RESEARCH result(s); no promotion "
                "is automatic." % c["keep_for_research"])
    if c.get("data_gaps"):
        return ("Acquire or backfill the held datasets; %d hypothesis/-es await "
                "point-in-time data." % c["data_gaps"])
    if c.get("grounded_hypotheses", 0) == 0:
        return "Await new grounded Stage 3 hypotheses; nothing to experiment."
    return "Continue bounded experimentation next cycle."
