"""
alpha_agent.evidence_observatory — Stage 7 shared contracts + read-only evidence
observatory model.

This module hosts:

  * The Stage 7 (Alpha Recovery) contract surface: terminal tokens, CLI modes,
    the single-outcome recovery-disposition vocabulary, deterministic run
    identifiers, the required per-run package file list, config loading +
    secret-scan validation, and the deterministic future-promotion checklist.
  * A deterministic, strictly READ-ONLY evidence-inventory model that scans the
    Stage 1..7 output roots (registry, ingestion, director, news_rss,
    experiments, backfill, recovery) plus injected operational paper-book
    context and produces one JSON-serialisable model for the "Alpha Agent
    Evidence Observatory" surfaced in Research & Audit and the Stage 4 report.

Hard safety invariants (never relaxed):
  * Nothing here creates an order / fill / signal / trade decision, promotes a
    model, mutates any portfolio / Alpha Paper Book / operational ledger, writes
    Paper Trader PostgreSQL, calls the prediction service or a Daily Close, runs
    the LLM, executes LLM-authored code, or installs a package.
  * Scanning is read-only: it reads immutable stage packages (``latest.json`` +
    per-run files) and never writes into any stage root here.
  * No credential value is ever read, stored or printed; configs may reference
    environment-variable NAMES but never a secret. ``load_config`` scans for and
    rejects any embedded secret.

Pure Python standard library only.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from .runtime_contracts import (canonical_json, coerce_iterable,  # noqa: F401
                                redact, scan_for_secrets)

STAGE7_STAGE = "7"
STAGE7_SCHEMA_VERSION = "1.0.0"
STAGE7_ENGINE_VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# Terminal tokens (exactly one printed per CLI invocation).
# --------------------------------------------------------------------------- #
READY = "ALPHA_AGENT_STAGE7_READY"
PARTIAL = "ALPHA_AGENT_STAGE7_PARTIAL"
VERIFIED = "ALPHA_AGENT_STAGE7_VERIFIED"
DRY_RUN = "ALPHA_AGENT_STAGE7_DRY_RUN"
NEED_MORE_EVIDENCE_TERMINAL = "ALPHA_AGENT_STAGE7_NEED_MORE_EVIDENCE"
BLOCKED = "ALPHA_AGENT_STAGE7_BLOCKED"

TERMINAL_TOKENS = frozenset({
    READY, PARTIAL, VERIFIED, DRY_RUN, NEED_MORE_EVIDENCE_TERMINAL, BLOCKED})

MODE_RECOVERY = "recovery"
MODE_VERIFY = "verify"
MODE_OBSERVATORY = "observatory"
MODE_DRY_RUN = "dry-run"
MODES = (MODE_RECOVERY, MODE_VERIFY, MODE_OBSERVATORY, MODE_DRY_RUN)

# --------------------------------------------------------------------------- #
# The single primary outcome (recovery disposition).
# --------------------------------------------------------------------------- #
DISP_CONFIRMED_RISK_OVERLAY = "CHAMPION_CONFIRMED_RISK_OVERLAY_REQUIRED"
DISP_CONFIRMED_NO_CHANGE = "CHAMPION_CONFIRMED_NO_CHANGE"
DISP_REJECTED = "CHAMPION_REJECTED"
DISP_NEED_MORE_EVIDENCE = "NEED_MORE_EVIDENCE"
RECOVERY_DISPOSITIONS = frozenset({
    DISP_CONFIRMED_RISK_OVERLAY, DISP_CONFIRMED_NO_CHANGE, DISP_REJECTED,
    DISP_NEED_MORE_EVIDENCE})

# --------------------------------------------------------------------------- #
# Reconstruction faithfulness classification (champion autopsy).
# --------------------------------------------------------------------------- #
RECON_EXACT = "EXACT_RECONSTRUCTION"
RECON_PARTIAL = "PARTIAL_RECONSTRUCTION"
RECON_UNVERIFIABLE = "UNVERIFIABLE_COMPONENT"
RECON_CLASSES = (RECON_EXACT, RECON_PARTIAL, RECON_UNVERIFIABLE)

# --------------------------------------------------------------------------- #
# Required immutable per-run package files (the Stage 7 output contract).
# --------------------------------------------------------------------------- #
REQUIRED_RUN_FILES = (
    "stage7_input.json",
    "evidence_inventory.json",
    "source_freshness.csv",
    "champion_reconstruction.json",
    "champion_forensics.csv",
    "risk_contributions.csv",
    "overlay_specs.jsonl",
    "overlay_results.csv",
    "overlay_subperiods.csv",
    "overlay_regimes.csv",
    "overlay_cost_sensitivity.csv",
    "alpha_campaign_plan.json",
    "alpha_experiment_results.jsonl",
    "alpha_evidence_decisions.jsonl",
    "recovery_disposition.json",
    "manual_risk_preview.json",
    "remaining_data_gaps.jsonl",
    "stage7_recovery_report.md",
    "run_manifest.json",
)

# The stage roots the observatory inventories, in canonical display order.
STAGE_KEYS = ("stage1", "stage2", "stage3", "stage3_5", "stage4", "stage5",
              "stage6", "stage7")

# Stage labels are deliberately PURE ASCII (hyphen separators, no em-dash). A
# multi-byte em-dash re-encoded on a downstream surface is exactly what produced
# the garbled "Stage 1 [garbled-dash] Research registry" seen before; ASCII
# labels make that impossible on every surface (Stage 7.2 reporting-contract
# fix; see the mojibake regression test).
_STAGE_LABEL = {
    "stage1": "Stage 1 - Research registry",
    "stage2": "Stage 2 - Source ingestion",
    "stage3": "Stage 3 - Grounded research director",
    "stage3_5": "Stage 3.5 - Official RSS / news",
    "stage4": "Stage 4 - Runtime & reports",
    "stage5": "Stage 5 - Experiment & evidence engine",
    "stage6": "Stage 6 - Historical backfill",
    "stage7": "Stage 7 - Alpha recovery",
}


# --------------------------------------------------------------------------- #
# Deterministic identifiers.
# --------------------------------------------------------------------------- #
def sha(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def config_hash(cfg: dict) -> str:
    return sha(canonical_json(cfg))[:16]


def stage7_run_id(as_of: str, cfg_hash: str, evidence_fingerprint: str,
                  schema_version: str = STAGE7_SCHEMA_VERSION,
                  engine_version: str = STAGE7_ENGINE_VERSION) -> str:
    """Deterministic Stage 7 run identity.

    The identity binds the run to (as_of, config, upstream evidence, package
    SCHEMA version, engine version). A change to the package schema or the
    engine version therefore produces a NEW run id — and hence a NEW run
    directory — so a schema/engine change can never silently overwrite an
    earlier immutable package that was written under the older schema.
    """
    return "stage7_" + sha("recovery", str(as_of), str(cfg_hash),
                           str(evidence_fingerprint),
                           str(schema_version), str(engine_version))[:16]


def evidence_fingerprint(inventory: dict) -> str:
    """Fingerprint the immutable evidence the recovery run consumed (the latest
    run id of every upstream stage) so the run id changes only when the evidence
    changes."""
    keyed = {k: (inventory.get("stages", {}).get(k, {}) or {}).get("run_id")
             for k in STAGE_KEYS if k != "stage7"}
    return sha(canonical_json(keyed))[:16]


# --------------------------------------------------------------------------- #
# Config loading + validation.
# --------------------------------------------------------------------------- #
_REQUIRED_CONFIG_KEYS = (
    "stage", "recovery_root", "stage_roots", "operational_ledger_roots",
    "champion", "bounds", "overlays", "promotion_gate",
)


class ConfigError(ValueError):
    """Raised when the Stage 7 config is missing or unsafe."""


def load_config(path: str | Path) -> dict:
    """Load, secret-scan and structurally validate the Stage 7 config."""
    p = Path(path)
    try:
        raw = p.read_text(encoding="utf-8-sig")
    except OSError as exc:
        raise ConfigError("cannot read config %s: %s" % (p, exc)) from exc
    try:
        cfg = json.loads(raw)
    except ValueError as exc:
        raise ConfigError("config %s is not valid JSON: %s" % (p, exc)) from exc
    if not isinstance(cfg, dict):
        raise ConfigError("config root must be a JSON object")

    problems = scan_for_secrets(cfg)
    if problems:
        raise ConfigError("config contains a secret value: %s" %
                          "; ".join(problems))

    missing = [k for k in _REQUIRED_CONFIG_KEYS if k not in cfg]
    if missing:
        raise ConfigError("config missing required keys: %s" %
                          ", ".join(missing))
    if str(cfg.get("stage")) != STAGE7_STAGE:
        raise ConfigError("config stage must be '%s'" % STAGE7_STAGE)
    if not isinstance(cfg.get("stage_roots"), dict):
        raise ConfigError("stage_roots must be an object")
    if not isinstance(cfg.get("champion"), dict):
        raise ConfigError("champion must be an object")
    return cfg


def bound(cfg: dict, key: str, default: Any) -> Any:
    return (cfg.get("bounds") or {}).get(key, default)


# --------------------------------------------------------------------------- #
# Deterministic future-promotion checklist (Workstream 5).
#
# Nothing is ever promoted in Stage 7. This produces the objective, ordered set
# of requirements a future phase must satisfy before it may create even a SHADOW
# challenger. Each requirement is evaluated against the recovery evidence; the
# gate as a whole passes only when every requirement is met.
# --------------------------------------------------------------------------- #
PROMOTION_REQUIREMENTS = (
    ("positive_out_of_sample", "Positive out-of-sample evidence on owned, "
     "point-in-time-safe data (rolling OOS window IC/return positive)."),
    ("net_of_cost_improvement", "Net-of-transaction-cost improvement over the "
     "current control and the passive benchmark."),
    ("acceptable_drawdown", "Maximum drawdown within the configured risk floor."),
    ("stability_periods_regimes", "Stability across subperiods and market "
     "regimes (no single-regime dependence)."),
    ("adequate_breadth", "Adequate breadth (universe / observations / periods "
     "above the evidence-gate minimums)."),
    ("low_concentration", "Acceptable name and sector concentration; no single "
     "name or sector dominates risk."),
    ("no_leakage", "No point-in-time leakage; every factor uses only data "
     "available at formation (no period-end / report-date lookahead)."),
    ("forward_shadow_evaluation", "A successful FORWARD shadow evaluation over "
     "a genuine out-of-sample forward window (not yet run)."),
)


def promotion_checklist(evidence: Optional[dict] = None) -> dict:
    """Deterministic promotion-gate checklist. ``evidence`` (optional) maps a
    requirement key to a bool/None; unknown or missing keys are ``met=None``
    (not yet demonstrated). The gate passes only when every requirement is
    explicitly True. Purely advisory — Stage 7 promotes nothing."""
    ev = evidence or {}
    items = []
    all_met = True
    for key, text in PROMOTION_REQUIREMENTS:
        raw = ev.get(key)
        met = True if raw is True else (False if raw is False else None)
        if met is not True:
            all_met = False
        items.append({"key": key, "requirement": text, "met": met})
    return {
        "gate": "FUTURE_PROMOTION_CHECKLIST",
        "promotion_allowed_now": False,
        "shadow_challenger_allowed_now": bool(all_met),
        "all_requirements_met": bool(all_met),
        "requirements": items,
        "note": ("Nothing is promoted in this phase. The next phase may create a "
                 "SHADOW challenger only when every requirement below is met; a "
                 "live promotion additionally requires successful forward shadow "
                 "evidence."),
    }


# --------------------------------------------------------------------------- #
# Read-only per-stage evidence readers.
# --------------------------------------------------------------------------- #
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
        if line:
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return out


def _read_csv(path: Path) -> list[dict]:
    import csv
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except OSError:
        return []


def _latest_run_dir(root: Path) -> tuple[Optional[str], Optional[Path], dict]:
    """Resolve the latest run of a stage root from ``latest.json`` (preferred)
    or the newest ``runs/<id>`` directory. Returns (run_id, run_dir, latest)."""
    latest = _read_json(root / "latest.json") or {}
    rid = latest.get("run_id")
    runs = root / "runs"
    if rid:
        rd = runs / rid
        if rd.exists():
            return rid, rd, latest
    if runs.exists():
        candidates = sorted((d for d in runs.iterdir() if d.is_dir()),
                            key=lambda d: d.name)
        if candidates:
            rd = candidates[-1]
            return rd.name, rd, latest
    return (rid or None), None, latest


def _mtime_iso(path: Path) -> Optional[str]:
    try:
        import datetime as _dt
        ts = path.stat().st_mtime
        return _dt.datetime.fromtimestamp(
            ts, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except OSError:
        return None


def _base(status: str, root: Path, **extra: Any) -> dict:
    out = {"status": status, "root": str(root), "run_id": None,
           "run_dir": None, "as_of": None}
    out.update(extra)
    return out


def _stage1(root: Path) -> dict:
    if not root.exists():
        return _base("UNAVAILABLE", root, note="registry root not present")
    # Registry is a local sqlite; report presence + freshness read-only (no
    # schema coupling). Any latest.json / manifest counts are surfaced if
    # present.
    dbs = sorted(root.glob("*.sqlite")) + sorted(root.glob("*.db"))
    latest = _read_json(root / "latest.json") or {}
    freshest = None
    for f in list(dbs) + [root / "latest.json"]:
        if f.exists():
            m = _mtime_iso(f)
            if m and (freshest is None or m > freshest):
                freshest = m
    return _base("PRESENT" if (dbs or latest) else "EMPTY", root,
                 run_id=latest.get("run_id"), as_of=freshest,
                 registry_files=len(dbs),
                 note="research registry (read-only)")


def _stage2(root: Path) -> dict:
    if not root.exists():
        return _base("UNAVAILABLE", root, note="ingestion root not present")
    rid, rd, latest = _latest_run_dir(root)
    families: list[dict] = []
    # Prefer the most recent Stage 6 coverage snapshot if it lives here; else a
    # stage2 package coverage file.
    cov: list[dict] = []
    if rd is not None:
        for name in ("coverage_after.csv", "normalized_record_counts.csv",
                     "data_coverage.csv"):
            cov = _read_csv(rd / name)
            if cov:
                break
    for r in cov:
        rc_ = r.get("record_count") or r.get("records") or 0
        try:
            rc_ = int(rc_)
        except (TypeError, ValueError):
            rc_ = 0
        if not rc_:
            continue
        families.append({
            "family": r.get("family") or r.get("record_type"),
            "provider": r.get("provider"),
            "record_count": rc_,
            "date_min": r.get("min_effective_date") or r.get("date_min"),
            "date_max": r.get("max_effective_date") or r.get("date_max"),
            "tickers": r.get("unique_tickers") or r.get("tickers"),
        })
    total = sum(f["record_count"] for f in families)
    return _base("PRESENT" if rid else "EMPTY", root, run_id=rid,
                 run_dir=str(rd) if rd else None,
                 as_of=(latest.get("as_of") or (_mtime_iso(rd) if rd else None)),
                 normalized_record_total=total, families=families)


def _stage3(root: Path) -> dict:
    if not root.exists():
        return _base("UNAVAILABLE", root, note="director root not present")
    rid, rd, latest = _latest_run_dir(root)
    events = analyzed = hyps = 0
    if rd is not None:
        events = len(_read_jsonl(rd / "structured_event_analysis.jsonl"))
        analyzed = events
        hp = _read_json(rd / "hypothesis_proposals.json")
        if isinstance(hp, dict):
            hyps = len(hp.get("hypotheses") or hp.get("proposals") or [])
        elif isinstance(hp, list):
            hyps = len(hp)
    return _base("PRESENT" if rid else "EMPTY", root, run_id=rid,
                 run_dir=str(rd) if rd else None,
                 as_of=(_mtime_iso(rd) if rd else None),
                 events_analyzed=analyzed, hypotheses_proposed=hyps)


def _stage3_5(root: Path) -> dict:
    if not root.exists():
        return _base("UNAVAILABLE", root, note="news_rss root not present")
    rid, rd, latest = _latest_run_dir(root)
    records = clusters = 0
    feeds_healthy = feeds_total = None
    if rd is not None:
        man = _read_json(rd / "run_manifest.json") or {}
        records = (man.get("records") or man.get("record_count")
                   or len(_read_jsonl(rd / "official_records.jsonl")))
        clusters = (man.get("clusters") or man.get("cluster_count")
                    or len(_read_jsonl(rd / "event_clusters.jsonl")))
        health = _read_csv(rd / "feed_health.csv")
        if health:
            feeds_total = len(health)
            # The feed-health package column is "health" with values HEALTHY /
            # HEALTHY_NOT_MODIFIED (a 304-not-modified feed IS healthy) / and
            # failure states like CIRCUIT_OPEN. Older packages used "status".
            # Reading the wrong column previously yielded a false 0 healthy that
            # disagreed with the canonical runtime count (Stage 7.2 fix).
            def _feed_ok(r: dict) -> bool:
                val = str(r.get("health") or r.get("status") or "").upper()
                return val.startswith("HEALTHY") or val in ("OK", "PRESENT")
            feeds_healthy = sum(1 for r in health if _feed_ok(r))
    return _base("PRESENT" if rid else "EMPTY", root, run_id=rid,
                 run_dir=str(rd) if rd else None,
                 as_of=(_mtime_iso(rd) if rd else None),
                 official_records=int(records or 0),
                 clusters=int(clusters or 0),
                 feeds_healthy=feeds_healthy, feeds_total=feeds_total)


def _stage4(root: Path) -> dict:
    if not root.exists():
        return _base("UNAVAILABLE", root, note="runtime root not present")
    reports = root / "reports"
    freshest = _mtime_iso(reports) if reports.exists() else _mtime_iso(root)
    return _base("PRESENT", root, as_of=freshest,
                 note="runtime & report store (read-only)")


def _stage5(root: Path) -> dict:
    if not root.exists():
        return _base("UNAVAILABLE", root, note="experiments root not present")
    rid, rd, latest = _latest_run_dir(root)
    completed = failed = keep = 0
    decisions: dict[str, int] = {}
    if rd is not None:
        metrics = _read_csv(rd / "evidence_metrics.csv")
        completed = len(metrics)
        for d in _read_jsonl(rd / "evidence_decisions.jsonl"):
            dec = str(d.get("decision") or "")
            decisions[dec] = decisions.get(dec, 0) + 1
            if dec == "KEEP_FOR_RESEARCH":
                keep += 1
        failed = len(_read_jsonl(rd / "data_gaps.jsonl"))
    return _base("PRESENT" if rid else "EMPTY", root, run_id=rid,
                 run_dir=str(rd) if rd else None,
                 as_of=(_mtime_iso(rd) if rd else None),
                 experiments_completed=completed, keep_for_research=keep,
                 decisions=decisions, data_holds=failed)


def _stage6(root: Path) -> dict:
    if not root.exists():
        return _base("UNAVAILABLE", root, note="backfill root not present")
    rid, rd, latest = _latest_run_dir(root)
    unlocked = latest.get("templates_unlocked") or []
    records = latest.get("records_written")
    gaps = 0
    date_start = date_end = None
    if rd is not None:
        man = _read_json(rd / "run_manifest.json") or {}
        records = records or man.get("records_written")
        date_start = man.get("date_start")
        date_end = man.get("date_end")
        # The manifest often omits the window; the immutable stage6_input.json
        # carries the authoritative date_start/date_end. Never fabricated — this
        # kills the null date_start/date_end defect (Stage 7.2).
        if not date_start or not date_end:
            si = _read_json(rd / "stage6_input.json") or {}
            date_start = date_start or si.get("date_start")
            date_end = date_end or si.get("date_end")
        gaps = len(_read_jsonl(rd / "unresolved_data_gaps.jsonl"))
        if not unlocked:
            unlocked = man.get("templates_unlocked") or []
    return _base("PRESENT" if rid else "EMPTY", root, run_id=rid,
                 run_dir=str(rd) if rd else None,
                 as_of=(latest.get("finished_at") or (_mtime_iso(rd) if rd
                        else None)),
                 records_written=records, templates_unlocked=list(unlocked),
                 remaining_data_gaps=gaps, date_start=date_start,
                 date_end=date_end)


def _stage7(root: Path, recovery_context: Optional[dict]) -> dict:
    if recovery_context:
        rc_ = dict(recovery_context)
        rc_.setdefault("status", "PRESENT")
        rc_.setdefault("root", str(root))
        return rc_
    if not root.exists():
        return _base("NONE_YET", root, note="no recovery run yet")
    rid, rd, latest = _latest_run_dir(root)
    disp = None
    evaluated = None
    if rd is not None:
        disp_doc = _read_json(rd / "recovery_disposition.json") or {}
        disp = disp_doc.get("disposition")
        # The number of recovery ideas actually evaluated == the immutable
        # per-experiment result rows (one row per evaluated experiment). Read
        # from the package so the observatory never leaves the count null
        # (Stage 7.2 live-wiring fix); never fabricated.
        results = _read_jsonl(rd / "alpha_experiment_results.jsonl")
        if results:
            evaluated = len(results)
    return _base("PRESENT" if rid else "NONE_YET", root, run_id=rid,
                 run_dir=str(rd) if rd else None,
                 as_of=(latest.get("finished_at") or (_mtime_iso(rd) if rd
                        else None)),
                 recovery_disposition=disp,
                 campaign_experiments=evaluated)


_STAGE_READERS = {
    "stage1": lambda root, ctx: _stage1(root),
    "stage2": lambda root, ctx: _stage2(root),
    "stage3": lambda root, ctx: _stage3(root),
    "stage3_5": lambda root, ctx: _stage3_5(root),
    "stage4": lambda root, ctx: _stage4(root),
    "stage5": lambda root, ctx: _stage5(root),
    "stage6": lambda root, ctx: _stage6(root),
    "stage7": lambda root, ctx: _stage7(root, ctx),
}


def _resolve_root(cfg: dict, key: str) -> Path:
    roots = cfg.get("stage_roots") or {}
    val = roots.get(key)
    if key == "stage7":
        val = val or cfg.get("recovery_root")
    return Path(val) if val else Path("__missing__%s" % key)


def _stale(as_of: Optional[str], today: Optional[str], days: int) -> bool:
    if not as_of or not today:
        return False
    try:
        import datetime as _dt
        a = _dt.date.fromisoformat(str(as_of)[:10])
        t = _dt.date.fromisoformat(str(today)[:10])
        return (t - a).days > days
    except (ValueError, TypeError):
        return False


def _default_schedule_status() -> str:
    """Lazy-import the canonical unverified schedule label (avoids an import
    cycle at module load)."""
    try:
        from . import report_renderer as _rr
        return _rr.SCHEDULE_UNVERIFIED
    except Exception:  # noqa: BLE001
        return "Not verified"


def build_evidence_inventory(cfg: dict, *,
                             recovery_context: Optional[dict] = None,
                             book_context: Optional[dict] = None,
                             today: Optional[str] = None,
                             schedule_status: Optional[str] = None,
                             market_data_through: Optional[str] = None) -> dict:
    """Deterministic, read-only evidence-inventory model spanning Stage 1..7.

    ``recovery_context`` (optional) is a fresh Stage 7 recovery summary to embed
    directly; otherwise the latest recovery package on disk is read.
    ``schedule_status`` is the resolved automatic-research-schedule state (one
    canonical source shared with the email/UI); when omitted it is 'Not
    verified', never a false 'On'. Never writes; never mutates any stage root."""
    freshness_days = int((cfg.get("freshness") or {}).get("stale_after_days", 7))
    stages: dict[str, dict] = {}
    for key in STAGE_KEYS:
        root = _resolve_root(cfg, key)
        try:
            block = _STAGE_READERS[key](root, recovery_context)
        except Exception as exc:  # noqa: BLE001 — never let one stage break all
            block = _base("ERROR", root, error=redact(str(exc))[:200])
        block["label"] = _STAGE_LABEL[key]
        block["stale"] = _stale(block.get("as_of"), today, freshness_days)
        stages[key] = block

    champion = dict(cfg.get("champion") or {})
    # Promote the last-known operational context (from config) to top-level
    # champion fields so the observatory always shows a value; a live read
    # (book_context) overrides these. These defaults are explicitly config-
    # sourced, never fabricated.
    _ctx_keys = ("nav", "cumulative_pnl", "cumulative_pnl_pct", "holdings_count",
                 "invested_pct", "spy_cumulative_return", "excess_vs_spy_pct",
                 "portfolio_realized_vol", "risk_checks_status")
    defaults = champion.get("operational_context_defaults") or {}
    for k in _ctx_keys:
        if champion.get(k) is None and defaults.get(k) is not None:
            champion[k] = defaults[k]
    if book_context:
        champion = {**champion, **{k: v for k, v in book_context.items()
                                   if k in _ctx_keys and v is not None}}

    s6 = stages.get("stage6", {})
    s5 = stages.get("stage5", {})
    s3 = stages.get("stage3", {})
    s2 = stages.get("stage2", {})
    s7 = stages.get("stage7", {})

    # Normalize the operational paper book so the critical fields are ALWAYS
    # present keys (populated from the live book context when evidence exists,
    # never fabricated) — this kills the null champion_model / book_name /
    # holdings_count / invested_pct defects (Stage 7.2).
    opb_in = dict(book_context or {})
    market_through = (opb_in.get("market_data_through")
                      or opb_in.get("nav_as_of_date")
                      or opb_in.get("valuation_date") or market_data_through)
    operational_paper_book = {
        **opb_in,
        "book_name": opb_in.get("book_name"),
        "champion_model": opb_in.get("champion_model") or champion.get(
            "champion_model") or champion.get("strategy_name"),
        "nav": opb_in.get("nav") if opb_in.get("nav") is not None
        else champion.get("nav"),
        "holdings_count": opb_in.get("holdings_count"),
        "invested_pct": opb_in.get("invested_pct"),
        "market_data_through": market_through,
    }

    # Counters are kept STRICTLY separate and labelled by scope so unrelated
    # totals are never merged under one heading (Stage 7.2 reporting-contract
    # fix). Stage 2 normalized ingestion, Stage 6 historical backfill, Stage 5
    # experiments completed and Stage 7 recovery ideas evaluated are distinct.
    s7_evaluated = ((recovery_context or {}).get("campaign_experiments")
                    if recovery_context else s7.get("campaign_experiments"))
    inv = {
        "schema_version": STAGE7_SCHEMA_VERSION,
        "engine_version": STAGE7_ENGINE_VERSION,
        "generated_for": "alpha_agent_evidence_observatory",
        "today": today,
        "market_data_through": market_through,
        "schedule_status": schedule_status or _default_schedule_status(),
        "stages": stages,
        "highlights": {
            # Back-compat label -> the Stage 6 historical-backfill record count
            # (the large survivorship-safe bar count), NOT a merge of scopes.
            "data_collected_records": s6.get("records_written"),
            "analyses_completed": s3.get("events_analyzed"),
            "experiments_completed": s5.get("experiments_completed"),
            "weak_ideas_rejected": sum(
                v for k, v in (s5.get("decisions") or {}).items()
                if str(k).startswith("REJECT")),
            # Stage 7 recovery ideas evaluated (distinct from Stage 5 experiments
            # completed) — surfaced non-null when a recovery package exists.
            "recovery_experiments_evaluated": s7_evaluated,
            "data_gaps": s6.get("remaining_data_gaps"),
            "templates_unlocked": s6.get("templates_unlocked") or [],
        },
        # Distinct, scope-labelled counters (never conflated).
        "counter_breakdown": {
            "stage2_normalized_records": s2.get("normalized_record_total"),
            "stage6_backfill_records": s6.get("records_written"),
            "stage5_experiments_completed": s5.get("experiments_completed"),
            "stage5_data_holds": s5.get("data_holds"),
            "stage7_recovery_experiments_evaluated": s7_evaluated,
        },
        "champion": champion,
        "recovery_disposition": (s7.get("recovery_disposition")
                                 or (recovery_context or {}).get(
                                     "recovery_disposition")),
        "operational_paper_book": operational_paper_book,
        "safety_badges": [
            "NO MODEL PROMOTION", "NO ORDERS", "PAPER ONLY",
            "READ-ONLY EVIDENCE", "AUTOMATION OFF",
        ],
        "evidence_paths": [
            {"stage": _STAGE_LABEL[k], "run_id": stages[k].get("run_id"),
             "path": stages[k].get("run_dir") or stages[k].get("root")}
            for k in STAGE_KEYS
        ],
    }
    return inv


def source_freshness_rows(inventory: dict, *, today: Optional[str] = None
                          ) -> list[dict]:
    """Deterministic per-stage freshness rows for ``source_freshness.csv``."""
    rows = []
    for key in STAGE_KEYS:
        b = (inventory.get("stages") or {}).get(key) or {}
        rows.append({
            "stage": key,
            "label": b.get("label") or _STAGE_LABEL.get(key, key),
            "status": b.get("status"),
            "run_id": b.get("run_id"),
            "as_of": b.get("as_of"),
            "stale": b.get("stale"),
            "path": b.get("run_dir") or b.get("root"),
        })
    return rows


def canonical_feed_health(book_context: Optional[dict],
                          stage3_5_block: Optional[dict] = None) -> dict:
    """ONE source of feed-health counts for the email, API and UI. Prefers the
    counts the runtime already computed for the email (carried on the book
    context) so every surface agrees; falls back to the Stage 3.5 package read."""
    bc = book_context or {}
    nr = bc.get("news_rss") or {}
    healthy = nr.get("healthy")
    enabled = nr.get("enabled")
    if healthy is None and enabled is None:
        s = stage3_5_block or {}
        healthy = s.get("feeds_healthy")
        enabled = s.get("feeds_total")
    return {"healthy": healthy, "enabled": enabled}


def observatory_payload(cfg: Optional[dict] = None, *,
                        config_path: Optional[str | Path] = None,
                        recovery_context: Optional[dict] = None,
                        book_context: Optional[dict] = None,
                        today: Optional[str] = None,
                        schedule_status: Optional[str] = None,
                        market_data_through: Optional[str] = None) -> dict:
    """Read-only payload for the API endpoint. Loads the Stage 7 config if only a
    path is given; returns a controlled empty-state payload (never raises) when
    the config is missing so the endpoint can always return HTTP 200.

    ``schedule_status`` / ``market_data_through`` come from the SAME canonical
    runtime source the email uses, so no surface hardcodes 'ON' or disagrees."""
    if cfg is None and config_path is not None:
        try:
            cfg = load_config(config_path)
        except ConfigError as exc:
            return {"status": "UNAVAILABLE",
                    "reason": redact(str(exc))[:200],
                    "stages": {}, "champion": {},
                    "schedule_status": schedule_status
                    or _default_schedule_status(),
                    "safety_badges": ["NO ORDERS", "PAPER ONLY",
                                      "READ-ONLY EVIDENCE"]}
    if cfg is None:
        return {"status": "UNAVAILABLE", "reason": "no Stage 7 config",
                "stages": {}, "champion": {},
                "schedule_status": schedule_status or _default_schedule_status(),
                "safety_badges": ["NO ORDERS", "PAPER ONLY",
                                  "READ-ONLY EVIDENCE"]}
    inv = build_evidence_inventory(cfg, recovery_context=recovery_context,
                                   book_context=book_context, today=today,
                                   schedule_status=schedule_status,
                                   market_data_through=market_data_through)
    inv["status"] = "OK"

    # Canonical, reconciled presentation blocks. The scorecard is produced by the
    # SAME deterministic formatter the email uses, so the API, UI and email render
    # byte-identical values for identical inputs. Feed-health and schedule state
    # come from single canonical sources shared with the email.
    from . import report_renderer as _rr  # lazy: no import cycle, no cost.
    inv["status_flags"] = list(_rr.STATUS_FLAGS)
    inv["scorecard"] = _rr.scorecard(book_context or {})
    _fh = canonical_feed_health(
        book_context, (inv.get("stages") or {}).get("stage3_5"))
    inv["news_rss_health"] = _fh
    # Top-level source-health contract (feeds_healthy / feeds_total) from the ONE
    # canonical feed-health source shared with the email and UI, so no surface
    # disagrees and the count is never left null when the runtime has it.
    inv["source_health"] = {"feeds_healthy": _fh.get("healthy"),
                            "feeds_total": _fh.get("enabled")}
    try:
        from . import risk_overlay_research as _ro
        rec_root = cfg.get("recovery_root") or (cfg.get("stage_roots")
                                                or {}).get("stage7")
        inv["forward_shadows"] = _ro.build_forward_shadows(
            {"recovery_root": rec_root},
            forward_series=(book_context or {}).get("forward_series") or [],
            baseline_date=(book_context or {}).get("forward_baseline_date"))
    except Exception as exc:  # noqa: BLE001 — read-only best effort.
        inv["forward_shadows"] = {"status": "UNAVAILABLE",
                                  "reason": redact(str(exc))[:120],
                                  "shadows": []}
    return inv


def all_terminal_tokens() -> list[str]:
    return sorted(TERMINAL_TOKENS)
