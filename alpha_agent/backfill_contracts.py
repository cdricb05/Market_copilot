"""
alpha_agent.backfill_contracts — Stage 6 historical-backfill & readiness contracts.

Pure, side-effect-free definitions shared by the Stage 6 historical-backfill
engine, its CLI, the Stage 4 report integration and the tests: terminal tokens,
the normalized-family <-> record-type mapping, the provider->family plan, the
deterministic experiment-readiness gates, the deterministic coverage-matrix
column contract, the Stage 1 reopening dispositions + family->template map, the
persistent backfill-state database schema, the required output-file contract,
deterministic identifiers, and config loading + validation + the secret-scan
guard.

Safety invariants enforced/relied on here (never relaxed downstream):
  * Stage 6 is ACQUISITION + READINESS only. Nothing in this module can create
    an order/fill/signal/trade-decision/model promotion/portfolio change/Daily
    Close/PostgreSQL write/prediction call. There is no network, DB, subprocess,
    LLM or clock read here.
  * No credential value is ever read, stored or printed; configs name only
    environment-variable NAMES. ``load_config`` scans for and rejects any
    embedded secret.
  * Point-in-time discipline is a first-class contract: ``available_at`` is only
    ever preserved as observed and NEVER fabricated (a genuinely-unknown
    availability stays null and is counted, never guessed); a fiscal period-end
    is NEVER treated as a public-availability date. The readiness gates refuse to
    mark a fundamental/earnings family usable unless real availability timestamps
    are present.

Standard library only (no numpy/pandas) so results are byte-reproducible.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
from pathlib import Path
from typing import Any, Optional

from .runtime_contracts import (canonical_json, redact,  # noqa: F401
                                scan_for_secrets)
from .source_contracts import (RT_CORPORATE_ACTION, RT_EARNINGS_EVENT,
                                RT_FILING_EVENT, RT_FUNDAMENTAL_FACT,
                                RT_INSIDER_FILING, RT_MACRO_OBSERVATION,
                                RT_MARKET_BAR, RT_NEWS_EVENT, RT_SECURITY_IDENTITY,
                                RT_SHORT_VOLUME, RT_UNIVERSE_MEMBERSHIP)

STAGE6_STAGE = "6"
STAGE6_SCHEMA_VERSION = "1.0.0"
STAGE6_ENGINE_VERSION = "1.0.0"
BACKFILL_CONTRACT_VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# Terminal tokens (exactly one printed per CLI invocation).
# --------------------------------------------------------------------------- #
READY = "ALPHA_AGENT_STAGE6_READY"
VERIFIED = "ALPHA_AGENT_STAGE6_VERIFIED"
PARTIAL = "ALPHA_AGENT_STAGE6_PARTIAL"
DATA_HOLD = "ALPHA_AGENT_STAGE6_DATA_HOLD"
DRY_RUN = "ALPHA_AGENT_STAGE6_DRY_RUN"
NO_NEW_DATA = "NO_NEW_HISTORICAL_DATA"
BLOCKED = "ALPHA_AGENT_STAGE6_BLOCKED"

TERMINAL_TOKENS = frozenset({
    READY, VERIFIED, PARTIAL, DATA_HOLD, DRY_RUN, NO_NEW_DATA, BLOCKED})

MODES = ("dry-run", "backfill", "verify", "coverage-report")

# --------------------------------------------------------------------------- #
# Logical acquisition families -> normalized record types + the provider that
# supplies each. Priority 1 = Norgate (local, survivorship-aware). Priority 2 =
# entitled/official providers. The engine resolves the plan from this map; a
# family with no owned provider is honestly reported as a gap, never faked.
# --------------------------------------------------------------------------- #
FAMILY_RECORD_TYPE: dict[str, str] = {
    "prices": RT_MARKET_BAR,
    "universe_membership": RT_UNIVERSE_MEMBERSHIP,
    "security_identity": RT_SECURITY_IDENTITY,
    "corporate_actions": RT_CORPORATE_ACTION,
    "fundamentals": RT_FUNDAMENTAL_FACT,
    "earnings": RT_EARNINGS_EVENT,
    "filings": RT_FILING_EVENT,
    "insider": RT_INSIDER_FILING,
    "news": RT_NEWS_EVENT,
    "short_volume": RT_SHORT_VOLUME,
    "macro": RT_MACRO_OBSERVATION,
}

# Which provider owns each family (priority 1 = Norgate local; 2 = entitled).
PROVIDER_FAMILIES: dict[str, dict] = {
    "norgate_local": {"priority": 1, "families": [
        "prices", "universe_membership", "security_identity",
        "corporate_actions"]},
    "eodhd": {"priority": 2, "families": [
        "fundamentals", "earnings", "insider", "news", "corporate_actions"]},
    "sec_edgar": {"priority": 2, "families": ["filings", "insider"]},
    "finra": {"priority": 2, "families": ["short_volume"]},
    "fred_alfred": {"priority": 2, "families": ["macro"]},
}

# --------------------------------------------------------------------------- #
# Deterministic experiment-readiness gates (configurable via cfg["readiness"]).
# A family is only marked usable for a template class when EVERY minimum is met
# AND the point-in-time / survivorship guards hold. These map directly to the
# Stage 5 template allowlist so that "templates unlocked" is a truthful function
# of acquired coverage, never an assumption.
# --------------------------------------------------------------------------- #
READINESS_GATES: dict[str, dict] = {
    "price_momentum": {
        "min_years": 5.0, "min_pit_members": 300, "min_monthly_periods": 36,
        "require_survivorship_safe": True,
        "families": ["prices"],
        "unlocks": ["price_momentum_rank"],
    },
    "sector_relative": {
        "min_years": 5.0, "min_pit_members": 300, "min_monthly_periods": 36,
        "require_survivorship_safe": True,
        "families": ["prices", "sector_map"],
        "unlocks": ["sector_relative_ranking"],
    },
    "fundamental_momentum": {
        "min_years": 5.0, "min_quarters": 16, "min_securities": 200,
        "require_availability_timestamps": True, "forbid_period_end_lookahead": True,
        "families": ["prices", "fundamentals"],
        "unlocks": ["fundamental_momentum_rank", "combined_factor_ranking"],
    },
    "earnings_drift": {
        "min_events": 200, "require_report_timestamps": True,
        "forbid_report_date_lookahead": True, "require_forward_windows": True,
        "families": ["prices", "earnings"],
        "unlocks": ["earnings_filing_event_drift",
                    "event_window_cross_sectional_return"],
    },
    "insider_event": {
        "min_events": 200, "require_filing_timestamps": True,
        "require_forward_windows": True,
        "families": ["prices", "insider"],
        "unlocks": ["insider_activity_event_study"],
    },
    "news_event": {
        "min_events": 200, "require_publication_timestamps": True,
        "require_forward_windows": True,
        "families": ["prices", "news"],
        "unlocks": ["news_regulatory_event_study"],
    },
}


def resolved_readiness(cfg: dict) -> dict:
    """Overlay configured readiness thresholds on the defaults (numbers only)."""
    out = {k: dict(v) for k, v in READINESS_GATES.items()}
    for fam, over in (cfg.get("readiness") or {}).items():
        if fam in out and isinstance(over, dict):
            for k, v in over.items():
                if isinstance(v, (int, float, bool)) and k in out[fam]:
                    out[fam][k] = v
    return out


def evaluate_readiness(gate: dict, fam_coverage: dict) -> dict:
    """Deterministically decide whether a readiness family is satisfied.

    ``fam_coverage`` maps a logical family name to its coverage row (see
    COVERAGE_COLUMNS). Returns {"usable", "blocker", "unlocks"}. NEVER marks a
    fundamental/earnings/insider/news family usable without real availability
    timestamps — a point-in-time honesty guard.
    """
    blockers: list[str] = []
    prices = fam_coverage.get("prices") or {}

    def _years(cov: dict) -> float:
        a, b = cov.get("min_effective_date"), cov.get("max_effective_date")
        if not a or not b:
            return 0.0
        try:
            da = _dt.date.fromisoformat(str(a)[:10])
            db = _dt.date.fromisoformat(str(b)[:10])
            return (db - da).days / 365.25
        except ValueError:
            return 0.0

    if "min_years" in gate:
        yrs = _years(prices)
        if yrs < gate["min_years"]:
            blockers.append("history %.2fy < %.1fy" % (yrs, gate["min_years"]))
    if "min_pit_members" in gate:
        members = int(prices.get("unique_tickers") or 0)
        if members < gate["min_pit_members"]:
            blockers.append("members %d < %d" % (members, gate["min_pit_members"]))
    if "min_monthly_periods" in gate:
        months = int(prices.get("unique_months") or 0)
        if months < gate["min_monthly_periods"]:
            blockers.append("months %d < %d" % (months,
                                                 gate["min_monthly_periods"]))
    if gate.get("require_survivorship_safe") and not prices.get(
            "survivorship_safe"):
        blockers.append("prices not survivorship-safe")

    # Non-price families required by this gate.
    for fam in gate.get("families", []):
        if fam in ("prices",):
            continue
        cov = fam_coverage.get(fam) or {}
        rc_count = int(cov.get("record_count") or 0)
        if fam == "sector_map":
            if int(cov.get("unique_months") or 0) < gate.get(
                    "min_monthly_periods", 1):
                blockers.append("sector_map has no dated time-series coverage")
            continue
        if "min_quarters" in gate and int(cov.get("unique_quarters") or 0) < \
                gate["min_quarters"]:
            blockers.append("%s quarters %d < %d" % (
                fam, int(cov.get("unique_quarters") or 0), gate["min_quarters"]))
        if "min_securities" in gate and int(cov.get("unique_tickers") or 0) < \
                gate["min_securities"]:
            blockers.append("%s securities %d < %d" % (
                fam, int(cov.get("unique_tickers") or 0), gate["min_securities"]))
        if "min_events" in gate and rc_count < gate["min_events"]:
            blockers.append("%s events %d < %d" % (fam, rc_count,
                                                    gate["min_events"]))
        # Point-in-time availability guards (never approximate).
        needs_ts = (gate.get("require_availability_timestamps")
                    or gate.get("require_report_timestamps")
                    or gate.get("require_filing_timestamps")
                    or gate.get("require_publication_timestamps"))
        if needs_ts and not cov.get("has_availability_timestamps"):
            blockers.append("%s lacks real availability timestamps "
                            "(point-in-time unusable; period-end never "
                            "substituted)" % fam)
        if rc_count == 0:
            blockers.append("%s has no acquired records" % fam)

    usable = not blockers
    return {"usable": usable, "blocker": "; ".join(blockers) or "",
            "unlocks": list(gate.get("unlocks", [])) if usable else []}


# --------------------------------------------------------------------------- #
# Deterministic coverage-matrix column contract.
# --------------------------------------------------------------------------- #
COVERAGE_COLUMNS = [
    "family", "record_type", "provider", "min_effective_date",
    "max_effective_date", "min_available_at", "max_available_at",
    "unique_tickers", "unique_securities", "unique_dates", "unique_months",
    "unique_quarters", "record_count", "null_available_at_count",
    "null_available_at_pct", "duplicate_prevention_count",
    "has_availability_timestamps", "point_in_time_usable", "survivorship_safe",
    "templates_unlocked", "remaining_blocker",
]

# --------------------------------------------------------------------------- #
# Stage 1 reopening lane.
# --------------------------------------------------------------------------- #
REOPEN_NEW_DATA = "NEW_DATA_AVAILABLE"
REOPEN_RESUME_INCOMPLETE = "RESUME_PRIOR_INCOMPLETE"
REOPEN_STILL_HELD = "STILL_DATA_HELD"
REOPEN_REJECT_DUPLICATE = "REJECT_EXACT_DUPLICATE"
REOPEN_REJECT_UNSUPPORTED = "REJECT_UNSUPPORTED_TEMPLATE"

REOPEN_DISPOSITIONS = frozenset({
    REOPEN_NEW_DATA, REOPEN_RESUME_INCOMPLETE, REOPEN_STILL_HELD,
    REOPEN_REJECT_DUPLICATE, REOPEN_REJECT_UNSUPPORTED})

# A prior family thread is reopen-eligible only if its canonical evidence
# classification indicates a data limitation (not a conclusive verdict).
REOPEN_ELIGIBLE_CLASSIFICATIONS = frozenset({
    "BLOCKED_BY_MISSING_DATA", "TESTED_INCONCLUSIVE", "NOT_YET_TESTED",
    "METADATA_INSUFFICIENT", "PARTIALLY_TESTED"})

# Classifications that represent a conclusive verdict — NEVER reopened merely
# because more price rows exist.
REOPEN_CONCLUSIVE_CLASSIFICATIONS = frozenset({
    "PROVEN_DISTINCT", "REJECT_FAMILY", "REJECTED", "TESTED_NO_SURVIVOR",
    "CONCLUSIVELY_REJECTED"})

# Stage 1 information family -> Stage 5 allowlisted template it would reopen
# under. Only families whose template is genuinely unlocked by acquired data can
# produce a NEW_DATA_AVAILABLE reopen; the rest stay STILL_DATA_HELD.
FAMILY_TO_STAGE5_TEMPLATE: dict[str, str] = {
    "price_momentum": "price_momentum_rank",
    "short_term_reversal": "price_momentum_rank",
    "long_term_reversal": "price_momentum_rank",
    "sector_exposure": "sector_relative_ranking",
    "value": "fundamental_momentum_rank",
    "growth": "fundamental_momentum_rank",
    "quality": "fundamental_momentum_rank",
    "profitability": "fundamental_momentum_rank",
    "investment": "fundamental_momentum_rank",
    "earnings": "earnings_filing_event_drift",
    "earnings_surprise": "earnings_filing_event_drift",
    "revenue_surprise": "earnings_filing_event_drift",
    "estimates_and_revisions": "earnings_filing_event_drift",
    "insider_activity": "insider_activity_event_study",
    "news_and_events": "news_regulatory_event_study",
}

# Canonical, honest research statement used when reopening a Stage 1 FAMILY
# thread (existing data-starved research, not a fabricated new idea). Wording
# deliberately matches the template so Stage 5's deterministic template mapper
# selects the intended template.
CANONICAL_FAMILY_STATEMENT: dict[str, str] = {
    "price_momentum": ("Cross-sectional price momentum (trailing 12-1 month "
                       "return, relative strength) ranks forward returns across "
                       "the survivorship-free S&P 500 universe."),
    "short_term_reversal": ("Cross-sectional one-month price reversal ranks "
                            "forward returns across the S&P 500 universe."),
    "long_term_reversal": ("Cross-sectional long-horizon price reversal ranks "
                           "forward returns across the S&P 500 universe."),
    "sector_exposure": ("Sector-relative price momentum (intra-sector relative "
                        "strength) ranks forward returns across the S&P 500 "
                        "universe."),
    "value": ("Cross-sectional value ranking (fundamental) predicts forward "
              "returns across the S&P 500 universe."),
    "quality": ("Cross-sectional quality/profitability ranking predicts forward "
                "returns across the S&P 500 universe."),
    "profitability": ("Gross profitability ranking predicts forward returns "
                      "across the S&P 500 universe."),
    "earnings": ("Post-earnings-announcement drift after the public report date "
                 "predicts forward returns."),
    "earnings_surprise": ("Earnings-surprise drift after the public report date "
                          "predicts forward returns."),
    "insider_activity": ("Insider Form-4 buying clusters predict forward "
                         "returns after the filing date."),
    "news_and_events": ("Regulatory / news events predict forward returns after "
                        "the publication timestamp."),
}


def reopen_information_family_for(family: str) -> Optional[str]:
    return FAMILY_TO_STAGE5_TEMPLATE.get(family)


# --------------------------------------------------------------------------- #
# Persistent Stage 6 backfill-state schema (stdlib sqlite3; FKs on; WAL set at
# connect). Exactly the ten tables required by the Stage 6 contract.
# --------------------------------------------------------------------------- #
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stage6_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backfill_runs (
    run_id TEXT PRIMARY KEY,
    as_of TEXT NOT NULL,
    mode TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    universe_fingerprint TEXT NOT NULL,
    date_start TEXT,
    date_end TEXT,
    status TEXT NOT NULL,
    terminal TEXT,
    records_written INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS provider_batches (
    batch_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    family TEXT NOT NULL,
    status TEXT NOT NULL,
    records_written INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS ticker_batches (
    batch_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    ticker_count INTEGER NOT NULL DEFAULT 0,
    tickers_json TEXT NOT NULL,
    date_start TEXT,
    date_end TEXT,
    status TEXT NOT NULL,
    records_written INTEGER NOT NULL DEFAULT 0,
    completed_at TEXT,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS date_ranges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    family TEXT NOT NULL,
    record_type TEXT NOT NULL,
    min_effective_date TEXT,
    max_effective_date TEXT,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS normalized_counts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    family TEXT NOT NULL,
    record_type TEXT NOT NULL,
    provider TEXT,
    record_count INTEGER NOT NULL DEFAULT 0,
    duplicate_prevention_count INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS provider_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    error_type TEXT NOT NULL,
    message TEXT,
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS entitlement_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    family TEXT NOT NULL,
    state TEXT NOT NULL,
    detail TEXT,
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS coverage_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    family TEXT NOT NULL,
    record_type TEXT NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    min_effective_date TEXT,
    max_effective_date TEXT,
    point_in_time_usable INTEGER NOT NULL DEFAULT 0,
    survivorship_safe INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS resumed_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    resumed_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS reopened_hypotheses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    hypothesis_id TEXT NOT NULL,
    information_family TEXT,
    template TEXT,
    disposition TEXT NOT NULL,
    prior_classification TEXT,
    data_version TEXT,
    detail TEXT,
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backfill_runs (run_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS ix_ticker_batches_status
    ON ticker_batches (run_id, status);
CREATE INDEX IF NOT EXISTS ix_normalized_counts_run
    ON normalized_counts (run_id);
CREATE INDEX IF NOT EXISTS ix_reopened_disp
    ON reopened_hypotheses (disposition);
"""

REQUIRED_TABLES = (
    "backfill_runs", "provider_batches", "ticker_batches", "date_ranges",
    "normalized_counts", "provider_errors", "entitlement_blocks",
    "coverage_snapshots", "resumed_batches", "reopened_hypotheses")

# Immutable per-run output files (Stage 6 run package).
REQUIRED_RUN_FILES = (
    "stage6_input.json", "provider_entitlements.json", "source_plan.json",
    "batch_results.jsonl", "normalized_record_counts.csv",
    "coverage_before.csv", "coverage_after.csv", "coverage_delta.csv",
    "point_in_time_audit.csv", "unresolved_data_gaps.jsonl",
    "reopened_hypotheses.jsonl", "experiment_activation.json",
    "stage6_backfill_report.md", "run_manifest.json",
)


# --------------------------------------------------------------------------- #
# Deterministic identifiers.
# --------------------------------------------------------------------------- #
def sha(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def config_hash(cfg: dict) -> str:
    return sha(canonical_json(cfg))[:16]


def universe_fingerprint(symbols: list[str]) -> str:
    return sha("univ", canonical_json(sorted(set(symbols))))[:16]


def stage6_run_id(as_of: str, cfg_hash: str, universe_fp: str) -> str:
    return "stage6_" + sha("run", str(as_of), str(cfg_hash),
                           str(universe_fp))[:16]


def ticker_batch_id(run_id: str, provider: str, tickers: list[str],
                    date_start: str, date_end: str) -> str:
    return "tb_" + sha(run_id, provider, canonical_json(sorted(set(tickers))),
                       str(date_start), str(date_end))[:16]


def provider_batch_id(run_id: str, provider: str, family: str) -> str:
    return "pb_" + sha(run_id, provider, family)[:16]


def data_version_id(universe_fp: str, date_start: str, date_end: str,
                    record_count: int) -> str:
    return "dv6_" + sha("dv", str(universe_fp), str(date_start), str(date_end),
                        str(record_count))[:16]


# --------------------------------------------------------------------------- #
# Config loading + validation.
# --------------------------------------------------------------------------- #
_REQUIRED_CONFIG_KEYS = (
    "stage", "backfill_root", "ingestion_root", "stage1_registry_root",
    "operational_ledger_roots", "universe", "date_range", "providers",
    "batch", "readiness", "reopen",
)


class ConfigError(ValueError):
    """Raised when the Stage 6 config is missing or unsafe."""


def load_config(path: str | Path) -> dict:
    """Load, secret-scan and structurally validate the Stage 6 config."""
    import json
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
    if str(cfg.get("stage")) != STAGE6_STAGE:
        raise ConfigError("config stage must be '%s'" % STAGE6_STAGE)

    dr = cfg.get("date_range") or {}
    for k in ("start", "end"):
        if k not in dr:
            raise ConfigError("date_range missing '%s'" % k)
    try:
        _dt.date.fromisoformat(str(dr.get("start")))
    except ValueError as exc:
        raise ConfigError("date_range.start not ISO date: %s" % exc) from exc

    batch = cfg.get("batch") or {}
    for k in ("ticker_batch_size", "max_universe_symbols"):
        v = batch.get(k)
        if not isinstance(v, int) or v <= 0:
            raise ConfigError("batch.%s must be a positive integer" % k)

    providers = cfg.get("providers") or {}
    if not isinstance(providers, dict) or not providers:
        raise ConfigError("providers must be a non-empty object")
    unknown = [p for p in providers if p not in PROVIDER_FAMILIES]
    if unknown:
        raise ConfigError("providers has unknown provider(s): %s" %
                          ", ".join(unknown))
    return cfg


def enabled_providers(cfg: dict) -> list[str]:
    out = []
    for name, pcfg in (cfg.get("providers") or {}).items():
        if name in PROVIDER_FAMILIES and (pcfg or {}).get("enabled"):
            out.append(name)
    return sorted(out, key=lambda n: (PROVIDER_FAMILIES[n]["priority"], n))


def all_terminal_tokens() -> list[str]:
    return sorted(TERMINAL_TOKENS)


def now_utc_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat()


def redact_config(cfg: dict) -> dict:
    """Return a copy safe to embed in an output package (drops any accidental
    credential-looking keys; configs never carry secrets, but this is a belt)."""
    def _clean(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: ("REDACTED" if ("key" in k.lower() or "token" in k.lower()
                                       or "secret" in k.lower()) else _clean(v))
                    for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean(x) for x in obj]
        return obj
    return _clean(cfg)
