"""
alpha_agent.experiment_contracts — Stage 5 experiment & evidence contracts.

Pure, side-effect-free definitions shared by the Stage 5 experiment factory, the
experiment runner, the Stage 4 runtime integration, the report renderer, the CLI
and the tests: terminal tokens, the deterministic experiment-template allowlist,
the feature/factor allowlist, the persistent experiment-state database schema,
deterministic identifiers, the pure evidence-metric math, the deterministic
evidence-gate engine, and config loading + validation + the secret-scan guard.

Safety invariants enforced here (never relaxed downstream):
  * Stage 5 is RESEARCH-ONLY. Nothing in this module can create an order, fill,
    signal, trade decision, model promotion, portfolio change, Paper Trader / DB
    mutation, prediction-service call or Daily Close. There is no network,
    PostgreSQL, subprocess, LLM or clock read here.
  * Experiment definitions come EXCLUSIVELY from the deterministic, allowlisted
    templates below. No LLM-authored factor, expression, SQL, Python or strategy
    code is ever constructed or executed. The LLM only supplies hypothesis TEXT.
  * All identifiers are deterministic functions of their inputs so completed
    cycles are idempotent and identical inputs reproduce identical ids/results.
  * No credential value is ever read, stored or printed; configs may name
    environment variables but never contain a secret. ``load_config`` scans for
    and rejects any embedded secret.

Numeric math is pure Python standard library only (no numpy/pandas) so results
are byte-reproducible across machines and require no third-party package.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from .runtime_contracts import (canonical_json, redact,  # noqa: F401
                                scan_for_secrets)

STAGE5_STAGE = "5"
STAGE5_SCHEMA_VERSION = "1.0.0"
STAGE5_ENGINE_VERSION = "1.0.0"
TEMPLATE_REGISTRY_VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# Terminal tokens (exactly one printed per CLI invocation).
# --------------------------------------------------------------------------- #
READY = "ALPHA_AGENT_STAGE5_READY"
VERIFIED = "ALPHA_AGENT_STAGE5_VERIFIED"
NO_EXPERIMENTABLE_HYPOTHESES = "NO_EXPERIMENTABLE_HYPOTHESES"
DATA_HOLD = "ALPHA_AGENT_STAGE5_DATA_HOLD"
PARTIAL = "ALPHA_AGENT_STAGE5_PARTIAL"
BLOCKED = "ALPHA_AGENT_STAGE5_BLOCKED"

TERMINAL_TOKENS = frozenset({
    READY, VERIFIED, NO_EXPERIMENTABLE_HYPOTHESES, DATA_HOLD, PARTIAL, BLOCKED})

# --------------------------------------------------------------------------- #
# Evidence-gate decisions.
# --------------------------------------------------------------------------- #
KEEP_FOR_RESEARCH = "KEEP_FOR_RESEARCH"
NEED_MORE_DATA = "NEED_MORE_DATA"
DECISION_DATA_HOLD = "DATA_HOLD"
DUPLICATE = "DUPLICATE"
REJECT_WEAK_EVIDENCE = "REJECT_WEAK_EVIDENCE"
REJECT_COST_SENSITIVITY = "REJECT_COST_SENSITIVITY"
REJECT_INSTABILITY = "REJECT_INSTABILITY"
REJECT_LEAKAGE_RISK = "REJECT_LEAKAGE_RISK"
EXPERIMENT_FAILED = "EXPERIMENT_FAILED"

EVIDENCE_DECISIONS = frozenset({
    KEEP_FOR_RESEARCH, NEED_MORE_DATA, DECISION_DATA_HOLD, DUPLICATE,
    REJECT_WEAK_EVIDENCE, REJECT_COST_SENSITIVITY, REJECT_INSTABILITY,
    REJECT_LEAKAGE_RISK, EXPERIMENT_FAILED})

# Rejection / non-keep decisions (used by the report + gate summary).
NON_KEEP_DECISIONS = EVIDENCE_DECISIONS - {KEEP_FOR_RESEARCH}

# --------------------------------------------------------------------------- #
# Data-gap / hold reasons.
# --------------------------------------------------------------------------- #
DATA_HOLD_UNSUPPORTED_TEMPLATE = "DATA_HOLD_UNSUPPORTED_TEMPLATE"
DATA_HOLD_MISSING_DATASET = "DATA_HOLD_MISSING_DATASET"
DATA_HOLD_INSUFFICIENT_HISTORY = "DATA_HOLD_INSUFFICIENT_HISTORY"
DATA_HOLD_INSUFFICIENT_COVERAGE = "DATA_HOLD_INSUFFICIENT_COVERAGE"
DATA_HOLD_NO_POINT_IN_TIME = "DATA_HOLD_NO_POINT_IN_TIME"

# --------------------------------------------------------------------------- #
# Deterministic experiment-template allowlist.
#
# Each template maps a class of grounded hypothesis to a fully-deterministic
# experiment specification. ``required_datasets`` are logical Stage 2 dataset
# family names; the runner resolves them against the real ingestion layout and
# emits a structured data gap when a family is unavailable (never an
# approximation). ``features`` list the allowlisted factor keys the template may
# use — the runner computes them from deterministic source code only.
# --------------------------------------------------------------------------- #
TPL_EVENT_WINDOW = "event_window_cross_sectional_return"
TPL_FUNDAMENTAL_MOMENTUM = "fundamental_momentum_rank"
TPL_PRICE_MOMENTUM = "price_momentum_rank"
TPL_EARNINGS_DRIFT = "earnings_filing_event_drift"
TPL_INSIDER_EVENT = "insider_activity_event_study"
TPL_NEWS_EVENT = "news_regulatory_event_study"
TPL_SECTOR_RELATIVE = "sector_relative_ranking"
TPL_COMBINED_FACTOR = "combined_factor_ranking"

TEMPLATES: dict[str, dict] = {
    TPL_PRICE_MOMENTUM: {
        "version": "1.0.0",
        "study_kind": "cross_sectional_rank",
        "required_datasets": ["prices"],
        "features": ["mom_12_1", "mom_6_1", "reversal_1m"],
        "default_feature": "mom_12_1",
        "rebalance": "monthly",
        "horizon_days": 21,
        "min_universe": 30,
        "min_periods": 24,
        "leakage_control": "factor_uses_data_through_t_return_t_to_t_plus_h",
        "expected_direction": "positive",
        "keywords": ["price momentum", "momentum", "trend", "12-1", "6-1",
                     "trailing return", "relative strength"],
    },
    TPL_FUNDAMENTAL_MOMENTUM: {
        "version": "1.0.0",
        "study_kind": "cross_sectional_rank",
        "required_datasets": ["prices", "fundamentals"],
        "features": ["fund_earnings_momentum", "fund_revenue_momentum",
                     "gross_profitability", "fcf_to_assets"],
        "default_feature": "fund_earnings_momentum",
        "rebalance": "monthly",
        "horizon_days": 63,
        "min_universe": 30,
        "min_periods": 16,
        "leakage_control": "pit_fundamentals_lagged_publication",
        "expected_direction": "positive",
        "keywords": ["fundamental momentum", "earnings momentum", "revenue",
                     "profitability", "fcf", "free cash flow", "accruals",
                     "fundamental trend"],
    },
    TPL_EVENT_WINDOW: {
        "version": "1.0.0",
        "study_kind": "event_window",
        "required_datasets": ["prices", "events"],
        "features": ["car_pre_event", "abnormal_volume"],
        "default_feature": "car_pre_event",
        "rebalance": "event",
        "horizon_days": 5,
        "min_universe": 20,
        "min_periods": 40,
        "leakage_control": "event_window_strictly_after_announcement",
        "expected_direction": "positive",
        "keywords": ["event", "announcement", "surprise", "reaction",
                     "window", "post-event"],
    },
    TPL_EARNINGS_DRIFT: {
        "version": "1.0.0",
        "study_kind": "event_window",
        "required_datasets": ["prices", "earnings"],
        "features": ["earnings_surprise", "sue", "post_earnings_return"],
        "default_feature": "earnings_surprise",
        "rebalance": "event",
        "horizon_days": 63,
        "min_universe": 20,
        "min_periods": 40,
        "leakage_control": "drift_window_strictly_after_report_date",
        "expected_direction": "positive",
        "keywords": ["earnings", "post-earnings", "pead", "drift", "sue",
                     "earnings surprise", "guidance", "filing", "10-q", "10-k"],
    },
    TPL_INSIDER_EVENT: {
        "version": "1.0.0",
        "study_kind": "event_window",
        "required_datasets": ["prices", "insider"],
        "features": ["net_insider_buys", "insider_cluster"],
        "default_feature": "net_insider_buys",
        "rebalance": "event",
        "horizon_days": 63,
        "min_universe": 20,
        "min_periods": 30,
        "leakage_control": "window_after_form4_filing_date",
        "expected_direction": "positive",
        "keywords": ["insider", "form 4", "insider buying", "insider selling",
                     "insider activity", "cluster buy"],
    },
    TPL_NEWS_EVENT: {
        "version": "1.0.0",
        "study_kind": "event_window",
        "required_datasets": ["prices", "news"],
        "features": ["news_sentiment", "regulatory_flag", "news_volume"],
        "default_feature": "news_sentiment",
        "rebalance": "event",
        "horizon_days": 10,
        "min_universe": 20,
        "min_periods": 40,
        "leakage_control": "window_after_publication_timestamp",
        "expected_direction": "positive",
        "keywords": ["news", "regulatory", "regulation", "sentiment",
                     "headline", "press", "sec", "fda", "doj"],
    },
    TPL_SECTOR_RELATIVE: {
        "version": "1.0.0",
        "study_kind": "cross_sectional_rank",
        "required_datasets": ["prices", "sector_map"],
        "features": ["sector_relative_momentum", "sector_relative_reversal"],
        "default_feature": "sector_relative_momentum",
        "rebalance": "monthly",
        "horizon_days": 21,
        "min_universe": 30,
        "min_periods": 24,
        "leakage_control": "sector_neutral_factor_through_t",
        "expected_direction": "positive",
        "keywords": ["sector", "industry", "sector-relative", "sector neutral",
                     "relative to peers", "intra-sector"],
    },
    TPL_COMBINED_FACTOR: {
        "version": "1.0.0",
        "study_kind": "cross_sectional_rank",
        "required_datasets": ["prices", "fundamentals"],
        "features": ["combo_momentum_quality", "combo_value_momentum"],
        "default_feature": "combo_momentum_quality",
        "rebalance": "monthly",
        "horizon_days": 63,
        "min_universe": 30,
        "min_periods": 16,
        "leakage_control": "each_leg_uses_data_through_t_only",
        "expected_direction": "positive",
        "keywords": ["combined", "combination", "multi-factor", "blend",
                     "composite", "quality and momentum", "value and momentum"],
    },
}

SUPPORTED_TEMPLATES = tuple(TEMPLATES.keys())

# Every allowlisted factor key across all templates (nothing else may be built).
FEATURE_ALLOWLIST = frozenset(
    f for tpl in TEMPLATES.values() for f in tpl["features"])

# Logical dataset families the templates can require.
DATASET_FAMILIES = frozenset(
    d for tpl in TEMPLATES.values() for d in tpl["required_datasets"])

# Resolution of each logical dataset family to the Stage 2 normalized
# record_type directories (alpha_agent.source_contracts.RECORD_TYPES). The
# runner uses this to locate real historical data and to emit a precise data
# gap when a family is absent — never to approximate.
FAMILY_TO_RECORD_TYPES: dict[str, tuple] = {
    "prices": ("MARKET_BAR",),
    "fundamentals": ("FUNDAMENTAL_FACT",),
    "earnings": ("EARNINGS_EVENT",),
    "insider": ("INSIDER_FILING",),
    "news": ("NEWS_EVENT",),
    "events": ("EARNINGS_EVENT", "FILING_EVENT", "NEWS_EVENT",
               "CORPORATE_ACTION"),
    "sector_map": ("SECURITY_IDENTITY",),
}

# Requirement tokens that CANNOT be safely expressed by any allowlisted,
# owned-data template (cross-firm relationship maps, purchased analyst/consensus
# estimates, foreign-listed coverage). A hypothesis whose required data mentions
# any of these is held as DATA_HOLD_UNSUPPORTED_TEMPLATE rather than forced onto
# an own-firm template it does not match — an honesty guard, never an
# approximation.
UNSUPPORTED_REQUIREMENT_TOKENS = (
    "supplier", "supply-chain", "supply chain", "read-through", "readthrough",
    "relationship map", "relationship mapping", "customer mapping",
    "consensus", "analyst estimate", "analyst estimates", "analyst revision",
    "estimate revision", "foreign-listed", "foreign listed",
)


# --------------------------------------------------------------------------- #
# Persistent Stage 5 experiment-state schema (stdlib sqlite3; FKs on; WAL set
# at connect). Exactly the eight tables required by the Stage 5 contract.
# --------------------------------------------------------------------------- #
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stage5_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hypotheses_seen (
    hypothesis_key TEXT PRIMARY KEY,
    hypothesis_id TEXT NOT NULL,
    template TEXT,
    hypothesis_sha TEXT NOT NULL,
    first_seen_run_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    disposition TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS experiment_specs (
    experiment_id TEXT PRIMARY KEY,
    hypothesis_id TEXT NOT NULL,
    hypothesis_key TEXT,
    template TEXT NOT NULL,
    template_version TEXT NOT NULL,
    spec_fingerprint TEXT NOT NULL,
    data_version TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    created_run_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (hypothesis_id, spec_fingerprint, data_version)
);
CREATE TABLE IF NOT EXISTS stage5_cycles (
    run_id TEXT PRIMARY KEY,
    as_of TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    terminal TEXT,
    hypotheses_considered INTEGER NOT NULL DEFAULT 0,
    experiments_completed INTEGER NOT NULL DEFAULT 0,
    experiments_failed INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS experiment_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    FOREIGN KEY (experiment_id) REFERENCES experiment_specs (experiment_id)
        ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES stage5_cycles (run_id) ON DELETE CASCADE,
    UNIQUE (experiment_id, run_id)
);
CREATE TABLE IF NOT EXISTS experiment_results (
    experiment_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    benchmark_json TEXT,
    regime_json TEXT,
    cost_json TEXT,
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (experiment_id) REFERENCES experiment_specs (experiment_id)
        ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS evidence_decisions (
    experiment_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    hypothesis_id TEXT NOT NULL,
    decision TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (experiment_id) REFERENCES experiment_specs (experiment_id)
        ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS data_gaps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    hypothesis_id TEXT,
    template TEXT,
    gap_reason TEXT NOT NULL,
    detail TEXT,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS failed_experiments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    experiment_id TEXT,
    hypothesis_id TEXT,
    failure_reason TEXT NOT NULL,
    detail TEXT,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_experiment_specs_hyp
    ON experiment_specs (hypothesis_id);
CREATE INDEX IF NOT EXISTS ix_evidence_decisions_dec
    ON evidence_decisions (decision);
CREATE INDEX IF NOT EXISTS ix_data_gaps_run ON data_gaps (run_id);
"""

REQUIRED_TABLES = ("hypotheses_seen", "experiment_specs", "experiment_runs",
                   "experiment_results", "evidence_decisions", "data_gaps",
                   "failed_experiments", "stage5_cycles")

# Immutable per-run output files.
REQUIRED_RUN_FILES = (
    "stage5_input.json", "source_packages.json", "hypothesis_intake.jsonl",
    "experiment_specs.jsonl", "data_coverage.csv", "experiment_results.jsonl",
    "evidence_metrics.csv", "benchmark_comparison.csv", "regime_results.csv",
    "cost_sensitivity.csv", "evidence_decisions.jsonl", "data_gaps.jsonl",
    "rejected_duplicates.jsonl", "stage5_experiment_report.md",
    "run_manifest.json",
)


# --------------------------------------------------------------------------- #
# Deterministic identifiers.
# --------------------------------------------------------------------------- #
def sha(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def config_hash(cfg: dict) -> str:
    return sha(canonical_json(cfg))[:16]


def spec_fingerprint(spec: dict) -> str:
    """Fingerprint the deterministic experiment design (not run-time fields)."""
    keyed = {k: spec.get(k) for k in (
        "template", "template_version", "feature", "universe", "date_start",
        "date_end", "rebalance", "horizon_days", "benchmark",
        "transaction_cost_bps", "required_datasets", "leakage_control")}
    return sha(canonical_json(keyed))[:16]


def hypothesis_sha(text: str) -> str:
    return sha("hyp", (text or "").strip())[:16]


def hypothesis_seen_key(hypothesis_id: str, template: str) -> str:
    """Dedup key: a given hypothesis under a given template is run once."""
    return sha("seen", str(hypothesis_id), str(template))[:20]


def experiment_id(hypothesis_id: str, template: str, spec_fp: str,
                  data_version: str) -> str:
    return "exp_" + sha(str(hypothesis_id), str(template), str(spec_fp),
                        str(data_version))[:16]


def stage5_run_id(as_of: str, cfg_hash: str, source_fingerprint: str) -> str:
    return "stage5_" + sha("cycle", str(as_of), str(cfg_hash),
                           str(source_fingerprint))[:16]


def data_version_id(source_fingerprint: str, date_start: str,
                    date_end: str) -> str:
    return "dv_" + sha(str(source_fingerprint), str(date_start),
                       str(date_end))[:16]


# --------------------------------------------------------------------------- #
# Pure evidence-metric math (stdlib only; no numpy/pandas).
# --------------------------------------------------------------------------- #
def _finite(values: Iterable[Any]) -> list[float]:
    out: list[float] = []
    for v in values:
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            out.append(f)
    return out


def mean(values: Sequence[Any]) -> Optional[float]:
    vals = _finite(values)
    if not vals:
        return None
    return sum(vals) / len(vals)


def stdev(values: Sequence[Any], *, sample: bool = True) -> Optional[float]:
    vals = _finite(values)
    n = len(vals)
    if n < (2 if sample else 1):
        return None
    m = sum(vals) / n
    ss = sum((x - m) ** 2 for x in vals)
    denom = (n - 1) if sample else n
    return math.sqrt(ss / denom) if denom > 0 else None


def tstat(values: Sequence[Any]) -> Optional[float]:
    """One-sample t-stat of the mean against zero."""
    vals = _finite(values)
    n = len(vals)
    if n < 2:
        return None
    m = sum(vals) / n
    sd = stdev(vals, sample=True)
    if sd is None or sd == 0.0:
        return None
    return m / (sd / math.sqrt(n))


def positive_ratio(values: Sequence[Any]) -> Optional[float]:
    vals = _finite(values)
    if not vals:
        return None
    return sum(1 for v in vals if v > 0) / len(vals)


def ranks(values: Sequence[float]) -> list[float]:
    """Average (fractional) ranks, 1-based, ties share the mean rank."""
    idx = sorted(range(len(values)), key=lambda i: values[i])
    out = [0.0] * len(values)
    i = 0
    while i < len(idx):
        j = i
        while j + 1 < len(idx) and values[idx[j + 1]] == values[idx[i]]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            out[idx[k]] = avg_rank
        i = j + 1
    return out


def pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx = sum(xs) / len(xs)
    my = sum(ys) / len(ys)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sxy / math.sqrt(sxx * syy)


def spearman(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    """Spearman rank correlation of one aligned cross-section (the rank IC)."""
    pairs = [(float(a), float(b)) for a, b in zip(xs, ys)
             if _is_num(a) and _is_num(b)]
    if len(pairs) < 3:
        return None
    xr = ranks([p[0] for p in pairs])
    yr = ranks([p[1] for p in pairs])
    return pearson(xr, yr)


def _is_num(v: Any) -> bool:
    try:
        return math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def rank_ic_series(cross_sections: Sequence[tuple]) -> list[float]:
    """Per-period rank IC from a sequence of (factor_vals, forward_returns)."""
    out: list[float] = []
    for factor_vals, fwd in cross_sections:
        ic = spearman(factor_vals, fwd)
        if ic is not None:
            out.append(ic)
    return out


def decile_spread_series(cross_sections: Sequence[tuple], *,
                         buckets: int = 10) -> list[float]:
    """Per-period top-minus-bottom bucket mean forward return."""
    out: list[float] = []
    for factor_vals, fwd in cross_sections:
        spread = _decile_spread_one(factor_vals, fwd, buckets)
        if spread is not None:
            out.append(spread)
    return out


def _decile_spread_one(factor_vals, fwd, buckets) -> Optional[float]:
    pairs = [(float(f), float(r)) for f, r in zip(factor_vals, fwd)
             if _is_num(f) and _is_num(r)]
    if len(pairs) < buckets:
        return None
    pairs.sort(key=lambda p: p[0])
    n = len(pairs)
    size = max(1, n // buckets)
    bottom = [p[1] for p in pairs[:size]]
    top = [p[1] for p in pairs[-size:]]
    mt, mb = mean(top), mean(bottom)
    if mt is None or mb is None:
        return None
    return mt - mb


def turnover(prev_weights: dict, weights: dict) -> float:
    """One-sided turnover = 0.5 * sum |w_t - w_{t-1}| across the union."""
    keys = set(prev_weights) | set(weights)
    return 0.5 * sum(abs(weights.get(k, 0.0) - prev_weights.get(k, 0.0))
                     for k in keys)


def apply_costs(gross_returns: Sequence[float], turnovers: Sequence[float],
                cost_bps: float) -> list[float]:
    """Net = gross - turnover * cost. cost_bps is per unit one-sided turnover."""
    rate = float(cost_bps) / 10000.0
    net: list[float] = []
    for i, g in enumerate(gross_returns):
        t = turnovers[i] if i < len(turnovers) else 0.0
        net.append(float(g) - t * rate)
    return net


def cumulative_return(period_returns: Sequence[float]) -> Optional[float]:
    vals = _finite(period_returns)
    if not vals:
        return None
    acc = 1.0
    for r in vals:
        acc *= (1.0 + r)
    return acc - 1.0


def max_drawdown(period_returns: Sequence[float]) -> Optional[float]:
    """Max drawdown (<=0) of the compounded equity curve of period returns."""
    vals = _finite(period_returns)
    if not vals:
        return None
    equity = 1.0
    peak = 1.0
    mdd = 0.0
    for r in vals:
        equity *= (1.0 + r)
        peak = max(peak, equity)
        if peak > 0:
            dd = equity / peak - 1.0
            mdd = min(mdd, dd)
    return mdd


def annualized_return(period_returns: Sequence[float], *,
                      periods_per_year: float) -> Optional[float]:
    vals = _finite(period_returns)
    if not vals:
        return None
    total = cumulative_return(vals)
    if total is None:
        return None
    years = len(vals) / float(periods_per_year)
    if years <= 0:
        return None
    base = 1.0 + total
    if base <= 0:
        return -1.0
    return base ** (1.0 / years) - 1.0


def annualized_vol(period_returns: Sequence[float], *,
                   periods_per_year: float) -> Optional[float]:
    sd = stdev(period_returns, sample=True)
    if sd is None:
        return None
    return sd * math.sqrt(float(periods_per_year))


def sharpe(period_returns: Sequence[float], *,
           periods_per_year: float) -> Optional[float]:
    m = mean(period_returns)
    sd = stdev(period_returns, sample=True)
    if m is None or sd is None or sd == 0.0:
        return None
    return (m / sd) * math.sqrt(float(periods_per_year))


def hit_rate(period_returns: Sequence[float]) -> Optional[float]:
    return positive_ratio(period_returns)


def subperiod_consistency(period_values: Sequence[float], *,
                          parts: int = 2) -> Optional[float]:
    """Fraction of equal-size subperiods whose mean has the same (positive) sign.

    Returns the share of subperiods with a positive mean; 1.0 means every
    subperiod agreed on a positive edge.
    """
    vals = _finite(period_values)
    if len(vals) < parts:
        return None
    size = len(vals) // parts
    if size == 0:
        return None
    signs = []
    for i in range(parts):
        chunk = vals[i * size:(i + 1) * size] if i < parts - 1 \
            else vals[i * size:]
        m = mean(chunk)
        if m is None:
            continue
        signs.append(1.0 if m > 0 else 0.0)
    if not signs:
        return None
    return sum(signs) / len(signs)


def periods_per_year_for(rebalance: str) -> float:
    return {"daily": 252.0, "weekly": 52.0, "monthly": 12.0,
            "quarterly": 4.0}.get(str(rebalance), 12.0)


# --------------------------------------------------------------------------- #
# Deterministic evidence-gate engine.
# --------------------------------------------------------------------------- #
DEFAULT_GATES: dict[str, float] = {
    "min_observations": 200,
    "min_universe": 30,
    "min_periods": 12,
    "min_ic_t": 2.0,
    "min_ic_positive_ratio": 0.50,
    "min_spread_t": 1.5,
    "min_oos_ic_mean": 0.0,
    "max_turnover": 1.5,
    "min_net_annualized_return": 0.0,
    "max_drawdown_floor": -0.60,
    "min_subperiod_consistency": 1.0,
    "min_regime_consistency": 0.50,
    "max_missing_data_rate": 0.35,
    "max_cost_erosion_ratio": 0.75,
    "min_champion_complementarity": 0.0,
}


def resolved_gates(cfg: dict) -> dict:
    gates = dict(DEFAULT_GATES)
    for k, v in (cfg.get("gates") or {}).items():
        if k in gates and isinstance(v, (int, float)):
            gates[k] = float(v)
    return gates


def evaluate_evidence(metrics: dict, gates: dict) -> dict:
    """Deterministically map computed metrics to one evidence decision.

    Ordering is by severity so the most specific rejection is surfaced. Returns
    ``{"decision", "reasons", "passed", "failed"}``. KEEP_FOR_RESEARCH requires
    every configured minimum to pass.
    """
    reasons: list[str] = []
    passed: list[str] = []
    failed: list[str] = []

    def _num(key):
        v = metrics.get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    # 0. Hard experiment failure.
    if metrics.get("experiment_failed"):
        return _decision(EXPERIMENT_FAILED,
                         [str(metrics.get("failure_reason")
                              or "experiment failed")], passed, failed)

    # 1. Structural data holds first (never scored as weak evidence).
    if metrics.get("data_gap"):
        return _decision(DECISION_DATA_HOLD,
                         [str(metrics.get("data_gap"))], passed, failed)

    # 2. Sample adequacy.
    obs = _num("observations")
    if obs is None or obs < gates["min_observations"]:
        failed.append("observations")
        reasons.append("insufficient observations (%s < %s)" %
                       (_fmt(obs), _fmt(gates["min_observations"])))
        return _decision(NEED_MORE_DATA, reasons, passed, failed)
    passed.append("observations")

    periods = _num("periods")
    if periods is None or periods < gates["min_periods"]:
        failed.append("periods")
        reasons.append("insufficient periods (%s < %s)" %
                       (_fmt(periods), _fmt(gates["min_periods"])))
        return _decision(NEED_MORE_DATA, reasons, passed, failed)
    passed.append("periods")

    uni = _num("universe")
    if uni is None or uni < gates["min_universe"]:
        failed.append("universe")
        reasons.append("universe too small (%s < %s)" %
                       (_fmt(uni), _fmt(gates["min_universe"])))
        return _decision(NEED_MORE_DATA, reasons, passed, failed)
    passed.append("universe")

    missing = _num("missing_data_rate")
    if missing is not None and missing > gates["max_missing_data_rate"]:
        failed.append("missing_data_rate")
        reasons.append("missing-data rate too high (%s > %s)" %
                       (_fmt(missing), _fmt(gates["max_missing_data_rate"])))
        return _decision(NEED_MORE_DATA, reasons, passed, failed)
    passed.append("missing_data_rate")

    # 3. Leakage risk.
    if metrics.get("leakage_warning"):
        failed.append("leakage")
        reasons.append("leakage warning: %s" %
                       str(metrics.get("leakage_detail") or "unspecified"))
        return _decision(REJECT_LEAKAGE_RISK, reasons, passed, failed)
    passed.append("leakage")

    # 4. Cost sensitivity — a gross edge that costs erase is not real.
    gross_ann = _num("gross_annualized_return")
    net_ann = _num("net_annualized_return")
    cost_flip = bool(metrics.get("cost_flips_sign"))
    erosion = _num("cost_erosion_ratio")
    turn = _num("turnover")
    if turn is not None and turn > gates["max_turnover"]:
        failed.append("turnover")
        reasons.append("turnover too high (%s > %s)" %
                       (_fmt(turn), _fmt(gates["max_turnover"])))
        return _decision(REJECT_COST_SENSITIVITY, reasons, passed, failed)
    if cost_flip or (net_ann is not None
                     and net_ann < gates["min_net_annualized_return"]) \
            or (erosion is not None and gross_ann is not None and gross_ann > 0
                and erosion > gates["max_cost_erosion_ratio"]):
        failed.append("cost_sensitivity")
        reasons.append(
            "edge does not survive costs (net_ann=%s, erosion=%s, flip=%s)" %
            (_fmt(net_ann), _fmt(erosion), cost_flip))
        return _decision(REJECT_COST_SENSITIVITY, reasons, passed, failed)
    passed.append("cost_sensitivity")

    # 5. Instability — subperiod / regime inconsistency.
    sub = _num("subperiod_consistency")
    reg = _num("regime_consistency")
    if sub is not None and sub < gates["min_subperiod_consistency"]:
        failed.append("subperiod_consistency")
        reasons.append("subperiod inconsistency (%s < %s)" %
                       (_fmt(sub), _fmt(gates["min_subperiod_consistency"])))
        return _decision(REJECT_INSTABILITY, reasons, passed, failed)
    if reg is not None and reg < gates["min_regime_consistency"]:
        failed.append("regime_consistency")
        reasons.append("regime inconsistency (%s < %s)" %
                       (_fmt(reg), _fmt(gates["min_regime_consistency"])))
        return _decision(REJECT_INSTABILITY, reasons, passed, failed)
    passed.append("stability")

    mdd = _num("max_drawdown")
    if mdd is not None and mdd < gates["max_drawdown_floor"]:
        failed.append("max_drawdown")
        reasons.append("drawdown beyond floor (%s < %s)" %
                       (_fmt(mdd), _fmt(gates["max_drawdown_floor"])))
        return _decision(REJECT_INSTABILITY, reasons, passed, failed)
    passed.append("max_drawdown")

    # 6. Statistical strength (weak-evidence catch-all).
    ic_t = _num("rank_ic_t")
    ic_pos = _num("rank_ic_positive_ratio")
    spread_t = _num("spread_t")
    oos_ic = _num("oos_ic_mean")
    weak = []
    if ic_t is None or abs(ic_t) < gates["min_ic_t"]:
        weak.append("rank_ic_t (%s < %s)" % (_fmt(ic_t), _fmt(gates["min_ic_t"])))
    if ic_pos is None or ic_pos < gates["min_ic_positive_ratio"]:
        weak.append("ic_positive_ratio (%s < %s)" %
                    (_fmt(ic_pos), _fmt(gates["min_ic_positive_ratio"])))
    if spread_t is None or abs(spread_t) < gates["min_spread_t"]:
        weak.append("spread_t (%s < %s)" %
                    (_fmt(spread_t), _fmt(gates["min_spread_t"])))
    if oos_ic is not None and oos_ic <= gates["min_oos_ic_mean"]:
        weak.append("oos_ic_mean (%s <= %s)" %
                    (_fmt(oos_ic), _fmt(gates["min_oos_ic_mean"])))
    if weak:
        failed.append("statistical_strength")
        reasons.append("weak evidence: " + "; ".join(weak))
        return _decision(REJECT_WEAK_EVIDENCE, reasons, passed, failed)
    passed.append("statistical_strength")

    # 7. Improvement / complementarity vs the relevant control.
    comp = _num("champion_complementarity")
    excess = _num("benchmark_excess_annualized")
    if excess is not None and excess <= 0:
        failed.append("benchmark_excess")
        reasons.append("no excess over benchmark (%s <= 0)" % _fmt(excess))
        return _decision(REJECT_WEAK_EVIDENCE, reasons, passed, failed)
    if comp is not None and comp < gates["min_champion_complementarity"]:
        failed.append("champion_complementarity")
        reasons.append("not complementary to champion (%s < %s)" %
                       (_fmt(comp), _fmt(gates["min_champion_complementarity"])))
        return _decision(REJECT_WEAK_EVIDENCE, reasons, passed, failed)
    passed.append("improvement_vs_control")

    reasons.append("all configured evidence minimums passed")
    return _decision(KEEP_FOR_RESEARCH, reasons, passed, failed)


def _decision(decision: str, reasons: list, passed: list, failed: list) -> dict:
    return {"decision": decision, "reasons": reasons,
            "passed": passed, "failed": failed}


def _fmt(v: Any) -> str:
    if v is None:
        return "n/a"
    try:
        return "%.4g" % float(v)
    except (TypeError, ValueError):
        return str(v)


# --------------------------------------------------------------------------- #
# Config loading + validation.
# --------------------------------------------------------------------------- #
_REQUIRED_CONFIG_KEYS = (
    "stage", "experiments_root", "stage1_registry_root",
    "stage2_ingestion_root", "stage3_director_root", "stage3_5_news_rss_root",
    "operational_ledger_roots", "bounds", "gates", "templates_enabled",
)

_REQUIRED_BOUNDS = ("max_new_hypotheses", "max_specs_per_hypothesis",
                    "max_experiments", "max_runtime_seconds", "max_workers")


class ConfigError(ValueError):
    """Raised when the Stage 5 config is missing or unsafe."""


def load_config(path: str | Path) -> dict:
    """Load, secret-scan and structurally validate the Stage 5 config."""
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
    if str(cfg.get("stage")) != STAGE5_STAGE:
        raise ConfigError("config stage must be '%s'" % STAGE5_STAGE)

    bounds = cfg.get("bounds") or {}
    missing_b = [k for k in _REQUIRED_BOUNDS if k not in bounds]
    if missing_b:
        raise ConfigError("bounds missing required keys: %s" %
                          ", ".join(missing_b))
    for k in _REQUIRED_BOUNDS:
        v = bounds.get(k)
        if not isinstance(v, (int, float)) or v <= 0:
            raise ConfigError("bounds.%s must be a positive number" % k)

    enabled = cfg.get("templates_enabled") or []
    unknown = [t for t in enabled if t not in TEMPLATES]
    if unknown:
        raise ConfigError("templates_enabled has unknown template(s): %s" %
                          ", ".join(unknown))
    return cfg


def enabled_templates(cfg: dict) -> list[str]:
    enabled = list(cfg.get("templates_enabled") or [])
    return [t for t in enabled if t in TEMPLATES]


def bound(cfg: dict, key: str, default: Any) -> Any:
    return (cfg.get("bounds") or {}).get(key, default)


def all_terminal_tokens() -> list[str]:
    return sorted(TERMINAL_TOKENS)
