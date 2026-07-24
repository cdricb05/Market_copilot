"""
api/forward_prediction_skill.py — Phase 28B: FORWARD PREDICTION SKILL, SHADOW
BOOKS AND OUTCOME MATURATION.

This module creates the TRUE FORWARD prediction evidence required to determine
whether the active model and its frozen challengers generate persistent,
cost-adjusted predictive skill: it captures immutable prediction snapshots
BEFORE outcomes are known, matures realized outcomes only after the exact
required number of eligible completed market sessions has passed, computes
paper-only shadow-book P&L under one consistent convention, and derives
cross-sectional prediction-skill metrics (rank IC, bucket spreads, membership
returns) exclusively from TRUE_FORWARD snapshots and matured outcomes.

STRICT SAFETY CONTRACT:
  * It never changes the active operational model, model weights, holdings,
    cash, targets, champion/challenger status or rebalance cadence.
  * It never creates orders, broker instructions or automation.
  * It never backfills fake forward predictions and never uses future
    information: a snapshot is captured only for the exact completed market
    date being closed, only when the frozen model's price-sensitive inputs
    reflect that same date, and never retroactively for an earlier date.
  * Historical reconstruction is NEVER mixed with true forward evidence — every
    snapshot carries forward_evidence_type=TRUE_FORWARD, and every metric here
    is computed only from those snapshots.

STORAGE (existing append-only desk-store conventions; NO database migration):
  * ``forward_prediction_snapshots.json`` — chain-hashed append-only ledger
    (the same ``paper_trading_desk`` primitives as every desk ledger; any
    rewrite of a recorded snapshot breaks the sha256 chain). Two row kinds:
      - MODEL_CROSS_SECTION: the full per-ticker prediction cross-section for
        one model at one market date (ranks, percentiles, normalized scores,
        eligibility, membership, operational-holding join);
      - BOOK_SNAPSHOT: one immutable snapshot per (model, book, market date)
        with ordered target membership, weights, sector exposure,
        concentration, expected turnover/cost vs the model's previous
        snapshot, and full point-in-time provenance.
  * ``forward_prediction_outcomes.json`` — chain-hashed append-only ledger;
    exactly ONE row per (model_id, market_date, horizon), appended only when
    the horizon's eligible-close count has passed. Aggregate metrics are
    frozen at maturation; per-ticker detail is deterministically re-derivable
    from the immutable snapshot + price store.
  * ``forward_prediction_prices.json`` — a completed-close price store with a
    FIRST-WRITE-WINS policy per (ticker, date): once a completed close is
    recorded it is never overwritten, so matured outcomes are replayable. The
    SPY series in this store IS the eligible-session calendar (weekends and
    holidays never count as eligible closes).
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api import paper_trading_desk as desk

PHASE = "28B"
SCHEMA_VERSION = 1

BENCHMARK_TICKER = desk.BENCHMARK_TICKER  # "SPY"
COST_BPS_PER_SIDE = desk.COST_BPS_PER_SIDE  # 12.5 (the desk's fill convention)
COST_RATE_PER_SIDE = desk.COST_RATE_PER_SIDE
TRANSACTION_COST_ASSUMPTION = "12.5 bps per side (25 bps round trip), the desk fill convention"
EQUAL_SHADOW_NOTIONAL = 100_000.0  # equal initial notional for every shadow simulation

# --------------------------------------------------------------------------- #
# Store files (inside the desk store dir, alongside the daily-close journal).
# --------------------------------------------------------------------------- #
SNAPSHOT_LEDGER_FILE = "forward_prediction_snapshots.json"
OUTCOME_LEDGER_FILE = "forward_prediction_outcomes.json"
PRICE_STORE_FILE = "forward_prediction_prices.json"
# Phase 28B.2 — frozen close-artifact bundles (recovery-only source) and the
# append-only evidence-incident ledger (missed captures + recovery audit rows).
ARTIFACT_LEDGER_FILE = "forward_close_artifacts.json"
INCIDENT_LEDGER_FILE = "forward_evidence_incidents.json"

KIND_CROSS_SECTION = "MODEL_CROSS_SECTION"
KIND_BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
KIND_OUTCOME = "OUTCOME"
KIND_ARTIFACT_BUNDLE = "CLOSE_ARTIFACT_BUNDLE"
KIND_CAPTURE_MISSED = "FORWARD_CAPTURE_MISSED"
KIND_RECOVERY = "FORWARD_EVIDENCE_RECOVERED"

TRUE_FORWARD = "TRUE_FORWARD"

# --------------------------------------------------------------------------- #
# Phase 28B.2 — forward-evidence state of a close (SEPARATE from the operational
# close status: an operationally valid close may still be evidence-incomplete).
# --------------------------------------------------------------------------- #
EVIDENCE_COMPLETE = "FORWARD_EVIDENCE_COMPLETE"
EVIDENCE_PARTIAL = "FORWARD_EVIDENCE_PARTIAL"
EVIDENCE_BLOCKED = "FORWARD_EVIDENCE_BLOCKED"

# Recovery statuses (Part E/F).
REC_RECOVERABLE = "RECOVERABLE_FROM_FROZEN_ARTIFACTS"
REC_NOT_RECOVERABLE = "NOT_RECOVERABLE_WITHOUT_RECOMPUTATION"
REC_ALREADY_PRESENT = "SNAPSHOTS_ALREADY_PRESENT"
REC_DATE_NOT_PROCESSED = "DATE_NOT_PROCESSED"

#: The explicit token for the evidence-only recovery POST (never an operator
#: default; never called automatically).
RECOVERY_CONFIRM_TOKEN = "CONFIRM_RECOVER_FROZEN_FORWARD_EVIDENCE"

#: The daily-close decision journal filename. OWNED by api/daily_close.py
#: (DAILY_CLOSE_JOURNAL_FILE); duplicated here read-only to avoid a circular
#: import. A regression test pins the two constants equal.
_CLOSE_JOURNAL_FILE = "daily_close_journal.json"

# --------------------------------------------------------------------------- #
# Supported model/book universe (the active strategy + the frozen challengers).
# --------------------------------------------------------------------------- #
ACTIVE_MODEL_ID = "fundamental_momentum_50_50_v1"
ACTIVE_BOOK_ID = "fundamental_momentum_50_50_top25"

ROLE_ACTIVE = "ACTIVE"
ROLE_SHADOW = "SHADOW"

#: (model_id, book_id, target size, role)
SUPPORTED_BOOKS = (
    (ACTIVE_MODEL_ID, "fundamental_momentum_50_50_top25", 25, ROLE_ACTIVE),
    (ACTIVE_MODEL_ID, "fundamental_momentum_50_50_top50", 50, ROLE_SHADOW),
    ("mom_6_1", "mom_6_1_top25", 25, ROLE_SHADOW),
    ("mom_6_1", "mom_6_1_top50", 50, ROLE_SHADOW),
    ("composite_sn", "composite_sn_top25", 25, ROLE_SHADOW),
    ("composite_sn", "composite_sn_top50", 50, ROLE_SHADOW),
)
SUPPORTED_MODEL_IDS = ("fundamental_momentum_50_50_v1", "mom_6_1", "composite_sn")
SHADOW_BOOK_LABEL = "RESEARCH SHADOW BOOK — NOT EXECUTED HOLDINGS"

#: Phase 28B.2 — the MANDATORY snapshot: the active operational Top-25 book. A
#: fresh close is evidence-complete only when this snapshot is persisted; shadow
#: books are expected but their absence is PARTIAL, not BLOCKED.
MANDATORY_BOOK_ID = ACTIVE_BOOK_ID

# --------------------------------------------------------------------------- #
# Maturation horizons (ELIGIBLE COMPLETED SESSIONS, never calendar days).
# --------------------------------------------------------------------------- #
HORIZONS = (1, 5, 20, 63)

# Maturation / outcome statuses.
OUT_PENDING = "PENDING"
OUT_MATURED = "MATURED"
OUT_COVERAGE_INCOMPLETE = "COVERAGE_INCOMPLETE"
OUT_BENCHMARK_UNAVAILABLE = "BENCHMARK_UNAVAILABLE"
OUT_SYMBOL_UNAVAILABLE = "SYMBOL_UNAVAILABLE"
OUT_NOT_ENOUGH_CLOSES = "NOT_ENOUGH_ELIGIBLE_CLOSES"

#: A matured cross-section needs at least this priced fraction to freeze metrics.
_MIN_OUTCOME_COVERAGE = 0.60
#: Full coverage threshold: at/above this the row is MATURED, below it (but at/
#: above the minimum) the row carries the explicit COVERAGE_INCOMPLETE status.
_FULL_OUTCOME_COVERAGE = 0.95
#: If coverage stays below the minimum, wait this many further eligible closes
#: for the first-write-wins price store to self-heal before freezing the row
#: with an explicit degraded status (never silently, never with invented prices).
_OUTCOME_GRACE_SESSIONS = 5

# Evidence states (Part E).
EV_NO_SNAPSHOTS = "NO_FORWARD_SNAPSHOTS"
EV_OUTCOMES_PENDING = "OUTCOMES_PENDING"
EV_INSUFFICIENT = "INSUFFICIENT_SAMPLE"
EV_PRELIMINARY = "PRELIMINARY_EVIDENCE"
EV_HORIZON_ALIGNED = "HORIZON_ALIGNED_EVIDENCE"
EV_COVERAGE_BLOCKED = "COVERAGE_BLOCKED"

#: Interpretation gates: (min matured obs, state, interpretation). These gates
#: NEVER promote or retire a model — they only bound what may be read into the
#: numbers.
EVIDENCE_GATES = (
    {"min_observations": 0, "max_observations": 4, "state": EV_INSUFFICIENT,
     "interpretation": "Pipeline verification only — no model conclusion."},
    {"min_observations": 5, "max_observations": 19, "state": EV_PRELIMINARY,
     "interpretation": "Preliminary diagnostics only — no model conclusion."},
    {"min_observations": 20, "max_observations": 62, "state": EV_PRELIMINARY,
     "interpretation": "Preliminary challenger comparison — still not horizon-aligned."},
    {"min_observations": 63, "max_observations": None, "state": EV_HORIZON_ALIGNED,
     "interpretation": "First horizon-aligned assessment window."},
)

#: Ratios / annualisation floors (aligned with Phase 27H/28A).
_MIN_RATIO_OBS = 20
#: Shadow-sim daily return requires at least this member-priced fraction; below
#: it the date is an explicit coverage gap (NAV carried flat and counted).
_SHADOW_MIN_DAY_COVERAGE = 0.90
#: Price-capture fetch lookback (calendar days) — enough to self-heal short gaps.
_PRICE_FETCH_LOOKBACK_DAYS = 21

# Research flag codes (Part F).
FLAG_PERSISTENT_NEGATIVE_EXCESS = "PERSISTENT_NEGATIVE_EXCESS"
FLAG_RANK_IC_DEGRADATION = "RANK_IC_DEGRADATION"
FLAG_COST_ADJUSTED_ALPHA_NEGATIVE = "COST_ADJUSTED_ALPHA_NEGATIVE"
FLAG_EXCESSIVE_TURNOVER = "EXCESSIVE_TURNOVER"
FLAG_MEMBERSHIP_INSTABILITY = "MEMBERSHIP_INSTABILITY"
FLAG_SCORE_INSTABILITY = "SCORE_INSTABILITY"
FLAG_SECTOR_CONCENTRATION = "SECTOR_CONCENTRATION"
FLAG_COVERAGE_DEGRADATION = "COVERAGE_DEGRADATION"
FLAG_DRAWDOWN_REVIEW = "DRAWDOWN_REVIEW"
FLAG_INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"

_CROSS_SECTION_ROW_FIELDS = [
    "ticker", "sector", "raw_signal", "normalized_score", "rank", "percentile",
    "eligible", "exclusion_reason", "in_top25", "in_top50",
    "held_operational", "operational_weight",
]


# --------------------------------------------------------------------------- #
# Small helpers.
# --------------------------------------------------------------------------- #
def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _r2(x):
    return None if x is None else round(float(x), 2)


def _r4(x):
    return None if x is None else round(float(x), 4)


def _r6(x):
    return None if x is None else round(float(x), 6)


def _mean(vals):
    return (sum(vals) / len(vals)) if vals else None


def _std(vals):
    n = len(vals)
    if n < 2:
        return None
    m = sum(vals) / n
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1))


def _median(vals):
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _safety(performed_write: bool = False) -> dict:
    """The explicit Phase 28B safety contract attached to every payload."""
    out = {
        "read_only": not performed_write,
        "performed_write": bool(performed_write),
        "paper_only": True,
        "diagnostic_only": True,
        "creates_orders": False,
        "broker_execution": False,
        "broker_enabled": False,
        "automation_enabled": False,
        "changes_operational_model": False,
        "changes_operational_holdings": False,
        "changes_model_weights": False,
        "promotes_challenger": False,
        "retires_model": False,
        "retrains_model": False,
        "prediction_service_used": False,
        "uses_future_information": False,
        "safety_badges": ["TRUE FORWARD EVIDENCE", "PAPER ONLY", "RESEARCH ONLY",
                          "NO ORDERS", "NO PROMOTION", "AUTOMATION OFF"],
    }
    # Phase 28B.1 — stable nested contract for API clients ($payload.safety).
    # Values mirror the top-level fields exactly; the top-level fields stay for
    # compatibility and must never be removed or renamed.
    out["safety"] = {k: out[k] for k in (
        "read_only", "paper_only", "diagnostic_only", "creates_orders",
        "broker_execution", "automation_enabled", "changes_operational_model",
        "changes_operational_holdings", "changes_model_weights",
        "promotes_challenger", "retrains_model")}
    return out


def _sdir(desk_dir):
    return desk._desk_dir(desk_dir)


# --------------------------------------------------------------------------- #
# Price store (completed closes; FIRST-WRITE-WINS per (ticker, date)).
# --------------------------------------------------------------------------- #
def read_price_store(desk_dir=None) -> dict:
    obj = desk._read_json(_sdir(desk_dir) / PRICE_STORE_FILE)
    if isinstance(obj, dict) and isinstance(obj.get("series"), dict):
        return obj
    return {"schema_version": SCHEMA_VERSION,
            "kind": "prediction_price_store_first_write_wins",
            "series": {}, "updated_at": None}


def _merge_prices(desk_dir, new_series: dict[str, list]) -> dict:
    """Merge fetched completed bars into the store. FIRST WRITE WINS: an already
    recorded (ticker, date) close is never overwritten, so previously matured
    outcomes remain replayable byte-for-byte."""
    store = read_price_store(desk_dir)
    series = store.get("series") or {}
    added = 0
    kept = 0
    for tk, bars in (new_series or {}).items():
        have = {d: v for d, v in (series.get(tk) or [])}
        for d, v in bars:
            if d in have:
                kept += 1
                continue
            have[d] = v
            added += 1
        series[tk] = [[d, have[d]] for d in sorted(have)]
    store["series"] = series
    store["schema_version"] = SCHEMA_VERSION
    store["kind"] = "prediction_price_store_first_write_wins"
    store["updated_at"] = _now_iso()
    if added:
        desk._atomic_write_json(_sdir(desk_dir) / PRICE_STORE_FILE, store)
    return {"prices_added": added, "prices_already_recorded": kept,
            "store_written": bool(added)}


def _price_exact(series: dict, ticker: str, d: Optional[str]) -> Optional[float]:
    """The recorded completed close for exactly this (ticker, date), or None."""
    if not d:
        return None
    for row in (series.get(ticker) or []):
        if row[0] == d:
            return _f(row[1])
    return None


def eligible_calendar(desk_dir=None, price_store: Optional[dict] = None) -> list[str]:
    """The eligible completed-session calendar = the recorded SPY completed
    closes (sorted ascending). Weekends/holidays are structurally absent."""
    store = price_store if price_store is not None else read_price_store(desk_dir)
    return sorted(d for d, _v in ((store.get("series") or {}).get(BENCHMARK_TICKER) or []))


# --------------------------------------------------------------------------- #
# Ledger reads.
# --------------------------------------------------------------------------- #
def _snapshot_rows(desk_dir=None) -> list[dict]:
    return desk._read_ledger(_sdir(desk_dir), SNAPSHOT_LEDGER_FILE)


def _outcome_rows(desk_dir=None) -> list[dict]:
    return [r for r in desk._read_ledger(_sdir(desk_dir), OUTCOME_LEDGER_FILE)
            if r.get("kind") == KIND_OUTCOME]


def _book_snapshots(rows: list[dict], book_id: Optional[str] = None) -> list[dict]:
    out = [r for r in rows if r.get("kind") == KIND_BOOK_SNAPSHOT
           and (book_id is None or r.get("book_id") == book_id)]
    return sorted(out, key=lambda r: (r.get("market_date") or "", r.get("book_id") or ""))


def _cross_sections(rows: list[dict], model_id: Optional[str] = None) -> list[dict]:
    out = [r for r in rows if r.get("kind") == KIND_CROSS_SECTION
           and (model_id is None or r.get("model_id") == model_id)]
    return sorted(out, key=lambda r: (r.get("market_date") or "", r.get("model_id") or ""))


def _cs_rows_as_dicts(cs: dict) -> list[dict]:
    fields = cs.get("row_fields") or _CROSS_SECTION_ROW_FIELDS
    return [dict(zip(fields, row)) for row in (cs.get("rows") or [])]


# --------------------------------------------------------------------------- #
# Cross-section construction from the frozen model's current build.
# --------------------------------------------------------------------------- #
def _ops_holdings_map(ops: Optional[dict]) -> dict[str, float]:
    """{ticker: current operational weight} for the actual operational holdings."""
    cs = (ops or {}).get("canonical_state") or {}
    ob_book = (ops or {}).get("operational_book") or {}
    out: dict[str, float] = {}
    for r in (cs.get("holdings_detail") or ob_book.get("holdings_detail") or []):
        tk = r.get("ticker")
        if not tk:
            continue
        w = _f(r.get("current_weight") if r.get("current_weight") is not None
               else r.get("weight"))
        out[str(tk).upper()] = w if w is not None else 0.0
    if not out:
        for tk in (ob_book.get("holdings") or {}):
            out[str(tk).upper()] = 0.0
    return out


def _family_membership(cur: dict, model_id: str) -> tuple[set, set]:
    books = ((cur.get("books") or {}).get("books")) or {}
    prefix = {"fundamental_momentum_50_50_v1": "fundamental_momentum_50_50",
              "mom_6_1": "mom_6_1", "composite_sn": "composite_sn"}[model_id]
    top25 = {c["ticker"] for c in (books.get(prefix + "_top25") or {}).get("constituents", [])}
    top50 = {c["ticker"] for c in (books.get(prefix + "_top50") or {}).get("constituents", [])}
    return top25, top50


def _build_cross_section(cur: dict, model_id: str, market_date: str,
                         holdings_map: dict[str, float]) -> dict:
    """One immutable per-ticker prediction cross-section for one model."""
    top25, top50 = _family_membership(cur, model_id)
    rows: list[list] = []
    n_eligible = 0
    if model_id == "fundamental_momentum_50_50_v1":
        source_map = ((cur.get("combined") or {}).get("combined")) or {}
        for tk in sorted(source_map):
            c = source_map[tk]
            rows.append([tk, c.get("sector") or "Unknown",
                         _r6(_f(c.get("combined_score"))), _r6(_f(c.get("combined_score"))),
                         c.get("rank"), _r6(_f(c.get("percentile"))), 1, None,
                         int(tk in top25), int(tk in top50),
                         int(tk in holdings_map), _r6(holdings_map.get(tk))])
            n_eligible += 1
        universe_as_of = cur.get("momentum_month")
    else:
        source_map = ((cur.get("scores") or {}).get(model_id)) or {}
        for tk in sorted(source_map):
            sc = source_map[tk]
            eligible = bool(sc.get("eligible"))
            rows.append([tk, sc.get("sector") or "Unknown",
                         _r6(_f(sc.get("raw_signal"))), _r6(_f(sc.get("normalized_score"))),
                         sc.get("rank") if eligible else None,
                         _r6(_f(sc.get("percentile"))) if eligible else None,
                         int(eligible), sc.get("exclusion_reason"),
                         int(tk in top25), int(tk in top50),
                         int(tk in holdings_map), _r6(holdings_map.get(tk))])
            if eligible:
                n_eligible += 1
        universe_as_of = (cur.get("fundamental_month") if model_id == "composite_sn"
                          else cur.get("momentum_month"))
    validations = ((cur.get("inputs") or {}).get("validations")) or {}
    model = mreg.model_by_id(model_id) or {}
    return {
        "kind": KIND_CROSS_SECTION,
        "schema_version": SCHEMA_VERSION,
        "model_id": model_id,
        "model_version": model.get("model_version") or "v1",
        "market_date": market_date,
        "source": "multi_horizon_engine.build_current (frozen model, owned local inputs)",
        "forward_evidence_type": TRUE_FORWARD,
        "created_at": _now_iso(),
        "price_data_through": market_date,
        "fundamental_data_as_of": cur.get("fundamental_as_of_date"),
        "universe_as_of": universe_as_of,
        "sector_metadata_as_of": "current owned Phase 10-F repaired GICS map (grouping only)",
        "benchmark_date": market_date,
        "eligible_universe_count": n_eligible,
        "universe_count": len(rows),
        "input_coverage": {
            "fundamental_names": validations.get("fundamental_names"),
            "momentum_names": validations.get("momentum_names"),
            "momentum_eligible": validations.get("momentum_eligible"),
            "risk_names": validations.get("risk_names"),
            "sector_map_names": validations.get("sector_map_names"),
            "fundamental_sector_coverage": validations.get("fundamental_sector_coverage"),
        },
        "row_fields": list(_CROSS_SECTION_ROW_FIELDS),
        "rows": rows,
    }


def _score_distribution(cur: dict, model_id: str) -> dict:
    if model_id == "fundamental_momentum_50_50_v1":
        vals = [_f(c.get("combined_score"))
                for c in (((cur.get("combined") or {}).get("combined")) or {}).values()]
    else:
        vals = [_f(sc.get("raw_signal"))
                for sc in (((cur.get("scores") or {}).get(model_id)) or {}).values()
                if sc.get("eligible")]
    vals = [v for v in vals if v is not None]
    return {"n": len(vals), "mean": _r6(_mean(vals)), "std": _r6(_std(vals)),
            "min": _r6(min(vals)) if vals else None,
            "max": _r6(max(vals)) if vals else None}


def _previous_book_snapshot(rows: list[dict], book_id: str,
                            before_date: str) -> Optional[dict]:
    prior = [r for r in _book_snapshots(rows, book_id)
             if (r.get("market_date") or "") < before_date]
    return prior[-1] if prior else None


def _build_book_snapshot(cur: dict, *, model_id: str, book_id: str, size: int,
                         role: str, market_date: str, existing_rows: list[dict],
                         cross_section: dict) -> Optional[dict]:
    book = (((cur.get("books") or {}).get("books")) or {}).get(book_id)
    if not book:
        return None
    model = mreg.model_by_id(model_id) or {}
    members = [c["ticker"] for c in (book.get("constituents") or [])]
    weights = {c["ticker"]: _r6(_f(c.get("weight"))) for c in (book.get("constituents") or [])}
    prev = _previous_book_snapshot(existing_rows, book_id, market_date)
    prev_members = set((prev or {}).get("membership") or [])
    if prev_members:
        cur_set = set(members)
        churn = len(cur_set - prev_members) + len(prev_members - cur_set)
        turnover = round(churn / ((len(cur_set) + len(prev_members)) or 1), 6)
        stability = round(len(cur_set & prev_members)
                          / (max(len(cur_set), len(prev_members)) or 1), 6)
        expected_cost = _r6(turnover * 2.0 * COST_RATE_PER_SIDE)
    else:
        turnover = 1.0 if members else None
        stability = None
        expected_cost = _r6(COST_RATE_PER_SIDE) if members else None
    exposure = book.get("sector_exposure") or {}
    top_sector = max(exposure.items(), key=lambda kv: kv[1])[0] if exposure else None
    return {
        "kind": KIND_BOOK_SNAPSHOT,
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": "fps_%s_%s" % (book_id, market_date),
        "market_date": market_date,
        "created_at": _now_iso(),
        "source": "multi_horizon_engine.build_current (frozen model, owned local inputs)",
        "forward_evidence_type": TRUE_FORWARD,
        "model_id": model_id,
        "model_version": model.get("model_version") or "v1",
        "book_id": book_id,
        "book_role": role,
        "book_size": {"target": size, "actual": book.get("size_actual")},
        "horizon": {"evaluation_horizons_eligible_closes": list(HORIZONS),
                    "signal_horizon": model.get("signal_horizon")},
        "rebalance_cadence": model.get("rebalance_frequency"),
        "transaction_cost_assumption": TRANSACTION_COST_ASSUMPTION,
        "price_data_through": market_date,
        "fundamental_data_as_of": cur.get("fundamental_as_of_date"),
        "universe_as_of": cross_section.get("universe_as_of"),
        "sector_metadata_as_of": cross_section.get("sector_metadata_as_of"),
        "benchmark_date": market_date,
        "input_coverage": cross_section.get("input_coverage"),
        "eligible_universe_count": cross_section.get("eligible_universe_count"),
        "membership": members,
        "target_weights": weights,
        "equal_weight": _r6(_f(book.get("equal_weight"))),
        "unallocated_weight": _r6(_f(book.get("unallocated_weight"))),
        "sector_exposure": exposure,
        "concentration": {
            "top_sector": top_sector,
            "top_sector_weight": exposure.get(top_sector) if top_sector else None,
            "max_individual_weight": _r6(_f(book.get("equal_weight"))),
            "sector_cap_fraction": book.get("sector_cap_fraction"),
        },
        "previous_snapshot_id": (prev or {}).get("snapshot_id"),
        "expected_turnover_vs_previous": turnover,
        "membership_stability_vs_previous": stability,
        "expected_transaction_cost_fraction": expected_cost,
        "score_distribution": _score_distribution(cur, model_id),
    }


# --------------------------------------------------------------------------- #
# Phase 28B.2 Part D — the FROZEN CLOSE-ARTIFACT BUNDLE. The exact point-in-time
# rows built by a fresh close are frozen (append-only, content-hashed) BEFORE the
# snapshot append, so a close whose snapshot persistence fails can later be
# recovered WITHOUT any recalculation, provider refresh or hindsight.
# --------------------------------------------------------------------------- #
def _content_hash(obj: Any) -> str:
    """Deterministic sha256 of the canonical JSON encoding of ``obj``."""
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"),
                                     default=str).encode("utf-8")).hexdigest()


def _artifact_rows(desk_dir=None) -> list[dict]:
    return [r for r in desk._read_ledger(_sdir(desk_dir), ARTIFACT_LEDGER_FILE)
            if r.get("kind") == KIND_ARTIFACT_BUNDLE]


def find_artifact_bundle(market_date: Optional[str], desk_dir=None) -> Optional[dict]:
    md = str(market_date or "")[:10]
    for r in _artifact_rows(desk_dir):
        if r.get("market_date") == md:
            return r  # first write wins — at most one bundle per market date
    return None


def _freeze_close_artifacts(*, desk_dir, market_date: str,
                            cross_sections: list[dict],
                            book_snapshots: list[dict]) -> dict:
    """Persist the frozen artifact bundle for one close (idempotent: an existing
    bundle for the date is never rewritten). The bundle holds the EXACT rows the
    close built — per-ticker scores/ranks/eligibility/sectors, membership,
    target weights, provenance dates and source identifiers — plus a
    deterministic content hash, so recovery can only ever replay, never rebuild."""
    existing = find_artifact_bundle(market_date, desk_dir)
    if existing is not None:
        return {"artifact_bundle_id": existing.get("artifact_bundle_id"),
                "artifact_hash": existing.get("artifact_hash"),
                "already_present": True, "performed_write": False}
    artifacts = {"cross_sections": cross_sections, "book_snapshots": book_snapshots}
    content_hash = _content_hash(artifacts)
    row = {
        "kind": KIND_ARTIFACT_BUNDLE,
        "schema_version": SCHEMA_VERSION,
        "artifact_bundle_id": "fca_%s" % market_date,
        "market_date": market_date,
        "created_at": _now_iso(),
        "source": "multi_horizon_engine.build_current (frozen model, owned local "
                  "inputs) — frozen during the original daily close",
        "forward_evidence_type": TRUE_FORWARD,
        "model_ids": sorted({r.get("model_id") for r in cross_sections
                             if r.get("model_id")}),
        "book_ids": sorted({r.get("book_id") for r in book_snapshots
                            if r.get("book_id")}),
        "artifact_hash": content_hash,
        "artifacts": artifacts,
        "note": ("Recovery-only frozen close artifacts. Consumed exclusively by the "
                 "token-gated evidence recovery path; never recalculated, never "
                 "refreshed from a provider, never used for any operational decision."),
    }
    try:
        desk._append_ledger(_sdir(desk_dir), ARTIFACT_LEDGER_FILE, [row])
    except Exception as exc:  # noqa: BLE001 — the bundle is belt-and-suspenders
        return {"artifact_bundle_id": None, "artifact_hash": None,
                "already_present": False, "performed_write": False,
                "error": str(exc)[:160]}
    return {"artifact_bundle_id": row["artifact_bundle_id"],
            "artifact_hash": content_hash,
            "already_present": False, "performed_write": True}


# --------------------------------------------------------------------------- #
# Part A — capture immutable TRUE_FORWARD snapshots (append-only, idempotent).
# --------------------------------------------------------------------------- #
def _capture_base(market_date: Optional[str]) -> dict:
    return {
        "phase": PHASE,
        "market_date": market_date,
        "snapshots_expected": len(SUPPORTED_BOOKS),
        "snapshots_created": 0,
        "snapshots_already_present": 0,
        "snapshots_unavailable": 0,
        "unavailable_reasons": {},
        "outcomes_newly_matured": 0,
        "idempotent": True,
        "performed_write": False,
        "created_orders": False,
        "changed_operational_model": False,
        "forward_evidence_type": TRUE_FORWARD,
        # Phase 28B.2 Part H — the explicit close-finalization evidence contract.
        "mandatory_book_id": MANDATORY_BOOK_ID,
        "mandatory_active_snapshot_created": False,
        "mandatory_active_snapshot_persisted": False,
        "persisted_snapshot_ids": [],
        "verification_complete": False,
        "artifact_bundle_id": None,
        "artifact_hash": None,
        "evidence_status": EVIDENCE_BLOCKED,
    }


def _classify_evidence(*, created: int, present: int, unavailable: int,
                       mandatory_persisted: bool) -> str:
    """Part B — mandatory-vs-optional classification. The active Top-25 snapshot
    decides BLOCKED; a shadow-only gap is explicit PARTIAL; zero-of-six is never
    an ordinary success."""
    if not mandatory_persisted:
        return EVIDENCE_BLOCKED
    if unavailable:
        return EVIDENCE_PARTIAL
    return EVIDENCE_COMPLETE if (created or present) else EVIDENCE_BLOCKED


def _progress_call(progress: Optional[Callable], stage: str) -> None:
    """Best-effort progress signal to the caller's UI writer (never raises)."""
    if progress is None:
        return
    try:
        progress(stage)
    except Exception:  # noqa: BLE001 — progress is display-only
        pass


def capture_snapshots(*, market_date: str, desk_dir=None,
                      current: Optional[dict] = None,
                      engine_loader: Optional[Callable] = None,
                      ops: Optional[dict] = None,
                      downloader=None,
                      fetch_prices: bool = True,
                      progress: Optional[Callable] = None) -> dict:
    """Capture the immutable TRUE_FORWARD prediction snapshots for ONE completed
    market date (Part A). Append-only and idempotent by (model, book, date);
    a re-run creates nothing. Never retroactive: a date earlier than an already
    captured snapshot is refused (that would be backfilling 'forward' evidence).

    Phase 28B.2 ATOMIC ORDERING: the point-in-time artifacts are FROZEN, the
    snapshot rows are APPENDED and then VERIFIED READABLE from storage BEFORE the
    (slow, network-bound) maturity-price capture runs — so the evidence is durable
    the moment it exists in memory, and a crash during the price fetch can no
    longer lose a close's forward snapshots."""
    md = str(market_date or "")[:10]
    out = _capture_base(md)
    if not md:
        out["status"] = "SNAPSHOTS_UNAVAILABLE"
        out["snapshots_unavailable"] = len(SUPPORTED_BOOKS)
        out["unavailable_reasons"] = {b[1]: "NO_MARKET_DATE" for b in SUPPORTED_BOOKS}
        return out

    rows = _snapshot_rows(desk_dir)

    # NEVER retroactive: refuse any date at/before the latest captured snapshot
    # unless it is exactly a re-run of that same date (idempotent no-op).
    latest = max((r.get("market_date") or "" for r in rows
                  if r.get("kind") == KIND_BOOK_SNAPSHOT), default=None)
    if latest and md < latest:
        out["status"] = "SNAPSHOTS_UNAVAILABLE"
        out["snapshots_unavailable"] = len(SUPPORTED_BOOKS)
        out["unavailable_reasons"] = {
            b[1]: "NO_RETROACTIVE_TRUE_FORWARD: snapshots exist through %s; a %s "
                  "snapshot created now would be backfilled, not forward." % (latest, md)
            for b in SUPPORTED_BOOKS}
        return out

    existing_books = {(r.get("model_id"), r.get("book_id"))
                     for r in rows if r.get("kind") == KIND_BOOK_SNAPSHOT
                     and r.get("market_date") == md}
    existing_cs = {r.get("model_id") for r in rows
                   if r.get("kind") == KIND_CROSS_SECTION and r.get("market_date") == md}

    cur = current
    if cur is None:
        try:
            cur = (engine_loader or eng.build_current)()
        except Exception as exc:  # noqa: BLE001 — degrade, never crash the close
            cur = {"status": "ENGINE_ERROR", "error": str(exc)[:160]}
    engine_ready = bool(cur and cur.get("status") == eng.STATUS_READY)
    model_md = str((cur or {}).get("market_as_of_date") or "")[:10] or None

    if not engine_ready:
        out["status"] = "SNAPSHOTS_UNAVAILABLE"
        out["snapshots_unavailable"] = len(SUPPORTED_BOOKS)
        out["unavailable_reasons"] = {
            b[1]: "MODEL_INPUTS_UNAVAILABLE (%s)" % ((cur or {}).get("status") or "no build")
            for b in SUPPORTED_BOOKS}
        return out
    if model_md != md:
        # The price-sensitive model inputs do not reflect this completed session:
        # a snapshot stamped with this date would NOT be point-in-time-honest.
        out["status"] = "SNAPSHOTS_UNAVAILABLE"
        out["snapshots_unavailable"] = len(SUPPORTED_BOOKS)
        out["unavailable_reasons"] = {
            b[1]: "MODEL_DATE_MISMATCH: model inputs reflect %s, not the closed "
                  "session %s — no snapshot is fabricated." % (model_md, md)
            for b in SUPPORTED_BOOKS}
        return out

    _progress_call(progress, "CAPTURE_FORWARD_BOOKS")
    holdings_map = _ops_holdings_map(ops)
    cross_by_model: dict[str, dict] = {}
    for model_id in SUPPORTED_MODEL_IDS:
        cross_by_model[model_id] = _build_cross_section(cur, model_id, md, holdings_map)

    created, present, unavailable = 0, 0, 0
    reasons: dict[str, str] = {}
    book_rows: list[dict] = []
    created_models: set = set()
    for model_id, book_id, size, role in SUPPORTED_BOOKS:
        if (model_id, book_id) in existing_books:
            present += 1
            continue
        snap = _build_book_snapshot(cur, model_id=model_id, book_id=book_id, size=size,
                                    role=role, market_date=md, existing_rows=rows,
                                    cross_section=cross_by_model[model_id])
        if snap is None:
            unavailable += 1
            reasons[book_id] = ("BOOK_UNAVAILABLE: the frozen model build did not "
                                "produce this book from the owned inputs.")
            continue
        book_rows.append(snap)
        created += 1
        created_models.add(model_id)

    # A cross-section is stored only alongside its family's book snapshot(s): a
    # build that produced no book appends nothing (no orphan / empty evidence).
    new_cs: list[dict] = [cross_by_model[m] for m in SUPPORTED_MODEL_IDS
                          if m in created_models and m not in existing_cs]
    new_rows: list[dict] = new_cs + book_rows

    # Phase 28B.2 STEP 1 — FREEZE the exact point-in-time artifacts (Part D)
    # before anything else is persisted: the recovery source of last resort.
    bundle = {"artifact_bundle_id": None, "artifact_hash": None,
              "already_present": False, "performed_write": False}
    if book_rows:
        bundle = _freeze_close_artifacts(desk_dir=desk_dir, market_date=md,
                                         cross_sections=new_cs,
                                         book_snapshots=book_rows)
    elif present:
        prior_bundle = find_artifact_bundle(md, desk_dir)
        if prior_bundle is not None:
            bundle = {"artifact_bundle_id": prior_bundle.get("artifact_bundle_id"),
                      "artifact_hash": prior_bundle.get("artifact_hash"),
                      "already_present": True, "performed_write": False}

    # STEP 2 — APPEND the immutable snapshots IMMEDIATELY (before the slow price
    # fetch). A failed append is explicit and recoverable from the frozen bundle.
    appended = False
    if new_rows:
        try:
            desk._append_ledger(_sdir(desk_dir), SNAPSHOT_LEDGER_FILE, new_rows)
            appended = True
        except Exception as exc:  # noqa: BLE001 — surfaced, never silent
            err = str(exc)[:120]
            for snap in book_rows:
                reasons[snap["book_id"]] = (
                    "SNAPSHOT_APPEND_FAILED: %s — the frozen close artifacts were "
                    "preserved for token-gated evidence recovery." % err)
            unavailable += created
            created = 0
            book_rows = []
            new_rows = []

    # STEP 3 — VERIFY the required snapshots are actually readable from storage.
    try:
        stored = _snapshot_rows(desk_dir)
    except Exception:  # noqa: BLE001
        stored = []
    stored_ids = {r.get("snapshot_id") for r in stored
                  if r.get("kind") == KIND_BOOK_SNAPSHOT and r.get("market_date") == md}
    expected_ids = {"fps_%s_%s" % (b[1], md) for b in SUPPORTED_BOOKS
                    if (b[0], b[1]) in existing_books
                    or any(s["book_id"] == b[1] for s in book_rows)}
    verification_complete = bool(expected_ids) and expected_ids <= stored_ids
    mandatory_persisted = ("fps_%s_%s" % (MANDATORY_BOOK_ID, md)) in stored_ids

    # STEP 4 — the slow maturity-price capture (network-bound; runs AFTER the
    # evidence is durable so a crash here can no longer lose snapshots).
    price_capture = {"prices_added": 0, "prices_already_recorded": 0,
                     "store_written": False, "tickers_requested": 0,
                     "tickers_priced": 0, "tickers_failed": 0,
                     "benchmark_priced": None, "fetched": False}
    if fetch_prices and (created or present):
        _progress_call(progress, "CAPTURE_MATURITY_PRICES")
        price_capture = _capture_prices(desk_dir=desk_dir, market_date=md,
                                        rows=(stored if stored else rows + new_rows),
                                        downloader=downloader)

    out.update({
        "status": ("SNAPSHOTS_CAPTURED" if created else
                   "SNAPSHOTS_ALREADY_PRESENT" if present else "SNAPSHOTS_UNAVAILABLE"),
        "snapshots_created": created,
        "snapshots_already_present": present,
        "snapshots_unavailable": unavailable,
        "unavailable_reasons": reasons,
        "performed_write": appended or bool(price_capture.get("store_written"))
                           or bool(bundle.get("performed_write")),
        "model_calc_date": model_md,
        "fundamental_data_as_of": cur.get("fundamental_as_of_date"),
        "price_capture": price_capture,
        "mandatory_active_snapshot_created": any(
            s["book_id"] == MANDATORY_BOOK_ID for s in book_rows),
        "mandatory_active_snapshot_persisted": mandatory_persisted,
        "persisted_snapshot_ids": sorted(stored_ids),
        "verification_complete": verification_complete,
        "artifact_bundle_id": bundle.get("artifact_bundle_id"),
        "artifact_hash": bundle.get("artifact_hash"),
        "evidence_status": _classify_evidence(
            created=created, present=present, unavailable=unavailable,
            mandatory_persisted=mandatory_persisted),
    })
    return out


def _pending_price_tickers(rows: list[dict], outcome_rows: list[dict]) -> set:
    """Tickers still owed a maturity price: every eligible name of any cross-
    section that does not yet have all horizon outcome rows."""
    done = {(o.get("model_id"), o.get("market_date"), o.get("horizon"))
            for o in outcome_rows}
    pending: set = set()
    for cs in _cross_sections(rows):
        key_all = all((cs.get("model_id"), cs.get("market_date"), h) in done
                      for h in HORIZONS)
        if key_all:
            continue
        for r in _cs_rows_as_dicts(cs):
            if r.get("eligible"):
                pending.add(r["ticker"])
    return pending


def _capture_prices(*, desk_dir, market_date: str, rows: list[dict],
                    downloader) -> dict:
    """Record completed closes (through the closed date) for the union of every
    eligible snapshot ticker with an unmatured horizon, plus SPY. Uses the SAME
    owned transport seam as the desk; per-ticker failures are isolated and
    explicit. First write wins — recorded closes are never restated."""
    outcome_rows = _outcome_rows(desk_dir)
    tickers = _pending_price_tickers(rows, outcome_rows)
    tickers.add(BENCHMARK_TICKER)
    try:
        dl, source = desk._resolve_downloader(downloader)
    except Exception as exc:  # noqa: BLE001
        return {"prices_added": 0, "prices_already_recorded": 0, "store_written": False,
                "tickers_requested": len(tickers), "tickers_priced": 0,
                "tickers_failed": len(tickers), "benchmark_priced": False,
                "fetched": False, "error": str(exc)[:120]}
    start = (date.fromisoformat(market_date)
             - timedelta(days=_PRICE_FETCH_LOOKBACK_DAYS)).isoformat()
    cutoff = date.fromisoformat(market_date) + timedelta(days=1)
    fetched: dict[str, list] = {}
    failed: list[str] = []
    for tk in sorted(tickers):
        try:
            payload = dl(desk._clean_symbol(tk), start)
            bars = desk._completed_bars(desk._normalize_bars(payload), cutoff)
        except Exception:  # noqa: BLE001 — per-ticker isolation; key never handled here
            failed.append(tk)
            continue
        if bars:
            fetched[tk] = [[d, v] for d, v in bars]
        else:
            failed.append(tk)
    merge = _merge_prices(desk_dir, fetched)
    return {**merge,
            "fetched": True,
            "source": source,
            "tickers_requested": len(tickers),
            "tickers_priced": len(fetched),
            "tickers_failed": len(failed),
            "failed_tickers": sorted(failed)[:25],
            "benchmark_priced": BENCHMARK_TICKER in fetched,
            "fetch_window_start": start,
            "completed_through": market_date}


# --------------------------------------------------------------------------- #
# Spearman rank IC (deterministic; unique integer prediction ranks).
# --------------------------------------------------------------------------- #
def _spearman_ic(pairs: list[tuple[float, float, str]]) -> Optional[float]:
    """Spearman rho between prediction rank (1 = best) and realized return.
    ``pairs`` = [(prediction_rank, realized_return, ticker)]. Return ranks are
    assigned descending (highest return = rank 1) with a deterministic ticker
    tie-break, so an aligned ordering yields +1."""
    n = len(pairs)
    if n < 3:
        return None
    by_ret = sorted(pairs, key=lambda p: (-p[1], p[2]))
    ret_rank = {p[2]: i + 1 for i, p in enumerate(by_ret)}
    by_pred = sorted(pairs, key=lambda p: (p[0], p[2]))
    pred_rank = {p[2]: i + 1 for i, p in enumerate(by_pred)}
    d2 = sum((pred_rank[p[2]] - ret_rank[p[2]]) ** 2 for p in pairs)
    return round(1.0 - (6.0 * d2) / (n * (n * n - 1)), 6)


# --------------------------------------------------------------------------- #
# Part C — OUTCOME MATURATION (eligible completed sessions only; idempotent).
# --------------------------------------------------------------------------- #
def _derive_outcome(cs: dict, *, horizon: int, maturity_date: str,
                    price_series: dict) -> dict:
    """Deterministically derive the per-ticker outcomes for one cross-section at
    one horizon from the immutable snapshot + first-write-wins price store."""
    d0 = cs.get("market_date")
    bench_entry = _price_exact(price_series, BENCHMARK_TICKER, d0)
    bench_mat = _price_exact(price_series, BENCHMARK_TICKER, maturity_date)
    bench_ret = ((bench_mat / bench_entry - 1.0) * 100.0
                 if (bench_entry and bench_mat) else None)
    tick_rows: list[dict] = []
    priced = 0
    total = 0
    for r in _cs_rows_as_dicts(cs):
        if not r.get("eligible"):
            continue
        total += 1
        tk = r["ticker"]
        p0 = _price_exact(price_series, tk, d0)
        p1 = _price_exact(price_series, tk, maturity_date)
        if p0 and p1:
            ret = (p1 / p0 - 1.0) * 100.0
            priced += 1
            status = "OK"
        else:
            ret = None
            status = OUT_SYMBOL_UNAVAILABLE
        excess = (ret - bench_ret) if (ret is not None and bench_ret is not None) else None
        tick_rows.append({"ticker": tk, "rank": r.get("rank"),
                          "percentile": r.get("percentile"),
                          "normalized_score": r.get("normalized_score"),
                          "in_top25": bool(r.get("in_top25")),
                          "in_top50": bool(r.get("in_top50")),
                          "entry_price": _r4(p0), "maturity_price": _r4(p1),
                          "total_return_pct": _r4(ret),
                          "benchmark_return_pct": _r4(bench_ret),
                          "excess_return_pct": _r4(excess),
                          "realized_positive": (None if ret is None else bool(ret > 0)),
                          "status": status})
    return {"snapshot_market_date": d0, "horizon": horizon,
            "maturity_market_date": maturity_date,
            "benchmark": {"ticker": BENCHMARK_TICKER,
                          "entry_price": _r4(bench_entry),
                          "maturity_price": _r4(bench_mat),
                          "return_pct": _r4(bench_ret)},
            "benchmark_available": bench_ret is not None,
            "rows": tick_rows,
            "coverage": {"priced": priced, "total": total,
                         "fraction": (round(priced / total, 4) if total else 0.0)}}


def _outcome_metrics(detail: dict) -> Optional[dict]:
    rows = [r for r in detail.get("rows") or [] if r.get("total_return_pct") is not None]
    if not rows:
        return None
    pairs = [(float(r["rank"]), float(r["total_return_pct"]), r["ticker"])
             for r in rows if r.get("rank") is not None]
    ic = _spearman_ic(pairs)
    n = len(rows)
    rets = [r["total_return_pct"] for r in rows]
    by_pct = [r for r in rows if r.get("percentile") is not None]
    top_dec = [r["total_return_pct"] for r in by_pct if r["percentile"] >= 0.9]
    bot_dec = [r["total_return_pct"] for r in by_pct if r["percentile"] <= 0.1]
    top25 = [r["total_return_pct"] for r in rows if r.get("in_top25")]
    top50 = [r["total_return_pct"] for r in rows if r.get("in_top50")]
    bench = detail.get("benchmark", {}).get("return_pct")
    top_avg = _mean(top_dec)
    bot_avg = _mean(bot_dec)
    t25 = _mean(top25)
    t50 = _mean(top50)
    return {
        "rank_ic_spearman": ic,
        "n_ic_names": len(pairs),
        "n_priced": n,
        "universe_avg_return_pct": _r4(_mean(rets)),
        "top_decile_return_pct": _r4(top_avg),
        "bottom_decile_return_pct": _r4(bot_avg),
        "top_minus_bottom_pp": _r4(top_avg - bot_avg) if (top_avg is not None
                                                          and bot_avg is not None) else None,
        "top25_avg_return_pct": _r4(t25),
        "top50_avg_return_pct": _r4(t50),
        "benchmark_return_pct": _r4(bench),
        "top25_excess_pp": _r4(t25 - bench) if (t25 is not None and bench is not None) else None,
        "top50_excess_pp": _r4(t50 - bench) if (t50 is not None and bench is not None) else None,
        "top25_hit_rate_pct": _r4(100.0 * sum(1 for x in top25 if x > 0) / len(top25))
                              if top25 else None,
    }


def mature_outcomes(*, desk_dir=None) -> dict:
    """Mature realized outcomes for every stored cross-section whose horizon has
    passed the EXACT required number of eligible completed sessions (Part C).

    * The eligible calendar is the recorded SPY completed-close series — never
      calendar-day arithmetic, so weekends/holidays never count.
    * Exactly one immutable OUTCOME row per (model, snapshot date, horizon);
      re-processing appends nothing (idempotent).
    * The original snapshot is never touched; prices are first-write-wins; a
      cross-section below the coverage floor waits a bounded number of further
      sessions for the price store to self-heal, then freezes with an explicit
      COVERAGE_INCOMPLETE / BENCHMARK_UNAVAILABLE status (metrics withheld)."""
    store = read_price_store(desk_dir)
    series = store.get("series") or {}
    calendar = eligible_calendar(desk_dir, price_store=store)
    rows = _snapshot_rows(desk_dir)
    existing = {(o.get("model_id"), o.get("market_date"), o.get("horizon"))
                for o in _outcome_rows(desk_dir)}
    new_rows: list[dict] = []
    pending: list[dict] = []
    for cs in _cross_sections(rows):
        model_id = cs.get("model_id")
        d0 = cs.get("market_date")
        after = [d for d in calendar if d > (d0 or "")]
        for h in HORIZONS:
            key = (model_id, d0, h)
            if key in existing:
                continue
            if len(after) < h:
                pending.append({"model_id": model_id, "market_date": d0, "horizon": h,
                                "status": OUT_PENDING,
                                "detail": OUT_NOT_ENOUGH_CLOSES,
                                "eligible_closes_elapsed": len(after),
                                "eligible_closes_required": h})
                continue
            maturity_date = after[h - 1]
            sessions_past = len(after) - h
            detail = _derive_outcome(cs, horizon=h, maturity_date=maturity_date,
                                     price_series=series)
            frac = detail["coverage"]["fraction"]
            bench_ok = detail["benchmark_available"]
            if (not bench_ok or frac < _MIN_OUTCOME_COVERAGE) \
                    and sessions_past < _OUTCOME_GRACE_SESSIONS:
                pending.append({"model_id": model_id, "market_date": d0, "horizon": h,
                                "status": OUT_PENDING,
                                "detail": (OUT_BENCHMARK_UNAVAILABLE if not bench_ok
                                           else OUT_COVERAGE_INCOMPLETE),
                                "coverage_fraction": frac,
                                "grace_sessions_remaining":
                                    _OUTCOME_GRACE_SESSIONS - sessions_past})
                continue
            if not bench_ok:
                status, metrics = OUT_BENCHMARK_UNAVAILABLE, None
            elif frac < _MIN_OUTCOME_COVERAGE:
                status, metrics = OUT_COVERAGE_INCOMPLETE, None
            elif frac < _FULL_OUTCOME_COVERAGE:
                status, metrics = OUT_COVERAGE_INCOMPLETE, _outcome_metrics(detail)
            else:
                status, metrics = OUT_MATURED, _outcome_metrics(detail)
            missing = [r["ticker"] for r in detail["rows"]
                       if r["status"] == OUT_SYMBOL_UNAVAILABLE]
            member_rows = [[r["ticker"], r["rank"], int(r["in_top25"]),
                            r["total_return_pct"], r["excess_return_pct"], r["status"]]
                           for r in detail["rows"] if r["in_top50"]]
            new_rows.append({
                "kind": KIND_OUTCOME,
                "schema_version": SCHEMA_VERSION,
                "outcome_id": "out_%s_%s_h%d" % (model_id, d0, h),
                "model_id": model_id,
                "market_date": d0,
                "horizon": h,
                "maturity_market_date": maturity_date,
                "status": status,
                "forward_evidence_type": TRUE_FORWARD,
                "benchmark": detail["benchmark"],
                "coverage": {**detail["coverage"],
                             "missing_tickers": sorted(missing)[:25]},
                "metrics": metrics,
                "member_outcome_fields": ["ticker", "rank", "in_top25",
                                          "total_return_pct", "excess_return_pct",
                                          "status"],
                "member_outcomes": member_rows,
                "derivation_note": (
                    "Aggregates frozen at maturation from the immutable snapshot + "
                    "first-write-wins price store; per-ticker detail is deterministically "
                    "re-derivable and the original prediction is never modified."),
            })
    if new_rows:
        desk._append_ledger(_sdir(desk_dir), OUTCOME_LEDGER_FILE, new_rows)
    return {"outcomes_newly_matured": len(new_rows),
            "pending_outcomes": pending,
            "pending_outcome_count": len(pending),
            "performed_write": bool(new_rows)}


def derive_outcome_detail(*, model_id: str, market_date: str, horizon: int,
                          desk_dir=None) -> dict:
    """Read-only, deterministic per-ticker outcome detail for one matured
    (model, date, horizon) — re-derived from the immutable stores."""
    rows = _snapshot_rows(desk_dir)
    cs = next((c for c in _cross_sections(rows, model_id)
               if c.get("market_date") == market_date), None)
    if cs is None:
        return {"status": "SNAPSHOT_NOT_FOUND", "model_id": model_id,
                "market_date": market_date, "horizon": horizon}
    outcome = next((o for o in _outcome_rows(desk_dir)
                    if o.get("model_id") == model_id
                    and o.get("market_date") == market_date
                    and o.get("horizon") == horizon), None)
    if outcome is None:
        return {"status": OUT_PENDING, "model_id": model_id,
                "market_date": market_date, "horizon": horizon}
    store = read_price_store(desk_dir)
    detail = _derive_outcome(cs, horizon=horizon,
                             maturity_date=outcome["maturity_market_date"],
                             price_series=store.get("series") or {})
    return {"status": outcome.get("status"), "model_id": model_id,
            "market_date": market_date, "horizon": horizon,
            "maturity_market_date": outcome.get("maturity_market_date"),
            "frozen_metrics": outcome.get("metrics"),
            **detail}


# --------------------------------------------------------------------------- #
# Part D — SHADOW PORTFOLIO P&L (paper-only simulation; one convention).
# --------------------------------------------------------------------------- #
def _entry_date(calendar: list[str], snapshot_date: str) -> Optional[str]:
    """A snapshot taken at a session's close is implementable only at the NEXT
    eligible close (no hindsight)."""
    for d in calendar:
        if d > snapshot_date:
            return d
    return None


def build_shadow_portfolio(book_id: str, *, desk_dir=None,
                           rows: Optional[list[dict]] = None,
                           price_store: Optional[dict] = None) -> dict:
    """Deterministic paper-only P&L simulation for one snapshot book (Part D).

    Controls applied consistently to every book: equal initial notional, the
    same eligible SPY calendar, the same first-write-wins price source, the
    same 12.5 bps/side cost convention (entry buys; churn buys+sells), and an
    explicit missing-data policy (a day below the member-coverage floor is a
    counted coverage gap with NAV carried flat — never an invented return)."""
    rows = rows if rows is not None else _snapshot_rows(desk_dir)
    store = price_store if price_store is not None else read_price_store(desk_dir)
    series = store.get("series") or {}
    calendar = eligible_calendar(desk_dir, price_store=store)
    snaps = _book_snapshots(rows, book_id)
    meta = next((b for b in SUPPORTED_BOOKS if b[1] == book_id), None)
    role = meta[3] if meta else ROLE_SHADOW
    base = {
        "book_id": book_id,
        "model_id": meta[0] if meta else None,
        "book_role": role,
        "research_shadow_label": (SHADOW_BOOK_LABEL if role == ROLE_SHADOW else
                                  "ACTIVE MODEL — snapshot-based simulation (the "
                                  "executed operational desk book is the P&L record)"),
        "evidence_type": TRUE_FORWARD,
        "equal_initial_notional": EQUAL_SHADOW_NOTIONAL,
        "transaction_cost_assumption": TRANSACTION_COST_ASSUMPTION,
        "price_source": "forward_prediction_prices.json (first write wins)",
        "missing_data_policy": (
            "A session with under %.0f%% of members priced is an explicit coverage "
            "gap: NAV is carried flat and the gap is counted — no return is invented."
            % (100 * _SHADOW_MIN_DAY_COVERAGE)),
    }
    if not snaps:
        return {**base, "status": EV_NO_SNAPSHOTS, "observations": 0,
                "message": "No TRUE_FORWARD snapshots captured for this book yet."}
    first_entry = _entry_date(calendar, snaps[0]["market_date"])
    if first_entry is None:
        return {**base, "status": EV_OUTCOMES_PENDING, "observations": 0,
                "start_date": None,
                "message": ("The first snapshot's entry close (the next eligible "
                            "session) has not completed yet.")}
    timeline = [d for d in calendar if d >= first_entry]
    # snapshot activation map: entry date -> membership
    activations: list[tuple[str, list[str], set]] = []
    for s in snaps:
        e = _entry_date(calendar, s["market_date"])
        if e is not None:
            activations.append((e, s.get("membership") or [], set(s.get("membership") or [])))
    nav = EQUAL_SHADOW_NOTIONAL
    gross_nav = EQUAL_SHADOW_NOTIONAL
    total_cost = 0.0
    total_turnover = 0.0
    active_members: Optional[list[str]] = None
    daily_rets: list[float] = []
    spy_rets: list[float] = []
    peak = nav
    max_dd = 0.0
    gaps = partial = full = 0
    est_dates: list[str] = []
    for i, d in enumerate(timeline):
        # (re)balance at this close: latest activation with entry date <= d
        due = [a for a in activations if a[0] <= d]
        target_members = due[-1][1] if due else None
        if target_members is not None and active_members is None:
            cost = nav * COST_RATE_PER_SIDE  # establishment: buy side
            nav -= cost
            total_cost += cost
            active_members = list(target_members)
            est_dates.append(d)
        elif target_members is not None and set(target_members) != set(active_members):
            adds = set(target_members) - set(active_members)
            drops = set(active_members) - set(target_members)
            churn_fraction = ((len(adds) + len(drops))
                              / (max(len(target_members), 1)))
            traded = nav * churn_fraction
            cost = traded * COST_RATE_PER_SIDE
            nav -= cost
            total_cost += cost
            total_turnover += churn_fraction
            active_members = list(target_members)
            est_dates.append(d)
        if i == 0 or active_members is None:
            continue
        prev = timeline[i - 1]
        rets = []
        for tk in active_members:
            p0 = _price_exact(series, tk, prev)
            p1 = _price_exact(series, tk, d)
            if p0 and p1:
                rets.append(p1 / p0 - 1.0)
        n_m = len(active_members) or 1
        if len(rets) / n_m < _SHADOW_MIN_DAY_COVERAGE:
            gaps += 1
            continue  # NAV carried flat; counted, never invented
        if len(rets) < n_m:
            partial += 1
        else:
            full += 1
        r = sum(rets) / len(rets)
        nav *= (1.0 + r)
        gross_nav *= (1.0 + r)
        daily_rets.append(r)
        s0 = _price_exact(series, BENCHMARK_TICKER, prev)
        s1 = _price_exact(series, BENCHMARK_TICKER, d)
        spy_rets.append((s1 / s0 - 1.0) if (s0 and s1) else float("nan"))
        peak = max(peak, nav)
        if peak:
            max_dd = min(max_dd, nav / peak - 1.0)
    n = len(daily_rets)
    spy_pair = [(daily_rets[k], spy_rets[k]) for k in range(n)
                if spy_rets[k] == spy_rets[k]]
    spy_total = 1.0
    for _r, s in spy_pair:
        spy_total *= (1.0 + s)
    spy_ret_pct = (spy_total - 1.0) * 100.0 if spy_pair else None
    net_ret = (nav / EQUAL_SHADOW_NOTIONAL - 1.0) * 100.0
    gross_ret = (gross_nav / EQUAL_SHADOW_NOTIONAL - 1.0) * 100.0
    vol = None
    vol_warning = None
    sd = _std(daily_rets)
    if sd is not None:
        if n >= _MIN_RATIO_OBS:
            vol = round(sd * math.sqrt(252) * 100.0, 4)
        else:
            vol_warning = ("Daily volatility withheld: only %d observations "
                           "(< %d)." % (n, _MIN_RATIO_OBS))
    up = sum(1 for r in daily_rets if r > 0)
    out_days = sum(1 for r, s in spy_pair if r > s)
    stabilities = [s.get("membership_stability_vs_previous") for s in snaps
                   if s.get("membership_stability_vs_previous") is not None]
    latest_snap = snaps[-1]
    conc = latest_snap.get("concentration") or {}
    sufficient = n >= _MIN_RATIO_OBS
    return {
        **base,
        "status": ("SIMULATED" if n else EV_OUTCOMES_PENDING),
        "observations": n,
        "start_date": timeline[0] if timeline else None,
        "latest_date": timeline[-1] if timeline else None,
        "snapshot_count": len(snaps),
        "gross_return_pct": _r4(gross_ret) if n else None,
        "estimated_transaction_cost": _r2(total_cost),
        "net_return_pct": _r4(net_ret) if n else None,
        "spy_return_pct": _r4(spy_ret_pct),
        "excess_return_pp": (_r4(net_ret - spy_ret_pct)
                             if (n and spy_ret_pct is not None) else None),
        "max_drawdown_pct": _r4(max_dd * 100.0) if n else None,
        "daily_volatility_annualized_pct": vol,
        "volatility_warning": vol_warning,
        "hit_rate_pct": _r4(100.0 * up / n) if n else None,
        "spy_outperformance_day_rate_pct": (_r4(100.0 * out_days / len(spy_pair))
                                            if spy_pair else None),
        "cumulative_turnover": _r4(total_turnover),
        "membership_stability_avg": _r4(_mean(stabilities)),
        "sector_concentration": conc,
        "coverage": {"full_days": full, "partial_days": partial,
                     "coverage_gap_days": gaps,
                     "min_day_coverage_fraction": _SHADOW_MIN_DAY_COVERAGE},
        "sample_sufficiency": ("FORWARD_SAMPLE_SUFFICIENT" if sufficient
                               else "INSUFFICIENT_FORWARD_SAMPLE"),
        "insufficient_message": (None if sufficient else
                                 "INSUFFICIENT FORWARD SAMPLE — NO MODEL CONCLUSION"),
        "rebalance_dates": est_dates,
    }


# --------------------------------------------------------------------------- #
# Part E — PREDICTION-SKILL METRICS (TRUE_FORWARD + matured outcomes ONLY).
# --------------------------------------------------------------------------- #
def _evidence_state(*, n_snapshots: int, n_matured: int,
                    n_coverage_blocked: int = 0) -> tuple[str, str]:
    if n_snapshots == 0:
        return EV_NO_SNAPSHOTS, "No TRUE_FORWARD snapshots exist yet."
    if n_matured == 0 and n_coverage_blocked > 0:
        return EV_COVERAGE_BLOCKED, ("Outcomes exist but price coverage was too "
                                     "incomplete to support metrics.")
    if n_matured == 0:
        return EV_OUTCOMES_PENDING, ("Snapshots are captured; no outcome has "
                                     "reached its required eligible-close count yet.")
    for g in EVIDENCE_GATES:
        lo = g["min_observations"]
        hi = g["max_observations"]
        if n_matured >= lo and (hi is None or n_matured <= hi):
            return g["state"], g["interpretation"]
    return EV_INSUFFICIENT, "Pipeline verification only — no model conclusion."


def build_prediction_skill(*, desk_dir=None,
                           rows: Optional[list[dict]] = None,
                           outcomes: Optional[list[dict]] = None) -> dict:
    """Cross-sectional prediction-skill metrics per (model, horizon), computed
    ONLY from TRUE_FORWARD snapshots and their matured outcome rows (Part E).
    Nothing is annualised on a small sample; every cell carries its own state."""
    rows = rows if rows is not None else _snapshot_rows(desk_dir)
    outcomes = outcomes if outcomes is not None else _outcome_rows(desk_dir)
    by_model_dates: dict[str, list[str]] = {}
    for cs in _cross_sections(rows):
        by_model_dates.setdefault(cs["model_id"], []).append(cs["market_date"])
    cells: list[dict] = []
    for model_id in SUPPORTED_MODEL_IDS:
        snap_dates = sorted(set(by_model_dates.get(model_id) or []))
        for h in HORIZONS:
            os_ = sorted((o for o in outcomes if o.get("model_id") == model_id
                          and o.get("horizon") == h),
                         key=lambda o: o.get("market_date") or "")
            with_metrics = [o for o in os_ if o.get("metrics")]
            blocked = [o for o in os_ if not o.get("metrics")]
            ics = [o["metrics"].get("rank_ic_spearman") for o in with_metrics
                   if o["metrics"].get("rank_ic_spearman") is not None]
            state, interp = _evidence_state(n_snapshots=len(snap_dates),
                                            n_matured=len(with_metrics),
                                            n_coverage_blocked=len(blocked))

            def _avg(key: str):
                vals = [o["metrics"].get(key) for o in with_metrics
                        if o["metrics"].get(key) is not None]
                return _r4(_mean(vals))
            cells.append({
                "model_id": model_id,
                "horizon_eligible_closes": h,
                "evidence_state": state,
                "interpretation": interp,
                "snapshot_count": len(snap_dates),
                "matured_observation_count": len(with_metrics),
                "coverage_blocked_count": len(blocked),
                "pending_count": max(len(snap_dates) - len(os_), 0),
                "ic_observation_count": len(ics),
                "ic_mean": _r4(_mean(ics)),
                "ic_median": _r4(_median(ics)),
                "ic_std": (_r4(_std(ics)) if len(ics) >= 2 else None),
                "ic_positive_rate_pct": (_r4(100.0 * sum(1 for x in ics if x > 0)
                                             / len(ics)) if ics else None),
                "top_decile_return_pct": _avg("top_decile_return_pct"),
                "bottom_decile_return_pct": _avg("bottom_decile_return_pct"),
                "top_minus_bottom_pp": _avg("top_minus_bottom_pp"),
                "top25_avg_return_pct": _avg("top25_avg_return_pct"),
                "top50_avg_return_pct": _avg("top50_avg_return_pct"),
                "benchmark_return_pct": _avg("benchmark_return_pct"),
                "top25_excess_pp": _avg("top25_excess_pp"),
                "top50_excess_pp": _avg("top50_excess_pp"),
                "top25_hit_rate_pct": _avg("top25_hit_rate_pct"),
                "annualized": False,
                "annualization_note": ("Never annualised: horizon returns are shown "
                                       "raw; a small forward sample is never scaled up."),
            })
    return {"cells": cells, "horizons": list(HORIZONS),
            "models": list(SUPPORTED_MODEL_IDS)}


# --------------------------------------------------------------------------- #
# Part F — deterministic RESEARCH-ONLY flags (never operational).
# --------------------------------------------------------------------------- #
def _flag(code: str, *, metric: str, value, threshold: str, sample_count: int,
          detail: str) -> dict:
    return {"flag": code, "metric": metric, "value": value, "threshold": threshold,
            "sample_count": sample_count, "actionable": False,
            "observation_only": True,
            "operational_effect": "NONE — research observation only; no model, "
                                  "target or order is changed.",
            "detail": detail}


#: Minimum matured/daily observations before any negative-performance flag may
#: fire — one losing day must never trigger retraining or replacement.
_FLAG_MIN_OBS = 5

def build_research_flags(*, skill: dict, portfolios: list[dict]) -> list[dict]:
    flags: list[dict] = []
    active_cells = [c for c in (skill.get("cells") or [])
                    if c["model_id"] == ACTIVE_MODEL_ID]
    n_matured_1d = next((c["matured_observation_count"] for c in active_cells
                         if c["horizon_eligible_closes"] == 1), 0)
    if n_matured_1d < _MIN_RATIO_OBS:
        flags.append(_flag(FLAG_INSUFFICIENT_SAMPLE,
                           metric="matured 1-close observations (active model)",
                           value=n_matured_1d,
                           threshold=">= %d for any performance interpretation"
                                     % _MIN_RATIO_OBS,
                           sample_count=n_matured_1d,
                           detail=("The forward sample is below the interpretation "
                                   "floor. Every other signal is observation-only; "
                                   "a single losing day never triggers retraining "
                                   "or model replacement.")))
    for c in active_cells:
        n = c["matured_observation_count"]
        if n >= _FLAG_MIN_OBS and c.get("ic_mean") is not None and c["ic_mean"] < 0 \
                and c.get("ic_positive_rate_pct") is not None \
                and c["ic_positive_rate_pct"] < 40.0:
            flags.append(_flag(FLAG_RANK_IC_DEGRADATION,
                               metric="mean Spearman rank IC @ %dc"
                                      % c["horizon_eligible_closes"],
                               value=c["ic_mean"],
                               threshold="mean IC < 0 AND positive-IC rate < 40% "
                                         "over >= %d matured observations" % _FLAG_MIN_OBS,
                               sample_count=n,
                               detail="Rank ordering is not predicting realized "
                                      "returns at this horizon so far."))
        if n >= _FLAG_MIN_OBS and c.get("top25_excess_pp") is not None \
                and c["top25_excess_pp"] < 0:
            flags.append(_flag(FLAG_PERSISTENT_NEGATIVE_EXCESS,
                               metric="avg Top-25 excess vs SPY @ %dc (pp)"
                                      % c["horizon_eligible_closes"],
                               value=c["top25_excess_pp"],
                               threshold="< 0 over >= %d matured observations"
                                         % _FLAG_MIN_OBS,
                               sample_count=n,
                               detail="The selected membership has lagged the "
                                      "benchmark on matured horizons so far."))
        blocked = c.get("coverage_blocked_count") or 0
        if blocked and blocked >= max(1, c["matured_observation_count"]):
            flags.append(_flag(FLAG_COVERAGE_DEGRADATION,
                               metric="coverage-blocked outcome rows @ %dc"
                                      % c["horizon_eligible_closes"],
                               value=blocked,
                               threshold=">= matured rows (coverage is the binding "
                                         "constraint)",
                               sample_count=blocked + c["matured_observation_count"],
                               detail="Price coverage, not the model, is limiting "
                                      "the evidence."))
    for p in portfolios:
        n = p.get("observations") or 0
        if n < _FLAG_MIN_OBS:
            continue
        if p.get("excess_return_pp") is not None and p["excess_return_pp"] < 0 \
                and p.get("net_return_pct") is not None and p["net_return_pct"] < 0:
            flags.append(_flag(FLAG_COST_ADJUSTED_ALPHA_NEGATIVE,
                               metric="%s net return %% / excess pp" % p["book_id"],
                               value={"net_return_pct": p["net_return_pct"],
                                      "excess_return_pp": p["excess_return_pp"]},
                               threshold="net < 0 AND excess < 0 over >= %d "
                                         "observations" % _FLAG_MIN_OBS,
                               sample_count=n,
                               detail="Cost-adjusted simulation is behind cash AND "
                                      "the benchmark so far."))
        if p.get("cumulative_turnover") is not None and n and \
                (p["cumulative_turnover"] / n) > 0.10:
            flags.append(_flag(FLAG_EXCESSIVE_TURNOVER,
                               metric="%s cumulative churn per session" % p["book_id"],
                               value=_r4(p["cumulative_turnover"] / n),
                               threshold="> 0.10 membership churn per eligible close",
                               sample_count=n,
                               detail="Membership is churning faster than the "
                                      "model's stated cadence implies."))
        if p.get("membership_stability_avg") is not None \
                and p["membership_stability_avg"] < 0.60:
            flags.append(_flag(FLAG_MEMBERSHIP_INSTABILITY,
                               metric="%s avg membership stability" % p["book_id"],
                               value=p["membership_stability_avg"],
                               threshold="< 0.60 average snapshot-to-snapshot overlap",
                               sample_count=p.get("snapshot_count") or 0,
                               detail="Successive targets share fewer names than a "
                                      "stable model should produce."))
        conc = (p.get("sector_concentration") or {})
        if conc.get("top_sector_weight") is not None \
                and conc["top_sector_weight"] > 0.30:
            flags.append(_flag(FLAG_SECTOR_CONCENTRATION,
                               metric="%s top sector weight" % p["book_id"],
                               value=conc.get("top_sector_weight"),
                               threshold="> 0.30 of the book in one sector",
                               sample_count=p.get("snapshot_count") or 0,
                               detail="Sector %s dominates the latest snapshot."
                                      % (conc.get("top_sector") or "?")))
        if p.get("max_drawdown_pct") is not None and p["max_drawdown_pct"] < -10.0:
            flags.append(_flag(FLAG_DRAWDOWN_REVIEW,
                               metric="%s max drawdown %%" % p["book_id"],
                               value=p["max_drawdown_pct"],
                               threshold="< -10% simulated drawdown",
                               sample_count=n,
                               detail="Simulated drawdown exceeds the review "
                                      "threshold — observation only."))
    return flags


# --------------------------------------------------------------------------- #
# Score stability (consecutive-snapshot score-distribution drift) — used by the
# skill payload; deterministic, snapshot-derived only.
# --------------------------------------------------------------------------- #
def _score_stability(rows: list[dict], model_id: str) -> Optional[dict]:
    dists = [(s.get("market_date"), s.get("score_distribution") or {})
             for s in _book_snapshots(rows)
             if s.get("model_id") == model_id and s.get("book_role") in (ROLE_ACTIVE,
                                                                         ROLE_SHADOW)
             and s.get("book_id", "").endswith("_top25")]
    if len(dists) < 2:
        return None
    shifts = []
    for (d0, a), (d1, b) in zip(dists, dists[1:]):
        ma, mb = _f(a.get("mean")), _f(b.get("mean"))
        sa = _f(a.get("std"))
        if ma is None or mb is None or not sa:
            continue
        shifts.append(abs(mb - ma) / sa)
    return {"n_pairs": len(shifts),
            "mean_abs_mean_shift_in_std_units": _r4(_mean(shifts))}


# --------------------------------------------------------------------------- #
# Part B helpers — capture status for the daily close (read-only + write paths).
# --------------------------------------------------------------------------- #
def read_capture_status(*, market_date: Optional[str], desk_dir=None) -> dict:
    """READ-ONLY presence summary of the TRUE_FORWARD snapshots for one market
    date (used by the daily-close GET and the ALREADY_PROCESSED re-run: no
    engine build, no fetch, no write)."""
    md = str(market_date or "")[:10] or None
    out = _capture_base(md)
    try:
        rows = _snapshot_rows(desk_dir)
    except Exception:  # noqa: BLE001
        rows = []
    present = [r for r in rows if r.get("kind") == KIND_BOOK_SNAPSHOT
               and r.get("market_date") == md]
    missing = [b[1] for b in SUPPORTED_BOOKS
               if not any(p.get("book_id") == b[1] for p in present)]
    mandatory_persisted = any(p.get("book_id") == MANDATORY_BOOK_ID for p in present)
    out.update({
        "status": ("SNAPSHOTS_PRESENT" if present and not missing else
                   "SNAPSHOTS_PARTIAL" if present else "SNAPSHOTS_NOT_CAPTURED"),
        "snapshots_already_present": len(present),
        "snapshots_unavailable": len(missing) if present else len(SUPPORTED_BOOKS),
        "unavailable_reasons": ({b: "NOT_CAPTURED_FOR_THIS_DATE" for b in missing}
                                if md else {}),
        "read_only_presence_check": True,
        "mandatory_active_snapshot_persisted": mandatory_persisted,
        "persisted_snapshot_ids": sorted(p.get("snapshot_id") for p in present
                                         if p.get("snapshot_id")),
        "evidence_status": _classify_evidence(
            created=0, present=len(present), unavailable=len(missing),
            mandatory_persisted=mandatory_persisted),
        "note": ("TRUE_FORWARD snapshots are captured only by a fresh daily close; "
                 "an already-processed date is never retroactively backfilled."),
    })
    return out


def capture_for_daily_close(*, market_date: str, desk_dir=None,
                            current: Optional[dict] = None,
                            downloader=None,
                            ops: Optional[dict] = None,
                            progress: Optional[Callable] = None) -> dict:
    """The ONE daily-close integration entry point (Part B): capture today's
    immutable snapshots (frozen -> appended -> verified BEFORE the slow price
    fetch), then check previously captured snapshots for newly matured outcomes.
    Degrade-safe by contract of the caller; idempotent — re-running the same
    close duplicates nothing."""
    capture = capture_snapshots(market_date=market_date, desk_dir=desk_dir,
                                current=current, ops=ops, downloader=downloader,
                                progress=progress)
    _progress_call(progress, "MATURE_OUTCOMES")
    try:
        matured = mature_outcomes(desk_dir=desk_dir)
    except Exception as exc:  # noqa: BLE001
        matured = {"outcomes_newly_matured": 0, "pending_outcome_count": None,
                   "performed_write": False, "error": str(exc)[:160]}
    capture["outcomes_newly_matured"] = matured.get("outcomes_newly_matured", 0)
    capture["pending_outcome_count"] = matured.get("pending_outcome_count")
    capture["performed_write"] = bool(capture.get("performed_write")
                                      or matured.get("performed_write"))
    return capture


# --------------------------------------------------------------------------- #
# Phase 28B.2 Parts E/F/G — MISSED-CLOSE RECOVERY (evidence-only, token-gated)
# and the append-only evidence-incident ledger.
# --------------------------------------------------------------------------- #
def _incident_rows(desk_dir=None) -> list[dict]:
    return desk._read_ledger(_sdir(desk_dir), INCIDENT_LEDGER_FILE)


def list_evidence_incidents(desk_dir=None) -> list[dict]:
    """Read-only: every recorded evidence incident (missed captures + recovery
    audit rows). NEVER counted as snapshots or matured outcomes — the incident
    ledger is a separate file no snapshot/outcome reader ever touches."""
    return _incident_rows(desk_dir)


def record_missed_capture(*, market_date: str, missing_books: list[str],
                          reason: str, desk_dir=None,
                          detected_by: str = "recovery_endpoint") -> dict:
    """Append ONE explicit FORWARD_CAPTURE_MISSED evidence incident (Part G).
    Idempotent per market date. The record is documentation only: it is never a
    snapshot, never an outcome, and never changes the (still valid) operational
    close. No snapshot is fabricated; the next eligible snapshot date stays
    unknown until the next genuinely fresh close."""
    md = str(market_date or "")[:10]
    existing = [r for r in _incident_rows(desk_dir)
                if r.get("kind") == KIND_CAPTURE_MISSED and r.get("market_date") == md]
    if existing:
        return {"status": "MISSED_CAPTURE_ALREADY_RECORDED", "market_date": md,
                "performed_write": False, "record": existing[0]}
    row = {
        "kind": KIND_CAPTURE_MISSED,
        "schema_version": SCHEMA_VERSION,
        "status": "FORWARD_CAPTURE_MISSED",
        "market_date": md,
        "expected_books": [b[1] for b in SUPPORTED_BOOKS],
        "missing_books": sorted(missing_books),
        "reason": str(reason)[:400],
        "detected_at": _now_iso(),
        "detected_by": detected_by,
        "close_remained_operationally_valid": True,
        "snapshot_fabricated": False,
        "counts_as_snapshot": False,
        "counts_as_outcome": False,
        "next_eligible_snapshot_date": None,
        "note": ("The first valid TRUE_FORWARD snapshot date is the next genuinely "
                 "new eligible close; this date is never hindsight-backfilled."),
    }
    desk._append_ledger(_sdir(desk_dir), INCIDENT_LEDGER_FILE, [row])
    return {"status": "MISSED_CAPTURE_RECORDED", "market_date": md,
            "performed_write": True, "record": row}


_REQUIRED_BUNDLE_BOOK_FIELDS = (
    "kind", "snapshot_id", "market_date", "model_id", "model_version", "book_id",
    "book_role", "membership", "target_weights", "price_data_through",
    "benchmark_date", "schema_version",
)


def load_recovery_status(*, market_date: str, desk_dir=None) -> dict:
    """READ-ONLY recovery inspection for one processed close date (Part F dry
    run). Decides — without writing anything and without touching any provider,
    engine or current data — whether the missing TRUE_FORWARD snapshots can be
    recovered EXCLUSIVELY from the frozen close-artifact bundle."""
    md = str(market_date or "")[:10]
    sdir = _sdir(desk_dir)
    try:
        journal = desk._read_ledger(sdir, _CLOSE_JOURNAL_FILE)
    except Exception:  # noqa: BLE001
        journal = []
    close_processed = any(r.get("market_date") == md for r in journal)

    try:
        rows = _snapshot_rows(desk_dir)
    except Exception:  # noqa: BLE001
        rows = []
    present_books = sorted({r.get("book_id") for r in rows
                            if r.get("kind") == KIND_BOOK_SNAPSHOT
                            and r.get("market_date") == md})
    missing_books = [b[1] for b in SUPPORTED_BOOKS if b[1] not in present_books]

    bundle = find_artifact_bundle(md, desk_dir)
    artifacts = (bundle or {}).get("artifacts") or {}
    hash_valid: Optional[bool] = None
    if bundle is not None:
        hash_valid = _content_hash(artifacts) == bundle.get("artifact_hash")

    recoverable: list[str] = []
    unavailable: dict[str, str] = {}
    requires_recalculation = False
    if not close_processed:
        status = REC_DATE_NOT_PROCESSED
        unavailable = {b: "DATE_NOT_PROCESSED: no daily close was recorded for "
                          "this date." for b in missing_books}
    elif not missing_books:
        status = REC_ALREADY_PRESENT
    elif bundle is None:
        status = REC_NOT_RECOVERABLE
        requires_recalculation = True
        unavailable = {b: "NO_FROZEN_ARTIFACTS: the original close preserved no "
                          "artifact bundle — recovery would require recomputation, "
                          "which is forbidden." for b in missing_books}
    elif not hash_valid:
        status = REC_NOT_RECOVERABLE
        requires_recalculation = True
        unavailable = {b: "ARTIFACT_HASH_INVALID: the frozen bundle does not match "
                          "its recorded content hash — provenance cannot be proven."
                       for b in missing_books}
    else:
        by_book = {r.get("book_id"): r for r in (artifacts.get("book_snapshots") or [])}
        for b in missing_books:
            row = by_book.get(b)
            if row is None:
                unavailable[b] = ("NOT_IN_FROZEN_BUNDLE: the original close did not "
                                  "produce this book — recovering it would require "
                                  "recalculation.")
                continue
            missing_fields = [f for f in _REQUIRED_BUNDLE_BOOK_FIELDS
                              if row.get(f) in (None, [], {})]
            if row.get("market_date") != md:
                missing_fields.append("market_date(mismatch)")
            if missing_fields:
                unavailable[b] = ("BUNDLE_ROW_INCOMPLETE (%s): recovery would "
                                  "require recalculation." % ", ".join(missing_fields[:6]))
                continue
            recoverable.append(b)
        requires_recalculation = bool(unavailable)
        status = REC_RECOVERABLE if recoverable else REC_NOT_RECOVERABLE
    return {
        "phase": PHASE,
        "market_date": md,
        "recovery_status": status,
        "close_processed": close_processed,
        "frozen_artifacts_found": bundle is not None,
        "artifact_bundle_id": (bundle or {}).get("artifact_bundle_id"),
        "artifact_timestamp": (bundle or {}).get("created_at"),
        "artifact_hash": (bundle or {}).get("artifact_hash"),
        "artifact_hash_valid": hash_valid,
        "snapshots_present": present_books,
        "recoverable_books": recoverable,
        "unavailable_books": unavailable,
        "requires_recalculation": requires_recalculation,
        "confirmation_required": RECOVERY_CONFIRM_TOKEN,
        "changes_operational_state": False,
        "evidence_incidents": [r for r in _incident_rows(desk_dir)
                               if r.get("market_date") == md],
        **_safety(False),
    }


def recover_missed_close(*, market_date: str, confirmation: Optional[str],
                         desk_dir=None, requested_by: str = "manual_api") -> dict:
    """Token-gated EVIDENCE-ONLY recovery of a processed close whose TRUE_FORWARD
    snapshots were never persisted (Part F). Replays the frozen artifact bundle
    VERBATIM — it never refreshes a provider, never fetches a price, never
    rebuilds a model, and never touches marks, P&L, decisions, holdings, cash,
    orders or model state. Idempotent: recovered/present books are never
    duplicated. When recovery is impossible, the explicit FORWARD_CAPTURE_MISSED
    incident is recorded instead (Part G)."""
    md = str(market_date or "")[:10]
    if confirmation != RECOVERY_CONFIRM_TOKEN:
        return {"status": "RECOVERY_CONFIRM_REQUIRED", "market_date": md,
                "performed_write": False, "recovered_books": [],
                "confirmation_required": RECOVERY_CONFIRM_TOKEN,
                "message": ("Evidence recovery requires confirmation='%s'."
                            % RECOVERY_CONFIRM_TOKEN),
                "changes_operational_state": False, **_safety(False)}
    rs = load_recovery_status(market_date=md, desk_dir=desk_dir)
    base = {"market_date": md, "recovery_status": rs["recovery_status"],
            "artifact_bundle_id": rs["artifact_bundle_id"],
            "artifact_hash": rs["artifact_hash"],
            "artifact_hash_valid": rs["artifact_hash_valid"],
            "unavailable_books": rs["unavailable_books"],
            "changes_operational_state": False, "idempotent": True}
    if rs["recovery_status"] == REC_DATE_NOT_PROCESSED:
        return {"status": "RECOVERY_REJECTED_DATE_NOT_PROCESSED", **base,
                "performed_write": False, "recovered_books": [],
                "message": ("No daily close was recorded for %s — there is no "
                            "close whose evidence could be recovered." % md),
                **_safety(False)}
    if rs["recovery_status"] == REC_ALREADY_PRESENT:
        return {"status": "RECOVERY_REJECTED_SNAPSHOTS_ALREADY_PRESENT", **base,
                "performed_write": False, "recovered_books": [],
                "message": ("Every expected TRUE_FORWARD snapshot for %s already "
                            "exists — nothing to recover, nothing written." % md),
                **_safety(False)}
    if rs["recovery_status"] != REC_RECOVERABLE:
        missed = record_missed_capture(
            market_date=md, missing_books=list(rs["unavailable_books"]),
            reason=("Recovery rejected: %s" % "; ".join(
                sorted(set(rs["unavailable_books"].values()))[:3]) or
                    REC_NOT_RECOVERABLE),
            desk_dir=desk_dir, detected_by=requested_by)
        return {"status": "RECOVERY_REJECTED_NOT_RECOVERABLE", **base,
                "performed_write": bool(missed.get("performed_write")),
                "recovered_books": [], "missed_capture_record": missed,
                "message": ("The frozen artifacts cannot support recovery without "
                            "recomputation — %s was recorded as "
                            "FORWARD_CAPTURE_MISSED; no snapshot was fabricated."
                            % md),
                **_safety(bool(missed.get("performed_write")))}

    bundle = find_artifact_bundle(md, desk_dir)
    artifacts = (bundle or {}).get("artifacts") or {}
    by_book = {r.get("book_id"): r for r in (artifacts.get("book_snapshots") or [])}
    rows = _snapshot_rows(desk_dir)
    present_cs = {r.get("model_id") for r in rows
                  if r.get("kind") == KIND_CROSS_SECTION and r.get("market_date") == md}
    recover_books = [by_book[b] for b in rs["recoverable_books"]]
    need_models = {r.get("model_id") for r in recover_books} - present_cs
    recover_cs = [c for c in (artifacts.get("cross_sections") or [])
                  if c.get("model_id") in need_models and c.get("market_date") == md]
    new_rows = recover_cs + recover_books  # VERBATIM frozen rows — nothing rebuilt
    desk._append_ledger(_sdir(desk_dir), SNAPSHOT_LEDGER_FILE, new_rows)
    stored_ids = {r.get("snapshot_id") for r in _snapshot_rows(desk_dir)
                  if r.get("kind") == KIND_BOOK_SNAPSHOT and r.get("market_date") == md}
    verified = all(("fps_%s_%s" % (b, md)) in stored_ids
                   for b in rs["recoverable_books"])
    audit = {
        "kind": KIND_RECOVERY,
        "schema_version": SCHEMA_VERSION,
        "status": "FORWARD_EVIDENCE_RECOVERED",
        "market_date": md,
        "recovered_books": sorted(rs["recoverable_books"]),
        "recovered_cross_sections": sorted(need_models),
        "unavailable_books": rs["unavailable_books"],
        "artifact_bundle_id": rs["artifact_bundle_id"],
        "artifact_hash": rs["artifact_hash"],
        "recovered_at": _now_iso(),
        "requested_by": requested_by,
        "source": "frozen close-artifact bundle (verbatim replay; no recalculation)",
    }
    try:
        desk._append_ledger(_sdir(desk_dir), INCIDENT_LEDGER_FILE, [audit])
    except Exception:  # noqa: BLE001 — the recovery itself already succeeded
        pass
    return {"status": "RECOVERED_FROM_FROZEN_ARTIFACTS", **base,
            "performed_write": True,
            "recovered_books": sorted(rs["recoverable_books"]),
            "recovered_cross_sections": sorted(need_models),
            "verification_complete": verified,
            "persisted_snapshot_ids": sorted(stored_ids),
            "recovery_audit_recorded": True,
            "message": ("Recovered %d TRUE_FORWARD snapshot(s) for %s verbatim from "
                        "the frozen close artifacts (no recalculation, no provider "
                        "access, no operational change)."
                        % (len(rs["recoverable_books"]), md)),
            **_safety(True)}


# --------------------------------------------------------------------------- #
# Part G — read-only public payloads.
# --------------------------------------------------------------------------- #
def _counts(rows: list[dict], outcomes: list[dict]) -> dict:
    snaps = _book_snapshots(rows)
    dates = sorted({s.get("market_date") for s in snaps if s.get("market_date")})
    matured = [o for o in outcomes if o.get("metrics")]
    blocked = [o for o in outcomes if not o.get("metrics")]
    # pending = every (model, date, horizon) without an outcome row
    have = {(o.get("model_id"), o.get("market_date"), o.get("horizon"))
            for o in outcomes}
    pending = 0
    for cs in _cross_sections(rows):
        for h in HORIZONS:
            if (cs.get("model_id"), cs.get("market_date"), h) not in have:
                pending += 1
    return {"snapshot_count": len(snaps),
            "snapshot_dates": dates,
            "latest_snapshot_date": dates[-1] if dates else None,
            "matured_outcome_count": len(matured),
            "coverage_blocked_outcome_count": len(blocked),
            "pending_outcome_count": pending}


def prediction_skill_summary(*, desk_dir=None) -> dict:
    """The concise summary embedded in GET /v1/evidence/forward (Part G)."""
    try:
        rows = _snapshot_rows(desk_dir)
        outcomes = _outcome_rows(desk_dir)
    except Exception:  # noqa: BLE001
        rows, outcomes = [], []
    c = _counts(rows, outcomes)
    active_dates = sorted({cs.get("market_date")
                           for cs in _cross_sections(rows, ACTIVE_MODEL_ID)})
    active_matured = [o for o in outcomes if o.get("model_id") == ACTIVE_MODEL_ID
                      and o.get("metrics")]
    state, interp = _evidence_state(n_snapshots=len(active_dates),
                                    n_matured=len(active_matured))
    return {
        "evidence_state": state,
        "interpretation": interp,
        "latest_snapshot_date": c["latest_snapshot_date"],
        "snapshot_count": c["snapshot_count"],
        "matured_outcome_count": c["matured_outcome_count"],
        "pending_outcome_count": c["pending_outcome_count"],
        "active_model_id": ACTIVE_MODEL_ID,
        "active_book_id": ACTIVE_BOOK_ID,
        "horizons_eligible_closes": list(HORIZONS),
        "forward_evidence_type": TRUE_FORWARD,
        "detail_route": "/v1/evidence/prediction-skill",
    }


def load_prediction_skill(*, desk_dir=None) -> dict:
    """GET /v1/evidence/prediction-skill — the full read-only evidence payload."""
    rows = _snapshot_rows(desk_dir)
    outcomes = _outcome_rows(desk_dir)
    counts = _counts(rows, outcomes)
    skill = build_prediction_skill(desk_dir=desk_dir, rows=rows, outcomes=outcomes)
    store = read_price_store(desk_dir)
    portfolios = [build_shadow_portfolio(b[1], desk_dir=desk_dir, rows=rows,
                                         price_store=store)
                  for b in SUPPORTED_BOOKS]
    flags = build_research_flags(skill=skill, portfolios=portfolios)
    active_dates = sorted({cs.get("market_date")
                           for cs in _cross_sections(rows, ACTIVE_MODEL_ID)})
    active_matured = [o for o in outcomes if o.get("model_id") == ACTIVE_MODEL_ID
                      and o.get("metrics")]
    state, interp = _evidence_state(n_snapshots=len(active_dates),
                                    n_matured=len(active_matured))
    calendar = eligible_calendar(desk_dir, price_store=store)
    score_stab = {m: _score_stability(rows, m) for m in SUPPORTED_MODEL_IDS}
    return {
        "status": state,
        "phase": PHASE,
        "evidence_state": state,
        "interpretation": interp,
        "generated_at": _now_iso(),
        "schema_version": SCHEMA_VERSION,
        "latest_snapshot_date": counts["latest_snapshot_date"],
        "snapshot_count": counts["snapshot_count"],
        "snapshot_dates": counts["snapshot_dates"],
        "matured_outcome_count": counts["matured_outcome_count"],
        "coverage_blocked_outcome_count": counts["coverage_blocked_outcome_count"],
        "pending_outcome_count": counts["pending_outcome_count"],
        "active_book": {"model_id": ACTIVE_MODEL_ID, "book_id": ACTIVE_BOOK_ID,
                        "label": "Active operational strategy (fundamental + "
                                 "momentum 50/50, Top-25)"},
        "shadow_books": [{"model_id": b[0], "book_id": b[1], "role": b[3],
                          "label": SHADOW_BOOK_LABEL}
                         for b in SUPPORTED_BOOKS if b[3] == ROLE_SHADOW],
        "horizons": list(HORIZONS),
        "prediction_skill": skill["cells"],
        "portfolio_comparison": portfolios,
        "research_flags": flags,
        "evidence_gates": {
            "gates": [dict(g) for g in EVIDENCE_GATES],
            "note": ("These gates bound interpretation only. They NEVER promote, "
                     "retire, retrain or replace a model, and never change the "
                     "operational book."),
        },
        "coverage": {
            "eligible_calendar_sessions": len(calendar),
            "eligible_calendar_latest": calendar[-1] if calendar else None,
            "price_store_tickers": len((store.get("series") or {})),
            "score_stability": score_stab,
        },
        "methodology": {
            "snapshot_capture": ("Immutable TRUE_FORWARD snapshots are appended by "
                                 "the manual daily close only, for the exact "
                                 "completed session whose model inputs are current; "
                                 "never retroactive, never backfilled."),
            "maturation": ("Outcomes mature only after the exact number of eligible "
                           "completed sessions (recorded SPY closes) — never "
                           "calendar-day arithmetic. One immutable row per "
                           "(model, date, horizon)."),
            "rank_ic": ("Spearman rank correlation between the snapshot's prediction "
                        "rank and the realized total return over matured, priced "
                        "names (deterministic ticker tie-break)."),
            "shadow_pnl": ("Equal $%s initial notional per book; entry at the NEXT "
                           "eligible close after each snapshot (no hindsight); %s; "
                           "same SPY dates and first-write-wins price source for "
                           "every book." % (format(EQUAL_SHADOW_NOTIONAL, ",.0f"),
                                            TRANSACTION_COST_ASSUMPTION)),
            "annualization": ("No metric is annualised below %d observations; "
                              "horizon returns are never scaled up." % _MIN_RATIO_OBS),
        },
        **_safety(False),
    }


def load_prediction_snapshots(*, model_id: Optional[str] = None,
                              book_id: Optional[str] = None,
                              market_date: Optional[str] = None,
                              limit: int = 50, desk_dir=None) -> dict:
    """GET /v1/evidence/prediction-skill/snapshots — immutable snapshot summaries
    (identity + membership + provenance; never provider credentials/secrets)."""
    try:
        limit = max(1, min(int(limit), 200))
    except (TypeError, ValueError):
        limit = 50
    rows = _book_snapshots(_snapshot_rows(desk_dir))
    if model_id:
        rows = [r for r in rows if r.get("model_id") == model_id]
    if book_id:
        rows = [r for r in rows if r.get("book_id") == book_id]
    if market_date:
        rows = [r for r in rows if r.get("market_date") == str(market_date)[:10]]
    out = []
    for r in rows[-limit:][::-1]:
        out.append({k: r.get(k) for k in (
            "snapshot_id", "market_date", "created_at", "source",
            "forward_evidence_type", "model_id", "model_version", "book_id",
            "book_role", "book_size", "horizon", "rebalance_cadence",
            "transaction_cost_assumption", "price_data_through",
            "fundamental_data_as_of", "universe_as_of", "sector_metadata_as_of",
            "benchmark_date", "input_coverage", "eligible_universe_count",
            "membership", "target_weights", "equal_weight", "sector_exposure",
            "concentration", "previous_snapshot_id",
            "expected_turnover_vs_previous", "membership_stability_vs_previous",
            "expected_transaction_cost_fraction", "score_distribution",
            "schema_version", "seq", "recorded_at", "chain_hash")})
    return {"status": ("PREDICTION_SNAPSHOTS_READY" if out else EV_NO_SNAPSHOTS),
            "phase": PHASE,
            "count": len(out),
            "total_stored": len(rows),
            "filters": {"model_id": model_id, "book_id": book_id,
                        "market_date": market_date, "limit": limit},
            "snapshots": out,
            "immutable": True,
            "append_only": True,
            "generated_at": _now_iso(),
            **_safety(False)}


def verify_prediction_ledgers(desk_dir=None) -> dict:
    """Chain-hash verification of both prediction ledgers (tamper-evidence)."""
    sdir = _sdir(desk_dir)
    reports = [desk.verify_ledger(sdir, SNAPSHOT_LEDGER_FILE),
               desk.verify_ledger(sdir, OUTCOME_LEDGER_FILE)]
    return {"all_intact": all(r["intact"] for r in reports), "ledgers": reports}


__all__ = [
    "PHASE", "SCHEMA_VERSION", "TRUE_FORWARD", "HORIZONS",
    "SNAPSHOT_LEDGER_FILE", "OUTCOME_LEDGER_FILE", "PRICE_STORE_FILE",
    "ARTIFACT_LEDGER_FILE", "INCIDENT_LEDGER_FILE",
    "KIND_CROSS_SECTION", "KIND_BOOK_SNAPSHOT", "KIND_OUTCOME",
    "KIND_ARTIFACT_BUNDLE", "KIND_CAPTURE_MISSED", "KIND_RECOVERY",
    "ACTIVE_MODEL_ID", "ACTIVE_BOOK_ID", "MANDATORY_BOOK_ID",
    "SUPPORTED_BOOKS", "SUPPORTED_MODEL_IDS",
    "SHADOW_BOOK_LABEL", "EQUAL_SHADOW_NOTIONAL",
    "EVIDENCE_COMPLETE", "EVIDENCE_PARTIAL", "EVIDENCE_BLOCKED",
    "REC_RECOVERABLE", "REC_NOT_RECOVERABLE", "REC_ALREADY_PRESENT",
    "REC_DATE_NOT_PROCESSED", "RECOVERY_CONFIRM_TOKEN",
    "OUT_PENDING", "OUT_MATURED", "OUT_COVERAGE_INCOMPLETE",
    "OUT_BENCHMARK_UNAVAILABLE", "OUT_SYMBOL_UNAVAILABLE", "OUT_NOT_ENOUGH_CLOSES",
    "EV_NO_SNAPSHOTS", "EV_OUTCOMES_PENDING", "EV_INSUFFICIENT", "EV_PRELIMINARY",
    "EV_HORIZON_ALIGNED", "EV_COVERAGE_BLOCKED", "EVIDENCE_GATES",
    "read_price_store", "eligible_calendar",
    "capture_snapshots", "mature_outcomes", "derive_outcome_detail",
    "capture_for_daily_close", "read_capture_status",
    "find_artifact_bundle", "load_recovery_status", "recover_missed_close",
    "record_missed_capture", "list_evidence_incidents",
    "build_shadow_portfolio", "build_prediction_skill", "build_research_flags",
    "prediction_skill_summary", "load_prediction_skill",
    "load_prediction_snapshots", "verify_prediction_ledgers",
]
