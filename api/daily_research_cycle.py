"""api/daily_research_cycle.py — Slice 3 canonical Persistent Daily Research Cycle.

ONE orchestration owner (Consolidation Roadmap Slice 3; TARGET_ARCHITECTURE context
"Research Cycle") for the daily research-and-reassessment pass, so the operator has
a single, idempotent, resumable action with NO hidden prerequisite button or command.

It **composes** the existing authoritative owners through explicit adapters and does
NOT reimplement their business logic (Principle 1 / 2 / 8):

  * market session / freshness / plan → ``api.data_freshness`` (built on
    ``engine.market_session``) — the eligible session, per-source cadence-aware
    freshness, the active-book identity and the cross-surface consistency verdict.
  * daily price/score input refresh    → ``api.alpha_target.run_refresh`` — the one
    writer of the owned model-input CSVs (it also DETECTS the monthly boundary and
    refuses to approximate the frozen ``mom_6_1`` monthly input).
  * monthly momentum input             → the canonical monthly-input adapter
    (``api.monthly_momentum_input``), which wraps an injectable emitter seam and owns
    the safe contract (due-ness / validation / idempotency). There is no safe
    automatic emitter bundled in the pure-stdlib repo, so a due month blocks HONESTLY
    through the adapter (never approximated) — the documented "August evidence gap".
  * universe scoring / rankings        → ``api.universe_scoring.build_universe_scoring``
    (Slice 4 canonical composition owner over the pure ``api.multi_horizon_engine``
    kernel; frozen strategy version; TOP25 / TOP50; content-level input-contract hash).
  * operational target                 → ``api.alpha_target.load_readiness`` — the
    target is PREPARED / LOADED, never silently confirmed (manual review preserved).
  * immutable forward evidence         → ``api.forward_prediction_skill`` — the ONE
    TRUE_FORWARD snapshot owner; the required snapshot set/count is derived from its
    ``SUPPORTED_BOOKS`` registry (never hard-coded), keyed to one eligible date and
    one input-contract hash (first-write-wins, never backdated).
  * portfolio assessment (bridge)      → ``api.daily_action_gate`` — the existing
    paper-only event gate for the SAME eligible session (a compatibility bridge, NOT
    the Milestone-2 Holding Opportunity-Cost engine).

SCOPE BOUNDARY (Slice 3): this module ORCHESTRATES existing owners; it does NOT
consolidate scoring (Slice 4), portfolio state (Slice 5), or implement the holding
opportunity-cost engine (Milestone 2). It NEVER executes the operational Daily Close
(a separate workflow), never auto-confirms a target, never promotes a model, never
recalibrates coefficients, and never creates a legacy Signal / TradeDecision / order
/ fill. Forward evidence is captured through the canonical evidence owner; Daily
Close idempotently confirms the SAME immutable bundle (one owner, one store).

Persistence: one canonical run manifest per (eligible session, input contract) under
a research root (``PAPER_TRADER_DRC_DIR``; never the operational ledger root except
through the already-authoritative evidence owner). Manifests are content-hashed and
atomically written; a completed run is reused; a safe incomplete run resumes.

Read-only STATUS (``load_daily_research_cycle_status``) plans deterministically and
reflects the latest persisted run without executing anything. Execution
(``run_daily_research_cycle``) is token-gated (``RUN_DAILY_RESEARCH_CYCLE``).
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import data_freshness as df
from paper_trader.engine import market_session as msession

PHASE = "29D-Slice3"

# Frozen confirmation token (part of the tested contract — Workstream N).
EXECUTE_CONFIRMATION = "RUN_DAILY_RESEARCH_CYCLE"

# --------------------------------------------------------------------------- #
# Frozen run-state vocabulary (Workstream B). First-matching-condition wins in the
# state machine; the exact precedence is documented inline and in
# docs/ARCHITECTURE_DECISIONS.md (D-13).
# --------------------------------------------------------------------------- #
NOT_STARTED = "NOT_STARTED"
WAITING_FOR_SESSION_CLOSE = "WAITING_FOR_SESSION_CLOSE"
WAITING_FOR_OWNED_DATA = "WAITING_FOR_OWNED_DATA"
PLANNING = "PLANNING"
REFRESHING_REQUIRED_INPUTS = "REFRESHING_REQUIRED_INPUTS"
VALIDATING_INPUT_ALIGNMENT = "VALIDATING_INPUT_ALIGNMENT"
SCORING_UNIVERSE = "SCORING_UNIVERSE"
PREPARING_TARGET = "PREPARING_TARGET"
CAPTURING_FORWARD_EVIDENCE = "CAPTURING_FORWARD_EVIDENCE"
RUNNING_PORTFOLIO_ASSESSMENT = "RUNNING_PORTFOLIO_ASSESSMENT"
COMPLETE = "COMPLETE"
COMPLETE_WITH_EVIDENCE_GAP = "COMPLETE_WITH_EVIDENCE_GAP"
BLOCKED = "BLOCKED"
FAILED = "FAILED"
INCONSISTENT = "INCONSISTENT"
RUN_IN_PROGRESS = "RUN_IN_PROGRESS"

RUN_STATES = (
    NOT_STARTED, WAITING_FOR_SESSION_CLOSE, WAITING_FOR_OWNED_DATA, PLANNING,
    REFRESHING_REQUIRED_INPUTS, VALIDATING_INPUT_ALIGNMENT, SCORING_UNIVERSE,
    PREPARING_TARGET, CAPTURING_FORWARD_EVIDENCE, RUNNING_PORTFOLIO_ASSESSMENT,
    COMPLETE, COMPLETE_WITH_EVIDENCE_GAP, BLOCKED, FAILED, INCONSISTENT,
    RUN_IN_PROGRESS,
)
# Terminal states (no further work for this contract).
_TERMINAL = frozenset({COMPLETE, COMPLETE_WITH_EVIDENCE_GAP, BLOCKED, FAILED,
                       INCONSISTENT})
# In-flight (execution) states.
_RUNNING = frozenset({PLANNING, REFRESHING_REQUIRED_INPUTS, VALIDATING_INPUT_ALIGNMENT,
                      SCORING_UNIVERSE, PREPARING_TARGET, CAPTURING_FORWARD_EVIDENCE,
                      RUNNING_PORTFOLIO_ASSESSMENT})
# Completion states that carry usable research outputs.
_COMPLETED = frozenset({COMPLETE, COMPLETE_WITH_EVIDENCE_GAP})

# Frozen inconsistency / persistence reason codes (Phase 29G.3 — Workstream B/C).
#   * A terminal COMPLETE run whose manifest cannot be validated or durably read back is
#     NEVER returned as COMPLETE; it is downgraded to INCONSISTENT with these codes while
#     the durable downstream-artifact references (evidence / HOC artifact) are preserved.
#   * When terminal downstream artifacts (an immutable Holding Opportunity-Cost artifact)
#     exist for the eligible session but the DRC run manifest is missing, STATUS reports
#     INCONSISTENT with this exact code (never NOT_STARTED) and a safe idempotent recovery
#     action — it never silently synthesises COMPLETE from downstream artifacts.
MANIFEST_CONTRACT_INCOMPLETE = "MANIFEST_CONTRACT_INCOMPLETE"
MANIFEST_PERSISTENCE_UNVERIFIED = "MANIFEST_PERSISTENCE_UNVERIFIED"
TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST = (
    "TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST")

# --------------------------------------------------------------------------- #
# Frozen ordered step vocabulary (Workstream B / C). One step_result per step.
# --------------------------------------------------------------------------- #
STEP_RESOLVE_SESSION = "RESOLVE_SESSION"
STEP_VALIDATE_CONSISTENCY = "VALIDATE_ACTIVE_BOOK_CONSISTENCY"
STEP_PLAN = "BUILD_EXECUTION_PLAN"
STEP_REFRESH_INPUTS = "REFRESH_REQUIRED_INPUTS"
STEP_VALIDATE_ALIGNMENT = "VALIDATE_INPUT_ALIGNMENT"
STEP_SCORE_UNIVERSE = "SCORE_UNIVERSE"
STEP_PREPARE_TARGET = "PREPARE_TARGET"
STEP_CAPTURE_EVIDENCE = "CAPTURE_FORWARD_EVIDENCE"
STEP_HOLDING_OPP_COST = "ASSESS_HOLDING_OPPORTUNITY_COST"
#: Stage 20 — the portfolio-level ECONOMIC CHANGE GATE. It sits between the per-holding
#: opportunity-cost assessment and the target engine: before Stage 20 the cycle built a
#: reallocation proposal on EVERY signal refresh, so the system rebalanced by default and
#: asked the operator to say no. Now the reassessment decides whether change is
#: economically justified at all, and BUILD_REALLOCATION_PROPOSAL runs only when it is.
STEP_REASSESS_PORTFOLIO = "REASSESS_PORTFOLIO"
STEP_BUILD_REALLOCATION = "BUILD_REALLOCATION_PROPOSAL"
STEP_RUN_RESEARCH_AGENT = "RUN_RESEARCH_AGENT"
STEP_RUN_ASSESSMENT = "RUN_PORTFOLIO_ASSESSMENT"

STEP_SEQUENCE = (
    STEP_RESOLVE_SESSION, STEP_VALIDATE_CONSISTENCY, STEP_PLAN, STEP_REFRESH_INPUTS,
    STEP_VALIDATE_ALIGNMENT, STEP_SCORE_UNIVERSE, STEP_PREPARE_TARGET,
    STEP_CAPTURE_EVIDENCE, STEP_HOLDING_OPP_COST, STEP_REASSESS_PORTFOLIO,
    STEP_BUILD_REALLOCATION, STEP_RUN_RESEARCH_AGENT, STEP_RUN_ASSESSMENT,
)

# Frozen step-status vocabulary.
S_PENDING = "PENDING"
S_OK = "OK"
S_REUSED = "REUSED"
S_SKIPPED = "SKIPPED"
S_BLOCKED = "BLOCKED"
S_FAILED = "FAILED"

# Frozen date-alignment vocabulary (Workstream F).
ALIGNED = "ALIGNED"
ALIGNED_WITH_SLOWER_CADENCE = "ALIGNED_WITH_SLOWER_CADENCE"
BLOCKED_STALE_REQUIRED_INPUT = "BLOCKED_STALE_REQUIRED_INPUT"
BLOCKED_MISSING_INPUT = "BLOCKED_MISSING_INPUT"
BLOCKED_FUTURE_DATA = "BLOCKED_FUTURE_DATA"
BLOCKED_INCONSISTENT_INPUT = "BLOCKED_INCONSISTENT_INPUT"
ALIGNMENT_UNKNOWN = "UNKNOWN"
ALIGNMENT_VOCAB = (ALIGNED, ALIGNED_WITH_SLOWER_CADENCE, BLOCKED_STALE_REQUIRED_INPUT,
                   BLOCKED_MISSING_INPUT, BLOCKED_FUTURE_DATA,
                   BLOCKED_INCONSISTENT_INPUT, ALIGNMENT_UNKNOWN)

# Cross-surface consistency vocabulary (shared with data_freshness Slice 1).
CONSISTENT = "CONSISTENT"
UNKNOWN = "UNKNOWN"

# Honest missing-implementation token when a due monthly momentum input has no wired
# emitter behind the canonical adapter (api.monthly_momentum_input). The frozen
# mom_6_1 monthly contract is never approximated intramonth. Matches alpha_target's
# required_next_action and monthly_momentum_input.MONTHLY_EMITTER_ACTION.
MONTHLY_EMITTER_ACTION = "RUN_RESEARCH_MONTHLY_INPUT_EMITTER"

# Accurate canonical badges (Phase 29D.1 audit): the Daily Research Cycle refreshes
# owned inputs, scores the universe and captures immutable forward evidence — it does
# NOT create legacy Signal rows (created_signals=False), so the badge reads "CREATES
# RESEARCH EVIDENCE ONLY", not the inaccurate "CREATES SIGNALS ONLY".
SAFETY_BADGES = ["PREVIEW ONLY", "PAPER ONLY", "NO LIVE ORDERS", "AUTOMATION OFF",
                 "MANUAL REVIEW", "CREATES RESEARCH EVIDENCE ONLY"]

# Persistence root. Never the operational ledger root (C:\Users\binis\.paper_trader);
# forward evidence is persisted through the canonical evidence owner, not here.
DRC_DIR_ENV = "PAPER_TRADER_DRC_DIR"
_DEFAULT_DRC_DIR = Path(r"D:\Stock_Prediction_app_data\daily_research_cycle")
_RUNS_SUBDIR = "runs"
_INDEX_FILE = "index.json"
_LOCK_FILE = "research_cycle.lock"
_LOCK_STALE_MINUTES = 45

# Read-only downstream-artifact root (Phase 29G.3 — Workstream C). Used ONLY to detect
# whether an immutable Holding Opportunity-Cost artifact already exists for an eligible
# session whose DRC run manifest is missing (the split-brain recovery signal). The DRC
# never writes here; it composes the canonical HOC read owner.
HOC_DIR_ENV = "PAPER_TRADER_HOC_DIR"
_DEFAULT_HOC_DIR = Path(r"D:\Stock_Prediction_app_data\holding_opportunity_cost")

# Slice 7 (Phase 29H) immutable reallocation-proposal root. The DRC never writes here
# directly; it composes the canonical Reallocation Proposal owner (api.reallocation_proposal),
# which persists under this root (isolated per run under a sandbox override).
REALLOC_DIR_ENV = "PAPER_TRADER_REALLOC_DIR"
_DEFAULT_REALLOC_DIR = Path(r"D:\Stock_Prediction_app_data\reallocation_proposals")

# Stage 20 immutable portfolio-reassessment root. The DRC never writes here directly; it
# composes the canonical Portfolio Reassessment owner (api.portfolio_reassessment), which
# persists under this root (isolated per run under a sandbox override).
REASSESSMENT_DIR_ENV = "PAPER_TRADER_REASSESSMENT_DIR"
_DEFAULT_REASSESSMENT_DIR = Path(r"D:\Stock_Prediction_app_data\portfolio_reassessments")

# Deterministic clock seam (tests / explicit callers).
NOW_ENV = "PAPER_TRADER_DRC_NOW"
_now_override: Optional[datetime] = None

# In-process single-flight guard (cross-process is the file lock below).
_RUN_LOCK = threading.Lock()

# Sentinel: "resolve the production monthly-emitter default" (distinct from an
# explicit None = no emitter). Tests always pass an explicit value; the production
# endpoint passes neither, so the canonical monthly adapter is wired here.
_DEFAULT_MONTHLY = object()


# --------------------------------------------------------------------------- #
# Small pure helpers.
# --------------------------------------------------------------------------- #
def _now() -> datetime:
    if _now_override is not None:
        return _now_override
    raw = os.environ.get(NOW_ENV)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


def _hash(payload: Any) -> str:
    """Deterministic short content hash over a JSON-canonicalised payload."""
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


def _safe(loader: Callable, warnings: list, label: str) -> Any:
    try:
        return loader()
    except Exception as exc:  # noqa: BLE001
        warnings.append("%s unavailable: %s" % (label, str(exc)[:160]))
        return None


def _safety(performed_write: bool = False) -> dict:
    return {
        "read_only": not performed_write,
        "preview_only": True,
        "paper_only": True,
        "manual_review": True,
        "automation_off": True,
        "wrote_to_operational_ledger": False,
        "wrote_to_database": False,
        "called_prediction": False,
        "ran_daily_close": False,
        "changed_holdings": False,
        "changed_cash_or_nav": False,
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "created_fills": False,
        "created_proposals": False,
        "promoted_model": False,
        "recalibrated_model": False,
        "automatic_promotion_allowed": False,
        "performed_research_write": bool(performed_write),
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# Persistence store (Workstream M). Content-hashed, atomically-written run
# manifests under a research root. NOT the operational ledger root.
# --------------------------------------------------------------------------- #
def _drc_dir(drc_dir=None) -> Path:
    if drc_dir is not None:
        return Path(drc_dir)
    raw = os.environ.get(DRC_DIR_ENV)
    return Path(raw) if raw else _DEFAULT_DRC_DIR


def _runs_dir(drc_dir=None) -> Path:
    return _drc_dir(drc_dir) / _RUNS_SUBDIR


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(blob)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _load_run(run_id: str, drc_dir=None) -> Optional[dict]:
    if not run_id:
        return None
    return _read_json(_runs_dir(drc_dir) / ("%s.json" % run_id))


def _save_run(record: dict, drc_dir=None) -> None:
    _atomic_write_json(_runs_dir(drc_dir) / ("%s.json" % record["run_id"]), record)


def _load_index(drc_dir=None) -> dict:
    return _read_json(_drc_dir(drc_dir) / _INDEX_FILE) or {}


def _update_index(*, eligible_date: str, idempotency_key: str,
                  input_contract_hash: str, run_id: str, state: str,
                  drc_dir=None) -> None:
    idx = _load_index(drc_dir)
    idx[eligible_date] = {
        "idempotency_key": idempotency_key,
        "input_contract_hash": input_contract_hash,
        "run_id": run_id, "state": state, "updated_at": _now_iso(),
    }
    _atomic_write_json(_drc_dir(drc_dir) / _INDEX_FILE, idx)


# --------------------------------------------------------------------------- #
# Bounded, recoverable cross-process lock (Workstream L). Stale locks are
# classified, never blindly trusted; unrelated reads are never blocked.
# --------------------------------------------------------------------------- #
def _lock_path(drc_dir=None) -> Path:
    return _drc_dir(drc_dir) / _LOCK_FILE


def _read_lock(drc_dir=None) -> Optional[dict]:
    return _read_json(_lock_path(drc_dir))


def _lock_is_stale(lock: dict) -> bool:
    started = _parse_dt(lock.get("started_at"))
    if started is None:
        return True
    age_min = (_now() - started).total_seconds() / 60.0
    return age_min > _LOCK_STALE_MINUTES


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _write_lock(*, idempotency_key: str, eligible_date: str, run_id: str,
                input_contract_hash: str, drc_dir=None) -> None:
    _atomic_write_json(_lock_path(drc_dir), {
        "idempotency_key": idempotency_key, "eligible_date": eligible_date,
        "run_id": run_id, "input_contract_hash": input_contract_hash,
        "started_at": _now_iso(), "pid": os.getpid()})


def _clear_lock(drc_dir=None) -> None:
    try:
        _lock_path(drc_dir).unlink()
    except OSError:
        pass


# --------------------------------------------------------------------------- #
# Idempotency (Workstream L). The key is derived from the proven contract; the
# input-contract hash is derived from the resolved input as-of dates.
# --------------------------------------------------------------------------- #
def _input_contract_hash(dates: dict) -> str:
    """Deterministic hash of the resolved research input as-of dates (the contract
    the scored universe / target / evidence are built against)."""
    keys = ("owned_price_date", "desk_mark_date", "benchmark_date", "valuation_date",
            "price_score_refresh_date", "monthly_input_date", "fundamental_date",
            "target_calc_date")
    return _hash({k: dates.get(k) for k in keys})


# Session-stable identity (Phase 29G.3 — Workstream C/D). The idempotency key and the
# raw ``input_contract_hash`` above depend on the FAST daily inputs the cycle itself
# refreshes to the eligible session (price_score_refresh / target_calc) — so a status
# read AFTER a run refreshed those inputs recomputes a DIFFERENT hash than the run
# persisted, which previously reset a COMPLETE run to NOT_STARTED (the split-brain).
# This hash keys ONLY the identity that is invariant across the cycle's own refresh:
# the eligible session, the active book, the frozen strategy / universe, and the
# SLOW-cadence inputs the cycle does NOT refresh (the monthly momentum month and the
# fundamental as-of quarter). Two runs for the same eligible session with the same slow
# inputs share this hash even when their fast inputs advanced; a genuinely different
# slow-input contract for the same date does NOT (first-write-wins is still enforced).
def _session_contract_hash(*, eligible: Any, active_book_id: Any, dates: dict) -> str:
    return _hash({
        "eligible_market_date": _iso(_coerce_date(eligible)),
        "active_book_id": active_book_id,
        "strategy_version": STRATEGY_VERSION, "universe_id": UNIVERSE_ID,
        "monthly_input_date": dates.get("monthly_input_date"),
        "fundamental_date": dates.get("fundamental_date")})


# --------------------------------------------------------------------------- #
# Read-only downstream-artifact probe (Workstream C). Detects whether an immutable
# Holding Opportunity-Cost artifact already exists for (active book, eligible session)
# WITHOUT a DRC run manifest — the split-brain recovery signal. Composes the canonical
# HOC read owner (never re-reads its store schema, never runs the engine, never writes).
# --------------------------------------------------------------------------- #
def _default_downstream_probe(*, active_book_id: Any, eligible: Any, hoc_dir=None) -> dict:
    from paper_trader.api import holding_opportunity_cost as hoc
    summ = hoc.load_assessment_summary(active_book_id=active_book_id,
                                       eligible_market_date=eligible, hoc_dir=hoc_dir) or {}
    return {
        "present": bool(summ.get("opportunity_cost_available")),
        "kind": "holding_opportunity_cost",
        "owner": "api.holding_opportunity_cost",
        "assessment_hash": summ.get("opportunity_cost_assessment_hash"),
        "state": summ.get("opportunity_cost_state"),
        "recommendation_counts": summ.get("opportunity_cost_recommendation_counts") or {},
        "data_gaps": list(summ.get("opportunity_cost_data_gaps") or []),
    }


# Required manifest fields for a TERMINAL-COMPLETE run (Workstream B rule 2). A COMPLETE
# manifest that omits any of these — or whose opportunity-cost step ran OK without an
# artifact reference — is NOT durable and must never be returned as COMPLETE.
_REQUIRED_TERMINAL_FIELDS = (
    "run_id", "idempotency_key", "session_contract_hash", "active_book_id",
    "eligible_market_date", "started_at", "completed_at", "state",
    "input_contract_hash")


def _validate_terminal_manifest(rec: dict) -> list:
    """Return a list of contract problems for a terminal COMPLETE manifest (empty ⇒ OK)."""
    problems: list[str] = []
    for f in _REQUIRED_TERMINAL_FIELDS:
        if rec.get(f) in (None, ""):
            problems.append("missing %s" % f)
    if not rec.get("completed_steps"):
        problems.append("no completed_steps")
    if not rec.get("step_results"):
        problems.append("no step_results")
    # If the Holding Opportunity-Cost step actually RAN (S_OK), a COMPLETE manifest must
    # carry its immutable artifact reference (id + assessment hash). A hermetically SKIPPED
    # HOC step (sandboxed run without canonical scoring) is exempt.
    hoc_step = next((s for s in (rec.get("step_results") or [])
                     if s.get("step_id") == STEP_HOLDING_OPP_COST), None)
    if hoc_step and hoc_step.get("status") == S_OK:
        if not rec.get("opportunity_cost_artifact_id") \
                or not rec.get("opportunity_cost_assessment_hash"):
            problems.append("opportunity-cost step OK but artifact reference missing")
    # Stage 20: if the Portfolio Reassessment step actually RAN (S_OK), a COMPLETE
    # manifest must carry its immutable reassessment reference (id + reassessment hash).
    # A hermetically SKIPPED reassessment step (no canonical scoring / no HOC) is exempt.
    prs_step = next((s for s in (rec.get("step_results") or [])
                     if s.get("step_id") == STEP_REASSESS_PORTFOLIO), None)
    if prs_step and prs_step.get("status") == S_OK:
        if not rec.get("portfolio_reassessment_id") \
                or not rec.get("portfolio_reassessment_hash"):
            problems.append("portfolio-reassessment step OK but artifact reference missing")
    # Slice 7 (Phase 29H): if the Reallocation Proposal step actually RAN (S_OK), a
    # COMPLETE manifest must carry its immutable proposal reference (id + proposal hash).
    # A hermetically SKIPPED / BLOCKED reallocation step (no canonical scoring / no HOC)
    # is exempt.
    realloc_step = next((s for s in (rec.get("step_results") or [])
                         if s.get("step_id") == STEP_BUILD_REALLOCATION), None)
    if realloc_step and realloc_step.get("status") == S_OK:
        if not rec.get("reallocation_proposal_id") \
                or not rec.get("reallocation_proposal_hash"):
            problems.append("reallocation step OK but proposal reference missing")
    # Slice 8 (Phase 29I): if the Research Agent step actually RAN (S_OK), a COMPLETE
    # manifest must carry its immutable assessment reference (id + assessment hash). A
    # hermetically SKIPPED / BLOCKED research-agent step (no canonical scoring / no active
    # book) is exempt.
    ra_step = next((s for s in (rec.get("step_results") or [])
                    if s.get("step_id") == STEP_RUN_RESEARCH_AGENT), None)
    if ra_step and ra_step.get("status") == S_OK:
        if not rec.get("research_agent_id") or not rec.get("research_agent_hash"):
            problems.append("research-agent step OK but assessment reference missing")
    return problems


def compute_idempotency_key(*, eligible_market_date: Any, active_book_id: Any,
                            strategy_version: Any, universe_id: Any,
                            input_contract_hash: str) -> str:
    return _hash({
        "eligible_market_date": _iso(_coerce_date(eligible_market_date)),
        "active_book_id": active_book_id, "strategy_version": strategy_version,
        "universe_id": universe_id, "input_contract_hash": input_contract_hash})


# --------------------------------------------------------------------------- #
# Refresh-owner registry (Workstream D / E). Which authoritative owner refreshes
# each REQUIRED research input, its declared cadence, and whether a safe automatic
# refresh path exists. Owned OPERATIONAL marks are prerequisites (session / owned
# gate), NOT refreshed here; slower non-required inputs are informational.
# --------------------------------------------------------------------------- #
_REFRESH_OWNERS = {
    "price_score_refresh": {
        "owner": "api.alpha_target.run_refresh",
        "cadence": df.DAILY, "auto_refreshable": True,
        "role": "daily_price_score_input"},
    "momentum_monthly": {
        # Phase 29D.1: the frozen monthly momentum input now has a DECLARED canonical
        # in-repo owner (the monthly-input adapter), which wraps an injectable emitter
        # seam, validates schema/period/provenance, is idempotent and NEVER approximates
        # intramonth. When no emitter is wired it BLOCKS honestly (owned by the adapter),
        # never NO_REFRESH_OWNER and never a "run a separate button" prerequisite.
        "owner": "api.monthly_momentum_input",
        "cadence": df.MONTHLY, "auto_refreshable": False,
        "missing_implementation": MONTHLY_EMITTER_ACTION,
        "role": "frozen_monthly_momentum_input"},
    # target_calculation is a REQUIRED reassessment input in data_freshness, but it is
    # an OUTPUT the cycle PREPARES downstream (STEP_PREPARE_TARGET → the canonical target
    # owner api.alpha_target.load_readiness), NOT a pre-scoring input to refresh. It has a
    # declared owner and is NEVER a NO_REFRESH_OWNER blocker (Phase 29D.1 correction).
    "target_calculation": {
        "owner": "api.alpha_target.load_readiness",
        "cadence": df.DAILY, "auto_refreshable": True,
        "prepared_downstream_by": STEP_PREPARE_TARGET,
        "role": "operational_target_preparation"},
}
# The frozen strategy / universe identity (from the champion registry). Recorded on
# every run; never changed mid-run (Workstream R).
STRATEGY_ID = "fundamental_momentum_50_50_v1"
STRATEGY_VERSION = "v1"
UNIVERSE_ID = "phase8v_combined_eodhd_price_fundamentals_universe"


# --------------------------------------------------------------------------- #
# Execution plan (Workstream D). Read-only: derived from api.data_freshness rows +
# the proven refresh-owner registry. Distinguishes current / required-stale /
# slower-due / missing / inconsistent / future-dated / non-blocking / unsupported.
# --------------------------------------------------------------------------- #
def build_execution_plan(freshness: dict, *, monthly_emitter_available: bool = False
                         ) -> dict:
    rows = freshness.get("source_freshness") or []
    eligible = freshness.get("eligible_market_date")
    steps: list[dict] = []
    current: list[str] = []
    required_stale: list[str] = []
    slower_due: list[str] = []
    missing: list[str] = []
    inconsistent: list[str] = []
    future_dated: list[str] = []
    nonblocking: list[str] = []
    unsupported: list[dict] = []
    prepared_downstream: list[dict] = []
    blockers: list[dict] = []

    for r in rows:
        sid = r["source_id"]
        status = r.get("status")
        required = bool(r.get("required_for_signal_refresh")
                        or r.get("required_for_portfolio_reassessment")
                        or r.get("required_for_true_forward_capture"))
        if status in df._SATISFIED:
            current.append(sid)
            continue
        if status == df.INCONSISTENT:
            inconsistent.append(sid)
        elif status == df.FUTURE_DATED:
            future_dated.append(sid)
        elif status == df.MISSING:
            missing.append(sid)
        if r.get("cadence") in (df.MONTHLY, df.QUARTERLY) and status == df.STALE:
            slower_due.append(sid)
        if not required:
            if status not in df._SATISFIED:
                nonblocking.append(sid)
            continue

        # A REQUIRED, non-satisfied input.
        spec = _REFRESH_OWNERS.get(sid)
        if spec is None:
            required_stale.append(sid)
            unsupported.append({"source_id": sid, "status": status,
                                "reason": "No proven authoritative refresh owner."})
            blockers.append({"code": "NO_REFRESH_OWNER", "source_id": sid,
                             "detail": r.get("operator_action")})
            continue
        # An input the cycle PREPARES downstream (e.g. the operational target via
        # STEP_PREPARE_TARGET) has a declared owner but is NOT a pre-scoring refresh
        # step and NEVER blocks the plan — it is produced by its own later step.
        if spec.get("prepared_downstream_by"):
            prepared_downstream.append({
                "source_id": sid, "status": status,
                "authoritative_owner": spec["owner"],
                "prepared_by_step": spec["prepared_downstream_by"],
                "expected_output_date": eligible,
                "role": spec.get("role")})
            continue
        # A REQUIRED, non-satisfied input to refresh (or an unsupported blocker).
        required_stale.append(sid)
        auto = bool(spec["auto_refreshable"]) or (
            sid == "momentum_monthly" and monthly_emitter_available)
        step = {
            "step_id": STEP_REFRESH_INPUTS, "source_id": sid,
            "display_name": r.get("display_name"), "status": status,
            "cadence": spec["cadence"], "authoritative_owner": spec["owner"],
            "expected_output_date": eligible,
            "can_refresh_automatically": auto,
            "blocking_scope": "signal_refresh+true_forward",
            "dependencies": ["owned_daily_prices", "desk_marks"],
        }
        if not auto:
            step["operator_provider_blocker"] = spec.get("missing_implementation",
                                                          "OPERATOR_ACTION_REQUIRED")
            blockers.append({
                "code": "UNSUPPORTED_AUTOMATIC_REFRESH", "source_id": sid,
                "missing_implementation": spec.get("missing_implementation"),
                "detail": ("The frozen monthly momentum input is due but no safe "
                           "automatic emitter is available; it is never approximated "
                           "intramonth.") if sid == "momentum_monthly"
                          else r.get("operator_action")})
        steps.append(step)

    plan_blocked = bool(blockers) or bool(inconsistent) or bool(future_dated) \
        or bool(missing and set(missing) & set(required_stale))
    return {
        "eligible_market_date": eligible,
        "current_inputs": sorted(current),
        "required_stale_inputs": sorted(required_stale),
        "slower_inputs_due": sorted(slower_due),
        "missing_inputs": sorted(missing),
        "inconsistent_inputs": sorted(inconsistent),
        "future_dated_inputs": sorted(future_dated),
        "nonblocking_inputs": sorted(nonblocking),
        "unsupported_refreshes": unsupported,
        "prepared_downstream_inputs": prepared_downstream,
        "refresh_steps": steps,
        "blockers": blockers,
        "plan_blocked": plan_blocked,
        "minimal": (not steps and not blockers),
    }


# --------------------------------------------------------------------------- #
# Date-alignment gate (Workstream F). Re-reads the freshness contract (post-refresh
# in the run path) and requires the REQUIRED research inputs to be satisfied and
# consistent before scoring / evidence capture. Unknown is not fresh.
# --------------------------------------------------------------------------- #
def evaluate_alignment(freshness: dict) -> dict:
    rows = freshness.get("source_freshness") or []
    consistency = freshness.get("consistency_status")
    required = [r for r in rows if (r.get("required_for_signal_refresh")
                                    or r.get("required_for_true_forward_capture"))]
    checks = []
    status = ALIGNED
    slower_ok = False
    for r in required:
        st = r.get("status")
        checks.append({"source_id": r["source_id"], "cadence": r.get("cadence"),
                       "as_of_date": r.get("as_of_date"), "status": st})
        if st == df.FUTURE_DATED:
            status = BLOCKED_FUTURE_DATA
        elif st == df.INCONSISTENT and status not in (BLOCKED_FUTURE_DATA,):
            status = BLOCKED_INCONSISTENT_INPUT
        elif st == df.MISSING and status in (ALIGNED, ALIGNED_WITH_SLOWER_CADENCE):
            status = BLOCKED_MISSING_INPUT
        elif st == df.STALE and status in (ALIGNED, ALIGNED_WITH_SLOWER_CADENCE):
            status = BLOCKED_STALE_REQUIRED_INPUT
        elif st == df.UNKNOWN and status == ALIGNED:
            status = ALIGNMENT_UNKNOWN
        if st == df.NOT_DUE:
            slower_ok = True
    if consistency == df.INCONSISTENT and status == ALIGNED:
        status = BLOCKED_INCONSISTENT_INPUT
    if status == ALIGNED and slower_ok:
        status = ALIGNED_WITH_SLOWER_CADENCE
    passed = status in (ALIGNED, ALIGNED_WITH_SLOWER_CADENCE)
    return {"status": status, "passed": passed, "checks": checks,
            "consistency_status": consistency,
            "eligible_market_date": freshness.get("eligible_market_date")}


# --------------------------------------------------------------------------- #
# Step-result builder (Workstream C). Every step carries the full audit contract.
# --------------------------------------------------------------------------- #
def _step(step_id, status, *, owner=None, as_of_date=None, input_hash=None,
          output_hash=None, records_read=0, records_written=0, reused=False,
          reason=None, error_code=None, error_detail=None,
          started_at=None, completed_at=None) -> dict:
    return {
        "step_id": step_id, "status": status,
        "started_at": started_at or _now_iso(),
        "completed_at": completed_at or _now_iso(),
        "owner": owner, "as_of_date": as_of_date,
        "input_hash": input_hash, "output_hash": output_hash,
        "records_read": records_read, "records_written": records_written,
        "reused": bool(reused), "reason": reason,
        "error_code": error_code, "error_detail": error_detail,
    }


# --------------------------------------------------------------------------- #
# Adapter seams (lazy imports; each injectable so tests never call a provider,
# prediction, or a real cycle). Defaults invoke the existing authoritative owners.
# --------------------------------------------------------------------------- #
def _default_freshness_loader(**kw):
    return df.load_data_freshness(**kw)


def _default_daily_refresh_fn(*, confirm, downloader, completed_through):
    from paper_trader.api import alpha_target as at
    return at.run_refresh(confirm=confirm, downloader=downloader,
                          completed_through=completed_through)


def _default_scoring_fn():
    # Slice 4: the Daily Research Cycle no longer interprets the raw scoring kernel
    # output directly - it delegates to the canonical composition owner, which calls
    # api.multi_horizon_engine.build_current exactly once, deep-copies it, and
    # normalises it into the frozen universe-scoring contract.
    from paper_trader.api import universe_scoring as us
    return us.build_universe_scoring()


def _default_target_loader():
    from paper_trader.api import alpha_target as at
    return at.load_readiness()


def _default_evidence_registry():
    from paper_trader.api import forward_prediction_skill as fps
    return list(fps.SUPPORTED_BOOKS)


def _default_evidence_capture_fn(*, market_date, current, ops, downloader):
    from paper_trader.api import forward_prediction_skill as fps
    return fps.capture_for_daily_close(market_date=market_date, current=current,
                                       ops=ops, downloader=downloader)


def _default_assessment_loader(*, today):
    from paper_trader.api import daily_action_gate as dag
    return dag.load_daily_action_gate(today=today)


def _default_holding_opp_cost_fn(*, scoring=None, hoc_dir=None):
    """The canonical Holding Opportunity-Cost engine (Slice 6, Milestone 2).

    Delegates to ``api.holding_opportunity_cost.run_and_persist`` — the sole
    composition/persistence owner — which sources the immutable input contract from
    ``portfolio_state`` / ``universe_scoring`` / ``price_panel``, runs the pure
    ``engine.holding_opportunity_cost`` kernel and persists the immutable artifact.
    The DRC never computes the opportunity cost itself. ``scoring`` (the canonical
    universe-scoring contract the cycle already built) is reused so the engine does not
    re-score; ``hoc_dir`` isolates the artifact root for a sandboxed run.
    """
    from paper_trader.api import holding_opportunity_cost as hoc
    return hoc.run_and_persist(scoring=scoring, hoc_dir=hoc_dir)


def _default_reassessment_fn(*, scoring=None, hoc_assessment=None, freshness=None,
                             reassessment_dir=None, hoc_dir=None):
    """The canonical Stage-20 Portfolio Reassessment owner (the ECONOMIC CHANGE GATE).

    Delegates to ``api.portfolio_reassessment.run_and_persist`` — the sole
    composition/persistence owner — which sources the immutable input contract from
    ``portfolio_state`` (holdings/weights/NAV/cash/corporate actions), the Slice-6
    ``hoc_assessment`` (per-holding recommendations/replacements/switching costs/risk),
    ``universe_scoring`` (ranking snapshot + frozen champion identity), ``data_freshness``
    (the canonical per-source point-in-time semantics) and its OWN immutable history (the
    churn / whipsaw controls), runs the pure ``engine.portfolio_reassessment`` kernel and
    persists ONE immutable reassessment artifact + ONE append-only history row.

    The DRC decides nothing here: it only asks the owner whether a portfolio change is
    economically justified. ``scoring`` / ``hoc_assessment`` (both already built earlier in
    the SAME cycle) are reused so nothing is recomputed; ``reassessment_dir`` isolates the
    artifact root for a sandboxed run. It creates no target, no order and no fill, and it
    approves nothing."""
    from paper_trader.api import portfolio_reassessment as prs
    return prs.run_and_persist(scoring=scoring, hoc_assessment=hoc_assessment,
                               freshness=freshness, reassessment_dir=reassessment_dir,
                               hoc_dir=hoc_dir)


def _default_reallocation_fn(*, scoring=None, hoc_assessment=None, reallocation_dir=None,
                             hoc_dir=None):
    """The canonical Reallocation Proposal engine (Slice 7, Milestone 3).

    Delegates to ``api.reallocation_proposal.run_and_persist`` — the sole
    composition/persistence owner — which sources the immutable input contract from
    ``portfolio_state`` (holdings/weights/NAV/cash) + the Slice-6 ``hoc_assessment``
    (recommendations/replacements/switching costs) + ``universe_scoring`` (candidate
    ranking) + ``price_panel`` (owned returns), runs the pure
    ``engine.reallocation_proposal`` kernel and persists the immutable proposal artifact.
    The DRC computes no allocation itself. ``scoring`` / ``hoc_assessment`` (both already
    built earlier in the SAME cycle) are reused so nothing is recomputed; ``reallocation_dir``
    isolates the artifact root for a sandboxed run; ``hoc_dir`` locates the HOC artifact
    when the assessment object is not passed. Review-only: it confirms no target and
    creates no order."""
    from paper_trader.api import reallocation_proposal as rp
    return rp.run_and_persist(scoring=scoring, hoc_assessment=hoc_assessment,
                              reallocation_dir=reallocation_dir, hoc_dir=hoc_dir)


def _default_research_agent_fn(*, scoring=None, reallocation=None, research_agent_dir=None,
                               hoc_dir=None, reallocation_dir=None, desk_dir=None):
    """The canonical Persistent Alpha Research Agent (Slice 8, Milestone 4).

    Delegates to ``api.research_agent.run_and_persist`` — the sole composition/persistence
    owner — which sources the immutable research-evidence contract from the authoritative
    evidence owners (champion/challenger identity, matured forward rank IC / decile spread,
    realized benchmark-relative performance / drawdown / turnover, the Slice-6 HOC and
    Slice-7 reallocation immutable histories, regime evidence), runs the pure
    ``engine.research_agent`` kernel and persists the immutable research-assessment artifact.
    The DRC evaluates no research state itself. ``scoring`` (the canonical universe-scoring
    contract the cycle already built) is reused for the champion identity + ranking
    coverage; ``hoc_dir`` / ``reallocation_dir`` locate the Slice-6/7 histories the agent
    reads; ``research_agent_dir`` isolates the artifact root for a sandboxed run.
    RESEARCH GOVERNANCE ONLY: it recommends bounded research actions but promotes / mutates
    nothing and creates no order."""
    from paper_trader.api import research_agent as ra
    return ra.run_and_persist(scoring=scoring, reallocation=reallocation,
                              research_agent_dir=research_agent_dir, hoc_dir=hoc_dir,
                              reallocation_dir=reallocation_dir, desk_dir=desk_dir)


def _default_refresh_confirm_token() -> str:
    from paper_trader.api import alpha_target as at
    return at.REFRESH_CONFIRM_TOKEN


def _default_monthly_emitter_fn():
    """The canonical monthly-input adapter, wired ONLY when an emitter is available
    (opt-in / injected). Default: None → a due month blocks honestly (owned by the
    adapter), never approximated. The adapter itself owns validation / idempotency."""
    from paper_trader.api import monthly_momentum_input as mmi
    return mmi.emit_if_due if mmi.emitter_is_wired() else None


def _default_monthly_available() -> bool:
    from paper_trader.api import monthly_momentum_input as mmi
    return mmi.emitter_is_wired()


def _default_monthly_emitter_status(eligible):
    """Read-only production monthly-emitter status (owner / availability / source-panel
    coverage) for the DRC status contract. Reads no provider; runs no subprocess."""
    from paper_trader.api import monthly_momentum_emitter as mme
    return mme.status(eligible=eligible)


def _monthly_owner_status(freshness: dict, facts: dict, monthly_available: bool,
                          warnings: list, *, emitter_status_fn: Optional[Callable] = None
                          ) -> dict:
    """Workstream G: the canonical monthly-momentum OWNER block exposed by the Daily
    Research Cycle status contract — the DECLARED owner + production producer, owner
    availability, current vs required month, refresh required / selected, the honest
    missing-implementation token, and (only when an emitter is actually wired) the
    owned source-panel date / coverage, emitter status and last error. Read-only; the
    production source-panel detail is never read in the hermetic default (unwired)."""
    cur_month = None
    for r in (freshness.get("source_freshness") or []):
        if r.get("source_id") == "momentum_monthly":
            d = r.get("as_of_date")
            cur_month = (str(d)[:7] if d else None)
            break
    eligible = facts.get("eligible")
    required_month = (str(eligible)[:7] if eligible else None)
    due = bool(required_month and (cur_month is None or required_month > cur_month))
    block = {
        "owner": "api.monthly_momentum_input",
        "producer": "api.monthly_momentum_emitter",
        "math_owner": "research.phase25_multi_horizon_inputs",
        "panel_owner": "research.phase24_daily_panel",
        "available": bool(monthly_available),
        "current_month": cur_month,
        "required_month": required_month,
        "due": due,
        "refresh_required": due,
        "refresh_selected": bool(due and monthly_available),
        "missing_implementation": (None if (monthly_available or not due)
                                   else MONTHLY_EMITTER_ACTION),
        "emitter_status": None,
        "last_error": None,
        "retry_classification": None,
        "source_panel_date": None,
        "source_panel_covered": None,
        "incremental_supported": False,
    }
    if monthly_available:
        fn = emitter_status_fn or _default_monthly_emitter_status
        st = _safe(lambda: fn(eligible), warnings, "Monthly emitter status")
        if isinstance(st, dict):
            sp = st.get("source_panel") or {}
            block["emitter_status"] = ("AVAILABLE" if st.get("available") else "UNAVAILABLE")
            block["source_panel_date"] = sp.get("panel_last_date")
            block["source_panel_covered"] = sp.get("covered")
            block["config"] = st.get("config")
    return block


# --------------------------------------------------------------------------- #
# Scoring adapter (Workstream G / I). The cycle delegates to the canonical
# universe-scoring composition owner (Slice 4) - it no longer interprets the raw
# scoring kernel output. ``built`` may be the canonical contract (the default
# path) or a raw kernel result (injected fakes); ``universe_scoring.normalize_built``
# handles both idempotently. Records the frozen strategy version, exclusions,
# coverage, TOP25 / TOP50 and the CANONICAL content-level input-contract hash. No
# second scoring engine.
# --------------------------------------------------------------------------- #
def _extract_scoring(built: dict) -> dict:
    from paper_trader.api import universe_scoring as us
    contract = us.normalize_built(built if isinstance(built, dict) else {})
    if contract.get("status") != us.STATUS_READY:
        return {"available": False,
                "status": (built or {}).get("status") or contract.get("status"),
                "reason": "Scoring engine inputs unavailable."}

    def _top(rows):
        return [{"ticker": c.get("ticker"), "rank": c.get("rank"),
                 "weight": c.get("weight"), "sector": c.get("sector")}
                for c in (rows or [])]

    top25 = _top(contract.get("top25"))
    top50 = _top(contract.get("top50"))
    return {
        "available": True,
        "strategy_id": STRATEGY_ID, "strategy_version": STRATEGY_VERSION,
        "universe_id": UNIVERSE_ID,
        "scoring_owner": "api.universe_scoring.build_universe_scoring",
        "ranking_date": contract.get("ranking_date"),
        "momentum_month": contract.get("momentum_month"),
        "fundamental_as_of_date": contract.get("fundamental_as_of_date"),
        "universe_size": contract.get("scored_count"),
        "scored_count": contract.get("scored_count"),
        "common_eligible_count": contract.get("combined_eligible_count"),
        "excluded_count": contract.get("excluded_count"),
        "exclusions": contract.get("exclusions") or {},
        "coverage_ratio": contract.get("coverage_ratio"),
        "top25": top25, "top50": top50,
        "top25_count": len(top25), "top50_count": len(top50),
        "primary_book_id": contract.get("primary_book_id"),
        # The CANONICAL content-level scoring input-contract hash (Workstream C);
        # distinct from the run-level date-based input_contract_hash on the facts.
        "input_contract_hash": contract.get("input_contract_hash"),
        "output_hash": _hash({"ranking_date": contract.get("ranking_date"),
                              "top25": [c["ticker"] for c in top25],
                              "top50": [c["ticker"] for c in top50]}),
    }


# --------------------------------------------------------------------------- #
# Resolve the run facts from the freshness contract (Workstream A/C/L).
# --------------------------------------------------------------------------- #
_SOURCE_DATE_KEY = {
    "owned_daily_prices": "owned_price_date", "desk_marks": "desk_mark_date",
    "benchmark": "benchmark_date", "operational_valuation": "valuation_date",
    "price_score_refresh": "price_score_refresh_date",
    "momentum_monthly": "monthly_input_date",
    "fundamental_quarterly": "fundamental_date",
    "target_calculation": "target_calc_date",
}


def _contract_dates(freshness: dict) -> dict:
    m: dict[str, Any] = {}
    for r in (freshness.get("source_freshness") or []):
        k = _SOURCE_DATE_KEY.get(r.get("source_id"))
        if k:
            m[k] = r.get("as_of_date")
    return m


def _facts(freshness: dict) -> dict:
    session = freshness.get("market_session") or {}
    ab = freshness.get("active_book") or {}
    dates = _contract_dates(freshness)
    ich = _input_contract_hash(dates)
    eligible = freshness.get("eligible_market_date")
    key = compute_idempotency_key(
        eligible_market_date=eligible, active_book_id=ab.get("active_book_id"),
        strategy_version=STRATEGY_VERSION, universe_id=UNIVERSE_ID,
        input_contract_hash=ich)
    session_hash = _session_contract_hash(
        eligible=eligible, active_book_id=ab.get("active_book_id"), dates=dates)
    return {
        "session_status": session.get("session_status") or msession.NO_CONFIRMED_DATA,
        "eligible": eligible,
        "expected": freshness.get("expected_completed_market_date"),
        "active_book_id": ab.get("active_book_id"),
        "active_book_name": ab.get("active_book_name"),
        "consistency_status": freshness.get("consistency_status"),
        "consistency_violations": freshness.get("consistency_violations") or [],
        "dates": dates, "input_contract_hash": ich, "idempotency_key": key,
        "session_contract_hash": session_hash,
        "owned_data_confirmed": bool(session.get("latest_confirmed_owned_data_date")),
        "session_operator_action": session.get("operator_action"),
    }


# --------------------------------------------------------------------------- #
# Canonical run/status contract builder (Workstream C).
# --------------------------------------------------------------------------- #
def _contract(*, state: str, facts: dict, run_id: Optional[str] = None,
              plan: Optional[dict] = None, alignment: Optional[dict] = None,
              scoring: Optional[dict] = None, target: Optional[dict] = None,
              evidence: Optional[dict] = None, assessment: Optional[dict] = None,
              step_results: Optional[list] = None, current_step: Optional[str] = None,
              failed_step: Optional[str] = None, reused: bool = False,
              resumed: bool = False, warnings: Optional[list] = None,
              blockers: Optional[list] = None, required_actions: Optional[list] = None,
              started_at: Optional[str] = None, completed_at: Optional[str] = None,
              monthly_owner: Optional[dict] = None, holding_opp_cost: Optional[dict] = None,
              portfolio_reassessment: Optional[dict] = None,
              reallocation_proposal: Optional[dict] = None,
              research_agent: Optional[dict] = None,
              performed_write: bool = False, executable: bool = False) -> dict:
    step_results = step_results or []
    done_ids = [s["step_id"] for s in step_results
                if s["status"] in (S_OK, S_REUSED)]
    skipped_ids = [s["step_id"] for s in step_results if s["status"] == S_SKIPPED]
    pending_ids = [s for s in STEP_SEQUENCE
                   if s not in done_ids and s not in skipped_ids
                   and s != (failed_step or current_step)]
    terminal = state in _TERMINAL
    sc = scoring or {}
    ev = evidence or {}
    tg = target or {}
    asmt = assessment or {}
    aln = alignment or {}
    hoc = holding_opp_cost or {}
    hoc_available = bool(hoc.get("available"))
    prs = portfolio_reassessment or {}
    prs_available = bool(prs.get("available"))
    rp = reallocation_proposal or {}
    rp_available = bool(rp.get("available"))
    ra = research_agent or {}
    ra_available = bool(ra.get("available"))
    return {
        "status": "OK",
        "phase": PHASE,
        "run_id": run_id,
        "idempotency_key": facts.get("idempotency_key"),
        "active_book_id": facts.get("active_book_id"),
        "active_book_name": facts.get("active_book_name"),
        "eligible_market_date": facts.get("eligible"),
        "expected_completed_market_date": facts.get("expected"),
        "evaluated_at": _now_iso(),
        "started_at": started_at,
        "completed_at": completed_at,
        "state": state,
        "state_vocabulary": list(RUN_STATES),
        "terminal": terminal,
        "executable": bool(executable),
        "confirmation_required": EXECUTE_CONFIRMATION,
        "reused_existing_run": bool(reused),
        "resumed_existing_run": bool(resumed),
        "current_step": current_step,
        "completed_steps": done_ids,
        "pending_steps": pending_ids,
        "skipped_steps": skipped_ids,
        "failed_step": failed_step,
        "step_results": step_results,
        "input_plan": plan,
        "monthly_owner": monthly_owner,
        "input_results": (plan or {}).get("refresh_results")
        if plan else None,
        "input_contract_hash": facts.get("input_contract_hash"),
        "session_contract_hash": facts.get("session_contract_hash"),
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "universe_id": UNIVERSE_ID,
        "universe_size": sc.get("universe_size"),
        "scored_count": sc.get("scored_count"),
        "excluded_count": sc.get("excluded_count"),
        "coverage_ratio": sc.get("coverage_ratio"),
        "ranking_date": sc.get("ranking_date"),
        "top25": sc.get("top25"),
        "top50": sc.get("top50"),
        "scoring": scoring,
        "target_date": tg.get("target_date"),
        "target_status": tg.get("target_status"),
        "target_operationally_approved": bool(tg.get("operationally_approved")),
        "target": target,
        "alignment_status": aln.get("status"),
        "alignment": alignment,
        "required_snapshot_count": ev.get("required_snapshot_count"),
        "captured_snapshot_count": ev.get("captured_snapshot_count"),
        "snapshot_bundle_id": ev.get("snapshot_bundle_id"),
        "evidence_status": ev.get("evidence_status"),
        "evidence": evidence,
        "assessment_date": asmt.get("assessment_date"),
        "assessment_status": asmt.get("assessment_status"),
        "assessment_result": asmt.get("assessment_result"),
        "assessment": assessment,
        # --- Slice 6 (Phase 29G) Holding Opportunity-Cost engine (Milestone 2) ---- #
        "opportunity_cost_owner": "api.holding_opportunity_cost",
        "opportunity_cost_state": hoc.get("state"),
        "opportunity_cost_required": True,
        "opportunity_cost_selected": hoc_available,
        "opportunity_cost_artifact_id": hoc.get("artifact_id"),
        "opportunity_cost_assessment_hash": hoc.get("assessment_hash"),
        "opportunity_cost_holding_count": hoc.get("holding_count"),
        "opportunity_cost_recommendation_counts": hoc.get("recommendation_counts"),
        "opportunity_cost_data_gaps": hoc.get("data_gaps"),
        "opportunity_cost_last_error": hoc.get("last_error"),
        "holding_opportunity_cost": holding_opp_cost,
        # --- Stage 20 Portfolio Reassessment — the ECONOMIC CHANGE GATE ----------- #
        "portfolio_reassessment_owner": "api.portfolio_reassessment",
        "portfolio_reassessment_state": prs.get("state"),
        "portfolio_reassessment_required": True,
        "portfolio_reassessment_selected": prs_available,
        "portfolio_reassessment_id": prs.get("reassessment_id"),
        "portfolio_reassessment_hash": prs.get("reassessment_hash"),
        "portfolio_reassessment_decision": prs.get("decision"),
        "portfolio_reassessment_proposal_required": bool(prs.get("proposal_required")),
        "portfolio_reassessment_actionable_count": prs.get("actionable_holding_count"),
        "portfolio_reassessment_expected_net_improvement": prs.get("expected_net_improvement"),
        "portfolio_reassessment_expected_one_way_turnover": prs.get("expected_one_way_turnover"),
        "portfolio_reassessment_blockers": prs.get("blockers"),
        "portfolio_reassessment_explanation": prs.get("explanation"),
        "portfolio_reassessment": portfolio_reassessment,
        # --- Slice 7 (Phase 29H) Reallocation Proposal engine (Milestone 3) ------- #
        "reallocation_proposal_owner": "api.reallocation_proposal",
        "reallocation_proposal_state": rp.get("state"),
        "reallocation_proposal_required": True,
        "reallocation_proposal_selected": rp_available,
        "reallocation_proposal_id": rp.get("proposal_id"),
        "reallocation_proposal_hash": rp.get("proposal_hash"),
        "reallocation_proposed_holding_count": rp.get("proposed_holding_count"),
        "reallocation_action_counts": rp.get("action_counts"),
        "reallocation_one_way_turnover": rp.get("one_way_turnover"),
        "reallocation_estimated_transaction_cost": rp.get("estimated_transaction_cost"),
        "reallocation_score_improvement": rp.get("score_improvement"),
        "reallocation_score_improvement_net_of_cost": rp.get("score_improvement_net_of_cost"),
        "reallocation_data_gaps": rp.get("data_gaps"),
        "reallocation_proposal": reallocation_proposal,
        # --- Slice 8 (Phase 29I) Persistent Alpha Research Agent (Milestone 4) ----- #
        "research_agent_owner": "api.research_agent",
        "research_agent_state": ra.get("state"),
        "research_agent_required": True,
        "research_agent_selected": ra_available,
        "research_agent_id": ra.get("assessment_id"),
        "research_agent_hash": ra.get("assessment_hash"),
        "research_agent_evidence_quality": ra.get("evidence_quality"),
        "research_agent_degradation_categories": ra.get("degradation_categories"),
        "research_agent_recalibration_state": ra.get("recalibration_state"),
        "research_agent_challenger_state": ra.get("challenger_state"),
        "research_agent_top_opportunity": ra.get("top_opportunity"),
        "research_agent_data_gaps": ra.get("data_gaps"),
        "research_agent": research_agent,
        "milestone2_limitation": (
            "The Milestone-2 Holding Opportunity-Cost engine (Slice 6) is implemented and "
            "runs inside this cycle (per-holding rank / deterioration / performance / risk / "
            "liquidity / replacement-candidate / switching-cost / net-improvement review, "
            "recommendation HOLD/REDUCE/EXIT/REPLACE/ADD). It remains review-only: no target "
            "is confirmed and no order is created. Its output feeds the Reallocation Proposal "
            "engine (Slice 7, implemented and review-only) — no recommendation is an approved "
            "reallocation or an order."),
        "milestone3_note": (
            "The Milestone-3 Reallocation Proposal engine (Slice 7) is implemented and runs "
            "inside this cycle after the Holding Opportunity-Cost step: it builds ONE coherent "
            "paper-only proposed target portfolio (retain / reduce / exit / replace / add) with "
            "turnover, transaction cost, before/after score, concentration and risk. It remains "
            "REVIEW ONLY — it confirms no operational or alpha target, creates no order/fill, "
            "changes no holding/cash/NAV and enables no automation. Manual review is mandatory."),
        "warnings": list(warnings or []),
        "blockers": list(blockers or []),
        "required_actions": list(required_actions or []),
        "consistency_status": facts.get("consistency_status"),
        "consistency_violations": facts.get("consistency_violations"),
        "safety": _safety(performed_write),
        "provenance": {
            "phase": PHASE,
            "daily_research_cycle_owner": "api.daily_research_cycle",
            "composed_owners": [
                "data_freshness.load_data_freshness (engine.market_session)",
                "alpha_target.run_refresh (daily price/score input)",
                "monthly_momentum_input.emit_if_due (canonical monthly adapter — "
                "honest DATA_HOLD when no emitter is wired; never approximated)",
                "universe_scoring.build_universe_scoring (canonical universe scoring "
                "over multi_horizon_engine.build_current — Slice 4)",
                "alpha_target.load_readiness (operational target — never auto-confirmed)",
                "forward_prediction_skill.capture_for_daily_close (immutable evidence)",
                "holding_opportunity_cost.run_and_persist (Slice 6 Milestone 2 "
                "opportunity-cost engine over engine.holding_opportunity_cost; review-only)",
                "reallocation_proposal.run_and_persist (Slice 7 Milestone 3 reallocation "
                "proposal engine over engine.reallocation_proposal; review-only)",
                "daily_action_gate.load_daily_action_gate (assessment bridge — consumes the "
                "opportunity-cost summary)",
            ],
            "note": "Read-only status planning; execution invokes the existing owners "
                    "through adapters and NEVER runs the operational Daily Close, "
                    "confirms a target, promotes a model, or creates orders/signals/"
                    "decisions/fills.",
        },
    }


# --------------------------------------------------------------------------- #
# Refresh-result classification (robust to injected fakes and the live owner).
# --------------------------------------------------------------------------- #
def _classify_refresh(res: Optional[dict]) -> str:
    st = str((res or {}).get("status") or "")
    rna = str((res or {}).get("required_next_action") or "")
    if rna == MONTHLY_EMITTER_ACTION or "NEEDS_MONTHLY" in st:
        return "MONTH_BOUNDARY"
    if "REFRESHED" in st or "ALREADY_FRESH" in st or (res or {}).get("performed_write"):
        return "OK"
    if "CONFIRM_REQUIRED" in st:
        return "CONFIRM_REQUIRED"
    if ("INSUFFICIENT" in st or "PROVIDER_BLOCKED" in st or "BLOCKED" in st
            or "DATA_HOLD" in st):
        return "BLOCKED"
    return "FAILED"


def _classify_monthly(res: Optional[dict]) -> str:
    """Map a monthly-adapter result to OK / BLOCKED / FAILED (robust to the canonical
    ``api.monthly_momentum_input`` vocabulary and to injected test fakes)."""
    st = str((res or {}).get("status") or "")
    if any(k in st for k in ("EMITTED", "CURRENT", "REUSED", "ALREADY")):
        return "OK"
    if (res or {}).get("performed_write"):
        return "OK"
    if "UNAVAILABLE" in st or "DATA_HOLD" in st or "HOLD" in st:
        return "BLOCKED"
    if "CONFLICT" in st or "INVALID" in st or "REJECT" in st:
        return "FAILED"
    return "FAILED"


def _extract_target(readiness: Optional[dict]) -> dict:
    r = readiness or {}
    dates = r.get("dates") or {}
    st = r.get("state")
    return {
        "available": bool(r),
        "target_date": dates.get("alpha_market_date"),
        "target_status": st,
        "required_next_action": r.get("required_next_action"),
        # A target is CALCULATED here, never silently confirmed/approved.
        "operationally_approved": (st == "CONFIRMED"),
        "snapshot_confirmation_allowed": r.get("snapshot_confirmation_allowed"),
        "confirm_required_token": r.get("confirm_required_token"),
        "output_hash": _hash({"target_date": dates.get("alpha_market_date"),
                              "state": st}),
    }


def _extract_evidence(capture: Optional[dict], registry: list,
                      eligible: Optional[str]) -> dict:
    c = capture or {}
    required = len(registry) if registry else c.get("snapshots_expected")
    captured = (int(c.get("snapshots_created") or 0)
                + int(c.get("snapshots_already_present") or 0))
    ev_status = c.get("evidence_status")
    mandatory = bool(c.get("mandatory_active_snapshot_persisted"))
    # EVIDENCE_COMPLETE → no gap; PARTIAL/BLOCKED → documented gap (never labelled
    # complete). The active book snapshot is mandatory (never silently omitted).
    complete = (str(ev_status).endswith("COMPLETE"))
    gap = (not complete) or (required is not None and captured < required)
    return {
        "available": bool(c),
        "required_snapshot_count": required,
        "captured_snapshot_count": captured,
        "snapshots_already_present": int(c.get("snapshots_already_present") or 0),
        "mandatory_active_snapshot_persisted": mandatory,
        "snapshot_bundle_id": c.get("artifact_bundle_id"),
        "artifact_hash": c.get("artifact_hash"),
        "evidence_status": ev_status,
        "market_date": eligible,
        "documented_gap": bool(gap),
        "output_hash": c.get("artifact_hash") or _hash(
            {"bundle": c.get("artifact_bundle_id"), "captured": captured}),
    }


def _extract_assessment(gate: Optional[dict], eligible: Optional[str]) -> dict:
    g = gate or {}
    return {
        "available": bool(g),
        "assessment_date": g.get("latest_completed_market_date") or g.get("evaluation_date"),
        "eligible_market_date": eligible,
        "assessment_status": g.get("outcome") or g.get("status"),
        "assessment_result": g.get("target_state") or g.get("outcome"),
        "headline": g.get("headline"),
        "current_for_eligible_session": bool(
            _coerce_date(g.get("latest_completed_market_date")) is not None
            and _coerce_date(eligible) is not None
            and _coerce_date(g.get("latest_completed_market_date")) == _coerce_date(eligible)),
        "output_hash": _hash({"date": g.get("latest_completed_market_date"),
                              "outcome": g.get("outcome")}),
    }


def _extract_holding_opp_cost(built: Optional[dict], eligible: Optional[str]) -> dict:
    """Normalize the Holding Opportunity-Cost engine output (Slice 6) for the DRC
    contract. ``built`` is the ``api.holding_opportunity_cost.run_and_persist`` result
    ``{"assessment": <kernel result>, "persistence": {...}}``. Purely descriptive —
    the DRC computes no opportunity cost."""
    b = built or {}
    assessment = b.get("assessment") or {}
    persistence = b.get("persistence") or {}
    state = assessment.get("assessment_state")
    counts = assessment.get("recommendation_counts") or {}
    gaps = (assessment.get("data_quality") or {}).get("data_gaps") or []
    available = bool(assessment) and state in ("READY", "DEGRADED")
    return {
        "available": available,
        "owner": "api.holding_opportunity_cost",
        "calculation_owner": "engine.holding_opportunity_cost",
        "state": state,
        "eligible_market_date": assessment.get("eligible_market_date") or eligible,
        "assessment_hash": assessment.get("assessment_hash"),
        "artifact_id": persistence.get("artifact_id"),
        "persistence_status": persistence.get("status"),
        "holding_count": len(assessment.get("holding_reviews") or []),
        "recommendation_counts": counts,
        "data_gaps": list(gaps),
    }


def _extract_reassessment(built: Optional[dict], eligible: Optional[str]) -> dict:
    """Normalize the Stage-20 Portfolio Reassessment output for the DRC contract.
    ``built`` is the ``api.portfolio_reassessment.run_and_persist`` result
    ``{"reassessment": <kernel result>, "persistence": {...}}``. Purely descriptive — the
    DRC evaluates no economics and gates nothing itself; it only reads the owner's
    verdict to decide whether the Slice-7 proposal step should run."""
    b = built or {}
    res = b.get("reassessment") or {}
    persistence = b.get("persistence") or {}
    dec = res.get("decision") or {}
    state = res.get("reassessment_state")
    available = bool(res) and state not in (None, "NOT_READY")
    return {
        "available": available,
        "owner": "api.portfolio_reassessment",
        "calculation_owner": "engine.portfolio_reassessment",
        "state": state,
        "eligible_market_date": res.get("eligible_market_date") or eligible,
        "reassessment_hash": res.get("reassessment_hash"),
        "reassessment_id": persistence.get("artifact_id"),
        "persistence_status": persistence.get("status"),
        "history_appended": bool(persistence.get("history_appended")),
        "decision": state,
        "proposal_required": bool(dec.get("proposal_required")),
        "actionable_holding_count": dec.get("actionable_holding_count"),
        "holdings_evaluated": dec.get("holdings_evaluated"),
        "expected_net_improvement": dec.get("expected_net_improvement"),
        "expected_one_way_turnover": dec.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": dec.get("expected_transaction_cost_usd"),
        "attention_count": (res.get("attention") or {}).get("count", 0),
        "blockers": list(dec.get("blockers") or []),
        "reason_codes": list(dec.get("reason_codes") or []),
        "explanation": res.get("explanation"),
        "data_gaps": list(res.get("data_gaps") or []),
    }


def _reassessment_gate(raw_reassessment: Optional[dict],
                       extracted: Optional[dict]) -> dict:
    """Ask the canonical Stage-20 owner whether the target engine may run.

    The DRC applies NO economics of its own: the verdict is produced by
    ``api.portfolio_reassessment.should_build_proposal`` (over the kernel result) and the
    extracted contract block is used only as a degrade-safe fallback when the raw kernel
    result is not available (a skipped / failed reassessment step). A missing or
    unavailable reassessment NEVER authorises a proposal — fail closed."""
    ex = extracted or {}
    if isinstance(raw_reassessment, dict) and raw_reassessment:
        try:
            from paper_trader.api import portfolio_reassessment as prs
            return prs.should_build_proposal(raw_reassessment)
        except Exception:  # noqa: BLE001 - degrade to the extracted block, never crash
            pass
    if ex.get("available") and ex.get("proposal_required"):
        return {"build_proposal": True, "reassessment_state": ex.get("state"),
                "reassessment_hash": ex.get("reassessment_hash"),
                "reason": "The portfolio-level economic gate cleared."}
    return {"build_proposal": False, "reassessment_state": ex.get("state"),
            "reassessment_hash": ex.get("reassessment_hash"),
            "reason": ("the portfolio reassessment decided %s, so no capital is "
                       "redeployed and no target is built."
                       % (ex.get("state") or "NOT_AVAILABLE"))}


def _extract_reallocation(built: Optional[dict], eligible: Optional[str]) -> dict:
    """Normalize the Reallocation Proposal engine output (Slice 7) for the DRC contract.
    ``built`` is the ``api.reallocation_proposal.run_and_persist`` result
    ``{"proposal": <kernel result>, "persistence": {...}}``. Purely descriptive — the
    DRC computes no allocation and creates no order/target."""
    b = built or {}
    proposal = b.get("proposal") or {}
    persistence = b.get("persistence") or {}
    state = proposal.get("proposal_state")
    counts = proposal.get("action_counts") or {}
    gaps = [g.get("code") for g in (proposal.get("data_gaps") or []) if not g.get("by_design")]
    available = bool(proposal) and state in ("READY", "DEGRADED")
    signal = proposal.get("signal") or {}
    turnover = proposal.get("turnover") or {}
    portfolio = proposal.get("portfolio") or {}
    return {
        "available": available,
        "owner": "api.reallocation_proposal",
        "calculation_owner": "engine.reallocation_proposal",
        "state": state,
        "eligible_market_date": proposal.get("eligible_market_date") or eligible,
        "proposal_hash": proposal.get("proposal_hash"),
        "proposal_id": persistence.get("proposal_id"),
        "persistence_status": persistence.get("status"),
        "superseded_proposal_id": persistence.get("superseded_proposal_id"),
        "action_counts": counts,
        "proposed_holding_count": portfolio.get("proposed_holding_count"),
        "one_way_turnover": turnover.get("one_way_turnover"),
        "estimated_transaction_cost": turnover.get("estimated_transaction_cost"),
        "score_improvement": signal.get("score_improvement"),
        "score_improvement_net_of_cost": signal.get("score_improvement_net_of_cost"),
        "data_gaps": list(gaps),
    }


def _extract_research_agent(built: Optional[dict], eligible: Optional[str]) -> dict:
    """Normalize the Persistent Alpha Research Agent output (Slice 8) for the DRC contract.
    ``built`` is the ``api.research_agent.run_and_persist`` result
    ``{"assessment": <kernel result>, "persistence": {...}}``. Purely descriptive — the DRC
    evaluates no research state, promotes no model, and creates no order/target."""
    b = built or {}
    assessment = b.get("assessment") or {}
    persistence = b.get("persistence") or {}
    state = assessment.get("research_state")
    degradation = assessment.get("degradation") or {}
    recal = assessment.get("recalibration") or {}
    challenger = assessment.get("challenger") or {}
    evq = assessment.get("evidence_quality") or {}
    opps = assessment.get("research_opportunities") or []
    gaps = [g.get("code") for g in (assessment.get("data_gaps") or []) if not g.get("by_design")]
    # Persistable (durable id+hash) only for a non-BLOCKED assessment that was actually
    # persisted; a BLOCKED / not-persisted assessment carries no immutable reference.
    persisted = bool(persistence.get("persisted")) and state != "BLOCKED"
    available = bool(assessment) and persisted
    return {
        "available": available,
        "owner": "api.research_agent",
        "calculation_owner": "engine.research_agent",
        "state": state,
        "eligible_market_date": assessment.get("eligible_market_date") or eligible,
        "assessment_hash": assessment.get("assessment_hash"),
        "assessment_id": persistence.get("assessment_id"),
        "persistence_status": persistence.get("status"),
        "superseded_assessment_id": persistence.get("superseded_assessment_id"),
        "evidence_quality": evq.get("state"),
        "degradation_categories": degradation.get("categories") or [],
        "recalibration_state": recal.get("state"),
        "recalibration_recommended": bool(recal.get("recommended")),
        "challenger_state": challenger.get("state"),
        "top_opportunity": (opps[0].get("opportunity_id") if opps else None),
        "opportunity_count": len(opps),
        "data_gaps": list(gaps),
    }


# --------------------------------------------------------------------------- #
# Pre-run gate (Workstream Q). Session / owned-data / consistency readiness — no
# writes, no seam calls, ephemeral (not persisted).
# --------------------------------------------------------------------------- #
def _pre_run_state(facts: dict) -> Optional[str]:
    if facts["consistency_status"] == df.INCONSISTENT:
        return INCONSISTENT
    if facts["session_status"] == msession.INCONSISTENT_FUTURE_DATA:
        return INCONSISTENT
    if facts["session_status"] == msession.BEFORE_SESSION_CLOSE:
        return WAITING_FOR_SESSION_CLOSE
    if (facts["session_status"] in (msession.WAITING_FOR_OWNED_DATA,
                                    msession.NO_CONFIRMED_DATA)
            or not facts["owned_data_confirmed"] or not facts["eligible"]):
        return WAITING_FOR_OWNED_DATA
    return None


def _reflect_completed_run(prior: dict, facts: dict, warnings: list) -> dict:
    """Reflect a persisted TERMINAL-COMPLETE manifest for the current eligible session
    (Workstream C). The stored contract (run_id, idempotency_key, hashes, completed
    steps, opportunity-cost artifact reference) is surfaced VERBATIM; the run is the
    authoritative research for the session. A benign fast-input drift (the cycle refreshed
    the inputs the raw hash is derived from) is annotated but NEVER resets the run."""
    out = dict(prior)
    out["reused_existing_run"] = True
    out["resumed_existing_run"] = False
    out["evaluated_at"] = _now_iso()
    out["executable"] = False
    out["state_vocabulary"] = list(RUN_STATES)
    ow = list(out.get("warnings") or [])
    prior_session = prior.get("session_contract_hash")
    cur_session = facts.get("session_contract_hash")
    fast_drift = (facts.get("input_contract_hash")
                  and prior.get("input_contract_hash")
                  and facts["input_contract_hash"] != prior["input_contract_hash"])
    session_stable = (prior_session is None or cur_session is None
                      or prior_session == cur_session)
    if fast_drift and session_stable:
        msg = ("Owned daily inputs advanced since the completed run for %s; the persisted "
               "research (scoring / target / evidence / opportunity-cost) remains "
               "authoritative for this eligible session and is reused (not recomputed)."
               % facts.get("eligible"))
        if msg not in ow:
            ow.append(msg)
    for w in warnings:
        if w not in ow:
            ow.append(w)
    out["warnings"] = ow
    return out


# --------------------------------------------------------------------------- #
# Public: read-only STATUS (Workstream C/N). Plans deterministically; reflects the
# latest persisted run; NEVER executes a step, refreshes an input, scores, captures
# evidence, or writes anything.
# --------------------------------------------------------------------------- #
def load_daily_research_cycle_status(
    *, now: Optional[datetime] = None, reference_today: Any = None,
    close_cutoff_et: Any = msession.DEFAULT_CLOSE_CUTOFF_ET, drc_dir=None,
    freshness: Optional[dict] = None, freshness_loader: Optional[Callable] = None,
    monthly_emitter_available: Any = _DEFAULT_MONTHLY,
    operational: Optional[dict] = None, inputs: Optional[dict] = None,
    daily_status: Optional[dict] = None, desk_marks: Optional[dict] = None,
    close_progress: Optional[dict] = None, forward_status: Optional[dict] = None,
    date_overrides: Optional[dict] = None, active_book_override: Any = None,
    downstream_artifacts_fn: Optional[Callable] = None,
) -> dict:
    warnings: list[str] = []
    # Resolve the canonical monthly-adapter availability when the caller did not pin
    # it (production). Tests pass an explicit bool. Default → False when no emitter
    # is wired, so a due month blocks honestly through the declared adapter owner.
    if monthly_emitter_available is _DEFAULT_MONTHLY:
        monthly_emitter_available = bool(
            _safe(_default_monthly_available, warnings, "Monthly emitter availability"))
    if freshness is None:
        loader = freshness_loader or _default_freshness_loader
        freshness = _safe(lambda: loader(
            now=now, reference_today=reference_today, close_cutoff_et=close_cutoff_et,
            operational=operational, inputs=inputs, daily_status=daily_status,
            desk_marks=desk_marks, daily_close_status=close_progress,
            forward_status=forward_status, date_overrides=date_overrides,
            active_book_override=active_book_override), warnings, "Data freshness") or {}
    for w in (freshness.get("warnings") or []):
        if w not in warnings:
            warnings.append(w)

    facts = _facts(freshness)
    plan = build_execution_plan(freshness,
                                monthly_emitter_available=monthly_emitter_available)
    monthly_owner = _monthly_owner_status(freshness, facts, monthly_emitter_available,
                                          warnings)

    # Reflect the latest persisted run for this eligible session, if any.
    prior = None
    if facts["eligible"]:
        idx = _load_index(drc_dir).get(facts["eligible"])
        if idx and idx.get("run_id"):
            prior = _load_run(idx["run_id"], drc_dir)

    pre = _pre_run_state(facts)
    if pre is not None:
        blockers = []
        required_actions = [{"gate": "market_session",
                             "action": facts.get("session_operator_action")}]
        if pre == INCONSISTENT:
            blockers = [{"code": "CROSS_SURFACE_INCONSISTENCY",
                         "detail": "Authoritative surfaces disagree; reconcile before running."}]
        return _contract(state=pre, facts=facts, plan=plan, warnings=warnings,
                         blockers=blockers, required_actions=required_actions,
                         monthly_owner=monthly_owner, executable=False)

    # Session is closed with owned data confirmed. Prefer an in-flight / persisted run.
    lock = _read_lock(drc_dir)
    if lock is not None and not _lock_is_stale(lock):
        running = _load_run(lock.get("run_id"), drc_dir)
        state = (running or {}).get("state") if running else PLANNING
        if state not in _RUNNING:
            state = PLANNING
        return _contract(state=state, facts=facts, plan=plan, run_id=lock.get("run_id"),
                         current_step=(running or {}).get("current_step"),
                         step_results=(running or {}).get("step_results") or [],
                         warnings=warnings + ["A Daily Research Cycle run is in progress."],
                         monthly_owner=monthly_owner, executable=False)

    # Workstream C: a persisted TERMINAL-COMPLETE manifest for this eligible session is
    # authoritative and is REFLECTED verbatim — never masked by a recomputed input-contract
    # hash. The cycle refreshes the very fast inputs the raw ``input_contract_hash`` is
    # derived from, so a post-run status read recomputes a DIFFERENT raw hash than the run
    # persisted; that benign drift must NEVER reset a COMPLETE run to NOT_STARTED. The
    # stored idempotency_key / hashes / HOC artifact reference are surfaced as-is.
    if prior and prior.get("state") in _COMPLETED:
        return _reflect_completed_run(prior, facts, warnings)

    # Workstream C: downstream TERMINAL artifacts (an immutable Holding Opportunity-Cost
    # artifact) exist for the eligible session but the DRC run manifest is missing → an
    # explicit INCONSISTENT recovery state (NEVER NOT_STARTED, and NEVER a silently
    # synthesised COMPLETE). The operator resumes through the normal idempotent DRC path.
    if prior is None and facts["eligible"] and facts["active_book_id"]:
        hoc_dir = (str(Path(drc_dir) / "holding_opportunity_cost") if drc_dir else None)
        probe = downstream_artifacts_fn or _default_downstream_probe
        ds = _safe(lambda: probe(active_book_id=facts["active_book_id"],
                                 eligible=facts["eligible"], hoc_dir=hoc_dir),
                   warnings, "Downstream-artifact probe") or {}
        if ds.get("present"):
            hoc_block = {"available": True, "owner": "api.holding_opportunity_cost",
                         "state": ds.get("state"),
                         "assessment_hash": ds.get("assessment_hash"),
                         "recommendation_counts": ds.get("recommendation_counts") or {},
                         "data_gaps": list(ds.get("data_gaps") or []),
                         "holding_count": None}
            return _contract(
                state=INCONSISTENT, facts=facts, plan=plan,
                warnings=warnings + [
                    "An immutable Holding Opportunity-Cost artifact exists for %s but the "
                    "Daily Research Cycle run manifest is missing; the run must be durably "
                    "recorded through a safe idempotent recovery (no evidence is fabricated "
                    "and the existing artifact is reused)." % facts["eligible"]],
                blockers=[{"code": TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST,
                           "detail": "Terminal downstream artifacts without a DRC manifest.",
                           "downstream": ds}],
                required_actions=[{"gate": "daily_research_cycle",
                    "action": ("Resume the Daily Research Cycle to durably record the run "
                               "manifest for the existing downstream artifacts (safe "
                               "idempotent recovery; reuses the immutable evidence / "
                               "opportunity-cost artifact)."),
                    "confirmation_required": EXECUTE_CONFIRMATION, "recovery": True}],
                holding_opp_cost=hoc_block, monthly_owner=monthly_owner, executable=True)

    if plan["plan_blocked"]:
        return _contract(state=BLOCKED, facts=facts, plan=plan, warnings=warnings,
                         blockers=plan["blockers"],
                         required_actions=_blocked_actions(plan),
                         monthly_owner=monthly_owner, executable=False)

    # Ready to run (minimal or with refreshable steps). Reflect a prior BLOCKED/FAILED.
    state = NOT_STARTED
    if prior and prior.get("input_contract_hash") == facts["input_contract_hash"] \
            and prior.get("state") in (BLOCKED, FAILED):
        state = prior["state"]
        warnings.append("The prior run for this contract terminated %s." % state)
    return _contract(state=state, facts=facts, plan=plan, warnings=warnings,
                     required_actions=[{"gate": "daily_research_cycle",
                                        "action": "Run the Daily Research Cycle.",
                                        "confirmation_required": EXECUTE_CONFIRMATION}],
                     monthly_owner=monthly_owner, executable=(state == NOT_STARTED))


def _blocked_actions(plan: dict) -> list:
    out = []
    for b in plan.get("blockers") or []:
        out.append({"gate": "source", "source_id": b.get("source_id"),
                    "code": b.get("code"),
                    "missing_implementation": b.get("missing_implementation"),
                    "action": b.get("detail")})
    return out


# --------------------------------------------------------------------------- #
# Public: EXECUTE (Workstream C/E/G/H/I/K/L). Token-gated, idempotent, resumable.
# Every provider / write boundary is an injectable seam so tests never call a
# provider, prediction, or a real cycle.
# --------------------------------------------------------------------------- #
def run_daily_research_cycle(
    *, confirm: Optional[str] = None, requested_by: str = "manual_ui",
    now: Optional[datetime] = None, reference_today: Any = None,
    close_cutoff_et: Any = msession.DEFAULT_CLOSE_CUTOFF_ET, drc_dir=None,
    downloader=None,
    freshness: Optional[dict] = None, freshness_loader: Optional[Callable] = None,
    daily_refresh_fn: Optional[Callable] = None,
    monthly_emitter_fn: Any = _DEFAULT_MONTHLY,
    scoring_fn: Optional[Callable] = None, target_loader: Optional[Callable] = None,
    evidence_capture_fn: Optional[Callable] = None,
    evidence_registry: Optional[list] = None,
    assessment_loader: Optional[Callable] = None,
    holding_opp_cost_fn: Optional[Callable] = None,
    reassessment_fn: Optional[Callable] = None,
    reallocation_proposal_fn: Optional[Callable] = None,
    research_agent_fn: Optional[Callable] = None,
    refresh_confirm_token: Optional[str] = None,
    operational: Optional[dict] = None, inputs: Optional[dict] = None,
    daily_status: Optional[dict] = None, desk_marks: Optional[dict] = None,
    close_progress: Optional[dict] = None, forward_status: Optional[dict] = None,
    date_overrides: Optional[dict] = None, active_book_override: Any = None,
) -> dict:
    if confirm != EXECUTE_CONFIRMATION:
        return {"status": "DAILY_RESEARCH_CYCLE_CONFIRM_REQUIRED", "phase": PHASE,
                "state": NOT_STARTED, "confirmation_required": EXECUTE_CONFIRMATION,
                "message": ("Running the Daily Research Cycle requires confirm='%s'."
                            % EXECUTE_CONFIRMATION), "safety": _safety(False)}

    # Resolve the canonical monthly adapter when the caller did not pin the seam
    # (production). Tests pass an explicit callable or None. Default → the adapter
    # when an emitter is wired, else None (a due month blocks honestly).
    if monthly_emitter_fn is _DEFAULT_MONTHLY:
        try:
            monthly_emitter_fn = _default_monthly_emitter_fn()
        except Exception:  # noqa: BLE001
            monthly_emitter_fn = None

    if not _RUN_LOCK.acquire(blocking=False):
        return _contract(state=RUN_IN_PROGRESS, facts=_facts(freshness or {}),
                         warnings=["Another Daily Research Cycle is running in-process."],
                         executable=False)
    try:
        return _run_locked(
            requested_by=requested_by, now=now, reference_today=reference_today,
            close_cutoff_et=close_cutoff_et, drc_dir=drc_dir, downloader=downloader,
            freshness=freshness, freshness_loader=freshness_loader,
            daily_refresh_fn=daily_refresh_fn, monthly_emitter_fn=monthly_emitter_fn,
            scoring_fn=scoring_fn, target_loader=target_loader,
            evidence_capture_fn=evidence_capture_fn, evidence_registry=evidence_registry,
            assessment_loader=assessment_loader, holding_opp_cost_fn=holding_opp_cost_fn,
            reassessment_fn=reassessment_fn,
            reallocation_proposal_fn=reallocation_proposal_fn,
            research_agent_fn=research_agent_fn,
            refresh_confirm_token=refresh_confirm_token,
            operational=operational, inputs=inputs, daily_status=daily_status,
            desk_marks=desk_marks, close_progress=close_progress,
            forward_status=forward_status, date_overrides=date_overrides,
            active_book_override=active_book_override)
    finally:
        _RUN_LOCK.release()


def _run_locked(*, requested_by, now, reference_today, close_cutoff_et, drc_dir,
                downloader, freshness, freshness_loader, daily_refresh_fn,
                monthly_emitter_fn, scoring_fn, target_loader, evidence_capture_fn,
                evidence_registry, assessment_loader, holding_opp_cost_fn,
                reassessment_fn, reallocation_proposal_fn, research_agent_fn,
                refresh_confirm_token,
                operational, inputs, daily_status, desk_marks, close_progress,
                forward_status, date_overrides, active_book_override) -> dict:
    warnings: list[str] = []
    loader = freshness_loader or _default_freshness_loader
    working_inputs = dict(inputs) if inputs is not None else None

    def _load_fresh(cur_inputs):
        return loader(
            now=now, reference_today=reference_today, close_cutoff_et=close_cutoff_et,
            operational=operational, inputs=cur_inputs, daily_status=daily_status,
            desk_marks=desk_marks, daily_close_status=close_progress,
            forward_status=forward_status, date_overrides=date_overrides,
            active_book_override=active_book_override)

    fr = freshness if freshness is not None else _safe(
        lambda: _load_fresh(working_inputs), warnings, "Data freshness") or {}
    facts = _facts(fr)
    monthly_available = monthly_emitter_fn is not None
    plan = build_execution_plan(fr, monthly_emitter_available=monthly_available)
    started_at = _now_iso()
    # Run-path monthly-owner block: hermetic (never reads the production source-panel
    # here — the read-only status endpoint owns that detail).
    monthly_owner_block = _monthly_owner_status(
        fr, facts, monthly_available, warnings, emitter_status_fn=lambda _e: None)

    # --- Pre-run gates: no writes, nothing persisted. ------------------------- #
    pre = _pre_run_state(facts)
    if pre is not None:
        return _contract(state=pre, facts=facts, plan=plan, warnings=warnings,
                         started_at=started_at, executable=False,
                         monthly_owner=monthly_owner_block,
                         required_actions=[{"gate": "market_session",
                                            "action": facts.get("session_operator_action")}])

    key = facts["idempotency_key"]
    ich = facts["input_contract_hash"]
    session_hash = facts["session_contract_hash"]
    run_id = "drc_%s_%s" % (facts["eligible"], key[:12])

    # --- Concurrency (Workstream L): classify the persisted lock first. ------- #
    lock = _read_lock(drc_dir)
    if lock is not None and not _lock_is_stale(lock):
        if lock.get("idempotency_key") == key:
            return _contract(state=RUN_IN_PROGRESS, facts=facts, plan=plan,
                             run_id=lock.get("run_id"),
                             warnings=["A run for this exact contract is in progress."],
                             executable=False)
        if lock.get("eligible_date") == facts["eligible"] \
                and lock.get("input_contract_hash") != ich:
            return _contract(state=INCONSISTENT, facts=facts, plan=plan,
                             warnings=["A conflicting run for %s (different input "
                                       "contract) is in progress." % facts["eligible"]],
                             blockers=[{"code": "CONFLICTING_CONTRACT_IN_PROGRESS"}],
                             executable=False)
        return _contract(state=RUN_IN_PROGRESS, facts=facts, plan=plan,
                         run_id=lock.get("run_id"),
                         warnings=["Another Daily Research Cycle run is in progress."],
                         executable=False)

    # --- Idempotency: reuse a completed run; refuse to overwrite a different
    #     contract for the same date; resume a safe incomplete run. ----------- #
    idx = _load_index(drc_dir).get(facts["eligible"]) or {}
    prior = _load_run(idx.get("run_id"), drc_dir) if idx.get("run_id") else None
    resumed = False
    prior_steps: dict[str, dict] = {}
    if prior:
        if prior.get("state") in _COMPLETED:
            # Workstream D — safe idempotent recovery: a completed run for this eligible
            # session is REUSED verbatim (never recomputed, never overwritten; creates no
            # duplicate evidence / HOC artifact). Reuse holds when EITHER the exact input
            # contract matches (normal idempotency), OR the SESSION-STABLE identity matches
            # (the fast daily inputs the cycle refreshes advanced, but the session + slow
            # inputs are identical), OR the prior manifest predates the session-hash field
            # (a legacy COMPLETE manifest is date-authoritative for recovery). Only a
            # genuinely DIFFERENT slow-input contract for the same date is refused.
            prior_session = prior.get("session_contract_hash")
            same_contract = (prior.get("input_contract_hash") == ich)
            same_session = (prior_session is not None and prior_session == session_hash)
            legacy_no_session = (prior_session is None)
            if same_contract or same_session or legacy_no_session:
                out = dict(prior)
                out["reused_existing_run"] = True
                out["evaluated_at"] = _now_iso()
                if not same_contract:
                    w = list(out.get("warnings") or [])
                    msg = ("Owned daily inputs advanced since the completed run; the "
                           "persisted research for %s is reused, not recomputed (safe "
                           "idempotent recovery)." % facts["eligible"])
                    if msg not in w:
                        w.append(msg)
                    out["warnings"] = w
                return out
            return _contract(state=INCONSISTENT, facts=facts, plan=plan,
                             warnings=["A completed run for %s used a DIFFERENT input "
                                       "contract (slow inputs differ); refusing to "
                                       "overwrite the immutable outputs (bundle "
                                       "first-write-wins)." % facts["eligible"]],
                             blockers=[{"code": "DIFFERENT_CONTRACT_SAME_DATE",
                                        "prior_hash": prior.get("input_contract_hash"),
                                        "current_hash": ich,
                                        "prior_session_hash": prior_session,
                                        "current_session_hash": session_hash}],
                             monthly_owner=monthly_owner_block, executable=False)
        if prior.get("input_contract_hash") == ich and prior.get("state") not in _TERMINAL:
            resumed = True
            run_id = prior.get("run_id") or run_id
            prior_steps = {s["step_id"]: s for s in (prior.get("step_results") or [])
                           if s.get("status") in (S_OK, S_REUSED)}
            started_at = prior.get("started_at") or started_at

    # --- Acquire the file lock and run the pipeline. -------------------------- #
    _write_lock(idempotency_key=key, eligible_date=facts["eligible"], run_id=run_id,
                input_contract_hash=ich, drc_dir=drc_dir)
    step_results: list[dict] = []
    performed_write = False

    def _build(state, extra_warnings=None, **kw):
        return _contract(state=state, facts=facts, run_id=run_id, plan=plan,
                         step_results=step_results, started_at=started_at,
                         resumed=resumed,
                         warnings=(warnings + list(extra_warnings or [])),
                         monthly_owner=monthly_owner_block,
                         performed_write=performed_write, **kw)

    def _persist(state, **kw):
        # Workstream B — terminal persistence/read-back contract. For a TERMINAL-COMPLETE
        # run: (1) VALIDATE the manifest contract; (2) atomically persist the manifest;
        # (3) atomically update the run index; (4) READ BACK and verify the same manifest
        # is durable. A manifest that fails validation or read-back is NEVER returned as
        # COMPLETE — it is downgraded to INCONSISTENT with a precise code while the durable
        # downstream-artifact references (scoring / target / evidence / HOC artifact) are
        # PRESERVED for recovery.
        rec = _build(state, **kw)
        refs = {k: kw[k] for k in ("alignment", "scoring", "target", "evidence",
                                   "assessment", "holding_opp_cost",
                                   "portfolio_reassessment", "reallocation_proposal",
                                   "research_agent", "completed_at")
                if k in kw}
        if state in _COMPLETED:
            problems = _validate_terminal_manifest(rec)
            if problems:
                rec = _build(INCONSISTENT,
                             extra_warnings=["Terminal manifest failed validation: "
                                             + "; ".join(problems)],
                             blockers=[{"code": MANIFEST_CONTRACT_INCOMPLETE,
                                        "detail": problems}], **refs)
                state = INCONSISTENT
        _save_run(rec, drc_dir)
        _update_index(eligible_date=facts["eligible"], idempotency_key=key,
                      input_contract_hash=ich, run_id=run_id, state=state,
                      drc_dir=drc_dir)
        # Read-back verification (Workstream B rule 7): the SAME manifest + index entry must
        # be durably readable back before a terminal completion is trusted.
        verify = _load_run(run_id, drc_dir)
        idx_back = _load_index(drc_dir).get(facts["eligible"]) or {}
        durable = bool(verify and verify.get("run_id") == run_id
                       and verify.get("state") == state
                       and verify.get("opportunity_cost_assessment_hash")
                       == rec.get("opportunity_cost_assessment_hash")
                       and verify.get("reallocation_proposal_hash")
                       == rec.get("reallocation_proposal_hash")
                       and verify.get("research_agent_hash")
                       == rec.get("research_agent_hash")
                       and idx_back.get("run_id") == run_id
                       and idx_back.get("state") == state)
        if not durable and state in _COMPLETED:
            fail = _build(INCONSISTENT,
                          extra_warnings=["Terminal manifest read-back failed; the completed "
                                          "research is preserved for recovery but the run is "
                                          "not durably recorded."],
                          blockers=[{"code": MANIFEST_PERSISTENCE_UNVERIFIED}], **refs)
            try:
                _save_run(fail, drc_dir)
                _update_index(eligible_date=facts["eligible"], idempotency_key=key,
                              input_contract_hash=ich, run_id=run_id, state=INCONSISTENT,
                              drc_dir=drc_dir)
            except Exception:  # noqa: BLE001
                pass
            return fail
        return rec

    def _reuse_or(step_id):
        return prior_steps.get(step_id)

    try:
        # STEP: resolve session + validate consistency (already computed in facts).
        step_results.append(_reuse_or(STEP_RESOLVE_SESSION) or _step(
            STEP_RESOLVE_SESSION, S_OK, owner="engine.market_session",
            as_of_date=facts["eligible"], reason="Eligible session confirmed."))
        if facts["consistency_status"] == df.INCONSISTENT:
            step_results.append(_step(STEP_VALIDATE_CONSISTENCY, S_BLOCKED,
                                      owner="api.data_freshness",
                                      error_code="CROSS_SURFACE_INCONSISTENCY"))
            _clear_lock(drc_dir)
            return _persist(INCONSISTENT, failed_step=STEP_VALIDATE_CONSISTENCY,
                            blockers=[{"code": "CROSS_SURFACE_INCONSISTENCY"}])
        step_results.append(_reuse_or(STEP_VALIDATE_CONSISTENCY) or _step(
            STEP_VALIDATE_CONSISTENCY, S_OK, owner="api.data_freshness",
            reason="Active book + cross-surface consistency validated."))

        # STEP: plan.
        step_results.append(_reuse_or(STEP_PLAN) or _step(
            STEP_PLAN, S_OK, owner="api.daily_research_cycle",
            output_hash=_hash(plan), records_read=len(fr.get("source_freshness") or []),
            reason="Deterministic execution plan built from data_freshness."))

        # STEP: refresh required inputs.
        if plan["plan_blocked"]:
            step_results.append(_step(STEP_REFRESH_INPUTS, S_BLOCKED,
                                      owner="api.alpha_target.run_refresh / "
                                            "api.monthly_momentum_input",
                                      error_code="UNSUPPORTED_AUTOMATIC_REFRESH",
                                      error_detail=str(plan["blockers"])[:200]))
            _clear_lock(drc_dir)
            return _persist(BLOCKED, failed_step=STEP_REFRESH_INPUTS,
                            blockers=plan["blockers"],
                            required_actions=_blocked_actions(plan))

        refresh_results = []
        token = refresh_confirm_token or _safe(_default_refresh_confirm_token,
                                               warnings, "Refresh token") \
            or "CONFIRM_ALPHA_TARGET_REFRESH"
        if _reuse_or(STEP_REFRESH_INPUTS):
            step_results.append(prior_steps[STEP_REFRESH_INPUTS])
            if working_inputs is not None:
                working_inputs["market_as_of_date"] = facts["eligible"]
        elif plan["refresh_steps"]:
            fn = daily_refresh_fn or _default_daily_refresh_fn
            emit = monthly_emitter_fn
            for st in plan["refresh_steps"]:
                sid = st["source_id"]
                if sid == "momentum_monthly":
                    if emit is None:
                        step_results.append(_step(STEP_REFRESH_INPUTS, S_BLOCKED,
                            owner="api.monthly_momentum_input",
                            error_code=MONTHLY_EMITTER_ACTION))
                        _clear_lock(drc_dir)
                        return _persist(BLOCKED, failed_step=STEP_REFRESH_INPUTS,
                            blockers=plan["blockers"], required_actions=_blocked_actions(plan))
                    res = emit(eligible=facts["eligible"])
                    refresh_results.append({"source_id": sid, "result": res})
                    mcls = _classify_monthly(res)
                    if mcls == "OK":
                        performed_write = performed_write or bool(
                            (res or {}).get("performed_write"))
                        if working_inputs is not None:
                            working_inputs["momentum_month"] = str(facts["eligible"])[:7]
                        continue
                    if mcls == "BLOCKED":
                        step_results.append(_step(STEP_REFRESH_INPUTS, S_BLOCKED,
                            owner="api.monthly_momentum_input",
                            error_code=MONTHLY_EMITTER_ACTION,
                            error_detail=str((res or {}).get("status"))[:160]))
                        _clear_lock(drc_dir)
                        return _persist(BLOCKED, failed_step=STEP_REFRESH_INPUTS,
                            blockers=[{"code": "UNSUPPORTED_AUTOMATIC_REFRESH",
                                       "source_id": "momentum_monthly",
                                       "missing_implementation": MONTHLY_EMITTER_ACTION,
                                       "detail": (res or {}).get("message")}],
                            required_actions=[{"gate": "source",
                                               "source_id": "momentum_monthly",
                                               "action": MONTHLY_EMITTER_ACTION}])
                    # FAILED: the emitter produced a conflicting/invalid artifact.
                    step_results.append(_step(STEP_REFRESH_INPUTS, S_FAILED,
                        owner="api.monthly_momentum_input",
                        error_code="MONTHLY_INPUT_FAILED",
                        error_detail=str((res or {}).get("status"))[:160]))
                    _clear_lock(drc_dir)
                    return _persist(FAILED, failed_step=STEP_REFRESH_INPUTS,
                        blockers=[{"code": "MONTHLY_INPUT_FAILED",
                                   "source_id": "momentum_monthly",
                                   "detail": (res or {}).get("status")}])
                res = fn(confirm=token, downloader=downloader,
                         completed_through=facts["eligible"])
                refresh_results.append({"source_id": sid, "result": res})
                cls = _classify_refresh(res)
                if cls == "MONTH_BOUNDARY":
                    if emit is None:
                        step_results.append(_step(STEP_REFRESH_INPUTS, S_BLOCKED,
                            owner="api.alpha_target.run_refresh",
                            error_code="NEEDS_MONTHLY_INPUT_REBUILD",
                            error_detail="Frozen monthly momentum input due; no safe emitter."))
                        _clear_lock(drc_dir)
                        return _persist(BLOCKED, failed_step=STEP_REFRESH_INPUTS,
                            blockers=[{"code": "UNSUPPORTED_AUTOMATIC_REFRESH",
                                       "source_id": "momentum_monthly",
                                       "missing_implementation": MONTHLY_EMITTER_ACTION}],
                            required_actions=[{"gate": "source",
                                               "source_id": "momentum_monthly",
                                               "action": MONTHLY_EMITTER_ACTION}])
                    mres = emit(eligible=facts["eligible"])
                    refresh_results.append({"source_id": "momentum_monthly",
                                            "result": mres})
                    if _classify_monthly(mres) != "OK":
                        step_results.append(_step(STEP_REFRESH_INPUTS, S_BLOCKED,
                            owner="api.monthly_momentum_input",
                            error_code=MONTHLY_EMITTER_ACTION,
                            error_detail=str((mres or {}).get("status"))[:160]))
                        _clear_lock(drc_dir)
                        return _persist(BLOCKED, failed_step=STEP_REFRESH_INPUTS,
                            blockers=[{"code": "UNSUPPORTED_AUTOMATIC_REFRESH",
                                       "source_id": "momentum_monthly",
                                       "missing_implementation": MONTHLY_EMITTER_ACTION,
                                       "detail": (mres or {}).get("message")}],
                            required_actions=[{"gate": "source",
                                               "source_id": "momentum_monthly",
                                               "action": MONTHLY_EMITTER_ACTION}])
                    if working_inputs is not None:
                        working_inputs["momentum_month"] = str(facts["eligible"])[:7]
                    res = fn(confirm=token, downloader=downloader,
                             completed_through=facts["eligible"])
                    refresh_results.append({"source_id": sid, "result": res, "retry": True})
                    cls = _classify_refresh(res)
                if cls == "OK":
                    performed_write = True
                    if working_inputs is not None:
                        working_inputs["market_as_of_date"] = facts["eligible"]
                elif cls == "BLOCKED":
                    step_results.append(_step(STEP_REFRESH_INPUTS, S_BLOCKED,
                        owner="api.alpha_target.run_refresh", error_code="REFRESH_BLOCKED",
                        error_detail=str((res or {}).get("status"))[:160]))
                    _clear_lock(drc_dir)
                    return _persist(BLOCKED, failed_step=STEP_REFRESH_INPUTS,
                        blockers=[{"code": "REFRESH_BLOCKED", "source_id": sid,
                                   "detail": (res or {}).get("status")}])
                elif cls in ("FAILED", "CONFIRM_REQUIRED"):
                    step_results.append(_step(STEP_REFRESH_INPUTS, S_FAILED,
                        owner="api.alpha_target.run_refresh", error_code="REFRESH_FAILED",
                        error_detail=str((res or {}).get("status"))[:160]))
                    _clear_lock(drc_dir)
                    return _persist(FAILED, failed_step=STEP_REFRESH_INPUTS,
                        blockers=[{"code": "REFRESH_FAILED", "source_id": sid,
                                   "detail": (res or {}).get("status")}])
            plan["refresh_results"] = refresh_results
            step_results.append(_step(STEP_REFRESH_INPUTS, S_OK,
                owner="api.alpha_target.run_refresh", as_of_date=facts["eligible"],
                records_written=len(refresh_results), output_hash=_hash(refresh_results),
                reason="Required research inputs refreshed to the eligible session."))
        else:
            step_results.append(_step(STEP_REFRESH_INPUTS, S_SKIPPED,
                reason="All required research inputs already current."))

        # STEP: validate input alignment (re-read post-refresh; do not score if it fails).
        fr2 = _safe(lambda: _load_fresh(working_inputs), warnings,
                    "Data freshness (post-refresh)") or fr
        alignment = evaluate_alignment(fr2)
        if not alignment["passed"]:
            step_results.append(_step(STEP_VALIDATE_ALIGNMENT, S_BLOCKED,
                owner="api.daily_research_cycle", as_of_date=facts["eligible"],
                error_code=alignment["status"]))
            _clear_lock(drc_dir)
            return _persist(BLOCKED, alignment=alignment,
                            failed_step=STEP_VALIDATE_ALIGNMENT,
                            blockers=[{"code": alignment["status"],
                                       "detail": "Required research inputs are not "
                                                 "date-aligned; scoring is blocked."}])
        step_results.append(_reuse_or(STEP_VALIDATE_ALIGNMENT) or _step(
            STEP_VALIDATE_ALIGNMENT, S_OK, owner="api.daily_research_cycle",
            as_of_date=facts["eligible"], output_hash=_hash(alignment),
            reason="Date-alignment gate passed: %s." % alignment["status"]))

        # STEP: score the full eligible universe (existing engine adapter).
        raw_scoring = None   # the RAW canonical universe-scoring contract (for the hoc engine)
        if _reuse_or(STEP_SCORE_UNIVERSE) and prior.get("scoring"):
            scoring = prior["scoring"]
            step_results.append(prior_steps[STEP_SCORE_UNIVERSE])
        else:
            built = _safe(scoring_fn or _default_scoring_fn, warnings,
                          "Universe scoring")
            raw_scoring = built
            scoring = _extract_scoring(built or {})
            if not scoring.get("available"):
                step_results.append(_step(STEP_SCORE_UNIVERSE, S_FAILED,
                    owner="api.universe_scoring.build_universe_scoring",
                    error_code="SCORING_INPUTS_UNAVAILABLE",
                    error_detail=str(scoring.get("status"))[:160]))
                _clear_lock(drc_dir)
                return _persist(FAILED, alignment=alignment,
                                failed_step=STEP_SCORE_UNIVERSE,
                                blockers=[{"code": "SCORING_INPUTS_UNAVAILABLE"}])
            step_results.append(_step(STEP_SCORE_UNIVERSE, S_OK,
                owner="api.universe_scoring.build_universe_scoring",
                as_of_date=scoring.get("ranking_date"),
                records_read=scoring.get("universe_size"),
                output_hash=scoring.get("output_hash"),
                reason="Full eligible universe scored via the canonical universe-"
                       "scoring owner; TOP25/TOP50 produced."))

        # STEP: prepare (never confirm) the operational target.
        if _reuse_or(STEP_PREPARE_TARGET) and prior.get("target"):
            target = prior["target"]
            step_results.append(prior_steps[STEP_PREPARE_TARGET])
        else:
            readiness = _safe(target_loader or _default_target_loader, warnings,
                              "Alpha-target readiness")
            target = _extract_target(readiness)
            if readiness is None:
                # Target preparation failed → do NOT capture evidence or run the
                # assessment (Workstream Q rule 5). Research scoring stays valid.
                step_results.append(_step(STEP_PREPARE_TARGET, S_FAILED,
                    owner="api.alpha_target.load_readiness",
                    error_code="TARGET_PREPARATION_FAILED"))
                _clear_lock(drc_dir)
                return _persist(FAILED, alignment=alignment, scoring=scoring,
                                target=target, failed_step=STEP_PREPARE_TARGET,
                                blockers=[{"code": "TARGET_PREPARATION_FAILED"}])
            step_results.append(_step(STEP_PREPARE_TARGET, S_OK,
                owner="api.alpha_target.load_readiness",
                as_of_date=target.get("target_date"), output_hash=target.get("output_hash"),
                reason="Operational target prepared (NOT auto-confirmed)."))

        # STEP: capture the immutable TRUE_FORWARD snapshot bundle (before Daily Close).
        registry = evidence_registry if evidence_registry is not None else _safe(
            _default_evidence_registry, warnings, "Evidence registry") or []
        if _reuse_or(STEP_CAPTURE_EVIDENCE) and prior.get("evidence"):
            evidence = prior["evidence"]
            step_results.append(prior_steps[STEP_CAPTURE_EVIDENCE])
        else:
            cap_fn = evidence_capture_fn or _default_evidence_capture_fn
            capture = _safe(lambda: cap_fn(market_date=facts["eligible"],
                                           current=_current_for_capture(scoring),
                                           ops=operational, downloader=downloader),
                            warnings, "Forward-evidence capture")
            evidence = _extract_evidence(capture, registry, facts["eligible"])
            performed_write = performed_write or bool((capture or {}).get("performed_write"))
            step_results.append(_step(STEP_CAPTURE_EVIDENCE,
                S_OK if evidence.get("available") else S_FAILED,
                owner="api.forward_prediction_skill.capture_for_daily_close",
                as_of_date=facts["eligible"],
                records_written=evidence.get("captured_snapshot_count"),
                output_hash=evidence.get("output_hash"),
                reason="Immutable TRUE_FORWARD bundle prepared (%s of %s)."
                       % (evidence.get("captured_snapshot_count"),
                          evidence.get("required_snapshot_count"))))

        # STEP: Holding Opportunity-Cost engine (Slice 6 / Milestone 2) — after
        # canonical scoring and before the portfolio-assessment completion step. It
        # reuses the canonical scoring the cycle already built, sources holdings + owned
        # prices, runs the pure kernel and persists an immutable artifact under an
        # isolated research root; review-only (never confirms a target / creates an order).
        raw_hoc_assessment = None
        hoc_subdir = str(Path(drc_dir) / "holding_opportunity_cost") if drc_dir else None
        if _reuse_or(STEP_HOLDING_OPP_COST) and prior.get("holding_opportunity_cost"):
            holding_opp = prior["holding_opportunity_cost"]
            step_results.append(prior_steps[STEP_HOLDING_OPP_COST])
        else:
            # Artifacts are isolated under the run's research root when a drc_dir override
            # is supplied (sandboxed runs); production (drc_dir is None) uses the canonical
            # PAPER_TRADER_HOC_DIR owned by api.holding_opportunity_cost.
            # Run the engine when an explicit seam is injected, this is a production run
            # (no drc_dir override), or the canonical universe contract is available. A
            # sandboxed run with a non-canonical scoring skips hermetically (no live
            # sourcing / no write).
            run_engine = (holding_opp_cost_fn is not None or drc_dir is None
                          or (isinstance(raw_scoring, dict) and bool(raw_scoring.get("rankings"))))
            if run_engine:
                hoc_fn = holding_opp_cost_fn or _default_holding_opp_cost_fn
                hoc_built = _safe(lambda: hoc_fn(scoring=raw_scoring, hoc_dir=hoc_subdir),
                                  warnings, "Holding Opportunity-Cost engine")
                raw_hoc_assessment = (hoc_built or {}).get("assessment")
                holding_opp = _extract_holding_opp_cost(hoc_built, facts["eligible"])
                step_results.append(_step(STEP_HOLDING_OPP_COST,
                    S_OK if holding_opp.get("available") else S_FAILED,
                    owner="api.holding_opportunity_cost.run_and_persist",
                    as_of_date=holding_opp.get("eligible_market_date"),
                    output_hash=holding_opp.get("assessment_hash"),
                    reason=("Per-holding opportunity-cost review produced (%s holdings; %s); "
                            "review-only, no target confirmed and no order created."
                            % (holding_opp.get("holding_count"),
                               holding_opp.get("recommendation_counts")))))
                if not holding_opp.get("available"):
                    warnings.append("The Holding Opportunity-Cost engine did not complete; "
                                    "the research outputs remain valid and reassessment is "
                                    "required.")
            else:
                holding_opp = {"available": False, "owner": "api.holding_opportunity_cost",
                               "state": "SKIPPED", "recommendation_counts": {},
                               "data_gaps": [], "holding_count": 0}
                step_results.append(_step(STEP_HOLDING_OPP_COST, S_SKIPPED,
                    owner="api.holding_opportunity_cost",
                    reason="Canonical universe scoring not available in this sandboxed run; "
                           "Holding Opportunity-Cost engine skipped (no sourcing / no write)."))

        # STEP: Portfolio Reassessment (Stage 20) — the ECONOMIC CHANGE GATE. It runs
        # after EVERY successful signal refresh + opportunity-cost assessment and decides
        # whether a portfolio change is economically justified at all. It consumes the
        # just-produced HOC assessment, the canonical scoring the cycle already built and
        # the canonical freshness contract, runs the pure kernel and persists ONE immutable
        # reassessment artifact + ONE append-only history row under an isolated research
        # root. SYSTEM ORCHESTRATION, never automatic execution: it approves nothing,
        # confirms no order plan and creates no order or fill.
        _prs_skipped = {"available": False, "owner": "api.portfolio_reassessment",
                        "state": "SKIPPED", "decision": "SKIPPED",
                        "proposal_required": False, "blockers": [], "reason_codes": [],
                        "data_gaps": [], "attention_count": 0}
        raw_reassessment = None
        reassess_subdir = (str(Path(drc_dir) / "portfolio_reassessments") if drc_dir else None)
        if _reuse_or(STEP_REASSESS_PORTFOLIO) and prior.get("portfolio_reassessment"):
            reassessment = prior["portfolio_reassessment"]
            step_results.append(prior_steps[STEP_REASSESS_PORTFOLIO])
        else:
            run_engine = (reassessment_fn is not None or drc_dir is None
                          or (isinstance(raw_scoring, dict) and bool(raw_scoring.get("rankings"))))
            if run_engine and holding_opp.get("available"):
                prs_fn = reassessment_fn or _default_reassessment_fn
                prs_built = _safe(
                    lambda: prs_fn(scoring=raw_scoring, hoc_assessment=raw_hoc_assessment,
                                   freshness=fr2, reassessment_dir=reassess_subdir,
                                   hoc_dir=hoc_subdir),
                    warnings, "Portfolio Reassessment engine")
                raw_reassessment = (prs_built or {}).get("reassessment")
                reassessment = _extract_reassessment(prs_built, facts["eligible"])
                step_results.append(_step(STEP_REASSESS_PORTFOLIO,
                    S_OK if reassessment.get("available") else S_FAILED,
                    owner="api.portfolio_reassessment.run_and_persist",
                    as_of_date=reassessment.get("eligible_market_date"),
                    output_hash=reassessment.get("reassessment_hash"),
                    reason=("Portfolio reassessment decided %s (%s actionable holding(s); "
                            "expected net improvement %s; expected one-way turnover %s). "
                            "Nothing is approved and no order is created."
                            % (reassessment.get("decision"),
                               reassessment.get("actionable_holding_count"),
                               reassessment.get("expected_net_improvement"),
                               reassessment.get("expected_one_way_turnover")))))
                if not reassessment.get("available"):
                    warnings.append("The Portfolio Reassessment engine did not complete; the "
                                    "research outputs and opportunity-cost review remain "
                                    "valid, but no portfolio-level decision was recorded.")
            elif run_engine and not holding_opp.get("available"):
                reassessment = dict(_prs_skipped)
                step_results.append(_step(STEP_REASSESS_PORTFOLIO, S_SKIPPED,
                    owner="api.portfolio_reassessment",
                    reason="Holding Opportunity-Cost assessment not available; portfolio "
                           "reassessment skipped (no sourcing / no write)."))
            else:
                reassessment = dict(_prs_skipped)
                step_results.append(_step(STEP_REASSESS_PORTFOLIO, S_SKIPPED,
                    owner="api.portfolio_reassessment",
                    reason="Canonical universe scoring not available in this sandboxed run; "
                           "portfolio reassessment skipped (no sourcing / no write)."))

        # STEP: Reallocation Proposal engine (Slice 7 / Milestone 3) — AFTER the Stage-20
        # ECONOMIC CHANGE GATE. Stage-20 boundary: the target engine runs ONLY when the
        # canonical reassessment owner returned PROPOSAL_READY. A CURRENT_NO_CHANGE /
        # CHANGE_CANDIDATE / BLOCKED_* / MANUAL_REVIEW_REQUIRED verdict deliberately
        # produces NO proposal, so the system no longer manufactures a change target on
        # every signal refresh and then asks the operator to reject it. When it does run it
        # consumes the just-produced HOC assessment + the canonical scoring the cycle
        # already built, sources holdings + owned prices, runs the pure kernel and persists
        # ONE immutable proposal artifact under an isolated research root; review-only
        # (never confirms a target / creates an order).
        _rp_skipped = {"available": False, "owner": "api.reallocation_proposal",
                       "state": "SKIPPED", "action_counts": {}, "data_gaps": [],
                       "proposed_holding_count": 0}
        raw_reallocation = None
        realloc_subdir = (str(Path(drc_dir) / "reallocation_proposals") if drc_dir else None)
        # The Stage-20 owner is the ONLY thing that authorises the target engine to run.
        _gate = _reassessment_gate(raw_reassessment, reassessment)
        if _reuse_or(STEP_BUILD_REALLOCATION) and prior.get("reallocation_proposal"):
            reallocation = prior["reallocation_proposal"]
            step_results.append(prior_steps[STEP_BUILD_REALLOCATION])
        elif not _gate["build_proposal"]:
            reallocation = dict(_rp_skipped)
            reallocation["state"] = "NOT_REQUIRED"
            reallocation["gate_reason"] = _gate["reason"]
            step_results.append(_step(STEP_BUILD_REALLOCATION, S_SKIPPED,
                owner="api.portfolio_reassessment (economic change gate)",
                as_of_date=reassessment.get("eligible_market_date"),
                input_hash=reassessment.get("reassessment_hash"),
                reason=("No reallocation proposal is required: %s" % _gate["reason"])))
        else:
            run_engine = (reallocation_proposal_fn is not None or drc_dir is None
                          or (isinstance(raw_scoring, dict) and bool(raw_scoring.get("rankings"))))
            if run_engine and holding_opp.get("available"):
                rp_fn = reallocation_proposal_fn or _default_reallocation_fn
                rp_built = _safe(
                    lambda: rp_fn(scoring=raw_scoring, hoc_assessment=raw_hoc_assessment,
                                  reallocation_dir=realloc_subdir, hoc_dir=hoc_subdir),
                    warnings, "Reallocation Proposal engine")
                raw_reallocation = (rp_built or {}).get("proposal")
                reallocation = _extract_reallocation(rp_built, facts["eligible"])
                step_results.append(_step(STEP_BUILD_REALLOCATION,
                    S_OK if reallocation.get("available") else S_FAILED,
                    owner="api.reallocation_proposal.run_and_persist",
                    as_of_date=reallocation.get("eligible_market_date"),
                    output_hash=reallocation.get("proposal_hash"),
                    reason=("Reallocation proposal produced (%s proposed holdings; %s); "
                            "review-only, no target confirmed and no order created."
                            % (reallocation.get("proposed_holding_count"),
                               reallocation.get("action_counts")))))
                if not reallocation.get("available"):
                    warnings.append("The Reallocation Proposal engine did not complete; the "
                                    "research outputs and opportunity-cost review remain valid.")
            elif run_engine and not holding_opp.get("available"):
                reallocation = _rp_skipped
                step_results.append(_step(STEP_BUILD_REALLOCATION, S_SKIPPED,
                    owner="api.reallocation_proposal",
                    reason="Holding Opportunity-Cost assessment not available; reallocation "
                           "proposal skipped (no sourcing / no write)."))
            else:
                reallocation = _rp_skipped
                step_results.append(_step(STEP_BUILD_REALLOCATION, S_SKIPPED,
                    owner="api.reallocation_proposal",
                    reason="Canonical universe scoring not available in this sandboxed run; "
                           "reallocation proposal skipped (no sourcing / no write)."))

        # STEP: Persistent Alpha Research Agent (Slice 8 / Milestone 4) — AFTER the
        # reallocation step and BEFORE the portfolio-assessment bridge. It consumes the
        # canonical scoring the cycle already built (champion identity + ranking coverage),
        # the just-produced Slice-6 HOC and Slice-7 reallocation immutable histories, and the
        # forward / performance / challenger evidence, runs the pure kernel and persists ONE
        # immutable research-assessment artifact under an isolated research root. RESEARCH
        # GOVERNANCE ONLY: it recommends bounded research actions but promotes / recalibrates /
        # mutates nothing and creates no order. Its findings never block the cycle.
        _ra_skipped = {"available": False, "owner": "api.research_agent", "state": "SKIPPED",
                       "degradation_categories": [], "data_gaps": []}
        if _reuse_or(STEP_RUN_RESEARCH_AGENT) and prior.get("research_agent"):
            research_agent = prior["research_agent"]
            step_results.append(prior_steps[STEP_RUN_RESEARCH_AGENT])
        else:
            ra_subdir = (str(Path(drc_dir) / "research_agent") if drc_dir else None)
            run_engine = (research_agent_fn is not None or drc_dir is None
                          or (isinstance(raw_scoring, dict) and bool(raw_scoring.get("rankings"))))
            if run_engine:
                ra_fn = research_agent_fn or _default_research_agent_fn
                ra_built = _safe(
                    lambda: ra_fn(scoring=raw_scoring, reallocation=raw_reallocation,
                                  research_agent_dir=ra_subdir, hoc_dir=hoc_subdir,
                                  reallocation_dir=realloc_subdir),
                    warnings, "Persistent Alpha Research Agent")
                research_agent = _extract_research_agent(ra_built, facts["eligible"])
                step_results.append(_step(STEP_RUN_RESEARCH_AGENT,
                    S_OK if research_agent.get("available") else S_FAILED,
                    owner="api.research_agent.run_and_persist",
                    as_of_date=research_agent.get("eligible_market_date"),
                    output_hash=research_agent.get("assessment_hash"),
                    reason=("Research-agent assessment produced (state=%s; evidence=%s; "
                            "recalibration=%s); research governance only, no model promoted "
                            "and no order created."
                            % (research_agent.get("state"),
                               research_agent.get("evidence_quality"),
                               research_agent.get("recalibration_state")))
                    if research_agent.get("available")
                    else ("The Research Agent produced no persistable assessment (state=%s); "
                          "the research outputs and prior-slice reviews remain valid."
                          % research_agent.get("state"))))
                if not research_agent.get("available"):
                    warnings.append("The Persistent Alpha Research Agent did not persist an "
                                    "assessment; the research outputs, opportunity-cost review "
                                    "and reallocation proposal remain valid.")
            else:
                research_agent = _ra_skipped
                step_results.append(_step(STEP_RUN_RESEARCH_AGENT, S_SKIPPED,
                    owner="api.research_agent",
                    reason="Canonical universe scoring not available in this sandboxed run; "
                           "Research Agent skipped (no sourcing / no write)."))

        # STEP: portfolio-assessment bridge (Daily Action Gate for the eligible session).
        # It consumes the just-persisted opportunity-cost summary for its compatibility fields.
        asmt_fn = assessment_loader or _default_assessment_loader
        gate = _safe(lambda: asmt_fn(today=facts["eligible"]), warnings,
                     "Portfolio-assessment bridge")
        assessment = _extract_assessment(gate, facts["eligible"])
        if not assessment.get("available"):
            step_results.append(_step(STEP_RUN_ASSESSMENT, S_FAILED,
                owner="api.daily_action_gate", error_code="ASSESSMENT_UNAVAILABLE"))
            warnings.append("The portfolio-assessment bridge did not complete; the "
                            "research outputs remain valid and reassessment is required.")
        else:
            step_results.append(_step(STEP_RUN_ASSESSMENT, S_OK,
                owner="api.daily_action_gate", as_of_date=assessment.get("assessment_date"),
                output_hash=assessment.get("output_hash"),
                reason="Paper-only assessment (bridge) run for the eligible session."))

        # --- Terminal classification (Workstream Q). --------------------------- #
        final = COMPLETE_WITH_EVIDENCE_GAP if evidence.get("documented_gap") else COMPLETE
        if evidence.get("documented_gap"):
            warnings.append("The prepared forward-evidence bundle has a documented gap "
                            "(%s of %s); it is attention-level and never labelled "
                            "complete." % (evidence.get("captured_snapshot_count"),
                                           evidence.get("required_snapshot_count")))
        _clear_lock(drc_dir)
        return _persist(final, alignment=alignment, scoring=scoring, target=target,
                        evidence=evidence, assessment=assessment,
                        holding_opp_cost=holding_opp,
                        portfolio_reassessment=reassessment,
                        reallocation_proposal=reallocation,
                        research_agent=research_agent, completed_at=_now_iso())
    except Exception as exc:  # noqa: BLE001
        warnings.append("Daily Research Cycle failed: %s" % str(exc)[:200])
        step_results.append(_step("UNCAUGHT", S_FAILED, error_code="UNCAUGHT_EXCEPTION",
                                  error_detail=str(exc)[:200]))
        _clear_lock(drc_dir)
        try:
            return _persist(FAILED, failed_step="UNCAUGHT")
        except Exception:  # noqa: BLE001
            return _contract(state=FAILED, facts=facts, plan=plan, run_id=run_id,
                             step_results=step_results, warnings=warnings,
                             failed_step="UNCAUGHT")


def _current_for_capture(scoring: dict) -> Optional[dict]:
    """The forward-evidence owner accepts an optional ``current`` scoring bundle; we
    do not reconstruct it here (the owner reads its own owned inputs when None)."""
    return None


# --------------------------------------------------------------------------- #
# Lightweight status alias (matches the data_freshness / workflow_state naming).
# --------------------------------------------------------------------------- #
def load_status(**kw) -> dict:
    return load_daily_research_cycle_status(**kw)


__all__ = [
    "PHASE", "EXECUTE_CONFIRMATION",
    "NOT_STARTED", "WAITING_FOR_SESSION_CLOSE", "WAITING_FOR_OWNED_DATA", "PLANNING",
    "REFRESHING_REQUIRED_INPUTS", "VALIDATING_INPUT_ALIGNMENT", "SCORING_UNIVERSE",
    "PREPARING_TARGET", "CAPTURING_FORWARD_EVIDENCE", "RUNNING_PORTFOLIO_ASSESSMENT",
    "COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP", "BLOCKED", "FAILED", "INCONSISTENT",
    "RUN_IN_PROGRESS", "RUN_STATES", "STEP_SEQUENCE", "ALIGNMENT_VOCAB",
    "MONTHLY_EMITTER_ACTION", "STRATEGY_ID", "STRATEGY_VERSION", "UNIVERSE_ID",
    "MANIFEST_CONTRACT_INCOMPLETE", "MANIFEST_PERSISTENCE_UNVERIFIED",
    "TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST",
    "build_execution_plan", "evaluate_alignment", "compute_idempotency_key",
    "load_daily_research_cycle_status", "load_status", "run_daily_research_cycle",
]
