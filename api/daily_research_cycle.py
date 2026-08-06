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
STEP_RUN_ASSESSMENT = "RUN_PORTFOLIO_ASSESSMENT"

STEP_SEQUENCE = (
    STEP_RESOLVE_SESSION, STEP_VALIDATE_CONSISTENCY, STEP_PLAN, STEP_REFRESH_INPUTS,
    STEP_VALIDATE_ALIGNMENT, STEP_SCORE_UNIVERSE, STEP_PREPARE_TARGET,
    STEP_CAPTURE_EVIDENCE, STEP_RUN_ASSESSMENT,
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
    return {
        "session_status": session.get("session_status") or msession.NO_CONFIRMED_DATA,
        "eligible": eligible,
        "expected": freshness.get("expected_completed_market_date"),
        "active_book_id": ab.get("active_book_id"),
        "active_book_name": ab.get("active_book_name"),
        "consistency_status": freshness.get("consistency_status"),
        "consistency_violations": freshness.get("consistency_violations") or [],
        "dates": dates, "input_contract_hash": ich, "idempotency_key": key,
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
        "input_results": (plan or {}).get("refresh_results")
        if plan else None,
        "input_contract_hash": facts.get("input_contract_hash"),
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
        "milestone2_limitation": (
            "This is a compatibility bridge to the existing Daily Action Gate for the "
            "eligible session; it is NOT the Milestone-2 Holding Opportunity-Cost "
            "engine (no replacement-candidate / switching-cost analysis is performed)."),
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
                "daily_action_gate.load_daily_action_gate (assessment bridge)",
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
                         executable=False)

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
                         executable=False)

    if prior and prior.get("input_contract_hash") == facts["input_contract_hash"] \
            and prior.get("state") in _COMPLETED:
        out = dict(prior)
        out["reused_existing_run"] = True
        out["evaluated_at"] = _now_iso()
        out["executable"] = False
        out["state_vocabulary"] = list(RUN_STATES)
        return out

    if prior and prior.get("state") == INCONSISTENT \
            and prior.get("input_contract_hash") != facts["input_contract_hash"]:
        warnings.append("A prior run for %s used a different input contract."
                        % facts["eligible"])

    if plan["plan_blocked"]:
        return _contract(state=BLOCKED, facts=facts, plan=plan, warnings=warnings,
                         blockers=plan["blockers"],
                         required_actions=_blocked_actions(plan), executable=False)

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
                     executable=(state == NOT_STARTED))


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
            assessment_loader=assessment_loader, refresh_confirm_token=refresh_confirm_token,
            operational=operational, inputs=inputs, daily_status=daily_status,
            desk_marks=desk_marks, close_progress=close_progress,
            forward_status=forward_status, date_overrides=date_overrides,
            active_book_override=active_book_override)
    finally:
        _RUN_LOCK.release()


def _run_locked(*, requested_by, now, reference_today, close_cutoff_et, drc_dir,
                downloader, freshness, freshness_loader, daily_refresh_fn,
                monthly_emitter_fn, scoring_fn, target_loader, evidence_capture_fn,
                evidence_registry, assessment_loader, refresh_confirm_token,
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

    # --- Pre-run gates: no writes, nothing persisted. ------------------------- #
    pre = _pre_run_state(facts)
    if pre is not None:
        return _contract(state=pre, facts=facts, plan=plan, warnings=warnings,
                         started_at=started_at, executable=False,
                         required_actions=[{"gate": "market_session",
                                            "action": facts.get("session_operator_action")}])

    key = facts["idempotency_key"]
    ich = facts["input_contract_hash"]
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
        if prior.get("input_contract_hash") == ich and prior.get("state") in _COMPLETED:
            out = dict(prior)
            out["reused_existing_run"] = True
            out["evaluated_at"] = _now_iso()
            return out
        if prior.get("input_contract_hash") != ich and prior.get("state") in _COMPLETED:
            return _contract(state=INCONSISTENT, facts=facts, plan=plan,
                             warnings=["A completed run for %s used a DIFFERENT input "
                                       "contract; refusing to overwrite the immutable "
                                       "outputs (bundle first-write-wins)."
                                       % facts["eligible"]],
                             blockers=[{"code": "DIFFERENT_CONTRACT_SAME_DATE",
                                        "prior_hash": prior.get("input_contract_hash"),
                                        "current_hash": ich}],
                             executable=False)
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

    def _persist(state, **kw):
        rec = _contract(state=state, facts=facts, run_id=run_id, plan=plan,
                        step_results=step_results, started_at=started_at,
                        resumed=resumed, warnings=warnings,
                        performed_write=performed_write, **kw)
        _save_run(rec, drc_dir)
        _update_index(eligible_date=facts["eligible"], idempotency_key=key,
                      input_contract_hash=ich, run_id=run_id, state=state,
                      drc_dir=drc_dir)
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
        if _reuse_or(STEP_SCORE_UNIVERSE) and prior.get("scoring"):
            scoring = prior["scoring"]
            step_results.append(prior_steps[STEP_SCORE_UNIVERSE])
        else:
            built = _safe(scoring_fn or _default_scoring_fn, warnings,
                          "Universe scoring")
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

        # STEP: portfolio-assessment bridge (Daily Action Gate for the eligible session).
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
                        completed_at=_now_iso())
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
    "build_execution_plan", "evaluate_alignment", "compute_idempotency_key",
    "load_daily_research_cycle_status", "load_status", "run_daily_research_cycle",
]
