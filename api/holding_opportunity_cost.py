r"""Phase 29G Slice 6 — Holding Opportunity-Cost composition, persistence & read owner.

This is the ONE canonical orchestration / validation / immutable-artifact /
read-contract owner for the Holding Opportunity-Cost Engine (Consolidation
Roadmap Slice 6 / Charter Milestone 2). It performs NO calculation of its own —
the single canonical calculation lives in ``engine.holding_opportunity_cost``.
This module only:

  1. SOURCES an immutable point-in-time assessment-input contract from the
     authoritative owners (``api.portfolio_state`` for holdings / weights / NAV /
     cash / sectors; ``api.universe_scoring`` for rank / score / eligibility /
     adv_dollar; ``api.price_panel`` for owned trailing close + dollar volume;
     ``engine.market_session`` for the previous eligible session; the previous
     eligible date's persisted artifact for prior-session rank), reusing the
     canonical construction constants from ``api.multi_horizon_engine`` and the
     transaction-cost constant from ``api.paper_trading_desk`` (never forked).
  2. RUNS the pure kernel.
  3. PERSISTS a completed production assessment as an immutable artifact under a
     dedicated research / decision-evidence root (atomic write, index/manifest,
     idempotent identical rerun, conflicting artifact rejected, interrupted write
     recoverable). It NEVER writes an operational ledger, PostgreSQL, order, fill,
     holding, cash or NAV.
  4. Exposes ONE read contract for ``GET /v1/operations/holding-opportunity-cost``
     (read-only) plus a lightweight summary for the Daily Action Gate.

The sole NORMAL execution path is the Daily Research Cycle
(``POST /v1/operations/daily-research-cycle/run``); there is deliberately NO
separate manual opportunity-cost execution endpoint. This module is preview-first,
paper-only, review-only: no target is confirmed, no order is created, no automation
is enabled.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.engine import data_gap_taxonomy as gaptax
from paper_trader.engine import holding_opportunity_cost as kernel

# Re-export the frozen vocabularies / versions so callers use one source.
SCHEMA_VERSION = kernel.SCHEMA_VERSION
DECISION_POLICY_VERSION = kernel.DECISION_POLICY_VERSION
COST_POLICY_VERSION = kernel.COST_POLICY_VERSION
INPUT_SCHEMA_VERSION = kernel.INPUT_SCHEMA_VERSION
RECOMMENDATION_VOCAB = kernel.RECOMMENDATION_VOCAB
COMPOSITION_OWNER = "api.holding_opportunity_cost"
PHASE = "29G-Slice6"

# Read-layer states extend the kernel's assessment states.
STATE_READY = kernel.STATE_READY
STATE_DEGRADED = kernel.STATE_DEGRADED
STATE_BLOCKED = kernel.STATE_BLOCKED
STATE_NO_ACTIVE_BOOK = kernel.STATE_NO_ACTIVE_BOOK
STATE_NOT_RUN = "NOT_RUN"
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_NO_ACTIVE_BOOK,
                    STATE_NOT_RUN, STATE_UNAVAILABLE)

# --------------------------------------------------------------------------- #
# Release 29.5 — ARTIFACT PROVENANCE. This module writes the artifact, so it owns the
# answer to "who produced this, and what does it claim to be?".
#
# WHY THIS EXISTS
# ---------------
# Two canonical owners legitimately call ``run_and_persist``:
#
#   * ``api.daily_research_cycle``  — the GOVERNED daily cycle, which persists a run
#     manifest binding this artifact to a run id.
#   * ``api.event_signal_refresh``  — the Release 28 incremental refresh that Release 29
#     continuous collection triggers whenever arriving information is material. It runs
#     many times a day, produces a perfectly real assessment, and persists NO manifest
#     because it is not the governed cycle.
#
# Before Release 29.5 the two were indistinguishable on disk, so an artifact written by
# the event cycle read as "a terminal DRC output whose manifest is missing" — a
# corruption signature. That put the workflow into RECOVERY, RECOVERY offers no
# executable stage, and the Daily Research Cycle could therefore never run to write the
# manifest whose absence caused the RECOVERY. The deadlock was structural.
#
# THE DISTINCTION
# ---------------
# An artifact is GOVERNED_DRC_TERMINAL only when it CLAIMS to be — i.e. it carries a
# ``drc_run_id``. Everything else is LIVE_PRE_DRC_SIGNAL: real, current, displayable
# signal state that does NOT prove the governed cycle ran. Absence of a claim is not a
# broken claim, so a legacy artifact written before this field existed classifies as
# LIVE_PRE_DRC_SIGNAL and can never manufacture a corruption verdict out of its own age.
#
# The claim is what fails closed: an artifact claiming a run id whose manifest is
# missing, unreadable or bound to a different session IS the corruption case, and
# ``api.daily_research_cycle`` (the manifest owner) is the only module entitled to
# adjudicate it. This module states the claim; it never validates a manifest.
#
# NOTHING HERE EVER PROVES COMPLETION. ``proves_drc_complete`` is unconditionally False:
# only a validated manifest held by the manifest owner proves a governed cycle ran.
# --------------------------------------------------------------------------- #
PROVENANCE_OWNER = "api.holding_opportunity_cost"
PROVENANCE_SCHEMA_VERSION = "holding_opportunity_cost.provenance.v1"

#: Canonical producers. Anything else is recorded verbatim and still classifies by CLAIM.
PRODUCER_DAILY_RESEARCH_CYCLE = "api.daily_research_cycle"
PRODUCER_EVENT_SIGNAL_REFRESH = "api.event_signal_refresh"
PRODUCER_UNRECORDED = "UNRECORDED"

#: Class 1 — live/pre-DRC signal state. May exist before a manifest; never proves one.
ARTIFACT_CLASS_LIVE_PRE_DRC = "LIVE_PRE_DRC_SIGNAL"
#: Class 2 — an artifact that CLAIMS governed DRC terminal provenance. The manifest owner
#: must be able to validate that claim or the state is corrupt.
ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL = "GOVERNED_DRC_TERMINAL"
ARTIFACT_CLASS_VOCABULARY = (ARTIFACT_CLASS_LIVE_PRE_DRC,
                             ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL)

#: The artifact key carrying the provenance block (top level — deliberately OUTSIDE
#: ``identity`` and ``assessment`` so ``assessment_hash`` and ``artifact_id`` are
#: unaffected and every existing artifact stays byte-valid and re-readable).
PROVENANCE_KEY = "produced_by"


def build_provenance(*, producer_owner: Any = None, drc_run_id: Any = None) -> dict:
    """The provenance block stamped into a newly written artifact.

    ``drc_run_id`` is supplied ONLY by the governed Daily Research Cycle, and only for
    the run that is persisting its own manifest. Supplying it is what makes the artifact
    claim Class 2 — and therefore what makes a missing manifest a corruption.
    """
    run_id = str(drc_run_id) if drc_run_id else None
    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "provenance_owner": PROVENANCE_OWNER,
        "producer_owner": str(producer_owner) if producer_owner else PRODUCER_UNRECORDED,
        "drc_run_id": run_id,
        "claims_drc_terminal": bool(run_id),
    }


def classify_artifact_provenance(artifact: Optional[dict]) -> dict:
    """Classify an artifact as Class 1 (live/pre-DRC) or Class 2 (claims DRC terminal).

    PURE. Reads only the artifact document. Opens no manifest, no index and no store —
    validating a claimed manifest belongs to the manifest owner, not here.
    """
    pb = ((artifact or {}).get(PROVENANCE_KEY) or {}) if isinstance(artifact, dict) else {}
    run_id = pb.get("drc_run_id") or None
    # A claim is made by carrying a run id. The boolean alone can never manufacture one:
    # a claim without an id names nothing the manifest owner could ever validate, so it
    # would be permanently unresolvable rather than fail-closed.
    claims = bool(run_id)
    return {
        "provenance_owner": PROVENANCE_OWNER,
        "provenance_schema_version": PROVENANCE_SCHEMA_VERSION,
        "producer_owner": pb.get("producer_owner") or PRODUCER_UNRECORDED,
        "drc_run_id": run_id,
        "claims_drc_terminal": claims,
        "artifact_class": (ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL if claims
                           else ARTIFACT_CLASS_LIVE_PRE_DRC),
        "artifact_class_vocabulary": list(ARTIFACT_CLASS_VOCABULARY),
        # INVARIANT (Release 29.5): an artifact NEVER proves the governed cycle ran.
        # Only a validated run manifest does, and this module holds no manifest.
        "proves_drc_complete": False,
        "manifest_owner": PRODUCER_DAILY_RESEARCH_CYCLE,
    }

# --- immutable artifact root (configurable; a research / decision-evidence root, --- #
# NEVER the operational ledger root). -------------------------------------------- #
HOC_DIR_ENV = "PAPER_TRADER_HOC_DIR"
_DEFAULT_HOC_DIR = Path(r"D:\Stock_Prediction_app_data\holding_opportunity_cost")
_ARTIFACTS_SUBDIR = "artifacts"
_INDEX_FILE = "index.json"

# Trailing bars pulled from the price panel (comfortably covers the 60-close
# return / volatility / drawdown windows and the covariance lookback).
_TRAILING_BARS = 140


# --------------------------------------------------------------------------- #
# Time / io helpers
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _hoc_dir(hoc_dir=None) -> Path:
    if hoc_dir is not None:
        return Path(hoc_dir)
    env = os.environ.get(HOC_DIR_ENV)
    return Path(env) if env else _DEFAULT_HOC_DIR


def _artifacts_dir(hoc_dir=None) -> Path:
    return _hoc_dir(hoc_dir) / _ARTIFACTS_SUBDIR


def _index_path(hoc_dir=None) -> Path:
    return _hoc_dir(hoc_dir) / _INDEX_FILE


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Atomic write: a temp file in the same dir then ``os.replace`` (interrupted
    writes leave the prior file intact and only an orphan .tmp, never a partial)."""
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


def _load_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _f(x: Any) -> Optional[float]:
    return kernel._f(x)


# --------------------------------------------------------------------------- #
# Release 54.3 — THE THREE HOC IDENTITY AXES
#
# Slice 6 asked ONE identity question — "is this the same assessment?" — answered
# it with ``assessment_hash``, and indexed exactly ONE artifact per (book,
# eligible session). Anything else arriving the same session was CONFLICT_REJECTED.
# That was safe (an immutable artifact was never overwritten) and correct for a
# once-a-day governed cycle.
#
# Continuous intraday management breaks it. The book can be economically identical
# all session — same holdings, same cash, same NAV — while the evidence behind the
# opportunity-cost conclusion moves: a newer ranking, a newer owned price window, a
# prior-rank snapshot that only just became available. That is a NEW point-in-time
# assessment of an unchanged portfolio, and refusing it stranded the system on the
# first artifact of the session: every later cycle computed a real HOC result that
# existed only in memory, and every reassessment built on it bound an
# ``hoc_assessment_hash`` nobody could ever retrieve. A governance gate must never
# accept an ephemeral hash as immutable evidence, so the governed intraday decision
# could not be reached at all.
#
# R54.3 separates the three questions Release 54.2 already separated for the
# portfolio reassessment, using the SAME words — one vocabulary, not two:
#
#   1. ECONOMIC IDENTITY   "which portfolio is this about?"  economic_state_hash
#   2. EVIDENCE IDENTITY   "which observations produced it?" assessment_evidence_hash
#   3. CONCLUSION IDENTITY "what did it conclude?"           decision_fingerprint
#
# The evidence identity binds CANONICAL BOUND EVIDENCE ONLY — every component is an
# input the kernel demonstrably consumes. It deliberately EXCLUDES:
#   * ``portfolio_state_hash``  — the Stage-21 trap: that document-wide hash embeds
#     this assessment's own output (via api.daily_action_gate), so it drifts the
#     moment the artifact is written and would make every rerun look different;
#   * ``economic_state_hash``   — that is the OTHER axis, compared separately;
#   * ``assessment_hash``       — the CONCLUSION, not the evidence behind it, and
#     self-referential besides;
#   * wall clock, request id, run id, event-cycle id, scheduler invocation id, the
#     materiality trigger fingerprint and the persistence timestamp — provenance,
#     never identity. Two triggers reaching the same conclusion from the same
#     evidence are ONE assessment; versioning them twice is evidence noise, which
#     is exactly what a poll-driven cycle generates.
# --------------------------------------------------------------------------- #
ASSESSMENT_EVIDENCE_IDENTITY_VERSION = (
    "holding_opportunity_cost.assessment_evidence_identity.v1")

#: The bound-evidence components that make one assessment of an unchanged
#: portfolio materially different from another. Each is either already published
#: by the Slice-6 input contract or is a deterministic fingerprint of a raw kernel
#: input that the contract carries — R54.3 introduces no new evidence SOURCE.
ASSESSMENT_EVIDENCE_COMPONENTS = (
    "universe_scoring_hash",
    "universe_input_contract_hash",
    "scoring_ranking_date",
    "corporate_actions_hash",
    "holdings_snapshot_fingerprint",
    "market_data_fingerprint",
    "previous_ranking_fingerprint",
    "prior_signal_fingerprint",
    "policy_fingerprint",
    "decision_policy_version",
    "cost_policy_version",
    "inputs_as_of_eligible_date",
)

#: Provenance that must NEVER reach the evidence hash. Named explicitly so the
#: exclusion is testable rather than merely intended.
EVIDENCE_EXCLUDED_PROVENANCE = (
    "generated_at", "persisted_at", "now", "wall_clock", "request_id", "run_id",
    "drc_run_id", "event_cycle_id", "scheduler_invocation_id",
    "materiality_trigger_fingerprint", "materiality_event_timestamp",
    "portfolio_state_hash", "economic_state_hash", "assessment_hash",
    "artifact_id",
)


def holdings_snapshot_fingerprint(input_contract: Optional[dict]) -> Optional[str]:
    """WHAT was held, at what weight and value, as ONE fingerprint.

    The kernel reads every one of these fields (weights drive concentration, HHI,
    risk contributions and the name-cap breach; market value drives days-to-
    liquidate), so a change in any of them is genuinely different evidence. It is
    a SAFETY NET on the evidence axis rather than the economic axis: when the
    state owner publishes no ``economic_state_hash`` (a legacy artifact, a
    hermetic caller), a real holdings change still creates a VERSION instead of
    being mistaken for identical evidence with a different answer.
    """
    ic = input_contract or {}
    pre = ic.get("holdings_snapshot_fingerprint")
    if pre:
        return pre
    rows = ic.get("positions")
    if not rows:
        return None
    return kernel.stable_hash([
        {"ticker": p.get("ticker"), "sector": p.get("sector"),
         "quantity": p.get("quantity"), "current_weight": p.get("current_weight"),
         "market_value": p.get("market_value"), "price": p.get("price")}
        for p in sorted(rows, key=lambda p: str(p.get("ticker") or ""))])


def market_data_fingerprint(input_contract: Optional[dict]) -> Optional[str]:
    """The OWNED market window this assessment actually saw, as ONE fingerprint.

    Trailing closes, median dollar volume and the aligned return matrix determine
    trailing return, realised volatility, max drawdown, days-to-liquidate and the
    covariance risk contributions. A newer owned window is therefore materially
    different evidence about an unchanged portfolio — the single most common
    reason a same-session intraday assessment legitimately differs. Nothing here
    is a clock: the fingerprint covers the DATA, and an unchanged window
    reproduces the identical value however many times it is read.
    """
    ic = input_contract or {}
    pre = ic.get("market_data_fingerprint")
    if pre:
        return pre
    trailing = ic.get("trailing_prices") or {}
    mdv = ic.get("median_dollar_volume") or {}
    aligned = ic.get("aligned_returns") or {}
    if not (trailing or mdv or aligned):
        return None
    return kernel.stable_hash({
        "trailing": {tk: {"dates": (trailing.get(tk) or {}).get("dates"),
                          "adj": (trailing.get(tk) or {}).get("adj"),
                          "ret": (trailing.get(tk) or {}).get("ret")}
                     for tk in sorted(trailing)},
        "median_dollar_volume": {tk: mdv.get(tk) for tk in sorted(mdv)},
        "aligned_dates": aligned.get("dates"),
        "aligned_series": {tk: (aligned.get("series") or {}).get(tk)
                           for tk in sorted(aligned.get("series") or {})},
    })


def previous_ranking_fingerprint(input_contract: Optional[dict]) -> Optional[str]:
    """The point-in-time PRIOR-rank snapshot, as ONE fingerprint.

    Rank CHANGE is what the deterioration rule reads, so a prior snapshot that
    became available (or resolved to a different session) changes what the
    assessment is entitled to conclude. Its availability state and source date
    are part of the same fact and travel with it.
    """
    ic = input_contract or {}
    pre = ic.get("previous_ranking_fingerprint")
    if pre:
        return pre
    prev = ic.get("previous_ranking")
    state = ic.get("previous_ranking_state")
    src = ic.get("previous_ranking_source_date")
    if prev is None and state is None and src is None:
        return None
    return kernel.stable_hash({
        "state": state, "source_date": src,
        "ranks": ({k: prev[k] for k in sorted(prev)} if isinstance(prev, dict)
                  else prev)})


def prior_signal_fingerprint(input_contract: Optional[dict]) -> Optional[str]:
    """The prior-signal inputs the kernel was handed, as ONE fingerprint."""
    ic = input_contract or {}
    pre = ic.get("prior_signal_fingerprint")
    if pre:
        return pre
    sig = ic.get("prior_signal")
    if not sig:
        return None
    return kernel.stable_hash(sig)


def policy_fingerprint(input_contract: Optional[dict] = None,
                       result: Optional[dict] = None) -> Optional[str]:
    """The RESOLVED decision/cost policy this assessment was run under.

    The frozen ``decision_policy_version`` / ``cost_policy_version`` labels are
    already separate components; this covers the resolved NUMERIC policy (entry
    rank, exit buffer, sector cap, name cap, liquidity floor, cost rate), because
    a construction constant moving silently changes every recommendation. It is
    read from the assessment the kernel itself published, which is exactly what a
    persisted artifact carries — so a historical artifact stays comparable.
    """
    ic = input_contract or {}
    pre = ic.get("policy_fingerprint")
    if pre:
        return pre
    pol = (result or {}).get("policy")
    if not pol:
        return None
    return kernel.stable_hash(pol)


def assessment_evidence_identity(*, input_contract: Optional[dict] = None,
                                 result: Optional[dict] = None) -> dict:
    """The evidence identity of ONE opportunity-cost assessment.

    Accepts a FULL or a COMPACTED input contract — both publish the same component
    names (the compacted one carries the fingerprints precomputed at write time),
    so a historical artifact stays comparable without a single byte of it being
    rewritten.
    """
    ic = input_contract or {}
    return {
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "scoring_ranking_date": ic.get("scoring_ranking_date"),
        "corporate_actions_hash": ic.get("corporate_actions_hash"),
        "holdings_snapshot_fingerprint": holdings_snapshot_fingerprint(ic),
        "market_data_fingerprint": market_data_fingerprint(ic),
        "previous_ranking_fingerprint": previous_ranking_fingerprint(ic),
        "prior_signal_fingerprint": prior_signal_fingerprint(ic),
        "policy_fingerprint": policy_fingerprint(ic, result),
        "decision_policy_version": DECISION_POLICY_VERSION,
        "cost_policy_version": COST_POLICY_VERSION,
        "inputs_as_of_eligible_date": ic.get("inputs_as_of_eligible_date"),
    }


def assessment_evidence_hash(evidence: Optional[dict]) -> str:
    ev = evidence or {}
    return kernel.stable_hash({"schema": ASSESSMENT_EVIDENCE_IDENTITY_VERSION,
                               **{k: ev.get(k)
                                  for k in ASSESSMENT_EVIDENCE_COMPONENTS}})


def decision_fingerprint(result: Optional[dict]) -> Optional[str]:
    """The CONCLUSION alone, stripped of provenance.

    ``assessment_hash`` covers the whole kernel result INCLUDING ``provenance``,
    which carries ``portfolio_state_hash`` — so two runs can differ in that hash
    while having reached an identical conclusion from identical evidence.
    Comparing the conclusion directly is what separates "the same assessment,
    re-run" from "identical evidence produced a DIFFERENT answer", and only the
    second is a genuine conflict.
    """
    res = result or {}
    if not res:
        return None
    return kernel.stable_hash({k: v for k, v in res.items()
                               if k not in ("provenance", "assessment_hash")})


# --------------------------------------------------------------------------- #
# Artifact identity (Workstream H)
# --------------------------------------------------------------------------- #
def artifact_identity(*, input_contract: dict, result: dict) -> dict:
    return {
        "eligible_market_date": input_contract.get("eligible_market_date"),
        "active_book_id": input_contract.get("active_book_id"),
        "portfolio_state_hash": input_contract.get("portfolio_state_hash"),
        # Stage 21 (Workstream 0E) — the ECONOMIC fingerprint of the portfolio this
        # assessment describes. R54.3's economic axis; NEVER ``portfolio_state_hash``
        # above, which embeds this assessment's own output.
        "economic_state_hash": input_contract.get("economic_state_hash"),
        # Stage 19.1 — the corporate-action registry the holdings/NAV this assessment
        # consumed were projected through.
        "corporate_actions_hash": input_contract.get("corporate_actions_hash"),
        "universe_scoring_hash": input_contract.get("universe_scoring_hash"),
        "decision_policy_version": DECISION_POLICY_VERSION,
        "assessment_hash": result.get("assessment_hash"),
        # Release 54.3 — the two identities that decide same-session versioning.
        # ``assessment_evidence_hash`` answers "is this the same ASSESSMENT?" and
        # ``decision_fingerprint`` answers "is this the same CONCLUSION?"; the
        # economic axis stays ``economic_state_hash`` above.
        "assessment_evidence_identity_version": ASSESSMENT_EVIDENCE_IDENTITY_VERSION,
        "assessment_evidence_hash": assessment_evidence_hash(
            assessment_evidence_identity(input_contract=input_contract,
                                         result=result)),
        "decision_fingerprint": decision_fingerprint(result),
    }


def artifact_id_for(identity: dict) -> str:
    book = (identity.get("active_book_id") or "book")
    date = (identity.get("eligible_market_date") or "nodate")
    h = (identity.get("assessment_hash") or "")[:12]
    return "hoc_%s_%s_%s" % (date, book, h)


def _index_key(active_book_id: Optional[str], eligible_market_date: Optional[str]) -> str:
    return "%s|%s" % (active_book_id or "?", eligible_market_date or "?")


def _compact_input_contract(ic: dict, result: Optional[dict] = None) -> dict:
    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "active_book_label": ic.get("active_book_label"),
        "valuation_date": ic.get("valuation_date"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        # Stage 21 (Workstream 0E) — persist BOTH sides of the currency comparison.
        "economic_state_hash": ic.get("economic_state_hash"),
        "economic_identity_version": ic.get("economic_identity_version"),
        "corporate_actions_hash": ic.get("corporate_actions_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "scoring_input_fingerprints": ic.get("scoring_input_fingerprints"),
        "scoring_ranking_date": ic.get("scoring_ranking_date"),
        "cost_policy_version": COST_POLICY_VERSION,
        "decision_policy_version": DECISION_POLICY_VERSION,
        "positions_count": len(ic.get("positions") or []),
        "universe_rows_count": len(ic.get("universe_rows") or []),
        "previous_ranking_state": ic.get("previous_ranking_state"),
        "previous_ranking_reason": ic.get("previous_ranking_reason"),
        "previous_ranking_source_date": ic.get("previous_ranking_source_date"),
        "inputs_as_of_eligible_date": ic.get("inputs_as_of_eligible_date"),
        # Release 54.3 — the raw kernel inputs are far too large to persist whole,
        # so their DETERMINISTIC fingerprints are persisted instead. This is what
        # lets the NEXT persist decide the version question, and lets an auditor
        # re-derive the evidence identity, from the compacted contract alone.
        "holdings_snapshot_fingerprint": holdings_snapshot_fingerprint(ic),
        "market_data_fingerprint": market_data_fingerprint(ic),
        "previous_ranking_fingerprint": previous_ranking_fingerprint(ic),
        "prior_signal_fingerprint": prior_signal_fingerprint(ic),
        "policy_fingerprint": policy_fingerprint(ic, result),
    }


# --------------------------------------------------------------------------- #
# Input-contract sourcing (Workstream B). Every source is injectable for tests.
# --------------------------------------------------------------------------- #
def _live_policy_overrides() -> dict:
    """Reuse the canonical construction + cost constants (never forked)."""
    ov: dict[str, Any] = {}
    try:
        import math as _m
        from paper_trader.api import multi_horizon_engine as eng
        n = eng.BOOK_SIZES[0]
        ov.update({
            "entry_rank": n,
            "exit_buffer_fraction": eng.EXIT_BUFFER_FRACTION,
            "exit_buffer_rank": _m.ceil(n * (1.0 + eng.EXIT_BUFFER_FRACTION)),
            "sector_cap_fraction": eng.SECTOR_CAP_FRACTION,
            "max_name_weight": eng.MAX_INDIVIDUAL_WEIGHT,
            "min_adv_dollar": eng.MIN_ADV_DOLLAR,
        })
    except Exception:  # noqa: BLE001 - degrade to kernel defaults
        pass
    try:
        from paper_trader.api import paper_trading_desk as desk
        ov.update({
            "cost_bps_per_side": desk.COST_BPS_PER_SIDE,
            "cost_rate_per_side": desk.COST_RATE_PER_SIDE,
            "round_trip_cost_bps": 2.0 * desk.COST_BPS_PER_SIDE,
        })
    except Exception:  # noqa: BLE001
        pass
    return ov


def resolve_policy(policy_overrides: Optional[dict] = None) -> dict:
    pol = dict(kernel.default_policy())
    pol.update(_live_policy_overrides())
    if policy_overrides:
        pol.update(policy_overrides)
    return pol


#: Release 50 - the Holding Opportunity-Cost assessment is the EQUITY sleeve's
#: review: its universe, ranks, entry / exit buffers and replacement candidates are
#: the approved US-equity model's. A non-equity position (a future, an FX spot) is
#: not "outside the eligible universe" - it belongs to a different sleeve, and its
#: review is owned by the opportunity frontier. Passing it here would make the
#: deterioration rule read it as NOT_IN_ELIGIBLE_UNIVERSE and recommend an EXIT it
#: never earned. It is excluded BY NAME, and the exclusion is reported.
_EQUITY_INSTRUMENT_TYPES = (None, "", "CASH_EQUITY")


def excluded_non_equity_positions(ps: dict) -> list[dict]:
    return [{"ticker": p.get("ticker"), "instrument_type": p.get("instrument_type"),
             "sleeve_id": p.get("sleeve_id"), "asset_class": p.get("asset_class"),
             "reviewed_by": "engine.opportunity_frontier"}
            for p in (ps.get("positions") or [])
            if p.get("instrument_type") not in _EQUITY_INSTRUMENT_TYPES]


def _positions_from_state(ps: dict) -> list[dict]:
    out = []
    for p in (ps.get("positions") or []):
        if p.get("instrument_type") not in _EQUITY_INSTRUMENT_TYPES:
            continue
        out.append({
            "ticker": p.get("ticker"),
            "sector": p.get("sector") or "Unknown",
            "quantity": p.get("quantity"),
            "current_weight": _f(p.get("portfolio_weight")),
            "market_value": _f(p.get("market_value")),
            "price": _f(p.get("price")),
            "target_weight": _f(p.get("target_weight")),
        })
    return out


def _universe_rows_from_scoring(scoring: dict) -> list[dict]:
    return list(scoring.get("rankings") or [])


def _holding_eligibility(*, held: set, scoring: dict) -> dict:
    """Eligibility for HELD names, reusing the canonical universe eligibility.

    In the eligible ``rankings`` -> eligible. Otherwise map the scored-but-excluded
    reason (``exclusions``) or flag NOT_IN_ELIGIBLE_UNIVERSE (a hard exit reason).
    """
    ranked = {r.get("ticker") for r in (scoring.get("rankings") or [])}
    exclusions = scoring.get("exclusions") or {}
    out: dict[str, dict] = {}
    for tk in held:
        if tk in ranked:
            out[tk] = {"eligible": True, "hard_codes": [], "exclusion_reason": None}
        else:
            reason = exclusions.get(tk)
            codes = [reason] if reason else ["NOT_IN_ELIGIBLE_UNIVERSE"]
            out[tk] = {"eligible": False, "hard_codes": codes, "exclusion_reason": reason}
    return out


def _trailing_prices(*, price_panel: dict, tickers: list, eligible: str) -> dict:
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    out: dict[str, dict] = {}
    for tk in tickers:
        s = series.get(tk)
        if not s:
            continue
        j = pp.asof_index(s.get("dates") or [], eligible)
        if j < 0:
            continue
        lo = max(0, j - _TRAILING_BARS + 1)
        out[tk] = {
            "dates": list(s["dates"][lo:j + 1]),
            "adj": list(s["adj"][lo:j + 1]),
            "ret": list(s["ret"][lo:j + 1]),
        }
    return out


def _median_dollar_volumes(*, price_panel: dict, tickers: list, eligible: str,
                           window: int = 20) -> dict:
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    out: dict[str, Optional[float]] = {}
    for tk in tickers:
        s = series.get(tk)
        if not s:
            out[tk] = None
            continue
        j = pp.asof_index(s.get("dates") or [], eligible)
        out[tk] = pp.trailing_median_dollar_volume(s, j, window) if j >= 0 else None
    return out


def _aligned_returns(*, price_panel: dict, tickers: list, eligible: str,
                     lookback: int) -> dict:
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    per: dict[str, dict] = {}
    date_sets: list[set] = []
    for tk in tickers:
        s = series.get(tk)
        if not s:
            continue
        j = pp.asof_index(s.get("dates") or [], eligible)
        if j < 1:
            continue
        dmap = {}
        for i in range(1, j + 1):
            r = s["ret"][i]
            if r is not None:
                dmap[s["dates"][i]] = float(r)
        if dmap:
            per[tk] = dmap
            date_sets.append(set(dmap.keys()))
    if not date_sets:
        return {"dates": [], "series": {}}
    common = set.intersection(*date_sets) if len(date_sets) > 1 else date_sets[0]
    common_sorted = sorted(common)[-lookback:]
    out = {tk: [per[tk].get(d) for d in common_sorted] for tk in per}
    return {"dates": common_sorted, "series": out}


def build_input_contract(*, portfolio_state: dict, scoring: dict,
                         price_panel: Optional[dict] = None,
                         previous_ranking: Optional[dict] = None,
                         previous_ranking_state: Optional[str] = None,
                         previous_ranking_reason: Optional[str] = None,
                         previous_ranking_source_date: Optional[str] = None,
                         prior_signal: Optional[dict] = None,
                         policy: Optional[dict] = None) -> dict:
    """Assemble the immutable point-in-time assessment-input contract (Workstream B).

    All inputs are sourced as of the portfolio-state eligible market date. Prior
    point-in-time rank is honestly reported UNAVAILABLE when no prior snapshot
    exists. No future rows; no current-snapshot-as-prior; no fabricated volume.
    """
    pol = policy or resolve_policy()
    ps = portfolio_state or {}
    sc = scoring or {}
    eligible = ((ps.get("dates") or {}).get("eligible_market_date"))
    active_book = ps.get("active_book") or {}
    active_book_id = active_book.get("book_id")

    positions = _positions_from_state(ps)
    held = {p["ticker"] for p in positions if p.get("ticker")}
    universe_rows = _universe_rows_from_scoring(sc)
    holding_elig = _holding_eligibility(held=held, scoring=sc)

    trailing = {}
    mdv = {}
    aligned = {"dates": [], "series": {}}
    if price_panel:
        held_list = sorted(held)
        trailing = _trailing_prices(price_panel=price_panel, tickers=held_list, eligible=eligible)
        mdv = _median_dollar_volumes(price_panel=price_panel, tickers=held_list, eligible=eligible)
        aligned = _aligned_returns(price_panel=price_panel, tickers=held_list,
                                   eligible=eligible, lookback=pol["covariance_lookback"])

    prev_state = previous_ranking_state or ("AVAILABLE" if previous_ranking else "UNAVAILABLE")
    prev_reason = previous_ranking_reason or (
        "" if previous_ranking else
        "No prior eligible-session ranking artifact is available; previous rank and "
        "rank change are reported as unavailable.")

    scoring_ranking_date = sc.get("ranking_date")
    inputs_as_of = bool(eligible and (not scoring_ranking_date
                                      or scoring_ranking_date == eligible))

    return {
        "schema_version": INPUT_SCHEMA_VERSION,
        "eligible_market_date": eligible,
        "active_book_id": active_book_id,
        "active_book_label": active_book.get("book_label"),
        "valuation_date": (ps.get("dates") or {}).get("valuation_date"),
        "portfolio_state_hash": ps.get("state_hash"),
        # Stage 21 (Workstream 0E): the ECONOMIC fingerprint of the portfolio this
        # assessment was computed against — holdings / cash / NAV / corporate actions
        # only. This is what a downstream consumer binds to when it asks "does this
        # assessment still describe the portfolio?". ``portfolio_state_hash`` above is
        # kept for continuity but must NEVER be used for that question: it embeds this
        # assessment's own output, so it drifts the moment the artifact is written.
        "economic_state_hash": ps.get("economic_state_hash"),
        "economic_identity_version": ps.get("economic_identity_version"),
        # Stage 19.1: the corporate-action registry state the CURRENT holdings / NAV
        # below were projected through (owned by api.corporate_actions, surfaced by
        # api.portfolio_state). Bound into the assessment identity.
        "corporate_actions_hash": ((ps.get("corporate_actions") or {})
                                   .get("registry_fingerprint")),
        "universe_scoring_hash": sc.get("output_hash"),
        "universe_input_contract_hash": sc.get("input_contract_hash"),
        "scoring_input_fingerprints": sc.get("input_fingerprints"),
        "scoring_ranking_date": scoring_ranking_date,
        "inputs_as_of_eligible_date": inputs_as_of,
        "cost_policy_version": COST_POLICY_VERSION,
        "decision_policy_version": DECISION_POLICY_VERSION,
        "nav": (ps.get("capital") or {}).get("nav"),
        "cash": (ps.get("capital") or {}).get("cash"),
        "positions": positions,
        "universe_rows": universe_rows,
        "holding_eligibility": holding_elig,
        "previous_ranking": previous_ranking,
        "previous_ranking_state": prev_state,
        "previous_ranking_reason": prev_reason,
        "previous_ranking_source_date": previous_ranking_source_date,
        "prior_signal": prior_signal or {},
        "trailing_prices": trailing,
        "median_dollar_volume": mdv,
        "aligned_returns": aligned,
    }


# --------------------------------------------------------------------------- #
# Default source loaders (injectable seams)
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader() -> dict:
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


def _default_scoring_loader() -> dict:
    from paper_trader.api import universe_scoring as us
    return us.build_universe_scoring()


def _default_price_panel_loader() -> Optional[dict]:
    # Stage 22.1 — the OPERATIONAL panel (owned current window composed over the frozen
    # research artifact). Reading the research artifact alone left every holding it never
    # covered with no return_20d / volatility_60d / dollar volume, which is what made
    # required_data_complete False for 10 of 25 real holdings on 2026-08-14.
    from paper_trader.api import price_panel as pp
    return pp.load_operational_price_panel()


def _prior_ranking_from_artifact(*, active_book_id, eligible_market_date, hoc_dir=None):
    """Prior eligible-session per-ticker rank from the previous eligible date's
    persisted artifact (PIT-honest: a real prior snapshot, never today's snapshot).

    Returns ``(ranking|None, state, reason, source_date)``.
    """
    from paper_trader.engine import market_session as ms
    from datetime import date as _date
    if not eligible_market_date:
        return None, "UNAVAILABLE", "No eligible market date to anchor a prior lookup.", None
    try:
        d = _date.fromisoformat(eligible_market_date)
        prior_date = ms.previous_trading_day(d).isoformat()
    except (ValueError, TypeError):
        return None, "UNAVAILABLE", "Eligible market date is not a valid ISO date.", None
    art = load_latest_artifact(active_book_id=active_book_id,
                               eligible_market_date=prior_date, hoc_dir=hoc_dir)
    if not art:
        return (None, "UNAVAILABLE",
                "No persisted opportunity-cost artifact exists for the previous eligible "
                "session (%s); previous rank is unavailable." % prior_date, prior_date)
    snapshot = ((art.get("assessment") or {}).get("diagnostics") or {}).get("rank_snapshot")
    if not snapshot:
        return (None, "UNAVAILABLE",
                "The previous session artifact (%s) carries no rank snapshot." % prior_date,
                prior_date)
    return dict(snapshot), "AVAILABLE", "", prior_date


# --------------------------------------------------------------------------- #
# Run (build contract + kernel). Does NOT persist.
# --------------------------------------------------------------------------- #
def run_assessment(*, input_contract: Optional[dict] = None,
                   portfolio_state: Optional[dict] = None,
                   scoring: Optional[dict] = None,
                   price_panel: Optional[dict] = None,
                   previous_ranking: Optional[dict] = None,
                   previous_ranking_state: Optional[str] = None,
                   previous_ranking_reason: Optional[str] = None,
                   prior_signal: Optional[dict] = None,
                   policy: Optional[dict] = None,
                   hoc_dir=None,
                   portfolio_state_loader: Optional[Callable] = None,
                   scoring_loader: Optional[Callable] = None,
                   price_panel_loader: Optional[Callable] = None) -> dict:
    """Build the input contract (unless supplied) and run the pure kernel.

    Returns ``{"input_contract": ..., "assessment": <kernel result>}``. Read-only:
    persists nothing.
    """
    pol = policy or resolve_policy()
    if input_contract is None:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
        sc = scoring if scoring is not None else (
            (scoring_loader or _default_scoring_loader)())
        pp_obj = price_panel
        if pp_obj is None and price_panel is None:
            try:
                pp_obj = (price_panel_loader or _default_price_panel_loader)()
            except Exception:  # noqa: BLE001
                pp_obj = None
        if previous_ranking is None and previous_ranking_state is None:
            ab = (ps.get("active_book") or {}).get("book_id")
            eligible = (ps.get("dates") or {}).get("eligible_market_date")
            previous_ranking, previous_ranking_state, previous_ranking_reason, prior_src = (
                _prior_ranking_from_artifact(active_book_id=ab,
                                             eligible_market_date=eligible, hoc_dir=hoc_dir))
        else:
            prior_src = None
        input_contract = build_input_contract(
            portfolio_state=ps, scoring=sc, price_panel=pp_obj,
            previous_ranking=previous_ranking, previous_ranking_state=previous_ranking_state,
            previous_ranking_reason=previous_ranking_reason,
            previous_ranking_source_date=prior_src, prior_signal=prior_signal, policy=pol)
    result = kernel.build_assessment(input_contract=input_contract, policy=pol)
    return {"input_contract": input_contract, "assessment": result}


# --------------------------------------------------------------------------- #
# Persist (immutable artifact) — Workstream H / Release 54.3 version chain
# --------------------------------------------------------------------------- #
#: Release 54.3 — the persistence outcomes, named once and spelled exactly as the
#: portfolio-reassessment owner spells them (R54.2). ``CREATED_ASSESSMENT_VERSION``
#: is the new one: the SAME economic portfolio, assessed again from materially
#: different evidence.
PERSIST_CREATED = "CREATED"
PERSIST_REUSED = "REUSED_EXISTING"
PERSIST_ECONOMIC_VERSION = "CREATED_NEW_VERSION"
PERSIST_ASSESSMENT_VERSION = "CREATED_ASSESSMENT_VERSION"
PERSIST_CONFLICT = "CONFLICT_REJECTED"
PERSIST_INCONSISTENT = "REJECTED_INCONSISTENT_IDENTITY"
PERSIST_NOT_PERSISTED = "NOT_PERSISTED"
PERSIST_STATUS_VOCAB = (PERSIST_CREATED, PERSIST_REUSED, PERSIST_ECONOMIC_VERSION,
                        PERSIST_ASSESSMENT_VERSION, PERSIST_CONFLICT,
                        PERSIST_INCONSISTENT, PERSIST_NOT_PERSISTED)
#: The outcomes that leave an exact, retrievable immutable artifact behind.
PERSIST_SUCCESS_STATUSES = (PERSIST_CREATED, PERSIST_REUSED,
                            PERSIST_ECONOMIC_VERSION, PERSIST_ASSESSMENT_VERSION)


def _read_indexed_artifact(entry: Optional[dict], hoc_dir=None) -> Optional[dict]:
    if not entry:
        return None
    art = _load_json(Path(entry.get("path"))) if entry.get("path") else None
    if art is None and entry.get("artifact_id"):
        art = _load_json(_artifacts_dir(hoc_dir) / ("%s.json" % entry.get("artifact_id")))
    return art if isinstance(art, dict) else None


def _existing_assessment_identity(existing: Optional[dict], hoc_dir=None) -> tuple:
    """``(assessment_evidence_hash, decision_fingerprint)`` of the indexed artifact.

    An index entry written before R54.3 carries neither. Rather than reinterpreting
    such an artifact — or rewriting it — both values are RECOMPUTED from what it
    already persisted: its own identity, its own compacted input contract and its
    own result. A historical artifact therefore becomes comparable without a single
    byte of it changing. What cannot be derived stays ``None``, and ``None`` is
    never treated as a match.
    """
    if not existing:
        return None, None
    ev_hash = existing.get("assessment_evidence_hash")
    fingerprint = existing.get("decision_fingerprint")
    if ev_hash and fingerprint:
        return ev_hash, fingerprint
    art = _read_indexed_artifact(existing, hoc_dir) or {}
    ident = art.get("identity") or {}
    if not ev_hash:
        ev_hash = ident.get("assessment_evidence_hash")
    if not ev_hash:
        ic = art.get("input_contract") or {}
        if ic or ident:
            merged = {**ident, **{k: v for k, v in ic.items() if v is not None}}
            ev_hash = assessment_evidence_hash(assessment_evidence_identity(
                input_contract=merged, result=art.get("assessment")))
    if not fingerprint:
        fingerprint = (ident.get("decision_fingerprint")
                       or decision_fingerprint(art.get("assessment")))
    return ev_hash, fingerprint


def _session_identity_conflicts(*, identity: dict, input_contract: dict,
                                result: dict) -> list[str]:
    """Point-in-time self-consistency (R54.3 Phase C case 5). An artifact whose own
    parts disagree about WHICH session or WHICH book it describes is impossible
    evidence and is never written — versioning relaxes no point-in-time rule."""
    ic = input_contract or {}
    res = result or {}
    elig = identity.get("eligible_market_date")
    book = identity.get("active_book_id")
    out: list[str] = []

    def _clash(label, a, b):
        if a is not None and b is not None and a != b:
            out.append("%s (%s != %s)" % (label, a, b))

    _clash("IDENTITY_VS_RESULT_SESSION", elig, res.get("eligible_market_date"))
    _clash("IDENTITY_VS_CONTRACT_SESSION", elig, ic.get("eligible_market_date"))
    _clash("IDENTITY_VS_RESULT_BOOK", book, res.get("active_book_id"))
    _clash("IDENTITY_VS_CONTRACT_BOOK", book, ic.get("active_book_id"))
    return out


#: Release 55.2.2 — the identity fields an index entry can supply on its own when
#: the artifact document itself is unreadable. Deliberately the subset the index
#: has always written; nothing here is derived, defaulted or invented.
_INDEXED_IDENTITY_FIELDS = (
    "eligible_market_date", "active_book_id", "portfolio_state_hash",
    "economic_state_hash", "universe_scoring_hash", "decision_policy_version",
    "assessment_hash", "assessment_evidence_hash", "decision_fingerprint")


def _stored_artifact_identity(existing: Optional[dict], hoc_dir=None):
    """R55.2.2 — the identity of the artifact the store ALREADY HOLDS.

    A REUSE outcome means the caller's freshly computed document was NOT written:
    the durable evidence is the artifact that was already there. Returning the
    recomputation's identity instead paired the EXISTING ``artifact_id`` with a
    hash that artifact does not carry, and every downstream consumer of
    :func:`artifact_binding` inherited that mismatch — which is precisely what
    the R54.3 exact-artifact governance check then refused. The document is
    authoritative; the index entry is the fallback for a file that cannot be
    read. ``None`` when neither can answer, and ``None`` is never a match.
    """
    if not existing:
        return None
    art = _read_indexed_artifact(existing, hoc_dir) or {}
    ident = art.get("identity")
    if isinstance(ident, dict) and ident.get("assessment_hash"):
        return dict(ident)
    if existing.get("assessment_hash"):
        return {k: existing.get(k) for k in _INDEXED_IDENTITY_FIELDS
                if existing.get(k) is not None}
    return None


def _unique_artifact_id(aid: str, identity: dict, hoc_dir=None) -> str:
    """Never let a new VERSION land on an existing artifact's path.

    ``artifact_id_for`` embeds ``assessment_hash``, so a collision means the two
    versions produced an identical kernel result — but if their identities differ
    the older file must still not be rewritten. Immutability is enforced here
    rather than assumed from the id scheme, and the suffix is DETERMINISTIC (the
    evidence hash), never a clock or a random token.
    """
    path = _artifacts_dir(hoc_dir) / ("%s.json" % aid)
    if not path.exists():
        return aid
    prior = _load_json(path) or {}
    if (prior.get("identity") or {}) == identity:
        return aid
    return "%s_%s" % (aid, (identity.get("assessment_evidence_hash") or "v")[:8])


def _reuse_outcome(existing: dict, identity: dict, hoc_dir=None) -> dict:
    """THE reuse result. One spelling for both reuse paths (R55.2.2).

    ``identity`` describes the document that was NOT written; the outcome
    therefore reports the identity of the artifact that IS held, and keeps the
    recomputation visible beside it rather than discarding it silently. When the
    two ``assessment_hash`` values differ — the documented Stage-21 case where a
    document-wide hash embeds its own output while the ECONOMIC state, the
    ASSESSMENT EVIDENCE and the CONCLUSION are all unchanged — the difference is
    named, so a consumer can see that a re-derivation happened and that the
    store's version is the one every binding must carry.
    """
    stored = _stored_artifact_identity(existing, hoc_dir) or dict(identity)
    recomputed = (identity or {}).get("assessment_hash")
    return {"status": PERSIST_REUSED, "artifact_id": existing.get("artifact_id"),
            "path": existing.get("path"), "persisted": True, "reused": True,
            "conflict": False, "economic_state_changed": False,
            "assessment_evidence_changed": False,
            "identity": stored,
            "recomputed_assessment_hash": recomputed,
            "recomputed_identity": dict(identity or {}),
            "reused_recomputed_document": bool(
                recomputed and stored.get("assessment_hash")
                and recomputed != stored.get("assessment_hash"))}


def persist_assessment(*, result: dict, input_contract: dict, hoc_dir=None,
                       now: Optional[datetime] = None,
                       produced_by: Any = None, drc_run_id: Any = None) -> dict:
    """Persist a completed production assessment as an immutable artifact.

    Release 54.3 — FIVE outcomes, decided on the three independent identity axes
    (the ECONOMIC portfolio, the ASSESSMENT EVIDENCE about it, and the CONCLUSION):

      1. same economic state + same evidence + same conclusion
         -> ``REUSED_EXISTING``. Idempotent: no second artifact. Re-running a
            cycle from unchanged evidence is not a new assessment.
            Release 55.2.2 — the outcome's ``identity`` is then the STORED
            artifact's, never the discarded recomputation's: reuse means the
            caller's document was not written, so binding its hash to the
            existing ``artifact_id`` would name an assessment the store does not
            hold. ``recomputed_assessment_hash`` keeps the re-derivation visible.
      2. same economic state + materially DIFFERENT assessment evidence
         -> ``CREATED_ASSESSMENT_VERSION``. A NEW immutable version is APPENDED.
            The portfolio being economically unchanged does not mean the
            opportunity-cost assessment is unchanged, and refusing this is what
            stranded continuous intraday management on the session's first
            artifact and left every later reassessment binding an unretrievable
            hash.
      3. the ECONOMIC state itself changed (holdings / cash / NAV / corporate
         actions) -> ``CREATED_NEW_VERSION``.
      4. same economic state + same evidence + a DIFFERENT conclusion
         -> ``CONFLICT_REJECTED``. Identical evidence yielding a different answer
            is not a new assessment, it is a determinism failure; the immutable
            artifact is never overwritten and the caller must resolve it.
      5. the artifact's own parts disagree about the session or the book
         -> ``REJECTED_INCONSISTENT_IDENTITY``. Impossible evidence, never written.

    Only production READY / DEGRADED assessments are persisted; BLOCKED /
    NO_ACTIVE_BOOK are not (nothing durable to record). NO artifact is EVER
    rewritten, in any outcome.

    Release 29.5 — ``produced_by`` / ``drc_run_id`` record WHO produced this artifact
    (see ``build_provenance``). They are written on CREATION only: reuse returns the
    existing artifact untouched, because an immutable artifact does not acquire a new
    claim by being read again. That is deliberate — when the governed cycle adopts an
    existing live artifact, its proof of completion is its own manifest binding, never
    a retroactive stamp on evidence it did not produce.
    """
    state = result.get("assessment_state")
    if state not in (STATE_READY, STATE_DEGRADED):
        return {"status": PERSIST_NOT_PERSISTED,
                "reason": "STATE_%s_NOT_PERSISTABLE" % state,
                "artifact_id": None, "persisted": False, "reused": False,
                "conflict": False, "economic_state_changed": False,
                "assessment_evidence_changed": False}

    identity = artifact_identity(input_contract=input_contract, result=result)
    conflicts = _session_identity_conflicts(identity=identity,
                                            input_contract=input_contract,
                                            result=result)
    if conflicts:
        return {"status": PERSIST_INCONSISTENT, "artifact_id": None,
                "persisted": False, "reused": False, "conflict": True,
                "economic_state_changed": False,
                "assessment_evidence_changed": False,
                "identity_conflicts": conflicts,
                "reason": "The assessment's own parts disagree about the session or "
                          "the book it describes: %s. Impossible evidence is never "
                          "persisted." % "; ".join(conflicts),
                "identity": identity}

    aid = artifact_id_for(identity)
    key = _index_key(identity["active_book_id"], identity["eligible_market_date"])
    index = _load_json(_index_path(hoc_dir)) or {}
    if not isinstance(index, dict):
        index = {}
    existing = index.get(key)

    # EXACT idempotency first, and independently of every other axis: the same
    # kernel result is the same assessment, whatever a legacy index entry can or
    # cannot tell us about the evidence behind it.
    if existing and existing.get("assessment_hash") == identity["assessment_hash"]:
        return _reuse_outcome(existing, identity, hoc_dir)

    new_econ = identity.get("economic_state_hash")
    prior_econ = existing.get("economic_state_hash") if existing else None
    economic_state_changed = bool(existing and new_econ and prior_econ
                                  and new_econ != prior_econ)

    prior_evidence_hash, prior_fingerprint = _existing_assessment_identity(
        existing, hoc_dir)
    new_evidence_hash = identity.get("assessment_evidence_hash")
    new_fingerprint = identity.get("decision_fingerprint")
    assessment_evidence_changed = bool(
        existing and not economic_state_changed
        and new_evidence_hash and prior_evidence_hash
        and new_evidence_hash != prior_evidence_hash)

    if existing and not economic_state_changed and not assessment_evidence_changed:
        # Same portfolio, same evidence. Either it is the same conclusion — a
        # re-run whose only difference is the document-wide state hash, which
        # embeds this assessment's own output (the Stage-21 trap) — or identical
        # evidence produced a different answer, which is a determinism failure and
        # never a version.
        if (new_fingerprint and prior_fingerprint
                and new_fingerprint == prior_fingerprint):
            return _reuse_outcome(existing, identity, hoc_dir)
        return {"status": PERSIST_CONFLICT, "artifact_id": aid,
                "existing_artifact_id": existing.get("artifact_id"),
                "existing_assessment_hash": existing.get("assessment_hash"),
                "persisted": False, "reused": False, "conflict": True,
                "economic_state_changed": False,
                "assessment_evidence_changed": False,
                "reason": "An immutable opportunity-cost artifact already exists for "
                          "this book + eligible date, bound to the SAME economic state "
                          "and the SAME assessment evidence, but recording a different "
                          "conclusion; it was not overwritten.",
                "identity": identity}

    if existing:
        aid = _unique_artifact_id(aid, identity, hoc_dir)
    payload = {
        "artifact_id": aid,
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(now),
        PROVENANCE_KEY: build_provenance(producer_owner=produced_by,
                                         drc_run_id=drc_run_id),
        "identity": identity,
        "input_contract": _compact_input_contract(input_contract, result),
        "assessment": result,
    }
    path = _artifacts_dir(hoc_dir) / ("%s.json" % aid)
    _atomic_write_json(path, payload)
    entry = {"artifact_id": aid, "path": str(path),
             "assessment_hash": identity["assessment_hash"],
             "portfolio_state_hash": identity["portfolio_state_hash"],
             "economic_state_hash": identity.get("economic_state_hash"),
             # Release 54.3 — indexed so the NEXT persist decides the version
             # question without reopening every artifact of the session.
             "assessment_evidence_hash": identity.get("assessment_evidence_hash"),
             "decision_fingerprint": identity.get("decision_fingerprint"),
             "universe_scoring_hash": identity["universe_scoring_hash"],
             "decision_policy_version": identity["decision_policy_version"],
             "eligible_market_date": identity["eligible_market_date"],
             "active_book_id": identity["active_book_id"],
             "supersedes_artifact_id": ((existing or {}).get("artifact_id")
                                        if existing else None),
             "generated_at": payload["generated_at"]}
    # Update the index AFTER the artifact write (interrupted-write recoverable: an
    # unindexed artifact is simply re-created on the next identical run). The newest
    # version sits at the top level — backward compatible for every existing reader —
    # and the full append-only chain is preserved under ``versions`` so a superseded
    # artifact stays discoverable and is never rewritten.
    prior_versions = list((existing or {}).get("versions") or [])
    if existing and not prior_versions:
        prior_versions = [{k: v for k, v in existing.items() if k != "versions"}]
    index[key] = {**entry, "versions": prior_versions + [entry]}
    _atomic_write_json(_index_path(hoc_dir), index)
    if economic_state_changed:
        status = PERSIST_ECONOMIC_VERSION
    elif assessment_evidence_changed:
        status = PERSIST_ASSESSMENT_VERSION
    else:
        status = PERSIST_CREATED
    return {"status": status, "artifact_id": aid, "path": str(path),
            "persisted": True, "reused": False, "conflict": False,
            "economic_state_changed": economic_state_changed,
            "assessment_evidence_changed": assessment_evidence_changed,
            "version_index": len(index[key]["versions"]),
            "prior_assessment_evidence_hash": prior_evidence_hash,
            "superseded_artifact_id": ((existing or {}).get("artifact_id")
                                       if existing else None),
            "identity": identity}


def load_latest_artifact(*, active_book_id: Optional[str],
                         eligible_market_date: Optional[str], hoc_dir=None,
                         economic_state_hash: Optional[str] = None) -> Optional[dict]:
    """Load the persisted artifact for an exact (active book, eligible date), or None.

    Release 54.3: when ``economic_state_hash`` is supplied the lookup resolves the
    NEWEST version bound to exactly that economic state, so a session that produced
    more than one version (because the portfolio genuinely changed mid-session)
    resolves the CURRENT-state assessment rather than the first one written that
    day. With no hint, the newest version wins. Superseded versions stay on disk
    and stay readable; nothing is rewritten.
    """
    index = _load_json(_index_path(hoc_dir)) or {}
    if not isinstance(index, dict):
        return None
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return None
    if economic_state_hash:
        versions = list(entry.get("versions") or [entry])
        for v in reversed(versions):
            if v.get("economic_state_hash") == economic_state_hash:
                art = _read_indexed_artifact(v, hoc_dir)
                if art is not None:
                    return art
    return _read_indexed_artifact(entry, hoc_dir)


def load_artifact_versions(*, active_book_id: Optional[str],
                           eligible_market_date: Optional[str],
                           hoc_dir=None) -> list[dict]:
    """The append-only version chain for ONE (book, session), oldest first.

    Release 54.3 — a session may now hold more than one immutable opportunity-cost
    assessment. The chain is the audit surface: every version that was ever
    authoritative, in the order it became so. The LAST entry is the current one.
    """
    index = _load_json(_index_path(hoc_dir)) or {}
    if not isinstance(index, dict):
        return []
    entry = index.get(_index_key(active_book_id, eligible_market_date))
    if not entry:
        return []
    versions = list(entry.get("versions") or [])
    return versions or [{k: v for k, v in entry.items() if k != "versions"}]


def load_artifact_by_id(*, artifact_id: str, active_book_id: Optional[str] = None,
                        eligible_market_date: Optional[str] = None,
                        hoc_dir=None) -> Optional[dict]:
    """Load ONE historical artifact by its exact id.

    Release 54.3 — an explicit-id read is immutable by construction: it resolves
    the artifact FILE itself and never the index pointer, so a caller holding an
    older ``hoc_artifact_id`` keeps receiving exactly the assessment it referenced,
    however many later versions the session acquired. This is the read the
    governance gate's retrievability proof depends on.
    """
    if not artifact_id:
        return None
    art = _load_json(_artifacts_dir(hoc_dir) / ("%s.json" % artifact_id))
    if isinstance(art, dict):
        return art
    for v in load_artifact_versions(active_book_id=active_book_id,
                                    eligible_market_date=eligible_market_date,
                                    hoc_dir=hoc_dir):
        if v.get("artifact_id") == artifact_id:
            return _read_indexed_artifact(v, hoc_dir)
    return None


# --------------------------------------------------------------------------- #
# Release 54.3 — THE DOWNSTREAM BINDING CONTRACT
#
# Everything that claims to depend on an opportunity-cost assessment — a portfolio
# reassessment, a reallocation proposal, a governed portfolio decision — must bind
# the EXACT persisted version, not "the same session" and not "the latest". This
# owner publishes that binding once, and it is copied verbatim downstream.
# --------------------------------------------------------------------------- #
BINDING_SCHEMA_VERSION = "holding_opportunity_cost.binding.v1"


def artifact_binding(persistence: Optional[dict] = None,
                     artifact: Optional[dict] = None) -> dict:
    """The exact-version binding a downstream consumer must record.

    Composed from THIS owner's own persistence outcome (preferred) or from a
    persisted artifact document. ``hoc_persisted`` is the load-bearing field: it
    is True only when the write left an exact, retrievable immutable artifact
    behind. A refused write stays visible AS a refused write — this function
    never repairs, defaults or infers one.
    """
    p = persistence or {}
    art = artifact or {}
    ident = (art.get("identity") or {}) if art else ((p.get("identity") or {}))
    status = p.get("status") if p else (PERSIST_REUSED if art else None)
    return {
        "schema_version": BINDING_SCHEMA_VERSION,
        "hoc_owner": COMPOSITION_OWNER,
        "hoc_artifact_id": (p.get("artifact_id") or art.get("artifact_id")),
        "hoc_assessment_hash": ident.get("assessment_hash"),
        "hoc_assessment_evidence_hash": ident.get("assessment_evidence_hash"),
        "hoc_decision_fingerprint": ident.get("decision_fingerprint"),
        "hoc_economic_state_hash": ident.get("economic_state_hash"),
        "hoc_eligible_market_date": ident.get("eligible_market_date"),
        "hoc_active_book_id": ident.get("active_book_id"),
        "hoc_persistence_status": status,
        "hoc_persisted": bool(status in PERSIST_SUCCESS_STATUSES),
        "hoc_assessment_evidence_changed": p.get("assessment_evidence_changed"),
        "hoc_economic_state_changed": p.get("economic_state_changed"),
        "hoc_supersedes_artifact_id": p.get("superseded_artifact_id"),
        "hoc_version_index": p.get("version_index"),
        # R55.2.2 — a reuse whose re-derived document hashed differently while
        # the economic state, the evidence and the conclusion were unchanged.
        # The binding above names the STORED version; this says the caller also
        # computed one, so nothing about the reuse is hidden from a consumer.
        "hoc_reused_recomputed_document": p.get("reused_recomputed_document"),
        "hoc_recomputed_assessment_hash": p.get("recomputed_assessment_hash"),
    }


def resolve_binding(*, binding: Optional[dict] = None,
                    persistence: Optional[dict] = None,
                    active_book_id: Optional[str] = None,
                    eligible_market_date: Optional[str] = None,
                    hoc_dir=None) -> dict:
    """PROVE that a claimed binding names an artifact that actually exists on disk.

    This is the io half of the contract, and it lives here because this module is
    the ONE owner of the artifact store. It reads the exact artifact by id and
    reports what it found; it decides no governance and repairs nothing. The
    governance gate stays pure and consumes ``hoc_artifact_retrievable`` /
    ``hoc_artifact_identity_matches`` as facts.
    """
    b = dict(binding or artifact_binding(persistence))
    aid = b.get("hoc_artifact_id")
    art = None
    if aid:
        art = load_artifact_by_id(artifact_id=aid, active_book_id=active_book_id,
                                  eligible_market_date=eligible_market_date,
                                  hoc_dir=hoc_dir)
    ident = (art or {}).get("identity") or {}
    found = art is not None
    claimed_hash = b.get("hoc_assessment_hash")
    stored_hash = ident.get("assessment_hash")
    hash_ok = (found and claimed_hash is not None and stored_hash is not None
               and str(claimed_hash) == str(stored_hash))
    book_ok = _eq_or_unknown(b.get("hoc_active_book_id") or active_book_id,
                             ident.get("active_book_id"))
    session_ok = _eq_or_unknown(
        b.get("hoc_eligible_market_date") or eligible_market_date,
        ident.get("eligible_market_date"))
    claimed_ev = b.get("hoc_assessment_evidence_hash")
    stored_ev = ident.get("assessment_evidence_hash")
    evidence_ok = (found and (claimed_ev is None or stored_ev is None
                              or str(claimed_ev) == str(stored_ev)))
    b.update({
        "hoc_artifact_retrievable": bool(found),
        "hoc_artifact_identity_matches": bool(hash_ok and book_ok and session_ok),
        "hoc_artifact_evidence_matches": bool(evidence_ok),
        "hoc_stored_assessment_hash": stored_hash,
        "hoc_stored_assessment_evidence_hash": stored_ev,
        "hoc_binding_resolved_by": COMPOSITION_OWNER,
        "hoc_binding_detail": (
            "no artifact id was claimed" if not aid else
            "artifact %s is not retrievable from the immutable store" % aid
            if not found else
            "artifact %s retrieved; assessment hash %s" % (aid, "matches"
                                                           if hash_ok else "MISMATCH")),
    })
    return b


def _eq_or_unknown(a: Any, b: Any) -> bool:
    """True when the two agree OR when either side is unknown (not comparable)."""
    if a is None or b is None:
        return True
    return str(a) == str(b)


def run_and_persist(*, portfolio_state: Optional[dict] = None,
                    scoring: Optional[dict] = None, price_panel: Optional[dict] = None,
                    previous_ranking: Optional[dict] = None,
                    previous_ranking_state: Optional[str] = None,
                    prior_signal: Optional[dict] = None, policy: Optional[dict] = None,
                    hoc_dir=None, now: Optional[datetime] = None,
                    portfolio_state_loader: Optional[Callable] = None,
                    scoring_loader: Optional[Callable] = None,
                    price_panel_loader: Optional[Callable] = None,
                    produced_by: Any = None, drc_run_id: Any = None) -> dict:
    """The composition entry: build -> kernel -> persist (idempotent).

    Called by BOTH canonical producers (``api.daily_research_cycle`` and
    ``api.event_signal_refresh``); ``produced_by`` / ``drc_run_id`` is how they identify
    themselves so the persisted artifact states what it is.

    Release 54.3 — the return additionally carries ``binding``: the EXACT persisted
    version every downstream consumer must record. Composing it here, at the one
    point where the assessment and its persistence outcome are both in hand, is
    what stops a caller inventing its own idea of "which HOC this was".
    """
    run = run_assessment(
        portfolio_state=portfolio_state, scoring=scoring, price_panel=price_panel,
        previous_ranking=previous_ranking, previous_ranking_state=previous_ranking_state,
        prior_signal=prior_signal, policy=policy, hoc_dir=hoc_dir,
        portfolio_state_loader=portfolio_state_loader, scoring_loader=scoring_loader,
        price_panel_loader=price_panel_loader)
    persist = persist_assessment(result=run["assessment"],
                                 input_contract=run["input_contract"], hoc_dir=hoc_dir,
                                 now=now, produced_by=produced_by, drc_run_id=drc_run_id)
    return {"input_contract": run["input_contract"], "assessment": run["assessment"],
            "persistence": persist, "binding": artifact_binding(persist)}


# --------------------------------------------------------------------------- #
# Read contract (Workstream J) — GET /v1/operations/holding-opportunity-cost
# --------------------------------------------------------------------------- #
def _active_book_block(ps: dict) -> dict:
    ab = (ps or {}).get("active_book") or {}
    return {
        "book_id": ab.get("book_id"),
        "book_label": ab.get("book_label"),
        "status": ab.get("status"),
        "initialized": ab.get("initialized"),
        "holdings_count": ab.get("holdings_count"),
        "is_dormant_legacy_book": ab.get("is_dormant_legacy_book", False),
        "owner_module": "api.operational_book",
    }


def _corporate_action_staleness(*, artifact: Optional[dict],
                                book_id: Optional[str],
                                portfolio_state: Optional[dict] = None,
                                actions_dir=None) -> dict:
    """Stage 19.1 — is a persisted assessment still valid against the CURRENT
    corporate-action registry? Pure delegation to ``api.corporate_actions``; this module
    owns no split arithmetic and no registry logic."""
    from paper_trader.api import corporate_actions as ca
    art = artifact or {}
    bound = ((art.get("identity") or {}).get("corporate_actions_hash")
             or (art.get("input_contract") or {}).get("corporate_actions_hash"))
    current = ((portfolio_state or {}).get("corporate_actions") or {}).get(
        "registry_fingerprint")
    try:
        if current is not None:
            actions = ((portfolio_state or {}).get("corporate_actions") or {}).get(
                "actions") or []
            return ca.staleness_vs_registry(
                bound, current={"fingerprint": current, "n_registered": len(actions),
                                "actions": actions})
        # Stage 22: ``actions_dir`` is an explicit seam so a HERMETIC caller resolves the
        # registry from ITS OWN root. Without it this read reached the operator's real
        # corporate-action registry from inside a synthetic scenario, which silently made
        # every fixture assessment "stale" against live evidence.
        return ca.staleness_vs_registry(bound, book_id=book_id, actions_dir=actions_dir)
    except Exception:  # noqa: BLE001 - a read contract must never crash
        return {"stale": False, "reason": None}


# --------------------------------------------------------------------------- #
# Stage 22 (Workstream C) — MACHINE-READABLE data-gap classification.
#
# This module owns the assessment ARTIFACT; the taxonomy itself lives in the pure
# kernel ``engine.data_gap_taxonomy`` (one classifier, no fork). Classification is a
# READ-layer description of what the immutable artifact already recorded: it runs no
# engine, changes no recommendation, writes nothing and — critically — leaves the
# artifact's ``assessment_hash`` untouched, so adding this contract can never turn an
# existing production artifact into a conflicting one.
# --------------------------------------------------------------------------- #
def classify_data_gaps(*, assessment: Optional[dict],
                       artifact: Optional[dict] = None) -> dict:
    """Classify every data gap in one assessment (delegates to the pure taxonomy)."""
    a = assessment or {}
    ic = (artifact or {}).get("input_contract") or {}
    try:
        return gaptax.classify_assessment_gaps(
            assessment=a,
            eligible_market_date=a.get("eligible_market_date")
            or ic.get("eligible_market_date"),
            previous_eligible_market_date=ic.get("previous_ranking_source_date"))
    except Exception:  # noqa: BLE001 — a read contract must never crash
        return gaptax.summarize([], eligible_market_date=a.get("eligible_market_date"))


def _read_payload(*, state: str, generated_at: str, eligible: Optional[str],
                  active_book: dict, artifact: Optional[dict], message: str,
                  policy: dict, assessment: Optional[dict],
                  input_contract: Optional[dict],
                  staleness: Optional[dict] = None) -> dict:
    a = assessment or {}
    art_meta = None
    if artifact:
        art_meta = {"artifact_id": artifact.get("artifact_id"),
                    "generated_at": artifact.get("generated_at"),
                    "identity": artifact.get("identity"),
                    "immutable": True,
                    "root_env": HOC_DIR_ENV}
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": kernel.CALCULATION_OWNER,
        "generated_at": generated_at,
        "state": state,
        "state_vocabulary": list(READ_STATE_VOCAB),
        "message": message,
        "eligible_market_date": eligible,
        "active_book": active_book,
        "input_contract": input_contract,
        "policy": a.get("policy") or policy,
        "portfolio_summary": a.get("portfolio_summary") or {},
        "recommendation_counts": a.get("recommendation_counts")
        or {k: 0 for k in RECOMMENDATION_VOCAB},
        "recommendation_vocabulary": list(RECOMMENDATION_VOCAB),
        "holding_reviews": a.get("holding_reviews") or [],
        "addition_candidates": a.get("addition_candidates") or [],
        "diagnostics": a.get("diagnostics") or {},
        "data_quality": a.get("data_quality") or {},
        # Stage 22 (Workstream C): every gap as a machine-readable record with its
        # ticker, metric, expected/available as-of dates, owner, blocking severity,
        # effect on the recommendation and safe fallback (or an explicit None).
        "data_gap_taxonomy": classify_data_gaps(assessment=a, artifact=artifact),
        "artifact": art_meta,
        "safety": a.get("safety") or kernel._safety(),
        "provenance": a.get("provenance")
        or {"composition_owner": COMPOSITION_OWNER, "calculation_owner": kernel.CALCULATION_OWNER},
        "assessment_hash": a.get("assessment_hash"),
        "assessment_state": a.get("assessment_state"),
        # Stage 19.1 — an assessment produced BEFORE a registered corporate action was
        # computed against holdings that no longer exist economically. The immutable
        # artifact is preserved and still readable, but it is explicitly NOT current.
        "stale": bool((staleness or {}).get("stale")),
        "staleness": staleness,
        "describes_current_holdings": not bool((staleness or {}).get("stale")),
        "sole_execution_path": "POST /v1/operations/daily-research-cycle/run",
    }


def load_holding_opportunity_cost(*, portfolio_state: Optional[dict] = None,
                                  artifact: Optional[dict] = None, hoc_dir=None,
                                  now: Optional[datetime] = None,
                                  portfolio_state_loader: Optional[Callable] = None) -> dict:
    """The read contract for the endpoint. READ-ONLY: it NEVER runs the engine —
    it returns the latest persisted assessment for the current active book + eligible
    date. When no production artifact exists it returns a readable ``NOT_RUN`` payload
    (the sole execution path is the Daily Research Cycle). Always degrade-safe."""
    generated_at = _now_iso(now)
    try:
        ps = portfolio_state if portfolio_state is not None else (
            (portfolio_state_loader or _default_portfolio_state_loader)())
    except Exception as exc:  # noqa: BLE001
        return _read_payload(state=STATE_UNAVAILABLE, generated_at=generated_at, eligible=None,
                             active_book={}, artifact=None,
                             message="Portfolio state is unavailable: %s" % str(exc)[:160],
                             policy=resolve_policy(), assessment=None, input_contract=None)

    active_book = _active_book_block(ps)
    eligible = (ps.get("dates") or {}).get("eligible_market_date")
    book_id = active_book.get("book_id")

    if not book_id:
        return _read_payload(state=STATE_NO_ACTIVE_BOOK, generated_at=generated_at,
                             eligible=eligible, active_book=active_book, artifact=None,
                             message="No active operational book; no assessment.",
                             policy=resolve_policy(), assessment=None, input_contract=None)

    art = artifact if artifact is not None else load_latest_artifact(
        active_book_id=book_id, eligible_market_date=eligible, hoc_dir=hoc_dir)
    if not art:
        return _read_payload(
            state=STATE_NOT_RUN, generated_at=generated_at, eligible=eligible,
            active_book=active_book, artifact=None,
            message=("No Holding Opportunity-Cost assessment has been produced for the "
                     "current active book and eligible session yet. Run the Daily Research "
                     "Cycle (POST /v1/operations/daily-research-cycle/run) to produce one."),
            policy=resolve_policy(), assessment=None, input_contract=None)

    assessment = art.get("assessment") or {}
    staleness = _corporate_action_staleness(artifact=art, book_id=book_id,
                                            portfolio_state=ps)
    msg = ("Latest Holding Opportunity-Cost assessment for the active book / eligible "
           "session.")
    if staleness.get("stale"):
        msg = ("This Holding Opportunity-Cost assessment was produced BEFORE a corporate "
               "action was registered, so it was computed against holdings that no longer "
               "describe the current portfolio. Run the Daily Research Cycle to produce a "
               "fresh assessment against the corrected portfolio state.")
    return _read_payload(
        state=assessment.get("assessment_state") or STATE_READY, generated_at=generated_at,
        eligible=eligible, active_book=active_book, artifact=art, message=msg,
        policy=assessment.get("policy") or resolve_policy(), assessment=assessment,
        input_contract=art.get("input_contract"), staleness=staleness)


# --------------------------------------------------------------------------- #
# Lightweight summary for the Daily Action Gate (Workstream K)
# --------------------------------------------------------------------------- #
def _absent_provenance_fields() -> dict:
    """Provenance keys for a summary with NO artifact. No artifact makes no claim, so it
    is not a governed terminal output and it proves nothing — stated explicitly rather
    than left absent, so a consumer never reads a missing key as an unknown claim."""
    return {
        "opportunity_cost_provenance": None,
        "opportunity_cost_artifact_class": None,
        "opportunity_cost_producer_owner": None,
        "opportunity_cost_claims_drc_terminal": False,
        "opportunity_cost_drc_run_id": None,
        "opportunity_cost_proves_drc_complete": False,
    }


def load_assessment_summary(*, active_book_id: Optional[str] = None,
                            eligible_market_date: Optional[str] = None,
                            artifact: Optional[dict] = None, hoc_dir=None,
                            actions_dir=None) -> dict:
    """A compact, read-only opportunity-cost summary the Daily Action Gate delegates to.

    Slice 6 (Phase 29G) performance repair — this loader is a PURE ARTIFACT READER.
    It reads ONLY the immutable opportunity-cost artifact index/artifact for the exact
    ``(active_book_id, eligible_market_date)`` context that the Daily Action Gate supplies
    from the authoritative operational state it already owns. It NEVER calls
    ``api.portfolio_state.load_portfolio_state``: that edge closed a circular
    recomposition (``portfolio_state`` composes the gate → the gate delegated to this
    summary → this summary loaded ``portfolio_state`` again → …) that recomputed the
    whole owned-data engine dozens of times per request. The read is bounded: one
    artifact-index lookup and at most one artifact read; no engine run, no live
    assessment, no provider / prediction call, no write. ``available=False`` with zeroed
    counts when no production artifact exists (the sole execution path stays the Daily
    Research Cycle)."""
    zero = {k: 0 for k in RECOMMENDATION_VOCAB}
    try:
        art = artifact if artifact is not None else load_latest_artifact(
            active_book_id=active_book_id, eligible_market_date=eligible_market_date,
            hoc_dir=hoc_dir)
    except Exception:  # noqa: BLE001 — a pure artifact read must never crash the gate
        return {"opportunity_cost_available": False, "opportunity_cost_assessment_hash": None,
                "opportunity_cost_recommendation_counts": zero,
                "opportunity_cost_replacement_count": 0, "opportunity_cost_exit_count": 0,
                "opportunity_cost_reduce_count": 0, "opportunity_cost_hold_count": 0,
                "opportunity_cost_add_count": 0, "opportunity_cost_data_gaps": [],
                "opportunity_cost_state": STATE_UNAVAILABLE,
                **_absent_provenance_fields()}
    if not art:
        return {"opportunity_cost_available": False, "opportunity_cost_assessment_hash": None,
                "opportunity_cost_recommendation_counts": zero,
                "opportunity_cost_replacement_count": 0, "opportunity_cost_exit_count": 0,
                "opportunity_cost_reduce_count": 0, "opportunity_cost_hold_count": 0,
                "opportunity_cost_add_count": 0, "opportunity_cost_data_gaps": [],
                "opportunity_cost_state": STATE_NOT_RUN,
                **_absent_provenance_fields()}
    a = art.get("assessment") or {}
    ident = art.get("identity") or {}
    ic = art.get("input_contract") or {}
    counts = a.get("recommendation_counts") or zero
    gaps = (a.get("data_quality") or {}).get("data_gaps") or []
    # Stage 22 (Workstream C): the MACHINE-READABLE gap taxonomy, classified at the
    # READ layer by the pure kernel. The immutable artifact and its assessment_hash are
    # untouched — classification is a description of already-recorded evidence, never a
    # recomputation of it — so an existing artifact can never be invalidated by adding
    # this contract. Consumers read ``blocking`` instead of parsing a string code.
    gap_summary = classify_data_gaps(assessment=a, artifact=art)
    prov = classify_artifact_provenance(art)
    # Stage 19.1: resolved from the corporate-action registry file alone by the ONE owner,
    # so this stays a PURE artifact reader (no engine run, no provider call, and no edge
    # back into the canonical state composer — the acyclic read contract is preserved).
    _stale = _corporate_action_staleness(artifact=art, book_id=active_book_id,
                                         actions_dir=actions_dir)
    return {
        "opportunity_cost_available": True,
        "opportunity_cost_stale": bool(_stale.get("stale")),
        "opportunity_cost_stale_reason": _stale.get("reason"),
        "opportunity_cost_assessment_hash": a.get("assessment_hash"),
        "opportunity_cost_state": a.get("assessment_state"),
        "opportunity_cost_recommendation_counts": counts,
        "opportunity_cost_replacement_count": counts.get("REPLACE", 0),
        "opportunity_cost_exit_count": counts.get("EXIT", 0),
        "opportunity_cost_reduce_count": counts.get("REDUCE", 0),
        "opportunity_cost_hold_count": counts.get("HOLD", 0),
        "opportunity_cost_add_count": counts.get("ADD", 0),
        "opportunity_cost_data_gaps": list(gaps),
        # Stage 22 (Workstream C) — severity is a PROPERTY of the gap, never inferred
        # from its string code by a downstream consumer.
        "opportunity_cost_data_gap_taxonomy": gap_summary,
        "opportunity_cost_blocking_gap_count": gap_summary["blocking_gap_count"],
        "opportunity_cost_non_blocking_gap_count": gap_summary["non_blocking_gap_count"],
        "opportunity_cost_gap_conclusion": gap_summary["conclusion"],
        # Stage 22 (Workstream E) — what this assessment is BOUND to, so a consumer can
        # prove it still describes the portfolio and session it claims to describe.
        "opportunity_cost_bound_eligible_market_date": (
            ident.get("eligible_market_date") or ic.get("eligible_market_date")
            or a.get("eligible_market_date")),
        "opportunity_cost_bound_active_book_id": (ident.get("active_book_id")
                                                  or ic.get("active_book_id")),
        "opportunity_cost_bound_economic_state_hash": ic.get("economic_state_hash"),
        "opportunity_cost_bound_corporate_actions_hash": (
            ident.get("corporate_actions_hash") or ic.get("corporate_actions_hash")),
        # NOTE (Stage 21, Workstream 0E): the whole-document state fingerprint is
        # deliberately NOT exposed as a binding field. It embeds this assessment's own
        # output, so it drifts the moment the artifact is written and would make every
        # fresh assessment look superseded. The ECONOMIC fingerprint above is the one
        # honest answer to "does this still describe the portfolio?".
        "opportunity_cost_artifact_id": art.get("artifact_id"),
        # Release 54.3 — the two same-session versioning identities, read verbatim
        # from the immutable artifact (recomputed for a pre-R54.3 artifact rather
        # than rewritten into it). A consumer binds the EXACT version with these.
        "opportunity_cost_assessment_evidence_hash": (
            ident.get("assessment_evidence_hash")
            or ic.get("assessment_evidence_hash")),
        "opportunity_cost_decision_fingerprint": ident.get("decision_fingerprint"),
        "opportunity_cost_assessment_evidence_identity_version": (
            ident.get("assessment_evidence_identity_version")),
        "opportunity_cost_holding_count": len(a.get("holding_reviews") or []),
        # Release 29.5 (Workstream provenance) — WHAT THIS ARTIFACT CLAIMS TO BE, so a
        # consumer never has to infer governance from the artifact merely existing. The
        # classification is a description of what was recorded; it validates no manifest
        # and asserts no completion (``proves_drc_complete`` is always False).
        "opportunity_cost_provenance": prov,
        "opportunity_cost_artifact_class": prov["artifact_class"],
        "opportunity_cost_producer_owner": prov["producer_owner"],
        "opportunity_cost_claims_drc_terminal": prov["claims_drc_terminal"],
        "opportunity_cost_drc_run_id": prov["drc_run_id"],
        "opportunity_cost_proves_drc_complete": False,
    }
