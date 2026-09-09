r"""api/canonical_forward_accrual.py - THE automatic prospective accrual owner
for canonical forward challenger registrations (Release 62.2).

THE GAP THIS CLOSES
-------------------
Release 62.1 built the canonical registrar
(:mod:`api.forward_challenger_registry`) and Release 62.1.1 built the governed
operator entrypoint that fills it. On 2026-09-09 four ACTIVE R58 freezes were
adopted and the live estate reported, correctly,
``canonical_forward_registration_count = 4``.

And then nothing would ever have happened. A registration NAMES its accrual
owner (``engine.shadow_portfolio_evidence``) and its maturation owner
(``alpha_agent.r52.runtime``), but no code path connected the two: the R46
tournament advances the R46 CONTRACT COHORT, the R56 owner advances a SESSION
COHORT of complete paper portfolios, and neither of them has ever looked at the
registry. A registered challenger is a clock nothing winds.

This module is that missing connection, and it is deliberately the smallest one
that can exist:

    canonical registration           api.forward_challenger_registry  (read)
      -> the frozen decision          the ORIGINATING release's own artifact
      -> prospective emission         engine.shadow_portfolio_evidence (kernel)
      -> forward accrual              engine.shadow_portfolio_evidence (kernel)
      -> maturation                   the instrument's own realised bar calendar
      -> effective independent obs    non-overlapping matured windows
      -> a human evidence gate        never this module

WHAT IT OWNS
------------
Exactly one thing: WHETHER a registered challenger has a legal, unemitted
prospective decision RIGHT NOW, and the durable record of what happened to that
opportunity. It is the ONE place that answers "is an observation due?" for a
canonical registration.

WHAT IT DOES NOT OWN, AND NEVER RE-IMPLEMENTS
---------------------------------------------
* It is NOT a second registry. Every registration is READ from
  :mod:`api.forward_challenger_registry`; this module writes none and amends
  none. Registrations stay immutable and first-write-wins.
* It is NOT a second signal implementation. The weight book is READ from the
  originating release's OWN immutable frozen artifact and bound by its
  ``record_hash`` to the hash the registrar recorded at adoption. A book whose
  hash does not match is refused, never repaired.
* It is NOT a second P&L kernel. Every return, cost and curve comes from
  :mod:`engine.shadow_portfolio_evidence`, the same pure kernel R56 uses.
* It is NOT a second scheduler. :mod:`alpha_agent.r52.runtime` already owns the
  research cadence and calls :func:`advance_canonical_forward_accrual` as one
  more stage. Nothing here holds a timer, a thread or a task definition.
* It is NOT a second calendar. Exchange-session classes resolve through
  :func:`api.forward_challenger_registry.next_exchange_session_after` (the
  authoritative NYSE supplier); every other class observes on the instrument's
  OWN realised bar calendar, read from the panel. No weekday arithmetic and no
  holiday table appears in this file.

NO BACKFILL, EVER - AND WHAT FORFEITURE ACTUALLY MEANS
------------------------------------------------------
A prospective decision for session ``S`` may be emitted only STRICTLY BEFORE
``S`` - the same shape as the frozen R46 entry rule, and the same shape as the
originating freeze's own inception rule ("the position is effective at the NEXT
close"). The position is entered at the close of ``S`` and the kernel scores only
bars dated strictly after it, so the emitter has seen neither the entry mark nor
any outcome.

Emitting once ``S`` has closed would be weaker in a way that matters. The book is
frozen, so nothing about WHAT is emitted could change - but WHETHER to emit could,
and an emitter that has seen the session it is stamping is an emitter that could
skip a bad one. The rule removes that possibility rather than trusting nobody to
use it: once ``S`` has arrived, the opportunity is FORFEITED and recorded as such
- deliberately absent evidence, never data to reconstruct.

Forfeiture is reserved for a REAL missed opportunity. A cadence boundary for
which the originating owner never froze a new decision is NOT a forfeiture and
is never counted as one: there was nothing to emit, because a rebalance is a
DECISION and no decision was taken. That case is reported as
``AWAITING_NEW_GOVERNED_FREEZE``, which is a fact about governance, not a loss.

RESEARCH ONLY. This module promotes no model, activates no sleeve, allocates no
capital, creates no order, no fill, no proposal and no approval, changes no
holding, cash or NAV, runs no daily close and calls no portfolio cycle. It
writes to its own research root and to nothing else.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.engine import shadow_portfolio_evidence as kernel

SCHEMA_VERSION = "canonical_forward_accrual.v1"
COMPOSITION_OWNER = "api.canonical_forward_accrual"
CALCULATION_OWNER = kernel.CALCULATION_OWNER
REGISTRAR_OWNER = "api.forward_challenger_registry"
MATURATION_OWNER = "alpha_agent.r52.runtime"
PHASE = "R62.2"
ROUTE = "/v1/research/alphaagent-outcomes"

STORE_DIR_ENV = "PAPER_TRADER_CANONICAL_FORWARD_ACCRUAL_DIR"
_DEFAULT_STORE_DIR = Path(r"D:\Stock_Prediction_app_data\canonical_forward_accrual")

_EMISSIONS_SUBDIR = "emissions"
_FORFEITURES_SUBDIR = "forfeitures"

#: The read model the ACCRUAL RUN persists, so a GET never has to recompute it.
#: Deriving these counters on a read path means loading the operational price
#: panel, and ``/v1/research/alphaagent-outcomes`` already loads it once through
#: the R56 owner - a second load would put a heavy composition back on a UI read
#: path, which is exactly the defect R62.1.1 spent a workstream removing. The
#: writer computes; the reader reads what was written, and can see when.
PROJECTION_ARTIFACT = "accrual_projection.json"

#: The research scale, deliberately identical to the R56 forward paper
#: portfolio scale so a dollar figure on either board means the same thing.
STARTING_CAPITAL = 100000.0

SAFETY_BADGES = ["RESEARCH ONLY", "PREVIEW ONLY", "MANUAL REVIEW", "NO ORDERS",
                 "ORDERS DISABLED", "AUTOMATION OFF", "NO MODEL PROMOTION"]


# --------------------------------------------------------------------------- #
# 1. THE STATE VOCABULARY
# --------------------------------------------------------------------------- #
#: What happened to ONE (registration, decision session) opportunity. These are
#: the only answers this module can give, and every one of them is a persisted
#: or directly derivable fact - never an estimate.
ACC_NOT_DUE = "NOT_DUE"
ACC_DUE = "DUE"
ACC_EMITTED = "EMITTED"
ACC_FORFEITED = "FORFEITED"
ACC_DATA_BLOCKED = "DATA_BLOCKED"
ACC_INTEGRITY_BLOCKED = "INTEGRITY_BLOCKED"
ACCRUAL_STATES = (ACC_NOT_DUE, ACC_DUE, ACC_EMITTED, ACC_FORFEITED,
                  ACC_DATA_BLOCKED, ACC_INTEGRITY_BLOCKED)

#: Why a cell is NOT_DUE. The distinction between "the session has not printed
#: yet" and "no governed decision exists for it" is the difference between
#: waiting and needing a human, so it is never collapsed into one word.
NOT_DUE_SESSION_NOT_REACHED = "OBSERVATION_SESSION_NOT_REACHED"
NOT_DUE_AWAITING_NEW_FREEZE = "AWAITING_NEW_GOVERNED_FREEZE"
NOT_DUE_REASONS = (NOT_DUE_SESSION_NOT_REACHED, NOT_DUE_AWAITING_NEW_FREEZE)

#: The one reason a real opportunity is lost. Named, so it can never be confused
#: with a boundary that simply had no decision to emit.
FORFEIT_WINDOW_CLOSED = "EMISSION_WINDOW_CLOSED_WHEN_THE_SESSION_BEGAN"

#: Why a book could not be resolved or priced.
BLOCK_SPEC_UNRESOLVABLE = "FROZEN_SPEC_OWNER_NOT_RESOLVABLE"
BLOCK_SPEC_MISSING = "FROZEN_DECISION_ARTIFACT_NOT_FOUND"
BLOCK_NO_PANEL = "PRICE_PANEL_UNAVAILABLE"
BLOCK_NO_SESSIONS = "NO_REALISED_SESSION_FOR_THIS_INSTRUMENT_SCOPE"
BLOCK_COVERAGE = "INSUFFICIENT_PRICED_WEIGHT_AT_THE_LATEST_REALISED_SESSION"
DATA_BLOCK_REASONS = (BLOCK_SPEC_UNRESOLVABLE, BLOCK_SPEC_MISSING,
                      BLOCK_NO_PANEL, BLOCK_NO_SESSIONS, BLOCK_COVERAGE)

#: Why a challenger may never advance at all. Each is a refusal, not a retry.
INTEGRITY_LIFECYCLE_CLOSED = "LIFECYCLE_STATE_IS_NOT_ADOPTABLE"
INTEGRITY_HASH_MISMATCH = "FROZEN_RECORD_HASH_DOES_NOT_MATCH_THE_REGISTRATION"
INTEGRITY_NO_IDENTITY = "REGISTRATION_DOES_NOT_NAME_A_COMPLETE_IDENTITY"
INTEGRITY_REASONS = (INTEGRITY_LIFECYCLE_CLOSED, INTEGRITY_HASH_MISMATCH,
                     INTEGRITY_NO_IDENTITY)

#: Emission outcomes.
EMIT_WROTE = "EMITTED"
EMIT_DUPLICATE = "ALREADY_EMITTED"
EMIT_REFUSED = "REFUSED"
EMISSION_OUTCOMES = (EMIT_WROTE, EMIT_DUPLICATE, EMIT_REFUSED)


# --------------------------------------------------------------------------- #
# 2. THE FROZEN DECISION - read from its originating owner, bound by hash
# --------------------------------------------------------------------------- #
#: ``originating release -> the owner whose immutable artifact holds the frozen
#: decision``. A release absent from this table is reported unresolvable and its
#: challenger is DATA_BLOCKED; a book is NEVER guessed, defaulted or rebuilt
#: from a second implementation of the signal.
FROZEN_DECISION_OWNERS = {
    "R58": "alpha_agent.r58.challengers (challengers/<challenger_id>.json)",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _iso_date(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (TypeError, ValueError):
        return None


def _r58_frozen_decision(challenger_id: str, session: Optional[str]) -> dict:
    """The R58 frozen decision record, read from the R58 owner's own root.

    R58 froze ONE decision per challenger, at its freeze session. Asking for a
    later session therefore legitimately finds nothing: R58 took no decision
    that day, and inventing one is precisely what this estate refuses to do.
    """
    try:
        from paper_trader.alpha_agent import r58 as R58
        path = R58.research_root() / "challengers" / ("%s.json" % challenger_id)
    except Exception as exc:                                # noqa: BLE001
        return {"found": False, "reason": BLOCK_SPEC_UNRESOLVABLE,
                "detail": "the R58 owner could not be read: %s" % str(exc)[:160]}
    try:
        rec = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"found": False, "reason": BLOCK_SPEC_MISSING,
                "detail": "no frozen decision artifact at %s" % path}
    if not isinstance(rec, dict):
        return {"found": False, "reason": BLOCK_SPEC_MISSING,
                "detail": "the frozen decision artifact is not a record"}
    frozen_session = _iso_date(rec.get("eligible_session"))
    if session is not None and frozen_session != session:
        return {"found": False, "reason": NOT_DUE_AWAITING_NEW_FREEZE,
                "frozen_decision_session": frozen_session,
                "detail": ("the originating owner froze no decision for %s; its "
                           "one frozen decision is dated %s, and a rebalance is "
                           "a decision this module may not take on its behalf"
                           % (session, frozen_session))}
    construction = rec.get("construction") or {}
    cost = rec.get("cost_policy") or {}
    return {
        "found": True,
        "source": str(path),
        "frozen_decision_session": frozen_session,
        "record_hash": rec.get("record_hash"),
        "spec_hash": rec.get("spec_hash"),
        "weights_hash": rec.get("weights_hash"),
        "weights": dict(rec.get("weights") or {}),
        "construction": dict(construction),
        "cadence_sessions": construction.get("rebalance_cadence_sessions"),
        "horizon_sessions": construction.get("evaluation_horizon_sessions"),
        "cost_bps_per_side": _f(cost.get("bps_per_side")),
        "benchmark": rec.get("benchmark"),
        "inception_rule": rec.get("inception_rule"),
    }


_RESOLVERS = {"R58": _r58_frozen_decision}


def resolve_frozen_decision(registration: dict,
                            session: Optional[str] = None) -> dict:
    """The frozen decision behind ONE registration, bound by ``record_hash``.

    ``session`` asks for the decision that was frozen FOR that session. Passing
    ``None`` asks for the registration's original freeze. The returned book is
    the originating owner's own; nothing is recomputed here.
    """
    reg = registration or {}
    ident = reg.get("identity") or {}
    challenger_id = reg.get("challenger_id") or ident.get("challenger_id")
    release = str(ident.get("release") or "")
    resolver = _RESOLVERS.get(release)
    if resolver is None:
        return {"resolved": False, "state": ACC_DATA_BLOCKED,
                "reason": BLOCK_SPEC_UNRESOLVABLE,
                "release": release or None,
                "declared_owners": dict(FROZEN_DECISION_OWNERS),
                "detail": ("release %r declares no frozen-decision owner, so its "
                           "book cannot be read; it is NOT reconstructed"
                           % (release or None))}
    found = resolver(str(challenger_id or ""), session)
    if not found.get("found"):
        reason = found.get("reason")
        state = (ACC_NOT_DUE if reason == NOT_DUE_AWAITING_NEW_FREEZE
                 else ACC_DATA_BLOCKED)
        return {"resolved": False, "state": state, "reason": reason,
                "release": release,
                "frozen_decision_session": found.get("frozen_decision_session"),
                "detail": found.get("detail")}

    expected = reg.get("freeze_record_hash") or ident.get("freeze_record_hash")
    actual = found.get("record_hash")
    if expected and actual and str(expected) != str(actual):
        return {"resolved": False, "state": ACC_INTEGRITY_BLOCKED,
                "reason": INTEGRITY_HASH_MISMATCH,
                "expected_record_hash": str(expected),
                "actual_record_hash": str(actual),
                "detail": ("the frozen artifact is not the record this "
                           "registration was made against; it is refused rather "
                           "than repaired")}
    return {"resolved": True, "state": None, "release": release,
            "identity_bound_by": "freeze_record_hash", **found}


def book_for_decision_session(registration: dict, session: str, *,
                              origin: Optional[dict] = None,
                              first_session: Optional[str] = None) -> dict:
    """WHICH frozen decision governs ONE decision session.

    The FIRST decision session is governed by the freeze that was ADOPTED. Its
    book was frozen before registration and uses only information available
    then, which is the whole point: the challenger enters late, on a stale book,
    and is measured strictly afterwards. That is a handicap, never a look-ahead.

    Every LATER cadence boundary is a NEW decision, and this module may not take
    one on the originating owner's behalf. It asks for a decision frozen FOR
    that session and reports ``AWAITING_NEW_GOVERNED_FREEZE`` when none exists.
    """
    if first_session is not None and str(session) == str(first_session):
        return (origin if origin is not None
                else resolve_frozen_decision(registration, None))
    return resolve_frozen_decision(registration, session)


# --------------------------------------------------------------------------- #
# 3. THE OBSERVATION GRID - each asset class on its own calendar
# --------------------------------------------------------------------------- #
def realised_sessions(weights: dict, series: dict,
                      *, as_of: Optional[str] = None) -> list:
    """Every session the panel actually printed for this book's instruments.

    This IS the instrument's own realised bar calendar - the same authority
    :mod:`alpha_agent.r46.clock` and the R56 kernel use - so a rates future, an
    FX pair or a volatility contract is never asked to keep NYSE sessions.
    """
    out: set = set()
    for tk in (weights or {}):
        for d in ((series or {}).get(tk) or {}).get("dates") or []:
            if as_of is None or str(d) <= as_of:
                out.add(str(d))
    return sorted(out)


def latest_realised_session(weights: dict, series: dict,
                            *, as_of: Optional[str] = None) -> Optional[str]:
    sessions = realised_sessions(weights, series, as_of=as_of)
    return sessions[-1] if sessions else None


def priced_share(weights: dict, series: dict, session: str) -> float:
    """The share of the book's invested weight that has a bar AT ``session``.

    The threshold this feeds is the kernel's own
    :data:`engine.shadow_portfolio_evidence.MIN_PRICED_WEIGHT`, so the estate
    has exactly one opinion about when a book is too thinly priced to score.
    """
    invested = 0.0
    priced = 0.0
    for tk, w in (weights or {}).items():
        wf = _f(w) or 0.0
        invested += wf
        s = (series or {}).get(tk) or {}
        dates = s.get("dates") or []
        adj = s.get("adj") or []
        for i, d in enumerate(dates):
            if str(d) == str(session):
                if i < len(adj) and adj[i] is not None:
                    priced += wf
                break
    if invested <= 0:
        return 0.0
    return priced / invested


def first_decision_session(registration: dict) -> Optional[str]:
    """The first legally observable session for ONE registration.

    For an exchange-session class the registrar already resolved it from the
    authoritative calendar and it is READ, never recomputed. For an
    instrument-calendar class the registrar deliberately published no session,
    and the first realised session strictly after registration is the answer -
    supplied by :func:`decision_grid` from the panel.
    """
    clock = (registration or {}).get("observation_clock") or {}
    return clock.get("first_eligible_observation_session")


def decision_grid(registration: dict, *, sessions: list,
                  cadence_sessions: Optional[int] = None) -> list:
    """The decision sessions this registration could ever emit on.

    The first is the registrar's first eligible observation session (or, for an
    instrument-calendar class, the first realised session strictly after
    registration). Subsequent entries are spaced by the frozen construction's
    own rebalance cadence on the SAME realised calendar. The grid never
    contains a session at or before registration.
    """
    reg = registration or {}
    registered_on = _iso_date(reg.get("registration_session"))
    grid_sessions = [s for s in sessions
                     if registered_on is None or s > registered_on]
    first = first_decision_session(reg)
    if first:
        # The registrar's session is the start of the clock whether or not it
        # has printed yet: a challenger waiting for tomorrow is ARMED, and a
        # grid that omitted the session would report it as having no future.
        grid = [s for s in grid_sessions if s >= first]
        if not grid or grid[0] != first:
            grid = [first] + grid
    else:
        grid = list(grid_sessions)
    if not grid:
        return []
    try:
        step = int(cadence_sessions) if cadence_sessions else 0
    except (TypeError, ValueError):
        step = 0
    if step <= 0:
        return [grid[0]] if grid else []
    return grid[::step]


# --------------------------------------------------------------------------- #
# 4. THE STORE - append-only, first-write-wins, immutable rows
# --------------------------------------------------------------------------- #
def store_dir(store_dir_override=None) -> Path:
    if store_dir_override is not None:
        return Path(store_dir_override)
    env = os.environ.get(STORE_DIR_ENV)
    return Path(env) if env else _DEFAULT_STORE_DIR


def _emission_path(identity_hash: str, session: str, store_dir_override=None):
    return (store_dir(store_dir_override) / _EMISSIONS_SUBDIR
            / str(identity_hash) / ("%s.json" % session))


def _forfeiture_path(identity_hash: str, session: str, store_dir_override=None):
    return (store_dir(store_dir_override) / _FORFEITURES_SUBDIR
            / str(identity_hash) / ("%s.json" % session))


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=1, sort_keys=True, default=str)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _load_json(path: Path) -> Optional[dict]:
    try:
        out = json.loads(Path(path).read_text(encoding="utf-8"))
        return out if isinstance(out, dict) else None
    except (OSError, ValueError):
        return None


def _rows_in(subdir: str, identity_hash: str, store_dir_override=None) -> list:
    d = store_dir(store_dir_override) / subdir / str(identity_hash)
    if not d.exists():
        return []
    rows = [_load_json(p) for p in sorted(d.glob("*.json"))]
    return [r for r in rows if r]


def load_emissions(identity_hash: str, store_dir_override=None) -> list:
    """Every prospective emission for ONE registration, oldest first."""
    rows = _rows_in(_EMISSIONS_SUBDIR, identity_hash, store_dir_override)
    rows.sort(key=lambda r: str(r.get("decision_session") or ""))
    return rows


def load_forfeitures(identity_hash: str, store_dir_override=None) -> list:
    """Every recorded forfeiture for ONE registration, oldest first."""
    rows = _rows_in(_FORFEITURES_SUBDIR, identity_hash, store_dir_override)
    rows.sort(key=lambda r: str(r.get("decision_session") or ""))
    return rows


def _safety() -> dict:
    return {
        "research_only": True,
        "paper_only": True,
        "read_only_for_every_operational_store": True,
        "writes_registrations": False,
        "amends_registrations": False,
        "backfilled_forward_evidence": False,
        "rewrote_history": False,
        "rebalanced_after_inception": False,
        "recomputed_a_signal": False,
        "promoted_model": False,
        "automatic_model_promotion_allowed": False,
        "activated_sleeve": False,
        "allocated_capital": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "created_orders": False,
        "created_order_plan": False,
        "created_fills": False,
        "created_proposal": False,
        "approved_anything": False,
        "ran_daily_close": False,
        "called_portfolio_cycle": False,
        "automation_enabled": False,
        "broker_enabled": False,
        "manual_review_remains_mandatory": True,
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# 5. EMISSION - one immutable prospective prediction, or nothing
# --------------------------------------------------------------------------- #
def emit_prospective_prediction(*, registration: dict, decision_session: str,
                                book: dict, store_dir_override=None,
                                now: Optional[str] = None) -> dict:
    """Write ONE immutable prospective prediction. Idempotent; never backdates.

    The record itself is built by the SAME pure kernel the R56 forward paper
    portfolios use, so there is exactly one definition in this estate of what a
    frozen forward book is and how it is costed. First write wins: an emission
    that already exists for ``(registration, decision session)`` is returned
    untouched and is never rewritten.
    """
    reg = registration or {}
    ident = reg.get("identity") or {}
    identity_hash = ident.get("identity_hash")
    if not identity_hash:
        return {"outcome": EMIT_REFUSED, "reason": INTEGRITY_NO_IDENTITY,
                "detail": "the registration does not carry an identity hash"}
    session = _iso_date(decision_session)
    if not session:
        return {"outcome": EMIT_REFUSED, "reason": BLOCK_NO_SESSIONS,
                "detail": "no decision session was supplied"}

    path = _emission_path(identity_hash, session, store_dir_override)
    existing = _load_json(path)
    if existing:
        return {"outcome": EMIT_DUPLICATE, "idempotent": True,
                "decision_session": session, "emission": existing,
                "path": str(path),
                "detail": ("this decision session already carries a prospective "
                           "emission; a second run appends nothing")}

    weights = dict(book.get("weights") or {})
    if not weights:
        return {"outcome": EMIT_REFUSED, "reason": BLOCK_SPEC_MISSING,
                "detail": "the frozen decision holds no weights"}

    ts = now or _now_iso()
    record = kernel.make_inception_record(
        challenger_id=str(reg.get("challenger_id")),
        label="Canonical forward challenger %s" % reg.get("challenger_id"),
        family=str(ident.get("model_family") or "CANONICAL_FORWARD_CHALLENGER"),
        strategy_identity={
            "registrar": REGISTRAR_OWNER,
            "identity_hash": identity_hash,
            "freeze_id": reg.get("freeze_id"),
            "freeze_record_hash": reg.get("freeze_record_hash"),
            "model_spec_hash": reg.get("model_spec_hash"),
            "feature_snapshot_hash": reg.get("feature_snapshot_hash"),
            "asset_class": reg.get("asset_class"),
            "release": ident.get("release"),
            "frozen_decision_session": book.get("frozen_decision_session"),
            "frozen_decision_owner": book.get("source"),
            "weights_hash": book.get("weights_hash"),
            "spec_hash": book.get("spec_hash"),
            "lane": "CANONICAL_PROSPECTIVE_FORWARD",
        },
        weights=weights,
        inception_session=session,
        inception_timestamp=ts,
        starting_capital=STARTING_CAPITAL,
        pit_input_identity={
            "frozen_decision_session": book.get("frozen_decision_session"),
            "freeze_record_hash": book.get("record_hash"),
            "weights_hash": book.get("weights_hash"),
            "spec_hash": book.get("spec_hash"),
            "information_cutoff": book.get("frozen_decision_session"),
            "no_information_after_the_freeze_was_used": True,
        },
        cost_bps_per_side=float(book.get("cost_bps_per_side") or 0.0),
        valuation_source=kernel.VALUATION_PRICE_PANEL,
        benchmark_id=None,
        notes=book.get("inception_rule"))

    row = {
        "schema_version": SCHEMA_VERSION,
        "owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "identity_hash": identity_hash,
        "challenger_id": reg.get("challenger_id"),
        "freeze_id": reg.get("freeze_id"),
        "asset_class": reg.get("asset_class"),
        "decision_session": session,
        "emitted_at": ts,
        "horizon_sessions": book.get("horizon_sessions")
                            or reg.get("horizon_sessions"),
        "cadence_sessions": book.get("cadence_sessions"),
        "observation_calendar_owner": (reg.get("observation_clock") or {})
                                      .get("calendar_owner"),
        "prediction": record,
        "backfilled": False,
        "records_are_immutable": True,
        "first_write_wins": True,
        "forward_evidence_starts_after": session,
        "safety": _safety(),
    }
    _atomic_write_json(path, row)
    return {"outcome": EMIT_WROTE, "idempotent": False,
            "decision_session": session, "emission": row, "path": str(path)}


def record_forfeiture(*, registration: dict, decision_session: str,
                      reason: str, detail: str, store_dir_override=None,
                      now: Optional[str] = None) -> dict:
    """Record ONE deliberately absent observation. Idempotent.

    Recording a forfeiture is the OPPOSITE of backfilling: it writes down that
    the evidence does not exist and never will.
    """
    reg = registration or {}
    identity_hash = (reg.get("identity") or {}).get("identity_hash")
    session = _iso_date(decision_session)
    if not identity_hash or not session:
        return {"outcome": EMIT_REFUSED, "reason": INTEGRITY_NO_IDENTITY}
    path = _forfeiture_path(identity_hash, session, store_dir_override)
    existing = _load_json(path)
    if existing:
        return {"outcome": EMIT_DUPLICATE, "idempotent": True,
                "forfeiture": existing, "path": str(path)}
    row = {
        "schema_version": SCHEMA_VERSION,
        "owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "identity_hash": identity_hash,
        "challenger_id": reg.get("challenger_id"),
        "freeze_id": reg.get("freeze_id"),
        "asset_class": reg.get("asset_class"),
        "decision_session": session,
        "recorded_at": now or _now_iso(),
        "state": ACC_FORFEITED,
        "reason": reason,
        "detail": detail,
        "backfill_refused": True,
        "evidence_is_deliberately_absent": True,
        "may_never_be_reconstructed": True,
        "safety": _safety(),
    }
    _atomic_write_json(path, row)
    return {"outcome": EMIT_WROTE, "idempotent": False, "forfeiture": row,
            "path": str(path)}


# --------------------------------------------------------------------------- #
# 6. MATURATION - on the instrument's own realised calendar
# --------------------------------------------------------------------------- #
def mature_emission(emission: dict, series: dict,
                    *, as_of: Optional[str] = None) -> dict:
    """Accrue and, if the horizon is complete, MATURE one emission.

    Maturity counts ``horizon_sessions`` sessions STRICTLY AFTER the decision
    session on the instrument's own realised bar calendar - the dates it
    actually printed - so holidays and market-specific closures are handled by
    observation rather than by an assumed holiday table.
    """
    row = emission or {}
    record = row.get("prediction") or {}
    try:
        horizon = int(row.get("horizon_sessions") or 0)
    except (TypeError, ValueError):
        horizon = 0
    forward = kernel.forward_sessions(record, series or {}, as_of=as_of)
    matured = bool(horizon) and len(forward) >= horizon
    maturity_session = forward[horizon - 1] if matured else None
    accrued = kernel.accrue_forward(record=record, price_series=series or {},
                                    as_of=maturity_session or as_of)
    return {
        "identity_hash": row.get("identity_hash"),
        "challenger_id": row.get("challenger_id"),
        "decision_session": row.get("decision_session"),
        "horizon_sessions": horizon or None,
        "forward_sessions_observed": len(forward),
        "matured": matured,
        "maturity_session": maturity_session,
        "next_maturity_session": (None if matured else
                                  "after %d more realised session(s)"
                                  % max(0, horizon - len(forward))),
        "sessions_scored": accrued.get("sessions_scored"),
        "net_cumulative_return": accrued.get("net_cumulative_return"),
        "gross_cumulative_return": accrued.get("gross_cumulative_return"),
        "evidence_state": accrued.get("evidence_state"),
        "uncovered_sessions": accrued.get("uncovered_sessions"),
        "accrual_owner": CALCULATION_OWNER,
    }


def effective_independent_observations(matured: list) -> dict:
    """How many matured observations are genuinely INDEPENDENT.

    Overlapping horizons are not independent evidence, and counting them as
    such is how a single lucky month becomes twenty confirmations. Windows are
    walked in chronological order and one is counted only when it begins
    strictly after the previous counted window ended.
    """
    rows = sorted((m for m in matured if m.get("matured")),
                  key=lambda m: str(m.get("decision_session") or ""))
    counted = []
    last_end = None
    for m in rows:
        start = str(m.get("decision_session") or "")
        end = str(m.get("maturity_session") or "")
        if not start or not end:
            continue
        if last_end is None or start >= last_end:
            counted.append({"decision_session": start, "maturity_session": end})
            last_end = end
    return {
        "effective_independent_observations": len(counted),
        "matured_observations": len(rows),
        "independent_windows": counted,
        "rule": ("a matured window counts only when it starts on or after the "
                 "previous counted window's maturity; overlapping horizons are "
                 "one observation observed repeatedly, not several"),
    }


# --------------------------------------------------------------------------- #
# 7. THE ONE ADVANCE - discover, emit, forfeit, mature
# --------------------------------------------------------------------------- #
def _lifecycle_blocked(registration: dict,
                       lifecycle_by_challenger: Optional[dict]) -> Optional[dict]:
    """Refuse a challenger whose lifecycle has closed since registration.

    The verdict is the ONE lifecycle owner's; this module holds no lifecycle
    rule. When no current verdict is available the registration's own immutable
    verdict is used and the fact that it could not be re-checked is REPORTED,
    because silently degrading to "probably still active" is how a withdrawn
    challenger would keep accruing.
    """
    reg = registration or {}
    cid = str(reg.get("challenger_id") or "")
    current = (lifecycle_by_challenger or {}).get(cid)
    rechecked = current is not None
    lc = current if rechecked else (reg.get("lifecycle_at_registration") or {})
    state = lc.get("lifecycle_state")
    adoptable = bool(lc.get("adoptable"))
    if state and not adoptable:
        return {"state": ACC_INTEGRITY_BLOCKED,
                "reason": INTEGRITY_LIFECYCLE_CLOSED,
                "lifecycle_state": state,
                "lifecycle_rechecked": rechecked,
                "detail": ("lifecycle state %s may never accrue forward "
                           "evidence; no emission, no maturation, ever" % state)}
    return {"state": None, "lifecycle_state": state,
            "lifecycle_rechecked": rechecked}


def assess_registration(*, registration: dict, series: dict,
                        lifecycle_by_challenger: Optional[dict] = None,
                        as_of: Optional[str] = None,
                        today: Optional[str] = None,
                        store_dir_override=None) -> dict:
    """The full accrual picture for ONE registration. Pure: it writes nothing.

    Every cell of the decision grid is classified, every existing emission is
    matured, and the registration-level state is the state of its earliest
    unresolved opportunity. Separating this from :func:`advance_canonical_forward_accrual`
    is deliberate: the read model and the audit can ask exactly what the writer
    would do without anything being written.
    """
    reg = registration or {}
    ident = reg.get("identity") or {}
    identity_hash = ident.get("identity_hash")
    out = {
        "identity_hash": identity_hash,
        "challenger_id": reg.get("challenger_id"),
        "freeze_id": reg.get("freeze_id"),
        "asset_class": reg.get("asset_class"),
        "release": ident.get("release"),
        "registration_session": reg.get("registration_session"),
        "registration_timestamp": reg.get("registered_at"),
        "prospective_effective_from": (reg.get("observation_clock") or {})
                                      .get("effective_from_session"),
        "observation_calendar_owner": (reg.get("observation_clock") or {})
                                      .get("calendar_owner"),
        "horizon_sessions": reg.get("horizon_sessions"),
        "accrual_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "maturation_owner": MATURATION_OWNER,
        "state_vocabulary": list(ACCRUAL_STATES),
        "cells": [],
        "emissions": [],
        "forfeitures": [],
        "predictions_emitted": 0,
        "forfeitures_recorded": 0,
        "matured_observations": 0,
        "pending_observations": 0,
        "effective_independent_observations": 0,
        "last_emission_session": None,
        "next_eligible_observation_session": None,
        "latest_blocker": None,
        "backfilled": False,
    }
    if not identity_hash:
        return {**out, "state": ACC_INTEGRITY_BLOCKED,
                "latest_blocker": INTEGRITY_NO_IDENTITY,
                "detail": "the registration does not carry an identity hash"}

    life = _lifecycle_blocked(reg, lifecycle_by_challenger)
    out["lifecycle_state"] = life.get("lifecycle_state")
    out["lifecycle_rechecked"] = life.get("lifecycle_rechecked")
    if life.get("state"):
        return {**out, "state": ACC_INTEGRITY_BLOCKED,
                "latest_blocker": life.get("reason"),
                "detail": life.get("detail")}

    # The ORIGINAL frozen decision, bound by hash. Resolving it also supplies
    # the cadence and horizon the construction itself declared.
    origin = resolve_frozen_decision(reg, None)
    out["frozen_decision"] = {k: origin.get(k) for k in
                              ("resolved", "reason", "detail", "release",
                               "frozen_decision_session", "record_hash",
                               "weights_hash", "spec_hash", "cadence_sessions",
                               "horizon_sessions", "source")}
    if not origin.get("resolved"):
        return {**out, "state": origin.get("state") or ACC_DATA_BLOCKED,
                "latest_blocker": origin.get("reason"),
                "detail": origin.get("detail")}

    weights = origin.get("weights") or {}
    horizon = origin.get("horizon_sessions") or reg.get("horizon_sessions")
    cadence = origin.get("cadence_sessions") or horizon
    out["horizon_sessions"] = horizon
    out["cadence_sessions"] = cadence
    out["instrument_scope_size"] = len(weights)

    sessions = realised_sessions(weights, series, as_of=as_of)
    if not sessions:
        return {**out, "state": ACC_DATA_BLOCKED,
                "latest_blocker": (BLOCK_NO_PANEL if not series
                                   else BLOCK_NO_SESSIONS),
                "detail": ("the panel prints no session for this book's "
                           "instruments, so no observation can be due")}
    latest = sessions[-1]
    out["latest_realised_session"] = latest
    # The SAME date authority the registrar used to derive the prospective
    # boundary at adoption (api.prospective_adoption.current_prospective_boundary
    # is today's UTC date). One clock opened these registrations; the same clock
    # decides whether their sessions have arrived.
    today = (_iso_date(today) or _iso_date(as_of)
             or datetime.now(timezone.utc).date().isoformat())

    emissions = load_emissions(identity_hash, store_dir_override)
    forfeits = load_forfeitures(identity_hash, store_dir_override)
    emitted_on = {str(e.get("decision_session")) for e in emissions}
    forfeited_on = {str(f.get("decision_session")) for f in forfeits}

    grid = decision_grid(reg, sessions=sessions, cadence_sessions=cadence)
    out["decision_grid"] = list(grid)
    out["first_decision_session"] = grid[0] if grid else None
    out["today"] = today
    first_s = grid[0] if grid else None
    cells = []
    next_turn_taken = False
    for s in grid:
        if s in emitted_on:
            cells.append({"decision_session": s, "state": ACC_EMITTED,
                          "reason": None})
            continue
        if s in forfeited_on:
            row = next((f for f in forfeits
                        if str(f.get("decision_session")) == s), {})
            cells.append({"decision_session": s, "state": ACC_FORFEITED,
                          "reason": row.get("reason"),
                          "backfill_refused": True})
            continue
        if s <= today:
            # The session has ARRIVED. Whatever this run now knows about it, a
            # row stamped with it would be a decision taken with the session in
            # view. The opportunity is gone; it is never written late.
            book = book_for_decision_session(reg, s, origin=origin,
                                             first_session=first_s)
            if not book.get("resolved"):
                cells.append({"decision_session": s,
                              "state": book.get("state") or ACC_DATA_BLOCKED,
                              "reason": book.get("reason"),
                              "detail": book.get("detail")})
                continue
            cells.append({"decision_session": s, "state": ACC_FORFEITED,
                          "reason": FORFEIT_WINDOW_CLOSED,
                          "detail": ("the emission window for %s closed when %s "
                                     "began; a prospective decision is made "
                                     "BEFORE its session, never after it"
                                     % (s, s)),
                          "backfill_refused": True,
                          "not_yet_recorded": True})
            continue
        # The session is still in the future. Only the NEXT unresolved boundary
        # takes its turn; a later one is not yet anybody's decision to make.
        if next_turn_taken:
            cells.append({"decision_session": s, "state": ACC_NOT_DUE,
                          "reason": NOT_DUE_SESSION_NOT_REACHED,
                          "detail": ("a later cadence boundary; the next one has "
                                     "not been decided yet")})
            continue
        next_turn_taken = True
        book = book_for_decision_session(reg, s, origin=origin,
                                         first_session=first_s)
        if not book.get("resolved"):
            cells.append({"decision_session": s,
                          "state": book.get("state") or ACC_DATA_BLOCKED,
                          "reason": book.get("reason"),
                          "detail": book.get("detail")})
            continue
        # Coverage is asked of the LATEST session the panel actually printed -
        # the marks this book would be entered against. The decision session has
        # not happened yet, so asking about its bars would always answer "none".
        share = priced_share(book.get("weights") or {}, series, latest)
        if share < kernel.MIN_PRICED_WEIGHT:
            cells.append({"decision_session": s, "state": ACC_DATA_BLOCKED,
                          "reason": BLOCK_COVERAGE,
                          "priced_share": round(share, 6),
                          "priced_at_session": latest,
                          "min_priced_weight": kernel.MIN_PRICED_WEIGHT,
                          "detail": ("only %.1f%% of the book's weight has a bar "
                                     "on %s; a flat mark would be a claim that "
                                     "the unpriced names did not move"
                                     % (share * 100.0, latest))})
            continue
        cells.append({"decision_session": s, "state": ACC_DUE, "reason": None,
                      "book_resolved": True, "priced_share": round(share, 6),
                      "priced_at_session": latest,
                      "detail": ("emitted strictly before %s; the position is "
                                 "entered at that session's close and scored "
                                 "only after it" % s)})
    out["cells"] = cells

    matured = [mature_emission(e, series, as_of=as_of) for e in emissions]
    indep = effective_independent_observations(matured)
    n_matured = sum(1 for m in matured if m.get("matured"))
    pending = [m for m in matured if not m.get("matured")]
    next_maturity = None
    for e, m in zip(emissions, matured):
        if not m.get("matured"):
            next_maturity = e.get("decision_session")
            break

    due = [c for c in cells if c["state"] == ACC_DUE]
    blocked = [c for c in cells if c["state"] in (ACC_DATA_BLOCKED,
                                                  ACC_INTEGRITY_BLOCKED)]
    not_reached = [c for c in cells
                   if c["state"] == ACC_NOT_DUE
                   and c.get("reason") == NOT_DUE_SESSION_NOT_REACHED]
    if due:
        state = ACC_DUE
    elif blocked:
        state = blocked[0]["state"]
    elif emissions:
        state = ACC_EMITTED
    elif [c for c in cells if c["state"] == ACC_FORFEITED]:
        state = ACC_FORFEITED
    else:
        state = ACC_NOT_DUE

    return {
        **out,
        "state": state,
        "emissions": [{"decision_session": e.get("decision_session"),
                       "emitted_at": e.get("emitted_at"),
                       "record_hash": (e.get("prediction") or {}).get("record_hash"),
                       "position_count": (e.get("prediction") or {}).get("position_count"),
                       "backfilled": False} for e in emissions],
        "forfeitures": [{"decision_session": f.get("decision_session"),
                         "reason": f.get("reason"),
                         "recorded_at": f.get("recorded_at"),
                         "backfill_refused": True} for f in forfeits],
        "maturation": matured,
        "predictions_emitted": len(emissions),
        "forfeitures_recorded": len(forfeits),
        "matured_observations": n_matured,
        "pending_observations": len(pending),
        "effective_independent_observations":
            indep["effective_independent_observations"],
        "independent_observation_rule": indep["rule"],
        "last_emission_session": (emissions[-1].get("decision_session")
                                  if emissions else None),
        "next_eligible_observation_session": (
            due[0]["decision_session"] if due
            else (not_reached[0]["decision_session"] if not_reached else None)),
        "next_maturity_from_decision_session": next_maturity,
        "latest_blocker": (blocked[0].get("reason") if blocked else None),
        "backfilled": False,
    }


def advance_canonical_forward_accrual(*, now: Optional[datetime] = None,
                                      price_panel: Optional[dict] = None,
                                      registrations: Optional[list] = None,
                                      lifecycle_by_challenger: Optional[dict] = None,
                                      registry_dir_override=None,
                                      store_dir_override=None,
                                      as_of: Optional[str] = None,
                                      today: Optional[str] = None,
                                      execute: bool = True) -> dict:
    """THE automatic advance. Called by :mod:`alpha_agent.r52.runtime`.

    Discovers every canonical registration, emits exactly the observations that
    are legally due right now, records the opportunities that were genuinely
    missed, and matures what has completed its horizon. ``execute=False`` makes
    it a pure dry run that writes nothing - the same classification, no store.
    """
    ts = _now_iso() if now is None else (
        now.isoformat() if isinstance(now, datetime) else str(now))
    # ONE instant decides both the emission stamp and whether a session has
    # arrived. Reading the clock twice would let a run that starts at 23:59:59
    # UTC forfeit a session it was still entitled to emit a second earlier.
    if today is None:
        today = _iso_date(ts)
    if registrations is None:
        try:
            from paper_trader.api import forward_challenger_registry as FCR
            registrations = FCR.load_registrations(registry_dir_override)
        except Exception as exc:                            # noqa: BLE001
            return {
                "schema_version": SCHEMA_VERSION, "owner": COMPOSITION_OWNER,
                "phase": PHASE, "generated_at": ts, "executed": False,
                "state": ACC_DATA_BLOCKED,
                "blocker": "CANONICAL_REGISTRY_UNREADABLE",
                "detail": str(exc)[:200], "challengers": [],
                "n_registered": 0, "safety": _safety(),
            }
    if price_panel is None:
        try:
            from paper_trader.api import price_panel as pp
            price_panel = pp.load_operational_price_panel()
        except Exception:                                   # noqa: BLE001
            price_panel = None
    series = (price_panel or {}).get("series") or {}

    results = []
    n_emitted = n_duplicate = n_forfeited = 0
    for reg in registrations or []:
        assessed = assess_registration(
            registration=reg, series=series,
            lifecycle_by_challenger=lifecycle_by_challenger, as_of=as_of,
            today=today, store_dir_override=store_dir_override)
        actions = []
        if execute:
            origin = resolve_frozen_decision(reg, None)
            first_s = assessed.get("first_decision_session")
            for cell in assessed.get("cells") or []:
                if cell["state"] == ACC_DUE:
                    book = book_for_decision_session(
                        reg, cell["decision_session"], origin=origin,
                        first_session=first_s)
                    if not book.get("resolved"):
                        continue
                    res = emit_prospective_prediction(
                        registration=reg,
                        decision_session=cell["decision_session"], book=book,
                        store_dir_override=store_dir_override, now=ts)
                    actions.append({"decision_session": cell["decision_session"],
                                    "action": "EMIT",
                                    "outcome": res.get("outcome"),
                                    "reason": res.get("reason")})
                    if res.get("outcome") == EMIT_WROTE:
                        n_emitted += 1
                    elif res.get("outcome") == EMIT_DUPLICATE:
                        n_duplicate += 1
                elif cell["state"] == ACC_FORFEITED and cell.get("not_yet_recorded"):
                    res = record_forfeiture(
                        registration=reg,
                        decision_session=cell["decision_session"],
                        reason=str(cell.get("reason")),
                        detail=str(cell.get("detail")),
                        store_dir_override=store_dir_override, now=ts)
                    actions.append({"decision_session": cell["decision_session"],
                                    "action": "FORFEIT",
                                    "outcome": res.get("outcome")})
                    if res.get("outcome") == EMIT_WROTE:
                        n_forfeited += 1
            if actions:
                # Re-read after writing so the reported counters are the STORE's
                # and never this function's arithmetic about what it just did.
                assessed = assess_registration(
                    registration=reg, series=series,
                    lifecycle_by_challenger=lifecycle_by_challenger,
                    as_of=as_of, today=today,
                    store_dir_override=store_dir_override)
        results.append({**assessed, "actions": actions})

    due_now = [r for r in results if r.get("state") == ACC_DUE]
    blocked = [r for r in results if r.get("state") in (ACC_DATA_BLOCKED,
                                                        ACC_INTEGRITY_BLOCKED)]
    armed = [r for r in results
             if r.get("state") in (ACC_NOT_DUE, ACC_EMITTED)
             and r.get("next_eligible_observation_session")]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "registrar_owner": REGISTRAR_OWNER,
        "maturation_owner": MATURATION_OWNER,
        "phase": PHASE,
        "generated_at": ts,
        "executed": bool(execute),
        "store_dir": str(store_dir(store_dir_override)),
        "state_vocabulary": list(ACCRUAL_STATES),
        "n_registered": len(results),
        "n_due_now": len(due_now),
        "n_emitted_this_run": n_emitted,
        "n_duplicates_skipped": n_duplicate,
        "n_forfeitures_recorded_this_run": n_forfeited,
        "n_blocked": len(blocked),
        "n_armed_for_a_future_session": len(armed),
        "predictions_emitted_total": sum(r.get("predictions_emitted") or 0
                                         for r in results),
        "matured_observations_total": sum(r.get("matured_observations") or 0
                                          for r in results),
        "effective_independent_observations_total": sum(
            r.get("effective_independent_observations") or 0 for r in results),
        "forfeitures_total": sum(r.get("forfeitures_recorded") or 0
                                 for r in results),
        "challengers": results,
        "backfill_allowed": False,
        "historical_result_is_never_forward_evidence": True,
        "automatic_promotion_allowed": False,
        "safety": _safety(),
    }
    if execute:
        # The RUN publishes the read model. A GET then costs one small file
        # read instead of a registry walk and a price-panel load.
        payload["projection_persisted"] = True
        persist_accrual_projection(payload,
                                   store_dir_override=store_dir_override)
    return payload


# --------------------------------------------------------------------------- #
# 8. THE READ MODEL - what an operator must be able to see, computed here
# --------------------------------------------------------------------------- #
def accrual_projection(advance: Optional[dict] = None, **kwargs) -> dict:
    """``identity_hash -> the accrual facts`` for injection into the registrar's
    read model.

    The registrar's records are IMMUTABLE, so its counters are zero forever by
    design. The living counters belong to this owner, and the registrar's row
    overlays them rather than either owner recomputing the other's numbers.
    """
    payload = advance if advance is not None else load_canonical_forward_accrual(
        **kwargs)
    out = {}
    for r in payload.get("challengers") or []:
        ih = r.get("identity_hash")
        if not ih:
            continue
        out[str(ih)] = {
            "current_accrual_state": r.get("state"),
            "accrual_state_vocabulary": list(ACCRUAL_STATES),
            "predictions_emitted": r.get("predictions_emitted"),
            "forfeitures": r.get("forfeitures_recorded"),
            "matured_observations": r.get("matured_observations"),
            "pending_observations": r.get("pending_observations"),
            "effective_independent_observations": r.get(
                "effective_independent_observations"),
            "last_emission_session": r.get("last_emission_session"),
            "next_eligible_observation_session": r.get(
                "next_eligible_observation_session"),
            "latest_blocker": r.get("latest_blocker"),
            "accrual_owner": COMPOSITION_OWNER,
            "backfilled": False,
        }
    return out


def _projection_path(store_dir_override=None) -> Path:
    return store_dir(store_dir_override) / PROJECTION_ARTIFACT


def persist_accrual_projection(advance: dict, *, store_dir_override=None) -> dict:
    """Write the read model this run produced. Called only by the WRITER."""
    body = {
        "schema_version": SCHEMA_VERSION,
        "owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "generated_at": advance.get("generated_at"),
        "n_registered": advance.get("n_registered"),
        "predictions_emitted_total": advance.get("predictions_emitted_total"),
        "matured_observations_total": advance.get("matured_observations_total"),
        "effective_independent_observations_total": advance.get(
            "effective_independent_observations_total"),
        "forfeitures_total": advance.get("forfeitures_total"),
        "by_identity": accrual_projection(advance),
        "accrual_state_vocabulary": list(ACCRUAL_STATES),
        "read_only": True,
    }
    _atomic_write_json(_projection_path(store_dir_override), body)
    return body


def load_accrual_projection(*, store_dir_override=None) -> dict:
    """``identity_hash -> accrual facts``, READ from the artifact the run wrote.

    Cheap by construction: one small JSON file, no price panel, no registry
    walk. Returns ``{}`` when no run has happened yet, which reads as "the
    accrual owner has published nothing" and leaves the registrar's own
    immutable zeros showing - never an invented count.
    """
    body = _load_json(_projection_path(store_dir_override)) or {}
    rows = body.get("by_identity")
    return dict(rows) if isinstance(rows, dict) else {}


def load_accrual_projection_artifact(*, store_dir_override=None) -> dict:
    """The whole persisted read model, including WHEN it was generated."""
    return _load_json(_projection_path(store_dir_override)) or {}


def load_canonical_forward_accrual(**kwargs) -> dict:
    """The READ-ONLY view. Identical classification, guaranteed to write nothing.

    This RECOMPUTES from the registry and the panel, so it is the honest answer
    and also the expensive one. Read paths should prefer
    :func:`load_accrual_projection`, which reads what the last run persisted.
    """
    kwargs.pop("execute", None)
    return advance_canonical_forward_accrual(execute=False, **kwargs)


__all__ = [
    "SCHEMA_VERSION", "COMPOSITION_OWNER", "CALCULATION_OWNER",
    "REGISTRAR_OWNER", "MATURATION_OWNER", "PHASE", "ROUTE", "STORE_DIR_ENV",
    "STARTING_CAPITAL", "SAFETY_BADGES",
    "ACCRUAL_STATES", "ACC_NOT_DUE", "ACC_DUE", "ACC_EMITTED", "ACC_FORFEITED",
    "ACC_DATA_BLOCKED", "ACC_INTEGRITY_BLOCKED",
    "NOT_DUE_REASONS", "NOT_DUE_SESSION_NOT_REACHED",
    "NOT_DUE_AWAITING_NEW_FREEZE", "FORFEIT_WINDOW_CLOSED",
    "DATA_BLOCK_REASONS", "BLOCK_SPEC_UNRESOLVABLE", "BLOCK_SPEC_MISSING",
    "BLOCK_NO_PANEL", "BLOCK_NO_SESSIONS", "BLOCK_COVERAGE",
    "INTEGRITY_REASONS", "INTEGRITY_LIFECYCLE_CLOSED",
    "INTEGRITY_HASH_MISMATCH", "INTEGRITY_NO_IDENTITY",
    "EMISSION_OUTCOMES", "EMIT_WROTE", "EMIT_DUPLICATE", "EMIT_REFUSED",
    "FROZEN_DECISION_OWNERS", "resolve_frozen_decision",
    "book_for_decision_session", "priced_share",
    "realised_sessions", "latest_realised_session", "first_decision_session",
    "decision_grid", "store_dir", "load_emissions", "load_forfeitures",
    "emit_prospective_prediction", "record_forfeiture", "mature_emission",
    "effective_independent_observations", "assess_registration",
    "advance_canonical_forward_accrual", "accrual_projection",
    "PROJECTION_ARTIFACT", "persist_accrual_projection",
    "load_accrual_projection", "load_accrual_projection_artifact",
    "load_canonical_forward_accrual",
]
