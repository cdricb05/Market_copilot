"""api/monthly_momentum_input.py — Phase 29D.1 canonical monthly momentum input adapter.

The ONE in-repo owner of the frozen monthly momentum input CONTRACT (Feature
Production domain; ``api.multi_horizon_engine`` reads ``current_momentum_scores.csv``
via ``load_inputs`` → ``momentum_month``). It is NOT a scoring engine and computes NO
``mom_6_1``: the frozen monthly momentum is produced by an owned survivorship-free
research emitter (external, deterministic, point-in-time safe). This adapter wraps
that emitter behind ONE narrow, injectable seam and owns the SAFE ORCHESTRATION
contract around it:

  * decides whether a new monthly emission is DUE for the eligible session (the
    persisted input month vs the eligible month) — runs ONLY when due;
  * uses data available as of the eligible session; never approximates INTRAMONTH
    and never BACKDATES (both are validation rejections, not silent behaviour);
  * validates the produced artifact's schema, PERIOD (``month_label`` == the due
    month) and PROVENANCE (``market_as_of_date`` <= eligible; the month strictly
    advances the prior artifact);
  * HASHES the frozen monthly content, atomically PERSISTS the canonical CSV, REUSES
    an identical existing artifact and REJECTS a conflicting one (first-write-wins
    per month);
  * exposes a clear status + errors and NEVER creates an order/signal/decision/fill,
    calls a provider / prediction service, mutates an operational ledger / database,
    or changes holdings / cash / NAV.

The frozen mom_6_1 monthly momentum is computed by an owned numpy/pandas emitter that
lives in the research repo; the pure-stdlib Paper Trader process never imports it.
Phase 29D.2 wires ONE production PRODUCER behind this adapter's seam — the subprocess
bridge ``api.monthly_momentum_emitter`` — activated by the ``api.app`` startup wiring
(``activate_production_emitter``). So when momentum_monthly is due, ONE ``RUN DAILY
RESEARCH CYCLE`` action resolves it with no separate command / button / restart / file
operation. The bridge remains an EXPLICIT seam: an injected callable (tests), the
activated production resolver (the running backend), or the opt-in env producer
(``PAPER_TRADER_MONTHLY_EMITTER_ENABLED``). When no producer is available a due month
still BLOCKS HONESTLY — owned by THIS adapter, never ``NO_REFRESH_OWNER`` and never a
"run a separate button" prerequisite. The Persistent Daily Research Cycle calls
``emit_if_due`` through its execution plan.

Emitter seam contract — an emitter callable is invoked as
``emitter_fn(month=<YYYY-MM>, eligible=<YYYY-MM-DD>, inputs_dir=<path>)`` and returns::

    {"month_label": "2026-08", "market_as_of_date": "2026-08-04",
     "rows": [{"ticker": "AAA", "mom_6_1": 0.12, "is_member": "1",
               "sector": "Tech", "market_as_of_date": "2026-08-04",
               "month_label": "2026-08"}, ...],
     "source": "research.phase25_multi_horizon_inputs"}

The adapter validates the returned artifact and owns all persistence; the emitter
never writes the canonical CSV itself.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import multi_horizon_engine as eng

PHASE = "29D.1"
CANONICAL_OWNER = "api.monthly_momentum_input"

# Honest missing-implementation token (identical to the DRC / alpha_target vocabulary
# so the operator vocabulary stays consistent when no emitter is wired).
MONTHLY_EMITTER_ACTION = "RUN_RESEARCH_MONTHLY_INPUT_EMITTER"

# Opt-in production wiring. Default OFF: the pure-stdlib repo never imports the
# external numpy/pandas emitter automatically.
EMITTER_ENABLED_ENV = "PAPER_TRADER_MONTHLY_EMITTER_ENABLED"

# --------------------------------------------------------------------------- #
# Frozen status vocabulary (part of the tested contract).
# --------------------------------------------------------------------------- #
S_CURRENT = "MONTHLY_INPUT_CURRENT"            # not due; the existing artifact stands
S_EMITTED = "MONTHLY_INPUT_EMITTED"            # due; a new artifact was produced + persisted
S_REUSED = "MONTHLY_INPUT_REUSED"              # due; an identical artifact already present
S_UNAVAILABLE = "MONTHLY_EMITTER_UNAVAILABLE"  # due; no emitter wired (honest DATA_HOLD)
S_CONFLICT = "MONTHLY_INPUT_CONFLICT"          # due; a DIFFERENT artifact already exists
S_INVALID = "MONTHLY_INPUT_INVALID"            # emitter output failed validation (rejected)
S_SOURCE_UNAVAILABLE = "MONTHLY_INPUT_SOURCE_UNAVAILABLE"  # cannot read the current input
STATUS_VOCAB = (S_CURRENT, S_EMITTED, S_REUSED, S_UNAVAILABLE, S_CONFLICT,
                S_INVALID, S_SOURCE_UNAVAILABLE)
# Statuses that mean "the monthly input is safely current for the eligible month".
_OK = frozenset({S_CURRENT, S_EMITTED, S_REUSED})

# The minimum frozen monthly schema an emitted artifact must carry.
REQUIRED_COLUMNS = ("ticker", "mom_6_1", "is_member", "sector",
                    "market_as_of_date", "month_label")
# The frozen monthly fields that DEFINE the artifact identity (market_as_of_date is
# the intramonth-advancing field and is deliberately excluded from the identity hash).
_IDENTITY_COLUMNS = ("ticker", "mom_6_1", "is_member", "sector", "month_label")

SAFETY_BADGES = ["PREVIEW ONLY", "READ OWNED DATA", "NO ORDERS", "AUTOMATION OFF",
                 "MANUAL REVIEW"]


# --------------------------------------------------------------------------- #
# Small pure helpers.
# --------------------------------------------------------------------------- #
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


def _eligible_month(value: Any) -> Optional[str]:
    d = _coerce_date(value)
    return d.strftime("%Y-%m") if d else None


def _month_str(value: Any) -> Optional[str]:
    """Normalize a month-ish value to a 'YYYY-MM' string, else None."""
    if value is None:
        return None
    s = str(value).strip()
    if len(s) >= 7 and s[4] == "-" and s[:4].isdigit() and s[5:7].isdigit():
        return s[:7]
    return None


def _hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


def _identity_hash(rows: list[dict]) -> str:
    """Deterministic content hash over the FROZEN monthly identity fields (order
    independent). Two artifacts with identical monthly content hash equal."""
    ident = sorted(
        tuple(str(r.get(c) if r.get(c) is not None else "") for c in _IDENTITY_COLUMNS)
        for r in rows)
    return _hash(ident)


def _inputs_dir(inputs_dir=None) -> Path:
    return eng._resolve(inputs_dir, eng.INPUTS_ENV, eng.DEFAULT_INPUTS)


def _mom_path(inputs_dir=None) -> Path:
    return _inputs_dir(inputs_dir) / eng.CUR_MOM_FILE


def _read_csv_rows(path: Path) -> tuple[Optional[list[dict]], Optional[list[str]]]:
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            return rows, list(reader.fieldnames or [])
    except OSError:
        return None, None


def _atomic_write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _current_month_of(rows: Optional[list[dict]]) -> tuple[Optional[str], Optional[str]]:
    """(month_label, market_as_of_date) of the persisted current input, else (None, None)."""
    if not rows:
        return None, None
    month = next((_month_str(r.get("month_label")) for r in rows if r.get("month_label")), None)
    as_of = next((str(r.get("market_as_of_date") or "")[:10]
                  for r in rows if r.get("market_as_of_date")), None)
    return month, (as_of or None)


def _safety() -> dict:
    return {
        "read_only_when_not_emitting": True,
        "preview_only": True,
        "paper_only": True,
        "manual_review": True,
        "automation_off": True,
        "approximates_intramonth": False,
        "backdates": False,
        "called_provider": False,
        "called_prediction": False,
        "wrote_to_operational_ledger": False,
        "wrote_to_database": False,
        "changed_holdings": False,
        "changed_cash_or_nav": False,
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "created_fills": False,
        "promoted_model": False,
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# Due-ness + emitter resolution.
# --------------------------------------------------------------------------- #
def is_due(*, eligible: Any, current_month: Any) -> bool:
    """A new monthly emission is DUE when the eligible month is strictly newer than
    the persisted input month. Never intramonth (same month → not due)."""
    em = _eligible_month(eligible)
    cm = _month_str(current_month)
    if em is None:
        return False
    if cm is None:
        return True          # no persisted month → the monthly input is missing / due
    return em > cm


# A deployment may register a concrete emitter callable (importable producer) here,
# or activate the production RESOLVER (Phase 29D.2). Both default to unset so the
# pure-stdlib repo stays hermetic (a due month blocks honestly) until an explicit
# deployment wiring activates the production subprocess bridge.
_REGISTERED_EMITTER: Optional[Callable] = None
_PRODUCTION_RESOLVER: Optional[Callable[[], Optional[Callable]]] = None


def register_emitter(fn: Optional[Callable]) -> None:
    """Register (or clear) the concrete monthly emitter producer for this process.
    Used by an explicit opt-in deployment wiring or a test; never called automatically."""
    global _REGISTERED_EMITTER
    _REGISTERED_EMITTER = fn


def activate_production_emitter(resolver: Optional[Callable[[], Optional[Callable]]]) -> None:
    """Phase 29D.2: activate (or clear) the production emitter RESOLVER — a zero-arg
    callable that returns a concrete seam-shaped emitter when the production
    environment is available, else None (honest DATA_HOLD). Called by the explicit
    deployment startup wiring (``api.app``) only; NEVER during import or tests, so the
    Paper Trader process stays pure-stdlib and the test suite stays hermetic."""
    global _PRODUCTION_RESOLVER
    _PRODUCTION_RESOLVER = resolver


def _lazy_production_resolver() -> Optional[Callable]:
    """Env opt-in fallback: resolve the production subprocess bridge on demand."""
    from paper_trader.api import monthly_momentum_emitter as mme
    return mme.resolve_production_emitter()


def resolve_default_emitter() -> Optional[Callable]:
    """Production emitter resolver. Default: None (no safe in-repo emitter), so a due
    month blocks HONESTLY through this adapter. An emitter is wired when a concrete
    producer was registered (tests / explicit deployment), when the production
    subprocess bridge RESOLVER was activated at startup (Phase 29D.2), or when the
    opt-in env is set. The pure-stdlib repo never imports the external numpy/pandas
    emitter automatically — the bridge shells out through an external Python."""
    if _REGISTERED_EMITTER is not None:
        return _REGISTERED_EMITTER
    resolver = _PRODUCTION_RESOLVER
    if resolver is None and str(os.environ.get(EMITTER_ENABLED_ENV, "")).strip().lower() \
            in ("1", "true", "yes"):
        resolver = _lazy_production_resolver
    if resolver is None:
        return None
    try:
        return resolver()
    except Exception:  # noqa: BLE001
        return None


def _resolve_emitter(emitter_fn: Optional[Callable]) -> Optional[Callable]:
    if emitter_fn is not None:
        return emitter_fn
    return resolve_default_emitter()


def emitter_is_wired(*, emitter_fn: Optional[Callable] = None) -> bool:
    """Whether an emitter implementation is available (injected or opt-in producer).
    False (production default) → a due month blocks honestly through this owner."""
    return _resolve_emitter(emitter_fn) is not None


# --------------------------------------------------------------------------- #
# Validation of an emitter-produced artifact.
# --------------------------------------------------------------------------- #
def _validate_artifact(artifact: Any, *, due_month: str, eligible: Any,
                       prev_month: Optional[str]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(artifact, dict):
        return False, ["Emitter did not return an artifact object."]

    if artifact.get("approximated") or artifact.get("intramonth") \
            or artifact.get("intramonth_approximation"):
        errors.append("INTRAMONTH_APPROXIMATION_FORBIDDEN: the frozen mom_6_1 monthly "
                      "input is never approximated intramonth.")

    rows = artifact.get("rows")
    if not isinstance(rows, list) or not rows:
        errors.append("EMPTY_OR_MISSING_ROWS: the artifact carries no rows.")
        rows = []

    month = _month_str(artifact.get("month_label"))
    if month is None and rows:
        month = _month_str(rows[0].get("month_label"))
    if month is None:
        errors.append("MISSING_PERIOD: the artifact has no month_label.")
    elif month != due_month:
        errors.append("PERIOD_MISMATCH: month_label %s != due month %s." % (month, due_month))

    # No backdating: the emitted month must strictly advance the prior input month.
    if prev_month is not None and month is not None and month <= prev_month:
        errors.append("BACKDATE_FORBIDDEN: emitted month %s does not advance the prior "
                      "input month %s." % (month, prev_month))

    as_of = _coerce_date(artifact.get("market_as_of_date"))
    if as_of is None and rows:
        as_of = _coerce_date(rows[0].get("market_as_of_date"))
    elig_d = _coerce_date(eligible)
    if as_of is None:
        errors.append("MISSING_PROVENANCE: the artifact has no market_as_of_date.")
    elif elig_d is not None and as_of > elig_d:
        errors.append("FUTURE_PROVENANCE: market_as_of_date %s is later than the "
                      "eligible session %s." % (as_of.isoformat(), elig_d.isoformat()))

    # Schema: every row carries the required frozen columns.
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            errors.append("BAD_ROW: row %d is not an object." % i)
            continue
        missing = [c for c in REQUIRED_COLUMNS if c not in r]
        if missing:
            errors.append("SCHEMA_MISSING_COLUMNS in row %d: %s" % (i, ", ".join(missing)))
            break

    return (not errors), errors


def _fieldnames_for(rows: list[dict]) -> list[str]:
    seen: list[str] = []
    for c in REQUIRED_COLUMNS:
        seen.append(c)
    for r in rows:
        for k in r:
            if k not in seen:
                seen.append(k)
    return seen


# --------------------------------------------------------------------------- #
# Read-only status.
# --------------------------------------------------------------------------- #
def monthly_status(*, eligible: Any, inputs_dir=None,
                   emitter_fn: Optional[Callable] = None) -> dict:
    """Read-only monthly-input status: current month, due-ness, declared owner and
    whether an emitter is wired. Performs NO write and runs NO emitter."""
    rows, _ = _read_csv_rows(_mom_path(inputs_dir))
    cur_month, cur_as_of = _current_month_of(rows)
    due = is_due(eligible=eligible, current_month=cur_month)
    wired = emitter_is_wired(emitter_fn=emitter_fn)
    return {
        "phase": PHASE,
        "authoritative_owner": CANONICAL_OWNER,
        "producer": "api.monthly_momentum_emitter",
        "eligible_month": _eligible_month(eligible),
        "current_month": cur_month,
        "current_market_as_of_date": cur_as_of,
        "due": bool(due),
        "emitter_wired": bool(wired),
        "missing_implementation": (None if wired or not due else MONTHLY_EMITTER_ACTION),
        "status_preview": (S_CURRENT if not due
                           else (S_EMITTED if wired else S_UNAVAILABLE)),
        "safety": _safety(),
    }


# --------------------------------------------------------------------------- #
# The canonical adapter seam the Daily Research Cycle calls.
# --------------------------------------------------------------------------- #
def emit_if_due(*, eligible: Any, inputs_dir=None,
                emitter_fn: Optional[Callable] = None, dry_run: bool = False) -> dict:
    """Emit the frozen monthly momentum input for the eligible session IF due.

    Signature matches the Daily Research Cycle monthly seam (``emit(eligible=...)``).
    Runs ONLY when a new monthly period is due, uses only data available as of the
    eligible session, validates schema / period / provenance, hashes the frozen
    content, atomically persists the canonical CSV, REUSES an identical existing
    artifact and REJECTS a conflicting one. Never approximates intramonth, never
    backdates, never substitutes a current snapshot for historical data. When no
    emitter is wired a due month is an honest DATA_HOLD owned by this adapter.
    """
    path = _mom_path(inputs_dir)
    existing_rows, existing_fields = _read_csv_rows(path)
    cur_month, cur_as_of = _current_month_of(existing_rows)
    eligible_month = _eligible_month(eligible)

    base = {
        "phase": PHASE,
        "authoritative_owner": CANONICAL_OWNER,
        "eligible": _coerce_date(eligible).isoformat() if _coerce_date(eligible) else None,
        "eligible_month": eligible_month,
        "current_month": cur_month,
        "due_month": eligible_month,
        "performed_write": False,
        "artifacts_written": [],
        "safety": _safety(),
    }

    if eligible_month is None:
        return {**base, "status": S_SOURCE_UNAVAILABLE, "due": False,
                "message": "No eligible session date supplied; nothing to do."}

    # A persisted artifact whose month is AHEAD of the eligible session is a conflict:
    # refuse to backdate / overwrite a NEWER artifact (first-write-wins per month).
    if cur_month is not None and eligible_month < cur_month:
        return {**base, "status": S_CONFLICT, "due": False,
                "message": ("The persisted monthly input month %s is AHEAD of the "
                            "eligible month %s; refusing to overwrite a newer artifact."
                            % (cur_month, eligible_month))}

    if not is_due(eligible=eligible, current_month=cur_month):
        # Same month → already current: REUSE the existing artifact (idempotent; no
        # write). This is the idempotent-rerun / identical-artifact-reuse path.
        return {**base, "status": S_CURRENT, "due": False,
                "message": "The monthly momentum input is already current for %s; the "
                           "existing artifact is reused (no write)." % eligible_month}

    emitter = _resolve_emitter(emitter_fn)
    if emitter is None:
        return {**base, "status": S_UNAVAILABLE, "due": True,
                "missing_implementation": MONTHLY_EMITTER_ACTION,
                "message": ("A new monthly momentum input for %s is due but no safe "
                            "emitter is wired to the canonical adapter (%s). The frozen "
                            "mom_6_1 monthly input is never approximated; provide the "
                            "owned survivorship-free monthly emission."
                            % (eligible_month, CANONICAL_OWNER))}

    # Run the emitter for the due month (owned data only; the emitter never writes
    # the canonical CSV — this adapter owns persistence).
    try:
        artifact = emitter(month=eligible_month, eligible=base["eligible"],
                           inputs_dir=str(_inputs_dir(inputs_dir)))
    except Exception as exc:  # noqa: BLE001
        # A recoverable DATA_HOLD (e.g. the owned source panel is behind the eligible
        # session, an unverifiable panel, a transient timeout, or an unavailable emitter
        # environment) blocks HONESTLY through this adapter (S_UNAVAILABLE) — never a
        # mixed input set. A hard error is an INVALID artifact (nothing written).
        if getattr(exc, "monthly_data_hold", False):
            return {**base, "status": S_UNAVAILABLE, "due": True,
                    "missing_implementation": MONTHLY_EMITTER_ACTION,
                    "emitter_status": getattr(exc, "emitter_status", None),
                    "retry_classification": getattr(exc, "retry_classification", "TRANSIENT"),
                    "last_error": str(exc)[:300],
                    "message": ("A new monthly momentum input for %s is due but the owned "
                                "emission is on hold: %s" % (eligible_month, str(exc)[:200]))}
        return {**base, "status": S_INVALID, "due": True,
                "errors": ["EMITTER_RAISED: %s" % str(exc)[:200]],
                "emitter_status": getattr(exc, "emitter_status", None),
                "retry_classification": getattr(exc, "retry_classification", "PERMANENT"),
                "last_error": str(exc)[:300],
                "message": "The monthly emitter raised; nothing was written."}

    ok, errors = _validate_artifact(artifact, due_month=eligible_month,
                                    eligible=eligible, prev_month=cur_month)
    if not ok:
        return {**base, "status": S_INVALID, "due": True, "errors": errors,
                "message": "The emitted monthly artifact failed validation; nothing "
                           "was written."}

    rows = list(artifact.get("rows") or [])
    new_hash = _identity_hash(rows)

    if dry_run:
        return {**base, "status": S_EMITTED, "due": True, "artifact_hash": new_hash,
                "dry_run": True,
                "message": "Dry-run: the monthly artifact for %s validated and would "
                           "be written." % eligible_month}

    # Old artifact identity (for the promotion manifest); the atomic replace preserves
    # the existing file until success, so a partial failure never leaves a mixed set.
    old_hash = _identity_hash(existing_rows) if existing_rows else None
    fieldnames = existing_fields if (existing_fields and cur_month is not None) \
        else _fieldnames_for(rows)
    _atomic_write_csv(path, list(fieldnames), rows)
    # Clear the canonical scoring cache ONLY after a successful validated promotion.
    cache_cleared = False
    try:
        eng.clear_cache()
        cache_cleared = True
    except Exception:  # noqa: BLE001
        cache_cleared = False

    as_of = _coerce_date(artifact.get("market_as_of_date"))
    promotion = {
        "canonical_path": str(path),
        "old_artifact_hash": old_hash,
        "new_artifact_hash": new_hash,
        "reused_identical": bool(old_hash is not None and old_hash == new_hash),
        "rows_written": len(rows),
        "cache_cleared": cache_cleared,
        "source": artifact.get("source"),
        "content_hash": artifact.get("content_hash"),
        "market_as_of_date": as_of.isoformat() if as_of else None,
        "month_label": eligible_month,
    }
    out = dict(base)
    out["performed_write"] = True
    out["artifacts_written"] = [str(path)]
    out["safety"] = {**_safety(), "read_only_when_not_emitting": False}
    return {**out, "status": S_EMITTED, "due": True, "artifact_hash": new_hash,
            "market_as_of_date": as_of.isoformat() if as_of else None,
            "rows_written": len(rows), "cache_cleared": cache_cleared,
            "promotion": promotion,
            "emitter_status": artifact.get("emitter_status"),
            "source_panel": artifact.get("source_panel"),
            "message": "Emitted and persisted the monthly momentum input for %s."
                       % eligible_month}


# --------------------------------------------------------------------------- #
# Result classifier the Daily Research Cycle uses to gate on the adapter outcome.
# --------------------------------------------------------------------------- #
def classify_result(result: Optional[dict]) -> str:
    """Map an ``emit_if_due`` result to OK / BLOCKED / FAILED for the orchestrator."""
    st = str((result or {}).get("status") or "")
    if st in _OK:
        return "OK"
    if st == S_UNAVAILABLE:
        return "BLOCKED"
    if st in (S_CONFLICT, S_INVALID, S_SOURCE_UNAVAILABLE):
        return "FAILED"
    # Robust to injected fakes that return their own vocabulary.
    if any(k in st for k in ("EMITTED", "CURRENT", "REUSED", "ALREADY")):
        return "OK"
    if (result or {}).get("performed_write"):
        return "OK"
    if any(k in st for k in ("UNAVAILABLE", "DATA_HOLD", "HOLD")):
        return "BLOCKED"
    return "FAILED"


__all__ = [
    "PHASE", "CANONICAL_OWNER", "MONTHLY_EMITTER_ACTION", "EMITTER_ENABLED_ENV",
    "S_CURRENT", "S_EMITTED", "S_REUSED", "S_UNAVAILABLE", "S_CONFLICT", "S_INVALID",
    "S_SOURCE_UNAVAILABLE", "STATUS_VOCAB", "REQUIRED_COLUMNS",
    "is_due", "emitter_is_wired", "resolve_default_emitter", "register_emitter",
    "activate_production_emitter", "monthly_status", "emit_if_due", "classify_result",
]
