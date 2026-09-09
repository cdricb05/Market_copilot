r"""api.forward_challenger_registry - THE canonical SIGNAL-CHALLENGER forward
owner (Release 62.1).

THE GAP THIS CLOSES
-------------------
Release 61 built the one governed prospective-ADOPTION operation
(:mod:`api.prospective_adoption`) and discovered that adoption had nowhere to
go. Two forward-evidence owners existed, and each owns a FROZEN COHORT rather
than a registration:

  * ``alpha_agent.r46.registry`` freezes the R46 challenger contract - a fixed
    list of specifications (``alpha_agent.r46.challengers.ALL_SPECS``) hashed
    into ``contract_hash``. A challenger that is not in that contract cannot be
    added to it without changing the contract every R46 row was emitted under.
  * ``api.shadow_portfolio_evidence`` freezes a SESSION COHORT of complete paper
    portfolios and says so plainly: "it registers a session cohort, not a single
    challenger".

So a qualified R58/R59 freeze had a real, immutable identity and no owner that
would accept it, and R61 reported that honestly as
``FORWARD_SIGNAL_NO_CANONICAL_REGISTRAR``. Four ACTIVE R58 freezes sat adoptable
and invisible to prospective evidence.

WHAT THIS MODULE IS
-------------------
The ONE place a qualified freeze becomes a REGISTERED prospective challenger,
generalised out of the two cohort owners rather than added beside them:

    qualified ACTIVE freeze
      -> exact immutable freeze identity
      -> canonical prospective registration      (THIS MODULE)
      -> prospective predictions                 (the accrual owner it names)
      -> canonical maturation                    (alpha_agent.r52.runtime)
      -> TRUE_FORWARD evidence
      -> human-gated review

It is asset-agnostic BY CONSTRUCTION. There is no ``ticker`` anywhere in the
contract: ``instrument_scope`` is a list of instrument identifiers whatever they
name, the asset class travels as data, and the OBSERVATION CALENDAR is resolved
per asset class from the canonical owner for that class - never from weekday
arithmetic, and never by imposing NYSE sessions on a market that does not keep
them.

WHAT IT IS NOT
--------------
It is NOT a second TRUE_FORWARD evidence store and it is NOT a second P&L owner.
It writes no prediction, no observation, no outcome and no score. It records a
REGISTRATION and the OBSERVATION CLOCK that registration starts, and it NAMES
the canonical owner that will accrue and mature the evidence. R46 and R56
challengers are registered here BY REFERENCE - their evidence, their ledgers and
their owners stay exactly where they are - so the estate has one authoritative
lifecycle without a second business owner.

NO BACKFILL, EVER
-----------------
Registration starts a clock; it never manufactures a past. The first legitimate
observation session is the first ELIGIBLE session STRICTLY AFTER the session in
which registration happened, resolved by that asset's own calendar owner. A
session that completed before registration can never become a prediction, and
this module holds no code path that could write one.

It promotes no model, activates no sleeve, allocates no capital, writes no
holding and creates no order. Registration starts a MEASUREMENT; it never starts
a position.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

SCHEMA_VERSION = "forward_challenger_registry.v1"
COMPOSITION_OWNER = "api.forward_challenger_registry"
PHASE = "R62.1"

REGISTRY_DIR_ENV = "PAPER_TRADER_FORWARD_CHALLENGER_REGISTRY_DIR"
_DEFAULT_REGISTRY_DIR = Path(
    r"D:\Stock_Prediction_app_data\forward_challenger_registry")

_REGISTRATIONS_SUBDIR = "registrations"

SAFETY_BADGES = ["RESEARCH ONLY", "PREVIEW ONLY", "MANUAL REVIEW",
                 "NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF",
                 "NO MODEL PROMOTION"]


# --------------------------------------------------------------------------- #
# 1. THE OBSERVATION CALENDAR - resolved per asset class, never assumed
# --------------------------------------------------------------------------- #
#: The AUTHORITATIVE exchange-session calendar for US equity sessions. It is the
#: Release-60.1 supplier (``engine.exchange_calendar``) read through the ONE
#: market-session interpreter (``engine.market_session``). Nothing here keeps a
#: holiday table of its own: a second calendar owner is how 2026-09-07 (Labor
#: Day) became a scheduled maturity in the first place.
CAL_EXCHANGE_NYSE = "engine.exchange_calendar+engine.market_session"

#: For every market whose sessions are NOT the NYSE's, the authoritative answer
#: is the instrument's OWN realised bar calendar - the rule
#: ``alpha_agent.r46.clock.maturity_session`` and ``alpha_agent.r46.judge``
#: already apply. This module refuses to impose equity sessions on a rates
#: future, an FX pair or a volatility surface point, and says so rather than
#: guessing.
CAL_INSTRUMENT_REALISED = "the instrument's own realised bar calendar " \
                          "(alpha_agent.r46.clock.maturity_session)"

#: ``asset class -> observation calendar owner``. A class absent from this table
#: is reported as undeclared and its clock is NOT computed - never defaulted to
#: weekdays.
#: Both the R59 research vocabulary and the R46 contract's own labels appear
#: here, because the estate really does use both and a table that covered only
#: one of them would leave live challengers undeclared.
OBSERVATION_CALENDARS: dict[str, str] = {
    # US cash-equity sessions - the NYSE calendar decides, authoritatively.
    "US_EQUITY": CAL_EXCHANGE_NYSE,
    "US_ETF": CAL_EXCHANGE_NYSE,
    # Everything else keeps its OWN realised bar calendar. A rates future, an FX
    # future and a volatility contract all trade on days the NYSE is shut, so
    # imposing equity sessions on them would be a fabrication, not a fix.
    "EQUITY_INDEX_FUTURES": CAL_INSTRUMENT_REALISED,
    "EQUITY_INDEX": CAL_INSTRUMENT_REALISED,
    "RATES_FUTURES": CAL_INSTRUMENT_REALISED,
    "RATES": CAL_INSTRUMENT_REALISED,
    "COMMODITY_FUTURES": CAL_INSTRUMENT_REALISED,
    "COMMODITY": CAL_INSTRUMENT_REALISED,
    "FX_FUTURES": CAL_INSTRUMENT_REALISED,
    "FX": CAL_INSTRUMENT_REALISED,
    "FUTURES": CAL_INSTRUMENT_REALISED,
    "MULTI_ASSET_FUTURES": CAL_INSTRUMENT_REALISED,
    "VOLATILITY": CAL_INSTRUMENT_REALISED,
    "VOLATILITY_FUTURES": CAL_INSTRUMENT_REALISED,
    "CROSS_ASSET": CAL_INSTRUMENT_REALISED,
    "CREDIT": CAL_INSTRUMENT_REALISED,
}

#: The classes whose observation sessions ARE exchange sessions this estate can
#: decide from a rule-based calendar. Only these get a computed session clock.
#: This is the ONE declaration of that membership; every other component asks
#: :func:`observation_calendar_for` rather than keeping a list of its own.
EXCHANGE_SESSION_ASSET_CLASSES = ("US_EQUITY", "US_ETF")

#: How far ahead the exchange calendar is consulted when resolving a clock.
_CALENDAR_HORIZON_DAYS = 400

CLOCK_RESOLVED = "RESOLVED_ON_THE_EXCHANGE_SESSION_CALENDAR"
CLOCK_INSTRUMENT_OWNED = "OWNED_BY_THE_INSTRUMENT_REALISED_CALENDAR"
CLOCK_CALENDAR_UNAVAILABLE = "EXCHANGE_CALENDAR_UNAVAILABLE_FOR_THIS_RANGE"
CLOCK_ASSET_CLASS_UNDECLARED = "ASSET_CLASS_HAS_NO_DECLARED_OBSERVATION_CALENDAR"
CLOCK_STATES = (CLOCK_RESOLVED, CLOCK_INSTRUMENT_OWNED,
                CLOCK_CALENDAR_UNAVAILABLE, CLOCK_ASSET_CLASS_UNDECLARED)


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def observation_calendar_for(asset_class: Any) -> dict:
    """WHICH calendar decides this asset class's observation sessions."""
    ac = str(asset_class or "")
    owner = OBSERVATION_CALENDARS.get(ac)
    return {
        "asset_class": ac or None,
        "calendar_owner": owner,
        "is_exchange_session_calendar": ac in EXCHANGE_SESSION_ASSET_CLASSES,
        "declared": owner is not None,
        "uses_weekday_arithmetic": False,
        "note": ("An exchange-session class is resolved from the authoritative "
                 "rule-based calendar; every other class keeps its own realised "
                 "bar calendar, which this module reads rather than replaces."),
    }


def _exchange_non_sessions(start: date, end: date):
    """The authoritative NYSE full-day closures in ``[start, end]``.

    Delegated to the ONE supplier. Returns ``None`` when the supplier cannot
    answer authoritatively for the whole range - which is a refusal, not an
    invitation to count weekdays.
    """
    try:
        from paper_trader.engine import exchange_calendar as EC
    except Exception:                                   # noqa: BLE001
        return None
    if not EC.calendar_available_between(start, end):
        return None
    return EC.non_sessions_between(start, end)


def next_exchange_session_after(session: Any) -> Optional[str]:
    """The first NYSE trading session STRICTLY AFTER ``session``, or None.

    Weekends AND full-day exchange closures are skipped, because the
    authoritative closure table is supplied to the ONE market-session
    interpreter. 2026-09-07 is Labor Day and can never be returned.
    """
    d = _as_date(session)
    if d is None:
        return None
    non = _exchange_non_sessions(d, d + timedelta(days=_CALENDAR_HORIZON_DAYS))
    if non is None:
        return None
    try:
        from paper_trader.engine import market_session as MS
    except Exception:                                   # noqa: BLE001
        return None
    return MS.next_trading_day(d, non).isoformat()


def is_eligible_observation_session(asset_class: Any, session: Any) -> dict:
    """Could this asset class legitimately observe on ``session``?

    Answers only where an authoritative calendar exists. For an
    instrument-calendar class the answer is explicitly UNDECIDABLE here, because
    the panel that knows is the instrument's own realised bar index.
    """
    cal = observation_calendar_for(asset_class)
    d = _as_date(session)
    if d is None:
        return {**cal, "session": None, "eligible": None,
                "state": CLOCK_ASSET_CLASS_UNDECLARED,
                "reason": "no session was supplied"}
    if not cal["is_exchange_session_calendar"]:
        return {**cal, "session": d.isoformat(), "eligible": None,
                "state": (CLOCK_INSTRUMENT_OWNED if cal["declared"]
                          else CLOCK_ASSET_CLASS_UNDECLARED),
                "reason": ("eligibility for this asset class is decided by the "
                           "instrument's own realised bar calendar, which this "
                           "module reads rather than replaces")}
    try:
        from paper_trader.engine import exchange_calendar as EC
    except Exception:                                   # noqa: BLE001
        return {**cal, "session": d.isoformat(), "eligible": None,
                "state": CLOCK_CALENDAR_UNAVAILABLE,
                "reason": "the exchange calendar supplier could not be read"}
    if not EC.is_supported(d):
        return {**cal, "session": d.isoformat(), "eligible": None,
                "state": CLOCK_CALENDAR_UNAVAILABLE,
                "reason": "the date lies outside the supported calendar range"}
    non_session = EC.is_non_session(d)
    return {**cal, "session": d.isoformat(), "eligible": not non_session,
            "state": CLOCK_RESOLVED,
            "holiday_name": EC.holiday_name(d),
            "is_weekend": EC.is_weekend(d),
            "reason": ("the exchange did not trade on this date"
                       if non_session else "the exchange traded on this date")}


def resolve_observation_clock(*, asset_class: Any, effective_from: Any,
                              horizon_sessions: Any) -> dict:
    """The prospective clock a registration starts. Pure; writes nothing.

    ``effective_from`` is the session in which registration happened. The first
    legitimate observation is the first eligible session STRICTLY AFTER it -
    which is exactly why no session that already completed can be turned into a
    prediction.
    """
    cal = observation_calendar_for(asset_class)
    eff = _as_date(effective_from)
    try:
        horizon = int(horizon_sessions) if horizon_sessions is not None else None
    except (TypeError, ValueError):
        horizon = None

    base = {
        **cal,
        "effective_from_session": eff.isoformat() if eff else None,
        "horizon_sessions": horizon,
        "first_eligible_observation_session": None,
        "next_expected_maturity_session": None,
        "accrued_eligible_sessions": 0,
        "backfilled": False,
        "sessions_between_inception_and_registration_are_never_synthesised": True,
    }
    if eff is None:
        return {**base, "state": CLOCK_ASSET_CLASS_UNDECLARED,
                "reason": "no registration session was declared"}
    if not cal["declared"]:
        return {**base, "state": CLOCK_ASSET_CLASS_UNDECLARED,
                "reason": ("asset class %r declares no observation calendar "
                           "owner; the clock is NOT computed rather than "
                           "guessed" % cal["asset_class"])}
    if not cal["is_exchange_session_calendar"]:
        return {**base, "state": CLOCK_INSTRUMENT_OWNED,
                "reason": ("this asset class observes on its own realised bar "
                           "calendar; the first eligible session and the "
                           "maturity are resolved by that panel at scoring "
                           "time, never by an equity holiday table")}

    first = next_exchange_session_after(eff)
    if first is None:
        return {**base, "state": CLOCK_CALENDAR_UNAVAILABLE,
                "reason": ("the authoritative exchange calendar could not "
                           "answer for this range; no clock is published "
                           "rather than one counted in weekdays")}
    maturity = first
    if horizon:
        for _ in range(horizon):
            nxt = next_exchange_session_after(maturity)
            if nxt is None:
                maturity = None
                break
            maturity = nxt
    else:
        maturity = None
    return {**base, "state": CLOCK_RESOLVED,
            "first_eligible_observation_session": first,
            "next_expected_maturity_session": maturity,
            "reason": ("the first eligible observation is the first exchange "
                       "session strictly after registration; the maturity "
                       "counts %s eligible sessions from it on the same "
                       "calendar" % (horizon if horizon else "no declared"))}


# --------------------------------------------------------------------------- #
# 2. THE ACCRUAL OWNERS - named, never re-implemented
# --------------------------------------------------------------------------- #
#: ``challenger class -> the owner that ACCRUES and MATURES its forward
#: evidence``. This module records the registration and the clock; the
#: arithmetic that turns marks into forward P&L belongs to these owners and is
#: never duplicated here.
EVIDENCE_ACCRUAL_OWNERS = {
    "FORWARD_SIGNAL_R46": "alpha_agent.r46 (emit/judge) advanced by "
                          "alpha_agent.r52.runtime",
    "FORWARD_PAPER_PORTFOLIO": "api.shadow_portfolio_evidence over "
                               "engine.shadow_portfolio_evidence",
    "FORWARD_SIGNAL_CANONICAL": "engine.shadow_portfolio_evidence (the one pure "
                                "weight-book forward accrual kernel), matured "
                                "by alpha_agent.r52.runtime",
}

#: Registration outcomes.
REGISTERED = "REGISTERED"
ALREADY_REGISTERED = "ALREADY_REGISTERED"
REFUSED_LIFECYCLE = "REFUSED_NOT_ADOPTABLE_LIFECYCLE_STATE"
REFUSED_LIFECYCLE_UNKNOWN = "REFUSED_LIFECYCLE_NOT_ESTABLISHED"
REFUSED_IDENTITY = "REFUSED_INCOMPLETE_IDENTITY"
REFUSED_BACKDATED = "REFUSED_BACKDATED_OBSERVATION_CLOCK"
REGISTRATION_OUTCOMES = (REGISTERED, ALREADY_REGISTERED, REFUSED_LIFECYCLE,
                         REFUSED_LIFECYCLE_UNKNOWN, REFUSED_IDENTITY,
                         REFUSED_BACKDATED)

#: Evidence states a REGISTERED challenger can be in. ``PROMOTION_READY`` is
#: deliberately absent: reaching an evidence gate is the gate owner's verdict and
#: a registrar that could award it would be a promotion path.
EV_AWAITING_FIRST_SESSION = "REGISTERED_AWAITING_FIRST_ELIGIBLE_SESSION"
EV_NO_MATURED_YET = "ACCRUING_NO_MATURED_OBSERVATION_YET"
EV_ACCRUING = "FORWARD_EVIDENCE_ACCRUING"
EV_CLOCK_NOT_RESOLVED = "OBSERVATION_CLOCK_NOT_RESOLVED"
EVIDENCE_STATES = (EV_AWAITING_FIRST_SESSION, EV_NO_MATURED_YET, EV_ACCRUING,
                   EV_CLOCK_NOT_RESOLVED)


# --------------------------------------------------------------------------- #
# 3. STORE
# --------------------------------------------------------------------------- #
def registry_dir(registry_dir_override=None) -> Path:
    if registry_dir_override is not None:
        return Path(registry_dir_override)
    env = os.environ.get(REGISTRY_DIR_ENV)
    return Path(env) if env else _DEFAULT_REGISTRY_DIR


def _registrations_dir(registry_dir_override=None) -> Path:
    return registry_dir(registry_dir_override) / _REGISTRATIONS_SUBDIR


def _registration_path(identity_hash: str, registry_dir_override=None) -> Path:
    return _registrations_dir(registry_dir_override) / ("%s.json" % identity_hash)


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
        out = json.loads(path.read_text(encoding="utf-8"))
        return out if isinstance(out, dict) else None
    except (OSError, ValueError):
        return None


def load_registration(identity_hash: str,
                      registry_dir_override=None) -> Optional[dict]:
    return _load_json(_registration_path(identity_hash, registry_dir_override))


def load_registrations(registry_dir_override=None) -> list[dict]:
    """Every registration on disk, oldest first. Read-only."""
    d = _registrations_dir(registry_dir_override)
    if not d.exists():
        return []
    rows = [_load_json(p) for p in sorted(d.glob("*.json"))]
    rows = [r for r in rows if r]
    rows.sort(key=lambda r: (str(r.get("registered_at") or ""),
                             str((r.get("identity") or {}).get("challenger_id")
                                 or "")))
    return rows


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict:
    return {
        "research_only": True,
        "paper_only": True,
        "read_only_for_every_operational_store": True,
        "writes_forward_predictions": False,
        "writes_forward_outcomes": False,
        "computes_pnl": False,
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
        "approved_anything": False,
        "automation_enabled": False,
        "broker_enabled": False,
        "backfilled_forward_evidence": False,
        "rewrote_history": False,
        "manual_review_remains_mandatory": True,
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# 4. THE ONE REGISTRATION OPERATION
# --------------------------------------------------------------------------- #
#: The ONLY lifecycle state that may begin accruing forward evidence. Read from
#: the ONE lifecycle owner so there is never a second rule; the tuple below is a
#: fail-closed default used only when that owner cannot be imported.
_ADOPTABLE_FALLBACK = ("ACTIVE",)


def _adoptable_states() -> tuple:
    try:
        from paper_trader.api import prospective_adoption as PA
        return tuple(PA.ADOPTABLE_STATES)
    except Exception:                                   # noqa: BLE001
        return _ADOPTABLE_FALLBACK


def register_forward_challenger(*, identity: dict,
                                observation_clock_starts: str,
                                challenger_class: str,
                                lifecycle: Optional[dict] = None,
                                freeze_row: Optional[dict] = None,
                                registry_dir_override=None,
                                now: Optional[str] = None) -> dict:
    """Register ONE qualified freeze prospectively. Idempotent, crash-safe.

    Every refusal happens BEFORE the store is touched, and the store write is a
    single atomic replace, so a crash leaves either nothing or a complete
    record - and re-running resolves to the SAME registration rather than a
    second one.

    ``lifecycle`` is the ONE lifecycle owner's verdict
    (:func:`api.prospective_adoption.classify_lifecycle`). It is required: a
    registrar that assumed a freeze was still ACTIVE could resurrect a WITHDRAWN
    one, which is exactly what must be impossible. ``freeze_row`` is an
    alternative - the lifecycle is then asked FROM that owner, never decided
    here.
    """
    ts = now or _now_iso()
    ident = dict(identity or {})
    ident_hash = ident.get("identity_hash")
    base = {
        "owner": COMPOSITION_OWNER, "schema_version": SCHEMA_VERSION,
        "phase": PHASE, "evaluated_at": ts,
        "identity": ident, "challenger_class": challenger_class,
        "outcome_vocabulary": list(REGISTRATION_OUTCOMES),
        "evidence_accrual_owner": EVIDENCE_ACCRUAL_OWNERS.get(challenger_class),
        "safety": _safety(),
    }

    # (a) LIFECYCLE, asked of the ONE owner. Fail closed when unestablished.
    lc = lifecycle
    if lc is None and freeze_row is not None:
        try:
            from paper_trader.api import prospective_adoption as PA
            lc = PA.classify_lifecycle(freeze_row)
        except Exception:                               # noqa: BLE001
            lc = None
    if not isinstance(lc, dict) or not lc.get("lifecycle_state"):
        return {**base, "outcome": REFUSED_LIFECYCLE_UNKNOWN,
                "registered": False,
                "reason": ("no lifecycle verdict was established for this "
                           "freeze; a registrar that assumed ACTIVE could "
                           "resurrect a withdrawn challenger")}
    if lc.get("lifecycle_state") not in _adoptable_states():
        return {**base, "outcome": REFUSED_LIFECYCLE, "registered": False,
                "lifecycle": lc,
                "never_resurrectable": bool(lc.get("never_resurrectable")),
                "reason": ("lifecycle state %s is not adoptable; only %s may "
                           "begin accruing forward evidence"
                           % (lc.get("lifecycle_state"),
                              ", ".join(_adoptable_states())))}

    # (b) IDENTITY. A registration whose challenger, freeze, model or asset
    #     class cannot be named is not a registration.
    missing = [k for k in ("challenger_id", "freeze_id", "asset_class",
                           "identity_hash") if not ident.get(k)]
    if missing:
        return {**base, "outcome": REFUSED_IDENTITY, "registered": False,
                "missing_identity_fields": missing,
                "reason": "the freeze identity does not name %s"
                          % ", ".join(missing)}

    # (c) NO BACKDATING. The clock may never open before the freeze existed.
    inception = str(ident.get("inception") or "")[:10]
    start = str(observation_clock_starts or "")[:10]
    if not start:
        return {**base, "outcome": REFUSED_BACKDATED, "registered": False,
                "reason": "no prospective observation session was declared"}
    if inception and start < inception:
        return {**base, "outcome": REFUSED_BACKDATED, "registered": False,
                "observation_clock_starts": start, "inception": inception,
                "reason": ("the declared observation clock (%s) precedes the "
                           "freeze inception (%s)" % (start, inception))}

    # (d) IDEMPOTENCY. The identity hash IS the key. FIRST WRITE WINS: an
    #     existing registration is returned untouched, never rewritten, so a
    #     repeated adoption of the same exact freeze cannot duplicate it and a
    #     retry after a partial run resolves to the same record.
    existing = load_registration(ident_hash, registry_dir_override)
    if existing:
        return {**base, "outcome": ALREADY_REGISTERED, "registered": True,
                "idempotent": True, "registration": existing,
                "registered_with": COMPOSITION_OWNER,
                "reason": "this exact freeze identity is already registered"}

    clock = resolve_observation_clock(
        asset_class=ident.get("asset_class"), effective_from=start,
        horizon_sessions=ident.get("horizon_sessions"))
    record = {
        "schema_version": SCHEMA_VERSION,
        "owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "registered_at": ts,
        "registration_session": start,
        "identity": ident,
        "challenger_id": ident.get("challenger_id"),
        "freeze_id": ident.get("freeze_id"),
        "freeze_record_hash": ident.get("freeze_record_hash"),
        "model_spec_hash": ident.get("model_spec_hash"),
        "feature_snapshot_hash": ident.get("feature_snapshot_hash"),
        "asset_class": ident.get("asset_class"),
        "instrument_scope": list(ident.get("instrument_scope") or []),
        "horizon_sessions": ident.get("horizon_sessions"),
        "price_mark_owner": ident.get("price_mark_owner"),
        "cost_model": dict(ident.get("cost_model") or {}),
        "lifecycle_at_registration": lc,
        "challenger_class": challenger_class,
        "evidence_accrual_owner": EVIDENCE_ACCRUAL_OWNERS.get(challenger_class),
        "maturation_owner": "alpha_agent.r52.runtime",
        "observation_clock": clock,
        # Registration creates a MEASUREMENT, not evidence. These three are zero
        # at registration and are only ever advanced by the accrual owner.
        "predictions_emitted": 0,
        "matured_observations": 0,
        "pending_observations": 0,
        "effective_independent_observations": 0,
        "forward_observations_at_registration": 0,
        "backfilled": False,
        "records_are_immutable": True,
        "first_write_wins": True,
        "promotion_allowed": False,
        "automatic_promotion_allowed": False,
        "manual_review_required": True,
        "safety": _safety(),
    }
    _atomic_write_json(_registration_path(ident_hash, registry_dir_override),
                       record)
    return {**base, "outcome": REGISTERED, "registered": True,
            "idempotent": False, "registration": record,
            "registered_with": COMPOSITION_OWNER}


# --------------------------------------------------------------------------- #
# 5. THE READ MODEL - what an operator must be able to see (R62.1 workstream H)
# --------------------------------------------------------------------------- #
def evidence_state(record: dict, *, today: Optional[str] = None,
                   accrual: Optional[dict] = None) -> dict:
    """The evidence state of ONE registration. No gate verdict is derived.

    ``accrual`` is the ACCRUAL owner's projection. A registration record is
    immutable, so its own ``matured_observations`` is zero forever; asking this
    function to decide the evidence state from that number alone would report
    ACCRUING_NO_MATURED_OBSERVATION_YET for the rest of the estate's life.
    """
    rec = record or {}
    clock = rec.get("observation_clock") or {}
    first = clock.get("first_eligible_observation_session")
    acc = accrual or {}
    matured = int((acc.get("matured_observations")
                   if acc.get("matured_observations") is not None
                   else rec.get("matured_observations")) or 0)
    now = str(today or datetime.now(timezone.utc).date().isoformat())[:10]
    if clock.get("state") not in (CLOCK_RESOLVED,):
        state = EV_CLOCK_NOT_RESOLVED
        because = clock.get("reason")
    elif matured > 0:
        state = EV_ACCRUING
        because = "%d matured forward observation(s) exist" % matured
    elif first and now < first:
        state = EV_AWAITING_FIRST_SESSION
        because = ("the first eligible observation session is %s; nothing has "
                   "been observed and nothing has been backfilled" % first)
    else:
        state = EV_NO_MATURED_YET
        because = ("the observation clock has opened and no horizon has "
                   "matured yet")
    return {
        "evidence_status": state,
        "evidence_status_vocabulary": list(EVIDENCE_STATES),
        "because": because,
        "next_legitimate_evidence_gate": ("a human governance review; this "
                                          "registrar awards no gate and "
                                          "promotes no model"),
        "promotion_ready": False,
        "promotion_ready_owner": "a human, through the existing governance",
    }


#: What the ACCRUAL owner - never this registrar - is entitled to say about a
#: registration. A registration record is immutable, so its own counters are
#: zero for life by design; the living numbers belong to the owner named in
#: ``evidence_accrual_owner`` and are OVERLAID here, never recomputed here.
ACCRUAL_OVERLAY_FIELDS = ("predictions_emitted", "matured_observations",
                          "pending_observations",
                          "effective_independent_observations",
                          "current_accrual_state", "last_emission_session",
                          "next_eligible_observation_session", "forfeitures",
                          "latest_blocker", "accrual_owner")


def registration_row(record: dict, *, today: Optional[str] = None,
                     accrual: Optional[dict] = None) -> dict:
    """ONE registration projected for the operator surfaces.

    Every field is READ from the persisted record, from the clock the registrar
    already resolved, or - for the counters in :data:`ACCRUAL_OVERLAY_FIELDS` -
    from the accrual owner's own projection supplied by the caller. No browser
    and no read model recomputes a maturity, a lifecycle or an evidence verdict
    from this, and this registrar computes no accrual of its own.
    """
    rec = record or {}
    clock = rec.get("observation_clock") or {}
    lc = rec.get("lifecycle_at_registration") or {}
    overlay = {k: v for k, v in (accrual or {}).items()
               if k in ACCRUAL_OVERLAY_FIELDS}
    return {
        "challenger_id": rec.get("challenger_id"),
        "freeze_id": rec.get("freeze_id"),
        "freeze_record_hash": rec.get("freeze_record_hash"),
        "model_spec_hash": rec.get("model_spec_hash"),
        "feature_snapshot_hash": rec.get("feature_snapshot_hash"),
        "identity_hash": (rec.get("identity") or {}).get("identity_hash"),
        "lifecycle_state": lc.get("lifecycle_state"),
        "asset_class": rec.get("asset_class"),
        "instrument_scope": list(rec.get("instrument_scope") or []),
        "instrument_scope_size": len(rec.get("instrument_scope") or []),
        "horizon_sessions": rec.get("horizon_sessions"),
        "canonical_registrar": rec.get("owner"),
        "evidence_accrual_owner": rec.get("evidence_accrual_owner"),
        "maturation_owner": rec.get("maturation_owner"),
        "registration_timestamp": rec.get("registered_at"),
        "prospective_effective_from": clock.get("effective_from_session"),
        "observation_calendar_owner": clock.get("calendar_owner"),
        "observation_clock_state": clock.get("state"),
        "predictions_emitted": rec.get("predictions_emitted"),
        "matured_observations": rec.get("matured_observations"),
        "pending_observations": rec.get("pending_observations"),
        "effective_independent_observations": rec.get(
            "effective_independent_observations"),
        "next_legitimate_maturity_session": clock.get(
            "next_expected_maturity_session"),
        "first_eligible_observation_session": clock.get(
            "first_eligible_observation_session"),
        "backfilled": bool(rec.get("backfilled")),
        **evidence_state(rec, today=today, accrual=accrual),
        # LAST, so the accrual owner's living counters win over the record's
        # immutable zeros for exactly the fields it owns and no others.
        **overlay,
        "accrual_projection_supplied": bool(accrual),
    }


def load_forward_challenger_registry(*, registry_dir_override=None,
                                     today: Optional[str] = None,
                                     accrual_by_identity: Optional[dict] = None
                                     ) -> dict:
    """The canonical read model over every prospective registration."""
    records = load_registrations(registry_dir_override)
    acc = accrual_by_identity or {}
    rows = [registration_row(
        r, today=today,
        accrual=acc.get(str((r.get("identity") or {}).get("identity_hash"))))
        for r in records]
    by_class: dict = {}
    for r in records:
        by_class.setdefault(str(r.get("challenger_class")), 0)
        by_class[str(r.get("challenger_class"))] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "registry_dir": str(registry_dir(registry_dir_override)),
        "n_registered": len(rows),
        "registrations": rows,
        "registrations_by_class": by_class,
        "observation_calendars": dict(OBSERVATION_CALENDARS),
        "exchange_session_asset_classes": list(EXCHANGE_SESSION_ASSET_CLASSES),
        "evidence_accrual_owners": dict(EVIDENCE_ACCRUAL_OWNERS),
        "registration_outcome_vocabulary": list(REGISTRATION_OUTCOMES),
        "evidence_status_vocabulary": list(EVIDENCE_STATES),
        "records_are_immutable": True,
        "backfill_allowed": False,
        "historical_result_is_never_forward_evidence": True,
        "automatic_promotion_allowed": False,
        "read_only": True,
        "safety": _safety(),
    }


__all__ = [
    "SCHEMA_VERSION", "COMPOSITION_OWNER", "PHASE", "SAFETY_BADGES",
    "REGISTRY_DIR_ENV", "registry_dir",
    "CAL_EXCHANGE_NYSE", "CAL_INSTRUMENT_REALISED", "OBSERVATION_CALENDARS",
    "EXCHANGE_SESSION_ASSET_CLASSES", "CLOCK_STATES", "CLOCK_RESOLVED",
    "CLOCK_INSTRUMENT_OWNED", "CLOCK_CALENDAR_UNAVAILABLE",
    "CLOCK_ASSET_CLASS_UNDECLARED", "observation_calendar_for",
    "next_exchange_session_after", "is_eligible_observation_session",
    "resolve_observation_clock", "EVIDENCE_ACCRUAL_OWNERS",
    "REGISTRATION_OUTCOMES", "REGISTERED", "ALREADY_REGISTERED",
    "REFUSED_LIFECYCLE", "REFUSED_LIFECYCLE_UNKNOWN", "REFUSED_IDENTITY",
    "REFUSED_BACKDATED", "EVIDENCE_STATES", "ACCRUAL_OVERLAY_FIELDS",
    "EV_AWAITING_FIRST_SESSION",
    "EV_NO_MATURED_YET", "EV_ACCRUING", "EV_CLOCK_NOT_RESOLVED",
    "load_registration", "load_registrations", "register_forward_challenger",
    "evidence_state", "registration_row", "load_forward_challenger_registry",
]
