"""api.prospective_adoption - THE governed prospective-adoption owner (R61).

THE DEFECT THIS EXISTS TO REMOVE
--------------------------------
Qualifying a challenger and starting its forward evidence were TWO unrelated
writes in two different stores, performed by a handler that only ever did the
first one. ``alpha_agent.r59.handlers`` wrote the freeze into research memory
and an artifact beside it; nothing then registered the challenger with any
forward-evidence owner. Five freezes therefore sit in the estate's memory as
FORWARD_FROZEN while no owner is accruing a single observation for them - and
because a freeze that accrues nothing looks exactly like a freeze that is
merely young, the gap survived two releases.

That is not a bug in one handler. It is a missing operation: there was no ONE
place where "this candidate is now a prospective challenger" happens, so there
was nowhere for the two halves to be made inseparable.

WHAT THIS MODULE IS
-------------------
The one governed operation for prospective adoption, and the one reconstruction
of a freeze's LIFECYCLE from persisted history. It:

  * decides, from persisted evidence alone, exactly ONE lifecycle state per
    freeze (:data:`LIFECYCLE_STATES`);
  * refuses to adopt anything that is not ACTIVE - a WITHDRAWN or INVALIDATED
    freeze can never be resurrected, by any caller, ever;
  * computes the asset-agnostic identity a forward registration needs, so a
    rates future, an FX future or a cross-asset sleeve is registrable on the
    same contract as an equity and nothing is squeezed into a ticker;
  * makes freeze -> registration RECOVERABLE rather than pretending it is
    atomic: an INTENT is durable BEFORE the registration is attempted, so a
    crash between the two halves leaves a named, resumable record instead of a
    silent orphan. Re-running resolves the intent; it never duplicates it.

WHAT IT IS NOT
--------------
It is NOT a second forward-evidence registry. It writes no forward observation,
no prediction and no outcome; it holds intents and delegates the registration
itself to the canonical owner for the challenger's class
(:data:`REGISTRARS`). A class with no canonical registrar wired is reported as
:data:`NO_CANONICAL_REGISTRAR` and adopted by nobody - fail closed, because a
fabricated registration is worse than a named gap.

RELEASE 62.1 - THE NAMED GAP IS CLOSED
--------------------------------------
R61 shipped with exactly one such gap, and it was real: the two forward owners
of the day (``alpha_agent.r46.registry`` and ``api.shadow_portfolio_evidence``)
each own a FROZEN COHORT rather than a registration, so a qualified R58/R59
freeze had an immutable identity and no owner that would accept it. R62.1 does
not add a third business owner beside them; it generalises the missing operation
into ONE canonical signal-challenger registrar,
:mod:`api.forward_challenger_registry`, and routes every class through this same
table. R46 and R56 keep their cohorts, their ledgers and their evidence exactly
as they are - they are reached here through compatibility adapters, never
rewritten - and every other qualified freeze registers on one asset-agnostic
contract instead of being squeezed into an equity ticker or left invisible.

It promotes no model, activates no sleeve, writes no holding, creates no order
and approves nothing. Adoption starts a measurement; it never starts a position.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

SCHEMA_VERSION = "prospective_adoption.v1"
COMPOSITION_OWNER = "api.prospective_adoption"
PHASE = "R61"

ADOPTION_DIR_ENV = "PAPER_TRADER_PROSPECTIVE_ADOPTION_DIR"
_DEFAULT_ADOPTION_DIR = Path(
    r"D:\Stock_Prediction_app_data\prospective_adoption")

_INTENTS_SUBDIR = "intents"

SAFETY_BADGES = ["RESEARCH ONLY", "PREVIEW ONLY", "MANUAL REVIEW",
                 "NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF"]

#: Adoption is a DELIBERATE act. No GET performs one, and the token is distinct
#: from every approval / promotion token in the estate so it can never satisfy
#: one of those by accident.
ADOPT_CONFIRM_TOKEN = "CONFIRM_PROSPECTIVE_FORWARD_ADOPTION"


# --------------------------------------------------------------------------- #
# 1. LIFECYCLE - reconstructed from persisted history, never assumed
# --------------------------------------------------------------------------- #
#: Frozen, still valid, and entitled to accrue forward evidence.
LC_ACTIVE = "ACTIVE"
#: Withdrawn by its own owner after the freeze - the historical candidate no
#: longer qualifies. R59's Calendar Term Structure challenger is the estate's
#: worked example: withdrawn AT INCEPTION with zero forward observations,
#: because the effective independent sample and the validation materiality both
#: failed once two gate defects were fixed. It must NEVER be resurrected.
LC_WITHDRAWN = "WITHDRAWN"
#: The INPUT the freeze rested on is not what its name claims.
LC_INVALIDATED = "INVALIDATED"
#: A later freeze of the same challenger identity replaced it.
LC_SUPERSEDED = "SUPERSEDED"
#: Forward evidence accrued to the gate this freeze was competing for.
LC_MATURED = "MATURED"
#: Forward evidence accrued and the challenger lost.
LC_FAILED = "FAILED"

LIFECYCLE_STATES = (LC_ACTIVE, LC_WITHDRAWN, LC_INVALIDATED, LC_SUPERSEDED,
                    LC_MATURED, LC_FAILED)

#: The ONLY lifecycle state a prospective adoption may act on.
ADOPTABLE_STATES = (LC_ACTIVE,)

#: Why a lifecycle state was chosen. Every freeze carries one of these, so a
#: classification is always attributable to a persisted fact.
EV_INVALIDATION_RECORDED = "OWNER_RECORDED_AN_INVALIDATION_REASON"
EV_WITHDRAWN_AT_INCEPTION = "OWNER_WITHDREW_THE_FREEZE_AT_INCEPTION"
EV_SUPERSEDED_BY_LATER_FREEZE = "A_LATER_FREEZE_CARRIES_THE_SAME_CHALLENGER_ID"
EV_FORWARD_VERDICT = "A_FORWARD_OWNER_RECORDED_A_TERMINAL_VERDICT"
EV_NO_TERMINAL_EVIDENCE = "NO_PERSISTED_FACT_ENDS_THIS_FREEZE"

#: A withdrawal is recognised from the owner's OWN sentence. These are the
#: fragments R59 actually writes; matching text rather than inventing a new
#: column keeps a historical row classifiable without being rewritten.
_WITHDRAWAL_FRAGMENTS = ("withdrawn at inception", "withdrawn", "no longer "
                         "qualifies")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            out = json.loads(value)
            return out if isinstance(out, dict) else {}
        except ValueError:
            return {}
    return {}


def classify_lifecycle(freeze_row: dict, *,
                       later_freezes_with_same_id: int = 0,
                       forward_verdict: Optional[str] = None) -> dict:
    """Resolve ONE freeze to exactly one lifecycle state, from persisted facts.

    Ordered most-terminal first, because a withdrawn freeze that also happens
    to look young is withdrawn, not young. ``forward_verdict`` is a canonical
    forward owner's own terminal word (``KILLED`` / ``MATURED`` / ...) when it
    has one; this module never derives a verdict of its own.
    """
    row = freeze_row or {}
    invalidated = row.get("invalidated_reason")
    text = str(invalidated or "").lower()
    if invalidated and any(f in text for f in _WITHDRAWAL_FRAGMENTS):
        return {"lifecycle_state": LC_WITHDRAWN,
                "evidence": EV_WITHDRAWN_AT_INCEPTION,
                "detail": str(invalidated),
                "adoptable": False,
                "never_resurrectable": True}
    if invalidated:
        return {"lifecycle_state": LC_INVALIDATED,
                "evidence": EV_INVALIDATION_RECORDED,
                "detail": str(invalidated),
                "adoptable": False,
                "never_resurrectable": True}
    verdict = str(forward_verdict or "").upper()
    if verdict in ("KILLED", "DEGRADED", "LOSING_TERMINAL", "FAILED"):
        return {"lifecycle_state": LC_FAILED, "evidence": EV_FORWARD_VERDICT,
                "detail": "a forward owner recorded %s" % verdict,
                "adoptable": False, "never_resurrectable": False}
    if verdict in ("MATURED", "FORWARD_GATE_REACHED", "PROMOTION_READY"):
        return {"lifecycle_state": LC_MATURED, "evidence": EV_FORWARD_VERDICT,
                "detail": "a forward owner recorded %s" % verdict,
                "adoptable": False, "never_resurrectable": False}
    if later_freezes_with_same_id > 0:
        return {"lifecycle_state": LC_SUPERSEDED,
                "evidence": EV_SUPERSEDED_BY_LATER_FREEZE,
                "detail": "%d later freeze(s) carry this challenger identity"
                          % later_freezes_with_same_id,
                "adoptable": False, "never_resurrectable": False}
    return {"lifecycle_state": LC_ACTIVE, "evidence": EV_NO_TERMINAL_EVIDENCE,
            "detail": "no persisted fact withdraws, invalidates, supersedes or "
                      "concludes this freeze",
            "adoptable": True, "never_resurrectable": False}


# --------------------------------------------------------------------------- #
# 2. THE ADOPTION IDENTITY - asset-agnostic by construction
# --------------------------------------------------------------------------- #
#: The identity a forward registration must carry. Deliberately free of every
#: equity assumption: there is no ``ticker`` field, ``instrument_scope`` is a
#: LIST of instrument identifiers whatever they name (a symbol, a contract
#: root, a currency pair, a rate tenor, a volatility surface point), and the
#: asset class travels as data. A futures, FX or cross-asset challenger is
#: registrable on exactly this contract without a single special case.
IDENTITY_FIELDS = (
    "challenger_id", "release", "asset_class", "instrument_scope",
    "venue", "sleeve", "economic_family", "model_family", "horizon_sessions",
    "freeze_id", "freeze_record_hash", "model_spec_hash",
    "feature_snapshot_hash", "inception", "cost_model", "price_mark_owner",
)

#: Asset classes the adoption contract is proven against. This is a COVERAGE
#: statement, not a filter: an unknown asset class adopts on the same contract
#: and is simply not yet covered by a test.
PROVEN_ASSET_CLASSES = ("US_EQUITY", "EQUITY_INDEX_FUTURES", "RATES_FUTURES",
                        "COMMODITY_FUTURES", "FX_FUTURES", "VOLATILITY",
                        "CROSS_ASSET")

#: What prices a challenger's forward observations, per asset class. Named
#: because a forward observation whose mark owner is unknown is not evidence.
DEFAULT_MARK_OWNERS = {
    "US_EQUITY": "api.price_panel",
    "EQUITY_INDEX_FUTURES": "alpha_agent.r38.native_contract_layer",
    "RATES_FUTURES": "alpha_agent.r38.native_contract_layer",
    "COMMODITY_FUTURES": "alpha_agent.r38.native_contract_layer",
    "FX_FUTURES": "alpha_agent.r38.native_contract_layer",
    "VOLATILITY": "alpha_agent.r38.native_contract_layer",
    "CROSS_ASSET": "alpha_agent.r38.native_contract_layer",
}


def _stable_hash(obj: Any) -> str:
    import hashlib
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def build_adoption_identity(freeze_row: dict, *,
                            instrument_scope: Optional[list] = None,
                            venue: Optional[str] = None,
                            sleeve: Optional[str] = None,
                            cost_model: Optional[dict] = None,
                            mark_owner: Optional[str] = None) -> dict:
    """The EXACT identity this adoption registers, asset-agnostic.

    Every value is read from the freeze the research owner persisted; nothing
    is defaulted into existence except the mark owner, which is looked up from
    a declared table by asset class and is ``None`` when the class is unknown.
    """
    row = freeze_row or {}
    challenger = _as_dict(row.get("forward_challenger"))
    spec = _as_dict(row.get("spec_json")) or _as_dict(row.get("spec"))
    ac = row.get("asset_class")
    scope = list(instrument_scope or spec.get("instrument_scope")
                 or spec.get("instruments") or [])
    identity = {
        "challenger_id": challenger.get("challenger_id"),
        "release": row.get("release"),
        "asset_class": ac,
        # A LIST, always. One equity is a scope of one; a curve sleeve is a
        # scope of many; an empty scope is declared empty rather than guessed.
        "instrument_scope": [str(x) for x in scope],
        "venue": venue or spec.get("venue"),
        "sleeve": sleeve or spec.get("sleeve") or row.get("economic_family"),
        "economic_family": row.get("economic_family"),
        "model_family": row.get("model_family"),
        "horizon_sessions": row.get("horizon_sessions"),
        "freeze_id": row.get("hypothesis_id"),
        "freeze_record_hash": challenger.get("record_hash"),
        "model_spec_hash": _stable_hash(spec) if spec else None,
        "feature_snapshot_hash": (spec.get("feature_snapshot_hash")
                                  or row.get("input_data_identity")),
        "inception": challenger.get("inception"),
        "cost_model": dict(cost_model or spec.get("cost_model") or {}),
        "price_mark_owner": mark_owner or DEFAULT_MARK_OWNERS.get(str(ac or "")),
    }
    identity["identity_hash"] = _stable_hash(
        {k: identity.get(k) for k in IDENTITY_FIELDS})
    return identity


# --------------------------------------------------------------------------- #
# 3. THE REGISTRARS - canonical forward-evidence owners, never re-implemented
# --------------------------------------------------------------------------- #
#: ``challenger class -> the canonical owner that accrues its forward evidence``.
#: A class absent from this table has no canonical registrar, and this module
#: refuses rather than inventing one. That refusal IS the architecture report:
#: it names precisely which challenger classes still have no forward home.
REGISTRARS: dict[str, str] = {
    "FORWARD_PAPER_PORTFOLIO": "api.shadow_portfolio_evidence",
    "FORWARD_SIGNAL_R46": "alpha_agent.r46.registry",
    # R62.1 - the canonical SIGNAL-challenger forward owner. Every qualified
    # freeze that is not a member of one of the two frozen cohorts above
    # registers here, on one asset-agnostic contract.
    "FORWARD_SIGNAL_CANONICAL": "api.forward_challenger_registry",
}

#: How a freeze's class is decided, from what the freeze itself records.
CLASS_PAPER_PORTFOLIO = "FORWARD_PAPER_PORTFOLIO"
CLASS_SIGNAL_R46 = "FORWARD_SIGNAL_R46"
#: R62.1 - the canonical class. A signal challenger whose forward evidence the
#: canonical registrar owns end to end.
CLASS_SIGNAL_CANONICAL = "FORWARD_SIGNAL_CANONICAL"
#: The R61 fail-closed answer, RETAINED. It is no longer reached by any release
#: the estate holds, and it must stay reachable: if a class is ever mapped to no
#: registrar again, the machinery that records a named, resumable gap instead of
#: inventing an owner is the thing that kept five orphans from being invisible.
CLASS_SIGNAL_UNREGISTERED = "FORWARD_SIGNAL_NO_CANONICAL_REGISTRAR"


def classify_challenger_class(freeze_row: dict) -> str:
    """Which forward-evidence owner OUGHT to accrue this freeze.

    R46 and R56 name their own cohort owners because those cohorts are frozen
    contracts that predate the canonical registrar and may never be edited.
    EVERY other qualified freeze - R58's four, R59's, and whatever a future
    release freezes - is the canonical registrar's, whatever its asset class.
    """
    row = freeze_row or {}
    release = str(row.get("release") or "").upper()
    if release == "R56":
        return CLASS_PAPER_PORTFOLIO
    if release == "R46":
        return CLASS_SIGNAL_R46
    return CLASS_SIGNAL_CANONICAL


# --------------------------------------------------------------------------- #
# 4. OUTCOMES
# --------------------------------------------------------------------------- #
ADOPTED = "ADOPTED"
ALREADY_ADOPTED = "ALREADY_ADOPTED"
REFUSED_LIFECYCLE = "REFUSED_NOT_ADOPTABLE_LIFECYCLE_STATE"
REFUSED_BACKDATED = "REFUSED_BACKDATED_OBSERVATION_CLOCK"
REFUSED_IDENTITY = "REFUSED_INCOMPLETE_IDENTITY"
REFUSED_CONFIRMATION = "ADOPTION_CONFIRMATION_REQUIRED"
NO_CANONICAL_REGISTRAR = "NO_CANONICAL_REGISTRAR"
REGISTRAR_FAILED = "REGISTRAR_FAILED_INTENT_RECOVERABLE"

ADOPTION_OUTCOMES = (ADOPTED, ALREADY_ADOPTED, REFUSED_LIFECYCLE,
                     REFUSED_BACKDATED, REFUSED_IDENTITY, REFUSED_CONFIRMATION,
                     NO_CANONICAL_REGISTRAR, REGISTRAR_FAILED)

#: Intent phases. The INTENT is durable before the registrar is called, so the
#: window in which a crash could orphan a freeze is a window in which a NAMED,
#: resumable record exists on disk instead.
INTENT_OPEN = "OPEN"
INTENT_COMMITTED = "COMMITTED"
INTENT_ABANDONED = "ABANDONED"
INTENT_PHASES = (INTENT_OPEN, INTENT_COMMITTED, INTENT_ABANDONED)


def adoption_dir(adoption_dir_override=None) -> Path:
    if adoption_dir_override is not None:
        return Path(adoption_dir_override)
    env = os.environ.get(ADOPTION_DIR_ENV)
    return Path(env) if env else _DEFAULT_ADOPTION_DIR


def _intents_dir(adoption_dir_override=None) -> Path:
    return adoption_dir(adoption_dir_override) / _INTENTS_SUBDIR


def _intent_path(identity_hash: str, adoption_dir_override=None) -> Path:
    return _intents_dir(adoption_dir_override) / ("%s.json" % identity_hash)


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


def load_intent(identity_hash: str, adoption_dir_override=None) -> Optional[dict]:
    """The durable adoption intent for one identity, or None."""
    return _load_json(_intent_path(identity_hash, adoption_dir_override))


def open_intents(adoption_dir_override=None) -> list[dict]:
    """Every intent that was written and never committed - the resumable set.

    A non-empty list after a clean run is the ONLY honest way to report "a
    freeze was taken but its forward evidence has not started"; before R61 that
    state had no record at all and was therefore invisible.
    """
    d = _intents_dir(adoption_dir_override)
    if not d.exists():
        return []
    rows = [_load_json(p) for p in sorted(d.glob("*.json"))]
    return [r for r in rows if r and r.get("phase") == INTENT_OPEN]


# --------------------------------------------------------------------------- #
# 5. THE ONE GOVERNED OPERATION
# --------------------------------------------------------------------------- #
def _safety() -> dict:
    return {
        "research_only": True,
        "paper_only": True,
        "promoted_model": False,
        "automatic_model_promotion_allowed": False,
        "activated_sleeve": False,
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
        "safety_badges": list(SAFETY_BADGES),
    }


def adopt_prospective_freeze(*, freeze_row: dict,
                             observation_clock_starts: str,
                             confirm: Optional[str] = None,
                             lifecycle: Optional[dict] = None,
                             instrument_scope: Optional[list] = None,
                             venue: Optional[str] = None,
                             sleeve: Optional[str] = None,
                             cost_model: Optional[dict] = None,
                             mark_owner: Optional[str] = None,
                             registrar: Optional[Callable] = None,
                             adoption_dir_override=None,
                             registry_dir_override=None,
                             now: Optional[str] = None) -> dict:
    """Adopt ONE prospective freeze into forward evidence. Idempotent.

    ``observation_clock_starts`` is the session from which forward observations
    may legitimately begin. It is the CALLER's declaration of what is available
    prospectively RIGHT NOW, and it is refused when it precedes the freeze's own
    inception - an adoption may only ever look forward. Sessions between an old
    inception and today are NOT synthesised, and this operation writes no
    observation of any kind: point-in-time integrity is absolute.

    ``registrar`` is an injectable seam over the canonical owner (so a test
    never touches a production store); production resolves it from
    :data:`REGISTRARS` by the freeze's class.
    """
    ts = now or _now_iso()
    if confirm != ADOPT_CONFIRM_TOKEN:
        return {"owner": COMPOSITION_OWNER, "outcome": REFUSED_CONFIRMATION,
                "adopted": False, "confirm_required_token": ADOPT_CONFIRM_TOKEN,
                "safety": _safety()}

    lc = lifecycle or classify_lifecycle(freeze_row)
    identity = build_adoption_identity(
        freeze_row, instrument_scope=instrument_scope, venue=venue,
        sleeve=sleeve, cost_model=cost_model, mark_owner=mark_owner)
    klass = classify_challenger_class(freeze_row)
    base = {
        "owner": COMPOSITION_OWNER, "schema_version": SCHEMA_VERSION,
        "phase": PHASE, "evaluated_at": ts,
        "identity": identity, "lifecycle": lc,
        "challenger_class": klass,
        "canonical_registrar": REGISTRARS.get(klass),
        "outcome_vocabulary": list(ADOPTION_OUTCOMES),
        "lifecycle_vocabulary": list(LIFECYCLE_STATES),
        "safety": _safety(),
    }

    # (a) LIFECYCLE. A withdrawn or invalidated freeze is refused here, before
    #     any store is touched, by every caller, forever.
    if lc.get("lifecycle_state") not in ADOPTABLE_STATES:
        return {**base, "outcome": REFUSED_LIFECYCLE, "adopted": False,
                "reason": ("lifecycle state %s is not adoptable; only %s may "
                           "begin accruing forward evidence"
                           % (lc.get("lifecycle_state"),
                              ", ".join(ADOPTABLE_STATES))),
                "never_resurrectable": bool(lc.get("never_resurrectable"))}

    # (b) IDENTITY. A registration whose challenger, freeze or model cannot be
    #     named is not a registration.
    missing = [k for k in ("challenger_id", "freeze_id", "asset_class")
               if not identity.get(k)]
    if missing:
        return {**base, "outcome": REFUSED_IDENTITY, "adopted": False,
                "missing_identity_fields": missing,
                "reason": "the freeze does not name %s" % ", ".join(missing)}

    # (c) NO BACKDATING. The clock starts when the evidence becomes legitimately
    #     observable, never at an inception that has already passed.
    inception = str(identity.get("inception") or "")[:10]
    start = str(observation_clock_starts or "")[:10]
    if not start:
        return {**base, "outcome": REFUSED_BACKDATED, "adopted": False,
                "reason": "no prospective observation session was declared"}
    if inception and start < inception:
        return {**base, "outcome": REFUSED_BACKDATED, "adopted": False,
                "observation_clock_starts": start, "inception": inception,
                "reason": ("the declared observation clock (%s) precedes the "
                           "freeze inception (%s); forward evidence may only "
                           "ever start after the instant it was frozen"
                           % (start, inception))}

    # (d) IDEMPOTENCY. The identity hash IS the key, so the same logical
    #     adoption re-run - after a crash, a retry or a duplicate mandate -
    #     resolves the SAME intent and never a second registration.
    ident_hash = identity["identity_hash"]
    existing = load_intent(ident_hash, adoption_dir_override)
    if existing and existing.get("phase") == INTENT_COMMITTED:
        return {**base, "outcome": ALREADY_ADOPTED, "adopted": True,
                "idempotent": True, "intent": existing,
                "reason": "this exact freeze identity is already registered"}

    # (e) REGISTRAR. Resolved by class; absent means refused, never invented.
    fn = registrar
    if fn is None:
        fn = _resolve_registrar(
            klass, lifecycle=lc, registry_dir_override=registry_dir_override)
    if fn is None:
        # An OPEN intent is still written: the estate must be able to SEE that
        # a qualified freeze is waiting for a forward home. That visibility is
        # the whole point - an unrecorded gap is how five orphans survived.
        intent = {
            "schema_version": SCHEMA_VERSION, "owner": COMPOSITION_OWNER,
            "phase": INTENT_OPEN, "opened_at": ts, "identity": identity,
            "challenger_class": klass, "lifecycle": lc,
            "observation_clock_starts": start,
            "blocked_reason": NO_CANONICAL_REGISTRAR,
            "detail": ("no canonical forward-evidence owner is declared for "
                       "challenger class %s; this freeze is recorded as an "
                       "OPEN adoption intent rather than registered by an "
                       "owner that does not exist" % klass),
        }
        _atomic_write_json(_intent_path(ident_hash, adoption_dir_override),
                           intent)
        return {**base, "outcome": NO_CANONICAL_REGISTRAR, "adopted": False,
                "intent": intent, "intent_is_resumable": True,
                "reason": intent["detail"]}

    # (f) THE RECOVERABLE PROTOCOL. Intent first, registration second, commit
    #     third. True cross-store atomicity does not exist here and is not
    #     claimed: what IS guaranteed is that every state a crash can leave
    #     behind is named on disk and resolvable by re-running this call.
    intent = {
        "schema_version": SCHEMA_VERSION, "owner": COMPOSITION_OWNER,
        "phase": INTENT_OPEN, "opened_at": ts, "identity": identity,
        "challenger_class": klass, "registrar": REGISTRARS.get(klass),
        "lifecycle": lc, "observation_clock_starts": start,
    }
    _atomic_write_json(_intent_path(ident_hash, adoption_dir_override), intent)
    try:
        registration = fn(identity=identity,
                          observation_clock_starts=start,
                          challenger_class=klass)
    except Exception as exc:  # noqa: BLE001 - a failed registrar leaves the
        # intent OPEN, which is exactly the resumable state it should leave.
        return {**base, "outcome": REGISTRAR_FAILED, "adopted": False,
                "intent": intent, "intent_is_resumable": True,
                "reason": "the canonical registrar failed: %s" % str(exc)[:200]}
    committed = {**intent, "phase": INTENT_COMMITTED, "committed_at": _now_iso(),
                 "registration": registration}
    _atomic_write_json(_intent_path(ident_hash, adoption_dir_override),
                       committed)
    return {**base, "outcome": ADOPTED, "adopted": True, "idempotent": False,
            "intent": committed, "registration": registration}


def _resolve_registrar(challenger_class: str, *,
                       lifecycle: Optional[dict] = None,
                       registry_dir_override=None) -> Optional[Callable]:
    """The canonical owner's own registration entry point, or None.

    Resolution is by NAME from :data:`REGISTRARS` and the callable is the
    owner's own; this module implements no registration of its own and never
    falls back to one. The two cohort owners are reached through COMPATIBILITY
    ADAPTERS that only ever READ their frozen contracts - R46's challenger
    cohort and R56's session cohort are never edited from here.
    """
    dotted = REGISTRARS.get(challenger_class)
    if not dotted:
        return None
    if challenger_class == CLASS_SIGNAL_CANONICAL:
        def _canonical(*, identity, observation_clock_starts, challenger_class):
            from paper_trader.api import forward_challenger_registry as FCR
            out = FCR.register_forward_challenger(
                identity=identity,
                observation_clock_starts=observation_clock_starts,
                challenger_class=challenger_class,
                # The lifecycle verdict travels WITH the registration. The
                # registrar re-checks it rather than trusting the caller, so a
                # withdrawn freeze is refused twice and resurrected never.
                lifecycle=lifecycle,
                registry_dir_override=registry_dir_override)
            if not out.get("registered"):
                raise RuntimeError(
                    "%s refused the registration: %s"
                    % (dotted, out.get("reason") or out.get("outcome")))
            return {"registered_with": dotted,
                    "outcome": out.get("outcome"),
                    "already_present": bool(out.get("idempotent")),
                    "registration": out.get("registration")}
        return _canonical
    if challenger_class == CLASS_PAPER_PORTFOLIO:
        def _shadow(*, identity, observation_clock_starts, challenger_class):
            from paper_trader.api import shadow_portfolio_evidence as r56
            records = r56.load_records()
            cid = identity.get("challenger_id")
            for rec in records or []:
                if str(rec.get("challenger_id")) == str(cid):
                    return {"registered_with": dotted, "already_present": True,
                            "record_hash": rec.get("record_hash")}
            # The R56 owner freezes a whole session's cohort from portfolio
            # inputs; it is never asked to invent a record for one challenger.
            raise RuntimeError(
                "api.shadow_portfolio_evidence registers a session cohort, not "
                "a single challenger; %s is not in its records" % cid)
        return _shadow
    if challenger_class == CLASS_SIGNAL_R46:
        def _r46(*, identity, observation_clock_starts, challenger_class):
            from paper_trader.api import prospective_tournament as r46
            board = r46.load_prospective_tournament()
            cid = str(identity.get("challenger_id"))
            for key in ("which_are_winning", "which_are_losing",
                        "which_are_too_early_to_judge", "which_were_killed",
                        "which_are_data_blocked"):
                for row in (board or {}).get(key) or []:
                    if str(row.get("challenger_id")) == cid:
                        return {"registered_with": dotted,
                                "already_present": True, "board_lane": key}
            raise RuntimeError(
                "the R46 challenger cohort is a frozen contract; %s is not in "
                "it and this module never edits that contract" % cid)
        return _r46
    return None


__all__ = [
    "SCHEMA_VERSION", "COMPOSITION_OWNER", "PHASE", "SAFETY_BADGES",
    "ADOPT_CONFIRM_TOKEN", "ADOPTION_DIR_ENV", "adoption_dir",
    "LIFECYCLE_STATES", "ADOPTABLE_STATES", "LC_ACTIVE", "LC_WITHDRAWN",
    "LC_INVALIDATED", "LC_SUPERSEDED", "LC_MATURED", "LC_FAILED",
    "classify_lifecycle", "IDENTITY_FIELDS", "PROVEN_ASSET_CLASSES",
    "DEFAULT_MARK_OWNERS", "build_adoption_identity", "REGISTRARS",
    "CLASS_PAPER_PORTFOLIO", "CLASS_SIGNAL_R46", "CLASS_SIGNAL_CANONICAL",
    "CLASS_SIGNAL_UNREGISTERED",
    "classify_challenger_class", "ADOPTION_OUTCOMES", "ADOPTED",
    "ALREADY_ADOPTED", "REFUSED_LIFECYCLE", "REFUSED_BACKDATED",
    "REFUSED_IDENTITY", "REFUSED_CONFIRMATION", "NO_CANONICAL_REGISTRAR",
    "REGISTRAR_FAILED", "INTENT_PHASES", "INTENT_OPEN", "INTENT_COMMITTED",
    "INTENT_ABANDONED", "load_intent", "open_intents",
    "adopt_prospective_freeze",
]
