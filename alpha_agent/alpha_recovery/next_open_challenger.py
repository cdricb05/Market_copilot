r"""alpha_agent.alpha_recovery.next_open_challenger - ONE new frozen challenger
that acts on the SAME information at the NEXT session's open, and therefore
needs no live data entitlement at all.

WHY IT EXISTS, AND WHY IT IS NOT A RESCUE
    ``REVERSED_SPY_PUT_CALL_SKEW_H5`` forms its decision from the 15:45 ET option
    snapshot on session ``t`` and enters at the 16:00 ET close on the SAME
    session. Those fifteen minutes are the only reason a LIVE OPRA subscription
    would be needed: the historical API publishes session ``t`` once ``t`` has
    ended - too late for ``t``'s own close, and in good time for the next open.

    ``entry_delay_diagnostic`` measured what those fifteen minutes are worth,
    against two declared boundaries and no others. Entering at the next session's
    open retained 98.7 % of the frozen net return (94.8 % against the Norgate
    source control) with a HIGHER t, a HIGHER Sharpe and a SMALLER drawdown. The
    boundary was therefore chosen by a completed diagnostic, BEFORE this module
    existed, and this module searches nothing: there is no alternate entry time
    here, no alternate sign, lookback, horizon or threshold, and none may be
    added.

WHAT IS INHERITED AND WHAT IS NOT - the distinction the whole module turns on
    INHERITED (by IMPORT, never retyped): the feature, its definition, the sign,
    the z-score and its lookback, the moneyness / expiry / parity / implied
    volatility construction, the strike band, the snapshot minute, the cost
    ladder and every frozen gate. They are read out of
    ``reversed_skew.frozen_specification()`` field by field, so the two
    challengers can never disagree about what the signal IS.

    NOT INHERITED, and deliberately: every piece of EVIDENCE. This challenger
    starts with historical qualification NONE, zero TRUE_FORWARD observations and
    capital eligibility NO. The +26.65 %/yr next-open figure was measured on the
    sample the sign was read off; it sized this hypothesis and it cannot test it.
    A challenger that inherited the incumbent's confirmation status would be
    claiming evidence for an entry boundary that no independent data has ever
    seen.

THE FIVE THINGS A FORWARD-EVIDENCE OWNER MUST KEEP APART
    information_session   the session whose 15:45 ET snapshot forms the decision
    decision_timestamp    when the decision was frozen - after t's data
                          published, before t+1 opened
    entry_session         the NEXT eligible session, whose OPEN is the mark
    entry_boundary        that open, named as data so it cannot be assumed
    maturity_session      five eligible sessions counted FROM THE ENTRY session

    They are different sessions and this module never lets them collapse into
    one. ``decision_context`` is carried INTO the frozen decision record, so a
    downstream reader cannot mistake the signal session for the entry session.

THE DATA DEPENDENCY THAT REPLACED THE ENTITLEMENT
    Removing the live requirement does not remove every requirement: session
    ``t``'s historical OPRA snapshot must be PUBLISHED before session ``t+1``
    opens. That is a vendor latency, it has never been measured on a weekday in
    this campaign, and it is not assumed here. ``opra_publication_probe`` owns
    the measurement; this module reads its answer and refuses to treat session
    ``t`` as available until the vendor says it is.

RESEARCH ONLY. Freezing a decision starts a MEASUREMENT. No promotion, no
capital, no order, no fill, no purchase, no subscription, no backfill.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

from paper_trader.engine import exchange_calendar as EC
from paper_trader.engine import market_hours as MH

from . import CAMPAIGN_ID, MIN_EFFECTIVE_PERIODS, now_iso, stable_hash, write_artifact
from . import forward_package as FP
from . import options_acquisition as OA
from . import options_surface as OS
from . import prospective_decision as PD
from . import reversed_skew as RS

CALCULATION_OWNER = "alpha_agent.alpha_recovery.next_open_challenger"
ARTIFACT_NAME = "next_open_challenger.json"

CHALLENGER_ID = "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"
RELEASE = RS.RELEASE
ASSET_CLASS = RS.ASSET_CLASS
VENUE = RS.VENUE
MARK_OWNER = RS.MARK_OWNER

#: The challenger this one shares a SIGNAL with and shares no EVIDENCE with.
SIBLING_CHALLENGER_ID = RS.CHALLENGER_ID

INCEPTION = "2026-09-12"

# --------------------------------------------------------------------------- #
# 1. THE ONE THING THAT DIFFERS
# --------------------------------------------------------------------------- #
#: The entry mark, named as data. 09:30 ET is the regular-session open, which is
#: the boundary the diagnostic measured - not "some time in the morning".
ENTRY_MARK_ET = (9, 30)

#: When a decision for an entry session may first be written. Midnight
#: exchange-local on the ENTRY session: by then the information session has
#: closed completely, so the emitter can see everything the rule is defined on
#: and nothing it will be scored against. The window is 9.5 hours wide ON
#: PURPOSE - a boundary that required a human at a particular minute would be a
#: boundary that gets missed.
DECISION_WINDOW_OPENS_ET = (0, 0)

#: The instant the information itself is complete, on the INFORMATION session.
#: Taken from the acquisition owner so it is the same 15:45 the surface was
#: built from.
INFORMATION_CUTOFF_ET = OA.SNAPSHOT_ET

EXECUTION_BOUNDARY = "NEXT_ELIGIBLE_SESSION_OPEN"
HORIZON_COUNTS_FROM = "ENTRY_SESSION"

#: What a live OPRA subscription would have cost, and what this challenger costs
#: instead. Stated as a number so no reader has to infer it.
NEW_SUBSCRIPTION_COST_USD = 0.0

#: The completed diagnostic that CHOSE this boundary. Recorded as provenance,
#: never as evidence.
BOUNDARY_SELECTED_BY = {
    "owner": "alpha_agent.alpha_recovery.entry_delay_diagnostic",
    "release": "R62.3.2",
    "measured": "two declared delayed boundaries and no others: next-session "
                "open and next-session close",
    "why_next_open": "it retained the most of the frozen edge (98.7 % against "
                     "the frozen baseline, 94.8 % against the Norgate source "
                     "control) with a higher t, a higher Sharpe and a smaller "
                     "drawdown",
    "no_entry_time_was_searched": True,
    "this_module_searches_nothing": True,
}

# --------------------------------------------------------------------------- #
# 2. WHAT IS INHERITED, AND WHAT IS REFUSED
# --------------------------------------------------------------------------- #
#: Every field of ``reversed_skew.frozen_specification()`` that describes the
#: SIGNAL. Copied verbatim, so the two challengers cannot drift apart about what
#: the feature is.
INHERITED_CONSTRUCTION_KEYS = (
    "underlying", "asset_class", "venue", "information_source",
    "feature", "feature_definition", "sign", "sign_is_reversed_relative_to",
    "horizon_sessions", "position", "zscore_lookback", "zscore_construction",
    "cadence", "overlapping", "snapshot_et", "moneyness_construction",
    "expiry_construction", "strike_band", "quote_schema", "implied_volatility",
    "cost_ladder_bps_per_side", "cost_primary_bps_per_side",
    "cost_stress_bps_per_side", "risk_sizing", "equal_risk_comparison",
    "qualification_gates", "incumbent", "price_mark_owner", "not_optimised",
    "protocol_sha256", "contract_sha256",
)

#: Every field that is EVIDENCE, IDENTITY or an EXECUTION RULE, and is therefore
#: replaced rather than copied. Listing them explicitly is what makes the
#: refusal auditable: a field added to the sibling specification later belongs in
#: exactly one of these two tuples, and a test fails until someone decides which.
NOT_INHERITED_KEYS = (
    "schema", "challenger_id", "inception", "emission_rule",
    "discovery_sample", "discovery_sample_is_qualification_evidence",
    "discovery_disclosure",
)

#: Why the diagnostic numbers may never be quoted as this challenger's evidence.
EVIDENCE_DISCLOSURE = (
    "THE NEXT-OPEN RESULT IS DIAGNOSTIC ONLY. It was measured on the %s -> %s "
    "sample, which is the sample the SIGN was discovered on, so its level "
    "inherits the whole post-hoc discovery disclosure and its retention ratio - "
    "however stable - is a comparison between two readings of one sample. This "
    "challenger therefore begins with NO historical qualification, NO inherited "
    "confirmation status and ZERO forward observations. Only sessions that did "
    "not exist when this specification was frozen can test it."
    % RS.DISCOVERY_SAMPLE)

EVIDENCE_AT_INCEPTION = {
    "historical_qualification": "NONE",
    "historical_qualification_inherited_from_sibling": False,
    "independent_historical_confirmation_inherited": False,
    "discovery_evidence_inherited": False,
    "true_forward_observations": 0,
    "capital_eligible": False,
    "promotion_allowed": False,
    "why": EVIDENCE_DISCLOSURE,
}

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "creates_orders": False,
    "creates_fills": False,
    "promotes_model": False,
    "allocates_capital": False,
    "activates_sleeve": False,
    "backfill_allowed": False,
    "records_are_immutable": True,
    "modifies_the_sibling_challenger": False,
    "purchases_data": False,
    "starts_trial_or_subscription": False,
    "new_subscription_cost_usd": NEW_SUBSCRIPTION_COST_USD,
    "requires_live_market_data": False,
}


# --------------------------------------------------------------------------- #
# 3. THE CALENDAR
# --------------------------------------------------------------------------- #
def _d(value) -> Optional[date]:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def is_eligible_session(value) -> bool:
    """Did the exchange trade on ``value``?

    Delegated to :mod:`engine.exchange_calendar`, the authoritative rule-based
    NYSE calendar this estate already resolves observation clocks on. An
    UNSUPPORTED date is not an eligible session here - the calendar declines to
    call it a closure, and this module declines to call it a trading day, which
    is the fail-closed reading.
    """
    d = _d(value)
    if d is None or not EC.is_supported(d):
        return False
    return not EC.is_non_session(d)


def next_eligible_session(session, n: int = 1) -> Optional[str]:
    """The ``n``-th eligible session STRICTLY AFTER ``session``."""
    d = _d(session)
    if d is None or n < 1:
        return None
    found = 0
    for _ in range(400):
        d = d + timedelta(days=1)
        if not EC.is_supported(d):
            return None
        if is_eligible_session(d):
            found += 1
            if found == n:
                return d.isoformat()
    return None                                       # pragma: no cover - defensive


def previous_eligible_session(session, n: int = 1) -> Optional[str]:
    """The ``n``-th eligible session STRICTLY BEFORE ``session``."""
    d = _d(session)
    if d is None or n < 1:
        return None
    found = 0
    for _ in range(400):
        d = d - timedelta(days=1)
        if not EC.is_supported(d):
            return None
        if is_eligible_session(d):
            found += 1
            if found == n:
                return d.isoformat()
    return None                                       # pragma: no cover - defensive


def _et_instant(session, hhmm: tuple) -> Optional[str]:
    """``(session, exchange-local wall clock)`` -> an aware UTC ISO instant.

    The timezone is resolved by :mod:`engine.market_hours`, the ONE owner of the
    Eastern clock, so a DST transition is handled in one place for the estate.
    """
    d = _d(session)
    if d is None:
        return None
    naive = datetime.combine(d, time(int(hhmm[0]), int(hhmm[1])))
    return MH.to_eastern(naive).astimezone(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# 4. THE FIVE SESSIONS, NEVER COLLAPSED
# --------------------------------------------------------------------------- #
def information_session_for(entry_session) -> Optional[str]:
    """The session whose 15:45 ET snapshot decides ``entry_session``.

    Exactly one eligible session before it. A Tuesday entry is decided by
    Monday's snapshot; a Tuesday entry after a Monday holiday is decided by the
    preceding Friday's, because the calendar - not the weekday - defines "next".
    """
    return previous_eligible_session(entry_session, 1)


def entry_session_for(information_session) -> Optional[str]:
    """The session this challenger enters on: the NEXT eligible one."""
    return next_eligible_session(information_session, 1)


def maturity_session_for(entry_session) -> Optional[str]:
    """Five eligible sessions counted FROM THE ENTRY session.

    The horizon is the frozen one and it is not re-declared here; only its
    STARTING POINT differs from the sibling, and that difference is the entire
    change this challenger makes.
    """
    return next_eligible_session(entry_session, int(RS.FROZEN_HORIZON))


def decision_context(entry_session) -> dict:
    """The whole vocabulary for ONE decision, as data.

    This is what travels INTO the frozen decision record. A reader holding the
    record can then answer "which session formed this?" and "which session was it
    entered on?" separately, without inferring either.
    """
    info = information_session_for(entry_session)
    entry = _d(entry_session)
    return {
        "challenger_id": CHALLENGER_ID,
        "information_session": info,
        "information_cutoff_et": list(INFORMATION_CUTOFF_ET),
        "information_cutoff_at": _et_instant(info, INFORMATION_CUTOFF_ET),
        "entry_session": entry.isoformat() if entry else None,
        "entry_boundary": EXECUTION_BOUNDARY,
        "entry_mark_et": list(ENTRY_MARK_ET),
        "entry_mark_at": _et_instant(entry_session, ENTRY_MARK_ET),
        "evaluation_horizon_sessions": int(RS.FROZEN_HORIZON),
        "horizon_counts_from": HORIZON_COUNTS_FROM,
        "maturity_session": maturity_session_for(entry_session),
        "calendar_owner": "engine.exchange_calendar",
        "signal_session_is_not_the_entry_session": True,
        "sessions_are_eligible_sessions_not_weekdays": True,
    }


# --------------------------------------------------------------------------- #
# 5. THE FROZEN SPECIFICATION
# --------------------------------------------------------------------------- #
def inherited_construction() -> dict:
    """The SIGNAL half of the specification, read out of the sibling.

    Read, not retyped. If the sibling's construction were ever edited, this
    challenger would move with it or the classification test would fail - and
    the sibling is frozen, so neither is expected to happen.
    """
    sib = RS.frozen_specification()
    return {k: sib[k] for k in INHERITED_CONSTRUCTION_KEYS if k in sib}


def unclassified_sibling_fields() -> list:
    """Fields of the sibling specification that neither tuple claims.

    Always empty. It exists so that adding a field to the frozen specification
    forces a DECISION about whether this challenger inherits it, rather than
    letting it be copied - or dropped - by accident.
    """
    known = set(INHERITED_CONSTRUCTION_KEYS) | set(NOT_INHERITED_KEYS)
    return sorted(set(RS.frozen_specification()) - known)


def frozen_specification() -> dict:
    """Everything this challenger nails down, in one immutable dict."""
    spec = {
        "schema": "alpha_recovery_forward_specification/1",
        "challenger_id": CHALLENGER_ID,
        **inherited_construction(),
        # ---- the ONE thing that differs, stated four ways so it cannot be
        # ---- read as the sibling's boundary by mistake
        "execution_boundary": EXECUTION_BOUNDARY,
        "entry_mark_et": "%02d:%02d" % ENTRY_MARK_ET,
        "information_session": "session t; the decision is formed from the "
                               "%02d:%02d ET snapshot on t" % INFORMATION_CUTOFF_ET,
        "entry_session": "the NEXT eligible session after t, entered at its open",
        "evaluation_horizon_counts_from": HORIZON_COUNTS_FROM,
        "decision_window_opens_et": "%02d:%02d" % DECISION_WINDOW_OPENS_ET,
        "emission_rule": (
            "the decision is formed from the %02d:%02d ET option snapshot on "
            "session t, frozen after that session's data is published by the "
            "historical vendor and BEFORE the next eligible session opens, "
            "entered at that session's %02d:%02d ET open and held for %d "
            "eligible sessions from the entry; a session whose window closes "
            "without a decision is MISSED for ever and is never backfilled"
            % (INFORMATION_CUTOFF_ET[0], INFORMATION_CUTOFF_ET[1],
               ENTRY_MARK_ET[0], ENTRY_MARK_ET[1], RS.FROZEN_HORIZON)),
        # ---- the data path, which is the reason this challenger exists
        "information_source_entitlement": "HISTORICAL_ONLY",
        "requires_live_market_data": False,
        "new_subscription_cost_usd": NEW_SUBSCRIPTION_COST_USD,
        "data_dependency": (
            "session t's historical %s / %s snapshot must be PUBLISHED before "
            "session t+1 opens; the latency is measured by "
            "alpha_agent.alpha_recovery.opra_publication_probe and is never "
            "assumed" % (OA.DATASET, OA.SCHEMA)),
        # ---- evidence, and the absence of it
        "inception": INCEPTION,
        "boundary_selected_by": BOUNDARY_SELECTED_BY,
        "evidence_at_inception": EVIDENCE_AT_INCEPTION,
        "sibling_challenger": {
            "challenger_id": SIBLING_CHALLENGER_ID,
            "shares": "the feature, the sign, the z-score, the horizon and the "
                      "whole option construction",
            "shares_no_evidence_with_this_challenger": True,
            "sibling_is_unmodified_by_this_challenger": True,
            "sibling_remains": "HISTORICALLY_CONFIRMED, same-session entry, "
                               "TRUE_FORWARD blocked on a live OPRA entitlement",
        },
        "not_optimised_in_this_release": {
            "alternate_entry_times": 0, "alternate_signs": 0,
            "alternate_lookbacks": 0, "alternate_horizons": 0,
            "alternate_thresholds": 0, "rescue_arms": 0,
            "parameter_search": False,
            "note": "the next-open boundary was chosen by a COMPLETED "
                    "diagnostic before this module existed; no further entry "
                    "time is tested here and none may be added"},
    }
    return spec


def specification_hash() -> str:
    return stable_hash(frozen_specification())


# --------------------------------------------------------------------------- #
# 6. THE FREEZE RECORD (the registration is performed by an operator script)
# --------------------------------------------------------------------------- #
def freeze_row() -> dict:
    """The immutable freeze, in the shape the canonical adoption owner reads."""
    sp = frozen_specification()
    record_hash = specification_hash()
    return {
        "hypothesis_id": "%s_%s" % (CHALLENGER_ID, record_hash[:12]),
        "release": RELEASE,
        "asset_class": ASSET_CLASS,
        "economic_family": OS.FAMILY,
        "model_family": "OPTION_SURFACE_SKEW_DIRECTIONAL",
        "horizon_sessions": int(RS.FROZEN_HORIZON),
        "outcome": "FORWARD_FROZEN",
        "invalidated_reason": None,
        "input_data_identity": stable_hash({
            "surface": str(OA.surface_path()), "schema": OA.SCHEMA,
            "dataset": OA.DATASET, "band": OA.MONEYNESS_BAND,
            "execution_boundary": EXECUTION_BOUNDARY}),
        "spec_json": {**sp, "instrument_scope": [OA.UNDERLYING], "venue": VENUE,
                      "sleeve": OS.FAMILY,
                      "cost_model": {"primary_bps_per_side": OS.COST_PRIMARY_BPS,
                                     "stress_bps_per_side": OS.COST_STRESS_BPS,
                                     "applied": "both legs of every decision"},
                      "feature_snapshot_hash": record_hash},
        "forward_challenger": {"challenger_id": CHALLENGER_ID,
                               "record_hash": record_hash,
                               "inception": INCEPTION},
    }


def freeze_record() -> dict:
    """The campaign-side immutable artifact, in ``forward_package`` shape."""
    sp = frozen_specification()
    body = {
        "schema": FP.RECORD_SCHEMA, "campaign_id": CAMPAIGN_ID,
        "challenger_id": CHALLENGER_ID,
        "cell_id": "OPTIONS|%s|REVERSED_%s|h%d|NEXT_OPEN"
                   % (OA.UNDERLYING, RS.SOURCE_SIGNAL, RS.FROZEN_HORIZON),
        "classification": "FROZEN_PROSPECTIVE_HYPOTHESIS",
        "why": ("the same frozen information, acted on at a boundary the owned "
                "historical entitlement can actually serve. The sibling needs a "
                "live feed for fifteen minutes; this one needs none at all."),
        "kind": "DELAYED_EXECUTION_BOUNDARY_OF_A_FROZEN_SIGNAL",
        "information_identity": {"family": OS.FAMILY, "dimension": OS.DIMENSION,
                                 "baseline": None, "signal": RS.SOURCE_SIGNAL,
                                 "sign": RS.FROZEN_SIGN,
                                 "execution_boundary": EXECUTION_BOUNDARY},
        "asset_class": ASSET_CLASS,
        "horizon_sessions": int(RS.FROZEN_HORIZON),
        "qualification_evidence": {
            "state": "NONE",
            "historical_qualification": "NONE",
            "inherited_from_sibling": False,
            "discovery_sample_excluded": True,
            "independent_historical_confirmation": "NOT_RUN_AND_NOT_INHERITED",
            "true_forward": "accrues from the first entry session after "
                            "registration; zero observations exist",
            "disclosure": EVIDENCE_DISCLOSURE},
        "forward_specification": sp,
        "freeze_record_hash": specification_hash(),
        "human_gated_adoption_command": FP.adoption_command(CHALLENGER_ID),
        "promotion_allowed": False, "live_registration_performed": False,
        "holdings_changed": False, "records_are_immutable": True,
    }
    body["record_hash"] = stable_hash(body)
    return body


REGISTRATION_SCOPE = {
    "approved": "forward evidence registration only",
    "not_approved": ["promote the model", "allocate capital",
                     "modify the portfolio", "create orders", "merge to live",
                     "deploy anything", "replace the incumbent",
                     "inherit the sibling's historical confirmation"],
    "backfill": "NONE - the first legitimate entry is the first eligible "
                "session STRICTLY AFTER registration, and a window that closes "
                "without a decision is MISSED for ever",
}

#: The registration is a governed act and the research package may never perform
#: one, exactly as for the sibling. Research builds the freeze record and an
#: exact command; a human runs the entrypoint.
REGISTRATION_OWNER = "scripts/register_next_open_challenger.py"
REGISTRATION_ARTIFACT = "next_open_challenger_registration.json"
DECISION_OWNER = "scripts/run_next_open_prospective_decision.py"


def true_forward_state() -> dict:
    """What the canonical owner holds, read from the operator script's artifact."""
    from . import read_artifact

    body = read_artifact(REGISTRATION_ARTIFACT)
    if not body or not body.get("registration"):
        return {"state": "NOT_REGISTERED", "challenger_id": CHALLENGER_ID,
                "registration_owner": REGISTRATION_OWNER,
                "true_forward_observations": 0,
                "capital_eligible_now": False}
    row = body["registration"]
    return {
        "state": "REGISTERED", "challenger_id": CHALLENGER_ID,
        "registration_owner": REGISTRATION_OWNER,
        "registration": row,
        "evidence_status": row.get("evidence_status"),
        "first_eligible_observation_session":
            row.get("first_eligible_observation_session"),
        "matured_observations": row.get("matured_observations"),
        "true_forward_observations": int(row.get("matured_observations") or 0),
        "backfilled": bool(row.get("backfilled")),
        "capital_eligible_now": False,
        "why_not_capital_eligible": (
            "this challenger has no historical qualification at all and zero "
            "forward observations; adoption is human-gated in every case"),
    }


# --------------------------------------------------------------------------- #
# 7. THE EMISSION POLICY AND THE DECISION CONTRACT
# --------------------------------------------------------------------------- #
#: What ``prospective_decision.declare_policy`` must be handed. Built here so the
#: operator script cannot choose a boundary of its own.
def policy_declaration(identity: Optional[dict] = None) -> dict:
    """The arguments for the ONE declaration this challenger ever makes.

    The emission window is declared on the ENTRY session and runs from midnight
    exchange-local to the 09:30 open. By midnight the information session has
    closed completely, so the emitter can see everything the rule reads and
    nothing it will be scored against - the mark is still nine and a half hours
    in the future.
    """
    return {
        "challenger_id": CHALLENGER_ID,
        "information_cutoff_et": tuple(DECISION_WINDOW_OPENS_ET),
        "entry_mark_et": tuple(ENTRY_MARK_ET),
        "rebalance_cadence_sessions": int(RS.FROZEN_HORIZON),
        "evaluation_horizon_sessions": int(RS.FROZEN_HORIZON),
        "cost_policy": {"bps_per_side": OS.COST_PRIMARY_BPS,
                        "ladder_bps_per_side": list(OS.COST_LADDER_BPS),
                        "applied": "both legs of every decision"},
        "instrument_scope": [OA.UNDERLYING],
        "identity": dict(identity or {}),
        "emission_rule": frozen_specification()["emission_rule"],
        "execution_contract": execution_contract(),
    }


def execution_contract() -> dict:
    """The declaration that keeps the signal session and the entry session apart.

    Stored in the policy file, so the boundary is on the record BEFORE any
    decision exists and cannot be described to fit one afterwards.
    """
    return {
        "execution_boundary": EXECUTION_BOUNDARY,
        "decision_session_is": "the ENTRY session",
        "information_session_is": "the eligible session immediately BEFORE the "
                                 "entry session",
        "information_cutoff_et_on_the_information_session":
            list(INFORMATION_CUTOFF_ET),
        "decision_window_opens_et_on_the_entry_session":
            list(DECISION_WINDOW_OPENS_ET),
        "entry_mark_et_on_the_entry_session": list(ENTRY_MARK_ET),
        "horizon_counts_from": HORIZON_COUNTS_FROM,
        "calendar_owner": "engine.exchange_calendar",
        "requires_live_market_data": False,
        "why_the_window_is_wide": (
            "nine and a half hours, so no human has to be present at a "
            "particular minute for a decision to be legitimate"),
    }


#: Refusals this challenger adds ON TOP of the generic window owner's. Each can
#: only ever REFUSE a freeze the generic owner would have allowed; none can
#: permit one it would have refused.
REFUSED_STALE_INPUT = "REFUSED_INPUT_IS_NOT_FROM_THE_INFORMATION_SESSION"
REFUSED_SOURCE_NOT_PUBLISHED = "REFUSED_SOURCE_SESSION_NOT_PUBLISHED_BY_THE_VENDOR"
EXTRA_REFUSALS = (REFUSED_STALE_INPUT, REFUSED_SOURCE_NOT_PUBLISHED)


def check_information_instant(entry_session, observed_at) -> dict:
    """Was the input observed at or before 15:45 ET on the INFORMATION session?

    The generic emission window can only speak about instants on the session it
    is given, and the session it is given here is the ENTRY session - so its
    cutoff (midnight) is nine hours looser than the truth. This is the tighter
    statement, and it belongs to the challenger that makes it.
    """
    ctx = decision_context(entry_session)
    cutoff = ctx.get("information_cutoff_at")
    out = {"information_session": ctx.get("information_session"),
           "declared_cutoff_at": cutoff, "observed_at": observed_at}
    if cutoff is None:
        return {**out, "checked": False, "violates": None,
                "reason": "no information session could be resolved"}
    if observed_at is None:
        # Fail CLOSED, like every other cutoff check in this estate: an input
        # that cannot be shown to predate the cutoff is refused, not assumed.
        return {**out, "checked": True, "violates": True, "failed_closed": True,
                "reason": "the input carries no observation instant, so it "
                          "cannot be shown to come from the information session"}
    a = _as_utc(observed_at)
    b = _as_utc(cutoff)
    if a is None or b is None:
        return {**out, "checked": True, "violates": True, "failed_closed": True,
                "reason": "an observation instant could not be parsed"}
    return {**out, "checked": True, "violates": bool(a > b),
            "reason": ("the input was observed after the 15:45 ET snapshot on "
                       "the information session" if a > b else
                       "the input predates the information session's snapshot")}


def _as_utc(value) -> Optional[datetime]:
    if value is None:
        return None
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        d = _d(raw)
        if d is None:
            return None
        dt = datetime(d.year, d.month, d.day)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# --------------------------------------------------------------------------- #
# 8. THE OPERATOR-FACING STATE MACHINE
# --------------------------------------------------------------------------- #
AWAITING_REGISTRATION = "AWAITING_PROSPECTIVE_REGISTRATION"
AWAITING_POLICY = "AWAITING_POLICY_DECLARATION"
AWAITING_INFORMATION_SESSION = "AWAITING_INFORMATION_SESSION"
AWAITING_SOURCE_PUBLICATION = "AWAITING_SOURCE_PUBLICATION"
READY_FOR_ENTRY = "READY_FOR_ENTRY"
DECISION_DUE_NOW = "DECISION_DUE_NOW"
AWAITING_MATURITY = "AWAITING_MATURITY"
MISSED = "MISSED_NO_DECISION_IN_THE_WINDOW"
STATES = (AWAITING_REGISTRATION, AWAITING_POLICY, AWAITING_INFORMATION_SESSION,
          AWAITING_SOURCE_PUBLICATION, READY_FOR_ENTRY, DECISION_DUE_NOW,
          AWAITING_MATURITY, MISSED)


def source_available(information_session, *, surface=None) -> dict:
    """Is session ``t``'s 15:45 snapshot actually IN HAND?

    Two independent questions, answered separately and never merged:

    ``owned``     the session is a row in the owned surface, which is the only
                  thing the freeze can actually be computed from.
    ``published`` the vendor exposes the session at all, per the publication
                  probe. This module NEVER infers it from the clock: "the
                  session has ended" is not "the vendor has published it", and
                  believing otherwise is the exact mistake a Saturday probe
                  once made in this campaign.
    """
    from . import opra_publication_probe as PP

    out = {"information_session": information_session,
           "owned": False, "published": None,
           "publication_probe_owner": PP.CALCULATION_OWNER}
    try:
        f = OS.features(surface=surface or OS.surface_path())
        dates = {str(x)[:10] for x in f["date"]}
        out["owned"] = str(information_session)[:10] in dates
        out["owned_last_session"] = max(dates) if dates else None
    except Exception as exc:                          # noqa: BLE001 - surface read
        out["owned_error"] = "%s: %s" % (type(exc).__name__, str(exc)[:160])
    st = PP.session_state(information_session)
    out["published"] = st.get("available")
    out["publication_state"] = st.get("state")
    out["first_available_time"] = st.get("first_available_time")
    out["publication_delay_seconds"] = st.get("publication_delay_seconds")
    out["available_before_next_open"] = st.get("available_before_next_open")
    out["usable_for_a_freeze"] = bool(out["owned"])
    return out


def entry_state(entry_session, *, now: Optional[str] = None,
                surface=None) -> dict:
    """Where ONE entry session stands, on the facts alone.

    Deliberately independent of whether the challenger is registered or has a
    declared policy. A window that has shut HAS shut, and an operator who has
    not finished the paperwork still needs to be told that the session is gone
    rather than that the paperwork is missing.
    """
    ts = now or now_iso()
    ctx = decision_context(entry_session)
    existing = PD.load_decision(CHALLENGER_ID, entry_session)
    src = source_available(ctx["information_session"], surface=surface)
    out = {"entry_session": ctx.get("entry_session"),
           "decision_context": ctx,
           "decision_for_entry_session": existing,
           "source": src, "now": ts}

    now_dt = _as_utc(ts)
    opens = _as_utc(_et_instant(entry_session, DECISION_WINDOW_OPENS_ET))
    mark = _as_utc(ctx.get("entry_mark_at"))
    cutoff = _as_utc(ctx.get("information_cutoff_at"))

    if existing:
        return {**out, "maturity_session": ctx["maturity_session"],
                "entry_state": (AWAITING_MATURITY
                                if (mark and now_dt and now_dt >= mark)
                                else READY_FOR_ENTRY)}
    if now_dt and mark and now_dt >= mark:
        return {**out, "entry_state": MISSED, "backfill_refused": True,
                "why": ("the %s open has passed and no decision was frozen; "
                        "this entry session is missed permanently and is never "
                        "created retrospectively" % entry_session)}
    if now_dt and cutoff and now_dt < cutoff:
        return {**out, "entry_state": AWAITING_INFORMATION_SESSION}
    if not src["usable_for_a_freeze"]:
        return {**out, "entry_state": AWAITING_SOURCE_PUBLICATION,
                "blocked_on": ("the historical vendor has not published session "
                               "%s, or it has not been appended to the owned "
                               "surface" % ctx["information_session"])}
    if now_dt and opens and now_dt < opens:
        return {**out, "entry_state": AWAITING_SOURCE_PUBLICATION,
                "blocked_on": ("the emission window for %s opens at %s"
                               % (entry_session, opens.isoformat()))}
    return {**out, "entry_state": DECISION_DUE_NOW,
            "next_action": "%s --freeze --session %s"
                           % (DECISION_OWNER, entry_session)}


def state(entry_session=None, *, now: Optional[str] = None,
          surface=None) -> dict:
    """Where this challenger stands, for one entry session or in general.

    ``state`` is the most BLOCKING fact: an unregistered challenger says so
    first, because nothing it freezes would count. ``entry_state`` is always
    present alongside it and reports the session's own lifecycle regardless.
    """
    ts = now or now_iso()
    reg = true_forward_state()
    pol = PD.load_policy(CHALLENGER_ID)
    held = PD.list_decisions(CHALLENGER_ID)
    out = {
        "calculation_owner": CALCULATION_OWNER,
        "challenger_id": CHALLENGER_ID,
        "now": ts,
        "registered": reg["state"] == "REGISTERED",
        "registration": reg,
        "policy_declared": bool(pol),
        "decisions_frozen": len(held),
        "frozen_sessions": [r.get("eligible_session") for r in held],
        "true_forward_observations": reg.get("true_forward_observations", 0),
        "capital_eligible": False,
        "state_vocabulary": list(STATES),
        "safety": dict(SAFETY),
    }
    if entry_session:
        out.update(entry_state(entry_session, now=ts, surface=surface))

    if not out["registered"]:
        out["state"] = AWAITING_REGISTRATION
        out["next_action"] = REGISTRATION_OWNER
    elif not pol:
        out["state"] = AWAITING_POLICY
        out["next_action"] = "%s --declare-policy" % DECISION_OWNER
    elif entry_session:
        out["state"] = out["entry_state"]
    else:
        out["state"] = "POLICY_DECLARED"
    return out


# --------------------------------------------------------------------------- #
# 9. THE ARTIFACT
# --------------------------------------------------------------------------- #
def forward_arithmetic() -> dict:
    """How long forward evidence takes, on the estate's own floor."""
    per_year = OS.PPY / float(RS.FROZEN_HORIZON)
    per_month = per_year / 12.0
    return {
        "cadence_sessions": int(RS.FROZEN_HORIZON),
        "independent_decisions_per_year": per_year,
        "independent_decisions_per_month": per_month,
        "min_effective_periods": MIN_EFFECTIVE_PERIODS,
        "months_to_reach_the_floor": MIN_EFFECTIVE_PERIODS / per_month,
        "marginal_data_cost_usd_per_session": 0.0,
        "why_zero": ("the option band for one session is bought from the "
                     "HISTORICAL entitlement the campaign already holds, out of "
                     "free credit; no subscription is started and no paid "
                     "dollar is authorised"),
        "why_this_is_a_floor_not_a_finish": (
            "reaching MIN_EFFECTIVE_PERIODS makes a measurement admissible, not "
            "conclusive"),
    }


def daily_contract() -> dict:
    """What has to happen each session, who owns it, and inside which window.

    Written down as data so nothing about the daily loop is implicit. The
    window is nine and a half hours wide and every step is idempotent, so a
    scheduler firing repeatedly inside it is harmless and a scheduler firing
    once is enough; what is NOT acceptable is a step nobody wrote down.
    """
    return {
        "window_et": "%02d:%02d -> %02d:%02d on the ENTRY session"
                     % (DECISION_WINDOW_OPENS_ET + ENTRY_MARK_ET),
        "window_hours": 9.5,
        "every_step_is_idempotent": True,
        "steps": [
            {"step": 1, "what": "measure whether the vendor has published the "
                                "information session",
             "owner": "alpha_agent.alpha_recovery.opra_publication_probe",
             "command": "%s --probe-publication --session <information session>"
                        % DECISION_OWNER,
             "cost_usd": 0.0, "downloads": False},
            {"step": 2, "what": "append the information session's 15:45 ET "
                                "snapshot to the owned surface",
             "owner": "alpha_agent.alpha_recovery.options_acquisition",
             "command": "scripts/run_alpha_recovery_offensive.py options",
             "cost_usd": "pennies, from the free credit already held; "
                         "$0 paid, no subscription",
             "invariant": "APPEND ONLY - a rebuild would move the sample the "
                          "sign was discovered on"},
            {"step": 3, "what": "freeze one immutable decision for the entry "
                                "session, inside the window",
             "owner": CALCULATION_OWNER,
             "command": "%s --cycle --session <information session>"
                        % DECISION_OWNER,
             "cost_usd": 0.0},
        ],
        "if_a_step_is_late": ("the entry session is MISSED and stays missed; "
                              "nothing is backfilled and no decision is created "
                              "retrospectively"),
        "operator_need_not_be_present": (
            "no step names a minute. Steps 1 and 3 are one command that is safe "
            "to run repeatedly anywhere inside the window."),
    }


def build(*, write: bool = True, entry_session=None, now: Optional[str] = None,
          surface=None) -> dict:
    """The whole challenger in one artifact: spec, contract, state, absence of
    evidence."""
    body = {
        "schema": "alpha_recovery_next_open_challenger/1",
        "calculation_owner": CALCULATION_OWNER,
        "challenger_id": CHALLENGER_ID,
        "headline": "REVERSED SPY PUT-CALL SKEW, ENTERED AT THE NEXT OPEN",
        "frozen_specification": frozen_specification(),
        "specification_hash": specification_hash(),
        "freeze_record": freeze_record(),
        "execution_contract": execution_contract(),
        "decision_context_example": decision_context(
            entry_session or next_eligible_session(INCEPTION, 2)),
        "evidence_at_inception": EVIDENCE_AT_INCEPTION,
        "historical_qualification_inherited": False,
        "true_forward_observations": 0,
        "capital_eligible": False,
        "new_subscription_cost_usd": NEW_SUBSCRIPTION_COST_USD,
        "boundary_selected_by": BOUNDARY_SELECTED_BY,
        "sibling_unchanged": True,
        "sibling_challenger_id": SIBLING_CHALLENGER_ID,
        "unclassified_sibling_fields": unclassified_sibling_fields(),
        "forward_arithmetic": forward_arithmetic(),
        "registration_scope": REGISTRATION_SCOPE,
        "registration_owner": REGISTRATION_OWNER,
        "decision_owner": DECISION_OWNER,
        "daily_contract": daily_contract(),
        "state": state(entry_session, now=now, surface=surface),
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


__all__ = [
    "CALCULATION_OWNER", "CHALLENGER_ID", "SIBLING_CHALLENGER_ID", "INCEPTION",
    "ENTRY_MARK_ET", "DECISION_WINDOW_OPENS_ET", "INFORMATION_CUTOFF_ET",
    "EXECUTION_BOUNDARY", "HORIZON_COUNTS_FROM", "NEW_SUBSCRIPTION_COST_USD",
    "INHERITED_CONSTRUCTION_KEYS", "NOT_INHERITED_KEYS", "EVIDENCE_DISCLOSURE",
    "EVIDENCE_AT_INCEPTION", "SAFETY", "STATES", "EXTRA_REFUSALS",
    "is_eligible_session", "next_eligible_session", "previous_eligible_session",
    "information_session_for", "entry_session_for", "maturity_session_for",
    "decision_context", "inherited_construction", "unclassified_sibling_fields",
    "frozen_specification", "specification_hash", "freeze_row", "freeze_record",
    "policy_declaration", "execution_contract", "check_information_instant",
    "source_available", "entry_state", "state", "forward_arithmetic",
    "daily_contract", "true_forward_state", "build",
]
