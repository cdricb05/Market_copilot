r"""engine/forward_emission_window.py - THE one owner of WHEN a prospective
decision for session ``S`` may legitimately be emitted.

THE GAP THIS CLOSES
-------------------
:mod:`api.canonical_forward_accrual` encoded exactly one emission rule, inline,
as ``if s <= today: FORFEITED``. That rule is correct for every challenger the
estate had when it was written: an R58 freeze is formed from data that is
complete before ``S`` begins, so requiring the emission to land STRICTLY BEFORE
``S`` costs it nothing and removes any possibility that an emitter which has
seen ``S`` could decline to stamp a bad one.

It is not correct for every challenger that can exist. A strategy may
legitimately form its decision from information that only exists DURING ``S`` -
an option snapshot taken in the afternoon, say - and enter at that session's own
close. Under the inline rule such a challenger is FORFEITED every single
session, forever, because its input arrives after the only window it was ever
given. That is not a safety property; it is an owner that cannot express the
strategy.

WHAT THIS MODULE OWNS
---------------------
One question, for any challenger, in one place: given a DECLARED boundary
policy, a decision session and an instant, is the emission window for that
session NOT YET OPEN, OPEN, or CLOSED?

It is deliberately generic. No feature, no signal, no instrument and no release
appears anywhere in this file. A challenger DECLARES its boundary as data and
this module answers; it does not know which strategies exist.

THE TWO BOUNDARIES, AND WHY BOTH ARE HONEST
-------------------------------------------
``PRIOR_SESSION_ONLY`` (the default, and the rule every existing registration
keeps): the window is everything strictly before ``S`` begins. This is the R62.2
rule unchanged - a registration that declares no policy behaves exactly as it
did - and it stays the default precisely so that widening the vocabulary cannot
silently loosen a challenger that never asked for it.

``SAME_SESSION_AFTER_DECLARED_CUTOFF``: the window OPENS at a declared
information cutoff on ``S`` and CLOSES at the declared entry mark on ``S``. Both
instants are declared in advance, in exchange-local time, and neither is
discoverable from the data. The emitter may therefore see the cutoff snapshot -
which is the information the strategy is defined on - and can never see the mark
it will be scored from, nor any outcome. The window between them is the only
interval in which the decision is both INFORMED and BLIND, and it is measured in
minutes rather than granted for the rest of the day, because "before midnight"
would let an emitter that has watched the close decide whether to stamp the
session at all.

WHAT IT REFUSES
---------------
* It never widens a window. An undeclared or unparseable policy resolves to
  ``PRIOR_SESSION_ONLY`` - the STRICTEST rule - so a malformed declaration can
  only ever cost a challenger an emission, never buy it one.
* It holds no clock. Every answer is a pure function of the instant it is
  handed, so a caller cannot be surprised by this module reading the wall clock
  at a different moment than the rest of the run.
* It holds no timezone table. Exchange-local time is resolved through
  :mod:`engine.market_hours`, the ONE owner of the Eastern clock in this estate.
* It decides nothing about WHETHER a decision exists, whether it is well formed,
  or whether it may be scored. It answers only "is the window open", which is a
  different question from "is there something to put in it".

Pure: no I/O, no network, no database, no wall clock. It creates no order, no
fill, no decision, no evidence and no capital allocation.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional

from paper_trader.engine import market_hours as _mh

CALCULATION_OWNER = "engine.forward_emission_window"
SCHEMA_VERSION = "forward_emission_window.v1"
PHASE = "R62.3"

#: The exchange-local clock. ONE owner; this module keeps no table of its own.
TIMEZONE_OWNER = "engine.market_hours"

# --------------------------------------------------------------------------- #
# 1. THE BOUNDARY VOCABULARY
# --------------------------------------------------------------------------- #
#: The emission must land STRICTLY BEFORE the decision session begins. This is
#: the R62.2 rule, unchanged, and it is the DEFAULT for everything that does not
#: declare otherwise.
BOUNDARY_PRIOR_SESSION = "PRIOR_SESSION_ONLY"

#: The emission may land ON the decision session, but only inside the interval
#: between a declared information cutoff and a declared entry mark.
BOUNDARY_SAME_SESSION = "SAME_SESSION_AFTER_DECLARED_CUTOFF"

BOUNDARIES = (BOUNDARY_PRIOR_SESSION, BOUNDARY_SAME_SESSION)

#: Window states. These are the only answers this module can give.
WINDOW_NOT_OPEN = "EMISSION_WINDOW_NOT_OPEN"
WINDOW_OPEN = "EMISSION_WINDOW_OPEN"
WINDOW_CLOSED = "EMISSION_WINDOW_CLOSED"
WINDOW_STATES = (WINDOW_NOT_OPEN, WINDOW_OPEN, WINDOW_CLOSED)

#: Why a declaration was not taken at face value. A policy is never "repaired"
#: into something more permissive; it falls back to the strictest rule and says
#: that it did.
DECL_ACCEPTED = "DECLARATION_ACCEPTED"
DECL_ABSENT = "NO_POLICY_DECLARED_DEFAULTING_TO_THE_STRICTEST_RULE"
DECL_UNKNOWN_BOUNDARY = "UNRECOGNISED_BOUNDARY_DEFAULTING_TO_THE_STRICTEST_RULE"
DECL_INCOMPLETE = "SAME_SESSION_BOUNDARY_WITHOUT_BOTH_INSTANTS"
DECL_NOT_ORDERED = "DECLARED_CUTOFF_IS_NOT_BEFORE_THE_DECLARED_ENTRY_MARK"
DECLARATION_STATES = (DECL_ACCEPTED, DECL_ABSENT, DECL_UNKNOWN_BOUNDARY,
                      DECL_INCOMPLETE, DECL_NOT_ORDERED)


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _as_instant(value: Any) -> Optional[datetime]:
    """Any accepted spelling of an instant -> an aware UTC datetime.

    A naive datetime is read as UTC, which is the convention every store in this
    estate already writes (``datetime.now(timezone.utc).isoformat()``). It is
    NOT read as exchange-local: guessing a timezone for a bare timestamp is how
    an emission lands four hours from where its author meant it.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            d = _as_date(raw)
            if d is None:
                return None
            dt = datetime(d.year, d.month, d.day)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _hhmm(value: Any) -> Optional[tuple]:
    """``(15, 45)`` / ``"15:45"`` / ``[15, 45]`` -> ``(15, 45)``."""
    if value is None:
        return None
    if isinstance(value, (tuple, list)) and len(value) == 2:
        try:
            h, m = int(value[0]), int(value[1])
        except (TypeError, ValueError):
            return None
    else:
        parts = str(value).split(":")
        if len(parts) != 2:
            return None
        try:
            h, m = int(parts[0]), int(parts[1])
        except ValueError:
            return None
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    return (h, m)


def exchange_local_instant(session: Any, hhmm: tuple) -> Optional[datetime]:
    """``(session, exchange-local wall clock)`` -> the aware UTC instant.

    The conversion is delegated to :mod:`engine.market_hours`, which owns the
    Eastern clock; this module never names a timezone, so a DST transition is
    handled in exactly one place for the whole estate.
    """
    d = _as_date(session)
    if d is None or not hhmm:
        return None
    naive = datetime.combine(d, time(hhmm[0], hhmm[1]))
    return _mh.to_eastern(naive).astimezone(timezone.utc)


# --------------------------------------------------------------------------- #
# 2. THE DECLARATION
# --------------------------------------------------------------------------- #
def normalise_policy(declaration: Optional[dict] = None) -> dict:
    """Read a declared boundary policy. NEVER widens it.

    Every failure path resolves to :data:`BOUNDARY_PRIOR_SESSION` and records
    WHY, so a challenger whose declaration is absent, misspelled or incomplete
    is held to the strictest rule rather than to the one it was reaching for.
    """
    decl = declaration if isinstance(declaration, dict) else None
    strict = {
        "boundary": BOUNDARY_PRIOR_SESSION,
        "information_cutoff_et": None,
        "entry_mark_et": None,
        "declaration_state": DECL_ABSENT,
        "boundary_vocabulary": list(BOUNDARIES),
        "owner": CALCULATION_OWNER,
        "timezone_owner": TIMEZONE_OWNER,
        "widened_by_this_module": False,
    }
    if not decl:
        return strict
    boundary = str(decl.get("boundary") or "").strip() or None
    if boundary is None:
        return strict
    if boundary == BOUNDARY_PRIOR_SESSION:
        return {**strict, "declaration_state": DECL_ACCEPTED}
    if boundary != BOUNDARY_SAME_SESSION:
        return {**strict, "declaration_state": DECL_UNKNOWN_BOUNDARY,
                "rejected_boundary": boundary}

    cutoff = _hhmm(decl.get("information_cutoff_et"))
    mark = _hhmm(decl.get("entry_mark_et"))
    if cutoff is None or mark is None:
        return {**strict, "declaration_state": DECL_INCOMPLETE,
                "rejected_boundary": boundary,
                "information_cutoff_et": cutoff, "entry_mark_et": mark}
    if cutoff >= mark:
        # A window that opens at or after the mark it must precede is not a
        # window. Refusing it is the difference between "informed and blind"
        # and "has already seen the price it will be scored against".
        return {**strict, "declaration_state": DECL_NOT_ORDERED,
                "rejected_boundary": boundary,
                "information_cutoff_et": cutoff, "entry_mark_et": mark}
    return {
        **strict,
        "boundary": BOUNDARY_SAME_SESSION,
        "information_cutoff_et": list(cutoff),
        "entry_mark_et": list(mark),
        "declaration_state": DECL_ACCEPTED,
    }


def declare_same_session(*, information_cutoff_et, entry_mark_et) -> dict:
    """Build a SAME-SESSION declaration. Convenience for a producer."""
    return normalise_policy({"boundary": BOUNDARY_SAME_SESSION,
                             "information_cutoff_et": information_cutoff_et,
                             "entry_mark_et": entry_mark_et})


# --------------------------------------------------------------------------- #
# 3. THE WINDOW
# --------------------------------------------------------------------------- #
def window_bounds(policy: Optional[dict], session: Any) -> dict:
    """The UTC instants between which an emission for ``session`` is legal.

    ``opens_at`` is ``None`` for the prior-session boundary: any instant in the
    past is early enough, so the window has no opening edge. ``closes_at`` is
    always present, because every boundary has a moment after which the emitter
    would know too much.
    """
    pol = policy if (policy or {}).get("boundary") in BOUNDARIES else \
        normalise_policy(policy)
    d = _as_date(session)
    out = {
        "session": d.isoformat() if d else None,
        "boundary": pol.get("boundary"),
        "declaration_state": pol.get("declaration_state"),
        "opens_at": None,
        "closes_at": None,
        "information_cutoff_at": None,
        "entry_mark_at": None,
        "owner": CALCULATION_OWNER,
    }
    if d is None:
        return {**out, "resolvable": False,
                "reason": "no decision session was supplied"}

    if pol.get("boundary") == BOUNDARY_SAME_SESSION:
        opens = exchange_local_instant(d, tuple(pol["information_cutoff_et"]))
        closes = exchange_local_instant(d, tuple(pol["entry_mark_et"]))
        if opens is None or closes is None:          # pragma: no cover - defensive
            return {**out, "resolvable": False,
                    "reason": "the declared instants could not be resolved"}
        return {**out, "resolvable": True,
                "opens_at": opens.isoformat(),
                "closes_at": closes.isoformat(),
                "information_cutoff_at": opens.isoformat(),
                "entry_mark_at": closes.isoformat(),
                "rule": ("the decision is formed from information declared "
                         "complete at %02d:%02d exchange-local on the session "
                         "itself, and must be frozen before the %02d:%02d mark "
                         "it will be entered at"
                         % (pol["information_cutoff_et"][0],
                            pol["information_cutoff_et"][1],
                            pol["entry_mark_et"][0], pol["entry_mark_et"][1]))}

    # PRIOR_SESSION_ONLY. The window shuts the instant the session begins, in
    # UTC - byte-for-byte the R62.2 comparison ``decision_session <= today``,
    # where ``today`` was that same UTC date.
    closes = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return {**out, "resolvable": True,
            "opens_at": None,
            "closes_at": closes.isoformat(),
            "information_cutoff_at": closes.isoformat(),
            "rule": ("a prospective decision for a session is made strictly "
                     "before that session; once it has begun the opportunity "
                     "is gone and is never written late")}


def classify(*, policy: Optional[dict], session: Any, now: Any) -> dict:
    """Is the emission window for ``session`` NOT OPEN, OPEN, or CLOSED?

    The single question this module exists to answer. Pure: ``now`` is supplied
    by the caller, never read from the machine.
    """
    pol = policy if (policy or {}).get("boundary") in BOUNDARIES else \
        normalise_policy(policy)
    bounds = window_bounds(pol, session)
    instant = _as_instant(now)
    out = {**bounds, "now": instant.isoformat() if instant else None,
           "window_state_vocabulary": list(WINDOW_STATES)}
    if not bounds.get("resolvable") or instant is None:
        # Fail CLOSED: an unanswerable window is never treated as open.
        return {**out, "state": WINDOW_CLOSED, "emittable": False,
                "reason": bounds.get("reason")
                          or "no instant was supplied to judge the window",
                "failed_closed": True}

    closes = _as_instant(bounds["closes_at"])
    opens = _as_instant(bounds["opens_at"])
    if opens is not None and instant < opens:
        return {**out, "state": WINDOW_NOT_OPEN, "emittable": False,
                "reason": ("the declared information cutoff for %s has not been "
                           "reached; the inputs this decision is defined on do "
                           "not exist yet" % bounds["session"])}
    if instant >= closes:
        return {**out, "state": WINDOW_CLOSED, "emittable": False,
                "reason": ("the emission window for %s closed at %s; a decision "
                           "stamped after it would have been taken by an emitter "
                           "that could already see what it was going to be "
                           "scored against" % (bounds["session"],
                                               bounds["closes_at"]))}
    return {**out, "state": WINDOW_OPEN, "emittable": True,
            "reason": ("the window for %s is open until %s"
                       % (bounds["session"], bounds["closes_at"]))}


def information_cutoff_at(policy: Optional[dict], session: Any) -> Optional[str]:
    """The declared instant after which NO information may enter the decision."""
    return window_bounds(policy, session).get("information_cutoff_at")


def violates_information_cutoff(*, policy: Optional[dict], session: Any,
                                observed_at: Any) -> dict:
    """Did an input observed at ``observed_at`` arrive after the declared cutoff?

    This is the companion refusal to :func:`classify`. The window says when a
    decision may be WRITTEN; this says what it may be written FROM. A decision
    inside its window that used an input from after the cutoff is exactly as
    contaminated as one written late.
    """
    cutoff = _as_instant(information_cutoff_at(policy, session))
    seen = _as_instant(observed_at)
    if cutoff is None:
        return {"checked": False, "violates": None,
                "reason": "no declared cutoff could be resolved for this session"}
    if seen is None:
        # Fail CLOSED: an input whose observation time is unknown cannot be
        # shown to precede the cutoff, so it is refused rather than assumed.
        return {"checked": True, "violates": True, "failed_closed": True,
                "declared_cutoff_at": cutoff.isoformat(), "observed_at": None,
                "reason": ("the input carries no observation instant, so it "
                           "cannot be shown to predate the declared cutoff")}
    return {"checked": True, "violates": bool(seen > cutoff),
            "declared_cutoff_at": cutoff.isoformat(),
            "observed_at": seen.isoformat(),
            "reason": ("the input was observed after the declared information "
                       "cutoff" if seen > cutoff else
                       "the input predates the declared information cutoff")}


def next_window_open_after(policy: Optional[dict], session: Any) -> Optional[str]:
    """When the window for ``session`` opens - for an operator contract.

    ``None`` for the prior-session boundary, which has no opening edge.
    """
    return window_bounds(policy, session).get("opens_at")


def describe(policy: Optional[dict] = None) -> dict:
    """The whole vocabulary, for an artifact or a read model."""
    pol = normalise_policy(policy)
    return {
        "schema_version": SCHEMA_VERSION,
        "owner": CALCULATION_OWNER,
        "phase": PHASE,
        "timezone_owner": TIMEZONE_OWNER,
        "boundary_vocabulary": list(BOUNDARIES),
        "window_state_vocabulary": list(WINDOW_STATES),
        "declaration_state_vocabulary": list(DECLARATION_STATES),
        "default_boundary": BOUNDARY_PRIOR_SESSION,
        "policy": pol,
        "undeclared_resolves_to_the_strictest_rule": True,
        "an_unanswerable_window_is_never_open": True,
        "backfill_allowed": False,
        "research_only": True,
    }


__all__ = [
    "CALCULATION_OWNER", "SCHEMA_VERSION", "PHASE", "TIMEZONE_OWNER",
    "BOUNDARIES", "BOUNDARY_PRIOR_SESSION", "BOUNDARY_SAME_SESSION",
    "WINDOW_STATES", "WINDOW_NOT_OPEN", "WINDOW_OPEN", "WINDOW_CLOSED",
    "DECLARATION_STATES", "DECL_ACCEPTED", "DECL_ABSENT",
    "DECL_UNKNOWN_BOUNDARY", "DECL_INCOMPLETE", "DECL_NOT_ORDERED",
    "exchange_local_instant", "normalise_policy", "declare_same_session",
    "window_bounds", "classify", "information_cutoff_at",
    "violates_information_cutoff", "next_window_open_after", "describe",
]
