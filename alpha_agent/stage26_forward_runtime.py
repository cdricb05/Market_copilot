r"""alpha_agent.stage26_forward_runtime - the S25 PROSPECTIVE RE-ARM adapter.

THE DEFECT THIS REPAIRS
-----------------------
``s25_operating_profitability`` (candidate ``c9_qualityprofi_e490533606``) was
frozen on 2026-08-16 with 100 names, 50 LONG / 50 SHORT, dollar-neutral, at a
63-session horizon. It received ZERO forward marks.

The Stage-26 evidence contract declares exactly one composition path to a mark::

    alpha_agent.runtime.run_tournament_tick          (once per production cycle)
      -> alpha_agent.tournament.run_tournament_cycle
      -> advance_shadow_books                        (ONE mark per evidence date)
      -> ShadowBook.record_mark                     (append-only, monotonic)

and that path's only production host was the ``AlphaAgent-Collect`` scheduled
task. That task was DISABLED on 2026-08-03 - THIRTEEN DAYS BEFORE the book was
created - so the book never had a single opportunity to accrue a mark. The
replacement runtime, ``PaperTrader-ResearchRuntime``, never inherited the
responsibility. The producer was never broken; its host was retired underneath
it.

WHAT THIS MODULE IS
-------------------
The smallest adapter that gives the EXISTING frozen identity a live host, by
re-parenting the FROZEN producer onto the ONE canonical runtime:

    alpha_agent.r52.runtime.research_runtime_cycle   (the ONE research cadence)
      -> stage26_forward_runtime.advance             (THIS MODULE - one stage)
      -> alpha_agent.tournament.advance_shadow_books (the UNCHANGED mark owner)
      -> ShadowBook.record_mark                      (the UNCHANGED guards)

It is deliberately an adapter and not an owner. It decides WHICH ONE session is
legally collectable right now, and then asks the frozen producer to do exactly
what it always did. It is the same shape as the two adapter stages the runtime
already hosts for the next-open and FX-carry challengers: the owner of the rule
keeps the rule, and the runtime supplies the cadence.

WHAT IT IS NOT
--------------
* NOT a second runtime, scheduler, task or daemon. It holds no timer, no thread
  and no task definition, and it is reached only from the existing cadence.
* NOT a second accrual owner or a second forward ledger. The mark is written by
  ``alpha_agent.tournament.advance_shadow_books`` into the SAME shadow book, and
  the h63 clock stays the frozen one. Nothing is recorded anywhere else.
* NOT a revival of ``AlphaAgent-Collect``. ``run_tournament_tick`` and
  ``run_tournament_cycle`` are NOT called and are not imported: this stage
  advances marks and does not activate books, generate experiments, score
  candidates or promote anything.
* NOT a second registry, identity or candidate. The candidate id, strategy spec
  hash, inception date and 100-name membership are READ and VERIFIED, never
  written. A drift in any of them fails the stage closed.
* NOT a signal implementation. No feature, rank, z-score, weight or position is
  computed here; the membership is read from the frozen book.

THE 21 FORFEITED SESSIONS, AND WHY A FLOOR IS NOT OPTIONAL
----------------------------------------------------------
2026-08-17 .. 2026-09-15 (21 NYSE sessions) are PERMANENTLY FORFEITED and are
never reconstructed. The data to price them exists, which is exactly why the
rule matters: a mark written today for 2026-08-17 would be a retrospective
computation wearing the label of a prospective one, and the only property this
book exists to measure is that its outcomes were unobservable when its
specification was frozen.

``ShadowBook.record_mark`` alone does NOT protect them. Its guard is "strictly
after inception, strictly after the latest mark", and with zero marks recorded
the only bound is inception 2026-08-16 - so the frozen producer would happily
accept 2026-09-15. The owned trailing panel's newest session, at the moment this
repair was written, WAS 2026-09-15, and every post-inception session it can
price is one of the 21. Wiring the producer up without a floor would therefore
have minted a forfeited session as its very first "forward" mark.

So two INDEPENDENT guards stand between this stage and a back-fill:

1.  a PROSPECTIVE EPOCH FLOOR, stamped once into the governance record when the
    repair is activated, below which no session is collectable. It is derived as
    ``max(the last forfeited session, the latest completed ELIGIBLE session at
    the activation wall clock)`` - so neither a stale panel nor a panel that has
    run ahead can move it;
2.  an explicit refusal of any session inside the declared forfeited window,
    which holds even if the floor were wrong.

THE SECOND DEFECT: DATA ARRIVAL IS NOT A SESSION CLOCK
------------------------------------------------------
The first activation derived the second term of that floor from the OWNED
PANEL's newest session, and a panel lags the exchange. At the real activation
instant - ``2026-09-16T20:15:01Z``, which is **16:15:01 ET** - the 2026-09-16
NYSE session had closed 15 minutes earlier, yet the panel's newest bar was
still 2026-09-15. The floor was therefore stamped at 2026-09-15 and 2026-09-16
was left collectable: the moment the panel caught up, an ALREADY-COMPLETED
session would have been marked as prospective evidence. An observation is
prospective only if its outcome was unobservable when the observer committed,
and 2026-09-16's outcome was fixed before the commit was written.

So the boundary question splits in two, and only one half belongs to the data:

*   WAS THE SESSION ALREADY COMPLETE when collection was authorised? That is a
    CLOCK AND CALENDAR question, answered by the canonical owners
    ``engine.market_session.resolve_expected_session`` and
    ``engine.exchange_calendar`` - and answered against the EXCHANGE CLOSE
    (``market_session.REGULAR_CLOSE_ET``, 16:00 ET), never against
    ``DEFAULT_CLOSE_CUTOFF_ET`` (17:30 ET), which is a DATA-ARRIVAL grace
    period. Using the arrival cutoff here is the defect itself, one level up.
*   CAN A LEGITIMATE POST-FLOOR SESSION BE PRICED right now? That one is the
    panel's, and it may only ever BLOCK a mark - never authorise one.

No second calendar is introduced. This module owns no holiday table, no
weekend rule and no cutoff of its own; it asks the owners and records who it
asked.

NO CATCH-UP, EVER
-----------------
One invocation marks AT MOST ONE session, and that session is the NEWEST
completed eligible one - never a walk forward through older unmarked sessions.
Sessions missed while the runtime was down are reported as skipped and are
never marked later: a mark computed days after its session is a retrospective
calculation wearing a prospective label, which is the very defect this repair
exists to refuse. There is no loop over dates in this module, and the producer
it calls takes exactly one ``evidence_date``.

RESEARCH ONLY. This module promotes no model, activates no sleeve, allocates no
capital, creates no order, no fill, no proposal and no approval, changes no
holding, cash or NAV, runs no daily close and calls no portfolio cycle. It
spends nothing.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import tempfile
from pathlib import Path
from typing import Optional

from .r59 import stage25_owner as S25O

SCHEMA_VERSION = "s25_prospective_rearm.v1"
COMPOSITION_OWNER = "alpha_agent.stage26_forward_runtime"
#: The UNCHANGED owner of a mark. This module never writes one.
MARK_OWNER = "alpha_agent.tournament.advance_shadow_books"
#: The ONE runtime that hosts this stage. There is no second one.
RUNTIME_OWNER = "alpha_agent.r52.runtime.research_runtime_cycle"
#: The retired host whose disappearance caused the defect. Named so the repair
#: records what it is NOT doing: this task stays disabled.
RETIRED_HOST = "AlphaAgent-Collect"
PHASE = "S25_CANONICAL_FORWARD_REARM"

STORE_DIR_ENV = "PAPER_TRADER_STAGE8_STORE_DIR"
_DEFAULT_STORE_DIR = Path(r"D:\Stock_Prediction_app_data\alpha_agent\stage8")
GOVERNANCE_ARTIFACT = "s25_prospective_rearm.json"


# --------------------------------------------------------------------------- #
# 1. THE FROZEN IDENTITY - read and verified, never written
# --------------------------------------------------------------------------- #
#: Every one of these is a FACT ABOUT AN EXISTING ARTIFACT, carried here as a
#: literal so the stage can refuse to run against anything else. They are not a
#: specification: nothing in this module can create a book, a candidate or a
#: membership, so a mismatch is always a drift in the frozen store and never a
#: disagreement about what should be built.
CANDIDATE_ID = "c9_qualityprofi_e490533606"
STRATEGY_NAME = "s25_operating_profitability"
SHADOW_BOOK_ID = "sb_%s" % CANDIDATE_ID
STRATEGY_SPEC_HASH = \
    "67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e"
ORIGINAL_INCEPTION = "2026-08-16"
MEMBERSHIP_SIZE = 100
HORIZON_DAYS = 63
BENCHMARK = "SPY"

#: THE HUMAN GOVERNANCE DECISION this module implements.
DECISION = "REARM_PROSPECTIVE_COLLECTION_ONLY"
DECISION_DATE = "2026-09-16"

#: The permanently forfeited window. Declared, not derived, because it is a
#: statement about what was lost rather than about what the data can price.
FORFEITED_FIRST = "2026-08-17"
FORFEITED_LAST = "2026-09-15"
FORFEITED_SESSION_COUNT = 21

#: What the authorisation does NOT do. Carried in the record so a later reader
#: cannot mistake a re-armed clock for a promoted strategy.
AUTHORISATION_EXCLUDES = (
    "does NOT reset the candidate",
    "does NOT reset its historical inception",
    "does NOT erase the missing 21 sessions",
    "does NOT backfill a mark",
    "does NOT promote the strategy",
    "does NOT make it capital eligible",
)

#: The FROZEN maturity rule, inspected rather than invented. The Stage-26
#: contract says: "forward_observations counts recorded marks; a horizon with
#: fewer marks than its length is PENDING, never scored", and
#: ``ShadowBook.replay`` implements exactly that (``len(marks)``). Maturity is
#: therefore defined by the NUMBER OF LEGITIMATELY COLLECTED MARKS and NOT by
#: sessions elapsed since inception - which is why a 21-session gap does not
#: corrupt the clock and prospective resumption needs no redefinition of it.
#: The clock was never running: zero marks is zero progress.
H63_MATURITY_RULE = "COUNT_OF_LEGITIMATELY_COLLECTED_MARKS"
H63_MATURITY_RULE_SOURCE = (
    "alpha_agent.stage26_challenger_expansion.forward_evidence_contract"
    " -> guarantees.pending_vs_matured_distinguished;"
    " alpha_agent.tournament.ShadowBook.replay -> forward_observations")


# --------------------------------------------------------------------------- #
# 1b. THE SESSION BOUNDARY - borrowed from the canonical owners, never invented
# --------------------------------------------------------------------------- #
#: The ONE owner of "which session had completed at this instant". Named in every
#: record this module writes so a reader can see the boundary was not guessed.
SESSION_BOUNDARY_OWNER = \
    "paper_trader.engine.market_session.resolve_expected_session"
#: The ONE authoritative NYSE calendar. This module declares no holiday itself.
CALENDAR_OWNER = "paper_trader.engine.exchange_calendar"
#: The boundary is the EXCHANGE CLOSE, not the owned-data arrival cutoff. This is
#: the whole correction: ``DEFAULT_CLOSE_CUTOFF_ET`` (17:30 ET) exists so an
#: operator is not told a session is ready before its EOD bars could have landed,
#: and using it to decide whether a session had HAPPENED is what let a completed
#: session stay collectable. 16:15 ET is after the close and before the cutoff.
SESSION_BOUNDARY_RULE = "EXCHANGE_REGULAR_CLOSE_ET_NOT_OWNED_DATA_ARRIVAL_CUTOFF"
EPOCH_FLOOR_RULE = (
    "max(last_permanently_forfeited_session,"
    " latest_completed_eligible_session_at_activation_wall_clock)")

#: The calendar query window is spelled as "from the start of LAST year", which
#: needs no date arithmetic at all. Only closures BETWEEN the resolved session
#: and the clock's own date can matter - at most a couple of weeks - so this is
#: enormously generous, and it deliberately avoids computing a date here: date
#: arithmetic in this module is the machinery a catch-up would need, and its
#: absence is a structural invariant the suite enforces.
_CALENDAR_LOOKBACK_YEARS = 1


# --------------------------------------------------------------------------- #
# 1c. THE CORRECTION TO THE ONE RECORD THAT WAS WRITTEN WITH THE DEFECT
# --------------------------------------------------------------------------- #
#: The live activation record is IMMUTABLE - it states truthfully what was done,
#: including the floor that was wrong - so the correction is declared here and
#: recorded beside it, rather than rewritten into it.
#:
#: It is BOUND TO THAT ONE RECORD by its activation timestamp and by the floor it
#: stamped, so it can never be applied to a different activation, and it only
#: ever RAISES a floor. It is declared in code rather than living only in the
#: correction artifact because "2026-09-16 can never be collected" must survive
#: the deletion of a JSON file: a rule that depends on an artifact being present
#: is exactly the kind of silence this book already lost 21 sessions to.
#:
#: The VALUE is not a guess and not a hard-code around the bug: it is what
#: ``latest_completed_eligible_session(DEFECTIVE_ACTIVATION_AT)`` derives from
#: the canonical owners, and a test recomputes it from them rather than trusting
#: this literal.
DEFECTIVE_ACTIVATION_AT = "2026-09-16T20:15:01.093648+00:00"
DEFECTIVE_ACTIVATION_FLOOR = "2026-09-15"
CORRECTED_ACTIVATION_FLOOR = "2026-09-16"
CORRECTION_ID = "S25_EPOCH_BOUNDARY_AND_SINGLE_AGENT_FIX_SEP17_V1"
CORRECTION_DEFECT = \
    "STALE_PANEL_COULD_LEAVE_AN_ALREADY_COMPLETED_SESSION_COLLECTABLE"
CORRECTION_SCHEMA_VERSION = "s25_prospective_epoch_correction.v1"
GOVERNANCE_CORRECTION_ARTIFACT = "s25_prospective_epoch_correction.json"
CORRECTION_CONFIRM_TOKEN = "CORRECT_S25_PROSPECTIVE_EPOCH"

#: A mark whose session had already completed when collection was authorised. It
#: is PRESERVED (an evidence row is never deleted or restated) and it counts
#: toward the RAW total only. It is not TRUE_FORWARD evidence, so it counts
#: toward none of: effective observations, the h63 clock, promotion evidence or
#: capital eligibility.
QUARANTINE_CLASS = "NONCOUNTING_PRE_EFFECTIVE_EPOCH_OBSERVATION"
QUARANTINE_REASON = \
    "SESSION_COMPLETED_BEFORE_GOVERNED_PROSPECTIVE_ACTIVATION_BOUNDARY"


# --------------------------------------------------------------------------- #
# 2. THE STATE VOCABULARY
# --------------------------------------------------------------------------- #
STATE_AWAITING_ACTIVATION = "AWAITING_ACTIVATION"
STATE_NOT_DUE = "NOT_DUE"
STATE_ADVANCED = "ADVANCED"
STATE_ALREADY_MARKED = "ALREADY_MARKED"
STATE_DATA_BLOCKED = "DATA_BLOCKED"
STATE_REFUSED_RETROACTIVE = "REFUSED_RETROACTIVE"
STATE_IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
STATE_NO_BOOK = "NO_BOOK"
STATE_FAILED = "FAILED"
STATES = (STATE_AWAITING_ACTIVATION, STATE_NOT_DUE, STATE_ADVANCED,
          STATE_ALREADY_MARKED, STATE_DATA_BLOCKED, STATE_REFUSED_RETROACTIVE,
          STATE_IDENTITY_MISMATCH, STATE_NO_BOOK, STATE_FAILED)

#: States the hosting runtime should read as forward PROGRESS.
PROGRESS_STATES = (STATE_ADVANCED,)
#: States that mean "correctly nothing to do right now".
QUIET_STATES = (STATE_NOT_DUE, STATE_ALREADY_MARKED, STATE_AWAITING_ACTIVATION)
#: States that mean "a human or a data owner must act".
BLOCKED_STATES = (STATE_DATA_BLOCKED,)
#: States that are a refusal, never a retry.
INTEGRITY_STATES = (STATE_REFUSED_RETROACTIVE, STATE_IDENTITY_MISMATCH,
                    STATE_NO_BOOK)

R_NO_ACTIVATION = "NO_GOVERNED_REARM_ACTIVATION_RECORD"
R_NO_NEW_SESSION = "NO_ELIGIBLE_SESSION_AFTER_THE_PROSPECTIVE_EPOCH_FLOOR"
R_DUE = "AN_ELIGIBLE_COMPLETED_SESSION_IS_UNMARKED"
R_FORFEITED_WINDOW = "SESSION_IS_INSIDE_THE_PERMANENTLY_FORFEITED_WINDOW"
R_NO_PANEL = "OWNED_PRICE_PANEL_UNAVAILABLE"
R_NO_BENCHMARK = "BENCHMARK_HAS_NO_SESSION_AXIS_IN_THE_PANEL"
R_COVERAGE = "PRICED_COVERAGE_BELOW_THE_FROZEN_NAV_KERNEL_FLOOR"
R_PRE_EFFECTIVE_EPOCH = "SESSION_IS_AT_OR_BEFORE_THE_EFFECTIVE_PROSPECTIVE_EPOCH"

ACTIVATE_CONFIRM_TOKEN = "REARM_S25_PROSPECTIVE_COLLECTION"


def _safety() -> dict:
    return {
        "research_only": True,
        "paper_only": True,
        "backfills_forward_rows": False,
        "creates_order": False,
        "creates_fill": False,
        "mutates_holdings": False,
        "mutates_cash": False,
        "mutates_nav": False,
        "promotes_model": False,
        "activates_sleeve": False,
        "allocates_capital": False,
        "runs_daily_close": False,
        "calls_portfolio_cycle": False,
        "may_spend_money": False,
        "revives_retired_scheduled_task": False,
        "retired_host_stays_disabled": RETIRED_HOST,
        "safety_badges": ["RESEARCH ONLY", "PREVIEW ONLY", "MANUAL REVIEW",
                          "NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF",
                          "NO MODEL PROMOTION"],
    }


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# 3. THE GOVERNANCE RECORD - first-write-wins, and it pins the epoch floor
# --------------------------------------------------------------------------- #
def store_dir(store_root=None) -> Path:
    if store_root is not None:
        return Path(store_root)
    raw = os.environ.get(STORE_DIR_ENV)
    return Path(raw) if raw else _DEFAULT_STORE_DIR


def governance_path(store_root=None) -> Path:
    return store_dir(store_root) / GOVERNANCE_ARTIFACT


def load_governance(store_root=None) -> Optional[dict]:
    """The stored re-arm authorisation, or ``None`` when it does not exist.

    ``None`` is the FAIL-CLOSED default: with no authorisation on this store no
    session is collectable, so a deployed-but-not-activated repair marks
    nothing. The record is never created as a side effect of reading it.
    """
    p = governance_path(store_root)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


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


def is_forfeited_session(session: Optional[str]) -> bool:
    """Is this session one of the 21 permanently forfeited ones?

    The SECOND of the two independent guards. It is a pure comparison against a
    declared window, so it holds even if the stored epoch floor is wrong,
    missing or somehow older than the gap.
    """
    if not session:
        return False
    return FORFEITED_FIRST <= str(session)[:10] <= FORFEITED_LAST


def epoch_floor_for(latest_completed_session: Optional[str]) -> str:
    """The prospective epoch floor, derived at activation - never declared.

    ``max(the last forfeited session, the latest completed ELIGIBLE session at
    the activation wall clock)``. The first term means a stale view at
    activation cannot lower the floor into the forfeited window; the second
    means a session that had ALREADY CLOSED when collection was authorised
    cannot be left collectable.

    PURE. The argument must be the CLOCK's answer, from
    :func:`latest_completed_eligible_session` - not the owned panel's newest
    bar, which lags the exchange and was the original defect.
    """
    latest = str(latest_completed_session or "")[:10]
    return max(FORFEITED_LAST, latest) if latest else FORFEITED_LAST


def _coerce_now(now=None) -> "_dt.datetime":
    """A timezone-aware instant from ``None`` / ISO string / datetime."""
    if now is None:
        return _dt.datetime.now(_dt.timezone.utc)
    if isinstance(now, _dt.datetime):
        ts = now
    else:
        ts = _dt.datetime.fromisoformat(str(now).replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=_dt.timezone.utc)


def latest_completed_eligible_session(now=None, *, resolver=None) -> str:
    """The latest ELIGIBLE session that had COMPLETED at ``now``. Delegated.

    This is the half of the boundary the DATA may not answer, so it is asked of
    the canonical owners and of nothing else:

    * ``engine.exchange_calendar`` supplies the AUTHORITATIVE closures, so a
      holiday is never mistaken for a session that could close;
    * ``engine.market_session.resolve_expected_session`` walks the clock back to
      the session that actually traded.

    The cutoff handed over is ``market_session.REGULAR_CLOSE_ET`` - the EXCHANGE
    CLOSE. ``DEFAULT_CLOSE_CUTOFF_ET`` is a data-arrival grace period and using
    it here would reintroduce the defect one level up: at 16:15 ET the session
    has happened whether or not its bars have landed.

    ``resolver`` is for hermetic tests and takes the same instant; production
    never passes it. No holiday, weekend or cutoff rule is implemented here.
    """
    ts = _coerce_now(now)
    if resolver is not None:
        return str(resolver(ts))[:10]
    from paper_trader.engine import exchange_calendar as XC   # noqa: PLC0415
    from paper_trader.engine import market_session as MS      # noqa: PLC0415
    today = MS.to_eastern(ts).date()
    non_sessions = XC.non_sessions_between(
        "%04d-01-01" % (today.year - _CALENDAR_LOOKBACK_YEARS),
        today.isoformat())
    return MS.resolve_expected_session(
        ts, close_cutoff_et=MS.REGULAR_CLOSE_ET,
        non_sessions=non_sessions).market_date_iso


# --------------------------------------------------------------------------- #
# 3b. THE APPEND-ONLY CORRECTION, AND THE EFFECTIVE FLOOR IT PRODUCES
# --------------------------------------------------------------------------- #
def correction_path(store_root=None) -> Path:
    """Beside the activation record, in the SAME governance store. Not inside
    it: the activation record is immutable."""
    return store_dir(store_root) / GOVERNANCE_CORRECTION_ARTIFACT


def load_corrections(store_root=None) -> Optional[dict]:
    """The stored correction log, or ``None`` when none has been recorded."""
    p = correction_path(store_root)
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def declared_correction_floor(gov: Optional[dict]) -> Optional[str]:
    """The DECLARED correction for the one record written with the defect.

    Returns the corrected floor when ``gov`` IS that record - matched on BOTH
    its activation timestamp and the floor it stamped - and ``None`` otherwise.
    A later activation, a re-activation with a different timestamp or a record
    whose floor has since been raised is left completely alone.
    """
    if not gov:
        return None
    if str(gov.get("activated_at") or "") != DEFECTIVE_ACTIVATION_AT:
        return None
    if str(gov.get("prospective_epoch_floor_session") or "")[:10] \
            != DEFECTIVE_ACTIVATION_FLOOR:
        return None
    return CORRECTED_ACTIVATION_FLOOR


def effective_epoch_floor(gov: Optional[dict],
                          corrections: Optional[dict] = None) -> Optional[str]:
    """The floor that GOVERNS collection now: the highest of every source.

    ``max(the stored floor, the declared correction for that stored record,
    every recorded correction's effective floor)``.

    MONOTONIC BY CONSTRUCTION - a correction can only ever RAISE the floor, so
    no artifact, and no replay of one, can reopen a session that has already
    been governed out. Returns ``None`` only when there is no activation at all,
    which is the fail-closed state in which nothing is collectable anyway.
    """
    if not gov:
        return None
    floors = [str(gov.get("prospective_epoch_floor_session") or "")[:10]]
    declared = declared_correction_floor(gov)
    if declared:
        floors.append(declared)
    for entry in ((corrections or {}).get("corrections") or ()):
        val = str((entry or {}).get(
            "effective_prospective_epoch_floor_session") or "")[:10]
        if val:
            floors.append(val)
    live = [f for f in floors if f]
    return max(live) if live else None


def governed_epoch(store_root=None, *, gov=None,
                   corrections=None) -> dict:
    """The governed prospective epoch for this store, and where it came from.

    READ-ONLY. One place every read model asks, so the original floor, the
    correction and the effective floor can never be reported inconsistently by
    two different surfaces.
    """
    gov = load_governance(store_root) if gov is None else gov
    corrections = (load_corrections(store_root) if corrections is None
                   else corrections)
    stored = str((gov or {}).get("prospective_epoch_floor_session") or "")[:10]
    effective = effective_epoch_floor(gov, corrections)
    recorded = [e.get("correction_id")
                for e in ((corrections or {}).get("corrections") or ())]
    return {
        "activated": bool(gov),
        "activated_at": (gov or {}).get("activated_at"),
        "original_prospective_epoch_floor_session": stored or None,
        "effective_prospective_epoch_floor_session": effective,
        "epoch_floor_was_corrected": bool(effective and stored
                                          and effective > stored),
        "declared_correction_applies": bool(declared_correction_floor(gov)),
        "recorded_correction_ids": [c for c in recorded if c],
        "correction_artifact_present": bool(corrections),
        "epoch_floor_rule": EPOCH_FLOOR_RULE,
        "session_boundary_owner": SESSION_BOUNDARY_OWNER,
        "session_boundary_rule": SESSION_BOUNDARY_RULE,
        "calendar_owner": CALENDAR_OWNER,
        "first_valid_evidence_session": (
            "the first eligible completed session STRICTLY AFTER %s" % effective
            if effective else None),
        "backfill": "FORBIDDEN",
    }


def classify_marks(marks, effective_floor: Optional[str]) -> dict:
    """Split RECORDED marks into VALID TRUE_FORWARD and QUARANTINED. PURE.

    A mark dated at or before the effective epoch is a NONCOUNTING
    PRE-EFFECTIVE-EPOCH OBSERVATION: its session had already completed when
    collection was authorised, so it is not forward evidence. It is PRESERVED
    exactly as recorded - this function reads, splits and counts, and never
    deletes, edits, reorders or restates a row.

    With no effective floor (no activation) NOTHING is asserted to be valid:
    the fail-closed default treats every recorded mark as unclassified rather
    than as evidence.
    """
    rows = list(marks or ())
    dates = [str((m or {}).get("date") or "")[:10] for m in rows]
    if not effective_floor:
        return {"raw_marks": len(rows), "raw_mark_dates": dates,
                "valid_marks": None, "valid_mark_dates": [],
                "quarantined_marks": None, "quarantined_mark_dates": [],
                "governed": False}
    valid = [d for d in dates if d and d > effective_floor]
    quarantined = [d for d in dates if d and d <= effective_floor]
    return {"raw_marks": len(rows), "raw_mark_dates": dates,
            "valid_marks": len(valid), "valid_mark_dates": valid,
            "quarantined_marks": len(quarantined),
            "quarantined_mark_dates": quarantined,
            "governed": True,
            "quarantine_class": QUARANTINE_CLASS if quarantined else None,
            "quarantine_reason": QUARANTINE_REASON if quarantined else None}


def governed_mark_view(store_root, book: Optional[dict],
                       shadow_book_id: str) -> Optional[dict]:
    """The governed RAW-vs-VALID split for the ONE identity this module governs.

    Returns ``None`` for any other shadow book, so a read model that asks about
    every book gets an answer only where a governed prospective epoch exists and
    is never handed a classification this module has no authority to make.
    """
    if str(shadow_book_id) != SHADOW_BOOK_ID:
        return None
    epoch = governed_epoch(store_root)
    view = classify_marks((book or {}).get("marks") or [],
                          epoch["effective_prospective_epoch_floor_session"])
    return {**view,
            "effective_prospective_epoch_floor_session":
                epoch["effective_prospective_epoch_floor_session"],
            "original_prospective_epoch_floor_session":
                epoch["original_prospective_epoch_floor_session"],
            "epoch_floor_was_corrected": epoch["epoch_floor_was_corrected"]}


def record_epoch_correction(*, confirm: Optional[str] = None,
                            now: Optional[str] = None, store_root=None,
                            correction_id: str = CORRECTION_ID,
                            resolver=None,
                            operator: str = "HUMAN_GOVERNANCE_DECISION") -> dict:
    """Record the APPEND-ONLY governance correction. First-write-wins per id.

    It NEVER touches the activation record: that document states truthfully what
    was done, wrong floor included, and rewriting it would destroy the only
    evidence that the defect existed. The correction is a new document beside it
    that says what the boundary should have been and why.

    APPEND-ONLY. An existing entry is never edited, reordered or removed; a
    re-run with the same ``correction_id`` appends nothing and reports
    ``ALREADY_RECORDED``. The correction can only RAISE the floor, and a request
    that would lower one is refused rather than applied.
    """
    if confirm != CORRECTION_CONFIRM_TOKEN:
        return {"state": "REFUSED", "reason": "CONFIRMATION_TOKEN_REQUIRED",
                "expected": CORRECTION_CONFIRM_TOKEN, "wrote": False}
    gov = load_governance(store_root)
    if not gov:
        return {"state": "REFUSED", "reason": R_NO_ACTIVATION, "wrote": False,
                "detail": ("there is no activation record to correct; a "
                           "correction may not create one")}
    existing = load_corrections(store_root) or {}
    entries = list(existing.get("corrections") or ())
    if any(str((e or {}).get("correction_id")) == str(correction_id)
           for e in entries):
        return {"state": "ALREADY_RECORDED", "wrote": False,
                "correction_id": correction_id,
                "corrections": existing,
                "effective_prospective_epoch_floor_session":
                    effective_epoch_floor(gov, existing)}

    stored = str(gov.get("prospective_epoch_floor_session") or "")[:10]
    activated_at = str(gov.get("activated_at") or "")
    # The corrected boundary is DERIVED from the canonical owners against the
    # activation's own wall clock. Nothing is typed in, and the activation
    # timestamp is the record's, not this run's.
    clock_session = latest_completed_eligible_session(activated_at,
                                                      resolver=resolver)
    corrected = epoch_floor_for(clock_session)
    if corrected <= stored:
        return {"state": "NOT_REQUIRED", "wrote": False,
                "original_prospective_epoch_floor_session": stored,
                "canonical_completed_session_at_activation": clock_session,
                "effective_prospective_epoch_floor_session": stored,
                "detail": ("the stored floor already governs the session that "
                           "had completed at activation; a correction may only "
                           "raise a floor")}

    book = S25O.read_book(store_dir(store_root), SHADOW_BOOK_ID)
    split = classify_marks(book.get("marks") or [], corrected)
    entry = {
        "correction_id": correction_id,
        "schema_version": CORRECTION_SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "recorded_at": now or _now_iso(),
        "recorded_by": operator,

        # --- the original record: PRESERVED, never rewritten -------------- #
        "original_activation_record": "PRESERVED",
        "original_activation_artifact": GOVERNANCE_ARTIFACT,
        "original_activation_mutated": False,
        "activation_timestamp": activated_at,
        "original_prospective_epoch_floor_session": stored,
        "original_latest_completed_session_at_activation":
            gov.get("latest_completed_session_at_activation"),

        # --- the defect, named ------------------------------------------- #
        "defect": CORRECTION_DEFECT,
        "defect_detail": (
            "the floor's second term was the OWNED PANEL's newest session, and a"
            " panel lags the exchange; at %s (16:15:01 ET) the %s session had"
            " closed 15 minutes earlier and the panel's newest bar was still"
            " %s, so an already-completed session was left collectable"
            % (activated_at, clock_session, stored)),

        # --- the corrected boundary, DERIVED ----------------------------- #
        "canonical_completed_market_session_at_activation": clock_session,
        "effective_prospective_epoch_floor_session": corrected,
        "first_valid_evidence_session":
            "the first eligible completed session STRICTLY AFTER %s" % corrected,
        "epoch_floor_rule": EPOCH_FLOOR_RULE,
        "session_boundary_owner": SESSION_BOUNDARY_OWNER,
        "session_boundary_rule": SESSION_BOUNDARY_RULE,
        "calendar_owner": CALENDAR_OWNER,

        # --- what a pre-epoch mark becomes, if one ever exists ----------- #
        "quarantine_class": QUARANTINE_CLASS,
        "quarantine_reason": QUARANTINE_REASON,
        "quarantined_counts_toward": ["RAW_MARK_COUNT"],
        "quarantined_counts_toward_none_of": [
            "TRUE_FORWARD_OBSERVATIONS", "EFFECTIVE_OBSERVATIONS",
            "H63_LEGITIMATE_MARKS", "PROMOTION_EVIDENCE",
            "CAPITAL_ELIGIBILITY_EVIDENCE"],
        "raw_marks_at_correction": split["raw_marks"],
        "raw_mark_dates_at_correction": split["raw_mark_dates"],
        "valid_marks_at_correction": split["valid_marks"],
        "quarantined_mark_dates_at_correction":
            split["quarantined_mark_dates"],
        "evidence_row_deleted": False,
        "evidence_row_restated": False,

        # --- the identity, UNCHANGED ------------------------------------- #
        "candidate_id": CANDIDATE_ID,
        "strategy_name": STRATEGY_NAME,
        "shadow_book_id": SHADOW_BOOK_ID,
        "strategy_spec_hash": STRATEGY_SPEC_HASH,
        "original_inception": ORIGINAL_INCEPTION,
        "candidate_identity": "UNCHANGED",
        "strategy_identity": "UNCHANGED",
        "membership_state": "UNCHANGED",
        "membership": MEMBERSHIP_SIZE,

        "backfill": "FORBIDDEN",
        "safety": _safety(),
    }
    doc = {
        "schema_version": CORRECTION_SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "append_only": True,
        "corrects_artifact": GOVERNANCE_ARTIFACT,
        "corrects_artifact_mutated": False,
        "candidate_id": CANDIDATE_ID,
        "shadow_book_id": SHADOW_BOOK_ID,
        "corrections": entries + [entry],
    }
    _atomic_write_json(correction_path(store_root), doc)
    return {"state": "CORRECTED", "wrote": True, "correction_id": correction_id,
            "correction": entry, "corrections": doc,
            "original_prospective_epoch_floor_session": stored,
            "canonical_completed_session_at_activation": clock_session,
            "effective_prospective_epoch_floor_session": corrected}


def activate(*, latest_completed_session: str = "",
             confirm: Optional[str] = None,
             now: Optional[str] = None, store_root=None, resolver=None,
             operator: str = "HUMAN_GOVERNANCE_DECISION") -> dict:
    """Authorise PROSPECTIVE collection for the EXISTING S25 identity. Once.

    First-write-wins: a second call returns the stored record unchanged, so
    re-running deployment can never move the epoch floor forward or backward.
    It writes ONE governance document and nothing else - no mark, no book, no
    candidate, no registration.

    THE FLOOR COMES FROM THE CLOCK AND THE CALENDAR, NOT FROM THE PANEL.
    ``latest_completed_session`` is the owned panel's newest session and is
    recorded for PROVENANCE ONLY - so a later reader can see how far the data
    lagged the exchange at activation. It no longer contributes to the floor,
    because a panel that lags would leave an already-completed session
    collectable, which is the defect this signature change closes.
    """
    if confirm != ACTIVATE_CONFIRM_TOKEN:
        return {"state": "REFUSED", "reason": "CONFIRMATION_TOKEN_REQUIRED",
                "expected": ACTIVATE_CONFIRM_TOKEN, "wrote": False}
    existing = load_governance(store_root)
    if existing:
        return {"state": "ALREADY_ACTIVE", "wrote": False,
                "governance": existing}
    activated_at = now or _now_iso()
    eligible = latest_completed_eligible_session(activated_at, resolver=resolver)
    floor = epoch_floor_for(eligible)
    panel_latest = str(latest_completed_session or "")[:10] or None
    record = {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE,

        # --- the human decision ---------------------------------------- #
        "decision": DECISION,
        "decision_date": DECISION_DATE,
        "decided_by": operator,
        "authorisation_excludes": list(AUTHORISATION_EXCLUDES),

        # --- the identity, UNCHANGED ----------------------------------- #
        "candidate_id": CANDIDATE_ID,
        "strategy_name": STRATEGY_NAME,
        "shadow_book_id": SHADOW_BOOK_ID,
        "strategy_spec_hash": STRATEGY_SPEC_HASH,
        "original_inception": ORIGINAL_INCEPTION,
        "membership": MEMBERSHIP_SIZE,
        "horizon_days": HORIZON_DAYS,
        "candidate_identity": "UNCHANGED",
        "strategy_identity": "UNCHANGED",
        "membership_state": "UNCHANGED",

        # --- what was lost --------------------------------------------- #
        "forfeited_sessions": {"first": FORFEITED_FIRST,
                               "last": FORFEITED_LAST,
                               "count": FORFEITED_SESSION_COUNT,
                               "state": "PERMANENTLY_FORFEITED"},
        "backfill": "FORBIDDEN",

        # --- the prospective epoch, distinct from inception ------------- #
        # The strategy's inception stays 2026-08-16. The OBSERVATION epoch
        # starts here, and the two are recorded separately so no reader can
        # mistake a re-armed clock for a book that has been accruing since
        # August.
        "activated_at": activated_at,
        # The CLOCK's answer - the term that actually sets the floor.
        "latest_completed_eligible_session_at_activation": eligible,
        # The PANEL's newest session - provenance only. The gap between this and
        # the line above IS the lag that the original rule mistook for a clock.
        "latest_completed_session_at_activation": panel_latest,
        "owned_panel_lagged_the_exchange_at_activation": bool(
            panel_latest and panel_latest < eligible),
        "prospective_epoch_floor_session": floor,
        "epoch_floor_rule": EPOCH_FLOOR_RULE,
        "session_boundary_owner": SESSION_BOUNDARY_OWNER,
        "session_boundary_rule": SESSION_BOUNDARY_RULE,
        "calendar_owner": CALENDAR_OWNER,
        "first_collectable_session":
            "the first eligible completed session STRICTLY AFTER %s" % floor,
        "observation_epoch_is_not_inception": True,

        # --- the frozen clock, preserved ------------------------------- #
        "h63_maturity_rule": H63_MATURITY_RULE,
        "h63_maturity_rule_source": H63_MATURITY_RULE_SOURCE,
        "marks_at_activation": 0,

        # --- the runtime ----------------------------------------------- #
        "runtime_owner": RUNTIME_OWNER,
        "mark_owner": MARK_OWNER,
        "retired_host_not_revived": RETIRED_HOST,
        "safety": _safety(),
    }
    _atomic_write_json(governance_path(store_root), record)
    return {"state": "ACTIVATED", "wrote": True, "governance": record}


# --------------------------------------------------------------------------- #
# 4. WHICH ONE SESSION IS COLLECTABLE - pure, and it never catches up
# --------------------------------------------------------------------------- #
def resolve_evidence_session(*, panel_sessions, last_recorded: Optional[str],
                             floor: Optional[str]) -> dict:
    """The ONE session this invocation may mark, or ``None``. PURE.

    ``last_recorded`` is the book's latest mark date, or its inception when no
    mark exists. The threshold is the LATER of that and the epoch floor, so both
    the frozen monotonic rule and the prospective epoch bind at once.

    The chosen session is the NEWEST eligible one. Older unmarked sessions are
    returned as ``skipped_sessions`` and are never marked by a later call:
    catching up would compute a mark for a session that has already closed,
    which is a retrospective number wearing a prospective label.
    """
    threshold = max(str(last_recorded or ""), str(floor or ""))
    after = [d for d in sorted({str(s)[:10] for s in (panel_sessions or ())})
             if d and d > threshold]
    # Guard 2 is applied HERE, before a session can be chosen, rather than only
    # as a check on the chosen one. A forfeited session is therefore never even
    # a candidate, so a wrong or corrupted floor cannot promote one.
    forfeited = [d for d in after if is_forfeited_session(d)]
    eligible = [d for d in after if not is_forfeited_session(d)]
    if not eligible:
        # Name the REAL cause. "Nothing after the floor" and "everything after
        # the floor is forfeited" are different facts, and reporting the second
        # as the first would hide a floor that had been moved into the gap.
        return {"session": None,
                "reason": (R_FORFEITED_WINDOW if forfeited else R_NO_NEW_SESSION),
                "threshold": threshold, "skipped_sessions": [],
                "n_skipped": 0, "forfeited_excluded": forfeited,
                "n_forfeited_excluded": len(forfeited)}
    return {"session": eligible[-1], "reason": R_DUE, "threshold": threshold,
            "skipped_sessions": eligible[:-1], "n_skipped": len(eligible) - 1,
            "forfeited_excluded": forfeited,
            "n_forfeited_excluded": len(forfeited)}


def panel_close_provider(panel: Optional[dict]):
    """A ``close_provider(symbols, date) -> {symbol: close}`` over the OWNED panel.

    EXACT SESSION MATCH ONLY. A symbol with no bar dated exactly ``date`` is
    omitted, never resolved to its nearest or latest close: forward-filling a
    missing session is how a book gets priced against a stale mark, and the
    frozen NAV kernel is entitled to see the real coverage and refuse.
    """
    series = ((panel or {}).get("series") or {})

    def _closes(symbols, date: str) -> dict:
        out: dict = {}
        d = str(date)[:10]
        for sym in (symbols or ()):
            ser = series.get(str(sym).upper())
            if not ser:
                continue
            dates = ser.get("dates") or []
            try:
                i = dates.index(d)
            except ValueError:
                continue
            adj = (ser.get("adj") or [])
            if i < len(adj) and adj[i] and float(adj[i]) > 0:
                out[str(sym)] = float(adj[i])
        return out

    return _closes


def _load_owned_panel(panel_loader=None, panel_path=None) -> Optional[dict]:
    """The owned trailing panel, through its ONE canonical reader.

    ``api.price_panel`` is the single trailing-price panel owner, so this reads
    nothing itself and keeps no CSV parser of its own. Injected in tests; lazily
    imported in production so this research module carries no import-time
    dependency on the application.
    """
    if panel_loader is not None:
        return panel_loader()
    from paper_trader.api import price_panel as PP    # noqa: PLC0415
    return PP.load_owned_current_panel(panel_path)


def _identity_drift(book: dict) -> list:
    """Every way the stored book fails to be the identity we re-armed."""
    inc = (book or {}).get("inception") or {}
    spec = inc.get("spec") or {}
    members = inc.get("membership") or []
    drift: list = []
    if str(book.get("candidate_id") or "") != CANDIDATE_ID:
        drift.append("candidate_id=%s" % book.get("candidate_id"))
    if str(spec.get("spec_hash") or "") != STRATEGY_SPEC_HASH:
        drift.append("spec_hash=%s" % str(spec.get("spec_hash"))[:16])
    if str(inc.get("date") or "")[:10] != ORIGINAL_INCEPTION:
        drift.append("inception=%s" % inc.get("date"))
    if len(members) != MEMBERSHIP_SIZE:
        drift.append("membership=%d" % len(members))
    return drift


# --------------------------------------------------------------------------- #
# 5. THE ONE STAGE THE CANONICAL RUNTIME CALLS
# --------------------------------------------------------------------------- #
def advance(*, now: Optional[str] = None, store_root=None, panel_loader=None,
            panel_path=None, registry_factory=None) -> dict:
    """Advance the frozen S25 book by AT MOST ONE prospective mark.

    Idempotent, restart-safe and bounded: it resolves one session, and the
    frozen producer refuses a duplicate or retroactive date on its own. Every
    exit is a structured state; nothing raises into the hosting runtime.
    """
    out = {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "mark_owner": MARK_OWNER,
        "runtime_owner": RUNTIME_OWNER,
        "phase": PHASE,
        "challenger_id": CANDIDATE_ID,
        "strategy_name": STRATEGY_NAME,
        "shadow_book_id": SHADOW_BOOK_ID,
        "at": now or _now_iso(),
        "backfill": "FORBIDDEN",
        "paid_dollars": 0.0,
        "h63_maturity_rule": H63_MATURITY_RULE,
        "forfeited_sessions": {"first": FORFEITED_FIRST, "last": FORFEITED_LAST,
                               "count": FORFEITED_SESSION_COUNT},
        "marks_written_this_run": 0,
        "safety": _safety(),
    }
    root = store_dir(store_root)

    # --- (a) the governed authorisation, or nothing happens -------------- #
    gov = load_governance(store_root)
    if not gov:
        return {**out, "state": STATE_AWAITING_ACTIVATION,
                "reason": R_NO_ACTIVATION,
                "detail": ("prospective collection is not authorised on this "
                           "store; no session is collectable and no mark can "
                           "be written")}
    # The GOVERNING floor is the EFFECTIVE one - the stored floor raised by any
    # declared or recorded correction. Reading the stored floor directly here is
    # what would let a session that had already closed at activation be marked
    # once the panel caught up. Both are reported: an operator must be able to
    # see that the floor in the immutable record is not the floor in force.
    epoch = governed_epoch(store_root, gov=gov)
    floor = epoch["effective_prospective_epoch_floor_session"] or ""
    out["prospective_epoch_floor_session"] = epoch[
        "original_prospective_epoch_floor_session"]
    out["effective_prospective_epoch_floor_session"] = floor
    out["epoch_floor_was_corrected"] = epoch["epoch_floor_was_corrected"]
    out["session_boundary_owner"] = SESSION_BOUNDARY_OWNER
    out["calendar_owner"] = CALENDAR_OWNER
    out["original_inception"] = ORIGINAL_INCEPTION
    out["governance_decision"] = gov.get("decision")

    # --- (b) the frozen identity, verified before anything is read ------- #
    book = S25O.read_book(root, SHADOW_BOOK_ID)
    if not (book.get("inception") or {}):
        return {**out, "state": STATE_NO_BOOK,
                "detail": "no shadow book with an inception snapshot at %s"
                          % S25O.book_path(root, SHADOW_BOOK_ID)}
    drift = _identity_drift(book)
    if drift:
        return {**out, "state": STATE_IDENTITY_MISMATCH, "drift": drift,
                "detail": ("the stored book is not the frozen identity this "
                           "stage re-armed; refusing rather than repairing")}
    marks = book.get("marks") or []
    last_recorded = (marks[-1].get("date") if marks
                     else (book.get("inception") or {}).get("date"))
    # RAW vs VALID, stated every cycle. ``marks_before`` stays the RAW count -
    # it is literally what is on disk - but it is never the governed evidence
    # count, which is why the valid/quarantined split travels beside it.
    split = classify_marks(marks, floor)
    out["marks_before"] = len(marks)
    out["raw_marks"] = split["raw_marks"]
    out["valid_marks_before"] = split["valid_marks"]
    out["quarantined_marks"] = split["quarantined_marks"]
    out["quarantined_mark_dates"] = split["quarantined_mark_dates"]
    out["quarantine_class"] = split.get("quarantine_class")
    out["quarantine_reason"] = split.get("quarantine_reason")
    out["last_recorded"] = last_recorded
    out["identity_verified"] = True

    # --- (c) the owned panel supplies the session axis -------------------- #
    try:
        panel = _load_owned_panel(panel_loader, panel_path)
    except Exception as exc:                          # noqa: BLE001
        return {**out, "state": STATE_DATA_BLOCKED, "reason": R_NO_PANEL,
                "detail": "%s: %s" % (type(exc).__name__, str(exc)[:200])}
    series = ((panel or {}).get("series") or {})
    if not series:
        return {**out, "state": STATE_DATA_BLOCKED, "reason": R_NO_PANEL,
                "detail": "the owned trailing panel holds no series"}
    bench_dates = (series.get(BENCHMARK) or {}).get("dates") or []
    if not bench_dates:
        return {**out, "state": STATE_DATA_BLOCKED, "reason": R_NO_BENCHMARK,
                "detail": ("the benchmark %s has no session axis, so no excess "
                           "return could be attributed to a mark" % BENCHMARK)}
    out["panel_latest_session"] = str(bench_dates[-1])[:10]

    # --- (d) the ONE collectable session --------------------------------- #
    res = resolve_evidence_session(panel_sessions=bench_dates,
                                   last_recorded=last_recorded, floor=floor)
    out["threshold_session"] = res["threshold"]
    out["n_sessions_skipped"] = res["n_skipped"]
    out["sessions_skipped"] = res["skipped_sessions"]
    out["n_forfeited_sessions_excluded"] = res.get("n_forfeited_excluded", 0)
    session = res["session"]
    if not session:
        return {**out, "state": STATE_NOT_DUE, "reason": res["reason"],
                "next_evidence_gate": ("the first eligible completed session "
                                       "strictly after %s" % res["threshold"]),
                "detail": ("no completed session after the prospective epoch "
                           "floor is unmarked; this is the correct state when "
                           "the repair has outrun the panel")}
    out["evidence_session"] = session

    # --- (e) defence in depth, independent of the resolver ---------------- #
    # Unreachable through resolve_evidence_session, which already excludes the
    # window. It stands for the caller that resolves a session some other way:
    # the refusal belongs next to the write, not only next to the filter.
    if is_forfeited_session(session):
        return {**out, "state": STATE_REFUSED_RETROACTIVE,
                "reason": R_FORFEITED_WINDOW,
                "detail": ("session %s lies inside the permanently forfeited "
                           "window %s..%s and can never be marked"
                           % (session, FORFEITED_FIRST, FORFEITED_LAST))}
    if session <= ORIGINAL_INCEPTION:
        return {**out, "state": STATE_REFUSED_RETROACTIVE,
                "reason": R_FORFEITED_WINDOW,
                "detail": "session %s is not after inception %s"
                          % (session, ORIGINAL_INCEPTION)}
    # The EFFECTIVE epoch, checked again immediately before the write. The
    # resolver already thresholds on it, so this is unreachable through
    # resolve_evidence_session - and that is the point: the guard belongs next
    # to the write as well as next to the filter, because the session that must
    # never be marked here (2026-09-16) is one the panel will eventually be able
    # to price perfectly well.
    if floor and session <= floor:
        return {**out, "state": STATE_REFUSED_RETROACTIVE,
                "reason": R_PRE_EFFECTIVE_EPOCH,
                "detail": ("session %s had already completed when prospective "
                           "collection was authorised (effective epoch floor "
                           "%s) and can never be TRUE_FORWARD evidence"
                           % (session, floor))}

    # --- (f) the FROZEN producer does the writing ------------------------ #
    # Only reached once a legal, strictly-prospective session exists, so the
    # registry handle is opened as late as possible and never merely to look.
    try:
        from . import tournament as T                 # noqa: PLC0415
        from . import stage26_challenger_expansion as S26   # noqa: PLC0415

        shadow_root = root / S25O.SHADOW_SUBDIR
        cfg = {"shadow_book_root": str(shadow_root)}
        provider = S26.make_shadow_mark_provider(
            shadow_root, close_provider=panel_close_provider(panel),
            benchmark=BENCHMARK)

        # SCOPED TO THE IDENTITY THIS STAGE RE-ARMED. advance_shadow_books is
        # the tournament's owner and iterates every ACTIVE book; this stage
        # re-armed exactly one, so any other book yields None - an honest
        # coverage diagnostic on a book this stage does not own, never a mark.
        def _scoped(candidate_id: str, date: str):
            if str(candidate_id) != CANDIDATE_ID:
                return None
            return provider(candidate_id, date)

        registry = (registry_factory() if registry_factory is not None
                    else T.CandidateRegistry(root / S25O.REGISTRY_DB))
        try:
            results = T.advance_shadow_books(registry, cfg,
                                             mark_provider=_scoped,
                                             evidence_date=session)
        finally:
            if registry_factory is None:
                registry.close()
    except Exception as exc:                          # noqa: BLE001
        return {**out, "state": STATE_FAILED,
                "detail": "%s: %s" % (type(exc).__name__, str(exc)[:220])}

    mine = [r for r in (results or [])
            if str(r.get("shadow_book_id")) == SHADOW_BOOK_ID]
    out["producer_results"] = mine
    out["n_out_of_scope_books"] = len(results or []) - len(mine)
    if not mine:
        return {**out, "state": STATE_FAILED,
                "detail": ("the frozen producer did not report on %s; the "
                           "registry may not list it as ACTIVE"
                           % SHADOW_BOOK_ID)}
    status = str(mine[0].get("status"))
    if status == "ADVANCED":
        after = S25O.read_book(root, SHADOW_BOOK_ID)
        after_split = classify_marks(after.get("marks") or [], floor)
        return {**out, "state": STATE_ADVANCED, "marks_written_this_run": 1,
                "marks_after": after_split["raw_marks"],
                "valid_marks_after": after_split["valid_marks"],
                "detail": "one prospective mark recorded for %s" % session}
    if status == "DATA_HOLD":
        return {**out, "state": STATE_DATA_BLOCKED, "reason": R_COVERAGE,
                "detail": ("the frozen NAV kernel refused %s rather than "
                           "assume unpriced names were flat" % session)}
    if status == "NO_ADVANCE":
        return {**out, "state": STATE_ALREADY_MARKED,
                "detail": "%s is not after the latest recorded mark" % session}
    if status == "NO_ADVANCE_RETROACTIVE":
        return {**out, "state": STATE_REFUSED_RETROACTIVE,
                "reason": R_FORFEITED_WINDOW,
                "detail": str(mine[0].get("reason"))[:200]}
    return {**out, "state": STATE_FAILED,
            "detail": "the frozen producer reported %s" % status}


def status(*, store_root=None, today: Optional[str] = None) -> dict:
    """READ-ONLY. What the re-armed stream looks like right now.

    Writes nothing and opens no read-write handle, so an operator or a health
    read model can ask without touching the store.
    """
    root = store_dir(store_root)
    gov = load_governance(store_root)
    corrections = load_corrections(store_root)
    epoch = governed_epoch(store_root, gov=gov, corrections=corrections)
    book = S25O.read_book(root, SHADOW_BOOK_ID)
    inc = (book.get("inception") or {})
    marks = book.get("marks") or []
    days = S25O.days_since(inc.get("date") or ORIGINAL_INCEPTION, today)
    # THE GOVERNED COUNT IS THE VALID COUNT. ``ShadowBook.replay`` reports
    # ``forward_observations = len(marks)`` and that stays true of the RAW
    # store, but a mark whose session had already closed at activation is not
    # forward evidence, so every governed number below is derived from the
    # VALID split and the raw count is published beside it under its own name.
    split = classify_marks(marks, epoch["effective_prospective_epoch_floor_session"])
    raw = split["raw_marks"]
    valid = split["valid_marks"]
    governed = valid if valid is not None else 0
    valid_dates = split["valid_mark_dates"]
    return {
        "schema_version": SCHEMA_VERSION,
        "challenger_id": CANDIDATE_ID,
        "strategy_name": STRATEGY_NAME,
        "shadow_book_id": SHADOW_BOOK_ID,
        "original_inception": inc.get("date"),
        "strategy_spec_hash": (inc.get("spec") or {}).get("spec_hash"),
        "membership": len(inc.get("membership") or []),
        "horizon_days": HORIZON_DAYS,
        "h63_maturity_rule": H63_MATURITY_RULE,

        # --- RAW: what is literally on disk ---------------------------- #
        "raw_marks": raw,
        "raw_mark_dates": split["raw_mark_dates"],

        # --- GOVERNED: what is TRUE_FORWARD evidence ------------------- #
        "valid_marks": valid,
        "valid_mark_dates": valid_dates,
        "quarantined_marks": split["quarantined_marks"],
        "quarantined_mark_dates": split["quarantined_mark_dates"],
        "quarantine_class": split.get("quarantine_class"),
        "quarantine_reason": split.get("quarantine_reason"),

        # ``marks`` and ``forward_observations`` are the GOVERNED count, so a
        # reader that never learns the new key names cannot be handed a
        # quarantined observation as evidence.
        "marks": governed,
        "last_mark": (valid_dates[-1] if valid_dates else None),
        "forward_observations": governed,
        "h63_valid_marks": governed,
        "effective_observations": governed,
        "horizon_pending": governed < HORIZON_DAYS,
        "days_since_inception": days,
        "stream_state": S25O.stream_state(
            status="ACTIVE" if inc else None, marks=governed, days=days),
        "rearmed": bool(gov),
        "governance_decision": (gov or {}).get("decision"),
        "prospective_epoch_floor_session":
            epoch["original_prospective_epoch_floor_session"],
        "effective_prospective_epoch_floor_session":
            epoch["effective_prospective_epoch_floor_session"],
        "epoch_floor_was_corrected": epoch["epoch_floor_was_corrected"],
        "first_valid_evidence_session": epoch["first_valid_evidence_session"],
        "governed_epoch": epoch,
        "forfeited_sessions": {"first": FORFEITED_FIRST, "last": FORFEITED_LAST,
                               "count": FORFEITED_SESSION_COUNT,
                               "state": "PERMANENTLY_FORFEITED"},
        "backfill": "FORBIDDEN",
        "runtime_owner": RUNTIME_OWNER,
        "mark_owner": MARK_OWNER,
        "retired_host_not_revived": RETIRED_HOST,
    }


__all__ = ["SCHEMA_VERSION", "COMPOSITION_OWNER", "MARK_OWNER", "RUNTIME_OWNER",
           "RETIRED_HOST", "PHASE", "STORE_DIR_ENV", "GOVERNANCE_ARTIFACT",
           "CANDIDATE_ID", "STRATEGY_NAME", "SHADOW_BOOK_ID",
           "STRATEGY_SPEC_HASH", "ORIGINAL_INCEPTION", "MEMBERSHIP_SIZE",
           "HORIZON_DAYS", "BENCHMARK", "DECISION", "DECISION_DATE",
           "FORFEITED_FIRST", "FORFEITED_LAST", "FORFEITED_SESSION_COUNT",
           "AUTHORISATION_EXCLUDES", "H63_MATURITY_RULE",
           "H63_MATURITY_RULE_SOURCE", "STATES", "PROGRESS_STATES",
           "QUIET_STATES", "BLOCKED_STATES", "INTEGRITY_STATES",
           "STATE_AWAITING_ACTIVATION", "STATE_NOT_DUE", "STATE_ADVANCED",
           "STATE_ALREADY_MARKED", "STATE_DATA_BLOCKED",
           "STATE_REFUSED_RETROACTIVE", "STATE_IDENTITY_MISMATCH",
           "STATE_NO_BOOK", "STATE_FAILED", "ACTIVATE_CONFIRM_TOKEN",
           "store_dir", "governance_path", "load_governance",
           "is_forfeited_session", "epoch_floor_for", "activate",
           "resolve_evidence_session", "panel_close_provider", "advance",
           "status",
           # Release S25-epoch-fix - the session boundary and its correction.
           "SESSION_BOUNDARY_OWNER", "CALENDAR_OWNER", "SESSION_BOUNDARY_RULE",
           "EPOCH_FLOOR_RULE", "DEFECTIVE_ACTIVATION_AT",
           "DEFECTIVE_ACTIVATION_FLOOR", "CORRECTED_ACTIVATION_FLOOR",
           "CORRECTION_ID", "CORRECTION_DEFECT", "CORRECTION_SCHEMA_VERSION",
           "GOVERNANCE_CORRECTION_ARTIFACT", "CORRECTION_CONFIRM_TOKEN",
           "QUARANTINE_CLASS", "QUARANTINE_REASON", "R_PRE_EFFECTIVE_EPOCH",
           "latest_completed_eligible_session", "correction_path",
           "load_corrections", "declared_correction_floor",
           "effective_epoch_floor", "governed_epoch", "classify_marks",
           "governed_mark_view", "record_epoch_correction"]
