"""alpha_agent.r52.timing_contract - ONE derived timing contract for research.

The scheduler must never become a second timing authority. Every rule in this
module is READ from the owner that already holds it:

* the TRUE_FORWARD ordering and the entry rule -
  :mod:`alpha_agent.r46.clock` (``entry_session_date``,
  ``outcome_window_start_utc``);
* each research lane's cadence and due predicate -
  :mod:`alpha_agent.r46.lanes` (``registry()``);
* the adopted continuation gate (a decision date is refused once the first
  session after it has opened) - :mod:`alpha_agent.r46.adopted_forward`
  (``outcome_window_start``);
* data freshness - :mod:`alpha_agent.r46.marketdata` (``last_session``) on
  the NAV calendar instrument declared by :mod:`alpha_agent.r46.trades`.

What this module DERIVES (and records the derivation of) is only the
invocation plan: WHEN a scheduled runtime invocation is worth making, and
whether a batch emission at a given instant would spend the next entry slot
on stale inputs. Emission at any instant is LEGAL by construction (the R46
ledger enforces the ordering); the derived policy protects slot QUALITY:

    a batch emitted on day D always carries entry date D+1 (next weekday,
    Eastern), and the ledger key is (challenger, version, instrument,
    entry_date, horizon) - so the FIRST emission for an entry date wins the
    slot. A morning emission on a trading day would freeze yesterday's inputs
    into tomorrow's entry while today's session is still to print. The policy
    therefore suppresses emission on a weekday until the owned data has
    refreshed (or until the final-retry threshold, when a legal stale
    emission beats a forfeited slot).

Research only. Writes one artifact into the R52 runtime root. Decides no
science.
"""
from __future__ import annotations

import datetime as _dt

from . import (ACCOUNTABILITY_START_DATE, artifact_body, read_json,
               runtime_dir, write_json)
from ..r46 import adopted_forward as AF
from ..r46 import clock as CK
from ..r46 import contract as C46
from ..r46 import lanes as LN
from ..r46 import marketdata as MD
from ..r46 import trades as TR

CALCULATION_OWNER = "alpha_agent.r52.timing_contract"

ARTIFACT = "research_timing_contract.json"

#: The synthetic lane id for the ONE R46 daily prediction batch (the ~36
#: active challengers emitted together by ``alpha_agent.r46.emit`` through the
#: canonical advance). It is a lane of the timing contract, not of
#: ``alpha_agent.r46.lanes`` - the lane registry tracks capture/continuation
#: owners, while the batch is the tournament's own emission step.
DAILY_BATCH_LANE = "r46_daily_batch"

#: The lane id for outcome scoring. Scoring has a due condition (a horizon
#: genuinely matured on the instrument's own realised calendar) and NO legal
#: cutoff: scoring later loses velocity, never legality.
OUTCOME_SCORING_LANE = "r46_outcome_scoring"

# --------------------------------------------------------------------------- #
# Derived clock facts (each carries its derivation, none is invented)
# --------------------------------------------------------------------------- #
#: When the owned nightly data refresh has, measured, delivered the session's
#: bars. Release 38 measured Norgate World Futures delivery at 16:59 ET and
#: the US equity/futures consolidated refresh completes shortly after; the
#: policy waits a settlement margin past that measurement.
DATA_REFRESH_EXPECTED_ET = "17:15"

#: After this wall-clock time on an emission day, a still-stale data path no
#: longer suppresses emission: a LEGAL emission on yesterday's cutoff beats a
#: forfeited entry slot, and the row records exactly what the rule saw.
FINAL_RETRY_ET = "21:30"

#: The derived scheduled-invocation plan. The Windows task fires the SAME
#: runtime at each of these local (America/New_York == machine-local) times;
#: the runtime decides what is due using the canonical owners. Times, not
#: authorities.
INVOCATION_PLAN = (
    {"local_time": "08:15", "purpose": "SCORING_AND_FORFEITURE_SWEEP",
     "emission": "SUPPRESSED_BY_POLICY",
     "derivation": "matured outcomes and overnight refusals exist by the "
                   "morning after the sessions that produced them; scoring "
                   "and forfeiture recording have no legal cutoff and are "
                   "safe at any hour, while a weekday-morning emission would "
                   "spend the next entry slot on stale inputs"},
    {"local_time": "17:45", "purpose": "POST_DATA_PRIMARY_EMISSION",
     "emission": "ALLOWED_WHEN_FRESH",
     "derivation": "measured nightly delivery (%s ET expected) plus margin; "
                   "the emission enters the NEXT session and the outcome "
                   "window opens at midnight Eastern, so the whole evening "
                   "is legal" % DATA_REFRESH_EXPECTED_ET},
    {"local_time": "19:45", "purpose": "POST_DATA_RETRY",
     "emission": "ALLOWED_WHEN_FRESH",
     "derivation": "bounded retry for a late data refresh or a machine that "
                   "was busy/asleep at the primary trigger"},
    {"local_time": "21:45", "purpose": "FINAL_RETRY_FAIL_OPEN",
     "emission": "ALLOWED_EVEN_IF_STALE",
     "derivation": "past %s ET a legal stale-input emission beats a "
                   "forfeited slot; the month-end continuation lanes still "
                   "require their own data and are never forced" % FINAL_RETRY_ET},
)

EMIT_OK_FRESH = "EMIT_OK_FRESH"
EMIT_OK_WEEKEND = "EMIT_OK_WEEKEND"
EMIT_OK_STALE_FINAL = "EMIT_OK_STALE_FINAL"
EMIT_SUPPRESSED_DATA_PENDING = "EMIT_SUPPRESSED_DATA_PENDING"
EMISSION_MODES = (EMIT_OK_FRESH, EMIT_OK_WEEKEND, EMIT_OK_STALE_FINAL,
                  EMIT_SUPPRESSED_DATA_PENDING)


def _parse_hhmm(s: str) -> _dt.time:
    h, m = str(s).split(":")
    return _dt.time(int(h), int(m))


def owned_last_session():
    """Freshness of the owned data path, from the canonical seam."""
    try:
        return MD.last_session(TR.NAV_CALENDAR_INSTRUMENT)
    except Exception:                     # noqa: BLE001 - freshness is a probe
        return None


def evaluate_emission_policy(now: _dt.datetime = None, *,
                             last_session=None) -> dict:
    """Should a runtime invocation at ``now`` let the batch emit?

    Pure given its inputs; ``last_session`` is injectable for tests. This
    gates only the DAILY BATCH step of the canonical advance. Lane owners
    (month-end continuation, captures) always run - their own due predicates
    and PIT gates decide, and this module never overrides an owner.
    """
    now = now or CK.now_utc()
    et = CK.to_eastern(now)
    today_et = et.date()
    last = last_session if last_session is not None else owned_last_session()
    entry = CK.entry_session_date(now)
    facts = {
        "now_utc": CK.iso(now),
        "now_eastern": str(et),
        "eastern_date": str(today_et),
        "owned_last_session": (str(last) if last else None),
        "entry_session_date": str(entry),
        "slot_cutoff_utc": CK.iso(CK.outcome_window_start_utc(entry)),
    }
    if today_et.weekday() in CK.WEEKEND:
        return dict(facts, emit=True, mode=EMIT_OK_WEEKEND,
                    reason="weekend invocation: identical inputs to the "
                           "prior trading evening for existing challengers "
                           "(the ledger key makes re-offers duplicates), and "
                           "the first legal emission for a newly frozen "
                           "challenger")
    if last is not None and last >= today_et:
        return dict(facts, emit=True, mode=EMIT_OK_FRESH,
                    reason="today's owned session has printed; the batch "
                           "carries today's cutoff into the next session's "
                           "entry")
    if et.time() >= _parse_hhmm(FINAL_RETRY_ET):
        return dict(facts, emit=True, mode=EMIT_OK_STALE_FINAL,
                    reason="final-retry threshold passed with the owned data "
                           "path still stale; a legal emission on the last "
                           "printed session beats a forfeited entry slot, "
                           "and the rows record the cutoff they actually saw")
    return dict(facts, emit=False, mode=EMIT_SUPPRESSED_DATA_PENDING,
                reason="weekday invocation before the owned data refresh: "
                       "emitting now would spend the %s entry slot on stale "
                       "inputs while a fresher legal emission is still "
                       "available this evening" % entry)


# --------------------------------------------------------------------------- #
# The contract - one row per prospective lane, every rule from its owner
# --------------------------------------------------------------------------- #
def _lane_rows(as_of: _dt.date) -> list:
    rows = []
    for lane in LN.registry():
        d = lane.due(as_of) or {}
        retired = lane.classification in LN.RETIRED_CLASSIFICATIONS
        if lane.cadence == LN.CADENCE_MONTH_END:
            cutoff_rule = ("alpha_agent.r46.adopted_forward.outcome_window_"
                           "start: midnight Eastern of the first weekday "
                           "after the decision date")
            start_rule = ("DATA_AVAILABILITY: the owner's panel must carry "
                          "the decision date, which requires the decision "
                          "session's own bars (nightly refresh)")
        elif lane.cadence == LN.CADENCE_WEEKLY and lane.adopted_from:
            cutoff_rule = ("alpha_agent.r46.adopted_forward.outcome_window_"
                           "start on the OWNER's own decision grid (every "
                           "5th VX session - not a weekday rule)")
            start_rule = "DATA_AVAILABILITY of the owner's own panel"
        elif retired:
            cutoff_rule = start_rule = None
        else:
            cutoff_rule = ("NONE: capture lanes stamp acquisition instants; "
                           "later capture loses freshness, never legality")
            start_rule = "ALWAYS: bounded capture on every invocation"
        rows.append({
            "lane_id": lane.lane_id,
            "owner": lane.owner,
            "asset_or_session_type": lane.information_family,
            "cadence": lane.cadence,
            "classification": lane.classification,
            "adopted_from": lane.adopted_from,
            "challengers": list(lane.challengers),
            "due_predicate_owner": "alpha_agent.r46.lanes.%s"
                                   % getattr(lane.due, "__name__", "due"),
            "due_today": bool(d.get("due")),
            "due_why": d.get("why"),
            "next_call_date": d.get("next_call_date"),
            "next_decision_date": d.get("next_decision_date"),
            "next_decision_date_source": d.get("next_decision_date_source"),
            "legal_emission_start_rule": start_rule,
            "legal_emission_cutoff_rule": cutoff_rule,
            "calendar_owner": ("alpha_agent.r46.clock + the owner's own "
                               "realised panel"),
            "required_upstream": ("owned Norgate nightly refresh"
                                  if lane.adopted_from in ("R39", "R40")
                                  else lane.owner),
            "latest_safe_retry_local_time": (
                None if retired else
                ("23:59 on the decision date (window opens at midnight "
                 "Eastern of the next weekday)"
                 if lane.adopted_from in ("R39", "R40")
                 else "any")),
        })
    return rows


def _daily_batch_row(as_of: _dt.date, now: _dt.datetime) -> dict:
    entry = CK.entry_session_date(now)
    return {
        "lane_id": DAILY_BATCH_LANE,
        "owner": "alpha_agent.r46.emit (through alpha_agent.r46.advance)",
        "asset_or_session_type": "ALL_ACTIVE_CHALLENGERS",
        "cadence": "DAILY",
        "classification": "TOURNAMENT_EMISSION",
        "adopted_from": None,
        "challengers": ["<every non-DATA_BLOCKED registry challenger>"],
        "due_predicate_owner": "alpha_agent.r46.clock.entry_session_date",
        "due_today": True,
        "due_why": "every Eastern calendar day offers exactly one entry "
                   "slot: the next weekday",
        "next_call_date": str(as_of),
        "next_decision_date": str(entry),
        "next_decision_date_source": "R46_ENTRY_RULE",
        "entry_rule": C46.ENTRY_RULE["id"],
        "legal_emission_start_rule": "any instant whose Eastern date "
                                     "precedes the entry date",
        "legal_emission_cutoff_rule": "alpha_agent.r46.clock."
                                      "outcome_window_start_utc(entry): "
                                      "midnight Eastern of the entry date",
        "slot_quality_rule": "first emission for an entry date wins the "
                             "ledger key; the policy waits for the owned "
                             "refresh before spending it (see "
                             "evaluate_emission_policy)",
        "calendar_owner": "alpha_agent.r46.clock",
        "required_upstream": "owned Norgate nightly refresh "
                             "(alpha_agent.r46.marketdata)",
        "latest_safe_retry_local_time": "23:59 Eastern on the emission day",
    }


def _outcome_scoring_row() -> dict:
    return {
        "lane_id": OUTCOME_SCORING_LANE,
        "owner": "alpha_agent.r46.judge (through alpha_agent.r46.advance)",
        "asset_or_session_type": "ALL_PENDING_PREDICTIONS",
        "cadence": "DAILY",
        "classification": "OUTCOME_SCORING",
        "adopted_from": None,
        "challengers": ["<every pending prediction>"],
        "due_predicate_owner": "alpha_agent.r46.judge.score_pending "
                               "(realised-session maturity)",
        "due_today": True,
        "due_why": "a prediction is scoreable the day its declared horizon "
                   "has genuinely matured on the instrument's own realised "
                   "bar calendar; scoring is idempotent per prediction_id",
        "legal_emission_start_rule": "maturity on the realised calendar",
        "legal_emission_cutoff_rule": "NONE: late scoring loses velocity, "
                                      "never legality",
        "calendar_owner": "the instrument's own realised bar calendar",
        "required_upstream": "owned Norgate nightly refresh",
        "latest_safe_retry_local_time": "any",
    }


def build(now: _dt.datetime = None, *, write: bool = True) -> dict:
    """Derive the timing contract from the canonical owners. Idempotent."""
    now = now or CK.now_utc()
    as_of = CK.eastern_date(now)
    policy = evaluate_emission_policy(now)
    from ..r46 import campaign_dir as _r46_campaign_dir
    cont = read_json(_r46_campaign_dir() / AF.ARTIFACT, default=None)
    lanes = [_daily_batch_row(as_of, now), _outcome_scoring_row()]
    lanes += _lane_rows(as_of)
    body = artifact_body(
        "r52_research_timing_contract/1", CALCULATION_OWNER,
        derived_at_utc=CK.iso(now),
        as_of_eastern_date=str(as_of),
        accountability_start_date=ACCOUNTABILITY_START_DATE,
        statement="one timing contract, derived from the canonical owners on "
                  "every build; the scheduler consumes it and adds no rule",
        timing_authorities={
            "true_forward_ordering": "alpha_agent.r46.clock",
            "lane_cadence": "alpha_agent.r46.lanes.registry",
            "adopted_continuation_gate": "alpha_agent.r46.adopted_forward",
            "data_freshness": "alpha_agent.r46.marketdata.last_session(%s)"
                              % TR.NAV_CALENDAR_INSTRUMENT,
        },
        n_lanes=len(lanes),
        lanes=lanes,
        emission_policy_now=policy,
        emission_mode_vocabulary=list(EMISSION_MODES),
        data_refresh_expected_et=DATA_REFRESH_EXPECTED_ET,
        final_retry_et=FINAL_RETRY_ET,
        invocation_plan=list(INVOCATION_PLAN),
        adopted_continuation_note=(cont or {}).get("statement"),
        scheduler_is_not_a_timing_authority=True,
        backfill_after_window_open="REFUSED_ALWAYS",
    )
    if write:
        write_json(runtime_dir() / ARTIFACT, body)
    return body


def load() -> dict:
    return read_json(runtime_dir() / ARTIFACT, default={}) or {}


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "DAILY_BATCH_LANE",
           "OUTCOME_SCORING_LANE", "INVOCATION_PLAN",
           "DATA_REFRESH_EXPECTED_ET", "FINAL_RETRY_ET", "EMISSION_MODES",
           "EMIT_OK_FRESH", "EMIT_OK_WEEKEND", "EMIT_OK_STALE_FINAL",
           "EMIT_SUPPRESSED_DATA_PENDING", "owned_last_session",
           "evaluate_emission_policy", "build", "load"]
