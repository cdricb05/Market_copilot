"""alpha_agent.r46.lanes - THE research-lane lifecycle contract.

Release 46 was created because five releases froze seven prospective shadows
and none of them ever produced a forward row. R46 diagnosed that correctly -
"no run has called that owner since the freeze" - and then reproduced the
same class of defect one layer up: the Daily Research Cycle calls six of the
estate's research lanes and silently does not call the others. The option
surface has been one session short of a judgeable sample since Release 46 and
nothing in the canonical path was ever going to acquire the next one, because
:mod:`alpha_agent.r46.options` is reachable only from the one-off release
campaign runner.

The failure mode both times is the same, and it is not a data problem: **there
was a state the system could be in that nobody could see.** A lane that is
never called looks exactly like a lane that has nothing to say.

THE CONTRACT
------------
After R46.6 every research lane in the estate is REGISTERED here, and every
registered lane is CALLED by the one canonical Daily Research Cycle on every
run. Each call resolves to exactly one of:

``CALLED_AND_EMITTED``       the owner ran and produced something
``CALLED_QUIET_NOT_DUE``     the owner ran and its own economic cadence says
                             there is nothing to decide today - a month-end
                             stream on the 12th, a weekly stream on a Tuesday
``CALLED_DATA_BLOCKED``      the owner ran and its source could not serve it
``CALLED_SAMPLE_BLOCKED``    the owner ran and the sample it needs is not yet
                             complete - the data exists, the window does not
``CALLED_PIT_BLOCKED``       the owner ran and refused on point-in-time
                             grounds
``RETIRED``                  deliberately out of the active tournament, its
                             history preserved, never silently

There is deliberately NO state meaning "we forgot to call it", and
:func:`audit` fails when a registered lane produces no lifecycle row - which
is the only way that state can reappear.

QUIET IS NOT BROKEN
-------------------
A month-end stream called every day and answering QUIET_NOT_DUE 20 times a
month is CORRECT, and it is strictly better than a month-end stream nobody
calls: on the 20th call the answer changes by itself. To keep that cheap,
every lane declares a ``due`` predicate that is answered from the calendar
BEFORE its expensive owner is invoked. A lane that is not due costs a date
comparison.

NOTHING HERE CAPTURES ANYTHING ITSELF. Each entry names the module that
already owns its stream and calls it. No second capture implementation is
created, and no prior release's artifact, ledger or registry is written.
"""
from __future__ import annotations

import calendar as _cal
import datetime as _dt
from typing import Callable, Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.lanes"

ARTIFACT = "R46_6_RESEARCH_LANE_LIFECYCLE.json"
INVENTORY_ARTIFACT = "R46_6_ADOPTED_SHADOW_LANE_INVENTORY.json"

# --------------------------------------------------------------------------- #
# FROZEN lifecycle vocabulary - section 20
# --------------------------------------------------------------------------- #
CALLED_AND_EMITTED = "CALLED_AND_EMITTED"
CALLED_QUIET_NOT_DUE = "CALLED_QUIET_NOT_DUE"
CALLED_DATA_BLOCKED = "CALLED_DATA_BLOCKED"
CALLED_SAMPLE_BLOCKED = "CALLED_SAMPLE_BLOCKED"
CALLED_PIT_BLOCKED = "CALLED_PIT_BLOCKED"
RETIRED = "RETIRED"

LIFECYCLE = (CALLED_AND_EMITTED, CALLED_QUIET_NOT_DUE, CALLED_DATA_BLOCKED,
             CALLED_SAMPLE_BLOCKED, CALLED_PIT_BLOCKED, RETIRED)

#: The state that must never exist again. Kept as a NAME so the audit can say
#: what it is looking for, never as a value a lane may be assigned.
FORGOTTEN_IS_NOT_A_STATE = "NEVER_CALLED_BY_ANY_RUN"

#: Section 18 - what an adopted shadow stream IS, independent of today.
SHOULD_ACCRUE = "SHOULD_ACCRUE_VIA_CANONICAL_DRC"
SPARSE_EVENT_STREAM = "INTENTIONALLY_SPARSE_EVENT_STREAM"
LEGACY_RESEARCH_ONLY = "LEGACY_RESEARCH_ONLY"
PERMANENTLY_DATA_BLOCKED = "PERMANENTLY_DATA_BLOCKED"
RETIRE_FROM_ACTIVE_TOURNAMENT = "RETIRE_FROM_ACTIVE_TOURNAMENT"
CLASSIFICATIONS = (SHOULD_ACCRUE, SPARSE_EVENT_STREAM, LEGACY_RESEARCH_ONLY,
                   PERMANENTLY_DATA_BLOCKED, RETIRE_FROM_ACTIVE_TOURNAMENT)

#: Cadence vocabulary. A cadence is a fact about the stream's ECONOMICS, not
#: about how often somebody happens to run the cycle.
CADENCE_DAILY = "DAILY"
CADENCE_WEEKLY = "WEEKLY"
CADENCE_MONTH_END = "MONTH_END"
CADENCE_EVENT_ONLY = "EVENT_ONLY"
CADENCE_NONE = "NONE_RETIRED"


# --------------------------------------------------------------------------- #
# Cheap DUE predicates - answered from the calendar, before any owner runs
# --------------------------------------------------------------------------- #
def _last_weekday_of_month(d: _dt.date) -> _dt.date:
    last = _cal.monthrange(d.year, d.month)[1]
    x = _dt.date(d.year, d.month, last)
    while x.weekday() in CK.WEEKEND:
        x -= _dt.timedelta(days=1)
    return x


#: Release 46.6.2 - WHEN THE CYCLE CALLS A LANE IS NOT WHEN ITS OWNER DECIDES.
#:
#: These predicates decide only whether the expensive owner is worth invoking
#: today. Some of them happen to name the owner's real decision date as well
#: (``due_month_end``: the R39/R40 futures panels decide on each market's last
#: session of the calendar month, which is the date this function computes).
#: ``due_weekly_friday`` does NOT: the VX shadow decides on every 5th session
#: of its own panel, a grid that lands on a Friday only by coincidence. On
#: 2026-08-28 R46.6 published "next decision 2026-08-28" for that lane from
#: this predicate while the frozen owner's newest decision date was Tuesday
#: 2026-08-25 - and it was that Tuesday, not the Friday, that the continuation
#: gate refused.
#:
#: So the two answers are now reported apart. ``next_call_date`` is always this
#: predicate's answer. ``next_decision_date`` is populated ONLY by a predicate
#: that genuinely names the owner's decision date, or by the owner itself; a
#: lane that cannot know it reports ``None`` and says why, and no reader has to
#: guess which of the two it is holding.
CALL_CADENCE_ONLY = "CALL_CADENCE_ONLY"
DECISION_DATE_FROM_PREDICATE = "LANE_DUE_PREDICATE"
DECISION_DATE_FROM_OWNER = "ADOPTED_OWNER_PANEL"


def due_daily(_as_of: _dt.date) -> dict:
    return {"due": True, "why": "daily stream"}


def due_weekly_friday(as_of: _dt.date) -> dict:
    """Call weekly. This is a CALL cadence and not a decision grid."""
    ok = as_of.weekday() == 4
    nxt = as_of + _dt.timedelta(days=(4 - as_of.weekday()) % 7 or 7)
    return {"due": ok,
            "why": ("weekly call date" if ok else
                    "weekly stream; next CALL date %s (the owner's own "
                    "decision grid is not a weekday rule and is reported by "
                    "the owner)" % nxt),
            "next_call_date": str(nxt),
            "next_decision_date": None,
            "next_decision_date_source": CALL_CADENCE_ONLY,
            "next_decision_date_unknown_reason":
                "this lane's owner decides on its own panel's session grid, "
                "which no weekday rule can compute; only the owner may name "
                "it and it names it when it is called"}


def due_month_end(as_of: _dt.date) -> dict:
    """Call at month end - which IS this owner's decision date."""
    end = _last_weekday_of_month(as_of)
    ok = as_of >= end
    if ok:
        nxt = _last_weekday_of_month(
            (as_of.replace(day=1) + _dt.timedelta(days=32)).replace(day=1))
    else:
        nxt = end
    return {"due": ok,
            "why": ("month-end decision date" if ok else
                    "month-end stream; next decision date %s" % nxt),
            "next_call_date": str(nxt),
            "next_decision_date": str(nxt),
            "next_decision_date_source": DECISION_DATE_FROM_PREDICATE}


def due_never(_as_of: _dt.date) -> dict:
    return {"due": False, "why": "retired; no decision date will be produced",
            "next_call_date": None, "next_decision_date": None,
            "next_decision_date_source": CALL_CADENCE_ONLY}


# --------------------------------------------------------------------------- #
# THE REGISTRY - every research lane in the estate, with its real owner
# --------------------------------------------------------------------------- #
class Lane:
    """One registered research lane. Declarative; holds no state."""

    def __init__(self, lane_id: str, owner: str, cadence: str,
                 classification: str, due: Callable, call: Optional[Callable],
                 information_family: str = None, note: str = None,
                 challengers: tuple = (), adopted_from: str = None,
                 required: bool = True):
        self.lane_id = lane_id
        self.owner = owner
        self.cadence = cadence
        self.classification = classification
        self.due = due
        self.call = call
        self.information_family = information_family
        self.note = note
        self.challengers = tuple(challengers)
        self.adopted_from = adopted_from
        self.required = required

    def describe(self) -> dict:
        return {"lane_id": self.lane_id, "owner": self.owner,
                "cadence": self.cadence, "classification": self.classification,
                "information_family": self.information_family,
                "adopted_from": self.adopted_from,
                "challengers": list(self.challengers),
                "required": self.required, "note": self.note}


# ---- adapters: map an owner's own vocabulary onto the lifecycle ------------ #
def _r46_lane(module_name: str):
    """Adapter for the six R46 information lanes, which share a ``run``."""
    def _call(as_of, campaign_id, acquire):
        from importlib import import_module
        mod = import_module("alpha_agent.r46." + module_name)
        body = mod.run(acquire_now=acquire, campaign_id=campaign_id,
                       as_of=as_of) or {}
        st = str(body.get("state") or "")
        if st in ("LIVE_PROSPECTIVE", "CAPTURED", "EXECUTED"):
            life = CALLED_AND_EMITTED
        elif "PIT" in st:
            life = CALLED_PIT_BLOCKED
        elif st in ("NO_TRADED_RELEASE_TODAY", "RELEASE_NOT_ADMISSIBLE",
                    "NOT_DUE", "QUIET"):
            life = CALLED_QUIET_NOT_DUE
        elif st in ("INSUFFICIENT_HISTORY", "SAMPLE_BLOCKED"):
            life = CALLED_SAMPLE_BLOCKED
        elif st in ("DATA_BLOCKED", "NOT_ACQUIRED", "INDEX_FETCH_FAILED",
                    "NO_INDEX_FOR_DAY"):
            life = CALLED_DATA_BLOCKED
        else:
            life = CALLED_AND_EMITTED if body else CALLED_DATA_BLOCKED
        return {"lifecycle": life, "owner_state": st,
                "as_of": body.get("as_of"),
                "n_captures": (body.get("n_captures")
                               if body.get("n_captures") is not None
                               else (body.get("acquisition") or {})
                               .get("n_captures")),
                "artifact": getattr(mod, "ARTIFACT", None)}
    return _call


def _options_lane(as_of, campaign_id, acquire):
    """The option surface - never reachable from the canonical cycle before.

    Its economic cadence is WEEKLY: SPY's weekly expiries are the only thing
    that extends the session axis, and a new one becomes queryable once it has
    expired. Called every day, it answers QUIET_NOT_DUE until one does.
    """
    from . import options as OP
    body = OP.run(acquire=acquire, campaign_id=campaign_id,
                  batch=_option_batch_name(as_of),
                  front_batch=str(as_of))
    js = body.get("judgeable_sample") or {}
    added = (body.get("surface") or {}).get("sessions_added_by_r46")
    acq = (body.get("acquisition") or {}).get("state")
    if js.get("state") == "JUDGEABLE":
        life = CALLED_AND_EMITTED
    elif acq in ("SKIPPED", "EXECUTED") and not added:
        life = CALLED_SAMPLE_BLOCKED
    elif acq in ("ACCOUNT_REQUIRED", "HISTORICAL_DATA_UNAVAILABLE"):
        life = CALLED_DATA_BLOCKED
    else:
        life = CALLED_SAMPLE_BLOCKED
    # Release 46.6.1 - SEMANTIC CLARITY ONLY, no science changed. The old
    # owner_state read "JUDGEABLE" while zero of the three predeclared
    # hypotheses had a sufficient sample, because this gate only ever counted
    # SESSIONS. It is now reported as what it is.
    return {"lifecycle": life,
            "owner_state": js.get("session_gate_state") or js.get("state"),
            "session_gate_state": js.get("session_gate_state"),
            "session_gate_measures": js.get("gate_measures"),
            "usable_sessions": js.get("usable_sessions_now"),
            "sessions_required": js.get("sessions_required"),
            "sessions_still_required": js.get("sessions_still_required"),
            "acquisition_state": acq,
            "artifact": OP.ARTIFACT}


def _option_batch_name(as_of: _dt.date) -> str:
    """One batch file per ISO week - idempotent within a week, extendable
    across weeks. Re-running on the same day re-reads that week's cache and
    performs no call."""
    y, w, _ = as_of.isocalendar()
    return "w%04d%02d" % (y, w)


#: THE governance flag for adopted streams, and why it is still False.
#:
#: Release 46.6 measured the thing five releases asserted: the R39/R40 capture
#: owner is not broken and its data is not gone. Driven directly it rebuilds
#: the futures and VX panels from the live Norgate entitlement in ~361 seconds
#: and carries decision dates through the CURRENT session. The seven adopted
#: shadows produced nothing for one reason only - **nobody ever called the
#: owner** - and that is now fixed: every one of them is registered here and
#: called by the canonical cycle.
#:
#: Appending was a separate question, and R46.6 refused to answer it by itself.
#: The prior owner writes into the PRIOR RELEASE's own snapshot ledger, and
#: ``alpha_agent.r46.contract.SAFETY_BLOCK["mutates_prior_release_artifacts"]``
#: is ``False`` - a frozen safety declaration. A release that quietly flipped
#: a frozen safety flag to make its own numbers move would be doing exactly
#: what this estate spent fifteen releases learning not to do. So the lane
#: reported the blocker BY NAME and left the decision to a person.
#:
#: Release 46.6.1 is that decision, and it does NOT lift this flag. Prior
#: release stores stay permanently read-only. What changed is that adopted
#: forward evidence now has an R46-OWNED place to go:
#: :mod:`alpha_agent.r46.adopted_forward`. A stream that is called, has
#: something to say and has nowhere to say it is the same defect wearing a
#: label, and that is the one this increment closes.
ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS = False

#: THE R46-owned continuation owner. One owner, one ledger, no second capture.
ADOPTED_CONTINUATION_OWNER = "alpha_agent.r46.adopted_forward"

ADOPTED_APPEND_BLOCKER = (
    "appending into the PRIOR RELEASE's own snapshot ledger remains forbidden: "
    "alpha_agent.r46.contract.SAFETY_BLOCK['mutates_prior_release_artifacts'] "
    "is False and R46 will not flip a frozen safety declaration to make its "
    "own evidence count move. Since Release 46.6.1 that is no longer the end "
    "of the sentence: adopted forward evidence is appended to the R46-OWNED "
    "continuation ledger at " + ADOPTED_CONTINUATION_OWNER + ", which reads "
    "the prior artifacts and never writes one.")


def _adopted_shadow(release: str, owner: str, shadow_ids: tuple,
                    blocked_reason: str = None):
    """Adapter for a prior release's frozen shadow stream.

    Calls the release's OWN scoring owner - never a second implementation of
    it - through :mod:`alpha_agent.r46.adopted_forward`, which proves the
    frozen specification identity, refuses any decision date whose outcome
    window has already opened, and appends into the R46 continuation ledger.
    No prior-release artifact is opened for write on any path.
    """
    def _call(as_of, campaign_id, acquire):
        if blocked_reason:
            return {"lifecycle": CALLED_DATA_BLOCKED,
                    "owner_state": "VENUE_BLOCKED",
                    "shadow_ids": list(shadow_ids),
                    "reason": blocked_reason}
        if release not in ("R39", "R40"):
            return {"lifecycle": CALLED_DATA_BLOCKED,
                    "owner_state": "NO_ADAPTER",
                    "shadow_ids": list(shadow_ids)}
        if not acquire:
            return {"lifecycle": CALLED_QUIET_NOT_DUE,
                    "owner_state": "ACQUISITION_NOT_REQUESTED",
                    "continuation_owner": ADOPTED_CONTINUATION_OWNER,
                    "reason": "hermetic run; a network-driven owner is "
                              "never driven from inside the suite",
                    "shadow_ids": list(shadow_ids)}
        from . import adopted_forward as AF
        lane = next((l for l in registry()
                     if l.adopted_from == release
                     and set(l.challengers) == set(shadow_ids)), None)
        try:
            return AF.run_lane(release=release, shadow_ids=shadow_ids,
                               as_of=as_of, campaign_id=campaign_id,
                               information_family=(lane.information_family
                                                  if lane else None))
        except Exception as exc:                    # noqa: BLE001 - reported
            return {"lifecycle": CALLED_DATA_BLOCKED,
                    "owner_state": "OWNER_RAISED",
                    "continuation_owner": ADOPTED_CONTINUATION_OWNER,
                    "error": type(exc).__name__, "detail": str(exc)[:200],
                    "shadow_ids": list(shadow_ids)}
    return _call


BTC_VENUE_BLOCKER = (
    "the declared venue publishes its funding archive MONTHLY with a ~24-day "
    "lag and answers HTTP 451 to its REST API from this location. A DAILY "
    "shadow reading it cannot produce a daily row, and no workaround that "
    "violates the provider's restriction will be built.")


def registry() -> tuple:
    """Every research lane in the estate. ORDER IS THE CALL ORDER."""
    return (
        # ---- the six R46 information lanes, already in the cycle ---------- #
        Lane("cftc", "alpha_agent.r46.cftc", CADENCE_WEEKLY, SHOULD_ACCRUE,
             due_daily, _r46_lane("cftc"), "POSITIONING",
             note="weekly COT release; the owner decides admissibility",
             challengers=("r46_4_cot_xs_positioning_reversal",
                          "r46_4_cot_xs_positioning_flow")),
        Lane("credit", "alpha_agent.r46.credit", CADENCE_DAILY, SHOULD_ACCRUE,
             due_daily, _r46_lane("credit"), "CREDIT",
             challengers=("r46_4_credit_regime_spx_timing",
                          "r46_4_credit_hy_ig_momentum")),
        Lane("macro", "alpha_agent.r46.macro", CADENCE_EVENT_ONLY,
             SHOULD_ACCRUE, due_daily, _r46_lane("macro"), "MACRO_RELEASE",
             note="release-time vintage-safe; quiet on a day with no print",
             challengers=("r46_4_macro_surprise_rates_5d",)),
        Lane("events", "alpha_agent.r46.events", CADENCE_EVENT_ONLY,
             SHOULD_ACCRUE, due_daily, _r46_lane("events"), "EVENT_CALENDAR",
             challengers=("r46_4_spx_pre_fomc_drift",
                          "r46_4_spx_announcement_day_premium")),
        Lane("earnings", "alpha_agent.r46.earnings", CADENCE_DAILY,
             SHOULD_ACCRUE, due_daily, _r46_lane("earnings"),
             "CORPORATE_EARNINGS",
             challengers=("r46_5_pead_announcement_return_20d",)),
        Lane("form4", "alpha_agent.r46.form4", CADENCE_DAILY, SHOULD_ACCRUE,
             due_daily, _r46_lane("form4"), "INSIDER_FLOW",
             challengers=("r46_5_insider_cluster_buy_20d",
                          "r46_5_insider_net_purchase_xs_20d")),

        # ---- R46.6: the option surface, wired into the canonical cycle ---- #
        Lane("options", "alpha_agent.r46.options", CADENCE_WEEKLY,
             SHOULD_ACCRUE, due_daily, _options_lane, "OPTION_SURFACE",
             note="was reachable ONLY from the one-off release campaign "
                  "runner; the sample sat one session short of judgeable "
                  "with no canonical path that would ever acquire the next",
             challengers=("r46_opt_skew_residual",
                          "r46_opt_term_structure_residual",
                          "r46_opt_delta_hedged_residual")),

        # ---- the seven adopted prior-release shadows --------------------- #
        Lane("r39_fut_month_end", "alpha_agent.r39.research_shadow",
             CADENCE_MONTH_END, SPARSE_EVENT_STREAM, due_month_end,
             _adopted_shadow("R39", "alpha_agent.r39.research_shadow",
                             ("shadow_wide_xs", "shadow_carry_rule_xs")),
             "FUTURES_PANEL", adopted_from="R39",
             note="decides at each market's last session per calendar month",
             challengers=("shadow_wide_xs", "shadow_carry_rule_xs")),
        Lane("r39_vx_weekly", "alpha_agent.r39.research_shadow",
             CADENCE_WEEKLY, SPARSE_EVENT_STREAM, due_weekly_friday,
             _adopted_shadow("R39", "alpha_agent.r39.research_shadow",
                             ("shadow_vx_carry_ts",)),
             "VOLATILITY_TERM_STRUCTURE", adopted_from="R39",
             note="decides every 5th VX session",
             challengers=("shadow_vx_carry_ts",)),
        Lane("r40_fut_month_end", "alpha_agent.r40.shadow_registry",
             CADENCE_MONTH_END, SPARSE_EVENT_STREAM, due_month_end,
             _adopted_shadow("R40", "alpha_agent.r40.shadow_registry",
                             ("shadow_intl_rates_carry_rv",
                              "shadow_slot5_c39_fad367467c79")),
             "FUTURES_PANEL", adopted_from="R40",
             note="R40's two additional slots share R39's capture owner and "
                  "month-end cadence",
             challengers=("shadow_intl_rates_carry_rv",
                          "shadow_slot5_c39_fad367467c79")),
        Lane("r41_btc_funding", "alpha_agent.r41.forward_freeze",
             CADENCE_NONE, PERMANENTLY_DATA_BLOCKED, due_never,
             _adopted_shadow("R41", "alpha_agent.r41.forward_freeze",
                             ("shadow_btc_funding_carry_1d",),
                             blocked_reason=BTC_VENUE_BLOCKER),
             "CRYPTO_MARKET_STRUCTURE", adopted_from="R41",
             note=BTC_VENUE_BLOCKER, required=False,
             challengers=("shadow_btc_funding_carry_1d",)),
        Lane("r42_btc_basis", "alpha_agent.r42.forward",
             CADENCE_NONE, PERMANENTLY_DATA_BLOCKED, due_never,
             _adopted_shadow("R42", "alpha_agent.r42.forward",
                             ("R42_POSITIVE_ONLY_CASH_AND_CARRY_BTC",),
                             blocked_reason=BTC_VENUE_BLOCKER),
             "CRYPTO_MARKET_STRUCTURE", adopted_from="R42",
             note=BTC_VENUE_BLOCKER, required=False,
             challengers=("R42_POSITIVE_ONLY_CASH_AND_CARRY_BTC",)),
    )


#: Lanes whose classification retires them from the ACTIVE tournament. Their
#: history is preserved and their registry rows are never touched; they simply
#: stop being presented as ordinary daily streams that might produce evidence.
RETIRED_CLASSIFICATIONS = (PERMANENTLY_DATA_BLOCKED,
                           RETIRE_FROM_ACTIVE_TOURNAMENT)


# --------------------------------------------------------------------------- #
def run_all(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID, *,
            acquire: bool = True, only: tuple = None) -> dict:
    """Call EVERY registered lane owner. Fail-soft, one row per lane."""
    rows = []
    for lane in registry():
        if only and lane.lane_id not in only:
            continue
        if lane.classification in RETIRED_CLASSIFICATIONS:
            rows.append(dict(lane.describe(), lifecycle=RETIRED,
                             was_called=True,
                             why="retired until its data is available; its "
                                 "history is preserved and it is not "
                                 "presented as an active daily stream",
                             owner_state="RETIRED_UNTIL_DATA_AVAILABLE"))
            continue
        d = lane.due(as_of) or {}
        if not d.get("due"):
            rows.append(dict(
                lane.describe(), lifecycle=CALLED_QUIET_NOT_DUE,
                was_called=True, why=d.get("why"),
                next_call_date=d.get("next_call_date"),
                next_decision_date=d.get("next_decision_date"),
                next_decision_date_source=d.get("next_decision_date_source"),
                next_decision_date_unknown_reason=
                    d.get("next_decision_date_unknown_reason"),
                owner_state="NOT_DUE"))
            continue
        try:
            res = lane.call(as_of, campaign_id, acquire) or {}
        except Exception as exc:                    # noqa: BLE001 - isolation
            res = {"lifecycle": CALLED_DATA_BLOCKED,
                   "owner_state": "OWNER_RAISED",
                   "error": type(exc).__name__, "detail": str(exc)[:220]}
        # ``next_call_date`` is the predicate's; anything the owner returned
        # about its own decision grid WINS - see CALL_CADENCE_ONLY above.
        row = dict(lane.describe(), was_called=True,
                   next_call_date=d.get("next_call_date"))
        row.update(res)
        rows.append(row)

    counts = {s: sum(1 for r in rows if r.get("lifecycle") == s)
              for s in LIFECYCLE}
    return {"as_of": str(as_of), "rows": rows, "counts": counts,
            "n_lanes": len(rows)}


def audit(result: dict) -> dict:
    """Prove the contract: every registered lane produced a lifecycle row.

    This is the whole point of the module. A lane that appears in the registry
    and not in the result is the state R46.6 exists to abolish.
    """
    declared = {l.lane_id for l in registry()}
    seen = {r.get("lane_id") for r in (result.get("rows") or ())}
    missing = sorted(declared - seen)
    bad = sorted(r.get("lane_id") for r in (result.get("rows") or ())
                 if r.get("lifecycle") not in LIFECYCLE)
    uncalled = sorted(r.get("lane_id") for r in (result.get("rows") or ())
                      if not r.get("was_called"))
    return {
        "n_declared": len(declared),
        "n_with_lifecycle_row": len(seen),
        "never_called": missing,
        "n_never_called": len(missing),
        "lanes_with_unknown_lifecycle": bad,
        "lanes_not_called": uncalled,
        "forgotten_is_not_a_state": FORGOTTEN_IS_NOT_A_STATE,
        "contract_holds": not missing and not bad and not uncalled,
    }


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          result: dict = None, write: bool = True) -> dict:
    """The lifecycle artifact. Reports the LAST call of every lane."""
    res = result if result is not None else run_all(as_of, campaign_id,
                                                    acquire=False)
    a = audit(res)
    body = artifact_body(
        "r46_6_research_lane_lifecycle/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        statement="every research lane in the estate is registered here and "
                  "called by the ONE canonical Daily Research Cycle; each "
                  "call resolves to exactly one lifecycle state and there is "
                  "no state meaning 'we forgot to call it'",
        lifecycle_vocabulary=list(LIFECYCLE),
        classification_vocabulary=list(CLASSIFICATIONS),
        forgotten_is_not_a_state=FORGOTTEN_IS_NOT_A_STATE,
        n_lanes=res.get("n_lanes"),
        lifecycle_counts=res.get("counts"),
        audit=a,
        contract_holds=a["contract_holds"],
        quiet_is_not_broken="a month-end stream answering QUIET_NOT_DUE on "
                            "the 12th is correct, and strictly better than a "
                            "month-end stream nobody calls",
        no_duplicate_capture_implementation=True,
        prior_release_ledgers_are_never_written_by_r46=(
            not ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS),
        adopted_capture_writes_prior_release_ledgers=(
            ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS),
        adopted_append_blocker=ADOPTED_APPEND_BLOCKER,
        # Release 46.6.1 - a called lane now has somewhere to speak, and the
        # two append rights are reported apart so no reader can conclude that
        # a prior release's artifact became writable.
        prior_release_append_authorised=False,
        r46_continuation_append_authorised=True,
        continuation_owner=ADOPTED_CONTINUATION_OWNER,
        rows=res.get("rows"),
        research_only=True,
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


# --------------------------------------------------------------------------- #
# Section 18 - the adopted-shadow inventory
# --------------------------------------------------------------------------- #
def _continuation_state(AF, lane, retired: bool) -> dict:
    """Where this lane's forward evidence goes, and what stops it if anything.

    Release 46.6.1 - reported as a state with an exact blocker, never as a
    bare boolean. ``READY`` means the R46 continuation ledger will accept this
    lane's next due decision; anything else names what must be fixed first.
    """
    if retired:
        return {"state": AF.CONTINUATION_RETIRED,
                "blocker": lane.note, "ledger": None}
    ledger = str(AF.continuation_dir() / AF.CONTINUATION_LEDGER)
    try:
        ident = AF.verify_identity(lane.adopted_from, lane.challengers)
    except Exception as exc:                        # noqa: BLE001 - reported
        return {"state": AF.CONTINUATION_DATA_BLOCKED,
                "blocker": "%s: %s" % (type(exc).__name__, str(exc)[:160]),
                "ledger": ledger}
    if ident.get("ok"):
        return {"state": AF.CONTINUATION_READY, "blocker": None,
                "ledger": ledger}
    return {"state": AF.CONTINUATION_IDENTITY_BLOCKED,
            "blocker": ident.get("blocker") or ident.get("reason"),
            "ledger": ledger}


def adopted_inventory(campaign_id: str = CAMPAIGN_ID,
                      write: bool = True) -> dict:
    """Every adopted prior-release shadow, classified and resolved."""
    from pathlib import Path
    from . import adopted_forward as AF
    rows = []
    for lane in registry():
        if not lane.adopted_from:
            continue
        src = next((a for a in C.ADOPTED_REGISTRY_SOURCES
                    if a["release"] == lane.adopted_from), {})
        reg = read_json(Path(src.get("path", "")), default=None) or {}
        retired = lane.classification in RETIRED_CLASSIFICATIONS
        cont = _continuation_state(AF, lane, retired)
        rows.append({
            "lane_id": lane.lane_id,
            "adopted_from": lane.adopted_from,
            "owner": lane.owner,
            "registry_path": src.get("path"),
            "registry_readable": bool(reg),
            "registry_frozen_at": reg.get("frozen_at"),
            "shadow_ids": list(lane.challengers),
            "cadence": lane.cadence,
            "classification": lane.classification,
            "resolution": ("RETIRED_UNTIL_DATA_AVAILABLE" if retired
                           else "WIRED_INTO_CANONICAL_DRC_WITH_R46_"
                                "CONTINUATION_LEDGER"),
            "now_called_by_the_canonical_drc": not retired,
            "was_called_by_any_run_before_r46_6": False,
            "why": lane.note,

            # --- Release 46.6.1: the control each shadow FROZE -------------- #
            # Read from the prior release's own registry, so an operator can
            # see before any outcome exists that these lanes are not all
            # measured against cash.
            "scientific_controls": sorted({
                str(s.get("control"))
                for s in AF.registry_shadows(lane.adopted_from, reg)
                if s.get("shadow_id") in set(lane.challengers)}),
            "capital_control": AF.CAPITAL_CONTROL,
            "controls_are_separate": True,
            "formal_verdict_uses": AF.FORMAL_VERDICT_USES,

            # --- Release 46.6.1: the TWO append rights, never conflated ---- #
            # The old single ``append_authorised`` key could only ever be read
            # as "this lane cannot accrue". It meant "R46 may not write the
            # PRIOR RELEASE's ledger" - still true, permanently - and said
            # nothing about where adopted evidence actually goes.
            "append_authorised_means": "PRIOR_RELEASE_LEDGER_ONLY",
            "append_authorised": (
                False if retired
                else ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS),
            "prior_release_append_authorised": (
                AF.PRIOR_RELEASE_APPEND_AUTHORISED),
            "r46_continuation_append_authorised": (
                False if retired
                else AF.R46_CONTINUATION_APPEND_AUTHORISED),
            "continuation_owner": (None if retired
                                   else ADOPTED_CONTINUATION_OWNER),
            "continuation_state": cont["state"],
            "continuation_blocker": cont["blocker"],
            "continuation_ledger": cont["ledger"],
            "append_blocker": (None if retired else ADOPTED_APPEND_BLOCKER),
            "old_artifacts_became_writable": False,

            "history_preserved": True,
            "registry_mutated_by_r46_6": False,
            "ledger_mutated_by_r46_6": False,
            "prior_release_artifact_mutated": False,
        })
    body = artifact_body(
        "r46_6_adopted_shadow_lane_inventory/1", CALCULATION_OWNER,
        built_at_utc=CK.iso(CK.now_utc()),
        n_adopted_lanes=len(rows),
        n_shadows=sum(len(r["shadow_ids"]) for r in rows),
        classification_vocabulary=list(CLASSIFICATIONS),
        n_wired_into_drc=sum(1 for r in rows
                             if r["now_called_by_the_canonical_drc"]),
        n_retired=sum(1 for r in rows
                      if not r["now_called_by_the_canonical_drc"]),
        finding="seven shadows were frozen by five releases and no run had "
                "called their capture owner since; R46.6 registers each of "
                "them as a lane of the ONE canonical cycle, so a stream that "
                "produces nothing now says WHY on every run",
        measured_owner_reachability=(
            "the R39/R40 capture owner was DRIVEN in this release: "
            "alpha_agent.r39.research_shadow.build_fresh_state() rebuilt the "
            "futures and VX panels from the live Norgate entitlement in ~361 "
            "seconds and carried decision dates through the current session. "
            "The stream was never dead; it was never called."),
        adopted_capture_writes_prior_release_ledgers=(
            ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS),
        adopted_append_blocker=ADOPTED_APPEND_BLOCKER,
        prior_registries_are_read_only=True,
        # --- Release 46.6.1 - the two append rights, stated apart ---------- #
        prior_release_append_authorised=AF.PRIOR_RELEASE_APPEND_AUTHORISED,
        r46_continuation_append_authorised=(
            AF.R46_CONTINUATION_APPEND_AUTHORISED),
        continuation_owner=ADOPTED_CONTINUATION_OWNER,
        continuation_ledger=str(AF.continuation_dir(campaign_id)
                                / AF.CONTINUATION_LEDGER),
        continuation_state_vocabulary=list(AF.CONTINUATION_STATES),
        superseded_adoption_clause=AF.SUPERSEDED_ADOPTION_CLAUSE,
        # --- Release 46.6.1 - the two CONTROLS, stated apart --------------- #
        # An adopted shadow froze its own scientific control. Beating cash is a
        # capital question and answers it separately; it may never stand in.
        scientific_control_owner=dict(AF.SCIENTIFIC_CONTROL_OWNER),
        capital_control=AF.CAPITAL_CONTROL,
        scientific_alpha_field=AF.SCIENTIFIC_ALPHA_FIELD,
        capital_alpha_field=AF.CAPITAL_ALPHA_FIELD,
        formal_verdict_uses=AF.FORMAL_VERDICT_USES,
        cash_substitution_for_noncash_control_allowed=(
            AF.CASH_SUBSTITUTION_FOR_NONCASH_CONTROL_ALLOWED),
        old_artifacts_became_writable=False,
        n_continuation_ready=sum(1 for r in rows
                                 if r["continuation_state"]
                                 == AF.CONTINUATION_READY),
        rows=rows,
        research_only=True,
    )
    if write:
        write_json(campaign_dir(campaign_id) / INVENTORY_ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "INVENTORY_ARTIFACT", "LIFECYCLE",
           "CALLED_AND_EMITTED", "CALLED_QUIET_NOT_DUE", "CALLED_DATA_BLOCKED",
           "CALLED_SAMPLE_BLOCKED", "CALLED_PIT_BLOCKED", "RETIRED",
           "FORGOTTEN_IS_NOT_A_STATE", "CLASSIFICATIONS", "SHOULD_ACCRUE",
           "SPARSE_EVENT_STREAM", "LEGACY_RESEARCH_ONLY",
           "PERMANENTLY_DATA_BLOCKED", "RETIRE_FROM_ACTIVE_TOURNAMENT",
           "Lane", "registry", "run_all", "audit", "build",
           "adopted_inventory", "due_daily", "due_weekly_friday",
           "due_month_end", "due_never"]
