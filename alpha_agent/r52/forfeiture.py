"""alpha_agent.r52.forfeiture - THE research forfeiture ledger.

A forfeited prediction is NOT missing data to reconstruct later. It is
deliberately absent TRUE_FORWARD evidence: the legal emission window closed
with no row on the record, and no later run may write one (the R46 ledger and
the continuation gate refuse it by construction). Before R52 that loss was a
silence - the estate could only see it by rebuilding an owner's panel and
noticing a date that never got a row. This module makes it a first-class,
append-only, chain-hashed fact.

One ledger, the canonical desk chain-hash primitives, the R52 runtime root.
Idempotent on ``(lane_id, challenger_scope, decision_date)``. Every row
carries ``backfill_refused: true`` - recording a forfeiture is the OPPOSITE
of backfilling: it writes down that the evidence does not exist and never
will.

Three sources, none of them a new timing authority:

* recorded refusals - the adopted-continuation owner already refuses a
  decision date whose outcome window has opened and writes the refusal into
  its artifact (R46.6.2). Those refusals are mirrored here verbatim, with
  their original evidence.
* the daily batch sweep - for every entry date since the R52 accountability
  start whose outcome window has opened, a date with zero prediction rows is
  a missed batch slot. The entry rule and the window come from
  :mod:`alpha_agent.r46.clock`; nothing is computed twice.
* the month-end sweep - for every month-end decision date since the
  accountability start (the predicate from :mod:`alpha_agent.r46.lanes`)
  whose window has opened, a continuation lane with neither a row nor a
  recorded refusal forfeited the date - typically because no runtime
  invocation happened inside the window at all.

Research only. Never touches the R46 ledgers, any prior release's artifact,
or any operational store.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

from . import (ACCOUNTABILITY_START_DATE, artifact_body, runtime_dir,
               write_json, read_json)
from ..r46 import adopted_forward as AF
from ..r46 import clock as CK
from ..r46 import lanes as LN
from ..r46 import ledger as LG

CALCULATION_OWNER = "alpha_agent.r52.forfeiture"

FORFEITURE_DIRNAME = "forfeitures"
FORFEITURE_LEDGER = "r52_forfeited_opportunities.json"
ARTIFACT = "forfeited_opportunities.json"

#: Scope value for a whole-batch forfeiture (every active challenger lost the
#: same entry date together; one row records it once).
SCOPE_BATCH = "BATCH_ALL_ACTIVE"

REASON_RUNTIME_NOT_INVOKED = "RUNTIME_NOT_INVOKED_IN_LEGAL_WINDOW"
REASON_WINDOW_OPEN_AT_INVOCATION = "OUTCOME_WINDOW_ALREADY_OPEN"
REASON_STRUCTURAL = "STRUCTURALLY_LATE_OWNER_GRID"
REASONS = (REASON_RUNTIME_NOT_INVOKED, REASON_WINDOW_OPEN_AT_INVOCATION,
           REASON_STRUCTURAL)

IDENTITY_KEY = ("lane_id", "challenger_scope", "decision_date")


def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


def forfeiture_dir() -> Path:
    d = runtime_dir() / FORFEITURE_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def rows() -> list:
    return _desk()._read_ledger(forfeiture_dir(), FORFEITURE_LEDGER)


def verify() -> dict:
    report = _desk().verify_ledger(forfeiture_dir(), FORFEITURE_LEDGER)
    return {"all_intact": bool(report.get("intact")), "ledgers": [report],
            "primitives": "api.paper_trading_desk chain-hash ledgers "
                          "(canonical)"}


def identity(row: dict) -> tuple:
    return tuple(str(row.get(k)) for k in IDENTITY_KEY)


def existing_identities() -> set:
    return {identity(r) for r in rows()}


def _row(*, lane_id: str, challenger_scope: str, decision_date: str,
         reason: str, observed_invocation_utc: str,
         legal_emission_start: str, legal_emission_cutoff_utc: str,
         upstream_data_state: str, scheduler_state: str,
         evidence: dict = None, n_cells_lost=None, source: str = None) -> dict:
    return {
        "lane_id": str(lane_id),
        "challenger_scope": str(challenger_scope),
        "decision_date": str(decision_date),
        "reason": str(reason),
        "observed_invocation_utc": observed_invocation_utc,
        "legal_emission_start": legal_emission_start,
        "legal_emission_cutoff_utc": legal_emission_cutoff_utc,
        "upstream_data_state": upstream_data_state,
        "scheduler_state": scheduler_state,
        "outcome_window_already_open": True,
        "backfill_refused": True,
        "n_cells_lost": n_cells_lost,
        "evidence": evidence or {},
        "source": source or CALCULATION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
    }


def append(candidates: list) -> dict:
    """Append forfeiture rows. Duplicates skipped, never overwritten."""
    seen = existing_identities()
    fresh, duplicates = [], []
    for row in candidates:
        for f in IDENTITY_KEY + ("reason", "backfill_refused"):
            if f not in row:
                raise ValueError("forfeiture row missing %r" % f)
        if row.get("backfill_refused") is not True:
            raise ValueError("a forfeiture row must refuse backfill")
        k = identity(row)
        if k in seen:
            duplicates.append(list(k))
            continue
        seen.add(k)
        fresh.append(row)
    appended = []
    if fresh:
        appended = _desk()._append_ledger(forfeiture_dir(),
                                          FORFEITURE_LEDGER, fresh)
    return {"n_offered": len(candidates), "n_appended": len(appended),
            "n_duplicates_skipped": len(duplicates),
            "duplicates": duplicates, "idempotent": True}


# --------------------------------------------------------------------------- #
# Source 1 - refusals the continuation owner already recorded
# --------------------------------------------------------------------------- #
def candidates_from_recorded_refusals(*, scheduler_state: str) -> list:
    """Mirror the adopted-continuation refusals, verbatim, with provenance."""
    from ..r46 import campaign_dir as r46_campaign_dir
    art = read_json(r46_campaign_dir() / AF.ARTIFACT, default=None) or {}
    out = []
    for lane_id, refusals in (art.get("refused_decision_dates") or {}).items():
        feas = (art.get("emission_feasibility") or {}).get(lane_id)
        for r in refusals or ():
            reason = (REASON_STRUCTURAL if feas == "STRUCTURALLY_LATE"
                      else REASON_WINDOW_OPEN_AT_INVOCATION)
            out.append(_row(
                lane_id=lane_id,
                challenger_scope=str(r.get("shadow_id") or "UNKNOWN"),
                decision_date=str(r.get("decision_date")),
                reason=reason,
                observed_invocation_utc=r.get("emission_attempted_utc"),
                legal_emission_start="the owner's panel carrying the "
                                     "decision date",
                legal_emission_cutoff_utc=r.get("outcome_window_start_utc"),
                upstream_data_state="OWNER_PANEL_PRESENT",
                scheduler_state=scheduler_state,
                evidence=dict(r),
                n_cells_lost=1,
                source="alpha_agent.r46.adopted_forward.refusal_evidence"))
    return out


# --------------------------------------------------------------------------- #
# Source 2 - the daily batch sweep
# --------------------------------------------------------------------------- #
def _weekdays(start: _dt.date, end: _dt.date) -> list:
    d, out = start, []
    while d <= end:
        if d.weekday() not in CK.WEEKEND:
            out.append(d)
        d += _dt.timedelta(days=1)
    return out


def candidates_from_daily_batch(now: _dt.datetime = None, *,
                                scheduler_state: str,
                                campaign_id: str = None) -> list:
    """Entry dates whose window opened with zero prediction rows on record."""
    now = now or CK.now_utc()
    from ..r46 import CAMPAIGN_ID
    cid = campaign_id or CAMPAIGN_ID
    preds = LG.predictions(cid)
    have = {str(p.get("effective_as_of")) for p in preds}
    from ..r46 import registry as RG
    registry = RG.load(cid)
    n_active = sum(1 for c in (registry.get("challengers") or ())
                   if c.get("state") != "DATA_BLOCKED")
    start = _dt.date.fromisoformat(ACCOUNTABILITY_START_DATE)
    # The first entry date R52 is accountable for is the one whose emission
    # day is the accountability start.
    first_entry = CK.next_weekday(start)
    out = []
    for entry in _weekdays(first_entry, CK.eastern_date(now)):
        window = CK.outcome_window_start_utc(entry)
        if now < window:
            continue                      # window not open yet - still legal
        if str(entry) in have:
            continue                      # the slot was used
        out.append(_row(
            lane_id="r46_daily_batch",
            challenger_scope=SCOPE_BATCH,
            decision_date=str(entry),
            reason=REASON_RUNTIME_NOT_INVOKED,
            observed_invocation_utc=CK.iso(now),
            legal_emission_start="any instant of the preceding Eastern "
                                 "calendar day(s) mapping to this entry",
            legal_emission_cutoff_utc=CK.iso(window),
            upstream_data_state="UNKNOWN_AT_SWEEP_TIME",
            scheduler_state=scheduler_state,
            evidence={"n_active_challengers_at_sweep": n_active,
                      "entry_rule": "R46_NEXT_TRADING_DAY_CLOSE"},
            n_cells_lost=n_active,
            source=CALCULATION_OWNER))
    return out


# --------------------------------------------------------------------------- #
# Source 3 - the month-end continuation sweep
# --------------------------------------------------------------------------- #
def candidates_from_month_end(now: _dt.datetime = None, *,
                              scheduler_state: str) -> list:
    """Month-end decision dates that got neither a row nor a refusal."""
    now = now or CK.now_utc()
    start = _dt.date.fromisoformat(ACCOUNTABILITY_START_DATE)
    cont_rows = AF.predictions()
    have = {(str(r.get("provenance", {}).get("lane_id") or r.get("lane_id")
             or ""), str(r.get("adopted_challenger_id")),
             str(r.get("decision_date"))) for r in cont_rows}
    have_by_ch_date = {(c, d) for _l, c, d in have}
    from ..r46 import campaign_dir as r46_campaign_dir
    art = read_json(r46_campaign_dir() / AF.ARTIFACT, default=None) or {}
    refused = {(l, str(r.get("shadow_id")), str(r.get("decision_date")))
               for l, rs in (art.get("refused_decision_dates") or {}).items()
               for r in rs or ()}
    out = []
    for lane in LN.registry():
        if lane.cadence != LN.CADENCE_MONTH_END:
            continue
        # every month-end decision date >= accountability start, window open
        ends = []
        probe = start
        while probe <= CK.eastern_date(now):
            e = LN._last_weekday_of_month(probe)
            if start <= e <= CK.eastern_date(now) and e not in ends:
                ends.append(e)
            probe = (probe.replace(day=1) + _dt.timedelta(days=32)).replace(day=1)
        for e in ends:
            window = AF.outcome_window_start(e)
            if now < window:
                continue
            for sid in lane.challengers:
                if (sid, str(e)) in have_by_ch_date:
                    continue
                if (lane.lane_id, sid, str(e)) in refused:
                    continue              # source 1 mirrors it with evidence
                out.append(_row(
                    lane_id=lane.lane_id,
                    challenger_scope=sid,
                    decision_date=str(e),
                    reason=REASON_RUNTIME_NOT_INVOKED,
                    observed_invocation_utc=CK.iso(now),
                    legal_emission_start="the owner's panel carrying the "
                                         "decision session (nightly refresh)",
                    legal_emission_cutoff_utc=CK.iso(window),
                    upstream_data_state="UNKNOWN_AT_SWEEP_TIME",
                    scheduler_state=scheduler_state,
                    evidence={"cadence": lane.cadence,
                              "due_predicate":
                                  "alpha_agent.r46.lanes.due_month_end"},
                    n_cells_lost=1,
                    source=CALCULATION_OWNER))
    return out


# --------------------------------------------------------------------------- #
def sweep(now: _dt.datetime = None, *, scheduler_state: str = "UNKNOWN",
          write: bool = True) -> dict:
    """Record every detectable forfeiture. Append-only, idempotent."""
    now = now or CK.now_utc()
    candidates = []
    stage_errors = []
    for name, fn in (
            ("recorded_refusals",
             lambda: candidates_from_recorded_refusals(
                 scheduler_state=scheduler_state)),
            ("daily_batch",
             lambda: candidates_from_daily_batch(
                 now, scheduler_state=scheduler_state)),
            ("month_end",
             lambda: candidates_from_month_end(
                 now, scheduler_state=scheduler_state))):
        try:
            candidates.extend(fn())
        except Exception as exc:          # noqa: BLE001 - isolation
            stage_errors.append({"source": name,
                                 "error": type(exc).__name__,
                                 "detail": str(exc)[:200]})
    result = append(candidates)
    all_rows = rows()
    body = artifact_body(
        "r52_forfeited_opportunities/1", CALCULATION_OWNER,
        swept_at_utc=CK.iso(now),
        accountability_start_date=ACCOUNTABILITY_START_DATE,
        n_candidates=len(candidates),
        n_appended=result["n_appended"],
        n_duplicates_skipped=result["n_duplicates_skipped"],
        n_total_forfeitures=len(all_rows),
        n_cells_lost_total=sum(int(r.get("n_cells_lost") or 0)
                               for r in all_rows),
        reasons=sorted({str(r.get("reason")) for r in all_rows}),
        by_lane={l: sum(1 for r in all_rows if r.get("lane_id") == l)
                 for l in sorted({str(r.get("lane_id")) for r in all_rows})},
        chain=verify(),
        sweep_stage_errors=stage_errors,
        ledger=str(forfeiture_dir() / FORFEITURE_LEDGER),
        rows=all_rows,
        backfill_refused_on_every_row=all(
            r.get("backfill_refused") is True for r in all_rows),
        a_forfeited_prediction_is_not_missing_data=True,
    )
    if write:
        write_json(runtime_dir() / ARTIFACT, body)
    return body


def load() -> dict:
    return read_json(runtime_dir() / ARTIFACT, default={}) or {}


__all__ = ["CALCULATION_OWNER", "FORFEITURE_LEDGER", "ARTIFACT",
           "IDENTITY_KEY", "SCOPE_BATCH", "REASONS",
           "REASON_RUNTIME_NOT_INVOKED", "REASON_WINDOW_OPEN_AT_INVOCATION",
           "REASON_STRUCTURAL", "forfeiture_dir", "rows", "verify",
           "identity", "existing_identities", "append",
           "candidates_from_recorded_refusals", "candidates_from_daily_batch",
           "candidates_from_month_end", "sweep", "load"]
