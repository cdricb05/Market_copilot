"""alpha_agent.r52.velocity_ops - evidence velocity made operational.

R46's velocity read model answers the scientific question (raw rows,
dependence-penalised effective observations, projections). What it cannot
answer is the OPERATIONAL question Release 51 exposed: how much evidence did
the calendar offer, and how much of it did the runtime actually collect?

    SCIENTIFICALLY_SLOW   - the calendar is the bottleneck. Not fixable, and
                            not fixable by cheating.
    OPERATIONALLY_MISSED  - the runtime was the bottleneck. R52 exists to
                            drive this number to zero, and this module
                            measures it per week so a regression is visible
                            the week it happens.

Pure composition over artifacts the canonical owners already wrote: the R46
velocity artifact, the R46 prediction ledger, and the R52 forfeiture ledger.
Calculates no new science and writes one read model into the R52 root.
"""
from __future__ import annotations

import datetime as _dt

from . import (ACCOUNTABILITY_START_DATE, artifact_body, read_json,
               runtime_dir, write_json)
from ..r46 import CAMPAIGN_ID
from ..r46 import campaign_dir as r46_campaign_dir
from ..r46 import clock as CK
from ..r46 import ledger as LG
from . import forfeiture as FF

CALCULATION_OWNER = "alpha_agent.r52.velocity_ops"

ARTIFACT = "evidence_velocity_operational.json"

VELOCITY_ARTIFACT = "R46_EVIDENCE_VELOCITY.json"


def _iso_week(day: _dt.date) -> str:
    y, w, _ = day.isocalendar()
    return "%04d-W%02d" % (y, w)


def _as_date(value):
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def build(now: _dt.datetime = None, *, campaign_id: str = CAMPAIGN_ID,
          write: bool = True) -> dict:
    now = now or CK.now_utc()
    today = CK.eastern_date(now)
    start = _dt.date.fromisoformat(ACCOUNTABILITY_START_DATE)

    sci = read_json(r46_campaign_dir(campaign_id) / VELOCITY_ARTIFACT,
                    default=None) or {}
    preds = LG.predictions(campaign_id)
    forfeits = FF.rows()

    weeks: dict = {}

    def _wk(day: _dt.date) -> dict:
        return weeks.setdefault(_iso_week(day), {
            "week": _iso_week(day),
            "eligible_batch_slots": 0,
            "batch_slots_used": 0,
            "predictions_emitted": 0,
            "forfeited_rows": 0,
            "cells_lost": 0,
        })

    # eligible batch slots: one per weekday entry date from accountability
    d = CK.next_weekday(start)
    while d <= today:
        _wk(d)["eligible_batch_slots"] += 1
        d += _dt.timedelta(days=1)
        while d.weekday() in CK.WEEKEND:
            d += _dt.timedelta(days=1)

    used_dates = set()
    for p in preds:
        e = _as_date(p.get("effective_as_of"))
        if e is None or e < CK.next_weekday(start):
            continue
        w = _wk(e)
        w["predictions_emitted"] += 1
        if str(e) not in used_dates:
            used_dates.add(str(e))
            w["batch_slots_used"] += 1

    for r in forfeits:
        e = _as_date(r.get("decision_date"))
        if e is None:
            continue
        w = _wk(e)
        w["forfeited_rows"] += 1
        w["cells_lost"] += int(r.get("n_cells_lost") or 0)

    rows = [weeks[k] for k in sorted(weeks)]
    for w in rows:
        offered = w["eligible_batch_slots"]
        missed = max(0, offered - w["batch_slots_used"])
        w["slots_missed_so_far"] = missed
        w["operational_capture_rate"] = (
            round(w["batch_slots_used"] / offered, 4) if offered else None)

    total_lost = sum(int(r.get("n_cells_lost") or 0) for r in forfeits)
    body = artifact_body(
        "r52_evidence_velocity_operational/1", CALCULATION_OWNER,
        built_at_utc=CK.iso(now),
        accountability_start_date=ACCOUNTABILITY_START_DATE,
        # ---- the scientific numbers, quoted from their owner -------------- #
        scientific_owner="alpha_agent.r46.velocity",
        raw_predictions_emitted=sci.get("raw_predictions_emitted"),
        raw_matured_rows=sci.get("raw_matured_rows"),
        effective_independent_observations=sci.get(
            "effective_independent_observations"),
        projected_effective_per_week=sci.get("projected_effective_per_week"),
        realised_effective_per_week=sci.get("realised_effective_per_week"),
        information_set_state=sci.get("information_set_state"),
        # ---- the operational split, computed here ------------------------- #
        weekly=rows,
        forfeited_opportunities_total=len(forfeits),
        forfeited_cells_total=total_lost,
        evidence_loss_due_to_runtime={
            "unit": "prospective cells whose legal window closed unemitted "
                    "since %s" % ACCOUNTABILITY_START_DATE,
            "n_cells": total_lost,
            "n_opportunities": len(forfeits),
        },
        the_two_bottlenecks={
            "SCIENTIFICALLY_SLOW": "the calendar limits effective "
                                   "observations; only time closes it",
            "OPERATIONALLY_MISSED": "the runtime failed to collect offered "
                                    "evidence; R52 drives this to zero",
        },
        research_only=True,
    )
    if write:
        write_json(runtime_dir() / ARTIFACT, body)
    return body


def load() -> dict:
    return read_json(runtime_dir() / ARTIFACT, default={}) or {}


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "build", "load"]
