r"""alpha_agent.agents_v2.campaign_clock - REAL working time, and what counts as measured (R100 repair).

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION.

Why this exists
---------------
R99's guard defined active time as WALL CLOCK since ``started_at_utc`` minus
hand-recorded idle exclusions. Anyone who simply re-ran the guard later was
granted the 180-minute hard stop on time nobody spent researching. A clock that
advances while nobody works can complete a campaign by waiting.

This clock cannot. Active time is the UNION of:

* runner SPANS - the machine was measuring (start and end recorded by the runner
  process itself; an unterminated span credits nothing beyond its start event);
* the time leading up to each WORK EVENT, capped at :data:`MAX_GAP_CREDIT_MINUTES`.
  A work event must name an artifact that EXISTS, and the (path, sha256) pair
  must be new - re-recording an unchanged file is refused, so time cannot be
  manufactured by touching the log.

Nothing reads the wall clock "since start". No event -> zero minutes. Running a
guard, a status read or a verification writes no event.

What counts as a valid measured experiment
------------------------------------------
:func:`valid_measurements` counts a run ONLY when it measured under the
FULL_RECIPE_V1 contract (frozen recipe and source matched), its admission was
G7 PASS at measurement time, and no later ruling withdrew or rejected it. A
REFUSED or FAILED run, an unbound (legacy) measurement, and a governance
exception are listed with their reason and never counted.
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

from . import provenance as PV

WORK_LOG = "WORK_LOG.jsonl"
#: The most a single gap between two work events can credit. Longer silence is
#: idle time and earns nothing beyond this.
MAX_GAP_CREDIT_MINUTES = 5.0
KIND_WORK = "WORK"
KIND_SPAN_START = "SPAN_START"
KIND_SPAN_END = "SPAN_END"
#: Run states that MEASURED something (a halted experiment is measured and settled).
MEASURED_STATES = ("MEASURED", "HALTED")


class ClockRefusal(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__("%s: %s" % (code, message))
        self.code = code


def _now() -> datetime:
    """The only time source. Tests patch this; no caller may pass a timestamp."""
    return datetime.now(timezone.utc)


def _iso(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def _log(campaign_dir) -> Path:
    return Path(campaign_dir) / WORK_LOG


def read_log(campaign_dir) -> list:
    p = _log(campaign_dir)
    if not p.exists():
        return []
    return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]


def _append(campaign_dir, rec: dict) -> dict:
    p = _log(campaign_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    return rec


def record_work(campaign_dir, *, activity: str, artifact, actor: str) -> dict:
    """Record one unit of work, evidenced by a NEW artifact."""
    if not activity or not actor:
        raise ClockRefusal("WORK_UNDESCRIBED", "activity and actor are required")
    a = Path(artifact)
    if not a.is_absolute():
        a = Path(campaign_dir) / a
    if not a.exists() or not a.is_file():
        raise ClockRefusal("WORK_ARTIFACT_MISSING", "%s does not exist" % a)
    sha = hashlib.sha256(a.read_bytes()).hexdigest()
    rel = a.resolve().as_posix()
    for r in read_log(campaign_dir):
        if r.get("kind") == KIND_WORK and r.get("artifact") == rel and r.get("artifact_sha256") == sha:
            raise ClockRefusal("WORK_ARTIFACT_UNCHANGED",
                               "%s was already recorded with these bytes; no new work" % a.name)
    return _append(campaign_dir, {"kind": KIND_WORK, "at_utc": _iso(_now()), "activity": activity,
                                  "actor": actor, "artifact": rel, "artifact_sha256": sha})


class span:
    """``with span(dir, "measure H_x", actor="runner"):`` - machine work in progress.
    The end is recorded even if the body raises (the time was still spent)."""

    def __init__(self, campaign_dir, activity: str, *, actor: str):
        self.dir, self.activity, self.actor = campaign_dir, activity, actor
        self.span_id = uuid.uuid4().hex

    def __enter__(self):
        if self.dir is not None:
            _append(self.dir, {"kind": KIND_SPAN_START, "at_utc": _iso(_now()), "span_id": self.span_id,
                               "activity": self.activity, "actor": self.actor})
        return self

    def __exit__(self, *exc):
        if self.dir is not None:
            _append(self.dir, {"kind": KIND_SPAN_END, "at_utc": _iso(_now()), "span_id": self.span_id,
                               "activity": self.activity, "actor": self.actor})
        return False


def active_minutes(campaign_dir) -> dict:
    """Active working minutes, as the union of span intervals and capped
    pre-event credits. Pure read; writes nothing."""
    log = sorted(read_log(campaign_dir), key=lambda r: r["at_utc"])
    cap = timedelta(minutes=MAX_GAP_CREDIT_MINUTES)
    intervals = []
    starts = {}
    prev = None
    for r in log:
        t = _parse(r["at_utc"])
        if prev is not None and t > prev:
            intervals.append((max(prev, t - cap), t))
        prev = t
        if r["kind"] == KIND_SPAN_START:
            starts[r["span_id"]] = t
        elif r["kind"] == KIND_SPAN_END and r["span_id"] in starts:
            intervals.append((starts.pop(r["span_id"]), t))
    intervals.sort()
    total = timedelta(0)
    cur_s = cur_e = None
    for s, e in intervals:
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                total += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        total += cur_e - cur_s
    first = _parse(log[0]["at_utc"]) if log else None
    last = _parse(log[-1]["at_utc"]) if log else None
    elapsed = ((last - first).total_seconds() / 60.0) if log else 0.0
    active = total.total_seconds() / 60.0
    return {"active_minutes": round(active, 3),
            "n_work_events": sum(1 for r in log if r["kind"] == KIND_WORK),
            "n_spans": sum(1 for r in log if r["kind"] == KIND_SPAN_END),
            "unterminated_spans": len(starts),
            "first_event_utc": log[0]["at_utc"] if log else None,
            "last_event_utc": log[-1]["at_utc"] if log else None,
            "uncredited_idle_minutes_between_events": round(max(0.0, elapsed - active), 3),
            "rule": ("union of runner spans and <= %.0f min before each work event; "
                     "wall clock since start is never counted" % MAX_GAP_CREDIT_MINUTES)}


# --------------------------------------------------------------------------- #
# What counts as a valid measured experiment                                   #
# --------------------------------------------------------------------------- #
def valid_measurements(results: Iterable[dict], governance_exceptions: Iterable[dict] = (),
                       *, cell_of: Optional[dict] = None) -> dict:
    """Split run results into the ones that COUNT and the ones that do not, with
    a reason for every exclusion."""
    exc_cells = {e.get("cell"): e.get("exception") for e in governance_exceptions or []}
    cell_of = cell_of or {}
    valid, excluded = [], []
    for r in results:
        eid = r.get("experiment_id")
        cell = r.get("cell_id") or cell_of.get(eid)
        why = None
        if r.get("state") not in MEASURED_STATES:
            why = "RUN_%s" % r.get("state")
        elif r.get("provenance_contract") != PV.PROVENANCE_CONTRACT_FULL:
            why = "UNBOUND_LEGACY_MEASUREMENT"
        elif not r.get("recipe_sha256") or not r.get("frozen_source_manifest_sha256"):
            why = "RECIPE_OR_SOURCE_UNBOUND"
        elif (r.get("admission") or {}).get("g7") != PV.ADMISSION_PASS:
            why = "NOT_ADMITTED_AT_MEASUREMENT"
        elif cell in exc_cells:
            why = exc_cells[cell]
        if why:
            excluded.append({"experiment_id": eid, "cell_id": cell, "reason": why})
        else:
            valid.append(eid)
    return {"n_valid": len(valid), "valid": valid, "excluded": excluded}
