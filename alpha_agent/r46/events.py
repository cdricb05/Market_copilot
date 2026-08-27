"""alpha_agent.r46.events - scheduled-event information. Provable timestamps only.

An event challenger is only as honest as the proof that the event's TIME was
known before the trade. Two calendars in this estate satisfy that without a
purchase:

* the **FOMC meeting schedule**, published by the Federal Reserve a year or
  more ahead on a public page; captured here as raw HTML with the acquisition
  instant, parsed into decision days (the last day of each meeting), with a
  frozen fallback list for the current year in case the page changes shape;
* the **FRED release calendars** the macro lane captures (CPI, Employment
  Situation), which list scheduled future dates months ahead.

Earnings dates are NOT used: the only earnings file on disk is a synthetic
test fixture, and no free source with provable per-name announcement
timestamps is owned. That gap is recorded, not papered over.

Two documented calendar effects are frozen as challengers, both expressed in
SPY at a one-session horizon and both flat on every other day:

* **pre-FOMC drift** (Lucca and Moench): long SPY from the close before a
  scheduled FOMC decision day to the close of the decision day;
* **announcement-day premium** (Savor and Wilson): long SPY over sessions
  that carry a scheduled CPI, Employment Situation or FOMC decision.

Neither rule has a parameter to sweep. Research only.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import re
import urllib.request
from pathlib import Path
from typing import Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import contract as C
from . import macro as MC

CALCULATION_OWNER = "alpha_agent.r46.events"

ARTIFACT = "R46_4_EVENT_LANE.json"
RAW_DIRNAME = "_data_events"
MANIFEST_NAME = "event_captures.json"


def raw_dir() -> Path:
    """Resolved at CALL time from the package root (hermetic under a test
    root); production is the R46 research root."""
    from . import RESEARCH_ROOT as _root
    return Path(_root) / RAW_DIRNAME


def manifest_path() -> Path:
    return raw_dir() / MANIFEST_NAME

FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
UA = "paper-trader-research/46.4 (research; contact via operator)"
HTTP_TIMEOUT = 90

#: Frozen fallback - the published 2026 schedule's decision days. Used only
#: when the page cannot be parsed, and recorded as such.
FROZEN_FOMC_DECISION_DAYS = {
    2026: ("2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
           "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09"),
}

MONTHS = {m.lower(): i for i, m in enumerate(
    ("January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"), start=1)}

ANNOUNCEMENT_FAMILIES = ("CPI", "EMPLOYMENT")


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _manifest() -> dict:
    return read_json(manifest_path(), default=None) or {
        "schema": "r46_4_event_captures/1", "captures": []}


def parse_fomc(html: str) -> dict:
    """Decision days per year from the Fed's calendar page.

    The page lists each meeting as a month block ("January", or "April/May"
    for a meeting that spans a month end) followed by a day range ("27-28",
    "30-1", "16-17*"). The decision day is the LAST day; when the month block
    carries two months, the last day belongs to the second.
    """
    out: dict = {}
    year = None
    pat = re.compile(
        r"(?P<year>\d{4})\s+FOMC\s+Meetings"
        r"|fomc-meeting__month[^>]*>\s*(?:<strong>)?\s*(?P<month>[A-Za-z]+"
        r"(?:/[A-Za-z]+)?)"
        r"|fomc-meeting__date[^>]*>\s*(?P<date>[0-9]{1,2}(?:-[0-9]{1,2})?)",
        re.IGNORECASE)
    month = None
    for m in pat.finditer(html):
        if m.group("year"):
            year = int(m.group("year"))
            month = None
            continue
        if m.group("month"):
            month = m.group("month")
            continue
        if m.group("date") and year and month:
            days = m.group("date").split("-")
            last_day = int(days[-1])
            parts = month.split("/")
            mon = MONTHS.get(parts[-1].lower())
            if mon is None:
                continue
            try:
                d = _dt.date(year, mon, last_day)
            except ValueError:
                continue
            out.setdefault(year, set()).add(d.isoformat())
    return {y: sorted(v) for y, v in out.items()}


def acquire(*, acquire: bool = True, today: _dt.date = None) -> dict:
    raw_dir().mkdir(parents=True, exist_ok=True)
    now = CK.now_utc()
    today = today or CK.eastern_date(now)
    man = _manifest()
    caps = list(man.get("captures") or [])
    have_today = any(str(c.get("acquired_day")) == str(today) for c in caps)
    rec = {"source": "FOMC_CALENDAR", "url": FOMC_URL}
    if have_today:
        rec["state"] = "ALREADY_CAPTURED_TODAY"
    elif not acquire:
        rec["state"] = "NOT_ACQUIRED"
    else:
        try:
            req = urllib.request.Request(FOMC_URL, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as fh:
                body = fh.read()
            parsed = parse_fomc(body.decode("utf-8", "replace"))
            stamp = now.strftime("%Y%m%dT%H%M%SZ")
            dest = raw_dir() / ("fomc_calendar_%s.html" % stamp)
            dest.write_bytes(body)
            cap = {"source": "FOMC_CALENDAR", "url": FOMC_URL,
                   "path": str(dest), "bytes": len(body), "sha256": _sha(body),
                   "parsed_decision_days": {str(k): v
                                            for k, v in parsed.items()},
                   "n_years_parsed": len(parsed),
                   "acquired_day": str(today),
                   "acquired_at_utc": CK.iso(now),
                   "acquired_at_utc_precise": CK.iso_precise(now),
                   "licence": "Federal Reserve Board, public"}
            caps.append(cap)
            rec.update({k: v for k, v in cap.items() if k != "path"})
            rec["state"] = "CAPTURED"
        except Exception as exc:                # noqa: BLE001 - reported
            rec["state"] = "FETCH_FAILED"
            rec["error"] = type(exc).__name__
    man["captures"] = caps
    man["n_captures"] = len(caps)
    man["updated_at_utc"] = CK.iso(now)
    write_json(manifest_path(), man)
    return {"results": [rec], "n_captures": len(caps),
            "money_spent_usd": 0.0}


def fomc_decision_days(year: int = None) -> dict:
    """Decision days with their provenance (PARSED_CAPTURE or FROZEN_FALLBACK)."""
    caps = [c for c in (_manifest().get("captures") or ())
            if c.get("source") == "FOMC_CALENDAR"
            and (c.get("parsed_decision_days") or {})]
    if caps:
        cap = max(caps, key=lambda c: c["acquired_at_utc"])
        parsed = cap["parsed_decision_days"]
        days = []
        for y, v in parsed.items():
            if year is None or int(y) == int(year):
                days.extend(v)
        if days:
            return {"days": sorted(days), "source": "PARSED_CAPTURE",
                    "acquired_at_utc": cap["acquired_at_utc"]}
    days = []
    for y, v in FROZEN_FOMC_DECISION_DAYS.items():
        if year is None or y == int(year):
            days.extend(v)
    return {"days": sorted(days), "source": "FROZEN_FALLBACK",
            "acquired_at_utc": None}


def announcement_days() -> dict:
    fomc = fomc_decision_days()
    out = {d: ["FOMC"] for d in fomc["days"]}
    for fam in ANNOUNCEMENT_FAMILIES:
        for d in MC.release_dates(fam):
            out.setdefault(d, []).append(fam)
    return {"days": dict(sorted(out.items())), "fomc_source": fomc["source"]}


def is_fomc_decision_day(d: _dt.date) -> bool:
    return str(d) in set(fomc_decision_days(d.year)["days"])


def is_announcement_day(d: _dt.date) -> bool:
    return str(d) in announcement_days()["days"]


def holding_session_for(emitted_at: _dt.datetime) -> _dt.date:
    """The session a one-session trade entered on the entry rule will HOLD."""
    entry = CK.entry_session_date(emitted_at)
    return CK.next_weekday(entry)


def run(*, acquire_now: bool = True, campaign_id: str = CAMPAIGN_ID,
        as_of: _dt.date = None) -> dict:
    now = CK.now_utc()
    as_of = as_of or CK.eastern_date(now)
    acq = acquire(acquire=acquire_now, today=as_of)
    fomc = fomc_decision_days(as_of.year)
    ann = announcement_days()
    upcoming = [d for d in ann["days"] if d > str(as_of)][:8]
    next_fomc = next((d for d in fomc["days"] if d > str(as_of)), None)
    ok = bool(fomc["days"])
    state = ("LIVE_PROSPECTIVE" if ok and fomc["source"] == "PARSED_CAPTURE"
             else "FROZEN_PENDING_EMISSION" if ok else "DATA_BLOCKED")
    body = artifact_body(
        "r46_4_event_lane/1", CALCULATION_OWNER,
        as_of=str(as_of),
        state=state,
        acquisition=acq,
        raw_root=str(raw_dir()),
        fomc={"decision_days_this_year": fomc["days"],
              "source": fomc["source"],
              "acquired_at_utc": fomc.get("acquired_at_utc"),
              "next_decision_day": next_fomc,
              "frozen_fallback": {str(k): list(v) for k, v in
                                  FROZEN_FOMC_DECISION_DAYS.items()}},
        announcement_calendar={"families": ["FOMC"] + list(
            ANNOUNCEMENT_FAMILIES), "n_days": len(ann["days"]),
            "upcoming": upcoming,
            "source": "FOMC page capture + FRED release calendars "
                      "(macro lane captures)"},
        point_in_time={
            "fomc_schedule": "published by the Board a year or more ahead; "
                             "captured raw with the acquisition instant",
            "release_calendars": "FRED release/dates lists scheduled future "
                                 "dates; captured by the macro lane",
            "earnings": "NOT_USED - the only earnings file on disk is a "
                        "synthetic fixture; no free per-name announcement "
                        "timestamp source is owned",
            "listed_issuer_events": "NOT_USED in this release",
        },
        holding_session_for_an_emission_now=str(holding_session_for(now)),
        challengers_frozen=["r46_4_spx_pre_fomc_drift",
                            "r46_4_spx_announcement_day_premium"],
        information_family="SCHEDULED_EVENT_CALENDAR",
        money_spent_usd=0.0,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "raw_dir",
           "FROZEN_FOMC_DECISION_DAYS", "ANNOUNCEMENT_FAMILIES", "parse_fomc",
           "acquire", "fomc_decision_days", "announcement_days",
           "is_fomc_decision_day", "is_announcement_day",
           "holding_session_for", "run"]
