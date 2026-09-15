r"""alpha_agent.alpha_recovery.scheduled_event_data - the frozen event calendars for the Alpha Agent
mechanism ``SCHEDULED_EVENT_EQUITY_PREMIUM_V1``.

DATES ONLY. Nothing in this module reads a price or a return.

* **FOMC** - regularly scheduled meeting decision days (the last day of each scheduled meeting).
  1994-2020 come from the Federal Reserve's historical-materials page for each year, captured raw
  with its sha256 and acquisition instant; 2021 onward come from the R46 event lane's capture of
  the FOMC calendar page. Conference calls, notation votes, unscheduled and cancelled meetings are
  excluded and counted, never silently dropped.
* **CPI and Employment Situation** - read by PATH from ONE frozen capture of the R46 macro lane:
  the FRED release calendars (release_id 10 and 50) and the ALFRED initial-release vintages of
  CPIAUCSL and PAYEMS, whose first ``realtime_start`` per reference month is the headline release
  day.

RESEARCH ONLY. The only network access is a public GET of the Federal Reserve pages, recorded in a
manifest; no purchase, credential, registration or live write.
"""
from __future__ import annotations

import calendar
import datetime as _dt
import hashlib
import html as _html
import json
import re
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path
from typing import Callable, Optional

from . import now_iso, research_root

CALCULATION_OWNER = "alpha_agent.alpha_recovery.scheduled_event_data"
DATA_SUBDIR = ("data", "scheduled_event_premium")
HISTORY_SUBDIR = "fomc_history"
MANIFEST_NAME = "fomc_history_manifest.json"

FOMC_HISTORY_URL = "https://www.federalreserve.gov/monetarypolicy/fomchistorical%d.htm"
HISTORY_YEARS = tuple(range(1994, 2021))
CALENDAR_FIRST_YEAR = 2021
UA = "paper-trader-research (research; contact via operator)"
HTTP_TIMEOUT = 60
FETCH_PAUSE_S = 1.0

R46_ROOT = Path(r"D:\Stock_Prediction_app_data\prospective_alpha_tournament_r46")
#: The one frozen capture of each owned calendar (read by path, never re-acquired here).
FROZEN_CAPTURES = {
    "CPI_RELEASE_DATES": "_data_macro/CPI_release_dates_20260914T215141Z.json",
    "EMPLOYMENT_RELEASE_DATES": "_data_macro/EMPLOYMENT_release_dates_20260914T215141Z.json",
    "CPI_INITIAL_RELEASES": "_data_macro/CPI_initial_releases_20260914T215141Z.json",
    "EMPLOYMENT_INITIAL_RELEASES": "_data_macro/EMPLOYMENT_initial_releases_20260914T215141Z.json",
    "FOMC_CALENDAR": "_data_events/fomc_calendar_20260914T215143Z.html",
}

K_SCHEDULED = "SCHEDULED"
K_UNSCHEDULED = "UNSCHEDULED"
K_CANCELLED = "CANCELLED"
K_CALL = "CONFERENCE_CALL"
K_NOTATION = "NOTATION_VOTE"
K_OTHER = "OTHER"

_MONTHS: dict = {}
for _i in range(1, 13):
    _MONTHS[calendar.month_name[_i].lower()] = _i
    _MONTHS[calendar.month_abbr[_i].lower()] = _i
_MONTHS["sept"] = 9


def data_dir() -> Path:
    return research_root().joinpath(*DATA_SUBDIR)


def history_dir() -> Path:
    return data_dir() / HISTORY_SUBDIR


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def file_sha256(path) -> Optional[str]:
    try:
        return _sha(Path(path).read_bytes())
    except OSError:
        return None


def capture_path(key: str, root: Optional[Path] = None) -> Path:
    return Path(root or R46_ROOT) / FROZEN_CAPTURES[key]


# --------------------------------------------------------------------------- #
# FOMC history acquisition (public Federal Reserve pages)
# --------------------------------------------------------------------------- #
def _default_fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as fh:
        return fh.read()


def read_manifest() -> dict:
    p = history_dir() / MANIFEST_NAME
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"schema": "scheduled_event_fomc_history/1", "pages": {}}


def acquire_fomc_history(years=HISTORY_YEARS, *, refresh: bool = False,
                         fetch: Optional[Callable[[str], bytes]] = None) -> dict:
    """Capture each year's historical FOMC page once; the manifest records every byte's hash."""
    fetch = fetch or _default_fetch
    d = history_dir()
    d.mkdir(parents=True, exist_ok=True)
    man = read_manifest()
    pages = dict(man.get("pages") or {})
    for y in years:
        dest = d / ("fomchistorical%d.htm" % y)
        if not refresh and str(y) in pages and dest.exists() and file_sha256(dest) == pages[str(y)]["sha256"]:
            continue
        url = FOMC_HISTORY_URL % y
        body = fetch(url)
        dest.write_bytes(body)
        pages[str(y)] = {"url": url, "path": str(dest), "bytes": len(body), "sha256": _sha(body),
                         "acquired_at_utc": now_iso()}
        if fetch is _default_fetch:
            time.sleep(FETCH_PAUSE_S)
    man.update({"pages": pages, "n_pages": len(pages), "updated_at_utc": now_iso(),
                "money_spent_usd": 0.0, "credential_used": False})
    (d / MANIFEST_NAME).write_text(json.dumps(man, indent=1, sort_keys=True), encoding="utf-8")
    return man


# --------------------------------------------------------------------------- #
# Parsers
# --------------------------------------------------------------------------- #
_H5 = re.compile(r"<h5[^>]*>(.*?)</h5>", re.S | re.I)
_DASH = "[-–—]"
_EVENT = re.compile(
    r"^(?P<m1>[A-Za-z]+(?:/[A-Za-z]+)?)\.?\s+(?P<d1>\d{1,2})"
    r"(?:\s*" + _DASH + r"\s*(?:(?P<m2>[A-Za-z]+)\.?\s+)?(?P<d2>\d{1,2}))?"
    r"(?P<rest>.*?)" + _DASH + r"\s*(?P<year>\d{4})\s*$")
#: Two scheduled headings fewer than this many calendar days apart are ONE meeting (2003 lists
#: "September 15 Meeting" and "September 16 Meeting"); its decision day is the later heading.
MERGE_SCHEDULED_WITHIN_DAYS = 7
_PANEL = re.compile(r"(\d{4})\s+FOMC\s+Meetings")
_CAL_MONTH = re.compile(r"fomc-meeting__month[^>]*>\s*(?:<strong>)?\s*([A-Za-z]+(?:/[A-Za-z]+)?)")
_CAL_DATE = re.compile(r"fomc-meeting__date[^>]*>\s*([^<]+?)\s*<")


def _text(raw: str) -> str:
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", raw))).strip()


def classify(label: str, *, default: str = K_OTHER) -> str:
    low = str(label).lower()
    if "conference call" in low:
        return K_CALL
    if "notation" in low:
        return K_NOTATION
    if "unscheduled" in low:
        return K_UNSCHEDULED
    if "cancel" in low:
        return K_CANCELLED
    if "meeting" in low:
        return K_SCHEDULED
    return default


def _next_month(m: int) -> int:
    return m % 12 + 1


def parse_history_page(html: str, year: int) -> list:
    """Events on one ``fomchistoricalYYYY.htm`` page: ``<h5>Month D-D Meeting - YYYY</h5>``."""
    rows = []
    for raw in _H5.findall(html):
        text = _text(raw)
        m = _EVENT.match(text)
        if not m or int(m.group("year")) != int(year):
            continue
        label = [p for p in m.group("m1").split("/") if p]
        m1, m_last = _MONTHS.get(label[0].lower()), _MONTHS.get(label[-1].lower())
        if m1 is None or m_last is None:
            continue
        m2 = _MONTHS.get(m.group("m2").lower()) if m.group("m2") else m1
        if m2 is None:
            continue
        d1, d2 = int(m.group("d1")), int(m.group("d2") or m.group("d1"))
        if m.group("d2") and not m.group("m2") and d2 < d1:
            m2 = m_last if len(label) > 1 else _next_month(m1)
        rows.append({"decision_day": str(_dt.date(int(year), m2, d2)), "heading": text,
                     "kind": classify(m.group("rest")), "source": "FED_HISTORICAL_PAGE"})
    return rows


def parse_calendar_page(html: str) -> dict:
    """``fomccalendars.htm``: one panel per year, month blocks ``Jan/Feb`` with dates ``31-1*``."""
    marks = [(m.start(), int(m.group(1))) for m in _PANEL.finditer(html)]
    out, problems = [], []
    for i, (pos, year) in enumerate(marks):
        seg = html[pos: marks[i + 1][0] if i + 1 < len(marks) else len(html)]
        months, dates = _CAL_MONTH.findall(seg), _CAL_DATE.findall(seg)
        if len(months) != len(dates):
            problems.append("PANEL_%d_MONTH_DATE_MISMATCH: %d months, %d dates" % (year, len(months), len(dates)))
            continue
        for mt, dtext in zip(months, dates):
            parts = [p for p in mt.split("/") if p]
            first, last = _MONTHS.get(parts[0].lower()), _MONTHS.get(parts[-1].lower())
            nums = [int(x) for x in re.findall(r"\d{1,2}", dtext.split("(")[0])]
            if first is None or last is None or not nums:
                problems.append("PANEL_%d_UNPARSED: %r %r" % (year, mt, dtext))
                continue
            d1, d2 = nums[0], nums[-1]
            month = first
            if d2 < d1:
                month = last if len(parts) > 1 else _next_month(first)
            elif len(parts) > 1 and len(nums) == 1:
                month = last
            out.append({"decision_day": str(_dt.date(year, month, d2)), "heading": "%s %s %d" % (mt, dtext, year),
                        "kind": classify(dtext, default=K_SCHEDULED), "source": "FED_CALENDAR_PAGE"})
    return {"rows": out, "problems": problems}


def fomc_events(*, history_root: Optional[Path] = None, calendar_path: Optional[Path] = None,
                years=HISTORY_YEARS, calendar_first_year: int = CALENDAR_FIRST_YEAR) -> dict:
    hroot = Path(history_root or history_dir())
    rows, problems = [], []
    for y in years:
        p = hroot / ("fomchistorical%d.htm" % y)
        if not p.exists():
            problems.append("HISTORY_PAGE_MISSING: %d" % y)
            continue
        rows += parse_history_page(p.read_text(encoding="utf-8", errors="replace"), y)
    cal = Path(calendar_path or capture_path("FOMC_CALENDAR"))
    if cal.exists():
        parsed = parse_calendar_page(cal.read_text(encoding="utf-8", errors="replace"))
        problems += parsed["problems"]
        rows += [r for r in parsed["rows"] if int(r["decision_day"][:4]) >= calendar_first_year]
    else:
        problems.append("CALENDAR_CAPTURE_MISSING: %s" % cal)
    by_year: dict = defaultdict(Counter)
    for r in rows:
        by_year[r["decision_day"][:4]][r["kind"]] += 1
    raw = sorted({r["decision_day"] for r in rows if r["kind"] == K_SCHEDULED})
    scheduled, merged = merge_close_meetings(raw)
    return {"rows": sorted(rows, key=lambda r: r["decision_day"]), "scheduled_decision_days": scheduled,
            "merged_headings": merged,
            "by_year": {y: dict(c) for y, c in sorted(by_year.items())}, "problems": problems}


def merge_close_meetings(days: list, within: int = MERGE_SCHEDULED_WITHIN_DAYS) -> tuple:
    """Collapse scheduled headings fewer than ``within`` days apart into their LATER day."""
    out, merged = [], []
    for d in sorted(days):
        if out and (_dt.date.fromisoformat(d) - _dt.date.fromisoformat(out[-1])).days < within:
            merged.append({"dropped": out[-1], "kept": d})
            out[-1] = d
        else:
            out.append(d)
    return out, merged


# --------------------------------------------------------------------------- #
# Release calendars (owned captures, read by path)
# --------------------------------------------------------------------------- #
def fred_release_dates(key: str, *, root: Optional[Path] = None) -> list:
    """Every date the FRED release calendar records for the release (as captured)."""
    p = capture_path(key, root)
    payload = json.loads(p.read_text(encoding="utf-8"))
    return sorted({str(r.get("date") or "")[:10] for r in payload.get("release_dates") or []
                   if r.get("date")})


def initial_release_days(key: str, *, root: Optional[Path] = None) -> dict:
    """First ``realtime_start`` per reference month in the ALFRED initial-release vintages."""
    p = capture_path(key, root)
    payload = json.loads(p.read_text(encoding="utf-8"))
    first: dict = {}
    for o in payload.get("observations") or []:
        ref, rs = str(o.get("date") or "")[:10], str(o.get("realtime_start") or "")[:10]
        if not ref or not rs:
            continue
        if ref not in first or rs < first[ref]:
            first[ref] = rs
    return first


__all__ = ["CALCULATION_OWNER", "FROZEN_CAPTURES", "HISTORY_YEARS", "acquire_fomc_history",
           "parse_history_page", "parse_calendar_page", "fomc_events", "fred_release_dates",
           "initial_release_days", "file_sha256", "classify"]
