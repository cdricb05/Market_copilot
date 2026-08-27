"""alpha_agent.r46.earnings - per-name earnings announcement instants, free and PIT-safe.

Every earlier release that wanted post-earnings drift stopped at the same
wall: the only earnings-date file on disk is a synthetic test fixture, and no
owned vendor field carries the announcement INSTANT. Release 46.5 closes the
gap with the one free source that does: a company announcing results files a
Form 8-K under Item 2.02 (Results of Operations and Financial Condition), and
EDGAR stamps that filing with the second it was accepted. The
``data.sec.gov/submissions`` document for each issuer lists every filing with
``acceptanceDateTime``, ``items`` and ``reportDate`` (the fiscal period).

That acceptance instant is the point-in-time key. A signal built at the close
of session ``d`` may use an announcement only if its acceptance instant is
before the emission instant AND it sits in a capture acquired before the
emission - a proof, not a rule. From the instant, the release is classified
BEFORE_OPEN / INTRADAY / AFTER_CLOSE on the Eastern clock and mapped to the
session whose close first reflects it (the REACTION session).

Raw captures live under the Release-46 research root with the acquisition
instant and sha256, one per issuer per day, never overwritten. The synthetic
fixture is refused by name: nothing in this module reads a file whose path
contains ``fixture``, ``synthetic`` or ``sample``.

One bounded challenger is frozen in :mod:`alpha_agent.r46.challengers`
before its first emission: post-earnings-announcement drift signed by the
announcement-window abnormal return (Chan, Jegadeesh and Lakonishok's
earnings-announcement-return formulation, which needs no consensus history
the estate does not own). No threshold was swept.
"""
from __future__ import annotations

import datetime as _dt
import gzip
import json
import time
from pathlib import Path
from typing import Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import sec as SEC

CALCULATION_OWNER = "alpha_agent.r46.earnings"

ARTIFACT = "R46_5_EARNINGS_LANE.json"
RAW_DIRNAME = "_data_earnings"
MANIFEST_NAME = "earnings_captures.json"

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK%s.json"
TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
EARNINGS_ITEM = "2.02"

#: Bounded per run. The S&P 500 is ~503 issuers; the budget covers one full
#: pass with headroom and a second pass on the same day reads the captures.
MAX_COMPANIES_PER_RUN = 600
#: Wall-clock budget for ONE acquisition. The daily research cycle calls this
#: lane; a cycle must never be held hostage by a third-party feed. ``None``
#: means unbounded, ZERO means no time at all, and a run that stops early
#: resumes next time because capture is keyed per issuer per day.
MAX_SECONDS_PER_RUN = 420
FORBIDDEN_PATH_TOKENS = ("fixture", "synthetic", "sample")

TIMING_BEFORE_OPEN = "BEFORE_OPEN"
TIMING_INTRADAY = "INTRADAY"
TIMING_AFTER_CLOSE = "AFTER_CLOSE"
TIMINGS = (TIMING_BEFORE_OPEN, TIMING_INTRADAY, TIMING_AFTER_CLOSE)
OPEN_ET = _dt.time(9, 30)
CLOSE_ET = _dt.time(16, 0)


def raw_dir() -> Path:
    from . import RESEARCH_ROOT as _root
    return Path(_root) / RAW_DIRNAME


def manifest_path() -> Path:
    return raw_dir() / MANIFEST_NAME


def _manifest() -> dict:
    return read_json(manifest_path(), default=None) or {
        "schema": "r46_5_earnings_captures/1", "captures": [],
        "ticker_maps": []}


def _forbidden(path) -> bool:
    p = str(path).lower()
    return any(tok in p for tok in FORBIDDEN_PATH_TOKENS)


def norm_ticker(t: str) -> str:
    """SEC 'BRK-B' and Norgate 'BRK.B' name the same share class."""
    return str(t or "").strip().upper().replace("-", ".").replace("/", ".")


def cik10(cik) -> str:
    return str(int(str(cik).strip())).zfill(10)


# --------------------------------------------------------------------------- #
# Acquisition
# --------------------------------------------------------------------------- #
def _latest_ticker_map(today: _dt.date) -> Optional[dict]:
    caps = [c for c in _manifest().get("ticker_maps") or ()
            if Path(c["path"]).exists()]
    if not caps:
        return None
    return max(caps, key=lambda c: c["acquired_at_utc"])


def ticker_to_cik(today: _dt.date = None, *, acquire: bool = True,
                  now: _dt.datetime = None) -> dict:
    """{normalised ticker: cik10} from the SEC's own map, captured raw."""
    now = now or CK.now_utc()
    today = today or CK.eastern_date(now)
    man = _manifest()
    cap = _latest_ticker_map(today)
    if (cap is None or str(cap.get("acquired_day")) != str(today)) and acquire:
        raw_dir().mkdir(parents=True, exist_ok=True)
        res = SEC.get(TICKER_MAP_URL)
        if res["body"]:
            stamp = now.strftime("%Y%m%dT%H%M%SZ")
            dest = raw_dir() / ("company_tickers_%s.json.gz" % stamp)
            dest.write_bytes(gzip.compress(res["body"]))
            cap = {"path": str(dest), "sha256": SEC.sha256(res["body"]),
                   "bytes": len(res["body"]), "acquired_day": str(today),
                   "acquired_at_utc": CK.iso(now), "url": TICKER_MAP_URL,
                   "licence": "SEC, public domain"}
            man.setdefault("ticker_maps", []).append(cap)
            write_json(manifest_path(), man)
    if cap is None:
        return {}
    try:
        doc = json.loads(gzip.decompress(Path(cap["path"]).read_bytes())
                         .decode("utf-8"))
    except Exception:                           # noqa: BLE001
        return {}
    out = {}
    rows = doc.values() if isinstance(doc, dict) else doc
    for r in rows:
        try:
            out[norm_ticker(r["ticker"])] = cik10(r["cik_str"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _extract_events(doc: dict, cik: str, ticker: str) -> list:
    """Every 8-K carrying Item 2.02, with its acceptance instant."""
    rec = ((doc.get("filings") or {}).get("recent")) or {}
    forms = rec.get("form") or []
    out = []
    for i, form in enumerate(forms):
        if str(form) not in ("8-K", "8-K/A"):
            continue
        items = str((rec.get("items") or [""] * len(forms))[i] or "")
        if EARNINGS_ITEM not in [x.strip() for x in items.split(",")]:
            continue
        acc = (rec.get("acceptanceDateTime") or [None] * len(forms))[i]
        out.append({
            "cik": cik, "ticker": ticker,
            "accession": (rec.get("accessionNumber") or [None] * len(forms))[i],
            "form": str(form),
            "filing_date": (rec.get("filingDate") or [None] * len(forms))[i],
            "report_date": (rec.get("reportDate") or [None] * len(forms))[i],
            "accepted_at_utc": acc,
            "items": items,
            "is_amendment": str(form).endswith("/A"),
        })
    return out


def acquire(*, acquire: bool = True, today: _dt.date = None,
            universe=None, budget_companies: int = MAX_COMPANIES_PER_RUN,
            budget_seconds: float = MAX_SECONDS_PER_RUN,
            now: _dt.datetime = None) -> dict:
    """Capture each issuer's submissions document once per day. Append-only."""
    started_at = time.monotonic()
    now = now or CK.now_utc()
    today = today or CK.eastern_date(now)
    raw_dir().mkdir(parents=True, exist_ok=True)
    ua = SEC.user_agent()
    if universe is None:
        from . import challengers as CH
        universe = CH._eq_universe()
    tickers = sorted({norm_ticker(t) for t in (universe or ()) if t})
    if not ua:
        return {"state": SEC.BLOCKED_NO_CONTACT,
                "n_captures": len(_manifest().get("captures") or []),
                "n_universe": len(tickers), "acquired": 0,
                "money_spent_usd": 0.0}
    if not acquire:
        return {"state": "NOT_ACQUIRED",
                "n_captures": len(_manifest().get("captures") or []),
                "n_universe": len(tickers), "acquired": 0,
                "money_spent_usd": 0.0}
    # The ticker map is captured FIRST and the manifest re-read afterwards.
    # Reading the manifest before this call and writing it after would drop
    # the ticker-map row this call appends - a lost update that leaves every
    # name looking unmapped, which a coverage gate would then read as
    # "nothing to capture" rather than as a defect.
    cmap = ticker_to_cik(today, acquire=True, now=now)
    man = _manifest()
    caps = list(man.get("captures") or [])
    have_today = {c.get("cik") for c in caps
                  if str(c.get("acquired_day")) == str(today)}
    unmapped = [t for t in tickers if t not in cmap]
    todo = [(t, cmap[t]) for t in tickers if t in cmap
            and cmap[t] not in have_today]
    acquired, failed, out_of_time = 0, [], False
    for ticker, cik in todo[:budget_companies]:
        if budget_seconds is not None and \
                (time.monotonic() - started_at) >= float(budget_seconds):
            out_of_time = True
            break
        res = SEC.get(SUBMISSIONS_URL % cik)
        if not res["body"]:
            failed.append({"ticker": ticker, "cik": cik,
                           "error": res["error"]})
            continue
        try:
            doc = json.loads(res["body"].decode("utf-8"))
        except ValueError:
            failed.append({"ticker": ticker, "cik": cik,
                           "error": "unparseable JSON"})
            continue
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        raw = raw_dir() / ("submissions_%s_%s.json.gz" % (cik, stamp))
        raw.write_bytes(gzip.compress(res["body"]))
        events = _extract_events(doc, cik, ticker)
        ext = raw_dir() / ("earnings_events_%s_%s.json" % (cik, stamp))
        write_json(ext, {"schema": "r46_5_earnings_events/1", "cik": cik,
                         "ticker": ticker, "entity": doc.get("name"),
                         "sec_tickers": doc.get("tickers"),
                         "acquired_at_utc": CK.iso(now),
                         "acquired_at_utc_precise": CK.iso_precise(now),
                         "source_sha256": SEC.sha256(res["body"]),
                         "n_events": len(events), "events": events})
        caps.append({"cik": cik, "ticker": ticker, "raw_path": str(raw),
                     "extract_path": str(ext),
                     "sha256": SEC.sha256(res["body"]),
                     "bytes": len(res["body"]),
                     "n_filings_listed": len(((doc.get("filings") or {})
                                              .get("recent") or {})
                                             .get("form") or []),
                     "n_earnings_8k": len(events),
                     "acquired_day": str(today),
                     "acquired_at_utc": CK.iso(now),
                     "acquired_at_utc_precise": CK.iso_precise(now),
                     "url": SUBMISSIONS_URL % cik,
                     "licence": "SEC EDGAR, public domain"})
        acquired += 1
    man["captures"] = caps
    man["n_captures"] = len(caps)
    man["updated_at_utc"] = CK.iso(now)
    write_json(manifest_path(), man)
    return {"state": "CAPTURED" if acquired else "NOTHING_NEW_TODAY",
            "n_universe": len(tickers), "n_mapped": len(tickers) - len(unmapped),
            "unmapped": unmapped[:40], "n_todo": len(todo),
            "acquired": acquired,
            "truncated_by_budget": bool(len(todo) > budget_companies),
            "budget_seconds": budget_seconds,
            "seconds_spent": round(time.monotonic() - started_at, 1),
            "time_budget_exhausted": out_of_time, "resumable": True,
            "failed": failed[:40], "n_failed": len(failed),
            "n_captures": len(caps),
            "user_agent_contact_masked": SEC.mask(SEC.contact()),
            "money_spent_usd": 0.0}


# --------------------------------------------------------------------------- #
# Point-in-time reads
# --------------------------------------------------------------------------- #
def classify_timing(accepted_at_utc) -> dict:
    """BEFORE_OPEN / INTRADAY / AFTER_CLOSE and the REACTION session (ET)."""
    t = CK.parse_iso(accepted_at_utc)
    if t is None:
        return {"timing": None, "reaction_session": None}
    et = CK.to_eastern(t)
    d = et.date()
    if d.weekday() in CK.WEEKEND:
        return {"timing": TIMING_AFTER_CLOSE,
                "reaction_session": str(CK.next_weekday(d))}
    if et.time() < OPEN_ET:
        return {"timing": TIMING_BEFORE_OPEN, "reaction_session": str(d)}
    if et.time() < CLOSE_ET:
        return {"timing": TIMING_INTRADAY, "reaction_session": str(d)}
    return {"timing": TIMING_AFTER_CLOSE,
            "reaction_session": str(CK.next_weekday(d))}


def events(as_of_instant: _dt.datetime = None) -> list:
    """PIT-admissible earnings events: accepted before ``as_of_instant`` and
    present in a capture acquired before it. Latest capture per issuer."""
    now = as_of_instant or CK.now_utc()
    cut = CK.iso_precise(now)
    latest: dict = {}
    for c in _manifest().get("captures") or ():
        if str(c.get("acquired_at_utc_precise") or c.get("acquired_at_utc")) \
                >= cut:
            continue
        if _forbidden(c.get("extract_path")):
            continue
        prev = latest.get(c["cik"])
        if prev is None or c["acquired_at_utc"] > prev["acquired_at_utc"]:
            latest[c["cik"]] = c
    out, seen = [], set()
    for c in latest.values():
        body = read_json(c["extract_path"], default=None) or {}
        for e in body.get("events") or ():
            acc = e.get("accepted_at_utc")
            if not acc or e.get("is_amendment"):
                continue
            if str(CK.iso_precise(CK.parse_iso(acc))) >= cut:
                continue
            key = (e["cik"], e.get("accession"))
            if key in seen:
                continue
            seen.add(key)
            tm = classify_timing(acc)
            out.append(dict(e, timing=tm["timing"],
                            reaction_session=tm["reaction_session"],
                            captured_at_utc=c["acquired_at_utc"]))
    out.sort(key=lambda e: str(e["accepted_at_utc"]))
    return out


def recent_announcements(cutoff_session: _dt.date, window_sessions: int,
                         as_of_instant: _dt.datetime = None) -> list:
    """Events whose REACTION session lies in the last ``window_sessions``
    weekdays ending at ``cutoff_session`` (inclusive)."""
    days = []
    d = cutoff_session
    while len(days) < int(window_sessions):
        if d.weekday() not in CK.WEEKEND:
            days.append(str(d))
        d -= _dt.timedelta(days=1)
    keep = set(days)
    return [e for e in events(as_of_instant)
            if e.get("reaction_session") in keep]


def captured_events() -> list:
    """Every event the captures HOLD, with no point-in-time filter.

    This is a COVERAGE fact - what was acquired - and it is reported
    separately from :func:`events`, which answers the different question of
    what a signal built at a given instant may legitimately see. On the run
    that performs the acquisition the two necessarily disagree: a capture
    cannot precede the instant it was taken, so nothing acquired in this run
    is admissible in this run. Reporting only the second number would make a
    successful acquisition read as an empty one.
    """
    latest: dict = {}
    for c in _manifest().get("captures") or ():
        if _forbidden(c.get("extract_path")):
            continue
        prev = latest.get(c["cik"])
        if prev is None or c["acquired_at_utc"] > prev["acquired_at_utc"]:
            latest[c["cik"]] = c
    out = []
    for c in latest.values():
        body = read_json(c["extract_path"], default=None) or {}
        for e in body.get("events") or ():
            if e.get("is_amendment") or not e.get("accepted_at_utc"):
                continue
            tm = classify_timing(e["accepted_at_utc"])
            out.append(dict(e, timing=tm["timing"],
                            reaction_session=tm["reaction_session"],
                            captured_at_utc=c["acquired_at_utc"]))
    out.sort(key=lambda e: str(e["accepted_at_utc"]))
    return out


def universe_coverage(universe=None, as_of_instant: _dt.datetime = None,
                      pit: bool = True) -> dict:
    """Is every emission-universe name either captured or recorded UNMAPPED?

    A cross-section built from half the universe is a cross-section of the
    half that happened to be captured, and its ranks would carry that
    selection. The challenger emits only when the universe is accounted for:
    captured, or acknowledged as having no CIK on the SEC's own map.
    """
    now = as_of_instant or CK.now_utc()
    cut = CK.iso_precise(now)
    if universe is None:
        from . import challengers as CH
        universe = CH._eq_universe()
    want = {norm_ticker(t) for t in (universe or ()) if t}
    have, unmapped = set(), set()
    for c in _manifest().get("captures") or ():
        if pit and str(c.get("acquired_at_utc_precise")
                       or c.get("acquired_at_utc")) >= cut:
            continue
        have.add(norm_ticker(c.get("ticker")))
    cmap = ticker_to_cik(acquire=False, now=now)
    if not cmap:
        return {"n_universe": len(want), "n_captured": len(want & have),
                "n_unmapped_on_sec_map": None, "unmapped": [],
                "n_missing": len(want - have), "missing": sorted(want - have)[:40],
                "complete": False,
                "rule": "REFUSED - the SEC ticker map could not be read, so "
                        "'unmapped' cannot be distinguished from 'not "
                        "captured' and the universe cannot be accounted for"}
    for t in want:
        if t not in cmap:
            unmapped.add(t)
    missing = sorted(want - have - unmapped)
    return {"n_universe": len(want), "n_captured": len(want & have),
            "n_unmapped_on_sec_map": len(unmapped),
            "unmapped": sorted(unmapped)[:40],
            "n_missing": len(missing), "missing": missing[:40],
            "complete": not missing,
            "rule": "every universe name is captured, or acknowledged as "
                    "carrying no CIK on the SEC's own ticker map"}


def run(*, acquire_now: bool = True, campaign_id: str = CAMPAIGN_ID,
        as_of: _dt.date = None,
        budget_seconds: float = MAX_SECONDS_PER_RUN) -> dict:
    now = CK.now_utc()
    as_of = as_of or CK.eastern_date(now)
    acq = acquire(acquire=acquire_now, today=as_of,
                  budget_seconds=budget_seconds, now=now)
    held = captured_events()
    admissible_now = events(now)
    caps = _manifest().get("captures") or []
    recent = [e for e in held if str(e.get("filing_date") or "")
              >= str(as_of - _dt.timedelta(days=30))]
    by_timing = {t: sum(1 for e in held if e.get("timing") == t)
                 for t in TIMINGS}
    live = bool(caps) and bool(held)
    body = artifact_body(
        "r46_5_earnings_lane/1", CALCULATION_OWNER,
        as_of=str(as_of),
        state=("LIVE_PROSPECTIVE" if live else
               "DATA_BLOCKED" if acq.get("state") == SEC.BLOCKED_NO_CONTACT
               else "FROZEN_PENDING_ACQUISITION"),
        acquisition=acq,
        n_captures=len(caps),
        n_issuers_captured=len({c.get("cik") for c in caps}),
        raw_root=str(raw_dir()),
        source={"document": "data.sec.gov/submissions/CIK##########.json",
                "event": "Form 8-K carrying Item %s (Results of Operations "
                         "and Financial Condition)" % EARNINGS_ITEM,
                "instant": "acceptanceDateTime as stamped by EDGAR",
                "fiscal_period": "reportDate on the filing",
                "ticker_map": TICKER_MAP_URL,
                "licence": "SEC EDGAR, public domain, $0"},
        coverage={"n_events": len(held),
                  "n_events_last_30_days": len(recent),
                  "earliest_accepted": (held[0]["accepted_at_utc"] if held
                                        else None),
                  "latest_accepted": (held[-1]["accepted_at_utc"] if held
                                      else None),
                  "by_timing": by_timing,
                  "n_issuers_with_events": len({e["cik"] for e in held}),
                  # A capture cannot precede the instant it was taken, so on
                  # the acquiring run this is 0 by construction and becomes
                  # the full count at the next emission instant.
                  "n_events_pit_admissible_at_this_instant":
                      len(admissible_now),
                  "captures_taken_in_this_run_are_admissible_from_the_next":
                      True},
        point_in_time={
            "key": "EDGAR acceptanceDateTime per filing; an event is "
                   "admissible at an emission instant only if accepted "
                   "before it AND present in a capture acquired before it",
            "reaction_session": "BEFORE_OPEN -> the filing's ET date; "
                                "INTRADAY -> the same session; AFTER_CLOSE "
                                "-> the next weekday",
            "no_future_calendar": "scheduled future announcement dates are "
                                  "NOT owned; the lane reacts to filings, it "
                                  "never anticipates them",
            "synthetic_fixture": "REFUSED by path token; no file named "
                                 "fixture/synthetic/sample is ever read",
            "amendments_excluded": True,
        },
        universe_coverage=universe_coverage(as_of_instant=now, pit=False),
        universe_coverage_pit_now=universe_coverage(as_of_instant=now,
                                                    pit=True),
        challengers_frozen=["r46_5_pead_announcement_return_20d"],
        information_family="EARNINGS_EVENTS",
        money_spent_usd=0.0, credential_written=False,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "raw_dir", "manifest_path",
           "EARNINGS_ITEM", "TIMINGS", "norm_ticker", "cik10",
           "ticker_to_cik", "acquire", "classify_timing", "events",
           "captured_events", "recent_announcements", "universe_coverage",
           "run"]
