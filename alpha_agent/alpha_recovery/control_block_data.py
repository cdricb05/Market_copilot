"""alpha_agent.alpha_recovery.control_block_data - the free SEC acquisition for
the Schedule 13D/13G control-block axis.

One source, published by the SEC itself, free, no key, no account, no licence:
EDGAR. Nothing here spends money.

WHAT IS ACQUIRED, and why it is enough

* **The event stream** comes from the per-issuer submissions histories already
  collected by the canonical owner :mod:`alpha_agent.r63.acquire`. A Schedule
  13D/G is filed BY a beneficial owner ABOUT an issuer, and EDGAR indexes it
  under the SUBJECT issuer's own CIK - measured, not assumed - so the estate's
  existing security -> CIK bridge closes identity in one hop and no CUSIP
  licence, and no CUSIP at all, is required for it.

* **The cover pages** are fetched one document per filing, because the reported
  PERCENT OF CLASS lives only in the document. The raw bytes are stored
  gzipped, so the parser can be improved later without re-fetching anything.

THREE MEASURED FACTS THIS MODULE ENCODES, each of which silently corrupts the
stream if assumed instead:

1. **EDGAR renamed the form type.** When the structured-filing mandate took
   effect on 2024-12-18, ``SC 13D`` became ``SCHEDULE 13D``. The canonical
   keep-list matches the prefix ``SC 13``, so every schedule filed since is
   absent from the shared filings index while still present in the raw
   histories. Both spellings are the same schedule and normalise together here;
   taking the index at face value would end the event stream in December 2024
   and make 21 months of the panel look eventless.

2. **``acceptanceDateTime`` is UTC, not Eastern**, despite naming a rule
   (EDGAR's 17:30 business-day cutoff) that is Eastern. Tested against 136k
   filings: reading the stamp as published reproduces the filing date EDGAR
   actually assigned 49.7% of the time and puts 14.1% of filings outside
   EDGAR's own 06:00-22:00 acceptance window; converting from UTC reproduces it
   96.4% of the time. Reading it as Eastern would move most after-hours filings
   a full session early - a look-ahead.

3. **The submissions index names the XSL-RENDERED document.**
   ``xslSCHEDULE_13G_X02/primary_doc.xml`` is HTML wearing an .xml extension;
   the machine-readable original is the same leaf at the accession ROOT. This
   is the identical trap the N-PORT measurement hit.

RESEARCH ONLY. No purchase, no subscription, no promotion, no capital, no
proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import gzip
import json
import re
import threading
import time
from concurrent import futures
from datetime import datetime, timedelta, timezone
from pathlib import Path

from alpha_agent.r63 import acquire as ACQ
from alpha_agent.r63 import panels as P

from . import now_iso, research_root

CALCULATION_OWNER = "alpha_agent.alpha_recovery.control_block_data"

#: Both EDGAR spellings of the same schedule. The right-hand side is the
#: canonical form used everywhere downstream.
FORM_MAP = {
    "SC 13D": "13D", "SCHEDULE 13D": "13D",
    "SC 13D/A": "13D/A", "SCHEDULE 13D/A": "13D/A",
    "SC 13G": "13G", "SCHEDULE 13G": "13G",
    "SC 13G/A": "13G/A", "SCHEDULE 13G/A": "13G/A",
}
#: SC 13E3 (going-private) is a different disclosure and is NOT a 5% beneficial
#: ownership schedule. It is excluded by omission, deliberately.
CANONICAL_FORMS = ("13D", "13D/A", "13G", "13G/A")

#: The date EDGAR's structured Schedule 13D/G mandate took effect.
STRUCTURED_ERA_START = "2024-12-18"

#: EDGAR assigns the NEXT business day to a submission accepted at or after
#: 17:30 Eastern. Used only to VERIFY the timezone reading, never to invent a date.
EDGAR_CUTOFF_ET_HOUR = 17.5

ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/%s/%s/%s"

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")

# The mandated cover-page caption. Row (9) on a Schedule 13D cover page and row
# (9) or (11) on a 13G; wording drifts across two decades of filing agents, so
# the caption is matched loosely and the VALUE strictly.
_PCT_CAPTION = re.compile(
    r"PERCENT(?:AGE)?\s+OF\s+CLASS\s+REPRESENTED\s+BY\s+(?:THE\s+)?AMOUNT\s+IN\s+"
    r"(?:ROW|BOX|LINE)?\s*\(?\s*(?:9|11)\s*\)?", re.I)
_PCT_FALLBACK = re.compile(r"PERCENT(?:AGE)?\s+OF\s+CLASS\s*[:\-]?\s*", re.I)
_PCT_VALUE = re.compile(r"(?<![\d.])([0-9]{1,3}(?:\.[0-9]{1,4})?)\s*%")
#: Many filing agents print the cover-page percent with NO per-cent sign at all
#: ("11. PERCENT OF CLASS REPRESENTED BY AMOUNT IN ROW (9)   7.91"). Measured:
#: 389 of 400 sampled failures are exactly this, so requiring the sign discards
#: 6% of the stream and breaks those filers' chains. A DECIMAL POINT is required
#: here so that the row number of the NEXT cover-page box ("12") can never be
#: read as a percentage.
_PCT_BARE = re.compile(r"(?<![\d.])([0-9]{1,3}\.[0-9]{1,4})(?![\d.%])")
#: Row 12's caption ends row 11's value. Truncating there stops the search
#: bleeding into the next box.
_NEXT_BOX = re.compile(r"TYPE\s+OF\s+REPORTING\s+PERSON", re.I)
_CUSIP = re.compile(
    r"CUSIP\s*(?:NO\.?|NUMBER|#)?\s*[:.\-]?\s*([0-9A-Z]{6}[\s-]?[0-9A-Z]{2}[\s-]?[0-9A-Z]?)\b", re.I)
_PERSON = re.compile(
    r"NAMES?\s+OF\s+REPORTING\s+PERSONS?[^A-Za-z0-9]{0,40}(?:I\.?R\.?S\.?[^A-Za-z0-9]*"
    r"IDENTIFICATION\s+NOS?\.?[^A-Za-z0-9]*OF\s+ABOVE\s+PERSONS?[^A-Za-z0-9]*"
    r"\(?ENTITIES\s+ONLY\)?)?[^A-Za-z0-9]{0,20}([A-Za-z0-9][^\n]{2,70}?)\s{2,}", re.I)

# The two structured schedules do NOT share an element vocabulary: a 13G says
# <classPercent> / <issuerCusipNumber>, a 13D says <percentOfClass> /
# <issuerCUSIP>. Reading only the 13G spelling silently drops every structured
# 13D - which is the activist half of the axis, and the half the hypothesis is
# really about. Both spellings are accepted.
_XML_PCT = re.compile(r"<(?:classPercent|percentOfClass)>\s*([0-9.]+)\s*</", re.I)
_XML_CUSIP = re.compile(r"<(?:issuerCusipNumber|issuerCUSIP)>\s*([^<]+)</", re.I)
_XML_ICIK = re.compile(r"<issuerCIK>\s*([0-9]+)\s*</", re.I)
_XML_PERSON = re.compile(r"<reportingPersonName>\s*([^<]+)</reportingPersonName>", re.I)
_XML_PERSON_CIK = re.compile(r"<reportingPersonCIK>\s*([0-9]+)\s*</", re.I)
_XML_AMEND = re.compile(r"<amendmentNo>\s*([^<]*)</amendmentNo>", re.I)
_XML_EVENT = re.compile(
    r"<(?:eventDateRequiresFilingThisStatement|dateOfEvent)>\s*([0-9\-]+)\s*</", re.I)


def data_dir() -> Path:
    return research_root() / "_data_sec_13dg"


def docs_dir() -> Path:
    return data_dir() / "docs"


def manifest_path() -> Path:
    return data_dir() / "acquisition_manifest.jsonl"


# --------------------------------------------------------------------------- #
# The acceptance instant
# --------------------------------------------------------------------------- #
def _et_offset_hours(ts_utc: datetime) -> int:
    """US Eastern offset from UTC for ``ts_utc``: -4 under daylight time,
    -5 otherwise. Second Sunday in March to first Sunday in November, the rule
    in force for every year this panel spans."""
    y = ts_utc.year

    def _nth_sunday(month: int, nth: int) -> datetime:
        d = datetime(y, month, 1, tzinfo=timezone.utc)
        d += timedelta(days=(6 - d.weekday()) % 7)          # first Sunday
        return d + timedelta(days=7 * (nth - 1))

    start = _nth_sunday(3, 2) + timedelta(hours=7)           # 02:00 ET = 07:00 UTC
    end = _nth_sunday(11, 1) + timedelta(hours=6)            # 02:00 ET = 06:00 UTC
    return -4 if start <= ts_utc < end else -5


def acceptance_et(stamp: str) -> datetime | None:
    """EDGAR's ``acceptanceDateTime`` as an EASTERN wall-clock instant.

    The stamp is published with a ``Z`` and is genuinely UTC - verified against
    EDGAR's own 17:30 business-day cutoff on 136,395 filings. Returned naive, in
    Eastern, because every downstream comparison is against a US session date.
    """
    s = str(stamp or "").strip()
    if not s:
        return None
    s = s.replace("Z", "").replace("T", " ")
    try:
        ts = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            ts = datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            return None
    ts = ts.replace(tzinfo=timezone.utc)
    return (ts + timedelta(hours=_et_offset_hours(ts))).replace(tzinfo=None)


# --------------------------------------------------------------------------- #
# The event stream, from the already-acquired submissions histories
# --------------------------------------------------------------------------- #
def enumerate_schedules(*, since: str = "2009-01-01", verbose: bool = True) -> list:
    """Every Schedule 13D/G in the acquired per-issuer histories.

    Deduplicated on (issuer CIK, accession): EDGAR lists one accession once per
    issuer, but an issuer's ``recent`` block and its older shard files overlap,
    and counting a filing twice would invent an event.
    """
    raw_dir = P.submissions_dir() / "raw"
    out, seen = [], set()
    files = sorted(raw_dir.glob("*.json.gz"))
    for k, p in enumerate(files):
        try:
            obj = json.loads(gzip.open(p, "rb").read().decode("utf-8"))
        except Exception:                                    # noqa: BLE001
            continue
        cik = str(obj.get("cik") or "").lstrip("0")
        if not cik:
            continue
        blocks = [(obj.get("filings") or {}).get("recent") or {}]
        blocks += list(obj.get("_r63_older_blocks") or [])
        for b in blocks:
            fs = b.get("form") or []
            fd = b.get("filingDate") or []
            ad = b.get("acceptanceDateTime") or []
            an = b.get("accessionNumber") or []
            dc = b.get("primaryDocument") or []
            for i, f in enumerate(fs):
                form = FORM_MAP.get(str(f).upper())
                if form is None:
                    continue
                acc = str(an[i]) if i < len(an) else ""
                if not acc or (cik, acc) in seen:
                    continue
                fdate = str(fd[i])[:10] if i < len(fd) else ""
                if not fdate or fdate < since:
                    continue
                seen.add((cik, acc))
                out.append({
                    "issuer_cik": cik, "form": form, "raw_form": str(f),
                    "filing_date": fdate,
                    "acceptance_utc": str(ad[i]) if i < len(ad) and ad[i] else "",
                    "accession": acc,
                    "primary_document": str(dc[i]) if i < len(dc) else "",
                })
        if verbose and k % 200 == 0:
            print("  enumerate %d/%d schedules=%d" % (k, len(files), len(out)), flush=True)
    out.sort(key=lambda r: (r["filing_date"], r["issuer_cik"], r["accession"]))
    return out


# --------------------------------------------------------------------------- #
# Documents
# --------------------------------------------------------------------------- #
def _doc_path(cik: str, accession: str) -> Path:
    acc = accession.replace("-", "")
    return docs_dir() / cik[-3:].zfill(3) / cik / ("%s.gz" % acc)


def _root_leaf(primary_document: str) -> str:
    """The machine-readable document at the accession ROOT.

    The submissions index names ``xslSCHEDULE_13G_X02/primary_doc.xml`` - the
    XSL-RENDERED view. Dropping the renderer directory yields the original.
    """
    return str(primary_document or "").rsplit("/", 1)[-1]


def document_url(row: dict) -> str:
    return ARCHIVE_URL % (row["issuer_cik"], row["accession"].replace("-", ""),
                          _root_leaf(row["primary_document"]))


#: Workers sharing ONE global rate gate. The SEC's stated fair-access limit is
#: a RATE (<= 10 requests/second), not a requirement to issue requests one at a
#: time, and a single-threaded loop spends most of its wall clock waiting for
#: round trips rather than respecting the limit: measured, one worker sustained
#: 3.4 requests/second against a 7.7/second budget. Three workers behind the
#: SAME gate keep the global rate identical to the single-threaded case and
#: stop wasting the difference. The gate, not the worker count, is what bounds
#: the rate - raising this number never raises the request rate.
FETCH_WORKERS = 3


class _RateGate:
    """One global minimum interval between request STARTS, shared by all
    workers. Honours ``ACQ.MIN_INTERVAL_S`` exactly as the serial loop does."""

    def __init__(self, interval: float):
        self._interval = float(interval)
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            due = max(now, self._next)
            self._next = due + self._interval
        delay = due - now
        if delay > 0:
            time.sleep(delay)


def fetch_documents(rows: list, *, headers: dict | None = None,
                    limit: int | None = None, workers: int = FETCH_WORKERS,
                    verbose: bool = True) -> dict:
    """Fetch and store each schedule's primary document, gzipped.

    Idempotent: a document already on disk is never re-requested, so the run is
    restartable. Raw bytes are kept so the parser can be improved without
    spending the network again.
    """
    headers = headers or _headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    todo = [r for r in rows if r.get("primary_document")]
    if limit:
        todo = todo[:int(limit)]
    data_dir().mkdir(parents=True, exist_ok=True)
    pending = []
    cached = 0
    for r in todo:
        p = _doc_path(r["issuer_cik"], r["accession"])
        if p.exists() and p.stat().st_size > 64:
            cached += 1
        else:
            pending.append(r)
    gate = _RateGate(ACQ.MIN_INTERVAL_S)
    counters = {"ok": 0, "failed": 0, "bytes": 0}
    lock = threading.Lock()
    t0 = time.time()

    def _one(r: dict) -> None:
        gate.wait()
        status, body = ACQ._get(document_url(r), headers)
        if status != 200 or not body:
            with lock:
                counters["failed"] += 1
            _append_manifest({"at": now_iso(), "accession": r["accession"],
                              "issuer_cik": r["issuer_cik"], "http": status,
                              "state": "FAILED"})
            return
        p = _doc_path(r["issuer_cik"], r["accession"])
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".%d.tmp" % threading.get_ident())
        with gzip.open(tmp, "wb") as fh:
            fh.write(body)
        tmp.replace(p)
        with lock:
            counters["ok"] += 1
            counters["bytes"] += len(body)
            n = counters["ok"]
        if verbose and n % 2000 == 0:
            rate = n / max(time.time() - t0, 1e-9)
            print("  documents %d/%d failed=%d %.1fGB %.1f req/s"
                  % (n, len(pending), counters["failed"],
                     counters["bytes"] / 1e9, rate), flush=True)

    if pending:
        with futures.ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            list(pool.map(_one, pending))
    return {"requested": len(todo), "fetched": counters["ok"], "cached": cached,
            "failed": counters["failed"], "bytes": counters["bytes"],
            "workers": int(workers), "min_interval_s": ACQ.MIN_INTERVAL_S,
            "dir": str(docs_dir())}


def _headers() -> dict | None:
    contact = ACQ.contact_email()
    if not contact:
        return None
    h = ACQ._headers(contact)
    h["Accept"] = "*/*"
    return h


def _append_manifest(row: dict) -> None:
    data_dir().mkdir(parents=True, exist_ok=True)
    with open(manifest_path(), "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #
def _flatten(raw: bytes) -> str:
    txt = raw.decode("utf-8", "replace")
    txt = _TAG.sub(" ", txt)
    for a, b in (("&nbsp;", " "), ("&#160;", " "), ("&amp;", "&"), ("&#37;", "%")):
        txt = txt.replace(a, b)
    return _WS.sub(" ", txt)


def _percents_from_text(flat: str) -> list:
    """Every cover-page percent in the filing.

    A joint filing repeats the cover page once per reporting person, so several
    percents are normal and are all returned; the caller decides how a filing's
    block is defined. A value above 100 is not a percent of class and is
    dropped rather than clipped.
    """
    out = []
    for m in _PCT_CAPTION.finditer(flat):
        win = flat[m.end():m.end() + 240]
        nb = _NEXT_BOX.search(win)
        if nb:
            win = win[:nb.start()]
        v = _PCT_VALUE.search(win) or _PCT_BARE.search(win)
        if v:
            out.append(float(v.group(1)))
    if not out:
        for m in _PCT_FALLBACK.finditer(flat):
            v = _PCT_VALUE.search(flat[m.end():m.end() + 120])
            if v:
                out.append(float(v.group(1)))
    return [p for p in out if 0.0 <= p <= 100.0]


def parse_document(raw: bytes, *, structured: bool) -> dict:
    """Read one schedule. Returns the fields the experiment needs and a state.

    ``structured`` selects the reader; it is decided by the document's own
    content, never by the filing date, because EDGAR's transition is per-filing.
    """
    head = raw[:2000].decode("utf-8", "replace")
    is_xml = "<edgarSubmission" in head or head.lstrip().startswith("<?xml")
    if is_xml:
        txt = raw.decode("utf-8", "replace")
        pcts = [float(x) for x in _XML_PCT.findall(txt) if _num_ok(x)]
        persons = [p.strip()[:80] for p in _XML_PERSON.findall(txt)]
        cus = _XML_CUSIP.search(txt)
        icik = _XML_ICIK.search(txt)
        am = _XML_AMEND.search(txt)
        ev = _XML_EVENT.search(txt)
        return {"reader": "STRUCTURED_XML",
                "percents": [p for p in pcts if 0.0 <= p <= 100.0],
                "reporting_persons": persons,
                "reporting_person_ciks": sorted(set(_XML_PERSON_CIK.findall(txt))),
                "cusip": re.sub(r"[^0-9A-Za-z]", "", cus.group(1))[:9] if cus else None,
                "issuer_cik_stated": icik.group(1).lstrip("0") if icik else None,
                "amendment_no": (am.group(1).strip() or None) if am else None,
                "event_date": ev.group(1) if ev else None,
                "state": "OK" if pcts else "NO_PERCENT"}
    flat = _flatten(raw)
    pcts = _percents_from_text(flat)
    cus = _CUSIP.search(flat)
    return {"reader": "TEXT_COVER_PAGE", "percents": pcts,
            "reporting_persons": [], "reporting_person_ciks": [],
            "cusip": re.sub(r"[^0-9A-Za-z]", "", cus.group(1))[:9] if cus else None,
            "issuer_cik_stated": None, "amendment_no": None, "event_date": None,
            "state": "OK" if pcts else "NO_PERCENT"}


def _num_ok(x: str) -> bool:
    try:
        float(x)
        return True
    except (TypeError, ValueError):
        return False


def parse_all(rows: list, *, verbose: bool = True) -> list:
    """Parse every stored document. Pure local work; no network."""
    out = []
    for k, r in enumerate(rows):
        p = _doc_path(r["issuer_cik"], r["accession"])
        rec = dict(r)
        if not p.exists():
            rec.update({"state": "NO_DOCUMENT", "percents": [], "reader": None,
                        "cusip": None, "reporting_persons": [],
                        "reporting_person_ciks": []})
            out.append(rec)
            continue
        try:
            raw = gzip.open(p, "rb").read()
        except Exception:                                    # noqa: BLE001
            rec.update({"state": "UNREADABLE", "percents": [], "reader": None,
                        "cusip": None, "reporting_persons": [],
                        "reporting_person_ciks": []})
            out.append(rec)
            continue
        rec.update(parse_document(raw, structured=r["filing_date"] >= STRUCTURED_ERA_START))
        out.append(rec)
        if verbose and k % 20000 == 0 and k:
            print("  parsed %d/%d" % (k, len(rows)), flush=True)
    return out


def acquire(*, since: str = "2009-01-01", verbose: bool = True) -> dict:
    """Enumerate the schedules and fetch their cover pages. Free; idempotent."""
    rows = enumerate_schedules(since=since, verbose=verbose)
    if verbose:
        print("control_block_data: %d schedules from %s" % (len(rows), since), flush=True)
    res = fetch_documents(rows, verbose=verbose)
    summary = {
        "calculation_owner": CALCULATION_OWNER,
        "acquired_at": now_iso(),
        "paid_dollars": 0, "subscription_started": False, "licence_accepted": False,
        "since": since, "schedules": len(rows), "documents": res,
        "form_mix": {f: sum(1 for r in rows if r["form"] == f) for f in CANONICAL_FORMS},
        "first_filing": rows[0]["filing_date"] if rows else None,
        "last_filing": rows[-1]["filing_date"] if rows else None,
        "acceptance_timezone": "UTC (verified against EDGAR's 17:30 ET cutoff)",
        "form_rename_handled": "SC 13D/G == SCHEDULE 13D/G from %s" % STRUCTURED_ERA_START,
        "user_agent_convention": "canonical SEC collector (git config user.email)",
    }
    data_dir().mkdir(parents=True, exist_ok=True)
    (data_dir() / "acquisition_summary.json").write_text(
        json.dumps(summary, indent=1, sort_keys=True), encoding="utf-8")
    (data_dir() / "schedules.jsonl").write_text(
        "\n".join(json.dumps(r, sort_keys=True) for r in rows), encoding="utf-8")
    return summary


def load_schedules() -> list:
    p = data_dir() / "schedules.jsonl"
    if not p.exists():
        return []
    return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]


def parsed_records(*, rebuild: bool = False, verbose: bool = True) -> list:
    """Every schedule with its cover page read. Cached, because re-reading 97k
    gzipped documents on every run is minutes of work with no new information;
    ``rebuild=True`` re-reads from the STORED bytes and never re-fetches."""
    p = data_dir() / "parsed.jsonl"
    if p.exists() and not rebuild:
        return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
    rows = parse_all(load_schedules(), verbose=verbose)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows), encoding="utf-8")
    return rows


# --------------------------------------------------------------------------- #
# Filer identity, in bulk
# --------------------------------------------------------------------------- #
FULL_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/%d/QTR%d/master.idx"


def _norm_accession(acc: str) -> str:
    """The ONE spelling of an accession number used as a key anywhere here."""
    return str(acc or "").replace("-", "").strip()


def _index_path(year: int, qtr: int) -> Path:
    return data_dir() / "full_index" / ("sc13_%d_q%d.tsv" % (year, qtr))


def fetch_full_index(*, first_year: int = 2009, last_year: int = 2026,
                     headers: dict | None = None, verbose: bool = True) -> dict:
    """The reporting person, from EDGAR's own quarterly index.

    A Schedule is indexed under BOTH parties: the SUBJECT issuer and the
    REPORTING PERSON each get a row for the same accession (measured: 19,053 of
    19,202 accessions in 2019Q1 carry exactly two CIKs). Grouping by accession
    therefore names the filer without opening a single filing header - and
    without ever matching on an issuer's name.

    ``master.idx`` is pipe-delimited, unlike the fixed-width ``form.idx`` whose
    column offsets drift; only the Schedule rows are kept, so ~3 GB of index
    becomes a few MB on disk.
    """
    headers = headers or _headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    want = tuple(FORM_MAP.keys())
    ok = cached = failed = 0
    kept = 0
    last = 0.0
    for year in range(int(first_year), int(last_year) + 1):
        for qtr in (1, 2, 3, 4):
            p = _index_path(year, qtr)
            if p.exists() and p.stat().st_size > 0:
                cached += 1
                kept += sum(1 for _ in p.open(encoding="utf-8"))
                continue
            wait = ACQ.MIN_INTERVAL_S - (time.time() - last)
            if wait > 0:
                time.sleep(wait)
            last = time.time()
            status, body = ACQ._get(FULL_INDEX_URL % (year, qtr), headers)
            if status != 200 or not body:
                failed += 1
                continue
            rows = []
            for ln in body.decode("latin-1").splitlines():
                parts = ln.split("|")
                if len(parts) != 5:
                    continue
                if parts[2].strip().upper() not in want:
                    continue
                # The index spells an accession WITH dashes and the submissions
                # history WITHOUT them. Both are normalised to the dashless
                # form here and at the lookup, because a key that differs only
                # in punctuation joins nothing and reports no error - the filer
                # would simply be "unidentified" for every filing.
                acc = _norm_accession(parts[4].rsplit("/", 1)[-1].replace(".txt", ""))
                rows.append("\t".join([parts[0].strip().lstrip("0"),
                                       parts[2].strip(), parts[3].strip(), acc]))
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("\n".join(rows), encoding="utf-8")
            ok += 1
            kept += len(rows)
            if verbose:
                print("  full-index %dQ%d -> %d schedule rows" % (year, qtr, len(rows)),
                      flush=True)
    return {"fetched": ok, "cached": cached, "failed": failed, "schedule_rows": kept}


def load_filer_map(issuer_ciks: set | None = None) -> dict:
    """accession -> the CIK that is NOT the subject issuer, i.e. the filer.

    Where an accession carries more than two CIKs (a group filing jointly) the
    filer is reported as the SORTED FIRST non-issuer CIK so the key is stable
    across runs; where it carries only one, the filer is unknown and the
    accession is absent - Cell B drops those and counts them.
    """
    by_acc: dict = {}
    for p in sorted((data_dir() / "full_index").glob("sc13_*.tsv")):
        for ln in p.read_text(encoding="utf-8").splitlines():
            if not ln.strip():
                continue
            cik, _form, _date, acc = ln.split("\t")
            by_acc.setdefault(_norm_accession(acc), set()).add(cik)
    out = {}
    for acc, ciks in by_acc.items():
        others = sorted(ciks - issuer_ciks) if issuer_ciks else sorted(ciks)
        if len(ciks) >= 2 and others:
            out[acc] = others[0]
    return out
