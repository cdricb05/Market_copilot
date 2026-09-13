"""alpha_agent.alpha_recovery.insider_form4_data - the SEC Form 4 open-market
PURCHASE stream and its census, for the insider-transaction axis.

Two sources, both published by the SEC itself, free, no key, no account, no
licence. Nothing here spends money.

* **The transactions** come from the SEC's own Insider Transactions Data Sets
  (``{year}q{quarter}_form345.zip``), ALREADY acquired by the R35 owner
  :mod:`alpha_agent.r35.acquisition` and registered for R63 as
  ``alpha_agent.r63.panels.FORM345_DIR``. They are read in place, never copied.
  Only a quarter the estate does not own is requested, into this campaign's own
  data directory.

* **The availability instant** comes from the per-issuer EDGAR submissions
  histories ALREADY acquired by the canonical owner :mod:`alpha_agent.r63.acquire`.
  EDGAR indexes a Form 4 under the ISSUER's own CIK (measured: 837,920 Form 4
  rows across the 1,080 stored histories), so the SEC acceptance timestamp joins
  to a data-set transaction on (issuer CIK, accession) in one hop.

THIS MODULE COMPUTES NO RETURN AND READS NO PRICE. It turns Form 4 filings into a
purchase stream and measures how much of the eligible universe that stream can
see. Keeping prices out of it is what makes "the cell was defined before any
return existed" a property of the code rather than a claim - a test pins it.

FACTS THIS MODULE ENCODES, each of which silently corrupts the stream if assumed

1. **The data sets carry a filing DATE, not an instant.** ``SUBMISSION.tsv`` has
   ``FILING_DATE`` only. The instant is joined from the submissions histories;
   where no history holds the accession, the CONSERVATIVE BOUND "17:30 Eastern on
   the filing date" is used - EDGAR assigns the next business day to anything
   accepted at or after 17:30, so the true instant can never be later. Such rows
   are labelled and counted; none is ever moved earlier.

2. **``acceptanceDateTime`` is UTC** (measured on the 13D/G axis). The one
   converter :func:`control_block_data.acceptance_et` is reused.

3. **The owner-relationship vocabulary drifts.** Early files glue tokens
   (``TenPercentOwnerOther``, ``Director,OfficerOther``); later files separate
   them with commas. A role is read by token CONTAINMENT, never by equality.

4. **``AFF10B5ONE`` exists only from the 2023 Form 4 amendments.** The column is
   ABSENT before, and later carries ``0``/``1``/``false``/``true``. An absent or
   blank value is UNOBSERVED (None) - never "not under a plan".

5. **Transaction value is not used.** ``TRANS_SHARES`` x ``TRANS_PRICEPERSHARE``
   are filer-entered and unvalidated (R35 measured a single filing implying
   $2.1e16); the estate records the value field as BLOCKED_SOURCE. Only the
   price's PRESENCE is kept, for the census.

RESEARCH ONLY. No purchase, no subscription, no promotion, no capital, no
proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import re
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

from alpha_agent.r63 import EQUITY_DISCOVERY_START
from alpha_agent.r63 import acquire as ACQ
from alpha_agent.r63 import panels as P
from alpha_agent.r63 import pit
from alpha_agent.sec_filing_behavior import parse_sec_date

from . import now_iso, research_root, write_artifact
from . import control_block_data as CBD
from . import control_block_events as CBE
from . import event_8k_data as E8D

CALCULATION_OWNER = "alpha_agent.alpha_recovery.insider_form4_data"
CENSUS_ARTIFACT = "insider_form4_census.json"

DATASET_URL = ("https://www.sec.gov/files/structureddata/data/"
               "insider-transactions-data-sets/%s_form345.zip")
#: The first quarter the estate owns. Earlier quarters exist at the SEC but
#: precede every scored window (the canonical grid starts 2011-07-01) by more
#: than three years, so they could only lengthen a look-back no cell needs.
FIRST_QUARTER = "2008q1"

#: Ownership forms whose EDGAR acceptance instant is joined.
OWNERSHIP_FORMS = ("4", "4/A", "5", "5/A")
PURCHASE_CODE = "P"

#: The estate's own record of why a Form 4 dollar value is not evidence.
VALUE_FIELD_RULE = ("BLOCKED_SOURCE FILER_ENTERED_FIELD_UNVALIDATED "
                    "(alpha_agent.r63.inventory; R35 contract "
                    "INSIDER_VALUE_WEIGHTING_ALLOWED = False)")

#: Availability-instant provenance, in order of preference.
SRC_ISSUER_HISTORY = "SUBMISSIONS_ISSUER_HISTORY"
SRC_OTHER_HISTORY = "SUBMISSIONS_OTHER_HISTORY"
SRC_FILING_DATE_BOUND = "FILING_DATE_1730ET_CONSERVATIVE_BOUND"

#: EDGAR's business-day cutoff, Eastern. A submission accepted at or after it
#: receives the next business day's filing date.
EDGAR_CUTOFF_ET = (17, 30)

csv.field_size_limit(2 ** 31 - 1)


def data_dir() -> Path:
    return research_root() / "_data_sec_form4"


def stream_path() -> Path:
    return data_dir() / "open_market_purchases.jsonl"


def archive_dirs() -> list:
    """The owned R35 acquisition first, then this campaign's own directory."""
    return [Path(P.FORM345_DIR), data_dir() / "archives"]


def archives() -> dict:
    """quarter -> archive path. An owned quarter is never shadowed."""
    out: dict = {}
    for d in archive_dirs():
        if not d.exists():
            continue
        for p in sorted(d.glob("*_form345.zip")):
            q = p.name.replace("_form345.zip", "")
            if re.match(r"^\d{4}q[1-4]$", q) and q >= FIRST_QUARTER:
                out.setdefault(q, p)
    return dict(sorted(out.items()))


def quarter_end(q: str) -> str:
    y, n = int(q[:4]), int(q[-1])
    nxt = date(y + 1, 1, 1) if n == 4 else date(y, 3 * n + 1, 1)
    return (nxt - timedelta(days=1)).isoformat()


def _next_quarter(q: str) -> str:
    y, n = int(q[:4]), int(q[-1])
    return "%dq%d" % (y + 1, 1) if n == 4 else "%dq%d" % (y, n + 1)


def observable_through(qs: dict | None = None) -> str | None:
    """The last calendar day the owned archive can report a filing for. After
    it, the absence of an event is UNOBSERVED, not an observation of none."""
    qs = archives() if qs is None else qs
    return quarter_end(max(qs)) if qs else None


# --------------------------------------------------------------------------- #
# Acquisition - only what the estate does not already own
# --------------------------------------------------------------------------- #
def acquire_missing(*, today: date | None = None, verbose: bool = True) -> dict:
    """Request each quarter after the last owned one, up to the current quarter.

    Idempotent and free. A quarter the SEC has not published answers 404 and is
    recorded as such - it is not an error, and nothing is fabricated for it.
    """
    owned = archives()
    today = today or date.today()
    current = "%dq%d" % (today.year, (today.month - 1) // 3 + 1)
    last = max(owned) if owned else FIRST_QUARTER
    todo = []
    q = _next_quarter(last) if owned else FIRST_QUARTER
    while q <= current:
        todo.append(q)
        q = _next_quarter(q)
    statuses: dict = {}
    headers = CBD._headers()
    if todo and not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT", "requested": todo}
    gate = CBD._RateGate(ACQ.MIN_INTERVAL_S)
    for q in todo:
        gate.wait()
        status, body = ACQ._get(DATASET_URL % q, headers)
        statuses[q] = int(status or 0)
        if status == 200 and body:
            out = data_dir() / "archives" / ("%s_form345.zip" % q)
            out.parent.mkdir(parents=True, exist_ok=True)
            tmp = out.with_suffix(".tmp")
            tmp.write_bytes(body)
            tmp.replace(out)
        if verbose:
            print("  form345 %s -> HTTP %s" % (q, status), flush=True)
    rec = {"at": now_iso(), "owned_last_quarter": last, "requested": todo,
           "http_status": statuses, "paid_dollars": 0,
           "unpublished": sorted(k for k, v in statuses.items() if v == 404)}
    data_dir().mkdir(parents=True, exist_ok=True)
    with open(data_dir() / "acquisition_manifest.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, sort_keys=True) + "\n")
    return rec


# --------------------------------------------------------------------------- #
# Field readers
# --------------------------------------------------------------------------- #
def relationship_flags(text) -> dict:
    """Role tokens by CONTAINMENT: early files glue ``TenPercentOwnerOther``."""
    s = str(text or "").upper().replace(" ", "")
    return {"director": "DIRECTOR" in s, "officer": "OFFICER" in s,
            "ten_percent_owner": "TENPERCENTOWNER" in s, "other": "OTHER" in s}


def boolean_flag(value) -> bool | None:
    """``1``/``true`` -> True, ``0``/``false`` -> False, anything else UNOBSERVED."""
    s = str(value if value is not None else "").strip().lower()
    if s in ("1", "true"):
        return True
    if s in ("0", "false"):
        return False
    return None


_T_PREFERRED = re.compile(r"PREFERRED|\bPFD\b|\bPREF\b")
_T_DEPOSITARY = re.compile(r"DEPOSITARY|\bADS\b|\bADR\b")
_T_DEBT = re.compile(r"\bNOTES?\b|DEBENTURE|\bBONDS?\b")
_T_WARRANT = re.compile(r"WARRANT|\bRIGHTS?\b")
_T_COMMON = re.compile(r"COMMON|ORDINARY|SHARE|STOCK")


def title_class(title) -> str:
    """The security a purchase bought. A non-common instrument is checked FIRST,
    so ``Depositary Shares "A" Preferred`` is never read as common because it
    says "shares"."""
    t = str(title or "").upper()
    for name, rx in (("PREFERRED", _T_PREFERRED), ("DEPOSITARY", _T_DEPOSITARY),
                     ("DEBT", _T_DEBT), ("WARRANT_OR_RIGHT", _T_WARRANT)):
        if rx.search(t):
            return name
    return "COMMON" if _T_COMMON.search(t) else "OTHER"


def _num(value) -> float | None:
    try:
        v = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return v if np.isfinite(v) else None


def _tsv(z: zipfile.ZipFile, member: str, counters: Counter):
    """Rows of one TSV member. The data sets are tab-delimited and unquoted."""
    with z.open(member) as fh:
        reader = csv.reader(io.TextIOWrapper(fh, encoding="utf-8", errors="replace",
                                             newline=""),
                            delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(reader, None)
        if not header:
            return
        n = len(header)
        for row in reader:
            if len(row) != n:
                counters["malformed_rows:" + member.rsplit("/", 1)[-1]] += 1
                if len(row) < n:
                    row = row + [""] * (n - len(row))
            yield dict(zip(header, row))


# --------------------------------------------------------------------------- #
# The stream
# --------------------------------------------------------------------------- #
def parse_archive(path: Path, ciks: set | None, counters: Counter, *,
                  quarter: str = "") -> list:
    """Every open-market PURCHASE row (code ``P``) of one quarterly archive for
    the issuers in ``ciks``, with its filing's metadata and reporting owners.

    Every other transaction code is COUNTED (so the cleaning is auditable) and
    dropped: grants, exercises, withholding, gifts, inheritances and the rest
    are compensation or estate mechanics, not a decision to commit capital.
    """
    z = zipfile.ZipFile(path)
    member = {n.rsplit("/", 1)[-1]: n for n in z.namelist()}
    meta: dict = {}
    for r in _tsv(z, member["SUBMISSION.tsv"], counters):
        cik = str(r.get("ISSUERCIK") or "").strip().lstrip("0")
        if not cik or (ciks is not None and cik not in ciks):
            continue
        dt = str(r.get("DOCUMENT_TYPE") or "").strip()
        counters["submissions_document_type:" + dt] += 1
        fd = parse_sec_date(r.get("FILING_DATE"))
        meta[CBD._norm_accession(r.get("ACCESSION_NUMBER"))] = {
            "issuer_cik": cik, "document_type": dt,
            "filing_date": fd.isoformat() if fd else None,
            "rule_10b5_1": boolean_flag(r.get("AFF10B5ONE")),
            "rule_10b5_1_column_present": "AFF10B5ONE" in r,
        }
    owners: dict = defaultdict(list)
    for r in _tsv(z, member["REPORTINGOWNER.tsv"], counters):
        acc = CBD._norm_accession(r.get("ACCESSION_NUMBER"))
        if acc not in meta:
            continue
        owners[acc].append({"cik": str(r.get("RPTOWNERCIK") or "").strip().lstrip("0"),
                            **relationship_flags(r.get("RPTOWNER_RELATIONSHIP")),
                            "title": str(r.get("RPTOWNER_TITLE") or "").strip()[:60]})
    rows = []
    for r in _tsv(z, member["NONDERIV_TRANS.tsv"], counters):
        acc = CBD._norm_accession(r.get("ACCESSION_NUMBER"))
        m = meta.get(acc)
        if m is None:
            continue
        code = str(r.get("TRANS_CODE") or "").strip().upper()
        counters["nonderivative_code:" + (code or "BLANK")] += 1
        if code != PURCHASE_CODE:
            continue
        td = parse_sec_date(r.get("TRANS_DATE"))
        title = str(r.get("SECURITY_TITLE") or "").strip()
        rows.append({
            **m, "accession": acc, "quarter": quarter,
            "trans_date": td.isoformat() if td else None,
            "shares": _num(r.get("TRANS_SHARES")),
            "acquired_disposed": str(r.get("TRANS_ACQUIRED_DISP_CD") or "").strip().upper(),
            "direct_indirect": str(r.get("DIRECT_INDIRECT_OWNERSHIP") or "").strip().upper(),
            "equity_swap": boolean_flag(r.get("EQUITY_SWAP_INVOLVED")),
            "security_title": title[:80], "title_class": title_class(title),
            "timeliness": str(r.get("TRANS_TIMELINESS") or "").strip().upper(),
            "shares_owned_following": _num(r.get("SHRS_OWND_FOLWNG_TRANS")),
            "price_present": (_num(r.get("TRANS_PRICEPERSHARE")) or 0.0) > 0.0,
            "owners": owners.get(acc, []),
        })
    return rows


def acceptance_for(accessions: set, *, verbose: bool = True) -> dict:
    """Acceptance instants for ``accessions`` from the stored issuer histories.

    Returns ``{"by_issuer": {(cik, acc): utc}, "by_accession": {acc: {utc}},
    "history_ciks": set}``. Only requested accessions are kept, so ~1.1M
    ownership rows never sit in memory at once.
    """
    by_issuer: dict = {}
    by_acc: dict = defaultdict(set)
    hist: set = set()
    files = sorted((P.submissions_dir() / "raw").glob("*.json.gz"))
    for k, p in enumerate(files):
        try:
            obj = json.loads(gzip.open(p, "rb").read().decode("utf-8"))
        except Exception:                                    # noqa: BLE001
            continue
        cik = str(obj.get("cik") or "").lstrip("0")
        if not cik:
            continue
        hist.add(cik)
        blocks = [(obj.get("filings") or {}).get("recent") or {}]
        blocks += list(obj.get("_r63_older_blocks") or [])
        for b in blocks:
            fs = b.get("form") or []
            an = b.get("accessionNumber") or []
            ad = b.get("acceptanceDateTime") or []
            for i, f in enumerate(fs):
                if str(f) not in OWNERSHIP_FORMS or i >= len(an) or i >= len(ad) or not ad[i]:
                    continue
                acc = CBD._norm_accession(an[i])
                if acc in accessions:
                    by_issuer[(cik, acc)] = str(ad[i])
                    by_acc[acc].add(str(ad[i]))
        if verbose and k % 250 == 0:
            print("  acceptance %d/%d matched=%d" % (k, len(files), len(by_issuer)), flush=True)
    return {"by_issuer": by_issuer, "by_accession": by_acc, "history_ciks": hist}


def join_acceptance(rows: list, acc: dict) -> Counter:
    """Attach the availability instant's source to each row, in preference
    order. A second history holding the same accession is used only when it
    names ONE instant; otherwise the conservative filing-date bound applies."""
    src: Counter = Counter()
    for r in rows:
        utc = acc["by_issuer"].get((r["issuer_cik"], r["accession"]))
        how = SRC_ISSUER_HISTORY
        if utc is None:
            cands = acc["by_accession"].get(r["accession"]) or set()
            if len(cands) == 1:
                utc, how = next(iter(cands)), SRC_OTHER_HISTORY
            else:
                how = SRC_FILING_DATE_BOUND
        r["acceptance_utc"] = utc or ""
        r["acceptance_source"] = how
        src[how] += 1
    return src


def build_stream(ciks: set | None, *, verbose: bool = True) -> dict:
    """Parse every owned archive, join the instant, write the stream. Local only."""
    counters: Counter = Counter()
    rows: list = []
    qs = archives()
    for q, path in qs.items():
        rows += parse_archive(path, ciks, counters, quarter=q)
        if verbose:
            print("  %s purchase rows=%d" % (q, len(rows)), flush=True)
    acc = acceptance_for({r["accession"] for r in rows}, verbose=verbose)
    src = join_acceptance(rows, acc)
    rows.sort(key=lambda r: (r["filing_date"] or "", r["issuer_cik"], r["accession"]))
    data_dir().mkdir(parents=True, exist_ok=True)
    stream_path().write_text("\n".join(json.dumps(r, sort_keys=True) for r in rows),
                             encoding="utf-8")
    summary = {"calculation_owner": CALCULATION_OWNER, "built_at": now_iso(),
               "archives": {q: str(p) for q, p in qs.items()},
               "quarters": len(qs), "first_quarter": min(qs) if qs else None,
               "last_quarter": max(qs) if qs else None,
               "observable_through": observable_through(qs),
               "purchase_rows": len(rows), "counters": dict(counters),
               "acceptance_source": dict(src),
               "submissions_histories": len(acc["history_ciks"]),
               "paid_dollars": 0}
    (data_dir() / "stream_summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True),
                                                   encoding="utf-8")
    return summary


def load_stream() -> list:
    p = stream_path()
    if not p.exists():
        return []
    return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]


def load_stream_summary() -> dict:
    p = data_dir() / "stream_summary.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


# --------------------------------------------------------------------------- #
# The availability instant against the session calendar
# --------------------------------------------------------------------------- #
def availability_instant(row: dict) -> datetime | None:
    """The Eastern wall-clock instant the filing became public.

    The SEC acceptance instant where a stored history holds it; otherwise
    17:30 Eastern on the filing date, which EDGAR's cutoff makes an UPPER bound
    on the true instant - later, never earlier.
    """
    if row.get("acceptance_utc"):
        et = CBD.acceptance_et(row["acceptance_utc"])
        if et is not None:
            return et
    fd = row.get("filing_date")
    if not fd:
        return None
    return datetime.fromisoformat(fd) + timedelta(hours=EDGAR_CUTOFF_ET[0],
                                                  minutes=EDGAR_CUTOFF_ET[1])


def decision_session(dates64: np.ndarray, row: dict) -> int | None:
    """The FIRST session whose 16:00 Eastern close is strictly after the
    availability instant. Refuses an instant before the calendar's first
    session rather than stacking it on day one."""
    if row.get("acceptance_utc") and CBD.acceptance_et(row["acceptance_utc"]) is not None:
        return E8D.decision_session(dates64, row["acceptance_utc"])
    fd = row.get("filing_date")
    if not fd:
        return None
    day = np.datetime64(fd, "D")
    if day.astype("datetime64[ns]") < dates64[0]:
        return None
    # 17:30 is after the 16:00 close, so the first eligible close is the next day's
    pos = int(np.searchsorted(dates64, (day + np.timedelta64(1, "D")).astype("datetime64[ns]"),
                              side="left"))
    return pos if pos < len(dates64) else None


# --------------------------------------------------------------------------- #
# Census - reads the eligibility mask and the calendar only
# --------------------------------------------------------------------------- #
def _quantiles(vals: list, qs=(0.5, 0.75, 0.9, 0.95, 0.99)) -> dict:
    if not vals:
        return {}
    a = np.sort(np.asarray(vals, dtype=float))
    return {str(q): float(a[int(q * (len(a) - 1))]) for q in qs}


def census(E: dict, elig: np.ndarray, rows: list, *, cik2rows: dict | None = None,
           id_meta: dict | None = None, acquisition: dict | None = None,
           stream_summary: dict | None = None, verbose: bool = True) -> dict:
    """The Phase-1 census. No return, no price."""
    from . import insider_form4_events as EV

    dates64 = np.asarray(E["dates"], dtype="datetime64[ns]")
    n_t = len(dates64)
    if cik2rows is None:
        cik2rows, id_meta = CBE.identity_map(E)
    syms = [str(s) for s in np.asarray(E["symbols"])]
    delisted = np.array([bool(CBE._DELISTED.match(s)) for s in syms])
    identified = np.zeros(len(syms), dtype=bool)
    for rr in cik2rows.values():
        identified[list(rr)] = True

    # ---- the purchase stream -------------------------------------------------
    doc, flag, di, tcls, t10, swap, src = (Counter() for _ in range(7))
    roles, lateness, by_year = Counter(), [], defaultdict(Counter)
    for r in rows:
        y = (r.get("filing_date") or "0000")[:4]
        doc[r.get("document_type")] += 1
        flag[r.get("acquired_disposed") or "BLANK"] += 1
        di[r.get("direct_indirect") or "BLANK"] += 1
        tcls[r.get("title_class")] += 1
        swap[str(r.get("equity_swap"))] += 1
        src[r.get("acceptance_source")] += 1
        t10["%s:%s" % (y, r.get("rule_10b5_1"))] += 1
        own = r.get("owners") or []
        od = any(o.get("director") or o.get("officer") for o in own)
        ten = any(o.get("ten_percent_owner") for o in own)
        roles["OFFICER_OR_DIRECTOR" if od else "TEN_PERCENT_OWNER_ONLY" if ten
              else "OTHER_ONLY" if own else "NO_OWNER_ROW"] += 1
        inst = availability_instant(r)
        if inst is not None and r.get("trans_date"):
            lateness.append((inst.date() - date.fromisoformat(r["trans_date"])).days)
        by_year[y]["purchase_rows"] += 1
    stream = {
        "purchase_rows": len(rows),
        "accessions": len({(r["issuer_cik"], r["accession"]) for r in rows}),
        "issuers": len({r["issuer_cik"] for r in rows}),
        "first_filing_date": min((r["filing_date"] for r in rows if r.get("filing_date")), default=None),
        "last_filing_date": max((r["filing_date"] for r in rows if r.get("filing_date")), default=None),
        "observable_through": (stream_summary or {}).get("observable_through") or observable_through(),
        "document_type": dict(doc), "acquired_disposed": dict(flag),
        "direct_indirect": dict(di), "title_class": dict(tcls), "equity_swap": dict(swap),
        "acceptance_source": dict(src),
        "acceptance_instant_share": (src.get(SRC_ISSUER_HISTORY, 0) + src.get(SRC_OTHER_HISTORY, 0))
        / max(len(rows), 1),
        "rule_10b5_1_by_year": dict(sorted(t10.items())),
        "reporter_roles": dict(roles),
        "publication_lag_days_quantiles": _quantiles(lateness),
        "publication_lag_over_30_days_share": float(np.mean([x > 30 for x in lateness])) if lateness else None,
        "price_field_present_share": float(np.mean([bool(r.get("price_present")) for r in rows])) if rows else None,
        "value_field_rule": VALUE_FIELD_RULE,
        "counters": (stream_summary or {}).get("counters"),
    }

    # ---- the frozen cell, built by its one owner -------------------------------
    built = EV.build_events(E, elig, rows=rows, cik2rows=cik2rows, verbose=verbose)
    ev = built["events"][EV.CELL_CLUSTER]
    ts = [e["t"] for e in ev]
    lock_t = int(np.searchsorted(dates64, np.datetime64(pit.LOCKBOX_START, "D").astype("datetime64[ns]")))
    last_elig = np.full(elig.shape[0], -1, dtype=int)
    for i in range(elig.shape[0]):
        w = np.where(elig[i])[0]
        if len(w):
            last_elig[i] = int(w[-1])
    exits = [int(last_elig[e["row"]] - e["t"] <= 63 and last_elig[e["row"]] < n_t - 6) for e in ev]
    klass = Counter(E8D.session_class(dates64, e["acceptance_utc"]) if e.get("acceptance_utc")
                    else "FILING_DATE_BOUND" for e in ev)
    dec21 = pit.decision_indices(np.asarray(E["dates"]), EQUITY_DISCOVERY_START, 21, 21)
    tsa = np.asarray(ts, dtype=int)
    per_period = [int(((tsa > t - 21) & (tsa <= t)).sum()) for t in np.asarray(dec21, dtype=int)]
    pp = np.asarray(per_period) if per_period else np.zeros(1)
    cell = {
        "cell": EV.CELL_CLUSTER, "spec": EV.CELL_SPECS[EV.CELL_CLUSTER],
        "build_stats": built["stats"],
        "eligible_events": len(ev),
        "first_event": ev[0]["date"] if ev else None, "last_event": ev[-1]["date"] if ev else None,
        "unique_issuers": len({e["issuer_cik"] for e in ev}),
        "unique_panel_rows": len({e["row"] for e in ev}),
        "events_on_delisted_rows": int(sum(delisted[e["row"]] for e in ev)),
        "unique_delisted_rows_with_event": len({e["row"] for e in ev if delisted[e["row"]]}),
        "events_by_year": dict(sorted(Counter(e["date"][:4] for e in ev).items())),
        "selection_events": int(sum(1 for t in ts if t < lock_t)),
        "lockbox_events": int(sum(1 for t in ts if t >= lock_t)),
        "unique_decision_sessions": len(set(ts)),
        "distinct_insiders_at_completion": dict(sorted(Counter(min(e["distinct_insiders"], 5)
                                                               for e in ev).items())),
        "completing_filing_session_class": dict(klass),
        "leaves_eligible_universe_within_63_sessions": float(np.mean(exits)) if exits else None,
        "events_per_grid_period_h21": {"mean": float(pp.mean()), "median": float(np.median(pp)),
                                       "share_periods_with_event": float((pp > 0).mean()),
                                       "periods": int(len(per_period))},
        "oos_sufficiency": E8D.oos_sufficiency(E["dates"], ts, horizons=(5, 21, 63)),
    }

    # ---- identity and survivorship --------------------------------------------
    coverage = CBE.coverage_report(E, elig, cik2rows, dec21)
    coverage.pop("per_session", None)
    el = elig[:, np.asarray(dec21, dtype=int)]
    dl = delisted[:, None] & el
    survivorship = {
        "panel_rows": len(syms), "delisted_rows": int(delisted.sum()),
        "delisted_rows_identified": int((delisted & identified).sum()),
        "live_rows_identified": int((~delisted & identified).sum()),
        "eligible_delisted_row_sessions_identified_share":
            float((dl & identified[:, None]).sum() / max(dl.sum(), 1)),
        "eligible_live_row_sessions_identified_share":
            float(((~delisted[:, None] & el) & identified[:, None]).sum()
                  / max((~delisted[:, None] & el).sum(), 1)),
    }

    # ---- the data end on each frozen grid ----------------------------------------
    end = stream["observable_through"]
    end_ix = int(np.searchsorted(dates64, np.datetime64(end, "D").astype("datetime64[ns]"),
                                 side="right")) if end else n_t
    data_end = {"observable_through": end, "first_unobservable_session":
                str(dates64[end_ix])[:10] if end_ix < n_t else None,
                "decision_sessions_after_data_end": {}}
    for h in (5, 21, 63):
        cad = int(min(h, 21))
        dec = pit.decision_indices(np.asarray(E["dates"]), EQUITY_DISCOVERY_START, cad, h)
        data_end["decision_sessions_after_data_end"][str(h)] = int((np.asarray(dec) >= end_ix).sum())

    # ---- CELL B feasibility: measured, then declined -----------------------------
    qual = [r for r in rows if EV.exclusion_reason(r) is None]
    pre = [r["shares_owned_following"] - r["shares"] for r in qual
           if r.get("shares_owned_following") is not None and r.get("shares") is not None]
    cell_b = {
        "insider_holdings_denominator": {
            "field": "SHRS_OWND_FOLWNG_TRANS",
            "populated_share": float(np.mean([r.get("shares_owned_following") is not None
                                              for r in qual])) if qual else None,
            "scope": "one ownership LINE (direct, or one indirect nature) - not the "
                     "insider's total beneficial ownership, and excluding derivatives",
            "indirect_line_share": float(np.mean([r.get("direct_indirect") == "I" for r in qual]))
            if qual else None,
            "pre_trade_line_holding_nonpositive_share": float(np.mean([p <= 0 for p in pre]))
            if pre else None,
        },
        "issuer_market_value_denominator": {
            "numerator": "TRANS_SHARES x TRANS_PRICEPERSHARE", "estate_rule": VALUE_FIELD_RULE},
        "threshold_in_law_or_regulation": None,
    }

    return {
        "schema": "alpha_recovery_insider_form4_census/1",
        "calculation_owner": CALCULATION_OWNER, "generated_at": now_iso(),
        "returns_computed": False, "prices_read": False, "paid_dollars": 0,
        "source": {"transactions": "SEC Insider Transactions Data Sets (form345), owned R35 acquisition",
                   "availability": "EDGAR submissions histories (acceptanceDateTime), acquired by "
                                   "alpha_agent.r63.acquire"},
        "acquisition": acquisition, "stream_summary": {k: v for k, v in (stream_summary or {}).items()
                                                       if k != "counters"},
        "panel": {"first_session": str(dates64[0])[:10], "last_session": str(dates64[-1])[:10],
                  "discovery_start": EQUITY_DISCOVERY_START, "lockbox_start": pit.LOCKBOX_START},
        "identity": {**(id_meta or {}), "ciks": len(cik2rows)},
        "coverage": coverage, "survivorship": survivorship, "data_end": data_end,
        "stream": stream, "cell": cell, "cell_b_feasibility": cell_b,
    }


def run_census(*, verbose: bool = True, write: bool = True) -> dict:
    from . import incumbent as INC
    E, elig = INC.equity_substrate()
    acquisition = acquire_missing(verbose=verbose)
    cik2rows, id_meta = CBE.identity_map(E)
    summary = build_stream(set(cik2rows), verbose=verbose)
    body = census(E, elig, load_stream(), cik2rows=cik2rows, id_meta=id_meta,
                  acquisition=acquisition, stream_summary=summary, verbose=verbose)
    if write:
        write_artifact(CENSUS_ARTIFACT, body)
    return body
