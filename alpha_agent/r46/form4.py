"""alpha_agent.r46.form4 - the daily SEC Form-4 insider-flow feed. Free, PIT-stamped.

An insider who buys or sells stock in the company they run must file a Form 4
with the SEC within two business days. EDGAR publishes every one, stamps it
with the second it was accepted, and lists the day's filings in a daily form
index. That is a genuinely new information family for this estate - informed
trading, distinct from price, positioning, credit and macro - and it is free.

**What is captured.** For each business day since the last complete capture
(bounded per run), the daily form index and the FULL submission text of every
Form 4 on it, stored gzip-compressed under the Release-46 research root with
the acquisition instant and sha256. Never overwritten: a partial day (index
read before EDGAR closed for the day) is re-captured later and deduplicated by
accession number.

**What is parsed.** The SEC-HEADER ``ACCEPTANCE-DATETIME`` (the point-in-time
key), the issuer (CIK, name, trading symbol), the reporting owner and their
relationship (director / officer / ten-percent owner / other, with title),
and every NON-DERIVATIVE transaction: date, code, shares, price, acquired or
disposed, shares owned after, direct or indirect. Derivative rows are counted,
not traded.

**Not all Form 4s are equal.** Transaction codes are CLASSIFIED and only
open-market purchases (``P``) and open-market sales (``S``) carry information
for the frozen challengers. Grants (``A``), option exercises (``M``), tax
withholding (``F``), gifts (``G``) and the administrative codes are recorded
and excluded by name.

**Point in time.** A transaction is admissible at an emission instant only if
its filing was accepted before that instant AND sits in a capture acquired
before it. The transaction date (which may be up to two business days earlier)
is never used as the observation instant.

Two bounded, economically motivated challengers are frozen in
:mod:`alpha_agent.r46.challengers` before their first emission: cluster
buying (several distinct insiders buying the same name in the open market
within a month) and the cross-sectional net-purchase ratio. No threshold was
swept; the constants are the literature's.
"""
from __future__ import annotations

import datetime as _dt
import gzip
import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import sec as SEC

CALCULATION_OWNER = "alpha_agent.r46.form4"

ARTIFACT = "R46_5_FORM4_LANE.json"
RAW_DIRNAME = "_data_form4"
MANIFEST_NAME = "form4_captures.json"

DAILY_INDEX_URL = ("https://www.sec.gov/Archives/edgar/daily-index/%d/QTR%d/"
                   "form.%s.idx")
ARCHIVE_URL = "https://www.sec.gov/Archives/%s"

#: Bounded per run: business days back from the last complete capture, and
#: filings fetched. A truncated day is recorded as such and resumed next run.
MAX_DAYS_PER_RUN = 3
MAX_FILINGS_PER_RUN = 2400
#: Wall-clock budget for ONE acquisition. The daily research cycle calls this
#: lane, and a cycle must never be held hostage by a third-party feed: when the
#: budget is spent the run stops cleanly, the day in progress is marked
#: incomplete, and the next run resumes from the per-day checkpoint without
#: re-fetching an accession it already holds. Bounded, resumable, and stated.
MAX_SECONDS_PER_RUN = 600
#: EDGAR accepts filings until 22:00 ET; a day captured before that is partial.
DAY_COMPLETE_AFTER_ET = _dt.time(22, 15)
FIRST_RUN_LOOKBACK_BUSINESS_DAYS = 3

#: SEC transaction codes -> economic class. Only P and S carry information
#: for the frozen challengers; everything else is recorded and excluded.
TRANSACTION_CLASSES = {
    "P": "OPEN_MARKET_PURCHASE",
    "S": "OPEN_MARKET_SALE",
    "A": "GRANT_AWARD",
    "M": "OPTION_EXERCISE",
    "F": "TAX_WITHHOLDING",
    "G": "GIFT",
    "C": "CONVERSION",
    "D": "DISPOSITION_TO_ISSUER",
    "X": "OPTION_EXERCISE_IN_THE_MONEY",
    "J": "OTHER_ADMINISTRATIVE",
    "W": "WILL_OR_INHERITANCE",
    "I": "DISCRETIONARY_PLAN",
    "L": "SMALL_ACQUISITION",
    "U": "TENDER_OR_MERGER",
    "Z": "VOTING_TRUST",
    "K": "EQUITY_SWAP",
    "H": "EXPIRATION_LONG",
    "O": "OUT_OF_THE_MONEY_EXERCISE",
    "E": "EXPIRATION_SHORT",
}
CLASS_OTHER = "OTHER_ADMINISTRATIVE"
INFORMATIVE_CODES = ("P", "S")

_IDX_LINE = re.compile(r"^(\S+)\s+(.*?)\s+(\d+)\s+(\d{8})\s+(edgar/data/\S+)\s*$")
_ACCEPT = re.compile(r"ACCEPTANCE-DATETIME>?\s*:?\s*(\d{14})")
_XML = re.compile(r"<XML>\s*(.*?)\s*</XML>", re.S | re.I)
_ACCESSION = re.compile(r"(\d{10}-\d{2}-\d{6})")


def raw_dir() -> Path:
    from . import RESEARCH_ROOT as _root
    return Path(_root) / RAW_DIRNAME


def manifest_path() -> Path:
    return raw_dir() / MANIFEST_NAME


def _manifest() -> dict:
    return read_json(manifest_path(), default=None) or {
        "schema": "r46_5_form4_captures/1", "captures": []}


def classify_code(code: str) -> str:
    return TRANSACTION_CLASSES.get(str(code or "").strip().upper(), CLASS_OTHER)


def norm_ticker(t: str) -> str:
    return str(t or "").strip().upper().replace("-", ".").replace("/", ".")


# --------------------------------------------------------------------------- #
# Parsing - pure functions over bytes
# --------------------------------------------------------------------------- #
def parse_daily_index(text: str) -> list:
    """Form 4 rows of a daily form index: (form, name, cik, date, file)."""
    out = []
    for line in text.splitlines():
        m = _IDX_LINE.match(line.strip())
        if not m:
            continue
        form = m.group(1)
        if form != "4":
            continue
        out.append({"form": form, "company": m.group(2).strip(),
                    "cik": m.group(3), "date_filed": m.group(4),
                    "file": m.group(5)})
    return out


def _text(node, path: str) -> Optional[str]:
    if node is None:
        return None
    el = node.find(path)
    if el is None:
        return None
    v = (el.text or "").strip()
    if not v:
        sub = el.find("value")
        v = (sub.text or "").strip() if sub is not None else ""
    return v or None


def _flag(node, path: str) -> bool:
    v = _text(node, path)
    return str(v or "").strip().lower() in ("1", "true", "yes")


def parse_submission_text(text: str) -> dict:
    """Acceptance instant, issuer, owners and non-derivative transactions."""
    m = _ACCEPT.search(text)
    accepted = None
    if m:
        s = m.group(1)
        try:
            et = _dt.datetime(int(s[0:4]), int(s[4:6]), int(s[6:8]),
                              int(s[8:10]), int(s[10:12]), int(s[12:14]),
                              tzinfo=CK._ET)
            accepted = CK.iso(et.astimezone(_dt.timezone.utc))
        except (ValueError, TypeError):
            accepted = None
    acc_m = _ACCESSION.search(text[:400])
    accession = acc_m.group(1) if acc_m else None
    xm = _XML.search(text)
    if not xm:
        return {"accession": accession, "accepted_at_utc": accepted,
                "parsed": False, "why": "no XML document"}
    try:
        root = ET.fromstring(xm.group(1))
    except ET.ParseError:
        return {"accession": accession, "accepted_at_utc": accepted,
                "parsed": False, "why": "XML parse error"}
    issuer = root.find("issuer")
    owners = []
    for ro in root.findall("reportingOwner"):
        rel = ro.find("reportingOwnerRelationship")
        owners.append({
            "cik": _text(ro, "reportingOwnerId/rptOwnerCik"),
            "name": _text(ro, "reportingOwnerId/rptOwnerName"),
            "is_director": _flag(rel, "isDirector"),
            "is_officer": _flag(rel, "isOfficer"),
            "is_ten_percent_owner": _flag(rel, "isTenPercentOwner"),
            "is_other": _flag(rel, "isOther"),
            "officer_title": _text(rel, "officerTitle"),
        })
    txs = []
    for tx in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
        code = _text(tx, "transactionCoding/transactionCode")
        shares = _text(tx, "transactionAmounts/transactionShares")
        price = _text(tx, "transactionAmounts/transactionPricePerShare")
        ad = _text(tx, "transactionAmounts/transactionAcquiredDisposedCode")
        txs.append({
            "security_title": _text(tx, "securityTitle"),
            "transaction_date": _text(tx, "transactionDate"),
            "transaction_code": code,
            "transaction_class": classify_code(code),
            "shares": _num(shares),
            "price_per_share": _num(price),
            "acquired_disposed": ad,
            "direction": ("BUY" if ad == "A" else "SELL" if ad == "D"
                          else None),
            "shares_owned_following": _num(_text(
                tx, "postTransactionAmounts/sharesOwnedFollowingTransaction")),
            "ownership": _text(tx, "ownershipNature/directOrIndirectOwnership"),
            "is_informative": str(code or "").upper() in INFORMATIVE_CODES,
        })
    n_deriv = len(root.findall("derivativeTable/derivativeTransaction"))
    return {
        "accession": accession,
        "accepted_at_utc": accepted,
        "parsed": True,
        "issuer_cik": _text(issuer, "issuerCik"),
        "issuer_name": _text(issuer, "issuerName"),
        "issuer_ticker": norm_ticker(_text(issuer, "issuerTradingSymbol")),
        "period_of_report": _text(root, "periodOfReport"),
        "owners": owners,
        "n_owners": len(owners),
        "transactions": txs,
        "n_non_derivative": len(txs),
        "n_derivative": n_deriv,
    }


def _num(v) -> Optional[float]:
    try:
        f = float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None
    return f


# --------------------------------------------------------------------------- #
# Acquisition
# --------------------------------------------------------------------------- #
def _trailing_business_days(cutoff: _dt.date, n: int) -> list:
    out, d = [], cutoff
    while len(out) < int(n):
        if d.weekday() not in CK.WEEKEND:
            out.append(str(d))
        d -= _dt.timedelta(days=1)
    return out


def _business_days(start: _dt.date, end: _dt.date) -> list:
    out, d = [], start
    while d <= end:
        if d.weekday() not in CK.WEEKEND:
            out.append(d)
        d += _dt.timedelta(days=1)
    return out


def _day_is_complete(day: _dt.date, now: _dt.datetime) -> bool:
    et = CK.to_eastern(now)
    return et.date() > day or (et.date() == day
                               and et.time() >= DAY_COMPLETE_AFTER_ET)


def _days_to_capture(today: _dt.date, now: _dt.datetime, max_days: int,
                     since: _dt.date = None) -> list:
    """Business days still owed a complete capture, oldest first.

    ``since`` lets an operator ask the OWNER for a bounded historical
    backfill rather than reaching around it. Backfilling is not look-ahead:
    a filing's acceptance instant is stamped by EDGAR and is immutable, so
    capturing it late changes when it becomes READABLE here, never when it
    became public. The point-in-time gate still refuses any capture taken
    after the instant a signal is read at.
    """
    caps = _manifest().get("captures") or []
    complete = {str(c.get("day")) for c in caps if c.get("complete")}
    if since is not None:
        start = since
    elif caps:
        last = max(str(c.get("day")) for c in caps)
        start = _dt.date.fromisoformat(last)
        if last in complete:
            start += _dt.timedelta(days=1)
    else:
        start = today
        for _ in range(FIRST_RUN_LOOKBACK_BUSINESS_DAYS - 1):
            start -= _dt.timedelta(days=1)
            while start.weekday() in CK.WEEKEND:
                start -= _dt.timedelta(days=1)
    days = [d for d in _business_days(start, today) if str(d) not in complete]
    return days[:max_days]


def backfill_start(cutoff: _dt.date, n_sessions: int) -> _dt.date:
    """The oldest business day a declared ``n_sessions`` window reaches."""
    return _dt.date.fromisoformat(_trailing_business_days(cutoff,
                                                          n_sessions)[-1])


def acquire(*, acquire: bool = True, today: _dt.date = None,
            max_days: int = MAX_DAYS_PER_RUN,
            budget_filings: int = MAX_FILINGS_PER_RUN,
            budget_seconds: float = MAX_SECONDS_PER_RUN,
            since: _dt.date = None,
            now: _dt.datetime = None) -> dict:
    now = now or CK.now_utc()
    today = today or CK.eastern_date(now)
    raw_dir().mkdir(parents=True, exist_ok=True)
    man = _manifest()
    caps = list(man.get("captures") or [])
    ua = SEC.user_agent()
    if not ua:
        return {"state": SEC.BLOCKED_NO_CONTACT, "n_captures": len(caps),
                "money_spent_usd": 0.0}
    if not acquire:
        return {"state": "NOT_ACQUIRED", "n_captures": len(caps),
                "money_spent_usd": 0.0}
    days = _days_to_capture(today, now, max_days, since)
    seen_acc = {a for c in caps for a in (c.get("accessions") or ())}
    fetched, results = 0, []
    started_at = time.monotonic()
    out_of_time = [False]

    def _spent():
        # ``None`` means unbounded; ZERO means no time at all. Guarding on
        # truthiness would read a fully spent budget as no budget - the
        # Release 46 lesson that a spent budget must be able to buy nothing.
        if budget_seconds is not None and \
                (time.monotonic() - started_at) >= float(budget_seconds):
            out_of_time[0] = True
        return out_of_time[0]
    for day in days:
        if _spent():
            break
        q = (day.month - 1) // 3 + 1
        url = DAILY_INDEX_URL % (day.year, q, day.strftime("%Y%m%d"))
        res = SEC.get(url)
        rec = {"day": str(day), "index_url": url}
        if res["status"] == 404:
            rec.update(state="NO_INDEX_FOR_DAY", n_form4=0)
            results.append(rec)
            continue
        if not res["body"]:
            rec.update(state="INDEX_FETCH_FAILED", error=res["error"])
            results.append(rec)
            continue
        rows = parse_daily_index(res["body"].decode("latin-1", "replace"))
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        idx_path = raw_dir() / ("form_index_%s_%s.idx.gz" % (day, stamp))
        idx_path.write_bytes(gzip.compress(res["body"]))
        raw_path = raw_dir() / ("form4_raw_%s_%s.jsonl.gz" % (day, stamp))
        parsed_rows, accessions, errors, truncated = [], [], [], False
        with gzip.open(raw_path, "wt", encoding="utf-8") as fh:
            for r in rows:
                acc_m = _ACCESSION.search(r["file"])
                acc = acc_m.group(1) if acc_m else r["file"]
                if acc in seen_acc:
                    continue
                if fetched >= budget_filings or _spent():
                    truncated = True
                    break
                got = SEC.get(ARCHIVE_URL % r["file"])
                fetched += 1
                if not got["body"]:
                    errors.append({"file": r["file"], "error": got["error"]})
                    continue
                text = got["body"].decode("utf-8", "replace")
                fh.write(json.dumps({"file": r["file"], "accession": acc,
                                     "sha256": SEC.sha256(got["body"]),
                                     "text": text}) + "\n")
                p = parse_submission_text(text)
                p.update({"file": r["file"], "accession": acc,
                          "index_cik": r["cik"], "index_company": r["company"],
                          "date_filed": r["date_filed"]})
                parsed_rows.append(p)
                accessions.append(acc)
                seen_acc.add(acc)
        parsed_path = raw_dir() / ("form4_rows_%s_%s.json" % (day, stamp))
        write_json(parsed_path, {"schema": "r46_5_form4_rows/1", "day": str(day),
                                 "acquired_at_utc": CK.iso(now),
                                 "n_filings": len(parsed_rows),
                                 "filings": parsed_rows})
        complete = _day_is_complete(day, now) and not truncated
        cap = {"day": str(day), "index_path": str(idx_path),
               "index_sha256": SEC.sha256(res["body"]),
               "raw_path": str(raw_path), "parsed_path": str(parsed_path),
               "n_form4_on_index": len(rows), "n_fetched": len(parsed_rows),
               "n_parsed": sum(1 for p in parsed_rows if p.get("parsed")),
               "n_errors": len(errors), "errors": errors[:20],
               "accessions": accessions, "truncated": truncated,
               "complete": complete,
               "acquired_at_utc": CK.iso(now),
               "acquired_at_utc_precise": CK.iso_precise(now),
               "licence": "SEC EDGAR, public domain"}
        caps.append(cap)
        rec.update({k: v for k, v in cap.items()
                    if k not in ("accessions", "errors")})
        rec["state"] = "CAPTURED"
        results.append(rec)
        # CHECKPOINT PER DAY. A multi-week backfill that only manifested its
        # captures at the end would lose every completed day to one
        # interruption, leaving the raw files on disk unreferenced - present,
        # paid for, and invisible to every reader. Each day is durable the
        # moment it is complete, and the next run resumes from it.
        man["captures"] = caps
        man["n_captures"] = len(caps)
        man["updated_at_utc"] = CK.iso(CK.now_utc())
        write_json(manifest_path(), man)
        if truncated:
            break
    man["captures"] = caps
    man["n_captures"] = len(caps)
    man["updated_at_utc"] = CK.iso(now)
    write_json(manifest_path(), man)
    return {"state": "CAPTURED" if fetched else "NOTHING_NEW",
            "days_considered": [str(d) for d in days], "results": results,
            "n_filings_fetched": fetched, "budget_filings": budget_filings,
            "budget_seconds": budget_seconds,
            "seconds_spent": round(time.monotonic() - started_at, 1),
            "time_budget_exhausted": out_of_time[0],
            "resumable": True,
            "max_days": max_days, "since": (str(since) if since else None),
            "n_captures": len(caps),
            "user_agent_contact_masked": SEC.mask(SEC.contact()),
            "money_spent_usd": 0.0}


# --------------------------------------------------------------------------- #
# Point-in-time reads
# --------------------------------------------------------------------------- #
def covered_days(as_of_instant: _dt.datetime = None,
                 pit: bool = True) -> set:
    """Business days whose Form-4 feed is COMPLETELY captured.

    A day is complete when its index was read after EDGAR closed for that day
    and every filing on it was fetched. A partial day is never counted: a
    cluster-buy signal computed over a window half of which was never captured
    is not the declared signal, and reporting it as one would be the first
    quiet lie in the chain.
    """
    now = as_of_instant or CK.now_utc()
    cut = CK.iso_precise(now)
    out = set()
    for c in _manifest().get("captures") or ():
        if not c.get("complete"):
            continue
        if pit and str(c.get("acquired_at_utc_precise")
                       or c.get("acquired_at_utc")) >= cut:
            continue
        out.add(str(c.get("day")))
    return out


def held_transactions() -> list:
    """Every transaction the captures HOLD, with no point-in-time filter.

    A COVERAGE fact, reported separately from :func:`transactions`. On the run
    that performs the acquisition the two necessarily disagree - a capture
    cannot precede the instant it was taken - and reporting only the second
    would make a successful capture read as an empty one.
    """
    out, seen = [], set()
    for c in _manifest().get("captures") or ():
        body = read_json(c.get("parsed_path"), default=None) or {}
        for f in body.get("filings") or ():
            if not f.get("parsed") or f.get("accession") in seen:
                continue
            seen.add(f.get("accession"))
            for tx in f.get("transactions") or ():
                out.append({"issuer_cik": f.get("issuer_cik"),
                            "issuer_ticker": f.get("issuer_ticker"),
                            "transaction_class": tx.get("transaction_class"),
                            "is_informative": bool(tx.get("is_informative"))})
    return out


def window_coverage(sessions, as_of_instant: _dt.datetime = None) -> dict:
    """Is every session of a declared window covered by a complete capture?"""
    want = [str(s) for s in sessions]
    have = covered_days(as_of_instant)
    missing = [s for s in want if s not in have]
    return {"n_sessions": len(want), "n_covered": len(want) - len(missing),
            "missing": missing[:20], "n_missing": len(missing),
            "complete": not missing,
            "rule": "a declared window emits only when EVERY session in it "
                    "is covered by a complete daily capture"}


def transactions(as_of_instant: _dt.datetime = None,
                 informative_only: bool = False) -> list:
    """Non-derivative transactions accepted before ``as_of_instant`` from
    captures acquired before it. One row per transaction."""
    now = as_of_instant or CK.now_utc()
    cut = CK.iso_precise(now)
    out, seen = [], set()
    for c in _manifest().get("captures") or ():
        if str(c.get("acquired_at_utc_precise") or c.get("acquired_at_utc")) \
                >= cut:
            continue
        body = read_json(c.get("parsed_path"), default=None) or {}
        for f in body.get("filings") or ():
            if not f.get("parsed") or not f.get("accepted_at_utc"):
                continue
            if str(f["accepted_at_utc"]) >= CK.iso(now):
                continue
            if f["accession"] in seen:
                continue
            seen.add(f["accession"])
            owners = f.get("owners") or []
            role = ("OFFICER" if any(o.get("is_officer") for o in owners)
                    else "DIRECTOR" if any(o.get("is_director") for o in owners)
                    else "TEN_PERCENT_OWNER" if any(
                        o.get("is_ten_percent_owner") for o in owners)
                    else "OTHER")
            for tx in f.get("transactions") or ():
                if informative_only and not tx.get("is_informative"):
                    continue
                out.append({
                    "accession": f["accession"],
                    "accepted_at_utc": f["accepted_at_utc"],
                    "issuer_cik": f.get("issuer_cik"),
                    "issuer_ticker": f.get("issuer_ticker"),
                    "issuer_name": f.get("issuer_name"),
                    "insider_cik": (owners[0].get("cik") if owners else None),
                    "insider_name": (owners[0].get("name") if owners else None),
                    "insider_role": role,
                    "officer_title": (owners[0].get("officer_title")
                                      if owners else None),
                    "transaction_date": tx.get("transaction_date"),
                    "transaction_code": tx.get("transaction_code"),
                    "transaction_class": tx.get("transaction_class"),
                    "shares": tx.get("shares"),
                    "price_per_share": tx.get("price_per_share"),
                    "direction": tx.get("direction"),
                    "ownership": tx.get("ownership"),
                    "shares_owned_following": tx.get("shares_owned_following"),
                    "is_informative": bool(tx.get("is_informative")),
                    "captured_at_utc": c.get("acquired_at_utc"),
                })
    out.sort(key=lambda r: (str(r["accepted_at_utc"]), str(r["accession"])))
    return out


def run(*, acquire_now: bool = True, campaign_id: str = CAMPAIGN_ID,
        as_of: _dt.date = None, max_days: int = MAX_DAYS_PER_RUN,
        budget_filings: int = MAX_FILINGS_PER_RUN,
        budget_seconds: float = MAX_SECONDS_PER_RUN,
        since: _dt.date = None) -> dict:
    now = CK.now_utc()
    as_of = as_of or CK.eastern_date(now)
    acq = acquire(acquire=acquire_now, today=as_of, max_days=max_days,
                  budget_filings=budget_filings,
                  budget_seconds=budget_seconds, since=since, now=now)
    caps = _manifest().get("captures") or []
    held = held_transactions()
    admissible_now = transactions(now)
    by_class: dict = {}
    for t in held:
        by_class[t["transaction_class"]] = by_class.get(
            t["transaction_class"], 0) + 1
    days = sorted({str(c.get("day")) for c in caps})
    complete_now = covered_days(now, pit=False)
    live = bool(caps) and bool(held)
    try:
        from . import marketdata as MD
        from . import trades as TR
        anchor = MD.last_session(TR.NAV_CALENDAR_INSTRUMENT) or as_of
    except Exception:                           # noqa: BLE001 - reported by state
        anchor = as_of
    body = artifact_body(
        "r46_5_form4_lane/1", CALCULATION_OWNER,
        as_of=str(as_of),
        state=("LIVE_PROSPECTIVE" if live else
               "DATA_BLOCKED" if acq.get("state") == SEC.BLOCKED_NO_CONTACT
               else "FROZEN_PENDING_ACQUISITION"),
        acquisition=acq,
        n_captures=len(caps),
        raw_root=str(raw_dir()),
        source={"index": "EDGAR daily form index (form.YYYYMMDD.idx)",
                "filing": "full submission text per Form 4, SEC-HEADER "
                          "ACCEPTANCE-DATETIME + ownershipDocument XML",
                "licence": "SEC EDGAR, public domain, $0"},
        coverage={"days_captured": days,
                  "first_day": days[0] if days else None,
                  "last_day": days[-1] if days else None,
                  "n_complete_days": len(complete_now),
                  "complete_days": sorted(complete_now),
                  "n_filings_fetched": sum(int(c.get("n_fetched") or 0)
                                           for c in caps),
                  "n_transactions": len(held),
                  "by_transaction_class": dict(sorted(by_class.items())),
                  "n_open_market_purchases": by_class.get(
                      "OPEN_MARKET_PURCHASE", 0),
                  "n_open_market_sales": by_class.get("OPEN_MARKET_SALE", 0),
                  "n_issuers": len({t["issuer_cik"] for t in held}),
                  # A capture cannot precede the instant it was taken, so on
                  # the acquiring run this is 0 by construction.
                  "n_transactions_pit_admissible_at_this_instant":
                      len(admissible_now),
                  "captures_taken_in_this_run_are_admissible_from_the_next":
                      True},
        transaction_classes=dict(TRANSACTION_CLASSES),
        informative_codes=list(INFORMATIVE_CODES),
        point_in_time={
            "key": "SEC-HEADER ACCEPTANCE-DATETIME (ET) per filing, converted "
                   "to UTC; admissible at an emission instant only if "
                   "accepted before it AND present in a capture acquired "
                   "before it",
            "transaction_date_is_not_the_observation_instant": True,
            "partial_days_recaptured_and_deduplicated_by_accession": True,
            "not_all_form4s_are_equivalent": "grants, exercises, withholding, "
                                             "gifts and administrative codes "
                                             "are excluded by name",
        },
        # Anchored on the LAST PRINTED SESSION, which is the session the
        # challengers evaluate - not on today's calendar date. Anchoring the
        # report on `as_of` while the rule anchors on the last printed bar
        # makes the lane say a window is one session short when the rule can
        # already read all of it.
        window_anchor_session=str(anchor),
        window_anchor_rule="the last printed session of the NAV calendar; the "
                           "challengers count their declared window back from "
                           "it, so the lane reports the same window",
        window_coverage={
            "cluster_buy_21_sessions": dict(window_coverage(
                _trailing_business_days(anchor, 21), now),
                complete_ignoring_capture_instant=not [
                    s for s in _trailing_business_days(anchor, 21)
                    if s not in complete_now]),
            "net_purchase_ratio_63_sessions": dict(window_coverage(
                _trailing_business_days(anchor, 63), now),
                complete_ignoring_capture_instant=not [
                    s for s in _trailing_business_days(anchor, 63)
                    if s not in complete_now]),
            "note": "a challenger emits only when its whole declared window "
                    "is covered by complete daily captures; the lane accrues "
                    "forward and says how far it has to go",
        },
        challengers_frozen=["r46_5_insider_cluster_buy_20d",
                            "r46_5_insider_net_purchase_xs_20d"],
        information_family="INSIDER_FLOW",
        money_spent_usd=0.0, credential_written=False,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "raw_dir", "manifest_path",
           "TRANSACTION_CLASSES", "INFORMATIVE_CODES", "classify_code",
           "norm_ticker", "parse_daily_index", "parse_submission_text",
           "acquire", "backfill_start", "covered_days", "held_transactions",
           "window_coverage", "transactions", "run"]
