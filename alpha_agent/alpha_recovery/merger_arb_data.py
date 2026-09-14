r"""alpha_agent.alpha_recovery.merger_arb_data - the free data layer for the Alpha Agent
mechanism ``MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_V1``.

WHY NOT THE OWNED SUBMISSIONS STORE
    The R63 submissions histories hold 1,080 CIKs selected on S&P 500 membership, and its
    canonical ``keep_form`` drops DEFM14A, PREM14A, SC TO-T and SC 14D9 altogether. A
    merger-arbitrage universe built on it would contain only index members and would
    silently lose every small or mid-cap target. The universe here comes from EDGAR's own
    quarterly ``master`` index, which lists EVERY filer.

WHAT IS ACQUIRED ($0, public, the estate's SEC user agent and rate gate)
    1. ``full-index/<year>/QTR<q>/master.zip`` - only the merger-form rows are kept.
    2. The HEAD of each merger-form filing's complete submission text: the SEC header
       (acceptance instant, form, subject company and filer CIKs) and the opening of the
       primary document, where the consideration, the trading symbol and (for tender-offer
       schedules) the CUSIP are stated. sec.gov ignores HTTP Range on these files
       (measured 2026-09-14: a DEFM14A returned 200 with all 5,983,713 bytes), so the head
       is read from the stream and the connection is closed.

WHAT IS PARSED (from the filing itself, never from a name match)
    consideration   cash price per share; all-shares versus partial offers; any stock,
                    election, exchange-offer or contingent-value-right consideration
    identity        trading-symbol statements, tagged SELF (the company or "the Shares")
                    or COUNTERPARTY; the cover-page CUSIP of a tender-offer schedule

This module reads NO price. Returns are read only by the executor, after its data gate.

RESEARCH ONLY. No purchase, subscription, registration, promotion, capital, order or fill.
"""
from __future__ import annotations

import collections
import gzip
import html
import io
import json
import re
import threading
import time
import urllib.error
import urllib.request
import zipfile
from concurrent import futures
from pathlib import Path
from typing import Optional

from alpha_agent.r63 import acquire as ACQ

from . import now_iso, research_root
from . import control_block_data as CBD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.merger_arb_data"

#: Filed by, or indexed under, the TARGET: merger proxies and information statements
#: and the target's tender-offer recommendation.
FORMS_TARGET = ("PREM14A", "DEFM14A", "PREM14C", "DEFM14C", "SC 14D9")
#: A third-party tender offer is indexed under BOTH the subject company and the bidder;
#: the subject is read from the filing's own SEC header.
FORMS_TENDER = ("SC TO-T",)
FORMS = FORMS_TARGET + FORMS_TENDER
FIRST_QUARTER = (2002, 1)
LAST_QUARTER = (2026, 3)
MASTER_ZIP_URL = "https://www.sec.gov/Archives/edgar/full-index/%d/QTR%d/master.zip"
ARCHIVES_URL = "https://www.sec.gov/Archives/%s"
HEAD_BYTES = 262144
FETCH_WORKERS = 3
#: Flattened characters searched for terms (the cover, the letter and the summary).
TEXT_WINDOW = 90000


def data_dir() -> Path:
    return research_root() / "_data_sec_merger"


def index_path(year: int, qtr: int) -> Path:
    return data_dir() / "full_index" / ("merger_%d_q%d.tsv" % (year, qtr))


def head_path(accession: str) -> Path:
    acc = CBD._norm_accession(accession)
    return data_dir() / "heads" / acc[:10] / ("%s.gz" % acc)


def manifest_path() -> Path:
    return data_dir() / "acquisition_manifest.jsonl"


def _append_manifest(row: dict) -> None:
    p = manifest_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


def quarters(first=FIRST_QUARTER, last=LAST_QUARTER) -> list:
    out, (y, q) = [], first
    while (y, q) <= tuple(last):
        out.append((y, q))
        y, q = (y, q + 1) if q < 4 else (y + 1, 1)
    return out


# --------------------------------------------------------------------------- #
# 1. The full quarterly index
# --------------------------------------------------------------------------- #
def fetch_full_index(*, first=FIRST_QUARTER, last=LAST_QUARTER, headers: Optional[dict] = None,
                     refetch_last: bool = False, verbose: bool = True) -> dict:
    """Keep only merger-form rows: ``cik  company  form  date  path``. Cached per quarter."""
    headers = headers or CBD._headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    ok = cached = failed = kept = 0
    last_req = 0.0
    qs = quarters(first, last)
    for (y, q) in qs:
        p = index_path(y, q)
        if p.exists() and p.stat().st_size > 0 and not (refetch_last and (y, q) == tuple(last)):
            cached += 1
            continue
        wait = ACQ.MIN_INTERVAL_S - (time.time() - last_req)
        if wait > 0:
            time.sleep(wait)
        last_req = time.time()
        status, body = ACQ._get(MASTER_ZIP_URL % (y, q), headers)
        if status != 200 or not body:
            failed += 1
            _append_manifest({"at": now_iso(), "index": "%dQ%d" % (y, q), "http": status,
                              "state": "FAILED"})
            continue
        zf = zipfile.ZipFile(io.BytesIO(body))
        txt = zf.read(zf.namelist()[0]).decode("latin-1")
        rows = []
        for ln in txt.splitlines():
            parts = ln.split("|")
            if len(parts) != 5 or parts[2].strip() not in FORMS:
                continue
            rows.append("\t".join([parts[0].strip().lstrip("0"), parts[1].strip(),
                                   parts[2].strip(), parts[3].strip(), parts[4].strip()]))
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text("\n".join(rows), encoding="utf-8")
        tmp.replace(p)
        ok += 1
        kept += len(rows)
        _append_manifest({"at": now_iso(), "index": "%dQ%d" % (y, q), "http": status,
                          "bytes": len(body), "rows_kept": len(rows), "state": "OK"})
        if verbose:
            print("  full-index %dQ%d -> %d merger-form rows" % (y, q, len(rows)), flush=True)
    return {"quarters": len(qs), "fetched": ok, "cached": cached, "failed": failed,
            "rows_kept_this_run": kept}


def load_index() -> list:
    """Every merger-form index row; one row per (CIK, accession)."""
    out, seen = [], set()
    for (y, q) in quarters():
        p = index_path(y, q)
        if not p.exists():
            continue
        for ln in p.read_text(encoding="utf-8").splitlines():
            parts = ln.split("\t")
            if len(parts) != 5:
                continue
            cik, company, form, date, path = parts
            acc = CBD._norm_accession(path.rsplit("/", 1)[-1].replace(".txt", ""))
            if (cik, acc) in seen:
                continue
            seen.add((cik, acc))
            out.append({"cik": cik, "company": company, "form": form, "date": date[:10],
                        "path": path, "accession": acc})
    out.sort(key=lambda r: (r["date"], r["cik"], r["accession"]))
    return out


def index_census(rows: Optional[list] = None) -> dict:
    rows = rows if rows is not None else load_index()
    by = collections.defaultdict(collections.Counter)
    acc_by_form = collections.defaultdict(set)
    for r in rows:
        by[r["date"][:4]][r["form"]] += 1
        acc_by_form[r["form"]].add(r["accession"])
    return {"rows": len(rows), "accessions_by_form": {f: len(a) for f, a in acc_by_form.items()},
            "rows_by_year": {y: dict(c) for y, c in sorted(by.items())}}


# --------------------------------------------------------------------------- #
# 2. Filing heads
# --------------------------------------------------------------------------- #
def _get_head(url: str, headers: dict, nbytes: int) -> tuple:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=ACQ.HTTP_TIMEOUT) as resp:
            return int(resp.status), resp.read(int(nbytes))
    except urllib.error.HTTPError as exc:
        return int(exc.code), b""
    except Exception:                                    # noqa: BLE001
        return 0, b""


def fetch_heads(rows: list, *, headers: Optional[dict] = None, workers: int = FETCH_WORKERS,
                nbytes: int = HEAD_BYTES, verbose: bool = True) -> dict:
    """Store the first ``nbytes`` of each filing's complete submission text, gzipped.

    Idempotent (a head already on disk is never re-requested) and bounded by ONE shared
    rate gate, whatever the worker count.
    """
    headers = headers or CBD._headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    by_acc = {}
    for r in rows:
        by_acc.setdefault(r["accession"], r)
    pending = [r for a, r in by_acc.items() if not head_path(a).exists()]
    cached = len(by_acc) - len(pending)
    gate = CBD._RateGate(ACQ.MIN_INTERVAL_S)
    counters = {"ok": 0, "failed": 0, "bytes": 0}
    lock = threading.Lock()
    t0 = time.time()

    def _one(r: dict) -> None:
        gate.wait()
        status, body = _get_head(ARCHIVES_URL % r["path"], headers, nbytes)
        if status != 200 or not body:
            with lock:
                counters["failed"] += 1
            _append_manifest({"at": now_iso(), "accession": r["accession"], "http": status,
                              "state": "HEAD_FAILED"})
            return
        p = head_path(r["accession"])
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".%d.tmp" % threading.get_ident())
        with gzip.open(tmp, "wb") as fh:
            fh.write(body)
        tmp.replace(p)
        with lock:
            counters["ok"] += 1
            counters["bytes"] += len(body)
            n = counters["ok"]
        if verbose and n % 1000 == 0:
            print("  heads %d/%d failed=%d %.2fGB %.1f req/s" % (
                n, len(pending), counters["failed"], counters["bytes"] / 1e9,
                n / max(time.time() - t0, 1e-9)), flush=True)

    if pending:
        with futures.ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            list(pool.map(_one, pending))
    return {"accessions": len(by_acc), "fetched": counters["ok"], "cached": cached,
            "failed": counters["failed"], "bytes": counters["bytes"],
            "min_interval_s": ACQ.MIN_INTERVAL_S, "workers": int(workers), "head_bytes": nbytes}


#: The deeper read for cash-like episodes whose 256 KB head states no resolvable symbol: the
#: market-price section of a merger proxy sits further into the document.
DEEP_BYTES = 2000000


def deep_path(accession: str) -> Path:
    acc = CBD._norm_accession(accession)
    return data_dir() / "deep" / acc[:10] / ("%s.gz" % acc)


def fetch_deep(rows: list, *, headers: Optional[dict] = None, workers: int = 8,
               nbytes: int = DEEP_BYTES, verbose: bool = True) -> dict:
    """The same governed fetch as :func:`fetch_heads`, deeper, into its own store."""
    headers = headers or CBD._headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    by_acc = {}
    for r in rows:
        by_acc.setdefault(r["accession"], r)
    pending = [r for a, r in by_acc.items() if not deep_path(a).exists()]
    gate = CBD._RateGate(ACQ.MIN_INTERVAL_S)
    counters = {"ok": 0, "failed": 0, "bytes": 0}
    lock = threading.Lock()

    def _one(r: dict) -> None:
        gate.wait()
        status, body = _get_head(ARCHIVES_URL % r["path"], headers, nbytes)
        if status != 200 or not body:
            with lock:
                counters["failed"] += 1
            _append_manifest({"at": now_iso(), "accession": r["accession"], "http": status,
                              "state": "DEEP_FAILED"})
            return
        p = deep_path(r["accession"])
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".%d.tmp" % threading.get_ident())
        with gzip.open(tmp, "wb") as fh:
            fh.write(body)
        tmp.replace(p)
        with lock:
            counters["ok"] += 1
            counters["bytes"] += len(body)

    if pending:
        with futures.ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            list(pool.map(_one, pending))
    return {"accessions": len(by_acc), "fetched": counters["ok"], "cached": len(by_acc) - len(pending),
            "failed": counters["failed"], "bytes": counters["bytes"], "deep_bytes": nbytes}


def read_head(accession: str) -> Optional[str]:
    """The deepest stored read of a filing: the deep read when one exists, else the head."""
    for p in (deep_path(accession), head_path(accession)):
        if not p.exists():
            continue
        try:
            return gzip.open(p, "rb").read().decode("latin-1")
        except (OSError, EOFError):
            continue
    return None


# --------------------------------------------------------------------------- #
# 3. The SEC header
# --------------------------------------------------------------------------- #
_ACCEPT = re.compile(r"<ACCEPTANCE-DATETIME>\s*(\d{14})")
_FTYPE = re.compile(r"CONFORMED SUBMISSION TYPE:\s*([^\r\n]+)")
_FILED = re.compile(r"FILED AS OF DATE:\s*(\d{8})")
_NAME = re.compile(r"COMPANY CONFORMED NAME:\s*([^\r\n]+)")
_CIK = re.compile(r"CENTRAL INDEX KEY:\s*(\d+)")
_SECTION = re.compile(r"^(SUBJECT COMPANY|FILED BY|FILER):\s*$", re.M)


def parse_header(text: str) -> dict:
    end = text.find("</SEC-HEADER>")
    hdr = text[:end] if end > 0 else text[:20000]
    out = {"acceptance_local": None, "form": None, "filed_as_of": None,
           "subject": None, "filer": None, "filed_by": None, "header_complete": end > 0}
    m = _ACCEPT.search(hdr)
    if m:
        s = m.group(1)
        out["acceptance_local"] = "%s-%s-%sT%s:%s:%s" % (s[:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14])
    m = _FTYPE.search(hdr)
    out["form"] = m.group(1).strip() if m else None
    m = _FILED.search(hdr)
    out["filed_as_of"] = ("%s-%s-%s" % (m.group(1)[:4], m.group(1)[4:6], m.group(1)[6:8])) if m else None
    marks = [(mm.group(1), mm.start()) for mm in _SECTION.finditer(hdr)]
    for i, (label, start) in enumerate(marks):
        stop = marks[i + 1][1] if i + 1 < len(marks) else len(hdr)
        block = hdr[start:stop]
        n, c = _NAME.search(block), _CIK.search(block)
        ent = {"name": n.group(1).strip() if n else None,
               "cik": c.group(1).lstrip("0") if c else None}
        key = {"SUBJECT COMPANY": "subject", "FILED BY": "filed_by", "FILER": "filer"}[label]
        if out.get(key) is None:
            out[key] = ent
    return out


def flatten(text: str) -> str:
    """The primary document's opening as plain text."""
    end = text.find("</SEC-HEADER>")
    body = text[end:] if end > 0 else text
    body = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", body)
    body = CBD._TAG.sub(" ", body)
    body = html.unescape(body).replace("\xa0", " ")
    return CBD._WS.sub(" ", body).strip()


# --------------------------------------------------------------------------- #
# 4. Terms and identity statements, from the filing's own words
# --------------------------------------------------------------------------- #
_MONEY = r"\$\s?([0-9]{1,4}(?:,[0-9]{3})*(?:\.[0-9]{1,6})?)"
_CASH_PRICE = [
    re.compile(r"(?:right to receive|entitled to receive|will receive|would receive|to receive|receive)\s+"
               r"(?:an amount in cash equal to\s+|cash in an amount equal to\s+)?" + _MONEY +
               r"\s*(?:per share\s*|per Share\s*)?,?\s*(?:net to the (?:seller|holder)s?\s+)?in cash", re.I),
    re.compile(r"(?:at a price of|at a purchase price of|for|price of)\s+" + _MONEY +
               r"\s*(?:per (?:share|Share))?,?\s*net to the (?:seller|holder)s?\s+in cash", re.I),
    re.compile(_MONEY + r"\s*(?:per (?:share|Share)\s*)?,?\s*in cash,?\s*without interest", re.I),
]
_ALL_SHARES = re.compile(
    r"all (?:of )?(?:the )?(?:issued and )?outstanding (?:shares|Shares|common stock)|"
    r"each (?:issued and )?(?:outstanding )?share of [^.]{0,80}?common stock[^.]{0,200}?"
    r"(?:will|shall|would) (?:be|automatically be) (?:converted|cancelled|canceled)", re.I)
#: A PARTIAL offer is an offer to purchase a stated NUMBER of shares. Measured on the first
#: 2,500 heads, a bare "up to N shares" matched option grants, share issuances and buyback
#: authorisations in merger proxies, where partial offers do not exist.
#: A PERCENTAGE partial offer is recognised only in the offer's own framing and below 100 %. The
#: classification audit missed Supervalu ("Offer to Purchase for Cash Up to 30% of the Outstanding
#: Shares"); among the included deals a bare "up to N%" matched standstills, lock-up options and
#: abandoned background proposals, and "tender offer for up to 100%" is a whole-company offer.
_PARTIAL = re.compile(
    r"(?:offer to purchase|tender offer (?:for|to purchase)|offer for)\s+(?:for cash\s+)?up to\s+"
    r"[0-9][0-9,]*\s+(?:of (?:the |its )?(?:issued and )?(?:outstanding )?)?(?:shares|Shares)|"
    r"partial tender offer|"
    r"\b(?:offer to purchase(?: for cash)?|offer (?:by [A-Z][A-Za-z]* )?to purchase|tender offer to purchase|"
    r"(?:is|are) offering to purchase)\s+up to\s+[1-9][0-9]?(?:\.[0-9]+)?\s?(?:%|percent) of\b", re.I)
_AMT = r"\$\s?[0-9][0-9,]*(?:\.[0-9]+)?"
#: A share count as a consideration clause states it ("0.6494 shares", "0.1553 of a share", "a number
#: of shares", "a fraction of a share"); never a comma-grouped integer, which is filing-fee arithmetic.
_SHARE_COUNT = r"(?:[0-9]*\.[0-9]+(?:\s+of\s+(?:an?|one))?|a number of|a fraction of an?|one|two)"
_SHARE_NOUN = r"(?:shares?|ordinary shares?|common shares?|ADSs?|American Depositary Shares?|units?)"
#: STOCK consideration is recognised by its SHAPE in the consideration clause - a number or a
#: fraction of shares received, an offer to EXCHANGE, a cash-and-stock TRANSACTION - never by
#: a bare phrase. Measured on the first 2,500 heads, "exchange offer" (202 hits) and "exchange
#: ratio" (103) were overwhelmingly no-shop boilerplate and competing-bid background inside
#: cash deals, and "will receive ... shares of ... common stock" matched option cash-outs.
#: The last two shapes were added after the classification audit missed DIRECTV ("$28.50 in cash
#: plus a number of shares of AT&T") and Alterra ("(1) 0.04315 ... shares of Markel ... and (2)
#: $10.00 in cash"): a cash amount joined to a share count, either order, enumerated or not.
#: Reviewed hit by hit among the included deals before freezing, every hit was a mixed deal.
_STOCK = re.compile(
    r"(?:converted into|exchanged for|right to receive|entitled to receive|will receive)\s+"
    r"(?:(?:\$\s?[0-9][0-9,]*(?:\.[0-9]+)?|cash)[^;]{0,60}?(?:\band\b|\bplus\b)\s+)?"
    r"(?:[0-9]+\.[0-9]+|[0-9]+|one|two|a fraction of an?|fraction of an?)\s+"
    r"(?:shares?|ordinary shares?|ADSs?|American Depositary Shares?|units?)\s+of\b"
    r"|\boffer to exchange\b"
    r"|\b(?:cash and stock|stock and cash|cash and shares|cash-and-stock|stock-and-cash)\s+"
    r"(?:exchange offer|tender offer|offer|merger|transaction|consideration|deal)\b"
    r"|\bcash (?:payment|consideration) and (?:[0-9.]+ )?shares? of\b"
    r"|\bmixed consideration\b"
    r"|(?:\(\s*(?:1|i|a)\s*\)\s*)?" + _AMT + r"\s+(?:per share\s+)?in cash[^;$]{0,80}?\b(?:and|plus)\s+"
    r"(?:\(\s*(?:2|ii|b)\s*\)\s*)?" + _SHARE_COUNT + r"\s+(?:\S+\s+){0,3}?" + _SHARE_NOUN + r"\b"
    r"|\(\s*(?:1|i|a)\s*\)\s*" + _SHARE_COUNT + r"\b[^;$]{0,160}?\b" + _SHARE_NOUN + r"\b[^;$]{0,200}?"
    r"\(\s*(?:2|ii|b)\s*\)\s*" + _AMT + r"\s+(?:per share\s+)?in cash", re.I)
#: An ELECTION between forms of merger consideration. Measured on the first 2,700 heads, a bare
#: "elect to receive" also matched appraisal rights ("stockholders who elect to receive appraisal
#: rights") and noteholders' options, which would have excluded cash deals.
_ELECTION = re.compile(
    r"\belect(?:ion)? to receive\b(?![^.]{0,60}\b(?:appraisal|notes?|debentures?|dividends?)\b)"
    r"(?=[^.]{0,120}\b(?:cash|stock|shares?|ADSs?)\b)|\bcash election\b|\bstock election\b", re.I)
_CVR = re.compile(r"contingent value right|\bCVRs?\b", re.I)
#: CONTINGENT CONSIDERATION under another name, joined to the per-share cash consideration clause. The
#: second classification audit missed Indevus ("$4.50 per Share, net to the seller in cash ... plus
#: contractual rights to receive up to an additional $3.00 per Share in contingent cash consideration
#: payments"). Among the included deals a bare contingent-payment phrase matched 32 filings, of which 8
#: gave target holders a contingent right; the rest were background proposals ("revised its offer to
#: $20.50 per share in cash, plus a $0.50 per share contingent payment"), earnouts of earlier
#: acquisitions, licensing milestones and employee awards. Anchoring on the consideration clause's own
#: verb and on the right being granted ("and (2) one", "plus contractual rights") keeps the 8.
_CONTINGENT = re.compile(
    r"(?:right to receive|entitled to receive|converted into|at a (?:purchase )?price (?:per share )?of)"
    r"[^;]{0,40}?\$\s?[0-9][0-9,]*(?:\.[0-9]+)?[^;]{0,60}?\bin cash\b[^;]{0,160}?\b(?:and|plus)\s+"
    r"(?:\(\s*(?:2|ii|b)\s*\)\s*)?(?:one|a|an|additional|contractual rights?)\b[^;]{0,120}?"
    r"\bcontingent (?:cash consideration|payments?|consideration|rights?)\b", re.I)
#: SECURITIES REGISTERED FOR THE DEAL. A "proxy statement/prospectus" exists because securities are
#: registered for issuance, but the phrase also survives in all-cash templates. Measured among the
#: included deals no consideration shape caught: 13 filings used the phrase; the 5 that also cited a
#: registration statement on Form S-4/F-4 or securities "to be issued" were securities deals (Knoll,
#: Zynga, Kimball, Macquarie Infrastructure) or an acquirer's own proxy (URS for Washington Group); the
#: 8 that did not were all-cash deals (LSI, Blue Buffalo, CH Energy, EnergySolutions, Mity, M&F
#: Worldwide, Dover Saddlery, Mondavi). "exchange ratio" alone would have excluded LSI and Mondavi.
_PROSPECTUS = re.compile(r"\b(?:joint )?(?:proxy|information) statement\s?/\s?prospectus\b", re.I)
_REGISTERED = re.compile(r"\bForm\s+[SF]-4\b|\b(?:shares|common stock|ordinary shares|common shares|units)\b"
                         r"[^.]{0,80}?\bto be issued\b", re.I)
_SYMBOL = re.compile(r"under the (?:ticker |trading )?symbols?\s*[\"'“‘]?\s*"
                     r"([A-Z]{1,5}(?:[.\-][A-Z]{1,2})?)\s*(?=[\"'”’.,;)\s])")
_PAREN = re.compile(r"\(\s*(?:NYSE(?:\s?(?:American|MKT|Amex|Arca))?|NASDAQ(?:\s?[A-Z]{2})?|Nasdaq(?:\s?[A-Z]{2})?|"
                    r"AMEX|OTCBB|OTC)\s*[:\-]\s*([A-Z]{1,5}(?:[.\-][A-Z]{1,2})?)\s*\)")
_SELF_CONTEXT = re.compile(r"(?:our|the Company(?:'s|’s)?|Company) (?:common stock|Common Stock|shares)|"
                           r"\bthe Shares\b|\bour Shares\b", re.I)
_COUNTER_CONTEXT = re.compile(r"\b(?:Parent|Purchaser|Acquir(?:er|or)|Buyer|Offeror|Merger Sub)\b")
#: A symbol statement made about over-the-counter quotation (not an exchange listing).
_OTC_CONTEXT = re.compile(r"\bOTC\b|OTCBB|Bulletin Board|Pink Sheets|pink sheets|over-the-counter|Pink OTC",
                          re.I)
_STOP_TICKERS = {"A", "I", "THE", "AND", "OR", "CEO", "USA", "US", "LLC", "INC", "NYSE", "SEC", "NASDAQ",
                 "AMEX", "OTC", "CUSIP", "ISIN", "N", "NA"}


def _money(s: str) -> Optional[float]:
    try:
        v = float(s.replace(",", ""))
    except (TypeError, ValueError):
        return None
    return v if 0.0 < v < 100000.0 else None


def extract_terms(flat: str) -> dict:
    text = flat[:TEXT_WINDOW]
    prices = []
    for rx in _CASH_PRICE:
        for m in rx.finditer(text):
            v = _money(m.group(1))
            if v is not None:
                prices.append({"price": v, "at": m.start(),
                               "snippet": text[max(0, m.start() - 120):m.end() + 40]})
    prices.sort(key=lambda p: p["at"])
    # Symbol statements are searched in the WHOLE stored text: measured on 252 cash-like episodes
    # with no symbol in the first 90K characters, 84 stated one further in, 83 of them only there
    # (the market-price section). Terms stay on the opening window, where the consideration is.
    tickers, seen_tk = [], set()
    for rx, kind in ((_SYMBOL, "SYMBOL_STATEMENT"), (_PAREN, "EXCHANGE_PARENTHESIS")):
        for m in rx.finditer(flat):
            t = m.group(1).replace("-", ".")
            if t in _STOP_TICKERS:
                continue
            before = flat[max(0, m.start() - 220):m.start()]
            ctx = ("SELF" if _SELF_CONTEXT.search(before) and not _COUNTER_CONTEXT.search(before[-90:])
                   else "COUNTERPARTY" if _COUNTER_CONTEXT.search(before) else "UNTAGGED")
            venue = ("OTC" if _OTC_CONTEXT.search(before[-160:]) or re.search(r"\.(?:OB|PK|OTC)$", t)
                     else "LISTED_OR_UNSTATED")
            if (t, ctx, venue) in seen_tk:
                continue
            seen_tk.add((t, ctx, venue))
            tickers.append({"ticker": re.sub(r"\.(?:OB|PK|OTC)$", "", t), "kind": kind, "context": ctx,
                            "venue": venue, "at": m.start()})
    cusips = []
    for m in CBD._CUSIP.finditer(text[:30000]):
        c = re.sub(r"[\s-]", "", m.group(1)).upper()
        if len(c) >= 8 and any(ch.isdigit() for ch in c[:6]):
            cusips.append(c[:9])
    return {
        "cash_prices": [{"price": p["price"], "at": p["at"]} for p in prices[:12]],
        "first_cash_snippet": prices[0]["snippet"] if prices else None,
        "distinct_cash_prices": sorted({p["price"] for p in prices}),
        "all_shares": bool(_ALL_SHARES.search(text)),
        "partial_offer": bool(_PARTIAL.search(text)),
        "securities_registered": bool(_PROSPECTUS.search(text) and _REGISTERED.search(text)),
        "stock_consideration": bool(_STOCK.search(text)) or bool(_PROSPECTUS.search(text)
                                                                 and _REGISTERED.search(text)),
        "election": bool(_ELECTION.search(text)),
        "contingent_consideration": bool(_CONTINGENT.search(text)),
        "contingent_value_right": bool(_CVR.search(text) or _CONTINGENT.search(text)),
        "tickers": tickers[:20],
        "cusips": sorted(set(cusips))[:5],
        "text_chars_searched": len(text),
    }


def parse_filing(accession: str) -> Optional[dict]:
    raw = read_head(accession)
    if raw is None:
        return None
    hdr = parse_header(raw)
    terms = extract_terms(flatten(raw))
    return {"accession": CBD._norm_accession(accession), **hdr, "terms": terms}


def parsed_path() -> Path:
    return data_dir() / "parsed_filings.jsonl"


def parse_all(rows: list, *, verbose: bool = True) -> dict:
    """Parse every stored head once; written as JSON lines, keyed by accession."""
    out_p = parsed_path()
    done = set()
    if out_p.exists():
        for ln in out_p.read_text(encoding="utf-8").splitlines():
            try:
                done.add(json.loads(ln)["accession"])
            except (ValueError, KeyError):
                continue
    accs = sorted({r["accession"] for r in rows} - done)
    n = 0
    with out_p.open("a", encoding="utf-8") as fh:
        for a in accs:
            rec = parse_filing(a)
            if rec is None:
                continue
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
            n += 1
            if verbose and n % 2000 == 0:
                print("  parsed %d/%d" % (n, len(accs)), flush=True)
    return {"parsed_this_run": n, "previously_parsed": len(done)}


def load_parsed() -> dict:
    out = {}
    p = parsed_path()
    if not p.exists():
        return out
    for ln in p.read_text(encoding="utf-8").splitlines():
        try:
            rec = json.loads(ln)
        except ValueError:
            continue
        out[rec["accession"]] = rec
    return out


__all__ = ["CALCULATION_OWNER", "FORMS", "FORMS_TARGET", "FORMS_TENDER", "data_dir", "quarters",
           "fetch_full_index", "load_index", "index_census", "fetch_heads", "read_head",
           "parse_header", "flatten", "extract_terms", "parse_filing", "parse_all", "load_parsed"]
