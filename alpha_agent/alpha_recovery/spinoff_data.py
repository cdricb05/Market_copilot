r"""alpha_agent.alpha_recovery.spinoff_data - the free data layer for the Alpha Agent mechanism
``SPINOFF_PARENT_HOLDER_FORCED_SELLING_V1``.

WHY EDGAR
    Norgate carries no spin-off event: ``capital_event_timeseries`` is empty around IBM/Kyndryl (2021),
    GE/GE Vernova (2024), 3M/Solventum (2024) and Danaher/Veralto (2023), and a spinco's first quote is its
    WHEN-ISSUED start (measured 2026-09-15). The owned R63 submissions store is selected on S&P 500
    membership (78 Form 10-12B filings) and would keep only spincos that later joined the index. Every
    registrant that lists a class of securities on an exchange under Section 12(b) files a Form 10-12B, and
    EDGAR's quarterly ``master`` index lists every one of them.

WHAT IS ACQUIRED ($0, public, the estate's SEC user agent and rate gate)
    1. ``full-index/<year>/QTR<q>/master.zip`` - only ``10-12B`` and ``10-12B/A`` rows are kept.
    2. The HEAD (first 256 KB) of each such filing's complete submission text.
    3. A DEEP read (first 2 MB) of the filings of distribution registrants whose heads state no listing symbol.
    4. ``data.sec.gov/submissions/CIK##########.json`` of each distribution registrant: its current name and
       every former name EDGAR records (identity only; no filing content).

WHAT IS PARSED (from the filing's own words)
    distribution  a pro rata distribution of the registrant's shares, or the Form 10's own defined terms for it
                  ("the Distribution", "The Spin-Off", "The Separation and Distribution")
    exclusions    emergence from bankruptcy, a Chapter 11 plan of reorganisation, a business development company,
                  a blank check COMPANY. Calibration round 1 measured that a bare "plan of reorganization" (the
                  Section 368 tax-free boilerplate) and a bare "blank check" (blank check preferred stock) excluded
                  187 real spin-offs, among them Dow, Embarq, Certegy, Hanesbrands and Washington Prime.
    listing       exchange-listing symbol statements (a listing verb and an exchange venue before "under the
                  symbol", or an exchange parenthesis); over-the-counter statements are excluded.

WHAT IS LINKED (symbols, names and quote DATES only)
    The registrant's names are every EDGAR name it has had: the index conformed names plus the current and
    former names of its submissions record. Norgate names a security by its LAST name and ticker, so a spinco
    renamed before delisting (Imation -> GlassBridge) links only through its later EDGAR name.
    SYMBOL   exactly one security whose base symbol is a stated listing symbol, first quoted inside
             [first filing, last filing + 365 days], whose Norgate name shares a distinctive token with any of the
             registrant's EDGAR names (a symbol stated for a parent or sibling does not link on its own)
    NAME     otherwise, exactly one security first quoted inside the window whose Norgate name carries the first
             distinctive token of the registrant's current EDGAR name or of its index name

This module reads NO price. Returns are read only by the executor, after its data gate.
RESEARCH ONLY. No purchase, subscription, registration, promotion, capital, order or fill.
"""
from __future__ import annotations

import bisect
import collections
import datetime as _dt
import gzip
import io
import json
import re
import threading
import time
import zipfile
from concurrent import futures
from pathlib import Path
from typing import Optional

from alpha_agent.r63 import acquire as ACQ

from . import now_iso, research_root
from . import control_block_data as CBD
from . import merger_arb_data as MD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.spinoff_data"

FORMS = ("10-12B", "10-12B/A")
FIRST_QUARTER = (1996, 1)
LAST_QUARTER = (2026, 3)
HEAD_BYTES = 262144
DEEP_BYTES = 2000000
FETCH_WORKERS = 3
TEXT_WINDOW = 120000
LINK_WINDOW_DAYS = 365
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK%010d.json"
LINKED_STATES = ("SYMBOL", "NAME")

#: Words that never identify a company on their own.
NAME_STOP = frozenset({
    "INC", "CORP", "CORPORATION", "CO", "COMPANY", "COMPANIES", "HOLDINGS", "HOLDING", "GROUP", "THE", "LTD",
    "LIMITED", "LLC", "PLC", "LP", "COMMON", "CLASS", "NEW", "DE", "OF", "AND", "TRUST", "INTERNATIONAL",
    "INTL", "NV", "SA", "AG", "STOCK", "SHARES", "ORDINARY", "SPINCO", "AMERICAN", "AMERICA", "NATIONAL",
    "FIRST", "UNITED", "GENERAL", "SYSTEMS", "TECHNOLOGIES", "TECHNOLOGY", "INDUSTRIES", "FINANCIAL", "ENERGY",
    "CAPITAL", "RESOURCES", "SERVICES", "PARTNERS", "ENTERPRISES", "BRANDS", "US", "USA", "GLOBAL", "WORLDWIDE",
    "DEL", "NY", "MD", "PA", "ADR", "REIT", "PROPERTIES", "COMMUNICATIONS", "SOLUTIONS", "PRODUCTS", "UNIT",
    "UNITS", "NEWCO", "HOLDCO", "SPIN", "PARENT"})


def data_dir() -> Path:
    return research_root() / "_data_sec_spinoff"


def index_path(year: int, qtr: int) -> Path:
    return data_dir() / "full_index" / ("spinoff_%d_q%d.tsv" % (year, qtr))


def head_path(accession: str) -> Path:
    acc = CBD._norm_accession(accession)
    return data_dir() / "heads" / acc[:10] / ("%s.gz" % acc)


def deep_path(accession: str) -> Path:
    acc = CBD._norm_accession(accession)
    return data_dir() / "deep" / acc[:10] / ("%s.gz" % acc)


def submissions_path(cik) -> Path:
    return data_dir() / "submissions" / ("CIK%010d.json" % int(cik))


def manifest_path() -> Path:
    return data_dir() / "acquisition_manifest.jsonl"


def parsed_path() -> Path:
    return data_dir() / "parsed_filings.jsonl"


def _append_manifest(row: dict) -> None:
    p = manifest_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


def quarters(first=FIRST_QUARTER, last=LAST_QUARTER) -> list:
    return MD.quarters(first, last)


def failed_quarters() -> list:
    """Index quarters absent on disk."""
    return [("%dQ%d" % q) for q in quarters() if not index_path(*q).exists()]


# --------------------------------------------------------------------------- #
# 1. The full quarterly index
# --------------------------------------------------------------------------- #
def fetch_full_index(*, first=FIRST_QUARTER, last=LAST_QUARTER, headers: Optional[dict] = None,
                     refetch_last: bool = False, verbose: bool = True) -> dict:
    """Keep only Form 10-12B rows: ``cik  company  form  date  path``. Cached per quarter."""
    headers = headers or CBD._headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    ok = cached = failed = kept = 0
    last_req = 0.0
    qs = quarters(first, last)
    for (y, q) in qs:
        p = index_path(y, q)
        if p.exists() and not (refetch_last and (y, q) == tuple(last)):
            cached += 1
            continue
        wait = ACQ.MIN_INTERVAL_S - (time.time() - last_req)
        if wait > 0:
            time.sleep(wait)
        last_req = time.time()
        status, body = ACQ._get(MD.MASTER_ZIP_URL % (y, q), headers)
        if status != 200 or not body:
            failed += 1
            _append_manifest({"at": now_iso(), "index": "%dQ%d" % (y, q), "http": status, "state": "FAILED"})
            continue
        zf = zipfile.ZipFile(io.BytesIO(body))
        txt = zf.read(zf.namelist()[0]).decode("latin-1")
        rows = []
        for ln in txt.splitlines():
            parts = ln.split("|")
            if len(parts) != 5 or parts[2].strip() not in FORMS:
                continue
            rows.append("\t".join([parts[0].strip().lstrip("0"), parts[1].strip(), parts[2].strip(),
                                   parts[3].strip(), parts[4].strip()]))
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text("\n".join(rows), encoding="utf-8")
        tmp.replace(p)
        ok += 1
        kept += len(rows)
        _append_manifest({"at": now_iso(), "index": "%dQ%d" % (y, q), "http": status, "bytes": len(body),
                          "rows_kept": len(rows), "state": "OK"})
        if verbose:
            print("  full-index %dQ%d -> %d Form 10-12B rows" % (y, q, len(rows)), flush=True)
    return {"quarters": len(qs), "fetched": ok, "cached": cached, "failed": failed, "rows_kept_this_run": kept}


def load_index() -> list:
    """Every Form 10-12B index row; one row per (CIK, accession)."""
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
            out.append({"cik": cik, "company": company, "form": form, "date": date[:10], "path": path,
                        "accession": acc})
    out.sort(key=lambda r: (r["date"], r["cik"], r["accession"]))
    return out


# --------------------------------------------------------------------------- #
# 2. Filing reads and submissions records
# --------------------------------------------------------------------------- #
def _fetch(rows: list, path_fn, *, headers: Optional[dict], workers: int, nbytes: int, label: str,
           verbose: bool) -> dict:
    headers = headers or CBD._headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    by_acc = {}
    for r in rows:
        by_acc.setdefault(r["accession"], r)
    pending = [r for a, r in by_acc.items() if not path_fn(a).exists()]
    gate = CBD._RateGate(ACQ.MIN_INTERVAL_S)
    counters = {"ok": 0, "failed": 0, "bytes": 0}
    lock = threading.Lock()
    t0 = time.time()

    def _one(r: dict) -> None:
        gate.wait()
        status, body = MD._get_head(MD.ARCHIVES_URL % r["path"], headers, nbytes)
        if status != 200 or not body:
            with lock:
                counters["failed"] += 1
            _append_manifest({"at": now_iso(), "accession": r["accession"], "http": status,
                              "state": "%s_FAILED" % label})
            return
        p = path_fn(r["accession"])
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".%d.tmp" % threading.get_ident())
        with gzip.open(tmp, "wb") as fh:
            fh.write(body)
        tmp.replace(p)
        with lock:
            counters["ok"] += 1
            counters["bytes"] += len(body)
            n = counters["ok"]
        if verbose and n % 500 == 0:
            print("  %s %d/%d failed=%d %.2fGB %.1f req/s" % (label.lower(), n, len(pending), counters["failed"],
                                                            counters["bytes"] / 1e9, n / max(time.time() - t0, 1e-9)),
                  flush=True)

    if pending:
        with futures.ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
            list(pool.map(_one, pending))
    return {"accessions": len(by_acc), "fetched": counters["ok"], "cached": len(by_acc) - len(pending),
            "failed": counters["failed"], "bytes": counters["bytes"], "read_bytes": nbytes}


def fetch_heads(rows: list, *, headers: Optional[dict] = None, workers: int = FETCH_WORKERS,
                verbose: bool = True) -> dict:
    return _fetch(rows, head_path, headers=headers, workers=workers, nbytes=HEAD_BYTES, label="HEAD", verbose=verbose)


def fetch_deep(rows: list, *, headers: Optional[dict] = None, workers: int = FETCH_WORKERS,
               verbose: bool = True) -> dict:
    return _fetch(rows, deep_path, headers=headers, workers=workers, nbytes=DEEP_BYTES, label="DEEP", verbose=verbose)


def fetch_submissions(ciks, *, headers: Optional[dict] = None, verbose: bool = True) -> dict:
    """The EDGAR submissions record of each CIK (current and former names). Idempotent, rate gated."""
    headers = headers or CBD._headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT"}
    todo = sorted({str(c) for c in ciks if not submissions_path(c).exists()}, key=int)
    ok = failed = 0
    last = 0.0
    for c in todo:
        wait = ACQ.MIN_INTERVAL_S - (time.time() - last)
        if wait > 0:
            time.sleep(wait)
        last = time.time()
        status, body = ACQ._get(SUBMISSIONS_URL % int(c), headers)
        if status != 200 or not body:
            failed += 1
            _append_manifest({"at": now_iso(), "submissions_cik": c, "http": status, "state": "SUBMISSIONS_FAILED"})
            continue
        p = submissions_path(c)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body if isinstance(body, (bytes, bytearray)) else str(body).encode("utf-8"))
        ok += 1
    if verbose:
        print("  submissions fetched %d failed %d of %d pending" % (ok, failed, len(todo)), flush=True)
    return {"ciks": len({str(c) for c in ciks}), "fetched": ok, "failed": failed, "cached": len({str(c) for c in ciks}) - len(todo)}


def edgar_identity(ciks, rows: list) -> dict:
    """{cik: {"names": [every EDGAR name], "latest": current name or None, "record": bool}} - names only."""
    idx_names = collections.defaultdict(list)
    for r in rows:
        if r["company"] not in idx_names[r["cik"]]:
            idx_names[r["cik"]].append(r["company"])
    out = {}
    for c in {str(c) for c in ciks}:
        names, latest, record = list(idx_names.get(c, [])), None, False
        p = submissions_path(c)
        if p.exists():
            try:
                body = json.loads(p.read_text(encoding="utf-8"))
                record = True
                latest = body.get("name")
                for n in [latest] + [f.get("name") for f in body.get("formerNames") or []]:
                    if n and n not in names:
                        names.append(n)
            except (ValueError, OSError):
                pass
        out[c] = {"names": names, "latest": latest, "record": record}
    return out


def read_head(accession: str) -> Optional[str]:
    """The deepest stored read: the deep read when one exists, else the head."""
    for p in (deep_path(accession), head_path(accession)):
        if not p.exists():
            continue
        try:
            return gzip.open(p, "rb").read().decode("latin-1")
        except (OSError, EOFError):
            continue
    return None


# --------------------------------------------------------------------------- #
# 3. Distribution, exclusion and listing statements
# --------------------------------------------------------------------------- #
_SEPARATION = re.compile(r"\bspin[- ]?off\b|\bspun[- ]off\b|\bseparation and distribution\b|\bthe separation\b", re.I)
_DISTRIBUTION = re.compile(
    r"\bpro rata (?:distribution|basis)\b|\bdistribution of (?:all|[0-9]{2,3}(?:\.[0-9]+)? ?%|"
    r"approximately [0-9]{2,3}(?:\.[0-9]+)? ?%)[^.]{0,40}? of (?:the |our )?(?:outstanding )?(?:shares|common stock)\b|"
    r"\brecord date for the distribution\b|\bdistribution (?:date|ratio)\b|"
    r"\(the [\"“”]?(?:Distribution|Spin-?Off)[\"“”]?\)|"
    r"[\"“](?:The )?(?:Spin-?Off|Distribution|Separation and Distribution)[\"”]", re.I)
_NOT_SPIN = re.compile(
    r"\bemerg(?:e|ed|es|ing|ence) from (?:chapter 11|bankruptcy)\b|"
    r"\bplan of reorganization (?:under|pursuant to|filed under|confirmed under) chapter 11\b|"
    r"\bconfirm(?:ed|ation of) (?:the |our |its )?(?:joint )?(?:chapter 11 )?plan of reorganization\b|"
    r"\bbusiness development company\b|\bblank check company\b", re.I)
_LISTED_VENUE = re.compile(r"\b(?:New York Stock Exchange|NYSE|Nasdaq|NASDAQ|American Stock Exchange|NYSE American|"
                           r"NYSE MKT|NYSE Amex|AMEX)\b")
_LISTING_VERB = re.compile(r"\b(?:list|listed|listing|trade|traded|trading|quoted)\b", re.I)


def extract_spinoff(flat: str) -> dict:
    head = flat[:TEXT_WINDOW]
    symbols, seen = [], set()
    for rx, kind in ((MD._SYMBOL, "SYMBOL_STATEMENT"), (MD._PAREN, "EXCHANGE_PARENTHESIS")):
        for m in rx.finditer(flat):
            t = m.group(1).replace("-", ".")
            if t in MD._STOP_TICKERS:
                continue
            before = flat[max(0, m.start() - 260):m.start()]
            if MD._OTC_CONTEXT.search(before[-160:]) or re.search(r"\.(?:OB|PK|OTC)$", t):
                continue
            listed = kind == "EXCHANGE_PARENTHESIS" or bool(_LISTED_VENUE.search(before)
                                                            and _LISTING_VERB.search(before[-200:]))
            if not listed or t in seen:
                continue
            seen.add(t)
            symbols.append({"ticker": t, "kind": kind, "at": m.start()})
    return {"separation_language": bool(_SEPARATION.search(head)),
            "distribution_language": bool(_DISTRIBUTION.search(head)),
            "not_spin_language": bool(_NOT_SPIN.search(head)),
            "symbols": symbols[:25], "text_chars_searched": len(flat)}


def parse_filing(accession: str) -> Optional[dict]:
    raw = read_head(accession)
    if raw is None:
        return None
    return {"accession": CBD._norm_accession(accession), **MD.parse_header(raw),
            "deep": deep_path(accession).exists(), "spinoff": extract_spinoff(MD.flatten(raw))}


def parse_all(rows: list, *, verbose: bool = True, reparse: bool = False) -> dict:
    """Parse every stored read; a filing re-parses when its deep read arrived after its last parse."""
    out_p = parsed_path()
    done = {}
    if out_p.exists() and not reparse:
        for ln in out_p.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(ln)
                done[rec["accession"]] = bool(rec.get("deep"))
            except (ValueError, KeyError):
                continue
    accs = sorted(a for a in {r["accession"] for r in rows}
                  if a not in done or (deep_path(a).exists() and not done[a]))
    n = 0
    out_p.parent.mkdir(parents=True, exist_ok=True)
    with out_p.open("w" if reparse else "a", encoding="utf-8") as fh:
        for a in accs:
            rec = parse_filing(a)
            if rec is None:
                continue
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
            n += 1
            if verbose and n % 1000 == 0:
                print("  parsed %d/%d" % (n, len(accs)), flush=True)
    return {"parsed_this_run": n, "previously_parsed": len(done)}


def load_parsed() -> dict:
    """Latest parse per accession (a later line - a deep re-parse - wins)."""
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


# --------------------------------------------------------------------------- #
# 4. Registrants, names and links
# --------------------------------------------------------------------------- #
def name_tokens(name) -> list:
    """Distinctive name tokens in order of appearance."""
    toks = re.findall(r"[A-Z0-9]+", str(name or "").upper())
    out = []
    for t in toks:
        if len(t) >= 3 and t not in NAME_STOP and t not in out:
            out.append(t)
    return out


def registrants(rows: list, parsed: dict) -> list:
    """One record per registrant CIK: filing window, distribution classification and stated listing symbols."""
    by = collections.defaultdict(list)
    for r in rows:
        by[r["cik"]].append(r)
    out = []
    for cik, rs in sorted(by.items()):
        rs.sort(key=lambda r: (r["date"], r["accession"]))
        sep = dist = not_spin = False
        votes = collections.Counter()
        for r in rs:
            s = (parsed.get(r["accession"]) or {}).get("spinoff") or {}
            sep |= bool(s.get("separation_language"))
            dist |= bool(s.get("distribution_language"))
            not_spin |= bool(s.get("not_spin_language"))
            for t in s.get("symbols") or []:
                votes[t["ticker"]] += 1
        out.append({"cik": cik, "company": rs[-1]["company"], "first_filed": rs[0]["date"], "last_filed": rs[-1]["date"],
                    "filings": len(rs), "parsed": sum(1 for r in rs if r["accession"] in parsed),
                    "separation": sep, "distribution": dist, "not_spin": not_spin,
                    "is_spinoff": bool(dist and not not_spin),
                    "symbols": [t for t, _n in votes.most_common(5)]})
    return out


def link_norgate(regs: list, universe: dict, names: dict, edgar: Optional[dict] = None) -> list:
    """``universe``: {base symbol: [(Norgate symbol, first quoted date)]}; ``names``: {Norgate symbol: name};
    ``edgar``: :func:`edgar_identity`. Symbols, names and dates only."""
    edgar = edgar or {}
    by_first = sorted((first, sym) for secs in universe.values() for sym, first in secs if first)
    firsts = [f for f, _s in by_first]
    out = []
    for g in regs:
        lo = g["first_filed"]
        hi = str(_dt.date.fromisoformat(g["last_filed"]) + _dt.timedelta(days=LINK_WINDOW_DAYS))
        ident = edgar.get(g["cik"]) or {}
        all_names = list(ident.get("names") or []) or [g["company"]]
        reg_tokens = {t for n in all_names for t in name_tokens(n)}
        keys = [k for k in (next(iter(name_tokens(ident.get("latest"))), None), next(iter(name_tokens(g["company"])), None))
                if k]
        cands = sorted({(sym, first, t) for t in g["symbols"] for sym, first in universe.get(t, [])
                        if first and lo <= first <= hi})
        confirmed = [c for c in cands if reg_tokens & set(name_tokens(names.get(c[0])))]
        state, link = None, None
        if len(confirmed) == 1:
            state, link = "SYMBOL", confirmed[0]
        elif len(confirmed) > 1:
            state = "SYMBOL_AMBIGUOUS"
        else:
            i0, i1 = bisect.bisect_left(firsts, lo), bisect.bisect_right(firsts, hi)
            hits = sorted({(sym, first, None) for first, sym in by_first[i0:i1]
                           if set(keys) & set(name_tokens(names.get(sym)))})
            if len(hits) == 1:
                state, link = "NAME", hits[0]
            elif hits:
                state = "NAME_AMBIGUOUS"
            else:
                state = "UNCONFIRMED_SYMBOL" if cands else ("NO_NAME_KEY" if not keys else "NO_QUOTE_IN_WINDOW")
        out.append({**g, "norgate_candidates": cands, "name_keys": keys, "edgar_names": all_names,
                    "link": link, "link_state": state})
    return out


def norgate_universe() -> dict:
    """Base symbol -> [(Norgate symbol, first quoted date)] over current and delisted US equities."""
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import norgatedata as nd
    out = collections.defaultdict(list)
    for db in ("US Equities", "US Equities Delisted"):
        for sym in nd.database_symbols(db):
            try:
                first = nd.first_quoted_date(sym)
            except Exception:                                    # noqa: BLE001
                continue
            out[sym.split("-")[0]].append((sym, str(first)[:10] if first is not None else None))
    return dict(out)


def norgate_names() -> dict:
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import norgatedata as nd
    out = {}
    for db in ("US Equities", "US Equities Delisted"):
        for sym in nd.database_symbols(db):
            try:
                out[sym] = nd.security_name(sym)
            except Exception:                                    # noqa: BLE001
                out[sym] = None
    return out


__all__ = ["FORMS", "fetch_full_index", "load_index", "fetch_heads", "fetch_deep", "fetch_submissions", "edgar_identity",
           "parse_all", "load_parsed", "extract_spinoff", "registrants", "link_norgate", "LINKED_STATES",
           "norgate_universe", "norgate_names", "name_tokens", "failed_quarters"]
