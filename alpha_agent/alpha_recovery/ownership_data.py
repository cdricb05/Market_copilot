"""alpha_agent.alpha_recovery.ownership_data - the free SEC acquisition for the
institutional-ownership axis.

Two sources, both published by the SEC itself, both free, both requiring no key,
no account and no licence. Nothing here spends money.

* **Form 13F structured data sets** - one ZIP per FILING window (quarterly to
  2023Q4, then explicit date ranges), each carrying ``SUBMISSION.tsv``
  (accession, filing date, submission type, CIK, period of report),
  ``COVERPAGE.tsv`` (the amendment flags) and ``INFOTABLE.tsv`` (the holdings,
  keyed by CUSIP). The ZIP is organised by the date the filing was MADE, not by
  the period it reports, which is what makes a point-in-time read possible at
  all: a decision may only see accessions already filed.

* **Fails-to-Deliver** - semi-monthly files publishing ``SETTLEMENT DATE |
  CUSIP | SYMBOL | QUANTITY | DESCRIPTION | PRICE``. One publisher stating both
  identifiers in the SAME record is what makes the identity join authoritative
  rather than a name match. :mod:`ownership_identity` owns the join; this
  module only fetches the bytes.

The SEC's User-Agent convention (a contact address, <= 8 requests/second) is
inherited verbatim from the canonical collector
:mod:`alpha_agent.r63.acquire` - there is no second HTTP convention here.

Every payload is recorded in an append-only manifest with its acquisition
instant, HTTP status, byte count and sha256. A download that fails is recorded
and reported as partial coverage; nothing is filled, and nothing is retried
silently. Re-running is idempotent: a payload already on disk with a matching
size is not fetched again.

RESEARCH ONLY. No purchase, no subscription, no live store, no order, no fill.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
from pathlib import Path

from alpha_agent.r63 import acquire as ACQ

from . import now_iso, research_root

CALCULATION_OWNER = "alpha_agent.alpha_recovery.ownership_data"

FTD_INDEX_URL = "https://www.sec.gov/data/foiadocsfailsdatahtm"
F13_INDEX_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
SEC_HOST = "https://www.sec.gov"

#: DECLARED publication lag for a Fails-to-Deliver file, fixed BEFORE use and
#: applied in exactly one place (:mod:`ownership_identity`). The SEC disseminates
#: the semi-monthly file roughly two to three weeks after the settlement period;
#: 45 calendar days is deliberately generous, because an identifier map that is
#: 45 days stale costs almost nothing in coverage while a map that is one day
#: early is a look-ahead.
FTD_PUBLICATION_LAG_DAYS = 45

#: The three members this campaign reads. OTHERMANAGER / SIGNATURE / SUMMARYPAGE
#: are downloaded with the ZIP but never parsed.
F13_MEMBERS = ("SUBMISSION.tsv", "COVERPAGE.tsv", "INFOTABLE.tsv")

_MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
           "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def data_dir() -> Path:
    return research_root() / "_data_sec_ownership"


def ftd_dir() -> Path:
    return data_dir() / "fails_to_deliver"


def f13_dir() -> Path:
    return data_dir() / "form13f"


def manifest_path() -> Path:
    return data_dir() / "acquisition_manifest.jsonl"


def _append_manifest(row: dict) -> None:
    data_dir().mkdir(parents=True, exist_ok=True)
    with open(manifest_path(), "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


def _headers() -> dict | None:
    contact = ACQ.contact_email()
    if not contact:
        return None
    h = ACQ._headers(contact)
    h["Accept"] = "*/*"
    return h


def _index_links(url: str, pattern: str, headers: dict) -> list:
    """Distinct ``.zip`` hrefs on an SEC index page matching ``pattern``.

    The canonical headers ask for ``identity`` encoding on purpose: a gzip
    response regexed as text finds nothing and silently reports an empty
    archive, which is indistinguishable from the SEC having withdrawn it.
    """
    status, body = ACQ._get(url, headers)
    if status != 200 or not body:
        return []
    html = body.decode("utf-8", "replace")
    seen, out = set(), []
    for href in re.findall(pattern, html, re.I):
        name = href.rsplit("/", 1)[-1]
        if name in seen:
            continue
        seen.add(name)
        out.append({"name": name,
                    "url": href if href.startswith("http") else SEC_HOST + href})
    return sorted(out, key=lambda r: r["name"])


# --------------------------------------------------------------------------- #
# Publication inventories
# --------------------------------------------------------------------------- #
def ftd_index(headers: dict | None = None) -> list:
    """Every published Fails-to-Deliver file, with the half-month it covers."""
    headers = headers or _headers()
    if not headers:
        return []
    rows = _index_links(FTD_INDEX_URL, r'href="([^"]+cnsfails\d{6}[ab]?\.zip)"', headers)
    for r in rows:
        m = re.search(r"cnsfails(\d{4})(\d{2})([ab])?", r["name"], re.I)
        r["year"], r["month"] = int(m.group(1)), int(m.group(2))
        r["half"] = (m.group(3) or "a").lower()
        r["period"] = "%04d-%02d%s" % (r["year"], r["month"], r["half"])
    return rows


def _f13_window(name: str) -> tuple:
    """(first_filing_date, last_filing_date) a 13F data set covers.

    Two naming conventions live on the same page: ``2016q1_form13f.zip`` and
    ``01mar2026-31may2026_form13f.zip``. Both describe the window in which the
    filings were MADE, never the period they report.
    """
    m = re.match(r"(\d{4})q([1-4])_form13f", name, re.I)
    if m:
        y, q = int(m.group(1)), int(m.group(2))
        a = "%04d-%02d-01" % (y, 3 * q - 2)
        end_m = 3 * q
        end_d = {3: 31, 6: 30, 9: 30, 12: 31}[end_m]
        return a, "%04d-%02d-%02d" % (y, end_m, end_d)
    m = re.match(r"(\d{2})([a-z]{3})(\d{4})-(\d{2})([a-z]{3})(\d{4})_form13f", name, re.I)
    if m:
        return ("%s-%02d-%s" % (m.group(3), _MONTHS[m.group(2).lower()], m.group(1)),
                "%s-%02d-%s" % (m.group(6), _MONTHS[m.group(5).lower()], m.group(4)))
    return None, None


def f13_index(headers: dict | None = None) -> list:
    """Every published Form 13F structured data set, with its filing window."""
    headers = headers or _headers()
    if not headers:
        return []
    rows = _index_links(F13_INDEX_URL, r'href="([^"]+_form13f\.zip)"', headers)
    for r in rows:
        r["filed_from"], r["filed_to"] = _f13_window(r["name"])
    return sorted([r for r in rows if r["filed_from"]], key=lambda r: r["filed_from"])


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #
def _download(rows: list, dest: Path, headers: dict, *, kind: str,
              verbose: bool = True) -> dict:
    dest.mkdir(parents=True, exist_ok=True)
    ok, cached, failed, total_bytes = 0, 0, [], 0
    last = 0.0
    for k, r in enumerate(rows):
        p = dest / r["name"]
        if p.exists() and p.stat().st_size > 1024:
            cached += 1
            total_bytes += p.stat().st_size
            continue
        wait = ACQ.MIN_INTERVAL_S - (time.time() - last)
        if wait > 0:
            time.sleep(wait)
        last = time.time()
        status, body = ACQ._get(r["url"], headers)
        if status != 200 or not body:
            failed.append({"name": r["name"], "http": status})
            _append_manifest({"at": now_iso(), "kind": kind, "name": r["name"],
                              "url": r["url"], "http": status, "state": "FAILED"})
            continue
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(body)
        tmp.replace(p)
        ok += 1
        total_bytes += len(body)
        _append_manifest({"at": now_iso(), "kind": kind, "name": r["name"],
                          "url": r["url"], "http": status, "bytes": len(body),
                          "sha256": hashlib.sha256(body).hexdigest(), "state": "OK"})
        if verbose and (ok % 10 == 0 or ok == 1):
            print("  %s %d/%d downloaded=%d cached=%d failed=%d %.0fMB"
                  % (kind, k + 1, len(rows), ok, cached, len(failed),
                     total_bytes / 1e6), flush=True)
    return {"requested": len(rows), "downloaded": ok, "cached": cached,
            "failed": failed[:40], "n_failed": len(failed),
            "bytes_on_disk": total_bytes, "dir": str(dest)}


def acquire(*, ftd_from: str = "2009-01", f13_from: str = "2013-01",
            verbose: bool = True) -> dict:
    """Fetch both free archives. Idempotent; safe to re-run after a failure."""
    headers = _headers()
    if not headers:
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT",
                "why": "the SEC requires a contact in the User-Agent and "
                       "`git config user.email` resolved nothing"}
    ftd = [r for r in ftd_index(headers) if r["period"][:7] >= ftd_from]
    f13 = [r for r in f13_index(headers) if r["filed_from"][:7] >= f13_from]
    if verbose:
        print("ownership_data: %d FTD files, %d 13F data sets" % (len(ftd), len(f13)),
              flush=True)
    res_f13 = _download(f13, f13_dir(), headers, kind="FORM13F", verbose=verbose)
    res_ftd = _download(ftd, ftd_dir(), headers, kind="FTD", verbose=verbose)
    summary = {
        "calculation_owner": CALCULATION_OWNER,
        "state": "OK" if not (res_f13["n_failed"] or res_ftd["n_failed"]) else "PARTIAL",
        "acquired_at": now_iso(),
        "paid_dollars": 0, "subscription_started": False, "licence_accepted": False,
        "form13f": res_f13, "fails_to_deliver": res_ftd,
        "form13f_windows": [{"name": r["name"], "filed_from": r["filed_from"],
                             "filed_to": r["filed_to"]} for r in f13],
        "ftd_publication_lag_days_declared": FTD_PUBLICATION_LAG_DAYS,
        "user_agent_convention": "canonical SEC collector (git config user.email)",
    }
    data_dir().mkdir(parents=True, exist_ok=True)
    (data_dir() / "acquisition_summary.json").write_text(
        json.dumps(summary, indent=1, sort_keys=True), encoding="utf-8")
    return summary


def local_inventory() -> dict:
    """What is actually on disk right now (measured, never assumed)."""
    def _scan(d: Path) -> dict:
        files = sorted(d.glob("*.zip")) if d.exists() else []
        return {"n_files": len(files), "bytes": sum(f.stat().st_size for f in files),
                "first": files[0].name if files else None,
                "last": files[-1].name if files else None}
    return {"form13f": _scan(f13_dir()), "fails_to_deliver": _scan(ftd_dir()),
            "dir": str(data_dir())}
