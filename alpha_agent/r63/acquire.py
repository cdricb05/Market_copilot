"""alpha_agent.r63.acquire - bounded, free, PIT-stampable acquisitions.

Two sources, both free and official, both acquired under the estate's own
collector conventions, both recorded in an append-only manifest with the
acquisition instant and a sha256 per payload:

* ``sec_submissions``  per-issuer filing histories from data.sec.gov for the
                       PANEL-F CIKs (the survivorship-safe equity universe's
                       resolved issuers). The SEC requires a User-Agent that
                       carries a contact; the canonical collector resolves it
                       from ``git config user.email`` and R63 does exactly the
                       same, never hard-coding it. Rate: <= 8 requests/second.
* ``eia_natural_gas``  the EIA bulk NG.zip archive (working gas in storage).

Nothing here spends money, creates an account, accepts a licence or touches a
live store. A failed request is recorded and the source is reported as
partial coverage; nothing is filled.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd

from . import now_iso, research_root
from . import panels as P

CALCULATION_OWNER = "alpha_agent.r63.acquire"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/%s"
EIA_NG_URL = "https://api.eia.gov/bulk/NG.zip"
UA_PRODUCT = "paper-trader-alpha-agent/2.0"
MIN_INTERVAL_S = 0.13
HTTP_TIMEOUT = 60
KEEP_FORMS_PREFIX = ("8-K", "10-K", "10-Q", "NT ", "S-", "SC 13", "DEF 14A")
KEEP_FORMS_EXACT = ("3", "4", "5", "3/A", "4/A", "5/A")


def keep_form(form: str) -> bool:
    """Insider forms are matched EXACTLY (a '4' prefix would admit 424B2
    prospectuses); disclosure families are matched by prefix."""
    f = str(form)
    return f in KEEP_FORMS_EXACT or f.startswith(KEEP_FORMS_PREFIX)


def contact_email() -> str | None:
    try:
        out = subprocess.run(["git", "config", "user.email"], capture_output=True,
                             text=True, timeout=10)
        v = (out.stdout or "").strip()
        return v or None
    except Exception:                                    # noqa: BLE001
        return None


def _headers(contact: str) -> dict:
    return {"User-Agent": "%s %s" % (UA_PRODUCT, contact),
            "Accept-Encoding": "identity", "Accept": "application/json"}


def _get(url: str, headers: dict) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return int(resp.status), resp.read()
    except urllib.error.HTTPError as exc:
        return int(exc.code), b""
    except Exception:                                    # noqa: BLE001
        return 0, b""


def _manifest_path(sub: Path) -> Path:
    return sub / "acquisition_manifest.jsonl"


def _append_manifest(sub: Path, row: dict) -> None:
    sub.mkdir(parents=True, exist_ok=True)
    with open(_manifest_path(sub), "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, sort_keys=True) + "\n")


def _rows_from_submission(cik10: str, obj: dict, block: dict) -> list:
    forms = block.get("form") or []
    fdate = block.get("filingDate") or []
    adt = block.get("acceptanceDateTime") or []
    n = min(len(forms), len(fdate))
    out = []
    for i in range(n):
        f = str(forms[i])
        if not keep_form(f):
            continue
        out.append({"cik": cik10.lstrip("0") or "0", "form": f,
                    "filing_date": str(fdate[i])[:10],
                    "acceptance": (str(adt[i]) if i < len(adt) and adt[i] else None)})
    return out


def sec_submissions(*, ciks: list | None = None, max_ciks: int | None = None,
                    since: str = "2009-01-01", verbose: bool = True) -> dict:
    """Acquire filing histories for the PANEL-F CIKs (or ``ciks``)."""
    contact = contact_email()
    sub = P.submissions_dir()
    sub.mkdir(parents=True, exist_ok=True)
    if not contact:
        _append_manifest(sub, {"at": now_iso(), "state": "BLOCKED_MISSING_USER_AGENT_CONTACT"})
        return {"state": "BLOCKED_MISSING_USER_AGENT_CONTACT", "ciks": 0}
    if ciks is None:
        eq = P.load_equity()
        joined = set(eq["panel_f"]["meta"].get("joined_symbols") or [])
        ciks = sorted({eq["sym2cik"][s] for s in joined if s in eq["sym2cik"]})
    if max_ciks:
        ciks = ciks[:int(max_ciks)]
    headers = _headers(contact)
    rows_all, ok, failed, cached = [], 0, [], 0
    raw_dir = sub / "raw"
    raw_dir.mkdir(exist_ok=True)
    last = 0.0
    for k, cik in enumerate(ciks):
        cik10 = str(cik).zfill(10)
        raw_p = raw_dir / ("CIK%s.json.gz" % cik10)
        if raw_p.exists():
            try:
                obj = json.loads(gzip.open(raw_p, "rb").read().decode("utf-8"))
                cached += 1
            except Exception:                            # noqa: BLE001
                obj = None
        else:
            obj = None
        if obj is None:
            wait = MIN_INTERVAL_S - (time.time() - last)
            if wait > 0:
                time.sleep(wait)
            last = time.time()
            status, body = _get(SEC_SUBMISSIONS_URL % ("CIK%s.json" % cik10), headers)
            if status != 200 or not body:
                failed.append({"cik": cik10, "http": status})
                _append_manifest(sub, {"at": now_iso(), "cik": cik10, "http": status,
                                       "state": "FAILED"})
                continue
            try:
                obj = json.loads(body.decode("utf-8"))
            except ValueError:
                failed.append({"cik": cik10, "http": status, "why": "NOT_JSON"})
                continue
            # older filing blocks live in separate files
            older = []
            for f in ((obj.get("filings") or {}).get("files") or []):
                name = f.get("name")
                if not name:
                    continue
                if str(f.get("filingTo") or "9999") < since:
                    continue
                wait = MIN_INTERVAL_S - (time.time() - last)
                if wait > 0:
                    time.sleep(wait)
                last = time.time()
                st2, b2 = _get(SEC_SUBMISSIONS_URL % name, headers)
                if st2 == 200 and b2:
                    try:
                        older.append(json.loads(b2.decode("utf-8")))
                    except ValueError:
                        pass
            obj["_r63_older_blocks"] = older
            with gzip.open(raw_p, "wb") as fh:
                fh.write(json.dumps(obj).encode("utf-8"))
            _append_manifest(sub, {"at": now_iso(), "cik": cik10, "http": status,
                                   "sha256": hashlib.sha256(body).hexdigest(),
                                   "older_blocks": len(older), "state": "OK"})
        recent = (obj.get("filings") or {}).get("recent") or {}
        rows_all.extend(_rows_from_submission(cik10, obj, recent))
        for blk in obj.get("_r63_older_blocks") or []:
            rows_all.extend(_rows_from_submission(cik10, obj, blk))
        ok += 1
        if verbose and k % 50 == 0:
            print("sec_submissions %d/%d ok=%d failed=%d" % (k, len(ciks), ok, len(failed)),
                  flush=True)
    df = pd.DataFrame(rows_all, columns=["cik", "form", "filing_date", "acceptance"])
    if len(df):
        df = df[df["filing_date"] >= since]
        df = df.drop_duplicates().sort_values(["cik", "filing_date", "form"])
        df.to_csv(sub / "filings_index.csv", index=False)
    summary = {"state": "OK" if ok else "FAILED", "ciks_requested": len(ciks),
               "ciks_ok": ok, "ciks_cached": cached, "failed": failed[:50],
               "n_failed": len(failed), "rows": int(len(df)),
               "first": str(df["filing_date"].min()) if len(df) else None,
               "last": str(df["filing_date"].max()) if len(df) else None,
               "forms": df["form"].value_counts().head(20).to_dict() if len(df) else {},
               "acceptance_populated": float(df["acceptance"].notna().mean()) if len(df) else 0.0,
               "contact_masked": contact[:2] + "***" if contact else None,
               "user_agent_convention": "canonical SEC collector (git config user.email)",
               "acquired_at": now_iso(), "store": str(sub)}
    (sub / "acquisition_summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True),
                                                  encoding="utf-8")
    P._CACHE.pop("submissions", None)
    return summary


def eia_natural_gas() -> dict:
    d = research_root() / "_data_eia"
    d.mkdir(parents=True, exist_ok=True)
    out = d / "NG.zip"
    if out.exists():
        return {"state": "CACHED", "path": str(out), "bytes": out.stat().st_size}
    status, body = _get(EIA_NG_URL, {"User-Agent": UA_PRODUCT})
    if status != 200 or not body:
        _append_manifest(d, {"at": now_iso(), "url": EIA_NG_URL, "http": status,
                             "state": "FAILED"})
        return {"state": "FAILED", "http": status}
    out.write_bytes(body)
    _append_manifest(d, {"at": now_iso(), "url": EIA_NG_URL, "http": status,
                         "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
                         "state": "OK"})
    P._CACHE.pop("eia", None)
    cache = P.derived_dir() / "eia_weekly_stocks.csv"
    if cache.exists():
        cache.unlink()
    return {"state": "OK", "path": str(out), "bytes": len(body)}
