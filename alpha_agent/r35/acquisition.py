"""alpha_agent.r35.acquisition - the ONE Release 35 external acquisition owner.

Every byte of third-party data this release uses arrives through this module,
lands in ``r35.acquisition_root()`` under its source's own directory, and is
recorded in a manifest with its URL, its size, its SHA-256 and the moment it was
fetched. Nothing else in the package opens a socket.

Three rules the module enforces rather than documents:

* **Free and public only.** ``contract.MAY_SPEND_MONEY`` is False, and there is
  no code path here that authenticates to a paid entitlement, starts a trial or
  creates an account. The one credential used is a FRED API key that this estate
  already holds, for a service whose data is free.
* **Idempotent.** A payload already on disk with the recorded size is not
  downloaded again. Re-running the campaign does not re-fetch 40 archives, and -
  more importantly - cannot silently replace an input that an earlier artifact
  was hashed against.
* **A failure is recorded, not raised.** A source that 404s, rate-limits or
  disappears produces an ``ok: False`` entry with its reason. The campaign then
  reports that family as blocked. A release that dies on one dead URL learns
  nothing from the other five.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

from .. import r35
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r35.acquisition"
MANIFEST_SCHEMA = "r35_acquisition_manifest/1"
ARTIFACT_NAME = "acquisition_manifest.json"

SRC_CFTC = "CFTC_COMMITMENTS_OF_TRADERS"
SRC_FRED = "FRED_ST_LOUIS_FED"
SRC_CBOE = "CBOE_VOLATILITY_INDICES"
SRC_EIA = "EIA_PETROLEUM_BULK"
SRC_SEC_INSIDER = "SEC_INSIDER_TRANSACTIONS_DATA_SETS"
SRC_OWNED_FSDS = "OWNED_SEC_FINANCIAL_STATEMENT_DATA_SETS"

SOURCES = (SRC_CFTC, SRC_FRED, SRC_CBOE, SRC_EIA, SRC_SEC_INSIDER,
           SRC_OWNED_FSDS)

#: Free-and-public licence facts, recorded so a reader does not have to trust a
#: sentence in a document about what this release was allowed to download.
SOURCE_LICENCE = {
    SRC_CFTC: "US Commodity Futures Trading Commission, public domain",
    SRC_FRED: "Federal Reserve Bank of St. Louis, free API, key already held",
    SRC_CBOE: "Cboe Global Markets, freely published index history",
    SRC_EIA: "US Energy Information Administration, public domain bulk file",
    SRC_SEC_INSIDER: "US Securities and Exchange Commission, public domain",
    SRC_OWNED_FSDS: "already owned by this estate; nothing is downloaded",
}

_last_request_at = {"t": 0.0}


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def source_dir(source: str) -> Path:
    return r35.acquisition_root() / str(source).lower()


def _throttle() -> None:
    gap = time.time() - _last_request_at["t"]
    if gap < _contract.HTTP_MIN_INTERVAL_SECONDS:
        time.sleep(_contract.HTTP_MIN_INTERVAL_SECONDS - gap)
    _last_request_at["t"] = time.time()


def _request(url: str):
    return urllib.request.Request(
        url, headers={"User-Agent": _contract.HTTP_USER_AGENT,
                      "Accept-Encoding": "identity"})


def fetch(url: str, dest: Path, *, min_bytes: int = 512,
          transport=None) -> dict:
    """Download one payload to ``dest`` unless a plausible copy already exists.

    ``transport`` is injectable so the whole acquisition layer is testable with
    no network: it is called as ``transport(url) -> bytes``.
    """
    dest = Path(dest)
    record = {"url": url, "path": str(dest), "fetched_at": None}
    if dest.exists() and dest.stat().st_size >= min_bytes:
        record.update({"ok": True, "reused_existing": True,
                       "size_bytes": int(dest.stat().st_size),
                       "sha256": r35.sha_file(dest)})
        return record

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        if transport is not None:
            payload = transport(url)
            tmp.write_bytes(payload)
        else:
            _throttle()
            with urllib.request.urlopen(
                    _request(url),
                    timeout=_contract.HTTP_TIMEOUT_SECONDS) as response:
                with open(tmp, "wb") as handle:
                    while True:
                        chunk = response.read(1 << 20)
                        if not chunk:
                            break
                        handle.write(chunk)
    except (urllib.error.HTTPError, urllib.error.URLError, OSError,
            TimeoutError) as exc:
        try:
            tmp.unlink()
        except OSError:
            pass
        code = getattr(exc, "code", None)
        record.update({"ok": False,
                       "reason": ("HTTP_%s" % code) if code
                       else "%s: %s" % (type(exc).__name__, str(exc)[:160])})
        return record

    size = tmp.stat().st_size
    if size < min_bytes:
        tmp.unlink()
        record.update({"ok": False,
                       "reason": "PAYLOAD_TOO_SMALL:%d_bytes" % size})
        return record
    os.replace(tmp, dest)
    record.update({"ok": True, "reused_existing": False,
                   "size_bytes": int(size), "sha256": r35.sha_file(dest),
                   "fetched_at": _now()})
    return record


# --------------------------------------------------------------------------- #
# Per-source acquisition
# --------------------------------------------------------------------------- #
def acquire_cftc(*, first_year: int = _contract.CFTC_FIRST_YEAR,
                 last_year: Optional[int] = None, transport=None) -> dict:
    """The annual Commitments of Traders legacy futures-only archives.

    The LEGACY report is chosen over the disaggregated one deliberately: it runs
    from 1986 with ONE schema, where the disaggregated report starts in 2006.
    The release needs length and consistency more than it needs the dealer /
    asset-manager breakdown, and mixing the two would put a schema change in the
    middle of the sample.
    """
    last = int(last_year or _dt.date.today().year)
    out, files = [], {}
    for year in range(int(first_year), last + 1):
        url = _contract.CFTC_COT_URL.format(year=year)
        dest = source_dir(SRC_CFTC) / ("deacot%d.zip" % year)
        rec = fetch(url, dest, transport=transport)
        rec["year"] = year
        out.append(rec)
        if rec.get("ok"):
            files[str(year)] = str(dest)
    ok_years = sorted(int(y) for y in files)
    return {"source": SRC_CFTC, "records": out, "files": files,
            "ok": bool(files),
            "years_acquired": ok_years,
            "first_year": ok_years[0] if ok_years else None,
            "last_year": ok_years[-1] if ok_years else None,
            "licence": SOURCE_LICENCE[SRC_CFTC]}


def _fred_key() -> Optional[str]:
    for name in _contract.FRED_API_KEY_ENV:
        value = os.environ.get(name)
        if value:
            return value
    return None


def fred_series_ids() -> tuple:
    """Every FRED series this release reads, declared in one place."""
    ids = ["DGS2", "DGS10", "DGS30", "DFII10", "T10YIE", "BAA10Y",
           _contract.FRED_US_SHORT_RATE]
    ids.extend(_contract.FRED_FOREIGN_SHORT_RATES.values())
    return tuple(sorted(set(ids)))


def acquire_fred(*, series_ids=None, transport=None) -> dict:
    """Daily and monthly FRED observations, one JSON payload per series.

    No credential is created here. The key this reads is one the estate already
    holds for a service whose data is free; its absence is a recorded blocker,
    not a prompt.
    """
    key = _fred_key()
    if not key and transport is None:
        return {"source": SRC_FRED, "ok": False, "files": {}, "records": [],
                "reason": "FRED_API_KEY_ABSENT_FROM_SHELL_ENVIRONMENT",
                "licence": SOURCE_LICENCE[SRC_FRED]}
    out, files = [], {}
    for sid in (series_ids or fred_series_ids()):
        query = urllib.parse.urlencode(
            {"series_id": sid, "api_key": key or "TEST", "file_type": "json",
             "observation_start": "1985-01-01"})
        url = "%s?%s" % (_contract.FRED_OBSERVATIONS_URL, query)
        dest = source_dir(SRC_FRED) / ("%s.json" % sid)
        rec = fetch(url, dest, min_bytes=64, transport=transport)
        # The key must never reach an artifact.
        rec["url"] = "%s?series_id=%s&api_key=REDACTED&file_type=json" % (
            _contract.FRED_OBSERVATIONS_URL, sid)
        rec["series_id"] = sid
        out.append(rec)
        if rec.get("ok"):
            files[sid] = str(dest)
    return {"source": SRC_FRED, "records": out, "files": files,
            "ok": len(files) == len(series_ids or fred_series_ids()),
            "series_acquired": sorted(files),
            "licence": SOURCE_LICENCE[SRC_FRED]}


def acquire_cboe(*, transport=None) -> dict:
    """VIX and VIX3M daily history: the two legs of the term-structure slope."""
    wanted = {"VIX": _contract.CBOE_VIX_URL,
              "VIX3M": _contract.CBOE_VIX3M_URL}
    out, files = [], {}
    for name, url in sorted(wanted.items()):
        dest = source_dir(SRC_CBOE) / ("%s_History.csv" % name)
        rec = fetch(url, dest, transport=transport)
        rec["index"] = name
        out.append(rec)
        if rec.get("ok"):
            files[name] = str(dest)
    return {"source": SRC_CBOE, "records": out, "files": files,
            "ok": len(files) == len(wanted),
            "licence": SOURCE_LICENCE[SRC_CBOE]}


def acquire_eia(*, transport=None) -> dict:
    """The EIA petroleum bulk archive, which carries dated NYMEX settlements.

    The futures-contract series inside it END on 2024-04-05 because EIA
    discontinued republishing NYMEX settlements, not because this download is
    stale. The coverage report states the end date; nothing extrapolates past
    it.
    """
    dest = source_dir(SRC_EIA) / "PET.zip"
    rec = fetch(_contract.EIA_PETROLEUM_BULK_URL, dest,
                min_bytes=1 << 20, transport=transport)
    return {"source": SRC_EIA, "records": [rec],
            "files": {"PET": str(dest)} if rec.get("ok") else {},
            "ok": bool(rec.get("ok")),
            "licence": SOURCE_LICENCE[SRC_EIA]}


def insider_quarters(*, first=_contract.SEC_INSIDER_FIRST,
                     today: Optional[_dt.date] = None) -> list:
    """Every (year, quarter) the SEC insider data sets could plausibly hold."""
    today = today or _dt.date.today()
    year, quarter = int(first[0]), int(first[1])
    out = []
    while (year, quarter) <= (today.year, (today.month - 1) // 3 + 1):
        out.append((year, quarter))
        quarter += 1
        if quarter > 4:
            year, quarter = year + 1, 1
    return out


def acquire_sec_insider(*, quarters=None, transport=None) -> dict:
    """Quarterly Form 3/4/5 structured data sets.

    The most recent quarter is routinely absent for weeks after it ends; a 404
    there is normal and is recorded as ``ok: False`` without failing the source.
    """
    out, files = [], {}
    for year, quarter in (quarters or insider_quarters()):
        url = _contract.SEC_INSIDER_URL.format(year=year, quarter=quarter)
        dest = source_dir(SRC_SEC_INSIDER) / ("%dq%d_form345.zip"
                                              % (year, quarter))
        rec = fetch(url, dest, min_bytes=1 << 14, transport=transport)
        rec.update({"year": year, "quarter": quarter})
        out.append(rec)
        if rec.get("ok"):
            files["%dq%d" % (year, quarter)] = str(dest)
    return {"source": SRC_SEC_INSIDER, "records": out, "files": files,
            "ok": bool(files), "quarters_acquired": sorted(files),
            "licence": SOURCE_LICENCE[SRC_SEC_INSIDER]}


def locate_owned_fsds(root: Optional[str] = None) -> dict:
    """The already-owned Financial Statement Data Sets, which are NOT fetched.

    They supply the point-in-time SIC classification the insider family needs.
    Downloading them again would be both wasteful and a second owner.
    """
    base = Path(root or _contract.OWNED_FSDS_ROOT)
    if not base.exists():
        return {"source": SRC_OWNED_FSDS, "ok": False, "files": {},
                "reason": "OWNED_FSDS_ROOT_ABSENT:%s" % base,
                "licence": SOURCE_LICENCE[SRC_OWNED_FSDS]}
    files = {}
    for quarter_dir in sorted(p for p in base.iterdir() if p.is_dir()):
        member = quarter_dir / "sub.txt"
        if member.exists():
            files[quarter_dir.name] = str(member)
    return {"source": SRC_OWNED_FSDS, "ok": bool(files), "files": files,
            "quarters": sorted(files), "downloaded": False,
            "licence": SOURCE_LICENCE[SRC_OWNED_FSDS]}


# --------------------------------------------------------------------------- #
# Manifest
# --------------------------------------------------------------------------- #
def acquire_all(*, transport=None, insider_quarter_list=None,
                cftc_last_year: Optional[int] = None) -> dict:
    """Run every acquisition and return one result keyed by source."""
    return {
        SRC_CFTC: acquire_cftc(last_year=cftc_last_year, transport=transport),
        SRC_FRED: acquire_fred(transport=transport),
        SRC_CBOE: acquire_cboe(transport=transport),
        SRC_EIA: acquire_eia(transport=transport),
        SRC_SEC_INSIDER: acquire_sec_insider(
            quarters=insider_quarter_list, transport=transport),
        SRC_OWNED_FSDS: locate_owned_fsds(),
    }


def _summarise(result: dict) -> dict:
    records = result.get("records") or []
    failed = [{"url": r.get("url"), "reason": r.get("reason")}
              for r in records if not r.get("ok")]
    total = sum(int(r.get("size_bytes") or 0) for r in records if r.get("ok"))
    return {"source": result.get("source"),
            "ok": bool(result.get("ok")),
            "licence": result.get("licence"),
            "payloads_acquired": len(result.get("files") or {}),
            "payload_bytes": total,
            "downloaded": bool(result.get("downloaded", True)),
            "failures": failed[:20],
            "failure_count": len(failed),
            "reason": result.get("reason")}


def manifest_artifact(results: dict, *, campaign_id: str, created_at: str
                      ) -> dict:
    """The immutable record of what was acquired, from where, and at what hash."""
    per_source = {name: _summarise(res) for name, res in sorted(results.items())}
    checksums = {}
    for name, res in sorted(results.items()):
        for rec in (res.get("records") or []):
            if rec.get("ok") and rec.get("sha256"):
                checksums["%s::%s" % (name, Path(rec["path"]).name)] = \
                    rec["sha256"]
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "acquisition_root": str(r35.acquisition_root()),
        "sources": per_source,
        "sources_ok": sorted(n for n, s in per_source.items() if s["ok"]),
        "sources_failed": sorted(n for n, s in per_source.items()
                                 if not s["ok"]),
        "payload_checksums": checksums,
        "payload_count": len(checksums),
        "total_bytes": sum(s["payload_bytes"] for s in per_source.values()),
        "money_spent": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
        "credentials_written_to_artifacts": False,
    }
    return r35.artifact_body(MANIFEST_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r35.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r35.write_json(path_for(body.get("campaign_id",
                                            _contract.CAMPAIGN_ID)), body)


def load(campaign_id: str = _contract.CAMPAIGN_ID) -> Optional[dict]:
    return r35.read_json(path_for(campaign_id))


def _cache_key(source: str, path: Path) -> str:
    """The key a loader expects, which is not always the file stem.

    ``load_cot`` is keyed by YEAR and ``load_insider_filings`` by YYYYqQ,
    because those are the units the sources are published in. Deriving the key
    here keeps a re-run off the network from disagreeing with the run that
    downloaded the files.
    """
    stem = path.stem
    if source == SRC_CFTC and stem.startswith("deacot"):
        return stem[len("deacot"):]
    if source == SRC_SEC_INSIDER and "_form345" in stem:
        return stem.split("_form345")[0]
    if source == SRC_CBOE and stem.endswith("_History"):
        return stem[:-len("_History")]
    return stem


def cached_results(campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    """Rebuild the file map from what is on disk, without any network call.

    The records carry the same size and content hash a live acquisition would
    record, so an offline re-run produces a manifest that says what it actually
    read rather than an empty one that says nothing.
    """
    out = {}
    for source in SOURCES:
        if source == SRC_OWNED_FSDS:
            out[source] = locate_owned_fsds()
            continue
        base = source_dir(source)
        files, records = {}, []
        if base.exists():
            for path in sorted(base.iterdir()):
                if not path.is_file() or path.name.endswith(".part"):
                    continue
                files[_cache_key(source, path)] = str(path)
                records.append({"url": None, "path": str(path), "ok": True,
                                "reused_existing": True,
                                "size_bytes": int(path.stat().st_size),
                                "sha256": r35.sha_file(path),
                                "fetched_at": None})
        out[source] = {"source": source, "ok": bool(files), "files": files,
                       "records": records, "licence": SOURCE_LICENCE[source]}
    return out


__all__ = ["CALCULATION_OWNER", "SOURCES", "SOURCE_LICENCE", "SRC_CFTC",
           "SRC_FRED", "SRC_CBOE", "SRC_EIA", "SRC_SEC_INSIDER",
           "SRC_OWNED_FSDS", "fetch", "acquire_cftc", "acquire_fred",
           "acquire_cboe", "acquire_eia", "acquire_sec_insider",
           "locate_owned_fsds", "acquire_all", "manifest_artifact",
           "insider_quarters", "fred_series_ids", "cached_results",
           "source_dir", "freeze", "load", "path_for"]
