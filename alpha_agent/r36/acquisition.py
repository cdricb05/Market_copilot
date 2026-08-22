"""alpha_agent.r36.acquisition - the ONE Release 36 external acquisition owner.

This module declares what Release 36 needs from outside the estate and where it
comes from. It does NOT implement downloading: the HTTP primitive, its throttle,
its idempotent reuse and its record-a-failure-rather-than-raise behaviour are
:func:`alpha_agent.r35.acquisition.fetch`, which is called here with a
Release-36 destination. One HTTP owner, two manifests.

Two rules the module enforces rather than documents.

**Nothing already on disk is downloaded again.** Release 35 acquired the CFTC
Commitments of Traders archives (41 files, 44 MB), the EIA petroleum bulk file
(54 MB) and the Cboe volatility index histories. Those are INPUTS, not artifacts
of a release, and re-fetching them would burn bandwidth to change bytes that an
earlier immutable artifact was hashed against. They are LOCATED and recorded
with ``downloaded: False``, exactly as Release 35 located the owned SEC
financial statement data sets.

**Free and public only.** ``contract.MAY_SPEND_MONEY`` is False and there is no
code path here that authenticates to a paid entitlement, starts a trial, creates
an account or changes a subscription tier. The one credential used is a FRED key
this estate already holds, for a service whose data is free, and it never
reaches an artifact.
"""
from __future__ import annotations

import datetime as _dt
import os
import urllib.parse
from pathlib import Path
from typing import Optional

from .. import r36
from ..r35 import acquisition as _r35_acquisition
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r36.acquisition"
MANIFEST_SCHEMA = "r36_acquisition_manifest/1"
ARTIFACT_NAME = "acquisition_manifest.json"

SRC_EIA_NG = "EIA_NATURAL_GAS_BULK"
SRC_FRED = "FRED_ST_LOUIS_FED"
SRC_R35_CFTC = "REUSED_R35_CFTC_COMMITMENTS_OF_TRADERS"
SRC_R35_EIA_PET = "REUSED_R35_EIA_PETROLEUM_BULK"
SRC_R35_CBOE = "REUSED_R35_CBOE_VOLATILITY_INDICES"
SRC_NORGATE = "OWNED_NORGATE_LOCAL_DATABASES"

DOWNLOADED_SOURCES = (SRC_EIA_NG, SRC_FRED)
LOCATED_SOURCES = (SRC_R35_CFTC, SRC_R35_EIA_PET, SRC_R35_CBOE, SRC_NORGATE)
SOURCES = DOWNLOADED_SOURCES + LOCATED_SOURCES

SOURCE_LICENCE = {
    SRC_EIA_NG: "US Energy Information Administration, public domain bulk file",
    SRC_FRED: "Federal Reserve Bank of St. Louis, free API, key already held",
    SRC_R35_CFTC: "US Commodity Futures Trading Commission, public domain; "
                  "already on disk from Release 35",
    SRC_R35_EIA_PET: "US Energy Information Administration, public domain; "
                     "already on disk from Release 35",
    SRC_R35_CBOE: "Cboe Global Markets, freely published index history; "
                  "already on disk from Release 35",
    SRC_NORGATE: "already owned by this estate; served locally, nothing is "
                 "downloaded",
}


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def source_dir(source: str) -> Path:
    return r36.acquisition_root() / str(source).lower()


# --------------------------------------------------------------------------- #
# What Release 36 reads from FRED
# --------------------------------------------------------------------------- #
def fred_series_ids() -> tuple:
    """Every FRED series this release reads, derived from the contract.

    Derived rather than typed: a currency added to ``FX_UNIVERSE`` without its
    rate and price series being downloaded would silently drop out of the
    cross-section, and a hand-maintained list is exactly how that happens.
    """
    ids = {_contract.FX_BASE_SHORT_RATE, _contract.FX_BASE_CPI,
           _contract.CASH_YIELD_SERIES}
    for _code, spec in _contract.FX_UNIVERSE.items():
        ids.add(spec[2])
        ids.add(spec[3])
    ids.update(_contract.RATES_BREAKEVEN_SIGNAL)
    ids.update(c for c in _contract.CREDIT_SPREAD_SIGNALS
               if not c.startswith("%"))
    ids.update(_contract.CRYPTO_LEGS)
    return tuple(sorted(ids))


def _fred_key() -> Optional[str]:
    for name in _contract.FRED_API_KEY_ENV:
        value = os.environ.get(name)
        if value:
            return value
    return None


def acquire_fred(*, series_ids=None, transport=None) -> dict:
    """One JSON payload per FRED series. The key never reaches an artifact."""
    wanted = tuple(series_ids or fred_series_ids())
    key = _fred_key()
    if not key and transport is None:
        return {"source": SRC_FRED, "ok": False, "files": {}, "records": [],
                "reason": "FRED_API_KEY_ABSENT_FROM_SHELL_ENVIRONMENT",
                "licence": SOURCE_LICENCE[SRC_FRED]}
    records, files = [], {}
    for sid in wanted:
        query = urllib.parse.urlencode(
            {"series_id": sid, "api_key": key or "TEST", "file_type": "json",
             "observation_start": "1970-01-01"})
        url = "%s?%s" % (_contract.FRED_OBSERVATIONS_URL, query)
        dest = source_dir(SRC_FRED) / ("%s.json" % sid)
        record = _r35_acquisition.fetch(url, dest, min_bytes=64,
                                        transport=transport)
        record["url"] = "%s?series_id=%s&api_key=REDACTED&file_type=json" % (
            _contract.FRED_OBSERVATIONS_URL, sid)
        record["series_id"] = sid
        records.append(record)
        if record.get("ok"):
            files[sid] = str(dest)
    return {"source": SRC_FRED, "records": records, "files": files,
            "ok": len(files) == len(wanted),
            "series_requested": list(wanted),
            "series_acquired": sorted(files),
            "series_failed": sorted(set(wanted) - set(files)),
            "licence": SOURCE_LICENCE[SRC_FRED]}


def acquire_eia_natural_gas(*, transport=None) -> dict:
    """The EIA natural gas bulk archive, which carries dated NYMEX settlements.

    The petroleum archive Release 35 already holds carries crude, heating oil,
    gasoline and propane; natural gas lives in its own archive and is the fifth
    curve this release needs.
    """
    dest = source_dir(SRC_EIA_NG) / "NG.zip"
    record = _r35_acquisition.fetch(
        _contract.EIA_NATURAL_GAS_BULK_URL, dest, min_bytes=1 << 20,
        transport=transport)
    return {"source": SRC_EIA_NG, "records": [record],
            "files": {"NG": str(dest)} if record.get("ok") else {},
            "ok": bool(record.get("ok")),
            "licence": SOURCE_LICENCE[SRC_EIA_NG]}


# --------------------------------------------------------------------------- #
# Located, never downloaded
# --------------------------------------------------------------------------- #
def _locate_r35(source: str, r35_source: str) -> dict:
    """Point at a Release-35 payload directory without touching the network.

    The file map comes from Release 35's own ``cached_results``, so the keys are
    the ones ITS loaders expect - a CFTC archive is keyed by year and a Cboe
    history by index name, neither of which is the file stem. Deriving the keys
    here instead would be a second copy of a naming convention, and the first
    time the two disagreed a lane would silently report its source absent.
    """
    cached = _r35_acquisition.cached_results()
    row = cached.get(r35_source) or {}
    files = dict(row.get("files") or {})
    if not files:
        return {"source": source, "ok": False, "files": {}, "records": [],
                "downloaded": False,
                "reason": "R35_PAYLOAD_ABSENT:%s"
                          % _r35_acquisition.source_dir(r35_source),
                "licence": SOURCE_LICENCE[source]}
    records = []
    for path in sorted(Path(p) for p in files.values()):
        records.append({"url": None, "path": str(path), "ok": True,
                        "reused_existing": True,
                        "size_bytes": int(path.stat().st_size),
                        "sha256": r36.sha_file(path), "fetched_at": None})
    return {"source": source, "ok": True, "files": files,
            "records": records, "downloaded": False,
            "reused_from": "release35",
            "reused_root": str(_r35_acquisition.source_dir(r35_source)),
            "licence": SOURCE_LICENCE[source]}


def locate_r35_cftc() -> dict:
    return _locate_r35(SRC_R35_CFTC, _r35_acquisition.SRC_CFTC)


def locate_r35_eia_petroleum() -> dict:
    return _locate_r35(SRC_R35_EIA_PET, _r35_acquisition.SRC_EIA)


def locate_r35_cboe() -> dict:
    return _locate_r35(SRC_R35_CBOE, _r35_acquisition.SRC_CBOE)


def locate_norgate() -> dict:
    """Norgate is served locally by the Data Updater. Nothing is downloaded."""
    return {"source": SRC_NORGATE, "ok": True, "files": {}, "records": [],
            "downloaded": False,
            "served": "locally by the installed Norgate Data Updater",
            "install_or_upgrade_attempted": False,
            "licence": SOURCE_LICENCE[SRC_NORGATE]}


# --------------------------------------------------------------------------- #
# Orchestration + manifest
# --------------------------------------------------------------------------- #
def acquire_all(*, transport=None) -> dict:
    return {
        SRC_EIA_NG: acquire_eia_natural_gas(transport=transport),
        SRC_FRED: acquire_fred(transport=transport),
        SRC_R35_CFTC: locate_r35_cftc(),
        SRC_R35_EIA_PET: locate_r35_eia_petroleum(),
        SRC_R35_CBOE: locate_r35_cboe(),
        SRC_NORGATE: locate_norgate(),
    }


def cached_results() -> dict:
    """Rebuild the file map from disk with no network call at all."""
    out = {}
    for source in DOWNLOADED_SOURCES:
        base = source_dir(source)
        files, records = {}, []
        if base.exists():
            for path in sorted(base.iterdir()):
                if not path.is_file() or path.name.endswith(".part"):
                    continue
                files[path.stem] = str(path)
                records.append({"url": None, "path": str(path), "ok": True,
                                "reused_existing": True,
                                "size_bytes": int(path.stat().st_size),
                                "sha256": r36.sha_file(path),
                                "fetched_at": None})
        out[source] = {"source": source, "ok": bool(files), "files": files,
                       "records": records,
                       "licence": SOURCE_LICENCE[source]}
    out[SRC_R35_CFTC] = locate_r35_cftc()
    out[SRC_R35_EIA_PET] = locate_r35_eia_petroleum()
    out[SRC_R35_CBOE] = locate_r35_cboe()
    out[SRC_NORGATE] = locate_norgate()
    return out


def _summarise(result: dict) -> dict:
    records = result.get("records") or []
    failed = [{"url": r.get("url"), "reason": r.get("reason")}
              for r in records if not r.get("ok")]
    total = sum(int(r.get("size_bytes") or 0) for r in records if r.get("ok"))
    return {"source": result.get("source"),
            "ok": bool(result.get("ok")),
            "licence": result.get("licence"),
            "payloads": len(result.get("files") or {}),
            "payload_bytes": total,
            "downloaded": bool(result.get("downloaded", True)),
            "reused_from": result.get("reused_from"),
            "failures": failed[:20],
            "failure_count": len(failed),
            "reason": result.get("reason")}


def manifest_artifact(results: dict, *, campaign_id: str, created_at: str
                      ) -> dict:
    per_source = {name: _summarise(res) for name, res in sorted(results.items())}
    checksums = {}
    for name, res in sorted(results.items()):
        for record in (res.get("records") or []):
            if record.get("ok") and record.get("sha256"):
                checksums["%s::%s" % (name, Path(record["path"]).name)] = \
                    record["sha256"]
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "http_owner": _r35_acquisition.CALCULATION_OWNER,
        "acquisition_root": str(r36.acquisition_root()),
        "sources": per_source,
        "sources_downloaded": sorted(
            n for n, s in per_source.items() if s["downloaded"] and s["ok"]),
        "sources_located_not_downloaded": sorted(
            n for n, s in per_source.items() if not s["downloaded"]),
        "sources_failed": sorted(n for n, s in per_source.items()
                                 if not s["ok"]),
        "payload_checksums": checksums,
        "payload_count": len(checksums),
        "total_bytes": sum(s["payload_bytes"] for s in per_source.values()),
        "money_spent": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
        "subscription_tier_changed": False,
        "credentials_written_to_artifacts": False,
    }
    return r36.artifact_body(MANIFEST_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r36.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r36.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = _contract.CAMPAIGN_ID) -> Optional[dict]:
    return r36.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "SOURCES", "DOWNLOADED_SOURCES",
           "LOCATED_SOURCES", "SOURCE_LICENCE", "SRC_EIA_NG", "SRC_FRED",
           "SRC_R35_CFTC", "SRC_R35_EIA_PET", "SRC_R35_CBOE", "SRC_NORGATE",
           "fred_series_ids", "acquire_fred", "acquire_eia_natural_gas",
           "locate_r35_cftc", "locate_r35_eia_petroleum", "locate_r35_cboe",
           "locate_norgate", "acquire_all", "cached_results",
           "manifest_artifact", "source_dir", "freeze", "load", "path_for"]
