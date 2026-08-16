"""alpha_agent/sec_financial_statement_sets.py — Stage 26 acquisition of the free
official **SEC Financial Statement Data Sets**, for the one artefact Stage 25
named as the number-one item in the research queue: a **per-filing,
effective-dated** assigned-SIC series.

Stage 25 could classify an issuer two ways and neither was good enough:

* Tier A (`PIT_XBRL_DISCLOSURE_SIGNATURE`) is leakage-safe but coarse — it
  resolves *business model* (bank / insurer / REIT / operating), never the
  Technology-versus-Industrials boundary the R&D question turns on;
* Tier B (`ENTITY_SIC_SNAPSHOT_CONTROL`) is fine-grained but is **today's**
  entity-level SIC, so every verdict resting on it is stamped provisional.

`sub.txt`, one member of each quarterly Financial Statement Data Set, carries
**per submission** both the assigned ``sic`` and the ``accepted`` timestamp.
That pair is exactly the ``(classification, available_at)`` observation that
:class:`alpha_agent.pit_sector.PitSicSeries` already consumes, so this module
acquires and parses; it does not invent a second classification mechanism.

**Bandwidth discipline.** The full quarterly archive averages ~50 MB and the
overwhelming majority of it is ``num.txt``, which this stage does not need.
Rather than mirror ~3.4 GB to read ~200 MB, the fetcher reads the remote zip's
central directory over HTTP Range and then pulls **only the ``sub.txt``
member** — three small requests per quarter. SEC fair-access rules are
respected with an identifying User-Agent and a configurable inter-request
delay, and every acquired member is content-hashed and cached, so a re-run
re-downloads nothing.

Pure standard library. No provider key, no quota, no vendor. Research-only:
this module writes under its configured cache root and nowhere else, and has no
code path to an order, a fill, a holding or any operational store.
"""
from __future__ import annotations

import hashlib
import json
import os
import struct
import time
import zlib
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

STAGE = "26"
CONTRACT_VERSION = "stage26-sec-fsds-1.0.0"

#: Official first-party source. The quarterly archives begin 2009Q2 — the first
#: quarter of mandatory XBRL — which is also where owned company-facts history
#: begins, so the two surfaces share a start date rather than needing a join.
SOURCE_HOST = "www.sec.gov"
SOURCE_PATH_TEMPLATE = "/files/dera/data/financial-statement-data-sets/%dq%d.zip"
FIRST_YEAR = 2009
FIRST_QUARTER = 2
MEMBER_NAME = "sub.txt"

# Dispositions.
D_COMPLETE = "COMPLETE"
D_CACHED = "CACHED"
D_NOT_PUBLISHED = "QUARTER_NOT_PUBLISHED"
D_MEMBER_MISSING = "MEMBER_NOT_IN_ARCHIVE"
D_TRANSPORT_ERROR = "TRANSPORT_ERROR"
D_RANGE_UNSUPPORTED = "RANGE_REQUESTS_UNSUPPORTED"

# Zip signatures.
_SIG_EOCD = b"PK\x05\x06"
_SIG_EOCD64_LOCATOR = b"PK\x06\x07"
_SIG_EOCD64 = b"PK\x06\x06"
_SIG_CENTRAL = b"PK\x01\x02"
_SIG_LOCAL = b"PK\x03\x04"

_EOCD_MAX_COMMENT = 65535
_TAIL_BYTES = 66560  # 64 KB + EOCD; enough to hold any legal comment


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(
        microsecond=0, tzinfo=None).isoformat() + "Z"


def quarter_url(year: int, quarter: int) -> str:
    return "https://%s%s" % (SOURCE_HOST, SOURCE_PATH_TEMPLATE % (year, quarter))


def source_identifier(year: int, quarter: int) -> str:
    """Stable host+path identity for provenance. The transient scheme is not part
    of the business identity of the artefact."""
    return "%s%s" % (SOURCE_HOST, SOURCE_PATH_TEMPLATE % (year, quarter))


def quarters_through(year: int, quarter: int, *, start_year: int = FIRST_YEAR,
                     start_quarter: int = FIRST_QUARTER) -> "list[tuple[int, int]]":
    """Every published quarter from the XBRL start through ``(year, quarter)``."""
    out: list[tuple[int, int]] = []
    y, q = int(start_year), int(start_quarter)
    while (y, q) <= (int(year), int(quarter)):
        out.append((y, q))
        q += 1
        if q > 4:
            y, q = y + 1, 1
    return out


def _default_transport() -> Callable:
    from .collectors.base import default_transport
    return default_transport


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


# --------------------------------------------------------------------------- #
# Remote partial-zip member reader
# --------------------------------------------------------------------------- #
class RemoteZipMemberFetcher:
    """Read ONE member out of a remote zip archive using HTTP Range requests.

    Three bounded requests replace a whole-archive download: the tail (to locate
    the central directory), the central directory (to locate the member) and the
    member's own byte range. The archive is never held in memory in full and
    never written to disk.
    """

    def __init__(self, url: str, *, headers: Optional[dict] = None,
                 transport: Optional[Callable] = None, timeout: float = 60.0):
        self.url = url
        self.headers = dict(headers or {})
        self.timeout = float(timeout)
        self._transport = transport or _default_transport()
        self._total: Optional[int] = None
        self._last_modified: Optional[str] = None
        self._etag: Optional[str] = None
        self.requests = 0
        self.bytes_fetched = 0

    # -- transport ---------------------------------------------------------- #
    def _request(self, method: str, extra: Optional[dict] = None) -> dict:
        hdrs = {**self.headers, **(extra or {})}
        self.requests += 1
        resp = self._transport({"method": method, "url": self.url,
                                "headers": hdrs}, self.timeout)
        return resp or {}

    def head(self) -> dict:
        resp = self._request("HEAD")
        status = resp.get("status")
        hdrs = {str(k).lower(): v for k, v in (resp.get("headers") or {}).items()}
        clen = hdrs.get("content-length")
        total = int(clen) if clen and str(clen).isdigit() else None
        self._total = total
        self._last_modified = hdrs.get("last-modified")
        self._etag = hdrs.get("etag")
        return {"status": status, "total_bytes": total,
                "accept_ranges": hdrs.get("accept-ranges"),
                "last_modified": self._last_modified, "etag": self._etag}

    def _range(self, lo: int, hi: int) -> bytes:
        """Inclusive byte range ``[lo, hi]``."""
        resp = self._request("GET", {"Range": "bytes=%d-%d" % (lo, hi)})
        if resp.get("status") not in (200, 206):
            raise TransportFailure("http_%s" % resp.get("status"))
        body = resp.get("body") or b""
        self.bytes_fetched += len(body)
        return body

    # -- zip structure ------------------------------------------------------- #
    def _central_directory(self) -> bytes:
        total = self._total
        if total is None:
            raise TransportFailure("no Content-Length; cannot range-read")
        tail_len = min(_TAIL_BYTES, total)
        tail = self._range(total - tail_len, total - 1)
        idx = tail.rfind(_SIG_EOCD)
        if idx < 0:
            raise MalformedArchive("no end-of-central-directory record in tail")
        eocd = tail[idx:idx + 22]
        cd_size, cd_offset = struct.unpack("<II", eocd[12:20])
        # ZIP64 escape: 0xFFFFFFFF sentinels mean the real values live in the
        # ZIP64 end-of-central-directory record located via its locator.
        if cd_size == 0xFFFFFFFF or cd_offset == 0xFFFFFFFF:
            lidx = tail.rfind(_SIG_EOCD64_LOCATOR, 0, idx)
            if lidx < 0:
                raise MalformedArchive("ZIP64 sentinel without a locator")
            (eocd64_off,) = struct.unpack("<Q", tail[lidx + 8:lidx + 16])
            rec = self._range(eocd64_off, eocd64_off + 55)
            if rec[:4] != _SIG_EOCD64:
                raise MalformedArchive("ZIP64 end-of-central-directory not found")
            cd_size, cd_offset = struct.unpack("<QQ", rec[40:56])
        if cd_offset >= total - tail_len:
            # The central directory is already inside the tail we hold.
            start = cd_offset - (total - tail_len)
            return tail[start:start + cd_size]
        return self._range(cd_offset, cd_offset + cd_size - 1)

    @staticmethod
    def _iter_central_entries(cd: bytes) -> "Iterable[dict]":
        pos = 0
        n = len(cd)
        while pos + 46 <= n and cd[pos:pos + 4] == _SIG_CENTRAL:
            method = struct.unpack("<H", cd[pos + 10:pos + 12])[0]
            csize, usize = struct.unpack("<II", cd[pos + 20:pos + 28])
            fn_len, extra_len, cmt_len = struct.unpack(
                "<HHH", cd[pos + 28:pos + 34])
            local_off = struct.unpack("<I", cd[pos + 42:pos + 46])[0]
            name = cd[pos + 46:pos + 46 + fn_len].decode("utf-8", "replace")
            extra = cd[pos + 46 + fn_len:pos + 46 + fn_len + extra_len]
            if csize == 0xFFFFFFFF or usize == 0xFFFFFFFF or \
                    local_off == 0xFFFFFFFF:
                usize, csize, local_off = _zip64_extra(
                    extra, usize, csize, local_off)
            yield {"name": name, "method": method, "compressed_size": csize,
                   "uncompressed_size": usize, "local_header_offset": local_off}
            pos += 46 + fn_len + extra_len + cmt_len

    def fetch_member(self, member: str) -> "tuple[bytes, dict]":
        """Return ``(decompressed_bytes, entry_metadata)`` for ONE member."""
        cd = self._central_directory()
        entry = None
        for e in self._iter_central_entries(cd):
            if e["name"].lower() == member.lower() or \
                    e["name"].rsplit("/", 1)[-1].lower() == member.lower():
                entry = e
                break
        if entry is None:
            raise MemberNotFound(member)
        off = entry["local_header_offset"]
        header = self._range(off, off + 29)
        if header[:4] != _SIG_LOCAL:
            raise MalformedArchive("local header signature missing")
        fn_len, extra_len = struct.unpack("<HH", header[26:30])
        data_start = off + 30 + fn_len + extra_len
        csize = int(entry["compressed_size"])
        raw = self._range(data_start, data_start + csize - 1)
        method = int(entry["method"])
        if method == 0:
            payload = raw
        elif method == 8:
            payload = zlib.decompress(raw, -zlib.MAX_WBITS)
        else:
            raise MalformedArchive("unsupported compression method %d" % method)
        meta = {**entry, "data_start": data_start,
                "last_modified": self._last_modified, "etag": self._etag,
                "archive_total_bytes": self._total,
                "bytes_fetched": self.bytes_fetched, "requests": self.requests}
        return payload, meta

    def list_members(self) -> "list[dict]":
        """Central-directory listing, without transferring any member payload."""
        return list(self._iter_central_entries(self._central_directory()))


def _zip64_extra(extra: bytes, usize: int, csize: int,
                 local_off: int) -> "tuple[int, int, int]":
    """Resolve 0xFFFFFFFF sentinels from the ZIP64 extended-information field."""
    pos = 0
    while pos + 4 <= len(extra):
        hid, hsz = struct.unpack("<HH", extra[pos:pos + 4])
        body = extra[pos + 4:pos + 4 + hsz]
        if hid == 0x0001:
            cur = 0
            if usize == 0xFFFFFFFF and cur + 8 <= len(body):
                usize = struct.unpack("<Q", body[cur:cur + 8])[0]
                cur += 8
            if csize == 0xFFFFFFFF and cur + 8 <= len(body):
                csize = struct.unpack("<Q", body[cur:cur + 8])[0]
                cur += 8
            if local_off == 0xFFFFFFFF and cur + 8 <= len(body):
                local_off = struct.unpack("<Q", body[cur:cur + 8])[0]
            break
        pos += 4 + hsz
    return usize, csize, local_off


class TransportFailure(RuntimeError):
    """A range request could not be completed."""


class MalformedArchive(ValueError):
    """The remote archive did not parse as a zip."""


class MemberNotFound(KeyError):
    """The requested member is not present in the archive's central directory."""


# --------------------------------------------------------------------------- #
# Acquisition (cached, content-hashed, idempotent)
# --------------------------------------------------------------------------- #
class FinancialStatementDataSetsAcquirer:
    """Acquire and cache the ``sub.txt`` member of each quarterly data set.

    A completed quarter is never re-fetched: its manifest records the member's
    SHA-256 and the archive's ``Last-Modified``/``ETag``, and a cached member
    whose bytes still hash to the recorded digest short-circuits the network
    entirely.
    """

    def __init__(self, *, cache_root: "str | Path", user_agent: str,
                 transport: Optional[Callable] = None, timeout: float = 60.0,
                 request_delay_seconds: float = 0.35,
                 sleep: Optional[Callable] = None, clock: Optional[Callable] = None):
        if not user_agent or "@" not in user_agent:
            raise ValueError(
                "SEC fair access requires an identifying User-Agent carrying a "
                "contact address; refusing to fetch without one")
        self.cache_root = Path(cache_root)
        self.user_agent = user_agent
        self.transport = transport
        self.timeout = float(timeout)
        self.request_delay_seconds = float(request_delay_seconds)
        self._sleep = sleep or time.sleep
        self._clock = clock or _utc_now_iso

    # -- paths -------------------------------------------------------------- #
    def quarter_dir(self, year: int, quarter: int) -> Path:
        return self.cache_root / ("%dq%d" % (int(year), int(quarter)))

    def member_path(self, year: int, quarter: int) -> Path:
        return self.quarter_dir(year, quarter) / MEMBER_NAME

    def manifest_path(self, year: int, quarter: int) -> Path:
        return self.quarter_dir(year, quarter) / "manifest.json"

    # -- one quarter --------------------------------------------------------- #
    def cached(self, year: int, quarter: int) -> Optional[dict]:
        """Return the manifest when a verified cached member exists, else None."""
        mp, fp = self.manifest_path(year, quarter), self.member_path(year, quarter)
        if not (mp.exists() and fp.exists()):
            return None
        try:
            man = json.loads(mp.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        digest = sha256_hex(fp.read_bytes())
        if digest != man.get("member_sha256"):
            return None
        return {**man, "disposition": D_CACHED}

    def acquire_quarter(self, year: int, quarter: int, *,
                        force: bool = False) -> dict:
        """Fetch (or reuse) ``sub.txt`` for ONE quarter."""
        if not force:
            hit = self.cached(year, quarter)
            if hit is not None:
                return hit
        url = quarter_url(year, quarter)
        fetcher = RemoteZipMemberFetcher(
            url, headers={"User-Agent": self.user_agent,
                          "Accept-Encoding": "identity"},
            transport=self.transport, timeout=self.timeout)
        head = fetcher.head()
        if head.get("status") == 404:
            return {"disposition": D_NOT_PUBLISHED, "year": year,
                    "quarter": quarter, "source": source_identifier(year, quarter)}
        if head.get("total_bytes") is None:
            return {"disposition": D_TRANSPORT_ERROR, "year": year,
                    "quarter": quarter, "reason": "no Content-Length",
                    "status": head.get("status"),
                    "source": source_identifier(year, quarter)}
        if str(head.get("accept_ranges") or "").lower() != "bytes":
            return {"disposition": D_RANGE_UNSUPPORTED, "year": year,
                    "quarter": quarter,
                    "reason": "server did not advertise byte ranges; a partial "
                              "member read is not possible",
                    "source": source_identifier(year, quarter)}
        self._sleep(self.request_delay_seconds)
        try:
            payload, meta = fetcher.fetch_member(MEMBER_NAME)
        except MemberNotFound:
            return {"disposition": D_MEMBER_MISSING, "year": year,
                    "quarter": quarter, "member": MEMBER_NAME,
                    "source": source_identifier(year, quarter)}
        except (TransportFailure, MalformedArchive, zlib.error) as exc:
            return {"disposition": D_TRANSPORT_ERROR, "year": year,
                    "quarter": quarter, "reason": "%s: %s" % (
                        type(exc).__name__, exc),
                    "source": source_identifier(year, quarter)}
        self._sleep(self.request_delay_seconds)

        d = self.quarter_dir(year, quarter)
        d.mkdir(parents=True, exist_ok=True)
        digest = sha256_hex(payload)
        tmp = self.member_path(year, quarter).with_suffix(".txt.tmp")
        tmp.write_bytes(payload)
        os.replace(tmp, self.member_path(year, quarter))
        manifest = {
            "contract_version": CONTRACT_VERSION,
            "year": int(year), "quarter": int(quarter),
            "source": source_identifier(year, quarter),
            "member": MEMBER_NAME,
            "member_bytes": len(payload),
            "member_sha256": digest,
            "archive_total_bytes": meta.get("archive_total_bytes"),
            "archive_last_modified": meta.get("last_modified"),
            "archive_etag": meta.get("etag"),
            "bytes_fetched_over_network": meta.get("bytes_fetched"),
            "http_requests": meta.get("requests"),
            "retrieved_at_utc": self._clock(),
            "member_path": str(self.member_path(year, quarter)),
            "license_note": "US federal government work; SEC EDGAR public data, "
                            "free for research use under SEC fair-access rules",
        }
        mtmp = self.manifest_path(year, quarter).with_suffix(".json.tmp")
        mtmp.write_text(json.dumps(manifest, indent=1, sort_keys=True),
                        encoding="utf-8")
        os.replace(mtmp, self.manifest_path(year, quarter))
        return {**manifest, "disposition": D_COMPLETE}

    # -- many quarters ------------------------------------------------------- #
    def acquire(self, quarters: "Iterable[tuple[int, int]]", *,
                force: bool = False, stop_after_not_published: int = 2) -> dict:
        """Acquire a sequence of quarters, tolerating an unpublished tail.

        The most recent quarter is published on a lag, so the caller may pass a
        range that runs past the end; ``stop_after_not_published`` consecutive
        404s end the sweep cleanly rather than hammering the host.
        """
        results: list[dict] = []
        misses = 0
        for (y, q) in quarters:
            res = self.acquire_quarter(y, q, force=force)
            results.append(res)
            if res.get("disposition") == D_NOT_PUBLISHED:
                misses += 1
                if misses >= int(stop_after_not_published):
                    break
            else:
                misses = 0
        ok = [r for r in results
              if r.get("disposition") in (D_COMPLETE, D_CACHED)]
        return {
            "contract_version": CONTRACT_VERSION,
            "quarters_requested": len(results),
            "quarters_acquired": len(ok),
            "quarters_downloaded": len([r for r in results
                                        if r.get("disposition") == D_COMPLETE]),
            "quarters_from_cache": len([r for r in results
                                        if r.get("disposition") == D_CACHED]),
            "total_member_bytes": sum(int(r.get("member_bytes") or 0) for r in ok),
            "total_network_bytes": sum(
                int(r.get("bytes_fetched_over_network") or 0) for r in results),
            "results": results,
            "failures": [r for r in results
                         if r.get("disposition") not in
                         (D_COMPLETE, D_CACHED, D_NOT_PUBLISHED)],
        }


# --------------------------------------------------------------------------- #
# sub.txt parsing -> point-in-time assigned-SIC observations
# --------------------------------------------------------------------------- #
#: Columns this stage depends on. `sub.txt` carries ~36 columns; naming the ones
#: we consume makes a schema change loud instead of silent.
REQUIRED_COLUMNS = ("adsh", "cik", "name", "sic", "form", "period", "filed",
                    "accepted")


def parse_sub_txt(payload: "bytes | str") -> "list[dict]":
    """Parse a ``sub.txt`` member into per-submission dicts.

    The file is tab-separated with a header row. Rows are returned verbatim
    (string-valued) apart from the columns this module normalises; a row missing
    a required column is skipped rather than guessed at.
    """
    if isinstance(payload, bytes):
        text = payload.decode("utf-8", "replace")
    else:
        text = payload
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if not lines or not lines[0].strip():
        return []
    header = [h.strip().lower() for h in lines[0].split("\t")]
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise SchemaChanged(
            "sub.txt is missing required column(s): %s" % ", ".join(missing))
    idx = {c: header.index(c) for c in header}
    out: list[dict] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < len(header):
            parts = parts + [""] * (len(header) - len(parts))
        row = {c: parts[i].strip() for c, i in idx.items()}
        out.append(row)
    return out


class SchemaChanged(ValueError):
    """The official file no longer carries a column this stage depends on."""


def _norm_accepted(value: str) -> Optional[str]:
    """``accepted`` is ``YYYY-MM-DD HH:MM:SS(.f)``; return an ISO-8601 string."""
    s = (value or "").strip()
    if len(s) < 10:
        return None
    s = s.replace(" ", "T")
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _norm_filed(value: str) -> Optional[str]:
    """``filed`` is ``YYYYMMDD``; return ``YYYY-MM-DD``."""
    s = "".join(ch for ch in (value or "") if ch.isdigit())
    if len(s) != 8:
        return None
    return "%s-%s-%s" % (s[:4], s[4:6], s[6:8])


def sic_observations(rows: "Iterable[dict]", *, source: str = "",
                     quarter: str = "") -> "list[dict]":
    """Project parsed ``sub.txt`` rows onto ``(cik, sic, available_at)``.

    ``available_at`` is the SEC **acceptance** timestamp — the moment the filing,
    and with it the header SIC the agency had assigned at that moment, became
    publicly observable. Where acceptance is absent the filed date is used and
    the row is labelled accordingly, so the weaker timestamp is never silently
    mixed with the stronger one. A row without a usable SIC or without any
    availability time is dropped; unknown stays unknown.
    """
    out: list[dict] = []
    for r in rows:
        cik = "".join(ch for ch in str(r.get("cik") or "") if ch.isdigit())
        sic = "".join(ch for ch in str(r.get("sic") or "") if ch.isdigit())
        if not cik or not sic:
            continue
        accepted = _norm_accepted(r.get("accepted") or "")
        filed = _norm_filed(r.get("filed") or "")
        available_at = accepted or filed
        if not available_at:
            continue
        out.append({
            "cik": str(int(cik)),
            "cik_padded": cik.zfill(10),
            "sic": int(sic[:4]) if sic[:4].isdigit() else None,
            "available_at": available_at,
            "availability_basis": "SEC_ACCEPTANCE" if accepted else "SEC_FILED_DATE",
            "adsh": r.get("adsh") or "",
            "form": r.get("form") or "",
            "period": r.get("period") or "",
            "filed": filed,
            "registrant_name": r.get("name") or "",
            "source": source,
            "quarter": quarter,
        })
    return out


def build_pit_sic_series(observations: "Iterable[dict]", *,
                         key_fields: "tuple[str, ...]" = ("cik", "cik_padded")):
    """Feed observations into the **existing** :class:`PitSicSeries`.

    This module deliberately owns acquisition and projection only; the
    no-look-ahead query rule, the SIC->sector taxonomy and the mapping version
    stay owned by ``alpha_agent.pit_sector``.
    """
    from .pit_sector import PitSicSeries
    series = PitSicSeries()
    for o in observations:
        for field in key_fields:
            key = o.get(field)
            if key:
                series.add(str(key), sic=o.get("sic"),
                           available_at=o.get("available_at"),
                           provenance="SEC FSDS sub.txt %s (%s)" % (
                               o.get("quarter") or "", o.get("availability_basis")))
    return series


def observation_summary(observations: "list[dict]") -> dict:
    """Coverage/provenance summary over acquired observations."""
    if not observations:
        return {"observations": 0, "distinct_ciks": 0, "first_available_at": None,
                "last_available_at": None, "acceptance_timestamped_pct": 0.0,
                "distinct_sic_codes": 0}
    ciks = {o["cik"] for o in observations}
    sics = {o["sic"] for o in observations if o.get("sic") is not None}
    dates = sorted(o["available_at"] for o in observations)
    accepted = sum(1 for o in observations
                   if o.get("availability_basis") == "SEC_ACCEPTANCE")
    forms: dict[str, int] = {}
    for o in observations:
        forms[o.get("form") or "?"] = forms.get(o.get("form") or "?", 0) + 1
    return {
        "observations": len(observations),
        "distinct_ciks": len(ciks),
        "distinct_sic_codes": len(sics),
        "first_available_at": dates[0],
        "last_available_at": dates[-1],
        "acceptance_timestamped_pct": round(
            100.0 * accepted / len(observations), 4),
        "forms_top": dict(sorted(forms.items(), key=lambda kv: -kv[1])[:10]),
    }


# --------------------------------------------------------------------------- #
# Generic quarterly SEC structured-data acquisition
#
# The SEC publishes several quarterly structured-data archives under the same
# contract: one zip per calendar quarter, tab-separated members inside, free,
# first-party, no key and no quota. Release 27 needs a SECOND one of them (the
# Insider Transactions Data Sets), and the release rule is one canonical SEC
# acquisition owner rather than a parser per family — so the machinery is
# generalised HERE, in the module that already owns it.
#
# `FinancialStatementDataSetsAcquirer` above is deliberately left byte-identical:
# it is released, it is what the Stage-26 `sub.txt` cache and its manifests were
# written by, and re-pointing it at a generalisation would invalidate a cache
# this campaign depends on for its three mandatory families.
# --------------------------------------------------------------------------- #
#: Registered quarterly data sets. A data set is (path template, first published
#: quarter, the members worth transferring). Naming the members matters: these
#: archives carry members an order of magnitude larger than the ones a given
#: family reads, and the whole point of the range reader is not to move them.
QUARTERLY_DATASETS = {
    "financial_statement_data_sets": {
        "path_template": SOURCE_PATH_TEMPLATE,
        "first": (2009, 2),
        "members": ("sub.txt",),
        "description": "SEC Financial Statement Data Sets (DERA)",
    },
    "insider_transactions_data_sets": {
        "path_template": "/files/structureddata/data/"
                         "insider-transactions-data-sets/%dq%d_form345.zip",
        "first": (2006, 1),
        "members": ("SUBMISSION.tsv", "REPORTINGOWNER.tsv", "NONDERIV_TRANS.tsv"),
        "description": "SEC Insider Transactions Data Sets, Forms 3/4/5 (DERA)",
    },
    "edgar_full_index": {
        "path_template": "/Archives/edgar/full-index/%d/QTR%d/master.zip",
        "first": (2009, 1),
        "members": ("master.idx",),
        "description": "SEC EDGAR quarterly full index — every filing of every "
                       "form type, by CIK and filing date",
    },
}


class QuarterlyDataSetAcquirer:
    """Acquire named members of any registered quarterly SEC data set.

    Same discipline as the released ``sub.txt`` acquirer: an identifying
    User-Agent carrying a contact address, an inter-request delay, a
    content-hashed cache that short-circuits the network on a re-run, and an
    atomic write so a partial transfer never becomes a cached member.
    """

    def __init__(self, dataset: str, *, cache_root: "str | Path",
                 user_agent: str, members: "Optional[tuple[str, ...]]" = None,
                 transport: Optional[Callable] = None, timeout: float = 60.0,
                 request_delay_seconds: float = 0.35,
                 sleep: Optional[Callable] = None,
                 clock: Optional[Callable] = None):
        if dataset not in QUARTERLY_DATASETS:
            raise ValueError("unregistered quarterly data set: %s" % dataset)
        if not user_agent or "@" not in user_agent:
            raise ValueError(
                "SEC fair access requires an identifying User-Agent carrying a "
                "contact address; refusing to fetch without one")
        self.dataset = dataset
        self.spec = QUARTERLY_DATASETS[dataset]
        self.members = tuple(members or self.spec["members"])
        self.cache_root = Path(cache_root)
        self.user_agent = user_agent
        self.transport = transport
        self.timeout = float(timeout)
        self.request_delay_seconds = float(request_delay_seconds)
        self._sleep = sleep or time.sleep
        self._clock = clock or _utc_now_iso

    # -- identity / paths ---------------------------------------------------- #
    def url(self, year: int, quarter: int) -> str:
        return "https://%s%s" % (SOURCE_HOST,
                                 self.spec["path_template"] % (year, quarter))

    def source_identifier(self, year: int, quarter: int) -> str:
        return "%s%s" % (SOURCE_HOST,
                         self.spec["path_template"] % (year, quarter))

    def quarter_dir(self, year: int, quarter: int) -> Path:
        return self.cache_root / ("%dq%d" % (int(year), int(quarter)))

    def member_path(self, year: int, quarter: int, member: str) -> Path:
        return self.quarter_dir(year, quarter) / member

    def manifest_path(self, year: int, quarter: int) -> Path:
        return self.quarter_dir(year, quarter) / "manifest.json"

    def quarters(self, through_year: int, through_quarter: int) -> "list[tuple[int, int]]":
        y, q = self.spec["first"]
        return quarters_through(through_year, through_quarter,
                                start_year=y, start_quarter=q)

    # -- one quarter --------------------------------------------------------- #
    def cached(self, year: int, quarter: int) -> Optional[dict]:
        mp = self.manifest_path(year, quarter)
        if not mp.exists():
            return None
        try:
            man = json.loads(mp.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        digests = man.get("member_sha256") or {}
        for member in self.members:
            fp = self.member_path(year, quarter, member)
            if not fp.exists() or sha256_hex(fp.read_bytes()) != digests.get(member):
                return None
        return {**man, "disposition": D_CACHED}

    def acquire_quarter(self, year: int, quarter: int, *,
                        force: bool = False) -> dict:
        if not force:
            hit = self.cached(year, quarter)
            if hit is not None:
                return hit
        base = {"dataset": self.dataset, "year": int(year), "quarter": int(quarter),
                "source": self.source_identifier(year, quarter)}
        fetcher = RemoteZipMemberFetcher(
            self.url(year, quarter),
            headers={"User-Agent": self.user_agent,
                     "Accept-Encoding": "identity"},
            transport=self.transport, timeout=self.timeout)
        head = fetcher.head()
        if head.get("status") == 404:
            return {**base, "disposition": D_NOT_PUBLISHED}
        if head.get("total_bytes") is None:
            return {**base, "disposition": D_TRANSPORT_ERROR,
                    "reason": "no Content-Length", "status": head.get("status")}
        if str(head.get("accept_ranges") or "").lower() != "bytes":
            return {**base, "disposition": D_RANGE_UNSUPPORTED,
                    "reason": "server did not advertise byte ranges"}

        payloads: "dict[str, bytes]" = {}
        for member in self.members:
            self._sleep(self.request_delay_seconds)
            try:
                payload, meta = fetcher.fetch_member(member)
            except MemberNotFound:
                return {**base, "disposition": D_MEMBER_MISSING, "member": member}
            except (TransportFailure, MalformedArchive, zlib.error) as exc:
                return {**base, "disposition": D_TRANSPORT_ERROR,
                        "member": member,
                        "reason": "%s: %s" % (type(exc).__name__, exc)}
            payloads[member] = payload

        d = self.quarter_dir(year, quarter)
        d.mkdir(parents=True, exist_ok=True)
        digests, sizes = {}, {}
        for member, payload in payloads.items():
            target = self.member_path(year, quarter, member)
            tmp = target.with_suffix(target.suffix + ".tmp")
            tmp.write_bytes(payload)
            os.replace(tmp, target)
            digests[member] = sha256_hex(payload)
            sizes[member] = len(payload)
        manifest = {
            "contract_version": CONTRACT_VERSION, **base,
            "description": self.spec["description"],
            "members": list(self.members),
            "member_sha256": digests, "member_bytes": sizes,
            "archive_total_bytes": meta.get("archive_total_bytes"),
            "archive_last_modified": meta.get("last_modified"),
            "archive_etag": meta.get("etag"),
            "bytes_fetched_over_network": fetcher.bytes_fetched,
            "http_requests": fetcher.requests,
            "retrieved_at_utc": self._clock(),
            "license_note": "US federal government work; SEC EDGAR public data, "
                            "free for research use under SEC fair-access rules",
        }
        mtmp = self.manifest_path(year, quarter).with_suffix(".json.tmp")
        mtmp.write_text(json.dumps(manifest, indent=1, sort_keys=True),
                        encoding="utf-8")
        os.replace(mtmp, self.manifest_path(year, quarter))
        return {**manifest, "disposition": D_COMPLETE}

    def acquire(self, quarters: "Iterable[tuple[int, int]]", *,
                force: bool = False, stop_after_not_published: int = 2,
                on_quarter: Optional[Callable] = None) -> dict:
        results: "list[dict]" = []
        misses = 0
        for (y, q) in quarters:
            res = self.acquire_quarter(y, q, force=force)
            results.append(res)
            if on_quarter is not None:
                on_quarter(res)
            if res.get("disposition") == D_NOT_PUBLISHED:
                misses += 1
                if misses >= int(stop_after_not_published):
                    break
            else:
                misses = 0
        ok = [r for r in results
              if r.get("disposition") in (D_COMPLETE, D_CACHED)]
        return {
            "contract_version": CONTRACT_VERSION, "dataset": self.dataset,
            "members": list(self.members),
            "quarters_requested": len(results), "quarters_acquired": len(ok),
            "quarters_downloaded": len([r for r in results
                                        if r.get("disposition") == D_COMPLETE]),
            "quarters_from_cache": len([r for r in results
                                        if r.get("disposition") == D_CACHED]),
            "total_member_bytes": sum(
                sum((r.get("member_bytes") or {}).values()) for r in ok),
            "total_network_bytes": sum(
                int(r.get("bytes_fetched_over_network") or 0) for r in results),
            "results": results,
            "failures": [r for r in results
                         if r.get("disposition") not in
                         (D_COMPLETE, D_CACHED, D_NOT_PUBLISHED)],
        }


def parse_tsv(payload: "bytes | str", *,
              required: "tuple[str, ...]" = ()) -> "list[dict]":
    """Parse a tab-separated SEC data-set member into per-row dicts.

    Same contract as :func:`parse_sub_txt` — header row, tab separated, a missing
    required column raises rather than being guessed at — generalised to the
    members of any registered quarterly data set.
    """
    text = payload.decode("utf-8", "replace") if isinstance(payload, bytes) \
        else payload
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if not lines or not lines[0].strip():
        return []
    header = [h.strip().upper() for h in lines[0].split("\t")]
    missing = [c for c in required if c.upper() not in header]
    if missing:
        raise SchemaChanged("member is missing required column(s): %s"
                            % ", ".join(missing))
    idx = {c: i for i, c in enumerate(header)}
    out: "list[dict]" = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < len(header):
            parts = parts + [""] * (len(header) - len(parts))
        out.append({c: parts[i].strip() for c, i in idx.items()})
    return out


__all__ = [
    "CONTRACT_VERSION", "STAGE", "SOURCE_HOST", "MEMBER_NAME", "REQUIRED_COLUMNS",
    "QUARTERLY_DATASETS", "QuarterlyDataSetAcquirer", "parse_tsv",
    "D_COMPLETE", "D_CACHED", "D_NOT_PUBLISHED", "D_MEMBER_MISSING",
    "D_TRANSPORT_ERROR", "D_RANGE_UNSUPPORTED",
    "quarter_url", "source_identifier", "quarters_through", "sha256_hex",
    "RemoteZipMemberFetcher", "FinancialStatementDataSetsAcquirer",
    "TransportFailure", "MalformedArchive", "MemberNotFound", "SchemaChanged",
    "parse_sub_txt", "sic_observations", "build_pit_sic_series",
    "observation_summary",
]
