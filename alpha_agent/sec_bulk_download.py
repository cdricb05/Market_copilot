"""
alpha_agent.sec_bulk_download — Stage 8 production resumable SEC BULK-ARCHIVE
downloader + extraction cursor (WS6).

The official SEC bulk archives (``companyfacts.zip`` ~1.39 GB, ``submissions.zip``
~1.55 GB) are genuinely accessible; being larger than the ingestion raw-object
cap is a SOFTWARE cap, not an external data-access blocker. This module mirrors
them to the large-data drive with a real production pipeline:

  * free-space PREFLIGHT against a configurable disk budget (never C:);
  * HTTP Range RESUME from a persistent byte-offset checkpoint;
  * bounded per-call SEGMENT download (restart-safe; the durable queue drives
    successive segments) — the whole archive is never held in memory;
  * SHA-256 checksum + ATOMIC completion (``.part`` -> final via os.replace);
  * archive version/date provenance (Last-Modified / ETag); a changed version
    restarts the download (corruption / staleness recovery);
  * a per-member EXTRACTION cursor (bounded members per call, checkpointed).

If local free space is genuinely insufficient the downloader returns a precise
``SEC_BULK_DISK_CAPACITY_REQUIRED`` blocker with exact required/available bytes —
never a silent skip and never an artificial software cap dressed up as a data
blocker.

Safety: writes only under the configured D: bulk root; no operational-ledger
write, no order/fill/signal/decision. The HTTP transport is injectable so the
pipeline is fully unit-testable with a fake transport (no real network).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import zipfile
from pathlib import Path
from typing import Any, Callable, Optional

STAGE = "8"

# Dispositions.
D_COMPLETE = "COMPLETE"
D_IN_PROGRESS = "IN_PROGRESS"
D_DISK_CAPACITY_REQUIRED = "SEC_BULK_DISK_CAPACITY_REQUIRED"
D_HEAD_UNAVAILABLE = "HEAD_UNAVAILABLE"
D_UNSAFE_DESTINATION = "UNSAFE_DESTINATION_DRIVE"
D_TRANSPORT_ERROR = "TRANSPORT_ERROR"

_DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024      # 64 MB per bounded segment
_DEFAULT_CHUNK_BYTES = 4 * 1024 * 1024         # 4 MB per HTTP range request
_DEFAULT_DISK_BUDGET_BYTES = 8 * 1024 * 1024 * 1024  # 8 GB
_DEFAULT_FREE_MARGIN_BYTES = 512 * 1024 * 1024  # keep 512 MB head-room


def _default_transport():
    from . import ingestion  # lazy: avoid heavy import at module load
    return ingestion.default_transport


def _drive_of(path: Path) -> str:
    return os.path.splitdrive(str(path.resolve()))[0].rstrip(":").upper()


class BulkArchiveDownloader:
    """Resumable, checksummed, disk-safe downloader for ONE bulk archive."""

    def __init__(self, *, url: str, dest_dir: str | Path, name: str,
                 headers: Optional[dict] = None,
                 disk_budget_bytes: int = _DEFAULT_DISK_BUDGET_BYTES,
                 segment_bytes: int = _DEFAULT_SEGMENT_BYTES,
                 chunk_bytes: int = _DEFAULT_CHUNK_BYTES,
                 free_margin_bytes: int = _DEFAULT_FREE_MARGIN_BYTES,
                 timeout: float = 60.0,
                 forbid_drives: "tuple[str, ...]" = ("C",),
                 transport: Optional[Callable] = None):
        self.url = url
        self.dest_dir = Path(dest_dir)
        self.name = name
        self.headers = dict(headers or {})
        self.disk_budget_bytes = int(disk_budget_bytes)
        self.segment_bytes = int(segment_bytes)
        self.chunk_bytes = int(chunk_bytes)
        self.free_margin_bytes = int(free_margin_bytes)
        self.timeout = float(timeout)
        self.forbid_drives = tuple(d.upper() for d in forbid_drives)
        self._transport = transport or _default_transport()

    # -- paths -------------------------------------------------------------- #
    @property
    def final_path(self) -> Path:
        return self.dest_dir / ("%s.zip" % self.name)

    @property
    def part_path(self) -> Path:
        return self.dest_dir / ("%s.zip.part" % self.name)

    @property
    def checkpoint_path(self) -> Path:
        return self.dest_dir / ("%s.checkpoint.json" % self.name)

    @property
    def manifest_path(self) -> Path:
        return self.dest_dir / ("%s.manifest.json" % self.name)

    # -- checkpoint --------------------------------------------------------- #
    def _read_checkpoint(self) -> dict:
        if self.checkpoint_path.exists():
            try:
                return json.loads(self.checkpoint_path.read_text(
                    encoding="utf-8"))
            except (OSError, ValueError):
                return {}
        return {}

    def _write_checkpoint(self, ck: dict) -> None:
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(ck, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.checkpoint_path)

    # -- HEAD / provenance -------------------------------------------------- #
    def head(self) -> dict:
        try:
            resp = self._transport(
                {"method": "HEAD", "url": self.url, "headers": self.headers},
                self.timeout)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "disposition": D_TRANSPORT_ERROR,
                    "error": "%s" % type(exc).__name__}
        hdrs = {str(k).lower(): v
                for k, v in (resp.get("headers") or {}).items()}
        clen = hdrs.get("content-length")
        size = int(clen) if clen and str(clen).isdigit() else None
        return {"ok": size is not None, "status": resp.get("status"),
                "total_bytes": size, "last_modified": hdrs.get("last-modified"),
                "etag": hdrs.get("etag"),
                "accept_ranges": hdrs.get("accept-ranges")}

    # -- disk preflight ----------------------------------------------------- #
    def preflight(self, total_bytes: int, already: int = 0) -> dict:
        """Verify the destination drive is allowed and has room for the REMAINING
        bytes (bounded by the disk budget) plus a safety margin."""
        drive = _drive_of(self.dest_dir)
        if drive in self.forbid_drives:
            return {"ok": False, "disposition": D_UNSAFE_DESTINATION,
                    "reason": "destination drive %s: is forbidden for bulk "
                              "archives (never store on C:)" % drive,
                    "drive": drive}
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        remaining = max(0, int(total_bytes) - int(already))
        need_budget = min(int(total_bytes), self.disk_budget_bytes)
        try:
            free = shutil.disk_usage(str(self.dest_dir)).free
        except OSError as exc:
            return {"ok": False, "disposition": D_TRANSPORT_ERROR,
                    "reason": "disk_usage failed: %s" % exc}
        required = remaining + self.free_margin_bytes
        if int(total_bytes) > self.disk_budget_bytes:
            return {"ok": False, "disposition": D_DISK_CAPACITY_REQUIRED,
                    "reason": "archive %d bytes exceeds the configured disk "
                              "budget %d bytes; raise bulk_disk_budget_bytes or "
                              "free space" % (int(total_bytes),
                                              self.disk_budget_bytes),
                    "required_bytes": int(total_bytes),
                    "budget_bytes": self.disk_budget_bytes,
                    "available_bytes": int(free)}
        if free < required:
            return {"ok": False, "disposition": D_DISK_CAPACITY_REQUIRED,
                    "reason": "insufficient free space on %s: need %d bytes "
                              "(remaining %d + margin %d), have %d"
                              % (drive, required, remaining,
                                 self.free_margin_bytes, int(free)),
                    "required_bytes": int(required),
                    "available_bytes": int(free),
                    "budget_bytes": self.disk_budget_bytes}
        return {"ok": True, "disposition": "OK", "available_bytes": int(free),
                "required_bytes": int(required), "remaining_bytes": remaining,
                "budget_bytes": self.disk_budget_bytes, "drive": drive}

    # -- bounded segment download ------------------------------------------ #
    def download_segment(self) -> dict:
        """Download up to ``segment_bytes`` more of the archive, resuming from the
        checkpoint via a Range header. Restart-safe and bounded; the durable
        queue drives successive segments to completion. Returns progress."""
        if self.final_path.exists():
            return self._completed_result(
                self.final_path.stat().st_size,
                self._read_checkpoint().get("sha256"),
                self._read_checkpoint().get("total"),
                already_complete=True)
        head = self.head()
        total = head.get("total_bytes")
        if not head.get("ok") or total is None:
            return {"ok": False, "disposition": D_HEAD_UNAVAILABLE,
                    "reason": "HEAD returned no Content-Length; cannot plan a "
                              "resumable download", "head": head}

        ck = self._read_checkpoint()
        offset = int(ck.get("bytes", 0)) if self.part_path.exists() else 0
        # Version/staleness recovery: a changed archive version restarts cleanly.
        version_changed = (
            ck and (ck.get("last_modified") != head.get("last_modified")
                    or ck.get("etag") != head.get("etag")))
        if version_changed and offset:
            try:
                self.part_path.unlink()
            except OSError:
                pass
            offset = 0

        pre = self.preflight(total, already=offset)
        if not pre.get("ok"):
            return {"ok": False, "disposition": pre["disposition"],
                    "reason": pre.get("reason"), "total_bytes": total,
                    "preflight": pre}

        self.dest_dir.mkdir(parents=True, exist_ok=True)
        seg_start = offset
        seg_budget = self.segment_bytes
        requests = 0
        mode = "ab" if offset and self.part_path.exists() else "wb"
        transport_error = None
        with self.part_path.open(mode) as fh:
            while (offset - seg_start) < seg_budget and offset < total:
                hi = min(offset + self.chunk_bytes - 1, total - 1)
                rng = {"Range": "bytes=%d-%d" % (offset, hi)}
                try:
                    resp = self._transport(
                        {"method": "GET", "url": self.url,
                         "headers": {**self.headers, **rng}}, self.timeout)
                except Exception as exc:  # noqa: BLE001
                    transport_error = type(exc).__name__
                    break
                requests += 1
                if resp.get("status") not in (200, 206):
                    transport_error = "http_%s" % resp.get("status")
                    break
                body = resp.get("body") or b""
                if not body:
                    break
                fh.write(body)
                offset += len(body)
                self._write_checkpoint({
                    "bytes": offset, "total": total,
                    "last_modified": head.get("last_modified"),
                    "etag": head.get("etag")})
                if len(body) < (hi - (offset - len(body)) + 1):
                    # Short read (server ended the range early) — stop this
                    # segment; next segment resumes from the new offset.
                    break

        complete = offset >= total
        if complete:
            sha = self._finalize()
            return self._completed_result(offset, sha, total)
        result = {
            "ok": transport_error is None, "disposition": D_IN_PROGRESS,
            "bytes_downloaded": offset, "total_bytes": total,
            "segment_bytes": offset - seg_start, "requests": requests,
            "progress_pct": round(100.0 * offset / total, 3) if total else None,
            "part_path": str(self.part_path),
            "checkpoint": str(self.checkpoint_path),
            "resumable": True, "complete": False,
            "provenance": {"last_modified": head.get("last_modified"),
                           "etag": head.get("etag")}}
        if transport_error:
            result["disposition"] = D_TRANSPORT_ERROR
            result["reason"] = ("segment interrupted (%s); durable checkpoint "
                                "preserved, next segment resumes"
                                % transport_error)
        return result

    def _finalize(self) -> Optional[str]:
        """Checksum the completed ``.part`` and atomically promote it to final."""
        sha = None
        if self.part_path.exists():
            h = hashlib.sha256()
            with self.part_path.open("rb") as fh:
                for blk in iter(lambda: fh.read(1 << 20), b""):
                    h.update(blk)
            sha = h.hexdigest()
            os.replace(self.part_path, self.final_path)
        ck = self._read_checkpoint()
        manifest = {
            "name": self.name, "url_host_path": _host_path(self.url),
            "total_bytes": ck.get("total"),
            "final_path": str(self.final_path), "sha256": sha,
            "last_modified": ck.get("last_modified"), "etag": ck.get("etag"),
            "atomic_complete": True}
        tmp = self.manifest_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(manifest, indent=1, sort_keys=True),
                       encoding="utf-8")
        os.replace(tmp, self.manifest_path)
        self._write_checkpoint({**ck, "bytes": ck.get("total"), "sha256": sha,
                                "complete": True})
        return sha

    def _completed_result(self, size: int, sha: Optional[str],
                          total: Optional[int], *,
                          already_complete: bool = False) -> dict:
        return {"ok": True, "disposition": D_COMPLETE, "complete": True,
                "bytes_downloaded": int(size),
                "total_bytes": int(total) if total else int(size),
                "sha256": sha, "final_path": str(self.final_path),
                "manifest": str(self.manifest_path),
                "already_complete": already_complete, "resumable": True}

    # -- extraction cursor -------------------------------------------------- #
    def extract_segment(self, *, max_members: int = 500,
                        extract_root: Optional[str | Path] = None) -> dict:
        """Extract the next bounded set of members from the completed archive,
        checkpointing the member index so a restart resumes mid-archive. The zip
        is opened streaming (per member) — never fully loaded into memory. Only
        valid once the download is COMPLETE."""
        if not self.final_path.exists():
            return {"ok": False, "disposition": D_IN_PROGRESS,
                    "reason": "archive download not complete; nothing to extract"}
        out_root = Path(extract_root) if extract_root else \
            (self.dest_dir / ("%s_extracted" % self.name))
        out_root.mkdir(parents=True, exist_ok=True)
        xck_path = self.dest_dir / ("%s.extract.checkpoint.json" % self.name)
        try:
            done = int(json.loads(xck_path.read_text(encoding="utf-8")).get(
                "members_done", 0)) if xck_path.exists() else 0
        except (OSError, ValueError):
            done = 0
        extracted = 0
        total_members = 0
        with zipfile.ZipFile(self.final_path) as zf:
            names = zf.namelist()
            total_members = len(names)
            for idx in range(done, min(done + int(max_members),
                                       total_members)):
                zf.extract(names[idx], path=str(out_root))
                extracted += 1
        new_done = done + extracted
        tmp = xck_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"members_done": new_done,
                                   "total_members": total_members},
                                  sort_keys=True), encoding="utf-8")
        os.replace(tmp, xck_path)
        return {"ok": True,
                "disposition": (D_COMPLETE if new_done >= total_members
                                else D_IN_PROGRESS),
                "members_extracted_this_call": extracted,
                "members_done": new_done, "total_members": total_members,
                "extract_root": str(out_root), "complete": new_done >= total_members,
                "resumable": True}


def _host_path(url: str) -> str:
    """URL host+path only (never any query/credential) for safe provenance."""
    try:
        from urllib.parse import urlsplit
        s = urlsplit(url)
        return "%s%s" % (s.netloc, s.path)
    except Exception:  # noqa: BLE001
        return "sec.gov/bulk"
