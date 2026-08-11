"""
alpha_agent/collectors/base.py — shared collector machinery for Alpha Agent Stage 2.

Provides:
  * an injectable HTTP transport (stdlib urllib; tests ALWAYS inject fakes);
  * bounded retries with exponential backoff (5xx / network errors only);
  * a client-side rate limiter per source;
  * a persistent circuit breaker (consecutive-failure threshold);
  * a content-addressed immutable raw-object archive with secret-free metadata;
  * response hygiene: zero-byte rejection, HTML-error rejection, 429 handling
    (never treated as data), raw-object size limits;
  * secret redaction for every persisted fingerprint and error message.

No PostgreSQL, no model APIs, no Paper Trader database. The only writes are the
raw archive under the supplied output root (via RawArchive).
"""
from __future__ import annotations

import email.utils
import json
from pathlib import Path
from typing import Any, Callable, Optional

from ..source_contracts import (
    CB_CLOSED, CB_OPEN, IdentityResolver, SH_BLOCKED_CONFIGURATION, SH_DEGRADED,
    SH_FAILED, SH_HEALTHY, SH_NOT_CONFIGURED, SH_NOT_RUN, SH_RATE_LIMITED,
    make_raw_object_id, redact_text, request_fingerprint, sha256_hex,
)

_HTML_MARKERS = (b"<!doctype html", b"<html")


def default_transport(request: dict, timeout: float) -> dict:
    """Real HTTP transport (stdlib urllib). Only used when no fake transport is
    injected — pytest always injects fakes and never reaches this function."""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(
        request["url"], headers=request.get("headers") or {},
        method=request.get("method", "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            headers = {k.lower(): v for k, v in resp.headers.items()}
            return {"status": resp.status, "headers": headers, "body": body, "error": None}
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read()
        except Exception:  # noqa: BLE001
            body = b""
        headers = {k.lower(): v for k, v in (exc.headers or {}).items()}
        return {"status": exc.code, "headers": headers, "body": body, "error": None}
    except Exception as exc:  # noqa: BLE001 — URLError, timeout, ssl
        return {"status": 0, "headers": {}, "body": b"", "error": "%s: %s" % (type(exc).__name__, exc)}


def _parse_http_datetime(value: Optional[str]) -> Optional[str]:
    """Parse an HTTP date header (e.g. Last-Modified) into ISO-8601 UTC, or None."""
    if not value:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(value)
        if dt is None:
            return None
        return dt.astimezone(tz=None).astimezone().isoformat() if dt.tzinfo is None else dt.isoformat()
    except Exception:  # noqa: BLE001
        return None


class RawArchive:
    """Content-addressed immutable raw-object store under <output_root>/raw.

    * Raw object ID = hash(source_id, content); identical repeated content
      reuses the SAME ID and is never re-written (duplicate prevented).
    * Existing files are NEVER overwritten.
    * Every object gets a sibling ``<id>.metadata.json`` whose request
      fingerprint is secret-free (redacted before it reaches this class).
    """

    def __init__(self, raw_root: Path, output_root: Path, known_ids: set[str],
                 max_bytes: int) -> None:
        self.raw_root = Path(raw_root)
        self.output_root = Path(output_root)
        self.known_ids = set(known_ids)
        self.max_bytes = int(max_bytes)
        self.duplicates_prevented = 0
        self.stored: list[dict] = []

    def store(self, *, source_id: str, content: bytes, extension: str,
              retrieved_at: str, business_date: Optional[str],
              source_native_id: str, request_fp: str, content_type: str,
              http_status: Optional[int], retry_count: int,
              published_at: Optional[str], license_note: str,
              parser_status: str = "PS_OK") -> dict:
        raw_id = make_raw_object_id(source_id, content)
        content_hash = sha256_hex(content)
        part_date = (business_date or retrieved_at or "")[:10] or "0000-00-00"
        yyyy, mm, dd = (part_date.split("-") + ["00", "00"])[:3]
        rel_dir = Path("raw") / source_id / yyyy / mm / dd
        ext = extension.lstrip(".") or "bin"
        rel_path = rel_dir / ("%s.%s" % (raw_id, ext))
        abs_path = self.output_root / rel_path
        duplicate = raw_id in self.known_ids
        if not duplicate:
            abs_path.parent.mkdir(parents=True, exist_ok=True)
            if not abs_path.exists():  # NEVER overwrite an existing raw object
                tmp = abs_path.with_suffix(abs_path.suffix + ".tmp")
                tmp.write_bytes(content)
                tmp.replace(abs_path)
            meta_path = abs_path.parent / ("%s.metadata.json" % raw_id)
            if not meta_path.exists():
                meta = {
                    "raw_object_id": raw_id, "source_id": source_id,
                    "retrieved_at": retrieved_at, "published_at": published_at,
                    "source_native_id": source_native_id,
                    "request_fingerprint": request_fp,
                    "content_hash": content_hash, "content_type": content_type,
                    "byte_size": len(content), "http_status": http_status,
                    "retry_count": retry_count, "parser_status": parser_status,
                    "license_note": license_note,
                    "business_date": business_date,
                }
                meta_path.write_text(json.dumps(meta, indent=1, sort_keys=True),
                                     encoding="utf-8")
            self.known_ids.add(raw_id)
        else:
            self.duplicates_prevented += 1
        record = {
            "raw_object_id": raw_id, "source_id": source_id,
            "storage_path": str(rel_path).replace("\\", "/"),
            "content_hash": content_hash, "content_type": content_type,
            "byte_size": len(content), "http_status": http_status,
            "retry_count": retry_count, "parser_status": parser_status,
            "retrieved_at": retrieved_at, "published_at": published_at,
            "source_native_id": source_native_id, "request_fingerprint": request_fp,
            "license_note": license_note, "business_date": business_date,
            "duplicate": duplicate,
        }
        if not duplicate:
            self.stored.append(record)
        return record


class CollectorContext:
    """Everything a collector needs, injected for testability. ``secrets`` holds
    in-memory secret VALUES used only to scrub outbound error text; it is never
    serialized."""

    def __init__(self, *, config: dict, source_cfg: dict, archive: RawArchive,
                 transport: Callable[[dict, float], dict],
                 now_iso: Callable[[], str],
                 clock: Callable[[], float], sleep: Callable[[float], None],
                 secrets: Optional[list[str]] = None,
                 user_agent: Optional[str] = None,
                 checkpoint: Optional[dict] = None,
                 env: Optional[dict] = None,
                 identity: Optional[IdentityResolver] = None) -> None:
        self.config = config
        self.source_cfg = source_cfg
        self.archive = archive
        self.transport = transport
        self.now_iso = now_iso
        self.clock = clock
        self.sleep = sleep
        self.secrets = list(secrets or [])
        self.user_agent = user_agent
        self.checkpoint = dict(checkpoint or {})
        self.env = env if env is not None else {}
        self.identity = identity if identity is not None else IdentityResolver()

    @property
    def limits(self) -> dict:
        return self.config.get("limits", {})

    @property
    def redaction(self) -> dict:
        return self.config.get("secret_redaction", {})


class BaseCollector:
    """Base class for all Stage 2 collectors."""

    source_id = "base"
    requires_credential = False

    def __init__(self, ctx: CollectorContext) -> None:
        self.ctx = ctx
        self.errors: list[dict] = []
        self.raw_objects: list[dict] = []
        self.records: list[dict] = []
        self.entitlements: list[dict] = []
        self.inventory: dict[str, Any] = {}
        self.requests_attempted = 0
        self.http_errors = 0
        self.retries_total = 0
        self.rate_limited_hits = 0
        self.last_attempt_at: Optional[str] = None
        self.last_success_at: Optional[str] = None
        self.newest_source_event_time: Optional[str] = None
        self.consecutive_failures = int(ctx.checkpoint.get("consecutive_failures", 0))
        self.circuit_state = ctx.checkpoint.get("circuit_state", CB_CLOSED)
        self.cursor: dict[str, Any] = dict(ctx.checkpoint.get("cursor", {}))
        self._last_request_clock: Optional[float] = None

    # ------------------------------------------------------------------ #
    # Plumbing
    # ------------------------------------------------------------------ #
    def _redact(self, text: str) -> str:
        return redact_text(text or "", self.ctx.secrets)

    def _fingerprint(self, method: str, url: str) -> str:
        red = self.ctx.redaction
        return request_fingerprint(
            method, url, red.get("redacted_query_params", []),
            red.get("redaction_placeholder", "REDACTED"))

    def note_event_time(self, iso_ts: Optional[str]) -> None:
        if iso_ts and (self.newest_source_event_time is None
                       or iso_ts > self.newest_source_event_time):
            self.newest_source_event_time = iso_ts

    def record_error(self, error_type: str, message: str,
                     http_status: Optional[int] = None, retry_count: int = 0) -> None:
        self.errors.append({
            "source_id": self.source_id, "occurred_at": self.ctx.now_iso(),
            "error_type": error_type, "http_status": http_status,
            "message": self._redact(message)[:1000], "retry_count": retry_count,
        })

    def _rate_limit_wait(self) -> None:
        min_interval = float(self.ctx.source_cfg.get("min_interval_seconds", 0.0))
        if min_interval <= 0:
            return
        now = self.ctx.clock()
        if self._last_request_clock is not None:
            elapsed = now - self._last_request_clock
            if elapsed < min_interval:
                self.ctx.sleep(min_interval - elapsed)
        self._last_request_clock = self.ctx.clock()

    def _breaker_threshold(self) -> int:
        return int(self.ctx.limits.get("circuit_breaker_threshold", 5))

    def _register_failure(self) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self._breaker_threshold():
            self.circuit_state = CB_OPEN

    def _register_success(self) -> None:
        self.consecutive_failures = 0
        self.circuit_state = CB_CLOSED
        self.last_success_at = self.ctx.now_iso()

    # ------------------------------------------------------------------ #
    # HTTP fetch with hygiene
    # ------------------------------------------------------------------ #
    def fetch(self, url: str, *, headers: Optional[dict] = None,
              native_id: str = "", business_date: Optional[str] = None,
              expect: str = "text", archive: bool = True,
              extension: str = "txt", license_note: Optional[str] = None,
              content_type: str = "text/plain",
              allow_404: bool = False, allow_400: bool = False) -> dict:
        """Fetch one URL with rate limiting, bounded retries + backoff, breaker
        integration and response hygiene. Returns::

            {"ok": bool, "status": int|None, "body": bytes|None,
             "raw": raw_object|None, "error": str|None,
             "rejected_reason": str|None, "retries": int,
             "published_at": str|None, "not_found": bool}
        """
        result = {"ok": False, "status": None, "body": None, "raw": None,
                  "error": None, "rejected_reason": None, "retries": 0,
                  "published_at": None, "not_found": False, "bad_request": False}
        if self.circuit_state == CB_OPEN:
            result["rejected_reason"] = "CIRCUIT_OPEN"
            self.record_error("CIRCUIT_OPEN",
                              "circuit breaker open; request suppressed: %s"
                              % self._fingerprint("GET", url))
            return result

        limits = self.ctx.limits
        max_retries = int(limits.get("max_retries", 3))
        backoff = float(limits.get("backoff_base_seconds", 1.0))
        multiplier = float(limits.get("backoff_multiplier", 2.0))
        timeout = float(limits.get("http_timeout_seconds", 30))
        max_bytes = int(limits.get("raw_object_max_bytes", 33554432))
        hdrs = dict(headers or {})
        if self.ctx.user_agent and "User-Agent" not in hdrs:
            hdrs["User-Agent"] = self.ctx.user_agent
        fp = self._fingerprint("GET", url)

        attempt = 0
        while True:
            self._rate_limit_wait()
            self.requests_attempted += 1
            self.last_attempt_at = self.ctx.now_iso()
            resp = self.ctx.transport({"method": "GET", "url": url, "headers": hdrs},
                                      timeout)
            status = resp.get("status", 0)
            body = resp.get("body", b"") or b""
            error = resp.get("error")
            result["status"] = status
            retryable = (error is not None) or status == 429 or 500 <= status <= 599
            if retryable and attempt < max_retries:
                attempt += 1
                self.retries_total += 1
                result["retries"] = attempt
                if status == 429:
                    self.rate_limited_hits += 1
                self.ctx.sleep(backoff * (multiplier ** (attempt - 1)))
                continue
            break

        if error is not None:
            self.http_errors += 1
            self._register_failure()
            result["error"] = self._redact(error)
            self.record_error("NETWORK_ERROR", "%s (%s)" % (error, fp),
                              retry_count=result["retries"])
            return result
        if status == 429:
            self.rate_limited_hits += 1
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "RATE_LIMITED"
            self.record_error("RATE_LIMITED",
                              "429 rate-limit response NOT treated as data (%s)" % fp,
                              http_status=429, retry_count=result["retries"])
            return result
        if allow_404 and status in (403, 404):
            # Expected gap: a not-yet-published dated file. FINRA's CDN and
            # some SEC paths answer 403 (S3-style AccessDenied), not 404.
            result["not_found"] = True
            return result
        if allow_400 and status == 400:
            # Caller opts in to inspect a 400 body (e.g. ALFRED signalling
            # "vintage dates exceed the maximum" or "series does not exist in
            # ALFRED for this realtime window"). NOT a hard failure: the breaker
            # is not tripped and the body is returned for the caller to classify.
            result["bad_request"] = True
            result["body"] = body
            return result
        if status == 404:
            result["not_found"] = True
            self.http_errors += 1
            self._register_failure()
            self.record_error("HTTP_ERROR", "404 (%s)" % fp, http_status=404,
                              retry_count=result["retries"])
            return result
        if status != 200:
            self.http_errors += 1
            self._register_failure()
            self.record_error("HTTP_ERROR", "HTTP %s (%s)" % (status, fp),
                              http_status=status, retry_count=result["retries"])
            return result
        if len(body) == 0:
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "ZERO_BYTE_RESPONSE"
            self.record_error("ZERO_BYTE_RESPONSE",
                              "zero-byte 200 response rejected (%s)" % fp,
                              http_status=200, retry_count=result["retries"])
            return result
        if expect != "html":
            head = body.lstrip()[:64].lower()
            if head.startswith(_HTML_MARKERS[0]) or head.startswith(_HTML_MARKERS[1]):
                self.http_errors += 1
                self._register_failure()
                result["rejected_reason"] = "HTML_ERROR_RESPONSE"
                self.record_error("HTML_ERROR_RESPONSE",
                                  "HTML page returned where %s expected; NOT parsed as data (%s)"
                                  % (expect, fp), http_status=200,
                                  retry_count=result["retries"])
                return result
        if len(body) > max_bytes:
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "RAW_OBJECT_TOO_LARGE"
            self.record_error("RAW_OBJECT_TOO_LARGE",
                              "%d bytes exceeds raw_object_max_bytes=%d (%s)"
                              % (len(body), max_bytes, fp), http_status=200)
            return result

        self._register_success()
        published_at = _parse_http_datetime(
            resp.get("headers", {}).get("last-modified"))
        result.update({"ok": True, "body": body, "published_at": published_at})
        if archive:
            raw = self.ctx.archive.store(
                source_id=self.source_id, content=body, extension=extension,
                retrieved_at=self.ctx.now_iso(), business_date=business_date,
                source_native_id=native_id, request_fp=fp,
                content_type=content_type, http_status=status,
                retry_count=result["retries"], published_at=published_at,
                license_note=license_note
                or self.ctx.source_cfg.get("license_note", ""))
            if not raw["duplicate"]:
                self.raw_objects.append(raw)
            result["raw"] = raw
        return result

    # ------------------------------------------------------------------ #
    # Lifecycle — implemented by each collector
    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        """Minimum-network entitlement/availability audit."""
        raise NotImplementedError

    def collect(self, as_of: str) -> dict:
        """Bounded real collection."""
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # Result assembly
    # ------------------------------------------------------------------ #
    def _default_overall_state(self, *, attempted: bool, collected_any: bool) -> str:
        if not self.ctx.source_cfg.get("enabled", False):
            return SH_NOT_CONFIGURED
        if not attempted:
            return SH_NOT_RUN
        if self.circuit_state == CB_OPEN:
            return SH_FAILED
        if self.rate_limited_hits and not collected_any:
            return SH_RATE_LIMITED
        if self.http_errors and collected_any:
            return SH_DEGRADED
        if not collected_any:
            return SH_FAILED
        return SH_HEALTHY

    def result(self, *, overall_state: str, credential_present: Optional[bool] = None,
               entitlement_summary: str = "") -> dict:
        counters = {
            "requests_attempted": self.requests_attempted,
            "http_errors": self.http_errors,
            "retries_total": self.retries_total,
            "rate_limited_hits": self.rate_limited_hits,
            "records_collected": len(self.records),
            "raw_objects_new": len(self.raw_objects),
        }
        health = {
            "source_id": self.source_id,
            "configured": bool(self.ctx.source_cfg),
            "enabled": bool(self.ctx.source_cfg.get("enabled", False)),
            "credential_present": credential_present,
            "entitlement_summary": entitlement_summary,
            "last_attempt_at": self.last_attempt_at,
            "last_success_at": self.last_success_at,
            "newest_source_event_time": self.newest_source_event_time,
            "records_collected": len(self.records),
            "raw_objects_new": len(self.raw_objects),
            "http_errors": self.http_errors,
            "retry_count": self.retries_total,
            "rate_limited_hits": self.rate_limited_hits,
            "consecutive_failures": self.consecutive_failures,
            "circuit_breaker_state": self.circuit_state,
            "overall_state": overall_state,
        }
        return {
            "source_id": self.source_id,
            "health": health,
            "entitlements": self.entitlements,
            "raw_objects": self.raw_objects,
            "records": self.records,
            "errors": self.errors,
            "inventory": self.inventory,
            "counters": counters,
            "cursor": self.cursor,
            "consecutive_failures": self.consecutive_failures,
            "circuit_state": self.circuit_state,
            "newest_source_event_time": self.newest_source_event_time,
            "last_attempt_at": self.last_attempt_at,
            "last_success_at": self.last_success_at,
        }

    def blocked_result(self, state: str, reason: str,
                       credential_present: Optional[bool] = None) -> dict:
        """Controlled result for a source that cannot run (missing credential,
        missing vendor integration, missing configuration). Never an exception."""
        if state not in (SH_BLOCKED_CONFIGURATION, SH_NOT_CONFIGURED,
                         SH_NOT_RUN) and not state.startswith("BLOCKED"):
            state = SH_BLOCKED_CONFIGURATION
        self.record_error("SOURCE_BLOCKED", reason)
        self.inventory.setdefault("blocked_reason", self._redact(reason))
        return self.result(overall_state=state,
                           credential_present=credential_present,
                           entitlement_summary=reason[:200])
