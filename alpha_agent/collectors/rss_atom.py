"""
alpha_agent/collectors/rss_atom.py — Stage 3.5 generic RSS 2.0 / Atom collector.

One bounded, deterministic collector for arbitrary official RSS/Atom feeds:

  * conditional polling with ETag (If-None-Match) and Last-Modified
    (If-Modified-Since); HTTP 304 is a CLEAN no-new-data result, never an error;
  * bounded retries + exponential backoff on 5xx / network errors, a per-feed
    circuit breaker and a per-feed rate limit;
  * response hygiene: redirect scheme validation, content-type validation,
    max-size and zero-byte rejection, HTML-error rejection;
  * immutable content-addressed raw archival (reuses the shared RawArchive);
  * SAFE XML parsing (DOCTYPE / entity payloads quarantined) via feed_contracts;
  * deterministic entity resolution (feed-to-company mapping + explicit exact
    ticker mention only — a regulator release is NEVER silently mapped to a
    company); unmatched items are quarantined, never guessed;
  * per-item dedup across feeds by (canonical URL, native id, title fingerprint).

The transport is always injected; pytest never touches the network.
"""
from __future__ import annotations

import re
from typing import Callable, Optional

from ..feed_contracts import (
    EM_AMBIGUOUS, EM_MATCHED_ALIAS, EM_MATCHED_EXACT, EM_UNMATCHED, SOURCE_ID,
    SC_COMPANY_IR, SC_COMPANY_NEWSROOM, build_feed_event_record, parse_feed,
)
from ..source_contracts import CB_CLOSED, CB_OPEN, make_raw_object_id
from .base import BaseCollector

_TICKER_TOKEN = re.compile(r"\(?([A-Z]{1,5})\)?")
_FEED_CONTENT_OK = ("xml", "rss", "atom", "text/plain")
_FEED_CONTENT_BAD = ("text/html", "application/xhtml")


def resolve_feed_item_entity(feed: dict, item: dict,
                             known_tickers: set[str]) -> dict:
    """Deterministic entity resolution. No LLM, no fuzzy matching.

    Priority:
      1. An explicit single feed->company mapping (covered_tickers on a company
         IR/newsroom feed) is an EXACT match.
      2. Otherwise, only tickers explicitly named as standalone uppercase tokens
         in the item title that are ALSO known tickers count. If the feed lists
         covered_tickers, mentions are restricted to that set.
      3. Zero deterministic matches -> UNMATCHED (quarantined, never guessed).
         Multiple distinct matches -> AMBIGUOUS (never merged)."""
    covered = [str(t).upper() for t in (feed.get("covered_tickers") or [])]
    category = feed.get("source_category", "")
    if len(covered) == 1 and category in (SC_COMPANY_IR, SC_COMPANY_NEWSROOM):
        t = covered[0]
        return {"mapped_tickers": [t], "mapped_entities": [feed.get("publisher")],
                "company_id": None, "state": EM_MATCHED_EXACT}
    allowed = set(covered) if covered else None
    title = item.get("title") or ""
    found: list[str] = []
    for tok in _TICKER_TOKEN.findall(title):
        if tok in known_tickers and (allowed is None or tok in allowed):
            if tok not in found:
                found.append(tok)
    if len(found) == 1:
        return {"mapped_tickers": found, "mapped_entities": [],
                "company_id": None,
                "state": EM_MATCHED_EXACT if covered else EM_MATCHED_ALIAS}
    if len(found) > 1:
        return {"mapped_tickers": sorted(found), "mapped_entities": [],
                "company_id": None, "state": EM_AMBIGUOUS}
    if covered:
        # A company feed with several covered tickers but no single mention:
        # record the scope as an alias mapping, never a silent single pick.
        return {"mapped_tickers": sorted(covered), "mapped_entities": [],
                "company_id": None,
                "state": EM_AMBIGUOUS if len(covered) > 1 else EM_MATCHED_EXACT}
    return {"mapped_tickers": [], "mapped_entities": [], "company_id": None,
            "state": EM_UNMATCHED}


class RssAtomCollector(BaseCollector):
    """Generic RSS/Atom collector, driven feed-by-feed by the Stage 3.5 engine."""

    source_id = SOURCE_ID
    requires_credential = False

    def __init__(self, ctx) -> None:  # noqa: ANN001
        super().__init__(ctx)
        self.per_feed: dict[str, dict] = {}
        self.seen_links: set[str] = set()
        self.seen_native: set[str] = set()
        self.seen_title_fp: set[str] = set()
        self.duplicates_prevented = 0
        self.known_tickers: set[str] = {
            str(t).upper() for t in ctx.source_cfg.get("known_tickers", [])}

    # ------------------------------------------------------------------ #
    def _limits(self) -> dict:
        return self.ctx.config.get("limits", {})

    def _conditional_fetch(self, feed: dict, prior_etag: Optional[str],
                           prior_lm: Optional[str]) -> dict:
        """One conditional GET for a feed. Returns a structured result; 304 is a
        clean no-new-data outcome. Never raises."""
        url = feed.get("feed_url", "")
        result = {"ok": False, "status": None, "body": None, "raw": None,
                  "etag": prior_etag, "last_modified": prior_lm,
                  "not_modified": False, "rejected_reason": None,
                  "error": None, "retries": 0}
        limits = self._limits()
        max_retries = int(limits.get("max_retries", 2))
        backoff = float(limits.get("backoff_base_seconds", 1.0))
        multiplier = float(limits.get("backoff_multiplier", 2.0))
        timeout = float(limits.get("http_timeout_seconds", 30))
        max_bytes = int(limits.get("raw_object_max_bytes", 8388608))
        hdrs = {"Accept": "application/rss+xml, application/atom+xml, "
                          "application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5"}
        if self.ctx.user_agent:
            hdrs["User-Agent"] = self.ctx.user_agent
        if prior_etag:
            hdrs["If-None-Match"] = prior_etag
        if prior_lm:
            hdrs["If-Modified-Since"] = prior_lm
        fp = self._fingerprint("GET", url)

        self._rate_limit_wait()
        attempt = 0
        resp: dict = {}
        while True:
            self.requests_attempted += 1
            self.last_attempt_at = self.ctx.now_iso()
            resp = self.ctx.transport(
                {"method": "GET", "url": url, "headers": dict(hdrs)}, timeout)
            status = resp.get("status", 0)
            error = resp.get("error")
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

        status = resp.get("status", 0)
        headers = {k.lower(): v for k, v in (resp.get("headers") or {}).items()}
        body = resp.get("body", b"") or b""
        error = resp.get("error")
        result["status"] = status
        result["etag"] = headers.get("etag", prior_etag)
        result["last_modified"] = headers.get("last-modified", prior_lm)

        # Redirect scheme validation (downgrade to a non-HTTP(S) scheme is fatal).
        for hop in resp.get("redirect_chain", []) or []:
            low = str(hop).lower()
            if not (low.startswith("http://") or low.startswith("https://")):
                result["rejected_reason"] = "UNSAFE_REDIRECT"
                self.record_error("UNSAFE_REDIRECT",
                                  "non-HTTP redirect hop rejected (%s)" % fp)
                self._register_failure()
                return result

        if error is not None:
            self.http_errors += 1
            self._register_failure()
            result["error"] = self._redact(error)
            self.record_error("NETWORK_ERROR", "%s (%s)" % (error, fp),
                              retry_count=result["retries"])
            return result
        if status == 304:
            self._register_success()
            result["not_modified"] = True
            result["ok"] = True
            return result
        if status == 429:
            self.rate_limited_hits += 1
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "RATE_LIMITED"
            self.record_error("RATE_LIMITED",
                              "429 not treated as data (%s)" % fp, http_status=429)
            return result
        if status != 200:
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "HTTP_%s" % status
            self.record_error("HTTP_ERROR", "HTTP %s (%s)" % (status, fp),
                              http_status=status, retry_count=result["retries"])
            return result
        ctype = str(headers.get("content-type", "")).lower()
        if ctype and any(bad in ctype for bad in _FEED_CONTENT_BAD) \
                and not any(ok in ctype for ok in ("xml", "rss", "atom")):
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "CONTENT_TYPE_NOT_FEED"
            self.record_error("CONTENT_TYPE_NOT_FEED",
                              "content-type %r is not a feed (%s)" % (ctype, fp),
                              http_status=200)
            return result
        if len(body) == 0:
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "ZERO_BYTE_RESPONSE"
            self.record_error("ZERO_BYTE_RESPONSE",
                              "zero-byte 200 rejected (%s)" % fp, http_status=200)
            return result
        head = body.lstrip()[:64].lower()
        if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "HTML_ERROR_RESPONSE"
            self.record_error("HTML_ERROR_RESPONSE",
                              "HTML page where feed expected; not parsed (%s)" % fp,
                              http_status=200)
            return result
        if len(body) > max_bytes:
            self.http_errors += 1
            self._register_failure()
            result["rejected_reason"] = "RAW_OBJECT_TOO_LARGE"
            self.record_error("RAW_OBJECT_TOO_LARGE",
                              "%d bytes exceeds %d (%s)" % (len(body), max_bytes, fp),
                              http_status=200)
            return result

        self._register_success()
        raw = self.ctx.archive.store(
            source_id=feed.get("feed_id", "rss_atom"), content=body,
            extension="xml", retrieved_at=self.ctx.now_iso(), business_date=None,
            source_native_id=feed.get("feed_id", ""), request_fp=fp,
            content_type="application/xml",
            http_status=status, retry_count=result["retries"],
            published_at=result["last_modified"],
            license_note=feed.get("license_status", ""))
        if not raw["duplicate"]:
            self.raw_objects.append(raw)
        result.update({"ok": True, "body": body, "raw": raw})
        return result

    # ------------------------------------------------------------------ #
    def collect_feed(self, feed: dict, checkpoint: dict, as_of: str) -> dict:
        """Fetch + parse one feed and emit normalized records. Returns per-feed
        health/checkpoint evidence. Circuit-open feeds are suppressed cleanly."""
        feed_id = feed.get("feed_id", "")
        cb_state = checkpoint.get("circuit_breaker_state", CB_CLOSED)
        cons_fail = int(checkpoint.get("consecutive_failures", 0))
        threshold = int(self._limits().get("circuit_breaker_threshold", 4))
        retrieved = self.ctx.now_iso()
        summary = {"feed_id": feed_id, "attempted": True, "not_modified": False,
                   "items_seen": 0, "records_new": 0, "duplicates_prevented": 0,
                   "malformed": False, "raw_object_id": None,
                   "etag": checkpoint.get("etag"),
                   "last_modified": checkpoint.get("last_modified"),
                   "latest_item_time": checkpoint.get("latest_item_time"),
                   "last_success": checkpoint.get("last_success"),
                   "consecutive_failures": cons_fail,
                   "circuit_breaker_state": cb_state, "rejected_reason": None,
                   "health": "HEALTHY", "error": None}
        if cb_state == CB_OPEN and cons_fail >= threshold:
            summary.update({"attempted": False, "health": "CIRCUIT_OPEN",
                            "rejected_reason": "CIRCUIT_OPEN"})
            return summary

        # Reset the base counters' failure view to this feed's persisted state so
        # the breaker is per-feed (the shared BaseCollector counters are ignored
        # for per-feed accounting; we track it explicitly here).
        self.consecutive_failures = cons_fail
        self.circuit_state = cb_state
        res = self._conditional_fetch(feed, checkpoint.get("etag"),
                                      checkpoint.get("last_modified"))
        summary["etag"] = res["etag"]
        summary["last_modified"] = res["last_modified"]
        summary["consecutive_failures"] = self.consecutive_failures
        summary["circuit_breaker_state"] = self.circuit_state

        if res.get("not_modified"):
            summary["not_modified"] = True
            summary["last_success"] = self.ctx.now_iso()
            summary["health"] = "HEALTHY_NOT_MODIFIED"
            return summary
        if not res["ok"]:
            summary["rejected_reason"] = res.get("rejected_reason") or "FETCH_FAILED"
            summary["error"] = res.get("error")
            summary["health"] = "FAILED"
            return summary

        summary["raw_object_id"] = res["raw"]["raw_object_id"] if res["raw"] else None
        summary["last_success"] = self.ctx.now_iso()
        parsed = parse_feed(res["body"])
        if parsed["malformed"]:
            summary["malformed"] = True
            summary["health"] = "DEGRADED"
            summary["rejected_reason"] = parsed["parser_status"]
            self.record_error("FEED_MALFORMED", "feed %s: %s"
                              % (feed_id, parsed["parser_status"]))
            return summary

        latest = summary["latest_item_time"] or ""
        emitted = 0
        dups = 0
        raw_id = summary["raw_object_id"]
        for item in parsed["items"]:
            summary["items_seen"] += 1
            if not item.get("title_present") and not item.get("native_id"):
                continue
            clink = item.get("canonical_link") or ""
            nid = item.get("native_id") or ""
            tfp = item.get("title_fingerprint") or ""
            dup_key_hit = ((clink and clink in self.seen_links)
                           or (nid and nid in self.seen_native)
                           or (tfp and clink and tfp in self.seen_title_fp))
            if dup_key_hit:
                dups += 1
                self.duplicates_prevented += 1
                continue
            if clink:
                self.seen_links.add(clink)
            if nid:
                self.seen_native.add(nid)
            if tfp:
                self.seen_title_fp.add(tfp)
            mapping = resolve_feed_item_entity(feed, item, self.known_tickers)
            rec = build_feed_event_record(
                feed=feed, item=item, raw_object_id=raw_id,
                retrieved_at=retrieved, mapping=mapping,
                license_note=feed.get("license_status", ""))
            self.records.append(rec)
            emitted += 1
            pub = item.get("publication_time") or item.get("updated_time") or ""
            if pub and pub > latest:
                latest = pub
        summary["records_new"] = emitted
        summary["duplicates_prevented"] = dups
        summary["latest_item_time"] = latest or None
        if latest:
            self.note_event_time(latest)
        summary["health"] = "HEALTHY" if emitted or not parsed["items"] else "DEGRADED"
        return summary


__all__ = ["RssAtomCollector", "resolve_feed_item_entity"]
