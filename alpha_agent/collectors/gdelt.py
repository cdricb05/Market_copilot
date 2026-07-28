"""
alpha_agent/collectors/gdelt.py — SOURCE 7 (OPTIONAL, DEFERRED): GDELT discovery feed.

Fully specified deferred adapter. It is DISABLED in the Stage 2 production
configuration (``enabled: false, deferred: true``) so that the six core
collectors are proven first; enabling it later requires only the config flag.

Contract when enabled:
  * discovery feed only — never a trusted sentiment oracle (source_confidence
    is capped below 1.0 and every entity mapping is marked AMBIGUOUS);
  * METADATA-ONLY storage: headline, canonical URL, domain, timestamps,
    language, source country, themes and a bounded snippet — NEVER unrestricted
    copyrighted full article text;
  * collection restricted to the configured query symbols (current investable
    universe subset) with a bounded article count;
  * stories deduplicated by canonical URL + content fingerprint.
"""
from __future__ import annotations

import json
import urllib.parse
from typing import Optional

from ..source_contracts import (
    EM_AMBIGUOUS, RT_NEWS_EVENT, SH_DEGRADED, SH_FAILED, SH_HEALTHY,
    SH_NOT_RUN, build_normalized_record, sha256_text,
)
from .base import BaseCollector

_TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term",
                    "utm_content", "fbclid", "gclid"}


def canonical_url(url: str) -> str:
    """Canonical story URL: lowercase scheme/host, tracking params stripped."""
    try:
        parts = urllib.parse.urlsplit(url)
        pairs = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query,
                                                           keep_blank_values=True)
                 if k.lower() not in _TRACKING_PARAMS]
        return urllib.parse.urlunsplit(
            (parts.scheme.lower(), parts.netloc.lower(), parts.path,
             urllib.parse.urlencode(pairs), ""))
    except Exception:  # noqa: BLE001
        return url


def _seendate_iso(seendate: str) -> Optional[str]:
    """GDELT seendate 'YYYYMMDDTHHMMSSZ' -> ISO-8601, or None."""
    s = (seendate or "").strip()
    if len(s) >= 15 and s[8] == "T":
        try:
            return "%s-%s-%sT%s:%s:%sZ" % (s[0:4], s[4:6], s[6:8],
                                           s[9:11], s[11:13], s[13:15])
        except Exception:  # noqa: BLE001
            return None
    return None


class GdeltCollector(BaseCollector):
    source_id = "gdelt"
    requires_credential = False

    def deferred_result(self) -> dict:
        reason = self.ctx.source_cfg.get("defer_reason", "deferred")
        self.inventory["deferred"] = True
        self.inventory["defer_reason"] = reason
        return self.result(overall_state=SH_NOT_RUN,
                           entitlement_summary="DEFERRED adapter: %s" % reason[:160])

    def audit(self) -> dict:
        if self.ctx.source_cfg.get("deferred") or not self.ctx.source_cfg.get("enabled"):
            return self.deferred_result()
        url = "%s?%s" % (self.ctx.source_cfg.get("base_url"),
                         urllib.parse.urlencode({"query": "markets", "mode": "artlist",
                                                 "maxrecords": 1, "format": "json"}))
        res = self.fetch(url, expect="text", archive=False, extension="json")
        return self.result(overall_state=SH_HEALTHY if res["ok"] else SH_FAILED,
                           entitlement_summary="public discovery feed")

    def collect(self, as_of: str) -> dict:
        if self.ctx.source_cfg.get("deferred") or not self.ctx.source_cfg.get("enabled"):
            return self.deferred_result()
        cfg = self.ctx.source_cfg
        retrieved = self.ctx.now_iso()
        max_articles = int(cfg.get("max_articles", 50))
        snippet_max = int(cfg.get("snippet_max_chars", 280))
        seen_fingerprints: set[str] = set(self.cursor.get("seen_fingerprints", []))
        queries_ok = 0
        for symbol in cfg.get("query_symbols", []):
            url = "%s?%s" % (cfg.get("base_url"), urllib.parse.urlencode({
                "query": '"%s" sourcelang:english' % symbol, "mode": "artlist",
                "maxrecords": max_articles, "format": "json", "timespan": "3d"}))
            res = self.fetch(url, expect="text", extension="json",
                             business_date=as_of, native_id="artlist|%s" % symbol,
                             content_type="application/json")
            if not res["ok"]:
                continue
            try:
                obj = json.loads(res["body"].decode("utf-8", errors="replace"))
            except ValueError:
                self.record_error("PARSE_ERROR", "GDELT artlist not JSON for %s" % symbol)
                continue
            queries_ok += 1
            for article in obj.get("articles", []) or []:
                if not isinstance(article, dict) or not article.get("url"):
                    continue
                curl = canonical_url(str(article["url"]))
                title = str(article.get("title") or "")[:snippet_max]
                fingerprint = sha256_text("%s|%s" % (curl, title))[:24]
                if fingerprint in seen_fingerprints:
                    continue
                seen_fingerprints.add(fingerprint)
                seen_iso = _seendate_iso(str(article.get("seendate") or ""))
                # METADATA ONLY — no article body is ever stored.
                payload = {
                    "title_snippet": title,
                    "canonical_url": curl,
                    "domain": article.get("domain"),
                    "language": article.get("language"),
                    "source_country": article.get("sourcecountry"),
                    "seen_time": seen_iso,
                    "publication_time": seen_iso,
                    "query_symbol": symbol,
                    "content_fingerprint": fingerprint,
                    "metadata_only": True,
                }
                self.records.append(build_normalized_record(
                    record_type=RT_NEWS_EVENT, source_id=self.source_id,
                    source_native_id="gdelt|%s" % fingerprint,
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=seen_iso,
                    effective_at=(seen_iso or "")[:10] or None,
                    available_at=seen_iso, ticker=symbol,
                    event_type="NEWS_DISCOVERY", payload=payload,
                    source_confidence=0.5,
                    entity_mapping_confidence=EM_AMBIGUOUS,
                    provenance="GDELT DOC 2.0 artlist (discovery feed; query-derived "
                               "entity association)"))
                if seen_iso:
                    self.note_event_time(seen_iso)
        self.cursor = {"last_collected_as_of": as_of,
                       "seen_fingerprints": sorted(seen_fingerprints)[-2000:]}
        total = len(cfg.get("query_symbols", []))
        if queries_ok == total and total:
            state = SH_HEALTHY
        elif queries_ok:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="public discovery feed (metadata-only)")
