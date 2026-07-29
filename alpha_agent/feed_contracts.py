"""
alpha_agent/feed_contracts.py — Alpha Agent Stage 3.5 News/RSS-Atom contracts.

The deterministic, standard-library vocabulary of the generalized News/RSS layer:

  * feed schema / collector / clustering versions;
  * the canonical feed-registry field contract and its closed enums
    (feed_format, source_category, trust_level);
  * a single bounded, SAFE RSS 2.0 + Atom parser (stdlib xml.etree only; XXE /
    DOCTYPE / external-entity payloads are quarantined, never expanded);
  * canonical-URL normalization, HTML stripping, bounded fields and stable
    source-native item IDs;
  * deterministic title / summary fingerprints for dedup and clustering;
  * the RSS source-category -> normalized record-type mapping and the
    build_feed_event_record() constructor that emits the SAME point-in-time
    normalized-record contract used everywhere else in the agent.

No network, no database, no model API. Every function is pure and deterministic.
"""
from __future__ import annotations

import datetime as _dt
import email.utils
import re
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Any, Optional

from .research_importers import sha256_text
from .source_contracts import (
    EM_AMBIGUOUS, EM_MATCHED_ALIAS, EM_MATCHED_EXACT, EM_UNMATCHED,
    RT_NEWS_EVENT, RT_PRESS_RELEASE, RT_REGULATORY_EVENT, build_normalized_record,
)

FEED_SCHEMA_VERSION = "1.0.0"
RSS_COLLECTOR_VERSION = "1.0.0"
CLUSTER_ALGO_VERSION = "1.0.0"

SOURCE_ID = "rss_atom"

# --------------------------------------------------------------------------- #
# Closed enums
# --------------------------------------------------------------------------- #
FF_RSS_2_0 = "RSS_2_0"
FF_ATOM = "ATOM"
FF_UNKNOWN = "UNKNOWN"
FEED_FORMATS = (FF_RSS_2_0, FF_ATOM, FF_UNKNOWN)

SC_COMPANY_IR = "COMPANY_IR"
SC_COMPANY_NEWSROOM = "COMPANY_NEWSROOM"
SC_REGULATOR = "REGULATOR"
SC_GOVERNMENT = "GOVERNMENT"
SC_CENTRAL_BANK = "CENTRAL_BANK"
SC_ECONOMIC_RELEASE = "ECONOMIC_RELEASE"
SC_CYBERSECURITY = "CYBERSECURITY"
SC_HEALTH_SAFETY = "HEALTH_SAFETY"
SC_MARKET_INFRASTRUCTURE = "MARKET_INFRASTRUCTURE"
SC_INDUSTRY = "INDUSTRY"
SC_OTHER_OFFICIAL = "OTHER_OFFICIAL"
SOURCE_CATEGORIES = (
    SC_COMPANY_IR, SC_COMPANY_NEWSROOM, SC_REGULATOR, SC_GOVERNMENT,
    SC_CENTRAL_BANK, SC_ECONOMIC_RELEASE, SC_CYBERSECURITY, SC_HEALTH_SAFETY,
    SC_MARKET_INFRASTRUCTURE, SC_INDUSTRY, SC_OTHER_OFFICIAL,
)

TL_PRIMARY_OFFICIAL = "PRIMARY_OFFICIAL"
TL_OFFICIAL_AGENCY = "OFFICIAL_AGENCY"
TL_COMPANY_DIRECT = "COMPANY_DIRECT"
TL_APPROVED_AGGREGATOR = "APPROVED_AGGREGATOR"
TL_UNKNOWN = "UNKNOWN"
TRUST_LEVELS = (TL_PRIMARY_OFFICIAL, TL_OFFICIAL_AGENCY, TL_COMPANY_DIRECT,
                TL_APPROVED_AGGREGATOR, TL_UNKNOWN)
# Only these trust levels may ever be enabled for collection.
ENABLE_ELIGIBLE_TRUST = (TL_PRIMARY_OFFICIAL, TL_OFFICIAL_AGENCY,
                         TL_COMPANY_DIRECT, TL_APPROVED_AGGREGATOR)

# Deterministic source-category -> normalized record-type mapping.
SOURCE_CATEGORY_TO_RECORD_TYPE = {
    SC_COMPANY_IR: RT_PRESS_RELEASE,
    SC_COMPANY_NEWSROOM: RT_PRESS_RELEASE,
    SC_REGULATOR: RT_REGULATORY_EVENT,
    SC_GOVERNMENT: RT_REGULATORY_EVENT,
    SC_CENTRAL_BANK: RT_REGULATORY_EVENT,
    SC_ECONOMIC_RELEASE: RT_REGULATORY_EVENT,
    SC_CYBERSECURITY: RT_REGULATORY_EVENT,
    SC_HEALTH_SAFETY: RT_REGULATORY_EVENT,
    SC_MARKET_INFRASTRUCTURE: RT_REGULATORY_EVENT,
    SC_INDUSTRY: RT_NEWS_EVENT,
    SC_OTHER_OFFICIAL: RT_NEWS_EVENT,
}

# The 24-field canonical feed-registry contract.
FEED_REGISTRY_FIELDS = (
    "feed_id", "feed_url", "canonical_url", "feed_format", "publisher",
    "source_category", "official_source", "trust_level", "license_status",
    "allowed_storage", "covered_tickers", "covered_sectors", "jurisdiction",
    "language", "enabled", "polling_interval_minutes", "priority",
    "discovery_method", "discovered_at", "validated_at", "last_attempt",
    "last_success", "latest_item_time", "etag", "last_modified",
    "consecutive_failures", "circuit_breaker_state", "notes",
)

# Bounded storage policy: never persist an unrestricted article body.
ALLOWED_STORAGE_BOUNDED = "BOUNDED_SNIPPET_ONLY"
DEFAULT_SUMMARY_MAX_CHARS = 600
DEFAULT_TITLE_MAX_CHARS = 300

# Tracking / campaign query parameters removed during canonicalization.
_TRACKING_PARAMS = frozenset((
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "utm_id", "utm_name", "utm_reader", "gclid", "fbclid", "mc_cid", "mc_eid",
    "igshid", "ref", "ref_src", "cmpid", "cid", "_ga",
))

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_ENTITY_RE = re.compile(r"&(#\d+|#x[0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]*);")
_NONALNUM_RE = re.compile(r"[^a-z0-9\s]+")
_UNSAFE_XML = re.compile(rb"<!DOCTYPE|<!ENTITY", re.IGNORECASE)

_NAMED_ENTITIES = {
    "amp": "&", "lt": "<", "gt": ">", "quot": '"', "apos": "'", "nbsp": " ",
    "mdash": "-", "ndash": "-", "hellip": "...", "rsquo": "'", "lsquo": "'",
    "ldquo": '"', "rdquo": '"', "#39": "'",
}


# --------------------------------------------------------------------------- #
# Text hygiene
# --------------------------------------------------------------------------- #
def _decode_entity(match: "re.Match[str]") -> str:
    body = match.group(1)
    if body.startswith("#x") or body.startswith("#X"):
        try:
            return chr(int(body[2:], 16))
        except ValueError:
            return " "
    if body.startswith("#"):
        try:
            return chr(int(body[1:]))
        except ValueError:
            return " "
    return _NAMED_ENTITIES.get(body, " ")


def strip_html(text: Optional[str]) -> str:
    """Deterministically strip HTML tags and decode common entities from a feed
    summary. No JavaScript is ever executed; script/style blocks are removed
    with their content, then all remaining tags. Whitespace is collapsed."""
    if not text:
        return ""
    out = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", text)
    out = _TAG_RE.sub(" ", out)
    out = _ENTITY_RE.sub(_decode_entity, out)
    out = _WS_RE.sub(" ", out).strip()
    return out


def bounded(text: Optional[str], max_chars: int) -> tuple[str, bool]:
    """Return (bounded_text, truncated?). Never persists unbounded content."""
    s = "" if text is None else str(text)
    if len(s) <= max_chars:
        return s, False
    return s[:max_chars].rstrip(), True


def canonical_link(url: Optional[str]) -> str:
    """Canonical-URL normalization for dedup: lowercase scheme+host, drop the
    default port, drop the fragment and tracking parameters, sort the remaining
    query, and drop a trailing slash. Deterministic and idempotent."""
    if not url:
        return ""
    raw = str(url).strip()
    try:
        parts = urllib.parse.urlsplit(raw)
    except ValueError:
        return raw
    scheme = (parts.scheme or "").lower()
    host = (parts.hostname or "").lower()
    if not host:
        # Relative or malformed link: normalize whitespace only.
        return raw
    netloc = host
    if parts.port and not ((scheme == "http" and parts.port == 80)
                           or (scheme == "https" and parts.port == 443)):
        netloc = "%s:%d" % (host, parts.port)
    path = parts.path or ""
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    pairs = [(k, v) for k, v in urllib.parse.parse_qsl(parts.query,
                                                       keep_blank_values=True)
             if k.lower() not in _TRACKING_PARAMS]
    query = urllib.parse.urlencode(sorted(pairs))
    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def normalize_title(title: Optional[str]) -> str:
    """Lowercased, punctuation-stripped, whitespace-collapsed title."""
    if not title:
        return ""
    low = strip_html(title).lower()
    low = _NONALNUM_RE.sub(" ", low)
    return _WS_RE.sub(" ", low).strip()


def title_fingerprint(title: Optional[str]) -> str:
    return sha256_text(normalize_title(title))[:32]


def title_tokens(title: Optional[str]) -> frozenset:
    return frozenset(t for t in normalize_title(title).split() if t)


def summary_fingerprint(summary: Optional[str]) -> str:
    return sha256_text(normalize_title(summary))[:32]


# --------------------------------------------------------------------------- #
# Timestamp parsing (never fabricate)
# --------------------------------------------------------------------------- #
def parse_timestamp(value: Optional[str]) -> Optional[str]:
    """Parse an RSS RFC-822 or Atom ISO-8601 timestamp to ISO-8601, else None.
    Never fabricates a value."""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    # RFC-822 (RSS pubDate).
    try:
        dt = email.utils.parsedate_to_datetime(text)
        if dt is not None:
            return dt.isoformat()
    except (TypeError, ValueError):
        pass
    # ISO-8601 (Atom updated/published).
    iso = text.replace("Z", "+00:00")
    try:
        return _dt.datetime.fromisoformat(iso).isoformat()
    except ValueError:
        pass
    try:  # date only
        return _dt.date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# SAFE RSS 2.0 / Atom parser
# --------------------------------------------------------------------------- #
def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _text_of(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    return (el.text or "").strip()


def _decode_bytes(body: bytes) -> str:
    """Decode using the XML declaration's encoding if present, else UTF-8."""
    head = body[:200].lower()
    match = re.search(rb'encoding=["\']([a-z0-9\-_]+)["\']', head)
    if match:
        try:
            return body.decode(match.group(1).decode("ascii"), errors="replace")
        except (LookupError, ValueError):
            pass
    return body.decode("utf-8", errors="replace")


def detect_format(body: bytes) -> str:
    try:
        root = ET.fromstring(_decode_bytes(body))
    except ET.ParseError:
        return FF_UNKNOWN
    tag = _strip_ns(root.tag).lower()
    if tag == "feed":
        return FF_ATOM
    if tag == "rss" or tag == "rdf":
        return FF_RSS_2_0
    return FF_UNKNOWN


def parse_feed(body: bytes, *, summary_max_chars: int = DEFAULT_SUMMARY_MAX_CHARS,
               title_max_chars: int = DEFAULT_TITLE_MAX_CHARS) -> dict:
    """Parse RSS 2.0 or Atom into a bounded, safe item list.

    Returns ``{"format", "parser_status", "malformed", "feed_title",
    "feed_updated", "items": [...]}``. Each item carries a stable native id,
    bounded title + summary, canonical link, publication/updated times, author
    and categories. Unsafe XML (DOCTYPE / ENTITY) is quarantined, never parsed.
    Malformed XML yields malformed=True with zero items — never an exception."""
    if not body:
        return {"format": FF_UNKNOWN, "parser_status": "PS_EMPTY",
                "malformed": True, "feed_title": None, "feed_updated": None,
                "items": []}
    if _UNSAFE_XML.search(body):
        return {"format": FF_UNKNOWN, "parser_status": "PS_QUARANTINE_UNSAFE_XML",
                "malformed": True, "feed_title": None, "feed_updated": None,
                "items": []}
    try:
        root = ET.fromstring(_decode_bytes(body))
    except ET.ParseError as exc:
        return {"format": FF_UNKNOWN, "parser_status": "PS_MALFORMED_XML:%s"
                % str(exc)[:80], "malformed": True, "feed_title": None,
                "feed_updated": None, "items": []}

    tag = _strip_ns(root.tag).lower()
    if tag == "feed":
        return _parse_atom(root, summary_max_chars, title_max_chars)
    if tag in ("rss", "rdf"):
        return _parse_rss(root, summary_max_chars, title_max_chars)
    return {"format": FF_UNKNOWN, "parser_status": "PS_UNSUPPORTED_ROOT:%s" % tag,
            "malformed": True, "feed_title": None, "feed_updated": None,
            "items": []}


def _children_by_local(el: ET.Element) -> dict[str, list[ET.Element]]:
    out: dict[str, list[ET.Element]] = {}
    for child in el:
        out.setdefault(_strip_ns(child.tag).lower(), []).append(child)
    return out


def _first(cmap: dict[str, list[ET.Element]], *names: str) -> Optional[ET.Element]:
    for name in names:
        if cmap.get(name):
            return cmap[name][0]
    return None


def _parse_rss(root: ET.Element, summary_max: int, title_max: int) -> dict:
    channel = None
    for child in root.iter():
        if _strip_ns(child.tag).lower() == "channel":
            channel = child
            break
    scope = channel if channel is not None else root
    cmap = _children_by_local(scope)
    feed_title = _text_of(_first(cmap, "title")) or None
    feed_updated = parse_timestamp(_text_of(_first(cmap, "lastbuilddate", "pubdate")))
    items: list[dict] = []
    for item in scope.iter():
        if _strip_ns(item.tag).lower() != "item":
            continue
        im = _children_by_local(item)
        title_raw = _text_of(_first(im, "title"))
        link = _text_of(_first(im, "link"))
        if not link and im.get("link"):
            link = im["link"][0].get("href", "") or _text_of(im["link"][0])
        guid_el = _first(im, "guid")
        guid = _text_of(guid_el)
        summary_raw = _text_of(_first(im, "description", "encoded", "summary"))
        published = parse_timestamp(_text_of(_first(im, "pubdate", "date", "published")))
        updated = parse_timestamp(_text_of(_first(im, "updated", "lastbuilddate")))
        author = _text_of(_first(im, "author", "creator")) or None
        categories = sorted({_text_of(c) for c in im.get("category", []) if _text_of(c)})
        items.append(_mk_item(title_raw, summary_raw, link, guid, published,
                              updated, author, categories, summary_max, title_max))
    return {"format": FF_RSS_2_0, "parser_status": "PS_OK", "malformed": False,
            "feed_title": feed_title, "feed_updated": feed_updated, "items": items}


def _parse_atom(root: ET.Element, summary_max: int, title_max: int) -> dict:
    cmap = _children_by_local(root)
    feed_title = _text_of(_first(cmap, "title")) or None
    feed_updated = parse_timestamp(_text_of(_first(cmap, "updated")))
    items: list[dict] = []
    for entry in root:
        if _strip_ns(entry.tag).lower() != "entry":
            continue
        em = _children_by_local(entry)
        title_raw = _text_of(_first(em, "title"))
        link = ""
        for le in em.get("link", []):
            rel = (le.get("rel") or "alternate").lower()
            if rel == "alternate" and le.get("href"):
                link = le.get("href")
                break
        if not link and em.get("link"):
            link = em["link"][0].get("href", "")
        guid = _text_of(_first(em, "id"))
        summary_raw = _text_of(_first(em, "summary", "content"))
        published = parse_timestamp(_text_of(_first(em, "published", "issued")))
        updated = parse_timestamp(_text_of(_first(em, "updated", "modified")))
        author = None
        author_el = _first(em, "author")
        if author_el is not None:
            for c in author_el:
                if _strip_ns(c.tag).lower() == "name":
                    author = (c.text or "").strip() or None
                    break
        categories = sorted({c.get("term") for c in em.get("category", [])
                             if c.get("term")})
        items.append(_mk_item(title_raw, summary_raw, link, guid, published,
                              updated, author, categories, summary_max, title_max))
    return {"format": FF_ATOM, "parser_status": "PS_OK", "malformed": False,
            "feed_title": feed_title, "feed_updated": feed_updated, "items": items}


def _mk_item(title_raw: str, summary_raw: str, link: str, guid: str,
             published: Optional[str], updated: Optional[str],
             author: Optional[str], categories: list[str],
             summary_max: int, title_max: int) -> dict:
    title_clean = strip_html(title_raw)
    title, title_trunc = bounded(title_clean, title_max)
    summary_clean = strip_html(summary_raw)
    summary, summary_trunc = bounded(summary_clean, summary_max)
    clink = canonical_link(link)
    native_id = stable_item_id(guid=guid, canonical=clink, title=title_clean,
                               published=published)
    warnings: list[str] = []
    if not published and not updated:
        warnings.append("PUBLICATION_TIME_ABSENT: item carried no pubDate/"
                        "updated; availability falls back to retrieval time")
    return {
        "native_id": native_id, "guid": guid or None,
        "title": title, "title_present": bool(title_clean),
        "bounded_summary": summary, "content_truncated": title_trunc or summary_trunc,
        "canonical_link": clink, "raw_link": link or None,
        "publication_time": published, "updated_time": updated,
        "author": author, "categories": categories,
        "title_fingerprint": title_fingerprint(title_clean),
        "summary_fingerprint": summary_fingerprint(summary_clean),
        "quality_warnings": warnings,
    }


def stable_item_id(*, guid: str, canonical: str, title: str,
                   published: Optional[str]) -> str:
    """Stable, source-native item ID: prefer an explicit guid/id, then the
    canonical link, then a deterministic hash of title + publication time. The
    SAME item always maps to the SAME id (idempotent dedup)."""
    if guid:
        return "it_" + sha256_text("guid|%s" % guid.strip())[:24]
    if canonical:
        return "it_" + sha256_text("link|%s" % canonical)[:24]
    return "it_" + sha256_text("tp|%s|%s" % (normalize_title(title),
                                             published or ""))[:24]


# --------------------------------------------------------------------------- #
# Normalized record construction
# --------------------------------------------------------------------------- #
def record_type_for_category(source_category: str) -> str:
    return SOURCE_CATEGORY_TO_RECORD_TYPE.get(source_category, RT_NEWS_EVENT)


def build_feed_event_record(*, feed: dict, item: dict, raw_object_id: Optional[str],
                            retrieved_at: str, mapping: dict,
                            license_note: str) -> dict:
    """Build the normalized News/RSS event record (source_id='rss_atom').

    * record_type derived deterministically from the feed's source_category;
    * available_at = item publication time when present, else retrieval time
      (flagged with an explicit quality warning — never silently fabricated);
    * only bounded title + capped summary are ever stored; feed_id lives in the
      normalized_payload, never conflated with the shared source_id."""
    record_type = record_type_for_category(feed.get("source_category", ""))
    publication = item.get("publication_time")
    updated = item.get("updated_time")
    warnings = list(item.get("quality_warnings", []))
    available_at = publication or retrieved_at
    effective = (publication or updated or retrieved_at or "")[:10] or None
    tickers = mapping.get("mapped_tickers", [])
    payload = {
        "feed_id": feed.get("feed_id"),
        "publisher": feed.get("publisher"),
        "source_category": feed.get("source_category"),
        "trust_level": feed.get("trust_level"),
        "official_source": feed.get("official_source"),
        "jurisdiction": feed.get("jurisdiction"),
        "language": feed.get("language"),
        "title": item.get("title"),
        "bounded_summary": item.get("bounded_summary"),
        "canonical_link": item.get("canonical_link"),
        "source_native_item_id": item.get("native_id"),
        "publication_time": publication,
        "updated_time": updated,
        "author": item.get("author"),
        "categories": item.get("categories", []),
        "mapped_tickers": tickers,
        "mapped_entities": mapping.get("mapped_entities", []),
        "entity_mapping_state": mapping.get("state", EM_UNMATCHED),
        "content_truncated": bool(item.get("content_truncated")),
        "allowed_storage": ALLOWED_STORAGE_BOUNDED,
        "cluster_id": None,
        "licensing_note": license_note,
        "title_fingerprint": item.get("title_fingerprint"),
        "summary_fingerprint": item.get("summary_fingerprint"),
    }
    single_ticker = tickers[0] if len(tickers) == 1 else None
    return build_normalized_record(
        record_type=record_type, source_id=SOURCE_ID,
        source_native_id="%s|%s" % (feed.get("feed_id"), item.get("native_id")),
        raw_object_id=raw_object_id, retrieved_at=retrieved_at,
        observed_at=publication, available_at=available_at,
        effective_at=effective, ticker=single_ticker,
        company_id=mapping.get("company_id"),
        event_type="%s:%s" % (record_type, feed.get("source_category")),
        payload=payload,
        entity_mapping_confidence=mapping.get("state", EM_UNMATCHED),
        provenance="RSS/Atom feed %s (%s) — official=%s trust=%s" % (
            feed.get("feed_id"), feed.get("publisher"),
            feed.get("official_source"), feed.get("trust_level")),
        quality_warnings=warnings)


__all__ = [
    "FEED_SCHEMA_VERSION", "RSS_COLLECTOR_VERSION", "CLUSTER_ALGO_VERSION",
    "SOURCE_ID", "FF_RSS_2_0", "FF_ATOM", "FF_UNKNOWN", "FEED_FORMATS",
    "SOURCE_CATEGORIES", "TRUST_LEVELS", "ENABLE_ELIGIBLE_TRUST",
    "SOURCE_CATEGORY_TO_RECORD_TYPE", "FEED_REGISTRY_FIELDS",
    "ALLOWED_STORAGE_BOUNDED", "DEFAULT_SUMMARY_MAX_CHARS",
    "DEFAULT_TITLE_MAX_CHARS", "SC_COMPANY_IR", "SC_COMPANY_NEWSROOM",
    "SC_REGULATOR", "SC_GOVERNMENT", "SC_CENTRAL_BANK", "SC_ECONOMIC_RELEASE",
    "SC_CYBERSECURITY", "SC_HEALTH_SAFETY", "SC_MARKET_INFRASTRUCTURE",
    "SC_INDUSTRY", "SC_OTHER_OFFICIAL", "TL_PRIMARY_OFFICIAL",
    "TL_OFFICIAL_AGENCY", "TL_COMPANY_DIRECT", "TL_APPROVED_AGGREGATOR",
    "TL_UNKNOWN", "strip_html", "bounded", "canonical_link", "normalize_title",
    "title_fingerprint", "title_tokens", "summary_fingerprint",
    "parse_timestamp", "detect_format", "parse_feed", "stable_item_id",
    "record_type_for_category", "build_feed_event_record",
]
