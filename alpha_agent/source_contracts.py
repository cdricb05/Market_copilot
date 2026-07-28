"""
alpha_agent/source_contracts.py — Alpha Agent Stage 2 canonical source & record contracts.

Defines the shared, deterministic vocabulary of the data-acquisition layer:

  * ingestion schema / collector versions;
  * normalized record types and the normalized-record contract constructor;
  * entitlement, source-health and entity-mapping state enums;
  * content-addressed raw-object IDs and deterministic normalized-record IDs;
  * secret redaction for request fingerprints, errors and logs;
  * point-in-time timestamp discipline (never substitute period_end for
    publication/availability; never fabricate a timestamp — use null + warning);
  * the deterministic identity-resolution layer (no LLM, no fuzzy matching);
  * the static Stage 1 information-family -> Stage 2 source gap mapping.

Standard library only. No network, no database drivers, no model APIs.
"""
from __future__ import annotations

import urllib.parse
from typing import Any, Optional

from .research_importers import canonical_json, sha256_hex, sha256_text  # noqa: F401

INGESTION_SCHEMA_VERSION = "1.0.0"
COLLECTOR_VERSION = "1.0.0"
RECORD_SCHEMA_VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# Normalized record types
# --------------------------------------------------------------------------- #
RT_MARKET_BAR = "MARKET_BAR"
RT_CORPORATE_ACTION = "CORPORATE_ACTION"
RT_UNIVERSE_MEMBERSHIP = "UNIVERSE_MEMBERSHIP"
RT_SECURITY_IDENTITY = "SECURITY_IDENTITY"
RT_FILING_EVENT = "FILING_EVENT"
RT_FUNDAMENTAL_FACT = "FUNDAMENTAL_FACT"
RT_INSIDER_FILING = "INSIDER_FILING"
RT_EARNINGS_EVENT = "EARNINGS_EVENT"
RT_NEWS_EVENT = "NEWS_EVENT"
RT_MACRO_OBSERVATION = "MACRO_OBSERVATION"
RT_SHORT_VOLUME = "SHORT_VOLUME"
RT_TRADING_HALT = "TRADING_HALT"
RT_SOURCE_HEALTH = "SOURCE_HEALTH"

RECORD_TYPES = (
    RT_MARKET_BAR, RT_CORPORATE_ACTION, RT_UNIVERSE_MEMBERSHIP, RT_SECURITY_IDENTITY,
    RT_FILING_EVENT, RT_FUNDAMENTAL_FACT, RT_INSIDER_FILING, RT_EARNINGS_EVENT,
    RT_NEWS_EVENT, RT_MACRO_OBSERVATION, RT_SHORT_VOLUME, RT_TRADING_HALT,
    RT_SOURCE_HEALTH,
)

# --------------------------------------------------------------------------- #
# Entitlement / health / entity states
# --------------------------------------------------------------------------- #
ENT_ENTITLED = "ENTITLED"
ENT_NOT_ENTITLED = "NOT_ENTITLED"
ENT_UNKNOWN = "UNKNOWN"
ENT_AUTH_FAILED = "AUTHENTICATION_FAILED"
ENT_RATE_LIMITED = "RATE_LIMITED"
ENTITLEMENT_STATES = (ENT_ENTITLED, ENT_NOT_ENTITLED, ENT_UNKNOWN,
                      ENT_AUTH_FAILED, ENT_RATE_LIMITED)

SH_HEALTHY = "HEALTHY"
SH_DEGRADED = "DEGRADED"
SH_BLOCKED_CREDENTIAL = "BLOCKED_CREDENTIAL"
SH_BLOCKED_ENTITLEMENT = "BLOCKED_ENTITLEMENT"
SH_BLOCKED_LICENSE = "BLOCKED_LICENSE"
SH_BLOCKED_CONFIGURATION = "BLOCKED_CONFIGURATION"
SH_RATE_LIMITED = "RATE_LIMITED"
SH_FAILED = "FAILED"
SH_NOT_CONFIGURED = "NOT_CONFIGURED"
SH_NOT_RUN = "NOT_RUN"
SOURCE_HEALTH_STATES = (
    SH_HEALTHY, SH_DEGRADED, SH_BLOCKED_CREDENTIAL, SH_BLOCKED_ENTITLEMENT,
    SH_BLOCKED_LICENSE, SH_BLOCKED_CONFIGURATION, SH_RATE_LIMITED, SH_FAILED,
    SH_NOT_CONFIGURED, SH_NOT_RUN,
)
BLOCKED_HEALTH_STATES = (SH_BLOCKED_CREDENTIAL, SH_BLOCKED_ENTITLEMENT,
                         SH_BLOCKED_LICENSE, SH_BLOCKED_CONFIGURATION,
                         SH_NOT_CONFIGURED)

EM_MATCHED_EXACT = "MATCHED_EXACT"
EM_MATCHED_HISTORICAL = "MATCHED_HISTORICAL"
EM_MATCHED_ALIAS = "MATCHED_ALIAS"
EM_AMBIGUOUS = "AMBIGUOUS"
EM_UNMATCHED = "UNMATCHED"
ENTITY_MAPPING_STATES = (EM_MATCHED_EXACT, EM_MATCHED_HISTORICAL,
                         EM_MATCHED_ALIAS, EM_AMBIGUOUS, EM_UNMATCHED)

GAP_ADDRESSED = "ADDRESSED"
GAP_PARTIALLY_ADDRESSED = "PARTIALLY_ADDRESSED"
GAP_STILL_BLOCKED = "STILL_BLOCKED"
GAP_NOT_APPLICABLE = "NOT_APPLICABLE"

# Circuit-breaker states persisted per source checkpoint.
CB_CLOSED = "CLOSED"
CB_OPEN = "OPEN"

# --------------------------------------------------------------------------- #
# Deterministic IDs
# --------------------------------------------------------------------------- #

def make_raw_object_id(source_id: str, content: bytes) -> str:
    """Content-addressed raw object ID (per source). Identical content retrieved
    again from the same source ALWAYS maps to the same ID."""
    import hashlib
    h = hashlib.sha256()
    h.update(source_id.encode("utf-8"))
    h.update(b"\x00")
    h.update(content)
    return "raw_" + h.hexdigest()[:32]


def make_record_id(record_type: str, source_id: str, source_native_id: str,
                   payload_hash: str) -> str:
    """Deterministic normalized-record ID. Duplicate (type, source, native id,
    payload) tuples always collapse to the same ID."""
    return "rec_" + sha256_text(
        "%s|%s|%s|%s" % (record_type, source_id, source_native_id, payload_hash))[:32]


def payload_hash_of(payload: Any) -> str:
    return sha256_text(canonical_json(payload))


# --------------------------------------------------------------------------- #
# Secret redaction
# --------------------------------------------------------------------------- #

def redact_url(url: str, redacted_params: list[str], placeholder: str = "REDACTED") -> str:
    """Return the URL with the VALUES of secret-bearing query parameters replaced.
    Matching is case-insensitive on the parameter name. The redacted form is the
    only form that may ever be persisted or logged."""
    try:
        parts = urllib.parse.urlsplit(url)
        if not parts.query:
            return url
        lowered = [p.lower() for p in redacted_params]
        out_pairs = []
        for key, val in urllib.parse.parse_qsl(parts.query, keep_blank_values=True):
            if key.lower() in lowered:
                out_pairs.append((key, placeholder))
            else:
                out_pairs.append((key, val))
        new_query = urllib.parse.urlencode(out_pairs)
        return urllib.parse.urlunsplit(
            (parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
    except Exception:  # noqa: BLE001 — redaction must never raise; fail safe
        base = url.split("?", 1)[0]
        return base + "?" + placeholder


def redact_text(text: str, secrets: list[str], placeholder: str = "***") -> str:
    """Replace every occurrence of every known in-memory secret VALUE in a text
    (error messages, diagnostics) with a placeholder before it is stored."""
    if not text:
        return text
    out = text
    for secret in secrets:
        if secret and len(secret) >= 6:
            out = out.replace(secret, placeholder)
    return out


def request_fingerprint(method: str, url: str, redacted_params: list[str],
                        placeholder: str = "REDACTED") -> str:
    """Deterministic, secret-free fingerprint of a request. Never contains the
    full URL when a secret could be in the query string."""
    return "%s %s" % (method.upper(), redact_url(url, redacted_params, placeholder))


# --------------------------------------------------------------------------- #
# Normalized record constructor (the ONE record contract)
# --------------------------------------------------------------------------- #
_CONTRACT_FIELDS = (
    "record_id", "record_schema_version", "record_type", "source_id",
    "source_native_id", "raw_object_id", "observed_at", "retrieved_at",
    "available_at", "effective_at", "ticker", "security_id", "company_id",
    "exchange", "event_type", "payload_hash", "source_confidence",
    "entity_mapping_confidence", "provenance", "normalized_payload",
)

# Point-in-time discipline: distinct optional timestamp fields carried in the
# normalized payload (never conflated): period_end, event_time, publication_time,
# acceptance_time, availability_time, retrieval_time, effective_date,
# revision_vintage_date.
PIT_PAYLOAD_FIELDS = ("period_end", "event_time", "publication_time",
                      "acceptance_time", "revision_vintage_date")


def build_normalized_record(*, record_type: str, source_id: str,
                            source_native_id: str, raw_object_id: Optional[str],
                            retrieved_at: str, payload: dict,
                            observed_at: Optional[str] = None,
                            available_at: Optional[str] = None,
                            effective_at: Optional[str] = None,
                            ticker: Optional[str] = None,
                            security_id: Optional[str] = None,
                            company_id: Optional[str] = None,
                            exchange: Optional[str] = None,
                            event_type: Optional[str] = None,
                            source_confidence: float = 1.0,
                            entity_mapping_confidence: str = EM_UNMATCHED,
                            provenance: str = "",
                            quality_warnings: Optional[list[str]] = None) -> dict:
    """Build a contract-complete normalized record. Missing timestamps and
    identifiers stay None and are surfaced as explicit quality warnings —
    NEVER fabricated, and period_end is NEVER substituted for publication or
    availability time."""
    if record_type not in RECORD_TYPES:
        raise ValueError("unknown record_type: %r" % (record_type,))
    if entity_mapping_confidence not in ENTITY_MAPPING_STATES:
        raise ValueError("unknown entity_mapping_confidence: %r" % (entity_mapping_confidence,))
    warnings = list(quality_warnings or [])
    if available_at is None:
        warnings.append("AVAILABLE_AT_UNKNOWN: source did not state an availability/"
                        "publication timestamp; left null (not fabricated)")
    if observed_at is None:
        warnings.append("OBSERVED_AT_UNKNOWN: left null (not fabricated)")
    ph = payload_hash_of(payload)
    rec = {
        "record_id": make_record_id(record_type, source_id, source_native_id, ph),
        "record_schema_version": RECORD_SCHEMA_VERSION,
        "record_type": record_type,
        "source_id": source_id,
        "source_native_id": source_native_id,
        "raw_object_id": raw_object_id,
        "observed_at": observed_at,
        "retrieved_at": retrieved_at,
        "available_at": available_at,
        "effective_at": effective_at,
        "ticker": ticker,
        "security_id": security_id,
        "company_id": company_id,
        "exchange": exchange,
        "event_type": event_type,
        "payload_hash": ph,
        "source_confidence": source_confidence,
        "entity_mapping_confidence": entity_mapping_confidence,
        "provenance": provenance,
        "normalized_payload": payload,
        "quality_warnings": warnings,
    }
    return rec


# --------------------------------------------------------------------------- #
# Deterministic identity resolution (NO LLM, NO fuzzy matching)
# --------------------------------------------------------------------------- #
class IdentityResolver:
    """Deterministic ticker/exchange/CIK identity layer.

    Sources of truth are registered explicitly (SEC ticker map, exchange
    directories, vendor security IDs). Conflicting identifiers are NEVER merged
    silently — each conflict is recorded and the mapping downgraded to
    AMBIGUOUS."""

    def __init__(self) -> None:
        self._ticker_to_ciks: dict[str, set[str]] = {}
        self._ticker_to_exchange: dict[str, set[str]] = {}
        self._ticker_to_security_ids: dict[str, set[str]] = {}
        self.conflicts: list[dict] = []

    @staticmethod
    def _norm_ticker(ticker: str) -> str:
        return (ticker or "").strip().upper()

    def register_cik(self, ticker: str, cik: str, origin: str) -> None:
        t = self._norm_ticker(ticker)
        if not t or not cik:
            return
        cik10 = str(cik).strip().lstrip("0") or "0"
        known = self._ticker_to_ciks.setdefault(t, set())
        if known and cik10 not in known:
            self.conflicts.append({
                "conflict_type": "TICKER_CIK_CONFLICT", "ticker": t,
                "existing": sorted(known), "incoming": cik10, "origin": origin})
        known.add(cik10)

    def register_exchange(self, ticker: str, exchange: str, origin: str) -> None:
        t = self._norm_ticker(ticker)
        if not t or not exchange:
            return
        known = self._ticker_to_exchange.setdefault(t, set())
        if known and exchange not in known:
            self.conflicts.append({
                "conflict_type": "TICKER_EXCHANGE_CONFLICT", "ticker": t,
                "existing": sorted(known), "incoming": exchange, "origin": origin})
        known.add(exchange)

    def register_security_id(self, ticker: str, security_id: str, origin: str) -> None:
        t = self._norm_ticker(ticker)
        if not t or not security_id:
            return
        self._ticker_to_security_ids.setdefault(t, set()).add(str(security_id))

    def resolve(self, ticker: str) -> dict:
        """Resolve a ticker to (cik, exchange, security_id, state)."""
        t = self._norm_ticker(ticker)
        ciks = sorted(self._ticker_to_ciks.get(t, set()))
        exchanges = sorted(self._ticker_to_exchange.get(t, set()))
        sec_ids = sorted(self._ticker_to_security_ids.get(t, set()))
        if len(ciks) == 1:
            state = EM_MATCHED_EXACT
            cik: Optional[str] = ciks[0]
        elif len(ciks) > 1:
            state = EM_AMBIGUOUS
            cik = None
        elif exchanges or sec_ids:
            state = EM_MATCHED_ALIAS
            cik = None
        else:
            state = EM_UNMATCHED
            cik = None
        return {
            "ticker": t, "cik": cik, "all_ciks": ciks,
            "exchange": exchanges[0] if len(exchanges) == 1 else None,
            "all_exchanges": exchanges,
            "security_id": sec_ids[0] if len(sec_ids) == 1 else None,
            "all_security_ids": sec_ids,
            "state": state,
        }


# --------------------------------------------------------------------------- #
# Stage 1 information-family -> Stage 2 source gap mapping (static, versioned
# with COLLECTOR_VERSION; classification is computed at run time from what was
# ACTUALLY collected and entitled — never assumed).
# --------------------------------------------------------------------------- #
STAGE1_GAP_SOURCE_MAPPING: list[dict] = [
    {"information_family": "short_activity", "candidate_source": "finra",
     "record_types": [RT_SHORT_VOLUME],
     "note": "Daily consolidated short-sale VOLUME (not short interest); deep biweekly short-interest history remains a separate need."},
    {"information_family": "liquidity", "candidate_source": "norgate_local",
     "record_types": [RT_MARKET_BAR],
     "note": "Volume/turnover from the survivorship-aware local price foundation."},
    {"information_family": "options", "candidate_source": "",
     "record_types": [],
     "note": "No Stage 2 source provides options IV/skew history; still requires a provider purchase."},
    {"information_family": "insider_activity", "candidate_source": "sec_edgar",
     "record_types": [RT_INSIDER_FILING],
     "note": "Form 4 filings from the official daily index; EODHD insider endpoint is a secondary candidate."},
    {"information_family": "news_and_events", "candidate_source": "eodhd",
     "record_types": [RT_NEWS_EVENT],
     "note": "Entitlement-dependent financial news; GDELT discovery adapter specified but deferred."},
    {"information_family": "estimates_and_revisions", "candidate_source": "eodhd",
     "record_types": [RT_EARNINGS_EVENT],
     "note": "True consensus-revision history remains provider-blocked; earnings calendar partially informs."},
    {"information_family": "earnings", "candidate_source": "eodhd",
     "record_types": [RT_EARNINGS_EVENT], "note": "Recent earnings calendar events."},
    {"information_family": "earnings_surprise", "candidate_source": "eodhd",
     "record_types": [RT_EARNINGS_EVENT], "note": "Actual-vs-estimate fields on calendar events."},
    {"information_family": "revenue_surprise", "candidate_source": "eodhd",
     "record_types": [RT_EARNINGS_EVENT], "note": "Revenue fields where entitled."},
    {"information_family": "macro_and_regime", "candidate_source": "fred_alfred",
     "record_types": [RT_MACRO_OBSERVATION],
     "note": "Pre-registered macro series with ALFRED vintage separation."},
    {"information_family": "corporate_actions", "candidate_source": "eodhd",
     "record_types": [RT_CORPORATE_ACTION],
     "note": "Dividends/splits; Norgate capital events are the local complement."},
    {"information_family": "filings_and_textual", "candidate_source": "sec_edgar",
     "record_types": [RT_FILING_EVENT], "note": "Official filing metadata + links (bounded window)."},
    {"information_family": "value", "candidate_source": "eodhd",
     "record_types": [RT_FUNDAMENTAL_FACT], "note": "Entitlement-dependent fundamentals."},
    {"information_family": "growth", "candidate_source": "eodhd",
     "record_types": [RT_FUNDAMENTAL_FACT], "note": "Entitlement-dependent fundamentals."},
    {"information_family": "investment", "candidate_source": "eodhd",
     "record_types": [RT_FUNDAMENTAL_FACT], "note": "Entitlement-dependent fundamentals."},
    {"information_family": "quality", "candidate_source": "eodhd",
     "record_types": [RT_FUNDAMENTAL_FACT], "note": "Entitlement-dependent fundamentals."},
    {"information_family": "profitability", "candidate_source": "eodhd",
     "record_types": [RT_FUNDAMENTAL_FACT], "note": "Entitlement-dependent fundamentals."},
    {"information_family": "price_momentum", "candidate_source": "norgate_local",
     "record_types": [RT_MARKET_BAR], "note": "Survivorship-aware daily bars."},
    {"information_family": "short_term_reversal", "candidate_source": "norgate_local",
     "record_types": [RT_MARKET_BAR], "note": "Survivorship-aware daily bars."},
    {"information_family": "long_term_reversal", "candidate_source": "norgate_local",
     "record_types": [RT_MARKET_BAR], "note": "Survivorship-aware daily bars."},
    {"information_family": "volatility", "candidate_source": "norgate_local",
     "record_types": [RT_MARKET_BAR], "note": "Daily bars support realized-vol construction."},
    {"information_family": "volume", "candidate_source": "finra",
     "record_types": [RT_SHORT_VOLUME, RT_MARKET_BAR], "note": "Total volume by venue + local bars."},
    {"information_family": "market_beta", "candidate_source": "norgate_local",
     "record_types": [RT_MARKET_BAR, RT_UNIVERSE_MEMBERSHIP], "note": "Bars + index membership."},
    {"information_family": "sector_exposure", "candidate_source": "norgate_local",
     "record_types": [RT_SECURITY_IDENTITY], "note": "Classification fields where observed."},
    {"information_family": "alternative_data", "candidate_source": "gdelt",
     "record_types": [RT_NEWS_EVENT], "note": "Discovery feed; adapter deferred."},
]


def classify_gap(*, evidence_classification: str, candidate_source: str,
                 source_health: Optional[str], records_collected: int,
                 entitlement_state: Optional[str]) -> str:
    """Deterministic Stage 1 gap classification from ACTUAL collection results."""
    interesting = {"BLOCKED_BY_MISSING_DATA", "TESTED_INCONCLUSIVE",
                   "NOT_YET_TESTED", "METADATA_INSUFFICIENT"}
    if evidence_classification not in interesting:
        return GAP_NOT_APPLICABLE
    if not candidate_source:
        return GAP_STILL_BLOCKED
    if source_health in BLOCKED_HEALTH_STATES or source_health in (SH_NOT_RUN, SH_FAILED, None):
        return GAP_STILL_BLOCKED
    if entitlement_state in (ENT_NOT_ENTITLED, ENT_AUTH_FAILED):
        return GAP_STILL_BLOCKED
    if records_collected > 0:
        # A bounded recent slice never claims a deep historical gap is closed.
        return GAP_PARTIALLY_ADDRESSED
    return GAP_STILL_BLOCKED
