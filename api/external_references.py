"""api/external_references.py - the EXTERNAL REFERENCE read model.

Release 30.1. Two jobs, both about links to material the system did NOT produce:

1.  It owns **what makes an external URL safe to expose as a link**
    (``safe_external_url``). One definition, used by every read model that hands
    a URL to a browser, so an adversarial or malformed reference arriving from a
    third-party feed cannot become an ``href`` anywhere in the product.

2.  It owns the small, declared set of **external market reference sites** the
    operator may open from the Markets page, and - crucially - it answers from
    the CANONICAL registries whether any of them is actually INGESTED and whether
    it carries any signal authority. It never asserts that itself.

Why the second job is a backend question at all
-----------------------------------------------
"Is this site influencing the portfolio?" has exactly one honest answer, and it
lives in ``api.source_capability`` (is it a registered, ingested source?) and
``engine.event_fabric`` (what authority does its family carry?). A hard-coded
"reference only" caption in the UI would be the FRONTEND asserting a backend
fact, and it would keep asserting it on the day someone wires one of these sites
into the collection lane. So the claim is derived here, every read, from the
owners that decide it.

This module owns no calculation, creates no signal, event, target, proposal,
decision or order, performs no network call, and writes nothing. A link is a
convenience for a human reading evidence; it is never an input.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlsplit

SCHEMA_VERSION = "external_references.v1"
COMPOSITION_OWNER = "api.external_references"
PHASE = "R30.1"

STATE_READY = "READY"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_VOCAB = (STATE_READY, STATE_UNAVAILABLE)

# --------------------------------------------------------------------------- #
# Safe external URL - ONE definition
# --------------------------------------------------------------------------- #
#: The only schemes that may ever reach an ``href``. A feed-supplied reference is
#: untrusted input: ``javascript:``, ``data:`` and ``vbscript:`` are executable in
#: a browser, and a relative or scheme-less string would resolve against our own
#: origin and impersonate an internal page. Anything outside this set is not
#: "sanitised" into something else - it is refused, and the caller renders text.
ALLOWED_URL_SCHEMES = ("http", "https")

#: Why a reference is not exposed as a link. Named, so the UI can stay silent
#: intelligently rather than guessing.
URL_OK = "CANONICAL_SOURCE_URL"
URL_ABSENT = "NO_CANONICAL_SOURCE_URL"
URL_NOT_A_URL = "REFERENCE_IS_NOT_A_URL"
URL_SCHEME_REFUSED = "URL_SCHEME_NOT_ALLOWED"
URL_MALFORMED = "URL_MALFORMED"
URL_STATE_VOCAB = (URL_OK, URL_ABSENT, URL_NOT_A_URL, URL_SCHEME_REFUSED,
                   URL_MALFORMED)

#: The maximum length of a URL this layer will hand to a browser. A reference
#: longer than this is far more likely to be a corrupted payload than a link an
#: operator wants to open.
MAX_URL_LENGTH = 2048


def safe_external_url(value: Any) -> dict:
    """Decide whether ``value`` may be exposed as a clickable external link.

    Pure, total and the ONE owner of this decision. Returns the URL only when it
    is an absolute ``http``/``https`` URL with a host; otherwise ``url`` is None
    and ``state`` names the reason, so the caller renders plain text rather than
    a link that goes nowhere or, worse, somewhere it should not.
    """
    raw = "" if value is None else str(value).strip()
    if not raw:
        return {"url": None, "state": URL_ABSENT, "host": None, "raw": None}
    if len(raw) > MAX_URL_LENGTH:
        return {"url": None, "state": URL_MALFORMED, "host": None, "raw": raw[:120]}
    # A control character inside a URL is how a scheme check gets bypassed
    # ("java\tscript:"), so the string is refused before it is parsed.
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in raw):
        return {"url": None, "state": URL_MALFORMED, "host": None, "raw": raw[:120]}
    try:
        parts = urlsplit(raw)
    except ValueError:
        return {"url": None, "state": URL_MALFORMED, "host": None, "raw": raw[:120]}
    scheme = (parts.scheme or "").lower()
    if not scheme:
        # A scheme-less reference is an identifier, not a link. Resolving it
        # against our own origin would present a third party's id as an
        # internal page.
        return {"url": None, "state": URL_NOT_A_URL, "host": None, "raw": raw[:120]}
    if scheme not in ALLOWED_URL_SCHEMES:
        return {"url": None, "state": URL_SCHEME_REFUSED, "host": None,
                "raw": raw[:120]}
    if not parts.netloc:
        return {"url": None, "state": URL_MALFORMED, "host": None, "raw": raw[:120]}
    return {"url": raw, "state": URL_OK, "host": parts.hostname, "raw": raw}


#: Every external link this product opens carries these attributes. Declared once
#: here and echoed in the payload so the requirement is verifiable from the API
#: rather than only by reading markup: ``noopener`` denies the opened page a
#: handle on our window, and ``noreferrer`` stops our URL leaking to it.
LINK_TARGET = "_blank"
LINK_REL = "noopener noreferrer"
LINK_POLICY = {
    "target": LINK_TARGET,
    "rel": LINK_REL,
    "allowed_schemes": list(ALLOWED_URL_SCHEMES),
    "doc": ("External references open in a NEW browser tab. noopener denies the "
            "opened page a handle on this window; noreferrer stops this URL "
            "leaking to it. A reference that is not an absolute http(s) URL is "
            "rendered as plain text - never as a link that cannot be trusted."),
}


# --------------------------------------------------------------------------- #
# The declared external market reference sites
# --------------------------------------------------------------------------- #
#: Sites an operator may open from Markets for their own reading. Each carries
#: the ``source_id`` it WOULD have if it were ever ingested, so the ingestion
#: question below is asked of the canonical registry by name rather than by
#: guesswork. None of them is ingested today, and this module does not decide
#: that - ``api.source_capability`` does, on every read.
MARKET_REFERENCE_SITES = (
    {"reference_id": "financialjuice",
     "label": "FinancialJuice",
     "url": "https://www.financialjuice.com/home",
     "candidate_source_id": "financialjuice",
     "describes": "Real-time financial newswire headlines."},
    {"reference_id": "trading_economics_indicators",
     "label": "Trading Economics - Indicators",
     "url": "https://tradingeconomics.com/indicators",
     "candidate_source_id": "trading_economics",
     "describes": "Cross-country macroeconomic indicator tables."},
    {"reference_id": "investing_economic_calendar",
     "label": "Investing.com - Economic Calendar",
     "url": "https://www.investing.com/economic-calendar",
     "candidate_source_id": "investing_com",
     "describes": "Scheduled macroeconomic releases and consensus."},
)

#: The states a reference site can be in.
REF_REFERENCE_ONLY = "REFERENCE_ONLY"
REF_INGESTED = "INGESTED_SOURCE"
REFERENCE_STATE_VOCAB = (REF_REFERENCE_ONLY, REF_INGESTED)

REFERENCE_DOC = (
    "These sites are OPERATOR READING. Nothing on them is collected, normalised "
    "into an event, given a signal authority, or allowed to reach a forecast, a "
    "target, a proposal or a portfolio decision. A link existing here is not "
    "ingestion: whether a site is actually a source is decided by "
    "api.source_capability and engine.event_fabric, and this feed reports their "
    "answer rather than asserting its own.")

SAFETY_BADGES = ["READ ONLY", "REFERENCE ONLY", "NOT A SIGNAL", "NOT INGESTED",
                 "NO ORDERS", "AUTOMATION OFF"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ingestion_state(candidate_source_id: str) -> dict:
    """Ask the CANONICAL registries whether this site is a live source.

    Degrades to "not registered" rather than raising: an unavailable registry
    must never be reported as ingestion.
    """
    registered = False
    ingested = False
    families: tuple = ()
    authorities: list = []
    try:
        from paper_trader.api import source_capability as sc
        sid = sc.canonical_source_id(candidate_source_id)
        entry = sc.SOURCE_BY_ID.get(sid)
        registered = entry is not None
        ingested = sid in sc.INGESTED_SOURCE_IDS
        families = tuple((entry or {}).get("event_families") or ())
    except Exception:                                          # noqa: BLE001
        pass
    if ingested and families:
        # Authority belongs to the event FAMILY, not to the site. Reading it from
        # the fabric's own table is the only way this feed can report an
        # authority without becoming a second place that assigns one.
        try:
            from paper_trader.engine import event_fabric as ef
            for fam in families:
                blk = ef.EVENT_FAMILIES.get(fam)
                if blk and blk.get("decision_authority"):
                    authorities.append(str(blk["decision_authority"]))
        except Exception:                                      # noqa: BLE001
            authorities = []
    return {
        "registered_source": bool(registered),
        "ingested": bool(ingested),
        "event_families": sorted(families),
        "signal_authorities": sorted(set(authorities)),
        "state": REF_INGESTED if ingested else REF_REFERENCE_ONLY,
        "owner": "api.source_capability + engine.event_fabric",
    }


def build_market_references() -> dict:
    """The compact external market reference feed. PURE READ, no network call."""
    rows = []
    for site in MARKET_REFERENCE_SITES:
        link = safe_external_url(site["url"])
        state = _ingestion_state(site["candidate_source_id"])
        rows.append({
            "reference_id": site["reference_id"],
            "label": site["label"],
            "describes": site["describes"],
            "url": link["url"],
            "url_state": link["state"],
            "host": link["host"],
            "opens_in_new_tab": True,
            "link_target": LINK_TARGET,
            "link_rel": LINK_REL,
            "reference_state": state["state"],
            "registered_source": state["registered_source"],
            "ingested": state["ingested"],
            "event_families": state["event_families"],
            "signal_authorities": state["signal_authorities"],
            "influences_portfolio_decisions": False,
        })
    ingested_any = any(r["ingested"] for r in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "state": STATE_READY,
        "state_vocabulary": list(STATE_VOCAB),
        "surface": "MARKETS",
        "surface_policy": ("Markets only. These references are deliberately absent "
                           "from Today, which is the operating surface and carries "
                           "only what the system itself concluded."),
        "rows": rows,
        "row_count": len(rows),
        "any_ingested": ingested_any,
        "reference_state_vocabulary": list(REFERENCE_STATE_VOCAB),
        "link_policy": dict(LINK_POLICY),
        "reference_doc": REFERENCE_DOC,
        "ingestion_owner": "api.source_capability",
        "authority_owner": "engine.event_fabric",
        "owns_no_calculation": True,
        "creates_no_event": True,
        "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                   "creates_decisions": False, "mutates_holdings": False,
                   "creates_signals": False},
    }


def load_external_market_references() -> dict:
    """Read surface. Degrades to UNAVAILABLE rather than raising."""
    try:
        return build_market_references()
    except Exception as exc:                                   # noqa: BLE001
        return {
            "schema_version": SCHEMA_VERSION,
            "composition_owner": COMPOSITION_OWNER, "phase": PHASE,
            "generated_at": _now_iso(), "state": STATE_UNAVAILABLE,
            "state_vocabulary": list(STATE_VOCAB), "rows": [], "row_count": 0,
            "blockers": [{"code": "EXTERNAL_REFERENCES_UNAVAILABLE",
                          "detail": type(exc).__name__}],
            "owns_no_calculation": True, "creates_no_event": True,
            "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                       "creates_decisions": False, "mutates_holdings": False,
                       "creates_signals": False},
        }


__all__ = [
    "SCHEMA_VERSION", "COMPOSITION_OWNER", "PHASE",
    "ALLOWED_URL_SCHEMES", "MAX_URL_LENGTH", "safe_external_url",
    "URL_OK", "URL_ABSENT", "URL_NOT_A_URL", "URL_SCHEME_REFUSED",
    "URL_MALFORMED", "URL_STATE_VOCAB",
    "LINK_TARGET", "LINK_REL", "LINK_POLICY",
    "MARKET_REFERENCE_SITES", "REF_REFERENCE_ONLY", "REF_INGESTED",
    "REFERENCE_STATE_VOCAB", "REFERENCE_DOC",
    "build_market_references", "load_external_market_references",
]
