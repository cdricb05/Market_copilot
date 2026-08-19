"""api/material_information.py - the MATERIAL INFORMATION / CAPITAL IMPACT read model.

Release 30. This module answers the operator question the product could not
previously answer: *what did the system just hear, how did it interpret it, and
what did that change?*

It is a READ MODEL and it owns NOTHING. Every field is read from the owner that
already decided it:

* the event, its family, its quality and its signal AUTHORITY - ``engine.event_fabric``
  through ``api.event_signal_refresh``
* what the event lane actually recalculated                   - ``api.event_signal_refresh``
* the holding-level conclusion                                - ``api.holding_opportunity_cost``
* the portfolio-level conclusion                              - ``api.portfolio_decision``
* the forecast, when one exists                               - ``api.return_forecast``

It classifies nothing, re-decides nothing, and cannot change an authority. If
this module and an owner ever disagree, the owner is right and this module has a
bug - which is exactly why it derives every impact flag from the owners'
published fields rather than from its own reading of the event.

**An EVENT_TRIGGER_ONLY event never becomes expected-return alpha.** The feed
reports ``forecast_affected`` from whether the event's declared authority is
permitted to reach the forecast layer at all - never from whether the forecast
happened to move.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

SCHEMA_VERSION = "material_information.v1"
COMPOSITION_OWNER = "api.material_information"
PHASE = "R30"

STATE_READY = "READY"
STATE_EMPTY = "NO_MATERIAL_INFORMATION"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_VOCAB = (STATE_READY, STATE_EMPTY, STATE_UNAVAILABLE)

#: How many entries the compact Today feed carries. Today is an operating
#: surface, not a log: the deep ledger lives in Research.
DEFAULT_LIMIT = 12

#: Which declared signal authorities may reach which calculation. The three
#: frozensets are READ from the fabric, never copied: a private table here would
#: be a second vocabulary for the same concept, and it would silently disagree
#: with the owner the first time an authority moved. That failure mode has cost
#: this project several releases already.
def authority_reach(authority: Optional[str]) -> dict:
    from paper_trader.engine import event_fabric as ef
    a = str(authority or "")
    return {
        # Only an alpha-bearing authority may move an expected-return forecast.
        "forecast": a in ef.ALPHA_BEARING_AUTHORITIES,
        "risk": a in ef.RISK_BEARING_AUTHORITIES,
        "hoc": a in ef.TRIGGER_BEARING_AUTHORITIES,
    }


def authority_reach_policy() -> dict:
    from paper_trader.engine import event_fabric as ef
    return {a: authority_reach(a) for a in sorted(ef.SIGNAL_AUTHORITIES)}


AUTHORITY_DOC = (
    "An event's decision authority is decided ONCE, by the source family, in "
    "engine.event_fabric. An EVENT_TRIGGER_ONLY event can cause a reassessment "
    "but can never contribute to expected return; only an OPERATIONAL_ALPHA "
    "authority - which today requires a validated point-in-time feature "
    "transformation - can do that.")

SAFETY_BADGES = ["READ ONLY", "PREVIEW ONLY", "NO ORDERS", "AUTOMATION OFF",
                 "MANUAL REVIEW"]

#: Release 30.1. The evidence behind a signal should be one click away, so each
#: row carries the CANONICAL source URL the normalized event already recorded in
#: ``payload_reference``. Two boundaries this must not cross:
#:
#: * the URL is never CONSTRUCTED. It is exposed only when the event itself
#:   carried one, because a link this layer assembled would be a claim about
#:   where the evidence lives rather than a record of it;
#: * opening an article changes NOTHING. The link is a convenience for a human
#:   reading evidence, never an input. Authority still comes from the source
#:   family, decided once in ``engine.event_fabric``, and an external article
#:   cannot become operational alpha by being readable.
#:
#: Whether a reference may be handed to a browser at all is decided by the ONE
#: owner of that question, ``api.external_references.safe_external_url``. A
#: feed-supplied string is untrusted input; a private check here would be a
#: second definition of "safe link" and the first to drift.
SOURCE_LINK_POLICY = "CANONICAL_EVENT_REFERENCE_ONLY_NEVER_CONSTRUCTED"
SOURCE_LINK_DOC = (
    "The source URL is the one the normalized event recorded in "
    "payload_reference. It is never constructed, never guessed and never "
    "inferred from a ticker or a source name. It opens the original evidence in "
    "a new tab and changes no signal, no authority and no interpretation; a "
    "reference that is not an absolute http(s) URL is rendered as plain text.")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


#: Every field a Material Information row carries so an operator can inspect the
#: evidence behind a signal without the browser computing anything. Declared as a
#: contract, asserted by ``tests/test_release30_1_operational_cutover.py``.
TRANSPARENCY_FIELDS = (
    "source", "source_family", "source_title", "source_url", "source_url_state",
    "source_host", "source_reference",
    "timestamp", "ingested_at",
    "ticker", "held",
    "event_type", "event_sub_type", "family",
    "signal_authority", "authority_reach", "event_quality",
    "point_in_time_status",
    "what_changed",
    "forecast_affected", "risk_affected", "hoc_affected",
    "hoc_recommendation", "hoc_deterioration_state",
    "portfolio_reassessed", "result",
)


def _link_policy() -> dict:
    """The external-link contract, read from its owner rather than restated."""
    try:
        from paper_trader.api import external_references as xr
        return dict(xr.LINK_POLICY)
    except Exception:                                          # noqa: BLE001
        return {}


def _url_state_vocabulary() -> list:
    try:
        from paper_trader.api import external_references as xr
        return list(xr.URL_STATE_VOCAB)
    except Exception:                                          # noqa: BLE001
        return []


def _source_link(event: dict) -> dict:
    """The canonical source URL this event recorded, if it recorded one.

    Delegates the safety decision to ``api.external_references`` - the ONE owner
    of what may become an ``href``. Degrades to "no link" rather than raising,
    because a missing link must never take down the capital-impact feed.
    """
    ref = (event or {}).get("payload_reference")
    try:
        from paper_trader.api import external_references as xr
        link = xr.safe_external_url(ref)
        return {"source_url": link["url"], "source_url_state": link["state"],
                "source_host": link["host"],
                "source_reference": None if link["url"] else link["raw"]}
    except Exception:                                          # noqa: BLE001
        return {"source_url": None, "source_url_state": "NO_CANONICAL_SOURCE_URL",
                "source_host": None, "source_reference": None}


def _source_title(event: dict) -> Optional[str]:
    """The headline the SOURCE published, verbatim, if it published one.

    Kept separate from ``what_changed`` so the UI can make the title itself the
    clickable thing without re-parsing a composed sentence.
    """
    inputs = (event or {}).get("materiality_inputs") or {}
    title = inputs.get("title") or inputs.get("headline")
    t = str(title).strip() if title else ""
    return t or None


def _hoc_by_ticker(hoc: Optional[dict]) -> dict:
    out = {}
    for r in ((hoc or {}).get("holding_reviews") or []):
        tk = r.get("ticker")
        if tk:
            out[tk] = r
    return out


def build(*, event_refresh: Optional[dict] = None,
          hoc: Optional[dict] = None,
          decision: Optional[dict] = None,
          forecast_summary: Optional[dict] = None,
          limit: int = DEFAULT_LIMIT) -> dict:
    """Assemble the compact capital-impact feed. PURE READ."""
    ev = event_refresh or {}
    events = list(ev.get("material_events") or [])
    # The event owner publishes holdings as bare tickers; older payloads used
    # position objects. Accept both rather than assuming one - a read model that
    # crashes on its owner's shape is a read model that hides the owner.
    holdings = set()
    for h in (ev.get("holdings") or []):
        tk = h.get("ticker") if isinstance(h, dict) else h
        if tk:
            holdings.add(str(tk))
    affected = {str(t) for t in (ev.get("affected_holdings") or [])}
    hoc_rows = _hoc_by_ticker(hoc)
    dec = decision or {}
    last_run = ev.get("last_run") or {}
    run_state = last_run.get("state")
    # "Was the portfolio reassessed?" is the EVENT LANE's answer, not ours: a
    # completed event cycle is the only thing that can have reassessed anything.
    reassessed = bool(run_state and str(run_state).startswith("REASSESSED"))

    rows = []
    for e in events:
        tk = e.get("primary_ticker") or (e.get("entities") or [None])[0]
        authority = e.get("decision_authority")
        reach = authority_reach(authority)
        held = bool(tk and tk in holdings)
        review = hoc_rows.get(tk) or {}
        link = _source_link(e)
        rows.append({
            "event_id": e.get("event_id"),
            "timestamp": (e.get("published_at") or e.get("source_timestamp")
                          or e.get("ingested_at")),
            "ingested_at": e.get("ingested_at"),
            "ticker": tk,
            "held": held,
            "source": e.get("source_id") or e.get("collector_id"),
            "source_family": e.get("source_family"),
            "source_title": _source_title(e),
            "source_url": link["source_url"],
            "source_url_state": link["source_url_state"],
            "source_host": link["source_host"],
            "source_reference": link["source_reference"],
            "event_type": e.get("event_type"),
            "event_sub_type": e.get("event_sub_type"),
            "family": e.get("family"),
            "signal_authority": authority,
            "event_quality": e.get("event_quality"),
            "point_in_time_status": e.get("point_in_time_status"),
            "what_changed": _what_changed(e),
            "forecast_affected": bool(reach["forecast"]),
            "risk_affected": bool(reach["risk"] and held),
            "hoc_affected": bool(reach["hoc"] and held),
            "hoc_recommendation": review.get("recommendation"),
            "hoc_deterioration_state": review.get("deterioration_state"),
            "portfolio_reassessed": reassessed,
            "result": dec.get("portfolio_decision_state"),
            "authority_reach": reach,
        })
    rows.sort(key=lambda r: (str(r.get("timestamp") or ""),
                             str(r.get("event_id") or "")), reverse=True)
    shown = rows[:max(0, int(limit))]

    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "state": STATE_READY if shown else STATE_EMPTY,
        "state_vocabulary": list(STATE_VOCAB),
        "eligible_market_date": ev.get("eligible_market_date"),
        "cycle_id": ev.get("cycle_id"),
        "last_event_cycle": {"run_id": last_run.get("run_id"),
                             "state": run_state,
                             "generated_at": last_run.get("generated_at")},
        "portfolio_reassessed": reassessed,
        "portfolio_decision": {
            "state": dec.get("portfolio_decision_state"),
            "headline": dec.get("label"),
            "provenance": dec.get("decision_provenance"),
            "approvable": bool(dec.get("approvable")),
            "requires_manual_review": bool(dec.get("requires_manual_review")),
        },
        "forecast": forecast_summary or {},
        "rows": shown,
        "row_count": len(shown),
        "total_material_events": len(rows),
        "material_events_affecting_holdings": sum(1 for r in rows if r["held"]),
        "affected_holdings": sorted(affected),
        "authority_doc": AUTHORITY_DOC,
        "authority_reach_policy": authority_reach_policy(),
        "authority_policy_owner": "engine.event_fabric",
        # Release 30.1: the row contract, declared. An operator inspecting the
        # evidence behind a signal should be able to see every one of these
        # WITHOUT the browser deriving any of them, and a reader of the API
        # should be able to check that claim without reading the markup.
        "transparency_fields": list(TRANSPARENCY_FIELDS),
        "source_link_policy": SOURCE_LINK_POLICY,
        "source_link_doc": SOURCE_LINK_DOC,
        "link_policy": _link_policy(),
        "source_url_state_vocabulary": _url_state_vocabulary(),
        "external_article_is_not_alpha": True,
        "owners": {
            "events": "engine.event_fabric via api.event_signal_refresh",
            "authority": "engine.event_fabric",
            "holding_conclusion": "api.holding_opportunity_cost",
            "portfolio_conclusion": "api.portfolio_decision",
            "forecast": "api.return_forecast",
        },
        "owns_no_calculation": True,
        "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                   "creates_decisions": False, "mutates_holdings": False},
    }


def _what_changed(event: dict) -> str:
    """A one-line, factual description built from the event's OWN fields.

    No interpretation is added here - if the fields do not say what changed, the
    feed says the event type and stops, rather than inventing a narrative.
    """
    et = str(event.get("event_type") or "EVENT").replace("_", " ").title()
    inputs = event.get("materiality_inputs") or {}
    pct = _f(inputs.get("change_pct") if "change_pct" in inputs
             else inputs.get("pct_change"))
    if pct is not None:
        return "%s %+.2f%%" % (et, pct * 100.0 if abs(pct) <= 1.5 else pct)
    val = _f(inputs.get("value"))
    if val is not None:
        period = inputs.get("period")
        return "%s %s%s" % (et, val, (" (%s)" % period) if period else "")
    # A headline the SOURCE published is a fact about the event, so it is shown
    # verbatim and truncated. "News" alone tells the operator nothing, and
    # inventing a summary would be worse than saying nothing.
    title = inputs.get("title") or inputs.get("headline")
    if title:
        t = str(title).strip()
        return "%s: %s" % (et, t if len(t) <= 70 else t[:69] + "…")
    return et


def load_material_information(*, limit: int = DEFAULT_LIMIT) -> dict:
    """Read surface. Degrades to UNAVAILABLE rather than raising."""
    try:
        from paper_trader.api import event_signal_refresh as esr
        from paper_trader.api import holding_opportunity_cost as hoc_owner
        from paper_trader.api import portfolio_decision as pd_owner
        from paper_trader.api import return_forecast as rfc
        ev = esr.load_event_signal_refresh_status()
        hoc = hoc_owner.load_holding_opportunity_cost()
        dec = pd_owner.load_portfolio_decision()
        fsum = rfc.summary()
        return build(event_refresh=ev, hoc=hoc, decision=dec,
                     forecast_summary=fsum, limit=limit)
    except Exception as exc:                                   # noqa: BLE001
        return {
            "schema_version": SCHEMA_VERSION,
            "composition_owner": COMPOSITION_OWNER, "phase": PHASE,
            "generated_at": _now_iso(), "state": STATE_UNAVAILABLE,
            "state_vocabulary": list(STATE_VOCAB), "rows": [], "row_count": 0,
            "blockers": [{"code": "MATERIAL_INFORMATION_UNAVAILABLE",
                          "detail": type(exc).__name__}],
            "owns_no_calculation": True,
            "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                       "creates_decisions": False, "mutates_holdings": False},
        }
