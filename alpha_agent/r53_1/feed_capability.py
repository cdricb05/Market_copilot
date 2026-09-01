r"""alpha_agent.r53_1.feed_capability - the intraday data capability record.

Reads the ONE lane owner's live verdict (:mod:`alpha_agent.r46.intraday` -
this module never probes a venue itself) plus one canonical feed snapshot,
and writes the two Release 53.1 Track-B artifacts:

* ``R53_1_INTRADAY_DATA_CAPABILITY.json`` - every owned source, its
  entitlement as MEASURED, timestamp semantics, delay, latency class,
  horizons it can support, and its legality for prospective evidence;
* ``R53_1_INTRADAY_FEED_GATE.json`` - the purchase-gate verdict. Owned
  sources ARE usable, so the gate's state is
  ``NOT_REQUIRED_OWNED_SOURCE_USABLE`` - with an honest statement of what a
  paid feed would still add, so a future purchase case starts from measured
  ground instead of a blank page. NO purchase is proposed.

THE SCIENTIFIC RULE, restated where it is enforced: a DELAYED feed still
produces TRUE_FORWARD evidence when only information actually observable at
emission is used, the prediction is timestamped after that observation, and
the outcome starts strictly after emission. Delay costs signal freshness,
never evidence validity - but it must be labelled, and the factory's
20-minute staleness ceiling refuses what it cannot honestly stamp.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, research_dir,
               safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53_1.feed_capability"
ARTIFACT_CAPABILITY = "R53_1_INTRADAY_DATA_CAPABILITY.json"
ARTIFACT_GATE = "R53_1_INTRADAY_FEED_GATE.json"

#: What each source can and cannot legally support, given its MEASURED shape.
#: (Bar-capable sources can both stamp signals and mark outcomes; quote-only
#: sources can corroborate freshness but cannot mark a 30-minute window's
#: entry and exit without a bar series.)
SOURCE_ROLES = {
    "yahoo_chart_bars": {
        "role": "PRIMARY - signal stamping AND outcome marking",
        "bars": True, "exact_timestamps": True,
        "horizons_supported": ["30m", "120m", "session_close"],
        "instruments": "US equities/ETFs/indices incl. ^VIX; single names "
                       "available (same venue), minute history ~30 days",
        "rate_limit_note": "free public capability; the adapter batches one "
                           "download per snapshot (13 instruments, 1 call)",
        "prospective_legality": "bar END timestamps are exchange-stamped; "
                                "emission uses COMPLETED bars only",
    },
    "tiingo_iex": {
        "role": "CORROBORATION - real-time last trade (IEX venue only)",
        "bars": False, "exact_timestamps": True,
        "horizons_supported": [],
        "instruments": "US listed (IEX top-of-book)",
        "rate_limit_note": "owned free key; hourly/daily caps apply",
        "prospective_legality": "legal but quote-only: cannot mark a window",
    },
    "finnhub_quote": {
        "role": "CORROBORATION - near-real-time quote",
        "bars": False, "exact_timestamps": True,
        "horizons_supported": [],
        "instruments": "US listed",
        "rate_limit_note": "owned free key; 60 calls/min",
        "prospective_legality": "legal but quote-only",
    },
    "eodhd_delayed_quote": {
        "role": "CORROBORATION - subscriber-entitled delayed OHLC quote",
        "bars": False, "exact_timestamps": True,
        "horizons_supported": [],
        "instruments": "US + global listed (subscription)",
        "rate_limit_note": "existing subscription; bounded request budget",
        "prospective_legality": "legal; ~15-16 min delay approaches the "
                                "factory's 20-minute staleness ceiling",
    },
}


def _lane(live: bool = True) -> dict:
    from ..r46 import intraday as il
    return il.probe(live_probe=live)


def build_capability(lane: Optional[dict] = None,
                     snapshot: Optional[dict] = None) -> dict:
    from .intraday_feed import build_snapshot
    lane = lane or _lane()
    snapshot = snapshot or build_snapshot()
    rows = []
    for s in (lane.get("sources") or []):
        row = dict(s)
        role = SOURCE_ROLES.get(s.get("source"))
        if role:
            row.update(role)
        rows.append(row)
    return {
        "lane_state": lane.get("state"),
        "probed_at_utc": lane.get("probed_at_utc"),
        "sources": rows,
        "feed_snapshot_measured": {
            "provider": snapshot.get("provider"),
            "instruments_served": sorted((snapshot.get("bars") or {}).keys()),
            "freshness_seconds": snapshot.get("freshness_seconds"),
            "failures": snapshot.get("failures"),
        },
        "delayed_feed_rule": (
            "a delayed feed still produces TRUE_FORWARD evidence when only "
            "already-observable information is used, the prediction is "
            "stamped after the observation, and the outcome starts strictly "
            "after emission; delay is labelled, never hidden"),
    }


def build_gate(capability: dict) -> dict:
    usable = capability["lane_state"] == "AVAILABLE_NOW"
    return {
        "state": ("NOT_REQUIRED_OWNED_SOURCE_USABLE" if usable
                  else "PURCHASE_GATE_OPEN"),
        "owned_sources_exhausted": True,
        "sources_evaluated": [s.get("source")
                              for s in capability["sources"]],
        "what_owned_capability_provides": (
            "5-minute exchange-stamped OHLCV bars for the current session "
            "on the liquid ETF/index layer (+^VIX), ~30 sessions of minute "
            "history, three independent quote corroborators - enough for "
            "every frozen R53 spec at every declared horizon"),
        "what_a_paid_feed_would_still_add": [
            "sub-minute bars / tick data (the frozen specs need none)",
            "deep minute HISTORY (>60 days) for single names - relevant "
            "only when a single-name intraday challenger family is "
            "nominated as a NEW version",
            "contractual real-time entitlement with an SLA (the free venue "
            "could throttle or change without notice - an OPERATING risk, "
            "not an evidence-validity risk)",
            "futures/FX/crypto intraday (Norgate is daily; the owned venue "
            "keys are equity-scoped)"],
        "purchase_proposed": False,
        "human_authorization_required_for_any_purchase": True,
    }


def write_artifacts() -> dict:
    cap = build_capability()
    gate = build_gate(cap)
    body_cap = artifact_body(
        "r53_1_intraday_data_capability/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        **cap, **safety_block())
    write_json(research_dir() / ARTIFACT_CAPABILITY, body_cap)
    body_gate = artifact_body(
        "r53_1_intraday_feed_gate/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        **gate, **safety_block())
    write_json(research_dir() / ARTIFACT_GATE, body_gate)
    return {"capability": body_cap, "gate": body_gate}
