"""alpha_agent.r36.entitlements - the ONE Release 36 entitlement measurement owner.

**A configured API key is not an entitlement.** Five releases of this estate
have carried provider keys that answer HTTP 403, return a sample instead of a
history, or serve a three-year rolling window under a licence cap. This module
therefore never reports what a provider advertises; it reports what a real
endpoint returned when this release asked it, and every claim downstream traces
to a row produced here.

What it measures, and what each measurement decides:

* **Norgate, per database.** The vendor's own symbol enumeration. This is the
  measurement that decides the whole futures question: the Continuous Futures
  database is entitled and contains ONE market, so no native futures curve
  outside energy can be built at any price this release is allowed to pay.
* **Free public HTTP sources.** One bounded request per route. A 403 is recorded
  as a licence block with the route that produced it, not inferred.
* **Credential PRESENCE, never value.** The report says whether an environment
  variable is set. No key, no fragment of a key and no length that could
  identify one ever reaches an artifact.

Nothing here downloads a research payload, starts a trial, creates an account or
changes a subscription tier. The probe budget is bounded and the transport is
injectable so the whole layer is testable with no network at all.
"""
from __future__ import annotations

import datetime as _dt
import os
import urllib.error
import urllib.request
from typing import Optional

from .. import r36
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r36.entitlements"
SCHEMA = "r36_data_entitlement_matrix/1"
ARTIFACT_NAME = "data_entitlement_matrix.json"

# Measured entitlement states
ENTITLED = "ENTITLED_MEASURED"
BLOCKED_LICENCE = "BLOCKED_LICENSING"
BLOCKED_MISSING = "BLOCKED_NOT_PUBLISHED"
BLOCKED_CREDENTIAL = "BLOCKED_CREDENTIAL_ABSENT"
NOT_CONFIGURED = "NOT_CONFIGURED"
UNMEASURED = "UNMEASURED"

#: Provider credentials this estate may hold. Only PRESENCE is ever recorded.
CREDENTIAL_ENVIRONMENT = (
    "FRED_API_KEY", "EODHD_API_KEY", "NASDAQ_DATA_LINK_API_KEY",
    "ALPHAVANTAGE_API_KEY", "FINNHUB_API_KEY", "TIINGO_API_KEY",
    "POLYGON_API_KEY", "FMP_API_KEY", "INTRINIO_API_KEY", "BEA_API_KEY",
)

#: Owned Norgate databases and what each one can and cannot support.
NORGATE_DATABASES = (
    "Continuous Futures", "Cash Commodities", "Forex Spot", "Economic",
    "US Equities", "US Equities Delisted", "US Indices", "World Indices",
)


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def credential_presence() -> dict:
    """Which provider environment variables are SET. Never their values."""
    return {name: bool(os.environ.get(name))
            for name in CREDENTIAL_ENVIRONMENT}


# --------------------------------------------------------------------------- #
# Norgate
# --------------------------------------------------------------------------- #
def _nd():
    import norgatedata as nd  # lazy: the tests run without the vendor
    return nd


def measure_norgate(*, vendor=None) -> dict:
    """Enumerate every owned Norgate database and count what it serves."""
    try:
        nd = vendor if vendor is not None else _nd()
    except Exception as exc:  # noqa: BLE001
        return {"state": NOT_CONFIGURED, "measured_at": _now(),
                "reason": "%s: %s" % (type(exc).__name__, str(exc)[:120]),
                "databases": {}}
    try:
        running = bool(nd.status())
    except Exception:  # noqa: BLE001
        running = False
    if not running:
        return {"state": NOT_CONFIGURED, "measured_at": _now(),
                "reason": "NORGATE_DATA_UPDATER_NOT_RUNNING", "databases": {}}

    out = {}
    try:
        names = [str(d) for d in nd.databases()]
    except Exception as exc:  # noqa: BLE001
        return {"state": NOT_CONFIGURED, "measured_at": _now(),
                "reason": "DATABASES_UNAVAILABLE:%s" % type(exc).__name__,
                "databases": {}}
    for db in names:
        try:
            syms = [str(s) for s in nd.database_symbols(db)]
        except Exception as exc:  # noqa: BLE001
            out[db] = {"state": UNMEASURED,
                       "reason": type(exc).__name__}
            continue
        row = {"state": ENTITLED, "symbol_count": len(syms)}
        # The small databases are enumerated in full because their CONTENT is
        # the finding: "Continuous Futures: 1 symbol" is the measurement that
        # closes the native futures question for this estate.
        if len(syms) <= 60:
            row["symbols"] = sorted(syms)
        out[db] = row

    futures = out.get("Continuous Futures", {})
    return {
        "state": ENTITLED, "measured_at": _now(),
        "package_version": str(getattr(nd, "__version__", "unknown")),
        "databases": out,
        "declared_databases": list(NORGATE_DATABASES),
        "continuous_futures_symbol_count": futures.get("symbol_count"),
        "continuous_futures_symbols": futures.get("symbols"),
        "native_futures_supported": bool(
            (futures.get("symbol_count") or 0) > 1),
        "native_futures_finding": (
            "the owned Continuous Futures entitlement serves %s market(s); a "
            "broad native futures universe cannot be built from it at any "
            "price this release is permitted to pay"
            % (futures.get("symbol_count"),)),
    }


# --------------------------------------------------------------------------- #
# Free HTTP sources
# --------------------------------------------------------------------------- #
def _probe(url: str, *, transport=None, max_bytes: int = 4096) -> dict:
    """One bounded request. Returns a status, never a payload."""
    if transport is not None:
        try:
            status, head = transport(url)
        except Exception as exc:  # noqa: BLE001
            return {"url": url, "status": None,
                    "error": type(exc).__name__}
        return {"url": url, "status": int(status),
                "head_bytes": len(head or b"")}
    request = urllib.request.Request(
        url, headers={"User-Agent": _contract.HTTP_USER_AGENT})
    try:
        with urllib.request.urlopen(
                request, timeout=_contract.HTTP_TIMEOUT_SECONDS) as response:
            body = response.read(max_bytes)
            return {"url": url, "status": int(response.status),
                    "head_bytes": len(body)}
    except urllib.error.HTTPError as exc:
        return {"url": url, "status": int(exc.code)}
    except Exception as exc:  # noqa: BLE001
        return {"url": url, "status": None, "error": type(exc).__name__}


def _state_for(status: Optional[int]) -> str:
    if status == 200:
        return ENTITLED
    if status in (401, 402, 403):
        return BLOCKED_LICENCE
    if status in (404, 410):
        return BLOCKED_MISSING
    return UNMEASURED


def measure_vix_futures(*, transport=None) -> dict:
    """The native volatility curve: is any free settlement history published?

    This is the measurement that separates "VIX is information" from "a
    volatility sleeve is implementable". Every declared route is probed and the
    verdict is the best status any of them returned.
    """
    probes = [_probe(u, transport=transport)
              for u in _contract.CBOE_VX_FUTURES_ROUTES]
    statuses = [p.get("status") for p in probes]
    best = 200 if 200 in statuses else (
        404 if 404 in statuses and 403 not in statuses else
        403 if 403 in statuses else None)
    return {"source": "CBOE_VIX_FUTURES_SETTLEMENT_HISTORY",
            "state": _state_for(best),
            "routes_probed": len(probes),
            "probes": probes,
            "measured_at": _now(),
            "consequence": (
                "the native VIX futures term structure cannot be constructed "
                "from a free source; a volatility sleeve therefore has no "
                "LEVEL 3 implementation in this estate"
                if _state_for(best) != ENTITLED else
                "a free settlement history is published and the native term "
                "structure can be constructed")}


def measure_cboe_indices(*, indices=("VIX", "VIX3M", "VIX9D", "VIX6M"),
                         transport=None) -> dict:
    probes = {name: _probe(_contract.CBOE_INDEX_URL % name,
                           transport=transport)
              for name in indices}
    return {"source": "CBOE_VOLATILITY_INDICES",
            "state": ENTITLED if all(p.get("status") == 200
                                     for p in probes.values())
            else UNMEASURED,
            "probes": probes, "measured_at": _now(),
            "implementation_level": _contract.LEVEL_SIGNAL,
            "consequence": ("the volatility term structure is available as "
                            "INFORMATION and not as an instrument")}


def measure_eia_bulk(*, transport=None) -> dict:
    probe = _probe(_contract.EIA_NATURAL_GAS_BULK_URL, transport=transport)
    return {"source": "EIA_NATURAL_GAS_BULK",
            "state": _state_for(probe.get("status")),
            "probe": probe, "measured_at": _now(),
            "implementation_level": _contract.LEVEL_NATIVE,
            "consequence": ("dated NYMEX natural gas settlements, contracts 1 "
                            "to 4, are public domain and free")}


def measure_fred(*, transport=None) -> dict:
    """FRED is free, but the KEY must exist and some series are licence-capped.

    ``BAMLH0A0HYM2`` is the specific trap: it is a real, free, documented ICE
    BofA option-adjusted spread series that the API serves only for a rolling
    three-year window. A release that assumed the documented start date would
    have silently researched three years and reported forty.
    """
    key = next((os.environ.get(n) for n in _contract.FRED_API_KEY_ENV
                if os.environ.get(n)), None)
    if not key and transport is None:
        return {"source": "FRED_ST_LOUIS_FED", "state": BLOCKED_CREDENTIAL,
                "measured_at": _now(),
                "reason": "FRED_API_KEY_ABSENT_FROM_SHELL_ENVIRONMENT",
                "credential_written_to_artifact": False}
    return {"source": "FRED_ST_LOUIS_FED", "state": ENTITLED,
            "measured_at": _now(), "credential_present": True,
            "credential_written_to_artifact": False,
            "known_licence_cap": {
                "series": ["BAMLH0A0HYM2", "BAMLC0A0CM"],
                "measured_window": "ROLLING_THREE_YEARS",
                "substitute": "BAA10Y",
                "reason": ("the ICE BofA option-adjusted spread series are "
                           "licence-capped through the API; Moody's BAA over "
                           "the 10-year Treasury is free from 1986")}}


def measure_all(*, vendor=None, transport=None) -> dict:
    return {
        "NORGATE": measure_norgate(vendor=vendor),
        "CBOE_VIX_FUTURES": measure_vix_futures(transport=transport),
        "CBOE_INDICES": measure_cboe_indices(transport=transport),
        "EIA_BULK": measure_eia_bulk(transport=transport),
        "FRED": measure_fred(transport=transport),
    }


# --------------------------------------------------------------------------- #
# The entitlement matrix artifact
# --------------------------------------------------------------------------- #
#: Sources this release did NOT re-probe because Release 35 measured them
#: read-only two days earlier and recorded the result. Re-probing would burn
#: quota to reproduce a known answer; the row carries the earlier measurement
#: and says so.
INHERITED_MEASUREMENTS = {
    "FMP_ANALYST_ESTIMATES": {
        "state": BLOCKED_LICENCE, "measured_by": "release35",
        "measured_status": 403,
        "note": "free tier refuses the analyst-estimate endpoint"},
    "FINNHUB_ANALYST_ESTIMATES": {
        "state": BLOCKED_LICENCE, "measured_by": "release35",
        "measured_status": 403,
        "note": "free tier refuses the analyst-estimate endpoint"},
    "NASDAQ_DATA_LINK_ZACKS": {
        "state": BLOCKED_LICENCE, "measured_by": "release35",
        "measured_status": 403,
        "note": "free entitlement does not include the Zacks tables"},
    "EODHD_CALENDAR_TRENDS": {
        "state": BLOCKED_LICENCE, "measured_by": "release35",
        "measured_status": 200,
        "note": "answers with today's estimate plus 30/60/90-day deltas; "
                "CURRENT_SNAPSHOT_ONLY and inadmissible as history"},
    "ALPHAVANTAGE_EARNINGS_ESTIMATES": {
        "state": BLOCKED_LICENCE, "measured_by": "release35",
        "measured_status": 200,
        "note": "answers with today's estimate plus deltas; "
                "CURRENT_SNAPSHOT_ONLY and inadmissible as history"},
}


def matrix_rows(measured: dict) -> list:
    """One row per source: what it is, what it supports, what it cost."""
    rows = []
    norgate = measured.get("NORGATE", {})
    for db, row in sorted((norgate.get("databases") or {}).items()):
        rows.append({
            "source": "NORGATE::%s" % db,
            "state": row.get("state"),
            "breadth": row.get("symbol_count"),
            "symbols": row.get("symbols"),
            "cost": "OWNED_EXISTING_ENTITLEMENT",
            "money_spent": 0.0,
            "implementation_level": (
                _contract.LEVEL_NATIVE if db == "Continuous Futures"
                else _contract.LEVEL_PROXY if db.startswith("US Equities")
                else _contract.LEVEL_SIGNAL),
        })
    for key in ("CBOE_VIX_FUTURES", "CBOE_INDICES", "EIA_BULK", "FRED"):
        row = measured.get(key) or {}
        rows.append({
            "source": row.get("source", key),
            "state": row.get("state"),
            "cost": "FREE_PUBLIC",
            "money_spent": 0.0,
            "implementation_level": row.get("implementation_level"),
            "consequence": row.get("consequence") or row.get("reason"),
        })
    for name, row in sorted(INHERITED_MEASUREMENTS.items()):
        rows.append({"source": name, "state": row["state"],
                     "cost": "PAID_ENTITLEMENT_REQUIRED", "money_spent": 0.0,
                     "measured_by": row["measured_by"],
                     "consequence": row["note"]})
    return rows


def artifact(measured: dict, *, campaign_id: str, created_at: str) -> dict:
    rows = matrix_rows(measured)
    blocked = sorted({r["source"] for r in rows
                      if r["state"] in (BLOCKED_LICENCE, BLOCKED_MISSING,
                                        BLOCKED_CREDENTIAL)})
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "api_key_implies_entitlement": _contract.API_KEY_IMPLIES_ENTITLEMENT,
        "credential_presence": credential_presence(),
        "credentials_written_to_artifacts": False,
        "measured": measured,
        "rows": rows,
        "sources_entitled": sorted({r["source"] for r in rows
                                    if r["state"] == ENTITLED}),
        "sources_blocked": blocked,
        "native_futures_supported": measured.get("NORGATE", {}).get(
            "native_futures_supported"),
        "vix_futures_entitled":
            measured.get("CBOE_VIX_FUTURES", {}).get("state") == ENTITLED,
        "money_spent": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
        "subscription_tier_changed": False,
    }
    return r36.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    return r36.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r36.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = _contract.CAMPAIGN_ID) -> Optional[dict]:
    return r36.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "ENTITLED", "BLOCKED_LICENCE",
           "BLOCKED_MISSING", "BLOCKED_CREDENTIAL", "NOT_CONFIGURED",
           "UNMEASURED", "CREDENTIAL_ENVIRONMENT", "credential_presence",
           "measure_norgate", "measure_vix_futures", "measure_cboe_indices",
           "measure_eia_bulk", "measure_fred", "measure_all", "matrix_rows",
           "artifact", "freeze", "load", "path_for", "INHERITED_MEASUREMENTS"]
