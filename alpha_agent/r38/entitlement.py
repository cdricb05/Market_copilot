"""alpha_agent.r38.entitlement - Phase 1: prove the LOCAL entitlement.

The operator's purchase confirmation proves a subscription exists on Norgate's
side. This module measures what the LOCAL installation actually serves, and it
classifies every provider call through the frozen six-state taxonomy before
any conclusion about the subscription is permitted.

The taxonomy exists because of a near-miss: Release 37 observed
``futures_market_session_contracts('&ES')`` raise ``ValueError`` and could
have recorded it as an entitlement wall. It is not. The installed client's own
source shows the function calls the ``futuresmarketsession/<symbol>/
sessioncontracts`` endpoint, whose identifier domain is the SESSION-symbol
namespace returned by ``futures_market_session_symbols()`` - and ``&ES`` is a
symbol from the Continuous Futures DATABASE namespace. Passing it there is a
caller defect: ``PARAMETER_ERROR``, never ``ENTITLEMENT_ERROR``. The
regression in ``tests/test_release38_native_futures_information_frontier.py``
keeps that distinction permanent.

Everything here is READ-ONLY against the local Norgate Data Updater's web API.
Nothing is purchased, changed, upgraded, renewed or downloaded by this module.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any, Callable, Optional

from .. import r38
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r38.entitlement"
SCHEMA = "r38_delivered_futures_entitlement/1"
ARTIFACT_NAME = C.ARTIFACT_NAMES["delivered_futures_entitlement"]

#: The legacy databases every prior release measured. A local state that
#: serves exactly these is indistinguishable from the pre-purchase baseline.
PRE_PURCHASE_DATABASES = (
    "Cash Commodities", "Continuous Futures", "Economic", "Forex Spot",
    "US Equities", "US Equities Delisted", "US Indices", "World Indices")

#: Exact operator actions when the entitlement has not synchronized locally.
#: These are instructions to a HUMAN; this release performs none of them.
OPERATOR_ACTIONS_WHEN_NOT_SYNCHRONIZED = (
    "Open the Norgate Data Updater application on this machine, open its "
    "Databases view, and check whether a Futures database is now offered; "
    "if offered, enable it and let NDU complete a full update",
    "If no Futures database is offered: exit the Norgate Data Updater from "
    "the system tray and relaunch it, forcing a fresh login and subscription "
    "re-enumeration, then let it complete an update",
    "If the Futures database still does not appear: verify on the Norgate "
    "account page that 'World Futures (Silver)' shows ACTIVE rather than "
    "pending, then contact Norgate support - the datafeed server's "
    "subscription list for this account did not include a futures database",
    "Free disk space on C: before the first futures download: NDU stages "
    "downloads through the Windows Temp folder on C:, C: holds under 7 GB "
    "free, and the NDU log records a prior 'Disk space on C:\\ ... "
    "insufficient' download failure; the initial futures history is "
    "multi-gigabyte",
)


def _nd():
    import norgatedata as nd
    return nd


def _attempt(fn: Callable, *args: Any) -> dict:
    try:
        value = fn(*args)
        return {"ok": True, "value": value}
    except Exception as exc:  # provider client raises bare ValueError
        return {"ok": False, "error_type": type(exc).__name__,
                "error": repr(exc)}


def classify_session_contracts_call(
        session_symbol: str,
        outcome: dict,
        *,
        delivered_session_symbols: list,
        continuous_database_symbols: list,
        dated_database_present: bool) -> str:
    """Classify one ``futures_market_session_contracts`` call.

    The identifier domain of the endpoint is the session-symbol namespace.
    The classification NEVER lets a caller defect masquerade as an
    entitlement limitation, and never lets a missing entitlement masquerade
    as a code defect.
    """
    if outcome.get("ok") and len(outcome.get("value") or []):
        return C.CALL_VALID_WITH_DATA
    if session_symbol in (continuous_database_symbols or []):
        # A Continuous-Futures DATABASE symbol (e.g. '&ES') passed to a
        # session endpoint is a caller defect regardless of entitlement,
        # and regardless of whether the endpoint raises or answers empty.
        return C.CALL_PARAMETER_ERROR
    if session_symbol not in (delivered_session_symbols or []):
        return C.CALL_UNSUPPORTED_MARKET
    if outcome.get("ok"):
        return C.CALL_EMPTY_HISTORY
    if not dated_database_present:
        # The identifier is valid and enumerable, and the dated-contract
        # database backing the endpoint has not been delivered locally.
        return C.CALL_ENTITLEMENT_ERROR
    return C.CALL_OTHER_PROVIDER_ERROR


def _dated_database_present(databases: list) -> bool:
    """True when a dated-contract futures database is served locally.

    The Continuous Futures database is NOT dated contracts; any other
    database whose name mentions futures is treated as the dated store.
    """
    for name in databases or []:
        if "futures" in str(name).lower() and name != "Continuous Futures":
            return True
    return False


def probe() -> dict:
    """READ-ONLY snapshot of the locally delivered futures entitlement."""
    nd = _nd()
    captured = _dt.datetime.now(_dt.timezone.utc).isoformat()

    package_version = getattr(nd, "__version__", None)
    status = _attempt(nd.status)
    databases = _attempt(nd.databases)
    db_list = databases.get("value") or []
    update_times = {db: _attempt(nd.last_database_update_time, db)
                    for db in db_list}

    markets = _attempt(nd.futures_market_symbols)
    sessions = _attempt(nd.futures_market_session_symbols)
    market_list = markets.get("value") or []
    session_list = sessions.get("value") or []

    continuous = _attempt(nd.database_symbols, "Continuous Futures")
    continuous_list = continuous.get("value") or []
    dated_present = _dated_database_present(db_list)

    # Per-session contract enumeration, each call classified.
    session_contracts = {}
    for sym in session_list:
        outcome = _attempt(nd.futures_market_session_contracts, sym)
        klass = classify_session_contracts_call(
            sym, outcome,
            delivered_session_symbols=session_list,
            continuous_database_symbols=continuous_list,
            dated_database_present=dated_present)
        contracts = outcome.get("value") or []
        session_contracts[sym] = {
            "classification": klass,
            "count": len(contracts) if outcome.get("ok") else 0,
            "ok": bool(outcome.get("ok")),
            "error_type": outcome.get("error_type"),
        }

    # The R37 near-miss, re-run and classified: '&ES' into the session
    # endpoint. This is the permanent API-contract experiment.
    amp_es = _attempt(nd.futures_market_session_contracts, "&ES")
    amp_es_class = classify_session_contracts_call(
        "&ES", amp_es,
        delivered_session_symbols=session_list,
        continuous_database_symbols=continuous_list,
        dated_database_present=dated_present)
    amp_es_session = _attempt(nd.futures_market_session_symbol, "&ES")

    contracts_accessible = any(
        row["classification"] in (C.CALL_VALID_WITH_DATA,)
        for row in session_contracts.values())

    if dated_present and contracts_accessible:
        sync_state = C.SYNC_SYNCHRONIZED
    elif dated_present or len(market_list) > C.PRE_PURCHASE_BASELINE_FUTURES_MARKETS:
        sync_state = C.SYNC_PARTIAL
    else:
        sync_state = C.SYNC_NOT_SYNCHRONIZED

    return {
        "captured_utc": captured,
        "read_only": True,
        "norgatedata_package_version": package_version,
        "package_pinned_at": "1.0.74",
        "ndu_status": status,
        "databases": db_list,
        "database_update_times": update_times,
        "pre_purchase_databases": list(PRE_PURCHASE_DATABASES),
        "dated_futures_database_present": dated_present,
        "futures_market_symbols": market_list,
        "futures_market_symbols_count": len(market_list),
        "futures_market_session_symbols": session_list,
        "continuous_futures_symbols": continuous_list,
        "session_contract_enumeration": session_contracts,
        "dated_contracts_accessible": contracts_accessible,
        "api_contract_experiment": {
            "call": "futures_market_session_contracts('&ES')",
            "outcome_ok": bool(amp_es.get("ok")),
            "classification": amp_es_class,
            "session_symbol_of_&ES": amp_es_session.get("value"),
            "finding": (
                "'&ES' is a Continuous-Futures database symbol; the "
                "sessioncontracts endpoint's identifier domain is the "
                "session-symbol namespace, so the R37 ValueError was a "
                "PARAMETER_ERROR, not an entitlement measurement"),
        },
        "sync_state": sync_state,
    }


def build(probe_result: dict, *, campaign_id: str = C.CAMPAIGN_ID,
          created_at: Optional[str] = None) -> dict:
    """The DELIVERED_FUTURES_ENTITLEMENT artifact body."""
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    sync_state = probe_result.get("sync_state")
    payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "inherited_purchase": dict(C.INHERITED_PURCHASE),
        "website_confirmation_is_not_local_synchronization":
            C.WEBSITE_CONFIRMATION_IS_NOT_LOCAL_SYNCHRONIZATION,
        "probe": probe_result,
        "sync_state": sync_state,
        "sync_vocab": list(C.SYNC_VOCAB),
        "provider_call_classification_vocab":
            list(C.PROVIDER_CALL_CLASSIFICATION_VOCAB),
        "a_programmer_error_is_not_an_entitlement_limitation":
            C.A_PROGRAMMER_ERROR_IS_NOT_AN_ENTITLEMENT_LIMITATION,
        "operator_actions_required":
            (list(OPERATOR_ACTIONS_WHEN_NOT_SYNCHRONIZED)
             if sync_state != C.SYNC_SYNCHRONIZED else []),
        "dependent_phases_blocked":
            (["PHASE_2_ENUMERATION", "PHASE_3_QUALITY_VALIDATION",
              "PHASE_4_RESEARCH_LAYER", "PHASE_5_COVERAGE_CLOSURE",
              "PHASE_7_TO_11_NATIVE_RESEARCH", "PHASE_16_ML_PANEL"]
             if sync_state != C.SYNC_SYNCHRONIZED else []),
    }
    return r38.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> None:
    path = path_for(body["campaign_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    r38.write_json(path, body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    path = path_for(campaign_id)
    if not path.exists():
        return None
    return r38.read_json(path)
