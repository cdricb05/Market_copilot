"""alpha_agent.r59.data_scout - the PERSISTENT MARKET-DATA SCOUT (R99.1).

R95 and R98 measured the data landscape once each and then nothing kept it
current: a vendor reply, a new free sample or a forward archive going stale had
no owner, so every campaign re-described the landscape from scratch. This
module is that owner. It is NOT a second registry: every dataset it tracks is a
row of the ONE data-opportunity frontier (``opportunities`` in the R59 research
memory, owned by :mod:`alpha_agent.r59.opportunities`). The scout's fields live
in that row's ``detail["scout"]`` block and its lifecycle state maps onto the
frontier's own state vocabulary, so the governor and every existing reader see
one answer.

DELTA, NOT CENSUS. Each run asks only "what changed since the last run?":

  daily   forward-archive freshness (state CLASS, never a timestamp), the
          sample inbox, vendor follow-up dates
  weekly  the daily checks plus official free-sample endpoints the registry
          declares auto-downloadable (none is declared by default)

If no tracked state changed the run returns ``NO_MATERIAL_DATA_DELTA``, writes
no row and no event, and stops.

SAMPLES. A file that lands in ``<research_root>/data_scout/samples/<id>/inbox``
is hashed, parsed, schema-checked, PIT-checked, delisted-checked and
coverage-checked against the row's declared ``sample_spec``. It becomes
SAMPLE_CERTIFIED_READY_TO_TEST (with an OPEN test obligation naming the next
governed steps) or SAMPLE_PIT_FAILED. A processed hash is never processed twice.

This module purchases nothing, starts no trial, sends no message, creates no
account and never calls a paid endpoint. A vendor request is recorded as SENT
only with operator-supplied evidence.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from .. import r59
from . import memory as M
from . import opportunities as OPP

OWNER = "alpha_agent.r59.data_scout"
#: The registry this module writes is the opportunities frontier, whose
#: calculation owner is unchanged.
REGISTRY_OWNER = OPP.CALCULATION_OWNER
SCOUT_VERSION = "R99_1_DATA_SCOUT_V1"

# --------------------------------------------------------------------------- #
# Lifecycle vocabulary and its projection onto the frontier's states.
# --------------------------------------------------------------------------- #
S_OWNED_ACTIVE = "OWNED_ACTIVE"
S_OWNED_UNDERUSED = "OWNED_UNDERUSED"
S_FREE_AVAILABLE = "FREE_AVAILABLE"
S_SAMPLE_REQUESTED = "SAMPLE_REQUESTED"
S_SAMPLE_RECEIVED = "SAMPLE_RECEIVED"
S_SAMPLE_PIT_FAILED = "SAMPLE_PIT_FAILED"
S_SAMPLE_READY = "SAMPLE_CERTIFIED_READY_TO_TEST"
S_TESTED_NO_ALPHA = "TESTED_NO_ALPHA"
S_TESTED_SURVIVOR = "TESTED_SURVIVOR"
S_QUOTE_REQUIRED = "QUOTE_REQUIRED"
S_AWAITING_VENDOR = "AWAITING_VENDOR"
S_BUY_PENDING = "BUY_RECOMMENDED_PENDING_HUMAN_APPROVAL"
S_DO_NOT_BUY = "DO_NOT_BUY"
S_NO_ACCESS = "NO_PRACTICAL_ACCESS"

SCOUT_STATES = (
    S_OWNED_ACTIVE, S_OWNED_UNDERUSED, S_FREE_AVAILABLE, S_SAMPLE_REQUESTED,
    S_SAMPLE_RECEIVED, S_SAMPLE_PIT_FAILED, S_SAMPLE_READY, S_TESTED_NO_ALPHA,
    S_TESTED_SURVIVOR, S_QUOTE_REQUIRED, S_AWAITING_VENDOR, S_BUY_PENDING,
    S_DO_NOT_BUY, S_NO_ACCESS)

#: Only OWNED_UNDERUSED reaches ALREADY_OWNED_UNUSED, the one state for which
#: the governor issues probe mandates (and its probe watermark keeps that from
#: becoming a busy loop).
SCOUT_TO_FRONTIER = {
    S_OWNED_ACTIVE: r59.DO_FREE_AVAILABLE,
    S_OWNED_UNDERUSED: r59.DO_ALREADY_OWNED_UNUSED,
    S_FREE_AVAILABLE: r59.DO_FREE_AVAILABLE,
    S_SAMPLE_REQUESTED: r59.DO_WAITING_FOR_SAMPLE,
    S_AWAITING_VENDOR: r59.DO_WAITING_FOR_SAMPLE,
    S_SAMPLE_RECEIVED: r59.DO_SAMPLE_UNDER_EVALUATION,
    S_SAMPLE_READY: r59.DO_SAMPLE_UNDER_EVALUATION,
    S_SAMPLE_PIT_FAILED: r59.DO_REJECTED_LOW_VALUE,
    S_TESTED_NO_ALPHA: r59.DO_EXHAUSTED,
    S_TESTED_SURVIVOR: r59.DO_PURCHASE_CANDIDATE,
    S_QUOTE_REQUIRED: r59.DO_BLOCKED,
    S_BUY_PENDING: r59.DO_PURCHASE_CANDIDATE,
    S_DO_NOT_BUY: r59.DO_REJECTED_LOW_VALUE,
    S_NO_ACCESS: r59.DO_BLOCKED,
}

#: Every scout row carries exactly these keys in ``detail["scout"]``.
REGISTRY_FIELDS = (
    "provider", "dataset", "information_family", "asset_classes", "fields",
    "history_start", "frequency", "pit_status", "revision_vintage_support",
    "delisted_inactive_support", "sample_available", "sample_status",
    "ownership", "current_entitlement", "price", "price_verified_date",
    "license_status", "research_novelty", "prior_experiments",
    "alpha_test_status", "last_checked", "next_check", "external_dependency",
    "vendor_request_status", "vendor_request_sent_date", "vendor_followup_due",
    "human_action_required", "recommended_action", "scout_state")

#: Fields whose change is MATERIAL. Timestamps never are.
_MATERIAL_KEYS = ("scout_state", "sample_status", "vendor_request_status",
                  "alpha_test_status", "lane_status", "price",
                  "human_action_required", "processed_sample_hashes")

LANE_ACTIVE, LANE_STALE, LANE_MISSING = "ACTIVE", "STALE", "MISSING"
#: The collection policy's own maximum staleness for these lanes (4 days -
#: a weekend plus a holiday; engine.collection_cadence).
LANE_MAX_STALENESS_DAYS = 4
LANE_MIN_COVERAGE = 0.95
UNIVERSE_REFRESH_DAYS = 90
VENDOR_FOLLOWUP_DAYS = 10
TEST_OBLIGATION_DAYS = 7
DAILY_EVERY = timedelta(hours=20)
WEEKLY_EVERY = timedelta(days=7)

META_RUN = "r99_1_data_scout_last_run"
META_ROOTS = "r99_1_data_scout_roots"

VENDOR_EVENTS = ("REQUEST_SENT", "VENDOR_RESPONSE", "QUOTE_RECEIVED",
                 "SAMPLE_DELIVERED", "NO_RESPONSE_FOLLOWUP_SENT")

R_NO_DELTA = "NO_MATERIAL_DATA_DELTA"
R_DELTA = "MATERIAL_DATA_DELTA"
R_NOT_DUE = "NOT_DUE"

_REPO = Path(__file__).resolve().parents[2]
_R98_ACCESS_MATRIX = (_REPO / "research" / "agents"
                      / "campaign_r98_alpha_information_acquisition"
                      / "R98_ACCESS_MATRIX.json")
_UNIVERSE_FILE = (_REPO / "configs" / "alpha_agent"
                  / "eodhd_forward_archive_universe.json")


def _today(now: datetime) -> date:
    return now.astimezone(timezone.utc).date()


def _iso(now: datetime) -> str:
    return now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_day(text) -> Optional[date]:
    try:
        return date.fromisoformat(str(text)[:10])
    except (TypeError, ValueError):
        return None


def _slug(text: str, n: int = 56) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", str(text).upper()).strip("_")[:n]


def samples_root(override=None) -> Path:
    """Where vendor or free samples are dropped: one inbox per opportunity."""
    return Path(override) if override else r59.research_root() / "data_scout" / "samples"


# --------------------------------------------------------------------------- #
# The bootstrap catalogue: what R95/R98 already established, imported once.
# --------------------------------------------------------------------------- #
def _entry(oid, *, title, provider, dataset, scout_state, information_family,
           asset_class=r59.AC_CROSS_ASSET, **scout) -> dict:
    block = {k: None for k in REGISTRY_FIELDS}
    block.update(provider=provider, dataset=dataset,
                 information_family=information_family,
                 scout_state=scout_state)
    block.update(scout)
    return {"opportunity_id": oid, "title": title, "asset_class": asset_class,
            "scout": block}


def _vendor(oid, *, title, provider, dataset, family, request, draft,
            sample_status, contact_allowed=True, vendor_status=None, **kw) -> dict:
    return _entry(
        oid, title=title, provider=provider, dataset=dataset,
        scout_state=S_AWAITING_VENDOR, information_family=family,
        ownership="PAID_NOT_OWNED", current_entitlement="NONE",
        sample_status=sample_status, external_dependency=provider,
        vendor_request_status=vendor_status or "REQUEST_DRAFTED_SEND_UNCONFIRMED",
        vendor_request_sent_date=None, vendor_followup_due=None,
        vendor_request={"type": "SAMPLE_REQUESTED", "asks": request,
                        "draft": draft, "contact_allowed": contact_allowed},
        human_action_required=("NONE - do not contact again"
                               if not contact_allowed else
                               "Record the send date with evidence "
                               "(data-scout --mode vendor-event) once sent"),
        recommended_action="On delivery drop the files in the sample inbox; "
                           "the scout certifies them and opens a test obligation",
        **kw)


_R98 = "research/agents/campaign_r98_alpha_information_acquisition"


def curated_catalogue() -> list:
    """Owned, live-vendor and collected rows whose state R95-R100 measured."""
    eq, rates = r59.AC_US_EQUITY, r59.AC_RATES
    out = [
        # ---- the four live vendor items ------------------------------------ #
        _vendor("DS_HOD_LEVEL2_24Y", title="HistoricalOptionData Level 2 EOD "
                "option chains 2002-present", provider="HistoricalOptionData.com",
                dataset="Level 2 EOD chains with IV and greeks (24-year history)",
                family="OPTIONS_IMPLIED", asset_class=eq,
                request="free full-market Level 2 month from 2012 or 2013",
                draft=_R98 + "/R98_VENDOR_REQUESTS/HISTORICALOPTIONDATA_PRE2018_SAMPLE.md",
                sample_status="FREE_2019_08_MONTH_CERTIFIED_WITH_CAVEATS (22 sessions; "
                              "insufficient for power)",
                price="USD 1,495 one-time (public list price)",
                price_verified_date="2026-10-03", history_start="2002",
                pit_status="EOD snapshot files; restatement UNKNOWN",
                delisted_inactive_support="UNKNOWN - asked in the request",
                prior_experiments=["R98_OPT_CPIV_SPREAD_H21 (candidate only, not preregistered)"],
                alpha_test_status="NOT_TESTED - needs 2011+ history",
                sample_spec={"format": "csv", "required_columns": [],
                             "min_history_years": 0.08}),
        _vendor("DS_ZACKS_NDL_EEH", title="Zacks estimate history via Nasdaq Data "
                "Link (ZEEH/EEH/SEH)", provider="Nasdaq Data Link / Zacks",
                dataset="ZACKS/EEH, SEH, ES earnings and sales estimate history",
                family="ANALYST_ESTIMATE_REVISIONS", asset_class=eq,
                request="historical PIT analyst-estimate/revision sample + pricing",
                draft=_R98 + "/R98_VENDOR_REQUESTS/NASDAQ_DATA_LINK_ZACKS_EEH_SEH_ES.md",
                sample_status="FREE_PREVIEW_CERTIFIED (29 Dow names x 2018; "
                              "backfill unproven)",
                price="QUOTE_REQUIRED", history_start="1979 (vendor text)",
                pit_status="daily vintages present in preview",
                revision_vintage_support="YES in preview",
                delisted_inactive_support="UNKNOWN",
                alpha_test_status="NOT_TESTED",
                sample_spec={"format": "csv", "required_columns": [],
                             "requires_vintages": True}),
        _vendor("DS_BLOOMBERG_PIT_ECON", title="Bloomberg Point-in-Time Economic "
                "Data (surveys frozen at release)", provider="Bloomberg Enterprise Data",
                dataset="PIT Economic Calendar, Actuals and Surveys (+Changes)",
                family="MACRO_CONSENSUS", asset_class=r59.AC_CROSS_ASSET,
                request="historical PIT macro-consensus sample + quote",
                draft=_R98 + "/R98_VENDOR_REQUESTS/BLOOMBERG_PIT_ECONOMIC_DATA.md",
                sample_status="NO_SAMPLE", price="QUOTE_REQUIRED",
                history_start="1997 (vendor)", pit_status="YES_DOCUMENTED",
                revision_vintage_support="YES_DOCUMENTED",
                alpha_test_status="NOT_TESTED",
                sample_spec={"format": "csv", "required_columns": [],
                             "requires_vintages": True}),
        _vendor("DS_INTRINIO_ZACKS", title="Intrinio US fundamentals + Zacks "
                "estimates trial", provider="Intrinio",
                dataset="Zacks estimates / fundamentals (trial)",
                family="ANALYST_ESTIMATE_REVISIONS", asset_class=eq,
                request="no-charge, non-auto-renewing trial (requested by Cedric, "
                        "four follow-ups)",
                draft="research/agents/campaign_r96_data_activation_alpha_offensive/"
                      "R96_INTRINIO_EXTERNAL_DEPENDENCY.json",
                sample_status="NO_SAMPLE", price="QUOTE_REQUIRED",
                contact_allowed=False,
                vendor_status="EXTERNAL_PENDING_NO_RESPONSE",
                alpha_test_status="NOT_TESTED"),
        # ---- EODHD (owned subscription) ------------------------------------ #
        _entry("EODHD_NEWS_UNIVERSE", title="EODHD news forward archive "
               "(1,775-name universe)", provider="EODHD",
               dataset="/news symbol-tagged feed", asset_class=eq,
               scout_state=S_OWNED_ACTIVE, information_family="NEWS_FLOW",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               frequency="intraday feed; archived daily (4-day lookback)",
               history_start="2026-10-02 (forward archive start)",
               pit_status="publication timestamp + first_ingested_at",
               lane="EODHD_NEWS", alpha_test_status="FORWARD_ONLY_NOT_YET_TESTABLE",
               recommended_action="keep collecting; forward-only"),
        _entry("DS_EODHD_ECON_EVENTS", title="EODHD economic-calendar daily "
               "snapshot (actual/estimate/previous)", provider="EODHD",
               dataset="/economic-events", asset_class=r59.AC_CROSS_ASSET,
               scout_state=S_OWNED_ACTIVE, information_family="MACRO_CONSENSUS",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               frequency="daily immutable snapshot (-7/+21 days)",
               history_start="2019 (vendor); archive 2026-10-02",
               pit_status="estimate frozen as observed by the archive",
               lane="EODHD_ECON_EVENTS",
               alpha_test_status="FORWARD_ONLY_NOT_YET_TESTABLE",
               recommended_action="keep collecting; settle the overwrite verdict "
                                  "once two snapshot days exist"),
        _entry("DS_EODHD_ESTIMATE_SNAPSHOTS", title="EODHD estimate-trend daily "
               "vintages (1,775 names)", provider="EODHD",
               dataset="/fundamentals Earnings.Trend + AnalystRatings",
               asset_class=eq, scout_state=S_OWNED_ACTIVE,
               information_family="ANALYST_ESTIMATE_REVISIONS",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               frequency="one immutable vintage per symbol per trading day",
               history_start="2026-07-31 (6 names); 2026-10-02 (universe)",
               pit_status="availability = capture date", lane="EODHD_ESTIMATES",
               alpha_test_status="FORWARD_ONLY_NOT_YET_TESTABLE",
               recommended_action="keep collecting; forward-only"),
        _entry("EODHD_EARNINGS_SESSION_TIMING", title="EODHD earnings history "
               "(actual/estimate/before-after-market) in the daily vintage",
               provider="EODHD", dataset="/fundamentals Earnings.History",
               asset_class=eq, scout_state=S_OWNED_ACTIVE,
               information_family="EARNINGS_SURPRISE",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               frequency="daily vintage", lane="EODHD_EARNINGS",
               prior_experiments=["R96 earnings-surprise cells (settled)"],
               alpha_test_status="TESTED_IN_R96",
               recommended_action="keep; vintages measure vendor estimate rewrites"),
        _entry("EODHD_CORPORATE_ACTION_HISTORY", title="EODHD dividends with "
               "declaration date incl. delisted", provider="EODHD",
               dataset="/div (declarationDate), /splits", asset_class=eq,
               scout_state=S_TESTED_NO_ALPHA,
               information_family="CORPORATE_EVENTS",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               history_start="2009 (declaration non-null >=98% from 2013)",
               delisted_inactive_support="K3 86.2% of extension delisted payers",
               prior_experiments=["H_63012714_6f505e87f7e1 (R98 DIV2_H21)"],
               alpha_test_status="NO_ALPHA_EVIDENCE at Discovery (net -1.55%/yr)",
               recommended_action="none; history stays re-downloadable while owned"),
        _entry("DS_EODHD_SHARES_STATS_HOLDERS", title="EODHD SharesStats + Holders "
               "snapshot (short %, float, institutional/fund holders)",
               provider="EODHD", dataset="/fundamentals SharesStats, Holders",
               asset_class=eq, scout_state=S_OWNED_UNDERUSED,
               information_family="POSITIONING_OWNERSHIP",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               frequency="current snapshot only (forward-only)",
               pit_status="snapshot; PIT only if archived daily",
               research_novelty="LOW - free SEC 13F and FINRA short interest "
                                "carry history; 13F breadth settled DEAD",
               lane="EODHD_SHARES_STATS",
               recommended_action="capture inside the existing daily vintage "
                                  "(zero extra API calls: same fundamentals request)"),
        _entry("DS_EODHD_ETF_HOLDINGS", title="EODHD ETF_Data holdings/sector "
               "weights snapshot", provider="EODHD", dataset="/fundamentals ETF_Data",
               asset_class=eq, scout_state=S_OWNED_UNDERUSED,
               information_family="ETF_HOLDINGS_FLOWS",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               frequency="current snapshot only (forward-only)",
               research_novelty="LOW - SEC N-PORT gives quarterly holdings history free",
               recommended_action="not collected; needs a mechanism before a lane"),
        _entry("DS_EODHD_DELISTED_GLOBAL", title="EODHD delisted list, international "
               "EOD, FX, government bond yields", provider="EODHD",
               dataset="exchange-symbol-list delisted=1, EOD (70 exchanges), "
                       "FOREX, GBOND, /ust", asset_class=r59.AC_CROSS_ASSET,
               scout_state=S_OWNED_UNDERUSED, information_family="REFERENCE_PRICES",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               history_start="US10Y 1980; EURUSD 1975",
               research_novelty="history is downloadable at any time while owned",
               recommended_action="no forward loss; pull only for a preregistered need"),
        _entry("DS_EODHD_OPTIONS", title="EODHD options (not in plan)",
               provider="EODHD", dataset="options", asset_class=eq,
               scout_state=S_NO_ACCESS, information_family="OPTIONS_IMPLIED",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="NOT_ENTITLED"),
        # ---- Norgate (owned subscription) ---------------------------------- #
        _entry("DS_NORGATE_DATED_FUTURES", title="Norgate dated futures contracts "
               "(27,501 contracts, 105 markets) incl. OI/volume",
               provider="Norgate Data", dataset="Futures DB per-contract OHLC, "
               "Volume, Open Interest", asset_class=r59.AC_CROSS_ASSET,
               scout_state=S_OWNED_ACTIVE, information_family="FUTURES_PRICES_CURVE",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               history_start="ZN 1982, ZQ 1988, SR3/SO3 2018, CRA 2020",
               prior_experiments=["every R96-R100 futures experiment (R38/R60 layer, "
                                  "deferred legs, R99 HE far leg)"],
               alpha_test_status="0 survivors across R99 (20) and R100 (3)",
               lane="NORGATE_LOCAL",
               recommended_action="adopt the R96 OI/volume per-market cost model "
                                  "through a governed cost decision; add an OI/ADV "
                                  "capacity check"),
        _entry("DS_NORGATE_PIT_MEMBERSHIP_DELISTED", title="Norgate PIT index "
               "membership + US delisted equities", provider="Norgate Data",
               dataset="index_constituent_timeseries, US Equities Delisted",
               asset_class=eq, scout_state=S_OWNED_ACTIVE,
               information_family="UNIVERSE_SURVIVORSHIP",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               history_start="S&P 500 1982; Russell 1990",
               delisted_inactive_support="YES", lane="NORGATE_LOCAL",
               recommended_action="core; no equivalent replacement owned"),
        _entry("DS_NORGATE_CAPITAL_EVENTS", title="Norgate capital_event / "
               "dividend_yield / unadjusted close / SPAC flags", provider="Norgate Data",
               dataset="capital_event_timeseries, dividend_yield_timeseries, "
                       "unadjusted_close_timeseries, blank_check_company_timeseries",
               asset_class=eq, scout_state=S_OWNED_UNDERUSED,
               information_family="CORPORATE_EVENTS",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               recommended_action="historical; read when a preregistered study needs it"),
        _entry("DS_NORGATE_WORLD_INDICES_CASH_COMMODITIES", title="Norgate World "
               "Indices (31) + Cash Commodities (100)", provider="Norgate Data",
               dataset="World Indices, Cash Commodities", asset_class=r59.AC_EQUITY_INDEX,
               scout_state=S_OWNED_UNDERUSED, information_family="REFERENCE_PRICES",
               ownership="OWNED_SUBSCRIPTION", current_entitlement="ENTITLED",
               recommended_action="historical; no forward loss"),
        # ---- free sources already collected or settled --------------------- #
        _entry("DS_CBOE_PUTCALL_DAILY", title="Cboe daily put/call statistics "
               "2006-2026", provider="Cboe (free)", dataset="daily options market "
               "statistics", asset_class=eq, scout_state=S_FREE_AVAILABLE,
               information_family="OPTIONS_IMPLIED", ownership="FREE",
               prior_experiments=["R98_PC1", "R98_PC1B"],
               alpha_test_status="GATE_REFUSED (return-free power/cost)"),
        _entry("DS_PUBLIC_IBKR_SPDR", title="IBKR shortable/fee file + SPDR NAV "
               "and shares outstanding (forward archive)", provider="IBKR / SSGA (free)",
               dataset="usa.txt; SPDR NAV-history workbooks", asset_class=eq,
               scout_state=S_FREE_AVAILABLE, information_family="POSITIONING_FLOWS",
               ownership="FREE", lane="PUBLIC_FORWARD_ARCHIVE",
               alpha_test_status="FORWARD_ONLY_NOT_YET_TESTABLE"),
    ]
    return out


#: R98 access-matrix rows absorbed into a curated row above:
#: (row index, provider exactly as R98 wrote it) -> the curated row. Both must
#: match, so a reordered or different matrix can never swallow a row.
_R98_ABSORBED = {
    (0, "Nasdaq Data Link (free key)"): "DS_ZACKS_NDL_EEH",
    (4, "EODHD (paid)"): "DS_EODHD_ESTIMATE_SNAPSHOTS",
    (5, "Cboe (free)"): "DS_CBOE_PUTCALL_DAILY",
    (6, "HistoricalOptionData.com (free samples)"): "DS_HOD_LEVEL2_24Y",
    (10, "EODHD (paid)"): "DS_EODHD_OPTIONS",
    (12, "EODHD (paid)"): "DS_EODHD_ECON_EVENTS",
    (15, "SSGA (free)"): "DS_PUBLIC_IBKR_SPDR",
    (16, "EODHD (paid)"): "EODHD_CORPORATE_ACTION_HISTORY",
    (17, "Norgate (paid)"): "DS_NORGATE_CAPITAL_EVENTS",
    (25, "Zacks via Nasdaq Data Link"): "DS_ZACKS_NDL_EEH",
    (26, "Intrinio"): "DS_INTRINIO_ZACKS",
    (27, "EODHD"): "DS_EODHD_ESTIMATE_SNAPSHOTS",
    (38, "HistoricalOptionData.com"): "DS_HOD_LEVEL2_24Y",
    (41, "Bloomberg"): "DS_BLOOMBERG_PIT_ECON"}
_R98_FAMILY = {"P1": "ANALYST_ESTIMATE_REVISIONS", "P2": "OPTIONS_IMPLIED",
               "P3": "MACRO_CONSENSUS", "P4": "POSITIONING_SHORT_INTEREST_FLOWS",
               "P5": "CORPORATE_EVENTS"}


def _r98_state(row: dict) -> str:
    ent = str(row.get("entitlement_state") or "")
    price = str(row.get("price_if_public") or "")
    if ent == "ALREADY_SETTLED":
        return S_TESTED_NO_ALPHA
    if ent.startswith("NOT_APPLICABLE") or ent in ("NOT_ENTITLED",
                                                   "NO_SCRIPTABLE_ACCESS"):
        return S_NO_ACCESS
    if ent.startswith("USED"):
        return S_OWNED_ACTIVE if row.get("owned_entitlement") else S_FREE_AVAILABLE
    if price.lower().startswith("free"):
        return S_FREE_AVAILABLE
    # A public list price is still not a quote: terms, history depth and
    # licence must come back from the vendor before any purchase gate runs.
    return S_QUOTE_REQUIRED


def r98_catalogue(path: Optional[Path] = None) -> list:
    """The R98 provider/access matrix, imported as registry rows (once)."""
    body = r59.read_json(Path(path) if path else _R98_ACCESS_MATRIX) or {}
    out = []
    for i, row in enumerate(body.get("rows") or []):
        if (i, row.get("provider")) in _R98_ABSORBED:
            continue
        ent = str(row.get("entitlement_state") or "")
        out.append(_entry(
            "DS_" + _slug("%s %s" % (row.get("provider"), row.get("product"))),
            title="%s - %s" % (row.get("provider"), row.get("product")),
            provider=row.get("provider"), dataset=row.get("product"),
            scout_state=_r98_state(row),
            information_family=_R98_FAMILY.get(row.get("family"), row.get("family")),
            asset_class=(r59.AC_CROSS_ASSET if row.get("family") == "P3"
                         else r59.AC_US_EQUITY),
            ownership=("OWNED_OR_FREE_KEY" if row.get("owned_entitlement")
                       else "NOT_OWNED"),
            current_entitlement=ent, history_start=row.get("history_start"),
            pit_status=row.get("pit_vintage_support"),
            revision_vintage_support=row.get("pit_vintage_support"),
            delisted_inactive_support=row.get("delisted_support"),
            sample_available=row.get("self_service_sample"),
            price=row.get("price_if_public"),
            price_verified_date=row.get("last_verified"),
            license_status=row.get("licensing"),
            alpha_test_status=("SETTLED" if ent == "ALREADY_SETTLED"
                               else "NOT_TESTED"),
            provenance={"source": "R98_ACCESS_MATRIX.json", "row": i,
                        "evidence": row.get("evidence"),
                        "access_tested": row.get("access_tested_in_r98")}))
    return out


def catalogue(r98_path: Optional[Path] = None) -> list:
    return curated_catalogue() + r98_catalogue(r98_path)


# --------------------------------------------------------------------------- #
# Registry writes - always through the ONE frontier writer.
# --------------------------------------------------------------------------- #
def _scout_rows(mem) -> dict:
    return {o["opportunity_id"]: o for o in mem.opportunities()
            if isinstance((o.get("detail") or {}).get("scout"), dict)}


def _material(block: dict) -> dict:
    return {k: block.get(k) for k in _MATERIAL_KEYS}


def _write(mem, oid: str, *, title: str, asset_class: str, block: dict,
           prior: Optional[dict] = None) -> None:
    state = block.get("scout_state")
    if state not in SCOUT_STATES:
        raise ValueError("unknown scout state: %s" % state)
    detail = dict((prior or {}).get("detail") or {})
    detail["scout"] = block
    detail["scout_owner"] = OWNER
    price = block.get("price")
    m = re.search(r"USD\s*\$?\s*([\d,]+(?:\.\d+)?)", str(price or ""))
    yearly = None
    if m and ("/yr" in str(price) or "year" in str(price).lower()):
        yearly = float(m.group(1).replace(",", ""))
    mem.set_opportunity(
        oid, title=title, state=SCOUT_TO_FRONTIER[state],
        asset_class=asset_class,
        information_need=(prior or {}).get("information_need")
        or "%s: %s" % (block.get("information_family"), block.get("dataset")),
        unlocks=(prior or {}).get("unlocks"),
        owned_but_unused=(state == S_OWNED_UNDERUSED),
        free_proxy=(prior or {}).get("free_proxy"),
        provider=block.get("provider"),
        pit_integrity=block.get("pit_status"),
        effective_sample=(prior or {}).get("effective_sample"),
        expected_value=(prior or {}).get("expected_value"),
        cost_usd_year=yearly if yearly is not None
        else (prior or {}).get("cost_usd_year"),
        gate_verdict=(prior or {}).get("gate_verdict"),
        detail=detail)


def bootstrap(mem, *, now: datetime, r98_path: Optional[Path] = None) -> dict:
    """Insert every catalogue row the registry does not yet track.

    INSERT-IF-ABSENT: a row the scout already tracks is never overwritten here,
    because after bootstrap the registry - not this catalogue - is the
    authority. An existing seed row (no scout block) is ADOPTED: its frontier
    fields are kept and the scout block is attached.
    """
    tracked = _scout_rows(mem)
    existing = {o["opportunity_id"]: o for o in mem.opportunities()}
    inserted, adopted = [], []
    for e in catalogue(r98_path):
        oid = e["opportunity_id"]
        if oid in tracked:
            continue
        block = dict(e["scout"])
        block["last_checked"] = _iso(now)
        block["next_check"] = _iso(now + (DAILY_EVERY if block.get("lane")
                                          else WEEKLY_EVERY))
        block["scout_version"] = SCOUT_VERSION
        prior = existing.get(oid)
        _write(mem, oid, title=e["title"], asset_class=e["asset_class"],
               block=block, prior=prior)
        (adopted if prior else inserted).append(oid)
    return {"inserted": inserted, "adopted": adopted}


# --------------------------------------------------------------------------- #
# Forward-archive lanes - measured on disk, classified, never timestamped.
# --------------------------------------------------------------------------- #
def _universe_size(universe_file: Path) -> int:
    body = r59.read_json(universe_file)
    if isinstance(body, dict):
        body = body.get("symbols") or []
    return len(body) if isinstance(body, list) else 0


def _classify(latest: Optional[date], coverage: Optional[float], today: date) -> str:
    if latest is None:
        return LANE_MISSING
    fresh = (today - latest).days <= LANE_MAX_STALENESS_DAYS
    covered = coverage is None or coverage >= LANE_MIN_COVERAGE
    return LANE_ACTIVE if (fresh and covered) else LANE_STALE


def _dated_dirs(root: Path) -> list:
    if not root.is_dir():
        return []
    return sorted((d for d in root.iterdir()
                   if d.is_dir() and _parse_day(d.name)), key=lambda d: d.name)


def measure_lanes(*, ingestion_root: Path, collection_root: Optional[Path],
                  now: datetime, universe_file: Path = _UNIVERSE_FILE) -> dict:
    """Read-only measurement of every forward lane the registry tracks.

    A lane's coverage is the BEST of its last three capture dates: the archive
    keys a day by UTC, so a trading day's evening passes start the next UTC
    date and a single date can legitimately be partial.
    """
    today = _today(now)
    vint = Path(ingestion_root) / "vintages"
    n_univ = _universe_size(universe_file) or None
    lanes: dict = {}

    # news
    db = vint / "eodhd_forward_archive" / "news" / "news_archive.sqlite"
    latest, cov, rows = None, None, {}
    if db.exists():
        conn = sqlite3.connect("file:%s?mode=ro" % db.as_posix(), uri=True)
        try:
            rows = dict(conn.execute(
                "SELECT capture_date, COUNT(*) FROM symbol_days "
                "GROUP BY capture_date ORDER BY capture_date DESC LIMIT 3").fetchall())
        finally:
            conn.close()
    if rows:
        latest = max(_parse_day(d) for d in rows)
        cov = (max(rows.values()) / n_univ) if n_univ else None
    lanes["EODHD_NEWS"] = {"latest": latest and latest.isoformat(),
                           "coverage": cov, "status": _classify(latest, cov, today)}

    # economic events
    snaps = vint / "eodhd_forward_archive" / "economic_events" / "snapshots"
    days = sorted(_parse_day(p.name.split(".")[0]) for p in snaps.glob("*.json.gz")
                  if _parse_day(p.name.split(".")[0])) if snaps.is_dir() else []
    latest = days[-1] if days else None
    lanes["EODHD_ECON_EVENTS"] = {"latest": latest and latest.isoformat(),
                                  "snapshot_days": len(days), "coverage": None,
                                  "status": _classify(latest, None, today)}

    # analyst vintages (+ the earnings history and shares stats they carry)
    adirs = _dated_dirs(vint / "eodhd_analyst")[-3:]
    counts = {d.name: sum(1 for _ in d.glob("*.json")) for d in adirs}
    best = max(adirs, key=lambda d: counts[d.name]) if adirs else None
    latest = _parse_day(adirs[-1].name) if adirs else None
    cov = (counts[best.name] / n_univ) if (best and n_univ) else None
    lanes["EODHD_ESTIMATES"] = {"latest": latest and latest.isoformat(),
                                "coverage": cov,
                                "status": _classify(latest, cov, today)}
    with_hist = sampled = 0
    if best is not None:
        for p in sorted(best.glob("*.json"))[:40]:
            body = r59.read_json(p) or {}
            sampled += 1
            with_hist += bool(body.get("earnings_history_recent"))
    hist_frac = (with_hist / sampled) if sampled else None
    # SharesStats/Holders were first captured by R99.1: a field the collector
    # only began writing is measured on the NEWEST vintage, not the fullest one.
    # Sampling the fullest of three dates read a pre-R99.1 day and reported a
    # live capture as MISSING while the first post-activation day was partial.
    stats_sampled = with_stats = 0
    if adirs:
        for p in sorted(adirs[-1].glob("*.json"))[:40]:
            body = r59.read_json(p) or {}
            stats_sampled += 1
            with_stats += bool(body.get("shares_stats"))
    lanes["EODHD_EARNINGS"] = {
        "latest": latest and latest.isoformat(), "coverage": cov,
        "earnings_history_fraction_of_sample": hist_frac,
        "status": (_classify(latest, cov, today)
                   if hist_frac and hist_frac >= 0.5 else
                   (LANE_MISSING if latest is None else LANE_STALE))}
    lanes["EODHD_SHARES_STATS"] = {
        "latest": latest and latest.isoformat(),
        "shares_stats_fraction_of_sample": ((with_stats / stats_sampled)
                                            if stats_sampled else None),
        "status": (LANE_ACTIVE if stats_sampled and with_stats / stats_sampled >= 0.5
                   and _classify(latest, None, today) == LANE_ACTIVE
                   else LANE_MISSING)}

    # public archive
    pub = vint / "public_forward_archive"
    ib = _dated_dirs(pub / "ibkr_shortable")
    sp = _dated_dirs(pub / "spdr_nav_history" / "vintages")
    latest = min((_parse_day(x[-1].name) for x in (ib, sp) if x), default=None) \
        if (ib and sp) else None
    lanes["PUBLIC_FORWARD_ARCHIVE"] = {"latest": latest and latest.isoformat(),
                                       "coverage": None,
                                       "status": _classify(latest, None, today)}

    # norgate local updater, as the collection service last saw it
    health = r59.read_json(Path(collection_root) / "source_runtime_health.json") \
        if collection_root else None
    rec = ((health or {}).get("sources") or {}).get("norgate_local") or {}
    latest = _parse_day(rec.get("last_success_at"))
    lanes["NORGATE_LOCAL"] = {"latest": latest and latest.isoformat(),
                              "coverage": None,
                              "status": (_classify(latest, None, today)
                                         if health is not None else LANE_MISSING)}

    age = None
    if universe_file.exists():
        age = (now - datetime.fromtimestamp(universe_file.stat().st_mtime,
                                            tz=timezone.utc)).days
    return {"lanes": lanes, "universe_size": n_univ,
            "universe_file_age_days": age,
            "universe_refresh_due": bool(age is not None
                                         and age > UNIVERSE_REFRESH_DAYS)}


# --------------------------------------------------------------------------- #
# Sample certification - pure, deterministic, dataset-agnostic.
# --------------------------------------------------------------------------- #
def _load_rows(path: Path) -> list:
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
        name = path.stem
    else:
        name = path.name
    if name.endswith(".json"):
        body = json.loads(raw.decode("utf-8-sig"))
        if isinstance(body, dict):
            body = body.get("rows") or body.get("data") or []
        return [r for r in body if isinstance(r, dict)]
    text = raw.decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(io.StringIO(text)))


def certify_sample(path: Path, spec: Optional[dict], *, received_at: datetime) -> dict:
    """Hash, parse, schema, PIT, delisted and coverage checks on ONE file."""
    spec = dict(spec or {})
    path = Path(path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    checks: list = []

    def check(name, ok, evidence):
        checks.append({"check": name, "passed": bool(ok), "evidence": evidence})

    try:
        rows = _load_rows(path)
        check("parse", True, "%d rows" % len(rows))
    except Exception as exc:                              # noqa: BLE001
        rows = []
        check("parse", False, "%s: %s" % (type(exc).__name__, str(exc)[:160]))
    cols = set(rows[0].keys()) if rows else set()
    need = list(spec.get("required_columns") or [])
    missing = [c for c in need if c not in cols]
    check("schema", bool(rows) and not missing,
          {"columns": sorted(cols)[:40], "missing_required": missing})

    asof = spec.get("asof_column")
    if asof:
        stamps = [_parse_day(r.get(asof)) for r in rows]
        good = [s for s in stamps if s]
        future = [s for s in good if s > _today(received_at)]
        check("pit_asof_parseable", rows and len(good) >= 0.99 * len(rows),
              "%d/%d parseable" % (len(good), len(rows)))
        check("pit_no_future_asof", not future,
              "%d as-of dates after receipt" % len(future))
        if spec.get("requires_vintages"):
            key_cols = list(spec.get("vintage_key_columns") or [])
            groups: dict = {}
            for r in rows:
                groups.setdefault(tuple(r.get(k) for k in key_cols), set()).add(
                    r.get(asof))
            multi = sum(1 for v in groups.values() if len(v) > 1)
            check("pit_revision_vintages", multi > 0,
                  "%d of %d keys carry more than one as-of vintage"
                  % (multi, len(groups)))
        if good:
            span = (max(good) - min(good)).days / 365.25
            floor = float(spec.get("min_history_years") or 0)
            check("coverage_history", span >= floor,
                  "%.2f years (floor %.2f)" % (span, floor))
    else:
        check("pit_asof_declared", False,
              "the dataset's sample_spec declares no asof_column; PIT cannot be "
              "certified until the data-foundation agent declares it")

    ent = spec.get("entity_column")
    probes = list(spec.get("delisted_probe_entities") or [])
    if ent and probes:
        present = {str(r.get(ent)) for r in rows}
        hit = [p for p in probes if p in present]
        check("delisted_inactive", len(hit) >= max(1, int(0.5 * len(probes))),
              {"probes": probes, "present": hit})
    if ent:
        floor_n = int(spec.get("min_entities") or 0)
        n = len({r.get(ent) for r in rows})
        check("coverage_entities", n >= floor_n, "%d entities (floor %d)" % (n, floor_n))

    ok = all(c["passed"] for c in checks)
    return {"file": path.name, "sha256": sha, "rows": len(rows),
            "verdict": S_SAMPLE_READY if ok else S_SAMPLE_PIT_FAILED,
            "failed": [c["check"] for c in checks if not c["passed"]],
            "checks": checks}


# --------------------------------------------------------------------------- #
# The run.
# --------------------------------------------------------------------------- #
def _due(meta: dict, kind: str, now: datetime) -> bool:
    last = (meta or {}).get(kind + "_at")
    if not last:
        return True
    try:
        t = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
    except ValueError:
        return True
    return now - t >= (DAILY_EVERY if kind == "daily" else WEEKLY_EVERY)


def _lane_state(entry_lane: str, lane: dict, current: str) -> str:
    """The lifecycle state a lane observation implies for its row."""
    status = (lane or {}).get("status")
    if entry_lane == "EODHD_SHARES_STATS":
        return S_OWNED_ACTIVE if status == LANE_ACTIVE else S_OWNED_UNDERUSED
    if current in (S_OWNED_ACTIVE, S_OWNED_UNDERUSED):
        return S_OWNED_ACTIVE if status == LANE_ACTIVE else S_OWNED_UNDERUSED
    return current


def _novelty(mem, block: dict) -> dict:
    mech = block.get("mechanism") or {}
    if not mech:
        return {"verdict": "NO_MECHANISM_DECLARED",
                "note": "the director must declare the mechanism before the "
                        "novelty check can run"}
    st = mem.mechanism_state(**{k: mech.get(k) for k in (
        "asset_class", "economic_family", "information_family", "model_family")})
    if st.get("query_valid") is False:
        return {"verdict": "INVALID_QUERY"}
    return {"verdict": ("SETTLED_DO_NOT_REPEAT" if st.get("mechanism_is_settled")
                        else "NOVEL_OR_UNSETTLED"),
            "n_matching": st.get("n_matching")}


def run(mem=None, *, mode: str = "auto", now: Optional[datetime] = None,
        ingestion_root=None, collection_root=None, sample_root=None,
        fetcher: Optional[Callable[[str], bytes]] = None,
        r98_path: Optional[Path] = None,
        universe_file: Path = _UNIVERSE_FILE) -> dict:
    """One scout pass. ``mode`` is auto | daily | weekly.

    ``auto`` does nothing unless a daily (20 h) or weekly (7 d) pass is due.
    Roots supplied by the caller are remembered, so the research loop - which
    may not import the application layer - reuses exactly the roots the
    operator CLI resolved through their owners.
    """
    mem = mem or M.open_memory()
    now = now or datetime.now(timezone.utc)
    meta = mem.get_meta(META_RUN) or {}
    roots = mem.get_meta(META_ROOTS) or {}
    if ingestion_root or collection_root:
        roots = {"ingestion_root": str(ingestion_root) if ingestion_root
                 else roots.get("ingestion_root"),
                 "collection_root": str(collection_root) if collection_root
                 else roots.get("collection_root")}
        if roots != (mem.get_meta(META_ROOTS) or {}):
            mem.set_meta(META_ROOTS, roots)
    if mode == "auto":
        mode = ("weekly" if _due(meta, "weekly", now) else
                "daily" if _due(meta, "daily", now) else None)
        if mode is None:
            return {"result": R_NOT_DUE, "owner": OWNER,
                    "last_daily_at": meta.get("daily_at"),
                    "last_weekly_at": meta.get("weekly_at")}

    boot = bootstrap(mem, now=now, r98_path=r98_path)
    rows = _scout_rows(mem)
    deltas: list = [{"opportunity_id": oid, "change": "INSERTED"}
                    for oid in boot["inserted"]] + \
                   [{"opportunity_id": oid, "change": "ADOPTED"}
                    for oid in boot["adopted"]]

    lanes = None
    if roots.get("ingestion_root"):
        lanes = measure_lanes(ingestion_root=Path(roots["ingestion_root"]),
                              collection_root=(Path(roots["collection_root"])
                                               if roots.get("collection_root") else None),
                              now=now, universe_file=universe_file)

    sroot = samples_root(sample_root)
    today = _today(now)
    for oid, row in sorted(rows.items()):
        block = dict(row["detail"]["scout"])
        before = _material(block)
        notes = []

        # 1. forward-lane freshness
        lane_key = block.get("lane")
        if lanes and lane_key in lanes["lanes"]:
            lane = lanes["lanes"][lane_key]
            block["lane_status"] = lane["status"]
            block["lane_observation"] = lane
            block["scout_state"] = _lane_state(lane_key, lane, block["scout_state"])
            if lane_key == "EODHD_NEWS":
                block["human_action_required"] = (
                    "REFRESH_FORWARD_ARCHIVE_UNIVERSE (file older than %d days)"
                    % UNIVERSE_REFRESH_DAYS if lanes["universe_refresh_due"] else None)

        # 2. vendor follow-up
        due = _parse_day(block.get("vendor_followup_due"))
        if (block.get("scout_state") == S_AWAITING_VENDOR and due
                and due <= today and (block.get("vendor_request") or {}).get(
                    "contact_allowed", True)):
            block["human_action_required"] = "VENDOR_FOLLOW_UP_DUE since %s" % due

        # 3. sample inbox
        inbox = sroot / oid / "inbox"
        done = list(block.get("processed_sample_hashes") or [])
        if inbox.is_dir():
            for f in sorted(p for p in inbox.iterdir() if p.is_file()):
                sha = hashlib.sha256(f.read_bytes()).hexdigest()
                if sha in done:
                    continue
                cert = certify_sample(f, block.get("sample_spec"), received_at=now)
                done.append(sha)
                block.setdefault("sample_certifications", []).append(
                    dict(cert, certified_at=_iso(now)))
                block["sample_status"] = "%s:%s" % (cert["verdict"], cert["file"])
                block["scout_state"] = cert["verdict"]
                if cert["verdict"] == S_SAMPLE_READY:
                    # No "interesting sample, test later": the obligation is
                    # opened here and stays OPEN until a governed test closes it.
                    block["test_obligation"] = {
                        "status": "OPEN", "opened_at": _iso(now),
                        "due": (today + timedelta(days=TEST_OBLIGATION_DAYS)).isoformat(),
                        "sample_sha256": sha, "novelty": _novelty(mem, block),
                        "next_steps": ["DELISTED/INACTIVE CHECK (data-foundation-agent "
                                       "certify_data)",
                                       "POWER/COST GATE (r61 mechanism_power + "
                                       "cost_budget, return-free)",
                                       "PRIOR-WORK NOVELTY CHECK (alpha_agents_v2.py "
                                       "check-mechanism)",
                                       "PREREGISTER (quant-research-director, "
                                       "FULL_RECIPE_V1)",
                                       "DISCOVERY TEST (canonical runner)"],
                        "owner": "quant-research-director"}
                    block["human_action_required"] = None
                    block["recommended_action"] = "RUN THE OPEN TEST OBLIGATION"
                else:
                    block["recommended_action"] = (
                        "record the failing checks with the vendor: %s"
                        % ", ".join(cert["failed"]))
                notes.append("sample %s -> %s" % (f.name, cert["verdict"]))
        if done:
            block["processed_sample_hashes"] = done

        # 4. weekly: official free sample auto-download (declared rows only)
        auto = block.get("free_sample") or {}
        if (mode == "weekly" and fetcher is not None and auto.get("url")
                and auto.get("auto_download_allowed") is True):
            try:
                body = fetcher(auto["url"])
                sha = hashlib.sha256(body).hexdigest()
                if sha not in done:
                    inbox.mkdir(parents=True, exist_ok=True)
                    name = auto.get("filename") or "free_sample.bin"
                    (inbox / ("%s_%s" % (sha[:12], name))).write_bytes(body)
                    notes.append("free sample downloaded (%s); certified next pass"
                                 % sha[:12])
                    block["sample_status"] = "DOWNLOADED_PENDING_CERTIFICATION"
            except Exception as exc:                       # noqa: BLE001
                notes.append("free sample fetch failed: %s" % type(exc).__name__)

        block["last_checked"] = _iso(now)
        block["next_check"] = _iso(now + (DAILY_EVERY if lane_key else WEEKLY_EVERY))
        after = _material(block)
        if after != before:
            _write(mem, oid, title=row["title"], asset_class=row.get("asset_class"),
                   block=block, prior=row)
            deltas.append({"opportunity_id": oid, "change": "STATE",
                           "before": before, "after": after, "notes": notes})
            if before.get("scout_state") != after.get("scout_state"):
                mem.event("DATA_SCOUT_STATE_CHANGED", subject=oid,
                          detail={"from": before.get("scout_state"),
                                  "to": after.get("scout_state"), "notes": notes})

    meta = dict(meta)
    meta[mode + "_at"] = _iso(now)
    if mode == "weekly":
        meta["daily_at"] = _iso(now)
    meta["last_result"] = R_DELTA if deltas else R_NO_DELTA
    mem.set_meta(META_RUN, meta)
    out = {"result": R_DELTA if deltas else R_NO_DELTA, "mode": mode,
           "owner": OWNER, "registry_owner": REGISTRY_OWNER,
           "ran_at": _iso(now), "deltas": deltas,
           "lanes": (lanes or {}).get("lanes"),
           "roots_known": bool(roots.get("ingestion_root"))}
    if deltas:
        mem.event("DATA_SCOUT_DELTA", subject=mode,
                  detail={"n": len(deltas),
                          "ids": [d["opportunity_id"] for d in deltas][:50]})
    return out


def record_vendor_event(mem, opportunity_id: str, *, event: str, on: str,
                        evidence: str, note: str = "", price: Optional[str] = None,
                        now: Optional[datetime] = None) -> dict:
    """Record a HUMAN vendor interaction. The scout itself contacts nobody.

    Idempotent on (event, date, evidence): replaying the same record changes
    nothing. A send is refused without evidence, and refused outright for a
    vendor the operator said must not be contacted again.
    """
    now = now or datetime.now(timezone.utc)
    if event not in VENDOR_EVENTS:
        raise ValueError("unknown vendor event: %s" % event)
    if not str(evidence or "").strip():
        raise ValueError("EVIDENCE_REQUIRED: a vendor event is recorded only with "
                         "operator-supplied evidence")
    day = _parse_day(on)
    if day is None:
        raise ValueError("on must be an ISO date")
    rows = _scout_rows(mem)
    row = rows.get(opportunity_id)
    if row is None:
        raise ValueError("UNKNOWN_OPPORTUNITY: %s" % opportunity_id)
    block = dict(row["detail"]["scout"])
    req = block.get("vendor_request") or {}
    if event in ("REQUEST_SENT", "NO_RESPONSE_FOLLOWUP_SENT") and \
            req.get("contact_allowed") is False:
        raise ValueError("CONTACT_NOT_ALLOWED: %s must not be contacted again"
                         % opportunity_id)
    key = hashlib.sha256(("%s|%s|%s" % (event, day.isoformat(),
                                        evidence.strip())).encode()).hexdigest()[:16]
    log = list(block.get("vendor_log") or [])
    if any(x.get("key") == key for x in log):
        return {"recorded": False, "duplicate": True, "opportunity_id": opportunity_id}
    log.append({"key": key, "event": event, "on": day.isoformat(),
                "evidence": evidence.strip()[:500], "note": note[:500],
                "recorded_at": _iso(now)})
    block["vendor_log"] = log
    if event in ("REQUEST_SENT", "NO_RESPONSE_FOLLOWUP_SENT"):
        block["vendor_request_status"] = "REQUEST_SENT" if event == "REQUEST_SENT" \
            else "FOLLOWED_UP"
        block["vendor_request_sent_date"] = block.get("vendor_request_sent_date") \
            or day.isoformat()
        block["vendor_followup_due"] = (day + timedelta(
            days=VENDOR_FOLLOWUP_DAYS)).isoformat()
        block["scout_state"] = S_AWAITING_VENDOR
        block["human_action_required"] = None
    elif event == "VENDOR_RESPONSE":
        block["vendor_request_status"] = "RESPONDED"
        block["human_action_required"] = "Read the response; record a quote or " \
                                         "deliver the sample to the inbox"
    elif event == "QUOTE_RECEIVED":
        block["vendor_request_status"] = "QUOTED"
        block["price"] = price or block.get("price")
        block["price_verified_date"] = day.isoformat()
    elif event == "SAMPLE_DELIVERED":
        block["vendor_request_status"] = "SAMPLE_DELIVERED"
        block["scout_state"] = S_SAMPLE_RECEIVED
        block["human_action_required"] = "Place the files in %s" % (
            samples_root() / opportunity_id / "inbox")
    _write(mem, opportunity_id, title=row["title"],
           asset_class=row.get("asset_class"), block=block, prior=row)
    mem.event("DATA_SCOUT_VENDOR_EVENT", subject=opportunity_id,
              detail={"event": event, "on": day.isoformat()})
    return {"recorded": True, "duplicate": False, "opportunity_id": opportunity_id,
            "scout_state": block["scout_state"],
            "vendor_request_status": block["vendor_request_status"]}


def status(mem) -> dict:
    """Read-only summary of the scout's registry rows."""
    rows = _scout_rows(mem)
    by_state: dict = {}
    for oid, r in rows.items():
        by_state.setdefault(r["detail"]["scout"].get("scout_state"), []).append(oid)
    awaiting = sorted(by_state.get(S_AWAITING_VENDOR, []))
    return {"owner": OWNER, "registry_owner": REGISTRY_OWNER,
            "n_tracked": len(rows),
            "by_scout_state": {k: len(v) for k, v in sorted(by_state.items())},
            "awaiting_vendor": [
                {"opportunity_id": oid,
                 "vendor_request_status": rows[oid]["detail"]["scout"].get(
                     "vendor_request_status"),
                 "sent_date": rows[oid]["detail"]["scout"].get(
                     "vendor_request_sent_date")} for oid in awaiting],
            "samples_ready_to_test": sorted(by_state.get(S_SAMPLE_READY, [])),
            "lanes": {oid: r["detail"]["scout"].get("lane_status")
                      for oid, r in rows.items() if r["detail"]["scout"].get("lane")},
            "last_run": mem.get_meta(META_RUN)}
