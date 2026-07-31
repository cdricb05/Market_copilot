"""
alpha_agent.source_exhaustion — Stage 8 source & entitlement exhaustion.

A machine-readable registry of EVERY relevant dataset the user owns or can
legally access for free, plus the read-only probes that classify each one and
the honest data-completeness contract that forbids an unqualified "no alpha".

This module is the WS1..WS7 + WS10 core:

  * A canonical SOURCE CATALOG (owned: Norgate capabilities + EODHD endpoints +
    local extracts on D:; free official: SEC EDGAR, FRED, BLS, BEA, Treasury,
    FINRA, exchange reference) with every required per-endpoint metadata field.
  * READ-ONLY probing that reuses the EXISTING Stage-2 collectors' ``audit()``
    path (``ingestion.run_ingestion(mode="audit")`` — minimum-network,
    ``archive=False``) and maps each collector's health/entitlement state onto
    the eight required accessibility classifications.
  * Point-in-time guards (after-close filings effective no earlier than the next
    valid session) and a prospective-collection boundary primitive (first
    snapshot date is a hard PIT floor; never backfill before it).
  * A coverage matrix (by field / symbol / date) and coverage-repair job specs.
  * The never-idle SEED PLANNER that turns the registry into a durable work-list.
  * The honest data-completeness contract (five graded conclusion levels; an
    unqualified ``NO_ALPHA`` conclusion is rejected).

Safety: read-only. No operational-ledger write, no order/fill/signal/decision,
no model promotion, no prediction call, no PostgreSQL. Probing uses only the
collectors' minimal audit path. Credentials are read from the environment by the
collectors and never persisted or logged here.
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from . import source_contracts as sc

STAGE = "8"
SCHEMA_VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# The eight required accessibility classifications (WS1).
# --------------------------------------------------------------------------- #
ACCESSIBLE_NOW = "ACCESSIBLE_NOW"
ACCESSIBLE_AFTER_REPAIR = "ACCESSIBLE_AFTER_REPAIR"
PROSPECTIVE_ONLY = "PROSPECTIVE_ONLY"
# A prospective family whose PRODUCTION forward-snapshot collector is wired and
# operational (an immutable daily vintage is being persisted). Distinct from
# PROSPECTIVE_ONLY, which merely identifies a snapshot-lookahead family with no
# collector yet.
PROSPECTIVE_COLLECTION_ACTIVE = "PROSPECTIVE_COLLECTION_ACTIVE"
PAID_NOT_OWNED = "PAID_NOT_OWNED"
LEGALLY_RESTRICTED = "LEGALLY_RESTRICTED"
INVALID_CREDENTIAL = "INVALID_CREDENTIAL"
PROVIDER_OUTAGE = "PROVIDER_OUTAGE"
NOT_RELEVANT = "NOT_RELEVANT"

CLASSIFICATIONS = (
    ACCESSIBLE_NOW, ACCESSIBLE_AFTER_REPAIR, PROSPECTIVE_ONLY,
    PROSPECTIVE_COLLECTION_ACTIVE, PAID_NOT_OWNED, LEGALLY_RESTRICTED,
    INVALID_CREDENTIAL, PROVIDER_OUTAGE, NOT_RELEVANT,
)

# Prospective family pins: an entitlement probe can prove the collector is
# reachable/entitled, but it CANNOT see the snapshot-vintage lookahead limitation.
# So a prospective pin is honoured even over an ENTITLED probe (an ENTITLED probe
# means the forward-snapshot collector is OPERATIONAL, i.e. ACTIVE), and is only
# overridden when the probe proves the lane is genuinely unreachable.
_PROSPECTIVE_PINS = (PROSPECTIVE_ONLY, PROSPECTIVE_COLLECTION_ACTIVE)

# Map the existing collector source-health / entitlement enums (source_contracts)
# onto the eight Stage-8 classifications. Missing/unknown falls through to
# ACCESSIBLE_AFTER_REPAIR (something to fix, never a silent "no data").
_HEALTH_TO_CLASSIFICATION = {
    sc.SH_HEALTHY: ACCESSIBLE_NOW,
    sc.SH_DEGRADED: PROVIDER_OUTAGE,
    sc.SH_RATE_LIMITED: PROVIDER_OUTAGE,
    sc.SH_FAILED: PROVIDER_OUTAGE,
    sc.SH_BLOCKED_CREDENTIAL: INVALID_CREDENTIAL,
    sc.SH_BLOCKED_ENTITLEMENT: PAID_NOT_OWNED,
    sc.SH_BLOCKED_LICENSE: LEGALLY_RESTRICTED,
    sc.SH_BLOCKED_CONFIGURATION: ACCESSIBLE_AFTER_REPAIR,
    sc.SH_NOT_CONFIGURED: ACCESSIBLE_AFTER_REPAIR,
    sc.SH_NOT_RUN: NOT_RELEVANT,
}
_ENTITLEMENT_TO_CLASSIFICATION = {
    sc.ENT_ENTITLED: ACCESSIBLE_NOW,
    sc.ENT_NOT_ENTITLED: PAID_NOT_OWNED,
    sc.ENT_AUTH_FAILED: INVALID_CREDENTIAL,
    sc.ENT_RATE_LIMITED: PROVIDER_OUTAGE,
    sc.ENT_UNKNOWN: ACCESSIBLE_AFTER_REPAIR,
}

# --------------------------------------------------------------------------- #
# Honest data-completeness contract (WS10).
# --------------------------------------------------------------------------- #
EXHAUSTIVE_WITH_ACCESSIBLE_DATA = "EXHAUSTIVE_WITH_ACCESSIBLE_DATA"
PROVISIONAL_COVERAGE_INCOMPLETE = "PROVISIONAL_COVERAGE_INCOMPLETE"
PROSPECTIVE_EVIDENCE_REQUIRED = "PROSPECTIVE_EVIDENCE_REQUIRED"
PREMIUM_DATA_MATERIAL = "PREMIUM_DATA_MATERIAL"
REJECTED_WITH_STRONG_EVIDENCE = "REJECTED_WITH_STRONG_EVIDENCE"

CONCLUSION_LEVELS = (
    EXHAUSTIVE_WITH_ACCESSIBLE_DATA, PROVISIONAL_COVERAGE_INCOMPLETE,
    PROSPECTIVE_EVIDENCE_REQUIRED, PREMIUM_DATA_MATERIAL,
    REJECTED_WITH_STRONG_EVIDENCE,
)
_FORBIDDEN_CONCLUSION = "NO_ALPHA"

# Required evidence keys before ANY signal-family conclusion may be drawn (WS10).
COMPLETENESS_EVIDENCE_KEYS = (
    "relevant_sources_inspected", "owned_entitlements_probed",
    "free_sources_attempted", "fields_acquired", "fields_unavailable",
    "historical_coverage", "universe_coverage", "point_in_time_quality",
    "experiment_sample_size", "remaining_inaccessible_information",
    "inaccessible_fields_could_change_conclusion",
)

# --------------------------------------------------------------------------- #
# Per-endpoint registry-entry field contract (WS1). Every source/endpoint the
# agent knows about carries exactly these keys.
# --------------------------------------------------------------------------- #
REGISTRY_ENTRY_FIELDS = (
    "provider", "endpoint", "information_family", "entitlement_status",
    "authentication_status", "legal_constraints", "rate_limits",
    "historical_start", "historical_end", "point_in_time_suitability",
    "universe_coverage", "field_coverage", "acquisition_status",
    "local_storage_location", "last_successful_acquisition",
    "next_acquisition_action", "failure_classification", "retry_policy",
    "classification", "collector_source_id", "cost",
)


def _entry(**kw) -> dict:
    """Build a registry entry with every contract field present (unknowns None)."""
    e = {k: None for k in REGISTRY_ENTRY_FIELDS}
    e.update(kw)
    unknown = set(kw) - set(REGISTRY_ENTRY_FIELDS)
    if unknown:
        raise ValueError("unknown registry field(s): %s" % ", ".join(
            sorted(unknown)))
    return e


# --------------------------------------------------------------------------- #
# Canonical source catalog. This is DATA, not a plan: every owned + free source
# family the agent must exhaust, each with a candidate collector (where one
# exists) and a catalog-level classification HINT that pins premium / prospective
# / legally-restricted families the probe cannot itself discover.
# --------------------------------------------------------------------------- #
def source_catalog() -> "list[dict]":
    D = "D:\\Stock_Prediction_app_data\\alpha_agent"
    cat: list[dict] = []

    # ---- OWNED: Norgate (local survivorship-safe vendor library) ---------- #
    for fam, note in (
        ("prices_adjusted", "adjusted daily OHLCV (total-return)"),
        ("prices_unadjusted", "unadjusted daily OHLCV"),
        ("dividends_splits_capital", "dividends / splits / capital events"),
        ("delisted_securities", "delisted / survivorship-safe universe"),
        ("index_membership", "historical S&P 500 / index constituents"),
        ("security_identity", "symbol & security identity history, exchange"),
        ("classifications", "sector / industry / GICS-style classification"),
    ):
        cat.append(_entry(
            provider="Norgate Data", endpoint=fam, information_family=fam,
            legal_constraints="licensed vendor; local use only",
            point_in_time_suitability="survivorship-safe (as-traded universe)",
            universe_coverage="US equities (Norgate US Stocks)",
            local_storage_location=D + "\\backfill",
            collector_source_id="norgate_local", cost="owned",
            retry_policy="local vendor; retry on NDU-not-running",
            classification=None))  # resolved by probe

    # ---- OWNED: EODHD (HTTP; entitlement varies per endpoint) -------------- #
    for fam, ep in (
        ("eod", "eod/{symbol}"), ("dividends", "div/{symbol}"),
        ("splits", "splits/{symbol}"), ("fundamentals", "fundamentals/{symbol}"),
        ("earnings", "calendar/earnings"),
        ("insider", "insider-transactions"), ("news", "news"),
    ):
        cat.append(_entry(
            provider="EODHD", endpoint=ep, information_family=fam,
            authentication_status="EODHD_API_KEY (env)",
            legal_constraints="licensed vendor; local use only",
            point_in_time_suitability=("snapshot for fundamentals (PIT-lookahead"
                                       " risk) — bars/events PIT-usable"),
            universe_coverage="global equities (subscription-dependent)",
            local_storage_location=D + "\\ingestion",
            collector_source_id="eodhd", cost="owned",
            retry_policy="bounded backoff; per-family entitlement probe",
            classification=None))

    # ---- FREE OFFICIAL: SEC EDGAR ----------------------------------------- #
    cat.append(_entry(
        provider="SEC EDGAR", endpoint="files/company_tickers.json",
        information_family="ticker_cik_map",
        authentication_status="declared User-Agent (git contact email)",
        legal_constraints="public; SEC fair-access UA + rate policy",
        point_in_time_suitability="reference map (current)",
        universe_coverage="all SEC filers",
        local_storage_location=D + "\\ingestion",
        collector_source_id="sec_edgar", cost="free",
        retry_policy="conservative <10 req/s; backoff", classification=None))
    cat.append(_entry(
        provider="SEC EDGAR", endpoint="Archives/edgar/daily-index",
        information_family="filing_index",
        authentication_status="declared User-Agent",
        legal_constraints="public; SEC fair-access policy",
        point_in_time_suitability="filing acceptance timestamp = PIT anchor",
        universe_coverage="all SEC filers",
        local_storage_location=D + "\\ingestion",
        collector_source_id="sec_edgar", cost="free",
        retry_policy="conservative; backoff", classification=None))
    # SEC data.sec.gov structured lanes — NOW IMPLEMENTED via the sec_edgar
    # collector: submissions carries the acceptanceDateTime PIT anchor for EVERY
    # form (incl. Form 4 + 8-K); companyfacts/companyconcept give TRUE point-in-
    # time XBRL facts (each fact carries its filed date). Transaction-level Form 4
    # XML parsing and 8-K Item 2.02 / EX-99 exhibit extraction remain a bounded
    # out-of-band enhancement (the filing event + PIT acceptance IS captured).
    for fam, ep, note in (
        ("companyfacts", "data.sec.gov/api/xbrl/companyfacts", None),
        ("company_concept", "data.sec.gov/api/xbrl/companyconcept", None),
        ("submissions", "data.sec.gov/submissions", None),
        ("form4_insider", "data.sec.gov/submissions (Form 4 + acceptance PIT)",
         "transaction-level Form 4 XML parse is a bounded out-of-band enhancement"),
        ("form8k_earnings", "data.sec.gov/submissions (8-K + acceptance PIT)",
         "8-K Item 2.02 / EX-99 exhibit extraction is a bounded out-of-band "
         "enhancement"),
    ):
        cat.append(_entry(
            provider="SEC EDGAR", endpoint=ep, information_family=fam,
            authentication_status="declared User-Agent",
            legal_constraints="public; SEC fair-access policy",
            point_in_time_suitability=("acceptance timestamp = PIT anchor; XBRL "
                                       "facts carry filed date (true PIT)"),
            universe_coverage="all SEC filers",
            local_storage_location=D + "\\ingestion",
            collector_source_id="sec_edgar", cost="free",
            next_acquisition_action=note,
            retry_policy="conservative; backoff",
            classification=None))
    # WS4: THREE SEPARATE bulk lanes — never conflated. (1) SEC_FULL_INDEX is the
    # bounded quarterly master.idx, acquired in-cycle (operational, not "bulk
    # facts"). (2) SEC_COMPANYFACTS_BULK and (3) SEC_SUBMISSIONS_BULK are the
    # multi-GB official archive zips: HEAD-probed for their real size; a resumable
    # ranged downloader + checkpoint + checksum is implemented and runs when the
    # archive fits the bounded cap, else the lane is a PRECISE evidence-based
    # blocker (measured Content-Length) — the per-CIK companyfacts/submissions
    # APIs already supply the same data PIT-correctly in-cycle.
    cat.append(_entry(
        provider="SEC EDGAR", endpoint="Archives/edgar/full-index (quarterly)",
        information_family="sec_full_index",
        authentication_status="declared User-Agent",
        legal_constraints="public; SEC fair-access policy",
        point_in_time_suitability=("filing date PIT; acceptance via submissions "
                                   "lane"),
        universe_coverage="all SEC filers",
        local_storage_location=D + "\\ingestion",
        collector_source_id="sec_edgar", cost="free",
        next_acquisition_action="bounded quarterly full-index acquired in-cycle",
        retry_policy="conservative; backoff",
        classification=None))
    for fam, ep in (
        ("sec_companyfacts_bulk",
         "Archives/edgar/daily-index/xbrl/companyfacts.zip"),
        ("sec_submissions_bulk",
         "Archives/edgar/daily-index/bulkdata/submissions.zip"),
    ):
        cat.append(_entry(
            provider="SEC EDGAR", endpoint=ep, information_family=fam,
            authentication_status="declared User-Agent",
            legal_constraints="public; SEC fair-access policy",
            point_in_time_suitability=("XBRL facts carry filed date; acceptance "
                                       "via submissions"),
            universe_coverage="all SEC filers",
            local_storage_location=D + "\\ingestion\\bulk\\sec_edgar",
            collector_source_id=None, cost="free",
            acquisition_status="BULK_HEAD_PROBED",
            next_acquisition_action=("HEAD-probed each cycle; resumable ranged "
                                     "download + checkpoint + SHA-256 runs when "
                                     "the archive fits the bounded raw-object cap, "
                                     "else out-of-band (per-CIK API covers it "
                                     "PIT-correctly in-cycle)"),
            failure_classification="MULTI_GB_ARCHIVE_EXCEEDS_BOUNDED_CAP",
            retry_policy="conservative; backoff",
            classification=ACCESSIBLE_AFTER_REPAIR))

    # ---- FREE OFFICIAL: macro (FRED / BLS / BEA / Treasury) --------------- #
    cat.append(_entry(
        provider="FRED / ALFRED", endpoint="fred/series/observations",
        information_family="macro_series",
        authentication_status="FRED_API_KEY (env)",
        legal_constraints="public API; attribution",
        point_in_time_suitability="ALFRED vintages = PIT-correct",
        universe_coverage="US macro series", local_storage_location=D +
        "\\ingestion", collector_source_id="fred_alfred", cost="free",
        retry_policy="bounded backoff", classification=None))
    # BLS + US Treasury — NOW IMPLEMENTED (public, credential-free collectors).
    for prov, ep, fam, sid in (
        ("BLS", "api.bls.gov/publicAPI/v2/timeseries", "labor_inflation", "bls"),
        ("US Treasury", "api.fiscaldata.treasury.gov", "treasury_rates",
         "us_treasury"),
    ):
        cat.append(_entry(
            provider=prov, endpoint=ep, information_family=fam,
            legal_constraints="public API", cost="free",
            point_in_time_suitability=("release-dated series (reference period; "
                                       "availability lag documented per record)"),
            universe_coverage="US macro",
            local_storage_location=D + "\\ingestion",
            collector_source_id=sid,
            retry_policy="bounded backoff", classification=None))
    # BEA — collector IMPLEMENTED but credential-gated by a FREE registered
    # UserID that is not provisioned in this environment (honest, resolvable
    # hard blocker; never a fabricated completion).
    cat.append(_entry(
        provider="BEA", endpoint="apps.bea.gov/api",
        information_family="national_accounts",
        authentication_status="BEA_API_KEY (free registration; env)",
        legal_constraints="public API", cost="free",
        point_in_time_suitability="release-dated series",
        universe_coverage="US macro", local_storage_location=D + "\\ingestion",
        collector_source_id="bea",
        acquisition_status="BLOCKED_CREDENTIAL",
        next_acquisition_action=("register a FREE BEA UserID at bea.gov/API/signup "
                                 "and set BEA_API_KEY; collector already "
                                 "implemented + wired"),
        failure_classification="CREDENTIAL_REQUIRED_FREE_REGISTRATION",
        retry_policy="bounded backoff",
        classification=ACCESSIBLE_AFTER_REPAIR))

    # ---- FREE OFFICIAL: FINRA short-sale / equity data -------------------- #
    cat.append(_entry(
        provider="FINRA", endpoint="cdn.finra.org short-sale volume",
        information_family="short_sale_volume",
        legal_constraints="public files", cost="free",
        point_in_time_suitability="daily file date = PIT anchor",
        universe_coverage="US equities",
        local_storage_location=D + "\\ingestion",
        collector_source_id="finra",
        retry_policy="bounded backoff; 403/404 = expected gap",
        classification=None))

    # ---- FREE OFFICIAL: exchange symbol / calendar / corporate reference --- #
    cat.append(_entry(
        provider="Nasdaq Trader", endpoint="nasdaqtrader.com symbol directory",
        information_family="symbol_reference",
        legal_constraints="public files", cost="free",
        point_in_time_suitability="reference (current)",
        universe_coverage="US listed symbols",
        local_storage_location=D + "\\ingestion",
        collector_source_id="nasdaq_trader",
        retry_policy="bounded backoff", classification=None))

    # ---- PROSPECTIVE-ONLY families ---------------------------------------- #
    # ENTITLEMENT CORRECTION (verified by live EODHD probe 2026-07-31): these
    # analyst families ARE entitled and present inside EODHD /fundamentals
    # (AnalystRatings, Earnings.Trend incl. earningsEstimateNumberOfAnalysts +
    # epsRevisionsUp/DownLast7/30days, Highlights.WallStreetTargetPrice; ~39
    # historical trend rows). They are NOT "no owned history". BUT the vendor
    # serves them as a CURRENT SNAPSHOT (no per-day vintages), so using them in a
    # backtest is PIT-lookahead-unsafe. The real, leakage-safe lane is therefore
    # a forward daily snapshot from a hard PIT floor — which is exactly
    # PROSPECTIVE_ONLY. Classification unchanged; the reason is corrected.
    # WS1: the production forward-snapshot collector (``eodhd_analyst``) is now
    # WIRED and operational — it persists ONE IMMUTABLE DAILY VINTAGE per security
    # and snapshot timestamp (availability = capture date; never backfilled before
    # the first snapshot). The families stay prospective (snapshot-vintage cannot
    # be used PIT-safely in a retrospective backtest) but are now
    # PROSPECTIVE_COLLECTION_ACTIVE, not merely PROSPECTIVE_ONLY.
    for fam, why in (
        ("analyst_estimate_revisions",
         "ENTITLED as EODHD fundamentals SNAPSHOT (Earnings.Trend "
         "epsRevisionsUp/Down*); production eodhd_analyst collector writes the "
         "immutable forward daily vintage that builds the real PIT history"),
        ("price_targets",
         "ENTITLED as EODHD fundamentals SNAPSHOT (WallStreetTargetPrice / "
         "AnalystRatings.TargetPrice); eodhd_analyst vintage is the real forward "
         "PIT lane"),
        ("estimate_counts",
         "ENTITLED as EODHD fundamentals SNAPSHOT "
         "(earningsEstimateNumberOfAnalysts); eodhd_analyst vintage is the real "
         "forward PIT lane"),
    ):
        cat.append(_entry(
            provider="EODHD (analyst fundamentals — forward daily vintage)",
            endpoint="fundamentals/{symbol} [AnalystRatings, Earnings.Trend]",
            information_family=fam,
            authentication_status="EODHD_API_KEY (env)",
            legal_constraints="collect forward from first snapshot; local use only",
            point_in_time_suitability="first snapshot date is a hard PIT floor; "
                                      "availability = daily capture date",
            universe_coverage="grows prospectively", cost="free_prospective",
            local_storage_location=D + "\\ingestion\\vintages\\eodhd_analyst",
            collector_source_id="eodhd_analyst",
            acquisition_status="PROSPECTIVE_COLLECTION_ACTIVE",
            next_acquisition_action="daily AlphaAgent-Collect refreshes the "
                                    "immutable vintage; never backfill",
            failure_classification=why, retry_policy="daily snapshot",
            classification=PROSPECTIVE_COLLECTION_ACTIVE))

    return cat


# --------------------------------------------------------------------------- #
# Classification from a live probe (WS1..WS5).
# --------------------------------------------------------------------------- #
def classify_from_health(overall_state: Optional[str],
                         *, entitlement_states: Optional["list[str]"] = None,
                         catalog_hint: Optional[str] = None) -> str:
    """Map a collector ``audit()`` health/entitlement result onto one of the
    eight classifications. A catalog hint that pins a premium / prospective /
    legally-restricted family is honoured only when the probe itself is
    inconclusive (so a real ENTITLED probe can upgrade a hint, and a real
    NOT_ENTITLED probe can downgrade one)."""
    # A definitive entitlement verdict wins.
    if entitlement_states:
        mapped = [_ENTITLEMENT_TO_CLASSIFICATION.get(s) for s in
                  entitlement_states]
        mapped = [m for m in mapped if m is not None]
        if any(m == ACCESSIBLE_NOW for m in mapped):
            probe_class = ACCESSIBLE_NOW
        elif mapped and all(m == PAID_NOT_OWNED for m in mapped):
            probe_class = PAID_NOT_OWNED
        elif any(m == INVALID_CREDENTIAL for m in mapped):
            probe_class = INVALID_CREDENTIAL
        else:
            probe_class = None
    else:
        probe_class = None
    if probe_class is None:
        probe_class = _HEALTH_TO_CLASSIFICATION.get(overall_state)
    # A prospective pin (snapshot-vintage lookahead) is a property the probe
    # cannot see. Keep the pin even over an ENTITLED/HEALTHY probe (which merely
    # proves the forward-snapshot collector is operational); only a genuinely
    # unreachable lane (invalid credential / not entitled / outage) overrides it.
    if catalog_hint in _PROSPECTIVE_PINS:
        if probe_class in (INVALID_CREDENTIAL, PAID_NOT_OWNED, PROVIDER_OUTAGE):
            return probe_class
        return catalog_hint
    # Prefer a decisive probe; else a catalog hint; else "needs repair".
    if probe_class in (ACCESSIBLE_NOW, PAID_NOT_OWNED, INVALID_CREDENTIAL,
                       LEGALLY_RESTRICTED):
        return probe_class
    if catalog_hint in CLASSIFICATIONS:
        return catalog_hint
    return probe_class or ACCESSIBLE_AFTER_REPAIR


def probe_sources(stage2_config: dict, *, output_root: Optional[str] = None,
                  env: Optional[dict] = None, contact_email: Optional[str] = None,
                  transport: Optional[Callable] = None) -> dict:
    """Run the EXISTING Stage-2 collectors in READ-ONLY ``audit`` mode and return
    ``{source_id: health_dict}``. Minimum-network (each collector's ``audit()``
    uses ``fetch(archive=False)``); no raw objects are persisted for the probe
    beyond the audit state rows the ingestion layer writes under ``output_root``
    (a temp dir by default, so the real ingestion store is untouched). Never
    writes an operational ledger."""
    from . import ingestion  # lazy: heavy import only when probing
    out = output_root
    tmp = None
    if out is None:
        tmp = tempfile.mkdtemp(prefix="stage8_audit_")
        out = tmp
    try:
        result = ingestion.run_ingestion(
            config=stage2_config, output_root=out, mode="audit",
            env=dict(os.environ) if env is None else env,
            contact_email=contact_email, transport=transport)
    except Exception as exc:  # noqa: BLE001 - probing must never raise upward
        return {"status": "PROBE_ERROR", "reason": "%s: %s" % (
            type(exc).__name__, str(exc)[:200])}
    health: dict = {}
    # run_ingestion returns a flat {source_id: overall_state} under
    # "source_states"; the per-family entitlement detail is written to
    # entitlement_audit.json in the run directory.
    for sid, overall in (result.get("source_states") or {}).items():
        health[sid] = {"overall_state": overall, "entitlement_states": [],
                       "entitlement_summary": None, "credential_present": None,
                       "records_collected": None, "last_success_at": None}
    run_dir = result.get("run_dir")
    if run_dir:
        try:
            ea = json.loads((Path(run_dir) / "entitlement_audit.json")
                            .read_text(encoding="utf-8"))
            for sid, blk in (ea.get("sources") or {}).items():
                if sid in health and isinstance(blk, dict):
                    health[sid]["entitlement_states"] = [
                        e.get("state") for e in (blk.get("entitlements") or [])
                        if isinstance(e, dict)]
                    health[sid]["entitlement_summary"] = blk.get("summary")
                    health[sid]["credential_present"] = blk.get(
                        "credential_present")
        except Exception:  # noqa: BLE001 - enrichment is best-effort
            pass
    # Defensive fallback to a nested {source_id: {"health": ...}} form.
    if not health:
        for sid, res in (result.get("sources") or {}).items():
            h = (res or {}).get("health") or {}
            health[sid] = {
                "overall_state": h.get("overall_state"),
                "entitlement_summary": h.get("entitlement_summary"),
                "credential_present": h.get("credential_present"),
                "entitlement_states": [
                    e.get("state") for e in (res or {}).get("entitlements") or []
                    if isinstance(e, dict)],
                "records_collected": h.get("records_collected"),
                "last_success_at": h.get("last_success_at")}
    return {"status": result.get("status", "OK"), "health": health}


def build_registry_snapshot(*, catalog: Optional["list[dict]"] = None,
                            probe: Optional[dict] = None,
                            generated_at: Optional[str] = None) -> dict:
    """Merge the catalog with live probe results into the machine-readable
    source registry. Each entry's ``classification`` and ``entitlement_status``
    are set from the probe when a collector exists; catalog hints stand
    otherwise. Returns a snapshot dict (JSON-serialisable)."""
    catalog = catalog if catalog is not None else source_catalog()
    probe_health = (probe or {}).get("health") or {}
    entries = []
    tally = {c: 0 for c in CLASSIFICATIONS}
    for base in catalog:
        e = dict(base)
        sid = e.get("collector_source_id")
        if sid and sid in probe_health:
            h = probe_health[sid]
            e["entitlement_status"] = h.get("entitlement_summary")
            e["authentication_status"] = (
                e.get("authentication_status")
                or ("credential present" if h.get("credential_present")
                    else e.get("authentication_status")))
            e["last_successful_acquisition"] = h.get("last_success_at")
            cls = classify_from_health(
                h.get("overall_state"),
                entitlement_states=h.get("entitlement_states"),
                catalog_hint=e.get("classification"))
            e["classification"] = cls
            if cls == ACCESSIBLE_NOW:
                # A live ENTITLED/HEALTHY probe upgrades the lane: clear ANY stale
                # catalog-hinted BLOCKED_* acquisition status (e.g. BEA once its
                # free UserID is provisioned) so the registry never shows a
                # blocked acquisition for a now-accessible source.
                e["acquisition_status"] = "ACCESSIBLE"
            elif e.get("acquisition_status") is None:
                e["acquisition_status"] = "BLOCKED_%s" % cls
        else:
            # No live probe: keep the catalog hint; default unresolved to repair.
            if e.get("classification") is None:
                e["classification"] = ACCESSIBLE_AFTER_REPAIR
        tally[e["classification"]] = tally.get(e["classification"], 0) + 1
        entries.append(e)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at or _utc_now_iso(),
        "classification_tally": tally,
        "source_count": len(entries),
        "sources": entries,
        "probe_status": (probe or {}).get("status"),
        "classifications": list(CLASSIFICATIONS),
    }


def write_registry_snapshot(snapshot: dict, path: str | Path) -> str:
    """Atomically persist the registry snapshot (temp + os.replace)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(snapshot, fh, sort_keys=True, indent=2)
        os.replace(tmp, str(p))
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return str(p)


# --------------------------------------------------------------------------- #
# Point-in-time guards (WS4/WS5) and prospective boundary (WS6).
# --------------------------------------------------------------------------- #
def _as_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    return date.fromisoformat(str(value)[:10])


def next_valid_session(d: Any, *, holidays: Optional["set[str]"] = None) -> str:
    """The next valid US-equity trading session on/after ``d`` is computed by
    skipping weekends and any provided holiday dates. Returns an ISO date."""
    holidays = holidays or set()
    cur = _as_date(d)
    while cur.weekday() >= 5 or cur.isoformat() in holidays:
        cur = cur + timedelta(days=1)
    return cur.isoformat()


def effective_session_for_filing(acceptance_iso: str, *,
                                  after_hour_local: int = 16,
                                  holidays: Optional["set[str]"] = None) -> str:
    """The earliest session a filing may influence. A filing accepted AT/AFTER
    the local close (default 16:00) is effective no earlier than the NEXT valid
    session; an intraday filing is effective the same session if valid, else the
    next. Prevents after-close information leaking into the same-day signal."""
    dt = datetime.fromisoformat(acceptance_iso.replace("Z", "+00:00")) \
        if "T" in acceptance_iso else datetime.fromisoformat(
            acceptance_iso + "T00:00:00")
    base = dt.date()
    if dt.hour >= after_hour_local:
        base = base + timedelta(days=1)
    return next_valid_session(base, holidays=holidays)


def prospective_boundary(field: str, first_snapshot_date: str) -> dict:
    """Record the hard PIT floor for a prospectively-collected field. History
    before ``first_snapshot_date`` must NEVER be backfilled with current
    values."""
    return {"field": field, "first_snapshot_date": first_snapshot_date,
            "hard_pit_floor": first_snapshot_date,
            "backfill_before_floor_allowed": False}


def violates_prospective_floor(boundary: dict, period_date: str) -> bool:
    """True if ``period_date`` precedes the prospective PIT floor (a lookahead
    violation that must be rejected)."""
    return _as_date(period_date) < _as_date(boundary["first_snapshot_date"])


# --------------------------------------------------------------------------- #
# Coverage matrix + repair specs (WS2/WS7).
# --------------------------------------------------------------------------- #
def coverage_matrix(records: "list[dict]") -> dict:
    """Coverage counts by field, symbol and date over a list of normalized
    records (each a dict with optional ``ticker``/``field``/``date`` keys)."""
    by_field: dict[str, int] = {}
    by_symbol: dict[str, int] = {}
    by_date: dict[str, int] = {}
    for r in records:
        f = r.get("field") or r.get("information_family")
        s = r.get("ticker") or r.get("symbol")
        d = (r.get("date") or r.get("effective_at") or "")[:10] or None
        if f:
            by_field[f] = by_field.get(f, 0) + 1
        if s:
            by_symbol[s] = by_symbol.get(s, 0) + 1
        if d:
            by_date[d] = by_date.get(d, 0) + 1
    return {"by_field": by_field, "by_symbol": by_symbol, "by_date": by_date,
            "field_count": len(by_field), "symbol_count": len(by_symbol),
            "date_count": len(by_date)}


def coverage_repair_specs(snapshot: dict) -> "list[dict]":
    """Emit COVERAGE_REPAIR / ACCESSIBLE_AFTER_REPAIR job specs from the registry
    (each blocked-but-fixable source becomes a durable repair job)."""
    from . import autonomous_research as ar
    specs = []
    for e in snapshot.get("sources", []):
        if e.get("classification") == ACCESSIBLE_AFTER_REPAIR:
            specs.append({
                "category": ar.CAT_COVERAGE_REPAIR,
                "lane": "%s.%s" % (e.get("collector_source_id")
                                   or (e.get("provider") or "src").lower(),
                                   e.get("information_family") or "family"),
                "payload": {"provider": e.get("provider"),
                            "endpoint": e.get("endpoint"),
                            "action": e.get("next_acquisition_action")},
                "priority": 3, "origin": "registry"})
    return specs


# --------------------------------------------------------------------------- #
# Never-idle SEED PLANNER (WS8 integration).
# --------------------------------------------------------------------------- #
# Experiment feature families the campaign should keep exploring (WS9). These
# are LANES, not promotions; the durable queue turns them into EXPERIMENT jobs.
PRICE_FAMILIES = ("cross_sectional_momentum", "residual_momentum",
                  "short_term_reversal", "medium_term_reversal", "volatility",
                  "downside_volatility", "liquidity", "volume",
                  "gap_event_reaction", "sector_relative_strength")
FUNDAMENTAL_FAMILIES = ("value", "profitability", "quality", "accruals",
                        "operating_efficiency", "balance_sheet_strength",
                        "asset_growth", "capex_growth", "rnd_intensity",
                        "fundamental_acceleration", "cash_flow_quality")
EVENT_FAMILIES = ("earnings_surprise", "post_earnings_drift", "filing_events",
                  "insider_purchases", "insider_sales", "corporate_actions")
COMBINATION_FAMILIES = ("quality_value", "quality_momentum", "value_momentum",
                        "earnings_momentum", "sector_neutral_composite",
                        "regime_conditioned_composite")


def default_seed_jobs(snapshot: Optional[dict] = None) -> "list[dict]":
    """Turn the source registry + feature families into a durable, never-empty
    work-list spanning all Stage-8 job categories. Idempotent enqueue means this
    can be called every replenishment without creating duplicates."""
    from . import autonomous_research as ar
    snapshot = snapshot or {}
    specs: list[dict] = []

    # (1) source discovery + entitlement re-probe (always at least these two).
    specs.append({"category": ar.CAT_SOURCE_DISCOVERY, "lane": "registry.rescan",
                  "payload": {"scope": "all"}, "priority": 5,
                  "origin": "planner"})
    specs.append({"category": ar.CAT_ENTITLEMENT_PROBE,
                  "lane": "registry.entitlements", "payload": {"scope": "all"},
                  "priority": 5, "origin": "planner"})

    # (2) acquisition lanes for every ACCESSIBLE_NOW source.
    for e in snapshot.get("sources", []):
        cls = e.get("classification")
        sid = e.get("collector_source_id") or (e.get("provider") or "src")
        lane = "%s.%s" % (sid, e.get("information_family") or "family")
        if cls == ACCESSIBLE_NOW:
            specs.append({"category": ar.CAT_DATA_ACQUISITION, "lane": lane,
                          "payload": {"provider": e.get("provider"),
                                      "endpoint": e.get("endpoint"),
                                      "collector_source_id":
                                          e.get("collector_source_id")},
                          "priority": 4, "origin": "planner"})
        elif cls in (PROSPECTIVE_ONLY, PROSPECTIVE_COLLECTION_ACTIVE):
            specs.append({"category": ar.CAT_PROSPECTIVE_SNAPSHOT, "lane": lane,
                          "payload": {"field": e.get("information_family"),
                                      "collector_source_id":
                                          e.get("collector_source_id")},
                          "priority": 2, "origin": "planner"})

    # (3) coverage repair for every ACCESSIBLE_AFTER_REPAIR source.
    specs.extend(coverage_repair_specs(snapshot))

    # (4) validation of what has been acquired.
    specs.append({"category": ar.CAT_DATA_VALIDATION, "lane": "validation.pit",
                  "payload": {"check": "point_in_time"}, "priority": 3,
                  "origin": "planner"})

    # (5) experiment families (unexplored + refinements) — WS9.
    for fam in PRICE_FAMILIES:
        specs.append({"category": ar.CAT_EXPERIMENT, "lane": "experiment.price.%s"
                      % fam, "payload": {"family": fam, "kind": "price"},
                      "priority": 2, "origin": "planner"})
    for fam in FUNDAMENTAL_FAMILIES:
        specs.append({"category": ar.CAT_EXPERIMENT,
                      "lane": "experiment.fundamental.%s" % fam,
                      "payload": {"family": fam, "kind": "fundamental"},
                      "priority": 1, "origin": "planner"})
    for fam in EVENT_FAMILIES:
        specs.append({"category": ar.CAT_EXPERIMENT, "lane": "experiment.event.%s"
                      % fam, "payload": {"family": fam, "kind": "event"},
                      "priority": 1, "origin": "planner"})
    for fam in COMBINATION_FAMILIES:
        specs.append({"category": ar.CAT_SIGNAL_COMBINATION,
                      "lane": "combination.%s" % fam,
                      "payload": {"family": fam}, "priority": 1,
                      "origin": "planner"})

    # (6) hypothesis generation + robustness (keep the funnel fed).
    specs.append({"category": ar.CAT_HYPOTHESIS_GENERATION,
                  "lane": "hypothesis.generate", "payload": {}, "priority": 1,
                  "origin": "planner"})
    specs.append({"category": ar.CAT_ROBUSTNESS_TEST, "lane": "robustness.sweep",
                  "payload": {}, "priority": 1, "origin": "planner"})
    return specs


def make_planner(snapshot_provider: Callable[[], Optional[dict]]) -> Callable:
    """Adapt ``default_seed_jobs`` into an ``autonomous_research`` planner
    (a callable taking the queue and returning seed specs)."""
    def _planner(_queue) -> "list[dict]":
        return default_seed_jobs(snapshot_provider())
    return _planner


# --------------------------------------------------------------------------- #
# Honest data-completeness contract (WS10).
# --------------------------------------------------------------------------- #
def data_completeness_conclusion(evidence: dict) -> dict:
    """Grade a signal-family conclusion honestly. Requires the full WS10 evidence
    set and NEVER returns an unqualified ``NO_ALPHA``. Returns
    ``{level, reconciles, missing_evidence, rationale}`` where ``level`` is one
    of the five graded conclusion levels."""
    missing = [k for k in COMPLETENESS_EVIDENCE_KEYS if k not in evidence]
    if missing:
        return {"level": PROVISIONAL_COVERAGE_INCOMPLETE, "reconciles": False,
                "missing_evidence": missing,
                "rationale": "cannot conclude: required completeness evidence "
                             "missing (%s)" % ", ".join(missing)}
    could_change = bool(evidence.get(
        "inaccessible_fields_could_change_conclusion"))
    remaining = evidence.get("remaining_inaccessible_information")
    sample = int(evidence.get("experiment_sample_size") or 0)
    hist = float(evidence.get("historical_coverage") or 0.0)
    univ = float(evidence.get("universe_coverage") or 0.0)
    prospective = bool(evidence.get("prospective_only"))
    premium_material = could_change and bool(evidence.get(
        "material_field_is_premium"))

    if premium_material:
        level = PREMIUM_DATA_MATERIAL
    elif prospective:
        level = PROSPECTIVE_EVIDENCE_REQUIRED
    elif could_change or hist < 0.60 or univ < 0.60 or sample < 20:
        level = PROVISIONAL_COVERAGE_INCOMPLETE
    elif evidence.get("rejected") and not could_change:
        level = REJECTED_WITH_STRONG_EVIDENCE
    else:
        level = EXHAUSTIVE_WITH_ACCESSIBLE_DATA
    return {"level": level, "reconciles": True, "missing_evidence": [],
            "remaining_inaccessible_information": remaining,
            "rationale": _conclusion_rationale(level)}


def _conclusion_rationale(level: str) -> str:
    return {
        EXHAUSTIVE_WITH_ACCESSIBLE_DATA:
            "every accessible owned + free source was inspected and probed; "
            "no accessible field remains that could change the finding.",
        PROVISIONAL_COVERAGE_INCOMPLETE:
            "coverage is incomplete (history/universe/sample below threshold); "
            "the finding is provisional, not final.",
        PROSPECTIVE_EVIDENCE_REQUIRED:
            "the discriminating field has no owned/free history; prospective "
            "snapshots must accumulate before a verdict.",
        PREMIUM_DATA_MATERIAL:
            "an inaccessible PREMIUM field could materially change the finding; "
            "a targeted purchase is the honest next step.",
        REJECTED_WITH_STRONG_EVIDENCE:
            "rejected on strong, reconciled evidence over adequate coverage; no "
            "accessible field could revive it.",
    }.get(level, "")


def assert_no_unqualified_no_alpha(conclusion_text: str) -> None:
    """Raise if a conclusion string uses an unqualified ``NO_ALPHA`` token
    (a completeness-contract violation)."""
    tokens = [t.strip().upper() for t in
              conclusion_text.replace(",", " ").split()]
    if _FORBIDDEN_CONCLUSION in tokens and not any(
            lvl in conclusion_text for lvl in CONCLUSION_LEVELS):
        raise ValueError(
            "unqualified NO_ALPHA is forbidden; use a graded conclusion level")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
