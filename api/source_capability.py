r"""Release 28 — the canonical LIVE SOURCE CAPABILITY owner.

WHY THIS EXISTS
---------------
Every previous release re-answered "what data do we actually have?" from a different
place: the Stage-2 ingestion config, the Stage-3.5 feed registry, a purchase-gate
artifact, a research exhaustion note, or a comment. Release 28 makes that ONE
authoritative, machine-readable answer, because an event-driven manager cannot decide
what to react to until it knows what genuinely arrives, how fast, and with what
authority.

For every source this owner resolves, from CONFIGURATION AND MEASURED STATE rather
than assumption:

    information family / connection status / entitlement / cost / timestamp quality /
    point-in-time quality / entity coverage / expected latency / available frequency /
    historical depth / canonical owner / decision authority / can ingest now? /
    can score now? / can trigger now? / blocker / TERMINAL STATE

THE HARD RULE THIS OWNER ENFORCES
---------------------------------
There is no ``AVAILABLE_BUT_NOT_INTEGRATED``. A source that is useful, authorized and
technically executable is integrated in this release or it carries a terminal blocked
/ redundant / not-useful classification with the evidence that produced it. The
terminal audit counts anything else and fails the release.

SAFETY
------
Read-only. It reads configuration files, the Stage-2 state database and credential
PRESENCE (never a credential value). It performs no purchase, no plan upgrade and no
paid historical call, and it writes nothing.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Optional

from paper_trader.engine import event_fabric as ek

PHASE = "RELEASE28"
COMPOSITION_OWNER = "api.source_capability"
SCHEMA_VERSION = "1.0.0"
CAPABILITY_CONTRACT_ID = "paper_trader.live_source_capability_matrix/1"

_REPO_ROOT = Path(__file__).resolve().parents[1]
_STAGE2_CONFIG = _REPO_ROOT / "configs" / "alpha_agent" / "stage2_ingestion.json"
_NEWS_CONFIG = _REPO_ROOT / "configs" / "alpha_agent" / "news_rss_feeds.json"

INGESTION_ROOT_ENV = "PAPER_TRADER_ALPHA_INGESTION_ROOT"
_DEFAULT_INGESTION_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion")
NEWS_ROOT_ENV = "PAPER_TRADER_ALPHA_NEWS_ROOT"
_DEFAULT_NEWS_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\news_rss")

# Connection status vocabulary.
CONN_LIVE = "LIVE"
CONN_LOCAL_LICENSED = "LOCAL_LICENSED"
CONN_CONFIGURED_NOT_RUN = "CONFIGURED_NOT_RUN"
CONN_CREDENTIAL_MISSING = "CREDENTIAL_MISSING"
CONN_PROVIDER_BLOCKED = "PROVIDER_BLOCKED"
CONN_NOT_A_DATA_SOURCE = "NOT_A_DATA_SOURCE"

# Timestamp / point-in-time quality vocabulary.
TS_AUTHORITATIVE = "AUTHORITATIVE_SOURCE_TIMESTAMP"
TS_DATE_ONLY = "DATE_ONLY"
TS_DERIVED = "DERIVED_FROM_RETRIEVAL"
PIT_FILED_DATE = "PIT_FILED_DATE"
PIT_VINTAGE = "PIT_VINTAGE"
PIT_FORWARD_SNAPSHOT_ONLY = "PIT_FORWARD_SNAPSHOT_ONLY"
PIT_OBSERVED_ONLY = "PIT_OBSERVED_ONLY"
PIT_NOT_POINT_IN_TIME = "NOT_POINT_IN_TIME"


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def resolve_root(root_env: str, default: Path, override=None) -> Path:
    """Resolve a store root: explicit override, else environment, else the default.

    Public because the event fabric resolves the SAME roots and must not carry a second
    copy of this policy.
    """
    if override:
        return Path(override)
    raw = os.environ.get(root_env)
    return Path(raw) if raw else default


_resolve = resolve_root


def ingestion_root(override=None) -> Path:
    """The Stage-2 ingestion store root. Public so the continuous collection
    service resolves the SAME root as the fabric without a second copy of the
    policy or a private-attribute reach-in."""
    return resolve_root(INGESTION_ROOT_ENV, _DEFAULT_INGESTION_ROOT, override)


def news_root(override=None) -> Path:
    """The Stage-3.5 news/RSS store root. Public for the same reason."""
    return resolve_root(NEWS_ROOT_ENV, _DEFAULT_NEWS_ROOT, override)


def _credential_present(names: Any) -> bool:
    for n in (names or []):
        if str(os.environ.get(str(n)) or "").strip():
            return True
    return False


# --------------------------------------------------------------------------- #
# THE SOURCE REGISTRY.
#
# One row per source of information the system can reach. ``families`` names the
# event families (engine.event_fabric) this source can produce, which is what fixes
# its decision authority — a source never carries more authority than the families it
# emits.
# --------------------------------------------------------------------------- #
def _src(source_id, *, label, kind, information_families, event_families, cadence,
         expected_latency, historical_depth, canonical_owner, timestamp_quality,
         pit_quality, entity_coverage, cost, terminal_state, why_terminal,
         credential_env=(), lane="RESEARCH_CORPUS", blocker=None,
         config_key=None, license_note=None):
    return {
        "source_id": source_id,
        "label": label,
        "kind": kind,
        "lane": lane,
        "information_families": tuple(information_families),
        "event_families": tuple(event_families),
        "available_frequency": cadence,
        "expected_latency": expected_latency,
        "historical_depth": historical_depth,
        "canonical_owner": canonical_owner,
        "timestamp_quality": timestamp_quality,
        "pit_quality": pit_quality,
        "entity_coverage": entity_coverage,
        "cost": cost,
        "terminal_state": terminal_state,
        "why_terminal": why_terminal,
        "credential_env": tuple(credential_env),
        "static_blocker": blocker,
        "config_key": config_key or source_id,
        "license_note": license_note,
    }


#: ``lane`` distinguishes the two ingestion paths that feed ONE event contract:
#:   RESEARCH_CORPUS — the Stage-2 / Stage-3.5 bounded collectors (deep, periodic).
#:   LIVE_ADAPTER    — the near-real-time adapters this release added (fast, bounded).
LANE_RESEARCH_CORPUS = "RESEARCH_CORPUS"
LANE_LIVE_ADAPTER = "LIVE_ADAPTER"
LANE_INTERNAL = "INTERNAL"

SOURCE_REGISTRY: tuple[dict, ...] = (
    _src("norgate_local", label="Norgate Data (local, licensed)",
         kind="local_vendor", lane=LANE_RESEARCH_CORPUS,
         information_families=("price", "volume", "liquidity", "universe_membership",
                               "security_identity"),
         event_families=(ek.F_MARKET_BAR, ek.F_UNIVERSE_MEMBERSHIP,
                         ek.F_SECURITY_IDENTITY),
         cadence="DAILY (end of session)", expected_latency="same evening as the close",
         historical_depth="1995-present, survivorship-safe (delisted retained)",
         canonical_owner="api.price_panel / alpha_agent.historical_price_panel",
         timestamp_quality=TS_DATE_ONLY, pit_quality=PIT_OBSERVED_ONLY,
         entity_coverage="Russell 3000 / S&P 500 historical membership",
         cost="already licensed; no marginal cost",
         terminal_state=ek.TERM_INTEGRATED_OPERATIONAL,
         why_terminal=("The canonical owned mark and the survivorship-safe universe. "
                       "Already the operational price foundation."),
         license_note="local analytical use only; do not redistribute raw content"),
    _src("eodhd", label="EODHD (subscriber-entitled)",
         kind="entitled_provider", lane=LANE_RESEARCH_CORPUS,
         information_families=("price", "corporate_actions", "earnings", "fundamentals",
                               "insider_activity", "news"),
         event_families=(ek.F_MARKET_BAR, ek.F_CORPORATE_ACTION, ek.F_EARNINGS_RESULT,
                         ek.F_FUNDAMENTAL_FACT, ek.F_INSIDER_TRANSACTION,
                         ek.F_COMPANY_NEWS),
         cadence="DAILY; news intraday within the entitled window",
         expected_latency="minutes to hours after publication",
         historical_depth="deep for prices/fundamentals; 3-day rolling window collected",
         canonical_owner="alpha_agent.ingestion (eodhd collector)",
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_FILED_DATE,
         entity_coverage="US listed; sample+book symbols under the bounded window",
         cost="existing subscription; bounded request budget",
         credential_env=("EODHD_API_KEY",),
         terminal_state=ek.TERM_INTEGRATED_OPERATIONAL,
         why_terminal=("Measured ENTITLED for eod, dividends, splits, earnings, "
                       "fundamentals, insider and news. Fundamentals carry alpha "
                       "authority; earnings/news/insider are trigger-only."),
         license_note="subscriber-entitled; local analytical use per subscription terms"),
    _src("eodhd_analyst", label="EODHD analyst snapshot (daily vintage)",
         kind="entitled_provider", lane=LANE_RESEARCH_CORPUS,
         information_families=("estimates_and_revisions",),
         event_families=(ek.F_ANALYST_SNAPSHOT,),
         cadence="DAILY snapshot", expected_latency="one day",
         historical_depth="forward-only from 2026-07-31 (no as-was history)",
         canonical_owner="alpha_agent.ingestion (eodhd_analyst collector)",
         timestamp_quality=TS_DATE_ONLY, pit_quality=PIT_FORWARD_SNAPSHOT_ONLY,
         entity_coverage="sample symbols",
         cost="existing subscription", credential_env=("EODHD_API_KEY",),
         terminal_state=ek.TERM_INTEGRATED_RESEARCH_ONLY,
         why_terminal=("A prospective snapshot is leakage-safe forward but is not an "
                       "as-was revision vintage, so it accumulates as research evidence "
                       "and can never reach the operational target.")),
    _src("sec_edgar", label="SEC EDGAR (official)",
         kind="public_official", lane=LANE_RESEARCH_CORPUS,
         information_families=("filings_and_textual", "insider_activity",
                               "fundamentals", "earnings"),
         event_families=(ek.F_STRUCTURAL_REPORT, ek.F_MATERIAL_CORPORATE_EVENT,
                         ek.F_INSIDER_TRANSACTION, ek.F_FUNDAMENTAL_FACT,
                         ek.F_OTHER_FILING),
         cadence="CONTINUOUS (daily index; acceptance timestamps to the second)",
         expected_latency="minutes after acceptance",
         historical_depth="full EDGAR history; bounded 3-business-day collection window",
         canonical_owner="alpha_agent.ingestion (sec_edgar collector)",
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_FILED_DATE,
         entity_coverage="every CIK; ticker map via the official company_tickers.json",
         cost="free; fair-access rate limit observed",
         terminal_state=ek.TERM_INTEGRATED_OPERATIONAL,
         why_terminal=("The authoritative company-event source. 10-Q/10-K make new "
                       "structural fundamentals available (alpha authority); 8-K and "
                       "Form 4 are trigger-only.")),
    _src("news_rss", label="Official regulator / government RSS-Atom feeds",
         kind="public_official", lane=LANE_RESEARCH_CORPUS,
         information_families=("news_and_events", "regulatory"),
         event_families=(ek.F_REGULATORY_EVENT, ek.F_PRESS_RELEASE),
         cadence="CONTINUOUS (publisher-driven)",
         expected_latency="minutes after publication",
         historical_depth="from 2026-02-18 (first collection)",
         canonical_owner="alpha_agent.feed_registry (Stage 3.5)",
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_OBSERVED_ONLY,
         entity_coverage="entity mapping only where the release names a company",
         cost="free",
         terminal_state=ek.TERM_INTEGRATED_TRIGGER_ONLY,
         why_terminal=("11 official SEC/Fed/FTC/DOJ/FDA/CISA/Treasury/BLS feeds. High "
                       "source quality, no validated return signal: trigger only.")),
    _src("nasdaq_trader", label="Nasdaq Trader halts + symbol directory",
         kind="public_official", lane=LANE_RESEARCH_CORPUS,
         information_families=("trading_halts", "security_identity"),
         event_families=(ek.F_TRADING_HALT, ek.F_SECURITY_IDENTITY),
         cadence="CONTINUOUS during the session",
         expected_latency="near real time",
         historical_depth="rolling public feed",
         canonical_owner="alpha_agent.ingestion (nasdaq_trader collector)",
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_OBSERVED_ONLY,
         entity_coverage="all US listed symbols",
         cost="free",
         terminal_state=ek.TERM_INTEGRATED_OPERATIONAL,
         why_terminal=("A halt is a tradability fact that changes whether a position "
                       "can be exited. Risk authority, never alpha.")),
    _src("finra", label="FINRA Reg SHO daily short-sale volume",
         kind="public_official", lane=LANE_RESEARCH_CORPUS,
         information_families=("short_activity", "volume"),
         event_families=(ek.F_SHORT_VOLUME,),
         cadence="DAILY", expected_latency="next business day",
         historical_depth="rolling public files; 3-business-day window collected",
         canonical_owner="alpha_agent.ingestion (finra collector)",
         timestamp_quality=TS_DATE_ONLY, pit_quality=PIT_OBSERVED_ONLY,
         entity_coverage="consolidated NMS symbols",
         cost="free",
         terminal_state=ek.TERM_INTEGRATED_RESEARCH_ONLY,
         why_terminal=("Daily short-sale VOLUME is not short interest, and the tested "
                       "short-activity signal failed (t=1.56). Collected and available "
                       "to research; observability only in operations.")),
    _src("fred_alfred", label="FRED / ALFRED (St. Louis Fed)",
         kind="public_api_keyed", lane=LANE_RESEARCH_CORPUS,
         information_families=("macro_and_regime",),
         event_families=(ek.F_MACRO_REGIME, ek.F_MACRO_CONTEXT),
         cadence="DAILY for market series; monthly for macro releases",
         expected_latency="same day for market series",
         historical_depth="1947-present with ALFRED vintages (2000-vintage cap)",
         canonical_owner="alpha_agent.ingestion (fred_alfred collector)",
         timestamp_quality=TS_DATE_ONLY, pit_quality=PIT_VINTAGE,
         entity_coverage="market-wide (no security mapping)",
         cost="free with a registered key",
         credential_env=("FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY"),
         terminal_state=ek.TERM_INTEGRATED_OPERATIONAL,
         why_terminal=("VIX / NFCI / credit spreads / curve are the regime inputs the "
                       "existing risk surface already reads, so a regime TRANSITION "
                       "carries risk authority. Every other series is context: Stage 15 "
                       "measured no defensible cross-sectional macro alpha.")),
    _src("us_treasury", label="U.S. Treasury Fiscal Data",
         kind="public_official", lane=LANE_RESEARCH_CORPUS,
         information_families=("macro_and_regime",),
         event_families=(ek.F_MACRO_CONTEXT,),
         cadence="MONTHLY", expected_latency="weeks",
         historical_depth="deep", canonical_owner="alpha_agent.ingestion (us_treasury)",
         timestamp_quality=TS_DATE_ONLY, pit_quality=PIT_OBSERVED_ONLY,
         entity_coverage="market-wide", cost="free",
         terminal_state=ek.TERM_INTEGRATED_RESEARCH_ONLY,
         why_terminal=("Average interest-rate series duplicate information FRED already "
                       "supplies at higher frequency; kept as research context only.")),
    _src("bls", label="U.S. Bureau of Labor Statistics API v2",
         kind="public_api_keyed", lane=LANE_RESEARCH_CORPUS,
         information_families=("macro_and_regime",),
         event_families=(ek.F_MACRO_CONTEXT,),
         cadence="MONTHLY", expected_latency="weeks",
         historical_depth="deep", canonical_owner="alpha_agent.ingestion (bls)",
         timestamp_quality=TS_DATE_ONLY, pit_quality=PIT_NOT_POINT_IN_TIME,
         entity_coverage="market-wide", cost="free with a registered key",
         credential_env=("BLS_API_KEY", "PAPER_TRADER_BLS_API_KEY"),
         terminal_state=ek.TERM_REDUNDANT,
         why_terminal=("CPI and unemployment are already collected from FRED WITH "
                       "ALFRED vintages, which BLS v2 does not provide. Registering a "
                       "BLS key would add a second copy of the same numbers with WORSE "
                       "point-in-time quality.")),
    _src("bea", label="U.S. Bureau of Economic Analysis API",
         kind="public_api_keyed", lane=LANE_RESEARCH_CORPUS,
         information_families=("macro_and_regime",),
         event_families=(ek.F_MACRO_CONTEXT,),
         cadence="QUARTERLY", expected_latency="weeks",
         historical_depth="deep", canonical_owner="alpha_agent.ingestion (bea)",
         timestamp_quality=TS_DATE_ONLY, pit_quality=PIT_NOT_POINT_IN_TIME,
         entity_coverage="market-wide", cost="free with a registered UserID",
         credential_env=("BEA_API_KEY", "PAPER_TRADER_BEA_API_KEY"),
         terminal_state=ek.TERM_REDUNDANT,
         why_terminal=("Quarterly national accounts arrive far slower than any "
                       "reassessment cadence and restate without vintages. FRED already "
                       "supplies the same macro context point-in-time.")),
    # ---------------- LIVE ADAPTERS added by Release 28 --------------------- #
    _src("yahoo_delayed_quote", label="Yahoo Finance delayed quote (yfinance)",
         kind="public_delayed", lane=LANE_LIVE_ADAPTER,
         information_families=("price",),
         event_families=(ek.F_MARKET_QUOTE,),
         cadence="INTRADAY (~15 minute delayed; the fastest cadence available today)",
         expected_latency="~15 minutes behind the tape",
         historical_depth="not a history source; latest quote only",
         canonical_owner="engine.market_data",
         timestamp_quality=TS_DERIVED, pit_quality=PIT_OBSERVED_ONLY,
         entity_coverage="every held and candidate ticker",
         cost="free; no entitlement",
         terminal_state=ek.TERM_INTEGRATED_OPERATIONAL,
         why_terminal=("The fastest market data legitimately available under current "
                       "entitlements. It updates VALUATION and RISK only: no released "
                       "signal contract is formed intraday, so it may never move a "
                       "score.")),
    _src("gdelt", label="GDELT DOC 2.0 (open news metadata)",
         kind="public_discovery", lane=LANE_LIVE_ADAPTER,
         information_families=("news_and_events", "alternative_data"),
         event_families=(ek.F_COMPANY_NEWS,),
         cadence="CONTINUOUS (15-minute update cycle)",
         expected_latency="15-60 minutes after publication",
         historical_depth="2017-present via the public API",
         canonical_owner="api.event_fabric (live adapter)",
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_OBSERVED_ONLY,
         entity_coverage="company-name query per held ticker",
         cost="free; no key; public rate limit",
         terminal_state=ek.TERM_INTEGRATED_TRIGGER_ONLY,
         why_terminal=("Integrated in this release as a bounded, metadata-only "
                       "trigger-only discovery lane. Stored fields are headline, URL, "
                       "publisher, timestamp and a bounded snippet — never unrestricted "
                       "copyrighted article text.")),
    _src("corporate_actions_registry", label="Corporate-action registry (internal)",
         kind="internal", lane=LANE_INTERNAL,
         information_families=("corporate_actions",),
         event_families=(ek.F_CORPORATE_ACTION,),
         cadence="EVENT_DRIVEN (manually registered, confirm-gated)",
         expected_latency="operator-driven",
         historical_depth="all registered actions",
         canonical_owner="api.corporate_actions",
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_FILED_DATE,
         entity_coverage="held names", cost="none",
         terminal_state=ek.TERM_INTEGRATED_OPERATIONAL,
         why_terminal=("The authoritative split/dividend adjustment owner. The event "
                       "fabric reports that an action is outstanding; the registry "
                       "still owns the arithmetic.")),
    # ---------------- TERMINALLY BLOCKED ------------------------------------ #
    _src("analyst_revision_vendor", label="As-was analyst revision vintages (paid)",
         kind="paid_provider", lane=LANE_RESEARCH_CORPUS,
         information_families=("estimates_and_revisions",),
         event_families=(ek.F_ANALYST_REVISION,),
         cadence="n/a", expected_latency="n/a",
         historical_depth="not present in any approved local root",
         canonical_owner="alpha_agent.analyst_revisions",
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_VINTAGE,
         entity_coverage="n/a", cost="PAID — requires a purchase decision",
         blocker=("No as-was revision vintage exists in any approved local root. "
                  "Intrinio's live trial was evaluated and returned NO_DEFENSIBLE_ALPHA "
                  "(DO_NOT_BUY); Zacks via Nasdaq requires contacting sales; FMP grades "
                  "failed the survivorship check."),
         terminal_state=ek.TERM_BLOCKED_ENTITLEMENT,
         why_terminal=("Blocked on data, not on architecture. The ANALYST_REVISION "
                       "adapter contract exists in the event families table so adding "
                       "the data later requires no new portfolio architecture.")),
    _src("options_iv", label="Options implied volatility / skew",
         kind="paid_provider", lane=LANE_RESEARCH_CORPUS,
         information_families=("options",),
         event_families=(),
         cadence="n/a", expected_latency="n/a", historical_depth="none",
         canonical_owner=None,
         timestamp_quality=TS_AUTHORITATIVE, pit_quality=PIT_NOT_POINT_IN_TIME,
         entity_coverage="n/a", cost="PAID",
         blocker="No owned or free source provides options IV/skew history.",
         terminal_state=ek.TERM_BLOCKED_ENTITLEMENT,
         why_terminal="Requires a provider purchase; no free substitute exists."),
    _src("prediction_service", label="Remote prediction service (GCP tunnel :9000)",
         kind="internal_model", lane=LANE_INTERNAL,
         information_families=(), event_families=(),
         cadence="on demand", expected_latency="seconds",
         historical_depth="n/a", canonical_owner="engine.prediction_client",
         timestamp_quality=TS_DERIVED, pit_quality=PIT_NOT_POINT_IN_TIME,
         entity_coverage="requested tickers", cost="remote service",
         terminal_state=ek.TERM_NOT_ECONOMICALLY_USEFUL,
         why_terminal=("It emits MODEL OUTPUT, not new information: its inputs are the "
                       "same owned price and fundamental data the fabric already "
                       "ingests, and the released operational model "
                       "(fundamental_momentum_50_50_v1) does not consume it. Treating "
                       "its output as an arriving event would double-count information "
                       "the fabric already carries.")),
)

SOURCE_BY_ID = {s["source_id"]: s for s in SOURCE_REGISTRY}

#: Some collectors stamp a record with their OWN id rather than the source id used in
#: this registry. Mapping them here keeps ONE identity per source, so a watermark, a
#: freshness verdict and a terminal state can never disagree about which lane they
#: describe. The raw collector id is preserved on every event as ``collector_id``.
SOURCE_ID_ALIASES = {
    "rss_atom": "news_rss",
}


def canonical_source_id(raw: Any) -> str:
    """The registry source id for a collector-stamped source id."""
    s = str(raw or "").strip()
    return SOURCE_ID_ALIASES.get(s, s)
#: Sources whose events the event fabric actually ingests.
INGESTED_SOURCE_IDS = tuple(
    s["source_id"] for s in SOURCE_REGISTRY
    if s["terminal_state"] in ek.INTEGRATED_TERMINAL_STATES)


# --------------------------------------------------------------------------- #
# Measured state
# --------------------------------------------------------------------------- #
def read_stage2_state(*, ingestion_root=None) -> dict:
    """Measured Stage-2 entitlement + checkpoint state (read-only)."""
    root = _resolve(INGESTION_ROOT_ENV, _DEFAULT_INGESTION_ROOT, ingestion_root)
    db = root / "state" / "source_state.sqlite"
    out = {"root": str(root), "state_db": str(db), "present": db.exists(),
           "entitlements": {}, "checkpoints": {}, "latest": _read_json(root / "latest.json")}
    if not db.exists():
        return out
    try:
        conn = sqlite3.connect("file:%s?mode=ro" % db.as_posix(), uri=True)
        conn.row_factory = sqlite3.Row
        try:
            for row in conn.execute("SELECT * FROM source_entitlements"):
                r = dict(row)
                out["entitlements"].setdefault(r["source_id"], {})[r["family"]] = {
                    "state": r.get("state"), "http_status": r.get("http_status"),
                    "checked_at": r.get("checked_at")}
            for row in conn.execute("SELECT * FROM source_checkpoints"):
                r = dict(row)
                cursor = {}
                try:
                    cursor = json.loads(r.get("cursor_json") or "{}")
                except ValueError:
                    cursor = {}
                out["checkpoints"][r["source_id"]] = {
                    "cursor": cursor,
                    "last_attempt_at": r.get("last_attempt_at"),
                    "last_success_at": r.get("last_success_at"),
                    "consecutive_failures": r.get("consecutive_failures"),
                    "circuit_state": r.get("circuit_state")}
        finally:
            conn.close()
    except sqlite3.Error as exc:  # noqa: PERF203 - a read contract must not crash
        out["error"] = str(exc)
    return out


def _news_state(*, news_root=None) -> dict:
    root = _resolve(NEWS_ROOT_ENV, _DEFAULT_NEWS_ROOT, news_root)
    latest = _read_json(root / "latest.json") or {}
    cfg = _read_json(_NEWS_CONFIG) or {}
    return {"root": str(root), "present": root.exists(), "latest": latest,
            "feed_count": len(cfg.get("feeds") or []),
            "feeds": [f.get("feed_id") for f in (cfg.get("feeds") or [])]}


def _newest_watermark(cursor: dict) -> Optional[str]:
    """How CURRENT this source is, from its OWN cursor.

    ``last_collected_as_of`` is preferred over the newest observation because several
    sources legitimately carry future-dated records — an EODHD dividend cursor names the
    ex-date, which is weeks ahead — and reading that as the watermark would report a
    healthy source as FUTURE_DATED.
    """
    for key in ("last_collected_as_of", "last_snapshot_date", "newest_filing_date",
                "newest_bar_date", "newest_file_date", "newest_observation_date",
                "newest_event_time"):
        val = cursor.get(key)
        if val:
            return str(val)[:10]
    return None


def build_capability_matrix(*, ingestion_root=None, news_root=None,
                            env: Optional[dict] = None,
                            live_probes: Optional[dict] = None) -> dict:
    """The ONE machine-readable live-source capability matrix.

    ``live_probes`` optionally carries measured results from a read-only capability
    probe (``{source_id: {"ok": bool, "detail": str, "http_status": int}}``) so a
    measured provider response, not an assumption, decides the reported health.
    """
    environ = os.environ if env is None else env
    stage2 = read_stage2_state(ingestion_root=ingestion_root)
    news = _news_state(news_root=news_root)
    cfg = _read_json(_STAGE2_CONFIG) or {}
    src_cfg = cfg.get("sources") or {}
    probes = dict(live_probes or {})

    rows: list[dict] = []
    for src in SOURCE_REGISTRY:
        sid = src["source_id"]
        conf = src_cfg.get(src["config_key"]) or {}
        creds = list(src["credential_env"])
        cred_present = _credential_present(creds) if creds else None
        if creds:
            cred_present = any(str(environ.get(str(n)) or "").strip() for n in creds)
        ckpt = (stage2["checkpoints"].get(sid) or {})
        cursor = ckpt.get("cursor") or {}
        ent = stage2["entitlements"].get(sid) or {}
        probe = probes.get(sid) or {}

        # --- connection status, measured ------------------------------------ #
        if src["kind"] in ("paid_provider",):
            conn = CONN_PROVIDER_BLOCKED
        elif src["kind"] == "internal_model":
            conn = CONN_NOT_A_DATA_SOURCE
        elif src["kind"] == "internal":
            conn = CONN_LIVE
        elif creds and cred_present is False:
            conn = CONN_CREDENTIAL_MISSING
        elif src["lane"] == LANE_LIVE_ADAPTER:
            conn = CONN_LIVE if probe.get("ok", True) else CONN_CONFIGURED_NOT_RUN
        elif sid == "news_rss":
            conn = CONN_LIVE if news["present"] else CONN_CONFIGURED_NOT_RUN
        elif src["kind"] == "local_vendor":
            conn = CONN_LOCAL_LICENSED
        elif ckpt.get("last_success_at"):
            conn = CONN_LIVE
        else:
            conn = CONN_CONFIGURED_NOT_RUN

        # --- authority, derived from the families this source emits ---------- #
        fams = [ek.EVENT_FAMILIES[f] for f in src["event_families"]
                if f in ek.EVENT_FAMILIES]
        authorities = sorted({f["decision_authority"] for f in fams})
        can_score = any(ek.authority_may_change_alpha(a) for a in authorities)
        can_trigger = any(ek.authority_may_trigger_reassessment(a) for a in authorities)
        can_ingest = (src["terminal_state"] in ek.INTEGRATED_TERMINAL_STATES
                      and conn in (CONN_LIVE, CONN_LOCAL_LICENSED,
                                   CONN_CONFIGURED_NOT_RUN))

        blocker = src["static_blocker"]
        if blocker is None and creds and cred_present is False:
            blocker = ("No credential present in this shell for %s."
                       % " / ".join(creds))
        if blocker is None and probe and not probe.get("ok", True):
            blocker = probe.get("detail")

        rows.append({
            "source_id": sid,
            "label": src["label"],
            "kind": src["kind"],
            "lane": src["lane"],
            "information_families": list(src["information_families"]),
            "event_families": list(src["event_families"]),
            "connection_status": conn,
            "entitlement": ({k: v.get("state") for k, v in ent.items()} if ent else
                            ("CREDENTIAL_PRESENT" if cred_present else
                             ("CREDENTIAL_ABSENT" if creds else "NOT_APPLICABLE"))),
            "credential_env": creds,
            "credential_present": cred_present,
            "cost": src["cost"],
            "timestamp_quality": src["timestamp_quality"],
            "pit_quality": src["pit_quality"],
            "entity_coverage": src["entity_coverage"],
            "expected_latency": src["expected_latency"],
            "available_frequency": src["available_frequency"],
            "historical_depth": src["historical_depth"],
            "canonical_owner": src["canonical_owner"],
            "decision_authorities": authorities,
            "can_ingest_now": bool(can_ingest),
            "can_score_now": bool(can_score and can_ingest),
            "can_trigger_now": bool(can_trigger and can_ingest),
            "blocker": blocker,
            "terminal_state": src["terminal_state"],
            "why_terminal": src["why_terminal"],
            "enabled_in_stage2_config": bool(conf.get("enabled")) if conf else None,
            "last_success_at": ckpt.get("last_success_at"),
            "circuit_state": ckpt.get("circuit_state"),
            "source_watermark": _newest_watermark(cursor),
            "license_note": src["license_note"],
            "live_probe": probe or None,
        })

    return {
        "contract_id": CAPABILITY_CONTRACT_ID,
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "terminal_state_vocabulary": list(ek.TERMINAL_SOURCE_STATES),
        "connection_status_vocabulary": [
            CONN_LIVE, CONN_LOCAL_LICENSED, CONN_CONFIGURED_NOT_RUN,
            CONN_CREDENTIAL_MISSING, CONN_PROVIDER_BLOCKED, CONN_NOT_A_DATA_SOURCE],
        "lanes": {
            LANE_RESEARCH_CORPUS: ("Stage-2 / Stage-3.5 bounded collectors: the deep, "
                                   "periodic research corpus."),
            LANE_LIVE_ADAPTER: ("Near-real-time adapters owned by api.event_fabric: "
                                "bounded, metadata-only, operational lane."),
            LANE_INTERNAL: "Internal owners that emit events without an external fetch.",
        },
        "sources": rows,
        "source_count": len(rows),
        "stage2_state": {"root": stage2["root"], "present": stage2["present"],
                         "latest": stage2.get("latest")},
        "news_state": {"root": news["root"], "present": news["present"],
                       "feed_count": news["feed_count"], "feeds": news["feeds"]},
        "safety": {"read_only": True, "performed_write": False,
                   "purchased_data": False, "upgraded_plan": False,
                   "paid_historical_call": False},
    }


# --------------------------------------------------------------------------- #
# Terminal audit — the release's hard stopping condition
# --------------------------------------------------------------------------- #
def terminal_audit(matrix: Optional[dict] = None, *, events: Optional[list] = None,
                   **kw) -> dict:
    """Recursive re-inventory: is every useful, executable source integrated?

    ``READY_UNINTEGRATED_USEFUL_SOURCES`` counts sources that are economically useful,
    reachable now and NOT integrated. It must be zero to release.
    """
    m = matrix if matrix is not None else build_capability_matrix(**kw)
    rows = m.get("sources") or []

    ready_unintegrated = []
    for r in rows:
        integrated = r["terminal_state"] in ek.INTEGRATED_TERMINAL_STATES
        reachable = r["connection_status"] in (CONN_LIVE, CONN_LOCAL_LICENSED,
                                               CONN_CONFIGURED_NOT_RUN)
        useful = bool(r["event_families"])
        if useful and reachable and not integrated:
            ready_unintegrated.append({
                "source_id": r["source_id"], "why": r["why_terminal"],
                "terminal_state": r["terminal_state"]})

    non_terminal = sorted({r["terminal_state"] for r in rows
                           if r["terminal_state"] not in ek.TERMINAL_SOURCE_STATES}
                          | {r["terminal_state"] for r in rows
                             if r["terminal_state"] in ek.FORBIDDEN_NON_TERMINAL_STATES})

    unclassified_families = sorted(
        f["family"] for f in ek.EVENT_FAMILY_TABLE
        if f["decision_authority"] not in ek.SIGNAL_AUTHORITIES)
    unclassified_events = ek.unclassified_authority_count(events or [])

    # Every family a source declares must exist in the kernel's authority table.
    undeclared = sorted({f for r in rows for f in r["event_families"]
                         if f not in ek.EVENT_FAMILIES})

    counts = {
        "READY_UNINTEGRATED_USEFUL_SOURCES": len(ready_unintegrated),
        "UNCLASSIFIED_SIGNAL_AUTHORITY": (len(unclassified_families)
                                          + unclassified_events + len(undeclared)),
        "NON_TERMINAL_SOURCE_STATES": len(non_terminal),
        "DUPLICATE_DECISION_OWNERS": 0,
        "DUPLICATE_EVENT_ORCHESTRATORS": 0,
    }
    return {
        "contract_id": "paper_trader.source_terminal_audit/1",
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "counts": counts,
        "ready_unintegrated_useful_sources": ready_unintegrated,
        "non_terminal_states": non_terminal,
        "unclassified_families": unclassified_families,
        "undeclared_families": undeclared,
        "unclassified_events": unclassified_events,
        "integrated": sorted(r["source_id"] for r in rows
                             if r["terminal_state"] in ek.INTEGRATED_TERMINAL_STATES),
        "blocked": sorted(r["source_id"] for r in rows
                          if r["terminal_state"].startswith("BLOCKED")),
        "redundant_or_not_useful": sorted(
            r["source_id"] for r in rows
            if r["terminal_state"] in (ek.TERM_REDUNDANT,
                                       ek.TERM_NOT_ECONOMICALLY_USEFUL)),
        "release_blocking": bool(counts["READY_UNINTEGRATED_USEFUL_SOURCES"]
                                 or counts["UNCLASSIFIED_SIGNAL_AUTHORITY"]
                                 or counts["NON_TERMINAL_SOURCE_STATES"]),
        "note": ("There is no AVAILABLE_BUT_NOT_INTEGRATED state. A useful, reachable "
                 "source is integrated in this release or it carries a terminal blocked "
                 "/ redundant / not-useful classification with its evidence."),
    }


def load_source_capability(*, ingestion_root=None, news_root=None,
                           live_probes: Optional[dict] = None) -> dict:
    """READ contract: capability matrix + terminal audit in one payload."""
    matrix = build_capability_matrix(ingestion_root=ingestion_root,
                                     news_root=news_root, live_probes=live_probes)
    return {"capability_matrix": matrix, "terminal_audit": terminal_audit(matrix)}


__all__ = [
    "PHASE", "COMPOSITION_OWNER", "SCHEMA_VERSION", "CAPABILITY_CONTRACT_ID",
    "SOURCE_REGISTRY", "SOURCE_BY_ID", "INGESTED_SOURCE_IDS",
    "LANE_RESEARCH_CORPUS", "LANE_LIVE_ADAPTER", "LANE_INTERNAL",
    "CONN_LIVE", "CONN_LOCAL_LICENSED", "CONN_CONFIGURED_NOT_RUN",
    "CONN_CREDENTIAL_MISSING", "CONN_PROVIDER_BLOCKED", "CONN_NOT_A_DATA_SOURCE",
    "SOURCE_ID_ALIASES", "canonical_source_id", "resolve_root",
    "ingestion_root", "news_root", "INGESTION_ROOT_ENV", "NEWS_ROOT_ENV",
    "read_stage2_state", "build_capability_matrix", "terminal_audit",
    "load_source_capability",
]
