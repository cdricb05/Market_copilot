"""
engine/collection_cadence.py — Release 29 CANONICAL COLLECTION CADENCE POLICY.

THE PURE CALCULATION OWNER for the question "which information source should be
collected right now, and which sources SHOULD currently be fresh?".

This module is deliberately pure: no filesystem, no network, no clock of its own
(every entry point takes an injected ``now``). It holds ONE authoritative cadence
policy per integrated source, the market-session-aware active-window rule, the
provider request budget, the adaptive backoff / circuit policy and the runtime
state machine whose counts the operator UI renders verbatim.

WHY THIS EXISTS
---------------
Release 28 built the processing path (event contract -> novelty -> materiality ->
holding opportunity cost -> reassessment -> proposal). It did NOT keep information
flowing into it, and it judged every source against ONE anchor date, so a monthly
macro lane and a market quote feed on a Sunday both read STALE. That produced the
operationally misleading "Sources fresh 1 / 17, degraded 10" surface.

The fix is a source-specific cadence: a source is only expected to be current
inside its OWN active window. A market quote feed is NOT_DUE on a Sunday; it is
not broken. A monthly release is NOT_DUE between releases. The KPI the operator
reads is therefore "of the sources that SHOULD be current now, how many are
healthy?" — never "how many of all 17 rows say FRESH".

TWO DISTINCT QUESTIONS, DELIBERATELY SEPARATED
---------------------------------------------
``due_window_active``  Should this source be CURRENT right now? Drives the KPI
                       denominator and the FRESH/DEGRADED health judgement.
``collect_now``        Should THIS iteration call the collector? Drives the work.
                       True only when the window is active, the minimum interval
                       has elapsed, no backoff is in force and budget remains.

A source can be due-window-active and healthy while ``collect_now`` is False —
that is the normal quiet case, and it is exactly why a service that wakes every
60 seconds does not make 17 provider calls a minute.

SAFETY
------
Nothing here creates an order, confirms a target, promotes a model or executes a
rebalance. This module decides only WHEN to ask a provider for information.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable, Optional

from . import market_hours as mh

PHASE = "RELEASE29"
CALCULATION_OWNER = "engine.collection_cadence"
CADENCE_POLICY_VERSION = "collection_cadence.v1"
CADENCE_CONTRACT_ID = "paper_trader.collection_cadence_policy/1"

# --------------------------------------------------------------------------- #
# Cadence kinds — WHY a source is collected when it is collected.
# --------------------------------------------------------------------------- #
K_CONTINUOUS_EVENT = "CONTINUOUS_EVENT"
K_INTRADAY_MARKET = "INTRADAY_MARKET"
K_SESSION_END = "SESSION_END"
K_DAILY_PUBLICATION = "DAILY_PUBLICATION"
K_MONTHLY_RELEASE = "MONTHLY_RELEASE"
K_QUARTERLY_RELEASE = "QUARTERLY_RELEASE"
K_LOCAL_FILE_WATCH = "LOCAL_FILE_WATCH"
K_INTERNAL_EVENT = "INTERNAL_EVENT"
K_RESEARCH_ONLY = "RESEARCH_ONLY"
K_BLOCKED = "BLOCKED"
K_NOT_A_SOURCE = "NOT_A_DATA_SOURCE"
CADENCE_KINDS = (K_CONTINUOUS_EVENT, K_INTRADAY_MARKET, K_SESSION_END,
                 K_DAILY_PUBLICATION, K_MONTHLY_RELEASE, K_QUARTERLY_RELEASE,
                 K_LOCAL_FILE_WATCH, K_INTERNAL_EVENT, K_RESEARCH_ONLY, K_BLOCKED,
                 K_NOT_A_SOURCE)

# --------------------------------------------------------------------------- #
# Market-session requirement — resolved by engine.market_hours, never by local
# weekday/time arithmetic in this module or any caller.
# --------------------------------------------------------------------------- #
SESSION_ANY = "ANY_TIME"                    # 24/7 publisher-driven
SESSION_OPEN_ONLY = "REGULAR_SESSION_ONLY"  # only while the tape is running
SESSION_TRADING_DAY = "TRADING_DAY_WINDOW"  # weekday, inside an ET hour window
SESSION_AFTER_CLOSE = "AFTER_REGULAR_CLOSE"  # weekday, after the close cutoff
SESSION_NEVER = "NEVER"                     # not collected on a clock
SESSION_REQUIREMENTS = (SESSION_ANY, SESSION_OPEN_ONLY, SESSION_TRADING_DAY,
                        SESSION_AFTER_CLOSE, SESSION_NEVER)

# --------------------------------------------------------------------------- #
# Runtime states — ONE partition over every source. The UI counts these cells;
# it derives none of them.
# --------------------------------------------------------------------------- #
RS_FRESH = "FRESH"
RS_DUE = "DUE"
RS_RUNNING = "RUNNING"
RS_NOT_DUE = "NOT_DUE"
RS_BACKOFF = "BACKOFF"
RS_DEGRADED = "DEGRADED"
RS_FAILED = "FAILED"
RS_BLOCKED = "BLOCKED"
RS_RESEARCH_ONLY = "RESEARCH_ONLY"
RS_DISABLED = "DISABLED"
RS_DEGRADED_CREDENTIAL = "DEGRADED_CREDENTIAL"
RUNTIME_STATES = (RS_FRESH, RS_DUE, RS_RUNNING, RS_NOT_DUE, RS_BACKOFF,
                  RS_DEGRADED, RS_FAILED, RS_BLOCKED, RS_RESEARCH_ONLY,
                  RS_DISABLED, RS_DEGRADED_CREDENTIAL)

#: States that mean "this source is expected to be current right now". The
#: operator KPI denominator.
DUE_WINDOW_STATES = frozenset({RS_FRESH, RS_DUE, RS_RUNNING, RS_DEGRADED})
#: States that mean "healthy given what is expected of it right now".
HEALTHY_STATES = frozenset({RS_FRESH, RS_DUE, RS_RUNNING, RS_NOT_DUE,
                            RS_RESEARCH_ONLY})
#: States that mean "the source cannot deliver and someone should know".
UNHEALTHY_STATES = frozenset({RS_BACKOFF, RS_DEGRADED, RS_FAILED,
                              RS_DEGRADED_CREDENTIAL})

# --------------------------------------------------------------------------- #
# Attention tiers — WHICH securities a per-symbol source may query.
# --------------------------------------------------------------------------- #
TIER_HOLDINGS = "TIER0_CURRENT_HOLDINGS"
TIER_CANDIDATES = "TIER1_REPLACEMENT_CANDIDATES"
TIER_UNIVERSE = "TIER2_ELIGIBLE_UNIVERSE"
TIER_GLOBAL = "GLOBAL_NO_PER_SYMBOL_QUERY"
ATTENTION_TIERS = (TIER_HOLDINGS, TIER_CANDIDATES, TIER_UNIVERSE, TIER_GLOBAL)

# --------------------------------------------------------------------------- #
# Backoff / circuit policy.
# --------------------------------------------------------------------------- #
CIRCUIT_CLOSED = "CLOSED"
CIRCUIT_HALF_OPEN = "HALF_OPEN"
CIRCUIT_OPEN = "OPEN"
CIRCUIT_STATES = (CIRCUIT_CLOSED, CIRCUIT_HALF_OPEN, CIRCUIT_OPEN)

ERR_RATE_LIMIT = "RATE_LIMIT"
ERR_SERVER = "SERVER_ERROR"
ERR_TIMEOUT = "TIMEOUT_OR_NETWORK"
ERR_AUTH = "AUTH_OR_ENTITLEMENT"
ERR_CLIENT = "CLIENT_ERROR"
ERR_UNKNOWN = "UNKNOWN"
ERROR_CATEGORIES = (ERR_RATE_LIMIT, ERR_SERVER, ERR_TIMEOUT, ERR_AUTH,
                    ERR_CLIENT, ERR_UNKNOWN)

#: Base backoff seconds by error category, doubled per consecutive failure and
#: clamped to the per-category ceiling. An auth/entitlement failure is NOT a
#: transient outage: it parks the source for a long time instead of hammering a
#: provider that has already said no.
BACKOFF_BASE_SECONDS = {
    ERR_RATE_LIMIT: 900.0,     # 15 min — a 429 means "stop asking", not "retry"
    ERR_SERVER: 120.0,
    ERR_TIMEOUT: 60.0,
    ERR_AUTH: 21600.0,         # 6 h
    ERR_CLIENT: 1800.0,
    ERR_UNKNOWN: 120.0,
}
BACKOFF_CEILING_SECONDS = {
    ERR_RATE_LIMIT: 14400.0,   # 4 h
    ERR_SERVER: 3600.0,
    ERR_TIMEOUT: 1800.0,
    ERR_AUTH: 86400.0,         # 24 h
    ERR_CLIENT: 21600.0,
    ERR_UNKNOWN: 3600.0,
}
#: Consecutive failures at which the circuit opens (the source stops being asked
#: until the backoff window expires, then gets exactly one probe: HALF_OPEN).
CIRCUIT_OPEN_THRESHOLD = 4

MIN_ITERATION_INTERVAL_SECONDS = 30
DEFAULT_ITERATION_INTERVAL_SECONDS = 60
MAX_WAKE_SECONDS = 900.0


def classify_http_error(*, status: Any = None, detail: Any = None) -> str:
    """Map a provider outcome onto ONE error category. Pure."""
    try:
        code = int(status) if status is not None else None
    except (TypeError, ValueError):
        code = None
    if code == 429:
        return ERR_RATE_LIMIT
    if code in (401, 402, 403):
        return ERR_AUTH
    if code is not None and 500 <= code <= 599:
        return ERR_SERVER
    if code is not None and 400 <= code <= 499:
        return ERR_CLIENT
    text = str(detail or "").lower()
    if any(w in text for w in ("timed out", "timeout", "temporarily",
                               "connection", "unreachable", "reset by peer",
                               "name or service not known", "getaddrinfo")):
        return ERR_TIMEOUT
    if not text and code is None:
        return ERR_UNKNOWN
    return ERR_UNKNOWN


def backoff_seconds(*, category: str, consecutive_failures: int) -> float:
    """Bounded exponential backoff for ONE source. Pure and deterministic."""
    base = BACKOFF_BASE_SECONDS.get(category, BACKOFF_BASE_SECONDS[ERR_UNKNOWN])
    ceiling = BACKOFF_CEILING_SECONDS.get(category,
                                          BACKOFF_CEILING_SECONDS[ERR_UNKNOWN])
    n = max(1, int(consecutive_failures or 1))
    # 2 ** (n - 1) grows fast; cap the exponent so a long outage cannot overflow.
    factor = float(2 ** min(n - 1, 12))
    return float(min(base * factor, ceiling))


def circuit_state_for(*, consecutive_failures: int, backoff_until: Optional[datetime],
                      now: datetime) -> str:
    """CLOSED / OPEN / HALF_OPEN from the failure streak and the backoff window."""
    n = int(consecutive_failures or 0)
    if n < CIRCUIT_OPEN_THRESHOLD:
        return CIRCUIT_CLOSED
    if backoff_until is not None and backoff_until > now:
        return CIRCUIT_OPEN
    return CIRCUIT_HALF_OPEN


# --------------------------------------------------------------------------- #
# The cadence policy table.
#
# Every value below is justified by the source's ACTUAL publication behaviour and
# entitlement as recorded in api.source_capability.SOURCE_REGISTRY — not by a
# convenient round number. ``why`` is part of the contract: a cadence nobody can
# justify is a cadence nobody should trust.
# --------------------------------------------------------------------------- #
def _policy(source_id, *, kind, session, normal_interval_seconds,
            minimum_interval_seconds, maximum_staleness_seconds, why,
            attention_tier=TIER_GLOBAL, collector_owner=None,
            window_start_et=None, window_end_et=None,
            max_calls_per_iteration=1, max_symbols_per_iteration=0,
            max_calls_per_hour=None, max_calls_per_day=None,
            timeout_seconds=30, max_retries=1, credential_env=(),
            collection_enabled=True, operational=True,
            catch_up_grace_seconds=None) -> dict:
    return {
        "source_id": source_id,
        "cadence_policy_id": "%s.%s" % (CADENCE_POLICY_VERSION, source_id),
        "cadence_kind": kind,
        "market_session_requirement": session,
        "active_window_start_et": window_start_et,
        "active_window_end_et": window_end_et,
        "normal_interval_seconds": float(normal_interval_seconds),
        "minimum_call_interval_seconds": float(minimum_interval_seconds),
        "maximum_staleness_seconds": (None if maximum_staleness_seconds is None
                                      else float(maximum_staleness_seconds)),
        "catch_up_grace_seconds": float(catch_up_grace_seconds
                                        if catch_up_grace_seconds is not None
                                        else max(normal_interval_seconds * 3, 3600)),
        "attention_tier": attention_tier,
        "collector_owner": collector_owner,
        "request_budget": {
            "max_calls_per_iteration": int(max_calls_per_iteration),
            "max_symbols_per_iteration": int(max_symbols_per_iteration),
            "max_calls_per_hour": (None if max_calls_per_hour is None
                                   else int(max_calls_per_hour)),
            "max_calls_per_day": (None if max_calls_per_day is None
                                  else int(max_calls_per_day)),
            "timeout_seconds": int(timeout_seconds),
            "max_retries": int(max_retries),
        },
        "credential_env": tuple(credential_env),
        "collection_enabled": bool(collection_enabled),
        "operational": bool(operational),
        "why": why,
    }


CADENCE_POLICY_TABLE: tuple[dict, ...] = (
    # ---- local licensed vendor: we WATCH it, we cannot fetch it -------------- #
    _policy("norgate_local", kind=K_LOCAL_FILE_WATCH, session=SESSION_AFTER_CLOSE,
            normal_interval_seconds=1800, minimum_interval_seconds=900,
            maximum_staleness_seconds=None, attention_tier=TIER_GLOBAL,
            collector_owner="alpha_agent.ingestion (norgate_local collector)",
            window_start_et="17:30", window_end_et="23:59",
            max_calls_per_iteration=1, timeout_seconds=120,
            why=("Norgate updates the LOCAL licensed store from its own desktop "
                 "updater; there is no programmatic provider refresh to call. The "
                 "service therefore WATCHES the local store after the regular "
                 "close and admits what the updater already wrote. Polling it "
                 "during the session would find yesterday's file.")),
    # ---- entitled provider: prices/actions daily, news intraday ------------- #
    _policy("eodhd", kind=K_SESSION_END, session=SESSION_TRADING_DAY,
            normal_interval_seconds=3600, minimum_interval_seconds=1800,
            maximum_staleness_seconds=93600,  # 26 h — one trading day plus slack
            attention_tier=TIER_CANDIDATES,
            collector_owner="alpha_agent.ingestion (eodhd collector)",
            window_start_et="06:00", window_end_et="22:00",
            max_calls_per_iteration=1, max_symbols_per_iteration=60,
            max_calls_per_hour=2, max_calls_per_day=24,
            timeout_seconds=30, credential_env=("EODHD_API_KEY",),
            why=("EOD prices, splits, dividends, earnings and fundamentals settle "
                 "once per session; the entitled news window updates intraday. One "
                 "bounded pass per hour inside the trading day covers both without "
                 "spending subscription quota on unchanged end-of-day rows.")),
    _policy("eodhd_analyst", kind=K_DAILY_PUBLICATION, session=SESSION_TRADING_DAY,
            normal_interval_seconds=86400, minimum_interval_seconds=43200,
            maximum_staleness_seconds=345600,  # 4 days — weekend + a holiday
            attention_tier=TIER_CANDIDATES, operational=False,
            collector_owner="alpha_agent.ingestion (eodhd_analyst collector)",
            window_start_et="07:00", window_end_et="22:00",
            max_calls_per_iteration=1, max_symbols_per_iteration=40,
            max_calls_per_day=1, credential_env=("EODHD_API_KEY",),
            why=("A DAILY prospective snapshot. Collecting it more than once a day "
                 "cannot produce a new vintage and only spends quota. It is "
                 "forward-snapshot-only evidence and can never reach the "
                 "operational target, so it is a research lane.")),
    # ---- official continuous filing feed ------------------------------------ #
    _policy("sec_edgar", kind=K_CONTINUOUS_EVENT, session=SESSION_TRADING_DAY,
            normal_interval_seconds=900, minimum_interval_seconds=600,
            maximum_staleness_seconds=345600,
            attention_tier=TIER_GLOBAL,
            collector_owner="alpha_agent.ingestion (sec_edgar collector)",
            window_start_et="06:00", window_end_et="22:00",
            max_calls_per_iteration=1, max_calls_per_hour=4,
            timeout_seconds=45, max_retries=1,
            why=("EDGAR accepts filings 06:00-22:00 ET on business days and stamps "
                 "acceptance to the second. A 15-minute delta read of the daily "
                 "index is fast enough to put an 8-K on the review list the same "
                 "hour while staying well inside the fair-access rate limit. The "
                 "collector reads the recent index, never the full history.")),
    # ---- publisher-driven official news ------------------------------------- #
    _policy("news_rss", kind=K_CONTINUOUS_EVENT, session=SESSION_ANY,
            normal_interval_seconds=900, minimum_interval_seconds=600,
            maximum_staleness_seconds=345600,
            attention_tier=TIER_GLOBAL,
            collector_owner="alpha_agent.feed_registry (Stage 3.5)",
            max_calls_per_iteration=1, timeout_seconds=45,
            why=("Regulators and government agencies publish at any hour, including "
                 "weekends. The Stage-3.5 collector uses conditional GETs "
                 "(ETag/If-Modified-Since), so a 15-minute poll of 11 feeds is "
                 "cheap and mostly returns 304.")),
    # ---- session-only tradability ------------------------------------------- #
    _policy("nasdaq_trader", kind=K_CONTINUOUS_EVENT, session=SESSION_OPEN_ONLY,
            normal_interval_seconds=600, minimum_interval_seconds=300,
            maximum_staleness_seconds=3600,
            attention_tier=TIER_GLOBAL,
            collector_owner="alpha_agent.ingestion (nasdaq_trader collector)",
            max_calls_per_iteration=1, max_calls_per_hour=6, timeout_seconds=30,
            why=("A trading halt is only declared while the tape is running, and it "
                 "is a tradability fact that changes whether a position can be "
                 "exited. Polling the halt feed overnight cannot discover a halt "
                 "that has not happened; the symbol directory is picked up by the "
                 "same pass.")),
    # ---- next-business-day publication -------------------------------------- #
    _policy("finra", kind=K_DAILY_PUBLICATION, session=SESSION_AFTER_CLOSE,
            normal_interval_seconds=86400, minimum_interval_seconds=21600,
            maximum_staleness_seconds=432000,  # 5 days
            attention_tier=TIER_GLOBAL, operational=False,
            collector_owner="alpha_agent.ingestion (finra collector)",
            window_start_et="18:00", window_end_et="23:59",
            max_calls_per_iteration=1, max_calls_per_day=2, timeout_seconds=45,
            why=("Reg SHO daily short-sale volume files publish once, on the NEXT "
                 "business day. One evening pass captures them. The tested "
                 "short-activity signal failed (t=1.56), so this is an "
                 "observability lane, never an alpha input.")),
    # ---- macro ---------------------------------------------------------------#
    _policy("fred_alfred", kind=K_DAILY_PUBLICATION, session=SESSION_TRADING_DAY,
            normal_interval_seconds=14400, minimum_interval_seconds=7200,
            maximum_staleness_seconds=345600,
            attention_tier=TIER_GLOBAL,
            collector_owner="alpha_agent.ingestion (fred_alfred collector)",
            window_start_et="07:00", window_end_et="20:00",
            max_calls_per_iteration=1, max_calls_per_hour=1, max_calls_per_day=4,
            timeout_seconds=30,
            credential_env=("FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY"),
            why=("VIX / NFCI / credit spreads / curve update at most daily and the "
                 "monthly macro releases land on a published schedule. Four passes "
                 "a trading day catches a same-day regime input without asking a "
                 "monthly series for a new value every minute.")),
    _policy("us_treasury", kind=K_MONTHLY_RELEASE, session=SESSION_TRADING_DAY,
            normal_interval_seconds=86400, minimum_interval_seconds=43200,
            maximum_staleness_seconds=4147200,  # 48 days — one monthly cycle + slack
            attention_tier=TIER_GLOBAL, operational=False,
            collector_owner="alpha_agent.ingestion (us_treasury collector)",
            window_start_et="08:00", window_end_et="20:00",
            max_calls_per_iteration=1, max_calls_per_day=1, timeout_seconds=30,
            why=("Fiscal Data average-interest-rate series are MONTHLY. A daily "
                 "probe is already generous; it duplicates information FRED "
                 "supplies at higher frequency, so it stays a research lane.")),
    # ---- redundant: deliberately not collected ------------------------------ #
    _policy("bls", kind=K_RESEARCH_ONLY, session=SESSION_NEVER,
            normal_interval_seconds=0, minimum_interval_seconds=0,
            maximum_staleness_seconds=None, collection_enabled=False,
            operational=False, credential_env=("BLS_API_KEY",),
            collector_owner="alpha_agent.ingestion (bls collector)",
            why=("REDUNDANT. CPI and unemployment already arrive from FRED WITH "
                 "ALFRED vintages, which BLS v2 does not provide. Scheduling it "
                 "would add a second copy of the same numbers with WORSE "
                 "point-in-time quality, so it is not scheduled at all.")),
    _policy("bea", kind=K_RESEARCH_ONLY, session=SESSION_NEVER,
            normal_interval_seconds=0, minimum_interval_seconds=0,
            maximum_staleness_seconds=None, collection_enabled=False,
            operational=False, credential_env=("BEA_API_KEY",),
            collector_owner="alpha_agent.ingestion (bea collector)",
            why=("REDUNDANT. Quarterly national accounts arrive far slower than any "
                 "reassessment cadence and restate without vintages. FRED already "
                 "supplies the same macro context point-in-time.")),
    # ---- live adapters (owned by api.event_fabric, driven by Release 28) ---- #
    _policy("yahoo_delayed_quote", kind=K_INTRADAY_MARKET, session=SESSION_OPEN_ONLY,
            normal_interval_seconds=900, minimum_interval_seconds=780,
            maximum_staleness_seconds=2700,
            attention_tier=TIER_HOLDINGS,
            collector_owner="api.event_fabric.capture_market_quotes",
            max_calls_per_iteration=1, max_symbols_per_iteration=40,
            max_calls_per_hour=5, timeout_seconds=30,
            why=("The quote is ~15 minutes delayed, so polling faster than 15 "
                 "minutes cannot produce a newer price — it only re-reads the same "
                 "delayed tick. Outside the regular session the provider has no new "
                 "value to give, so the source is NOT_DUE rather than stale.")),
    _policy("gdelt", kind=K_CONTINUOUS_EVENT, session=SESSION_ANY,
            normal_interval_seconds=3600, minimum_interval_seconds=1800,
            maximum_staleness_seconds=172800,  # 2 days
            attention_tier=TIER_HOLDINGS,
            collector_owner="api.event_fabric.capture_gdelt_news",
            max_calls_per_iteration=1, max_symbols_per_iteration=8,
            max_calls_per_hour=1, max_calls_per_day=24, timeout_seconds=25,
            why=("GDELT's free endpoint rate-limits bursts and returned HTTP 429 "
                 "under Release-28 probing. Its own update cycle is 15 minutes, but "
                 "an hourly bounded pass over at most 8 held names stays inside the "
                 "public limit. A 429 opens a long backoff instead of a retry "
                 "loop.")),
    # ---- internal ------------------------------------------------------------#
    _policy("corporate_actions_registry", kind=K_INTERNAL_EVENT, session=SESSION_NEVER,
            normal_interval_seconds=0, minimum_interval_seconds=0,
            maximum_staleness_seconds=None, collection_enabled=False,
            collector_owner="api.corporate_actions",
            why=("An INTERNAL, operator-registered, confirm-gated registry. There is "
                 "no provider to poll: a corporate action enters the system when the "
                 "operator registers it. Scheduling it would be theatre.")),
    # ---- terminally blocked --------------------------------------------------#
    _policy("analyst_revision_vendor", kind=K_BLOCKED, session=SESSION_NEVER,
            normal_interval_seconds=0, minimum_interval_seconds=0,
            maximum_staleness_seconds=None, collection_enabled=False,
            operational=False, collector_owner=None,
            why=("BLOCKED on entitlement, not architecture. No as-was revision "
                 "vintage exists in any approved local root; Intrinio's trial "
                 "returned DO_NOT_BUY. Nothing to schedule.")),
    _policy("options_iv", kind=K_BLOCKED, session=SESSION_NEVER,
            normal_interval_seconds=0, minimum_interval_seconds=0,
            maximum_staleness_seconds=None, collection_enabled=False,
            operational=False, collector_owner=None,
            why="BLOCKED on entitlement. No owned or free source provides IV/skew."),
    _policy("prediction_service", kind=K_NOT_A_SOURCE, session=SESSION_NEVER,
            normal_interval_seconds=0, minimum_interval_seconds=0,
            maximum_staleness_seconds=None, collection_enabled=False,
            operational=False, collector_owner=None,
            why=("NOT A DATA SOURCE. It emits MODEL OUTPUT over the same owned "
                 "inputs the fabric already ingests; polling it would double-count "
                 "information rather than acquire any.")),
)

CADENCE_POLICY_BY_ID: dict[str, dict] = {
    p["source_id"]: p for p in CADENCE_POLICY_TABLE}

#: Sources whose collection this service actually drives on a clock.
SCHEDULED_SOURCE_IDS: tuple[str, ...] = tuple(
    p["source_id"] for p in CADENCE_POLICY_TABLE if p["collection_enabled"])


def policy_for(source_id: Any) -> Optional[dict]:
    return CADENCE_POLICY_BY_ID.get(str(source_id or ""))


# --------------------------------------------------------------------------- #
# Active-window resolution. The market-session facts come from the canonical
# engine.market_hours primitive; this module performs NO weekday or clock
# arithmetic of its own beyond comparing an ET wall-clock time to the policy's
# declared window.
# --------------------------------------------------------------------------- #
def _et_minutes(value: Any) -> Optional[int]:
    """'HH:MM' -> minutes past ET midnight. Returns None for an absent bound."""
    if not value:
        return None
    try:
        hh, mm = str(value).split(":")
        return int(hh) * 60 + int(mm)
    except (ValueError, AttributeError):
        return None


def resolve_window(*, policy: dict, now: datetime,
                   session: Optional[dict] = None) -> dict:
    """Is this source's collection window OPEN at ``now``? Pure.

    ``session`` is the canonical market-session fact block produced by
    ``engine.market_hours.session_state``; it is injected so a hermetic harness
    can drive a fake clock through a full week without touching the real one.
    """
    sess = session if session is not None else mh.session_state(now)
    req = policy["market_session_requirement"]
    if req == SESSION_NEVER or not policy["collection_enabled"]:
        return {"active": False, "reason": "This source is not collected on a clock.",
                "session_phase": sess["phase"], "market_session": sess}
    if req == SESSION_ANY:
        return {"active": True, "reason": "Publisher-driven; collected at any hour.",
                "session_phase": sess["phase"], "market_session": sess}
    if req == SESSION_OPEN_ONLY:
        active = bool(sess["regular_session_open"])
        return {"active": active,
                "reason": ("The regular session is open." if active else
                           "The regular session is closed (%s), so this source has "
                           "no new value to publish." % sess["phase"]),
                "session_phase": sess["phase"], "market_session": sess}
    # TRADING_DAY / AFTER_CLOSE both require a weekday first.
    if not sess["is_weekday"]:
        return {"active": False,
                "reason": "Not a weekday (%s); this source does not publish."
                          % sess["phase"],
                "session_phase": sess["phase"], "market_session": sess}
    start = _et_minutes(policy["active_window_start_et"])
    end = _et_minutes(policy["active_window_end_et"])
    if req == SESSION_AFTER_CLOSE and start is None:
        start = _et_minutes("17:30")
    minutes = int(sess["et_minutes"])
    lo = 0 if start is None else start
    hi = 24 * 60 if end is None else end
    active = lo <= minutes <= hi
    return {"active": active,
            "reason": ("Inside this source's %s-%s ET publication window."
                       % (policy["active_window_start_et"] or "00:00",
                          policy["active_window_end_et"] or "23:59")
                       if active else
                       "Outside this source's %s-%s ET publication window."
                       % (policy["active_window_start_et"] or "00:00",
                          policy["active_window_end_et"] or "23:59")),
            "session_phase": sess["phase"], "market_session": sess}


# --------------------------------------------------------------------------- #
# Runtime resolution — ONE state per source.
# --------------------------------------------------------------------------- #
def _dt(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _age_seconds(then: Optional[datetime], now: datetime) -> Optional[float]:
    if then is None:
        return None
    try:
        return max(0.0, (now - then).total_seconds())
    except TypeError:  # naive/aware mismatch — treat as unmeasurable, never crash
        return None


def resolve_source_runtime(*, policy: dict, state: Optional[dict], now: datetime,
                           session: Optional[dict] = None,
                           credential_available: Optional[bool] = None,
                           terminal_state: Optional[str] = None,
                           running: bool = False) -> dict:
    """THE per-source runtime row. Pure; one state, with the reason that produced it.

    ``state`` is this source's durable runtime record (last attempt, last success,
    failure streak, backoff, counters). Everything else is derived here so no
    caller — and above all no browser — re-derives a source's health.
    """
    sid = policy["source_id"]
    st = dict(state or {})
    window = resolve_window(policy=policy, now=now, session=session)
    last_attempt = _dt(st.get("last_attempt_at"))
    last_success = _dt(st.get("last_success_at"))
    last_new_info = _dt(st.get("last_new_information_at"))
    backoff_until = _dt(st.get("backoff_until"))
    failures = int(st.get("consecutive_failures") or 0)
    circuit = circuit_state_for(consecutive_failures=failures,
                                backoff_until=backoff_until, now=now)
    since_attempt = _age_seconds(last_attempt, now)
    since_success = _age_seconds(last_success, now)

    # next_due_at: when the scheduler may next CALL this collector.
    #
    # It is deliberately ABSENT while the source's own publication window is closed.
    # A weekend row that reads "does not publish today — next due 00:13" states two
    # contradictory things, and the second one is not true: no calendar here can say
    # when the window reopens (there is no exchange-holiday calendar), so the honest
    # answer is the reason, not a fabricated timestamp.
    if not policy["collection_enabled"] or not window["active"]:
        next_due = None
    elif last_attempt is None:
        next_due = now
    else:
        next_due = last_attempt + timedelta(
            seconds=policy["normal_interval_seconds"])
    if backoff_until is not None and next_due is not None and backoff_until > next_due:
        next_due = backoff_until

    interval_elapsed = (last_attempt is None or since_attempt is None
                        or since_attempt >= policy["minimum_call_interval_seconds"])
    due_by_clock = (last_attempt is None or since_attempt is None
                    or since_attempt >= policy["normal_interval_seconds"])
    in_backoff = backoff_until is not None and backoff_until > now

    max_stale = policy["maximum_staleness_seconds"]
    stale = (max_stale is not None and since_success is not None
             and since_success > max_stale)
    never_collected = last_success is None

    # ---- ONE state, resolved by explicit precedence --------------------------#
    reason: str
    if terminal_state and str(terminal_state).startswith("BLOCKED"):
        rs = RS_BLOCKED
        reason = "Terminally %s; there is nothing to collect." % terminal_state
    elif policy["cadence_kind"] == K_BLOCKED:
        rs = RS_BLOCKED
        reason = policy["why"]
    elif policy["cadence_kind"] == K_NOT_A_SOURCE:
        rs = RS_DISABLED
        reason = policy["why"]
    elif not policy["collection_enabled"]:
        rs = RS_DISABLED
        reason = policy["why"]
    elif credential_available is False:
        rs = RS_DEGRADED_CREDENTIAL
        reason = ("A required credential (%s) is not visible to the collection "
                  "worker, so this source cannot be collected. Other sources "
                  "continue." % ", ".join(policy["credential_env"]))
    elif circuit == CIRCUIT_OPEN:
        rs = RS_FAILED
        reason = ("%d consecutive failures opened the circuit; the next probe is "
                  "scheduled rather than retried in a loop." % failures)
    elif in_backoff:
        rs = RS_BACKOFF
        reason = ("In adaptive backoff after %s; not called until the window "
                  "expires." % (st.get("last_error_category") or "a failure"))
    elif running:
        rs = RS_RUNNING
        reason = "Being collected in the current iteration."
    elif not window["active"]:
        rs = RS_NOT_DUE
        reason = window["reason"]
    elif stale:
        rs = RS_DEGRADED
        reason = ("Inside its own publication window but the last successful "
                  "collection is older than this source's %.0f-minute staleness "
                  "tolerance." % (max_stale / 60.0))
    elif not policy["operational"]:
        rs = RS_RESEARCH_ONLY
        reason = ("Collected on cadence as research/observability evidence; it can "
                  "never reach the operational target.")
    elif never_collected or due_by_clock:
        rs = RS_DUE
        reason = ("Never collected by this service yet." if never_collected else
                  "Its %.0f-minute collection interval has elapsed."
                  % (policy["normal_interval_seconds"] / 60.0))
    else:
        rs = RS_FRESH
        reason = ("Collected %.0f minutes ago, inside its %.0f-minute interval."
                  % ((since_attempt or 0) / 60.0,
                     policy["normal_interval_seconds"] / 60.0))

    collect_now = bool(
        policy["collection_enabled"] and window["active"] and not in_backoff
        and circuit != CIRCUIT_OPEN and credential_available is not False
        and interval_elapsed and (due_by_clock or never_collected))

    return {
        "source_id": sid,
        "cadence_policy_id": policy["cadence_policy_id"],
        "cadence_kind": policy["cadence_kind"],
        "market_session_requirement": policy["market_session_requirement"],
        "active_window_et": ((policy["active_window_start_et"] or "00:00") + "-"
                             + (policy["active_window_end_et"] or "23:59")),
        "attention_tier": policy["attention_tier"],
        "collector_owner": policy["collector_owner"],
        "normal_interval_seconds": policy["normal_interval_seconds"],
        "minimum_call_interval_seconds": policy["minimum_call_interval_seconds"],
        "maximum_staleness_seconds": max_stale,
        "request_budget": dict(policy["request_budget"]),
        "operational": policy["operational"],
        "runtime_state": rs,
        "health_reason": reason,
        "due_window_active": bool(window["active"]),
        "due_window_reason": window["reason"],
        "session_phase": window["session_phase"],
        "collect_now": collect_now,
        "next_due_at": (next_due.isoformat() if next_due else None),
        "last_attempt_at": st.get("last_attempt_at"),
        "last_success_at": st.get("last_success_at"),
        "last_new_information_at": st.get("last_new_information_at"),
        "seconds_since_last_success": (None if since_success is None
                                       else round(since_success, 1)),
        "seconds_since_last_new_information": _age_seconds(last_new_info, now),
        "watermark": st.get("watermark"),
        "consecutive_failures": failures,
        "circuit_state": circuit,
        "backoff_until": st.get("backoff_until"),
        "last_error": st.get("last_error"),
        "last_error_category": st.get("last_error_category"),
        "last_http_status": st.get("last_http_status"),
        "rate_limit_count": int(st.get("rate_limit_count") or 0),
        "request_count_today": int(st.get("request_count_today") or 0),
        "new_event_count_today": int(st.get("new_event_count_today") or 0),
        "duplicate_count_today": int(st.get("duplicate_count_today") or 0),
        "credential_env": list(policy["credential_env"]),
        "credential_available": credential_available,
        "terminal_capability_state": terminal_state,
        "why_this_cadence": policy["why"],
    }


def summarize_runtime(rows: Iterable[dict]) -> dict:
    """THE authoritative source-health denominator the operator UI renders.

    Answers "of the sources that SHOULD be current right now, how many are
    healthy?" instead of "how many of every row we know about say FRESH".
    """
    rows = list(rows or [])
    by_state: dict[str, int] = {s: 0 for s in RUNTIME_STATES}
    for r in rows:
        s = str(r.get("runtime_state") or "")
        by_state[s] = by_state.get(s, 0) + 1
    # The OPERATIONAL denominator. A research/observability lane is collected on
    # its own cadence and reported under RESEARCH_ONLY, but it is deliberately not
    # counted here: it can never reach the operational target, so its health must
    # not dilute the answer to "is the decision surface being fed?".
    due_rows = [r for r in rows
                if r.get("due_window_active") and r.get("operational")]
    healthy_due = [r for r in due_rows
                   if str(r.get("runtime_state")) in HEALTHY_STATES]
    unhealthy = [r for r in rows
                 if str(r.get("runtime_state")) in UNHEALTHY_STATES]
    integrated = [r for r in rows
                  if str(r.get("runtime_state")) not in (RS_BLOCKED, RS_DISABLED)]
    return {
        "contract_id": "paper_trader.source_runtime_health_summary/1",
        "calculation_owner": CALCULATION_OWNER,
        "total_sources": len(rows),
        "integrated_sources": len(integrated),
        "due_now": len(due_rows),
        "healthy_due": len(healthy_due),
        "not_due": by_state.get(RS_NOT_DUE, 0),
        "backoff": by_state.get(RS_BACKOFF, 0),
        "degraded": by_state.get(RS_DEGRADED, 0),
        "failed": by_state.get(RS_FAILED, 0),
        "degraded_credential": by_state.get(RS_DEGRADED_CREDENTIAL, 0),
        "blocked": by_state.get(RS_BLOCKED, 0),
        "disabled": by_state.get(RS_DISABLED, 0),
        "research_only": by_state.get(RS_RESEARCH_ONLY, 0),
        "research_lane_due": sum(1 for r in rows if r.get("due_window_active")
                                 and not r.get("operational")),
        "fresh": by_state.get(RS_FRESH, 0),
        "due_and_uncollected": by_state.get(RS_DUE, 0),
        "unhealthy_source_ids": sorted(str(r.get("source_id")) for r in unhealthy),
        "by_runtime_state": by_state,
        "headline": ("%d of %d source(s) that should be current now are healthy"
                     % (len(healthy_due), len(due_rows))
                     if due_rows else
                     "No source is expected to publish right now"),
        "note": ("The denominator is the set of sources whose OWN publication "
                 "window is open at this moment — not every source in the "
                 "registry. A market feed on a Sunday and a monthly release "
                 "between publications are NOT_DUE, never degraded."),
    }


def next_wake_seconds(rows: Iterable[dict], *, now: datetime,
                      floor_seconds: float = MIN_ITERATION_INTERVAL_SECONDS,
                      ceiling_seconds: float = MAX_WAKE_SECONDS) -> float:
    """Seconds until the next meaningful due check. Bounded on both ends.

    The service wakes on this interval instead of a fixed minute, so a quiet
    Sunday night costs one cheap due-check every few minutes rather than 17
    provider calls.
    """
    best: Optional[float] = None
    for r in (rows or []):
        # A source whose publication window is CLOSED cannot become collectable
        # until the window opens, and a window only opens on a clock boundary the
        # ceiling already guarantees we re-check. Counting it here would drag every
        # wake down to the floor on a Sunday for no benefit.
        in_backoff = str(r.get("runtime_state")) in (RS_BACKOFF, RS_FAILED)
        if not r.get("due_window_active") and not in_backoff:
            continue
        candidate = r.get("backoff_until") if in_backoff else r.get("next_due_at")
        nd = _dt(candidate)
        if nd is None:
            continue
        delta = (nd - now).total_seconds()
        if delta < 0:
            delta = 0.0
        best = delta if best is None else min(best, delta)
    if best is None:
        return float(ceiling_seconds)
    return float(min(max(best, floor_seconds), ceiling_seconds))


def policy_contract() -> dict:
    """Machine-readable cadence contract. Persisted as release evidence."""
    return {
        "contract_id": CADENCE_CONTRACT_ID,
        "policy_version": CADENCE_POLICY_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "cadence_kinds": list(CADENCE_KINDS),
        "market_session_requirements": list(SESSION_REQUIREMENTS),
        "runtime_states": list(RUNTIME_STATES),
        "attention_tiers": list(ATTENTION_TIERS),
        "circuit_states": list(CIRCUIT_STATES),
        "error_categories": list(ERROR_CATEGORIES),
        "circuit_open_threshold": CIRCUIT_OPEN_THRESHOLD,
        "backoff_base_seconds": dict(BACKOFF_BASE_SECONDS),
        "backoff_ceiling_seconds": dict(BACKOFF_CEILING_SECONDS),
        "scheduled_source_ids": list(SCHEDULED_SOURCE_IDS),
        "sources": [dict(p) for p in CADENCE_POLICY_TABLE],
        "market_session_owner": "engine.market_hours",
        "note": ("Every cadence is justified against the source's actual "
                 "publication behaviour and entitlement. A source is expected to "
                 "be current only inside its own window."),
    }


__all__ = [
    "PHASE", "CALCULATION_OWNER", "CADENCE_POLICY_VERSION", "CADENCE_CONTRACT_ID",
    "CADENCE_KINDS", "SESSION_REQUIREMENTS", "RUNTIME_STATES", "ATTENTION_TIERS",
    "CIRCUIT_STATES", "ERROR_CATEGORIES", "DUE_WINDOW_STATES", "HEALTHY_STATES",
    "UNHEALTHY_STATES", "CIRCUIT_OPEN_THRESHOLD",
    "K_CONTINUOUS_EVENT", "K_INTRADAY_MARKET", "K_SESSION_END",
    "K_DAILY_PUBLICATION", "K_MONTHLY_RELEASE", "K_QUARTERLY_RELEASE",
    "K_LOCAL_FILE_WATCH", "K_INTERNAL_EVENT", "K_RESEARCH_ONLY", "K_BLOCKED",
    "K_NOT_A_SOURCE",
    "SESSION_ANY", "SESSION_OPEN_ONLY", "SESSION_TRADING_DAY",
    "SESSION_AFTER_CLOSE", "SESSION_NEVER",
    "RS_FRESH", "RS_DUE", "RS_RUNNING", "RS_NOT_DUE", "RS_BACKOFF", "RS_DEGRADED",
    "RS_FAILED", "RS_BLOCKED", "RS_RESEARCH_ONLY", "RS_DISABLED",
    "RS_DEGRADED_CREDENTIAL",
    "TIER_HOLDINGS", "TIER_CANDIDATES", "TIER_UNIVERSE", "TIER_GLOBAL",
    "CIRCUIT_CLOSED", "CIRCUIT_HALF_OPEN", "CIRCUIT_OPEN",
    "ERR_RATE_LIMIT", "ERR_SERVER", "ERR_TIMEOUT", "ERR_AUTH", "ERR_CLIENT",
    "ERR_UNKNOWN",
    "CADENCE_POLICY_TABLE", "CADENCE_POLICY_BY_ID", "SCHEDULED_SOURCE_IDS",
    "MIN_ITERATION_INTERVAL_SECONDS", "DEFAULT_ITERATION_INTERVAL_SECONDS",
    "MAX_WAKE_SECONDS",
    "policy_for", "classify_http_error", "backoff_seconds", "circuit_state_for",
    "resolve_window", "resolve_source_runtime", "summarize_runtime",
    "next_wake_seconds", "policy_contract",
]
