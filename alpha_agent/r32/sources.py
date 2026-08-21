"""alpha_agent.r32.sources - the ONE Release 32 data source registry owner.

Phase 1. Inventories what this project ALREADY OWNS at zero marginal cost, and
decides - by MEASUREMENT, not by assertion - whether each source may be used as
historical point-in-time information.

The central finding this module exists to encode:

    A daily series is not automatically point-in-time.

Norgate's ``Economic`` database carries 144 macro series, many with history back
to the 1940s, all quoted every business day. It is tempting to treat them as
observable state variables. They are not. Two independent defects:

1. **Reference-period dating.** ``sample_change_dates`` measures when each series
   changes value. Every statistical release in that database changes on the
   FIRST BUSINESS DAY OF THE PERIOD IT MEASURES - CPI for month M appears on day
   one of month M, GDP for a quarter appears on day one of that quarter. The
   actual publication is weeks later. Reading the series at its own timestamp is
   therefore look-ahead of roughly one publication lag, every single period.
2. **Revision-current values.** The stored number is today's revised vintage,
   not what was printed at the time.

Either defect alone disqualifies the series as historical real-time information.
So this module classifies by the measured change-day fingerprint and stamps the
statistical releases ``REVISED_NOT_PIT``. Market observables - yields, index
levels, volatility indices, FX, commodity indices - change on most trading days
because they ARE the market's real-time opinion, and are admissible.

``docs/INFORMATION_PURCHASE_GATE.md`` governs what happens when a source is
blocked: it becomes a purchase CANDIDATE, never a fabricated feature.
"""
from __future__ import annotations

import datetime as _dt
import logging
from pathlib import Path
from typing import Optional

from .. import r32
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r32.sources"
REGISTRY_SCHEMA = "r32_data_source_registry/1"
ARTIFACT_NAME = "data_source_registry.json"

# --------------------------------------------------------------------------- #
# Admissibility states
# --------------------------------------------------------------------------- #
#: Observable in real time at its own timestamp. Admissible as history.
PIT_MARKET_OBSERVABLE = "PIT_MARKET_OBSERVABLE"

#: Carries an explicit as-of/vintage timestamp distinct from the reference
#: period, so the value known at time t can be reconstructed. Admissible.
PIT_VINTAGE_DATED = "PIT_VINTAGE_DATED"

#: Stamped at the reference period and/or carrying revised values. NOT
#: admissible as historical information at its own timestamp.
REVISED_NOT_PIT = "REVISED_NOT_PIT"

#: Only a current snapshot exists. Usable to describe TODAY, never backwards.
CURRENT_SNAPSHOT_ONLY = "CURRENT_SNAPSHOT_ONLY"

#: Owned but its coverage is too thin or too survivorship-skewed to carry a
#: verdict. May support, never decide.
COVERAGE_LIMITED = "COVERAGE_LIMITED"

#: Present but unusable for research: licence, format, or measurement blocker.
SOURCE_BLOCKED = "SOURCE_BLOCKED"

ADMISSIBILITY_STATES = (
    PIT_MARKET_OBSERVABLE, PIT_VINTAGE_DATED, REVISED_NOT_PIT,
    CURRENT_SNAPSHOT_ONLY, COVERAGE_LIMITED, SOURCE_BLOCKED,
)

#: Alias kept so consumers can enumerate the full set without implying that
#: every member is admissible - the name ``ADMISSIBILITY_STATES`` reads that way.
ADMISSIBLE_STATES_ALL = ADMISSIBILITY_STATES

#: The states a sleeve may build a historical feature from.
ADMISSIBLE_FOR_HISTORY = (PIT_MARKET_OBSERVABLE, PIT_VINTAGE_DATED)

# --------------------------------------------------------------------------- #
# The measured classifier
# --------------------------------------------------------------------------- #
#: A series whose value changes on at most this fraction of observations is not
#: a market observable; it is a periodically stamped statistic.
MARKET_CHANGE_RATE_FLOOR = 0.50

#: ... and if those rare changes land on the first business days of a period,
#: the series is reference-period dated, which is look-ahead.
PERIOD_START_DAY_MAX = 4
PERIOD_START_FRACTION_FLOOR = 0.80


def classify_change_fingerprint(change_days: list, n_observations: int,
                                n_changes: int) -> dict:
    """Classify a series from WHEN it changes value. Pure; no I/O.

    ``change_days`` is the day-of-month of each observed value change.

    This is the whole point-in-time test, expressed as arithmetic so it can be
    unit-tested with synthetic inputs and cannot drift into an opinion.
    """
    if n_observations <= 0:
        return {"admissibility": SOURCE_BLOCKED,
                "reason": "NO_OBSERVATIONS",
                "change_rate": None, "period_start_fraction": None}
    change_rate = float(n_changes) / float(n_observations)
    if change_rate >= MARKET_CHANGE_RATE_FLOOR:
        return {"admissibility": PIT_MARKET_OBSERVABLE,
                "reason": "CHANGES_ON_MOST_OBSERVATIONS",
                "change_rate": change_rate, "period_start_fraction": None}
    if not change_days:
        return {"admissibility": SOURCE_BLOCKED,
                "reason": "CONSTANT_SERIES",
                "change_rate": change_rate, "period_start_fraction": None}
    at_start = sum(1 for d in change_days if int(d) <= PERIOD_START_DAY_MAX)
    frac = float(at_start) / float(len(change_days))
    if frac >= PERIOD_START_FRACTION_FLOOR:
        return {
            "admissibility": REVISED_NOT_PIT,
            "reason": "STAMPED_AT_REFERENCE_PERIOD_START_NOT_PUBLICATION_DATE",
            "change_rate": change_rate, "period_start_fraction": frac}
    return {"admissibility": COVERAGE_LIMITED,
            "reason": "INFREQUENT_UPDATES_PUBLICATION_DATING_UNPROVEN",
            "change_rate": change_rate, "period_start_fraction": frac}


# --------------------------------------------------------------------------- #
# Norgate access (read-only)
# --------------------------------------------------------------------------- #
NORGATE_DATABASES = (
    "US Equities", "US Equities Delisted", "US Indices", "World Indices",
    "Forex Spot", "Cash Commodities", "Continuous Futures", "Economic",
)


def _norgate():
    logging.disable(logging.WARNING)
    import norgatedata as nd  # imported lazily: absent in CI
    return nd


def measure_series(symbol: str, *, nd=None, since: str = "2010-01-01") -> dict:
    """Measure one Norgate series' change fingerprint. Read-only."""
    nd = nd or _norgate()
    out = {"symbol": symbol}
    try:
        df = nd.price_timeseries(symbol, format="pandas-dataframe",
                                 start_date=since)
    except Exception as exc:  # noqa: BLE001
        out.update({"admissibility": SOURCE_BLOCKED,
                    "reason": f"PROBE_FAILED_{type(exc).__name__}",
                    "error": str(exc)[:200]})
        return out
    if df is None or len(df) == 0:
        out.update({"admissibility": SOURCE_BLOCKED, "reason": "EMPTY_SERIES"})
        return out
    col = "Close" if "Close" in df.columns else df.columns[-1]
    close = df[col]
    changed = close.ne(close.shift())
    changed.iloc[0] = False           # the first observation is not a change
    change_index = close.index[changed]
    verdict = classify_change_fingerprint(
        [d.day for d in change_index], int(len(close)), int(len(change_index)))
    out.update(verdict)
    out.update({"observations": int(len(close)),
                "changes": int(len(change_index)),
                "first": str(close.index[0])[:10],
                "last": str(close.index[-1])[:10]})
    return out


def measure_database(db: str, *, nd=None, sample: int = 0,
                     since: str = "2010-01-01") -> dict:
    """Measure a whole Norgate database. ``sample`` 0 means every symbol."""
    nd = nd or _norgate()
    try:
        symbols = list(nd.database_symbols(db))
    except Exception as exc:  # noqa: BLE001
        return {"database": db, "available": False,
                "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    probe = symbols if not sample else symbols[:sample]
    rows = [measure_series(s, nd=nd, since=since) for s in probe]
    by_state = {}
    for r in rows:
        by_state.setdefault(r.get("admissibility"), []).append(r["symbol"])
    return {"database": db, "available": True, "symbols": len(symbols),
            "probed": len(rows),
            "by_admissibility": {k: len(v) for k, v in by_state.items()},
            "symbols_by_admissibility": {k: sorted(v)[:400]
                                         for k, v in by_state.items()},
            "rows": rows}


# --------------------------------------------------------------------------- #
# The declared zero-cost source inventory
# --------------------------------------------------------------------------- #
#: Every source this project can read without spending money. ``cost`` is the
#: MARGINAL cost of using it for Release 32 research, which is zero for
#: everything here - subscriptions already paid for are not a new spend, and
#: Release 32 may not create one.
ZERO_COST_SOURCES = (
    {"source_id": "norgate_us_equities",
     "provider": "Norgate Data", "family": "PRICE_EQUITY",
     "access": "LOCAL_SUBSCRIPTION", "marginal_cost_usd": 0.0,
     "note": "point-in-time index membership including delisted securities; "
             "the Release 31 primary sample was built from this"},
    {"source_id": "norgate_us_indices",
     "provider": "Norgate Data", "family": "INDEX_LEVEL",
     "access": "LOCAL_SUBSCRIPTION", "marginal_cost_usd": 0.0,
     "note": "index levels, total-return variants, GICS sector indices, market "
             "breadth, volatility indices, put/call ratios"},
    {"source_id": "norgate_world_indices",
     "provider": "Norgate Data", "family": "INDEX_LEVEL",
     "access": "LOCAL_SUBSCRIPTION", "marginal_cost_usd": 0.0},
    {"source_id": "norgate_forex_spot",
     "provider": "Norgate Data", "family": "FX",
     "access": "LOCAL_SUBSCRIPTION", "marginal_cost_usd": 0.0},
    {"source_id": "norgate_cash_commodities",
     "provider": "Norgate Data", "family": "COMMODITY",
     "access": "LOCAL_SUBSCRIPTION", "marginal_cost_usd": 0.0},
    {"source_id": "norgate_continuous_futures",
     "provider": "Norgate Data", "family": "FUTURES",
     "access": "LOCAL_SUBSCRIPTION", "marginal_cost_usd": 0.0},
    {"source_id": "norgate_economic",
     "provider": "Norgate Data", "family": "MACRO_AND_RATES",
     "access": "LOCAL_SUBSCRIPTION", "marginal_cost_usd": 0.0,
     "note": "MIXED: market-observable yields and bond total-return indices "
             "sit in the same database as reference-period-dated statistical "
             "releases. Classified per series, never per database."},
    {"source_id": "sec_edgar",
     "provider": "SEC EDGAR", "family": "FILINGS",
     "access": "PUBLIC_HTTP", "marginal_cost_usd": 0.0},
    {"source_id": "fred_alfred",
     "provider": "St. Louis Fed ALFRED", "family": "MACRO_VINTAGE",
     "access": "PUBLIC_API", "marginal_cost_usd": 0.0,
     "note": "the ONLY genuinely vintage-dated macro source owned. Vintages "
             "begin ~2000, which caps how far back macro can legitimately go."},
    {"source_id": "us_treasury",
     "provider": "US Treasury", "family": "RATES",
     "access": "PUBLIC_API", "marginal_cost_usd": 0.0},
    {"source_id": "eodhd",
     "provider": "EODHD", "family": "FUNDAMENTAL_AND_PRICE",
     "access": "PAID_SUBSCRIPTION_ALREADY_HELD", "marginal_cost_usd": 0.0},
    {"source_id": "finra", "provider": "FINRA", "family": "SHORT_INTEREST",
     "access": "PUBLIC_HTTP", "marginal_cost_usd": 0.0},
    {"source_id": "nasdaq_trader", "provider": "Nasdaq Trader",
     "family": "SYMBOL_DIRECTORY", "access": "PUBLIC_HTTP",
     "marginal_cost_usd": 0.0},
    {"source_id": "bls", "provider": "US BLS", "family": "MACRO_RELEASE",
     "access": "PUBLIC_API", "marginal_cost_usd": 0.0},
    {"source_id": "bea", "provider": "US BEA", "family": "MACRO_RELEASE",
     "access": "PUBLIC_API", "marginal_cost_usd": 0.0},
    {"source_id": "gdelt", "provider": "GDELT", "family": "NEWS_EVENT",
     "access": "PUBLIC_HTTP", "marginal_cost_usd": 0.0,
     "note": "article metadata only. Article TEXT may never become a feature "
             "silently; see the prohibited-substitution list."},
)

#: Substitutions that are forbidden however convenient. Each one has produced a
#: plausible, wrong result somewhere in this project's history.
PROHIBITED_SUBSTITUTIONS = (
    {"forbidden": "revised macro history used as real-time information",
     "instead": "ALFRED vintages, or declare the period unmeasurable"},
    {"forbidden": "current analyst snapshots used as historical revisions",
     "instead": "a prospective revision ledger, forward only"},
    {"forbidden": "GDELT article text used as an alpha feature",
     "instead": "event timestamps only, and only where PIT-defensible"},
    {"forbidden": "external reference links used as features",
     "instead": "nothing; a link is not an observation"},
    {"forbidden": "current sector membership applied backward in time",
     "instead": "declare historical sector UNMEASURABLE_PIT"},
    {"forbidden": "ETF history extended before the fund's inception",
     "instead": "the underlying index, when one legitimately exists"},
)


def build(*, campaign_id: str = _contract.CAMPAIGN_ID,
          measurements: Optional[dict] = None,
          created_at: Optional[str] = None) -> dict:
    """Assemble the data source registry artifact."""
    measurements = measurements or {}
    sources = []
    for src in ZERO_COST_SOURCES:
        row = dict(src)
        m = measurements.get(src["source_id"])
        if m:
            row["measured"] = m
        sources.append(row)
    total_cost = sum(float(s.get("marginal_cost_usd") or 0.0) for s in sources)
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at or _dt.datetime.now().isoformat(timespec="seconds"),
        "admissibility_states": list(ADMISSIBILITY_STATES),
        "admissible_for_history": list(ADMISSIBLE_FOR_HISTORY),
        "classifier": {
            "method": "MEASURED_CHANGE_DAY_FINGERPRINT",
            "market_change_rate_floor": MARKET_CHANGE_RATE_FLOOR,
            "period_start_day_max": PERIOD_START_DAY_MAX,
            "period_start_fraction_floor": PERIOD_START_FRACTION_FLOOR,
            "rationale":
                "A daily series is not automatically point-in-time. A value "
                "that first appears on the first business day of the period it "
                "measures cannot have been known then.",
        },
        "sources": sources,
        "source_count": len(sources),
        "total_marginal_cost_usd": total_cost,
        "prohibited_substitutions": list(PROHIBITED_SUBSTITUTIONS),
    }
    body = r32.artifact_body(REGISTRY_SCHEMA, payload)
    body["registry_hash"] = r32.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r32.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = _contract.CAMPAIGN_ID) -> Optional[dict]:
    return r32.read_json(path_for(campaign_id))
