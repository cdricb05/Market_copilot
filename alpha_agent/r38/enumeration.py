"""alpha_agent.r38.enumeration - Phase 2: enumerate what was actually bought.

Builds the immutable FUTURES_MARKET_REGISTRY and DATED_CONTRACT_REGISTRY from
delivered bytes - never from the vendor's marketing site. Every number in the
registries is the result of a local API answer.

Bounded by design: contract SYMBOL lists are recorded in full; per-contract
metadata (first/last quoted, first notice) is probed for the oldest contract,
the newest contract and a small evenly spaced sample per market, because a
metadata call per each of ~27,000 contracts would take hours for information
the quality phase samples anyway. The bound is declared in the artifact
(``per_contract_metadata_policy``), so nothing reads as fuller than it is.
"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Any, Callable, Optional

from .. import r38
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r38.enumeration"
MARKET_SCHEMA = "r38_futures_market_registry/1"
CONTRACT_SCHEMA = "r38_dated_contract_registry/1"
MARKET_ARTIFACT = C.ARTIFACT_NAMES["futures_market_registry"]
CONTRACT_ARTIFACT = C.ARTIFACT_NAMES["dated_contract_registry"]

#: Per-market cap on per-contract metadata probes (oldest + newest + evenly
#: spaced interior samples). The full symbol census is always complete.
CONTRACT_METADATA_SAMPLES_PER_MARKET = 8

#: A market whose newest contract stopped quoting more than this many days
#: before the freshest quote in the whole delivery is recorded INACTIVE.
INACTIVE_AFTER_DAYS = 45

_MONTH_CODES = "FGHJKMNQUVXZ"
_CONTRACT_RE = re.compile(
    r"^(?P<root>.+?)-(?P<year>\d{4})(?P<month>[" + _MONTH_CODES + r"])$")


def _nd():
    import norgatedata as nd
    return nd


def _attempt(fn: Callable, *args: Any) -> dict:
    try:
        return {"ok": True, "value": fn(*args)}
    except Exception as exc:
        return {"ok": False, "error_type": type(exc).__name__,
                "error": repr(exc)}


def _value(outcome: dict, default=None):
    return outcome.get("value") if outcome.get("ok") else default


def parse_contract_symbol(symbol: str) -> dict:
    """Delivery month/year from a dated symbol like ``ES-2026U``."""
    m = _CONTRACT_RE.match(str(symbol))
    if not m:
        return {"symbol": symbol, "parsed": False,
                "delivery_year": None, "delivery_month": None}
    return {"symbol": symbol, "parsed": True,
            "delivery_year": int(m.group("year")),
            "delivery_month": _MONTH_CODES.index(m.group("month")) + 1}


def sessions_for_market(market: str, session_symbols: list) -> list:
    """Session symbols belonging to one market: the exact symbol plus
    digit-suffixed session variants (``FDAX`` -> ``FDAX``, ``FDAX9``)."""
    out = []
    for s in session_symbols:
        if s == market:
            out.append(s)
        elif s.startswith(market) and s[len(market):].isdigit():
            out.append(s)
    return sorted(out, key=lambda s: (len(s), s))


def classify_market(market: str) -> dict:
    row = C.MARKET_GROUPS.get(market)
    if row is None:
        return {"asset_class": C.UNCLASSIFIED, "economic_group": C.UNCLASSIFIED,
                "cost_group": C.UNCLASSIFIED, "declared": False}
    asset_class, group, cost_group = row
    return {"asset_class": asset_class, "economic_group": group,
            "cost_group": cost_group, "declared": True}


def _sample_indices(n: int, k: int) -> list:
    if n <= k:
        return list(range(n))
    step = (n - 1) / float(k - 1)
    return sorted({int(round(i * step)) for i in range(k)})


def _price_fields_probe(nd, symbol: str) -> dict:
    """Which OHLC/settlement/volume/OI fields the delivered series carries."""
    try:
        df = nd.price_timeseries(symbol, timeseriesformat="pandas-dataframe")
    except Exception as exc:
        return {"ok": False, "error_type": type(exc).__name__,
                "error": repr(exc)}
    if df is None or not len(df):
        return {"ok": True, "rows": 0, "fields": []}
    return {"ok": True, "rows": int(len(df)),
            "fields": [str(c) for c in df.columns],
            "first_date": str(df.index.min().date()),
            "last_date": str(df.index.max().date())}


def enumerate_market(nd, market: str, session_symbols: list) -> dict:
    """One market's registry row, from delivered answers only."""
    sessions = sessions_for_market(market, session_symbols)
    primary = sessions[0] if sessions else market

    name = _attempt(nd.futures_market_name, market)
    session_rows = {}
    primary_contracts: list = []
    for s in sessions:
        contracts = _attempt(nd.futures_market_session_contracts, s)
        symbols = _value(contracts, []) or []
        session_rows[s] = {
            "session_name": _value(_attempt(nd.futures_market_session_name, s)),
            "session_type": _value(_attempt(nd.futures_market_session_type, s)),
            "contract_count": len(symbols),
            "enumeration_ok": bool(contracts.get("ok")),
        }
        if s == primary:
            primary_contracts = symbols

    parsed = [parse_contract_symbol(s) for s in primary_contracts]
    years = [p["delivery_year"] for p in parsed if p["parsed"]]
    ordered = sorted(
        (p for p in parsed if p["parsed"]),
        key=lambda p: (p["delivery_year"], p["delivery_month"]))
    oldest = ordered[0]["symbol"] if ordered else (
        primary_contracts[0] if primary_contracts else None)
    newest = ordered[-1]["symbol"] if ordered else (
        primary_contracts[-1] if primary_contracts else None)

    detail = {}
    idx = _sample_indices(len(ordered), CONTRACT_METADATA_SAMPLES_PER_MARKET)
    for i in idx:
        sym = ordered[i]["symbol"]
        detail[sym] = {
            "first_quoted": _value(_attempt(nd.first_quoted_date, sym)),
            "last_quoted": _value(_attempt(nd.last_quoted_date, sym)),
            "first_notice": _value(_attempt(nd.first_notice_date, sym)),
        }

    meta_symbol = newest
    meta = {}
    if meta_symbol:
        meta = {
            "security_name": _value(_attempt(nd.security_name, meta_symbol)),
            "exchange": _value(_attempt(nd.exchange_name, meta_symbol)),
            "exchange_full":
                _value(_attempt(nd.exchange_name_full, meta_symbol)),
            "currency": _value(_attempt(nd.currency, meta_symbol)),
            "point_value": _value(_attempt(nd.point_value, meta_symbol)),
            "tick_size": _value(_attempt(nd.tick_size, meta_symbol)),
            "lowest_ever_tick_size":
                _value(_attempt(nd.lowest_ever_tick_size, meta_symbol)),
            "margin": _value(_attempt(nd.margin, meta_symbol)),
        }

    first_quoted = None
    last_quoted = None
    if oldest and oldest in detail:
        first_quoted = detail[oldest]["first_quoted"]
    elif oldest:
        first_quoted = _value(_attempt(nd.first_quoted_date, oldest))
    if newest and newest in detail:
        last_quoted = detail[newest]["last_quoted"]
    elif newest:
        last_quoted = _value(_attempt(nd.last_quoted_date, newest))

    fields = _price_fields_probe(nd, newest) if newest else {"ok": False}

    # Activity must be judged near the FRONT of the curve: the newest listed
    # contract of an active market can be a far-dated strip month that trades
    # rarely, and the calendar-front contract of a market that ceases trading
    # well before its delivery month (Brent) can already have expired. The
    # freshest bar among the next three undelivered months is robust to both.
    today = _dt.date.today()
    front_idx = None
    for i, p in enumerate(ordered):
        if _dt.date(p["delivery_year"], p["delivery_month"], 28) >= today:
            front_idx = i
            break
    if front_idx is None and ordered:
        front_idx = len(ordered) - 1
    front = ordered[front_idx]["symbol"] if front_idx is not None else newest
    front_probe = _price_fields_probe(nd, front) if front else {"ok": False}
    recent_last_bar = front_probe.get("last_date")
    if front_idx is not None:
        for p in ordered[front_idx + 1:front_idx + 3]:
            probe = _price_fields_probe(nd, p["symbol"])
            candidate = probe.get("last_date")
            if candidate and (recent_last_bar is None
                              or candidate > recent_last_bar):
                recent_last_bar = candidate

    row = {
        "market": market,
        "market_name": _value(name),
        "sessions": session_rows,
        "primary_session": primary,
        "contract_count_primary_session": len(primary_contracts),
        "contract_symbols_parseable": len(ordered),
        "delivery_years": {
            "min": min(years) if years else None,
            "max": max(years) if years else None,
        },
        "oldest_contract": oldest,
        "newest_contract": newest,
        "first_quoted_date": first_quoted,
        "last_quoted_date": last_quoted,
        "first_notice_available": any(
            d.get("first_notice") for d in detail.values()),
        "sampled_contract_detail": detail,
        "metadata": meta,
        "price_fields_probe": fields,
        "front_contract": front,
        "front_price_probe": front_probe,
        "recent_last_bar": recent_last_bar,
    }
    row.update(classify_market(market))
    return row


def build(*, campaign_id: str = C.CAMPAIGN_ID,
          created_at: Optional[str] = None,
          progress: Optional[Callable[[str], None]] = None) -> dict:
    """Enumerate every delivered market. Returns both artifact bodies."""
    nd = _nd()
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    markets = sorted(_value(_attempt(nd.futures_market_symbols), []) or [])
    session_symbols = _value(
        _attempt(nd.futures_market_session_symbols), []) or []

    rows = {}
    contract_lists = {}
    for market in markets:
        row = enumerate_market(nd, market, session_symbols)
        rows[market] = row
        # Full census: every session's complete symbol list.
        lists = {}
        for s in row["sessions"]:
            out = _attempt(nd.futures_market_session_contracts, s)
            lists[s] = _value(out, []) or []
        contract_lists[market] = lists
        if progress is not None:
            progress("%s: %d contracts" % (
                market, row["contract_count_primary_session"]))

    # ---- aggregates, all computed from the rows just measured ----
    # Activity is judged on ``recent_last_bar`` (freshest bar among the next
    # three undelivered months) - never on ``last_quoted_date`` (a live
    # contract's scheduled final trading day lies in the future) and never on
    # the newest LISTED contract (a far strip month can trade rarely).
    last_bar_dates = [r.get("recent_last_bar") for r in rows.values()
                      if r.get("recent_last_bar")]
    freshest = max(last_bar_dates) if last_bar_dates else None
    freshest_day = (_dt.date.fromisoformat(freshest[:10])
                    if freshest else None)
    for r in rows.values():
        state = "UNKNOWN"
        last_bar = r.get("recent_last_bar")
        if last_bar and freshest_day is not None:
            age = (freshest_day - _dt.date.fromisoformat(last_bar[:10])).days
            state = "ACTIVE" if age <= INACTIVE_AFTER_DAYS else "INACTIVE"
        r["activity_state"] = state

    def _count_by(key: str) -> dict:
        out: dict = {}
        for r in rows.values():
            out[r.get(key) or "UNKNOWN"] = out.get(r.get(key) or "UNKNOWN", 0) + 1
        return dict(sorted(out.items()))

    distinct_contracts = {
        m: len({sym for lst in lists.values() for sym in lst})
        for m, lists in contract_lists.items()}
    primary_contracts_total = sum(
        r["contract_count_primary_session"] for r in rows.values())

    history_distribution: dict = {}
    for r in rows.values():
        fq = r["first_quoted_date"]
        bucket = (fq[:3] + "0s") if fq else "UNKNOWN"
        history_distribution[bucket] = history_distribution.get(bucket, 0) + 1

    metadata_coverage = {
        field: sum(1 for r in rows.values()
                   if (r["metadata"] or {}).get(field) is not None)
        for field in ("point_value", "tick_size", "margin", "currency",
                      "exchange", "lowest_ever_tick_size")}
    fields_seen: dict = {}
    for r in rows.values():
        for f in (r["price_fields_probe"] or {}).get("fields", []):
            fields_seen[f] = fields_seen.get(f, 0) + 1

    exchange_counts: dict = {}
    for r in rows.values():
        key = (r["metadata"] or {}).get("exchange") or "UNKNOWN"
        exchange_counts[key] = exchange_counts.get(key, 0) + 1

    market_payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "source": "Norgate Data Updater local 'Futures' database, delivered "
                  "2026-08-22 (NDU log: price_future distribution at "
                  "16:59:57 ET)",
        "total_futures_markets": len(markets),
        "total_dated_contracts_primary_sessions": primary_contracts_total,
        "total_dated_contracts_distinct": sum(distinct_contracts.values()),
        "markets_by_asset_class": _count_by("asset_class"),
        "markets_by_economic_group": _count_by("economic_group"),
        "markets_by_exchange": dict(sorted(exchange_counts.items())),
        "markets_by_activity_state": _count_by("activity_state"),
        "history_distribution_by_first_quoted_decade":
            dict(sorted(history_distribution.items())),
        "metadata_coverage_counts": metadata_coverage,
        "price_field_availability_counts": dict(sorted(fields_seen.items())),
        "unclassified_markets": sorted(
            m for m, r in rows.items() if not r["declared"]),
        "per_contract_metadata_policy": {
            "samples_per_market": CONTRACT_METADATA_SAMPLES_PER_MARKET,
            "note": "full symbol census; per-contract dates probed for "
                    "oldest, newest and evenly spaced interior samples only",
        },
        "markets": rows,
    }

    contract_payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "total_markets": len(markets),
        "distinct_contract_counts": distinct_contracts,
        "contract_symbols": contract_lists,
    }
    return {
        "market_registry": r38.artifact_body(MARKET_SCHEMA, market_payload),
        "contract_registry": r38.artifact_body(CONTRACT_SCHEMA,
                                               contract_payload),
    }


def market_path(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / MARKET_ARTIFACT


def contract_path(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / CONTRACT_ARTIFACT


def freeze(built: dict) -> None:
    market = built["market_registry"]
    contracts = built["contract_registry"]
    mp = market_path(market["campaign_id"])
    mp.parent.mkdir(parents=True, exist_ok=True)
    r38.write_json(mp, market)
    r38.write_json(contract_path(contracts["campaign_id"]), contracts)


def load_market_registry(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    path = market_path(campaign_id)
    if not path.exists():
        return None
    return r38.read_json(path)


def load_contract_registry(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    path = contract_path(campaign_id)
    if not path.exists():
        return None
    return r38.read_json(path)
