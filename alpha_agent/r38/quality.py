"""alpha_agent.r38.quality - Phase 3: validate the actual delivered data.

A deliberately DIVERSE validation sample - old expired contracts, recent
expired contracts, live contracts, every delivered asset class - is inspected
for the failure modes that would poison research downstream: missing OHLC,
duplicate or non-monotonic dates, silent gaps, absurd discontinuities,
volume/open-interest absence, expiry-identity mismatches and missing
first-notice metadata on physically delivered markets.

No Alpha conclusion is drawn here (``ALPHA_CONCLUSIONS_IN_THIS_MODULE`` is
False and stays False): a validation sample proves the BYTES, not a strategy.
Checksums and provenance are persisted for every series inspected.
"""
from __future__ import annotations

import datetime as _dt
import hashlib as _hashlib
from typing import Any, Callable, Optional

from .. import r38
from . import contract as C
from . import enumeration as EN

CALCULATION_OWNER = "alpha_agent.r38.quality"
SCHEMA = "r38_contract_data_quality_report/1"
ARTIFACT_NAME = C.ARTIFACT_NAMES["contract_data_quality_report"]

ALPHA_CONCLUSIONS_IN_THIS_MODULE = False

#: The frozen diverse validation set - one entry per release-mandated slot,
#: every market confirmed deliverable by the Phase-2 enumeration.
VALIDATION_MARKETS = {
    "ENERGY": ("CL", "NG", "BRN", "HO"),
    "METALS": ("GC", "SI", "HG"),
    "GRAINS": ("ZC", "ZW", "ZS"),
    "SOFTS": ("KC", "SB", "CC"),
    "LIVESTOCK": ("LE", "HE"),
    "RATES": ("ZT", "ZF", "ZN", "ZB", "SR3"),
    "FX": ("6E", "6J", "6B"),
    "US_EQUITY_INDEX": ("ES", "NQ", "RTY"),
    "INTERNATIONAL_INDEX": ("FDAX", "FESX", "SNK"),
    "VOLATILITY": ("VX",),
}

#: |log return| beyond this is counted as a suspicious discontinuity. Wide on
#: purpose: limit moves are real, and only pathological jumps should count.
DISCONTINUITY_ABS_LOG_RETURN = 0.5

#: An intra-life gap of more than this many calendar days between consecutive
#: bars is counted as a coverage gap.
GAP_CALENDAR_DAYS = 7


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


def select_contracts(ordered_symbols: list, *, today: _dt.date) -> dict:
    """Old-expired, mid-history-expired, recently-expired and current picks.

    ``ordered_symbols`` must already be delivery-ordered. Expiry identity is
    taken from the parsed delivery month, never from observed prices.
    """
    parsed = [EN.parse_contract_symbol(s) for s in ordered_symbols]
    parsed = [p for p in parsed if p["parsed"]]
    expired = [p for p in parsed
               if _dt.date(p["delivery_year"], p["delivery_month"], 1)
               < _dt.date(today.year, today.month, 1)]
    live = [p for p in parsed if p not in expired]
    picks = {}
    if expired:
        picks["old_expired"] = expired[0]["symbol"]
        picks["mid_expired"] = expired[len(expired) // 2]["symbol"]
        picks["recent_expired"] = expired[-1]["symbol"]
    if live:
        picks["current_front"] = live[0]["symbol"]
    return picks


def inspect_series(nd, symbol: str) -> dict:
    """Every structural check on one delivered contract series."""
    import numpy as np

    try:
        df = nd.price_timeseries(symbol, timeseriesformat="pandas-dataframe")
    except Exception as exc:
        return {"symbol": symbol, "readable": False,
                "error_type": type(exc).__name__, "error": repr(exc)}
    if df is None or not len(df):
        return {"symbol": symbol, "readable": True, "rows": 0,
                "empty": True}

    fields = [str(c) for c in df.columns]
    close = df["Close"] if "Close" in df.columns else None
    checks: dict = {
        "symbol": symbol,
        "readable": True,
        "empty": False,
        "rows": int(len(df)),
        "fields": fields,
        "first_bar": str(df.index.min().date()),
        "last_bar": str(df.index.max().date()),
        "index_timezone": str(df.index.tz) if df.index.tz else "NAIVE_DAILY",
        "index_monotonic": bool(df.index.is_monotonic_increasing),
        "duplicate_dates": int(len(df) - df.index.nunique()),
        "has_ohlc": all(f in fields for f in ("Open", "High", "Low", "Close")),
        "has_volume": "Volume" in fields,
        "has_open_interest": "Open Interest" in fields,
        "nan_close": int(close.isna().sum()) if close is not None else None,
        "nonpositive_close": (int((close <= 0).sum())
                              if close is not None else None),
    }
    if checks["has_ohlc"]:
        bad_hl = ((df["High"] < df[["Open", "Close"]].max(axis=1))
                  | (df["Low"] > df[["Open", "Close"]].min(axis=1)))
        checks["ohlc_violations"] = int(bad_hl.sum())
    if close is not None and len(close) > 1:
        logret = np.log(close.astype(float)).diff().dropna()
        checks["max_abs_log_return"] = (float(logret.abs().max())
                                        if len(logret) else None)
        checks["discontinuities"] = int(
            (logret.abs() > DISCONTINUITY_ABS_LOG_RETURN).sum())
        gaps = df.index.to_series().diff().dt.days.dropna()
        checks["gaps_over_threshold"] = int(
            (gaps > GAP_CALENDAR_DAYS).sum())
        checks["max_gap_days"] = int(gaps.max()) if len(gaps) else None
    if checks["has_volume"]:
        checks["nonzero_volume_share"] = float(
            (df["Volume"].fillna(0) > 0).mean())
    if checks["has_open_interest"]:
        checks["nonzero_open_interest_share"] = float(
            (df["Open Interest"].fillna(0) > 0).mean())

    csv_bytes = df.to_csv().encode("utf-8")
    checks["sha256"] = _hashlib.sha256(csv_bytes).hexdigest()
    return checks


def inspect_contract(nd, symbol: str, *, role: str) -> dict:
    """Series checks plus expiry-identity and metadata checks."""
    row = inspect_series(nd, symbol)
    row["role"] = role
    row["currency"] = _value(_attempt(nd.currency, symbol))
    row["exchange"] = _value(_attempt(nd.exchange_name, symbol))
    row["first_notice_date"] = _value(_attempt(nd.first_notice_date, symbol))
    row["scheduled_last_quoted"] = _value(_attempt(nd.last_quoted_date, symbol))
    # Expiry identity: an EXPIRED contract's final bar may not lie after its
    # scheduled final trading day.
    if (role != "current_front" and row.get("last_bar")
            and row.get("scheduled_last_quoted")):
        row["last_bar_after_scheduled_expiry"] = (
            row["last_bar"] > str(row["scheduled_last_quoted"])[:10])
    return row


def validate_market(nd, market: str, contract_symbols: list, *,
                    today: _dt.date) -> dict:
    ordered = sorted(
        (EN.parse_contract_symbol(s) for s in contract_symbols),
        key=lambda p: (p["delivery_year"] or 0, p["delivery_month"] or 0))
    ordered_symbols = [p["symbol"] for p in ordered if p["parsed"]]
    picks = select_contracts(ordered_symbols, today=today)
    inspected = {role: inspect_contract(nd, sym, role=role)
                 for role, sym in picks.items()}

    problems = []
    for role, row in inspected.items():
        if not row.get("readable"):
            problems.append("%s:UNREADABLE" % role)
            continue
        if row.get("empty"):
            problems.append("%s:EMPTY" % role)
            continue
        if not row.get("has_ohlc"):
            problems.append("%s:NO_OHLC" % role)
        if row.get("duplicate_dates"):
            problems.append("%s:DUPLICATE_DATES" % role)
        if not row.get("index_monotonic"):
            problems.append("%s:NON_MONOTONIC" % role)
        if row.get("nan_close"):
            problems.append("%s:NAN_CLOSE" % role)
        if row.get("nonpositive_close"):
            problems.append("%s:NONPOSITIVE_CLOSE" % role)
        if row.get("ohlc_violations"):
            problems.append("%s:OHLC_VIOLATIONS" % role)
        if row.get("last_bar_after_scheduled_expiry"):
            problems.append("%s:BARS_AFTER_EXPIRY" % role)
    state = "PASS" if not problems else (
        "WATCH" if all(":DUPLICATE_DATES" not in p and ":UNREADABLE" not in p
                       and ":BARS_AFTER_EXPIRY" not in p for p in problems)
        else "FAIL")
    return {"market": market, "selected": picks, "contracts": inspected,
            "problems": problems, "state": state}


def build(*, campaign_id: str = C.CAMPAIGN_ID,
          created_at: Optional[str] = None,
          today: Optional[_dt.date] = None) -> dict:
    nd = _nd()
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    today = today or _dt.date.today()

    registry = EN.load_contract_registry(campaign_id)
    markets_checked = {}
    for slot, markets in VALIDATION_MARKETS.items():
        for market in markets:
            if registry is not None:
                lists = registry["contract_symbols"].get(market, {})
                primary = (sorted(lists, key=lambda s: (len(s), s)) or [None])[0]
                symbols = lists.get(primary, []) if primary else []
            else:
                symbols = _value(
                    _attempt(nd.futures_market_session_contracts, market),
                    []) or []
            markets_checked[market] = validate_market(
                nd, market, symbols, today=today)
            markets_checked[market]["slot"] = slot

    states = [m["state"] for m in markets_checked.values()]
    payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "alpha_conclusions_in_this_module": ALPHA_CONCLUSIONS_IN_THIS_MODULE,
        "validation_slots": {k: list(v) for k, v in VALIDATION_MARKETS.items()},
        "markets_checked": len(markets_checked),
        "contracts_inspected": sum(
            len(m["contracts"]) for m in markets_checked.values()),
        "states": {"PASS": states.count("PASS"),
                   "WATCH": states.count("WATCH"),
                   "FAIL": states.count("FAIL")},
        "discontinuity_abs_log_return_threshold":
            DISCONTINUITY_ABS_LOG_RETURN,
        "gap_calendar_days_threshold": GAP_CALENDAR_DAYS,
        "settlement_field_note": (
            "the delivered daily Close on a dated futures contract is the "
            "session settlement as distributed by the vendor; OHLC, Volume "
            "and Open Interest are separate delivered fields"),
        "markets": markets_checked,
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
