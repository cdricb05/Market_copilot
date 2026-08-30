r"""api/market_reference_data.py - Release 50: the ONE operational seam over the
OWNED non-equity reference data and marks (Norgate Data Updater, served locally).

Owned since Release 37/38: Continuous Futures (112 markets), dated Futures, Forex
Spot (57 pairs), US / World Indices, Cash Commodities, Economic. This module
exposes exactly what OPERATIONAL owners need and nothing research-shaped:

* instrument classification of a symbol (``&ZN`` is a continuous future, ``EURUSD``
  an FX spot pair, ``AAPL`` a cash equity);
* futures contract metadata - point value (multiplier), initial margin, currency,
  tick size, exchange - the inputs of the position contract;
* daily settlement closes (and volumes) for a non-equity symbol, in the SAME
  bar shape the desk's mark owner already normalises, so the desk mark store stays
  the ONE mark store and this module never writes one;
* USD conversion for a non-USD instrument currency from the owned Forex Spot
  database.

It OPENS owned data. It acquires nothing, installs nothing, writes nothing, spends
nothing. The vendor client is imported under a guard (the package logs a
deprecation warning at import that a warnings-as-errors filter would otherwise
turn into a silent NO_DATA). A JSON fixture (``PAPER_TRADER_REFERENCE_DATA_FIXTURE``)
replaces the vendor entirely for hermetic tests and never touches the vendor.

This is an OPERATIONAL owner: it imports no research module and no research
module may be needed for the operational book to value a position.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from paper_trader.engine import instrument_contract as ic

PHASE = "R50"
OWNER = "api.market_reference_data"

FIXTURE_ENV = "PAPER_TRADER_REFERENCE_DATA_FIXTURE"
DEFAULT_START = "2015-01-01"

#: Symbol prefixes the owned Norgate databases use for non-equity series.
_FUTURES_PREFIX = "&"
_INDEX_PREFIXES = ("$", "%", "#")

#: Continuous-futures roots grouped into the declared asset classes. A root that
#: is not listed is still describable (its metadata comes from the vendor) but is
#: classified UNCLASSIFIED_FUTURES and never enters a sleeve by accident.
FUTURES_ASSET_CLASS_BY_ROOT = {
    # US equity index futures
    "ES": ic.AC_EQUITY_INDEX_FUTURES, "NQ": ic.AC_EQUITY_INDEX_FUTURES,
    "RTY": ic.AC_EQUITY_INDEX_FUTURES, "YM": ic.AC_EQUITY_INDEX_FUTURES,
    "EMD": ic.AC_EQUITY_INDEX_FUTURES, "MES": ic.AC_EQUITY_INDEX_FUTURES,
    "MNQ": ic.AC_EQUITY_INDEX_FUTURES, "M2K": ic.AC_EQUITY_INDEX_FUTURES,
    "MYM": ic.AC_EQUITY_INDEX_FUTURES,
    # international equity index futures (non-USD)
    "FDAX": ic.AC_INTL_EQUITY_INDEX_FUTURES, "FESX": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "FSMI": ic.AC_INTL_EQUITY_INDEX_FUTURES, "FCE": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "NIY": ic.AC_INTL_EQUITY_INDEX_FUTURES, "NKD": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "HSI": ic.AC_INTL_EQUITY_INDEX_FUTURES, "MHI": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "YAP": ic.AC_INTL_EQUITY_INDEX_FUTURES, "SXF": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "LFT": ic.AC_INTL_EQUITY_INDEX_FUTURES, "SSG": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "HTW": ic.AC_INTL_EQUITY_INDEX_FUTURES, "SNK": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "KOS": ic.AC_INTL_EQUITY_INDEX_FUTURES, "SCN": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    "MET": ic.AC_INTL_EQUITY_INDEX_FUTURES, "FTDX": ic.AC_INTL_EQUITY_INDEX_FUTURES,
    # rates
    "ZT": ic.AC_RATES_FUTURES, "ZF": ic.AC_RATES_FUTURES, "ZN": ic.AC_RATES_FUTURES,
    "TN": ic.AC_RATES_FUTURES, "ZB": ic.AC_RATES_FUTURES, "UB": ic.AC_RATES_FUTURES,
    "SR3": ic.AC_RATES_FUTURES, "ZQ": ic.AC_RATES_FUTURES, "FGBS": ic.AC_RATES_FUTURES,
    "FGBM": ic.AC_RATES_FUTURES, "FGBL": ic.AC_RATES_FUTURES, "FGBX": ic.AC_RATES_FUTURES,
    "FOAT": ic.AC_RATES_FUTURES, "FBTP": ic.AC_RATES_FUTURES, "CGB": ic.AC_RATES_FUTURES,
    "YIB": ic.AC_RATES_FUTURES, "YIR": ic.AC_RATES_FUTURES, "YXT": ic.AC_RATES_FUTURES,
    "YYT": ic.AC_RATES_FUTURES, "SJB": ic.AC_RATES_FUTURES, "SO3": ic.AC_RATES_FUTURES,
    "LLG": ic.AC_RATES_FUTURES, "LSU": ic.AC_RATES_FUTURES, "LEU": ic.AC_RATES_FUTURES,
    # commodities
    "CL": ic.AC_COMMODITY_FUTURES, "BRN": ic.AC_COMMODITY_FUTURES, "WBS": ic.AC_COMMODITY_FUTURES,
    "HO": ic.AC_COMMODITY_FUTURES, "RB": ic.AC_COMMODITY_FUTURES, "NG": ic.AC_COMMODITY_FUTURES,
    "GAS": ic.AC_COMMODITY_FUTURES, "GC": ic.AC_COMMODITY_FUTURES, "SI": ic.AC_COMMODITY_FUTURES,
    "HG": ic.AC_COMMODITY_FUTURES, "PL": ic.AC_COMMODITY_FUTURES, "PA": ic.AC_COMMODITY_FUTURES,
    "ZC": ic.AC_COMMODITY_FUTURES, "ZS": ic.AC_COMMODITY_FUTURES, "ZW": ic.AC_COMMODITY_FUTURES,
    "ZM": ic.AC_COMMODITY_FUTURES, "ZL": ic.AC_COMMODITY_FUTURES, "ZO": ic.AC_COMMODITY_FUTURES,
    "ZR": ic.AC_COMMODITY_FUTURES, "KE": ic.AC_COMMODITY_FUTURES, "MWE": ic.AC_COMMODITY_FUTURES,
    "LE": ic.AC_COMMODITY_FUTURES, "GF": ic.AC_COMMODITY_FUTURES, "HE": ic.AC_COMMODITY_FUTURES,
    "KC": ic.AC_COMMODITY_FUTURES, "SB": ic.AC_COMMODITY_FUTURES, "CC": ic.AC_COMMODITY_FUTURES,
    "CT": ic.AC_COMMODITY_FUTURES, "OJ": ic.AC_COMMODITY_FUTURES, "LBR": ic.AC_COMMODITY_FUTURES,
    "DC": ic.AC_COMMODITY_FUTURES, "RS": ic.AC_COMMODITY_FUTURES, "LRC": ic.AC_COMMODITY_FUTURES,
    "LCC": ic.AC_COMMODITY_FUTURES, "LWB": ic.AC_COMMODITY_FUTURES, "LSU2": ic.AC_COMMODITY_FUTURES,
    "AFB": ic.AC_COMMODITY_FUTURES, "AWM": ic.AC_COMMODITY_FUTURES, "CRA": ic.AC_COMMODITY_FUTURES,
    "GWM": ic.AC_COMMODITY_FUTURES, "EUA": ic.AC_COMMODITY_FUTURES,
    # volatility
    "VX": ic.AC_VOLATILITY_FUTURES,
    # FX futures
    "6A": ic.AC_FX_FUTURES, "6B": ic.AC_FX_FUTURES, "6C": ic.AC_FX_FUTURES,
    "6E": ic.AC_FX_FUTURES, "6J": ic.AC_FX_FUTURES, "6M": ic.AC_FX_FUTURES,
    "6N": ic.AC_FX_FUTURES, "6S": ic.AC_FX_FUTURES, "DX": ic.AC_FX_FUTURES,
    # crypto (CME cash-settled futures)
    "BTC": ic.AC_CRYPTO_FUTURES, "ETH": ic.AC_CRYPTO_FUTURES, "MBT": ic.AC_CRYPTO_FUTURES,
}
UNCLASSIFIED_FUTURES = "UNCLASSIFIED_FUTURES"


# --------------------------------------------------------------------------- #
# Fixture seam (hermetic tests) and the guarded vendor import
# --------------------------------------------------------------------------- #
def _fixture() -> Optional[dict]:
    path = os.environ.get(FIXTURE_ENV)
    if not path:
        return None
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"metadata": {}, "closes": {}, "fx_symbols": [], "futures_symbols": []}


def _nd():
    """Import the vendor client without letting the caller's warning filters
    decide whether owned data is readable (the package logs a deprecation warning
    at import; under warnings-as-errors that becomes a silent NO_DATA)."""
    if _fixture() is not None:
        raise RuntimeError("REFERENCE_DATA_FIXTURE_ACTIVE")
    logging.disable(logging.WARNING)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import norgatedata as nd  # type: ignore
    return nd


def available() -> bool:
    if _fixture() is not None:
        return True
    try:
        _nd()
        return True
    except Exception:  # noqa: BLE001
        return False


@lru_cache(maxsize=1)
def provider_state() -> dict:
    fx = _fixture()
    if fx is not None:
        return {"state": "FIXTURE", "owner": OWNER, "databases": list(fx.get("databases") or []),
                "fixture": True}
    try:
        nd = _nd()
    except Exception as exc:  # noqa: BLE001
        return {"state": "NOT_CONFIGURED", "owner": OWNER, "error": repr(exc)[:200],
                "databases": [], "fixture": False}
    out: dict[str, Any] = {"state": "OK", "owner": OWNER, "databases": [], "last_update": {},
                           "fixture": False}
    try:
        out["databases"] = list(nd.databases())
    except Exception as exc:  # noqa: BLE001
        out["state"] = "DEGRADED"
        out["databases_error"] = repr(exc)[:200]
    for db in out["databases"]:
        try:
            out["last_update"][db] = str(nd.last_database_update_time(db))
        except Exception as exc:  # noqa: BLE001
            out["last_update"][db] = "ERR:" + type(exc).__name__
    return out


def reset_cache() -> None:
    for fn in (provider_state, futures_metadata, daily_bars, fx_spot_symbols,
               continuous_futures_symbols):
        try:
            fn.cache_clear()
        except AttributeError:
            pass


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def fx_spot_symbols() -> tuple:
    fx = _fixture()
    if fx is not None:
        return tuple(sorted(fx.get("fx_symbols") or []))
    try:
        return tuple(sorted(_nd().database_symbols("Forex Spot")))
    except Exception:  # noqa: BLE001
        return ()


@lru_cache(maxsize=1)
def continuous_futures_symbols() -> tuple:
    fx = _fixture()
    if fx is not None:
        return tuple(sorted(fx.get("futures_symbols") or []))
    try:
        return tuple(sorted(s for s in _nd().database_symbols("Continuous Futures")
                            if not s.endswith("_CCB")))
    except Exception:  # noqa: BLE001
        return ()


def futures_root(symbol: str) -> Optional[str]:
    s = str(symbol or "").strip()
    if not s.startswith(_FUTURES_PREFIX):
        return None
    root = s[1:]
    if root.endswith("_CCB"):
        root = root[:-4]
    return root or None


def classify_symbol(symbol: str) -> dict:
    """What kind of instrument a symbol denotes, by the owned databases' own
    conventions. A plain ticker is a US cash equity (the pre-R50 contract)."""
    s = str(symbol or "").strip()
    root = futures_root(s)
    if root is not None:
        cls = FUTURES_ASSET_CLASS_BY_ROOT.get(root, UNCLASSIFIED_FUTURES)
        return {"symbol": s, "instrument_type": ic.IT_FUTURE, "asset_class": cls,
                "root": root, "owned_database": "Continuous Futures",
                "classified": cls != UNCLASSIFIED_FUTURES}
    if s.startswith(_INDEX_PREFIXES):
        return {"symbol": s, "instrument_type": None, "asset_class": None,
                "owned_database": "Indices", "classified": False,
                "reason": "an index is a reference series, not an investable instrument"}
    if s.upper() in set(fx_spot_symbols()):
        return {"symbol": s.upper(), "instrument_type": ic.IT_FX_SPOT,
                "asset_class": ic.AC_FX_SPOT, "owned_database": "Forex Spot",
                "classified": True}
    return {"symbol": s.upper(), "instrument_type": ic.IT_CASH_EQUITY,
            "asset_class": ic.AC_US_EQUITY, "owned_database": "US Equities (EODHD marks)",
            "classified": True}


def is_owned_non_equity_symbol(symbol: str) -> bool:
    c = classify_symbol(symbol)
    return c.get("instrument_type") in (ic.IT_FUTURE, ic.IT_FX_SPOT)


# --------------------------------------------------------------------------- #
# Futures metadata (the position-contract inputs)
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1024)
def futures_metadata(symbol: str) -> dict:
    """Point value, initial margin, currency, tick, exchange for one continuous
    market. Every field is read from the owned database; nothing is guessed."""
    s = str(symbol or "").strip()
    fx = _fixture()
    if fx is not None:
        m = (fx.get("metadata") or {}).get(s)
        if not m:
            return {"symbol": s, "state": "NOT_FOUND", "fixture": True}
        return {"symbol": s, "state": "OK", "fixture": True,
                "market_name": m.get("market_name") or s,
                "point_value": float(m.get("point_value")),
                "initial_margin": float(m.get("initial_margin") or 0.0),
                "currency": str(m.get("currency") or "USD").upper(),
                "tick_size": m.get("tick_size"), "exchange": m.get("exchange"),
                "session_symbol": m.get("session_symbol")}
    try:
        nd = _nd()
    except Exception as exc:  # noqa: BLE001
        return {"symbol": s, "state": "PROVIDER_UNAVAILABLE", "error": repr(exc)[:160]}
    out: dict[str, Any] = {"symbol": s, "state": "OK", "fixture": False}
    try:
        out["market_name"] = nd.futures_market_name(s)
        out["point_value"] = float(nd.point_value(s))
        out["initial_margin"] = float(nd.margin(s) or 0.0)
        out["currency"] = str(nd.currency(s) or "USD").upper()
        out["tick_size"] = float(nd.tick_size(s)) if nd.tick_size(s) is not None else None
        out["exchange"] = nd.exchange_name(s)
        try:
            out["session_symbol"] = nd.futures_market_session_symbol(s)
        except Exception:  # noqa: BLE001
            out["session_symbol"] = None
    except Exception as exc:  # noqa: BLE001
        return {"symbol": s, "state": "NOT_FOUND", "error": repr(exc)[:160]}
    if not out.get("point_value"):
        return {"symbol": s, "state": "NOT_FOUND", "error": "no point value"}
    return out


def descriptor_for(symbol: str, *, sleeve_id: Optional[str] = None,
                   metadata: Optional[dict] = None) -> Optional[dict]:
    """The instrument descriptor of an owned non-equity symbol (None when the
    symbol cannot be described from owned data)."""
    c = classify_symbol(symbol)
    if c.get("instrument_type") == ic.IT_FUTURE:
        m = metadata if metadata is not None else futures_metadata(c["symbol"])
        if m.get("state") != "OK":
            return None
        cls = c["asset_class"] if c["asset_class"] in ic.ASSET_CLASSES else ic.AC_COMMODITY_FUTURES
        return ic.describe(
            c["symbol"], asset_class=cls, instrument_type=ic.IT_FUTURE,
            sleeve_id=sleeve_id or ("sleeve_" + cls.lower()),
            currency=m.get("currency") or "USD", multiplier=m["point_value"],
            initial_margin_per_unit=m.get("initial_margin") or 0.0,
            tick_size=m.get("tick_size"), label=m.get("market_name") or c["symbol"],
            reference_data_owner=OWNER)
    if c.get("instrument_type") == ic.IT_FX_SPOT:
        base = c["symbol"][:3]
        return ic.describe(
            c["symbol"], asset_class=ic.AC_FX_SPOT, instrument_type=ic.IT_FX_SPOT,
            sleeve_id=sleeve_id or "sleeve_fx_spot", currency="USD", multiplier=1.0,
            label="%s spot" % c["symbol"], reference_data_owner=OWNER)
    return None


# --------------------------------------------------------------------------- #
# Marks (daily settlement / close) - in the desk mark owner's bar shape
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=2048)
def daily_bars(symbol: str, start: str = DEFAULT_START) -> tuple:
    """``((date, close, volume), ...)`` for one owned non-equity symbol. Futures
    and FX carry NO stock-style adjustment; the series is the owned settlement."""
    s = str(symbol or "").strip()
    fx = _fixture()
    if fx is not None:
        rows = (fx.get("closes") or {}).get(s) or []
        out = []
        for r in rows:
            try:
                d = str(r[0])[:10]
                if d < str(start)[:10]:
                    continue
                out.append((d, float(r[1]), float(r[2]) if len(r) > 2 and r[2] is not None else None))
            except (TypeError, ValueError, IndexError):
                continue
        return tuple(sorted(out))
    try:
        nd = _nd()
        df = nd.price_timeseries(s, start_date=str(start), format="pandas-dataframe",
                                 padding_setting=nd.PaddingType.NONE)
    except Exception:  # noqa: BLE001
        return ()
    if df is None or not len(df):
        return ()
    out = []
    for idx, row in df.iterrows():
        try:
            d = str(idx)[:10]
            c = float(row["Close"])
        except (KeyError, TypeError, ValueError):
            continue
        v = None
        try:
            v = float(row["Volume"]) if "Volume" in df.columns else None
        except (TypeError, ValueError):
            v = None
        out.append((d, c, v))
    return tuple(sorted(out))


def mark_downloader(symbol: str, start: str) -> list:
    """An EODHD-shaped payload (``[{"date", "close"}, ...]``) for one owned
    non-equity symbol, so the desk's ONE mark owner normalises and stores it
    exactly as it stores an equity series. The desk routes here; this never writes."""
    return [{"date": d, "close": c} for d, c, _v in daily_bars(symbol, start)]


def latest_session(symbol: str) -> Optional[str]:
    bars = daily_bars(symbol)
    return bars[-1][0] if bars else None


def average_daily_volume(symbol: str, *, as_of: Optional[str] = None,
                         window: int = 20) -> Optional[float]:
    bars = [b for b in daily_bars(symbol) if b[2] is not None
            and (as_of is None or b[0] <= str(as_of)[:10])]
    tail = bars[-int(window):]
    if not tail:
        return None
    return sum(b[2] for b in tail) / len(tail)


# --------------------------------------------------------------------------- #
# USD conversion from the owned Forex Spot database
# --------------------------------------------------------------------------- #
def fx_pair_for(currency: str) -> Optional[dict]:
    """The owned spot pair that converts ``currency`` to USD, and its direction."""
    c = str(currency or "USD").upper()
    if c == ic.REPORTING_CURRENCY:
        return None
    syms = set(fx_spot_symbols())
    if (c + "USD") in syms:
        return {"symbol": c + "USD", "direction": "MULTIPLY"}
    if ("USD" + c) in syms:
        return {"symbol": "USD" + c, "direction": "DIVIDE"}
    return {"symbol": None, "direction": None}


def fx_to_usd(currency: str, *, as_of: Optional[str] = None) -> dict:
    """USD per one unit of ``currency`` at (or before) ``as_of``. USD is 1.0 by
    identity. A pair the estate does not own is a named gap, never a guess."""
    c = str(currency or "USD").upper()
    if c == ic.REPORTING_CURRENCY:
        return {"currency": c, "fx_to_usd": 1.0, "state": "IDENTITY", "pair": None,
                "as_of": as_of, "owner": OWNER}
    pair = fx_pair_for(c)
    if not pair or not pair.get("symbol"):
        return {"currency": c, "fx_to_usd": None, "state": "PAIR_NOT_OWNED", "pair": None,
                "as_of": as_of, "owner": OWNER}
    bars = daily_bars(pair["symbol"])
    hit = None
    for d, close, _v in bars:
        if as_of is None or d <= str(as_of)[:10]:
            hit = (d, close)
    if hit is None or not hit[1]:
        return {"currency": c, "fx_to_usd": None, "state": "NO_RATE_AT_OR_BEFORE_AS_OF",
                "pair": pair["symbol"], "as_of": as_of, "owner": OWNER}
    rate = hit[1] if pair["direction"] == "MULTIPLY" else (1.0 / hit[1])
    return {"currency": c, "fx_to_usd": rate, "state": "OK", "pair": pair["symbol"],
            "direction": pair["direction"], "rate_date": hit[0], "as_of": as_of,
            "owner": OWNER}


def fx_series_id(currency: str) -> Optional[str]:
    """The desk mark-store series id that carries this currency's USD rate."""
    pair = fx_pair_for(currency)
    return (pair or {}).get("symbol")


def safety() -> dict:
    return {"owner": OWNER, "read_only": True, "opens_owned_data_only": True,
            "acquires_nothing": True, "writes_nothing": True, "spends_nothing": True,
            "research_imports": [], "safety_badges": ["READ ONLY", "OWNED DATA ONLY",
                                                      "NO PURCHASE", "NO WRITE"]}


__all__ = [
    "PHASE", "OWNER", "FIXTURE_ENV", "FUTURES_ASSET_CLASS_BY_ROOT", "UNCLASSIFIED_FUTURES",
    "available", "provider_state", "reset_cache", "fx_spot_symbols",
    "continuous_futures_symbols", "futures_root", "classify_symbol",
    "is_owned_non_equity_symbol", "futures_metadata", "descriptor_for", "daily_bars",
    "mark_downloader", "latest_session", "average_daily_volume", "fx_pair_for",
    "fx_to_usd", "fx_series_id", "safety",
]
