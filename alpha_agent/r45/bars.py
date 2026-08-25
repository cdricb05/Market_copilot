"""alpha_agent.r45.bars - one bar contract for every instrument class.

A Release-45 PANEL is a UTC-indexed frame with exactly three things on it:

    close             the traded price at the end of the minute
    half_bps          what ONE side of a round trip costs, in basis points
    cost_source       how ``half_bps`` was obtained, carried on the frame

The third column exists because two of the four lanes cannot see a quote.
Owned Dukascopy rows carry the broker's OWN bid/ask, so their cost is
observed. Acquired minute aggregates carry no quote at all, so their cost is
ESTIMATED from the bars' own high/low by Corwin-Schultz (1) - and every card,
every artifact and every sentence that ever reports such a number says so.

Nothing is interpolated. A minute with no trade has no bar, and an event that
cannot find a bar within the frozen tolerance is DROPPED and counted, never
filled forward.

(1) Corwin & Schultz, "A Simple Way to Estimate Bid-Ask Spreads from Daily
    High and Low Prices", Journal of Finance 67(2), 2012.
"""
from __future__ import annotations

import gzip
from pathlib import Path

import numpy as np
import pandas as pd

from ..r44 import intraday as R44IN
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.bars"

ACQUIRED_ROOT = C.RESEARCH_ROOT / "_data_intraday"
_PANEL_CACHE: dict = {}

K_CS = 3.0 - 2.0 * np.sqrt(2.0)


# --------------------------------------------------------------------------- #
# Corwin-Schultz
# --------------------------------------------------------------------------- #
def corwin_schultz_half_bps(high, low, *, smooth: int = 11,
                            floor_bps: float = 0.25) -> pd.Series:
    """Half of the proportional bid-ask spread, in bps, per bar.

    Estimated from consecutive-bar high/low ranges only - no quote is used
    and none is available. Negative estimates (the estimator's known small
    sample artefact) are set to zero and the declared class floor is then
    applied, so a quiet minute can never print free execution.
    """
    h = pd.to_numeric(high, errors="coerce").astype(float)
    lo = pd.to_numeric(low, errors="coerce").astype(float)
    ok = (h > 0) & (lo > 0)
    h = h.where(ok)
    lo = lo.where(ok)

    lh = np.log(h / lo)
    beta = lh.pow(2) + lh.pow(2).shift(-1)
    h2 = pd.concat([h, h.shift(-1)], axis=1).max(axis=1)
    l2 = pd.concat([lo, lo.shift(-1)], axis=1).min(axis=1)
    gamma = np.log(h2 / l2) ** 2

    alpha = (np.sqrt(2.0 * beta) - np.sqrt(beta)) / K_CS \
        - np.sqrt(gamma / K_CS)
    s = 2.0 * (np.exp(alpha) - 1.0) / (1.0 + np.exp(alpha))
    s = s.where(np.isfinite(s)).clip(lower=0.0)

    half = s / 2.0 * 1e4
    if smooth and smooth > 1:
        half = half.rolling(smooth, center=True, min_periods=1).median()
    return half.fillna(float(floor_bps)).clip(lower=float(floor_bps))


# --------------------------------------------------------------------------- #
# Panels
# --------------------------------------------------------------------------- #
def _owned_panel(symbol: str):
    df = R44IN.load_bars(symbol)
    if df is None or df.empty:
        return None
    px = pd.to_numeric(df["close"], errors="coerce")
    sp = pd.to_numeric(df["spread"], errors="coerce")
    out = pd.DataFrame({
        "close": px,
        "half_bps": (sp / 2.0) / px * 1e4,
    }, index=df.index)
    out = out[np.isfinite(out["close"]) & (out["close"] > 0)]
    out.attrs["cost_source"] = C.COST_SOURCE_OBSERVED
    out.attrs["symbol"] = symbol
    out.attrs["instrument_class"] = \
        C.OWNED_MINUTE_INSTRUMENTS[symbol]["class"]
    return out


def _acquired_path(symbol: str) -> Path:
    return ACQUIRED_ROOT / f"{symbol.replace('=', '_')}.csv.gz"


def _acquired_panel(symbol: str, klass: str):
    p = _acquired_path(symbol)
    if not p.exists():
        return None
    with gzip.open(p, "rt") as fh:
        df = pd.read_csv(fh)
    if df.empty:
        return None
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
    df = df.dropna(subset=["close"]).sort_values("ts_utc")
    df = df.drop_duplicates(subset=["ts_utc"], keep="last").set_index("ts_utc")
    floor = C.ESTIMATED_HALF_SPREAD_FLOOR_BPS.get(klass, 0.25)
    half = corwin_schultz_half_bps(df["high"], df["low"], floor_bps=floor)
    out = pd.DataFrame({"close": pd.to_numeric(df["close"], errors="coerce"),
                        "half_bps": half}, index=df.index)
    if "volume" in df.columns:
        out["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    out = out[np.isfinite(out["close"]) & (out["close"] > 0)]
    out.attrs["cost_source"] = C.COST_SOURCE_ESTIMATED
    out.attrs["symbol"] = symbol
    out.attrs["instrument_class"] = klass
    out.attrs["bar_seconds"] = int(
        pd.Series(out.index).diff().dt.total_seconds().median() or 60)
    return out


def panel(symbol: str):
    """The panel for ``symbol``, whatever class it belongs to."""
    if symbol in _PANEL_CACHE:
        return _PANEL_CACHE[symbol]
    if symbol in C.OWNED_MINUTE_INSTRUMENTS:
        out = _owned_panel(symbol)
    elif symbol in C.LISTED_MINUTE_INSTRUMENTS:
        out = _acquired_panel(symbol, "LISTED_ETF")
    elif symbol in C.NATIVE_FUTURES_INSTRUMENTS:
        out = _acquired_panel(symbol, "NATIVE_FUTURES")
    else:
        out = None
    _PANEL_CACHE[symbol] = out
    return out


def instrument_class(symbol: str) -> str:
    for table in (C.OWNED_MINUTE_INSTRUMENTS, C.LISTED_MINUTE_INSTRUMENTS,
                  C.NATIVE_FUTURES_INSTRUMENTS):
        if symbol in table:
            return table[symbol]["class"]
    return "UNKNOWN"


def sleeve(symbol: str) -> str:
    for table in (C.LISTED_MINUTE_INSTRUMENTS, C.NATIVE_FUTURES_INSTRUMENTS):
        if symbol in table:
            return table[symbol].get("sleeve", "UNKNOWN")
    spec = C.OWNED_MINUTE_INSTRUMENTS.get(symbol, {})
    return {"GOLD": "GOLD", "SP500": "EQUITY", "DAX": "EQUITY",
            "EURO_BUND": "RATES", "WTI": "ENERGY",
            "EURUSD": "FX", "USDJPY": "FX"}.get(
                spec.get("underlying", ""), "UNKNOWN")


def coverage(symbol: str) -> dict:
    df = panel(symbol)
    if df is None or df.empty:
        return {"symbol": symbol, "state": "HISTORICAL_DATA_UNAVAILABLE",
                "instrument_class": instrument_class(symbol)}
    return {
        "symbol": symbol, "state": "AVAILABLE",
        "instrument_class": instrument_class(symbol),
        "sleeve": sleeve(symbol),
        "cost_source": df.attrs.get("cost_source"),
        "n_bars": int(len(df)),
        "first": str(df.index[0]), "last": str(df.index[-1]),
        "median_half_spread_bps": float(np.nanmedian(df["half_bps"])),
        "p90_half_spread_bps": float(np.nanpercentile(df["half_bps"], 90)),
        "spread_is_observed":
            df.attrs.get("cost_source") == C.COST_SOURCE_OBSERVED,
    }
