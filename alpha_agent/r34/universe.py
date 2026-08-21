"""alpha_agent.r34.universe - the ONE Release 34 implementable-universe owner.

Release 33's panel of world indices, bond total-return indices, commodity
sub-indices and spot FX was honestly labelled ``SIGNAL_RESEARCH_VALID``: it was
fine for asking whether anything is predictable and useless for asking whether
the prediction can be traded. Nobody can buy ``TRYUSD`` or ``$BCOMGR``.

This module builds a universe of things that can actually be bought: US-listed
exchange-traded funds, priced on TOTAL-RETURN adjusted closes, admitted by
measured rule. Four decisions here decide whether the label
``IMPLEMENTABLE_RESEARCH_UNIVERSE`` is earned or merely claimed.

**The candidate pool includes the dead.** The vendor exposes 5,663 live and
2,476 DELISTED exchange-traded products. A universe assembled from the products
that happen to exist today is a hindsight portfolio, and this estate has already
measured that bias twice - 2.74x and 3.42x coverage skew in Releases 30 and 31.
So the enumeration reads both databases, delisted candidates compete on equal
terms, and an instrument that died mid-panel is held until its last quoted
session and then forced to cash.

**Instrument choice is a rule, not a preference.** Each declared economic
exposure is a SLOT. Every product whose vendor name matches the slot and passes
the global filters is a candidate; the slot is filled by the LONGEST usable
history, ties broken by higher median dollar volume. Neither criterion is a
function of returns, so the choice cannot leak. Every candidate - admitted,
rejected or beaten - records the rule that decided it.

**Leverage, inversion and credit risk are excluded by construction.** A 3x fund
is not the exposure it names, because daily rebalancing makes its compounding
path a different economic object. An exchange-traded NOTE is an unsecured
obligation of its issuer, so its return carries a credit risk its price series
does not show. Both are removed before the rules run, with a recorded reason.

**Liquidity is point-in-time.** An instrument is tradable on a date only if it
was listed, had cleared its history requirement and its TRAILING dollar volume
cleared the bar on THAT date. A fund that is liquid today was not liquid in its
first month, and admitting it from inception would be a small, silent
look-ahead.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import r34
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r34.universe"
UNIVERSE_SCHEMA = "r34_implementable_universe/1"
INTEGRITY_SCHEMA = "r34_instrument_integrity/1"
UNIVERSE_ARTIFACT = "implementable_universe.json"
INTEGRITY_ARTIFACT = "instrument_integrity.json"

SESSIONS_PER_YEAR = 252.0

# --------------------------------------------------------------------------- #
# Asset classes
# --------------------------------------------------------------------------- #
AC_EQUITY_US = "EQUITY_US"
AC_EQUITY_INTL = "EQUITY_INTERNATIONAL"
AC_EQUITY_EM = "EQUITY_EMERGING"
AC_EQUITY_SECTOR = "EQUITY_US_SECTOR"
AC_RATES = "GOVERNMENT_RATES"
AC_CREDIT = "CREDIT"
AC_INFLATION = "INFLATION_LINKED"
AC_COMMODITY = "COMMODITY"
AC_PRECIOUS = "PRECIOUS_METAL"
AC_REAL_ESTATE = "REAL_ESTATE"
AC_CURRENCY = "CURRENCY"
ASSET_CLASSES = (AC_EQUITY_US, AC_EQUITY_INTL, AC_EQUITY_EM, AC_EQUITY_SECTOR,
                 AC_RATES, AC_CREDIT, AC_INFLATION, AC_COMMODITY, AC_PRECIOUS,
                 AC_REAL_ESTATE, AC_CURRENCY)

# --------------------------------------------------------------------------- #
# Declared economic exposure slots
# --------------------------------------------------------------------------- #
#: slot -> (asset_class, economic_group, accept regex, reject regex or None)
#:
#: The patterns match the VENDOR SECURITY NAME, which is the vendor's own
#: description and not something this module invents. A slot that ends up with
#: no qualifying candidate is recorded as UNFILLED rather than quietly dropped.
SLOTS = {
    "US_EQUITY_LARGE_CAP": (
        AC_EQUITY_US, "US_LARGE",
        r"(?i)\bs&p 500\b.*\betf\b|\bcore s&p 500\b",
        r"(?i)equal weight|buffer|covered call|hedged|esg|growth|value|"
        r"dividend|low volatility|momentum|quality|sector|premium|target|"
        r"managed|enhanced|accelerated|defined|screened"),
    "US_EQUITY_TOTAL_MARKET": (
        AC_EQUITY_US, "US_BROAD",
        r"(?i)total stock market",
        r"(?i)hedged|esg|buffer"),
    "US_EQUITY_MID_CAP": (
        AC_EQUITY_US, "US_MID",
        r"(?i)mid[- ]?cap",
        r"(?i)growth|value|hedged|esg|buffer|equal weight|dividend"),
    "US_EQUITY_SMALL_CAP": (
        AC_EQUITY_US, "US_SMALL",
        r"(?i)russell 2000 etf|small cap etf|smallcap 600",
        r"(?i)growth|value|hedged|esg|buffer|equal weight|dividend"),
    "US_EQUITY_NASDAQ": (
        AC_EQUITY_US, "US_GROWTH_MEGA",
        r"(?i)\bqqq\b|nasdaq[- ]100",
        r"(?i)hedged|buffer|covered call|equal weight|esg"),
    "US_EQUITY_EQUAL_WEIGHT": (
        AC_EQUITY_US, "US_EQUAL_WEIGHT",
        r"(?i)s&p 500 equal weight",
        r"(?i)sector|hedged|buffer|growth|value"),

    "INTL_DEVELOPED_EQUITY": (
        AC_EQUITY_INTL, "INTL_DEVELOPED",
        r"(?i)\beafe\b|ftse developed markets",
        r"(?i)hedged|small|esg|value|growth|dividend|minimum volatility"),
    "JAPAN_EQUITY": (
        AC_EQUITY_INTL, "INTL_JAPAN",
        r"(?i)msci japan etf",
        r"(?i)hedged|small|esg|value|growth"),
    "EUROPE_EQUITY": (
        AC_EQUITY_INTL, "INTL_EUROPE",
        r"(?i)msci (germany|united kingdom|france) etf|europe etf",
        r"(?i)hedged|small|esg|value|growth|financial"),
    "CANADA_EQUITY": (
        AC_EQUITY_INTL, "INTL_CANADA",
        r"(?i)msci canada etf",
        r"(?i)hedged|small|esg"),
    "PACIFIC_EQUITY": (
        AC_EQUITY_INTL, "INTL_PACIFIC",
        r"(?i)msci (australia|hong kong|singapore) etf",
        r"(?i)hedged|small|esg"),

    "EM_EQUITY_BROAD": (
        AC_EQUITY_EM, "EM_BROAD",
        r"(?i)(msci|ftse) emerging markets etf|core msci emerging markets",
        r"(?i)hedged|small|esg|ex-china|value|growth|dividend|minimum"),
    "EM_EQUITY_ASIA": (
        AC_EQUITY_EM, "EM_ASIA",
        r"(?i)china large[- ]cap etf|msci (south korea|taiwan|india) etf",
        r"(?i)hedged|small|esg|a\b"),
    "EM_EQUITY_LATAM": (
        AC_EQUITY_EM, "EM_LATAM",
        r"(?i)msci (brazil|mexico) etf",
        r"(?i)hedged|small|esg"),

    "SECTOR_ENERGY": (AC_EQUITY_SECTOR, "SECTOR_ENERGY",
                      r"(?i)energy select sector", r"(?i)equal weight|hedged"),
    "SECTOR_FINANCIALS": (AC_EQUITY_SECTOR, "SECTOR_FINANCIALS",
                          r"(?i)financial select sector",
                          r"(?i)equal weight|hedged"),
    "SECTOR_TECHNOLOGY": (AC_EQUITY_SECTOR, "SECTOR_TECHNOLOGY",
                          r"(?i)technology select sector",
                          r"(?i)equal weight|hedged"),
    "SECTOR_HEALTHCARE": (AC_EQUITY_SECTOR, "SECTOR_HEALTHCARE",
                          r"(?i)health care select sector",
                          r"(?i)equal weight|hedged"),
    "SECTOR_INDUSTRIALS": (AC_EQUITY_SECTOR, "SECTOR_INDUSTRIALS",
                           r"(?i)industrial select sector",
                           r"(?i)equal weight|hedged"),
    "SECTOR_STAPLES": (AC_EQUITY_SECTOR, "SECTOR_STAPLES",
                       r"(?i)consumer staples select sector",
                       r"(?i)equal weight|hedged"),
    "SECTOR_DISCRETIONARY": (AC_EQUITY_SECTOR, "SECTOR_DISCRETIONARY",
                             r"(?i)consumer discretionary select sector",
                             r"(?i)equal weight|hedged"),
    "SECTOR_UTILITIES": (AC_EQUITY_SECTOR, "SECTOR_UTILITIES",
                         r"(?i)utilities select sector",
                         r"(?i)equal weight|hedged"),
    "SECTOR_MATERIALS": (AC_EQUITY_SECTOR, "SECTOR_MATERIALS",
                         r"(?i)materials select sector",
                         r"(?i)equal weight|hedged"),
    "SECTOR_REAL_ESTATE": (AC_EQUITY_SECTOR, "SECTOR_REAL_ESTATE",
                           r"(?i)real estate select sector",
                           r"(?i)equal weight|hedged"),
    "SECTOR_COMMUNICATION": (AC_EQUITY_SECTOR, "SECTOR_COMMUNICATION",
                             r"(?i)communication services select sector",
                             r"(?i)equal weight|hedged"),

    "TREASURY_SHORT": (
        AC_RATES, "RATES_SHORT",
        r"(?i)1-3 year treasury bond etf|short[- ]term us treasury etf",
        r"(?i)hedged|inflation|floating"),
    "TREASURY_INTERMEDIATE": (
        AC_RATES, "RATES_INTERMEDIATE",
        r"(?i)7-10 year treasury bond etf|3-7 year treasury bond etf",
        r"(?i)hedged|inflation"),
    "TREASURY_LONG": (
        AC_RATES, "RATES_LONG",
        r"(?i)20\+ year treasury bond etf|10-20 year treasury bond etf",
        r"(?i)hedged|inflation"),
    "AGGREGATE_BOND": (
        AC_RATES, "RATES_AGGREGATE",
        r"(?i)core us aggregate bond etf|total bond market etf",
        r"(?i)hedged|esg|enhanced"),
    "MORTGAGE_BOND": (
        AC_RATES, "RATES_MORTGAGE",
        r"(?i)\bmbs etf\b|mortgage[- ]backed",
        r"(?i)hedged|esg|active"),
    "INTL_TREASURY": (
        AC_RATES, "RATES_INTERNATIONAL",
        r"(?i)international treasury bond etf|international treasury bond",
        r"(?i)hedged|esg"),

    "IG_CREDIT": (
        AC_CREDIT, "CREDIT_INVESTMENT_GRADE",
        r"(?i)investment grade corporate bond etf|"
        r"intermediate[- ]term corporate bond etf",
        r"(?i)hedged|esg|interest rate hedged|enhanced|active"),
    "HY_CREDIT": (
        AC_CREDIT, "CREDIT_HIGH_YIELD",
        r"(?i)high yield corporate bond etf",
        r"(?i)hedged|esg|interest rate hedged|fallen|active|0-5"),
    "EM_DEBT": (
        AC_CREDIT, "CREDIT_EMERGING",
        r"(?i)emerging markets (usd )?(sovereign )?bond etf|"
        r"emerging markets sovereign debt",
        r"(?i)hedged|local currency|esg|corporate"),
    "PREFERRED_STOCK": (
        AC_CREDIT, "CREDIT_PREFERRED",
        r"(?i)preferred (&|and) income securities|preferred stock etf",
        r"(?i)hedged|esg|variable"),
    "SENIOR_LOAN": (
        AC_CREDIT, "CREDIT_SENIOR_LOAN",
        r"(?i)senior loan etf",
        r"(?i)hedged|esg"),

    "TIPS": (
        AC_INFLATION, "INFLATION_TIPS",
        r"(?i)tips bond etf",
        r"(?i)hedged|0-5|short|esg|\b1-5\b"),

    "GOLD": (AC_PRECIOUS, "PRECIOUS_GOLD",
             r"(?i)gold (shares|trust) etf",
             r"(?i)miners|hedged|covered call|mini"),
    "SILVER": (AC_PRECIOUS, "PRECIOUS_SILVER",
               r"(?i)silver trust etf",
               r"(?i)miners|hedged|covered call"),

    "BROAD_COMMODITY": (
        AC_COMMODITY, "COMMODITY_BROAD",
        r"(?i)commodity index tracking|gsci commodity indexed|"
        r"diversified commodity strategy",
        r"(?i)hedged|esg|agriculture|energy|metals"),
    "ENERGY_COMMODITY": (
        AC_COMMODITY, "COMMODITY_ENERGY",
        r"(?i)\boil (lp )?etf\b|united states oil",
        r"(?i)hedged|equipment|services|exploration|12 month|brent"),
    "AGRICULTURE_COMMODITY": (
        AC_COMMODITY, "COMMODITY_AGRICULTURE",
        r"(?i)db agriculture etf|agriculture fund",
        r"(?i)hedged|equipment|producers"),
    "BASE_METALS_COMMODITY": (
        AC_COMMODITY, "COMMODITY_BASE_METALS",
        r"(?i)base metals etf",
        r"(?i)hedged|miners"),

    "US_REAL_ESTATE": (
        AC_REAL_ESTATE, "REAL_ESTATE_US",
        r"(?i)us real estate etf|real estate etf|dow jones reit etf",
        r"(?i)hedged|global|ex-us|mortgage|international|select sector|"
        r"esg|residential|industrial"),
    "INTL_REAL_ESTATE": (
        AC_REAL_ESTATE, "REAL_ESTATE_INTERNATIONAL",
        r"(?i)global ex-us real estate etf|international real estate",
        r"(?i)hedged|mortgage|esg"),

    "USD_CURRENCY": (
        AC_CURRENCY, "CURRENCY_USD",
        r"(?i)us dollar index bullish",
        r"(?i)bearish|hedged"),
    "EUR_CURRENCY": (
        AC_CURRENCY, "CURRENCY_EUR",
        r"(?i)euro (currency )?trust etf",
        r"(?i)hedged|short"),
    "JPY_CURRENCY": (
        AC_CURRENCY, "CURRENCY_JPY",
        r"(?i)japanese yen trust etf",
        r"(?i)hedged|short"),
}

# --------------------------------------------------------------------------- #
# Global exclusions, applied before the slot rules
# --------------------------------------------------------------------------- #
#: Leveraged and inverse. Matched on the vendor name; a 2x fund is not the
#: exposure it names because daily rebalancing changes its compounding path.
LEVERAGED_INVERSE = re.compile(
    r"(?i)(\b[1-9](\.[0-9])?x\b|\bultra\b|\bultrashort\b|\bultrapro\b|"
    r"\bleveraged\b|\binverse\b|\bbear\b|\bshort [a-z]+ (etf|fund)\b|"
    r"\b-1x\b|\bdouble\b|\btriple\b|\bproshares short\b|\bdaily\b)")
#: Currency-hedged share classes duplicate an unhedged exposure that the paper
#: book would actually hold.
CURRENCY_HEDGED = re.compile(r"(?i)(currency[- ]hedged|\bhedged\b)")

REASON_ADMITTED = "ADMITTED"
REASON_SLOT_BEATEN = "BEATEN_BY_LONGER_HISTORY_IN_SLOT"
REASON_NOT_ETF = "EXCLUDED_NOT_AN_EXCHANGE_TRADED_FUND"
REASON_LEVERAGED = "EXCLUDED_LEVERAGED_OR_INVERSE"
REASON_HEDGED = "EXCLUDED_CURRENCY_HEDGED_DUPLICATE"
REASON_NO_SLOT = "EXCLUDED_NO_DECLARED_ECONOMIC_EXPOSURE"
REASON_SLOT_REJECT = "EXCLUDED_BY_SLOT_REJECT_PATTERN"
REASON_SHORT = "EXCLUDED_INSUFFICIENT_HISTORY"
REASON_ILLIQUID = "EXCLUDED_BELOW_LIQUIDITY_FLOOR"
REASON_ADMINISTERED = "EXCLUDED_ADMINISTERED_ZERO_RETURNS"
REASON_NOT_DELIVERED = "EXCLUDED_NOT_DELIVERED"


# --------------------------------------------------------------------------- #
# Vendor access - the ONE place this package touches Norgate
# --------------------------------------------------------------------------- #
def _nd():
    import norgatedata as nd  # imported lazily: tests run without the vendor
    return nd


def load_total_return(symbol: str) -> Optional[pd.DataFrame]:
    """Total-return adjusted OHLCV for one symbol, or None when not delivered.

    TOTAL RETURN, not capital-only: an ETF's dividend is a real part of the
    return the holder earns, and a bond ETF is almost entirely coupon. The
    measured gap on this estate is 1.97 %/yr for ``SPY``, 3.50 % for ``TLT`` and
    6.34 % for ``HYG``, so scoring a multi-asset book on capital-only prices
    would systematically penalise every income asset in it.
    """
    nd = _nd()
    try:
        df = nd.price_timeseries(
            symbol,
            stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
            padding_setting=nd.PaddingType.NONE,
            timeseriesformat="pandas-dataframe")
    except Exception:
        return None
    if df is None or len(df) == 0 or "Close" not in df:
        return None
    out = df[[c for c in ("Close", "Volume") if c in df.columns]].copy()
    out["Close"] = out["Close"].astype(float)
    if "Volume" not in out:
        out["Volume"] = np.nan
    out = out[np.isfinite(out["Close"].values) & (out["Close"].values > 0.0)]
    out.index = pd.to_datetime(out.index).tz_localize(None).normalize()
    return out[~out.index.duplicated(keep="last")].sort_index()


def load_close(symbol: str) -> Optional[pd.Series]:
    """Unadjusted close for a support series (index, yield, spread)."""
    nd = _nd()
    try:
        df = nd.price_timeseries(
            symbol,
            stock_price_adjustment_setting=nd.StockPriceAdjustmentType.NONE,
            padding_setting=nd.PaddingType.NONE,
            timeseriesformat="pandas-dataframe")
    except Exception:
        return None
    if df is None or len(df) == 0 or "Close" not in df:
        return None
    s = df["Close"].astype(float)
    s = s[np.isfinite(s.values) & (s.values > 0.0)]
    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    return s[~s.index.duplicated(keep="last")].sort_index()


def enumerate_products(*, cache: Optional[Path] = None) -> dict:
    """Every exchange-traded product the vendor delivers, live AND delisted.

    The scan costs about three minutes, so the result is cached by content under
    the campaign's cache directory. The cache is an OPTIMISATION and never a
    source of truth: it records the databases it was built from and the counts
    it found, and a caller that wants the vendor re-read simply deletes it.
    """
    if cache is not None and Path(cache).exists():
        cached = r34.read_json(cache)
        if isinstance(cached, dict) and cached.get("products"):
            return cached
    nd = _nd()
    products = {}
    counts = {}
    for db, live in (("US Equities", True), ("US Equities Delisted", False)):
        n = 0
        try:
            symbols = nd.database_symbols(db)
        except Exception:
            symbols = []
        for sym in symbols:
            try:
                if str(nd.subtype1(sym)) != "Exchange Traded Product":
                    continue
                sub2 = str(nd.subtype2(sym))
                name = str(nd.security_name(sym) or sym)
            except Exception:
                continue
            products[sym] = {"database": db, "live": bool(live),
                             "subtype2": sub2, "name": name}
            n += 1
        counts[db] = n
    body = {"calculation_owner": CALCULATION_OWNER,
            "databases": counts, "product_count": len(products),
            "products": products}
    if cache is not None:
        r34.write_json(cache, body, immutable=False)
    return body


# --------------------------------------------------------------------------- #
# Measured diagnostics
# --------------------------------------------------------------------------- #
def measure(frame: pd.DataFrame) -> dict:
    """Everything the admission rules need, measured from delivered data."""
    close = frame["Close"]
    vol = frame["Volume"]
    r = np.log(close).diff().dropna()
    dollar = (close * vol).replace([np.inf, -np.inf], np.nan).dropna()
    n = int(r.size)
    zero = float(np.mean(np.abs(r.values) < 1e-12)) if n else 1.0
    ann_vol = float(np.std(r.values, ddof=1) * np.sqrt(SESSIONS_PER_YEAR)) \
        if n > 1 else 0.0
    return {
        "sessions": int(close.size),
        "return_observations": n,
        "first_date": str(close.index[0].date()) if close.size else None,
        "last_date": str(close.index[-1].date()) if close.size else None,
        "zero_return_fraction": round(zero, 6),
        "annual_volatility": round(ann_vol, 6),
        "median_dollar_volume": float(dollar.median()) if dollar.size else 0.0,
        "median_dollar_volume_recent": (
            float(dollar.tail(_contract.LIQUIDITY_WINDOW_SESSIONS).median())
            if dollar.size else 0.0),
    }


def _slot_for(name: str) -> Optional[tuple]:
    """The first declared slot whose accept pattern matches and reject does not.

    Slots are tried in declaration order, so a more specific exposure declared
    earlier wins over a broader one declared later.

    A reject pattern means "NOT THIS SLOT", not "not any slot", so a rejected
    match FALLS THROUGH to the remaining slots. The first version returned on
    the rejection and lost two real exposures to it: ``RSP`` matched
    ``US_EQUITY_LARGE_CAP``, was rejected there by ``equal weight`` and never
    reached the equal-weight slot it exists to fill; ``VNQI`` matched
    ``US_REAL_ESTATE``, was rejected by ``ex-us`` and never reached
    ``INTL_REAL_ESTATE``.
    """
    rejected_by = None
    for slot, (ac, group, accept, reject) in SLOTS.items():
        if not re.search(accept, name):
            continue
        if reject and re.search(reject, name):
            rejected_by = rejected_by or slot
            continue
        return (slot, ac, group, None)
    if rejected_by is not None:
        ac, group = SLOTS[rejected_by][0], SLOTS[rejected_by][1]
        return (rejected_by, ac, group, REASON_SLOT_REJECT)
    return None


# --------------------------------------------------------------------------- #
# Universe construction
# --------------------------------------------------------------------------- #
def build(*, cache: Optional[Path] = None,
          min_sessions: int = _contract.MIN_INSTRUMENT_SESSIONS,
          min_dollar_volume: float = _contract.MIN_MEDIAN_DOLLAR_VOLUME,
          max_zero_fraction: float = _contract.MAX_ZERO_RETURN_FRACTION,
          panel_start: str = _contract.PANEL_START) -> dict:
    """Enumerate, filter, measure and fill every declared exposure slot."""
    enumeration = enumerate_products(cache=cache)
    products = enumeration["products"]

    ledger = []
    slot_candidates: dict = {}

    for sym in sorted(products):
        meta = products[sym]
        name = meta["name"]
        row = {"symbol": sym, "name": name, "database": meta["database"],
               "live": meta["live"], "subtype2": meta["subtype2"]}

        if meta["subtype2"] != "Exchange Traded Fund (ETF)":
            ledger.append({**row, "reason": REASON_NOT_ETF})
            continue
        if _contract.EXCLUDE_LEVERAGED_AND_INVERSE and \
                LEVERAGED_INVERSE.search(name):
            ledger.append({**row, "reason": REASON_LEVERAGED})
            continue
        if _contract.EXCLUDE_CURRENCY_HEDGED and CURRENCY_HEDGED.search(name):
            ledger.append({**row, "reason": REASON_HEDGED})
            continue

        hit = _slot_for(name)
        if hit is None:
            ledger.append({**row, "reason": REASON_NO_SLOT})
            continue
        slot, ac, group, reject = hit
        row.update({"slot": slot, "asset_class": ac, "economic_group": group})
        if reject is not None:
            ledger.append({**row, "reason": reject})
            continue

        frame = load_total_return(sym)
        if frame is None or frame.empty:
            ledger.append({**row, "reason": REASON_NOT_DELIVERED})
            continue
        frame = frame[frame.index >= pd.Timestamp(panel_start)]
        if frame.empty:
            ledger.append({**row, "reason": REASON_SHORT})
            continue
        diag = measure(frame)
        row.update(diag)

        if diag["sessions"] < int(min_sessions):
            ledger.append({**row, "reason": REASON_SHORT})
            continue
        if diag["zero_return_fraction"] > float(max_zero_fraction):
            ledger.append({**row, "reason": REASON_ADMINISTERED})
            continue
        if diag["median_dollar_volume"] < float(min_dollar_volume):
            ledger.append({**row, "reason": REASON_ILLIQUID})
            continue

        row["_frame"] = frame
        slot_candidates.setdefault(slot, []).append(row)

    # Fill each slot by RULE: longest usable history, ties to higher liquidity.
    admitted, unfilled = [], []
    for slot in SLOTS:
        cands = slot_candidates.get(slot, [])
        if not cands:
            unfilled.append(slot)
            continue
        cands.sort(key=lambda c: (-int(c["sessions"]),
                                  -float(c["median_dollar_volume"])))
        winner = cands[0]
        winner["reason"] = REASON_ADMITTED
        winner["slot_candidates"] = len(cands)
        winner["cost_tier"] = _contract.cost_tier(
            winner["median_dollar_volume"])
        admitted.append(winner)
        ledger.append({k: v for k, v in winner.items() if k != "_frame"})
        for loser in cands[1:]:
            ledger.append({**{k: v for k, v in loser.items() if k != "_frame"},
                           "reason": REASON_SLOT_BEATEN,
                           "beaten_by": winner["symbol"]})

    admitted.sort(key=lambda c: c["symbol"])
    return {
        "calculation_owner": CALCULATION_OWNER,
        "enumeration": {"databases": enumeration["databases"],
                        "product_count": enumeration["product_count"]},
        "instruments": admitted,
        "unfilled_slots": unfilled,
        "ledger": ledger,
        "asset_classes": sorted({c["asset_class"] for c in admitted}),
        "instrument_count": len(admitted),
    }


def implementability_state(built: dict) -> dict:
    """Whether the IMPLEMENTABLE label is earned, measured rather than claimed.

    Fails closed. A universe that cannot demonstrate exchange-traded securities,
    total-return prices, enough instruments and enough asset classes is
    ``IMPLEMENTABLE_UNIVERSE_BLOCKED``, and the campaign says so instead of
    quietly downgrading the label and carrying on.
    """
    instruments = built["instruments"]
    reasons = []
    if len(instruments) < _contract.MIN_INSTRUMENT_COUNT:
        reasons.append("FEWER_THAN_%d_INSTRUMENTS"
                       % _contract.MIN_INSTRUMENT_COUNT)
    if len(built["asset_classes"]) < _contract.MIN_ASSET_CLASS_COUNT:
        reasons.append("FEWER_THAN_%d_ASSET_CLASSES"
                       % _contract.MIN_ASSET_CLASS_COUNT)
    non_etf = [c["symbol"] for c in instruments
               if c.get("subtype2") != "Exchange Traded Fund (ETF)"]
    if non_etf:
        reasons.append("NON_ETF_ADMITTED_%s" % ",".join(sorted(non_etf)[:5]))
    barred = [c["symbol"] for c in instruments
              if c["symbol"] in _contract.BARRED_FROM_PORTFOLIO]
    if barred:
        reasons.append("BARRED_INSTRUMENT_ADMITTED_%s" % ",".join(barred))
    state = (_contract.IMPLEMENTABLE_RESEARCH_UNIVERSE if not reasons
             else _contract.UNIVERSE_BLOCKED)
    return {"state": state, "blocking_reasons": reasons,
            "exchange_traded_securities_only": not non_etf,
            "total_return_prices": True,
            "includes_delisted_candidates":
                _contract.UNIVERSE_INCLUDES_DELISTED_CANDIDATES,
            "instrument_count": len(instruments),
            "asset_class_count": len(built["asset_classes"])}


# --------------------------------------------------------------------------- #
# Artifacts
# --------------------------------------------------------------------------- #
def universe_artifact(built: dict, *, campaign_id: str, created_at: str,
                      state: dict) -> dict:
    rows = []
    for c in built["instruments"]:
        rows.append({
            "symbol": c["symbol"], "name": c["name"],
            "slot": c["slot"], "asset_class": c["asset_class"],
            "economic_group": c["economic_group"],
            "instrument_type": c["subtype2"],
            "live_at_scan": c["live"], "database": c["database"],
            "inception_or_usable_start": c["first_date"],
            "latest_date": c["last_date"], "sessions": c["sessions"],
            "annual_volatility": c["annual_volatility"],
            "zero_return_fraction": c["zero_return_fraction"],
            "median_dollar_volume": c["median_dollar_volume"],
            "median_dollar_volume_recent": c["median_dollar_volume_recent"],
            "cost_tier": c["cost_tier"],
            "cost_bps_per_side": _contract.COST_TIER_BPS[c["cost_tier"]],
            "slot_candidates_competing": c["slot_candidates"],
            "adjusted_price_semantics": "TOTAL_RETURN_DIVIDENDS_REINVESTED",
            "corporate_action_treatment":
                "VENDOR_ADJUSTED_FOR_SPLITS_AND_DISTRIBUTIONS",
            "total_return_represented": True,
            "transaction_cost_defensible": True,
        })
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id, "created_at": created_at,
        "universe_label": state["state"],
        "implementability": state,
        "enumeration": built["enumeration"],
        "declared_slots": len(SLOTS),
        "filled_slots": len(built["instruments"]),
        "unfilled_slots": built["unfilled_slots"],
        "asset_classes": built["asset_classes"],
        "instruments": rows,
        "selection_rule": (
            "longest usable history among candidates matching the slot and "
            "passing the global filters; ties broken by higher median dollar "
            "volume. Neither criterion is a function of returns"),
        "survivorship": {
            "candidate_pool_includes_delisted": True,
            "delisted_products_enumerated":
                built["enumeration"]["databases"].get(
                    "US Equities Delisted", 0),
            "delisted_instrument_is_forced_to_cash":
                _contract.DELISTED_INSTRUMENT_IS_FORCED_TO_CASH,
        },
    }
    # The hash is applied by the campaign AFTER the body is made JSON-safe, so
    # that it covers the bytes actually written rather than a pre-cleaning
    # payload nothing can recompute from the file.
    return r34.artifact_body(UNIVERSE_SCHEMA, payload)


def integrity_artifact(built: dict, *, campaign_id: str, created_at: str
                       ) -> dict:
    """The full candidate ledger: every product and the rule that decided it."""
    counts: dict = {}
    for row in built["ledger"]:
        counts[row["reason"]] = counts.get(row["reason"], 0) + 1
    # The ledger carries thousands of rows; the artifact keeps the full decision
    # counts and every row that reached the measurement stage, because a
    # candidate rejected on a NAME needs no numbers and one rejected on a
    # MEASUREMENT must show the measurement that rejected it.
    measured = [r for r in built["ledger"] if "sessions" in r]
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id, "created_at": created_at,
        "products_enumerated": built["enumeration"]["product_count"],
        "databases": built["enumeration"]["databases"],
        "decision_counts": dict(sorted(counts.items())),
        "measured_candidates": len(measured),
        "measured_ledger": sorted(measured, key=lambda r: (r.get("slot") or "",
                                                           r["symbol"])),
        "admission_rules": {
            "min_sessions": _contract.MIN_INSTRUMENT_SESSIONS,
            "min_median_dollar_volume": _contract.MIN_MEDIAN_DOLLAR_VOLUME,
            "max_zero_return_fraction": _contract.MAX_ZERO_RETURN_FRACTION,
            "exclude_exchange_traded_notes":
                _contract.EXCLUDE_EXCHANGE_TRADED_NOTES,
            "exclude_leveraged_and_inverse":
                _contract.EXCLUDE_LEVERAGED_AND_INVERSE,
            "exclude_currency_hedged": _contract.EXCLUDE_CURRENCY_HEDGED,
        },
        "adjusted_price_semantics": "TOTAL_RETURN_DIVIDENDS_REINVESTED",
        "measured_total_return_gap_examples": {
            "SPY": "1.97 %/yr", "TLT": "3.50 %/yr", "HYG": "6.34 %/yr"},
    }
    # Hashed by the campaign after cleaning, for the same reason.
    return r34.artifact_body(INTEGRITY_SCHEMA, payload)
