"""alpha_agent.r33.universe - the ONE Release 33 research universe owner.

Release 33 was commissioned to run a BROAD continuous-futures campaign. The
owned Norgate Continuous Futures entitlement contains exactly ONE market
(``&ES``), which this module measures rather than assumes. The broad
cross-market universe is therefore assembled from what the estate actually
holds - world equity indices, ICE/FTSE bond total-return indices, Bloomberg
commodity sub-indices and Forex Spot - and the resulting universe is labelled
``SIGNAL_RESEARCH_VALID`` and never ``FUTURES_IMPLEMENTABILITY_PROVEN``.

Selection is by MEASURED rule, not by opinion. Every rule below produces a
recorded reason for every candidate, including the ones that were kept:

R1  FX is expressed as XXXUSD (value of one unit of the foreign currency in
    USD) and is SOURCED from whichever quote direction the vendor delivers with
    better numerical resolution. ``JPYUSD`` prints 23.9 % identical closes and
    ``USDJPY`` prints 0.7 %; that is a rounding artifact of quoting a ~0.0068
    number, not a property of the yen.
R2  A market whose best-resolution series repeats its previous close more than
    ``MAX_ZERO_RETURN_FRACTION`` of the time is ADMINISTERED, not traded.
R3  A market with annualised volatility below ``MIN_ANNUAL_VOLATILITY`` is
    PEGGED. Its "returns" are a policy, and a model that predicts them is
    predicting an announcement.
R4  A market correlated above ``MAX_DUPLICATE_CORRELATION`` with an already
    admitted market is a DUPLICATE. The longer history is kept. Duplicates
    inflate an apparent breadth of forty markets that is really twenty-five
    bets, and leave-market-out then understates concentration.
R5  A market must deliver ``MIN_MARKET_SESSIONS`` usable sessions and must
    still be updating at the panel end.
R6  A market that is a COMPOSITE of markets already admitted is excluded and is
    retained only as a global state variable. ``$STOXX50`` is its members;
    ``$BCOM`` is its sub-indices; ``#CUGC`` is a copper/gold RATIO and is not an
    investable market at all.

Currency denomination is taken from the vendor's authoritative ``currency()``
field and is CHECKED against a measured diagnostic, because that field is
demonstrably unreliable for emerging markets: ``$NIF`` and ``$SEN`` are both
Indian indices and the vendor labels one USD and the other INR. The check
regresses each index's return on its currency's USD return and compares the
loading with known anchors. The diagnostic is recorded and, for emerging
markets, it is NOT decisive - a local EM index is genuinely correlated with its
own currency through risk appetite, so a high loading does not prove
translation. Markets whose loading contradicts their label are flagged
``CURRENCY_LABEL_UNCERTAIN`` and are re-tested in robustness rather than
silently trusted.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import r33
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r33.universe"
UNIVERSE_SCHEMA = "r33_futures_universe/1"
INVENTORY_SCHEMA = "r33_data_inventory/1"
UNIVERSE_ARTIFACT = "futures_universe.json"
INVENTORY_ARTIFACT = "data_inventory.json"

# --------------------------------------------------------------------------- #
# Measured selection thresholds
# --------------------------------------------------------------------------- #
MAX_ZERO_RETURN_FRACTION = 0.10

#: R3 applies to FX ONLY. A currency held in a band has an annualised
#: volatility near zero because a central bank decided so, and predicting it
#: means predicting an announcement. A SHORT-DURATION BOND index also has low
#: volatility, but for the honest reason that it is a short-duration bond: the
#: first version of this rule excluded the 1-3 year Treasury index at 1.43 %
#: and that was the rule being wrong, not the market.
MIN_ANNUAL_VOLATILITY = 0.02
#: Literal rather than ``AC_FX``: the asset-class constants are declared below.
PEG_RULE_APPLIES_TO = ("FX",)

MAX_DUPLICATE_CORRELATION = 0.98
MIN_MARKET_SESSIONS = _contract.MIN_MARKET_SESSIONS
MAX_STALE_SESSIONS = 10

#: R4 tie-break. When two markets are duplicates the longer history normally
#: wins, but length is not economic primacy: EURUSD begins in 1999 and DKKUSD
#: in 1991, and dropping the euro because the Danish krone - which is held in a
#: narrow band AGAINST the euro - has eight more years of history would be
#: absurd. A named preference wins over the length rule and must state why.
DUPLICATE_PREFERENCE = {
    ("EURUSD", "DKKUSD"): (
        "EURUSD",
        "the Danish krone is maintained in a narrow band against the euro, so "
        "the euro is the primary market and the krone is its satellite"),
}

#: Loading of an index return on its currency's USD return. A mechanically
#: translated index carries the whole currency move, so only a loading close to
#: one contradicts a LOCAL label. Moderate loadings are uninformative: an
#: emerging or commodity-bloc equity market genuinely moves with its own
#: currency through risk appetite, and the first version of this threshold
#: flagged nine markets on exactly that confound.
CURRENCY_BETA_LOCAL_MAX = 0.85
CURRENCY_BETA_USD_MIN = 0.30

# --------------------------------------------------------------------------- #
# Asset classes and economic groups
# --------------------------------------------------------------------------- #
AC_EQUITY = "EQUITY_INDEX"
AC_GOVT = "GOVERNMENT_BOND"
AC_CREDIT = "CREDIT_BOND"
AC_COMMODITY = "COMMODITY"
AC_PRECIOUS = "PRECIOUS_METAL"
AC_FX = "FX"
ASSET_CLASSES = (AC_EQUITY, AC_GOVT, AC_CREDIT, AC_COMMODITY, AC_PRECIOUS,
                 AC_FX)

#: Candidate equity indices: symbol -> economic group. Composites are excluded
#: below by rule R6 and are listed in ``COMPOSITE_EXCLUSIONS``.
EQUITY_CANDIDATES = {
    "&ES": "EQUITY_NORTH_AMERICA",
    "$SPTSX": "EQUITY_NORTH_AMERICA",
    "$SPTSX60": "EQUITY_NORTH_AMERICA",
    "$MXX": "EQUITY_LATAM",
    "$BVSP": "EQUITY_LATAM",
    "$CAC": "EQUITY_EUROPE",
    "$DAX": "EQUITY_EUROPE",
    "$TDX": "EQUITY_EUROPE",
    "$FT100": "EQUITY_EUROPE",
    "$FTSEMIB": "EQUITY_EUROPE",
    "$IBEX": "EQUITY_EUROPE",
    "$SMI": "EQUITY_EUROPE",
    "$OMNX40": "EQUITY_EUROPE",
    "$RTS": "EQUITY_EMERGING_EMEA",
    "$ZADOW": "EQUITY_EMERGING_EMEA",
    "$N225": "EQUITY_ASIA_DEVELOPED",
    "$XJO": "EQUITY_ASIA_DEVELOPED",
    "$HS": "EQUITY_ASIA_DEVELOPED",
    "$SIMSCI": "EQUITY_ASIA_DEVELOPED",
    "$STI": "EQUITY_ASIA_DEVELOPED",
    "$KO": "EQUITY_ASIA_EMERGING",
    "$TWMSCI": "EQUITY_ASIA_EMERGING",
    "$SSEC": "EQUITY_ASIA_EMERGING",
    "$XIN0": "EQUITY_ASIA_EMERGING",
    "$XIN9": "EQUITY_ASIA_EMERGING",
    "$NIF": "EQUITY_ASIA_EMERGING",
    "$SEN": "EQUITY_ASIA_EMERGING",
}

BOND_CANDIDATES = {
    "$IDCOT1TR": (AC_GOVT, "BOND_1_3Y"),
    "$IDCOT3TR": (AC_GOVT, "BOND_3_7Y"),
    "$IDCOT7TR": (AC_GOVT, "BOND_7_10Y"),
    "$IDCOT10TR": (AC_GOVT, "BOND_10_20Y"),
    "$IDCOT20TR": (AC_GOVT, "BOND_20Y_PLUS"),
    "$ICET25TR": (AC_GOVT, "BOND_25Y_PLUS"),
    "$USBIGCORP": (AC_CREDIT, "CREDIT_INVESTMENT_GRADE"),
    "$SP5IGBIT": (AC_CREDIT, "CREDIT_INVESTMENT_GRADE"),
}

COMMODITY_CANDIDATES = {
    "$BCOMEN": (AC_COMMODITY, "COMMODITY_ENERGY"),
    "@WTI": (AC_COMMODITY, "COMMODITY_ENERGY"),
    "$BCOMIN": (AC_COMMODITY, "COMMODITY_INDUSTRIAL_METALS"),
    "$BCOMGR": (AC_COMMODITY, "COMMODITY_GRAINS"),
    "$BCOMSO": (AC_COMMODITY, "COMMODITY_SOFTS"),
    "$BCOMLI": (AC_COMMODITY, "COMMODITY_LIVESTOCK"),
    "$BCOMPR": (AC_PRECIOUS, "COMMODITY_PRECIOUS_METALS"),
    "XAUUSD": (AC_PRECIOUS, "PRECIOUS_GOLD"),
    "XAGUSD": (AC_PRECIOUS, "PRECIOUS_SILVER"),
}

#: FX currency codes considered, grouped economically. Each is resolved to a
#: XXXUSD series by rule R1.
FX_CANDIDATES = {
    "EUR": "FX_MAJOR", "JPY": "FX_MAJOR", "GBP": "FX_MAJOR",
    "CHF": "FX_MAJOR", "AUD": "FX_COMMODITY_BLOC",
    "CAD": "FX_COMMODITY_BLOC", "NZD": "FX_COMMODITY_BLOC",
    "NOK": "FX_COMMODITY_BLOC", "SEK": "FX_EUROPE_MINOR",
    "DKK": "FX_EUROPE_MINOR", "CZK": "FX_EUROPE_MINOR",
    "HUF": "FX_EUROPE_MINOR", "PLN": "FX_EUROPE_MINOR",
    "TRY": "FX_EMERGING", "ZAR": "FX_EMERGING", "MXN": "FX_EMERGING",
    "BRL": "FX_EMERGING", "CLP": "FX_EMERGING", "ILS": "FX_EMERGING",
    "SGD": "FX_ASIA", "KRW": "FX_ASIA", "TWD": "FX_ASIA",
    "INR": "FX_ASIA", "HKD": "FX_ASIA", "CNY": "FX_ASIA",
    "MYR": "FX_ASIA", "RUB": "FX_EMERGING",
}

#: R6 - composites of admitted markets, and one ratio. Excluded from the
#: prediction universe; available to :mod:`alpha_agent.r33.features` as global
#: state variables, which is what they are actually good for.
COMPOSITE_EXCLUSIONS = {
    "$OOI": "S&P Global 100 - composite of admitted equity markets",
    "$W1DOW": "Dow Jones Global - composite of admitted equity markets",
    "$W2DOW": "Dow Jones Global ex-US - composite of admitted equity markets",
    "$E3X": "FTSEurofirst 300 - composite of admitted European markets",
    "$STOXX50": "Euro STOXX 50 - composite of admitted euro-area markets",
    "$BCOM": "Bloomberg Commodity Index - composite of admitted sub-indices",
    "$BCOMTR": "Bloomberg Commodity total-return - composite",
    "$BCOMXE": "Bloomberg ex-Energy - composite of admitted sub-indices",
    "$BCOMAG": "Bloomberg Agriculture - composite of grains, softs, livestock",
    "$BCOMPE": "Bloomberg Petroleum - subset of the admitted energy sub-index",
    "$CRB": "FTSE/CoreCommodity CRB - composite of admitted commodities",
    "$SPGSCI": "S&P GSCI Spot - composite of admitted commodities",
    "$IDCOTCTR": "ICE Treasury Core - composite of admitted duration buckets",
    "$USBIG": "FTSE US Broad IG - composite of government and corporate",
    "$USDX": "US Dollar Index - composite of admitted FX majors",
    "#CUGC": "Copper/Gold RATIO - a derived statistic, not an investable market",
}

#: Measured, not assumed: excluded before the rules run because the vendor
#: DELIVERS materially less than it advertises, or the series is below its own
#: price resolution. Both are re-measured by :func:`inventory` every run.
KNOWN_DEFECTIVE = {
    "$USTSY": "vendor advertises 1990; delivers from 2022 only",
    "$IDCOTSTR": "0-1 year bond index repeats its close 33 % of sessions",
}

#: Vendor currency-code aliases.
CURRENCY_ALIASES = {"MXP": "MXN"}

# Selection reasons
KEEP = "ADMITTED"
DROP_COMPOSITE = "EXCLUDED_COMPOSITE_OR_RATIO"
DROP_DEFECTIVE = "EXCLUDED_DEFECTIVE_DELIVERY"
DROP_SHORT = "EXCLUDED_INSUFFICIENT_HISTORY"
DROP_STALE = "EXCLUDED_STALE_SERIES"
DROP_ADMINISTERED = "EXCLUDED_ADMINISTERED_ZERO_RETURNS"
DROP_PEGGED = "EXCLUDED_PEGGED_LOW_VOLATILITY"
DROP_DUPLICATE = "EXCLUDED_DUPLICATE_OF_ADMITTED_MARKET"
DROP_UNAVAILABLE = "EXCLUDED_NOT_DELIVERED"


# --------------------------------------------------------------------------- #
# Vendor access - the ONE place this package touches Norgate
# --------------------------------------------------------------------------- #
def _nd():
    import norgatedata as nd  # imported lazily: tests run without the vendor
    return nd


def load_close(symbol: str) -> Optional[pd.Series]:
    """Unadjusted close series for one symbol, or None when not delivered."""
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


def vendor_currency(symbol: str) -> str:
    try:
        cur = str(_nd().currency(symbol) or "USD").upper()
    except Exception:
        return "USD"
    return CURRENCY_ALIASES.get(cur, cur)


def vendor_name(symbol: str) -> str:
    try:
        return str(_nd().security_name(symbol) or symbol)
    except Exception:
        return symbol


# --------------------------------------------------------------------------- #
# Measured series diagnostics
# --------------------------------------------------------------------------- #
def series_diagnostics(close: pd.Series) -> dict:
    """Everything the selection rules need, measured from delivered data."""
    r = np.log(close).diff().dropna()
    n = int(r.size)
    if n == 0:
        return {"sessions": int(close.size), "return_observations": 0,
                "zero_return_fraction": 1.0, "annual_volatility": 0.0}
    zero = float(np.mean(np.abs(r.values) < 1e-12))
    vol = float(np.std(r.values, ddof=1) * math.sqrt(252.0)) if n > 1 else 0.0
    return {
        "sessions": int(close.size),
        "return_observations": n,
        "first_date": str(close.index[0].date()),
        "last_date": str(close.index[-1].date()),
        "zero_return_fraction": round(zero, 6),
        "annual_volatility": round(vol, 6),
    }


def resolve_fx(code: str) -> dict:
    """R1: build XXXUSD from whichever direction has better resolution."""
    direct, inverse = f"{code}USD", f"USD{code}"
    cand = {}
    for sym, inverted in ((direct, False), (inverse, True)):
        s = load_close(sym)
        if s is None or s.size < 2:
            continue
        d = series_diagnostics(s)
        cand[sym] = {"series": s, "inverted": inverted, "diagnostics": d}
    if not cand:
        return {"code": code, "state": DROP_UNAVAILABLE, "series": None}
    best_sym = min(cand, key=lambda k: cand[k]["diagnostics"]["zero_return_fraction"])
    chosen = cand[best_sym]
    series = 1.0 / chosen["series"] if chosen["inverted"] else chosen["series"]
    return {
        "code": code,
        "symbol": f"{code}USD",
        "source_symbol": best_sym,
        "sourced_inverted": bool(chosen["inverted"]),
        "quote_resolution_by_direction": {
            k: v["diagnostics"]["zero_return_fraction"] for k, v in cand.items()},
        "series": series,
        "state": KEEP,
    }


def currency_denomination_diagnostic(index_close: pd.Series,
                                     fx_close: Optional[pd.Series]) -> dict:
    """Regress index return on its currency's USD return.

    A USD-translated index carries the currency move mechanically and shows a
    loading near one. A local-currency index shows a loading near zero - EXCEPT
    in emerging markets, where equity and currency genuinely move together with
    risk appetite. The diagnostic is therefore evidence, not a verdict.
    """
    if fx_close is None or fx_close.size < 50:
        return {"state": "NO_FX_SERIES", "beta": None, "t_stat": None}
    a = np.log(index_close).diff().dropna()
    b = np.log(fx_close).diff().dropna()
    j = pd.concat([a, b], axis=1, join="inner").dropna()
    j.columns = ["idx", "fx"]
    j = j[(j["idx"].abs() < 0.25) & (j["fx"].abs() < 0.25)]
    if len(j) < 250:
        return {"state": "INSUFFICIENT_OVERLAP", "beta": None, "t_stat": None}
    x = j["fx"].to_numpy(); y = j["idx"].to_numpy()
    vx = float(np.var(x, ddof=1))
    if vx <= 0:
        return {"state": "DEGENERATE_FX", "beta": None, "t_stat": None}
    beta = float(np.cov(x, y, ddof=1)[0, 1] / vx)
    resid = y - beta * x
    se = float(math.sqrt(max(np.var(resid, ddof=2), 0.0) / (vx * (len(x) - 1))))
    return {"state": "OK", "beta": round(beta, 4),
            "t_stat": round(beta / se, 2) if se > 0 else None,
            "observations": int(len(j))}


# --------------------------------------------------------------------------- #
# Universe construction
# --------------------------------------------------------------------------- #
def _candidate_table() -> list:
    rows = []
    for sym, group in EQUITY_CANDIDATES.items():
        rows.append({"symbol": sym, "asset_class": AC_EQUITY,
                     "economic_group": group})
    for sym, (ac, group) in BOND_CANDIDATES.items():
        rows.append({"symbol": sym, "asset_class": ac, "economic_group": group})
    for sym, (ac, group) in COMMODITY_CANDIDATES.items():
        rows.append({"symbol": sym, "asset_class": ac, "economic_group": group})
    return rows


def build(*, panel_start: str = _contract.PANEL_START) -> dict:
    """Measure every candidate, apply R1-R6, and return the admitted universe.

    Returns a dict with ``markets`` (admitted, each carrying its close series),
    ``decisions`` (EVERY candidate with its measured reason) and ``state_series``
    (the composites, kept as global state variables).
    """
    start = pd.Timestamp(panel_start)
    decisions, admitted = [], []

    def record(symbol, asset_class, group, state, reason, diag=None, **extra):
        row = {"symbol": symbol, "asset_class": asset_class,
               "economic_group": group, "state": state, "reason": reason}
        if diag:
            row.update(diag)
        row.update(extra)
        decisions.append(row)
        return row

    for sym, why in COMPOSITE_EXCLUSIONS.items():
        record(sym, "COMPOSITE", None, DROP_COMPOSITE, why)
    for sym, why in KNOWN_DEFECTIVE.items():
        record(sym, "DEFECTIVE", None, DROP_DEFECTIVE, why)

    # ---- non-FX candidates ------------------------------------------------ #
    for row in _candidate_table():
        sym, ac, group = row["symbol"], row["asset_class"], row["economic_group"]
        close = load_close(sym)
        if close is None or close.size < 2:
            record(sym, ac, group, DROP_UNAVAILABLE, "vendor delivered nothing")
            continue
        cur = vendor_currency(sym)
        diag = series_diagnostics(close)
        diag["currency"] = cur
        diag["security_name"] = vendor_name(sym)
        window = close[close.index >= start]
        if window.size < MIN_MARKET_SESSIONS:
            record(sym, ac, group, DROP_SHORT,
                   f"{window.size} sessions since {panel_start} "
                   f"< {MIN_MARKET_SESSIONS}", diag)
            continue
        if diag["zero_return_fraction"] > MAX_ZERO_RETURN_FRACTION:
            record(sym, ac, group, DROP_ADMINISTERED,
                   f"repeats its close {diag['zero_return_fraction']:.1%} "
                   f"of sessions", diag)
            continue
        # R3 is deliberately NOT applied here: see PEG_RULE_APPLIES_TO.
        admitted.append({"symbol": sym, "asset_class": ac,
                         "economic_group": group, "currency": cur,
                         "close": close, "diagnostics": diag,
                         "source": "NORGATE_INDEX_OR_COMMODITY"})

    # ---- FX candidates ---------------------------------------------------- #
    fx_series: dict = {}
    for code, group in FX_CANDIDATES.items():
        res = resolve_fx(code)
        sym = res.get("symbol") or f"{code}USD"
        if res["state"] != KEEP or res["series"] is None:
            record(sym, AC_FX, group, DROP_UNAVAILABLE,
                   "neither quote direction delivered")
            continue
        close = res["series"]
        fx_series[code] = close
        diag = series_diagnostics(close)
        diag["currency"] = code
        diag["source_symbol"] = res["source_symbol"]
        diag["sourced_inverted"] = res["sourced_inverted"]
        diag["quote_resolution_by_direction"] = res["quote_resolution_by_direction"]
        window = close[close.index >= start]
        if window.size < MIN_MARKET_SESSIONS:
            record(sym, AC_FX, group, DROP_SHORT,
                   f"{window.size} sessions < {MIN_MARKET_SESSIONS}", diag)
            continue
        if diag["zero_return_fraction"] > MAX_ZERO_RETURN_FRACTION:
            record(sym, AC_FX, group, DROP_ADMINISTERED,
                   f"repeats its close {diag['zero_return_fraction']:.1%} of "
                   f"sessions even in its better-resolution direction", diag)
            continue
        if diag["annual_volatility"] < MIN_ANNUAL_VOLATILITY:
            record(sym, AC_FX, group, DROP_PEGGED,
                   f"annualised volatility {diag['annual_volatility']:.2%}", diag)
            continue
        admitted.append({"symbol": sym, "asset_class": AC_FX,
                         "economic_group": group, "currency": code,
                         "close": close, "diagnostics": diag,
                         "source": "NORGATE_FOREX_SPOT"})

    # ---- R4 duplicates ---------------------------------------------------- #
    #: A named preference is processed FIRST so it wins over the length rule.
    preferred = {win for win, _why in DUPLICATE_PREFERENCE.values()}
    admitted.sort(key=lambda m: (0 if m["symbol"] in preferred else 1,
                                 -m["close"][m["close"].index >= start].size,
                                 m["symbol"]))
    kept: list = []
    for m in admitted:
        r_new = np.log(m["close"]).diff().dropna()
        dup_of, dup_rho = None, None
        for k in kept:
            r_old = np.log(k["close"]).diff().dropna()
            j = pd.concat([r_new, r_old], axis=1, join="inner").dropna()
            if len(j) < 250:
                continue
            rho = float(np.corrcoef(j.iloc[:, 0], j.iloc[:, 1])[0, 1])
            if abs(rho) > MAX_DUPLICATE_CORRELATION:
                dup_of, dup_rho = k["symbol"], round(rho, 4)
                break
        if dup_of:
            d = dict(m["diagnostics"]); d["duplicate_correlation"] = dup_rho
            why = DUPLICATE_PREFERENCE.get((dup_of, m["symbol"]))
            reason = f"correlation {dup_rho} with {dup_of}"
            if why:
                reason += f"; {why[1]}"
            record(m["symbol"], m["asset_class"], m["economic_group"],
                   DROP_DUPLICATE, reason, d, duplicate_of=dup_of)
            continue
        kept.append(m)

    # ---- currency diagnostics + staleness --------------------------------- #
    admitted_fx_codes = {k["currency"] for k in kept
                         if k["asset_class"] == AC_FX}
    panel_end = max(k["close"].index[-1] for k in kept) if kept else None
    final: list = []
    for m in kept:
        if panel_end is not None:
            gap = int(np.busday_count(m["close"].index[-1].date(),
                                      panel_end.date()))
            if gap > MAX_STALE_SESSIONS:
                record(m["symbol"], m["asset_class"], m["economic_group"],
                       DROP_STALE, f"last observation {gap} sessions before "
                                   f"the panel end", m["diagnostics"])
                continue
        cur = m["currency"]
        if m["asset_class"] == AC_EQUITY and cur != "USD":
            if cur not in admitted_fx_codes:
                # The currency failed R2/R3, so its variance is a policy band.
                # A regression on it produces an unstable beta that says
                # nothing about how the index is denominated.
                diag = {"state": "FX_NOT_ADMITTED_PEGGED_OR_ADMINISTERED",
                        "beta": None, "t_stat": None}
                uncertain = False
            else:
                diag = currency_denomination_diagnostic(m["close"],
                                                        fx_series.get(cur))
                beta = diag.get("beta")
                uncertain = beta is not None and beta > CURRENCY_BETA_LOCAL_MAX
        elif m["asset_class"] == AC_EQUITY:
            diag = currency_denomination_diagnostic(
                m["close"], fx_series.get(_home_currency_guess(m["symbol"])))
            beta = diag.get("beta")
            uncertain = beta is not None and beta < CURRENCY_BETA_USD_MIN
        else:
            diag, uncertain = {"state": "NOT_APPLICABLE"}, False
        m["currency_diagnostic"] = diag
        m["currency_label_uncertain"] = bool(uncertain)
        record(m["symbol"], m["asset_class"], m["economic_group"], KEEP,
               "admitted", m["diagnostics"],
               currency=cur, currency_diagnostic=diag,
               currency_label_uncertain=bool(uncertain))
        final.append(m)

    state_series = {}
    for sym in COMPOSITE_EXCLUSIONS:
        s = load_close(sym)
        if s is not None and s.size >= MIN_MARKET_SESSIONS:
            state_series[sym] = s

    return {"markets": final, "decisions": decisions,
            "state_series": state_series, "fx_series": fx_series,
            "panel_start": panel_start}


#: Home currency for equity indices the vendor labels USD, used only to run the
#: contradiction diagnostic against them.
_HOME_CURRENCY_GUESS = {"$BVSP": "BRL", "$NIF": "INR", "$SSEC": "CNY",
                        "$RTS": "RUB"}


def _home_currency_guess(symbol: str) -> Optional[str]:
    return _HOME_CURRENCY_GUESS.get(symbol)


# --------------------------------------------------------------------------- #
# Artifacts
# --------------------------------------------------------------------------- #
def summarise(built: dict) -> dict:
    markets = built["markets"]
    by_class: dict = {}
    by_group: dict = {}
    for m in markets:
        by_class[m["asset_class"]] = by_class.get(m["asset_class"], 0) + 1
        by_group[m["economic_group"]] = by_group.get(m["economic_group"], 0) + 1
    firsts = [m["diagnostics"]["first_date"] for m in markets]
    lasts = [m["diagnostics"]["last_date"] for m in markets]
    return {
        "market_count": len(markets),
        "asset_class_count": len(by_class),
        "economic_group_count": len(by_group),
        "markets_by_asset_class": dict(sorted(by_class.items())),
        "markets_by_economic_group": dict(sorted(by_group.items())),
        "earliest_first_date": min(firsts) if firsts else None,
        "latest_last_date": max(lasts) if lasts else None,
        "currency_label_uncertain": sorted(
            m["symbol"] for m in markets if m.get("currency_label_uncertain")),
    }


def build_universe_artifact(built: dict, *, campaign_id: str,
                            created_at: str) -> dict:
    markets = [{
        "symbol": m["symbol"],
        "asset_class": m["asset_class"],
        "economic_group": m["economic_group"],
        "currency": m["currency"],
        "source": m["source"],
        "security_name": m["diagnostics"].get("security_name"),
        "first_usable_date": m["diagnostics"]["first_date"],
        "last_usable_date": m["diagnostics"]["last_date"],
        "sessions": m["diagnostics"]["sessions"],
        "zero_return_fraction": m["diagnostics"]["zero_return_fraction"],
        "annual_volatility": m["diagnostics"]["annual_volatility"],
        "currency_diagnostic": m.get("currency_diagnostic"),
        "currency_label_uncertain": m.get("currency_label_uncertain", False),
        "implementability_state": _contract.SIGNAL_RESEARCH_VALID,
    } for m in sorted(built["markets"], key=lambda x: x["symbol"])]

    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "selection_rules": {
            "R1_fx_expressed_as_xxxusd_sourced_by_resolution": True,
            "R2_max_zero_return_fraction": MAX_ZERO_RETURN_FRACTION,
            "R3_min_annual_volatility": MIN_ANNUAL_VOLATILITY,
            "R4_max_duplicate_correlation": MAX_DUPLICATE_CORRELATION,
            "R5_min_market_sessions": MIN_MARKET_SESSIONS,
            "R5_max_stale_sessions": MAX_STALE_SESSIONS,
            "R6_composites_excluded": sorted(COMPOSITE_EXCLUSIONS),
        },
        "summary": summarise(built),
        "markets": markets,
        "decisions": sorted(built["decisions"], key=lambda d: str(d["symbol"])),
        "state_series_available": sorted(built["state_series"]),
        "implementability": {
            "state": _contract.UNIVERSE_IMPLEMENTABILITY_STATE,
            "futures_implementability_claimable":
                _contract.FUTURES_IMPLEMENTABILITY_CLAIMABLE,
            "reason": (
                "the owned Continuous Futures entitlement contains ONE market "
                "(&ES). Roll yield, contract selection, execution price and "
                "futures transaction-cost semantics are not supported by the "
                "owned data for any other market in this universe, so a signal "
                "result here is NOT a proof of futures implementability"),
            "additional_gaps": [
                "spot FX excludes the interest differential, which is a "
                "first-order component of an FX position's return",
                "commodity sub-indices are index levels, not positions in a "
                "roll-managed futures stack",
                "equity index returns exclude dividends while bond index "
                "returns include coupon",
            ],
        },
    }
    body = r33.artifact_body(UNIVERSE_SCHEMA, payload)
    body["universe_hash"] = r33.sha(payload)
    return body


def build_inventory_artifact(built: dict, *, campaign_id: str,
                             created_at: str, vendor_databases: dict,
                             dividend_gap: Optional[dict] = None) -> dict:
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "vendor_databases": vendor_databases,
        "continuous_futures_entitlement": {
            "symbols": vendor_databases.get("Continuous Futures", {}).get(
                "symbols", []),
            "count": vendor_databases.get("Continuous Futures", {}).get(
                "count", 0),
            "finding": (
                "the owned Continuous Futures database contains ONE market. "
                "The broad cross-market universe required by this release is "
                "therefore assembled from world equity indices, bond "
                "total-return indices, commodity sub-indices and Forex Spot, "
                "and is labelled SIGNAL_RESEARCH_VALID"),
        },
        "return_definition": {
            "equity_indices_exclude_dividends":
                _contract.EQUITY_INDICES_EXCLUDE_DIVIDENDS,
            "bond_indices_include_coupon":
                _contract.BOND_INDICES_INCLUDE_COUPON,
            "fx_spot_excludes_carry": _contract.FX_SPOT_EXCLUDES_CARRY,
            "measured_equity_dividend_gap": dividend_gap,
            "consequence": (
                "a naive long-only cross-market comparison is biased towards "
                "bonds. The primary economic construction is therefore "
                "cross-sectional and zero-mean within asset class, where a "
                "constant per-market drift offset largely cancels"),
        },
        "universe_summary": summarise(built),
        "decisions": sorted(built["decisions"], key=lambda d: str(d["symbol"])),
    }
    body = r33.artifact_body(INVENTORY_SCHEMA, payload)
    body["inventory_hash"] = r33.sha(payload)
    return body


def measure_dividend_gap() -> Optional[dict]:
    """Measure the equity dividend drag from the owned ``$SPX``/``$SPXTR`` pair.

    Declared rather than assumed: the price index and the total-return index of
    the SAME benchmark differ by exactly the reinvested dividend, so the gap is
    observable instead of being a remembered rule of thumb.
    """
    px, tr = load_close("$SPX"), load_close("$SPXTR")
    if px is None or tr is None:
        return None
    j = pd.concat([np.log(px).diff(), np.log(tr).diff()], axis=1,
                  join="inner").dropna()
    if len(j) < 500:
        return None
    gap = float((j.iloc[:, 1] - j.iloc[:, 0]).mean() * 252.0)
    return {"annualised_gap": round(gap, 6), "observations": int(len(j)),
            "first_date": str(j.index[0].date()),
            "last_date": str(j.index[-1].date()),
            "measured_from": list(_contract.DIVIDEND_GAP_MEASURED_FROM)}


def vendor_database_summary() -> dict:
    """What the vendor ACTUALLY delivers, measured at run time.

    Recorded because the headline finding of this release's data inventory is a
    measurement: the owned Continuous Futures database contains one market.
    """
    nd = _nd()
    out = {}
    for name in nd.databases():
        try:
            syms = nd.database_symbols(name)
        except Exception as exc:
            out[name] = {"count": None, "error": f"{type(exc).__name__}: {exc}"}
            continue
        entry = {"count": len(syms)}
        if len(syms) <= 20:
            entry["symbols"] = list(syms)
        out[name] = entry
    return out


def universe_path(campaign_id: str) -> Path:
    return r33.campaign_dir(campaign_id) / UNIVERSE_ARTIFACT


def inventory_path(campaign_id: str) -> Path:
    return r33.campaign_dir(campaign_id) / INVENTORY_ARTIFACT
