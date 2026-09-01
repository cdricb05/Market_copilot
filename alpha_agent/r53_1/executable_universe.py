r"""alpha_agent.r53_1.executable_universe - making cross-market alpha
EXECUTABLE at the actual NAV.

Release 53 measured that at ~$99k NAV every canonical non-equity contract
fails ``UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV`` - the unit-size wall binds
AHEAD of the evidence gate. This module attacks that wall with owned data
only:

1. **micro/mini contract probe** - every plausible smaller economically
   equivalent contract is looked up in the OWNED Norgate databases through
   the canonical reference-data owner (``api.market_reference_data``);
   nothing is assumed, availability and point values are read, not guessed;
2. **feasibility arithmetic** - unit notional, notional/NAV, minimum NAV
   under the production 10% name cap and under shadow 15/20/25% risk-unit
   limits, whole-unit executability at the actual NAV;
3. **ETF proxy analysis** - liquid ETF implementations classified honestly:
   SAME_THESIS_SAME_MARKET / PROXY_WITH_BASIS_RISK / NOT_EQUIVALENT;
4. **futures risk-model study** - notional is a SAFETY cap, not a risk
   model: per contract the study reports dollar volatility, margin ratio and
   a stress loss so a future risk owner could size by volatility
   contribution while KEEPING the notional cap as a hard limit.

SHADOW research only. No production limit is weakened, no instrument is
substituted in production, no order exists.
"""
from __future__ import annotations

from typing import Any, Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, research_dir,
               safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53_1.executable_universe"

ARTIFACT_MAP = "R53_1_EXECUTABLE_INSTRUMENT_MAP.json"
ARTIFACT_MICRO = "R53_1_MICRO_CONTRACT_FEASIBILITY.json"
ARTIFACT_PROXY = "R53_1_PROXY_INSTRUMENT_ANALYSIS.json"

#: Production name cap plus the three SHADOW risk-unit limits to test.
CAP_FRACTIONS = (0.10, 0.15, 0.20, 0.25)

#: Candidate smaller/economically-equivalent contracts per canonical market.
#: CANDIDATES, not assumptions - the probe reads the owned database and only
#: what it actually serves survives into the map.
MICRO_PROBE_CANDIDATES = {
    "&ES": ("&MES",), "&NQ": ("&MNQ",), "&RTY": ("&M2K",), "&YM": ("&MYM",),
    "&BTC": ("&MBT",), "&ETH": ("&MET",),
    "&GC": ("&MGC",), "&SI": ("&SIL",), "&CL": ("&MCL", "&QM"),
    "&NG": ("&QG", "&MNG"), "&HG": ("&MHG", "&QC"),
    "&ZN": ("&10Y", "&ZT"), "&ZB": ("&30Y",), "&ZF": ("&5YY",),
    "&ZT": ("&2YY",),
    "&VX": ("&VXM",),
    "&6E": ("&M6E",), "&6A": ("&M6A",), "&6B": ("&M6B",),
    "&6J": ("&MJY",), "&6C": ("&MCD",), "&6S": ("&MSF",),
}

#: The canonical contracts of the serious non-equity sleeves (reference-data
#: facts: these are the instruments the R46/R51/R52 challenger books trade).
SLEEVE_CANONICAL_CONTRACTS = {
    "sleeve_equity_index_futures": ("&ES", "&NQ", "&RTY", "&YM"),
    "sleeve_volatility_futures": ("&VX",),
    "sleeve_commodity_futures": ("&GC", "&SI", "&CL", "&HG", "&NG"),
    "sleeve_fx_futures": ("&6E", "&6B", "&6J", "&6A", "&6C", "&6S"),
    "sleeve_rates_futures": ("&ZN", "&ZT", "&ZB", "&ZF"),
    "sleeve_crypto_futures": ("&BTC", "&ETH"),
}

#: Liquid ETF implementations, classified BEFORE prices are read. The label
#: answers one question: does holding the ETF express the SAME economic
#: thesis in the SAME market, or something correlated-but-different?
ETF_PROXIES = (
    {"sleeve": "sleeve_equity_index_futures", "future": "&ES", "etf": "SPY",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "same index, full replication, negligible tracking error; the "
             "cost of carry moves from roll to expense ratio (9.45bp)"},
    {"sleeve": "sleeve_equity_index_futures", "future": "&NQ", "etf": "QQQ",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "same index (Nasdaq-100), full replication, 20bp expense"},
    {"sleeve": "sleeve_equity_index_futures", "future": "&RTY", "etf": "IWM",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "same index (Russell 2000), 19bp expense"},
    {"sleeve": "sleeve_commodity_futures", "future": "&GC", "etf": "GLD",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "physically-backed spot gold; expresses the gold thesis without "
             "roll yield - basis to the FUTURES curve exists but the R46 "
             "commodity momentum signal is a price-direction thesis, which "
             "spot metal carries; 40bp expense"},
    {"sleeve": "sleeve_commodity_futures", "future": "&SI", "etf": "SLV",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "physically-backed spot silver, 50bp expense"},
    {"sleeve": "sleeve_commodity_futures", "future": "&CL", "etf": "USO",
     "classification": "PROXY_WITH_BASIS_RISK",
     "note": "holds a rolling futures strip: contango roll cost detaches USO "
             "from spot WTI materially over weeks - the exact horizon the "
             "momentum signal trades; usable only with the roll drag "
             "modelled"},
    {"sleeve": "sleeve_commodity_futures", "future": "&NG", "etf": "UNG",
     "classification": "PROXY_WITH_BASIS_RISK",
     "note": "rolling futures strip with severe historical contango decay"},
    {"sleeve": "sleeve_rates_futures", "future": "&ZN", "etf": "IEF",
     "classification": "PROXY_WITH_BASIS_RISK",
     "note": "7-10y Treasury ladder vs the CTD-driven 10y note future: same "
             "duration thesis, different instrument (cash bonds, coupon "
             "carry, no roll); duration ~7.6 vs ~6.3 - sizeable but "
             "modellable basis"},
    {"sleeve": "sleeve_rates_futures", "future": "&ZB", "etf": "TLT",
     "classification": "PROXY_WITH_BASIS_RISK",
     "note": "20+y ladder vs the bond future's CTD basket"},
    {"sleeve": "sleeve_rates_futures", "future": "&ZT", "etf": "SHY",
     "classification": "PROXY_WITH_BASIS_RISK",
     "note": "1-3y ladder; tiny duration means the signal mostly cannot be "
             "expressed at equity-scale position sizes"},
    {"sleeve": "sleeve_fx_futures", "future": "&DX", "etf": "UUP",
     "classification": "PROXY_WITH_BASIS_RISK",
     "note": "long-USD basket via futures inside a fund wrapper; K-1 tax "
             "wrapper, 77bp expense, tracks DXY direction adequately"},
    {"sleeve": "sleeve_fx_futures", "future": "&6E", "etf": "FXE",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "trust holding actual euro deposits: spot EURUSD exposure; the "
             "R51 carry signal is a CIP/forward-curve thesis, and holding "
             "spot euro earns the deposit rate - the carry leg survives via "
             "the trust's interest accrual net of 40bp"},
    {"sleeve": "sleeve_fx_futures", "future": "&6J", "etf": "FXY",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "trust holding yen deposits, 40bp expense"},
    {"sleeve": "sleeve_volatility_futures", "future": "&VX", "etf": "VIXY",
     "classification": "NOT_EQUIVALENT",
     "note": "short-term VX futures strip with structural roll decay; the "
             "R46 VX term-structure CARRY signal trades the curve shape "
             "itself - an ETF that IS the roll cannot express a curve-"
             "relative position; and the current signal is short/flat, "
             "which a long-only book cannot hold via a long-vol ETF"},
    {"sleeve": "sleeve_crypto_futures", "future": "&BTC", "etf": "IBIT",
     "classification": "SAME_THESIS_SAME_MARKET",
     "note": "spot bitcoin ETF; expresses spot direction without CME basis; "
             "NOTE: not in the owned survivorship-safe equity panel - "
             "pricing exists but PIT research history does not"},
)


def _mrd():
    from paper_trader.api import market_reference_data as mrd
    return mrd


def actual_nav() -> Optional[float]:
    try:
        from paper_trader.api import portfolio_state as psmod
        ps = psmod.load_portfolio_state()
        nav = (((ps or {}).get("capital") or {}).get("nav"))
        return float(nav) if nav is not None else None
    except Exception:  # noqa: BLE001
        return None


# --------------------------------------------------------------------------- #
# Contract probe (owned data only)
# --------------------------------------------------------------------------- #
def probe_contract(symbol: str) -> dict:
    """Everything owned data says about one continuous futures market."""
    mrd = _mrd()
    meta = mrd.futures_metadata(symbol)
    out: dict[str, Any] = {"symbol": symbol, "meta_state": meta.get("state")}
    if meta.get("state") != "OK":
        out.update({"owned": False, "detail": meta.get("error")})
        return out
    bars = mrd.daily_bars(symbol)
    out.update({
        "owned": bool(bars),
        "market_name": meta.get("market_name"),
        "exchange": meta.get("exchange"),
        "currency": meta.get("currency"),
        "point_value": meta.get("point_value"),
        "initial_margin_per_unit": meta.get("initial_margin"),
        "n_sessions": len(bars),
        "first_session": bars[0][0] if bars else None,
        "last_session": bars[-1][0] if bars else None,
        "last_close": bars[-1][1] if bars else None,
    })
    if bars:
        closes = [b[1] for b in bars[-61:]]
        rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))
                if closes[i - 1]]
        if len(rets) >= 20:
            mean = sum(rets) / len(rets)
            var = sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)
            out["daily_sigma_60d"] = round(var ** 0.5, 6)
        vols = [b[2] for b in bars[-21:] if b[2]]
        out["median_volume_21d"] = (sorted(vols)[len(vols) // 2]
                                    if vols else None)
        pv, close = out["point_value"], out["last_close"]
        if pv and close:
            out["unit_notional_usd"] = round(float(pv) * float(close), 2)
            if out.get("daily_sigma_60d"):
                out["dollar_vol_per_unit_daily"] = round(
                    out["unit_notional_usd"] * out["daily_sigma_60d"], 2)
            if out.get("initial_margin_per_unit"):
                out["margin_over_notional"] = round(
                    out["initial_margin_per_unit"] / out["unit_notional_usd"], 4)
            out["stress_loss_3sigma_usd"] = (
                round(3.0 * out["dollar_vol_per_unit_daily"], 2)
                if out.get("dollar_vol_per_unit_daily") else None)
    return out


def feasibility_row(contract: dict, nav: float) -> dict:
    """Whole-unit executability of one probed contract at one NAV."""
    row = {"symbol": contract["symbol"], "nav": nav}
    un = contract.get("unit_notional_usd")
    if not un:
        row["state"] = "NOT_PRICEABLE_FROM_OWNED_DATA"
        return row
    row["unit_notional_usd"] = un
    row["notional_over_nav"] = round(un / nav, 4)
    for cap in CAP_FRACTIONS:
        key = "cap_%d" % int(cap * 100)
        row["min_nav_under_" + key] = round(un / cap, 2)
        row["executable_under_" + key] = bool(un <= cap * nav)
    margin = contract.get("initial_margin_per_unit")
    if margin:
        row["margin_per_unit_usd"] = margin
        row["margin_executable"] = bool(margin <= nav)
    row["state"] = ("EXECUTABLE_AT_PRODUCTION_CAP"
                    if row.get("executable_under_cap_10")
                    else ("EXECUTABLE_ONLY_UNDER_WIDER_CAP"
                          if row.get("executable_under_cap_25")
                          else "NOT_EXECUTABLE_EVEN_AT_25PCT"))
    return row


# --------------------------------------------------------------------------- #
# The full study
# --------------------------------------------------------------------------- #
def run_study(nav: Optional[float] = None) -> dict:
    nav = float(nav) if nav else (actual_nav() or 99000.0)
    probed: dict[str, dict] = {}

    def _probe(sym: str) -> dict:
        if sym not in probed:
            probed[sym] = probe_contract(sym)
        return probed[sym]

    sleeves = {}
    for sleeve_id, canonicals in SLEEVE_CANONICAL_CONTRACTS.items():
        rows = []
        for canon in canonicals:
            c = _probe(canon)
            entry = {
                "canonical": canon,
                "canonical_probe": c,
                "canonical_feasibility": (feasibility_row(c, nav)
                                          if c.get("owned") else None),
                "alternatives": [],
            }
            for alt in MICRO_PROBE_CANDIDATES.get(canon, ()):
                a = _probe(alt)
                alt_row = {"symbol": alt, "probe": a,
                           "feasibility": (feasibility_row(a, nav)
                                           if a.get("owned") else None)}
                if a.get("owned") and c.get("owned"):
                    big = c.get("unit_notional_usd") or 0
                    small = a.get("unit_notional_usd") or 0
                    if big and small:
                        alt_row["size_ratio_vs_canonical"] = round(
                            small / big, 4)
                entry["alternatives"].append(alt_row)
            owned_alts = [a for a in entry["alternatives"]
                          if (a.get("feasibility") or {}).get(
                              "executable_under_cap_10")]
            entry["unit_size_problem_solved_at_production_cap"] = bool(
                owned_alts
                or (entry["canonical_feasibility"] or {}).get(
                    "executable_under_cap_10"))
            entry["solving_contracts"] = [a["symbol"] for a in owned_alts]
            rows.append(entry)
        sleeves[sleeve_id] = rows

    return {"nav": nav, "sleeves": sleeves,
            "probed_symbols": sorted(probed.keys()),
            "n_owned": sum(1 for p in probed.values() if p.get("owned"))}


def proxy_analysis() -> dict:
    """ETF proxy pricing and share-level feasibility (trivially granular)."""
    from paper_trader.engine import market_data as md
    tickers = sorted({p["etf"] for p in ETF_PROXIES})
    prices, failures = md.fetch_latest_prices(tickers)
    px = {r["ticker"]: float(r["price"]) for r in prices}
    rows = []
    for p in ETF_PROXIES:
        row = dict(p)
        price = px.get(p["etf"])
        row["last_price"] = price
        row["unit_notional_usd"] = price          # one share
        row["granularity"] = "ONE_SHARE"
        row["survivorship_safe_history_owned"] = p["etf"] not in ("IBIT",)
        rows.append(row)
    return {"proxies": rows, "price_failures": failures,
            "classification_vocabulary": ["SAME_THESIS_SAME_MARKET",
                                          "PROXY_WITH_BASIS_RISK",
                                          "NOT_EQUIVALENT"]}


def risk_model_study(study: dict) -> dict:
    """Notional caps are safety limits, not a risk model: what volatility-
    aware sizing would look like, per owned contract, SHADOW only."""
    rows = []
    seen = set()
    for sleeve_rows in study["sleeves"].values():
        for entry in sleeve_rows:
            for c in ([entry["canonical_probe"]]
                      + [a["probe"] for a in entry["alternatives"]]):
                sym = c.get("symbol")
                if not c.get("owned") or sym in seen:
                    continue
                seen.add(sym)
                dv = c.get("dollar_vol_per_unit_daily")
                rows.append({
                    "symbol": sym,
                    "unit_notional_usd": c.get("unit_notional_usd"),
                    "daily_sigma_60d": c.get("daily_sigma_60d"),
                    "dollar_vol_per_unit_daily": dv,
                    "margin_over_notional": c.get("margin_over_notional"),
                    "stress_loss_3sigma_usd": c.get("stress_loss_3sigma_usd"),
                    "units_for_50bp_daily_nav_vol_at_nav": (
                        round((0.005 * study["nav"]) / dv, 2) if dv else None),
                    "units_under_10pct_notional_cap_at_nav": (
                        round((0.10 * study["nav"])
                              / c["unit_notional_usd"], 2)
                        if c.get("unit_notional_usd") else None),
                })
    return {
        "question": "should the cross-asset risk owner eventually size "
                    "futures by volatility contribution / margin / stress "
                    "loss while RETAINING notional caps as safety limits?",
        "observation": "a notional cap treats a 2-year note future and a "
                       "bitcoin future as the same risk per dollar of "
                       "notional; the dollar-vol column shows they differ "
                       "by orders of magnitude",
        "shadow_only": True,
        "production_change": "NONE - research input to a future release",
        "contracts": rows,
    }


def write_artifacts() -> dict:
    nav = actual_nav() or 99000.0
    study = run_study(nav)
    proxies = proxy_analysis()
    risk = risk_model_study(study)

    body_map = artifact_body(
        "r53_1_executable_instrument_map/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        nav_used=study["nav"],
        nav_source="api.portfolio_state (canonical)" if actual_nav()
                   else "fallback constant",
        sleeves=study["sleeves"],
        probed_symbols=study["probed_symbols"],
        n_owned=study["n_owned"],
        futures_risk_model_study=risk,
        **safety_block())
    write_json(research_dir() / ARTIFACT_MAP, body_map)

    micro = artifact_body(
        "r53_1_micro_contract_feasibility/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        nav_used=study["nav"], cap_fractions=list(CAP_FRACTIONS),
        summary={
            sleeve: {
                "solved": [e["canonical"] for e in rows
                           if e["unit_size_problem_solved_at_production_cap"]],
                "unsolved": [e["canonical"] for e in rows
                             if not e[
                                 "unit_size_problem_solved_at_production_cap"]],
                "solving_contracts": sorted({s for e in rows
                                             for s in e["solving_contracts"]}),
            } for sleeve, rows in study["sleeves"].items()},
        **safety_block())
    write_json(research_dir() / ARTIFACT_MICRO, micro)

    body_proxy = artifact_body(
        "r53_1_proxy_instrument_analysis/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        **proxies, **safety_block())
    write_json(research_dir() / ARTIFACT_PROXY, body_proxy)

    return {"map": body_map, "micro": micro, "proxy": body_proxy}
