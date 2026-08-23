"""alpha_agent.r41.data_inventory - Track 1/3: the MEASURED frequency
inventory of everything the estate can research TODAY, and the horizon map
it implies.

Each family carries the eight contract facts (SOURCE_FREQUENCY,
OBSERVABLE_LATENCY, EARLIEST_HISTORY, LATEST_HISTORY,
TARGET_HORIZONS_SUPPORTED, DECISION_HORIZONS_SUPPORTED,
IMPLEMENTATION_LATENCY, PIT_STATE). Dates are measured from local bytes
where they exist (curve store, acquisition manifests), never typed from
memory of a marketing page.
"""
from __future__ import annotations

import datetime as _dt
import json

from . import artifact_body, campaign_dir, data_dir, read_json, sha, write_json
from . import contract as C
from . import curve_state as CS

CALCULATION_OWNER = "alpha_agent.r41.data_inventory"
ARTIFACT_NAME = "owned_data_frequency_inventory.json"

DAILY_HORIZONS = ["1s", "2s", "5s", "10s", "21s", "42s", "63s", "EVENT"]
INTRADAY_HORIZONS = ["1m", "5m", "15m", "30m", "60m", "2h", "4h"] \
    + DAILY_HORIZONS


def _curve_facts() -> dict:
    man = read_json(data_dir("curves") / "store_manifest.json") or {}
    mk = man.get("markets", {})
    ok = {k: v for k, v in mk.items() if v.get("state") in ("OK", "CACHED")}
    firsts = [v.get("first") for v in ok.values() if v.get("first")]
    lasts = [v.get("last") for v in ok.values() if v.get("last")]
    return {"n_markets": len(ok),
            "earliest": min(firsts) if firsts else None,
            "latest": max(lasts) if lasts else None}


def _duka_facts() -> dict:
    man = json.loads((data_dir("dukascopy") / "acquisition_manifest.json")
                     .read_text(encoding="utf-8")) \
        if (data_dir("dukascopy") / "acquisition_manifest.json").exists() \
        else {}
    per = {}
    for key in man:
        sym = key.rsplit("_", 1)[0]
        month = key.rsplit("_", 1)[1]
        row = per.setdefault(sym, {"months": 0, "first": month,
                                   "last": month, "bars": 0})
        row["months"] += 1
        row["first"] = min(row["first"], month)
        row["last"] = max(row["last"], month)
        row["bars"] += man[key].get("bars", 0)
    return per


def build(campaign_id: str = None) -> dict:
    curve = _curve_facts()
    duka = _duka_facts()
    binance_man = json.loads(
        (data_dir("binance") / "acquisition_manifest.json").read_text(
            encoding="utf-8")) \
        if (data_dir("binance") / "acquisition_manifest.json").exists() else {}
    fams = {}
    fams["NORGATE_DATED_FUTURES"] = {
        "SOURCE_FREQUENCY": "DAILY_SETTLEMENT",
        "OBSERVABLE_LATENCY": "same evening (NDU hourly sync)",
        "EARLIEST_HISTORY": curve["earliest"],
        "LATEST_HISTORY": curve["latest"],
        "TARGET_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "IMPLEMENTATION_LATENCY": "next session open/settlement",
        "PIT_STATE": "PIT_TRUE (dated contracts, exchange schedule "
                     "metadata, observable roll)",
        "breadth": "%d markets, full dated curves" % curve["n_markets"],
        "level": "LEVEL_3_NATIVE"}
    fams["NORGATE_US_EQUITIES_ETF"] = {
        "SOURCE_FREQUENCY": "DAILY", "OBSERVABLE_LATENCY": "same evening",
        "EARLIEST_HISTORY": "1990s (delisted included)",
        "LATEST_HISTORY": "current",
        "TARGET_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "IMPLEMENTATION_LATENCY": "next session",
        "PIT_STATE": "PIT_TRUE via phase-24 identity layer",
        "level": "LEVEL_3_NATIVE"}
    fams["CBOE_VOL_INDICES"] = {
        "SOURCE_FREQUENCY": "DAILY",
        "OBSERVABLE_LATENCY": "same evening (public CSV)",
        "EARLIEST_HISTORY": "VIX/SKEW 1990; VIX6M 2008; VIX3M 2009; "
                            "VIX9D 2011; VVIX 2006; VIX1D 2022",
        "LATEST_HISTORY": "current",
        "TARGET_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "IMPLEMENTATION_LATENCY": "via VX futures next session",
        "PIT_STATE": "PIT_TRUE (published index closes)",
        "level": "LEVEL_1_SIGNAL (indices are not tradeable)"}
    fams["GOV_YIELD_CURVES_FREE"] = {
        "SOURCE_FREQUENCY": "DAILY",
        "OBSERVABLE_LATENCY": "next day (official publications)",
        "EARLIEST_HISTORY": "US CMT 1990 (FRED); ECB AAA 2004; JGB 1974; "
                            "BoC 1990s; RBA current file",
        "LATEST_HISTORY": "current",
        "TARGET_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "IMPLEMENTATION_LATENCY": "via bond futures next session",
        "PIT_STATE": "PIT_TRUE (daily publications; yields not revised)",
        "level": "LEVEL_1_SIGNAL"}
    fams["DUKASCOPY_MINUTE"] = {
        "SOURCE_FREQUENCY": "TICK / 1-MINUTE",
        "OBSERVABLE_LATENCY": "real-time capable; historical files public",
        "EARLIEST_HISTORY": {"EURUSD": "2003 (ticks measured)",
                             "XAUUSD": "<=2010", "USA500IDXUSD": "2014",
                             "LIGHTCMDUSD": "2014", "BUNDTREUR": "2018",
                             "acquired": duka},
        "LATEST_HISTORY": "current",
        "TARGET_HORIZONS_SUPPORTED": INTRADAY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": INTRADAY_HORIZONS,
        "IMPLEMENTATION_LATENCY": "1 bar",
        "PIT_STATE": "PIT_TRUE (as-traded ticks with bid/ask)",
        "level": "FX/metals LEVEL_3-adjacent (one venue's liquidity); "
                 "index/commodity/bond CFDs LEVEL_2_PROXY"}
    fams["BINANCE_ARCHIVE"] = {
        "SOURCE_FREQUENCY": "1-MINUTE klines (signed taker flow), 8h "
                            "funding, daily metrics",
        "OBSERVABLE_LATENCY": "real-time capable",
        "EARLIEST_HISTORY": "spot 2017-08; perp 2019-09; funding 2019-09; "
                            "metrics 2021",
        "LATEST_HISTORY": "current",
        "TARGET_HORIZONS_SUPPORTED": INTRADAY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": INTRADAY_HORIZONS + ["8h_funding"],
        "IMPLEMENTATION_LATENCY": "1 bar / next funding",
        "PIT_STATE": "PIT_TRUE (exchange archive); survivorship handled "
                     "via full symbol listing",
        "months_acquired": len(binance_man),
        "level": "LEVEL_3_NATIVE (one venue)"}
    fams["TIINGO_IEX_MINUTE"] = {
        "SOURCE_FREQUENCY": "1-MINUTE (IEX prints only, no volume)",
        "OBSERVABLE_LATENCY": "real-time capable on the existing key",
        "EARLIEST_HISTORY": "2017-01 (measured)",
        "LATEST_HISTORY": "current",
        "TARGET_HORIZONS_SUPPORTED": INTRADAY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": INTRADAY_HORIZONS,
        "IMPLEMENTATION_LATENCY": "1 bar",
        "PIT_STATE": "PIT_TRUE (historical prints)",
        "level": "LEVEL_2 (IEX venue subset of consolidated tape)"}
    fams["TARDIS_L2_SAMPLE_DAYS"] = {
        "SOURCE_FREQUENCY": "TICK / L2 order book",
        "OBSERVABLE_LATENCY": "n/a (historical samples)",
        "EARLIEST_HISTORY": "first day of each month, 2020-2026 (acquired)",
        "LATEST_HISTORY": "2026-07-01",
        "TARGET_HORIZONS_SUPPORTED": ["1m", "5m", "event"],
        "DECISION_HORIZONS_SUPPORTED": ["date-sampled research only"],
        "IMPLEMENTATION_LATENCY": "n/a",
        "PIT_STATE": "PIT_TRUE",
        "level": "LEVEL_3_NATIVE (sampled days; not a continuous panel)"}
    fams["MACRO_ALFRED_COT_EIA_NYFED_SEC"] = {
        "SOURCE_FREQUENCY": "WEEKLY / MONTHLY (release-stamped)",
        "OBSERVABLE_LATENCY": "release schedule",
        "EARLIEST_HISTORY": "varies (ALFRED vintages; COT 1986; EIA 1990s; "
                            "NY Fed 1998; SEC 1993)",
        "LATEST_HISTORY": "current",
        "TARGET_HORIZONS_SUPPORTED": DAILY_HORIZONS,
        "DECISION_HORIZONS_SUPPORTED": ["EVENT", "5s", "21s"],
        "IMPLEMENTATION_LATENCY": "next session after release",
        "PIT_STATE": "PIT_TRUE via R39 vintage discipline "
                     "(revised-snapshot series excluded)",
        "level": "LEVEL_1_SIGNAL"}
    fams["ANALYST_REVISIONS"] = {
        "SOURCE_FREQUENCY": "DAILY vintages (would be)",
        "OBSERVABLE_LATENCY": "n/a",
        "EARLIEST_HISTORY": "NOT OWNED - Zacks NDL key serves a "
                            "megacap SAMPLE tier (~Dow names) with "
                            "current-snapshot estimates only",
        "LATEST_HISTORY": "n/a",
        "TARGET_HORIZONS_SUPPORTED": [],
        "DECISION_HORIZONS_SUPPORTED": [],
        "IMPLEMENTATION_LATENCY": "n/a",
        "PIT_STATE": "BLOCKED - historical consensus VINTAGES require "
                     "purchase (Zacks full / Steele / Intrinio)",
        "level": "MISSING_INFORMATION_LANE"}
    fams["OPTIONS_SURFACES"] = {
        "SOURCE_FREQUENCY": "DAILY EOD chains (would be)",
        "OBSERVABLE_LATENCY": "n/a",
        "EARLIEST_HISTORY": "NOT OWNED - AlphaVantage HISTORICAL_OPTIONS "
                            "is premium-only (measured 2026-08-23); free "
                            "Cboe indices are LEVEL 1 summaries",
        "LATEST_HISTORY": "n/a",
        "TARGET_HORIZONS_SUPPORTED": [],
        "DECISION_HORIZONS_SUPPORTED": [],
        "IMPLEMENTATION_LATENCY": "n/a",
        "PIT_STATE": "BLOCKED - PAYMENT_REQUIRED",
        "level": "MISSING_INFORMATION_LANE"}
    fams["INTRADAY_NATIVE_FUTURES"] = {
        "SOURCE_FREQUENCY": "1-MINUTE (would be)",
        "OBSERVABLE_LATENCY": "n/a",
        "EARLIEST_HISTORY": "NOT OWNED - Norgate is EOD; free lanes cover "
                            "CFD proxies only",
        "LATEST_HISTORY": "n/a",
        "TARGET_HORIZONS_SUPPORTED": [],
        "DECISION_HORIZONS_SUPPORTED": [],
        "IMPLEMENTATION_LATENCY": "n/a",
        "PIT_STATE": "BLOCKED - PAYMENT_REQUIRED (Databento / FirstRate / "
                     "Portara)",
        "level": "MISSING_INFORMATION_LANE"}

    answers = {
        "researchable_intraday_today": [
            "FX spot (EURUSD, USDJPY, majors) - Dukascopy 1m, 2003->",
            "gold/silver spot - Dukascopy 1m, <=2010->",
            "S&P 500 / DAX via index CFD proxies - 2014-> (LEVEL 2)",
            "WTI/Brent via CFD proxies - 2014-> (LEVEL 2)",
            "Bund / T-bond via CFD proxies - 2018-> (LEVEL 2)",
            "BTC/ETH spot+perp with signed flow and funding - 2017->",
            "US equity ETFs via Tiingo IEX 1m - 2017-> (no volume)",
        ],
        "needs_new_data": [
            "native intraday FUTURES (rates, energy, index) with volume/OI",
            "options/volatility surfaces (any underlying)",
            "historical analyst-revision vintages",
            "deep credit history (ICE OAS now capped at ~3y on FRED)",
            "consolidated-tape equity trades/quotes",
        ]}
    body = artifact_body("r41_owned_data_frequency_inventory/1", {
        "calculation_owner": CALCULATION_OWNER,
        "measured_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "families": fams, "answers": answers,
        "no_interpolated_intraday": C.NO_INTERPOLATED_INTRADAY})
    body["inventory_hash"] = sha(body)
    write_json(campaign_dir() / ARTIFACT_NAME, body, immutable=False)
    return body
