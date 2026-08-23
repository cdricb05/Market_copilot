"""alpha_agent.r41.provider_frontier - Track 3: the 2026 provider landscape,
from MEASURED probes and current public pricing pages (fetched this
session), never from memory of provider prestige.

Every row is either a PROBE RESULT (an HTTP call this campaign made) or a
PRICE FACT with its source; blockers use the contract vocabulary. This
registry feeds :mod:`purchase_engine`.
"""
from __future__ import annotations

import datetime as _dt

from . import artifact_body, campaign_dir, sha, write_json

CALCULATION_OWNER = "alpha_agent.r41.provider_frontier"
ARTIFACT_NAME = "provider_frontier_2026.json"

#: A. historical intraday futures
INTRADAY_FUTURES = {
    "DATABENTO_GLBX_MDP3": {
        "coverage": "all CME/CBOT/NYMEX/COMEX futures+options, full book, "
                    "16+ years, 1-minute OHLCV and trades",
        "pricing": "usage $/GB; flat plans $199/mo (Standard), $1750/mo "
                   "(Plus, annual), $4500/mo (Unlimited, annual); $125 free "
                   "credits on signup (6-month expiry, one per team)",
        "source": "databento.com/pricing (fetched 2026-08-23)",
        "blocker": "ACCOUNT_REQUIRED (credits) / PAYMENT_REQUIRED (bulk)",
        "pit_quality": "exchange-native MDP3, definitive",
        "survivorship": "full (delisted contracts included)",
        "note": "cheapest serious per-GB entry for minute bars of ~20 "
                "specific markets; $125 credits would cover a meaningful "
                "OHLCV-1m sample if the operator opens an account"},
    "FIRSTRATE_FUTURES_BUNDLE": {
        "coverage": "130 most active futures, 1/5/30/60-minute + tick, "
                    "individual contracts AND continuous, ~15 years",
        "pricing": "bundle price not shown on the fetched page (order page "
                   "gated); samples free (two-week per instrument; the "
                   "no-registration SPY/SPX/VIX samples measured here are "
                   "ONE YEAR of 1-minute bars)",
        "source": "firstratedata.com (fetched 2026-08-23) + downloaded "
                  "samples (hashes in the acquisition manifest)",
        "blocker": "PAYMENT_REQUIRED",
        "pit_quality": "vendor-consolidated bars; volume included",
        "survivorship": "active-contract bias possible on delisted markets",
        "note": "flat-file simplicity; historically ~$100-400/bundle class"},
    "PORTARA_CQG": {
        "coverage": "deep institutional futures history (1949->), tick/1m",
        "pricing": "quote-based; no public checkout",
        "source": "prior R37 research; not re-fetched (no public price)",
        "blocker": "PAYMENT_REQUIRED + sales conversation",
        "pit_quality": "high", "survivorship": "full"},
    "MASSIVE_FUTURES": {
        "coverage": "CME group trades/quotes/aggregates + flat files",
        "pricing": "futures tier pricing not exposed on the fetched "
                   "pricing page (stocks tiers $29-199/mo shown)",
        "source": "massive.com/pricing (fetched 2026-08-23; Polygon.io "
                  "rebranded to Massive in 2026)",
        "blocker": "PAYMENT_REQUIRED",
        "note": "existing POLYGON_API_KEY predates the rebrand; measured: "
                "minute aggregates serve only a RECENT window on this key "
                "(2026-08-14 OK, 2024-08-14 NOT_AUTHORIZED)"},
    "KIBOT": {
        "coverage": "futures/stocks/ETF intraday history",
        "pricing": "per-dataset; free samples now REQUIRE LOGIN "
                   "(measured '401 Not Logged In', 2026-08-23)",
        "blocker": "ACCOUNT_REQUIRED"},
    "DUKASCOPY_DATAFEED": {
        "coverage": "FX spot ticks 2003->, metals, index/energy/bond CFDs "
                    "2014/2018->, per-hour bi5 + per-day minute candles",
        "pricing": "$0, no account, public datafeed",
        "source": "measured and ACQUIRED this session (manifest hashes)",
        "blocker": None,
        "pit_quality": "as-traded ticks with bid/ask (one venue)",
        "note": "the free intraday lane this release opened"},
}

#: B. options / volatility surfaces
OPTIONS_SURFACES = {
    "CBOE_DATASHOP_OPTION_EOD": {
        "coverage": "OPRA-wide EOD option summaries (SPY/SPX/VIX...), "
                    "2012->, quotes/OHLC/volume/OI, IV+greeks add-on",
        "pricing": "cart-priced per symbol-year; account required to "
                   "check out; sample file available",
        "source": "datashop.cboe.com/option-eod-summary (fetched "
                  "2026-08-23)",
        "blocker": "ACCOUNT_REQUIRED + PAYMENT_REQUIRED"},
    "ORATS_NEAR_EOD": {
        "coverage": "5000+ underlyings since 2007; bid/ask, IV, greeks, "
                    "smoothed surfaces, volume, OI",
        "pricing": "$99/mo recurring; FULL HISTORY 2007-present $599 "
                   "one-time (S3 delivery)",
        "source": "orats.com/near-eod-data (fetched 2026-08-23)",
        "blocker": "PAYMENT_REQUIRED",
        "note": "cheapest credible full options-surface history"},
    "THETADATA": {
        "coverage": "OPRA options history, tick-capable",
        "pricing": "Options Value $40/mo (4y), Standard $80/mo (8y), "
                   "Pro $160/mo (12y)",
        "source": "thetadata.net/pricing (fetched 2026-08-23)",
        "blocker": "PAYMENT_REQUIRED + ACCOUNT_REQUIRED"},
    "OPTIONMETRICS_IVYDB": {
        "coverage": "the academic standard, 1996->, full surfaces",
        "pricing": "institutional licence, quote-based (WRDS channel)",
        "blocker": "PAYMENT_REQUIRED + LICENCE_REQUIRED"},
    "ALPHAVANTAGE_HISTORICAL_OPTIONS": {
        "coverage": "15+ years US chains with IV/greeks",
        "pricing": "PREMIUM-ONLY on the existing key (measured "
                   "2026-08-23: 'premium endpoint'); premium from "
                   "$49.99/mo at 75 req/min",
        "source": "probe + alphavantage.co/premium",
        "blocker": "PAYMENT_REQUIRED",
        "note": "at 75 req/min a full SPY daily-chain history is "
                "feasible in days on the cheapest tier"},
    "INTRINIO_OPTIONS": {
        "coverage": "US options incl IV surfaces (new endpoints per the "
                    "2026-08-18 marketing mail)",
        "pricing": "'from $150/mo, no sales call' (marketing mail "
                   "2026-07-14 in the operator's inbox)",
        "blocker": "PAYMENT_REQUIRED"},
    "CBOE_FREE_INDICES": {
        "coverage": "VIX 1990->, VIX9D/3M/6M/1D, VVIX, SKEW daily",
        "pricing": "$0 public CSV",
        "source": "ACQUIRED this session",
        "blocker": None,
        "note": "term-structure/skew SUMMARIES only - not a surface"},
}

#: C. analyst expectations / revisions
ANALYST_REVISIONS = {
    "STEELE_BARCOMB": {
        "coverage": "historical consensus vintages (the R38-drafted "
                    "5-ticker sample: AAPL/MON/META/HTZ/CALM)",
        "status": "request drafted since R38, NEVER SENT by the operator; "
                  "nothing received (inbox searched 2026-08-23); R40 inbox "
                  "+ schema/PIT validator ready",
        "blocker": "OPERATOR_ACTION_REQUIRED (MAY_SEND_VENDOR_EMAIL is "
                   "False) then PAYMENT_REQUIRED"},
    "ZACKS_VIA_NDL": {
        "coverage": "measured 2026-08-23: the existing NASDAQ_DATA_LINK "
                    "key NOW returns data (R37 got 403) - but only a "
                    "megacap SAMPLE tier (MSFT/JPM/XOM/JNJ/WMT/MMM/GE/BA "
                    "yes; NVDA/ORCL/AMD/F/T/CALM empty), and estimate "
                    "tables (EE/AR) are CURRENT SNAPSHOTS, not vintages; "
                    "ES has surprise history 2018->",
        "blocker": "PAYMENT_REQUIRED (full tier; 'contact sales' per "
                   "Phase 12-A)"},
    "INTRINIO_ZACKS": {
        "coverage": "tested in a live trial (Stage 17 era): "
                    "NO_DEFENSIBLE_ALPHA, DO_NOT_BUY stands",
        "blocker": "PRIOR_EVIDENCE_NEGATIVE"},
    "FINNHUB_REC_TRENDS": {
        "coverage": "monthly buy/hold/sell counts, free on the existing "
                    "key (measured); current-listing survivorship only",
        "blocker": "SURVIVORSHIP_FAILURE for cross-sectional research"},
}

#: D/E/F. credit, crypto, HF equities
OTHER_LANES = {
    "ICE_BOFA_OAS_VIA_FRED": {
        "coverage": "measured 2026-08-23: daily OAS series now serve only "
                    "~3 years of history on FRED (786 obs; the pre-2023 "
                    "history has been licence-restricted)",
        "blocker": "LICENCE_REQUIRED for deep history (ICE)"},
    "FINRA_TRACE_CORPORATE": {
        "coverage": "trade-level corporate bonds via academic/commercial "
                    "licence; FINRA public site serves aggregates",
        "blocker": "LICENCE_REQUIRED"},
    "BINANCE_VISION": {
        "coverage": "full public archive: 1m klines (signed taker flow), "
                    "funding, OI/positioning metrics, all symbols incl "
                    "delisted", "pricing": "$0, no account",
        "source": "ACQUIRED this session", "blocker": None},
    "TARDIS_DEV": {
        "coverage": "first-of-month FREE days of exchange-native L2/trades "
                    "across venues; full history paid",
        "pricing": "$0 sampled / paid for full",
        "source": "ACQUIRED sample days this session",
        "blocker": "PAYMENT_REQUIRED for the continuous panel"},
    "NYSE_TAQ_WRDS": {
        "coverage": "consolidated trades/quotes 1993->",
        "blocker": "PAYMENT_REQUIRED + LICENCE_REQUIRED (academic "
                   "channel)"},
    "TIINGO_IEX_MINUTE": {
        "coverage": "measured: 1-minute IEX bars 2017-01-> on the EXISTING "
                    "free key, multi-day ranges per call (no volume field)",
        "pricing": "$0 within the free tier's 50 req/hr",
        "source": "ACQUIRED SPY/QQQ this session", "blocker": None},
}


def build() -> dict:
    body = artifact_body("r41_provider_frontier/1", {
        "calculation_owner": CALCULATION_OWNER,
        "measured_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "intraday_futures": INTRADAY_FUTURES,
        "options_surfaces": OPTIONS_SURFACES,
        "analyst_revisions": ANALYST_REVISIONS,
        "other_lanes": OTHER_LANES,
        "method": "every 'measured' row is an HTTP probe this campaign "
                  "made; price rows cite the fetched page; nothing was "
                  "purchased, no account created, no trial started",
    })
    body["frontier_hash"] = sha(body)
    write_json(campaign_dir() / ARTIFACT_NAME, body, immutable=False)
    return body
