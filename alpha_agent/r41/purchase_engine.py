"""alpha_agent.r41.purchase_engine - Track 16: the data-purchase decision.

Ranks candidate purchases by EXPECTED ALPHA-INFORMATION GAIN PER DOLLAR -
information distinctness x research cells unlocked x history depth,
divided by first-year cost - using THIS release's measured evidence (which
free lanes already answered what, and which gross edges died only on cost
or on missing information). Recommends; never purchases. The canonical
Slice-9 acquisition gate remains the authority at purchase time.
"""
from __future__ import annotations

import datetime as _dt

from . import artifact_body, campaign_dir, sha, write_json

CALCULATION_OWNER = "alpha_agent.r41.purchase_engine"
ARTIFACT_NAME = "R41_DATA_PURCHASE_DECISION.json"

CANDIDATES = [
    {"rank": 1, "dataset": "ORATS Near End-of-Day historical archive",
     "provider": "ORATS", "tier": "historical archive (S3) + optionally "
     "one month of the $99 recurring feed",
     "price_usd": "599 one-time (+99 optional single month)",
     "history": "2007 -> present (19+ years)",
     "coverage": "5000+ US underlyings incl SPX/SPY/VIX complexes; "
                 "bid/ask, IV, greeks, smoothed surfaces, volume, OI",
     "pit_proof": "daily as-published snapshots; smoothing is "
                  "vendor-side but raw quotes included - validate "
                  "put-call parity per the R40 Intrinio-style validator",
     "sample_result": "no free sample (ORATS University shows format); "
                      "schema documented publicly",
     "research_cells_unlocked": [
         "variance-risk premium per name and index (daily, 19y)",
         "IV term-structure RV (calendar spreads beyond VX)",
         "skew RV / tail-premium books",
         "index-vs-constituent dispersion",
         "delta-hedged option return factories",
         "options-implied expectations for the equity event lab"],
     "why_current_data_cannot_answer": "the estate owns ZERO option "
     "chains; Cboe free indices are scalar summaries (no strikes, no "
     "greeks, no OI); AlphaVantage options are premium-walled; the whole "
     "R41 vol lab was confined to the VX curve and found nothing "
     "advanceable there",
     "expected_decision_value": "opens the largest still-closed "
     "information family (institutional vol RV) at one-time cost; "
     "permanent archive; per-dollar the deepest unlock available",
     "expected_information_distinctness": "HIGH - option-surface state is "
     "not derivable from anything owned",
     "implementation_cost": "medium (chain parsing, surface QC, "
     "put-call-parity validation; the R40 sample-inbox pattern reuses)",
     "recommendation": "PURCHASE_CANDIDATE - route through the canonical "
                       "RESEARCH_ACQUISITION gate; not authorised here"},
    {"rank": 2, "dataset": "Alpha Vantage premium (1 month) - "
     "HISTORICAL_OPTIONS pilot",
     "provider": "Alpha Vantage", "tier": "$49.99/mo, 75 req/min",
     "price_usd": "50 for one month",
     "history": "15+ years of daily US option chains with IV/greeks",
     "coverage": "per-symbol daily chains (SPY, SPX?, QQQ, single names)",
     "pit_proof": "date-parameterised historical chains; spot-check "
                  "against known events before trusting",
     "sample_result": "probe measured the endpoint exists and is "
                      "premium-gated on the current key",
     "research_cells_unlocked": ["SPY/QQQ variance premium + skew at "
                                 "daily cadence (pilot scale)"],
     "why_current_data_cannot_answer": "same as ORATS",
     "expected_decision_value": "cheapest possible falsification of the "
     "options lane before the ORATS commitment; ~11k requests pulls "
     "3 underlyings x 15y in hours at 75/min",
     "expected_information_distinctness": "HIGH (same family as ORATS, "
     "narrower)",
     "implementation_cost": "low",
     "recommendation": "CHEAPER_PILOT_ALTERNATIVE"},
    {"rank": 3, "dataset": "Databento GLBX.MDP3 minute bars via the $125 "
     "signup credits",
     "provider": "Databento", "tier": "free signup credits (usage-priced "
     "after)",
     "price_usd": "0 cash (ACCOUNT_REQUIRED - operator must create it; "
                  "credits expire in 6 months)",
     "history": "16+ years, exchange-native",
     "coverage": "OHLCV-1m + trades for chosen CME/CBOT/NYMEX markets "
                 "(ES, ZN/ZF/ZB, CL/NG, GC, 6E ... within credit budget)",
     "pit_proof": "exchange MDP3 - definitive",
     "sample_result": "not probed (account wall); pricing page fetched",
     "research_cells_unlocked": [
         "native intraday rates RV (replaces the Bund/T-bond CFD proxies)",
         "intraday curve/roll behaviour with real volume/OI",
         "ES microstructure vs the USA500 CFD proxy",
         "intraday Fibonacci on native futures"],
     "why_current_data_cannot_answer": "the free intraday lanes are one "
     "broker's CFD quotes without volume; several R41 intraday findings "
     "need native confirmation",
     "expected_decision_value": "highest per-dollar (zero cash) but "
     "requires the operator's account decision",
     "expected_information_distinctness": "MEDIUM-HIGH (native volume/OI "
     "vs owned proxies)",
     "implementation_cost": "low (documented API)",
     "recommendation": "OPERATOR_ACCOUNT_DECISION"},
    {"rank": 4, "dataset": "FirstRate futures bundle",
     "provider": "FirstRate Data", "tier": "130-futures intraday bundle",
     "price_usd": "unlisted on the fetched page (historically a few "
                  "hundred one-time)",
     "history": "~15 years 1-minute + tick",
     "coverage": "130 active futures, individual contracts + continuous",
     "pit_proof": "vendor bars; active-market survivorship caveat",
     "sample_result": "1-year 1-minute SPY/SPX/VIX samples acquired and "
                      "parsed this session",
     "research_cells_unlocked": ["bulk intraday futures panels"],
     "why_current_data_cannot_answer": "as rank 3",
     "expected_decision_value": "flat-file bulk alternative to rank 3",
     "expected_information_distinctness": "MEDIUM-HIGH",
     "implementation_cost": "low",
     "recommendation": "ALTERNATIVE_TO_RANK_3"},
    {"rank": 5, "dataset": "ThetaData Options Standard",
     "provider": "ThetaData", "tier": "$80/mo (8y history)",
     "price_usd": "80/mo recurring",
     "history": "8 years", "coverage": "OPRA-wide",
     "recommendation": "DOMINATED - less history than ORATS one-time at "
                       "a recurring price",
     "expected_information_distinctness": "HIGH but dominated"},
    {"rank": 6, "dataset": "Zacks full estimate history (NDL premium)",
     "provider": "Nasdaq Data Link / Zacks", "tier": "contact sales",
     "price_usd": "unknown (historically $$$k/yr)",
     "history": "long", "coverage": "broad US",
     "why_deprioritised": "the estate has PAID for this family's lesson "
     "twice: Stage 13C OOS non-replication (t -0.29) and the Intrinio "
     "live-trial NO_DEFENSIBLE_ALPHA; the measured sample tier shows "
     "current-snapshot estimates (not vintages) even when entitled",
     "recommendation": "DO_NOT_PURSUE_UNTIL_STEELE_SAMPLE_VALIDATES",
     "expected_information_distinctness": "MEDIUM (negative priors)"},
]

TOP = CANDIDATES[0]


def build() -> dict:
    body = artifact_body("r41_data_purchase_decision/1", {
        "calculation_owner": CALCULATION_OWNER,
        "decided_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "ranking_metric": "expected alpha-information gain per dollar "
                          "(distinctness x cells x depth / first-year "
                          "cost), evidence-based",
        "candidates": CANDIDATES,
        "top_candidate": TOP,
        "purchase_authorised": False,
        "authority": "engine.data_expansion_gate (RESEARCH_ACQUISITION "
                     "context) at purchase time; operator decision",
        "no_automatic_purchase": True,
    })
    body["purchase_decision_hash"] = sha(body)
    write_json(campaign_dir() / ARTIFACT_NAME, body, immutable=False)
    return body
