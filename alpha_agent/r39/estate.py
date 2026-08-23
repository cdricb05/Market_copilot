"""alpha_agent.r39.estate - the UNIVERSAL_DATA_ESTATE owner.

One inventory of every information family the estate owns, each classified
for PIT safety, survivorship state and tradable implementation, with a named
admission role or a named exclusion reason from the frozen vocabulary. A
family may be excluded only for a reason in ``contract.EXCLUSION_REASONS``;
"we did not get to it" is spelled ``DATA_AVAILABLE_BUT_NOT_TESTED`` and is
NEVER counted as a rejected hypothesis.

PIT rules enforced here as classification, and in ``universal_state`` as
code:

* market-quoted series (rates, spreads, index levels, settlements) are
  PIT-safe with a one-session publication lag;
* revised statistical series are PIT-safe ONLY through true ALFRED vintages
  (``realtime_start``); a current-vintage snapshot of a revised series is a
  PIT_FAILURE for history and is excluded from features;
* CFTC positioning carries the Release-35 publication lag;
* nothing is forward-filled before it was observable.
"""
from __future__ import annotations

from pathlib import Path

from .. import r39
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.estate"
SCHEMA = "r39_universal_data_estate/1"
ARTIFACT_NAME = "universal_data_estate.json"

DATA_ROOT = Path(r"D:\Stock_Prediction_app_data")
R38_V4 = DATA_ROOT / "native_futures_r38" / \
    "r38_native_futures_information_frontier_v4"
NATIVE_LAYER_DIR = R38_V4 / "native_contract_layer"
R38_PANEL = R38_V4 / "ml_ready_native_futures_panel.csv"
R30_PRICE_DATASET = DATA_ROOT / "release30_zero_base_adaptive_allocator" / \
    "_cache" / "dataset_price_only.npz"
R30_FUND_DATASET = DATA_ROOT / "release30_zero_base_adaptive_allocator" / \
    "_cache" / "dataset_fundamental_matched.npz"
FRED_R36_DIR = DATA_ROOT / "global_multi_asset_frontier_r36" / "acquired" / \
    "fred_st_louis_fed"
FRED_R35_DIR = DATA_ROOT / "orthogonal_information_r35" / "acquired" / \
    "fred_st_louis_fed"
ALFRED_PIT_DIR = DATA_ROOT / "predictive_edge_r33" / \
    "r33_predictive_edge_v2" / "pit_cache"
VIX_DIR = DATA_ROOT / "orthogonal_information_r35" / "acquired" / \
    "cboe_volatility_indices"
COT_DIR = DATA_ROOT / "orthogonal_information_r35" / "acquired" / \
    "cftc_commitments_of_traders"
INSIDER_DIR = DATA_ROOT / "orthogonal_information_r35" / "acquired" / \
    "sec_insider_transactions_data_sets"
NYFED_CSV = DATA_ROOT / "native_market_data_gate_r37" / "acquired" / \
    "nyfed" / "primary_dealer_timeseries.csv"

#: FRED series admitted as PIT-safe MARKET-QUOTED observations (not revised),
#: each with a declared conservative availability lag in sessions.
FRED_MARKET_QUOTED = {
    "BAA10Y": 1, "DFII10": 1, "T10YIE": 1, "DTB3": 1,
    "DGS2": 1, "DGS10": 1, "DGS30": 1,
    "CBBTCUSD": 1, "CBETHUSD": 1,
}
#: Revised statistical snapshot series (foreign CPI levels, OECD interbank
#: monthly averages fetched at ONE 2026 vintage) - PIT_FAILURE as features.
FRED_REVISED_SNAPSHOT_EXCLUDED = True


def _fp(path, *, hash_limit_mb: float = 64.0) -> dict:
    p = Path(path)
    try:
        big = p.stat().st_size > hash_limit_mb * 1e6
    except OSError:
        big = False
    return r39.file_fingerprint(p, content_hash=not big)


def families() -> list:
    """Every information family, classified. Declarative facts only."""
    return [
        {
            "family": "NATIVE_FUTURES_DAILY_LAYER",
            "asset_classes": ["COMMODITY", "RATES", "FX", "US_EQUITY",
                              "INTERNATIONAL_EQUITY", "VOLATILITY"],
            "role": "STATE_BACKBONE",
            "admitted": True,
            "source": str(NATIVE_LAYER_DIR),
            "pit_basis": "daily official settlements, observable "
                         "first-notice/last-trade roll (R38 frozen policy), "
                         "one-session decision lag",
            "survivorship": "per-market contract series complete incl. "
                            "expired; MARKET list is current-composition "
                            "(declared, inherited from R38)",
            "coverage": "68 markets, 1978-2026",
        },
        {
            "family": "NATIVE_FUTURES_ML_PANEL",
            "asset_classes": ["COMMODITY", "RATES", "FX", "US_EQUITY",
                              "INTERNATIONAL_EQUITY", "VOLATILITY"],
            "role": "STATE_BACKBONE",
            "admitted": True,
            "source": str(R38_PANEL),
            "pit_basis": "decision-stamped rows, forward-only targets, "
                         "missingness masks, per-row costs and controls "
                         "(R38 ml_contract)",
            "survivorship": "as daily layer",
            "coverage": "31,175 rows, 68 markets, monthly decisions",
        },
        {
            "family": "CFTC_COT_POSITIONING",
            "asset_classes": ["COMMODITY", "RATES", "FX"],
            "role": "FEATURE_OVERLAY",
            "admitted": True,
            "source": str(COT_DIR),
            "pit_basis": "Release-35 deacot archive with the measured "
                         "publication lag; consumed via the R38 panel's "
                         "cot_commercial_z with its has_cot mask",
            "survivorship": "full archive 1986-2026",
            "coverage": "weekly, mapped markets only",
        },
        {
            "family": "FRED_MARKET_QUOTED",
            "asset_classes": ["RATES", "CREDIT", "VOLATILITY", "CRYPTO"],
            "role": "FEATURE_OVERLAY",
            "admitted": True,
            "source": str(FRED_R35_DIR) + " ; " + str(FRED_R36_DIR),
            "series_lags_sessions": dict(FRED_MARKET_QUOTED),
            "pit_basis": "market-quoted series (Treasury yields, credit "
                         "spread, breakeven, bills, Coinbase spot) are not "
                         "statistically revised; one-session declared lag",
            "survivorship": "not applicable (index series)",
            "coverage": "daily, 1986-2026 (varies)",
        },
        {
            "family": "FRED_REVISED_SNAPSHOT",
            "asset_classes": ["MACRO"],
            "role": "EXCLUDED",
            "admitted": False,
            "excluded_reason": "PIT_FAILURE",
            "source": str(FRED_R36_DIR),
            "detail": "foreign CPI / OECD monthly-average series were "
                      "fetched at one 2026 vintage; revised levels at a "
                      "single realtime are not point-in-time history. The "
                      "PIT-safe route is ALFRED vintages, which this estate "
                      "holds for a US subset only.",
        },
        {
            "family": "ALFRED_VINTAGES_US",
            "asset_classes": ["MACRO"],
            "role": "FEATURE_OVERLAY",
            "admitted": True,
            "source": str(ALFRED_PIT_DIR),
            "pit_basis": "true ALFRED realtime_start vintage stamps "
                         "(Release-33/Stage-15B acquisition)",
            "survivorship": "not applicable",
            "coverage": "US CPI/GDP-class series, vintage-stamped",
        },
        {
            "family": "CBOE_VOLATILITY_INDICES",
            "asset_classes": ["VOLATILITY"],
            "role": "FEATURE_OVERLAY",
            "admitted": True,
            "source": str(VIX_DIR),
            "pit_basis": "exchange-published index closes, same-day "
                         "publication, one-session decision lag",
            "survivorship": "not applicable",
            "coverage": "VIX 1990-2026, VIX3M 2007-2026",
        },
        {
            "family": "US_EQUITY_XS_PRICE_PANEL",
            "asset_classes": ["US_EQUITY"],
            "role": "STATE_BACKBONE",
            "admitted": True,
            "source": str(R30_PRICE_DATASET),
            "pit_basis": "price-derived features at monthly decision dates "
                         "with forward labels built strictly ahead "
                         "(Release-30 dataset contract; source panel "
                         "russell1000_cp_daily, current+past constituents)",
            "survivorship": "survivorship-safe: delisted/inactive names "
                            "included via Norgate current+past universe",
            "coverage": "304 monthly dates 2001-2026, ~753 names/date, "
                        "14 price features, forward 5/20/21/60-session "
                        "returns",
        },
        {
            "family": "US_EQUITY_FUNDAMENTAL_MATCHED",
            "asset_classes": ["US_EQUITY"],
            "role": "SECONDARY_FEATURE_BLOCK",
            "admitted": True,
            "source": str(R30_FUND_DATASET),
            "pit_basis": "as price panel plus SEC-companyfacts PIT "
                         "fundamentals (filed-date aligned)",
            "survivorship": "coverage-skewed subset (Release-30 measured "
                            "2.74x size skew) - DECLARED, judged only "
                            "against its own control",
            "coverage": "~247 names/date, 21 features",
        },
        {
            "family": "ETF_TOTAL_RETURN_SLEEVE",
            "asset_classes": ["CREDIT", "RATES", "US_EQUITY", "COMMODITY",
                              "REAL_ESTATE", "INTERNATIONAL_EQUITY"],
            "role": "STATE_BACKBONE",
            "admitted": True,
            "source": "norgatedata (existing entitlement, read-only, "
                       "package pinned 1.0.74)",
            "pit_basis": "daily total-return closes, one-session decision "
                         "lag",
            "survivorship": "FIXED declared list of currently-listed ETFs - "
                            "a current-composition caveat carried by every "
                            "artifact that uses this sleeve; the sleeve "
                            "exists to reach the CREDIT and REAL_ESTATE "
                            "cells futures cannot express",
            "coverage": "credit (HYG/LQD), rates (TLT/IEF/SHY), equity "
                        "(SPY/EEM/EFA), commodity (GLD), REIT (VNQ), "
                        "cash (BIL)",
        },
        {
            "family": "SEC_INSIDER_TRANSACTIONS",
            "asset_classes": ["US_EQUITY"],
            "role": "AVAILABLE_NOT_USED",
            "admitted": True,
            "used_in_state": False,
            "research_status": "DATA_AVAILABLE_BUT_NOT_TESTED",
            "source": str(INSIDER_DIR),
            "detail": "73 quarterly Form-345 archives (2008-2026). An "
                      "equity-event lane needs identity joins and event-time "
                      "alignment this release did not budget; recorded as "
                      "available, never as rejected.",
        },
        {
            "family": "SEC_COMPANYFACTS_FUNDAMENTALS",
            "asset_classes": ["US_EQUITY"],
            "role": "AVAILABLE_NOT_USED",
            "admitted": True,
            "used_in_state": False,
            "research_status": "DATA_AVAILABLE_BUT_NOT_TESTED",
            "source": "stage24/stage25 PIT companyfacts indexes",
            "detail": "already consumed by the R30 fundamental-matched "
                      "block; broader direct integration was terminally "
                      "mined by Stages 23-26 with hand features "
                      "(OWNED_EXHAUSTED) and is not re-run here.",
        },
        {
            "family": "NYFED_PRIMARY_DEALER_POSITIONING",
            "asset_classes": ["RATES"],
            "role": "AVAILABLE_NOT_USED",
            "admitted": True,
            "used_in_state": False,
            "research_status": "DATA_AVAILABLE_BUT_NOT_TESTED",
            "source": str(NYFED_CSV),
            "detail": "weekly dealer positioning acquired by R37; join and "
                      "lag work not budgeted this release.",
        },
        {
            "family": "EIA_PHYSICAL_SUPPLY_DEMAND",
            "asset_classes": ["COMMODITY"],
            "role": "AVAILABLE_NOT_USED",
            "admitted": True,
            "used_in_state": False,
            "research_status": "DATA_AVAILABLE_BUT_NOT_TESTED",
            "source": str(DATA_ROOT / "orthogonal_information_r35" /
                          "acquired" / "eia_petroleum_bulk"),
            "detail": "petroleum/natural-gas physical balances (R35/R36 "
                      "bulk); the fundamental-supply-demand information leg "
                      "R38 named as missing remains unintegrated.",
        },
        {
            "family": "LBMA_PRECIOUS_FIXES",
            "asset_classes": ["COMMODITY"],
            "role": "NOT_USED_REDUNDANT",
            "admitted": True,
            "used_in_state": False,
            "research_status": "VALIDLY_REPRESENTED_BY_GROUP_TEST",
            "source": str(DATA_ROOT / "native_market_data_gate_r37" /
                          "acquired" / "lbma"),
            "detail": "GC/SI/PL/PA native settlements carry the same "
                      "economic information at higher fidelity.",
        },
        {
            "family": "CRYPTO_OUTRIGHT_SLEEVE",
            "asset_classes": ["CRYPTO"],
            "role": "EXCLUDED",
            "admitted": False,
            "excluded_reason": "INSUFFICIENT_HISTORY",
            "source": "CME crypto futures (delivered) / FRED Coinbase spot",
            "detail": "the implementable native instruments (CME BTC/ETH "
                      "futures, 2017-) fail the estate's frozen 10-year "
                      "floor, measured by R38. Coinbase SPOT indices are "
                      "admitted as cross-asset INFORMATION only "
                      "(FRED_MARKET_QUOTED family).",
        },
        {
            "family": "NEWS_RSS_EVENT_FABRIC",
            "asset_classes": ["EVENTS"],
            "role": "EXCLUDED",
            "admitted": False,
            "excluded_reason": "INSUFFICIENT_HISTORY",
            "source": "alpha_agent news_rss / event_fabric stores",
            "detail": "collection began 2026; no research-scale history.",
        },
        {
            "family": "ANALYST_REVISIONS",
            "asset_classes": ["US_EQUITY"],
            "role": "EXCLUDED",
            "admitted": False,
            "excluded_reason": "SURVIVORSHIP_FAILURE",
            "source": "provider trials (Intrinio frozen artifacts)",
            "detail": "every accessible historical analyst archive failed "
                      "the survivorship-safe bar (Stage 13C OOS "
                      "non-replication, Stage 17 FMP survivorship-FAIL, "
                      "Intrinio DO_NOT_BUY). The Steele/Intrinio sample "
                      "lane continues in parallel as "
                      "SCHEMA_AND_PIT_VALIDATION_ONLY.",
        },
        {
            "family": "INTERNATIONAL_SINGLE_NAME_EQUITY",
            "asset_classes": ["INTERNATIONAL_EQUITY"],
            "role": "EXCLUDED",
            "admitted": False,
            "excluded_reason": "UNAVAILABLE",
            "detail": "no owned survivorship-safe international single-name "
                      "history; index exposure is carried natively by the "
                      "futures backbone.",
        },
    ]


def build() -> dict:
    fams = families()
    fingerprints = {
        "native_layer_manifest": _fp(NATIVE_LAYER_DIR / "layer_manifest.json"),
        "r38_ml_panel": _fp(R38_PANEL),
        "r30_price_dataset": _fp(R30_PRICE_DATASET),
        "r30_fundamental_dataset": _fp(R30_FUND_DATASET),
        "vix_history": _fp(VIX_DIR / "VIX_History.csv"),
        "vix3m_history": _fp(VIX_DIR / "VIX3M_History.csv"),
    }
    admitted = [f["family"] for f in fams if f["admitted"]]
    used = [f["family"] for f in fams
            if f["admitted"] and f.get("used_in_state", True)]
    excluded = {f["family"]: f["excluded_reason"] for f in fams
                if not f["admitted"]}
    bad = [r for r in excluded.values() if r not in C.EXCLUSION_REASONS]
    if bad:
        raise ValueError("unnamed exclusion reason(s): %r" % bad)
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": C.CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "families": fams,
        "n_families": len(fams),
        "admitted_families": admitted,
        "families_used_in_state": used,
        "excluded_families": excluded,
        "exclusion_vocabulary": list(C.EXCLUSION_REASONS),
        "input_fingerprints": fingerprints,
        "forward_fill_before_observable": False,
        "asset_coverage_assessed": [
            "US individual equities", "inactive/delisted equities",
            "US sector/equity baskets", "international equity indices",
            "commodity futures", "Treasury futures",
            "international government futures", "FX futures",
            "volatility/VX", "credit", "crypto", "rates", "inflation",
            "real estate proxies", "cross-asset structures"],
    })
    body["estate_hash"] = r39.sha(body)
    return body


def path_for(campaign_id: str = C.CAMPAIGN_ID) -> Path:
    return r39.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r39.write_json(path_for(body["campaign_id"]), body)
