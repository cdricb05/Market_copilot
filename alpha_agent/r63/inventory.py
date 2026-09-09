"""alpha_agent.r63.inventory - OWNED INFORMATION CERTIFICATION (Stage 1).

No canonical R62.2 certification artifact exists in the estate (measured:
``git grep`` finds no owned-information certification owner), so R63 builds
it as its first internal stage and does not ask anyone to run anything first.

Every meaningful owned or entitled FIELD is a row. A row carries provider,
source, dataset, field, normalised field, the ontology dimension it observes,
asset classes, horizons, history, cadence, point-in-time status, available_at
and effective_at semantics, the canonical collector, normaliser and consumer,
the research families that used it, evidence ids, and ONE disposition:

    USED       feeds an operational decision or a research BASELINE
    TESTED     prosecuted with evidence that is not uniformly negative (a
               forward-frozen challenger, or an R63 cell with conditional
               value somewhere), so the field is still live research
    REJECTED   prosecuted and the evidence is uniformly negative, or R63
               measured it REDUNDANT / NO_CONDITIONAL_VALUE everywhere
    BLOCKED    cannot be tested honestly (PIT, coverage, entitlement, cross-
               section, history) - with the blocker named

UNKNOWN is invalid at completion and a test proves the artifact carries none.
A field being ingested does not make it TESTED; a feature being implemented
does not make it evidence. Dispositions are DERIVED from evidence sources at
build time, never typed in.
"""
from __future__ import annotations

import numpy as np

from . import (AC_COMMODITY, AC_CREDIT, AC_CROSS_ASSET, AC_EQUITY_INDEX, AC_FX,
               AC_RATES, AC_US_EQUITY, AC_VOLATILITY, D_BLOCKED, D_REJECTED,
               D_TESTED, D_USED, DISPOSITIONS, PIT_BLOCKED, PIT_LAGGED, PIT_MARKET,
               PIT_TRUE, PIT_UNVERIFIED, R58_INVENTORY, R59_OPPORTUNITY_FRONTIER,
               read_json, stable_hash, write_artifact)
from . import ontology as ONT
from . import panels as P
from . import sensitivity as S

CALCULATION_OWNER = "alpha_agent.r63.inventory"
INVENTORY_SCHEMA = "r63_information_inventory/1"
ARTIFACT_NAME = "information_inventory.json"

_FUT = [AC_EQUITY_INDEX, AC_RATES, AC_COMMODITY, AC_FX, AC_VOLATILITY, AC_CROSS_ASSET]
_ALL = [AC_US_EQUITY] + _FUT + [AC_CREDIT]
_H_ALL = [1, 5, 21, 63]


_DIM_IDS = set(ONT.DIMENSION_IDS)


def _f(*a, blocker=None, note=None):
    """One registry row. The normalised-field argument may be omitted when the
    raw field IS the normalised field; the omission is detected by the
    dimension id landing in its slot, so no row can shift silently."""
    if len(a) == 17 and a[4] in _DIM_IDS:
        a = a[:4] + (a[3],) + a[4:]
    if len(a) != 18:
        raise ValueError("registry row has %d positional fields, expected 18: %r" % (len(a), a[:4]))
    (provider, source, dataset, field, normalized, dimension, asset_classes,
     horizons, history, cadence, pit, available_at, effective_at, collector,
     normalizer, consumer, families, role) = a
    if dimension not in _DIM_IDS:
        raise ValueError("unknown dimension %r in registry row %r" % (dimension, a[:4]))
    return {"provider": provider, "source": source, "dataset": dataset, "field": field,
            "normalized_field": normalized, "dimension_id": dimension,
            "asset_classes": list(asset_classes), "horizons": list(horizons),
            "history": history, "cadence": cadence, "pit_status": pit,
            "available_at_semantics": available_at, "effective_at_semantics": effective_at,
            "canonical_collector": collector, "canonical_normalizer": normalizer,
            "consumer": consumer, "research_families": list(families), "role": role,
            "blocker": blocker, "note": note}


# role: BASELINE (used by an operational or baseline decision), RESEARCH
# (tested or testable), BLOCKED_SOURCE (cannot be tested), PROSPECTIVE (owned
# history too short; forward only).
FIELDS = (
    # ---------------------------------------------------------------- Norgate
    _f("Norgate Data", "norgatedata local (D:\\NorgateData)", "US Equities + Delisted",
       "Close (TOTALRETURN)", "tr_close", "PRICE_RETURN_STATE", [AC_US_EQUITY], _H_ALL,
       "2004-06-01..2026-09-03 (R57 panel)", "daily", PIT_MARKET, "session close",
       "session date", "alpha_agent.r57.panel", "alpha_agent.r57.panel",
       "R57/R58/R59 kernels; operational champion", ["PRICE_STATE"], "BASELINE"),
    _f("Norgate Data", "norgatedata local", "US Equities", "Close (UNADJUSTED), Volume",
       "un_close, volume", "LIQUIDITY", [AC_US_EQUITY], _H_ALL, "2004-06..2026-09",
       "daily", PIT_MARKET, "session close", "session date", "alpha_agent.r57.panel",
       "alpha_agent.r57.panel", "eligibility floors; liquidity factors", ["PRICE_STATE", "PRICE_VOLUME"], "BASELINE"),
    _f("Norgate Data", "norgatedata index_constituent_timeseries", "S&P 500 membership",
       "Index Constituent", "member", "EVENT_INFORMATION", [AC_US_EQUITY], _H_ALL,
       "2004-06..2026-09", "daily", PIT_TRUE, "membership date", "membership date",
       "alpha_agent.r57.panel", "alpha_agent.r57.panel", "PIT universe", ["PRICE_STATE"], "BASELINE",
       note="universe definition, not a signal"),
    _f("Norgate Data", "norgatedata classification_at_level", "GICS sector",
       "GICS Name (CURRENT)", "sectors", "DISPERSION", [AC_US_EQUITY], _H_ALL,
       "current only", "snapshot", PIT_BLOCKED, "none", "none", "alpha_agent.r57.panel",
       "alpha_agent.r57.panel", "sector diagnostics only", [], "BLOCKED_SOURCE",
       blocker="NO_POINT_IN_TIME_GICS_HISTORY"),
    _f("Norgate Data", "norgatedata Futures (dated)", "R41 dated-contract curve store",
       "ret1 (front contract, observable roll)", "ret1", "PRICE_RETURN_STATE", _FUT, _H_ALL,
       "1977-11..2026-08 (94 admitted markets)", "daily", PIT_MARKET, "settlement",
       "session date", "alpha_agent.r41.curve_state", "alpha_agent.r63.panels",
       "R63 futures substrate; R38/R59 native families", ["PRICE_STATE"], "BASELINE"),
    _f("Norgate Data", "norgatedata Futures (dated)", "R41 curve store", "ret2, ret3, c1..c3, slope_ann, slope23_ann",
       "slope_ann", "CARRY", _FUT, _H_ALL, "1977..2026", "daily", PIT_MARKET, "settlement",
       "session date", "alpha_agent.r41.curve_state", "alpha_agent.r63.features",
       "R59 CALENDAR_TERM_STRUCTURE; R63 CARRY", ["FUTURES_TERM_STRUCTURE", "CALENDAR_STRUCTURE", "FUTURES_CURVE"], "RESEARCH"),
    _f("Norgate Data", "norgatedata Futures (dated)", "R41 curve store", "c1..c8, dte1..dte8 (ZQ, SR3)",
       "ZQ_path_6m, SR3_path_6m", "POLICY_EXPECTATIONS", _FUT + [AC_CREDIT], _H_ALL,
       "ZQ 1988..2026; SR3 2018..2026", "daily", PIT_MARKET, "settlement", "session date",
       "alpha_agent.r41.curve_state", "alpha_agent.r63.panels.load_policy_path",
       "R63 POLICY_EXPECTATIONS", [], "RESEARCH"),
    _f("Norgate Data", "norgatedata Futures (dated)", "R41 curve store", "v1, v2, oi1, oi2, oi3",
       "v1, oi1", "VOLUME_PARTICIPATION", _FUT, _H_ALL, "1977..2026", "daily", PIT_MARKET,
       "settlement", "session date", "alpha_agent.r41.curve_state", "alpha_agent.r63.features",
       "R63 baseline", ["PRICE_VOLUME"], "BASELINE"),
    _f("Norgate Data", "norgatedata Continuous Futures", "R57 futures panel", "close_a, close_b (back-adjusted)",
       "close_a", "TREND", _FUT, _H_ALL, "2004-06..2026-09 (103 markets)", "daily", PIT_MARKET,
       "settlement", "session date", "alpha_agent.r57.futures", "alpha_agent.r57.futures",
       "R57/R59 futures families (dollar P&L basis)", ["PRICE_STATE"], "BASELINE",
       note="close_b measured to be the same front contract; carry from it was INVALIDATED (R59)"),
    # ---------------------------------------------------------------- SEC
    _f("SEC EDGAR", "companyfacts XBRL (stage24 index)", "sec_companyfacts_stage24.sqlite",
       "NetCashProvidedByUsedInOperatingActivities, PaymentsToAcquirePropertyPlantAndEquipment, Assets",
       "fcf_to_assets", "FREE_CASH_FLOW", [AC_US_EQUITY], [21, 63], "filed 2009-04..2026-07",
       "quarterly, filed", PIT_TRUE, "SEC filed date", "period end", "alpha_agent.collectors.sec_edgar / stage24",
       "alpha_agent.r58.fundamentals (TTM + YTD_DIFF)", "operational champion leg; R58 A2/B0; R63 baseline",
       ["FUNDAMENTAL_FACT"], "BASELINE"),
    _f("SEC EDGAR", "companyfacts XBRL", "sec_companyfacts_stage24.sqlite", "NetIncomeLoss, OperatingIncomeLoss, Assets",
       "accruals_to_assets, opinc_to_assets", "FUNDAMENTAL_LEVELS", [AC_US_EQUITY], [21, 63],
       "filed 2009-04..2026-07", "quarterly, filed", PIT_TRUE, "SEC filed date", "period end",
       "stage24", "alpha_agent.r58.fundamentals", "R58 A1/A3; R63 baseline", ["FUNDAMENTAL_FACT", "PRICE_AND_FUNDAMENTAL"], "BASELINE"),
    _f("SEC EDGAR", "companyfacts XBRL", "sec_companyfacts_stage24.sqlite",
       "OperatingIncomeLoss (prior), Assets (prior), Revenues, InventoryNet, AccountsReceivableNetCurrent",
       "d_opinc_to_assets, asset_growth, sales_growth, d_wc_to_revenue", "FUNDAMENTAL_CHANGE",
       [AC_US_EQUITY], [21, 63], "filed 2009-04..2026-07", "quarterly, filed", PIT_TRUE,
       "SEC filed date", "period end", "stage24", "alpha_agent.r58.fundamentals",
       "R58 C1/C2/C3; R63 FUNDAMENTAL_CHANGE", ["FUNDAMENTAL_CHANGE"], "RESEARCH"),
    _f("SEC EDGAR", "companyfacts XBRL", "sec_companyfacts_stage24.sqlite", "ResearchAndDevelopmentExpense",
       "rnd_to_assets", "FUNDAMENTAL_LEVELS", [AC_US_EQUITY], [21, 63], "filed 2009..2026 (~40% of names)",
       "quarterly, filed", PIT_TRUE, "SEC filed date", "period end", "stage24", "alpha_agent.r58.fundamentals",
       "R58 C4 (DATA_HOLD_COVERAGE; sector bet)", ["FUNDAMENTAL_FACT"], "RESEARCH",
       note="R58: lockbox excess vanishes ex-largest sector"),
    _f("SEC EDGAR", "companyfacts XBRL", "sec_companyfacts_stage24.sqlite", "filed (periodic filing dates)",
       "obs_age_days, filed_ix", "CORPORATE_DISCLOSURES", [AC_US_EQUITY], [5, 21, 63],
       "2009..2026", "event", PIT_TRUE, "SEC filed date", "filed date", "stage24",
       "alpha_agent.r58.panel_f", "R58 C5 post-filing drift; R63 CORPORATE_DISCLOSURES", ["FILING_EVENT"], "RESEARCH"),
    _f("SEC EDGAR", "Form 3/4/5 structured data sets (R35 acquisition)", "sec_insider_transactions_data_sets 2008Q1..2026Q1",
       "NONDERIV_TRANS.TRANS_CODE (P/S), SUBMISSION.FILING_DATE, ISSUERCIK",
       "net_buy_filings_63, buy_filings_126, sell_filings_126", "INSIDER_BEHAVIOUR", [AC_US_EQUITY], [5, 21, 63],
       "2008-01..2026-03 (766,387 issuer-filing-days, 14,023 CIKs)", "event (quarterly bulk)", PIT_TRUE,
       "FILING_DATE (day precision) + 1 session", "transaction date (never used)",
       "alpha_agent.r35.acquisition", "alpha_agent.r35.information.load_insider_filings (counted, never valued)",
       "R35 sector-ETF level only; R63 STOCK level (new)", ["INSIDER_FLOW"], "RESEARCH"),
    _f("SEC EDGAR", "Form 3/4/5 structured data sets", "sec_insider_transactions_data_sets",
       "TRANS_SHARES, TRANS_PRICEPERSHARE", "(rejected)", "INSIDER_BEHAVIOUR", [AC_US_EQUITY], [5, 21, 63],
       "2008..2026", "event", PIT_TRUE, "FILING_DATE", "transaction date", "alpha_agent.r35.acquisition",
       "none", "none", [], "BLOCKED_SOURCE", blocker="FILER_ENTERED_FIELD_UNVALIDATED (R35 measured 2.1e16 dollar filing)"),
    _f("SEC EDGAR", "submissions API (R63 acquisition)", "_data_sec_submissions/filings_index.csv",
       "form, filingDate, acceptanceDateTime (8-K, NT 10-K/Q, 10-K/A, 10-Q/A)",
       "k8_rate_z, nt_flag, amendment_count", "DISCLOSURE_INTENSITY_LANGUAGE", [AC_US_EQUITY], [5, 21, 63],
       "per-issuer histories 2009..2026 for the PANEL-F CIKs", "event", PIT_TRUE,
       "acceptanceDateTime (filing date + 1 session used)", "filing date",
       "alpha_agent.r63.acquire.sec_submissions", "alpha_agent.r63.features.equity_disclosure_blocks",
       "R58 prospective challenger (0.8y); R63 historical (new)", ["FILING_EVENT"], "RESEARCH"),
    _f("SEC EDGAR", "submissions API (R63 acquisition)", "_data_sec_submissions", "10-K / 10-Q filing calendar",
       "days_to_expected_filing", "EVENT_INFORMATION", [AC_US_EQUITY], [5, 21, 63],
       "2009..2026", "event", PIT_TRUE, "filing date + 1 session", "filing date",
       "alpha_agent.r63.acquire.sec_submissions", "alpha_agent.r63.features.equity_disclosure_blocks",
       "R63 EVENT_INFORMATION (expected filing window)", ["SCHEDULED_EVENT_CALENDAR"], "RESEARCH"),
    _f("SEC EDGAR", "submissions API (daily collector)", "normalized FILING_EVENT / INSIDER_FILING",
       "form_type, acceptance_datetime", "filing events (prospective)", "DISCLOSURE_INTENSITY_LANGUAGE",
       [AC_US_EQUITY], [5, 21], "2025-11-24..", "daily", PIT_TRUE, "acceptanceDateTime", "filing date",
       "alpha_agent.collectors.sec_edgar", "alpha_agent.collectors.sec_edgar", "R58_DISCLOSURE_INTENSITY_V1 (forward)",
       ["FILING_EVENT"], "PROSPECTIVE"),
    _f("SEC EDGAR", "Form 13F data sets", "not acquired", "INFOTABLE.CUSIP, VALUE, SSHPRNAMT",
       "13f_breadth_change", "OWNERSHIP_INSTITUTIONAL_FLOW", [AC_US_EQUITY], [21, 63],
       "2013Q2.. (free)", "quarterly, 45-day lag", PIT_TRUE, "filing date", "quarter end",
       "none", "none", "none", [], "BLOCKED_SOURCE",
       blocker="NO_OWNED_CUSIP_TO_ISSUER_BRIDGE (13F identifies holdings by CUSIP; Norgate and EDGAR identity carry none)"),
    _f("SEC EDGAR", "8-K text / 10-K MD&A", "not parsed", "filing text", "language tone",
       "DISCLOSURE_INTENSITY_LANGUAGE", [AC_US_EQUITY], [5, 21], "n/a", "event", PIT_TRUE,
       "acceptance", "filing date", "none", "none", "none", [], "BLOCKED_SOURCE",
       blocker="TEXT_NOT_OWNED (the R58 rule: GDELT/article text may never become alpha silently; no PIT text corpus is owned)"),
    # ---------------------------------------------------------------- FRED / ALFRED
    _f("FRED", "fred_daily_panel (R41)", "DGS3MO, DGS2, DGS5, DGS10, DGS30", "CMT_*",
       "cmt_2s10s, cmt_5s30s, cmt_butterfly_2_5_10, cmt_2y_minus_3m",
       "TERM_STRUCTURE", [AC_RATES, AC_EQUITY_INDEX, AC_CROSS_ASSET, AC_CREDIT] + [AC_COMMODITY, AC_FX, AC_VOLATILITY], _H_ALL,
       "1990-01..2026-08", "daily", PIT_MARKET, "published same session (unrevised)", "session date",
       "alpha_agent.r41 acquisition", "alpha_agent.r63.features.market_conditioner_blocks",
       "R35 risk premia; R46 rates lanes; R63 TERM_STRUCTURE/CURVE_SHAPE/RATES_EXPECTATIONS",
       ["MACRO_RATES_LEVELS"], "RESEARCH"),
    _f("FRED", "fred_daily_panel (R41)", "BAMLH0A0HYM2, BAMLC0A0CM (+ rating buckets)", "OAS_HY, OAS_IG",
       "CREDIT_CONDITIONS", _FUT + [AC_CREDIT], _H_ALL, "1996-12..2026-08", "daily", PIT_MARKET,
       "published next morning (1-session lag applied)", "session date", "alpha_agent.r41 acquisition",
       "alpha_agent.r63.features", "R46 credit lane (forward); R63 CREDIT_CONDITIONS", ["CREDIT_SPREADS"], "RESEARCH",
       note="ALFRED vintages for these two series begin 2023-08; the daily market series is unrevised"),
    _f("FRED", "fred_daily_panel (R41)", "T5YIE, T10YIE, DFII5, DFII10", "BE_5Y, BE_10Y, REAL_*",
       "INFLATION_EXPECTATIONS", _FUT + [AC_CREDIT], _H_ALL, "2003-01..2026-08", "daily", PIT_MARKET,
       "same session", "session date", "alpha_agent.r41 acquisition", "alpha_agent.r63.features",
       "R35 risk premia (ETF cross-section); R63 INFLATION_EXPECTATIONS", ["MACRO_RATES_LEVELS"], "RESEARCH"),
    _f("FRED", "fred_daily_panel (R41)", "VIXCLS, OVXCLS, GVZCLS", "VIX, OVX, GVZ",
       "VOLATILITY_EXPECTATIONS_IV", _FUT + [AC_CREDIT], _H_ALL, "1990 / 2007 / 2008..2026", "daily", PIT_MARKET,
       "same session", "session date", "alpha_agent.r41 acquisition", "alpha_agent.r63.features.instrument_iv_block",
       "R35 IV term structure; R63 instrument-specific IV + VRP", ["VOLATILITY_TERM_STRUCTURE"], "RESEARCH"),
    _f("FRED", "fred_daily_panel (R41)", "EFFR, SOFR", "sofr_minus_effr", "FUNDING_LIQUIDITY_CONDITIONS",
       _FUT + [AC_CREDIT], _H_ALL, "2016 / 2018..2026", "daily", PIT_MARKET, "same session", "session date",
       "alpha_agent.r41 acquisition", "alpha_agent.r63.features", "R63 FUNDING", [], "RESEARCH"),
    _f("FRED", "fred_daily_panel (R41)", "TRI_HY, TRI_IG (ICE BofA total return index values)", "HY_TRI_EXCESS",
       "PRICE_RETURN_STATE", [AC_CREDIT], _H_ALL, "1996..2026", "daily", PIT_MARKET, "same session",
       "session date", "alpha_agent.r41 acquisition", "alpha_agent.r63.experiments.assemble_credit_proxy",
       "R63 CREDIT_PROXY substrate (index proxy, disclosed)", [], "BASELINE"),
    _f("FRED/ALFRED", "normalized MACRO_OBSERVATION (fred_alfred)", "UNRATE, CPIAUCSL, ICSA, NFCI vintages",
       "unrate_first_release, cpi_yoy_first_release, icsa_chg_4w, nfci_latest_known",
       "MACRO_CHANGE", _FUT + [AC_CREDIT], [5, 21, 63], "vintages 1999-11 (UNRATE/CPI), 2009-05 (ICSA), 2011-05 (NFCI)",
       "monthly / weekly, vintage", PIT_TRUE, "realtime_start (first session strictly after)", "observation period",
       "alpha_agent.collectors.fred_alfred", "alpha_agent.r63.panels.load_alfred", "R39 MACRO_OVERLAY; R63 MACRO_*",
       ["MACRO_OBSERVATION", "PRICE_AND_MACRO"], "RESEARCH"),
    _f("FRED/ALFRED", "normalized MACRO_OBSERVATION", "UNRATE, ICSA, CPIAUCSL first releases vs naive forecast",
       "surprise_vs_naive_forecast", "MACRO_SURPRISES", _FUT + [AC_CREDIT], [5, 21, 63], "1999..2026", "release",
       PIT_TRUE, "realtime_start", "observation period", "alpha_agent.collectors.fred_alfred", "alpha_agent.r63.features",
       "R45 intraday event study (DOES_NOT_REPLICATE); R63 daily-horizon proxy", ["MACRO_RELEASE_SURPRISE"], "RESEARCH",
       note="a NAIVE-forecast surprise; no consensus history is owned"),
    _f("FRED/ALFRED", "normalized MACRO_OBSERVATION", "DFF, DGS2, DGS10, T10Y2Y, VIXCLS, SOFR vintages",
       "(superseded by the unrevised daily market panel)", "TERM_STRUCTURE", _FUT, _H_ALL,
       "vintages 2005..2026", "daily", PIT_TRUE, "realtime_start", "session", "alpha_agent.collectors.fred_alfred",
       "alpha_agent.r63.panels", "R39 overlays; R63 uses the market panel", ["MACRO_OBSERVATION"], "RESEARCH",
       note="market series are unrevised; the vintage carries no extra information"),
    _f("BEA", "normalized MACRO_OBSERVATION (bea)", "NIPA T10101", "GDP components", "MACRO_LEVELS",
       _FUT, [21, 63], "1947..2026", "quarterly", PIT_BLOCKED, "null (RELEASE_LAG_UNKNOWN)", "reference period",
       "alpha_agent.collectors.bea", "none", "none", [], "BLOCKED_SOURCE", blocker="TIMESTAMP_INSUFFICIENT"),
    _f("BLS", "normalized MACRO_OBSERVATION (bls)", "CUUR0000SA0, LNS14000000, CES0000000001", "CPI, U-rate, payrolls",
       "MACRO_LEVELS", _FUT, [21, 63], "2023..2026", "monthly", PIT_BLOCKED, "null", "reference period",
       "alpha_agent.collectors.bls", "none", "none", [], "BLOCKED_SOURCE",
       blocker="TIMESTAMP_INSUFFICIENT and REDUNDANT_WITH_ALFRED"),
    _f("US Treasury", "fiscaldata avg_interest_rates", "avg_interest_rate_amt", "(unused)", "MACRO_LEVELS",
       [AC_RATES], [21, 63], "2025-11..2026-08", "monthly", PIT_BLOCKED, "null", "record date",
       "alpha_agent.collectors.us_treasury", "none", "none", [], "BLOCKED_SOURCE",
       blocker="TIMESTAMP_INSUFFICIENT and 9 months of history"),
    _f("FRED (R35)", "fred_st_louis_fed JSON", "IR3TIB01{EZ,JP,CA,MX,GB,CH,US}M156N", "fx_rate_differential",
       "CARRY", [AC_FX, AC_CROSS_ASSET], [21, 63], "1985..2026", "monthly, 2-month publication lag", PIT_LAGGED,
       "month M observable from month M+2", "month", "alpha_agent.r35.acquisition", "alpha_agent.r35.information.load_fred",
       "R35 FX_INTEREST_CARRY (no increment on ETFs)", ["POSITIONING"], "RESEARCH",
       note="R63 uses the dated-contract slope, which IS the FX carry, daily"),
    # ---------------------------------------------------------------- Cboe
    _f("Cboe", "VIX index history CSVs (R41/R35)", "VIX9D, VIX, VIX3M, VIX6M, VVIX, SKEW", "vix_term_slope, vvix_z, skew_z",
       "VOLATILITY_EXPECTATIONS_IV", _FUT + [AC_CREDIT], _H_ALL, "1990 / 2009 / 2011..2026", "daily", PIT_MARKET,
       "same session", "session date", "alpha_agent.r41 acquisition", "alpha_agent.r63.panels.load_cboe",
       "R35 IV term structure; R41 vol lab; R46 vx term carry; R63", ["VOLATILITY_TERM_STRUCTURE"], "RESEARCH"),
    # ---------------------------------------------------------------- CFTC
    _f("CFTC", "Commitments of Traders legacy futures-only annual archives (R35)", "deacot1986..2026",
       "Noncommercial/Commercial Long/Short, Open Interest", "spec_net_z_156w, spec_net_chg_13w, comm_net_z",
       "POSITIONING_COMMITMENTS", _FUT, [5, 21, 63], "1986-01..2026-08 (39 mapped markets)", "weekly, Friday release",
       PIT_LAGGED, "report Tuesday + 6 calendar days + 1 session", "report Tuesday", "alpha_agent.r35.acquisition",
       "alpha_agent.r35.information.load_cot -> alpha_agent.r63.panels.load_cot",
       "R35 FUTURES_POSITIONING (ETFs); R39 POSITIONING; R46 cot lanes (forward); R63 per market",
       ["CFTC_POSITIONING", "POSITIONING"], "RESEARCH"),
    # ---------------------------------------------------------------- EIA
    _f("EIA", "petroleum bulk archive PET.zip (R35)", "WCESTUS1, WGTSTUS1, WDISTUS1 weekly ending stocks",
       "stocks_z_5y_seasonal, d_stocks_4w", "INVENTORY", [AC_COMMODITY, AC_CROSS_ASSET], [5, 21, 63],
       "1982-08..2026-08", "weekly, Wednesday release", PIT_LAGGED, "period + 7 calendar days + 1 session",
       "week ending", "alpha_agent.r35.acquisition", "alpha_agent.r63.panels.load_eia_weekly",
       "NEVER TESTED before R63", [], "RESEARCH"),
    _f("EIA", "natural gas bulk archive NG.zip (R63 acquisition)", "NW2_EPG0_SWO_R48_BCF weekly working gas in storage",
       "ng_storage_z_5y_seasonal, d_storage_4w", "INVENTORY", [AC_COMMODITY], [5, 21, 63], "1994..2026 if acquired",
       "weekly, Thursday release", PIT_LAGGED, "period + 7 calendar days + 1 session", "week ending",
       "alpha_agent.r63.acquire.eia_natural_gas", "alpha_agent.r63.panels.load_eia_weekly", "R63 INVENTORY (NG)", [], "RESEARCH"),
    _f("EIA", "petroleum bulk archive", "RCLC1..RCLC4 NYMEX settlements (to 2024-04)", "eia_wti_curve",
       "FUTURES_BASIS", [AC_COMMODITY], [21, 63], "1983..2024-04 (discontinued)", "daily", PIT_MARKET, "same session",
       "session", "alpha_agent.r35.acquisition", "alpha_agent.r35.information.load_eia_curve",
       "R35 COMMODITY_TERM_STRUCTURE (superseded by the Norgate dated curve)", ["FUTURES_CURVE"], "RESEARCH"),
    # ---------------------------------------------------------------- FINRA / Nasdaq
    _f("FINRA", "Reg SHO daily short-sale volume", "normalized SHORT_VOLUME", "ShortVolume, TotalVolume",
       "short_volume_ratio_21", "SHORT_POSITIONING", [AC_US_EQUITY], [5, 21], "2026-07-23.. (history files HTTP 403)",
       "daily", PIT_TRUE, "file Last-Modified", "business date", "alpha_agent.collectors.finra",
       "alpha_agent.collectors.finra", "R58_SHORT_VOLUME_PRESSURE_V1 (forward)", ["SHORT_VOLUME"], "PROSPECTIVE",
       blocker="HISTORY_NOT_SERVED_FREE (measured 2026-09-09)"),
    _f("Nasdaq Trader", "trading halts RSS", "normalized TRADING_HALT", "halt reason, time", "EVENT_INFORMATION",
       [AC_US_EQUITY], [1, 5], "2019-02..2026-09 (411 tickers, micro-cap heavy)", "event", PIT_TRUE, "RSS pub time",
       "halt time", "alpha_agent.collectors.nasdaq_trader", "alpha_agent.collectors.nasdaq_trader", "R58 refused (UNIVERSE_MISMATCH)",
       [], "BLOCKED_SOURCE", blocker="UNIVERSE_MISMATCH"),
    # ---------------------------------------------------------------- EODHD
    _f("EODHD", "eodhd_analyst daily vintage", "earningsEstimateAvg, epsTrend*, epsRevisionsUp/Down, Rating, TargetPrice",
       "revision_breadth_30d", "ANALYST_REVISIONS", [AC_US_EQUITY], [5, 21, 63], "2026-07-31.. (19 vintage days, 6 symbols)",
       "daily snapshot, first-write-wins", PIT_TRUE, "capture date", "capture date", "alpha_agent.collectors.eodhd_analyst",
       "alpha_agent.collectors.eodhd_analyst", "forward-only vintage build", [], "PROSPECTIVE",
       blocker="NO_HISTORICAL_VINTAGES (a current snapshot may never be differenced into history)"),
    _f("EODHD", "fundamentals / calendar endpoints", "earnings calendar (BeforeMarket/AfterMarket), historical EPS estimate vs actual",
       "earnings surprise (vendor)", "EARNINGS_EXPECTATIONS", [AC_US_EQUITY], [1, 5, 21], "vendor history",
       "event", PIT_UNVERIFIED, "vendor report date (estimate PIT unverifiable)", "report date",
       "alpha_agent.collectors.eodhd", "none", "R46 PEAD lane (forward, SEC 8-K based)", ["EARNINGS_EVENTS"], "BLOCKED_SOURCE",
       blocker="PIT_UNVERIFIED (the vendor estimate at report time cannot be proven as-was); the SEC XBRL seasonal-difference surprise is TESTED (R58 C1 grid)"),
    _f("EODHD", "dividends / splits", "normalized CORPORATE_ACTION", "ex-date, declaration date", "corporate action events",
       "EVENT_INFORMATION", [AC_US_EQUITY], [5, 21], "5 tickers collected; entitlement covers 2010..", "event", PIT_TRUE,
       "declaration date", "ex-date", "alpha_agent.collectors.eodhd", "alpha_agent.collectors.eodhd",
       "R58 refused (NOT_A_CROSS_SECTION); R59 FREE_AVAILABLE", [], "BLOCKED_SOURCE",
       blocker="NOT_A_CROSS_SECTION_YET (configuration limit: sample_symbols = 7); a 1,900-symbol pull is a collector change, not R63 scope"),
    _f("EODHD", "news", "normalized NEWS_EVENT", "headline, publication ts", "news events", "EVENT_INFORMATION",
       [AC_US_EQUITY], [1, 5], "2026-07-27.. (7 tickers)", "event", PIT_TRUE, "publication ts", "publication ts",
       "alpha_agent.collectors.eodhd", "alpha_agent.collectors.eodhd", "none", [], "PROSPECTIVE",
       blocker="HISTORY_NOT_SERVED (R59 probe) and NOT_A_CROSS_SECTION"),
    # ---------------------------------------------------------------- intraday / options / crypto (owned, out of scope)
    _f("Dukascopy / Binance / Tiingo / R45 minute panels", "R41/R42/R45 intraday stores", "1-minute bars",
       "intraday", "PRICE_RETURN_STATE", [AC_EQUITY_INDEX, AC_FX, AC_COMMODITY], [], "2003..2026 (selected symbols)",
       "1-minute", PIT_TRUE, "bar time", "bar time", "alpha_agent.r41/r42/r45 acquisition", "n/a",
       "R41/R45 labs (NO_QUALIFIED_ALPHA); R53.1 cannot emit forward rows", ["PRICE_STATE"], "RESEARCH",
       note="intraday horizon is out of R63 scope; recorded as owned"),
    _f("Polygon", "SPY option surface samples (R44/R45/R46)", "polygon_spy_option_surface*.csv.gz", "IV by strike/expiry",
       "iv_skew, iv_term", "VOLATILITY_EXPECTATIONS_IV", [AC_EQUITY_INDEX, AC_VOLATILITY], [1, 5, 21],
       "free window only (2026-08..09)", "daily", PIT_TRUE, "capture", "quote date", "alpha_agent.r46.options",
       "alpha_agent.r46.options", "R46 options lane (forward)", ["VOLATILITY_TERM_STRUCTURE"], "PROSPECTIVE",
       blocker="HISTORICAL_SURFACE_PAYMENT_REQUIRED"),
    # ---------------------------------------------------------------- not owned, named
    _f("(none)", "Baltic Exchange / freight", "not owned", "BDI", "baltic_dry_chg", "SHIPPING_TRANSPORT",
       [AC_COMMODITY, AC_CROSS_ASSET], [21, 63], "n/a", "daily", PIT_MARKET, "n/a", "n/a", "none", "none", "none", [],
       "BLOCKED_SOURCE", blocker="NOT_OWNED_NO_FREE_PIT_HISTORY"),
    _f("(none)", "ICI / ETF issuer flows", "not owned", "weekly net flows", "etf_flow_4w", "ETF_FUND_FLOW", _ALL, [21, 63],
       "n/a", "weekly", PIT_LAGGED, "n/a", "n/a", "none", "none", "none", [], "BLOCKED_SOURCE",
       blocker="NOT_OWNED (free aggregate exists at asset-class level; sourcing ladder recorded)"),
    _f("(none)", "USDA / commodity physical balances", "not owned", "production, consumption, exports", "physical balance",
       "COMMODITY_SUPPLY_DEMAND", [AC_COMMODITY], [21, 63], "n/a", "monthly", PIT_LAGGED, "n/a", "n/a", "none", "none",
       "none", [], "BLOCKED_SOURCE", blocker="NOT_OWNED (USDA NASS requires a key: STATE_SAMPLE_REQUIRED per R37)"),
    _f("(none)", "analyst consensus revenue / EPS history", "not owned", "consensus panel", "revenue_consensus",
       "REVENUE_EXPECTATIONS", [AC_US_EQUITY], [21, 63], "n/a", "daily", PIT_BLOCKED, "n/a", "n/a", "none", "none",
       "none", [], "BLOCKED_SOURCE", blocker="PAID_AND_PIT_UNVERIFIED (Intrinio DO_NOT_BUY; FMP/Finnhub/NDL 403; EODHD/AV snapshot only)"),
)


# --------------------------------------------------------------------------- #
# Evidence sources
# --------------------------------------------------------------------------- #
def memory_evidence() -> dict:
    """information_family -> counts by outcome and up to 5 hypothesis ids,
    read from the live ResearchMemory through its READ-ONLY handle. Absence is
    reported, never faked as zero."""
    try:
        from ..r59 import memory as M
        if not M.memory_present():
            return {"state": "RESEARCH_MEMORY_NOT_PRESENT", "families": {}}
        mem = M.open_memory_readonly()
        rows = mem.list_hypotheses(limit=50000)
    except Exception as exc:                              # noqa: BLE001
        return {"state": "UNREADABLE:%s" % type(exc).__name__, "families": {}}
    fam: dict = {}
    for r in rows:
        f = r.get("information_family") or "UNLABELLED"
        slot = fam.setdefault(f, {"n": 0, "by_outcome": {}, "asset_classes": set(),
                                  "horizons": set(), "evidence_ids": []})
        slot["n"] += 1
        o = r.get("outcome") or "UNSETTLED"
        slot["by_outcome"][o] = slot["by_outcome"].get(o, 0) + 1
        if r.get("asset_class"):
            slot["asset_classes"].add(r["asset_class"])
        if r.get("horizon_sessions") is not None:
            slot["horizons"].add(int(r["horizon_sessions"]))
        if len(slot["evidence_ids"]) < 5:
            slot["evidence_ids"].append(r.get("hypothesis_id"))
    for f in fam.values():
        f["asset_classes"] = sorted(f["asset_classes"])
        f["horizons"] = sorted(f["horizons"])
    return {"state": "OK", "n_hypotheses": len(rows), "families": fam}


def r63_evidence(cells: list | None) -> dict:
    """dimension -> summary of R63 cells (any conditional value? all negative?)."""
    out: dict = {}
    for c in cells or []:
        d = c.get("dimension")
        s = out.setdefault(d, {"cells": 0, "scored": 0, "t_max": None, "verdicts": {},
                               "cell_ids_positive": [], "best_cell": None})
        s["cells"] += 1
        v = c.get("verdict")
        s["verdicts"][v] = s["verdicts"].get(v, 0) + 1
        t = (c.get("conditional") or {}).get("t")
        if t is not None:
            s["scored"] += 1
            if s["t_max"] is None or t > s["t_max"]:
                s["t_max"], s["best_cell"] = t, c.get("cell_id")
            if t >= S.CONDITIONAL_T_FLOOR and len(s["cell_ids_positive"]) < 8:
                s["cell_ids_positive"].append(c.get("cell_id"))
    return out


def _disposition(field: dict, mem: dict, r63: dict, substrate: dict) -> tuple:
    role = field["role"]
    if role == "BLOCKED_SOURCE":
        return D_BLOCKED, "blocked source: %s" % field.get("blocker")
    if role == "PROSPECTIVE":
        return D_BLOCKED, "PROSPECTIVE_ONLY: owned history too short to partition; %s" % (field.get("blocker") or "forward only")
    if role == "BASELINE":
        return D_USED, "feeds an operational decision or a research baseline"
    # RESEARCH: derive from evidence
    dim = field["dimension_id"]
    ev63 = r63.get(dim)
    fams = field["research_families"]
    mem_rows = [mem["families"].get(f) for f in fams if mem.get("families", {}).get(f)]
    n_mem = sum(m["n"] for m in mem_rows)
    mem_nonneg = any(m["by_outcome"].get("FORWARD_FROZEN", 0) + m["by_outcome"].get("QUALIFIED", 0)
                     + m["by_outcome"].get("NEEDS_MORE_EVIDENCE", 0) > 0 for m in mem_rows)
    if ev63 and ev63["scored"] > 0:
        pos = len(ev63["cell_ids_positive"])
        cand = ev63["verdicts"].get(S.V_CANDIDATE, 0)
        if cand or pos:
            return D_TESTED, ("R63 measured conditional value in %d cell(s) (best %s, t=%.2f)"
                              % (pos, ev63["best_cell"], ev63["t_max"]))
        return D_REJECTED, ("R63 measured no conditional value in %d scored cell(s) (t_max=%.2f); "
                            "prior memory rows=%d" % (ev63["scored"], ev63["t_max"], n_mem))
    if n_mem:
        if mem_nonneg:
            return D_TESTED, "prosecuted %d memory hypotheses; forward evidence accruing" % n_mem
        return D_REJECTED, "prosecuted %d memory hypotheses, uniformly NO_ALPHA_EVIDENCE/REJECTED" % n_mem
    if ev63 and ev63["cells"] and ev63["scored"] == 0:
        return D_BLOCKED, "R63 cells were DATA_HOLD: %s" % ",".join(sorted(ev63["verdicts"]))
    return D_BLOCKED, "no evidence produced and no honest test path recorded for this field"


def certify(cells: list | None = None, *, substrate: dict | None = None) -> dict:
    mem = memory_evidence()
    r63 = r63_evidence(cells)
    sub = substrate or {}
    r58 = read_json(R58_INVENTORY) or {}
    r59 = read_json(R59_OPPORTUNITY_FRONTIER) or {}
    rows = []
    for f in FIELDS:
        disp, why = _disposition(f, mem, r63, sub)
        assert disp in DISPOSITIONS
        fams = f["research_families"]
        mem_ev = {k: {"n": v["n"], "by_outcome": v["by_outcome"], "evidence_ids": v["evidence_ids"]}
                  for k, v in mem.get("families", {}).items() if k in fams}
        ev63 = r63.get(f["dimension_id"])
        row = dict(f)
        row.update({"field_id": stable_hash([f["provider"], f["source"], f["dataset"], f["field"]])[:12],
                    "disposition": disp, "disposition_reason": why,
                    "memory_evidence": mem_ev,
                    "r63_evidence": ({"cells": ev63["cells"], "scored": ev63["scored"], "t_max": ev63["t_max"],
                                      "best_cell": ev63["best_cell"], "verdicts": ev63["verdicts"],
                                      "evidence_ids": ev63["cell_ids_positive"]} if ev63 else None),
                    "information_class": ONT.dimension(f["dimension_id"])["information_class"]})
        rows.append(row)
    counts = {d: sum(1 for r in rows if r["disposition"] == d) for d in DISPOSITIONS}
    providers = sorted({r["provider"] for r in rows})
    by_dim: dict = {}
    for r in rows:
        by_dim.setdefault(r["dimension_id"], []).append(r["disposition"])
    unknown = [r["field_id"] for r in rows if r["disposition"] not in DISPOSITIONS]
    body = {
        "schema": INVENTORY_SCHEMA, "calculation_owner": CALCULATION_OWNER,
        "certification_rule": {
            "USED": "feeds an operational decision or a research baseline",
            "TESTED": "prosecuted with evidence that is not uniformly negative",
            "REJECTED": "prosecuted; evidence uniformly negative or R63 measured no conditional value",
            "BLOCKED": "cannot be tested honestly; blocker named",
            "UNKNOWN_IS_INVALID": True},
        "prior_certification_consumed": {"r58_information_inventory": bool(r58),
                                         "r59_data_opportunity_frontier": bool(r59),
                                         "canonical_r62_2_certification": "NOT_FOUND - built here"},
        "research_memory": {"state": mem.get("state"), "n_hypotheses": mem.get("n_hypotheses"),
                            "families_seen": sorted(mem.get("families", {}).keys())},
        "n_fields": len(rows), "n_providers": len(providers), "providers": providers,
        "counts": counts, "unknown_fields": unknown,
        "dimension_coverage": {d: sorted(set(v)) for d, v in sorted(by_dim.items())},
        "dimensions_without_any_field": sorted(d for d in ONT.DIMENSION_IDS if d not in by_dim),
        "fields": sorted(rows, key=lambda r: (r["provider"], r["dataset"], r["field"])),
        "ontology_hash": ONT.ontology_hash(),
    }
    write_artifact(ARTIFACT_NAME, body)
    return body
