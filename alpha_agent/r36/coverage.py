"""alpha_agent.r36.coverage - the ONE Release 36 coverage-matrix owner.

The matrix answers, for every asset class x sub-asset x strategy family cell:
what native market exists, what would implement it, what point-in-time history
this estate owns, what is free, what needs money, what has been tested, what was
only ever tested through a proxy, and what the blocker actually is.

Two design choices make it evidence rather than opinion.

**The cell states are DERIVED, not typed.** A cell's state comes from the
executed experiment registry, the measured entitlement matrix and a declared
table of markets - so a configuration that failed to execute cannot leave a cell
looking tested, and adding a strategy to the contract without running it shows
up as untested rather than as nothing.

**A proxy result never closes a native cell.** When a configuration whose
implementation level is LEVEL 2 covers a cell, the cell becomes
``TESTED_PROXY_ONLY`` and its native counterpart keeps whatever blocker it has.
This is the distinction the previous five releases did not record, and it is the
reason "an ETF failed" was at risk of being read as "the asset class has no
alpha".

The one state a completed release must justify is ``NOT_TESTED_DATA_AVAILABLE``:
it means a cell was executable and was not executed, which is a budget decision
and has to be visible as one.
"""
from __future__ import annotations

from typing import Optional

from .. import r36
from . import contract as _contract
from . import experiments as _experiments

CALCULATION_OWNER = "alpha_agent.r36.coverage"
SCHEMA = "r36_global_multi_asset_coverage_matrix/1"
ARTIFACT_NAME = "global_multi_asset_coverage_matrix.json"

C = _contract
F = _contract  # strategy-family constants live on the contract

#: Family sets reused across sub-assets, so a market's applicable strategies are
#: declared once.
DIRECTIONAL_FAMILIES = (C.SF_TREND, C.SF_MOMENTUM, C.SF_VALUE,
                        C.SF_MEAN_REVERSION, C.SF_MACRO_CONDITIONAL)
CURVE_FAMILIES = (C.SF_CARRY, C.SF_ROLL, C.SF_CURVE, C.SF_RELATIVE_VALUE)
CROSS_FAMILIES = (C.SF_CROSS_SECTIONAL, C.SF_RELATIVE_VALUE)
EQUITY_FAMILIES = (C.SF_TREND, C.SF_MOMENTUM, C.SF_VALUE, C.SF_MEAN_REVERSION,
                   C.SF_CROSS_SECTIONAL, C.SF_EVENT_DRIVEN,
                   C.SF_MACRO_CONDITIONAL, C.SF_RELATIVE_VALUE)

#: The declared market table. Each row is a real market, the instrument that
#: would implement it, what this estate holds, and what stands in the way.
#:
#: key -> dict with:
#:   asset_class, sub_asset, native_instrument, proxies, source, history,
#:   frequency, pit, survivorship, level, blocker, blocker_reason,
#:   prior_evidence, families, lane, priority
MARKETS = {
    # ------------------------------------------------------------------ A --
    "US_EQUITY_LARGE_CAP": dict(
        asset_class="US_EQUITY", sub_asset="LARGE_CAP",
        native_instrument="listed common stock",
        proxies=("SPY", "VTI", "QQQ"),
        source="Norgate US Equities + Delisted (14,639 live / 27,194 dead)",
        history="1928 -> 2026", frequency="DAILY",
        pit="TOTAL_RETURN_ADJUSTED", survivorship="DELISTED_RETAINED",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="R31 searched the price/fundamental frontier and found "
                       "no robust alpha; R23-R27 exhausted the owned factor "
                       "frontier",
        families=EQUITY_FAMILIES, lane=None, priority="CLOSED"),
    "US_EQUITY_MID_SMALL": dict(
        asset_class="US_EQUITY", sub_asset="MID_AND_SMALL_CAP",
        native_instrument="listed common stock",
        proxies=("MDY", "IWM"),
        source="Norgate Russell 2000 / S&P 600 current-and-past watchlists",
        history="1990 -> 2026", frequency="DAILY",
        pit="TOTAL_RETURN_ADJUSTED", survivorship="DELISTED_RETAINED",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="R31 and Phase 8-V/8-W measured that widening the "
                       "universe DILUTED rather than added alpha",
        families=EQUITY_FAMILIES, lane=None, priority="CLOSED"),
    "US_EQUITY_SECTORS": dict(
        asset_class="US_EQUITY", sub_asset="SECTORS_AND_INDUSTRIES",
        native_instrument="listed common stock, sector baskets",
        proxies=("XLE", "XLF", "XLK", "and the rest of the SPDR set"),
        source="Norgate US Equities + owned PIT SIC from the SEC statement sets",
        history="1998 -> 2026", frequency="DAILY",
        pit="PIT_SIC_FROM_OWNED_FSDS", survivorship="DELISTED_RETAINED",
        level=C.LEVEL_PROXY, blocker=None, blocker_reason=None,
        prior_evidence="R32 tested a sector-rotation sleeve; R34 held nine "
                       "sector funds in the 47-instrument cross-section",
        families=EQUITY_FAMILIES, lane=None, priority="CLOSED"),
    "US_EQUITY_SINGLE_NAME_EVENTS": dict(
        asset_class="US_EQUITY", sub_asset="EVENT_DRIVEN_SINGLE_NAME",
        native_instrument="listed common stock around a corporate event",
        proxies=(),
        source="SEC full-index filings, insider Form 3/4/5 data sets",
        history="2006 -> 2026", frequency="EVENT",
        pit="FILING_DATE_OBSERVABLE", survivorship="CIK_KEYED",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="R27 tested eight event families across 49 hypotheses "
                       "with zero survivors; R35 measured insider filing "
                       "intensity at rank IC 0.041, t 2.72 standalone and "
                       "-0.0002 conditional",
        families=(C.SF_EVENT_DRIVEN, C.SF_CROSS_SECTIONAL), lane=None,
        priority="CLOSED"),
    "US_EQUITY_ANALYST_EXPECTATIONS": dict(
        asset_class="US_EQUITY", sub_asset="ANALYST_EXPECTATION_CHANGE",
        native_instrument="listed common stock ranked on revision",
        proxies=(),
        source="no admissible historical consensus panel",
        history="NONE", frequency="NONE",
        pit="CURRENT_SNAPSHOT_ONLY", survivorship="CURRENT_MEMBERS_ONLY",
        level=C.LEVEL_NATIVE, blocker=C.STATE_BLOCKED_COST,
        blocker_reason="six free entitlements were probed read-only in R35: "
                       "FMP, Finnhub and Nasdaq Data Link answer HTTP 403; "
                       "EODHD and Alpha Vantage answer with today's estimate "
                       "plus 30/60/90-day deltas, which is CURRENT_SNAPSHOT "
                       "and inadmissible as history. The owned Intrinio/Zacks "
                       "extract is ONE retrieval day over a current-members "
                       "universe. Stage 13A returns TRIAL_DATA_INSUFFICIENT "
                       "and the R32 purchase gate returns EVALUATED_DO_NOT_BUY",
        prior_evidence="Stage 13A/13B/13C, R33 lane C, R35 lane A - all "
                       "blocked on a paid entitlement, never on method",
        families=(C.SF_EVENT_DRIVEN, C.SF_CROSS_SECTIONAL, C.SF_MOMENTUM),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    # ------------------------------------------------------------------ B --
    "INTL_EQUITY_DEVELOPED": dict(
        asset_class="INTERNATIONAL_EQUITY", sub_asset="DEVELOPED_EX_US",
        native_instrument="local listed stock or an index future",
        proxies=("EFA", "and 27 world index series"),
        source="Norgate World Indices (31) + US-listed country funds",
        history="1990 -> 2026", frequency="DAILY",
        pit="PRICE_INDEX_LOCAL_CURRENCY", survivorship="INDEX_LEVEL",
        level=C.LEVEL_SIGNAL,
        blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="the owned Continuous Futures entitlement serves ONE "
                       "market, so no index future outside &ES is available; "
                       "local single stocks are not in any owned database",
        prior_evidence="R33 predicted 27 world equity indices and labelled the "
                       "whole universe SIGNAL_RESEARCH_VALID",
        families=DIRECTIONAL_FAMILIES + CROSS_FAMILIES, lane=None,
        priority="NEEDS_PAID_ENTITLEMENT"),
    "INTL_EQUITY_EMERGING": dict(
        asset_class="INTERNATIONAL_EQUITY", sub_asset="EMERGING_MARKETS",
        native_instrument="local listed stock or an index future",
        proxies=("EEM",),
        source="Norgate World Indices + US-listed country funds",
        history="1993 -> 2026", frequency="DAILY",
        pit="PRICE_INDEX_LOCAL_CURRENCY", survivorship="INDEX_LEVEL",
        level=C.LEVEL_SIGNAL,
        blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="same entitlement wall as developed ex-US, plus a "
                       "currency-label defect the vendor's own metadata "
                       "carries for Indian indices",
        prior_evidence="R33 measured the currency-denomination diagnostic and "
                       "flagged CURRENCY_LABEL_UNCERTAIN markets",
        families=DIRECTIONAL_FAMILIES + CROSS_FAMILIES, lane=None,
        priority="NEEDS_PAID_ENTITLEMENT"),
    # ------------------------------------------------------------------ C --
    "FX_G10": dict(
        asset_class="FX", sub_asset="G10_DELIVERABLE",
        native_instrument="one-month deliverable forward",
        proxies=("FXE", "FXY", "UUP"),
        source="Norgate Forex Spot + FRED/OECD three-month interbank rates",
        history="1991 -> 2026", frequency="DAILY_SPOT_MONTHLY_RATE",
        pit="SPOT_SAME_SESSION_RATE_LAGGED_TWO_MONTHS",
        survivorship="NO_CURRENCY_DELISTS",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="R33 held FX SPOT with no carry leg and declared "
                       "FX_SPOT_EXCLUDES_CARRY; R34 held three currency funds",
        families=(C.SF_CARRY, C.SF_TREND, C.SF_MOMENTUM, C.SF_VALUE,
                  C.SF_MEAN_REVERSION, C.SF_CROSS_SECTIONAL,
                  C.SF_POSITIONING, C.SF_MACRO_CONDITIONAL),
        lane=C.LANE_FX, priority="EXECUTED"),
    "FX_EMERGING": dict(
        asset_class="FX", sub_asset="EMERGING_DELIVERABLE",
        native_instrument="one-month deliverable forward",
        proxies=("CEW",),
        source="Norgate Forex Spot + FRED/OECD three-month interbank rates",
        history="1991 -> 2026", frequency="DAILY_SPOT_MONTHLY_RATE",
        pit="SPOT_SAME_SESSION_RATE_LAGGED_TWO_MONTHS",
        survivorship="RATE_SERIES_END_IS_A_REAL_EXIT",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="R33's single attractive result was dominated by TRYUSD "
                       "and failed leave-one-market-out",
        families=(C.SF_CARRY, C.SF_TREND, C.SF_MOMENTUM, C.SF_VALUE,
                  C.SF_CROSS_SECTIONAL, C.SF_MACRO_CONDITIONAL),
        lane=C.LANE_FX, priority="EXECUTED"),
    "FX_NDF": dict(
        asset_class="FX", sub_asset="NON_DELIVERABLE_FORWARDS",
        native_instrument="non-deliverable forward",
        proxies=(),
        source="none owned or free",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="NONE", level=C.LEVEL_NATIVE,
        blocker=C.STATE_BLOCKED_COST,
        blocker_reason="NDF fixing and forward-point history is a paid "
                       "interbank product; no free source publishes it, and "
                       "the currencies that need it are exactly the ones "
                       "whose spot this estate must treat as administered",
        prior_evidence="never tested",
        families=(C.SF_CARRY, C.SF_CROSS_SECTIONAL), lane=None,
        priority="NEEDS_PAID_ENTITLEMENT"),
    "FX_OPTIONS": dict(
        asset_class="FX", sub_asset="CURRENCY_OPTIONS",
        native_instrument="OTC currency option",
        proxies=(), source="none owned or free",
        history="NONE", frequency="NONE", pit="NONE", survivorship="NONE",
        level=C.LEVEL_NATIVE, blocker=C.STATE_BLOCKED_COST,
        blocker_reason="implied volatility, risk reversals and butterflies for "
                       "currencies are a paid interbank product; fabricating a "
                       "surface from current quotes is a prohibited "
                       "substitution",
        prior_evidence="never tested",
        families=(C.SF_VRP, C.SF_CURVE), lane=None,
        priority="NEEDS_PAID_ENTITLEMENT"),
    # ------------------------------------------------------------------ D --
    "CMDTY_ENERGY_CRUDE": dict(
        asset_class="COMMODITY", sub_asset="ENERGY_CRUDE",
        native_instrument="dated NYMEX WTI contract",
        proxies=("USO", "DBC", "$BCOMEN"),
        source="EIA petroleum bulk, contracts 1-4",
        history="1983 -> 2024-04 (publisher discontinued)", frequency="DAILY",
        pit="SETTLEMENT_PUBLISHED_NEXT_MORNING",
        survivorship="CONTRACT_SERIES_COMPLETE",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="R33 held BCOM sub-indices; R34 held USO; neither is a "
                       "curve",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_POSITIONING,
                                   C.SF_SEASONALITY, C.SF_CROSS_SECTIONAL),
        lane=C.LANE_COMMODITY, priority="EXECUTED"),
    "CMDTY_ENERGY_GAS": dict(
        asset_class="COMMODITY", sub_asset="ENERGY_NATURAL_GAS",
        native_instrument="dated NYMEX Henry Hub contract",
        proxies=("UNG", "DBE"),
        source="EIA natural gas bulk, contracts 1-4",
        history="1994 -> 2024-04", frequency="DAILY",
        pit="SETTLEMENT_PUBLISHED_NEXT_MORNING",
        survivorship="CONTRACT_SERIES_COMPLETE",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="never tested at contract level",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_POSITIONING,
                                   C.SF_SEASONALITY, C.SF_CROSS_SECTIONAL),
        lane=C.LANE_COMMODITY, priority="EXECUTED"),
    "CMDTY_ENERGY_PRODUCTS": dict(
        asset_class="COMMODITY", sub_asset="ENERGY_REFINED_PRODUCTS",
        native_instrument="dated NYMEX heating oil, RBOB and propane contracts",
        proxies=("UGA", "DBE"),
        source="EIA petroleum bulk, contracts 1-4",
        history="1980 -> 2024-04 (propane terminated 2009)", frequency="DAILY",
        pit="SETTLEMENT_PUBLISHED_NEXT_MORNING",
        survivorship="A_TERMINATED_CONTRACT_IS_INCLUDED",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="never tested at contract level",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_POSITIONING,
                                   C.SF_SEASONALITY, C.SF_CROSS_SECTIONAL),
        lane=C.LANE_COMMODITY, priority="EXECUTED"),
    "CMDTY_PRECIOUS": dict(
        asset_class="COMMODITY", sub_asset="PRECIOUS_METALS",
        native_instrument="COMEX gold and silver futures",
        proxies=("GLD", "SLV", "XAUUSD", "XAGUSD", "$BCOMPR"),
        source="Norgate Cash Commodities and Forex Spot (metal spot only)",
        history="1982 -> 2026", frequency="DAILY",
        pit="SPOT_SAME_SESSION", survivorship="NOT_APPLICABLE",
        level=C.LEVEL_PROXY,
        blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="no dated contract series is owned or published free "
                       "for metals; EIA covers energy only and the Norgate "
                       "Continuous Futures entitlement serves one market, so "
                       "no roll yield, curve carry or calendar spread can be "
                       "measured",
        prior_evidence="R33 held metal spot; R34 held GLD and SLV; R36 uses "
                       "gold spot in one cross-asset relationship",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_POSITIONING,
                                   C.SF_CROSS_SECTIONAL),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "CMDTY_INDUSTRIAL": dict(
        asset_class="COMMODITY", sub_asset="INDUSTRIAL_METALS",
        native_instrument="LME and COMEX base metal futures",
        proxies=("DBB", "$BCOMIN"),
        source="Norgate Cash Commodities sub-index only",
        history="1991 -> 2026", frequency="DAILY",
        pit="INDEX_LEVEL", survivorship="INDEX_LEVEL",
        level=C.LEVEL_SIGNAL, blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="no dated contract series owned or free; a sub-index is "
                       "a weighted roll of contracts nobody here can see",
        prior_evidence="R33 held the BCOM industrial sub-index",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_POSITIONING,
                                   C.SF_CROSS_SECTIONAL),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "CMDTY_GRAINS": dict(
        asset_class="COMMODITY", sub_asset="GRAINS_AND_OILSEEDS",
        native_instrument="CBOT corn, wheat and soybean futures",
        proxies=("CORN", "WEAT", "SOYB", "DBA", "$BCOMGR"),
        source="Norgate Cash Commodities sub-index only",
        history="1991 -> 2026", frequency="DAILY",
        pit="INDEX_LEVEL", survivorship="INDEX_LEVEL",
        level=C.LEVEL_SIGNAL, blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="no dated contract series owned or free; USDA publishes "
                       "supply and demand but not settlements",
        prior_evidence="R33 held the BCOM grains sub-index",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_POSITIONING,
                                   C.SF_SEASONALITY, C.SF_SUPPLY_DEMAND,
                                   C.SF_CROSS_SECTIONAL),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "CMDTY_SOFTS": dict(
        asset_class="COMMODITY", sub_asset="SOFTS",
        native_instrument="ICE coffee, sugar, cotton and cocoa futures",
        proxies=("CANE", "$BCOMSO"),
        source="Norgate Cash Commodities sub-index only",
        history="1991 -> 2026", frequency="DAILY",
        pit="INDEX_LEVEL", survivorship="INDEX_LEVEL",
        level=C.LEVEL_SIGNAL, blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="no dated contract series owned or free",
        prior_evidence="R33 held the BCOM softs sub-index",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_SEASONALITY,
                                   C.SF_CROSS_SECTIONAL),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "CMDTY_LIVESTOCK": dict(
        asset_class="COMMODITY", sub_asset="LIVESTOCK",
        native_instrument="CME live cattle and lean hog futures",
        proxies=("$BCOMLI",),
        source="Norgate Cash Commodities sub-index only",
        history="1991 -> 2026", frequency="DAILY",
        pit="INDEX_LEVEL", survivorship="INDEX_LEVEL",
        level=C.LEVEL_SIGNAL, blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="no dated contract series owned or free",
        prior_evidence="R33 held the BCOM livestock sub-index",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_SEASONALITY),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    # ------------------------------------------------------------------ E --
    "RATES_US_CURVE": dict(
        asset_class="RATES", sub_asset="US_TREASURY_CURVE",
        native_instrument="Treasury note and bond futures",
        proxies=("SHY", "IEF", "TLT", "$IDCOT1TR through $IDCOT20TR"),
        source="Norgate Economic constant-maturity yields + ICE BofA Treasury "
               "total-return indices",
        history="signal 1962 -> 2026, tradable legs 2005 -> 2026",
        frequency="DAILY",
        pit="YIELD_AND_INDEX_SAME_SESSION",
        survivorship="INDEX_LEVEL_NO_DEATH",
        level=C.LEVEL_PROXY,
        blocker=None, blocker_reason=None,
        prior_evidence="R33 predicted the duration-bucket indices; R34 held "
                       "SHY, IEF and TLT; neither ran a curve trade",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_VALUE,
                                   C.SF_MEAN_REVERSION,
                                   C.SF_MACRO_CONDITIONAL),
        lane=C.LANE_RATES, priority="EXECUTED"),
    "RATES_TREASURY_FUTURES": dict(
        asset_class="RATES", sub_asset="TREASURY_FUTURES",
        native_instrument="CBOT 2y, 5y, 10y, 30y futures",
        proxies=("SHY", "IEF", "TLT"),
        source="none owned or free",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="NONE", level=C.LEVEL_NATIVE,
        blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="the owned Continuous Futures entitlement serves one "
                       "market and it is &ES; without contract data there is "
                       "no cheapest-to-deliver, no basis and no roll",
        prior_evidence="never available",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_POSITIONING),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "RATES_INTERNATIONAL": dict(
        asset_class="RATES", sub_asset="INTERNATIONAL_GOVERNMENT",
        native_instrument="Bund, JGB and Gilt futures",
        proxies=("BWX", "IGOV"),
        source="FRED/OECD long-term government bond yields, monthly",
        history="1960 -> 2026", frequency="MONTHLY",
        pit="PUBLISHED_IN_ARREARS", survivorship="INDEX_LEVEL",
        level=C.LEVEL_SIGNAL, blocker=C.STATE_BLOCKED_ENTITLEMENT,
        blocker_reason="a monthly average yield is not a tradable return and "
                       "no international bond future is entitled; the "
                       "US-listed proxies carry an unhedged currency leg that "
                       "would dominate the rates signal",
        prior_evidence="R34 held BWX inside the 47-fund cross-section",
        families=CURVE_FAMILIES + (C.SF_TREND, C.SF_RELATIVE_VALUE),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "RATES_INFLATION_LINKED": dict(
        asset_class="RATES", sub_asset="INFLATION_LINKED",
        native_instrument="Treasury Inflation-Protected Securities",
        proxies=("TIP", "SCHP", "STIP", "LTPZ"),
        source="Norgate US Equities (TIP) + FRED DFII and breakeven series",
        history="2003 -> 2026", frequency="DAILY",
        pit="SAME_SESSION", survivorship="NOT_APPLICABLE",
        level=C.LEVEL_PROXY, blocker=None, blocker_reason=None,
        prior_evidence="R34 held TIP; no breakeven relative value was run",
        families=(C.SF_RELATIVE_VALUE, C.SF_VALUE, C.SF_MEAN_REVERSION,
                  C.SF_CARRY),
        lane=C.LANE_RATES, priority="EXECUTED"),
    # ------------------------------------------------------------------ F --
    "CREDIT_INVESTMENT_GRADE": dict(
        asset_class="CREDIT", sub_asset="INVESTMENT_GRADE",
        native_instrument="individual corporate bonds",
        proxies=("LQD", "$USBIGCORP"),
        source="Norgate FTSE US Broad IG corporate total-return index + "
               "Moody's BAA yield",
        history="index 1994 -> 2026, hedged legs 2005 -> 2026",
        frequency="DAILY",
        pit="SAME_SESSION", survivorship="INDEX_LEVEL_NO_DEATH",
        level=C.LEVEL_PROXY, blocker=None, blocker_reason=None,
        prior_evidence="R32 and R34 held credit funds; no spread strategy was "
                       "run",
        families=(C.SF_CARRY, C.SF_MOMENTUM, C.SF_MEAN_REVERSION,
                  C.SF_RELATIVE_VALUE, C.SF_MACRO_CONDITIONAL),
        lane=C.LANE_CREDIT, priority="EXECUTED"),
    "CREDIT_HIGH_YIELD": dict(
        asset_class="CREDIT", sub_asset="HIGH_YIELD",
        native_instrument="individual high-yield bonds",
        proxies=("HYG", "JNK"),
        source="Norgate %CCCHYS spread series; FRED ICE OAS is licence-capped "
               "to a rolling three-year window",
        history="spread 1996 -> 2026, fund 2007 -> 2026", frequency="DAILY",
        pit="SAME_SESSION", survivorship="INDEX_LEVEL_NO_DEATH",
        level=C.LEVEL_PROXY, blocker=None, blocker_reason=None,
        prior_evidence="R34 held HYG; no spread strategy was run",
        families=(C.SF_CARRY, C.SF_MOMENTUM, C.SF_MEAN_REVERSION,
                  C.SF_RELATIVE_VALUE),
        lane=C.LANE_CREDIT, priority="EXECUTED"),
    "CREDIT_SINGLE_NAME": dict(
        asset_class="CREDIT", sub_asset="SINGLE_NAME_BONDS_AND_CDS",
        native_instrument="corporate bond, CDS, CDX index",
        proxies=("LQD", "HYG"),
        source="none owned or free",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="NONE", level=C.LEVEL_NATIVE,
        blocker=C.STATE_BLOCKED_COST,
        blocker_reason="TRACE bond-level transaction history and CDS quotes "
                       "are paid products; ratings-migration and recovery "
                       "panels likewise. Capital-structure and credit-equity "
                       "relative value cannot be attempted without them",
        prior_evidence="never tested",
        families=(C.SF_CARRY, C.SF_RELATIVE_VALUE, C.SF_EVENT_DRIVEN,
                  C.SF_CROSS_SECTIONAL, C.SF_LIQUIDITY),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "CREDIT_LOANS_PREFERRED_EM": dict(
        asset_class="CREDIT", sub_asset="LOANS_PREFERREDS_EM_CREDIT",
        native_instrument="bank loans, preferred shares, sovereign bonds",
        proxies=("BKLN", "PFF", "EMB", "PCY"),
        source="Norgate US Equities (funds only)",
        history="2007 -> 2026", frequency="DAILY",
        pit="SAME_SESSION", survivorship="DELISTED_RETAINED",
        level=C.LEVEL_PROXY, blocker=C.STATE_BLOCKED_PIT,
        blocker_reason="the only owned observation is a fund price; loan "
                       "prices, preferred dividends and sovereign spread "
                       "curves are not owned, so a spread strategy would be "
                       "fitted to a fund's tracking error",
        prior_evidence="R34 held BKLN, PFF and PCY in the 47-fund cross-section",
        families=(C.SF_CARRY, C.SF_RELATIVE_VALUE, C.SF_LIQUIDITY),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    # ------------------------------------------------------------------ G --
    "VOL_VIX_FUTURES": dict(
        asset_class="VOLATILITY", sub_asset="VIX_FUTURES_TERM_STRUCTURE",
        native_instrument="Cboe VX futures",
        proxies=("VIXY", "VXX", "VIXM"),
        source="Cboe settlement history is not free",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="NONE", level=C.LEVEL_NATIVE,
        blocker=C.STATE_BLOCKED_LICENSING,
        blocker_reason="five published routes to the VX settlement history "
                       "were probed and every one answered 403 or 404; without "
                       "dated settlements there is no roll, no term-structure "
                       "carry and no native volatility sleeve",
        prior_evidence="R33 and R35 used VIX as a FEATURE and never as an "
                       "instrument",
        families=(C.SF_CARRY, C.SF_ROLL, C.SF_CURVE, C.SF_VRP),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    "VOL_LONG_ETP": dict(
        asset_class="VOLATILITY", sub_asset="LONG_VOLATILITY_ETP",
        native_instrument="Cboe VX futures held through a fund",
        proxies=("VIXY", "VIXM"),
        source="Norgate US Equities + free Cboe index term structure",
        history="2011 -> 2026", frequency="DAILY",
        pit="SAME_SESSION", survivorship="PRODUCT_NEVER_TERMINATED",
        level=C.LEVEL_PROXY, blocker=None, blocker_reason=None,
        prior_evidence="never tested as a sleeve",
        families=(C.SF_CURVE, C.SF_VRP, C.SF_MACRO_CONDITIONAL),
        lane=C.LANE_VOL, priority="EXECUTED"),
    "VOL_SHORT_ETP": dict(
        asset_class="VOLATILITY", sub_asset="SHORT_VOLATILITY_ETP",
        native_instrument="short Cboe VX futures held through a fund",
        proxies=("SVXY", "XIV", "ZIV"),
        source="Norgate US Equities, and the terminated products are absent",
        history="INCOMPLETE", frequency="DAILY",
        pit="SAME_SESSION", survivorship="TERMINATED_PRODUCTS_ABSENT",
        level=C.LEVEL_PROXY, blocker=C.STATE_BLOCKED_SURVIVORSHIP,
        blocker_reason=C.SHORT_VOLATILITY_BLOCK_REASON,
        prior_evidence="never tested, and cannot be tested from what survives",
        families=(C.SF_CARRY, C.SF_ROLL, C.SF_VRP),
        lane=None, priority="NEEDS_SURVIVORSHIP_SAFE_HISTORY"),
    "VOL_OPTIONS_SURFACE": dict(
        asset_class="VOLATILITY", sub_asset="EQUITY_INDEX_OPTIONS",
        native_instrument="listed index option, variance swap",
        proxies=("BXM", "PUT"),
        source="no historical surface owned or free",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="NONE", level=C.LEVEL_NATIVE,
        blocker=C.STATE_BLOCKED_COST,
        blocker_reason="a historical implied-volatility surface is a paid "
                       "product; building one from current quotes is a "
                       "prohibited substitution, so skew, dispersion and "
                       "variance-swap research cannot begin",
        prior_evidence="never tested",
        families=(C.SF_VRP, C.SF_CURVE, C.SF_RELATIVE_VALUE),
        lane=None, priority="NEEDS_PAID_ENTITLEMENT"),
    # ------------------------------------------------------------------ H --
    "CRYPTO_MAJOR_SPOT": dict(
        asset_class="CRYPTO", sub_asset="MAJOR_SPOT",
        native_instrument="spot bitcoin and ether",
        proxies=("BITO", "GBTC", "IBIT"),
        source="FRED republication of Coinbase spot",
        history="BTC 2014-12 -> 2026, ETH 2016-05 -> 2026", frequency="DAILY",
        pit="SAME_SESSION",
        survivorship="TWO_LARGEST_THROUGHOUT_SO_NOT_A_SELECTION",
        level=C.LEVEL_PROXY, blocker=None, blocker_reason=None,
        prior_evidence="never tested",
        families=(C.SF_TREND, C.SF_MOMENTUM, C.SF_CROSS_SECTIONAL),
        lane=C.LANE_CRYPTO, priority="EXECUTED"),
    "CRYPTO_BASIS_FUNDING": dict(
        asset_class="CRYPTO", sub_asset="FUTURES_BASIS_AND_FUNDING",
        native_instrument="CME futures and perpetual funding",
        proxies=(), source="none owned or free with provable point-in-time "
                           "integrity",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="NONE", level=C.LEVEL_NATIVE,
        blocker=C.STATE_BLOCKED_PIT,
        blocker_reason="perpetual funding history is published by venues whose "
                       "own listing and delisting record is not retrievable "
                       "point-in-time, and CME settlement history is not free",
        prior_evidence="never tested",
        families=(C.SF_CARRY, C.SF_ROLL, C.SF_CURVE), lane=None,
        priority="NEEDS_PAID_ENTITLEMENT"),
    "CRYPTO_BROAD": dict(
        asset_class="CRYPTO", sub_asset="BROAD_TOKEN_CROSS_SECTION",
        native_instrument="spot tokens beyond the two majors",
        proxies=(), source="no point-in-time listing record",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="CURRENT_SURVIVORS_ONLY",
        level=C.LEVEL_NATIVE, blocker=C.STATE_BLOCKED_SURVIVORSHIP,
        blocker_reason=C.CRYPTO_BROAD_UNIVERSE_BLOCK_REASON,
        prior_evidence="never tested",
        families=(C.SF_CROSS_SECTIONAL, C.SF_MOMENTUM, C.SF_LIQUIDITY),
        lane=None, priority="NEEDS_SURVIVORSHIP_SAFE_HISTORY"),
    # ------------------------------------------------------------------ I --
    "REAL_ESTATE_LISTED": dict(
        asset_class="REAL_ESTATE", sub_asset="LISTED_REITS",
        native_instrument="listed REIT common stock",
        proxies=("IYR", "RWX"),
        source="Norgate US Equities + Delisted",
        history="1990 -> 2026", frequency="DAILY",
        pit="TOTAL_RETURN_ADJUSTED", survivorship="DELISTED_RETAINED",
        level=C.LEVEL_NATIVE, blocker=None, blocker_reason=None,
        prior_evidence="R34 held IYR and RWX; a listed REIT is an equity and "
                       "was inside R31's equity frontier",
        families=EQUITY_FAMILIES, lane=None, priority="CLOSED"),
    "REAL_ESTATE_DIRECT": dict(
        asset_class="REAL_ESTATE", sub_asset="DIRECT_PROPERTY",
        native_instrument="physical property or a private fund",
        proxies=("IYR",),
        source="appraisal indices only",
        history="NONE_TRADABLE", frequency="QUARTERLY",
        pit="APPRAISAL_SMOOTHED", survivorship="NONE",
        level=C.LEVEL_NATIVE, blocker=C.STATE_OUT_OF_SCOPE,
        blocker_reason="a paper portfolio cannot hold physical property, and "
                       "an appraisal-smoothed index is not a return anyone can "
                       "earn; this is out of scope by construction, not by "
                       "blocker",
        prior_evidence="not applicable",
        families=(C.SF_VALUE, C.SF_CARRY), lane=None, priority="OUT_OF_SCOPE"),
    # ------------------------------------------------------------------ J --
    "INFLATION_SWAPS": dict(
        asset_class="INFLATION", sub_asset="INFLATION_SWAPS",
        native_instrument="zero-coupon inflation swap",
        proxies=("TIP",), source="none owned or free",
        history="NONE", frequency="NONE", pit="NONE",
        survivorship="NONE", level=C.LEVEL_NATIVE,
        blocker=C.STATE_BLOCKED_COST,
        blocker_reason="inflation swap curves are a paid interbank product; "
                       "the Treasury breakeven is the free substitute and is "
                       "already covered by the inflation-linked cell",
        prior_evidence="never tested",
        families=(C.SF_CARRY, C.SF_CURVE, C.SF_RELATIVE_VALUE), lane=None,
        priority="NEEDS_PAID_ENTITLEMENT"),
    # ------------------------------------------------------------------ K --
    "CROSS_ASSET_RV": dict(
        asset_class="CROSS_ASSET", sub_asset="CROSS_ASSET_RELATIVE_VALUE",
        native_instrument="a pair of positions in two different asset classes",
        proxies=("SPY", "$IDCOT7TR", "XAUUSD", "$USBIGCORP", "#CUGC"),
        source="Norgate indices and funds + FRED real yields and breakevens",
        history="2005 -> 2026", frequency="DAILY",
        pit="SAME_SESSION", survivorship="INDEX_LEVEL",
        level=C.LEVEL_PROXY, blocker=None, blocker_reason=None,
        prior_evidence="R32 tested a cross-asset trend sleeve; no interpretable "
                       "relative-value pair was ever run",
        families=(C.SF_RELATIVE_VALUE, C.SF_MACRO_CONDITIONAL, C.SF_CURVE,
                  C.SF_CARRY),
        lane=C.LANE_CROSS_ASSET, priority="EXECUTED"),
}


def _lane_family_results(executed: list) -> dict:
    """(lane, family) -> the executed configurations that covered it."""
    out = {}
    for row in executed:
        for family in row.get("families") or ():
            out.setdefault((row["lane"], family), []).append(row)
    return out


def _cell_state(market: dict, family: str, covering: list) -> tuple:
    """The terminal state of one cell, and the evidence behind it."""
    if family not in market["families"]:
        return (C.STATE_NOT_APPLICABLE,
                "this strategy family has no economic meaning in this market")
    if covering:
        survivors = [r for r in covering if r.get("qualified")]
        level = market["level"]
        if level == C.LEVEL_NATIVE:
            state = (C.STATE_TESTED_NATIVE_SURVIVOR if survivors
                     else C.STATE_TESTED_NATIVE_REJECTED)
        elif level == C.LEVEL_PROXY:
            state = C.STATE_TESTED_PROXY_ONLY
        else:
            state = C.STATE_TESTED_SIGNAL_ONLY
        names = sorted(r["name"] for r in covering)
        return (state, "Release 36 executed %s" % ", ".join(names))
    if market.get("blocker"):
        return (market["blocker"], market.get("blocker_reason") or "")
    if market["level"] == C.LEVEL_SIGNAL:
        return (C.STATE_TESTED_SIGNAL_ONLY,
                market.get("prior_evidence") or "signal research only")
    if market.get("priority") == "CLOSED":
        return (C.STATE_TESTED_PROXY_ONLY if market["level"] == C.LEVEL_PROXY
                else C.STATE_TESTED_NATIVE_REJECTED,
                market.get("prior_evidence") or "")
    return (C.STATE_NOT_TESTED_AVAILABLE,
            "data is available and no Release-36 configuration covered this "
            "family in this market")


def build(executed: list) -> list:
    """Every cell of the global matrix, with a terminal state each."""
    by_lane_family = _lane_family_results(executed)
    cells = []
    for key, market in sorted(MARKETS.items()):
        lane = market.get("lane")
        for family in C.STRATEGY_FAMILIES:
            covering = []
            if lane and family in market["families"]:
                covering = by_lane_family.get((lane, family), [])
            state, evidence = _cell_state(market, family, covering)
            cells.append({
                "market_key": key,
                "asset_class": market["asset_class"],
                "sub_asset": market["sub_asset"],
                "strategy_family": family,
                "native_instrument": market["native_instrument"],
                "proxy_instruments_previously_used": list(market["proxies"]),
                "data_source": market["source"],
                "history": market["history"],
                "frequency": market["frequency"],
                "point_in_time_state": market["pit"],
                "survivorship_state": market["survivorship"],
                "implementation_level": market["level"],
                "prior_release_evidence": market["prior_evidence"],
                "state": state,
                "evidence": evidence,
                "configurations": sorted(r["name"] for r in covering),
                "blocker": market.get("blocker"),
                "blocker_reason": market.get("blocker_reason"),
                "next_executable_action": _next_action(market, state),
                "priority": market.get("priority"),
            })
    return cells


def _next_action(market: dict, state: str) -> str:
    if state == C.STATE_NOT_APPLICABLE:
        return "none - the family does not apply to this market"
    if state in (C.STATE_TESTED_NATIVE_SURVIVOR,):
        return ("register the survivor for prospective evidence through the "
                "canonical forward owner; historical evidence cannot promote "
                "it")
    if state == C.STATE_TESTED_NATIVE_REJECTED:
        return "none - the native market was tested and rejected on merit"
    if state == C.STATE_TESTED_PROXY_ONLY:
        return ("acquire the native instrument's history; the proxy result "
                "does not close this cell")
    if state == C.STATE_TESTED_SIGNAL_ONLY:
        return ("acquire a tradable instrument for this market; the signal "
                "research cannot be implemented")
    if state == C.STATE_BLOCKED_ENTITLEMENT:
        return ("purchase or otherwise obtain the vendor entitlement named in "
                "the blocker, then re-run the lane")
    if state == C.STATE_BLOCKED_LICENSING:
        return ("obtain a licensed feed for the settlement history named in "
                "the blocker")
    if state == C.STATE_BLOCKED_COST:
        return ("take the purchase decision through the released Information "
                "Purchase Gate; this release was not authorised to spend")
    if state == C.STATE_BLOCKED_SURVIVORSHIP:
        return ("obtain a delisting-complete history; nothing built from "
                "survivors can answer this cell")
    if state == C.STATE_BLOCKED_PIT:
        return "obtain a point-in-time source; a current snapshot cannot serve"
    if state == C.STATE_OUT_OF_SCOPE:
        return "none - out of scope by construction"
    if state == C.STATE_NOT_TESTED_AVAILABLE:
        return "execute it; the data is already here"
    return "review"


def summarise(cells: list) -> dict:
    by_state, by_class = {}, {}
    for cell in cells:
        by_state[cell["state"]] = by_state.get(cell["state"], 0) + 1
        bucket = by_class.setdefault(cell["asset_class"], {})
        bucket[cell["state"]] = bucket.get(cell["state"], 0) + 1
    applicable = [c for c in cells if c["state"] != C.STATE_NOT_APPLICABLE]
    tested_native = [c for c in applicable
                     if c["state"] in (C.STATE_TESTED_NATIVE_REJECTED,
                                       C.STATE_TESTED_NATIVE_SURVIVOR)]
    proxy_only = [c for c in applicable
                  if c["state"] == C.STATE_TESTED_PROXY_ONLY]
    signal_only = [c for c in applicable
                   if c["state"] == C.STATE_TESTED_SIGNAL_ONLY]
    blocked = [c for c in applicable if c["state"].startswith("BLOCKED_")]
    untested = [c for c in applicable
                if c["state"] == C.STATE_NOT_TESTED_AVAILABLE]
    total = max(len(applicable), 1)
    return {
        "cells_total": len(cells),
        "cells_applicable": len(applicable),
        "cells_not_applicable": len(cells) - len(applicable),
        "by_state": dict(sorted(by_state.items())),
        "by_asset_class": {k: dict(sorted(v.items()))
                           for k, v in sorted(by_class.items())},
        "tested_native": len(tested_native),
        "tested_native_share": round(len(tested_native) / total, 4),
        "tested_proxy_only": len(proxy_only),
        "tested_proxy_only_share": round(len(proxy_only) / total, 4),
        "tested_signal_only": len(signal_only),
        "blocked": len(blocked),
        "blocked_share": round(len(blocked) / total, 4),
        "still_untested_but_executable": len(untested),
        "still_untested_cells": sorted(
            "%s::%s" % (c["market_key"], c["strategy_family"])
            for c in untested),
        "every_cell_is_terminal": all(c["state"] in C.TERMINAL_STATES
                                      for c in cells),
        "ambiguous_cells": sorted(
            "%s::%s" % (c["market_key"], c["strategy_family"])
            for c in cells if c["state"] not in C.TERMINAL_STATES),
    }


def blocked_frontier(cells: list) -> list:
    """One row per blocked market: what it is and what would unlock it."""
    seen, rows = set(), []
    for cell in cells:
        if not cell["state"].startswith("BLOCKED_"):
            continue
        if cell["market_key"] in seen:
            continue
        seen.add(cell["market_key"])
        rows.append({
            "market_key": cell["market_key"],
            "asset_class": cell["asset_class"],
            "sub_asset": cell["sub_asset"],
            "native_instrument": cell["native_instrument"],
            "state": cell["state"],
            "blocker_reason": cell["blocker_reason"],
            "next_executable_action": cell["next_executable_action"],
            "priority": cell["priority"],
        })
    return sorted(rows, key=lambda r: (r["state"], r["market_key"]))


def artifact(cells: list, *, campaign_id: str, created_at: str,
             entitlements: Optional[dict] = None) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "implementation_levels": list(C.LEVELS),
        "proxy_may_close_a_native_frontier":
            C.PROXY_MAY_CLOSE_A_NATIVE_FRONTIER,
        "terminal_states": list(C.TERMINAL_STATES),
        "markets": len(MARKETS),
        "summary": summarise(cells),
        "blocked_frontier": blocked_frontier(cells),
        "cells": cells,
        "entitlement_sources_blocked": (entitlements or {}).get(
            "sources_blocked"),
        "native_futures_supported": (entitlements or {}).get(
            "native_futures_supported"),
    }
    return r36.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r36.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r36.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r36.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "MARKETS", "build", "summarise",
           "blocked_frontier", "artifact", "freeze", "load", "path_for",
           "EXECUTED_MARKER"]

#: Re-exported so the campaign and the tests agree on what "executed" means.
EXECUTED_MARKER = _experiments.EXECUTED
