"""alpha_agent.r63.ontology - ONE canonical economic-information ontology.

The unit of this ontology is an INFORMATION DIMENSION: a latent economic
state the market could be sensitive to. It is deliberately NOT a feature, a
formula, a dataset or a vendor. Twenty momentum formulas are one dimension;
a dataset that carries three dimensions is three rows in the inventory and
one row in the provider ledger.

Every dimension declares:

    dimension_id     stable identifier (never renamed; retire, never reuse)
    information_class  the coarse group used for orthogonality reporting
    latent_state     what it is, in one sentence
    not_this         what it is commonly confused with
    pit_nature       how the information becomes observable (a market price
                     on its own session, a publication with a lag, a filing
                     with an acceptance instant, a revised statistic that
                     needs vintages)
    applies_to       asset classes where the dimension is economically
                     meaningful (an empty tuple means "all")
    memory_families  the ``information_family`` labels prior releases stamped
                     on ResearchMemory rows that belong to this dimension, so
                     the certification can count what was actually tested

The ordering is fixed and the whole object is hashed, so the ontology is
deterministic across runs and a test can prove it.
"""
from __future__ import annotations

from . import (AC_COMMODITY, AC_CREDIT, AC_CROSS_ASSET, AC_EQUITY_INDEX, AC_FX,
               AC_RATES, AC_US_EQUITY, AC_VOLATILITY, stable_hash)

CALCULATION_OWNER = "alpha_agent.r63.ontology"
ONTOLOGY_SCHEMA = "r63_information_ontology/1"
ARTIFACT_NAME = "information_ontology.json"

# PIT natures.
PN_MARKET = "MARKET_PRICE_OWN_SESSION"
PN_PUBLICATION_LAG = "PUBLICATION_WITH_DECLARED_LAG"
PN_FILING_INSTANT = "FILING_ACCEPTANCE_INSTANT"
PN_VINTAGE = "REVISED_STATISTIC_NEEDS_VINTAGE"
PN_VENDOR_SNAPSHOT = "VENDOR_SNAPSHOT_PIT_UNVERIFIED"

IC_PRICE = "PRICE_STATE"
IC_FUNDAMENTAL = "FUNDAMENTAL"
IC_EXPECTATIONS = "EXPECTATIONS"
IC_DISCLOSURE = "DISCLOSURE_AND_BEHAVIOUR"
IC_POSITIONING = "POSITIONING_AND_FLOW"
IC_CURVE = "CARRY_AND_CURVE"
IC_MACRO = "MACRO_AND_POLICY"
IC_PHYSICAL = "PHYSICAL_SUPPLY_DEMAND"
IC_RISK = "CROSS_ASSET_AND_RISK_STATE"
IC_EVENT = "EVENT_AND_CALENDAR"
INFORMATION_CLASSES = (IC_PRICE, IC_FUNDAMENTAL, IC_EXPECTATIONS, IC_DISCLOSURE,
                       IC_POSITIONING, IC_CURVE, IC_MACRO, IC_PHYSICAL, IC_RISK,
                       IC_EVENT)

_ALL = ()
_FUT = (AC_EQUITY_INDEX, AC_RATES, AC_COMMODITY, AC_FX, AC_VOLATILITY,
        AC_CROSS_ASSET)
_EQ = (AC_US_EQUITY,)


def _d(dimension_id, information_class, latent_state, not_this, pit_nature,
       applies_to, memory_families, typical_observables):
    return {
        "dimension_id": dimension_id,
        "information_class": information_class,
        "latent_state": latent_state,
        "not_this": not_this,
        "pit_nature": pit_nature,
        "applies_to": list(applies_to),
        "memory_families": list(memory_families),
        "typical_observables": list(typical_observables),
    }


DIMENSIONS = (
    # ---------------------------------------------------------------- price
    _d("PRICE_RETURN_STATE", IC_PRICE,
       "where price sits relative to its own recent path (1-21 session returns, "
       "distance from highs and lows)",
       "trend or momentum: this is the level of the recent path, not its "
       "persistence",
       PN_MARKET, _ALL, ("PRICE_STATE", "PRICE_STATE_PLACEBO"),
       ("ret_5", "ret_21", "dist_52w_high")),
    _d("TREND", IC_PRICE,
       "time-series persistence of an instrument's OWN direction over 3-12 "
       "months",
       "cross-sectional momentum, which ranks instruments against each other",
       PN_MARKET, _ALL, ("PRICE_STATE",), ("ret_252_skip_21", "ma_cross")),
    _d("MOMENTUM", IC_PRICE,
       "cross-sectional relative strength: which instruments led their peers "
       "over 3-12 months",
       "trend, which needs no peer",
       PN_MARKET, _ALL, ("PRICE_STATE",), ("xs_rank(ret_126_skip_21)",)),
    _d("REVERSAL", IC_PRICE,
       "short-horizon mean reversion of 1-5 session and 1-month moves",
       "a negative momentum coefficient at long horizons",
       PN_MARKET, _ALL, ("PRICE_STATE",), ("ret_5", "ret_21")),
    _d("REALISED_VOLATILITY", IC_PRICE,
       "the realised variance regime of the instrument and its change",
       "implied volatility, which is an expectation and lives in "
       "VOLATILITY_EXPECTATIONS_IV",
       PN_MARKET, _ALL, ("PRICE_STATE",), ("vol_21", "vol_63", "vol_ratio")),
    _d("TAIL_CRASH_STATE", IC_PRICE,
       "drawdown depth, realised skew and jump frequency: is the instrument "
       "in or near a crash state",
       "realised volatility level",
       PN_MARKET, _ALL, ("PRICE_STATE",), ("drawdown_252", "skew_63",
                                           "max_abs_ret_21")),
    _d("LIQUIDITY", IC_PRICE,
       "how expensive it is to trade: dollar volume, Amihud illiquidity, "
       "spread proxies",
       "volume as a participation signal",
       PN_MARKET, _ALL, ("PRICE_STATE", "PRICE_VOLUME"),
       ("log_adv", "amihud_63")),
    _d("VOLUME_PARTICIPATION", IC_PRICE,
       "abnormal participation: volume and open-interest relative to their "
       "own baseline",
       "liquidity, which is a cost state",
       PN_MARKET, _ALL, ("PRICE_VOLUME",), ("vol_z_21", "oi_chg_21")),
    # ---------------------------------------------------------- fundamental
    _d("FUNDAMENTAL_LEVELS", IC_FUNDAMENTAL,
       "the level of profitability, leverage, asset intensity and accruals as "
       "of the latest filing",
       "fundamental change",
       PN_FILING_INSTANT, _EQ, ("FUNDAMENTAL_FACT", "PRICE_AND_FUNDAMENTAL"),
       ("fcf_to_assets", "accruals_to_assets", "opinc_to_assets")),
    _d("FUNDAMENTAL_CHANGE", IC_FUNDAMENTAL,
       "the direction in which profitability, working capital and asset "
       "growth are moving year on year",
       "level",
       PN_FILING_INSTANT, _EQ, ("FUNDAMENTAL_CHANGE",),
       ("d_opinc_to_assets", "asset_growth", "sales_growth")),
    _d("FREE_CASH_FLOW", IC_FUNDAMENTAL,
       "cash generation after investment, relative to the asset base",
       "earnings, which include accruals",
       PN_FILING_INSTANT, _EQ, ("FUNDAMENTAL_FACT",), ("fcf_to_assets",)),
    # --------------------------------------------------------- expectations
    _d("EARNINGS_EXPECTATIONS", IC_EXPECTATIONS,
       "the consensus forecast of earnings and its surprise against reported "
       "earnings",
       "a revision, which is the CHANGE in the consensus",
       PN_VENDOR_SNAPSHOT, _EQ, ("EARNINGS_EVENTS",), ("eps_consensus",
                                                       "sue_analyst")),
    _d("REVENUE_EXPECTATIONS", IC_EXPECTATIONS,
       "the consensus forecast of revenue and its surprise",
       "earnings expectations",
       PN_VENDOR_SNAPSHOT, _EQ, (), ("revenue_consensus",)),
    _d("ANALYST_REVISIONS", IC_EXPECTATIONS,
       "dated changes in consensus estimates and recommendations",
       "today's consensus differenced against itself, which the purchase gate "
       "forbids",
       PN_VENDOR_SNAPSHOT, _EQ, (), ("revision_breadth_30d",)),
    _d("DISPERSION", IC_EXPECTATIONS,
       "disagreement: the dispersion of forecasts for one name, and the "
       "cross-sectional dispersion of realised returns as its free proxy",
       "volatility",
       PN_VENDOR_SNAPSHOT, _ALL, (), ("analyst_dispersion", "xs_return_dispersion")),
    # ----------------------------------------------- disclosure / behaviour
    _d("CORPORATE_DISCLOSURES", IC_DISCLOSURE,
       "the timing and freshness of periodic disclosure: filing age, expected "
       "filing window, lateness",
       "the content of the filing (fundamentals)",
       PN_FILING_INSTANT, _EQ, ("FILING_EVENT",), ("obs_age_days",
                                                   "days_to_expected_filing")),
    _d("DISCLOSURE_INTENSITY_LANGUAGE", IC_DISCLOSURE,
       "how much and how urgently a company is disclosing: 8-K rate against "
       "its own baseline, amendments, late-filing notices, and language tone",
       "periodic filing timing",
       PN_FILING_INSTANT, _EQ, ("FILING_EVENT",), ("k8_rate_z", "nt_filings",
                                                   "amendment_rate")),
    _d("INSIDER_BEHAVIOUR", IC_DISCLOSURE,
       "open-market purchases and sales by officers and directors, counted at "
       "the SEC filing instant",
       "option exercises, awards and tax withholding, which are compensation "
       "calendar",
       PN_FILING_INSTANT, _EQ, ("INSIDER_FLOW",), ("net_buy_filings_63",
                                                   "buy_share_126")),
    _d("SHORT_POSITIONING", IC_POSITIONING,
       "short interest and daily short-sale participation",
       "put-call skew",
       PN_PUBLICATION_LAG, _EQ, ("SHORT_VOLUME",), ("short_volume_ratio_21",
                                                    "short_interest_days")),
    _d("POSITIONING_COMMITMENTS", IC_POSITIONING,
       "who holds the futures risk: speculative and commercial net positioning "
       "from the CFTC Commitments of Traders report",
       "open interest, which says how much risk is held, not by whom",
       PN_PUBLICATION_LAG, _FUT, ("CFTC_POSITIONING", "POSITIONING"),
       ("spec_net_oi_z_156w", "spec_net_chg_13w")),
    _d("OWNERSHIP_INSTITUTIONAL_FLOW", IC_POSITIONING,
       "changes in institutional ownership breadth from 13F filings",
       "insider behaviour",
       PN_FILING_INSTANT, _EQ, (), ("13f_breadth_change",)),
    _d("ETF_FUND_FLOW", IC_POSITIONING,
       "creations, redemptions and mutual-fund flows into an asset class",
       "positioning commitments",
       PN_PUBLICATION_LAG, _ALL, (), ("etf_flow_4w",)),
    # ---------------------------------------------------------- carry/curve
    _d("CARRY", IC_CURVE,
       "the return earned if nothing changes: interest differential in FX, "
       "roll yield in commodities, dividend-minus-financing in index futures",
       "momentum",
       PN_MARKET, _FUT, ("FUTURES_TERM_STRUCTURE", "CALENDAR_STRUCTURE"),
       ("slope_ann", "fx_rate_differential")),
    _d("FUTURES_BASIS", IC_CURVE,
       "the spread between the front and the deferred dated contract",
       "the back-adjustment offset of a continuous series (an artifact)",
       PN_MARKET, _FUT, ("FUTURES_CURVE", "FUTURES_TERM_STRUCTURE"),
       ("basis_front_deferred",)),
    _d("TERM_STRUCTURE", IC_CURVE,
       "the slope of a yield or volatility curve across maturities",
       "the level of the curve",
       PN_MARKET, (AC_RATES, AC_EQUITY_INDEX, AC_VOLATILITY, AC_CROSS_ASSET,
                   AC_CREDIT), ("VOLATILITY_TERM_STRUCTURE", "MACRO_RATES_LEVELS"),
       ("cmt_2s10s", "vix3m_over_vix")),
    _d("CURVE_SHAPE", IC_CURVE,
       "curvature and butterflies: the middle of the curve against its ends",
       "slope",
       PN_MARKET, (AC_RATES, AC_CROSS_ASSET), ("MACRO_RATES_LEVELS",),
       ("cmt_butterfly_2_5_10",)),
    # ---------------------------------------------------------------- macro
    _d("MACRO_LEVELS", IC_MACRO,
       "the level of activity, labour and inflation statistics as first "
       "published",
       "the revised series a database serves today",
       PN_VINTAGE, _FUT + (AC_CREDIT,), ("MACRO_OBSERVATION", "PRICE_AND_MACRO"),
       ("unrate_first_release", "nfci_level")),
    _d("MACRO_CHANGE", IC_MACRO,
       "the direction of macro statistics between vintages: is the economy "
       "accelerating or slowing as known at the time",
       "level",
       PN_VINTAGE, _FUT + (AC_CREDIT,), ("MACRO_OBSERVATION",),
       ("d_unrate_3m_first_release", "icsa_chg_4w")),
    _d("MACRO_SURPRISES", IC_MACRO,
       "the first-release value against what was expected before the release",
       "the change in the series",
       PN_VINTAGE, _FUT + (AC_CREDIT,), ("MACRO_RELEASE_SURPRISE",),
       ("surprise_vs_naive_forecast",)),
    _d("POLICY_EXPECTATIONS", IC_MACRO,
       "the market-implied path of the policy rate over the next months",
       "the current policy rate",
       PN_MARKET, _FUT + (AC_CREDIT,), (), ("ff_implied_chg_1m",
                                              "cmt_3m_minus_effr")),
    _d("RATES_EXPECTATIONS", IC_MACRO,
       "expected future nominal rates beyond the policy horizon: the 2-year "
       "yield against the policy rate and its change",
       "policy expectations",
       PN_MARKET, _FUT + (AC_CREDIT,), ("MACRO_RATES_LEVELS",),
       ("cmt_2y_minus_effr", "d_cmt_2y_21")),
    _d("INFLATION_EXPECTATIONS", IC_MACRO,
       "market-implied breakeven inflation and its change",
       "realised inflation",
       PN_MARKET, _FUT + (AC_CREDIT,), (), ("be_10y", "d_be_10y_21")),
    _d("CREDIT_CONDITIONS", IC_MACRO,
       "the price of credit risk: high-yield and investment-grade option-"
       "adjusted spreads, their change and the HY-IG differential",
       "equity volatility",
       PN_MARKET, _FUT + (AC_CREDIT,), ("CREDIT_SPREADS",),
       ("oas_hy_z", "d_oas_hy_21", "hy_minus_ig")),
    _d("VOLATILITY_EXPECTATIONS_IV", IC_MACRO,
       "option-implied volatility, its term structure and the variance risk "
       "premium against realised volatility",
       "realised volatility",
       PN_MARKET, _ALL, ("VOLATILITY_TERM_STRUCTURE",),
       ("vix_level", "vix_term_slope", "vrp_21", "ovx", "gvz")),
    # ------------------------------------------------------------- physical
    _d("COMMODITY_SUPPLY_DEMAND", IC_PHYSICAL,
       "physical balance: production, consumption and trade of the underlying",
       "inventory, which is the accumulated balance",
       PN_PUBLICATION_LAG, (AC_COMMODITY, AC_CROSS_ASSET), (),
       ("production_chg", "consumption_chg")),
    _d("INVENTORY", IC_PHYSICAL,
       "stocks of the physical commodity relative to seasonal norms, and "
       "their weekly change",
       "the futures curve, which prices the inventory state",
       PN_PUBLICATION_LAG, (AC_COMMODITY, AC_CROSS_ASSET), (),
       ("crude_stocks_z_5y_seasonal", "d_stocks_4w")),
    _d("SHIPPING_TRANSPORT", IC_PHYSICAL,
       "freight rates and transport congestion as a demand and bottleneck "
       "signal",
       "commodity prices",
       PN_MARKET, (AC_COMMODITY, AC_CROSS_ASSET), (), ("baltic_dry_chg",)),
    # ------------------------------------------------------- cross-asset
    _d("CROSS_ASSET_TRANSMISSION", IC_RISK,
       "lagged information transmitted from one asset class to another: "
       "equities to commodities, the dollar to everything, rates to equities",
       "a contemporaneous correlation",
       PN_MARKET, _ALL, ("CROSS_ASSET_LEAD_LAG",),
       ("lag_ret_usd_21", "lag_ret_equity_21", "lag_ret_rates_21")),
    _d("RISK_APPETITE", IC_RISK,
       "the composite willingness to hold risk: implied volatility, credit "
       "spreads and equity drawdown together",
       "any one of its components",
       PN_MARKET, _ALL, (), ("risk_appetite_composite_z",)),
    _d("FUNDING_LIQUIDITY_CONDITIONS", IC_RISK,
       "the cost and availability of funding: overnight rate spreads, "
       "financial conditions indices",
       "credit conditions, which price default risk",
       PN_VINTAGE, _FUT + (AC_CREDIT,), (), ("sofr_minus_effr", "nfci_z")),
    # ------------------------------------------------------------ events
    _d("EVENT_INFORMATION", IC_EVENT,
       "scheduled information arrivals: earnings dates, macro release "
       "calendars, policy meetings, and the reaction to them",
       "the content of the release",
       PN_PUBLICATION_LAG, _ALL, ("SCHEDULED_EVENT_CALENDAR", "EARNINGS_EVENTS"),
       ("days_to_scheduled_event", "post_event_drift")),
    _d("CALENDAR_SEASONALITY", IC_EVENT,
       "deterministic calendar structure: month of year, turn of month, "
       "agricultural seasons",
       "event information, which is a dated arrival",
       PN_MARKET, _ALL, ("CALENDAR_STRUCTURE",), ("month_of_year",
                                                  "turn_of_month")),
)

DIMENSION_IDS = tuple(d["dimension_id"] for d in DIMENSIONS)
_BY_ID = {d["dimension_id"]: d for d in DIMENSIONS}

#: Baseline information the estate ALREADY USES to make decisions: the price
#: block for every class, plus PIT fundamentals for equities (the operational
#: champion's other leg). Every conditional measurement controls for these.
BASELINE_DIMENSIONS = {
    "DEFAULT": ("PRICE_RETURN_STATE", "TREND", "MOMENTUM", "REVERSAL",
                "REALISED_VOLATILITY", "TAIL_CRASH_STATE", "LIQUIDITY",
                "VOLUME_PARTICIPATION"),
    AC_US_EQUITY: ("PRICE_RETURN_STATE", "TREND", "MOMENTUM", "REVERSAL",
                   "REALISED_VOLATILITY", "TAIL_CRASH_STATE", "LIQUIDITY",
                   "VOLUME_PARTICIPATION", "FUNDAMENTAL_LEVELS",
                   "FREE_CASH_FLOW"),
}


def dimension(dimension_id: str) -> dict:
    return dict(_BY_ID[dimension_id])


def baseline_for(asset_class: str) -> tuple:
    return BASELINE_DIMENSIONS.get(asset_class, BASELINE_DIMENSIONS["DEFAULT"])


def applies(dimension_id: str, asset_class: str) -> bool:
    ap = _BY_ID[dimension_id]["applies_to"]
    return (not ap) or (asset_class in ap)


def family_to_dimensions() -> dict:
    """``information_family`` label on a ResearchMemory row -> dimension ids."""
    out: dict = {}
    for d in DIMENSIONS:
        for fam in d["memory_families"]:
            out.setdefault(fam, []).append(d["dimension_id"])
    return {k: sorted(v) for k, v in sorted(out.items())}


def ontology_hash() -> str:
    return stable_hash([dict(d) for d in DIMENSIONS])


def build() -> dict:
    """The machine-readable ontology artifact body (deterministic)."""
    by_class: dict = {}
    for d in DIMENSIONS:
        by_class.setdefault(d["information_class"], []).append(d["dimension_id"])
    return {
        "schema": ONTOLOGY_SCHEMA,
        "calculation_owner": CALCULATION_OWNER,
        "unit_of_analysis": "INFORMATION_DIMENSION - a latent economic state, "
                            "never a formula, dataset or provider",
        "rule": "formula variants of one economic state are ONE dimension; a "
                "dataset carrying k states contributes to k dimensions",
        "information_classes": list(INFORMATION_CLASSES),
        "dimensions": [dict(d) for d in DIMENSIONS],
        "n_dimensions": len(DIMENSIONS),
        "by_class": {k: by_class[k] for k in INFORMATION_CLASSES if k in by_class},
        "baseline_dimensions": {k: list(v) for k, v in BASELINE_DIMENSIONS.items()},
        "memory_family_map": family_to_dimensions(),
        "pit_natures": [PN_MARKET, PN_PUBLICATION_LAG, PN_FILING_INSTANT,
                        PN_VINTAGE, PN_VENDOR_SNAPSHOT],
        "ontology_hash": ontology_hash(),
    }
