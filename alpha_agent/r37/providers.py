"""alpha_agent.r37.providers - the ONE Release 37 provider / dataset long list.

Every serious candidate this release challenged, with the properties that were
actually established and the evidence class that established each one. Nothing
here is a summary of a sales page presented as a fact: each row carries
``evidence`` naming how its claims were obtained, and rows whose numbers came
only from vendor documentation say so.

The rows deliberately include candidates that LOSE. A long list that contains
only the winner is a justification, not a review, and the three questions the
operator will actually ask - "why not the cheap one?", "why not the deep one?",
"why not the equity one?" - can only be answered by rows that were scored and
rejected.

Cells unlocked are NOT declared here. They are derived by
:mod:`alpha_agent.r37.unlock` from the frozen Release-36 coverage matrix and the
``markets_covered`` mapping below, so a vendor cannot be credited with a market
its declared instruments do not implement.
"""
from __future__ import annotations

from typing import Optional

from .. import r37
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r37.providers"
SCHEMA = "r37_provider_long_list/1"
ARTIFACT_NAME = "provider_long_list.json"
SCORECARD_SCHEMA = "r37_provider_scorecard/1"
SCORECARD_ARTIFACT = "provider_scorecard.json"

C = _contract

# --------------------------------------------------------------------------- #
# Lanes. Priority order is the release's, not a vendor's.
# --------------------------------------------------------------------------- #
LANE_FUTURES = "FUTURES_AND_DERIVATIVES"
LANE_ANALYST = "ANALYST_EXPECTATIONS"
LANE_OPTIONS = "OPTIONS_AND_VOLATILITY"
LANE_CREDIT = "CREDIT"
LANE_CRYPTO = "CRYPTO"
LANE_OTHER = "OTHER_ORTHOGONAL"
LANES = (LANE_FUTURES, LANE_ANALYST, LANE_OPTIONS, LANE_CREDIT, LANE_CRYPTO,
         LANE_OTHER)

#: Every field a scorecard row must carry. A row missing one of these is a
#: defect, not a shorter row, and :func:`validate` says which field is missing.
SCORECARD_FIELDS = (
    "dataset_id", "provider", "dataset_name", "lane", "source_url",
    "asset_classes", "instruments", "implementation_level",
    "dated_contracts_available", "history_start", "history_end",
    "history_years", "frequency",
    "breadth", "inactive_discontinued_coverage", "point_in_time_semantics",
    "survivorship_property", "identity_metadata", "settlement_and_ohlc",
    "volume", "open_interest", "event_fields", "delivery_mechanism",
    "sample_availability", "sample_quality", "licence_constraints",
    "research_use_rights", "redistribution_constraints", "commercial_terms",
    "monthly_cost_usd", "annual_cost_usd", "one_time_cost_usd",
    "minimum_commitment", "trial_required", "account_required",
    "credit_card_required", "implementation_complexity",
    "estimated_storage_gb", "data_engineering_complexity",
    "markets_covered", "markets_partial", "future_research_lanes",
    "incremental_distinctness", "confidence_in_vendor_claims",
    "open_questions", "evidence", "gate_state", "gate_reason",
    # --- machine-readable classifications the scorer reads ------------------ #
    # The prose fields above are the human evidence; these five are the ONLY
    # thing alpha_agent.r37.scoring looks at. Keeping them separate stops a
    # scorer from parsing a sentence, and makes every factor auditable against
    # its declared vocabulary.
    "pit_class", "survivorship_class", "licence_class", "identity_class",
    "opacity_class",
)

#: Every classification field, with the contract mapping whose keys it must use.
CLASSIFICATION_FIELDS = ("pit_class", "survivorship_class", "licence_class",
                         "identity_class", "opacity_class")

UNKNOWN = "UNKNOWN"


def _row(**kw):
    """One scorecard row with every declared field present."""
    row = {f: kw.get(f) for f in SCORECARD_FIELDS}
    return row


# --------------------------------------------------------------------------- #
# LANE A - futures and derivatives. Release 36 made this the prior.
# --------------------------------------------------------------------------- #
NORGATE_FUTURES = _row(
    dataset_id="norgate_futures_package",
    history_years=45.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='STRONG',
    opacity_class='RAW_AS_PUBLISHED',
    provider="Norgate Data",
    dataset_name="Futures Package (individual dated contracts + continuous)",
    lane=LANE_FUTURES,
    source_url="https://norgatedata.com/futurespackage.php",
    asset_classes=["COMMODITY", "RATES", "FX", "EQUITY_INDEX",
                   "INTERNATIONAL_EQUITY", "VOLATILITY", "CRYPTO"],
    instruments=(
        "~100 futures markets across 11 exchange groups. Metals GC/SI/HG/PL/PA; "
        "grains ZC/ZW/ZS/ZM/ZL/ZO/ZR/KE; softs KC/SB/CT/CC/OJ/RS; livestock "
        "LE/GF/HE/DC; rates ZT/ZF/ZN/TN/ZB/UB/ZQ/SR3/LEU; energy CL/HO/NG/RB "
        "plus ICE Brent and gasoil; FX 6A/6B/6C/6E/6J/6S/6M/6N; index "
        "ES/NQ/RTY/YM plus ASX SPI 200, Eurex DAX and Euro STOXX 50, SGX "
        "Nikkei 225 and MSCI Singapore; Cboe VX from 2004; CME BTC and ETH"),
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="~1977-1980, or the market's first trading day",
    history_end="current, updated daily",
    frequency="DAILY",
    breadth="~100 markets; individual contracts plus unadjusted and "
            "back-adjusted spot-month continuous series",
    inactive_discontinued_coverage=(
        "expired contracts are retained as individual series; this is the "
        "point of a dated-contract archive and the vendor's own back-adjustment "
        "documentation depends on it"),
    point_in_time_semantics=(
        "official daily SETTLEMENT price as the close, not the last trade; a "
        "settlement is the number the exchange published that evening and is "
        "therefore observable-as-published"),
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata=(
        "MEASURED on the installed client: first_notice_date, "
        "last_quoted_date, first_quoted_date, point_value, tick_size, "
        "lowest_ever_tick_size, margin, currency, exchange_name, "
        "futures_market_session_contracts and futures_market_symbols all exist "
        "in norgatedata 1.0.74 and return real values for the entitled market"),
    settlement_and_ohlc="official settlement close plus OHLC",
    volume=True,
    open_interest=True,
    event_fields="delivery month, first notice day, last trading day",
    delivery_mechanism=(
        "local Norgate Data Updater database read through the norgatedata "
        "Python client - ALREADY INSTALLED (v1.0.74) and ALREADY INTEGRATED by "
        "alpha_agent.r33.universe and alpha_agent.r34.universe"),
    sample_availability=(
        "no public sample file; the estate's own entitled market &ES is the "
        "sample and was measured live by this release"),
    sample_quality=(
        "MEASURED: &ES returns point_value 50.0, tick_size 0.25, margin "
        "16060.0, currency USD, exchange CME, first_quoted_date 1997-09-09. "
        "futures_market_symbols() returns exactly ONE market and "
        "futures_market_session_contracts('&ES') raises, which is the "
        "entitlement wall rather than a client limitation"),
    licence_constraints="single-user research and personal trading licence",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="no redistribution; this estate redistributes "
                              "nothing",
    commercial_terms="ADD-ON PACKAGE on an existing paid Norgate account; no "
                     "new vendor relationship, no new contract counterparty",
    monthly_cost_usd=None,
    annual_cost_usd=270.0,
    one_time_cost_usd=0.0,
    minimum_commitment="6 months (USD 148.50) or 12 months (USD 270)",
    trial_required=False,
    account_required=False,
    credit_card_required=True,
    implementation_complexity="LOW",
    estimated_storage_gb=3.0,
    data_engineering_complexity=(
        "LOW - the reader, the admissibility rules and the point-in-time "
        "alignment convention already exist and are already tested"),
    markets_covered=[
        "CMDTY_PRECIOUS", "CMDTY_INDUSTRIAL", "CMDTY_GRAINS", "CMDTY_SOFTS",
        "CMDTY_LIVESTOCK", "RATES_TREASURY_FUTURES", "VOL_VIX_FUTURES",
        "INTL_EQUITY_DEVELOPED"],
    markets_partial=["RATES_INTERNATIONAL", "INTL_EQUITY_EMERGING",
                     "CRYPTO_BASIS_FUNDING"],
    future_research_lanes=(
        "roll yield and curve carry across five commodity sectors; "
        "cheapest-to-deliver and basis on the Treasury curve; a native VX term "
        "structure with settlements; commodity seasonality with 45 years of "
        "dated contracts; cross-sectional commodity momentum against a real "
        "collateral return; CME crypto basis against owned spot"),
    incremental_distinctness=(
        "HIGH - the estate owns no dated contract outside the EIA energy "
        "archive, so 36 commodity, 6 rates, 7 international-equity and 4 "
        "volatility cells have no instrument at all today"),
    confidence_in_vendor_claims="HIGH",
    open_questions=(
        "does the package include Eurex Bund / JGB / Gilt, which would move "
        "RATES_INTERNATIONAL from partial to full? does it include an emerging "
        "index future? is the USD 270 figure the futures package price rather "
        "than the site-wide package tier?"),
    evidence=[C.EVIDENCE_VENDOR_PAGE, C.EVIDENCE_LOCAL_CLIENT,
              C.EVIDENCE_PRIOR_RELEASE],
    gate_state=C.STATE_BUY_RECOMMENDED,
    gate_reason=(
        "native dated contracts, ~45 years of history, official settlements, "
        "volume and open interest, expired contracts retained, full contract "
        "identity metadata proven present in the ALREADY INSTALLED client, on "
        "an ALREADY PAID vendor account, for USD 270 a year"),
)

DATABENTO_GLBX = _row(
    dataset_id="databento_glbx_mdp3",
    history_years=16.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='STRONG',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="Databento",
    dataset_name="CME Globex MDP 3.0 historical (GLBX.MDP3)",
    lane=LANE_FUTURES,
    source_url="https://databento.com/datasets/GLBX.MDP3",
    asset_classes=["COMMODITY", "RATES", "FX", "EQUITY_INDEX", "CRYPTO"],
    instruments="all CME, CBOT, NYMEX and COMEX listed expirations",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="~2010 (the MDP 3.0 era); 16+ years on the Unlimited plan",
    history_end="current",
    frequency="TICK_TO_DAILY",
    breadth="exchange-wide; every listed expiration",
    inactive_discontinued_coverage="expired contracts present within the "
                                   "covered window",
    point_in_time_semantics="raw market-data capture, timestamped as received",
    survivorship_property="DISCONTINUED_RETAINED_WITHIN_WINDOW",
    identity_metadata="full instrument definitions from the feed",
    settlement_and_ohlc="derived from the feed rather than the exchange "
                        "settlement file",
    volume=True,
    open_interest=True,
    event_fields="instrument definition messages",
    delivery_mechanism="HTTP API and client libraries; per-GB downloads",
    sample_availability="USD 125 free credits for new signups, expiring after "
                        "six months",
    sample_quality="NOT MEASURED - obtaining the sample requires creating a "
                   "commercial account, which this release may not do",
    licence_constraints="redistribution generally permitted after 24 hours, "
                        "subject to the original publisher",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="24-hour delay for redistribution",
    commercial_terms="pay-as-you-go per GB, or Standard USD 199/month "
                     "(12 months of L1 history only), Plus USD 1,750/month, "
                     "Unlimited USD 4,500/month with an annual contract",
    monthly_cost_usd=199.0,
    annual_cost_usd=2388.0,
    one_time_cost_usd=0.0,
    minimum_commitment="monthly on Standard; annual contract above it",
    trial_required=False,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="MEDIUM",
    estimated_storage_gb=200.0,
    data_engineering_complexity=(
        "HIGH - a raw market-data feed must be aggregated into daily bars and "
        "a settlement convention chosen, which is exactly the vendor "
        "transformation this estate prefers to receive already made by the "
        "exchange"),
    markets_covered=[],
    markets_partial=["CMDTY_PRECIOUS", "CMDTY_INDUSTRIAL", "CMDTY_GRAINS",
                     "CMDTY_SOFTS", "CMDTY_LIVESTOCK",
                     "RATES_TREASURY_FUTURES"],
    future_research_lanes="intraday microstructure, which this estate has no "
                          "decision cadence for",
    incremental_distinctness="HIGH in content, LOW in marginal value once a "
                             "daily settlement archive exists",
    confidence_in_vendor_claims="HIGH",
    open_questions="what does 16 years of daily settlements for 40 markets "
                   "actually cost per GB?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_NO_COST_VALUE,
    gate_reason=(
        "the affordable tier carries 12 months of history, and the tier with "
        "16+ years costs USD 54,000 a year. The history floor is 15 years, so "
        "the only qualifying tier fails cost/value against a USD 270 "
        "alternative by two orders of magnitude"),
)

FIRSTRATE_FUTURES = _row(
    dataset_id="firstrate_data_futures",
    history_years=19.0,
    pit_class='UNKNOWN',
    survivorship_class='PARTIAL_INACTIVE_COVERAGE',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='WEAK',
    opacity_class='OPAQUE_VENDOR_TRANSFORM',
    provider="FirstRate Data",
    dataset_name="Historical futures bundle (individual + continuous)",
    lane=LANE_FUTURES,
    source_url="https://firstratedata.com/it/futures",
    asset_classes=["COMMODITY", "RATES", "FX", "EQUITY_INDEX", "VOLATILITY"],
    instruments="~122-130 most active futures contracts",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="2007 (vendor markets it as 15 years)",
    history_end="current, with one month of updates included",
    frequency="1_MINUTE_TO_DAILY",
    breadth="most-active contracts only",
    inactive_discontinued_coverage=(
        "individual contracts are included, but the universe is defined as "
        "'most active', which is a survivorship rule applied to markets"),
    point_in_time_semantics="traded bars; no exchange settlement series stated",
    survivorship_property="PARTIAL_INACTIVE_COVERAGE",
    identity_metadata="ticker and contract code; no first notice or expiry "
                      "field documented",
    settlement_and_ohlc="OHLC bars; SETTLEMENT NOT DOCUMENTED",
    volume=True,
    open_interest=False,
    event_fields=None,
    delivery_mechanism="one-off download link plus a month of updates",
    sample_availability="public sample files",
    sample_quality="NOT MEASURED - not required, because the missing fields "
                   "are missing by the vendor's own description",
    licence_constraints="single-user",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="no redistribution",
    commercial_terms="one-time purchase USD 299.95 - 399.95",
    monthly_cost_usd=None,
    annual_cost_usd=0.0,
    one_time_cost_usd=349.95,
    minimum_commitment="none",
    trial_required=False,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="MEDIUM",
    estimated_storage_gb=40.0,
    data_engineering_complexity="MEDIUM - a new reader and a new identity map",
    markets_covered=[],
    markets_partial=["CMDTY_PRECIOUS", "CMDTY_GRAINS", "CMDTY_SOFTS",
                     "RATES_TREASURY_FUTURES"],
    future_research_lanes="intraday execution research, which is out of scope",
    incremental_distinctness="MEDIUM",
    confidence_in_vendor_claims="MEDIUM",
    open_questions="does any daily file carry settlement and open interest?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_NO_LOW_VALUE,
    gate_reason=(
        "no documented settlement series and no open interest, so roll yield, "
        "curve carry and positioning - the three families that make a futures "
        "archive worth buying - cannot be built. It is also dominated on "
        "history (2007 against ~1980) and on price by the recommendation"),
)

CSI_UNFAIR_ADVANTAGE = _row(
    dataset_id="csi_unfair_advantage_futures",
    history_years=45.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='MODERATE',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="CSI Data",
    dataset_name="Unfair Advantage futures database",
    lane=LANE_FUTURES,
    source_url="https://www.csidata.com/?page_id=14",
    asset_classes=["COMMODITY", "RATES", "FX", "EQUITY_INDEX"],
    instruments="world futures markets back to the first day of trading; the "
                "personal tier processes up to 59 futures markets",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="first day of trading for most markets",
    history_end="current",
    frequency="DAILY",
    breadth="59 markets on the personal tier; more on the professional tier",
    inactive_discontinued_coverage="delisted markets retained",
    point_in_time_semantics="daily settlement",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="contract identity and roll metadata",
    settlement_and_ohlc="settlement plus OHLC",
    volume=True,
    open_interest=True,
    event_fields="delivery and notice dates",
    delivery_mechanism="Unfair Advantage desktop application and local database",
    sample_availability="none published without contact",
    sample_quality=UNKNOWN,
    licence_constraints="personal-and-private-use tier, or a professional tier "
                        "for business use",
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints=UNKNOWN,
    commercial_terms="price not published for the futures database; the "
                     "published figure is USD 125 per year per instrument for "
                     "OPTIONS history with a five-year minimum",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=False,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="MEDIUM",
    estimated_storage_gb=10.0,
    data_engineering_complexity="MEDIUM - a new desktop application and a new "
                                "export path",
    markets_covered=[],
    markets_partial=["CMDTY_PRECIOUS", "CMDTY_INDUSTRIAL", "CMDTY_GRAINS",
                     "CMDTY_SOFTS", "CMDTY_LIVESTOCK",
                     "RATES_TREASURY_FUTURES"],
    future_research_lanes="the same lanes as the recommendation",
    incremental_distinctness="HIGH in content, ZERO once the recommendation is "
                             "held",
    confidence_in_vendor_claims="MEDIUM",
    open_questions="what does the futures database cost, and does the personal "
                   "tier's 59-market cap bind?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_BLOCKED_CONTACT,
    gate_reason="no published price for the futures database; a quote requires "
                "a sales conversation this release may not have",
)

PORTARA_CQG = _row(
    dataset_id="portara_cqg_futures",
    history_years=60.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='MODERATE',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="Portara / CQG DataFactory",
    dataset_name="Historical daily and intraday futures (individual contracts)",
    lane=LANE_FUTURES,
    source_url="https://portaracqg.com/historical-daily-futures-data/",
    asset_classes=["COMMODITY", "RATES", "FX", "EQUITY_INDEX"],
    instruments="global futures and FX; individual contracts or continuous",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="daily from 1899 for the deepest markets; 1987 for one-minute",
    history_end="current",
    frequency="TICK_TO_DAILY",
    breadth="the widest of any candidate",
    inactive_discontinued_coverage="deep, by construction",
    point_in_time_semantics="five price points per day, so the close can be "
                            "based on either last trade or settlement",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="contract identity and roll metadata",
    settlement_and_ohlc="settlement available as a distinct field",
    volume=True,
    open_interest=True,
    event_fields="roll and expiry",
    delivery_mechanism="one-off data dumps or a subscription; local files",
    sample_availability="none published without contact",
    sample_quality=UNKNOWN,
    licence_constraints=UNKNOWN,
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints=UNKNOWN,
    commercial_terms="quote required; marketed as far cheaper than a direct "
                     "CQG DataFactory purchase, which does not make it cheap",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=False,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="MEDIUM",
    estimated_storage_gb=60.0,
    data_engineering_complexity="MEDIUM to HIGH - bulk file ingestion and a "
                                "new identity layer",
    markets_covered=[],
    markets_partial=["CMDTY_PRECIOUS", "CMDTY_INDUSTRIAL", "CMDTY_GRAINS",
                     "CMDTY_SOFTS", "CMDTY_LIVESTOCK",
                     "RATES_TREASURY_FUTURES", "RATES_INTERNATIONAL",
                     "INTL_EQUITY_DEVELOPED"],
    future_research_lanes="pre-1980 commodity history, which no other "
                          "candidate offers",
    incremental_distinctness="HIGH, and the only candidate that adds history "
                             "BEFORE the recommendation's start",
    confidence_in_vendor_claims="MEDIUM",
    open_questions="what is the actual quote for ~40 daily markets with "
                   "settlement and open interest?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_BLOCKED_CONTACT,
    gate_reason="quote required; the natural time to ask is AFTER the USD 270 "
                "package has shown whether deeper history is the binding "
                "constraint",
)

CME_DATAMINE = _row(
    dataset_id="cme_datamine_end_of_day",
    history_years=30.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='STRONG',
    opacity_class='RAW_AS_PUBLISHED',
    provider="CME Group",
    dataset_name="DataMine historical end-of-day and settlement",
    lane=LANE_FUTURES,
    source_url="https://www.cmegroup.com/market-data/datamine-api.html",
    asset_classes=["COMMODITY", "RATES", "FX", "EQUITY_INDEX", "CRYPTO"],
    instruments="every CME Group listed contract",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="varies by product; deep for the flagship contracts",
    history_end="current",
    frequency="DAILY",
    breadth="CME Group only - no ICE, no Eurex, no SGX, no ASX",
    inactive_discontinued_coverage="complete for CME Group",
    point_in_time_semantics="the exchange's own settlement, which is the "
                            "definitive source",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="authoritative contract specifications",
    settlement_and_ohlc="authoritative settlement",
    volume=True,
    open_interest=True,
    event_fields="full contract specification",
    delivery_mechanism="DataMine API / cloud delivery",
    sample_availability="the free public settlement route was probed by this "
                        "release and answered HTTP 403",
    sample_quality="MEASURED: HTTP 403",
    licence_constraints="exchange market-data licence; derived-data and "
                        "internal-use terms are negotiated",
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints="strict",
    commercial_terms="quote required; exchange direct licensing",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=False,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="MEDIUM",
    estimated_storage_gb=20.0,
    data_engineering_complexity="MEDIUM",
    markets_covered=[],
    markets_partial=["CMDTY_PRECIOUS", "CMDTY_INDUSTRIAL", "CMDTY_GRAINS",
                     "CMDTY_SOFTS", "CMDTY_LIVESTOCK",
                     "RATES_TREASURY_FUTURES"],
    future_research_lanes="none the recommendation does not also open",
    incremental_distinctness="MEDIUM - authoritative, but single-exchange",
    confidence_in_vendor_claims="HIGH",
    open_questions="what is the licence cost for a single-user research use?",
    evidence=[C.EVIDENCE_VENDOR_PAGE, C.EVIDENCE_LIVE_PROBE],
    gate_state=C.STATE_BLOCKED_CONTACT,
    gate_reason="exchange direct licensing requires a commercial conversation, "
                "and covers one exchange group where the recommendation covers "
                "eleven",
)

CBOE_DATASHOP_VX = _row(
    dataset_id="cboe_datashop_vx_futures",
    history_years=22.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='STRONG',
    opacity_class='RAW_AS_PUBLISHED',
    provider="Cboe Global Markets",
    dataset_name="DataShop CFE VIX futures trades, quotes and settlements",
    lane=LANE_FUTURES,
    source_url="https://datashop.cboe.com/volatility-index-futures-data",
    asset_classes=["VOLATILITY"],
    instruments="Cboe VX dated futures",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="2004-04",
    history_end="current",
    frequency="DAILY_AND_INTRADAY",
    breadth="one market",
    inactive_discontinued_coverage="expired VX contracts retained",
    point_in_time_semantics="exchange settlement",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="expiry and contract identity",
    settlement_and_ohlc="settlement",
    volume=True,
    open_interest=True,
    event_fields="final settlement (VRO)",
    delivery_mechanism="DataShop per-product purchase",
    sample_availability="the free settlement route was re-probed by this "
                        "release and answered HTTP 403",
    sample_quality="MEASURED: HTTP 403 - Release 36's BLOCKED_LICENSING stands",
    licence_constraints="per-product licence; Cboe index licensing starts "
                        "around USD 1,000/month for index-linked uses",
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints="strict",
    commercial_terms="per-product purchase; price depends on the product and "
                     "the window",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=False,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="LOW",
    estimated_storage_gb=1.0,
    data_engineering_complexity="LOW",
    markets_covered=[],
    markets_partial=["VOL_VIX_FUTURES"],
    future_research_lanes="a native volatility term structure",
    incremental_distinctness=(
        "ZERO conditional on the recommendation, which carries Cboe VX from "
        "2004 inside a USD 270 package. This is the clearest case in the "
        "release of a purchase that is only rational if the cheaper one is "
        "refused"),
    confidence_in_vendor_claims="HIGH",
    open_questions="none that change the decision",
    evidence=[C.EVIDENCE_VENDOR_PAGE, C.EVIDENCE_LIVE_PROBE],
    gate_state=C.STATE_NO_LOW_VALUE,
    gate_reason="the four cells it would unlock are inside the recommendation "
                "already; buying both would be paying twice for one curve",
)

CBOE_CFE_VOLOI_FREE = _row(
    dataset_id="cboe_cfe_volume_open_interest_free",
    history_years=22.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='MODERATE',
    opacity_class='RAW_AS_PUBLISHED',
    provider="Cboe Futures Exchange",
    dataset_name="CFE daily volume and open interest by product",
    lane=LANE_FUTURES,
    source_url=C.CBOE_CFE_VOLOI_URL,
    asset_classes=["VOLATILITY"],
    instruments="every CFE product, VX included - as a daily TOTAL, not by "
                "expiry",
    implementation_level=C.LEVEL_SIGNAL,
    dated_contracts_available=False,
    history_start="2004-03-26",
    history_end="current",
    frequency="DAILY",
    breadth="one exchange",
    inactive_discontinued_coverage="products the exchange delisted remain as "
                                   "columns with values up to their last day",
    point_in_time_semantics="published daily by the exchange",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="product name only; there is NO expiry key",
    settlement_and_ohlc="NONE - this file carries no price",
    volume=True,
    open_interest=True,
    event_fields=None,
    delivery_mechanism="one public CSV over HTTPS, no account",
    sample_availability="the file itself is the sample",
    sample_quality=(
        "ACQUIRED AND VALIDATED: 5,639 rows from 2004-03-26, wide format, one "
        "row per date and one Volume/OI column pair per product. This release "
        "first recorded it as per-dated-contract on the strength of the "
        "exchange's page description; PARSING THE BYTES DISPROVED THAT, and "
        "the row was corrected. It is the only claim in the long list a sample "
        "actually overturned"),
    licence_constraints="published without an account; furnished without "
                        "warranty by the exchange",
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints="not redistributed by this estate",
    commercial_terms="free",
    monthly_cost_usd=0.0,
    annual_cost_usd=0.0,
    one_time_cost_usd=0.0,
    minimum_commitment="none",
    trial_required=False,
    account_required=False,
    credit_card_required=False,
    implementation_complexity="LOW",
    estimated_storage_gb=0.05,
    data_engineering_complexity="LOW",
    markets_covered=[],
    markets_partial=["VOL_VIX_FUTURES"],
    future_research_lanes="aggregate VX participation as a conditioning "
                          "variable; NOT per-contract term-structure "
                          "positioning, which this file cannot support",
    incremental_distinctness=(
        "LOW - genuinely new to this estate and free, and weaker than it first "
        "appeared. It carries no price and no expiry, so it can neither make "
        "the volatility curve native nor support a positioning study by "
        "contract"),
    confidence_in_vendor_claims="HIGH",
    open_questions="none - the sample answered them",
    evidence=[C.EVIDENCE_LIVE_PROBE, C.EVIDENCE_SAMPLE_VALIDATED],
    gate_state=C.STATE_FREE_ACQUIRE_NOW,
    gate_reason="free, public, 22 years of daily product-level volume and open "
                "interest, and Release 36 did not find it - worth having and "
                "worth far less than its page implies",
)

# --------------------------------------------------------------------------- #
# LANE B - analyst expectations, the strongest competing EQUITY purchase
# --------------------------------------------------------------------------- #
ZACKS_CONSENSUS = _row(
    dataset_id="zacks_consensus_history",
    history_years=47.0,
    pit_class='TRUE_POINT_IN_TIME',
    survivorship_class='UNKNOWN',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='MODERATE',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="Zacks Investment Research",
    dataset_name="Zacks consensus estimates and recommendations history",
    lane=LANE_ANALYST,
    source_url="https://zacksdata.com/datasets/consensus-data/",
    asset_classes=["US_EQUITY"],
    instruments="listed common stock ranked on revision",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=None,
    history_start="annual EPS consensus from 1979; quarterly EPS from 1982",
    history_end="current",
    frequency="DAILY_TO_WEEKLY_VINTAGES",
    breadth="broad US coverage",
    inactive_discontinued_coverage="stated as point-in-time and research-grade",
    point_in_time_semantics="TRUE_POINT_IN_TIME per the vendor",
    survivorship_property="UNKNOWN",
    identity_metadata="vendor identifiers requiring a mapping layer",
    settlement_and_ohlc=None,
    volume=None,
    open_interest=None,
    event_fields="EPS and revenue consensus, estimate changes, revision "
                 "breadth, dispersion, analyst count, announcement dates",
    delivery_mechanism="institutional delivery, or the ZACKS tables on Nasdaq "
                       "Data Link",
    sample_availability="the free Nasdaq Data Link key was re-probed by this "
                        "release and answered HTTP 403",
    sample_quality="MEASURED: HTTP 403, unchanged since Phase 12-A and "
                   "Release 35",
    licence_constraints="institutional licence",
    research_use_rights="RESEARCH_USE_CLEAR once licensed",
    redistribution_constraints="strict",
    commercial_terms="no published price; contact sales",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=True,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="HIGH",
    estimated_storage_gb=30.0,
    data_engineering_complexity=(
        "HIGH - a vintage-keyed panel, an identifier bridge to the owned CIK "
        "layer, and a delisted-issuer join that the estate has failed to build "
        "from every free source it has tried"),
    markets_covered=[],
    markets_partial=["US_EQUITY_ANALYST_EXPECTATIONS"],
    future_research_lanes=(
        "revision breadth and dispersion as a cross-sectional equity signal, "
        "as an event-driven signal around announcements, and as an input to "
        "sector rotation - the five families the Information Purchase Gate "
        "already requires a sample to be tested across"),
    incremental_distinctness=(
        "HIGH in principle - it is the one information family this estate has "
        "never legitimately held - and unproven in practice: Stage 13B found "
        "t = 2.27 on sales PEAD and Stage 13C found the out-of-sample result "
        "did not replicate at t = -0.29"),
    confidence_in_vendor_claims="MEDIUM",
    open_questions=(
        "what does a single-user research licence cost? what is the delisted "
        "and inactive issuer ratio? are the vintages daily or period-end?"),
    evidence=[C.EVIDENCE_VENDOR_PAGE, C.EVIDENCE_LIVE_PROBE,
              C.EVIDENCE_PRIOR_RELEASE],
    gate_state=C.STATE_BLOCKED_CONTACT,
    gate_reason=(
        "no published price and no obtainable sample without a sales "
        "conversation. It unlocks 3 cells where the futures recommendation "
        "unlocks 53, at a cost that is unknown but institutional, so it cannot "
        "rank first on any measurable basis"),
)

LSEG_IBES = _row(
    dataset_id="lseg_ibes_estimates",
    history_years=50.0,
    pit_class='TRUE_POINT_IN_TIME',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='STRONG',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="LSEG (Refinitiv)",
    dataset_name="I/B/E/S Estimates, consensus and detail",
    lane=LANE_ANALYST,
    source_url="https://www.lseg.com/en/data-analytics/financial-data/"
               "company-data/ibes-estimates",
    asset_classes=["US_EQUITY", "INTERNATIONAL_EQUITY"],
    instruments="global listed equity, 20+ forecast measures",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=None,
    history_start="1976 for US summary history",
    history_end="current",
    frequency="MONTHLY_AND_DAILY_VINTAGES",
    breadth="global, tens of thousands of issuers",
    inactive_discontinued_coverage="the academic product is explicitly "
                                   "research-grade with inactive coverage",
    point_in_time_semantics="TRUE_POINT_IN_TIME",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="strong, with its own identifier universe",
    settlement_and_ohlc=None,
    volume=None,
    open_interest=None,
    event_fields="EPS, revenue, EBITDA, price targets, recommendations",
    delivery_mechanism="enterprise feed, or via a university WRDS entitlement",
    sample_availability="none without an institutional relationship",
    sample_quality=UNKNOWN,
    licence_constraints="enterprise licence; academic access requires an "
                        "institutional affiliation this estate does not have",
    research_use_rights="RESEARCH_USE_CLEAR once licensed",
    redistribution_constraints="strict",
    commercial_terms="enterprise pricing; five figures a year is the normal "
                     "entry point",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment="annual enterprise contract",
    trial_required=True,
    account_required=True,
    credit_card_required=False,
    implementation_complexity="HIGH",
    estimated_storage_gb=100.0,
    data_engineering_complexity="HIGH",
    markets_covered=[],
    markets_partial=["US_EQUITY_ANALYST_EXPECTATIONS"],
    future_research_lanes="the same three cells, plus international equity "
                          "expectations if the index-future entitlement ever "
                          "arrives",
    incremental_distinctness="HIGH",
    confidence_in_vendor_claims="HIGH",
    open_questions="is there any single-user tier at all?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_NO_COST_VALUE,
    gate_reason=(
        "the gold standard for this data, priced for institutions. Three "
        "unlocked cells against an enterprise contract fails cost/value at "
        "this estate's scale by any arithmetic"),
)

FREE_ANALYST_TIERS = _row(
    dataset_id="free_analyst_estimate_tiers",
    history_years=0.0,
    pit_class='CURRENT_SNAPSHOT_ONLY',
    survivorship_class='SURVIVORS_ONLY',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='WEAK',
    opacity_class='OPAQUE_VENDOR_TRANSFORM',
    provider="FMP, Finnhub, Nasdaq Data Link, EODHD, Alpha Vantage",
    dataset_name="Free-tier analyst estimate endpoints",
    lane=LANE_ANALYST,
    source_url="https://site.financialmodelingprep.com/developer/docs",
    asset_classes=["US_EQUITY"],
    instruments="listed common stock",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=None,
    history_start="NONE - today's estimate plus 30/60/90-day deltas",
    history_end="today",
    frequency="SNAPSHOT",
    breadth="current members only",
    inactive_discontinued_coverage="NONE",
    point_in_time_semantics="CURRENT_SNAPSHOT_ONLY",
    survivorship_property="SURVIVORS_ONLY",
    identity_metadata="ticker only",
    settlement_and_ohlc=None,
    volume=None,
    open_interest=None,
    event_fields="current consensus and recent deltas",
    delivery_mechanism="REST",
    sample_availability="the estate holds keys for all five",
    sample_quality="MEASURED by Release 35: three answer HTTP 403, two answer "
                   "with today's estimate plus deltas",
    licence_constraints="free tier",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="no redistribution",
    commercial_terms="free, or paid tiers that were not measured to add "
                     "vintages",
    monthly_cost_usd=0.0,
    annual_cost_usd=0.0,
    one_time_cost_usd=0.0,
    minimum_commitment="none",
    trial_required=False,
    account_required=False,
    credit_card_required=False,
    implementation_complexity="LOW",
    estimated_storage_gb=0.1,
    data_engineering_complexity="LOW",
    markets_covered=[],
    markets_partial=[],
    future_research_lanes="none",
    incremental_distinctness="ZERO",
    confidence_in_vendor_claims="HIGH",
    open_questions="none",
    evidence=[C.EVIDENCE_PRIOR_RELEASE],
    gate_state=C.STATE_NO_PIT,
    gate_reason=(
        "a current consensus is not a historical consensus, and writing one "
        "backwards is a prohibited substitution. This row exists so the "
        "question is recorded as settled rather than re-asked in Release 38"),
)

INTRINIO_PRIOR = _row(
    dataset_id="intrinio_analyst_estimates",
    history_years=0.0,
    pit_class='UNKNOWN',
    survivorship_class='SURVIVORS_ONLY',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='MODERATE',
    opacity_class='OPAQUE_VENDOR_TRANSFORM',
    provider="Intrinio",
    dataset_name="Analyst estimates and revisions",
    lane=LANE_ANALYST,
    source_url="https://intrinio.com/data/analyst-estimates",
    asset_classes=["US_EQUITY"],
    instruments="listed common stock",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=None,
    history_start="trial extract only",
    history_end="trial extract only",
    frequency="DAILY",
    breadth="current-members universe in the trial extract",
    inactive_discontinued_coverage="failed a survivorship-safe 16-year test",
    point_in_time_semantics="UNKNOWN for history",
    survivorship_property="SURVIVORS_ONLY",
    identity_metadata="ticker and vendor id",
    settlement_and_ohlc=None,
    volume=None,
    open_interest=None,
    event_fields="estimates and revisions",
    delivery_mechanism="REST",
    sample_availability="a live trial was already run by this estate",
    sample_quality="MEASURED: NO_DEFENSIBLE_ALPHA, DO_NOT_BUY",
    licence_constraints="subscription",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="no redistribution",
    commercial_terms="subscription; previously evaluated",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=True,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="MEDIUM",
    estimated_storage_gb=5.0,
    data_engineering_complexity="MEDIUM",
    markets_covered=[],
    markets_partial=[],
    future_research_lanes="none",
    incremental_distinctness="ZERO on the evidence already gathered",
    confidence_in_vendor_claims="HIGH",
    open_questions="none - this was tested with real money's worth of effort "
                   "and answered",
    evidence=[C.EVIDENCE_PRIOR_RELEASE],
    gate_state=C.STATE_NO_LOW_VALUE,
    gate_reason="a live trial already returned NO_DEFENSIBLE_ALPHA and the "
                "R32 gate already recorded EVALUATED_DO_NOT_BUY; re-opening a "
                "settled question is not new information",
)

# --------------------------------------------------------------------------- #
# LANE C - options and volatility
# --------------------------------------------------------------------------- #
ORATS_OPTIONS = _row(
    dataset_id="orats_options_history",
    history_years=19.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='STRONG',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="ORATS",
    dataset_name="Historical end-of-day options with greeks and a smoothed "
                 "implied-volatility surface",
    lane=LANE_OPTIONS,
    source_url="https://orats.com/data-api",
    asset_classes=["VOLATILITY", "US_EQUITY"],
    instruments="US listed option chains, every strike and expiration",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="2007",
    history_end="current",
    frequency="DAILY_AND_INTRADAY",
    breadth="the whole US options universe",
    inactive_discontinued_coverage="expired strikes and expirations retained",
    point_in_time_semantics="end-of-day snapshots as observed",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="OCC-style option identity",
    settlement_and_ohlc="option marks and greeks",
    volume=True,
    open_interest=True,
    event_fields="earnings dates and dividend forecasts",
    delivery_mechanism="REST API and bulk files",
    sample_availability="documented API with sample responses; a real extract "
                        "requires a subscription",
    sample_quality="NOT MEASURED",
    licence_constraints="subscription; no price published on the fetched page",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="no redistribution",
    commercial_terms="subscription; quote or checkout required",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=True,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="HIGH",
    estimated_storage_gb=500.0,
    data_engineering_complexity=(
        "HIGH - a surface is a four-dimensional object and every research "
        "question on it needs an interpolation convention this estate would "
        "have to own and defend"),
    markets_covered=[],
    markets_partial=["VOL_OPTIONS_SURFACE"],
    future_research_lanes="variance risk premium, skew, dispersion, and an "
                          "options-implied input to the equity cross-section",
    incremental_distinctness="HIGH - the estate holds no option data of any "
                             "kind",
    confidence_in_vendor_claims="MEDIUM",
    open_questions="what does the historical bulk tier cost, and does it "
                   "include SPX index options rather than only equities?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_SAMPLE_REQUIRED,
    gate_reason=(
        "a credible mechanism and a real gap, but three unlocked cells, half a "
        "terabyte of storage against 6.8 GB free on the system drive, and the "
        "highest data-engineering cost of any candidate. It needs a sample and "
        "a price before it can be ranked, and neither is obtainable without an "
        "account"),
)

OPTIONMETRICS_IVYDB = _row(
    dataset_id="optionmetrics_ivydb_us",
    history_years=30.0,
    pit_class='TRUE_POINT_IN_TIME',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='STRONG',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="OptionMetrics",
    dataset_name="IvyDB US - historical option prices and volatility surfaces",
    lane=LANE_OPTIONS,
    source_url="https://optionmetrics.com/products/ivy-db-us/",
    asset_classes=["VOLATILITY", "US_EQUITY"],
    instruments="US listed options including SPX index options",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="1996",
    history_end="current",
    frequency="DAILY",
    breadth="the reference dataset for academic option research",
    inactive_discontinued_coverage="complete",
    point_in_time_semantics="TRUE_POINT_IN_TIME",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="strong",
    settlement_and_ohlc="closing bid/ask, greeks, standardised surfaces",
    volume=True,
    open_interest=True,
    event_fields="dividends and corporate events",
    delivery_mechanism="bulk files or a WRDS entitlement",
    sample_availability="none without an institutional relationship",
    sample_quality=UNKNOWN,
    licence_constraints="institutional",
    research_use_rights="RESEARCH_USE_CLEAR once licensed",
    redistribution_constraints="strict",
    commercial_terms="institutional pricing; quote required",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment="annual",
    trial_required=True,
    account_required=True,
    credit_card_required=False,
    implementation_complexity="HIGH",
    estimated_storage_gb=400.0,
    data_engineering_complexity="HIGH",
    markets_covered=[],
    markets_partial=["VOL_OPTIONS_SURFACE"],
    future_research_lanes="the same as ORATS, with a longer history",
    incremental_distinctness="HIGH",
    confidence_in_vendor_claims="HIGH",
    open_questions="is there any non-institutional tier?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_BLOCKED_CONTACT,
    gate_reason="institutional-only licensing with no published price and no "
                "obtainable sample",
)

# --------------------------------------------------------------------------- #
# LANE D - credit
# --------------------------------------------------------------------------- #
TRACE_HISTORICAL = _row(
    dataset_id="finra_trace_historical",
    history_years=24.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='MODERATE',
    opacity_class='RAW_AS_PUBLISHED',
    provider="FINRA",
    dataset_name="TRACE historical corporate bond transactions (academic / "
                 "enhanced)",
    lane=LANE_CREDIT,
    source_url="https://www.finra.org/finra-data/browse-catalog/trace",
    asset_classes=["CREDIT"],
    instruments="corporate bond transactions at CUSIP level",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=None,
    history_start="2002",
    history_end="current, with an 18-month dissemination lag on the enhanced "
                "file",
    frequency="TRANSACTION",
    breadth="the whole reported US corporate bond market",
    inactive_discontinued_coverage="matured and defaulted issues present",
    point_in_time_semantics="OBSERVED_AS_PUBLISHED",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="CUSIP; a bond-to-issuer bridge would have to be built",
    settlement_and_ohlc="transaction prices, not a curve",
    volume=True,
    open_interest=False,
    event_fields="trade side and size",
    delivery_mechanism="FINRA data purchase or a WRDS entitlement",
    sample_availability="none free at the historical tier",
    sample_quality=UNKNOWN,
    licence_constraints="FINRA data licence",
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints="strict",
    commercial_terms="quote required; academic tier requires an institution",
    monthly_cost_usd=None,
    annual_cost_usd=None,
    one_time_cost_usd=None,
    minimum_commitment=UNKNOWN,
    trial_required=False,
    account_required=True,
    credit_card_required=True,
    implementation_complexity="HIGH",
    estimated_storage_gb=150.0,
    data_engineering_complexity=(
        "VERY HIGH - transactions are not a spread curve. Building one needs a "
        "reference database of terms and conditions the estate does not have "
        "and this candidate does not include"),
    markets_covered=[],
    markets_partial=["CREDIT_SINGLE_NAME"],
    future_research_lanes="capital-structure relative value, credit-equity "
                          "lead-lag, liquidity premium",
    incremental_distinctness="HIGH",
    confidence_in_vendor_claims="HIGH",
    open_questions="what is the price, and is a bond reference database "
                   "included or a second purchase?",
    evidence=[C.EVIDENCE_VENDOR_PAGE],
    gate_state=C.STATE_BLOCKED_CONTACT,
    gate_reason=(
        "quote required, and it is the only candidate whose data would need a "
        "SECOND purchase - a bond reference database - before a single "
        "research question could be asked"),
)

# --------------------------------------------------------------------------- #
# LANE E - crypto
# --------------------------------------------------------------------------- #
BINANCE_PUBLIC_ARCHIVE = _row(
    dataset_id="binance_public_data_archive",
    history_years=7.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='SURVIVORS_ONLY',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='WEAK',
    opacity_class='RAW_AS_PUBLISHED',
    provider="Binance",
    dataset_name="Public data archive - perpetual funding rates and klines",
    lane=LANE_CRYPTO,
    source_url="https://data.binance.vision/",
    asset_classes=["CRYPTO"],
    instruments="perpetual and dated futures on the venue's own listings",
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=True,
    history_start="2019 for USD-margined perpetuals",
    history_end="current",
    frequency="DAILY_AND_INTRADAY",
    breadth="hundreds of currently-listed symbols",
    inactive_discontinued_coverage=(
        "the archive is organised by CURRENTLY LISTED symbol. A symbol the "
        "venue removed is not enumerable, so the listing and delisting record "
        "cannot be reconstructed"),
    point_in_time_semantics="published as observed",
    survivorship_property="SURVIVORS_ONLY",
    identity_metadata="venue symbol only",
    settlement_and_ohlc="mark and index prices, funding rates",
    volume=True,
    open_interest=True,
    event_fields="funding intervals",
    delivery_mechanism="public HTTPS archive, no account",
    sample_availability="the archive itself; one monthly file was probed by "
                        "this release and answered HTTP 200",
    sample_quality="MEASURED: HTTP 200 on a 2024 BTCUSDT funding-rate archive",
    licence_constraints="published without an account",
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints="not redistributed by this estate",
    commercial_terms="free",
    monthly_cost_usd=0.0,
    annual_cost_usd=0.0,
    one_time_cost_usd=0.0,
    minimum_commitment="none",
    trial_required=False,
    account_required=False,
    credit_card_required=False,
    implementation_complexity="LOW",
    estimated_storage_gb=5.0,
    data_engineering_complexity="LOW",
    markets_covered=[],
    markets_partial=[],
    future_research_lanes="funding and basis for the two majors already "
                          "admitted, and nothing wider",
    incremental_distinctness=(
        "LOW for the blocked cell. Release 36 blocked CRYPTO_BASIS_FUNDING on "
        "POINT_IN_TIME grounds precisely because a venue's own listing record "
        "is not retrievable, and a free archive of the survivors does not "
        "answer that objection - it is the objection"),
    confidence_in_vendor_claims="MEDIUM",
    open_questions="is there any venue that publishes a delisting record?",
    evidence=[C.EVIDENCE_LIVE_PROBE],
    gate_state=C.STATE_NO_SURVIVORSHIP,
    gate_reason=(
        "free and real, and it does not clear the block. Acquiring it would "
        "let a broad crypto cross-section be built out of survivors, which is "
        "the exact construction Release 36 refused"),
)

# --------------------------------------------------------------------------- #
# LANE F - other orthogonal, measured this release
# --------------------------------------------------------------------------- #
LBMA_BENCHMARKS = _row(
    dataset_id="lbma_precious_metal_benchmarks",
    history_years=58.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='NOT_APPLICABLE',
    licence_class='RESEARCH_USE_AMBIGUOUS',
    identity_class='MODERATE',
    opacity_class='RAW_AS_PUBLISHED',
    provider="London Bullion Market Association",
    dataset_name="LBMA gold, silver and platinum benchmark prices",
    lane=LANE_OTHER,
    source_url=C.LBMA_GOLD_PM_URL,
    asset_classes=["COMMODITY"],
    instruments="the daily benchmark fixing, in USD, GBP and EUR",
    implementation_level=C.LEVEL_SIGNAL,
    dated_contracts_available=False,
    history_start="gold and silver 1968; platinum 1990",
    history_end="current",
    frequency="DAILY",
    breadth="three metals",
    inactive_discontinued_coverage="not applicable - a benchmark does not "
                                   "expire",
    point_in_time_semantics="published daily as the fixing",
    survivorship_property="NOT_APPLICABLE",
    identity_metadata="metal and currency",
    settlement_and_ohlc="a single fixing price; NOT a futures settlement",
    volume=False,
    open_interest=False,
    event_fields=None,
    delivery_mechanism="public JSON over HTTPS, no account",
    sample_availability="the file itself",
    sample_quality="ACQUIRED AND VALIDATED by this release; gold from "
                   "1968-04-01, silver from 1968-01-02",
    licence_constraints="published freely for information",
    research_use_rights="RESEARCH_USE_AMBIGUOUS",
    redistribution_constraints="not redistributed by this estate",
    commercial_terms="free",
    monthly_cost_usd=0.0,
    annual_cost_usd=0.0,
    one_time_cost_usd=0.0,
    minimum_commitment="none",
    trial_required=False,
    account_required=False,
    credit_card_required=False,
    implementation_complexity="LOW",
    estimated_storage_gb=0.02,
    data_engineering_complexity="LOW",
    markets_covered=[],
    markets_partial=[],
    future_research_lanes=(
        "a 58-year metals price series to test a curve signal AGAINST once "
        "dated contracts exist, and a control for the metals lane"),
    incremental_distinctness=(
        "LOW for the blocked cells and genuinely useful as a control. A fixing "
        "is a LEVEL 1 SIGNAL: it cannot be held, it has no roll and no carry, "
        "so it cannot close a single metals cell. Recording it as if it could "
        "would be the exact substitution Release 36 spent a release refusing"),
    confidence_in_vendor_claims="HIGH",
    open_questions="none",
    evidence=[C.EVIDENCE_LIVE_PROBE, C.EVIDENCE_SAMPLE_VALIDATED],
    gate_state=C.STATE_FREE_ACQUIRE_NOW,
    gate_reason="free, long, real, and honestly labelled as a signal rather "
                "than an instrument",
)

NYFED_POSITIONING = _row(
    dataset_id="nyfed_primary_dealer_positions",
    history_years=28.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='NOT_APPLICABLE',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='MODERATE',
    opacity_class='RAW_AS_PUBLISHED',
    provider="Federal Reserve Bank of New York",
    dataset_name="Primary dealer positions and financing",
    lane=LANE_OTHER,
    source_url=C.NYFED_PRIMARY_DEALER_URL,
    asset_classes=["RATES"],
    instruments="dealer net positions by Treasury sector and by maturity",
    implementation_level=C.LEVEL_SIGNAL,
    dated_contracts_available=False,
    history_start="1998 for the legacy series; 2013 for the current schema",
    history_end="current, weekly with a publication lag",
    frequency="WEEKLY",
    breadth="the whole primary-dealer community",
    inactive_discontinued_coverage="series breaks are published, not hidden",
    point_in_time_semantics="published with a stated lag, so an as-of rule is "
                            "constructible",
    survivorship_property="NOT_APPLICABLE",
    identity_metadata="series code",
    settlement_and_ohlc=None,
    volume=False,
    open_interest=True,
    event_fields=None,
    delivery_mechanism="public CSV API, no account",
    sample_availability="the file itself",
    sample_quality="ACQUIRED AND VALIDATED by this release",
    licence_constraints="US government publication",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="none",
    commercial_terms="free",
    monthly_cost_usd=0.0,
    annual_cost_usd=0.0,
    one_time_cost_usd=0.0,
    minimum_commitment="none",
    trial_required=False,
    account_required=False,
    credit_card_required=False,
    implementation_complexity="LOW",
    estimated_storage_gb=0.05,
    data_engineering_complexity="LOW",
    markets_covered=[],
    markets_partial=[],
    future_research_lanes=(
        "a POSITIONING feature for the rates curve, which becomes testable the "
        "moment Treasury futures exist - it is the rates analogue of the COT "
        "report Release 35 already owns for commodities"),
    incremental_distinctness="MEDIUM, and CONDITIONAL: it is worth having "
                             "because the recommended purchase makes it usable",
    confidence_in_vendor_claims="HIGH",
    open_questions="none",
    evidence=[C.EVIDENCE_LIVE_PROBE, C.EVIDENCE_SAMPLE_VALIDATED],
    gate_state=C.STATE_FREE_ACQUIRE_NOW,
    gate_reason="free, official, point-in-time-stampable, and complementary to "
                "the recommended purchase",
)

NORGATE_OWNED = _row(
    dataset_id="norgate_owned_entitlement",
    history_years=98.0,
    pit_class='OBSERVED_AS_PUBLISHED',
    survivorship_class='DISCONTINUED_RETAINED',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='STRONG',
    opacity_class='DOCUMENTED_TRANSFORM',
    provider="Norgate Data",
    dataset_name="The eight databases this estate already pays for",
    lane=LANE_OTHER,
    source_url="https://norgatedata.com/",
    asset_classes=["US_EQUITY", "FX", "RATES", "COMMODITY", "EQUITY_INDEX"],
    instruments=("US Equities 14,639 live and 27,194 delisted; US Indices "
                 "1,609; World Indices 31; Forex Spot 57; Economic 144; Cash "
                 "Commodities 15; Continuous Futures 1"),
    implementation_level=C.LEVEL_NATIVE,
    dated_contracts_available=False,
    history_start="1928 for US equities",
    history_end="current",
    frequency="DAILY",
    breadth="measured live by this release",
    inactive_discontinued_coverage="DELISTED_RETAINED for US equities",
    point_in_time_semantics="TOTAL_RETURN_ADJUSTED with historical index "
                            "constituents",
    survivorship_property="DISCONTINUED_RETAINED",
    identity_metadata="strong",
    settlement_and_ohlc="OHLC",
    volume=True,
    open_interest=False,
    event_fields="capital events, dividends, index membership",
    delivery_mechanism="local database, norgatedata client",
    sample_availability="owned",
    sample_quality=("MEASURED: 8 databases enumerated live; Continuous Futures "
                    "returns exactly ONE symbol, &ES, which independently "
                    "reconfirms the Release-33 and Release-36 finding"),
    licence_constraints="existing single-user subscription",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="no redistribution",
    commercial_terms="already paid",
    monthly_cost_usd=0.0,
    annual_cost_usd=0.0,
    one_time_cost_usd=0.0,
    minimum_commitment="already committed",
    trial_required=False,
    account_required=False,
    credit_card_required=False,
    implementation_complexity="NONE",
    estimated_storage_gb=0.0,
    data_engineering_complexity="NONE",
    markets_covered=[],
    markets_partial=[],
    future_research_lanes="none new",
    incremental_distinctness="ZERO - it is the baseline",
    confidence_in_vendor_claims="HIGH",
    open_questions="which subscription tier is held, and does the futures "
                   "package attach to it as an add-on?",
    evidence=[C.EVIDENCE_LOCAL_CLIENT],
    gate_state=C.STATE_ENTITLEMENT_OWNED,
    gate_reason="already owned and already used; recorded so the baseline the "
                "purchase is measured against is explicit",
)

USDA_QUICKSTATS = _row(
    dataset_id="usda_nass_quickstats",
    history_years=60.0,
    pit_class='REVISED_HISTORY',
    survivorship_class='NOT_APPLICABLE',
    licence_class='RESEARCH_USE_CLEAR',
    identity_class='MODERATE',
    opacity_class='RAW_AS_PUBLISHED',
    provider="US Department of Agriculture",
    dataset_name="NASS Quick Stats agricultural supply and demand",
    lane=LANE_OTHER,
    source_url="https://quickstats.nass.usda.gov/api",
    asset_classes=["COMMODITY"],
    instruments="acreage, yield, production, stocks by crop",
    implementation_level=C.LEVEL_SIGNAL,
    dated_contracts_available=False,
    history_start="1866 for some series",
    history_end="current",
    frequency="MONTHLY_AND_ANNUAL",
    breadth="all major US crops and livestock",
    inactive_discontinued_coverage="not applicable",
    point_in_time_semantics="published with release dates; revisions exist and "
                            "would need vintage handling",
    survivorship_property="NOT_APPLICABLE",
    identity_metadata="commodity and statistic codes",
    settlement_and_ohlc=None,
    volume=False,
    open_interest=False,
    event_fields="report release dates",
    delivery_mechanism="REST API requiring a FREE registered key",
    sample_availability="free key registration by email",
    sample_quality="MEASURED: HTTP 401 without a key",
    licence_constraints="US government publication",
    research_use_rights="RESEARCH_USE_CLEAR",
    redistribution_constraints="none",
    commercial_terms="free, key required",
    monthly_cost_usd=0.0,
    annual_cost_usd=0.0,
    one_time_cost_usd=0.0,
    minimum_commitment="none",
    trial_required=False,
    account_required=True,
    credit_card_required=False,
    implementation_complexity="LOW",
    estimated_storage_gb=2.0,
    data_engineering_complexity="MEDIUM - revisions need vintage handling",
    markets_covered=[],
    markets_partial=[],
    future_research_lanes=(
        "the FUNDAMENTAL_SUPPLY_DEMAND family for grains, which is one of the "
        "nine cells the recommended purchase opens and the only one that needs "
        "a second source to be answered properly"),
    incremental_distinctness="MEDIUM, and CONDITIONAL on the grains "
                             "entitlement existing",
    confidence_in_vendor_claims="HIGH",
    open_questions="none",
    evidence=[C.EVIDENCE_LIVE_PROBE],
    gate_state=C.STATE_SAMPLE_REQUIRED,
    gate_reason=(
        "free but key-gated. Registering a key is a HUMAN action - it submits "
        "an email address to a third party - so it is recorded as an operator "
        "action rather than performed"),
)

# --------------------------------------------------------------------------- #
# The long list
# --------------------------------------------------------------------------- #
DATASETS = {
    r["dataset_id"]: r for r in (
        NORGATE_FUTURES, DATABENTO_GLBX, FIRSTRATE_FUTURES,
        CSI_UNFAIR_ADVANTAGE, PORTARA_CQG, CME_DATAMINE, CBOE_DATASHOP_VX,
        CBOE_CFE_VOLOI_FREE,
        ZACKS_CONSENSUS, LSEG_IBES, FREE_ANALYST_TIERS, INTRINIO_PRIOR,
        ORATS_OPTIONS, OPTIONMETRICS_IVYDB,
        TRACE_HISTORICAL,
        BINANCE_PUBLIC_ARCHIVE,
        LBMA_BENCHMARKS, NYFED_POSITIONING, NORGATE_OWNED, USDA_QUICKSTATS,
    )
}


def rows() -> list:
    """Every scorecard row, ordered by lane then dataset id."""
    order = {lane: i for i, lane in enumerate(LANES)}
    return sorted((dict(r) for r in DATASETS.values()),
                  key=lambda r: (order.get(r["lane"], 99), r["dataset_id"]))


def by_lane() -> dict:
    out = {lane: [] for lane in LANES}
    for row in rows():
        out[row["lane"]].append(row["dataset_id"])
    return out


def validate() -> dict:
    """Structural proof that the long list obeys its own contract.

    Three failure modes, each of which has actually happened somewhere in this
    repository's history: a row that quietly drops a field, a state outside the
    frozen vocabulary, and a row that claims a market without naming the
    evidence class that established it.
    """
    vocab = {"pit_class": C.PIT_FACTOR,
             "survivorship_class": C.SURVIVORSHIP_FACTOR,
             "licence_class": C.LICENCE_FACTOR,
             "identity_class": C.IDENTITY_FACTOR,
             "opacity_class": C.OPACITY_FACTOR}
    missing_fields, bad_states, unevidenced, no_reason = [], [], [], []
    bad_classes = []
    for row in rows():
        for field in SCORECARD_FIELDS:
            if field not in row:
                missing_fields.append("%s::%s" % (row["dataset_id"], field))
        for field, allowed in vocab.items():
            value = row.get(field)
            if value is None or value not in allowed:
                bad_classes.append("%s::%s=%s" % (row["dataset_id"], field,
                                                  value))
        if row.get("history_years") is None:
            missing_fields.append("%s::history_years" % row["dataset_id"])
        if row["gate_state"] not in C.GATE_STATES:
            bad_states.append("%s::%s" % (row["dataset_id"], row["gate_state"]))
        if not row.get("evidence"):
            unevidenced.append(row["dataset_id"])
        for ev in (row.get("evidence") or []):
            if ev not in C.EVIDENCE_CLASSES:
                bad_states.append("%s::EVIDENCE::%s" % (row["dataset_id"], ev))
        if (row.get("markets_covered") or row.get("markets_partial")) \
                and not row.get("evidence"):
            unevidenced.append(row["dataset_id"])
        if not row.get("gate_reason"):
            no_reason.append(row["dataset_id"])
    return {"n_rows": len(DATASETS),
            "lanes": by_lane(),
            "classification_fields": list(CLASSIFICATION_FIELDS),
            "missing_fields": sorted(missing_fields),
            "states_outside_vocabulary": sorted(set(bad_states)),
            "classifications_outside_vocabulary": sorted(set(bad_classes)),
            "rows_without_evidence": sorted(set(unevidenced)),
            "rows_without_a_reason": sorted(set(no_reason)),
            "valid": not (missing_fields or bad_states or bad_classes
                          or unevidenced or no_reason)}


def artifact(*, campaign_id: str, created_at: str) -> dict:
    checked = validate()
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "scorecard_fields": list(SCORECARD_FIELDS),
        "lanes": list(LANES),
        "rows": rows(),
        "n_datasets": len(DATASETS),
        "by_lane": by_lane(),
        "validation": checked,
        "a_marketing_claim_is_not_a_measurement":
            C.A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT,
        "money_spent_usd": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
        "subscriptions_changed": 0,
    }
    return r37.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r37.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "SCHEMA", "SCORECARD_SCHEMA", "LANES",
           "LANE_FUTURES", "LANE_ANALYST", "LANE_OPTIONS", "LANE_CREDIT",
           "LANE_CRYPTO", "LANE_OTHER", "SCORECARD_FIELDS",
           "CLASSIFICATION_FIELDS", "DATASETS",
           "rows", "by_lane", "validate", "artifact", "freeze", "load",
           "path_for"]
