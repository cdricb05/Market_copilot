"""alpha_agent.r35.contract - the ONE Release 35 campaign contract.

Everything the campaign is NOT allowed to decide after seeing a result lives
here: the source families, the acquisition endpoints, the instrument mappings,
the point-in-time publication rules, the feature transforms, the orthogonality
thresholds, the increment gates, the frozen conversion configuration, the
multiple-testing denominator, the evidence state and the verdict vocabulary.

The reason this file is long and the campaign is short: R34 measured that the
binding constraint is information, so the ONLY degree of freedom Release 35 is
permitted is *which information the model may see*. Model architecture, panel,
universe, partition, calibration, sizing, horizon combination, turnover rule and
portfolio construction are all IMPORTED FROZEN. If a number moves between the
two arms of a comparison, information moved it, because nothing else could.

Three declarations are worth reading before anything else:

``FRESH_UNSEEN_EVIDENCE_EXISTS = False``
    R31, R32, R33 and R34 all selected on market outcomes through August 2026.
    Newly acquired FEATURES do not make an already-consumed OUTCOME period
    fresh. A historically strong Release-35 result can therefore establish a
    RESEARCH CANDIDATE and can never, in this release, establish Alpha.

``ALPHA_PASS_REQUIRES = VERDICT_QUALIFIED``
    and ``VERDICT_QUALIFIED`` requires ``genuinely_independent_evidence_exists``,
    which is False by the line above. ``ALPHA_RESULT = PASS`` is structurally
    unreachable here, stated in advance rather than discovered afterwards.

``ORTHOGONALITY_IS_A_GATE = True``
    A source is not new because its raw correlation with a base feature is low.
    It is new to the extent that its variance SURVIVES a regression on the whole
    base information set, and it is useful only if that surviving part predicts.
"""
from __future__ import annotations

from typing import Optional

from .. import r35
from ..r31 import sha

CALCULATION_OWNER = "alpha_agent.r35.contract"
CONTRACT_SCHEMA = "r35_orthogonal_information_contract/1"
ARTIFACT_NAME = "research_contract.json"

CAMPAIGN_ID = "r35_orthogonal_information_v1"

# --------------------------------------------------------------------------- #
# What is FROZEN, and from where
# --------------------------------------------------------------------------- #
#: The base information set is Release 33's feature registry as Release 34 used
#: it: 28 declared, economically interpretable price / trend / risk-state
#: features on Release 34's implementable ETF universe. R35 never edits it.
BASE_INFORMATION_SET = "R34_FROZEN"
BASE_FEATURE_OWNER = "alpha_agent.r33.features"
BASE_MODEL_OWNER = "alpha_agent.r33.models"
BASE_UNIVERSE_OWNER = "alpha_agent.r34.universe"
BASE_PANEL_OWNER = "alpha_agent.r34.panel"
BASE_FORECAST_OWNER = "alpha_agent.r34.forecast"
BASE_WALKFORWARD_OWNER = "alpha_agent.r34.walkforward"
BASE_ECONOMICS_OWNER = "alpha_agent.r34.economics"

#: No new predictor search, no adaptive grid growth, no second learner library.
NEW_PREDICTOR_SEARCH_ALLOWED = False
ADAPTIVE_SEARCH_ALLOWED = False
MODEL_ARCHITECTURE_SEARCH_ALLOWED = False
CONVERSION_LAYER_SEARCH_ALLOWED = False

#: R34's winning conversion configuration, copied here as data and never tuned.
#: Every economic evaluation in this release - base arm and every candidate arm -
#: runs through exactly this configuration, so an economic difference cannot be
#: a conversion difference.
FROZEN_CONVERSION = {
    "calibration": "BAYESIAN_SHRINKAGE_TO_ZERO",
    "sizing": "EXPECTED_RETURN_OVER_PREDICTED_VARIANCE",
    "horizons": (5, 20),
    "weighting": "EQUAL_WEIGHT_A_PRIORI",
    "turnover": "TURNOVER_PENALISED_TARGET",
    "turnover_param": 5.0,
    "portfolio": "SHRUNK_MEAN_VARIANCE",
    "primary_horizon": 20,
    "source": "r34_prediction_to_pnl_v2::FINALIST::COMBINED_BEST",
}

#: Horizons the predictive increment is measured at. R34's own three.
HORIZONS = (5, 20, 60)
PRIMARY_HORIZON = 20

# --------------------------------------------------------------------------- #
# Money
# --------------------------------------------------------------------------- #
MAY_SPEND_MONEY = False
MAY_START_PROVIDER_TRIAL = False
MAY_CREATE_PROVIDER_ACCOUNT = False
#: Free and public acquisition IS authorised, and is the point of the release.
MAY_ACQUIRE_FREE_PUBLIC_DATA = True
#: An already-paid entitlement may be read. A new one may not be bought.
MAY_USE_EXISTING_ENTITLEMENTS = True

# --------------------------------------------------------------------------- #
# Source families
# --------------------------------------------------------------------------- #
FAM_POSITIONING = "FUTURES_POSITIONING"
FAM_FX_CARRY = "FX_INTEREST_CARRY"
FAM_COMMODITY_CURVE = "COMMODITY_TERM_STRUCTURE"
FAM_IV_TERM = "IMPLIED_VOLATILITY_TERM_STRUCTURE"
FAM_RISK_PREMIA = "MARKET_IMPLIED_RISK_PREMIA"
FAM_INSIDER = "INSIDER_TRANSACTION_INTENSITY"
FAM_ANALYST = "ANALYST_EXPECTATION_CHANGE"

#: The families whose data this release actually acquires and tests.
ACQUIRED_FAMILIES = (FAM_POSITIONING, FAM_FX_CARRY, FAM_COMMODITY_CURVE,
                     FAM_IV_TERM, FAM_RISK_PREMIA, FAM_INSIDER)
#: Declared, ranked, and NOT acquirable at zero cost. Recorded, never faked.
BLOCKED_FAMILIES = (FAM_ANALYST,)
ALL_FAMILIES = ACQUIRED_FAMILIES + BLOCKED_FAMILIES

#: Lane assignment, so the write-up and the artifacts agree on which commissioned
#: lane each family answers.
LANE_OF_FAMILY = {
    FAM_ANALYST: "A_ANALYST_EXPECTATION_CHANGE",
    FAM_POSITIONING: "B_MARKET_IMPLIED_AND_CARRY",
    FAM_FX_CARRY: "B_MARKET_IMPLIED_AND_CARRY",
    FAM_COMMODITY_CURVE: "B_MARKET_IMPLIED_AND_CARRY",
    FAM_IV_TERM: "B_MARKET_IMPLIED_AND_CARRY",
    FAM_RISK_PREMIA: "B_MARKET_IMPLIED_AND_CARRY",
    FAM_INSIDER: "C_CORPORATE_EVENT_INFORMATION",
}

#: Why each family is economically distinct from the base set. These are claims
#: the orthogonality measurement then has to support; a family whose measured
#: residual share is small is labelled REDUNDANT regardless of what is written
#: here.
DISTINCTNESS_CLAIM = {
    FAM_POSITIONING: (
        "who is positioned, and how crowded. The base set observes PRICE; the "
        "Commitments of Traders report observes the reported POSITIONS of "
        "speculators and hedgers in the underlying futures market, which price "
        "does not contain"),
    FAM_FX_CARRY: (
        "the interest differential. R33 declared FX carry ABSENT because the "
        "owned estate had US short rates and no foreign short rates; this is "
        "the missing leg, not a proxy for it"),
    FAM_COMMODITY_CURVE: (
        "the shape of the futures curve. R33 declared commodity carry ABSENT "
        "because the owned Continuous Futures entitlement is one market; this "
        "is a real contract-1..4 settlement curve, never manufactured from "
        "spot momentum"),
    FAM_IV_TERM: (
        "the TERM STRUCTURE of implied volatility and the variance risk "
        "premium. The base set already carries the VIX LEVEL and its one-month "
        "change; the slope between 30-day and 93-day implied volatility, and "
        "the gap between implied and realised variance, are different objects"),
    FAM_RISK_PREMIA: (
        "market-priced compensation for credit, inflation and curve risk. The "
        "base set carries a 10y-3m slope and a Baa-Aaa quality spread; real "
        "yields, breakeven inflation, curve curvature and the Baa-over-"
        "Treasury credit premium are not those"),
    FAM_INSIDER: (
        "what corporate officers and directors did with their own money, "
        "timestamped at the moment the filing became public. Nothing in a "
        "price series contains it"),
    FAM_ANALYST: (
        "analyst belief updates that arrive between filings and are disjoint "
        "from both realised fundamentals and price"),
}

# --------------------------------------------------------------------------- #
# Acquisition contracts - the exact free public endpoints
# --------------------------------------------------------------------------- #
#: A courteous, identifying User-Agent. Public data services ask for one.
HTTP_USER_AGENT = "PaperTraderResearch/1.0 (binisti@gmail.com)"
HTTP_TIMEOUT_SECONDS = 300
#: Politeness delay between requests to the same host, seconds.
HTTP_MIN_INTERVAL_SECONDS = 0.15

CFTC_COT_URL = "https://www.cftc.gov/files/dea/history/deacot{year}.zip"
CFTC_FIRST_YEAR = 1986
CBOE_VIX_URL = ("https://cdn.cboe.com/api/global/us_indices/daily_prices/"
                "VIX_History.csv")
CBOE_VIX3M_URL = ("https://cdn.cboe.com/api/global/us_indices/daily_prices/"
                  "VIX3M_History.csv")
EIA_PETROLEUM_BULK_URL = "https://www.eia.gov/opendata/bulk/PET.zip"
SEC_INSIDER_URL = ("https://www.sec.gov/files/structureddata/data/"
                   "insider-transactions-data-sets/{year}q{quarter}_form345.zip")
SEC_INSIDER_FIRST = (2008, 1)
FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY_ENV = ("FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY")

#: The FSDS quarters already owned by this estate; Release 35 reads them and
#: downloads nothing for the point-in-time SIC series.
OWNED_FSDS_ROOT = (r"D:\Stock_Prediction_app_data\alpha_agent\identity"
                   r"\sec_bulk\financial_statement_data_sets")

# --------------------------------------------------------------------------- #
# Point-in-time publication rules
# --------------------------------------------------------------------------- #
#: Every information series is broadcast onto the panel calendar this many
#: sessions AFTER it became public. Uniform with R33's global-state treatment,
#: and it costs nothing.
BROADCAST_LAG_SESSIONS = 1

#: The Commitments of Traders report is stamped with a TUESDAY and released the
#: following FRIDAY afternoon. Six calendar days covers the ordinary Tuesday ->
#: Friday gap plus a holiday-shifted release, and the value is not observable
#: before then. It is the ONLY family whose publication date must be inferred
#: rather than read, so the campaign also runs a lag SENSITIVITY at
#: ``COT_PUBLICATION_LAG_STRESS_DAYS`` - long enough to cover the 2013 and
#: 2018-19 shutdown catch-ups, when reports appeared weeks late.
COT_PUBLICATION_LAG_DAYS = 6
COT_PUBLICATION_LAG_STRESS_DAYS = 28

#: OECD 3-month interbank rates are MONTHLY and published in arrears. A month-M
#: value is treated as observable from the first session of month M+2.
OECD_RATE_PUBLICATION_LAG_MONTHS = 2

#: Daily market observables - Treasury constant maturities, TIPS yields,
#: breakevens, Moody's yields, CBOE volatility indices and NYMEX settlements -
#: carry their own session as the publication date, then the broadcast lag.
MARKET_OBSERVABLE_PUBLICATION_LAG_DAYS = 0

#: An SEC insider filing is public at its FILING_DATE and at no earlier moment.
#: The transaction date inside it is NOT a publication date and is never used as
#: one; that is the single mistake this family could make.
INSIDER_OBSERVABLE_AT = "FILING_DATE"
INSIDER_TRANSACTION_DATE_MAY_BE_OBSERVABLE = False

#: The point-in-time SIC classification uses the FSDS ``accepted`` timestamp and
#: the released no-look-ahead reader, so a 2024 reclassification cannot travel
#: backwards into 2011.
PIT_SECTOR_OWNER = "alpha_agent.pit_sector"

#: Fabrication rules, stated as prohibitions because each has a tempting
#: shortcut that would have produced a nicer-looking release.
PROHIBITED_SUBSTITUTIONS = (
    "a present-day consensus written back onto historical dates",
    "a revision series reconstructed by differencing current estimates",
    "commodity carry manufactured from spot price momentum",
    "a futures curve claimed from a continuous or back-adjusted series",
    "a renamed VIX level presented as new implied-volatility information",
    "current index constituents or current sector labels backfilled into "
    "history",
    "a filing dated at its fiscal period rather than at its acceptance",
)

# --------------------------------------------------------------------------- #
# Instrument mapping - which market prices which instrument
# --------------------------------------------------------------------------- #
#: How a mapping is justified. DIRECT means the futures market's underlying IS
#: the instrument's stated exposure; PROXY means it is the dominant economic
#: driver of that exposure but not the same index. Both are admitted; the tier
#: is recorded so a reader can discount the second.
MAP_DIRECT = "DIRECT"
MAP_PROXY = "PROXY"

#: CFTC contract market CODES, which survive the renames that market names do
#: not: 138741 has been "S&P 500 STOCK INDEX" under three exchange names, and
#: 067651 is "CRUDE OIL, LIGHT 'SWEET'" in 1995 and "WTI-PHYSICAL" in 2026.
#: Positions are SUMMED across the codes of one mapping, which is what handles
#: the big -> e-mini -> micro migration without a judgement call. The published
#: "Consolidated" rows (13874+, 20974+) are deliberately EXCLUDED: they are
#: CFTC's own aggregate of the components and would double count.
COT_MAPPING = {
    "SPY": (("138741", "13874A", "13874U"), MAP_DIRECT, "S&P 500 futures"),
    "VTI": (("138741", "13874A", "13874U"), MAP_PROXY, "S&P 500 futures"),
    "RSP": (("138741", "13874A", "13874U"), MAP_PROXY, "S&P 500 futures"),
    "QQQ": (("209741", "209742", "209747"), MAP_DIRECT, "Nasdaq-100 futures"),
    "IWM": (("239742", "239747", "23977A"), MAP_DIRECT, "Russell 2000 futures"),
    "MDY": (("338741", "33874A"), MAP_DIRECT, "S&P 400 MidCap futures"),
    "TLT": (("020601", "020604"), MAP_DIRECT, "US Treasury bond futures"),
    "IEF": (("043602", "043607"), MAP_DIRECT, "10-year Treasury note futures"),
    "SHY": (("042601",), MAP_DIRECT, "2-year Treasury note futures"),
    "GLD": (("088691", "088695"), MAP_DIRECT, "COMEX gold futures"),
    "SLV": (("084691", "084602"), MAP_DIRECT, "COMEX silver futures"),
    "USO": (("067651", "06765A"), MAP_DIRECT, "NYMEX WTI futures"),
    "DBC": (("067651", "06765A"), MAP_PROXY, "NYMEX WTI futures"),
    "DBA": (("002602", "002601", "005602", "005601", "001602", "001601",
             "001612", "001611", "001626", "001621", "080732", "007601",
             "026603"), MAP_PROXY, "grains, oilseeds and sugar futures"),
    "FXE": (("099741",), MAP_DIRECT, "CME Euro FX futures"),
    "FXY": (("097741",), MAP_DIRECT, "CME Japanese yen futures"),
    "UUP": (("098662",), MAP_DIRECT, "ICE US Dollar Index futures"),
}
#: Rows the mapping must never absorb, named so the exclusion is visible.
COT_EXCLUDED_CODES = ("13874+", "20974+", "13874W", "43874A", "43874Q",
                      "299741", "399741")

#: OECD 3-month interbank rate series, by FRED id. The US leg is the subtrahend
#: for every differential, so it is declared once.
FRED_US_SHORT_RATE = "IR3TIB01USM156N"
FRED_FOREIGN_SHORT_RATES = {
    "EZ": "IR3TIB01EZM156N",
    "JP": "IR3TIB01JPM156N",
    "CA": "IR3TIB01CAM156N",
    "MX": "IR3TIB01MXM156N",
    "GB": "IR3TIB01GBM156N",
    "CH": "IR3TIB01CHM156N",
}
#: The USD index basket, renormalised over the currencies whose short rate this
#: estate can observe. SEK (4.2 % of DXY) has no admitted series and is dropped
#: by renormalisation rather than silently treated as zero carry.
USDX_BASKET = {"EZ": 0.576, "JP": 0.136, "GB": 0.119, "CA": 0.091,
               "CH": 0.036}

#: instrument -> (currency key or "USDX_SHORT", tier, reading)
FX_CARRY_MAPPING = {
    "FXE": ("EZ", MAP_DIRECT, "long EUR versus USD"),
    "FXY": ("JP", MAP_DIRECT, "long JPY versus USD"),
    "EWJ": ("JP", MAP_PROXY, "unhedged Japanese equity carries the yen leg"),
    "EWG": ("EZ", MAP_PROXY, "unhedged German equity carries the euro leg"),
    "EWC": ("CA", MAP_PROXY,
            "unhedged Canadian equity carries the loonie leg"),
    "EWW": ("MX", MAP_PROXY, "unhedged Mexican equity carries the peso leg"),
    "UUP": ("USDX_SHORT", MAP_DIRECT,
            "long USD against the index basket: the differential reverses"),
}

#: EIA daily NYMEX settlement series for contracts 1..4. These are SETTLEMENT
#: PRICES OF DATED CONTRACTS, which is what makes a curve a curve.
EIA_WTI_CONTRACTS = ("PET.RCLC1.D", "PET.RCLC2.D", "PET.RCLC3.D",
                     "PET.RCLC4.D")
COMMODITY_CURVE_MAPPING = {
    "USO": ("WTI", MAP_DIRECT, "the fund holds WTI futures directly"),
    "DBC": ("WTI", MAP_PROXY, "energy is the dominant weight of the index"),
    "XLE": ("WTI", MAP_PROXY,
            "the crude curve is the dominant economic driver of energy "
            "producer earnings"),
}

#: Research sectors from ``alpha_agent.pit_sector.sic_to_sector`` mapped to the
#: sector ETF that expresses them. Real estate has no SIC range of its own in
#: that released mapping - 6500-6599 sits inside Financials - so XLRE, IYR and
#: RWX are UNMAPPED and recorded as such rather than being given the Financials
#: series under a different name.
INSIDER_SECTOR_MAPPING = {
    "XLB": "Materials",
    "XLI": "Industrials",
    "XLP": "ConsumerStaples",
    "XLE": "Energy",
    "XLK": "Technology",
    "XLY": "ConsumerDiscretionary",
    "XLV": "HealthCare",
    "XLC": "CommunicationServices",
    "XLU": "Utilities",
    "XLF": "Financials",
}
INSIDER_UNMAPPED_SECTOR_ETFS = ("XLRE", "IYR", "RWX")
#: Open-market purchases and sales only. Awards, option exercises, tax
#: withholding and gifts are not discretionary opinions about value.
INSIDER_TRANSACTION_CODES = ("P", "S")

#: The insider family is COUNTED, never valued. ``TRANS_SHARES`` and
#: ``TRANS_PRICEPERSHARE`` are unvalidated filer-entered fields and the acquired
#: archives contain single filings implying 2.1e16 dollars, so a value-weighted
#: aggregate measures data-entry error. Measured on the acquired data BEFORE any
#: predictive evaluation; the owner is ``information.INSIDER_VALUE_*``.
INSIDER_VALUE_WEIGHTING_ALLOWED = False

#: A sector-date needs at least this many directional filings in its trailing
#: window before the ratio means anything. Below it the value is structurally
#: absent and filled neutral, rather than being a two-filing opinion.
MIN_INSIDER_FILINGS_IN_WINDOW = 20

#: The point-in-time SIC series only classifies issuers that appear in the owned
#: financial statement data sets, so its coverage RAMPS: 0 % in 2008, 24 % in
#: 2010, 96 % from 2012. The family is admitted only from the first year that
#: clears this share, measured rather than assumed.
INSIDER_MIN_CLASSIFIED_SHARE = 0.90

#: A caveat that belongs in the contract because it changes what one feature
#: means: in the released SIC map, oil and gas EXTRACTION (SIC 1311) falls in
#: the mining range and classifies as Materials, so the Energy bucket carries
#: refiners only. R35 does not redefine that mapping - a second sector taxonomy
#: would be a second owner - it records the consequence and lets the filing
#: density floor above handle the thin bucket.
INSIDER_SECTOR_MAP_CAVEAT = (
    "alpha_agent.pit_sector maps SIC 1000-1499 (mining, including oil and gas "
    "extraction) to Materials and reserves Energy for SIC 2900-2999 petroleum "
    "refining; the Energy insider series is therefore refiners only")

# --------------------------------------------------------------------------- #
# The new features - small, declared, and frozen before any evaluation
# --------------------------------------------------------------------------- #
#: feature name -> (family, economic reading, structurally-absent fill)
#: A feature that is ABSENT BY CONSTRUCTION for an instrument is filled with a
#: neutral ZERO rather than a cross-sectional median, so "no such market exists
#: for this instrument" stays distinguishable from "this instrument happened to
#: be median". The convention is R33's, for exactly the same reason.
FILL_NEUTRAL_ZERO = "NEUTRAL_ZERO"

NEW_FEATURES = {
    # ---- FUTURES_POSITIONING ------------------------------------------------
    "cot_spec_net_oi": (
        FAM_POSITIONING,
        "speculator net long as a share of open interest", FILL_NEUTRAL_ZERO),
    "cot_spec_net_z156": (
        FAM_POSITIONING,
        "how extreme that positioning is against its own trailing three years",
        FILL_NEUTRAL_ZERO),
    "cot_spec_net_chg_13w": (
        FAM_POSITIONING,
        "one-quarter change in speculator net positioning", FILL_NEUTRAL_ZERO),
    "cot_oi_chg_13w": (
        FAM_POSITIONING,
        "one-quarter change in total open interest: participation",
        FILL_NEUTRAL_ZERO),
    # ---- FX_INTEREST_CARRY --------------------------------------------------
    "fx_carry_diff": (
        FAM_FX_CARRY,
        "foreign minus US three-month interbank rate", FILL_NEUTRAL_ZERO),
    "fx_carry_chg_63": (
        FAM_FX_CARRY,
        "one-quarter change in the interest differential", FILL_NEUTRAL_ZERO),
    # ---- COMMODITY_TERM_STRUCTURE ------------------------------------------
    "cmdty_front_basis": (
        FAM_COMMODITY_CURVE,
        "annualised log basis between contract 1 and contract 2: positive is "
        "backwardation", FILL_NEUTRAL_ZERO),
    "cmdty_curve_slope": (
        FAM_COMMODITY_CURVE,
        "annualised log slope from contract 1 to contract 4",
        FILL_NEUTRAL_ZERO),
    "cmdty_basis_chg_63": (
        FAM_COMMODITY_CURVE,
        "one-quarter change in the front basis", FILL_NEUTRAL_ZERO),
    # ---- IMPLIED_VOLATILITY_TERM_STRUCTURE ---------------------------------
    "iv_term_slope": (
        FAM_IV_TERM,
        "log ratio of 93-day to 30-day implied volatility", FILL_NEUTRAL_ZERO),
    "iv_term_slope_chg_21": (
        FAM_IV_TERM,
        "one-month change in the implied volatility term slope",
        FILL_NEUTRAL_ZERO),
    "variance_risk_premium": (
        FAM_IV_TERM,
        "implied variance minus trailing realised variance",
        FILL_NEUTRAL_ZERO),
    # ---- MARKET_IMPLIED_RISK_PREMIA ----------------------------------------
    "real_yield_10y": (
        FAM_RISK_PREMIA, "ten-year TIPS yield", FILL_NEUTRAL_ZERO),
    "breakeven_10y_chg_63": (
        FAM_RISK_PREMIA,
        "one-quarter change in ten-year breakeven inflation",
        FILL_NEUTRAL_ZERO),
    "curve_curvature": (
        FAM_RISK_PREMIA,
        "twice the ten-year less the two- and thirty-year yields",
        FILL_NEUTRAL_ZERO),
    "credit_premium_baa10y": (
        FAM_RISK_PREMIA,
        "Baa corporate yield over the ten-year Treasury", FILL_NEUTRAL_ZERO),
    # ---- INSIDER_TRANSACTION_INTENSITY -------------------------------------
    "insider_net_buy_63": (
        FAM_INSIDER,
        "sector insider buy-minus-sell share of open-market filings over one "
        "quarter, counted by filing", FILL_NEUTRAL_ZERO),
    "insider_net_buy_anomaly": (
        FAM_INSIDER,
        "that quarter against the sector's own trailing year",
        FILL_NEUTRAL_ZERO),
    "insider_market_net_buy_63": (
        FAM_INSIDER,
        "the same measure across every filing issuer: the market aggregate",
        FILL_NEUTRAL_ZERO),
}
NEW_FEATURE_NAMES = tuple(sorted(NEW_FEATURES))


def features_of(family: str) -> tuple:
    """The feature names belonging to one information family."""
    return tuple(sorted(n for n, spec in NEW_FEATURES.items()
                        if spec[0] == family))


#: Trailing windows, in sessions or weeks, declared once.
COT_Z_WINDOW_WEEKS = 156
COT_CHANGE_WEEKS = 13
CARRY_CHANGE_SESSIONS = 63
IV_CHANGE_SESSIONS = 21
IV_REALISED_WINDOW_SESSIONS = 21
BREAKEVEN_CHANGE_SESSIONS = 63
INSIDER_WINDOW_SESSIONS = 63
INSIDER_ANOMALY_WINDOW_SESSIONS = 252
COMMODITY_CHANGE_SESSIONS = 63

# --------------------------------------------------------------------------- #
# Orthogonality - measured before prediction, and a GATE
# --------------------------------------------------------------------------- #
ORTHOGONALITY_IS_A_GATE = True
ORTHOGONALITY_MEASURED_BEFORE_PREDICTION = True
#: Raw correlation alone may NOT establish distinctness. The decisive quantity
#: is the share of a candidate feature's variance that survives a regression on
#: the entire base information set, measured on TRAINING rows only.
DISTINCTNESS_IS_RAW_CORRELATION_ONLY = False
RESIDUAL_SHARE_OWNER = "alpha_agent.orthogonality"

REDUNDANT = "REDUNDANT"
PARTIALLY_REDUNDANT = "PARTIALLY_REDUNDANT"
DISTINCT = "DISTINCT"
REDUNDANCY_STATES = (REDUNDANT, PARTIALLY_REDUNDANT, DISTINCT)

#: Residual share thresholds. A feature whose variance is more than 90 %
#: explained by the base set is REDUNDANT and its family cannot claim novelty
#: from it.
REDUNDANT_RESIDUAL_SHARE_MAX = 0.10
PARTIAL_RESIDUAL_SHARE_MAX = 0.35

#: A family is admitted to the predictive stage when at least one of its
#: features is not REDUNDANT. A family every one of whose features is REDUNDANT
#: is recorded as reproducing the base set and is not tested further - it has
#: already answered the release question for itself.
FAMILY_ADMITTED_IF_ANY_FEATURE_NOT_REDUNDANT = True

#: Coverage floors. A family that covers too little of the evaluation panel
#: cannot support an inference, and saying so before the run is the only way to
#: keep it from being decided afterwards.
MIN_FAMILY_ROW_COVERAGE = 0.05
MIN_FAMILY_EVALUATION_DATES = 40

# --------------------------------------------------------------------------- #
# The predictive increment
# --------------------------------------------------------------------------- #
RANDOM_SPLIT_ALLOWED = False
NESTED_SELECTION_INSIDE_TRAINING_ONLY = True
NESTED_SELECTION_ARRANGEMENT = (
    "expanding TRAIN block -> INNER_VALIDATION block -> embargo -> EVALUATION "
    "block, per fold, imported unchanged from alpha_agent.r34.walkforward")
NON_OVERLAPPING_FORECAST_DATES = True
STANDARDISATION_FITTED_ON_TRAINING_ONLY = True
IMPUTATION_FITTED_ON_TRAINING_ONLY = True

PRIMARY_INCREMENT_STATISTIC = "PAIRED_PER_DATE_RANK_IC_DIFFERENCE"
INCREMENT_STATISTIC_MEANING = (
    "for each evaluation date, the cross-sectional rank IC of the arm that "
    "sees BASE + NEW minus the rank IC of the arm that sees BASE, on the SAME "
    "date, the SAME instruments and the SAME realised returns; the campaign "
    "reports the mean of that difference and its Newey-West t-statistic")

#: PRIMARY: both arms use the SAME model configuration, selected on the BASE
#: arm's inner-validation block inside training. Holding the architecture fixed
#: is what lets a difference be attributed to information rather than to a
#: luckier model draw.
MODEL_HELD_FIXED_ACROSS_ARMS = True
#: SECONDARY, reported and never decisive: each arm selects its own model by
#: the same inner-validation protocol, which is what a deployment would do.
FREE_MODEL_SELECTION_IS_SECONDARY = True

#: Gates on the increment. Frozen here, before the first fit.
MIN_INCREMENT_RANK_IC = 0.005
MIN_INCREMENT_T_STAT = 2.0
MIN_INCREMENT_EVALUATION_DATES = 40
INCREMENT_SIGN_MUST_BE_POSITIVE = True

# --------------------------------------------------------------------------- #
# The economic increment
# --------------------------------------------------------------------------- #
#: Only families that clear the predictive gate reach the expensive stage. The
#: base arm ALWAYS runs, because without it there is no increment to speak of.
ECONOMIC_STAGE_REQUIRES_PREDICTIVE_SURVIVAL = True
COST_BASE = "TRADED_NOTIONAL"
ECONOMIC_CONTROL = "VOLATILITY_MATCHED_BENCHMARK_AND_CASH"
EXCESS_OVER_CASH_MAY_RANK = False
PRIMARY_ECONOMIC_STATISTIC = "AFTER_COST_EXCESS_UTILITY_OVER_CONTROL"
RISK_AVERSION_GAMMA = 2.0
#: The economic increment is a PAIRED difference against the base arm's book on
#: the same dates, not a standalone Sharpe.
PRIMARY_ECONOMIC_INCREMENT = "AFTER_COST_EXCESS_UTILITY_MINUS_BASE_ARM"
MIN_ECONOMIC_INCREMENT_T_STAT = 2.0
MIN_ECONOMIC_INCREMENT_ANNUALISED = 0.0025

#: R34's concentration gates, unchanged, because the failure mode they exist to
#: catch is unchanged.
LEAVE_ONE_INSTRUMENT_OUT_REQUIRED = True
LEAVE_ONE_ASSET_CLASS_OUT_REQUIRED = True
CONCENTRATION_GATE_FROZEN_BEFORE_EVALUATION = True

#: Robustness the campaign must run for anything it promotes.
COST_SENSITIVITY_REQUIRED = True
SUBPERIOD_STABILITY_REQUIRED = True
PUBLICATION_LAG_SENSITIVITY_REQUIRED = True

# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
DENOMINATOR_COUNTS_ALL_EXECUTED = True
CONTROLS_ENTER_DENOMINATOR = False
FDR_Q = 0.10
#: A Benjamini-Hochberg rejection on a two-sided p-value can be a significant
#: LOSS. Only the positive-direction rejections may support a qualification, and
#: the two lists are reported separately. R34 learned this the hard way.
ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY = True

MAX_PRIMARY_CONFIGS = 80
#: Config families, enumerated from the frozen grids rather than typed. R34's
#: v1 hand-typed 12 for a family its grid enumerated 18 of; deriving the number
#: is the only way plan and enumeration cannot disagree.
CONFIG_FAMILIES = {
    # one per (candidate information set) x (horizon)
    "PREDICTIVE_INCREMENT": (len(ACQUIRED_FAMILIES) + 1) * len(HORIZONS),
    # each family's information ALONE, at the primary horizon, as a diagnostic
    "STANDALONE_DIAGNOSTIC": len(ACQUIRED_FAMILIES),
    # base arm plus at most every candidate set
    "ECONOMIC_CONVERSION": len(ACQUIRED_FAMILIES) + 2,
}
PLANNED_CONFIG_TOTAL = sum(CONFIG_FAMILIES.values())

# --------------------------------------------------------------------------- #
# Evidence honesty
# --------------------------------------------------------------------------- #
FRESH_UNSEEN_EVIDENCE_EXISTS = False
FRESH_UNSEEN_EVIDENCE_REASON = (
    "Releases 31, 32, 33 and 34 all selected on market outcomes through "
    "August 2026, and R33's lockbox was opened eight times. Acquiring a NEW "
    "FEATURE does not make an ALREADY-CONSUMED OUTCOME PERIOD unseen: the "
    "returns this release is scored against are the same returns four "
    "campaigns have already looked at. No untouched historical block remains, "
    "so this release produces HISTORICAL_WALK_FORWARD_EVIDENCE and never a "
    "lockbox result.")
A_FOLD_MAY_BE_CALLED_A_LOCKBOX = False
EVIDENCE_HISTORICAL = "HISTORICAL_WALK_FORWARD_EVIDENCE"
EVIDENCE_INDEPENDENT = "INDEPENDENT_FORWARD_EVIDENCE"

# --------------------------------------------------------------------------- #
# Verdicts, and the three separate results
# --------------------------------------------------------------------------- #
VERDICT_QUALIFIED = "R35_ORTHOGONAL_INFORMATION_ADDS_INCREMENTAL_EDGE"
VERDICT_NO_EDGE = "R35_NO_INCREMENTAL_INFORMATION_EDGE"
VERDICT_ACQUISITION_BLOCKED = "R35_SOURCE_ACQUISITION_BLOCKED"
VERDICT_INTEGRITY_BLOCKED = "R35_DATA_INTEGRITY_BLOCKED"
VERDICTS = (VERDICT_QUALIFIED, VERDICT_NO_EDGE, VERDICT_ACQUISITION_BLOCKED,
            VERDICT_INTEGRITY_BLOCKED)

VERDICT_MEANING = {
    VERDICT_QUALIFIED: (
        "at least one acquired information family is measurably distinct from "
        "the base set, adds out-of-sample predictive information conditional "
        "on it, survives multiple-testing control in the POSITIVE direction, "
        "and converts that into after-cost excess over a risk-matched control "
        "that the base arm does not earn"),
    VERDICT_NO_EDGE: (
        "the acquisition worked and the information did not. Every acquired "
        "family was measured for distinctness and tested for incremental "
        "prediction, and none produced an increment that survives"),
    VERDICT_ACQUISITION_BLOCKED: (
        "no acquired family reached the predictive stage because acquisition "
        "or coverage failed, so the release has not answered its question"),
    VERDICT_INTEGRITY_BLOCKED: (
        "an acquired family failed a point-in-time or survivorship invariant "
        "and the campaign refused to score it rather than scoring it anyway"),
}

SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True
#: Three results, not two. R35 adds RESEARCH_CANDIDATE_RESULT between them,
#: because a genuinely positive historical increment is a real finding AND is
#: not Alpha, and collapsing those two into one word is how a release starts
#: lying to itself.
RESULT_NAMES = ("SYSTEM_RESULT", "RESEARCH_CANDIDATE_RESULT", "ALPHA_RESULT")
ALPHA_PASS_REQUIRES = VERDICT_QUALIFIED
ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE = True
RESEARCH_CANDIDATE_PASS_REQUIRES = VERDICT_QUALIFIED

#: Nothing is promoted, registered as a signal, or activated by this release. If
#: a candidate survives, the handoff describes what a later CONTROLLED
#: integration would have to do; the canonical forward-evidence owner is named
#: and NOT written to.
FORWARD_EVIDENCE_OWNER = "api.forward_evidence"
MAY_REGISTER_FORWARD_CANDIDATE = False
MAY_CREATE_SECOND_TRUE_FORWARD_STORE = False

PANEL_START = "1999-01-04"


def genuinely_independent_evidence_exists() -> bool:
    """Whether this release holds evidence no earlier release has consumed.

    One function, one answer, referenced by the verdict builder. It returns
    ``FRESH_UNSEEN_EVIDENCE_EXISTS`` and nothing else, so a qualified verdict
    cannot be reached by a caller that forgot to ask.
    """
    return bool(FRESH_UNSEEN_EVIDENCE_EXISTS)


def evidence_label() -> str:
    return (EVIDENCE_INDEPENDENT if genuinely_independent_evidence_exists()
            else EVIDENCE_HISTORICAL)


def verdict_ceiling_without_fresh_evidence() -> str:
    """The best verdict reachable given the evidence this release actually has.

    Without independent evidence the ceiling is a RESEARCH CANDIDATE, and the
    campaign is told so by this function rather than by a comment.
    """
    return (VERDICT_QUALIFIED if genuinely_independent_evidence_exists()
            else VERDICT_NO_EDGE)


def _payload() -> dict:
    return {
        "campaign_id": CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "question": (
            "does genuinely new, economically orthogonal, point-in-time "
            "information add incremental out-of-sample prediction AND "
            "incremental after-cost risk-matched economics beyond the frozen "
            "Release-34 information set?"),
        "frozen_from_earlier_releases": {
            "base_information_set": BASE_INFORMATION_SET,
            "base_feature_owner": BASE_FEATURE_OWNER,
            "base_model_owner": BASE_MODEL_OWNER,
            "base_universe_owner": BASE_UNIVERSE_OWNER,
            "base_panel_owner": BASE_PANEL_OWNER,
            "base_forecast_owner": BASE_FORECAST_OWNER,
            "base_walkforward_owner": BASE_WALKFORWARD_OWNER,
            "base_economics_owner": BASE_ECONOMICS_OWNER,
            "conversion": dict(FROZEN_CONVERSION,
                               horizons=list(FROZEN_CONVERSION["horizons"])),
            "new_predictor_search_allowed": NEW_PREDICTOR_SEARCH_ALLOWED,
            "adaptive_search_allowed": ADAPTIVE_SEARCH_ALLOWED,
            "model_architecture_search_allowed":
                MODEL_ARCHITECTURE_SEARCH_ALLOWED,
            "conversion_layer_search_allowed": CONVERSION_LAYER_SEARCH_ALLOWED,
        },
        "money": {
            "may_spend_money": MAY_SPEND_MONEY,
            "may_start_provider_trial": MAY_START_PROVIDER_TRIAL,
            "may_create_provider_account": MAY_CREATE_PROVIDER_ACCOUNT,
            "may_acquire_free_public_data": MAY_ACQUIRE_FREE_PUBLIC_DATA,
            "may_use_existing_entitlements": MAY_USE_EXISTING_ENTITLEMENTS,
        },
        "families": {
            "acquired": list(ACQUIRED_FAMILIES),
            "blocked": list(BLOCKED_FAMILIES),
            "lane": dict(LANE_OF_FAMILY),
            "distinctness_claim": dict(DISTINCTNESS_CLAIM),
        },
        "acquisition": {
            "cftc_cot": CFTC_COT_URL, "cftc_first_year": CFTC_FIRST_YEAR,
            "cboe_vix": CBOE_VIX_URL, "cboe_vix3m": CBOE_VIX3M_URL,
            "eia_petroleum_bulk": EIA_PETROLEUM_BULK_URL,
            "sec_insider": SEC_INSIDER_URL,
            "sec_insider_first": list(SEC_INSIDER_FIRST),
            "fred_observations": FRED_OBSERVATIONS_URL,
            "owned_fsds_root": OWNED_FSDS_ROOT,
            "user_agent": HTTP_USER_AGENT,
        },
        "point_in_time": {
            "broadcast_lag_sessions": BROADCAST_LAG_SESSIONS,
            "cot_publication_lag_days": COT_PUBLICATION_LAG_DAYS,
            "cot_publication_lag_stress_days":
                COT_PUBLICATION_LAG_STRESS_DAYS,
            "oecd_rate_publication_lag_months":
                OECD_RATE_PUBLICATION_LAG_MONTHS,
            "market_observable_publication_lag_days":
                MARKET_OBSERVABLE_PUBLICATION_LAG_DAYS,
            "insider_observable_at": INSIDER_OBSERVABLE_AT,
            "insider_transaction_date_may_be_observable":
                INSIDER_TRANSACTION_DATE_MAY_BE_OBSERVABLE,
            "pit_sector_owner": PIT_SECTOR_OWNER,
            "prohibited_substitutions": list(PROHIBITED_SUBSTITUTIONS),
        },
        "mapping": {
            "cot": {k: {"codes": list(v[0]), "tier": v[1], "market": v[2]}
                    for k, v in sorted(COT_MAPPING.items())},
            "cot_excluded_codes": list(COT_EXCLUDED_CODES),
            "fx_carry": {k: {"currency": v[0], "tier": v[1], "reading": v[2]}
                         for k, v in sorted(FX_CARRY_MAPPING.items())},
            "usdx_basket": dict(USDX_BASKET),
            "commodity_curve": {
                k: {"market": v[0], "tier": v[1], "reading": v[2]}
                for k, v in sorted(COMMODITY_CURVE_MAPPING.items())},
            "eia_wti_contracts": list(EIA_WTI_CONTRACTS),
            "insider_sector": dict(INSIDER_SECTOR_MAPPING),
            "insider_unmapped_sector_etfs": list(INSIDER_UNMAPPED_SECTOR_ETFS),
            "insider_transaction_codes": list(INSIDER_TRANSACTION_CODES),
            "insider_value_weighting_allowed": INSIDER_VALUE_WEIGHTING_ALLOWED,
            "min_insider_filings_in_window": MIN_INSIDER_FILINGS_IN_WINDOW,
            "insider_min_classified_share": INSIDER_MIN_CLASSIFIED_SHARE,
            "insider_sector_map_caveat": INSIDER_SECTOR_MAP_CAVEAT,
        },
        "new_features": {n: {"family": f, "reading": r, "absent_fill": a}
                         for n, (f, r, a) in sorted(NEW_FEATURES.items())},
        "new_feature_count": len(NEW_FEATURES),
        "orthogonality": {
            "is_a_gate": ORTHOGONALITY_IS_A_GATE,
            "measured_before_prediction":
                ORTHOGONALITY_MEASURED_BEFORE_PREDICTION,
            "distinctness_is_raw_correlation_only":
                DISTINCTNESS_IS_RAW_CORRELATION_ONLY,
            "residual_share_owner": RESIDUAL_SHARE_OWNER,
            "redundant_residual_share_max": REDUNDANT_RESIDUAL_SHARE_MAX,
            "partial_residual_share_max": PARTIAL_RESIDUAL_SHARE_MAX,
            "min_family_row_coverage": MIN_FAMILY_ROW_COVERAGE,
            "min_family_evaluation_dates": MIN_FAMILY_EVALUATION_DATES,
        },
        "predictive_increment": {
            "random_split_allowed": RANDOM_SPLIT_ALLOWED,
            "nested_selection_inside_training_only":
                NESTED_SELECTION_INSIDE_TRAINING_ONLY,
            "nested_selection_arrangement": NESTED_SELECTION_ARRANGEMENT,
            "non_overlapping_forecast_dates": NON_OVERLAPPING_FORECAST_DATES,
            "standardisation_fitted_on_training_only":
                STANDARDISATION_FITTED_ON_TRAINING_ONLY,
            "imputation_fitted_on_training_only":
                IMPUTATION_FITTED_ON_TRAINING_ONLY,
            "primary_statistic": PRIMARY_INCREMENT_STATISTIC,
            "statistic_meaning": INCREMENT_STATISTIC_MEANING,
            "model_held_fixed_across_arms": MODEL_HELD_FIXED_ACROSS_ARMS,
            "free_model_selection_is_secondary":
                FREE_MODEL_SELECTION_IS_SECONDARY,
            "min_increment_rank_ic": MIN_INCREMENT_RANK_IC,
            "min_increment_t_stat": MIN_INCREMENT_T_STAT,
            "min_increment_evaluation_dates": MIN_INCREMENT_EVALUATION_DATES,
            "increment_sign_must_be_positive":
                INCREMENT_SIGN_MUST_BE_POSITIVE,
            "horizons": list(HORIZONS), "primary_horizon": PRIMARY_HORIZON,
        },
        "economic_increment": {
            "requires_predictive_survival":
                ECONOMIC_STAGE_REQUIRES_PREDICTIVE_SURVIVAL,
            "cost_base": COST_BASE,
            "control": ECONOMIC_CONTROL,
            "excess_over_cash_may_rank": EXCESS_OVER_CASH_MAY_RANK,
            "primary_statistic": PRIMARY_ECONOMIC_STATISTIC,
            "primary_increment": PRIMARY_ECONOMIC_INCREMENT,
            "risk_aversion_gamma": RISK_AVERSION_GAMMA,
            "min_increment_t_stat": MIN_ECONOMIC_INCREMENT_T_STAT,
            "min_increment_annualised": MIN_ECONOMIC_INCREMENT_ANNUALISED,
            "leave_one_instrument_out_required":
                LEAVE_ONE_INSTRUMENT_OUT_REQUIRED,
            "leave_one_asset_class_out_required":
                LEAVE_ONE_ASSET_CLASS_OUT_REQUIRED,
            "concentration_gate_frozen_before_evaluation":
                CONCENTRATION_GATE_FROZEN_BEFORE_EVALUATION,
            "cost_sensitivity_required": COST_SENSITIVITY_REQUIRED,
            "subperiod_stability_required": SUBPERIOD_STABILITY_REQUIRED,
            "publication_lag_sensitivity_required":
                PUBLICATION_LAG_SENSITIVITY_REQUIRED,
        },
        "multiple_testing": {
            "denominator_counts_all_executed":
                DENOMINATOR_COUNTS_ALL_EXECUTED,
            "controls_enter_denominator": CONTROLS_ENTER_DENOMINATOR,
            "fdr_q": FDR_Q,
            "only_positive_rejections_may_qualify":
                ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY,
            "config_families": dict(CONFIG_FAMILIES),
            "planned_config_total": PLANNED_CONFIG_TOTAL,
            "max_primary_configs": MAX_PRIMARY_CONFIGS,
        },
        "evidence": {
            "fresh_unseen_evidence_exists": FRESH_UNSEEN_EVIDENCE_EXISTS,
            "fresh_unseen_evidence_reason": FRESH_UNSEEN_EVIDENCE_REASON,
            "a_fold_may_be_called_a_lockbox": A_FOLD_MAY_BE_CALLED_A_LOCKBOX,
            "evidence_label": evidence_label(),
            "verdict_ceiling_without_fresh_evidence":
                verdict_ceiling_without_fresh_evidence(),
            "genuinely_independent_evidence_exists":
                genuinely_independent_evidence_exists(),
        },
        "verdicts": {
            "vocabulary": list(VERDICTS),
            "meaning": dict(VERDICT_MEANING),
            "result_names": list(RESULT_NAMES),
            "alpha_pass_requires": ALPHA_PASS_REQUIRES,
            "alpha_pass_also_requires_independent_evidence":
                ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE,
            "research_candidate_pass_requires":
                RESEARCH_CANDIDATE_PASS_REQUIRES,
            "system_and_alpha_results_are_separate":
                SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE,
        },
        "forward_handoff": {
            "forward_evidence_owner": FORWARD_EVIDENCE_OWNER,
            "may_register_forward_candidate": MAY_REGISTER_FORWARD_CANDIDATE,
            "may_create_second_true_forward_store":
                MAY_CREATE_SECOND_TRUE_FORWARD_STORE,
        },
        "panel_start": PANEL_START,
    }


def build(*, campaign_id: str = CAMPAIGN_ID, created_at: str) -> dict:
    """The frozen contract artifact, content-hashed."""
    payload = _payload()
    payload["campaign_id"] = campaign_id
    payload["created_at"] = created_at
    body = r35.artifact_body(CONTRACT_SCHEMA, payload)
    body["contract_hash"] = sha(
        {k: v for k, v in payload.items() if k != "created_at"})
    return body


def contract_hash() -> str:
    """The content hash of the frozen declarations, independent of run time."""
    payload = _payload()
    payload.pop("created_at", None)
    payload.pop("campaign_id", None)
    return sha(payload)


def path_for(campaign_id: str = CAMPAIGN_ID):
    return r35.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    path = path_for(body.get("campaign_id", CAMPAIGN_ID))
    return r35.write_json(path, body)


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    return r35.read_json(path_for(campaign_id))
