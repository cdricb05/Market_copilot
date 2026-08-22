"""alpha_agent.r38.contract - the frozen Release 38 research contract.

Everything this release is allowed to do, everything it refuses to do, every
frozen experiment configuration and every rule by which delivered data may be
judged is declared HERE, before any enumeration result or economic outcome is
looked at. A rule written after seeing which market wins is not a rule.

The disciplines this release exists to enforce:

* **A purchase confirmation is not a synchronized database.** The operator's
  Norgate receipt proves an entitlement exists somewhere; only the local
  Norgate Data Updater's delivered bytes prove the estate can research it.
  Phase 1 measures that, and every downstream phase is gated on the answer.
* **A programmer error is not an entitlement wall.** Every provider call is
  classified into an explicit six-state taxonomy before any conclusion about
  the subscription is allowed. Release 37 observed
  ``futures_market_session_contracts('&ES')`` raise and correctly refused to
  call it an entitlement failure; Release 38 proves the distinction and keeps
  a regression on it forever.
* **Expected unlocks are not measured unlocks.** Release 37's ~53 cells are an
  EXPECTATION derived from the frozen Release-36 matrix and a vendor's
  declared instrument list. Release 38 replaces expectation with delivered,
  verified fact - whatever the number turns out to be. Truth wins.
* **Nothing is bought and nothing is renewed.** The World Futures subscription
  was purchased MANUALLY by the operator before this release began. Release 38
  reads it, measures it, and reports whether renewal would be justified - to a
  human, through the ONE canonical gate, with zero authority of its own.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

from .. import r38
from ..r33 import contract as _r33_contract
from ..r36 import contract as _r36_contract

CALCULATION_OWNER = "alpha_agent.r38.contract"
CONTRACT_SCHEMA = "r38_native_futures_information_frontier_contract/1"
ARTIFACT_NAME = "research_contract.json"

CAMPAIGN_ID = "r38_native_futures_information_frontier_v4"

#: Superseded runs, kept with their artifacts on disk and their defects named.
SUPERSEDED_CAMPAIGNS: dict = {
    "r38_native_futures_information_frontier_v3": {
        "superseded_reason":
            "SUPERSEDED_CALENDAR_FRONT_CAN_EXPIRE_BEFORE_DELIVERY",
        "defects": (
            "v3 judged activity on the calendar-front contract (nearest "
            "delivery month >= today). Markets that cease trading well "
            "before their delivery month - Brent ceases about two months "
            "before delivery - showed a stale front bar and Brent was "
            "recorded INACTIVE. v4 takes the freshest bar among the next "
            "three undelivered months",
        ),
        "artifacts_retained": True,
    },
    "r38_native_futures_information_frontier_v2": {
        "superseded_reason": "SUPERSEDED_ACTIVITY_JUDGED_ON_THE_WRONG_CONTRACT",
        "defects": (
            "market activity was judged on the last price bar of the NEWEST "
            "LISTED contract. For strip-quoted markets the furthest listed "
            "month can trade once and go quiet while the front is among the "
            "most liquid futures in the world - Henry Hub natural gas was "
            "recorded INACTIVE on that defect. v3 judges activity on the "
            "nearest UNEXPIRED contract's last bar",
        ),
        "artifacts_retained": True,
    },
    "r38_native_futures_information_frontier_v1": {
        "superseded_reason": "SUPERSEDED_CLASSIFICATION_MAP_INCOMPLETE",
        "defects": (
            "the declared market-classification map was written before "
            "enumeration and covered only the symbols the release "
            "anticipated; 34 of the 105 DELIVERED markets (Australian "
            "grains, London softs, UK gas, ICE WTI, EUA emissions, CME "
            "crypto, SGX/HKFE/ASX/ME index and rates futures, micro "
            "contracts) enumerated as UNCLASSIFIED. The map was completed "
            "from the delivered vendor names and exchanges - metadata, not "
            "outcomes - together with declared duplicate-underlying "
            "exclusions, and the enumeration was re-frozen under v2",
        ),
        "artifacts_retained": True,
    },
}

# --------------------------------------------------------------------------- #
# The inherited purchase - a FACT of this release, never an act of it
# --------------------------------------------------------------------------- #
#: The operator manually subscribed before Release 38 began. This release
#: reports the acquisition as an inherited fact. It did not and may not spend.
INHERITED_PURCHASE = {
    "subscription": "Norgate World Futures (Silver Package)",
    "term_months": 6,
    "expiry_shown_by_vendor": "2027-02-22",
    "purchased_by": "OPERATOR_MANUALLY",
    "purchased_before_release38_began": True,
    "purchased_by_release38": False,
    "us_stocks_package_remains_active_separately": True,
    "authorising_evidence": (
        "Release 37 canonical acquisition gate: "
        "RESEARCH_ACQUISITION_RECOMMENDED for norgate_futures_package, "
        "manually approved and executed by the operator"),
}
MONEY_SPENT_BY_R38_USD = 0.0
PURCHASE_MADE_BY_THIS_RELEASE = False
RENEWAL_DECIDED_BY_THIS_RELEASE = False

# --------------------------------------------------------------------------- #
# Ownership refusals - the decisions this release does NOT own
# --------------------------------------------------------------------------- #
R38_DEFINES_ITS_OWN_ACQUISITION_AUTHORITY = False
R38_DEFINES_ITS_OWN_COVERAGE_AUTHORITY = False
ACQUISITION_DECISION_OWNER = "engine.data_expansion_gate (POST_ACQUISITION_VALUE)"
COVERAGE_MATRIX_OWNER = "alpha_agent.r36.coverage"
UNLOCK_EXPECTATION_OWNER = "alpha_agent.r37.unlock"
CANONICAL_GATE_IS_AUTHORITATIVE = True
R38_MAY_OVERRIDE_THE_CANONICAL_GATE = False
PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE = False
RENEWAL_AUTHORITY_GRANTED_BY_THIS_RELEASE = False
ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE = False
ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL = False
ACQUISITION_REQUIRES_MANUAL_OPERATOR_APPROVAL = True

MAY_SPEND_MONEY = False
MAY_START_PROVIDER_TRIAL = False
MAY_CREATE_PROVIDER_ACCOUNT = False
MAY_CHANGE_SUBSCRIPTION_TIER = False
MAY_RENEW_SUBSCRIPTION = False
MAY_ACCEPT_LICENCE_AGREEMENT = False
MAY_SUBMIT_PAYMENT_DETAILS = False
MAY_PURCHASE_CLOUD_COMPUTE = False
MAY_INSTALL_CUDA = False
MAY_DOWNLOAD_MODEL_WEIGHTS = False
MAY_UPGRADE_NORGATE_PACKAGES = False  # norgatedata stays at 1.0.74 (pinned)


def purchase_authority() -> dict:
    """Every Release-38 state, including a renewal recommendation, carries
    ``purchase_authorised: False``. There is no other code path."""
    return {"purchase_authorised": False,
            "renewal_authorised": False,
            "decided_by": "OPERATOR_MANUAL_REVIEW_ONLY",
            "calculation_owner": ACQUISITION_DECISION_OWNER}


# --------------------------------------------------------------------------- #
# Expectation vs measurement
# --------------------------------------------------------------------------- #
EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS = True
UNLOCK_BECOMES_MEASURED_ONLY_AFTER_ENTITLEMENT_ACTIVATION = True
TRUTH_WINS_OVER_EXPECTATION = True
#: Read from the frozen R37 unlock map at run time, NEVER hard-coded into a
#: result. This constant exists only so a defective read is detectable.
R37_EXPECTED_FULL_UNLOCK_CELLS_FOR_CROSS_CHECK = 53
R37_EXPECTED_PARTIAL_UNLOCK_CELLS_FOR_CROSS_CHECK = 15

# --------------------------------------------------------------------------- #
# Provider-call classification - the six-state taxonomy (Phase 1)
# --------------------------------------------------------------------------- #
#: A conclusion about the SUBSCRIPTION may only be drawn from calls classified
#: ENTITLEMENT_ERROR; a call classified PARAMETER_ERROR says something about
#: the CALLER and nothing about the entitlement.
CALL_VALID_WITH_DATA = "VALID_REQUEST_WITH_DATA"
CALL_PARAMETER_ERROR = "PARAMETER_ERROR"
CALL_ENTITLEMENT_ERROR = "ENTITLEMENT_ERROR"
CALL_EMPTY_HISTORY = "EMPTY_HISTORY"
CALL_UNSUPPORTED_MARKET = "UNSUPPORTED_MARKET"
CALL_OTHER_PROVIDER_ERROR = "OTHER_PROVIDER_ERROR"
PROVIDER_CALL_CLASSIFICATION_VOCAB = (
    CALL_VALID_WITH_DATA, CALL_PARAMETER_ERROR, CALL_ENTITLEMENT_ERROR,
    CALL_EMPTY_HISTORY, CALL_UNSUPPORTED_MARKET, CALL_OTHER_PROVIDER_ERROR)
A_PROGRAMMER_ERROR_IS_NOT_AN_ENTITLEMENT_LIMITATION = True

# --------------------------------------------------------------------------- #
# Local entitlement synchronization states (Phase 1)
# --------------------------------------------------------------------------- #
SYNC_SYNCHRONIZED = "SYNCHRONIZED"
SYNC_PARTIAL = "PARTIALLY_SYNCHRONIZED"
SYNC_NOT_SYNCHRONIZED = "NOT_SYNCHRONIZED_LOCALLY"
SYNC_VOCAB = (SYNC_SYNCHRONIZED, SYNC_PARTIAL, SYNC_NOT_SYNCHRONIZED)
#: The pre-purchase baseline this estate has measured three times (R33, R36,
#: R37): the Continuous Futures database serves exactly ONE market, ``&ES``.
#: A local state indistinguishable from that baseline is NOT_SYNCHRONIZED.
PRE_PURCHASE_BASELINE_FUTURES_MARKETS = 1
PRE_PURCHASE_BASELINE_MARKET_SYMBOL = "ES"
WEBSITE_CONFIRMATION_IS_NOT_LOCAL_SYNCHRONIZATION = True

# --------------------------------------------------------------------------- #
# R36 cell recomputation vocabulary (Phase 5)
# --------------------------------------------------------------------------- #
CELL_NATIVE_VERIFIED = "NATIVE_DATA_VERIFIED_RESEARCHABLE"
CELL_PARTIALLY_UNLOCKED = "PARTIALLY_UNLOCKED"
CELL_PROXY_ONLY_REMAINS = "PROXY_ONLY_REMAINS"
CELL_STILL_BLOCKED_ENTITLEMENT = "STILL_BLOCKED_ENTITLEMENT"
CELL_STILL_BLOCKED_HISTORY = "STILL_BLOCKED_HISTORY"
CELL_STILL_BLOCKED_METADATA = "STILL_BLOCKED_METADATA"
CELL_STILL_BLOCKED_PIT = "STILL_BLOCKED_PIT"
CELL_STILL_BLOCKED_SURVIVORSHIP = "STILL_BLOCKED_SURVIVORSHIP"
CELL_NOT_APPLICABLE = "NOT_ECONOMICALLY_APPLICABLE"
CELL_RECOMPUTATION_VOCAB = (
    CELL_NATIVE_VERIFIED, CELL_PARTIALLY_UNLOCKED, CELL_PROXY_ONLY_REMAINS,
    CELL_STILL_BLOCKED_ENTITLEMENT, CELL_STILL_BLOCKED_HISTORY,
    CELL_STILL_BLOCKED_METADATA, CELL_STILL_BLOCKED_PIT,
    CELL_STILL_BLOCKED_SURVIVORSHIP, CELL_NOT_APPLICABLE)
#: A cell may be recorded NATIVE_DATA_VERIFIED_RESEARCHABLE only when delivered
#: dated contracts implement the R36 market's native instrument with usable
#: history, metadata, point-in-time and survivorship properties - measured,
#: not inferred from the vendor's site.
VERIFIED_REQUIRES_DELIVERED_BYTES = True
MIN_VERIFIED_HISTORY_YEARS = 10.0
MIN_VERIFIED_MARKETS_PER_GROUP = 2

# --------------------------------------------------------------------------- #
# Point-in-time discipline
# --------------------------------------------------------------------------- #
NO_HINDSIGHT_ROLL = True
NO_SURVIVOR_ONLY_UNIVERSE = True
NO_CURRENT_CONTRACT_UNIVERSE_BACKFILL = True
NO_SILENT_CONTINUOUS_SUBSTITUTION = True
VENDOR_CONTINUOUS_SERIES_ARE_DERIVED_FEATURES_ONLY = True
DOCUMENT_THE_GAP_WHEN_HISTORY_IS_UNRECOVERABLE = True

# --------------------------------------------------------------------------- #
# The frozen roll policy (Phases 4/6/7) - declared before any outcome
# --------------------------------------------------------------------------- #
#: ONE observable roll rule, declared here, never searched. Exit the held
#: contract at the EARLIER of (first notice date - FN_BUFFER sessions) and
#: (last quoted date - LT_BUFFER sessions); cash-settled markets without a
#: first notice use the last-quoted leg alone. The next contract is the
#: nearest-dated contract whose own roll date has not passed.
ROLL_POLICY = "OBSERVABLE_FIRST_NOTICE_LAST_TRADE"
ROLL_FIRST_NOTICE_BUFFER_SESSIONS = 2
ROLL_LAST_TRADE_BUFFER_SESSIONS = 5
NO_ROLL_RULE_SEARCH = True
ROLL_RULE_MAY_REFERENCE_OUTCOMES = False

# --------------------------------------------------------------------------- #
# Declared market classification (Phase 2)
# --------------------------------------------------------------------------- #
#: Session symbol -> (asset_class, economic_group, cost_group). DECLARED from
#: exchange product knowledge, before enumeration. A delivered market absent
#: from this map is classified UNCLASSIFIED, reported, and excluded from the
#: frozen experiments - a declared limitation, never a silent invention.
CLASSIFICATION_IS_DECLARED_NOT_INVENTED = True
UNCLASSIFIED = "UNCLASSIFIED"
MARKET_GROUPS = {
    # -- energy (NYMEX/ICE) --
    "CL": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "HO": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "RB": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "NG": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "BRN": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "B": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "GAS": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "WTI": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "PA-N": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    # -- precious metals (COMEX/NYMEX) --
    "GC": ("COMMODITY", "PRECIOUS_METALS", "PRECIOUS_METAL_FUTURE"),
    "SI": ("COMMODITY", "PRECIOUS_METALS", "PRECIOUS_METAL_FUTURE"),
    "PL": ("COMMODITY", "PRECIOUS_METALS", "PRECIOUS_METAL_FUTURE"),
    "PA": ("COMMODITY", "PRECIOUS_METALS", "PRECIOUS_METAL_FUTURE"),
    # -- industrial metals --
    "HG": ("COMMODITY", "INDUSTRIAL_METALS", "INDUSTRIAL_METAL_FUTURE"),
    "ALI": ("COMMODITY", "INDUSTRIAL_METALS", "INDUSTRIAL_METAL_FUTURE"),
    # -- grains and oilseeds (CBOT/KC/MGE) --
    "ZC": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "ZW": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "ZS": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "ZM": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "ZL": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "ZO": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "ZR": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "KE": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "KW": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "MWE": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "MW": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "RS": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    # -- softs (ICE US) --
    "KC": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "SB": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "CC": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "CT": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "OJ": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "LBR": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "LBS": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "DC": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "RC": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "W": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    # -- livestock (CME) --
    "LE": ("COMMODITY", "LIVESTOCK", "LIVESTOCK_FUTURE"),
    "GF": ("COMMODITY", "LIVESTOCK", "LIVESTOCK_FUTURE"),
    "HE": ("COMMODITY", "LIVESTOCK", "LIVESTOCK_FUTURE"),
    # -- US rates (CBOT/CME) --
    "ZT": ("RATES", "TREASURY_FUTURES", "TREASURY_FUTURE"),
    "ZF": ("RATES", "TREASURY_FUTURES", "TREASURY_FUTURE"),
    "ZN": ("RATES", "TREASURY_FUTURES", "TREASURY_FUTURE"),
    "TN": ("RATES", "TREASURY_FUTURES", "TREASURY_FUTURE"),
    "ZB": ("RATES", "TREASURY_FUTURES", "TREASURY_FUTURE"),
    "UB": ("RATES", "TREASURY_FUTURES", "TREASURY_FUTURE"),
    "ZQ": ("RATES", "SHORT_RATE_FUTURES", "TREASURY_FUTURE"),
    "SR3": ("RATES", "SHORT_RATE_FUTURES", "TREASURY_FUTURE"),
    "GE": ("RATES", "SHORT_RATE_FUTURES", "TREASURY_FUTURE"),
    "ED": ("RATES", "SHORT_RATE_FUTURES", "TREASURY_FUTURE"),
    # -- international rates (Eurex/ICE EU/MX/TMX/SFE) --
    "FGBS": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "FGBM": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "FGBL": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "FGBX": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "FBTP": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "FOAT": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "R": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "G": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "CGB": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "JGB": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "XT": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "YT": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    # -- FX futures (CME) --
    "6E": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6J": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6B": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6A": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6C": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6S": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6N": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6M": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6L": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "6Z": ("FX", "FX_FUTURES", "FX_FUTURE"),
    "DX": ("FX", "FX_FUTURES", "FX_FUTURE"),
    # -- US equity index futures (CME/CBOT) --
    "ES": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "NQ": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "YM": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "RTY": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "EMD": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "TF": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "SP": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "ND": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    "DJ": ("US_EQUITY", "US_INDEX_FUTURES", "US_EQUITY_INDEX_FUTURE"),
    # -- international equity index futures --
    "FDAX": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
             "INTL_EQUITY_INDEX_FUTURE"),
    "FESX": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
             "INTL_EQUITY_INDEX_FUTURE"),
    "FSMI": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
             "INTL_EQUITY_INDEX_FUTURE"),
    "FTDX": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
             "INTL_EQUITY_INDEX_FUTURE"),
    "Z": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
          "INTL_EQUITY_INDEX_FUTURE"),
    "FCE": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "AEX": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "MFX": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "NK": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
           "INTL_EQUITY_INDEX_FUTURE"),
    "NIY": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "SSG": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "TWN": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "SIN": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "HSI": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "MHI": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "HHI": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "AP": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
           "INTL_EQUITY_INDEX_FUTURE"),
    "KOS": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    # -- volatility (Cboe) --
    "VX": ("VOLATILITY", "VIX_FUTURES_TERM_STRUCTURE", "VIX_FUTURE"),
    # -- classified from DELIVERED vendor names/exchanges (metadata, recorded
    #    before any economic outcome was viewed) --
    "AFB": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "AWM": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "LWB": ("COMMODITY", "GRAINS_AND_OILSEEDS", "GRAIN_FUTURE"),
    "GWM": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "WBS": ("COMMODITY", "ENERGY", "ENERGY_FUTURE"),
    "LCC": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "LRC": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "LSU": ("COMMODITY", "SOFTS", "SOFT_FUTURE"),
    "EUA": ("COMMODITY", "EMISSIONS", "EMISSIONS_FUTURE"),
    "GD": ("COMMODITY", "COMMODITY_INDEX", "COMMODITY_INDEX_FUTURE"),
    "BTC": ("CRYPTO", "CRYPTO_FUTURES", "CRYPTO_FUTURE"),
    "ETH": ("CRYPTO", "CRYPTO_FUTURES", "CRYPTO_FUTURE"),
    "MBT": ("CRYPTO", "CRYPTO_FUTURES_MICRO", "CRYPTO_FUTURE"),
    "MET": ("CRYPTO", "CRYPTO_FUTURES_MICRO", "CRYPTO_FUTURE"),
    "CRA": ("RATES", "INTERNATIONAL_SHORT_RATE", "INTL_RATES_FUTURE"),
    "LEU": ("RATES", "INTERNATIONAL_SHORT_RATE", "INTL_RATES_FUTURE"),
    "SO3": ("RATES", "INTERNATIONAL_SHORT_RATE", "INTL_RATES_FUTURE"),
    "YIB": ("RATES", "INTERNATIONAL_SHORT_RATE", "INTL_RATES_FUTURE"),
    "YIR": ("RATES", "INTERNATIONAL_SHORT_RATE", "INTL_RATES_FUTURE"),
    "LLG": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "SJB": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "YXT": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "YYT": ("RATES", "INTERNATIONAL_GOVERNMENT", "INTL_RATES_FUTURE"),
    "LFT": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "SXF": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "YAP": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "SNK": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "NKD": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES",
            "INTL_EQUITY_INDEX_FUTURE"),
    "HTW": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES_EMERGING",
            "INTL_EQUITY_INDEX_FUTURE"),
    "SCN": ("INTERNATIONAL_EQUITY", "INTL_INDEX_FUTURES_EMERGING",
            "INTL_EQUITY_INDEX_FUTURE"),
    "M2K": ("US_EQUITY", "US_INDEX_FUTURES_MICRO", "US_EQUITY_INDEX_FUTURE"),
    "MES": ("US_EQUITY", "US_INDEX_FUTURES_MICRO", "US_EQUITY_INDEX_FUTURE"),
    "MNQ": ("US_EQUITY", "US_INDEX_FUTURES_MICRO", "US_EQUITY_INDEX_FUTURE"),
    "MYM": ("US_EQUITY", "US_INDEX_FUTURES_MICRO", "US_EQUITY_INDEX_FUTURE"),
}

#: One instrument per economic underlying in any cross-section. Micro-sized
#: duplicates and second listings of an underlying already carried by a
#: longer-history delivered market are excluded from experiment universes -
#: declared here from metadata, before any outcome was viewed.
DUPLICATE_UNDERLYING_EXCLUSIONS = {
    "M2K": "RTY", "MES": "ES", "MNQ": "NQ", "MYM": "YM",
    "MBT": "BTC", "MET": "ETH",
    "WBS": "CL",           # ICE WTI duplicates NYMEX WTI
    "NKD": "SNK", "NIY": "SNK",  # Nikkei duplicates of the 1990-history SGX listing
    "MHI": "HSI",          # mini Hang Seng
}
#: A commodity INDEX future aggregates markets already in the cross-section.
COMMODITY_INDEX_GROUPS_EXCLUDED_FROM_XS = ("COMMODITY_INDEX",)

# --------------------------------------------------------------------------- #
# Transaction costs (Phase 12) - MODELLED, labelled as modelled
# --------------------------------------------------------------------------- #
COST_BASE = _r36_contract.COST_BASE  # "TRADED_NOTIONAL"
COST_MODEL_STATE = "MODELLED_NOT_OBSERVED"
#: Per-side bps by cost group. Groups already priced by Release 36 keep their
#: value; the newly delivered groups get a deliberately conservative figure.
COST_BPS_PER_SIDE = {
    "ENERGY_FUTURE": 5.0,           # r36 value retained
    "PRECIOUS_METAL_FUTURE": 5.0,   # r36 value retained
    "INDUSTRIAL_METAL_FUTURE": 8.0,
    "GRAIN_FUTURE": 8.0,
    "SOFT_FUTURE": 10.0,
    "LIVESTOCK_FUTURE": 10.0,
    "TREASURY_FUTURE": 2.0,         # r36 TREASURY_INDEX analogue
    "INTL_RATES_FUTURE": 3.0,
    "FX_FUTURE": 3.0,
    "US_EQUITY_INDEX_FUTURE": 3.0,  # r36 EQUITY_INDEX analogue
    "INTL_EQUITY_INDEX_FUTURE": 5.0,
    "VIX_FUTURE": 15.0,             # r36 VOLATILITY_ETP analogue
    "CRYPTO_FUTURE": 25.0,          # r36 CRYPTO value retained
    "EMISSIONS_FUTURE": 8.0,
    "COMMODITY_INDEX_FUTURE": 5.0,
}
COST_SENSITIVITY_MULTIPLIERS = _r36_contract.COST_SENSITIVITY_MULTIPLIERS
COST_STRESS_MULTIPLIER = _r36_contract.COST_STRESS_MULTIPLIER

# --------------------------------------------------------------------------- #
# Controls (Phase 13) - the control matches the traded exposure
# --------------------------------------------------------------------------- #
CONTROL_PASSIVE_ROLL_BASKET = "VOL_MATCHED_PASSIVE_EQUAL_WEIGHT_FRONT_ROLL"
CONTROL_RISK_MATCHED_CASH = "RISK_MATCHED_CASH_ZERO_EXPOSURE"
NO_UNIVERSAL_SPY_BENCHMARK = True
NO_DECAYING_ETP_AS_UNIVERSAL_VOL_CONTROL = True
PASSIVE_BETA_IS_NOT_ALPHA = True

# --------------------------------------------------------------------------- #
# The frozen primary experiment family (Phase 6)
# --------------------------------------------------------------------------- #
#: Every entry below is a PRIMARY configuration and enters the
#: multiple-testing denominator when executed - and when a configuration
#: cannot execute (its data was not delivered, its universe floor fails) it is
#: recorded with the reason, never quietly removed. No optimizer, no grid, no
#: result-driven expansion: this tuple may not grow after outcomes are seen.
#:
#: signal/universe/control text is the binding definition the implementation
#: must follow; cadence sessions follow Release 36 (monthly 21, weekly 5).
FROZEN_PRIMARY_CONFIGURATIONS = (
    {"name": "CMDTY_XS_MOMENTUM_12_1", "lane": "COMMODITY_NATIVE",
     "family": "CROSS_SECTIONAL",
     "universe": ("delivered COMMODITY markets in declared groups with >= 3y "
                  "delivered history; requires >= 6 markets"),
     "signal": ("252-21 session trailing return of the frozen-roll front "
                "series; long top third, short bottom third, equal weight"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "CMDTY_XS_CARRY", "lane": "COMMODITY_NATIVE",
     "family": "CARRY",
     "universe": ("delivered COMMODITY markets with a dated second contract "
                  "observable; requires >= 6 markets"),
     "signal": ("annualised log(F1/F2) between the held front and the next "
                "dated contract; long top third (backwardation), short "
                "bottom third (contango), equal weight"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "CMDTY_TS_TREND_12M", "lane": "COMMODITY_NATIVE",
     "family": "TREND",
     "universe": "every delivered COMMODITY market with >= 3y history",
     "signal": ("sign of the trailing 252-session frozen-roll return per "
                "market; positions inverse-volatility scaled to equal risk"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "CMDTY_SEASONALITY", "lane": "COMMODITY_NATIVE",
     "family": "SEASONALITY",
     "universe": ("delivered GRAINS/SOFTS/LIVESTOCK/ENERGY markets with "
                  ">= 6y history; requires >= 6 markets"),
     "signal": ("trailing mean same-calendar-month front-roll return over "
                "all prior years (current year excluded); long top third, "
                "short bottom third"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "CMDTY_COT_HEDGING_PRESSURE", "lane": "COMMODITY_NATIVE",
     "family": "POSITIONING",
     "universe": ("delivered COMMODITY markets mappable to an owned R35 CFTC "
                  "Commitments-of-Traders series; requires >= 6 markets"),
     "signal": ("commercial net position z-score (156-week window), lagged "
                "4 business days to publication as released by R35; long the "
                "most net-SHORT commercial third (hedging-pressure premium), "
                "short the most net-LONG third"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "CMDTY_CALENDAR_SPREAD_MR", "lane": "COMMODITY_NATIVE",
     "family": "MEAN_REVERSION",
     "universe": ("delivered ENERGY/GRAINS/SOFTS markets with both a front "
                  "and second dated contract; requires >= 4 markets"),
     "signal": ("z-score of log(F2)-log(F1) against its trailing 252-session "
                "mean; long the spread (long F2, short F1) when z <= -1, "
                "short when z >= +1, flat otherwise, per market"),
     "cadence_sessions": 21, "control": CONTROL_RISK_MATCHED_CASH},
    {"name": "RATES_TS_TREND", "lane": "RATES_NATIVE",
     "family": "TREND",
     "universe": "delivered TREASURY_FUTURES tenors; requires >= 3 tenors",
     "signal": ("sign of the trailing 252-session frozen-roll return per "
                "tenor; inverse-volatility scaled"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "RATES_XS_CURVE_CARRY", "lane": "RATES_NATIVE",
     "family": "CURVE_TERM_STRUCTURE",
     "universe": "delivered TREASURY_FUTURES tenors; requires >= 3 tenors",
     "signal": ("annualised log(F1/F2) calendar slope per tenor as the carry "
                "estimate; long the top half of tenors by carry, short the "
                "bottom half, equal weight"),
     "cadence_sessions": 21, "control": CONTROL_RISK_MATCHED_CASH},
    {"name": "VX_TERM_STRUCTURE_CARRY", "lane": "VOLATILITY_NATIVE",
     "family": "CARRY",
     "universe": "delivered Cboe VX dated contracts",
     "signal": ("short the front VX contract when the curve is in contango "
                "(F2 > F1 at decision time), long when in backwardation, "
                "one unit scaled to constant volatility"),
     "cadence_sessions": 5, "control": CONTROL_RISK_MATCHED_CASH},
    {"name": "VX_CALENDAR_SLOPE_MR", "lane": "VOLATILITY_NATIVE",
     "family": "CURVE_TERM_STRUCTURE",
     "universe": "delivered Cboe VX dated contracts (front and second)",
     "signal": ("z-score of log(F2)-log(F1) against its trailing 252-session "
                "mean; long the spread when z <= -1, short when z >= +1, "
                "flat otherwise"),
     "cadence_sessions": 5, "control": CONTROL_RISK_MATCHED_CASH},
    {"name": "INTL_IDX_XS_MOMENTUM", "lane": "INTERNATIONAL_EQUITY_NATIVE",
     "family": "CROSS_SECTIONAL",
     "universe": ("delivered INTL_INDEX_FUTURES markets with >= 3y history; "
                  "requires >= 4 markets; LOCAL-currency excess returns"),
     "signal": ("252-21 session trailing local-currency front-roll return; "
                "long top third, short bottom third, equal weight"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "INTL_IDX_TS_TREND", "lane": "INTERNATIONAL_EQUITY_NATIVE",
     "family": "TREND",
     "universe": "delivered INTL_INDEX_FUTURES markets with >= 3y history",
     "signal": ("sign of trailing 252-session local-currency return per "
                "market; inverse-volatility scaled"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
    {"name": "FX_FUT_CARRY_IMPLEMENTATION", "lane": "FX_NATIVE",
     "family": "CARRY",
     "universe": ("delivered CME FX futures mappable to the R36 forward "
                  "universe; requires >= 5 currencies"),
     "signal": ("the carry signal read NATIVELY off the futures calendar "
                "slope: annualised log(F1/F2), which by covered interest "
                "parity is the foreign-minus-USD short-rate differential "
                "R36 measured from OECD rates; long top third, short bottom "
                "third. This is an IMPLEMENTATION COMPARISON of an "
                "already-measured exposure, not a new discovery"),
     "cadence_sessions": 21, "control": CONTROL_PASSIVE_ROLL_BASKET},
)
FROZEN_PRIMARY_COUNT = len(FROZEN_PRIMARY_CONFIGURATIONS)
CONFIGURATION_CEILING = 20  # hard ceiling; the frozen tuple is below it
DENOMINATOR_COUNTS_ALL_EXECUTED = True
CONTROLS_ENTER_DENOMINATOR = False
ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY = True
NO_OPTIMIZER_CAMPAIGN = True
NO_GENETIC_SEARCH = True
NO_RESULT_DRIVEN_EXPANSION = True
FX_SAME_EXPOSURE_IS_NOT_A_NEW_DISCOVERY = True

# --------------------------------------------------------------------------- #
# Statistics - reused owners, frozen thresholds
# --------------------------------------------------------------------------- #
#: Frozen signal parameters, declared before any outcome was seen.
MOMENTUM_LOOKBACK_SESSIONS = 252
MOMENTUM_SKIP_SESSIONS = 21
TREND_LOOKBACK_SESSIONS = 252
VOLATILITY_WINDOW_SESSIONS = 63
SEASONALITY_MIN_PRIOR_YEARS = 5
COT_Z_WINDOW_WEEKS = 156
SPREAD_Z_WINDOW_SESSIONS = 252
SPREAD_ENTRY_Z = 1.0
SINGLE_MARKET_TARGET_VOL = 0.10
#: COT publication lag is Release 35's released constant (6 calendar days).
COT_PUBLICATION_LAG_OWNER = "alpha_agent.r35.contract.COT_PUBLICATION_LAG_DAYS"

MIN_DECISION_PERIODS = _r36_contract.MIN_DECISION_PERIODS  # 60
MIN_EXCESS_T_STAT = _r36_contract.MIN_EXCESS_T_STAT        # 2.0
FDR_Q = _r33_contract.FDR_Q                                # 0.10
MULTIPLE_TESTING_OWNER = "alpha_agent.r31.multiple_testing"
ECONOMIC_JUDGE_OWNER = "alpha_agent.r34.economics"
MDE_OWNER = "alpha_agent.r36.experiments.minimum_detectable_excess"
QUALIFICATION_CONDITIONS = _r36_contract.QUALIFICATION_CONDITIONS

#: Failure classification vocabulary (Phase 13).
FAIL_NEGATIVE = "NEGATIVE"
FAIL_UNDERPOWERED = "UNDERPOWERED"
FAIL_INDISTINGUISHABLE = "ECONOMICALLY_INDISTINGUISHABLE"
FAIL_DATA_BLOCKED = "DATA_BLOCKED"
FAIL_CONTROL_FAILURE = "CONTROL_FAILURE"
FAILURE_VOCAB = (FAIL_NEGATIVE, FAIL_UNDERPOWERED, FAIL_INDISTINGUISHABLE,
                 FAIL_DATA_BLOCKED, FAIL_CONTROL_FAILURE)

# --------------------------------------------------------------------------- #
# Result separation (never collapsed)
# --------------------------------------------------------------------------- #
RESULT_AXES = ("SYSTEM_RESULT", "DATA_ENTITLEMENT_RESULT",
               "DATA_CAPABILITY_RESULT", "RESEARCH_CANDIDATE_RESULT",
               "ALPHA_RESULT", "POST_ACQUISITION_VALUE_RESULT")
VERDICT_NOT_SYNCED = "R38_ENTITLEMENT_NOT_SYNCHRONIZED_LOCALLY"
VERDICT_QUALIFIED = "R38_NATIVE_FUTURES_ALPHA_QUALIFIED"
VERDICT_NO_QUALIFIED_ALPHA = "R38_FRONTIER_MEASURED_NO_QUALIFIED_ALPHA"
VERDICT_VOCAB = (VERDICT_NOT_SYNCED, VERDICT_QUALIFIED,
                 VERDICT_NO_QUALIFIED_ALPHA)
#: ALPHA_RESULT may be PASS only alongside the qualified verdict; enforced by
#: ``campaign.build_verdict``, not by prose.
ALPHA_PASS_REQUIRES_VERDICT = VERDICT_QUALIFIED
ALPHA_RESULT_NOT_TESTED = "NOT_TESTED"
A_WORKING_PIPELINE_IS_NOT_ALPHA = True
HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE = True
A_RENEWAL_RECOMMENDATION_IS_NOT_AUTOMATIC_RENEWAL = True

# --------------------------------------------------------------------------- #
# Artifact names
# --------------------------------------------------------------------------- #
ARTIFACT_NAMES = {
    "research_contract": "research_contract.json",
    "delivered_futures_entitlement": "delivered_futures_entitlement.json",
    "futures_market_registry": "futures_market_registry.json",
    "dated_contract_registry": "dated_contract_registry.json",
    "contract_data_quality_report": "contract_data_quality_report.json",
    "r37_expected_vs_r38_actual_unlocks":
        "r37_expected_vs_r38_actual_unlocks.json",
    "updated_global_multi_asset_coverage":
        "updated_global_multi_asset_coverage.json",
    "native_futures_experiment_registry":
        "native_futures_experiment_registry.json",
    "multiple_testing_results": "multiple_testing_results.json",
    "native_futures_economics": "native_futures_economics.json",
    "post_acquisition_data_gate_result":
        "post_acquisition_data_gate_result.json",
    "ml_ready_native_futures_contract":
        "ml_ready_native_futures_contract.json",
    "intrinio_steele_sample_request": "intrinio_steele_sample_request.json",
    "final_verdict": "final_verdict.json",
}


def build(*, campaign_id: str = CAMPAIGN_ID,
          created_at: Optional[str] = None) -> dict:
    """The frozen research contract artifact body."""
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "campaign_family": r38.CAMPAIGN_FAMILY,
        "superseded_campaigns": SUPERSEDED_CAMPAIGNS,
        "inherited_purchase": dict(INHERITED_PURCHASE),
        "money_spent_by_r38_usd": MONEY_SPENT_BY_R38_USD,
        "acquisition_decision_owner": ACQUISITION_DECISION_OWNER,
        "coverage_matrix_owner": COVERAGE_MATRIX_OWNER,
        "unlock_expectation_owner": UNLOCK_EXPECTATION_OWNER,
        "purchase_authority": purchase_authority(),
        "provider_call_classification_vocab":
            list(PROVIDER_CALL_CLASSIFICATION_VOCAB),
        "sync_vocab": list(SYNC_VOCAB),
        "cell_recomputation_vocab": list(CELL_RECOMPUTATION_VOCAB),
        "roll_policy": {
            "policy": ROLL_POLICY,
            "first_notice_buffer_sessions": ROLL_FIRST_NOTICE_BUFFER_SESSIONS,
            "last_trade_buffer_sessions": ROLL_LAST_TRADE_BUFFER_SESSIONS,
            "no_roll_rule_search": NO_ROLL_RULE_SEARCH,
        },
        "cost_model": {
            "base": COST_BASE,
            "state": COST_MODEL_STATE,
            "bps_per_side": dict(COST_BPS_PER_SIDE),
            "stress_multiplier": COST_STRESS_MULTIPLIER,
        },
        "frozen_primary_configurations":
            [dict(c) for c in FROZEN_PRIMARY_CONFIGURATIONS],
        "frozen_primary_count": FROZEN_PRIMARY_COUNT,
        "configuration_ceiling": CONFIGURATION_CEILING,
        "denominator_counts_all_executed": DENOMINATOR_COUNTS_ALL_EXECUTED,
        "fdr_q": FDR_Q,
        "min_decision_periods": MIN_DECISION_PERIODS,
        "min_excess_t_stat": MIN_EXCESS_T_STAT,
        "qualification_conditions": list(QUALIFICATION_CONDITIONS),
        "result_axes": list(RESULT_AXES),
        "verdict_vocab": list(VERDICT_VOCAB),
        "alpha_pass_requires_verdict": ALPHA_PASS_REQUIRES_VERDICT,
        "expected_unlocks_are_not_measured_unlocks":
            EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS,
        "truth_wins_over_expectation": TRUTH_WINS_OVER_EXPECTATION,
    }
    return r38.artifact_body(CONTRACT_SCHEMA, payload)


def contract_hash(body: dict) -> str:
    hashable = {k: v for k, v in body.items() if k != "created_at"}
    return r38.sha(hashable)


def path_for(campaign_id: str = CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> None:
    path = path_for(body["campaign_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    r38.write_json(path, body)


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    path = path_for(campaign_id)
    if not path.exists():
        return None
    return r38.read_json(path)
