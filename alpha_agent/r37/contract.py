"""alpha_agent.r37.contract - the frozen Release 37 research contract.

Everything this release is allowed to do, everything it refuses to do, and every
rule by which a dataset may be scored, is declared HERE and frozen into a hashed
artifact before any provider is looked at. A rule written after seeing which
vendor wins is not a rule.

The three disciplines this release exists to enforce:

* **A marketing claim is not a measurement.** Six releases of this estate have
  been misled by a vendor page. A dataset may only be credited with unlocking a
  Release-36 cell if its DECLARED INSTRUMENTS implement that market's native
  instrument, and the claim must name the evidence that established it.
* **Nothing is bought.** Not a subscription, not a trial, not an account, not a
  licence, not a quote acceptance, not a payment detail, not an hour of cloud
  compute. ``BUY_RECOMMENDED_FOR_OPERATOR_REVIEW`` is a recommendation to a
  human and is never authority.
* **The gate already exists.** The dataset purchase/integration calculation is
  ``engine.data_expansion_gate`` (Slice 9) and the ten-condition information
  gate is ``alpha_agent.r32.purchase_gate``. Release 37 composes them. A fourth
  gate would be a fourth answer to one question.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

from .. import r37
from ..r35 import contract as _r35_contract
from ..r36 import contract as _r36_contract

CALCULATION_OWNER = "alpha_agent.r37.contract"
CONTRACT_SCHEMA = "r37_native_market_data_gate_contract/1"
ARTIFACT_NAME = "research_contract.json"

CAMPAIGN_ID = "r37_native_market_data_gate_v5"

#: Superseded runs, kept with their artifacts on disk and their defects named.
#: A release that quietly re-ran until it liked the output would be worthless,
#: so a corrected run gets a NEW campaign id and the old one stays readable.
SUPERSEDED_CAMPAIGNS = {
    "r37_native_market_data_gate_v1": {
        "superseded_reason": "SUPERSEDED_GATE_RESULT_WAS_READ_FROM_THE_WRAPPER",
        "defects": (
            "``api.data_expansion.run_evaluation`` returns "
            "``{input_contract, evaluation}``; v1 read the wrapper as though it "
            "were the kernel result, so EVERY Slice-9 verdict serialised as "
            "null. The composition owner had run correctly and the release was "
            "reporting nothing it said",
            "a free route that could not be reached at all was recorded as "
            "``still_blocked = False`` - a transport error reported as good "
            "news. An unreachable probe is UNMEASURED, and v2 says so",
            "the Cboe volume/open-interest sample failed to parse because "
            "``csv.DictReader`` returns a LIST for the rest-key on a ragged "
            "row, which made the duplicate-detection key unhashable",
        ),
        "artifacts_retained": True,
    },
    "r37_native_market_data_gate_v2": {
        "superseded_reason": "SUPERSEDED_A_SAMPLE_DISPROVED_A_SCORECARD_CLAIM",
        "defects": (
            "the Cboe CFE volume/open-interest row was recorded as "
            "PER-DATED-CONTRACT on the strength of the exchange's page "
            "description. v2 downloaded the file and parsed it, and it is WIDE "
            "and PRODUCT-LEVEL: one row per date, one Volume and one OI column "
            "per product, and no expiry key anywhere. The claim was corrected, "
            "the dataset's ``dated_contracts_available`` became False and its "
            "incremental distinctness fell from MEDIUM to LOW",
            "this is the release's own rule - "
            "A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT - catching the release "
            "itself, and it is the reason Track B downloads samples rather "
            "than summarising pages",
        ),
        "artifacts_retained": True,
    },
    "r37_native_market_data_gate_v3": {
        "superseded_reason": "SUPERSEDED_THE_COMPUTE_INVENTORY_COULD_NOT_SEE_RAM",
        "defects": (
            "``cpu_and_memory`` measured installed memory with ``os.sysconf``, "
            "which does not exist on Windows - the only platform this estate "
            "runs on. The COMPUTE_INVENTORY therefore reported "
            "``total_ram_gb: null`` on a 64 GB machine, and every RAM-dependent "
            "feasibility verdict in the ML readiness matrix fell through to the "
            "VRAM test alone. v4 reads ``GlobalMemoryStatusEx`` through ctypes "
            "and records which mechanism answered",
            "the verdicts happened to be unchanged, which is exactly why the "
            "defect was worth fixing rather than tolerating: an artifact that "
            "cannot show its own inputs cannot be checked",
        ),
        "artifacts_retained": True,
    },
    "r37_native_market_data_gate_v4": {
        "superseded_reason": "SUPERSEDED_TWO_COMPETING_ACQUISITION_TRUTHS",
        "defects": (
            "v4 ran every candidate through the canonical Slice-9 gate, got "
            "INSUFFICIENT_EVIDENCE for all twenty because that gate requires "
            "measured out-of-sample lift, and then reported its OWN "
            "capability-per-dollar recommendation beside it. Both halves were "
            "honest and the pair was incoherent: two apparently competing "
            "answers to one question, with the release's own answer carrying "
            "the headline. The defect was in the SEMANTICS of the canonical "
            "gate, not in either result - it modelled only the "
            "post-acquisition question, and asking it a pre-acquisition "
            "question is circular",
            "Release 37.1 extends the ONE canonical gate with an explicit "
            "decision context. RESEARCH_ACQUISITION answers 'is this worth "
            "paying to LEARN', requires no measured lift, and is strict about "
            "everything else including two things it now demands and v4 never "
            "declared: a named capability the acquisition unlocks and an "
            "expectation of economic distinctness from owned data. "
            "POST_ACQUISITION_VALUE is unchanged, remains the default, and its "
            "INSUFFICIENT_EVIDENCE verdicts are still recorded verbatim",
            "v4 reported that eight of thirteen model families 'run here "
            "today'. Eight met the HARDWARE minima; the environment holds "
            "numpy and pandas and nothing else, so the number that could "
            "actually be executed was one. Readiness is now a five-value class "
            "computed from the measured library inventory as well as the "
            "measured hardware",
            "the investment recommendation itself is UNCHANGED. The canonical "
            "acquisition gate independently reaches "
            "RESEARCH_ACQUISITION_RECOMMENDED for the same single candidate, "
            "with no failed dimension and no outstanding blocker",
        ),
        "artifacts_retained": True,
    },
}

# --------------------------------------------------------------------------- #
# What this release inherits, and what it is forbidden to re-derive
# --------------------------------------------------------------------------- #
#: The Release-36 frontier is the INPUT to this release, not something it may
#: recompute. The cell states, the markets and the blockers are read from the
#: released ``alpha_agent.r36.coverage`` owner and its frozen matrix.
INHERITED = {
    "release36_coverage_matrix": (
        "alpha_agent.r36.coverage - 608 cells, 200 applicable, 95 blocked; "
        "Release 37 reads it and never re-types it"),
    "release36_entitlement_discipline": (
        "alpha_agent.r36.entitlements - a configured API key is not an "
        "entitlement; only a real endpoint answer counts"),
    "release35_http_primitive": (
        "alpha_agent.r35.acquisition.fetch - the ONE outbound HTTP owner for "
        "research payloads, idempotent and checksum-recording"),
    "release32_information_gate": (
        "alpha_agent.r32.purchase_gate - the ten information conditions"),
    "slice9_dataset_gate": (
        "engine.data_expansion_gate via api.data_expansion - the ONE dataset "
        "acquisition/purchase/integration calculation. Sixteen dimensions in "
        "the POST_ACQUISITION_VALUE context and eighteen in the "
        "RESEARCH_ACQUISITION context, selected by an explicit decision "
        "context rather than assumed"),
    "release31_hashing": (
        "alpha_agent.r31 - artifact writing, hashing and immutability"),
}

#: Questions this release may NOT reopen. Each was answered terminally and
#: re-running it would spend the release proving what is already recorded.
SETTLED_AND_NOT_REOPENED = (
    "R31 mathematical/price/fundamental frontier on US equities",
    "R32 six zero-cost multi-asset sleeves",
    "R33 66-market predictive edge",
    "R34 47-fund prediction-to-PnL conversion",
    "R35 six free orthogonal information families",
    "R36 34 native multi-asset configurations, including the FX carry lane",
)

#: A release that reruns an exhausted campaign has not found new information.
MAY_RERUN_EXHAUSTED_CAMPAIGNS = False
MAY_LAUNCH_ALPHA_CAMPAIGN = False
MAY_RUN_OPTIMISER_SEARCH = False
ML_TRAINING_CAMPAIGN_IN_SCOPE = False
MARKET_STRUCTURE_EXPERIMENT_IN_SCOPE = False

# --------------------------------------------------------------------------- #
# Commercial safety - the whole subject of this release
# --------------------------------------------------------------------------- #
MAY_SPEND_MONEY = False
MAY_START_PROVIDER_TRIAL = False
MAY_CREATE_PROVIDER_ACCOUNT = False
MAY_CHANGE_SUBSCRIPTION_TIER = False
MAY_ACCEPT_LICENCE_AGREEMENT = False
MAY_SUBMIT_PAYMENT_DETAILS = False
MAY_PURCHASE_CLOUD_COMPUTE = False
MAY_INSTALL_CUDA = False
MAY_DOWNLOAD_MODEL_WEIGHTS = False

#: Acquisition that IS permitted, spelled out so the boundary is not a matter of
#: interpretation. Everything here costs zero and creates no obligation.
PERMITTED_ACQUISITION = (
    "a public archive published without an account",
    "a vendor sample file published without a commercial commitment",
    "a government or exchange public-domain archive",
    "an entitlement this estate ALREADY pays for",
    "a provider sample already present on local disk",
)

#: Acquisition that requires a HUMAN and is recorded as such.
REQUIRES_OPERATOR_ACTION = (
    "any paid subscription or historical add-on",
    "any trial requiring a credit card or a contract",
    "any new commercial account whose creation accepts terms",
    "any evaluation agreement, NDA or licence acceptance",
    "any vendor sample released only after a sales conversation",
)

# --------------------------------------------------------------------------- #
# Honesty rules about vendor claims
# --------------------------------------------------------------------------- #
A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT = True

#: The rule that stops a scorecard becoming a brochure: a cell may be counted as
#: unlocked only when the dataset's declared instrument list implements the
#: market's native instrument, and the claim carries its evidence class.
VENDOR_CLAIM_ALONE_MAY_UNLOCK_A_CELL = False
UNLOCK_REQUIRES_DECLARED_INSTRUMENT_MAPPING = True

#: A partially-covered market is reported, and does NOT enter the headline.
PARTIAL_UNLOCK_COUNTS_IN_HEADLINE = False

#: Release 37.1. Every cell this release claims is an EXPECTED unlock, derived
#: from the frozen Release-36 matrix and the dataset's declared instruments. It
#: becomes a MEASURED unlock only after the entitlement is activated and the
#: delivered markets and contracts are enumerated. The distinction is the same
#: one A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT makes, applied to this release's
#: own arithmetic instead of to a vendor's page.
EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS = True
UNLOCK_BECOMES_MEASURED_ONLY_AFTER_ENTITLEMENT_ACTIVATION = True
UNLOCK_MEASUREMENT_NOTE = (
    "~53 Release-36 cells is an EXPECTED unlock. Verification is the first step "
    "of Release 38: activate the entitlement, enumerate the delivered markets "
    "and dated contracts, and record the MEASURED unlock against this claim. A "
    "shortfall is a finding, not a failure of the purchase decision")

# --------------------------------------------------------------------------- #
# Where the acquisition decision is taken - Release 37.1
# --------------------------------------------------------------------------- #
#: The ONE canonical gate, asked the RESEARCH_ACQUISITION question, decides
#: whether a dataset should be acquired. This release's own gate states are a
#: TRIAGE vocabulary - which lane a candidate is in and what a human must do
#: next - and are never a competing acquisition authority.
ACQUISITION_DECISION_OWNER = "engine.data_expansion_gate"
ACQUISITION_DECISION_CONTEXT = "RESEARCH_ACQUISITION"
POST_ACQUISITION_DECISION_CONTEXT = "POST_ACQUISITION_VALUE"
R37_DEFINES_ITS_OWN_ACQUISITION_AUTHORITY = False
R37_STATES_ARE_TRIAGE_LABELS = True
R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED = False

#: What a positive Stage-A result is NOT. Stated here because the single most
#: available misreading of this release is that a recommendation to buy data is
#: evidence the data works.
ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE = False
ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL = False
ACQUISITION_RECOMMENDATION_IS_PURCHASE_AUTHORITY = False
ACQUISITION_REQUIRES_MANUAL_OPERATOR_APPROVAL = True

#: Inherited verbatim from Release 36: a proxy result may not close a native
#: frontier, so a proxy dataset may not be credited with unlocking a native cell.
PROXY_MAY_CLOSE_A_NATIVE_FRONTIER = _r36_contract.PROXY_MAY_CLOSE_A_NATIVE_FRONTIER
LEVEL_SIGNAL = _r36_contract.LEVEL_SIGNAL
LEVEL_PROXY = _r36_contract.LEVEL_PROXY
LEVEL_NATIVE = _r36_contract.LEVEL_NATIVE
LEVELS = _r36_contract.LEVELS

#: Evidence classes, weakest to strongest. Every scorecard row carries one, and
#: the score is not allowed to treat them as equivalent.
EVIDENCE_VENDOR_PAGE = "VENDOR_PUBLISHED_DOCUMENTATION"
EVIDENCE_VENDOR_QUOTE = "VENDOR_QUOTED_TO_THIS_ESTATE"
EVIDENCE_LIVE_PROBE = "LIVE_ENDPOINT_PROBE_BY_THIS_RELEASE"
EVIDENCE_LOCAL_CLIENT = "OWNED_CLIENT_API_MEASURED_LOCALLY"
EVIDENCE_SAMPLE_VALIDATED = "REAL_SAMPLE_DOWNLOADED_AND_VALIDATED"
EVIDENCE_PRIOR_RELEASE = "MEASURED_BY_AN_EARLIER_RELEASE"
EVIDENCE_CLASSES = (EVIDENCE_VENDOR_PAGE, EVIDENCE_VENDOR_QUOTE,
                    EVIDENCE_LIVE_PROBE, EVIDENCE_LOCAL_CLIENT,
                    EVIDENCE_SAMPLE_VALIDATED, EVIDENCE_PRIOR_RELEASE)

#: Substitutions that remain prohibited. Carried forward and extended with the
#: two this release could plausibly have made.
PROHIBITED_SUBSTITUTIONS = _r36_contract.PROHIBITED_SUBSTITUTIONS + (
    "a vendor's published coverage table may never be recorded as a measured "
    "sample validation",
    "a currently-listed contract universe may never be written back onto "
    "historical dates as though those contracts had always existed",
    "a back-adjusted continuous series may never be reported as dated-contract "
    "coverage, because the adjustment is a vendor transformation nobody here "
    "can invert",
    "a benchmark or fixing price (LBMA, a cash index) may never be reported as "
    "a futures settlement",
)

# --------------------------------------------------------------------------- #
# Purchase-gate state vocabulary
# --------------------------------------------------------------------------- #
STATE_BUY_RECOMMENDED = "BUY_RECOMMENDED_FOR_OPERATOR_REVIEW"
STATE_SAMPLE_REQUIRED = "SAMPLE_REQUIRED"
STATE_SAMPLE_PASSED = "SAMPLE_PASSED_PURCHASE_REVIEW_REQUIRED"
STATE_SAMPLE_FAILED = "SAMPLE_FAILED"
STATE_ENTITLEMENT_OWNED = "ENTITLEMENT_ALREADY_OWNED_USE_IT"
STATE_FREE_ACQUIRE_NOW = "FREE_DATA_AVAILABLE_ACQUIRE_NOW"
STATE_NO_LOW_VALUE = "DO_NOT_BUY_LOW_INCREMENTAL_VALUE"
STATE_NO_PIT = "DO_NOT_BUY_PIT_FAILURE"
STATE_NO_SURVIVORSHIP = "DO_NOT_BUY_SURVIVORSHIP_FAILURE"
STATE_NO_HISTORY = "DO_NOT_BUY_INSUFFICIENT_HISTORY"
STATE_NO_LICENCE = "DO_NOT_BUY_LICENSE_RISK"
STATE_NO_COST_VALUE = "DO_NOT_BUY_COST_VALUE_FAILURE"
STATE_BLOCKED_CONTACT = "BLOCKED_VENDOR_CONTACT_REQUIRED"

GATE_STATES = (
    STATE_BUY_RECOMMENDED, STATE_SAMPLE_REQUIRED, STATE_SAMPLE_PASSED,
    STATE_SAMPLE_FAILED, STATE_ENTITLEMENT_OWNED, STATE_FREE_ACQUIRE_NOW,
    STATE_NO_LOW_VALUE, STATE_NO_PIT, STATE_NO_SURVIVORSHIP,
    STATE_NO_HISTORY, STATE_NO_LICENCE, STATE_NO_COST_VALUE,
    STATE_BLOCKED_CONTACT,
)

#: States that would let money leave the estate if a human approved them. There
#: is exactly one, and it is a recommendation rather than an authorisation.
STATES_IMPLYING_SPEND = (STATE_BUY_RECOMMENDED,)
PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE = False

#: Every row must terminate. "Interesting" and "future work" are not states.
EVERY_CANDIDATE_MUST_BE_TERMINAL = True

# --------------------------------------------------------------------------- #
# Research requirements a futures / expectations dataset must satisfy
# --------------------------------------------------------------------------- #
#: The fields a serious dated-contract futures archive should carry. A provider
#: that supplies only a vendor-defined back-adjusted continuous series does not
#: get full credit, because roll yield, calendar spreads and curve carry are all
#: differences between two DIFFERENT contracts quoted the same day.
FUTURES_REQUIRED_FIELDS = (
    "exchange", "market_symbol", "contract_symbol", "delivery_month",
    "delivery_year", "expiry_or_last_trade_date", "first_notice_date",
    "daily_settlement", "open", "high", "low", "close", "volume",
    "open_interest", "point_value", "tick_size", "currency",
)
FUTURES_DESIRED_FIELDS = (
    "trading_calendar", "discontinued_contract_retained",
    "raw_non_hindsight_roll_information", "historical_contract_metadata",
)
FUTURES_MIN_HISTORY_YEARS_PREFERRED = 20
FUTURES_MIN_HISTORY_YEARS_FLOOR = 15
CONTINUOUS_ONLY_IS_FULL_CREDIT = False

#: Analyst expectations remain the strongest competing EQUITY purchase, and the
#: bar is the one Stage 13A/13B/13C and Release 35 already established.
ANALYST_MIN_HISTORY_YEARS = 15
ANALYST_MIN_ISSUERS = 800
ANALYST_REQUIRES_INACTIVE_COVERAGE = True
ANALYST_CURRENT_SNAPSHOT_IS_NOT_HISTORY = True

# --------------------------------------------------------------------------- #
# The scoring formula - declared BEFORE any provider is scored
# --------------------------------------------------------------------------- #
#: One-time costs are spread over this many years so a perpetual dump and an
#: annual subscription are comparable.
COST_AMORTISATION_YEARS = 5.0

#: Free data would divide by zero. The floor makes the score finite and is
#: reported alongside it, so nobody reads an enormous number as a measurement.
FREE_COST_FLOOR_USD = 1.0

#: Integrity factors. Each is a MULTIPLIER on the unlocked-cell count, so a
#: dataset that unlocks many cells badly cannot outrank one that unlocks fewer
#: cells properly. Every value is declared here and never searched.
NATIVE_STRUCTURE_FACTOR = {
    LEVEL_NATIVE: 1.00,     # dated contracts / the instrument itself
    LEVEL_PROXY: 0.10,      # a fund that tracks the market
    LEVEL_SIGNAL: 0.05,     # a series that predicts it but cannot be held
}
CONTINUOUS_ONLY_FACTOR = 0.35   # vendor-rolled series, no dated contracts
PIT_FACTOR = {"TRUE_POINT_IN_TIME": 1.00, "OBSERVED_AS_PUBLISHED": 1.00,
              "REVISED_HISTORY": 0.40, "CURRENT_SNAPSHOT_ONLY": 0.10,
              "UNKNOWN": 0.30}
#: ``NOT_APPLICABLE`` is a real category rather than a shrug: a benchmark
#: fixing or a government statistic has no universe to be biased, so it is not
#: penalised for lacking delisted coverage it could never have.
SURVIVORSHIP_FACTOR = {"DISCONTINUED_RETAINED": 1.00,
                       "NOT_APPLICABLE": 1.00,
                       "PARTIAL_INACTIVE_COVERAGE": 0.60,
                       "SURVIVORS_ONLY": 0.20, "UNKNOWN": 0.40}
LICENCE_FACTOR = {"RESEARCH_USE_CLEAR": 1.00, "RESEARCH_USE_AMBIGUOUS": 0.50,
                  "RESEARCH_USE_PROHIBITED": 0.00, "UNKNOWN": 0.50}
IDENTITY_FACTOR = {"STRONG": 1.00, "MODERATE": 0.70, "WEAK": 0.35,
                   "UNKNOWN": 0.50}
OPACITY_FACTOR = {"RAW_AS_PUBLISHED": 1.00, "DOCUMENTED_TRANSFORM": 0.80,
                  "OPAQUE_VENDOR_TRANSFORM": 0.40, "UNKNOWN": 0.60}

#: History is rewarded up to the preferred floor and not beyond, because a
#: dataset with 60 years is not four times better than one with 20 for a monthly
#: decision cadence.
HISTORY_REFERENCE_YEARS = 20.0

#: Breadth across economically distinct asset classes is rewarded sub-linearly.
BREADTH_BASE = 1.0
BREADTH_PER_ASSET_CLASS = 0.15

SCORE_FORMULA = (
    "research_value_points = cells_unlocked_full"
    " * native_structure_factor"
    " * continuous_only_factor_if_no_dated_contracts"
    " * pit_factor * survivorship_factor * licence_factor"
    " * identity_factor * opacity_factor"
    " * min(1, history_years / HISTORY_REFERENCE_YEARS)"
    " * (BREADTH_BASE + BREADTH_PER_ASSET_CLASS * distinct_asset_classes);"
    " annualised_cost_usd = annual_usd + one_time_usd / COST_AMORTISATION_YEARS;"
    " RESEARCH_VALUE_PER_DOLLAR_SCORE = research_value_points"
    " / max(annualised_cost_usd, FREE_COST_FLOOR_USD)"
)

SCORE_IS_A_RANKING_AID_NOT_AN_OPTIMISER = True
SCORE_MAY_OVERRIDE_A_HARD_GATE = False

# --------------------------------------------------------------------------- #
# Free public routes this release probes. One bounded request each.
# --------------------------------------------------------------------------- #
HTTP_TIMEOUT_SECONDS = _r35_contract.HTTP_TIMEOUT_SECONDS
HTTP_MIN_INTERVAL_SECONDS = _r35_contract.HTTP_MIN_INTERVAL_SECONDS
HTTP_USER_AGENT = _r35_contract.HTTP_USER_AGENT

#: Cboe CFE volume and open interest, per DATED contract, 2004 -> present. This
#: is a free route Release 36 did not find; its five probed routes were all
#: settlement routes, and every one answered 403 or 404.
CBOE_CFE_VOLOI_URL = ("https://cdn.cboe.com/data/us/futures/market_statistics/"
                      "historical_data/cfevoloi.csv")
#: The settlement route, re-probed to confirm the Release-36 licence block still
#: stands rather than assuming it.
CBOE_CFE_DAILY_SETTLEMENT_URL = (
    "https://cdn.cboe.com/data/us/futures/market_statistics/daily_settlement/"
    "2020/CFE_20200102_DailySettlement.csv")
#: LBMA precious-metal benchmark fixings. Free and long, and deliberately NOT
#: credited with unlocking a metals cell: a fixing is not a futures settlement.
LBMA_GOLD_PM_URL = "https://prices.lbma.org.uk/json/gold_pm.json"
LBMA_SILVER_URL = "https://prices.lbma.org.uk/json/silver.json"
LBMA_PLATINUM_AM_URL = "https://prices.lbma.org.uk/json/platinum_am.json"
#: New York Fed primary-dealer positioning: free, weekly, and a genuine
#: POSITIONING input for the rates lane once a rates instrument exists.
NYFED_PRIMARY_DEALER_URL = (
    "https://markets.newyorkfed.org/api/pd/get/all/timeseries.csv")

FREE_ROUTES = {
    "CBOE_CFE_VOLUME_OPEN_INTEREST": CBOE_CFE_VOLOI_URL,
    "CBOE_CFE_DAILY_SETTLEMENT": CBOE_CFE_DAILY_SETTLEMENT_URL,
    "LBMA_GOLD_PM": LBMA_GOLD_PM_URL,
    "LBMA_SILVER": LBMA_SILVER_URL,
    "LBMA_PLATINUM_AM": LBMA_PLATINUM_AM_URL,
    "NYFED_PRIMARY_DEALER_POSITIONS": NYFED_PRIMARY_DEALER_URL,
}

#: Routes probed to CONFIRM a block rather than to acquire anything. A release
#: that assumes yesterday's 403 is today's 403 is quoting, not measuring.
BLOCK_CONFIRMATION_ROUTES = {
    "NASDAQ_DATA_LINK_ZACKS_EE":
        "https://data.nasdaq.com/api/v3/datatables/ZACKS/EE.json"
        "?qopts.per_page=1",
    "CME_PUBLIC_SETTLEMENTS":
        "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/"
        "437/FUT?tradeDate=01/02/2020&pageSize=500",
}

# --------------------------------------------------------------------------- #
# Verdict vocabulary
# --------------------------------------------------------------------------- #
VERDICT_RECOMMENDED = "R37_DATA_INVESTMENT_RECOMMENDED"
VERDICT_NO_INVESTMENT = "R37_NO_DATA_INVESTMENT_JUSTIFIED"
VERDICT_BLOCKED = "R37_ALL_CANDIDATES_BLOCKED_ON_VENDOR_CONTACT"
VERDICTS = (VERDICT_RECOMMENDED, VERDICT_NO_INVESTMENT, VERDICT_BLOCKED)

#: The three results Release 33 onward reports separately. Release 37 is a
#: PURCHASE release, so its middle result is about the purchase case rather than
#: about a research candidate, and its ALPHA_RESULT is structurally NOT_TESTED -
#: this release runs no experiment and may not claim one.
RESULT_NAMES = ("SYSTEM_RESULT", "PURCHASE_RECOMMENDATION_RESULT",
                "ALPHA_RESULT")
SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True
ALPHA_RESULT_IS_STRUCTURALLY_NOT_TESTED = True
ALPHA_RESULT_VALUE = "NOT_TESTED"
ALPHA_RESULT_REASON = (
    "Release 37 executes no strategy, fits no model and judges no book. A "
    "purchase recommendation is not evidence of alpha, and a release that let "
    "its own recommendation stand in for a result would be the exact error "
    "Releases 33 to 36 were built to prevent.")


def alpha_result() -> str:
    """Always NOT_TESTED. There is no code path that returns anything else."""
    return ALPHA_RESULT_VALUE


def purchase_authority(state: str) -> dict:
    """What a gate state does and does not authorise.

    Every state - including the single recommending one - returns
    ``authorised=False``. The distinction the caller gets is whether a HUMAN now
    has a decision to make, not whether money may move.
    """
    return {"state": state,
            "purchase_authorised": False,
            "human_decision_required": state in STATES_IMPLYING_SPEND,
            "money_spent_usd": 0.0,
            "authority_note": (
                "BUY_RECOMMENDED_FOR_OPERATOR_REVIEW is a recommendation to a "
                "person; this release holds no purchasing authority")}


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def build(*, campaign_id: str = CAMPAIGN_ID,
          created_at: Optional[str] = None) -> dict:
    """Freeze the contract into a hashed artifact body."""
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at or _now(),
        "calculation_owner": CALCULATION_OWNER,
        "inherited": dict(INHERITED),
        "superseded_campaigns": {
            k: dict(v, defects=list(v["defects"]))
            for k, v in SUPERSEDED_CAMPAIGNS.items()},
        "settled_and_not_reopened": list(SETTLED_AND_NOT_REOPENED),
        "may_rerun_exhausted_campaigns": MAY_RERUN_EXHAUSTED_CAMPAIGNS,
        "may_launch_alpha_campaign": MAY_LAUNCH_ALPHA_CAMPAIGN,
        "may_run_optimiser_search": MAY_RUN_OPTIMISER_SEARCH,
        "ml_training_campaign_in_scope": ML_TRAINING_CAMPAIGN_IN_SCOPE,
        "market_structure_experiment_in_scope":
            MARKET_STRUCTURE_EXPERIMENT_IN_SCOPE,
        "commercial_safety": {
            "may_spend_money": MAY_SPEND_MONEY,
            "may_start_provider_trial": MAY_START_PROVIDER_TRIAL,
            "may_create_provider_account": MAY_CREATE_PROVIDER_ACCOUNT,
            "may_change_subscription_tier": MAY_CHANGE_SUBSCRIPTION_TIER,
            "may_accept_licence_agreement": MAY_ACCEPT_LICENCE_AGREEMENT,
            "may_submit_payment_details": MAY_SUBMIT_PAYMENT_DETAILS,
            "may_purchase_cloud_compute": MAY_PURCHASE_CLOUD_COMPUTE,
            "may_install_cuda": MAY_INSTALL_CUDA,
            "may_download_model_weights": MAY_DOWNLOAD_MODEL_WEIGHTS,
        },
        "permitted_acquisition": list(PERMITTED_ACQUISITION),
        "requires_operator_action": list(REQUIRES_OPERATOR_ACTION),
        "honesty_rules": {
            "a_marketing_claim_is_not_a_measurement":
                A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT,
            "vendor_claim_alone_may_unlock_a_cell":
                VENDOR_CLAIM_ALONE_MAY_UNLOCK_A_CELL,
            "unlock_requires_declared_instrument_mapping":
                UNLOCK_REQUIRES_DECLARED_INSTRUMENT_MAPPING,
            "partial_unlock_counts_in_headline":
                PARTIAL_UNLOCK_COUNTS_IN_HEADLINE,
            "proxy_may_close_a_native_frontier":
                PROXY_MAY_CLOSE_A_NATIVE_FRONTIER,
            "continuous_only_is_full_credit": CONTINUOUS_ONLY_IS_FULL_CREDIT,
            "expected_unlocks_are_not_measured_unlocks":
                EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS,
            "unlock_becomes_measured_only_after_entitlement_activation":
                UNLOCK_BECOMES_MEASURED_ONLY_AFTER_ENTITLEMENT_ACTIVATION,
            "unlock_measurement_note": UNLOCK_MEASUREMENT_NOTE,
        },
        "acquisition_decision": {
            "owner": ACQUISITION_DECISION_OWNER,
            "context": ACQUISITION_DECISION_CONTEXT,
            "post_acquisition_context": POST_ACQUISITION_DECISION_CONTEXT,
            "r37_defines_its_own_acquisition_authority":
                R37_DEFINES_ITS_OWN_ACQUISITION_AUTHORITY,
            "r37_states_are_triage_labels": R37_STATES_ARE_TRIAGE_LABELS,
            "r37_may_recommend_what_the_canonical_gate_refused":
                R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED,
            "acquisition_recommendation_is_alpha_evidence":
                ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE,
            "acquisition_recommendation_is_integration_approval":
                ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL,
            "acquisition_recommendation_is_purchase_authority":
                ACQUISITION_RECOMMENDATION_IS_PURCHASE_AUTHORITY,
            "acquisition_requires_manual_operator_approval":
                ACQUISITION_REQUIRES_MANUAL_OPERATOR_APPROVAL,
        },
        "evidence_classes": list(EVIDENCE_CLASSES),
        "prohibited_substitutions": list(PROHIBITED_SUBSTITUTIONS),
        "gate_states": list(GATE_STATES),
        "purchase_authority_granted_by_this_release":
            PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE,
        "every_candidate_must_be_terminal": EVERY_CANDIDATE_MUST_BE_TERMINAL,
        "futures_required_fields": list(FUTURES_REQUIRED_FIELDS),
        "futures_desired_fields": list(FUTURES_DESIRED_FIELDS),
        "futures_min_history_years_preferred":
            FUTURES_MIN_HISTORY_YEARS_PREFERRED,
        "futures_min_history_years_floor": FUTURES_MIN_HISTORY_YEARS_FLOOR,
        "analyst_requirements": {
            "min_history_years": ANALYST_MIN_HISTORY_YEARS,
            "min_issuers": ANALYST_MIN_ISSUERS,
            "requires_inactive_coverage": ANALYST_REQUIRES_INACTIVE_COVERAGE,
            "current_snapshot_is_not_history":
                ANALYST_CURRENT_SNAPSHOT_IS_NOT_HISTORY,
        },
        "scoring": {
            "formula": SCORE_FORMULA,
            "cost_amortisation_years": COST_AMORTISATION_YEARS,
            "free_cost_floor_usd": FREE_COST_FLOOR_USD,
            "native_structure_factor": dict(NATIVE_STRUCTURE_FACTOR),
            "continuous_only_factor": CONTINUOUS_ONLY_FACTOR,
            "pit_factor": dict(PIT_FACTOR),
            "survivorship_factor": dict(SURVIVORSHIP_FACTOR),
            "licence_factor": dict(LICENCE_FACTOR),
            "identity_factor": dict(IDENTITY_FACTOR),
            "opacity_factor": dict(OPACITY_FACTOR),
            "history_reference_years": HISTORY_REFERENCE_YEARS,
            "breadth_base": BREADTH_BASE,
            "breadth_per_asset_class": BREADTH_PER_ASSET_CLASS,
            "score_is_a_ranking_aid_not_an_optimiser":
                SCORE_IS_A_RANKING_AID_NOT_AN_OPTIMISER,
            "score_may_override_a_hard_gate": SCORE_MAY_OVERRIDE_A_HARD_GATE,
        },
        "free_routes": dict(FREE_ROUTES),
        "block_confirmation_routes": dict(BLOCK_CONFIRMATION_ROUTES),
        "result_names": list(RESULT_NAMES),
        "alpha_result_is_structurally_not_tested":
            ALPHA_RESULT_IS_STRUCTURALLY_NOT_TESTED,
        "alpha_result": alpha_result(),
        "alpha_result_reason": ALPHA_RESULT_REASON,
        "verdicts": list(VERDICTS),
    }
    body = r37.artifact_body(CONTRACT_SCHEMA, payload)
    body["contract_hash"] = r37.sha(payload)
    return body


def path_for(campaign_id: str = CAMPAIGN_ID) -> Path:
    return r37.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r37.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(path_for(campaign_id))


__all__ = [
    "CALCULATION_OWNER", "CONTRACT_SCHEMA", "CAMPAIGN_ID", "INHERITED",
    "SUPERSEDED_CAMPAIGNS",
    "SETTLED_AND_NOT_REOPENED", "MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
    "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_CHANGE_SUBSCRIPTION_TIER",
    "MAY_ACCEPT_LICENCE_AGREEMENT", "MAY_SUBMIT_PAYMENT_DETAILS",
    "MAY_PURCHASE_CLOUD_COMPUTE", "MAY_INSTALL_CUDA",
    "MAY_DOWNLOAD_MODEL_WEIGHTS", "ML_TRAINING_CAMPAIGN_IN_SCOPE",
    "MARKET_STRUCTURE_EXPERIMENT_IN_SCOPE", "PERMITTED_ACQUISITION",
    "REQUIRES_OPERATOR_ACTION", "A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT",
    "VENDOR_CLAIM_ALONE_MAY_UNLOCK_A_CELL",
    "UNLOCK_REQUIRES_DECLARED_INSTRUMENT_MAPPING",
    "PARTIAL_UNLOCK_COUNTS_IN_HEADLINE",
    "EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS",
    "UNLOCK_BECOMES_MEASURED_ONLY_AFTER_ENTITLEMENT_ACTIVATION",
    "UNLOCK_MEASUREMENT_NOTE", "ACQUISITION_DECISION_OWNER",
    "ACQUISITION_DECISION_CONTEXT", "POST_ACQUISITION_DECISION_CONTEXT",
    "R37_DEFINES_ITS_OWN_ACQUISITION_AUTHORITY", "R37_STATES_ARE_TRIAGE_LABELS",
    "R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED",
    "ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE",
    "ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL",
    "ACQUISITION_RECOMMENDATION_IS_PURCHASE_AUTHORITY",
    "ACQUISITION_REQUIRES_MANUAL_OPERATOR_APPROVAL",
    "PROHIBITED_SUBSTITUTIONS",
    "EVIDENCE_CLASSES", "EVIDENCE_VENDOR_PAGE", "EVIDENCE_VENDOR_QUOTE",
    "EVIDENCE_LIVE_PROBE", "EVIDENCE_LOCAL_CLIENT",
    "EVIDENCE_SAMPLE_VALIDATED", "EVIDENCE_PRIOR_RELEASE", "GATE_STATES",
    "STATE_BUY_RECOMMENDED", "STATE_SAMPLE_REQUIRED", "STATE_SAMPLE_PASSED",
    "STATE_SAMPLE_FAILED", "STATE_ENTITLEMENT_OWNED",
    "STATE_FREE_ACQUIRE_NOW", "STATE_NO_LOW_VALUE", "STATE_NO_PIT",
    "STATE_NO_SURVIVORSHIP", "STATE_NO_HISTORY", "STATE_NO_LICENCE",
    "STATE_NO_COST_VALUE", "STATE_BLOCKED_CONTACT", "purchase_authority",
    "SCORE_FORMULA", "COST_AMORTISATION_YEARS", "FREE_COST_FLOOR_USD",
    "NATIVE_STRUCTURE_FACTOR", "CONTINUOUS_ONLY_FACTOR", "PIT_FACTOR",
    "SURVIVORSHIP_FACTOR", "LICENCE_FACTOR", "IDENTITY_FACTOR",
    "OPACITY_FACTOR", "HISTORY_REFERENCE_YEARS", "BREADTH_BASE",
    "BREADTH_PER_ASSET_CLASS", "FREE_ROUTES", "BLOCK_CONFIRMATION_ROUTES",
    "FUTURES_REQUIRED_FIELDS", "FUTURES_DESIRED_FIELDS",
    "CONTINUOUS_ONLY_IS_FULL_CREDIT", "RESULT_NAMES", "alpha_result",
    "VERDICTS", "VERDICT_RECOMMENDED", "VERDICT_NO_INVESTMENT",
    "VERDICT_BLOCKED", "LEVEL_SIGNAL", "LEVEL_PROXY", "LEVEL_NATIVE",
    "LEVELS", "build", "freeze", "load", "path_for",
]
