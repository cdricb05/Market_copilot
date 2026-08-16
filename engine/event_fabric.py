r"""Release 28 — the canonical NORMALIZED EVENT contract (PURE kernel).

WHY THIS EXISTS
---------------
Before Release 28 the Paper Trader recomputed the SAME unchanged information on a
timer and asked the same question of it. Every ingredient of an information-driven
manager already existed — a Stage-2 point-in-time record store with real source
timestamps, a canonical freshness owner, a holding opportunity-cost engine, an
economic-change gate and a proposal engine — but nothing connected "new information
arrived" to "reassess the portfolio". This kernel is that missing vocabulary.

It answers three questions about ONE piece of arriving information, and nothing else:

    1. WHAT KIND OF INFORMATION IS IT?   -> family + signal speed
    2. WHAT IS IT ALLOWED TO DECIDE?     -> decision authority
    3. WHAT DOES IT INVALIDATE?          -> business concepts -> calculations

THE THREE SIGNAL SPEEDS
-----------------------
``STRUCTURAL``   what we fundamentally want to own. Changes when economically
                 relevant fundamental information changes (a 10-Q, a new fact).
``TACTICAL``     has something happened that could change the thesis? (8-K, news,
                 earnings, insider activity). An event is NOT automatically alpha.
``MARKET_RISK``  even if the thesis is unchanged, is the AMOUNT of capital still
                 appropriate? (price, volatility, liquidity, drawdown, tradability).

DECISION AUTHORITY IS THE SAFETY BOUNDARY
-----------------------------------------
An unvalidated news headline may legitimately cause the manager to LOOK AGAIN. It may
never silently add or subtract expected alpha. That distinction is enforced here, in
one table, rather than being re-argued at each call site::

    OPERATIONAL_ALPHA    may change the operational expected-return ranking
    RESEARCH_ALPHA       tracked and compared; may NEVER touch the operational target
    OPERATIONAL_RISK     may change valuation / risk state under existing policy
    EVENT_TRIGGER_ONLY   may trigger a reassessment; may NEVER change a score
    OBSERVABILITY_ONLY   recorded and shown; decides nothing
    BLOCKED              adapter exists, data does not qualify (entitlement/PIT)

PURITY
------
This module is a pure function of its arguments. It performs NO IO, reads no clock,
opens no store, touches no network, imports no ``api.*`` module and writes nothing.
It creates no order, approves nothing and promotes no model.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable, Optional

PHASE = "RELEASE28"
CALCULATION_OWNER = "engine.event_fabric"
EVENT_SCHEMA_VERSION = "1.0.0"
EVENT_CONTRACT_ID = "paper_trader.normalized_event/1"
#: Bumped whenever the family/authority table below changes meaning. Persisted on
#: every event so an old event is never reinterpreted under a new policy.
AUTHORITY_POLICY_VERSION = "event_authority.v1"
EXTRACTOR_VERSION = "release28.extractor.v1"

# --------------------------------------------------------------------------- #
# Frozen vocabularies (part of the tested contract)
# --------------------------------------------------------------------------- #
SPEED_STRUCTURAL = "STRUCTURAL"
SPEED_TACTICAL = "TACTICAL"
SPEED_MARKET_RISK = "MARKET_RISK"
SIGNAL_SPEEDS = (SPEED_STRUCTURAL, SPEED_TACTICAL, SPEED_MARKET_RISK)

AUTH_OPERATIONAL_ALPHA = "OPERATIONAL_ALPHA"
AUTH_RESEARCH_ALPHA = "RESEARCH_ALPHA"
AUTH_OPERATIONAL_RISK = "OPERATIONAL_RISK"
AUTH_EVENT_TRIGGER_ONLY = "EVENT_TRIGGER_ONLY"
AUTH_OBSERVABILITY_ONLY = "OBSERVABILITY_ONLY"
AUTH_BLOCKED = "BLOCKED"
SIGNAL_AUTHORITIES = (AUTH_OPERATIONAL_ALPHA, AUTH_RESEARCH_ALPHA, AUTH_OPERATIONAL_RISK,
                      AUTH_EVENT_TRIGGER_ONLY, AUTH_OBSERVABILITY_ONLY, AUTH_BLOCKED)

#: The ONLY authority permitted to move the operational expected-return ranking.
ALPHA_BEARING_AUTHORITIES = frozenset({AUTH_OPERATIONAL_ALPHA})
#: The ONLY authority permitted to move operational valuation / risk state.
RISK_BEARING_AUTHORITIES = frozenset({AUTH_OPERATIONAL_ALPHA, AUTH_OPERATIONAL_RISK})
#: Authorities permitted to REQUEST a reassessment (looking again is always safe).
TRIGGER_BEARING_AUTHORITIES = frozenset({AUTH_OPERATIONAL_ALPHA, AUTH_OPERATIONAL_RISK,
                                         AUTH_EVENT_TRIGGER_ONLY})
#: Authority that may never reach ANY operational calculation.
NON_OPERATIONAL_AUTHORITIES = frozenset({AUTH_RESEARCH_ALPHA, AUTH_OBSERVABILITY_ONLY,
                                         AUTH_BLOCKED})

NOV_NEW = "NEW"
NOV_DUPLICATE = "DUPLICATE"
NOV_SYNDICATED = "SYNDICATED"
NOV_FOLLOW_UP = "FOLLOW_UP"
NOV_CORRECTION = "CORRECTION"
NOV_MATERIAL_UPDATE = "MATERIAL_UPDATE"
NOV_RETRACTION = "RETRACTION"
NOVELTY_STATES = (NOV_NEW, NOV_DUPLICATE, NOV_SYNDICATED, NOV_FOLLOW_UP,
                  NOV_CORRECTION, NOV_MATERIAL_UPDATE, NOV_RETRACTION)
#: Novelty states that carry genuinely NEW information into the decision path.
INFORMATIVE_NOVELTY = frozenset({NOV_NEW, NOV_CORRECTION, NOV_MATERIAL_UPDATE,
                                 NOV_RETRACTION})

#: Terminal source-integration states. There is deliberately NO
#: "AVAILABLE_BUT_NOT_INTEGRATED" / "GOOD_NEXT_RELEASE" / "READY_TO_BUILD_LATER":
#: a source that is useful, authorized and executable is integrated, not deferred.
TERM_INTEGRATED_OPERATIONAL = "INTEGRATED_OPERATIONAL"
TERM_INTEGRATED_TRIGGER_ONLY = "INTEGRATED_TRIGGER_ONLY"
TERM_INTEGRATED_RESEARCH_ONLY = "INTEGRATED_RESEARCH_ONLY"
TERM_REDUNDANT = "REDUNDANT_WITH_EXISTING_SOURCE"
TERM_BLOCKED_ENTITLEMENT = "BLOCKED_ENTITLEMENT"
TERM_BLOCKED_DATA_QUALITY = "BLOCKED_DATA_QUALITY"
TERM_BLOCKED_PIT = "BLOCKED_PIT"
TERM_BLOCKED_LICENSE = "BLOCKED_LICENSE"
TERM_NOT_ECONOMICALLY_USEFUL = "NOT_ECONOMICALLY_USEFUL"
TERMINAL_SOURCE_STATES = (
    TERM_INTEGRATED_OPERATIONAL, TERM_INTEGRATED_TRIGGER_ONLY,
    TERM_INTEGRATED_RESEARCH_ONLY, TERM_REDUNDANT, TERM_BLOCKED_ENTITLEMENT,
    TERM_BLOCKED_DATA_QUALITY, TERM_BLOCKED_PIT, TERM_BLOCKED_LICENSE,
    TERM_NOT_ECONOMICALLY_USEFUL,
)
INTEGRATED_TERMINAL_STATES = frozenset({
    TERM_INTEGRATED_OPERATIONAL, TERM_INTEGRATED_TRIGGER_ONLY,
    TERM_INTEGRATED_RESEARCH_ONLY})
#: States that are NOT terminal and must never appear in a released capability matrix.
FORBIDDEN_NON_TERMINAL_STATES = ("AVAILABLE_BUT_NOT_INTEGRATED", "GOOD_NEXT_RELEASE",
                                 "READY_TO_BUILD_LATER", "TODO", "PLANNED")

# --------------------------------------------------------------------------- #
# Business concepts — the dependency-graph nodes an event can invalidate.
# --------------------------------------------------------------------------- #
C_MARK = "mark"
C_RETURN = "return"
C_VOLATILITY = "volatility"
C_LIQUIDITY = "liquidity"
C_DRAWDOWN = "drawdown"
C_RISK_CONTRIBUTION = "risk_contribution"
C_CONCENTRATION = "concentration"
C_TRADABILITY = "tradability"
C_FUNDAMENTAL_INPUT = "fundamental_input"
C_STRUCTURAL_ALPHA = "structural_alpha"
C_RESEARCH_CHALLENGER = "research_challenger"
C_THESIS_REVIEW = "thesis_review"
C_CORPORATE_ACTION = "corporate_action"
C_UNIVERSE_MEMBERSHIP = "universe_membership"
C_SECURITY_IDENTITY = "security_identity"
C_REGIME = "regime"
C_SHORT_ACTIVITY = "short_activity"
BUSINESS_CONCEPTS = (
    C_MARK, C_RETURN, C_VOLATILITY, C_LIQUIDITY, C_DRAWDOWN, C_RISK_CONTRIBUTION,
    C_CONCENTRATION, C_TRADABILITY, C_FUNDAMENTAL_INPUT, C_STRUCTURAL_ALPHA,
    C_RESEARCH_CHALLENGER, C_THESIS_REVIEW, C_CORPORATE_ACTION,
    C_UNIVERSE_MEMBERSHIP, C_SECURITY_IDENTITY, C_REGIME, C_SHORT_ACTIVITY,
)

# --------------------------------------------------------------------------- #
# Calculations — the dependency-graph leaves. Each names its EXISTING owner; this
# release adds no second engine for any of them.
# --------------------------------------------------------------------------- #
CALC_MARKET_RISK_STATE = "MARKET_RISK_STATE"
CALC_PORTFOLIO_VALUATION = "PORTFOLIO_VALUATION"
CALC_UNIVERSE_SCORING = "UNIVERSE_SCORING"
CALC_HOLDING_OPPORTUNITY_COST = "HOLDING_OPPORTUNITY_COST"
CALC_PORTFOLIO_REASSESSMENT = "PORTFOLIO_REASSESSMENT"
CALC_REALLOCATION_PROPOSAL = "REALLOCATION_PROPOSAL"
CALC_RESEARCH_EVIDENCE = "RESEARCH_EVIDENCE"

#: Canonical owner of every calculation the graph can reach. The event lane calls
#: these owners; it never reimplements one.
CALCULATION_OWNERS = {
    CALC_MARKET_RISK_STATE: "api.price_panel",
    CALC_PORTFOLIO_VALUATION: "api.portfolio_valuation",
    CALC_UNIVERSE_SCORING: "api.universe_scoring",
    CALC_HOLDING_OPPORTUNITY_COST: "api.holding_opportunity_cost",
    CALC_PORTFOLIO_REASSESSMENT: "api.portfolio_reassessment",
    CALC_REALLOCATION_PROPOSAL: "api.reallocation_proposal",
    CALC_RESEARCH_EVIDENCE: "api.research_agent",
}
#: Execution order. A downstream calculation never runs before its inputs.
CALCULATION_ORDER = (
    CALC_MARKET_RISK_STATE, CALC_PORTFOLIO_VALUATION, CALC_UNIVERSE_SCORING,
    CALC_HOLDING_OPPORTUNITY_COST, CALC_PORTFOLIO_REASSESSMENT,
    CALC_REALLOCATION_PROPOSAL, CALC_RESEARCH_EVIDENCE,
)

#: concept -> (signals it feeds, calculations that must refresh). This is the WHOLE
#: dependency map; ``affected_calculations`` is a pure projection of it.
CONCEPT_DEPENDENCIES: dict[str, dict] = {
    C_MARK: {"signals": ["owned_mark"],
             "calculations": [CALC_MARKET_RISK_STATE, CALC_PORTFOLIO_VALUATION]},
    C_RETURN: {"signals": ["trailing_return", "relative_strength"],
               "calculations": [CALC_MARKET_RISK_STATE, CALC_PORTFOLIO_VALUATION]},
    C_VOLATILITY: {"signals": ["realized_volatility", "downside_deviation"],
                   "calculations": [CALC_MARKET_RISK_STATE,
                                    CALC_HOLDING_OPPORTUNITY_COST]},
    C_LIQUIDITY: {"signals": ["median_dollar_volume"],
                  "calculations": [CALC_MARKET_RISK_STATE,
                                   CALC_HOLDING_OPPORTUNITY_COST]},
    C_DRAWDOWN: {"signals": ["max_drawdown"],
                 "calculations": [CALC_MARKET_RISK_STATE,
                                  CALC_HOLDING_OPPORTUNITY_COST]},
    C_RISK_CONTRIBUTION: {"signals": ["portfolio_risk_contribution"],
                          "calculations": [CALC_MARKET_RISK_STATE,
                                           CALC_HOLDING_OPPORTUNITY_COST]},
    C_CONCENTRATION: {"signals": ["weight_concentration"],
                      "calculations": [CALC_MARKET_RISK_STATE,
                                       CALC_PORTFOLIO_REASSESSMENT]},
    C_TRADABILITY: {"signals": ["halt_state"],
                    "calculations": [CALC_MARKET_RISK_STATE,
                                     CALC_HOLDING_OPPORTUNITY_COST]},
    C_FUNDAMENTAL_INPUT: {"signals": ["fundamental_panel"],
                          "calculations": [CALC_UNIVERSE_SCORING,
                                           CALC_HOLDING_OPPORTUNITY_COST]},
    C_STRUCTURAL_ALPHA: {"signals": ["composite_sn", "fundamental_momentum_50_50_v1"],
                         "calculations": [CALC_UNIVERSE_SCORING,
                                          CALC_HOLDING_OPPORTUNITY_COST,
                                          CALC_PORTFOLIO_REASSESSMENT]},
    C_RESEARCH_CHALLENGER: {"signals": ["s25_operating_profitability"],
                            "calculations": [CALC_RESEARCH_EVIDENCE]},
    C_THESIS_REVIEW: {"signals": [],
                      "calculations": [CALC_HOLDING_OPPORTUNITY_COST,
                                       CALC_PORTFOLIO_REASSESSMENT]},
    C_CORPORATE_ACTION: {"signals": ["split_adjustment"],
                         "calculations": [CALC_PORTFOLIO_VALUATION,
                                          CALC_HOLDING_OPPORTUNITY_COST]},
    C_UNIVERSE_MEMBERSHIP: {"signals": ["eligible_universe"],
                            "calculations": [CALC_UNIVERSE_SCORING]},
    C_SECURITY_IDENTITY: {"signals": ["identity_map"], "calculations": []},
    C_REGIME: {"signals": ["volatility_state"], "calculations": [CALC_MARKET_RISK_STATE]},
    C_SHORT_ACTIVITY: {"signals": [], "calculations": []},
}

# --------------------------------------------------------------------------- #
# THE ONE EVENT-FAMILY TABLE.
#
# Every family declares its speed, its decision authority, WHY it has that authority,
# and the business concepts it invalidates. Nothing else in the system is permitted to
# grant an event more authority than this table gives it.
# --------------------------------------------------------------------------- #
def _fam(family, *, record_types, speed, authority, concepts, why, event_types=(),
         event_type_prefixes=(), excluded_event_types=(), catch_all=False,
         cadence="EVENT_DRIVEN"):
    return {
        "family": family,
        "record_types": tuple(record_types),
        "event_types": tuple(event_types),
        "event_type_prefixes": tuple(event_type_prefixes),
        "excluded_event_types": tuple(excluded_event_types),
        "catch_all": bool(catch_all),
        "signal_speed": speed,
        "decision_authority": authority,
        "concepts": tuple(concepts),
        "cadence": cadence,
        "why_authority": why,
    }


#: Structural financial reports: the ONLY filings that make new fundamental
#: information available, and therefore the only ones that may move a score.
F_STRUCTURAL_REPORT = "structural_financial_report"
F_MATERIAL_CORPORATE_EVENT = "material_corporate_event"
F_INSIDER_TRANSACTION = "insider_transaction"
F_OTHER_FILING = "other_filing"
F_EARNINGS_RESULT = "earnings_result"
F_GUIDANCE_CHANGE = "guidance_change"
F_FUNDAMENTAL_FACT = "fundamental_fact"
F_ANALYST_SNAPSHOT = "analyst_estimate_snapshot"
F_ANALYST_REVISION = "analyst_revision_as_was"
F_COMPANY_NEWS = "company_news"
F_REGULATORY_EVENT = "regulatory_event"
F_PRESS_RELEASE = "company_press_release"
F_MARKET_BAR = "market_bar"
F_MARKET_QUOTE = "market_quote"
F_CORPORATE_ACTION = "corporate_action"
F_TRADING_HALT = "trading_halt"
F_SHORT_VOLUME = "short_volume"
F_UNIVERSE_MEMBERSHIP = "universe_membership"
F_SECURITY_IDENTITY = "security_identity"
F_MACRO_REGIME = "macro_regime_release"
F_MACRO_CONTEXT = "macro_context_release"

#: FRED/ALFRED series whose movement the existing risk surface already interprets as a
#: market-regime state. Every OTHER macro series is context only — Stage 15 measured no
#: defensible cross-sectional macro alpha, so a CPI print may not rank a stock.
REGIME_MACRO_SERIES = ("VIXCLS", "NFCI", "BAMLH0A0HYM2", "BAMLC0A0CM", "T10Y2Y")
REGIME_MACRO_FAMILIES = ("volatility_regime", "liquidity_financial_conditions",
                         "credit_conditions", "yield_curve")

#: Filing form types that carry genuinely NEW structural fundamentals.
STRUCTURAL_FORMS = ("10-Q", "10-Q/A", "10-K", "10-K/A", "20-F", "20-F/A", "40-F")
#: Filing form types that are material corporate EVENTS (trigger, never alpha).
MATERIAL_EVENT_FORMS = ("8-K", "8-K/A", "6-K")
INSIDER_FORMS = ("3", "3/A", "4", "4/A", "5", "5/A")

EVENT_FAMILY_TABLE: tuple[dict, ...] = (
    # ---------------- MARKET / RISK (fast) --------------------------------- #
    _fam(F_MARKET_BAR, record_types=("MARKET_BAR",), event_types=("EOD_BAR",),
         catch_all=True,
         speed=SPEED_MARKET_RISK, authority=AUTH_OPERATIONAL_RISK,
         concepts=(C_MARK, C_RETURN, C_VOLATILITY, C_LIQUIDITY, C_DRAWDOWN,
                   C_RISK_CONTRIBUTION, C_CONCENTRATION),
         cadence="DAILY",
         why=("Owned end-of-day bars are the canonical operational mark. Valuation and "
              "risk may always be recomputed from them; the released model's formation "
              "cadence decides whether a SCORE may move, not the arrival of a bar.")),
    _fam(F_MARKET_QUOTE, record_types=("MARKET_QUOTE",),
         event_types=("INTRADAY_QUOTE", "DELAYED_QUOTE"), catch_all=True,
         speed=SPEED_MARKET_RISK, authority=AUTH_OPERATIONAL_RISK,
         concepts=(C_MARK, C_RETURN, C_DRAWDOWN, C_RISK_CONTRIBUTION),
         cadence="INTRADAY",
         why=("A faster-than-daily quote may update valuation and risk state. It may "
              "NOT update an expected-return score: no released signal contract is "
              "formed at intraday frequency, so an intraday feature is not alpha "
              "merely because it exists.")),
    _fam(F_TRADING_HALT, record_types=("TRADING_HALT",),
         event_type_prefixes=("TRADING_HALT",), catch_all=True,
         speed=SPEED_MARKET_RISK, authority=AUTH_OPERATIONAL_RISK,
         concepts=(C_TRADABILITY,), cadence="EVENT_DRIVEN",
         why=("A halt changes whether a position can be traded at all, which is a "
              "risk/tradability fact under existing policy, not a view on value.")),
    _fam(F_CORPORATE_ACTION, record_types=("CORPORATE_ACTION",),
         speed=SPEED_STRUCTURAL, authority=AUTH_OPERATIONAL_RISK,
         concepts=(C_CORPORATE_ACTION, C_MARK), cadence="EVENT_DRIVEN",
         why=("Splits and dividends change share counts and marks arithmetically. The "
              "canonical corporate-action registry owns the adjustment; this event only "
              "reports that one is outstanding.")),
    # ---------------- STRUCTURAL (slow) ------------------------------------ #
    _fam(F_STRUCTURAL_REPORT, record_types=("FILING_EVENT", "INSIDER_FILING"),
         event_types=STRUCTURAL_FORMS,
         speed=SPEED_STRUCTURAL, authority=AUTH_OPERATIONAL_ALPHA,
         concepts=(C_FUNDAMENTAL_INPUT, C_STRUCTURAL_ALPHA), cadence="QUARTERLY",
         why=("A periodic report is the moment genuinely new fundamental information "
              "becomes public. The operational model is built on exactly this "
              "information, so it may move a score — through the canonical scoring "
              "owner, at the model's own formation cadence.")),
    _fam(F_FUNDAMENTAL_FACT, record_types=("FUNDAMENTAL_FACT",),
         excluded_event_types=("ANALYST_PRICE_TARGET_VINTAGE", "ANALYST_ESTIMATE_VINTAGE",
                               "ANALYST_RATING_VINTAGE"),
         speed=SPEED_STRUCTURAL, authority=AUTH_OPERATIONAL_ALPHA,
         concepts=(C_FUNDAMENTAL_INPUT, C_STRUCTURAL_ALPHA), cadence="QUARTERLY",
         why=("A filed accounting fact is a direct input to the released fundamental "
              "leg. Point-in-time by construction: read from the filed date, never "
              "back-dated.")),
    _fam(F_UNIVERSE_MEMBERSHIP, record_types=("UNIVERSE_MEMBERSHIP",),
         speed=SPEED_STRUCTURAL, authority=AUTH_OPERATIONAL_RISK,
         concepts=(C_UNIVERSE_MEMBERSHIP,), cadence="EVENT_DRIVEN",
         why=("Index membership decides ELIGIBILITY, not desirability. It changes which "
              "names may be scored; it expresses no view on any of them.")),
    _fam(F_SECURITY_IDENTITY, record_types=("SECURITY_IDENTITY",),
         speed=SPEED_STRUCTURAL, authority=AUTH_OBSERVABILITY_ONLY,
         concepts=(C_SECURITY_IDENTITY,), cadence="STATIC",
         why=("Identity/classification is observed metadata used to MAP events to "
              "securities. It decides nothing on its own.")),
    # ---------------- TACTICAL / CATALYST ---------------------------------- #
    _fam(F_MATERIAL_CORPORATE_EVENT, record_types=("FILING_EVENT", "INSIDER_FILING"),
         event_types=MATERIAL_EVENT_FORMS,
         speed=SPEED_TACTICAL, authority=AUTH_EVENT_TRIGGER_ONLY,
         concepts=(C_THESIS_REVIEW,), cadence="EVENT_DRIVEN",
         why=("An 8-K says something happened that management considered material. No "
              "validated signal maps its content to expected return, so it earns a "
              "REVIEW of the affected holding and nothing more.")),
    _fam(F_INSIDER_TRANSACTION, record_types=("INSIDER_FILING", "FILING_EVENT"),
         event_types=INSIDER_FORMS,
         speed=SPEED_TACTICAL, authority=AUTH_EVENT_TRIGGER_ONLY,
         concepts=(C_THESIS_REVIEW,), cadence="EVENT_DRIVEN",
         why=("Form 4 activity is a catalyst worth looking at. The project has never "
              "validated an insider-activity signal, so it may not score.")),
    # The earnings lane is the record-type DEFAULT (``catch_all``) so that every way an
    # earnings release actually arrives is captured: the vendor calendar
    # (EARNINGS_REPORT) and the SEC 8-K Item 2.02 results release
    # (``8-K_ITEM_2.02``), which the collector emits as an EARNINGS_EVENT. A guidance
    # sub-type still wins on its prefix.
    _fam(F_EARNINGS_RESULT, record_types=("EARNINGS_EVENT",),
         event_types=("EARNINGS_REPORT", "EARNINGS_RESULT"), catch_all=True,
         speed=SPEED_TACTICAL, authority=AUTH_EVENT_TRIGGER_ONLY,
         concepts=(C_THESIS_REVIEW,), cadence="QUARTERLY",
         why=("An earnings result is the single most reliable moment for a thesis to "
              "break. Stage 13C measured that the sales-surprise signal did NOT "
              "replicate out of sample, so capture and trigger — never score.")),
    _fam(F_GUIDANCE_CHANGE, record_types=("EARNINGS_EVENT",),
         event_type_prefixes=("GUIDANCE",),
         speed=SPEED_TACTICAL, authority=AUTH_EVENT_TRIGGER_ONLY,
         concepts=(C_THESIS_REVIEW,), cadence="EVENT_DRIVEN",
         why=("Guidance raised/lowered/withdrawn is a thesis fact. No validated "
              "guidance signal exists, so it triggers review only.")),
    _fam(F_COMPANY_NEWS, record_types=("NEWS_EVENT",),
         speed=SPEED_TACTICAL, authority=AUTH_EVENT_TRIGGER_ONLY,
         concepts=(C_THESIS_REVIEW,), cadence="EVENT_DRIVEN",
         why=("News awareness is real value: material news identifies WHICH holding to "
              "reassess. Positive headline = buy is not implemented and is not "
              "permitted, because no historically validated news signal exists.")),
    _fam(F_PRESS_RELEASE, record_types=("PRESS_RELEASE",),
         speed=SPEED_TACTICAL, authority=AUTH_EVENT_TRIGGER_ONLY,
         concepts=(C_THESIS_REVIEW,), cadence="EVENT_DRIVEN",
         why=("A company release is first-party, higher-confidence news. Same "
              "authority: it may direct attention, not scoring.")),
    _fam(F_REGULATORY_EVENT, record_types=("REGULATORY_EVENT",),
         speed=SPEED_TACTICAL, authority=AUTH_EVENT_TRIGGER_ONLY,
         concepts=(C_THESIS_REVIEW,), cadence="EVENT_DRIVEN",
         why=("Official regulator/enforcement/recall feeds are high-quality catalyst "
              "evidence for a named company. Trigger-only for the same reason as news.")),
    # ---------------- MACRO ------------------------------------------------- #
    _fam(F_MACRO_REGIME, record_types=("MACRO_OBSERVATION",),
         speed=SPEED_MARKET_RISK, authority=AUTH_OPERATIONAL_RISK,
         concepts=(C_REGIME,), cadence="DAILY",
         why=("Volatility, credit-spread, financial-conditions and curve series are the "
              "inputs the existing risk surface already reads as a regime state. A new "
              "observation refreshes the CONTEXT; only a regime STATE TRANSITION is "
              "material.")),
    _fam(F_MACRO_CONTEXT, record_types=("MACRO_OBSERVATION",),
         speed=SPEED_MARKET_RISK, authority=AUTH_OBSERVABILITY_ONLY,
         concepts=(), cadence="MONTHLY",
         why=("Stage 15 measured no defensible cross-sectional macro alpha (0 FDR "
              "survivors). A CPI or payroll print is therefore recorded and shown, and "
              "may not rank or re-weight a single stock.")),
    # ---------------- RESEARCH-ONLY / BLOCKED ------------------------------- #
    _fam(F_ANALYST_SNAPSHOT, record_types=("FUNDAMENTAL_FACT",),
         event_type_prefixes=("ANALYST_",),
         speed=SPEED_STRUCTURAL, authority=AUTH_RESEARCH_ALPHA,
         concepts=(C_RESEARCH_CHALLENGER,), cadence="DAILY",
         why=("A prospective vendor SNAPSHOT is leakage-safe going forward but is not "
              "an as-was revision vintage. It accumulates as research evidence and may "
              "never touch the operational target.")),
    _fam(F_ANALYST_REVISION, record_types=("ANALYST_REVISION",),
         speed=SPEED_STRUCTURAL, authority=AUTH_BLOCKED,
         concepts=(), cadence="EVENT_DRIVEN",
         why=("Genuine as-was consensus-revision vintages are not present in any "
              "approved local root and remain provider-blocked. The adapter contract "
              "exists so adding the data later needs no new portfolio architecture; "
              "until then the family is BLOCKED and admits nothing.")),
    _fam(F_SHORT_VOLUME, record_types=("SHORT_VOLUME",),
         speed=SPEED_MARKET_RISK, authority=AUTH_OBSERVABILITY_ONLY,
         concepts=(C_SHORT_ACTIVITY,), cadence="DAILY",
         why=("FINRA publishes daily short-sale VOLUME, not short interest. The tested "
              "short-activity signal failed (t=1.56), so this is observability only.")),
    # ---------------- CATCH-ALL FILINGS ------------------------------------- #
    _fam(F_OTHER_FILING, record_types=("FILING_EVENT", "INSIDER_FILING"),
         speed=SPEED_TACTICAL, authority=AUTH_OBSERVABILITY_ONLY,
         concepts=(), cadence="EVENT_DRIVEN", catch_all=True,
         why=("Registration statements, prospectuses, proxies and free-writing "
              "prospectuses are recorded for completeness. None maps to a validated "
              "signal and none is a reliable thesis catalyst, so none decides anything.")),
)

EVENT_FAMILIES: dict[str, dict] = {f["family"]: f for f in EVENT_FAMILY_TABLE}


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _is_regime_macro(payload: Optional[dict]) -> bool:
    p = payload or {}
    series = _upper(p.get("series_id"))
    family = str(p.get("macro_family") or "").strip().lower()
    return series in REGIME_MACRO_SERIES or family in REGIME_MACRO_FAMILIES


def classify_event(*, record_type: Any, event_type: Any = None,
                   payload: Optional[dict] = None) -> dict:
    """Resolve ONE arriving record to its family, speed and decision authority.

    FAILS CLOSED. A record type this table does not know is returned with
    ``classified=False`` and ``OBSERVABILITY_ONLY`` authority, so an unrecognised feed
    can never acquire decision power by default — and the terminal audit counts it as
    an unclassified authority, which must be zero to release.
    """
    rt = _upper(record_type)
    et = _upper(event_type)

    # Macro splits on the SERIES, not on the record type: only the regime series the
    # existing risk surface already reads carry risk authority.
    if rt == "MACRO_OBSERVATION":
        fam = EVENT_FAMILIES[F_MACRO_REGIME if _is_regime_macro(payload)
                             else F_MACRO_CONTEXT]
        return _classification(fam, classified=True)

    exact: Optional[dict] = None
    prefix: Optional[dict] = None
    catch: Optional[dict] = None
    for fam in EVENT_FAMILY_TABLE:
        if rt not in fam["record_types"]:
            continue
        if et and et in {_upper(x) for x in fam["excluded_event_types"]}:
            continue
        if fam["event_types"] and et in {_upper(x) for x in fam["event_types"]}:
            exact = exact or fam
            continue
        if fam["event_type_prefixes"] and any(
                et.startswith(_upper(p)) for p in fam["event_type_prefixes"]):
            prefix = prefix or fam
            continue
        if fam["catch_all"]:
            catch = catch or fam
            continue
        if not fam["event_types"] and not fam["event_type_prefixes"]:
            # A family scoped by record type alone (e.g. NEWS_EVENT) matches when no
            # more specific family claimed the event type.
            catch = catch or fam
    chosen = exact or prefix or catch
    if chosen is None:
        return {
            "family": "unmapped_record_type",
            "signal_speed": SPEED_TACTICAL,
            "decision_authority": AUTH_OBSERVABILITY_ONLY,
            "concepts": (),
            "cadence": "EVENT_DRIVEN",
            "why_authority": ("No family claims this record type. FAIL CLOSED: it is "
                              "recorded, shown and permitted to decide nothing until it "
                              "is explicitly classified."),
            "classified": False,
            "authority_policy_version": AUTHORITY_POLICY_VERSION,
        }
    return _classification(chosen, classified=True)


def _classification(fam: dict, *, classified: bool) -> dict:
    return {
        "family": fam["family"],
        "signal_speed": fam["signal_speed"],
        "decision_authority": fam["decision_authority"],
        "concepts": tuple(fam["concepts"]),
        "cadence": fam["cadence"],
        "why_authority": fam["why_authority"],
        "classified": classified,
        "authority_policy_version": AUTHORITY_POLICY_VERSION,
    }


# --------------------------------------------------------------------------- #
# Authority guards — the safety boundary, expressed once.
# --------------------------------------------------------------------------- #
def authority_may_change_alpha(authority: Any) -> bool:
    """True only for the authority permitted to move the operational ranking."""
    return str(authority) in ALPHA_BEARING_AUTHORITIES


def authority_may_change_risk(authority: Any) -> bool:
    return str(authority) in RISK_BEARING_AUTHORITIES


def authority_may_trigger_reassessment(authority: Any) -> bool:
    return str(authority) in TRIGGER_BEARING_AUTHORITIES


def authority_touches_operational_target(authority: Any) -> bool:
    """False for RESEARCH_ALPHA / OBSERVABILITY_ONLY / BLOCKED — the research lane can
    never reach the operational target portfolio, whatever it measures."""
    return str(authority) not in NON_OPERATIONAL_AUTHORITIES


def unclassified_authority_count(events: Iterable[dict]) -> int:
    """How many events failed classification. Must be zero to release."""
    n = 0
    for e in (events or []):
        if not (e or {}).get("classified", False):
            n += 1
        elif str((e or {}).get("decision_authority")) not in SIGNAL_AUTHORITIES:
            n += 1
    return n


# --------------------------------------------------------------------------- #
# Deterministic identity + idempotency
# --------------------------------------------------------------------------- #
def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def content_fingerprint(payload: Any) -> str:
    return sha256_text(canonical_json(payload))


def idempotency_key(*, source_id: Any, record_type: Any, source_event_id: Any,
                    payload_fingerprint: Any) -> str:
    """The key that makes re-ingesting the SAME raw event a no-op.

    Deliberately excludes ingestion/observation time: the same filing retrieved twice
    on different days is one event, not two.
    """
    return sha256_text("|".join([
        str(source_id or ""), _upper(record_type), str(source_event_id or ""),
        str(payload_fingerprint or "")]))


def make_event_id(idem_key: str) -> str:
    return "evt_" + str(idem_key)[:32]


#: Contract-complete field list. Every persisted event carries exactly these keys.
EVENT_FIELDS = (
    "event_id", "event_schema_version", "event_contract_id", "idempotency_key",
    "source_id", "collector_id", "source_event_id", "source_family", "record_type",
    "event_type",
    "event_sub_type", "family", "signal_speed", "decision_authority",
    "authority_policy_version", "why_authority", "classified", "concepts",
    "cadence", "source_timestamp", "published_at", "accepted_at", "effective_at",
    "first_observed_at", "ingested_at", "entities", "primary_ticker",
    "identity_confidence", "source_quality", "event_quality", "materiality_inputs",
    "novelty", "novelty_reason", "duplicate_of", "supersedes", "superseded_by",
    "payload_fingerprint", "payload_reference", "extractor_version",
    "point_in_time_status", "quality_warnings",
)

PIT_OK = "POINT_IN_TIME_OK"
PIT_UNKNOWN_AVAILABILITY = "AVAILABILITY_TIMESTAMP_UNKNOWN"
PIT_SNAPSHOT_PROSPECTIVE = "PROSPECTIVE_SNAPSHOT_FORWARD_ONLY"
PIT_STATES = (PIT_OK, PIT_UNKNOWN_AVAILABILITY, PIT_SNAPSHOT_PROSPECTIVE)


def build_event(*, source_id: Any, record_type: Any, source_event_id: Any,
                payload: Optional[dict] = None, event_type: Any = None,
                event_sub_type: Any = None, source_family: Any = None,
                collector_id: Any = None,
                source_timestamp: Any = None, published_at: Any = None,
                accepted_at: Any = None, effective_at: Any = None,
                first_observed_at: Any = None, ingested_at: Any = None,
                entities: Any = None, primary_ticker: Any = None,
                identity_confidence: Any = None, source_quality: Any = None,
                event_quality: Any = None, materiality_inputs: Optional[dict] = None,
                payload_reference: Any = None,
                quality_warnings: Optional[list] = None) -> dict:
    """Build ONE contract-complete normalized event.

    Timestamps are NEVER fabricated. A source that did not state an availability time
    leaves ``published_at`` null and the event is marked
    ``AVAILABILITY_TIMESTAMP_UNKNOWN`` — unknown stays unknown.
    """
    warnings = list(quality_warnings or [])
    cls = classify_event(record_type=record_type, event_type=event_type, payload=payload)
    fp = content_fingerprint(payload if payload is not None else {})
    idem = idempotency_key(source_id=source_id, record_type=record_type,
                           source_event_id=source_event_id, payload_fingerprint=fp)

    pit = PIT_OK
    if published_at is None and accepted_at is None:
        pit = PIT_UNKNOWN_AVAILABILITY
        warnings.append(
            "PUBLICATION_TIME_UNKNOWN: the source stated no publication/acceptance "
            "timestamp; left null (never fabricated, never back-filled from a period "
            "end or an observation date).")
    if cls["family"] == F_ANALYST_SNAPSHOT:
        pit = PIT_SNAPSHOT_PROSPECTIVE

    ents = [str(t).strip().upper() for t in (entities or []) if str(t or "").strip()]
    if primary_ticker and str(primary_ticker).strip().upper() not in ents:
        ents.insert(0, str(primary_ticker).strip().upper())

    return {
        "event_id": make_event_id(idem),
        "event_schema_version": EVENT_SCHEMA_VERSION,
        "event_contract_id": EVENT_CONTRACT_ID,
        "idempotency_key": idem,
        "source_id": str(source_id or ""),
        # The collector that actually produced the record, when it differs from the
        # canonical source id in the capability registry (e.g. the Stage-3.5 RSS
        # collector writes ``rss_atom`` for the ``news_rss`` source).
        "collector_id": (str(collector_id) if collector_id else str(source_id or "")),
        "source_event_id": (str(source_event_id) if source_event_id is not None else None),
        "source_family": (str(source_family) if source_family else None),
        "record_type": _upper(record_type),
        "event_type": (_upper(event_type) or None),
        "event_sub_type": (str(event_sub_type) if event_sub_type else None),
        "family": cls["family"],
        "signal_speed": cls["signal_speed"],
        "decision_authority": cls["decision_authority"],
        "authority_policy_version": cls["authority_policy_version"],
        "why_authority": cls["why_authority"],
        "classified": cls["classified"],
        "concepts": list(cls["concepts"]),
        "cadence": cls["cadence"],
        "source_timestamp": source_timestamp,
        "published_at": published_at,
        "accepted_at": accepted_at,
        "effective_at": effective_at,
        "first_observed_at": first_observed_at,
        "ingested_at": ingested_at,
        "entities": ents,
        "primary_ticker": (str(primary_ticker).strip().upper() if primary_ticker else
                           (ents[0] if ents else None)),
        "identity_confidence": (str(identity_confidence) if identity_confidence else
                                "UNMATCHED"),
        "source_quality": (str(source_quality) if source_quality else "UNKNOWN"),
        "event_quality": (str(event_quality) if event_quality else "UNKNOWN"),
        "materiality_inputs": dict(materiality_inputs or {}),
        "novelty": NOV_NEW,
        "novelty_reason": "First observation of this idempotency key.",
        "duplicate_of": None,
        "supersedes": None,
        "superseded_by": None,
        "payload_fingerprint": fp,
        "payload_reference": (str(payload_reference) if payload_reference else None),
        "extractor_version": EXTRACTOR_VERSION,
        "point_in_time_status": pit,
        "quality_warnings": warnings,
    }


# --------------------------------------------------------------------------- #
# Novelty / deduplication
#
# A news system is useless if one story becomes ten signals. Novelty is decided
# against what has ALREADY been observed — never by rewriting the earlier event.
# --------------------------------------------------------------------------- #
_WORD_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = frozenset((
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by",
    "from", "as", "is", "are", "was", "were", "be", "been", "it", "its", "that", "this",
    "after", "over", "into", "amid", "says", "said", "will", "has", "have", "new"))
_CORRECTION_MARKERS = ("correction:", "corrected:", "corrects ", "(corrected)")
_RETRACTION_MARKERS = ("retraction:", "retracted:", "withdraws story", "kills story")
_FOLLOW_UP_MARKERS = ("update ", "update:", "recap:", "wrapup", "wrap-up", "explainer:",
                      "analysis:", "factbox")


def story_shingle(title: Any, *, k: int = 8) -> str:
    """A deterministic content shingle for near-duplicate detection.

    Case/punctuation/stopword insensitive; the same story syndicated by five wires
    collapses to one shingle. Purely lexical — no model call, no network.
    """
    words = [w for w in _WORD_RE.findall(str(title or "").lower()) if w not in _STOPWORDS]
    if not words:
        return ""
    return sha256_text(" ".join(sorted(words[:k])))[:24]


def supersession_key(event: dict) -> str:
    """The key under which a re-issue SUPERSEDES an earlier event.

    It includes the effective date deliberately. Several collectors reuse a native id
    across days — a symbol-directory row is ``nasdaqlisted|ABNB`` every session, and a
    daily bar repeats its ticker — so keying supersession on the native id alone would
    declare 25 genuinely distinct daily observations to be 24 "corrections" of the
    first. A re-issue only supersedes when it describes the SAME point in time.
    """
    return "%s|%s|%s" % (event.get("source_id"), event.get("source_event_id"),
                         str(event.get("effective_at") or "")[:10])


def classify_novelty(*, event: dict, seen_idempotency_keys: Any = None,
                     seen_shingles: Optional[dict] = None,
                     seen_source_event_ids: Optional[dict] = None,
                     seen_links: Optional[dict] = None) -> dict:
    """Decide whether this event carries NEW information.

    Precedence, most specific first:

      1. the identical payload was already ingested            -> DUPLICATE
      2. the same CANONICAL DOCUMENT was already ingested      -> SYNDICATED
      3. the same native id for the same DATE, changed payload -> UPDATE / CORRECTION
      4. the same story CONTENT from anywhere                  -> SYNDICATED / FOLLOW_UP
      5. otherwise                                             -> NEW

    Rule 2 exists because collectors legitimately re-collect one document under several
    scopes: a wire article about five holdings is fetched once per symbol, and a single
    SEC accession is seen by both the daily-index and the submissions lane. Those copies
    differ in collection metadata, so without this rule each would look like a
    "correction" of the last and would re-trigger a reassessment for one story.

    ``seen_shingles`` / ``seen_links`` map to the FIRST event_id that carried them, so a
    later duplicate points AT the original instead of overwriting it.
    """
    keys = set(seen_idempotency_keys or ())
    shingles = dict(seen_shingles or {})
    native = dict(seen_source_event_ids or {})
    links = dict(seen_links or {})
    title = str(((event.get("materiality_inputs") or {}).get("title")) or "")
    low = title.lower()

    if event.get("idempotency_key") in keys:
        return {"novelty": NOV_DUPLICATE, "duplicate_of": event.get("event_id"),
                "reason": ("Identical (source, record type, native id, payload) already "
                           "observed; re-ingestion is a no-op.")}

    ref = str(event.get("payload_reference") or "").strip()
    if ref and ref in links:
        return {"novelty": NOV_SYNDICATED, "duplicate_of": links[ref],
                "reason": ("The same canonical document was already ingested (%s). It is "
                           "ONE information event however many collection scopes "
                           "retrieved it." % ref[:120])}

    native_key = supersession_key(event)
    if event.get("source_event_id") and native_key in native:
        prior = native[native_key]
        if any(m in low for m in _CORRECTION_MARKERS):
            return {"novelty": NOV_CORRECTION, "duplicate_of": prior,
                    "reason": "Source re-issued the same native id with a correction marker."}
        return {"novelty": NOV_MATERIAL_UPDATE, "duplicate_of": prior,
                "reason": ("Source re-issued the same native id with a changed payload; "
                           "recorded as a new immutable event that supersedes the prior "
                           "one. The earlier event is never rewritten.")}

    shingle = event.get("content_shingle") or story_shingle(title)
    if shingle and shingle in shingles:
        prior = shingles[shingle]
        if any(m in low for m in _RETRACTION_MARKERS):
            return {"novelty": NOV_RETRACTION, "duplicate_of": prior,
                    "reason": "Retraction of a story already observed."}
        if any(m in low for m in _CORRECTION_MARKERS):
            return {"novelty": NOV_CORRECTION, "duplicate_of": prior,
                    "reason": "Correction of a story already observed."}
        if any(m in low for m in _FOLLOW_UP_MARKERS):
            return {"novelty": NOV_FOLLOW_UP, "duplicate_of": prior,
                    "reason": ("Follow-up coverage of a story already observed; carries "
                               "no new fact and must not re-trigger a reassessment.")}
        return {"novelty": NOV_SYNDICATED, "duplicate_of": prior,
                "reason": ("The same story from another outlet. ONE information event; "
                           "the duplicate is linked, not counted again.")}
    return {"novelty": NOV_NEW, "duplicate_of": None,
            "reason": "No prior event carries this identity or content."}


def apply_novelty(event: dict, verdict: dict) -> dict:
    """Return a COPY of the event carrying its novelty verdict. Never mutates history."""
    out = dict(event)
    out["novelty"] = verdict.get("novelty", NOV_NEW)
    out["novelty_reason"] = verdict.get("reason")
    out["duplicate_of"] = verdict.get("duplicate_of")
    if out["novelty"] in (NOV_CORRECTION, NOV_MATERIAL_UPDATE, NOV_RETRACTION):
        out["supersedes"] = verdict.get("duplicate_of")
    return out


def carries_new_information(event: dict) -> bool:
    return str((event or {}).get("novelty")) in INFORMATIVE_NOVELTY


# --------------------------------------------------------------------------- #
# The dependency graph — event -> concepts -> signals -> calculations
# --------------------------------------------------------------------------- #
def concepts_for_events(events: Iterable[dict]) -> list[str]:
    """The union of business concepts the given events invalidated (ordered)."""
    hit: set[str] = set()
    for e in (events or []):
        if not carries_new_information(e):
            continue
        for c in (e.get("concepts") or []):
            hit.add(str(c))
    return [c for c in BUSINESS_CONCEPTS if c in hit]


def affected_calculations(concepts: Iterable[str]) -> list[str]:
    """The calculations that must refresh for the given concepts, in execution order."""
    hit: set[str] = set()
    for c in (concepts or []):
        dep = CONCEPT_DEPENDENCIES.get(str(c))
        if not dep:
            continue
        for calc in dep["calculations"]:
            hit.add(calc)
    return [c for c in CALCULATION_ORDER if c in hit]


def affected_signals(concepts: Iterable[str]) -> list[str]:
    out: list[str] = []
    for c in (concepts or []):
        dep = CONCEPT_DEPENDENCIES.get(str(c))
        if not dep:
            continue
        for s in dep["signals"]:
            if s not in out:
                out.append(s)
    return out


def build_dependency_graph() -> dict:
    """The whole map, machine-readable: family -> concepts -> signals -> calculations."""
    families = []
    for fam in EVENT_FAMILY_TABLE:
        concepts = list(fam["concepts"])
        families.append({
            "family": fam["family"],
            "record_types": list(fam["record_types"]),
            "signal_speed": fam["signal_speed"],
            "decision_authority": fam["decision_authority"],
            "why_authority": fam["why_authority"],
            "cadence": fam["cadence"],
            "concepts": concepts,
            "signals": affected_signals(concepts),
            "calculations": affected_calculations(concepts),
            "may_change_alpha": authority_may_change_alpha(fam["decision_authority"]),
            "may_change_risk": authority_may_change_risk(fam["decision_authority"]),
            "may_trigger_reassessment":
                authority_may_trigger_reassessment(fam["decision_authority"]),
            "reaches_operational_target":
                authority_touches_operational_target(fam["decision_authority"]),
        })
    return {
        "graph_id": "paper_trader.event_dependency_graph/1",
        "calculation_owner": CALCULATION_OWNER,
        "authority_policy_version": AUTHORITY_POLICY_VERSION,
        "phase": PHASE,
        "signal_speeds": list(SIGNAL_SPEEDS),
        "signal_authorities": list(SIGNAL_AUTHORITIES),
        "novelty_states": list(NOVELTY_STATES),
        "terminal_source_states": list(TERMINAL_SOURCE_STATES),
        "business_concepts": list(BUSINESS_CONCEPTS),
        "concept_dependencies": {k: {"signals": list(v["signals"]),
                                     "calculations": list(v["calculations"])}
                                 for k, v in CONCEPT_DEPENDENCIES.items()},
        "calculation_owners": dict(CALCULATION_OWNERS),
        "calculation_order": list(CALCULATION_ORDER),
        "families": families,
        "creates_orders": False,
        "automatic_execution": False,
        "note": ("One dependency map. An event refresh recomputes only the calculations "
                 "its concepts reach; the daily full refresh recomputes all of them "
                 "through the SAME owners."),
    }


def event_contract() -> dict:
    """The machine-readable normalized-event contract."""
    return {
        "contract_id": EVENT_CONTRACT_ID,
        "event_schema_version": EVENT_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "authority_policy_version": AUTHORITY_POLICY_VERSION,
        "extractor_version": EXTRACTOR_VERSION,
        "fields": list(EVENT_FIELDS),
        "point_in_time_states": list(PIT_STATES),
        "identity": {
            "idempotency_key": ("sha256(source_id | record_type | source_event_id | "
                                "payload_fingerprint) — deliberately EXCLUDES ingestion "
                                "and observation time so the same raw event retrieved "
                                "twice is one event."),
            "event_id": "evt_ + first 32 hex chars of the idempotency key",
        },
        "immutability": ("Append-only. A correction or material update is a NEW event "
                         "that SUPERSEDES the earlier one; the earlier event's payload, "
                         "timestamps and authority are never rewritten."),
        "point_in_time": ("Every timestamp is the one the source stated. A missing "
                          "publication time stays null and is flagged; a period end is "
                          "never substituted for an availability time; a current "
                          "snapshot is never inserted into historical time."),
        "safety": {"creates_orders": False, "mutates_operational_state": False,
                   "promotes_models": False, "read_only_sources": True},
    }


__all__ = [
    "PHASE", "CALCULATION_OWNER", "EVENT_SCHEMA_VERSION", "EVENT_CONTRACT_ID",
    "AUTHORITY_POLICY_VERSION", "EXTRACTOR_VERSION",
    "SPEED_STRUCTURAL", "SPEED_TACTICAL", "SPEED_MARKET_RISK", "SIGNAL_SPEEDS",
    "AUTH_OPERATIONAL_ALPHA", "AUTH_RESEARCH_ALPHA", "AUTH_OPERATIONAL_RISK",
    "AUTH_EVENT_TRIGGER_ONLY", "AUTH_OBSERVABILITY_ONLY", "AUTH_BLOCKED",
    "SIGNAL_AUTHORITIES", "ALPHA_BEARING_AUTHORITIES", "RISK_BEARING_AUTHORITIES",
    "TRIGGER_BEARING_AUTHORITIES", "NON_OPERATIONAL_AUTHORITIES",
    "NOV_NEW", "NOV_DUPLICATE", "NOV_SYNDICATED", "NOV_FOLLOW_UP", "NOV_CORRECTION",
    "NOV_MATERIAL_UPDATE", "NOV_RETRACTION", "NOVELTY_STATES", "INFORMATIVE_NOVELTY",
    "TERMINAL_SOURCE_STATES", "INTEGRATED_TERMINAL_STATES",
    "FORBIDDEN_NON_TERMINAL_STATES", "TERM_INTEGRATED_OPERATIONAL",
    "TERM_INTEGRATED_TRIGGER_ONLY", "TERM_INTEGRATED_RESEARCH_ONLY", "TERM_REDUNDANT",
    "TERM_BLOCKED_ENTITLEMENT", "TERM_BLOCKED_DATA_QUALITY", "TERM_BLOCKED_PIT",
    "TERM_BLOCKED_LICENSE", "TERM_NOT_ECONOMICALLY_USEFUL",
    "BUSINESS_CONCEPTS", "CONCEPT_DEPENDENCIES", "CALCULATION_OWNERS",
    "CALCULATION_ORDER", "EVENT_FAMILY_TABLE", "EVENT_FAMILIES", "EVENT_FIELDS",
    "PIT_OK", "PIT_UNKNOWN_AVAILABILITY", "PIT_SNAPSHOT_PROSPECTIVE", "PIT_STATES",
    "REGIME_MACRO_SERIES", "STRUCTURAL_FORMS", "MATERIAL_EVENT_FORMS", "INSIDER_FORMS",
    "F_STRUCTURAL_REPORT", "F_MATERIAL_CORPORATE_EVENT", "F_INSIDER_TRANSACTION",
    "F_OTHER_FILING", "F_EARNINGS_RESULT", "F_GUIDANCE_CHANGE", "F_FUNDAMENTAL_FACT",
    "F_ANALYST_SNAPSHOT", "F_ANALYST_REVISION", "F_COMPANY_NEWS", "F_REGULATORY_EVENT",
    "F_PRESS_RELEASE", "F_MARKET_BAR", "F_MARKET_QUOTE", "F_CORPORATE_ACTION",
    "F_TRADING_HALT", "F_SHORT_VOLUME", "F_UNIVERSE_MEMBERSHIP", "F_SECURITY_IDENTITY",
    "F_MACRO_REGIME", "F_MACRO_CONTEXT",
    "CALC_MARKET_RISK_STATE", "CALC_PORTFOLIO_VALUATION", "CALC_UNIVERSE_SCORING",
    "CALC_HOLDING_OPPORTUNITY_COST", "CALC_PORTFOLIO_REASSESSMENT",
    "CALC_REALLOCATION_PROPOSAL", "CALC_RESEARCH_EVIDENCE",
    "classify_event", "build_event", "classify_novelty", "apply_novelty",
    "carries_new_information", "story_shingle", "idempotency_key", "make_event_id",
    "supersession_key",
    "content_fingerprint", "canonical_json", "sha256_text",
    "authority_may_change_alpha", "authority_may_change_risk",
    "authority_may_trigger_reassessment", "authority_touches_operational_target",
    "unclassified_authority_count", "concepts_for_events", "affected_calculations",
    "affected_signals", "build_dependency_graph", "event_contract",
]
