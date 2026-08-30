r"""api/investability_registry.py - Release 50: the ONE operational investability
registry - which sleeves and instruments may compete for OPERATIONAL paper capital,
and, for every one that may not, exactly why.

The registry is DATA. Each sleeve record declares its asset class, strategy family,
currency, the owner of every operational concern (mark, signal, score, risk, cost,
liquidity, capacity, accounting, execution), its model-approval state with the
evidence behind it, its point-in-time state, its execution / settlement /
collateral semantics, and the fourteen capability flags Release 50 classifies:

    DATA_AVAILABLE  SIGNAL_AVAILABLE  MODEL_APPROVED_FOR_OPERATION  PIT_VALID
    CURRENT_MARK_AVAILABLE  USD_VALUATION_SUPPORTED  RISK_SUPPORTED  COST_SUPPORTED
    LIQUIDITY_SUPPORTED  CAPACITY_SUPPORTED  POSITION_ACCOUNTING_SUPPORTED
    PAPER_EXECUTION_SUPPORTED  RECONCILIATION_SUPPORTED  ->  CAPITAL_ELIGIBLE

``CAPITAL_ELIGIBLE`` is DERIVED, never declared: every capability must hold AND the
sleeve's operational signal must be APPROVED. The registry is extensible - a new
sleeve is a new record, not a new code path - and it is never hard-coded around a
fixed list of asset classes.

Two rules that are the point of the file:

* Research is not operation. A Release-46 challenger, an adopted prior-release
  shadow, or any research verdict is REFERENCED here as evidence and can never
  make a sleeve eligible. Approval is a governed, manual, evidence-gated model
  decision (the third operating cycle); there is no code path that promotes.
* Plumbing gaps are closed, not deferred. Every non-equity sleeve below carries
  TRUE for mark / USD valuation / risk / cost / liquidity / capacity /
  accounting / execution / reconciliation because Release 50 implemented them; the
  ONE thing that still blocks each of them is named, with the evidence state.

Read-only. Owns no store. Writes nothing. Imports no research module.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional

from paper_trader.api import market_reference_data as mrd
from paper_trader.engine import instrument_contract as ic

PHASE = "R50"
OWNER = "api.investability_registry"
SCHEMA_VERSION = "investability_registry.v1"
ROUTE = "/v1/operations/investability-registry"

# --------------------------------------------------------------------------- #
# Vocabularies
# --------------------------------------------------------------------------- #
CAPABILITIES = (
    "DATA_AVAILABLE", "SIGNAL_AVAILABLE", "MODEL_APPROVED_FOR_OPERATION", "PIT_VALID",
    "CURRENT_MARK_AVAILABLE", "USD_VALUATION_SUPPORTED", "RISK_SUPPORTED",
    "COST_SUPPORTED", "LIQUIDITY_SUPPORTED", "CAPACITY_SUPPORTED",
    "POSITION_ACCOUNTING_SUPPORTED", "PAPER_EXECUTION_SUPPORTED",
    "RECONCILIATION_SUPPORTED",
)

APPROVED = "APPROVED_FOR_OPERATION"
RESEARCH_ONLY = "RESEARCH_ONLY"
NOT_REGISTERED = "NO_OPERATIONAL_MODEL_REGISTERED"
APPROVAL_VOCAB = (APPROVED, RESEARCH_ONLY, NOT_REGISTERED)

PIT_OK = "PIT_OK"
PIT_UNKNOWN = "PIT_UNKNOWN"

R_NO_APPROVED_SIGNAL = "NO_APPROVED_OPERATIONAL_SIGNAL"
R_DATA_UNAVAILABLE = "OWNED_DATA_UNAVAILABLE_IN_THIS_PROCESS"
R_MARK_UNAVAILABLE = "NO_TRUSTWORTHY_CURRENT_MARK"
R_ACCOUNTING = "ACCOUNTING_SEMANTICS_NOT_DEFINABLE_WITHOUT_FABRICATION"
R_CAPABILITY = "CAPABILITY_MISSING"

PROMOTION_GOVERNANCE = {
    "who_approves": "the operator, manually, through the evidence-gated model "
                    "recalibration cycle (Charter operating cycle 3)",
    "automatic_promotion": False,
    "research_verdict_promotes": False,
    "r46_challenger_direct_path": False,
    "how_a_sleeve_becomes_eligible": (
        "its operational signal owner is registered with APPROVED_FOR_OPERATION and "
        "approval evidence, after matured TRUE_FORWARD evidence clears the frozen "
        "gates; every other capability is already implemented by Release 50, so no "
        "further code is needed for it to compete for capital"),
    "this_module_can_promote": False,
}

_PRIMARY_EQUITY_MODEL_ID = "fundamental_momentum_50_50_v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# The declared sleeves. One record per sleeve; a new sleeve is a new record.
# --------------------------------------------------------------------------- #
def _owners(*, mark, signal, score, risk="engine.cross_asset_risk "
            "(engine.holding_opportunity_cost.build_covariance)",
            cost="engine.instrument_contract (declared cost policy)",
            liquidity="api.market_reference_data (owned daily volume)",
            capacity="engine.constrained_reallocation (participation + unit granularity)",
            accounting="api.paper_trading_desk.book_nav (engine.instrument_contract semantics)",
            execution="api.rebalance_execution + api.paper_trading_desk.settle_due_orders") -> dict:
    return {"mark_owner": mark, "signal_owner": signal, "score_or_expected_return_owner": score,
            "risk_owner": risk, "cost_owner": cost, "liquidity_owner": liquidity,
            "capacity_owner": capacity, "accounting_owner": accounting,
            "execution_owner": execution}


def _futures_sleeve(sleeve_id: str, label: str, asset_class: str, roots: list,
                    research: dict, blocker_note: str, currency: str = "USD") -> dict:
    return {
        "sleeve_id": sleeve_id, "label": label, "asset_class": asset_class,
        "strategy_family": research.get("family") or "NONE_APPROVED",
        "instrument_type": ic.IT_FUTURE, "currency": currency,
        "instrument_ids": ["&" + r for r in roots],
        "representative_instrument": "&" + roots[0],
        "owners": _owners(
            mark="api.paper_trading_desk via api.market_reference_data (owned Norgate settlement)",
            signal=research.get("signal_owner"),
            score=None),
        "model_approval_state": RESEARCH_ONLY if research.get("challengers") else NOT_REGISTERED,
        "approval_evidence": {
            "state": research.get("state"),
            "research_reference": research.get("challengers") or [],
            "verdict": research.get("verdict"),
            "promotion_path": PROMOTION_GOVERNANCE["how_a_sleeve_becomes_eligible"],
        },
        "pit_state": PIT_OK,
        "pit_note": ("owned daily settlement bars; a mark is stored only for a completed "
                     "session and a fill can only use a settlement strictly after the "
                     "marks known at approval"),
        "execution_convention": ic.EXECUTION_CONVENTION_BY_TYPE[ic.IT_FUTURE],
        "settlement_semantics": ic.SETTLEMENT_SEMANTICS[ic.IT_FUTURE],
        "collateral_semantics": ic.COLLATERAL_SEMANTICS[ic.IT_FUTURE],
        "declared_capabilities": {
            "USD_VALUATION_SUPPORTED": True, "RISK_SUPPORTED": True, "COST_SUPPORTED": True,
            "LIQUIDITY_SUPPORTED": True, "CAPACITY_SUPPORTED": True,
            "POSITION_ACCOUNTING_SUPPORTED": True, "PAPER_EXECUTION_SUPPORTED": True,
            "RECONCILIATION_SUPPORTED": True, "SIGNAL_AVAILABLE": bool(research.get("challengers")),
        },
        "r50_activation_attempt": {
            "implemented_in_r50": [
                "instrument descriptor (point value, initial margin, currency, tick) from owned data",
                "USD valuation with FX conversion from the owned Forex Spot database",
                "variation-margin position accounting inside the ONE desk NAV replay",
                "collateral / capital-usage / notional / exposure semantics",
                "declared conservative per-class transaction + holding cost policy",
                "contract-volume liquidity and unit-granularity capacity",
                "cross-asset risk integration on owned daily settlements",
                "opportunity-frontier row with a normalised score slot",
                "cross-asset constraints in the feasible-target kernel",
                "whole-contract order plan with collateral reconciliation",
                "NEXT_SESSION_SETTLEMENT paper fill + realised P&L at close",
                "decision evidence on USD marks"],
            "remaining_blocker": R_NO_APPROVED_SIGNAL,
            "implementable_within_r50": False,
            "why_not": blocker_note,
        },
    }


def declared_sleeves() -> list[dict]:
    """The declared registry. Order is presentation order."""
    equity = {
        "sleeve_id": ic.DEFAULT_EQUITY_SLEEVE,
        "label": "US Equities - fundamental / momentum 50/50 (approved operational model)",
        "asset_class": ic.AC_US_EQUITY,
        "strategy_family": "CROSS_SECTIONAL_FUNDAMENTAL_MOMENTUM_BLEND",
        "instrument_type": ic.IT_CASH_EQUITY, "currency": "USD",
        "instrument_ids": None, "instrument_universe_owner": "api.universe_scoring",
        "representative_instrument": "SPY",
        "owners": _owners(
            mark="api.paper_trading_desk (owned EODHD adjusted close)",
            signal="api.universe_scoring (fundamental_momentum_50_50_v1)",
            score="api.universe_scoring (combined percentile; expected return NOT_CALIBRATED)",
            liquidity="api.universe_scoring (adv_dollar)"),
        "model_approval_state": APPROVED,
        "approval_evidence": {
            "state": "APPROVED_OPERATIONAL_MODEL",
            "model_id": _PRIMARY_EQUITY_MODEL_ID,
            "registry": "api.multi_horizon_registry / api.universe_scoring.PRIMARY_MODEL_ID",
            "activation": "governed target confirmation (Phase 25 / 27A.2) on the live book",
            "verdict": "OPERATIONAL",
        },
        "pit_state": PIT_OK,
        "pit_note": "owned point-in-time monthly inputs + completed-session marks",
        "execution_convention": ic.EXECUTION_CONVENTION_BY_TYPE[ic.IT_CASH_EQUITY],
        "settlement_semantics": ic.SETTLEMENT_SEMANTICS[ic.IT_CASH_EQUITY],
        "collateral_semantics": ic.COLLATERAL_SEMANTICS[ic.IT_CASH_EQUITY],
        "declared_capabilities": {c: True for c in CAPABILITIES},
        "r50_activation_attempt": {"implemented_in_r50": ["unchanged; already operational"],
                                   "remaining_blocker": None,
                                   "implementable_within_r50": True, "why_not": None},
    }
    cash = {
        "sleeve_id": ic.CASH_SLEEVE, "label": "USD cash (a real asset choice)",
        "asset_class": ic.AC_CASH, "strategy_family": "CASH", "instrument_type": ic.IT_CASH,
        "currency": "USD", "instrument_ids": [ic.CASH_INSTRUMENT_ID],
        "representative_instrument": ic.CASH_INSTRUMENT_ID,
        "owners": _owners(mark="identity (USD at par)", signal="declared policy",
                          score="engine.zero_base_allocator.CASH_RETURN (0.0, declared)",
                          risk="none (riskless by declaration)", cost="none",
                          liquidity="unlimited", capacity="unlimited",
                          accounting="api.paper_trading_desk.book_nav (ledger cash)",
                          execution="residual of every governed plan"),
        "model_approval_state": APPROVED,
        "approval_evidence": {"state": "DECLARED_POLICY", "verdict": "CASH_IS_A_REAL_ASSET_CHOICE",
                              "cash_return_policy": ic.CASH_RETURN_POLICY},
        "pit_state": PIT_OK, "pit_note": "cash has no mark",
        "execution_convention": "NONE", "settlement_semantics": ic.SETTLEMENT_SEMANTICS[ic.IT_CASH],
        "collateral_semantics": ic.COLLATERAL_SEMANTICS[ic.IT_CASH],
        "declared_capabilities": {c: True for c in CAPABILITIES},
        "r50_activation_attempt": {"implemented_in_r50": ["unchanged"], "remaining_blocker": None,
                                   "implementable_within_r50": True, "why_not": None},
    }
    why_r46 = ("the only operational-signal candidates are Release-46 prospective "
               "challengers (TRUE_FORWARD tournament, evidence TOO_EARLY / FORWARD_PENDING - "
               "no matured cohort has cleared the frozen forward gates) or prior-release "
               "research shadows. Approval is an evidence-gated manual model decision; "
               "granting it inside a development release would be an automatic promotion, "
               "which the charter and the R46 contract forbid. Nothing else blocks the sleeve.")
    fut = [
        _futures_sleeve("sleeve_equity_index_futures", "US equity index futures",
                        ic.AC_EQUITY_INDEX_FUTURES, ["ES", "NQ", "RTY", "YM", "MES", "MNQ", "M2K", "MYM"],
                        {"family": "TREND / CALENDAR (research)", "state": "R46_PROSPECTIVE_TOURNAMENT",
                         "challengers": ["r46_spx_trend_200d", "r46_3_spx_turn_of_month",
                                         "r46_4_spx_pre_fomc_drift", "r46_4_credit_regime_spx_timing"],
                         "verdict": "FORWARD_PENDING / TOO_EARLY", "signal_owner": None},
                        why_r46),
        _futures_sleeve("sleeve_rates_futures", "US rates futures",
                        ic.AC_RATES_FUTURES, ["ZN", "ZF", "ZT", "TN", "ZB", "UB", "SR3"],
                        {"family": "CURVE CARRY / RV / MACRO SURPRISE (research)",
                         "state": "R46_PROSPECTIVE_TOURNAMENT",
                         "challengers": ["r46_rates_curve_rv_5d", "r46_3_rates_curve_carry",
                                         "r46_4_macro_surprise_rates_5d"],
                         "verdict": "FORWARD_PENDING / TOO_EARLY", "signal_owner": None},
                        why_r46),
        _futures_sleeve("sleeve_commodity_futures", "Commodity futures",
                        ic.AC_COMMODITY_FUTURES, ["CL", "GC", "SI", "HG", "NG", "ZC", "ZS", "ZW",
                                                  "RB", "HO", "PL", "PA", "KC", "SB", "CT"],
                        {"family": "CROSS-SECTIONAL MOMENTUM / CURVE CARRY (research)",
                         "state": "R46_PROSPECTIVE_TOURNAMENT",
                         "challengers": ["r46_comdty_xs_mom_252", "r46_3_comdty_curve_carry",
                                         "r46_fut_ts_mom_252", "r46_3_fut_xs_mom_252"],
                         "verdict": "FORWARD_PENDING / TOO_EARLY", "signal_owner": None},
                        why_r46),
        _futures_sleeve("sleeve_volatility_futures", "Volatility futures (VX)",
                        ic.AC_VOLATILITY_FUTURES, ["VX"],
                        {"family": "TERM-STRUCTURE CARRY (research)",
                         "state": "R46_PROSPECTIVE_TOURNAMENT + R39 adopted shadow",
                         "challengers": ["r46_vx_term_carry_5d", "r46_3_vx_term_carry_1d", "r39_vx_weekly"],
                         "verdict": "FORWARD_PENDING / TOO_EARLY", "signal_owner": None},
                        why_r46 + " Roll cost for VX is the highest of any class (declared 156 bps/yr)."),
        _futures_sleeve("sleeve_fx_futures", "FX futures (CME)",
                        ic.AC_FX_FUTURES, ["6E", "6J", "6B", "6A", "6C", "6S", "6M", "6N", "DX"],
                        {"family": "CROSS-SECTIONAL MOMENTUM / CARRY (research)",
                         "state": "R46_PROSPECTIVE_TOURNAMENT + R36/R43 historical",
                         "challengers": ["r46_fx_xs_mom_252"],
                         "verdict": "FORWARD_PENDING / TOO_EARLY; R36 FX carry IC was historical only",
                         "signal_owner": None},
                        why_r46),
        _futures_sleeve("sleeve_international_index_futures", "International equity index futures (non-USD)",
                        ic.AC_INTL_EQUITY_INDEX_FUTURES, ["FDAX", "FESX", "NIY", "HSI", "FSMI"],
                        {"family": "NONE", "state": "NO_RESEARCH_CANDIDATE", "challengers": [],
                         "verdict": "NOT_RESEARCHED", "signal_owner": None},
                        "no operational or research signal exists for this sleeve; it is registered "
                        "because Release 50 proves non-USD valuation (EUR / JPY / HKD / CHF contracts "
                        "through the owned Forex Spot conversion)."),
        _futures_sleeve("sleeve_crypto_futures", "Crypto (CME cash-settled futures)",
                        ic.AC_CRYPTO_FUTURES, ["BTC", "ETH", "MBT"],
                        {"family": "BASIS / FUNDING CARRY (research)",
                         "state": "R41 survived historically; R42 priced the real premium BELOW the "
                                  "cash control; R46 has no active crypto challenger",
                         "challengers": ["r41_btc_funding_carry (adopted shadow)", "r42_crypto_basis (adopted shadow)"],
                         "verdict": "REAL_PREMIUM_BELOW_CASH_CONTROL (R42); DO_NOT_ACTIVATE",
                         "signal_owner": None},
                        "the only crypto edge with historical support (basis / funding carry) was "
                        "shown by Release 42 to earn less than remunerated cash collateral after "
                        "costs; nothing later reversed that finding. Accounting and execution are "
                        "implemented (the contract is a CME future); the blocker is the evidence."),
    ]
    fx_spot = {
        "sleeve_id": "sleeve_fx_spot", "label": "FX spot (owned Forex Spot database)",
        "asset_class": ic.AC_FX_SPOT, "strategy_family": "CARRY (research)",
        "instrument_type": ic.IT_FX_SPOT, "currency": "USD",
        "instrument_ids": None, "instrument_universe_owner": "api.market_reference_data.fx_spot_symbols",
        "representative_instrument": "EURUSD",
        "owners": _owners(mark="api.paper_trading_desk via api.market_reference_data (owned daily close)",
                          signal=None, score=None),
        "model_approval_state": RESEARCH_ONLY,
        "approval_evidence": {"state": "R36 / R43 historical carry research; no prospective cohort",
                              "research_reference": ["r36_fx_carry (historical)", "r43_carry (historical)"],
                              "verdict": "HISTORICAL_ONLY_NOT_APPROVED",
                              "promotion_path": PROMOTION_GOVERNANCE["how_a_sleeve_becomes_eligible"]},
        "pit_state": PIT_OK, "pit_note": "owned daily spot closes",
        "execution_convention": ic.EXECUTION_CONVENTION_BY_TYPE[ic.IT_FX_SPOT],
        "settlement_semantics": ic.SETTLEMENT_SEMANTICS[ic.IT_FX_SPOT],
        "collateral_semantics": ic.COLLATERAL_SEMANTICS[ic.IT_FX_SPOT],
        "declared_capabilities": {"USD_VALUATION_SUPPORTED": True, "RISK_SUPPORTED": True,
                                  "COST_SUPPORTED": True, "LIQUIDITY_SUPPORTED": False,
                                  "CAPACITY_SUPPORTED": True, "POSITION_ACCOUNTING_SUPPORTED": True,
                                  "PAPER_EXECUTION_SUPPORTED": True, "RECONCILIATION_SUPPORTED": True,
                                  "SIGNAL_AVAILABLE": False},
        "r50_activation_attempt": {
            "implemented_in_r50": ["spot conversion accounting (full notional, USD reporting)",
                                   "owned daily close marks through the ONE desk mark owner",
                                   "declared cost + financing policy", "cross-asset risk integration"],
            "remaining_blocker": R_NO_APPROVED_SIGNAL,
            "implementable_within_r50": False,
            "why_not": ("no approved operational signal (R36/R43 carry results are historical, "
                        "never frozen prospectively); the owned spot database carries no volume, "
                        "so liquidity is declared UNAVAILABLE rather than invented - a second "
                        "genuine gap that only a data purchase could close, which R50 may not make."),
        },
    }
    event_macro = {
        "sleeve_id": "sleeve_event_macro", "label": "Event / macro-surprise sleeves (research lanes)",
        "asset_class": ic.AC_EQUITY_INDEX_FUTURES, "strategy_family": "EVENT / MACRO (research)",
        "instrument_type": ic.IT_FUTURE, "currency": "USD",
        "instrument_ids": ["&ES", "&ZN"], "representative_instrument": "&ES",
        "owners": _owners(mark="api.paper_trading_desk via api.market_reference_data",
                          signal=None, score=None),
        "model_approval_state": RESEARCH_ONLY,
        "approval_evidence": {"state": "R45 macro event alpha + R46.4 macro / event lanes",
                              "research_reference": ["r46_4_spx_pre_fomc_drift",
                                                     "r46_4_spx_announcement_day_premium",
                                                     "r46_4_macro_surprise_rates_5d"],
                              "verdict": "FORWARD_PENDING / TOO_EARLY",
                              "promotion_path": PROMOTION_GOVERNANCE["how_a_sleeve_becomes_eligible"]},
        "pit_state": PIT_OK, "pit_note": "expressed through owned index / rates futures",
        "execution_convention": ic.EXECUTION_CONVENTION_BY_TYPE[ic.IT_FUTURE],
        "settlement_semantics": ic.SETTLEMENT_SEMANTICS[ic.IT_FUTURE],
        "collateral_semantics": ic.COLLATERAL_SEMANTICS[ic.IT_FUTURE],
        "declared_capabilities": {"USD_VALUATION_SUPPORTED": True, "RISK_SUPPORTED": True,
                                  "COST_SUPPORTED": True, "LIQUIDITY_SUPPORTED": True,
                                  "CAPACITY_SUPPORTED": True, "POSITION_ACCOUNTING_SUPPORTED": True,
                                  "PAPER_EXECUTION_SUPPORTED": True, "RECONCILIATION_SUPPORTED": True,
                                  "SIGNAL_AVAILABLE": True},
        "r50_activation_attempt": {
            "implemented_in_r50": ["shares every futures capability implemented above"],
            "remaining_blocker": R_NO_APPROVED_SIGNAL, "implementable_within_r50": False,
            "why_not": why_r46},
    }
    return [equity, cash] + fut + [fx_spot, event_macro]


# --------------------------------------------------------------------------- #
# Live probes (degrade-safe; they read owned data and never write)
# --------------------------------------------------------------------------- #
def _probe_sleeve(rec: dict, *, probe: bool, as_of: Optional[str]) -> dict:
    it = rec.get("instrument_type")
    if it in (ic.IT_CASH_EQUITY, ic.IT_CASH):
        return {"DATA_AVAILABLE": True, "CURRENT_MARK_AVAILABLE": True,
                "freshness_state": "OWNED_OPERATIONAL", "probe": "not required"}
    if not probe:
        return {"DATA_AVAILABLE": None, "CURRENT_MARK_AVAILABLE": None,
                "freshness_state": "NOT_PROBED", "probe": "skipped"}
    rep = rec.get("representative_instrument")
    if not mrd.available():
        return {"DATA_AVAILABLE": False, "CURRENT_MARK_AVAILABLE": False,
                "freshness_state": R_DATA_UNAVAILABLE, "probe": rep,
                "detail": "the owned reference-data provider is not readable in this process"}
    latest = None
    meta_ok = True
    try:
        if it == ic.IT_FUTURE:
            meta_ok = mrd.futures_metadata(rep).get("state") == "OK"
        latest = mrd.latest_session(rep)
    except Exception:  # noqa: BLE001
        latest = None
    fresh = "UNKNOWN"
    if latest:
        fresh = ("CURRENT" if (as_of is None or latest >= str(as_of)[:10])
                 else "BEHIND_AS_OF (%s < %s)" % (latest, as_of))
    return {"DATA_AVAILABLE": bool(meta_ok and latest), "CURRENT_MARK_AVAILABLE": bool(latest),
            "freshness_state": fresh, "probe": rep, "latest_session": latest,
            "metadata_ok": meta_ok}


def _classify(rec: dict, probed: dict, approval: Optional[dict]) -> dict:
    caps = {c: False for c in CAPABILITIES}
    caps.update({k: bool(v) for k, v in (rec.get("declared_capabilities") or {}).items()
                 if k in caps})
    caps["DATA_AVAILABLE"] = bool(probed.get("DATA_AVAILABLE"))
    caps["CURRENT_MARK_AVAILABLE"] = bool(probed.get("CURRENT_MARK_AVAILABLE"))
    caps["PIT_VALID"] = rec.get("pit_state") == PIT_OK
    state = (approval or {}).get("model_approval_state") or rec.get("model_approval_state")
    caps["MODEL_APPROVED_FOR_OPERATION"] = state == APPROVED
    if approval and approval.get("signal_scores") is not None:
        caps["SIGNAL_AVAILABLE"] = True
    missing = [c for c in CAPABILITIES if not caps[c]]
    eligible = not missing
    if eligible:
        reason = None
    elif not caps["MODEL_APPROVED_FOR_OPERATION"]:
        reason = R_NO_APPROVED_SIGNAL
    elif not caps["DATA_AVAILABLE"] or not caps["CURRENT_MARK_AVAILABLE"]:
        reason = (R_DATA_UNAVAILABLE if probed.get("freshness_state") == R_DATA_UNAVAILABLE
                  else R_MARK_UNAVAILABLE)
    else:
        reason = R_CAPABILITY
    return {"capabilities": caps, "missing_capabilities": missing,
            "capital_eligible": eligible, "capital_ineligible_reason": reason,
            "model_approval_state": state}


# --------------------------------------------------------------------------- #
# The read model
# --------------------------------------------------------------------------- #
def load_investability_registry(*, approvals: Optional[dict] = None,
                                probe: bool = True, as_of: Optional[str] = None,
                                nav: Optional[float] = None) -> dict:
    """The ONE registry read. ``approvals`` is an INJECTION seam for hermetic tests
    (``{sleeve_id: {"model_approval_state": ..., "approval_evidence": ...,
    "signal_scores": {...}}}``); production passes nothing and reads the declared
    states. There is no route, file or flag through which a caller can promote."""
    approvals = dict(approvals or {})
    rows = []
    for rec in declared_sleeves():
        sid = rec["sleeve_id"]
        appr = approvals.get(sid)
        probed = _probe_sleeve(rec, probe=probe, as_of=as_of)
        cls = _classify(rec, probed, appr)
        row = dict(rec)
        row.pop("declared_capabilities", None)
        row.update(cls)
        row["freshness_state"] = probed.get("freshness_state")
        row["probe"] = {k: v for k, v in probed.items()
                        if k not in ("DATA_AVAILABLE", "CURRENT_MARK_AVAILABLE")}
        if appr:
            row["approval_evidence"] = dict(appr.get("approval_evidence") or
                                            {"state": "INJECTED_FOR_HERMETIC_TEST"})
            row["approval_injected"] = True
            row["signal_scores"] = dict(appr.get("signal_scores") or {})
        else:
            row["approval_injected"] = False
        rows.append(row)
    eligible = [r for r in rows if r["capital_eligible"]]
    ineligible = [r for r in rows if not r["capital_eligible"]]
    return {
        "schema_version": SCHEMA_VERSION, "phase": PHASE, "owner": OWNER, "route": ROUTE,
        "generated_at": _now_iso(), "as_of": as_of,
        "capabilities_vocabulary": list(CAPABILITIES),
        "approval_vocabulary": list(APPROVAL_VOCAB),
        "sleeves": rows,
        "sleeve_count": len(rows),
        "capital_eligible_sleeve_ids": [r["sleeve_id"] for r in eligible],
        "capital_eligible_asset_classes": sorted({r["asset_class"] for r in eligible}),
        "capital_ineligible": [{"sleeve_id": r["sleeve_id"], "asset_class": r["asset_class"],
                                "reason": r["capital_ineligible_reason"],
                                "missing_capabilities": r["missing_capabilities"],
                                "model_approval_state": r["model_approval_state"]}
                               for r in ineligible],
        "non_equity_eligible_sleeve_ids": [r["sleeve_id"] for r in eligible
                                           if r["asset_class"] not in (ic.AC_US_EQUITY, ic.AC_CASH)],
        "eligibility_policy": {
            "capital_eligible_is_derived": True,
            "rule": "every capability TRUE and MODEL_APPROVED_FOR_OPERATION",
            "silent_exclusion": False,
            "extensible": "a new sleeve is a new declared record; no asset class is hard-coded",
        },
        "promotion_governance": dict(PROMOTION_GOVERNANCE),
        "approvals_injected": sorted(approvals),
        "reference_data": mrd.provider_state() if probe else {"state": "NOT_PROBED"},
        "safety": {"read_only": True, "writes_nothing": True, "promotes_nothing": True,
                   "creates_orders": False, "automation_off": True,
                   "safety_badges": ["READ ONLY", "NO MODEL PROMOTION", "NO ORDERS",
                                     "AUTOMATION OFF", "MANUAL REVIEW"]},
    }


def sleeve_map(registry: dict) -> dict:
    return {r["sleeve_id"]: r for r in (registry or {}).get("sleeves") or []}


def eligible_non_equity_instruments(registry: dict, *, nav: Optional[float] = None,
                                    as_of: Optional[str] = None,
                                    max_name_weight: float = 0.10,
                                    metadata_loader: Optional[Callable] = None,
                                    mark_loader: Optional[Callable] = None) -> list[dict]:
    """Instrument descriptors of every CAPITAL-ELIGIBLE non-equity sleeve, each with
    its unit notional, capital-usage ratio and whether ONE unit is executable at
    the book's NAV under the name cap (unit granularity is a real capacity limit:
    a $112k contract cannot be a 4% position of a $99k book)."""
    out = []
    md = metadata_loader or mrd.futures_metadata
    for r in (registry or {}).get("sleeves") or []:
        if not r.get("capital_eligible") or r.get("asset_class") in (ic.AC_US_EQUITY, ic.AC_CASH):
            continue
        scores = r.get("signal_scores") or {}
        for sym in (r.get("instrument_ids") or list(scores)):
            if r.get("instrument_type") == ic.IT_FUTURE:
                meta = md(sym)
                if meta.get("state") != "OK":
                    continue
                d = mrd.descriptor_for(sym, sleeve_id=r["sleeve_id"], metadata=meta)
            else:
                d = mrd.descriptor_for(sym, sleeve_id=r["sleeve_id"])
            if d is None:
                continue
            mark = None
            if mark_loader is not None:
                mark = mark_loader(sym)
            else:
                bars = [b for b in mrd.daily_bars(sym) if as_of is None or b[0] <= str(as_of)[:10]]
                mark = bars[-1][1] if bars else None
            fx = mrd.fx_to_usd(d["currency"], as_of=as_of)
            fxv = fx.get("fx_to_usd")
            un = ic.unit_notional_usd(d, mark, fxv) if (mark is not None and fxv is not None) else None
            navv = float(nav) if nav else None
            executable = bool(un is not None and navv and un <= max_name_weight * navv)
            d = dict(d)
            d.update({
                "opportunity_score": scores.get(sym),
                "mark": mark, "fx_to_usd": fxv, "fx_state": fx.get("state"),
                "unit_notional_usd": un,
                "capital_usage_ratio": ic.capital_usage_ratio(d, mark, fxv) if mark else None,
                "average_daily_volume_units": mrd.average_daily_volume(sym, as_of=as_of),
                "executable_at_nav": executable,
                "executability_reason": (None if executable else
                                         ("UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV" if un is not None
                                          else "MARK_OR_FX_UNAVAILABLE")),
            })
            out.append(d)
    return out


def activation_attempts(registry: Optional[dict] = None) -> list[dict]:
    """The documented per-sleeve activation attempt (handoff artifact)."""
    reg = registry or load_investability_registry(probe=False)
    out = []
    for r in reg["sleeves"]:
        att = r.get("r50_activation_attempt") or {}
        out.append({"sleeve_id": r["sleeve_id"], "asset_class": r["asset_class"],
                    "model_approval_state": r["model_approval_state"],
                    "capital_eligible": r["capital_eligible"],
                    "blocker": r["capital_ineligible_reason"],
                    "implementation_attempted": att.get("implemented_in_r50"),
                    "remaining_blocker": att.get("remaining_blocker"),
                    "implementable_within_r50": att.get("implementable_within_r50"),
                    "why_not_resolved_in_r50": att.get("why_not"),
                    "research_evidence": r.get("approval_evidence")})
    return out


__all__ = [
    "PHASE", "OWNER", "SCHEMA_VERSION", "ROUTE", "CAPABILITIES", "APPROVED", "RESEARCH_ONLY",
    "NOT_REGISTERED", "APPROVAL_VOCAB", "R_NO_APPROVED_SIGNAL", "R_DATA_UNAVAILABLE",
    "R_MARK_UNAVAILABLE", "R_ACCOUNTING", "R_CAPABILITY", "PROMOTION_GOVERNANCE",
    "declared_sleeves", "load_investability_registry", "sleeve_map",
    "eligible_non_equity_instruments", "activation_attempts",
]
