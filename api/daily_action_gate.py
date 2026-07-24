"""
api/daily_action_gate.py — Phase 27C/27D: the DAILY EVENT-DRIVEN ACTION GATE.

The operational book (Alpha Paper Book #1) previously became calendar-driven:
a completed data refresh updated the model scores, but no portfolio action was
permitted until the scheduled monthly review date. This module replaces that
with a daily *event* gate:

    completed EOD refresh
      -> recalculate the eligible sleeves (owned data; NO prediction tunnel)
      -> rebuild the current combined target
      -> compare the target against the ACTUAL 25 operational holdings
      -> evaluate risk / eligibility / membership / drift / concentration /
         materiality gates
      -> return ONE canonical result:
             NO_ACTION_TODAY  or  PROPOSAL_READY (manual approval required)

A scheduled monthly/quarterly review remains a MANDATORY comprehensive review
checkpoint, but it is NOT the only date on which the system may propose action:
a hard eligibility or risk event before the scheduled date still produces a
proposal. The scheduled date never *suppresses* a proposal.

Daily recalculation ≠ retraining. This module recomputes current prices, ranks,
risk, eligibility, the combined target and the target-versus-actual comparison
from the EXISTING frozen model. It never retrains, refits or optimises a model
parameter, never replaces the champion, never promotes a sleeve, and keeps the
fast sleeve inactive. It composes existing read-only services only
(``multi_horizon_engine`` for the owned-data model target + ``operational_book``
for the actual holdings and the canonical monthly review clock) and performs NO
writes: no orders, no fills, no snapshots, no automation, no broker.

Phase 27D completes the production layer:
  * a single canonical ``target_state`` mapping (CURRENT_ALIGNED / PROPOSAL_READY /
    APPROVAL_REQUIRED / DATA_NOT_READY / ORDERS_SUBMITTED / FORWARD_TRACKING_ACTIVE)
    consumed verbatim by every operator surface — so no page shows a stale
    "READY TO CONFIRM" when the book is aligned;
  * a canonical ``checks_performed`` list (13 daily risk / control checks) with a
    ``checks_summary``; every status and threshold comes from backend Python, so
    JavaScript only renders the contract;
  * position-weight-limit and sector-concentration breaches that deterministically
    propose a resize toward the current target weight (never auto-executed), or a
    data/integrity blocker when the model target itself violates its cap;
  * the "ECONOMIC ACTION GATE" is renamed to the accurate MATERIALITY / COST-CONTROL
    check — it does not forecast an expected-alpha benefit; it applies a turnover /
    materiality floor (with modeled paper execution cost) that prevents trivial
    resize-only churn.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api import paper_trading_desk as desk

PHASE = "27D"

_EPS = 1e-9

# --------------------------------------------------------------------------- #
# Canonical outcomes (the ONE vocabulary every operator surface renders)
# --------------------------------------------------------------------------- #
OUTCOME_DATA_NOT_READY = "DATA_NOT_READY"
OUTCOME_NO_ACTION_TODAY = "NO_ACTION_TODAY"
OUTCOME_PROPOSAL_READY = "PROPOSAL_READY"
OUTCOME_APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
OUTCOME_ORDERS_SUBMITTED = "ORDERS_SUBMITTED"
OUTCOME_FORWARD_TRACKING = "FORWARD_TRACKING"

ALL_OUTCOMES = (OUTCOME_DATA_NOT_READY, OUTCOME_NO_ACTION_TODAY, OUTCOME_PROPOSAL_READY,
                OUTCOME_APPROVAL_REQUIRED, OUTCOME_ORDERS_SUBMITTED, OUTCOME_FORWARD_TRACKING)

# --------------------------------------------------------------------------- #
# Canonical target state (Phase 27D — the ONE state driving operator wording;
# derived deterministically from the outcome so no page re-invents "READY TO
# CONFIRM" / "review required" when the book is actually aligned).
# --------------------------------------------------------------------------- #
TARGET_STATE_CURRENT_ALIGNED = "CURRENT_ALIGNED"
TARGET_STATE_PROPOSAL_READY = "PROPOSAL_READY"
TARGET_STATE_APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
TARGET_STATE_DATA_NOT_READY = "DATA_NOT_READY"
TARGET_STATE_ORDERS_SUBMITTED = "ORDERS_SUBMITTED"
TARGET_STATE_FORWARD_TRACKING = "FORWARD_TRACKING_ACTIVE"

ALL_TARGET_STATES = (TARGET_STATE_CURRENT_ALIGNED, TARGET_STATE_PROPOSAL_READY,
                     TARGET_STATE_APPROVAL_REQUIRED, TARGET_STATE_DATA_NOT_READY,
                     TARGET_STATE_ORDERS_SUBMITTED, TARGET_STATE_FORWARD_TRACKING)

_TARGET_STATE_LABELS = {
    TARGET_STATE_CURRENT_ALIGNED: "CURRENT — ALIGNED WITH HOLDINGS",
    TARGET_STATE_PROPOSAL_READY: "PROPOSAL READY — MANUAL REVIEW REQUIRED",
    TARGET_STATE_APPROVAL_REQUIRED: "MANUAL APPROVAL REQUIRED",
    TARGET_STATE_DATA_NOT_READY: "DATA REFRESH REQUIRED",
    TARGET_STATE_ORDERS_SUBMITTED: "PAPER ORDERS SUBMITTED",
    TARGET_STATE_FORWARD_TRACKING: "FORWARD TRACKING — ALIGNED WITH HOLDINGS",
}

# --------------------------------------------------------------------------- #
# Trigger categories (why a daily proposal may fire before the scheduled review)
# --------------------------------------------------------------------------- #
TRIGGER_HARD_ELIGIBILITY = "HARD_ELIGIBILITY_EVENT"
TRIGGER_HARD_RISK = "HARD_RISK_EVENT"
TRIGGER_MEMBERSHIP_CHANGE = "MATERIAL_TARGET_MEMBERSHIP_CHANGE"
TRIGGER_WEIGHT_DRIFT = "MATERIAL_WEIGHT_DRIFT"
TRIGGER_POSITION_LIMIT = "POSITION_WEIGHT_LIMIT_BREACH"
TRIGGER_SECTOR_CONCENTRATION = "SECTOR_CONCENTRATION_BREACH"
# Phase 27D — renamed from the imprecise "ECONOMIC_ACTION_GATE". The gate does NOT
# forecast an expected-alpha benefit and compare it numerically to cost; it applies
# a turnover / materiality floor (with modeled paper execution cost) that prevents
# trivial resize-only churn. ``TRIGGER_ECONOMIC_GATE`` remains as a deprecated alias.
TRIGGER_MATERIALITY_COST_CONTROL = "MATERIALITY_COST_CONTROL"
TRIGGER_ECONOMIC_GATE = TRIGGER_MATERIALITY_COST_CONTROL  # deprecated alias
TRIGGER_SCHEDULED_REVIEW = "SCHEDULED_FULL_REVIEW"

ALL_TRIGGER_CATEGORIES = (TRIGGER_HARD_ELIGIBILITY, TRIGGER_HARD_RISK,
                          TRIGGER_MEMBERSHIP_CHANGE, TRIGGER_WEIGHT_DRIFT,
                          TRIGGER_POSITION_LIMIT, TRIGGER_SECTOR_CONCENTRATION,
                          TRIGGER_MATERIALITY_COST_CONTROL, TRIGGER_SCHEDULED_REVIEW)

# --------------------------------------------------------------------------- #
# Canonical daily check codes (Phase 27D). Every status/threshold is computed in
# backend Python and rendered verbatim by the UI.
# --------------------------------------------------------------------------- #
CHECK_DATA_FRESHNESS = "DATA_FRESHNESS"
CHECK_TARGET_ALIGNMENT = "TARGET_ALIGNMENT"
CHECK_ELIGIBILITY = "ELIGIBILITY"
CHECK_UNIVERSE_MEMBERSHIP = "CURRENT_UNIVERSE_MEMBERSHIP"
CHECK_LIQUIDITY = "LIQUIDITY"
CHECK_POSITION_WEIGHT_LIMIT = "POSITION_WEIGHT_LIMIT"
CHECK_SECTOR_CONCENTRATION = "SECTOR_CONCENTRATION"
CHECK_MATERIAL_WEIGHT_DRIFT = "MATERIAL_WEIGHT_DRIFT"
CHECK_RISK_DATA_AVAILABILITY = "RISK_DATA_AVAILABILITY"
CHECK_VOLATILITY_MONITOR = "VOLATILITY_MONITOR"
CHECK_DRAWDOWN_MONITOR = "DRAWDOWN_MONITOR"
CHECK_MATERIALITY_COST_CONTROL = "MATERIALITY_COST_CONTROL"
CHECK_SCHEDULED_FULL_REVIEW = "SCHEDULED_FULL_REVIEW"

ALL_CHECK_CODES = (CHECK_DATA_FRESHNESS, CHECK_TARGET_ALIGNMENT, CHECK_ELIGIBILITY,
                   CHECK_UNIVERSE_MEMBERSHIP, CHECK_LIQUIDITY, CHECK_POSITION_WEIGHT_LIMIT,
                   CHECK_SECTOR_CONCENTRATION, CHECK_MATERIAL_WEIGHT_DRIFT,
                   CHECK_RISK_DATA_AVAILABILITY, CHECK_VOLATILITY_MONITOR,
                   CHECK_DRAWDOWN_MONITOR, CHECK_MATERIALITY_COST_CONTROL,
                   CHECK_SCHEDULED_FULL_REVIEW)

CHECK_LABELS = {
    CHECK_DATA_FRESHNESS: "Data freshness",
    CHECK_TARGET_ALIGNMENT: "Target vs actual alignment",
    CHECK_ELIGIBILITY: "Holding eligibility",
    CHECK_UNIVERSE_MEMBERSHIP: "Current universe membership",
    CHECK_LIQUIDITY: "Liquidity",
    CHECK_POSITION_WEIGHT_LIMIT: "Position-weight limit",
    CHECK_SECTOR_CONCENTRATION: "Sector concentration",
    CHECK_MATERIAL_WEIGHT_DRIFT: "Material weight drift",
    CHECK_RISK_DATA_AVAILABILITY: "Risk data availability",
    CHECK_VOLATILITY_MONITOR: "Volatility monitor",
    CHECK_DRAWDOWN_MONITOR: "Drawdown monitor",
    CHECK_MATERIALITY_COST_CONTROL: "Materiality / cost control",
    CHECK_SCHEDULED_FULL_REVIEW: "Scheduled full review",
}

# Check status vocabulary.
CHK_PASS = "PASS"
CHK_WARN = "WARN"
CHK_TRIGGERED = "TRIGGERED"
CHK_NOT_AVAILABLE = "NOT_AVAILABLE"
CHK_MONITOR_ONLY = "MONITOR_ONLY"

# --------------------------------------------------------------------------- #
# OPERATIONAL POLICY CONSTANTS (controls, NOT retrained model parameters).
#
# Each derives explicitly from an EXISTING platform assumption. They are declared
# in backend code (never hidden in UI JavaScript) and reported verbatim in the
# canonical result under ``policy`` so the operator can see the thresholds.
# --------------------------------------------------------------------------- #

#: The platform's EXISTING paper execution model — 12.5 bps per side
#: (``paper_trading_desk.COST_BPS_PER_SIDE``); 25 bps round-trip. Unchanged.
EXECUTION_COST_BPS_PER_SIDE = desk.COST_BPS_PER_SIDE           # 12.5
EXECUTION_COST_RATE_PER_SIDE = desk.COST_RATE_PER_SIDE         # 0.00125

#: Membership hysteresis. A held name that has left the combined target is a
#: MATERIAL removal only once its current combined rank falls beyond
#: ``ceil(N * (1 + fraction))``. This REUSES the engine's exit buffer
#: (``multi_horizon_engine.EXIT_BUFFER_FRACTION`` = 0.20) verbatim, so a one- or
#: two-position rank wobble never triggers churn.
MEMBERSHIP_EXIT_BUFFER_FRACTION = eng.EXIT_BUFFER_FRACTION     # 0.20

#: Material weight drift. A held name inside the target is only resized when its
#: actual weight differs from the target weight by at least this (2 percentage
#: points). Matches the operational holdings dashboard REVIEW drift band
#: (``operational_book._holding_status``: |drift| >= 0.02 -> REVIEW), so integer-
#: share rounding and ordinary market-move drift are ignored.
MATERIAL_WEIGHT_DRIFT = 0.02

#: Minimum-action turnover floor for the MATERIALITY / COST-CONTROL check. A
#: proposal made up ONLY of resizes must move at least this much one-way turnover
#: (≈ three quarters of one equal-weight position in a 25-name book:
#: 0.75 * 1/25 = 0.03) to be worth the modeled execution cost. We never fabricate
#: an expected-return "benefit"; the control is expressed conservatively as a
#: materiality floor: a change can never be proposed solely because a weight
#: drifted by market noise. Hard eligibility / hard risk / membership /
#: concentration-limit changes always bypass this floor (they are mandatory), so a
#: genuine event is never suppressed.
MIN_ACTION_TURNOVER = 0.03

#: Hard individual-position weight cap (``multi_horizon_engine.MAX_INDIVIDUAL_WEIGHT``
#: = 0.10). A held weight above this is a position-limit breach that proposes a
#: resize toward the current target weight (never auto-executed).
MAX_INDIVIDUAL_WEIGHT = eng.MAX_INDIVIDUAL_WEIGHT             # 0.10

#: Sector concentration cap (``multi_horizon_engine.SECTOR_CAP_FRACTION`` = 0.25).
#: Applies to KNOWN sectors only (Unknown-sector names are exempt, exactly as the
#: engine constructs the book).
SECTOR_CAP_FRACTION = eng.SECTOR_CAP_FRACTION                 # 0.25

#: Minimum dollar-liquidity floor (``multi_horizon_engine.MIN_ADV_DOLLAR`` = $10M/day).
MIN_ADV_DOLLAR = eng.MIN_ADV_DOLLAR                          # 1.0e7

#: Reason codes (emitted by ``multi_horizon_engine``) that make a HELD name
#: HARD-INELIGIBLE — it can no longer be scored / is not a current member / fails
#: the liquidity floor. A held name carrying one of these is a hard eligibility
#: event regardless of the scheduled review date.
HARD_ELIGIBILITY_CODES = frozenset({
    "NOT_CURRENT_MEMBER", "LIQUIDITY_FILTER_FAILED", "MISSING_MOMENTUM",
    "MISSING_COMPOSITE_SN", "MOMENTUM_HISTORY_INSUFFICIENT",
})

#: Reason codes that make a held name a HARD RISK / data-integrity event
#: (corporate-action artifact or a blocking stale-fundamental state).
HARD_RISK_CODES = frozenset({
    "DATA_QUALITY_BLOCK", "EXTREME_MOMENTUM", "FUNDAMENTAL_DATA_STALE",
})

#: Frozen operational identity (never mutated here).
STRATEGY_NAME = "fundamental_momentum_50_50_v1"
TARGET_BOOK_NAME = "fundamental_momentum_50_50_top25"


def _policy() -> dict:
    return {
        "execution_cost_bps_per_side": EXECUTION_COST_BPS_PER_SIDE,
        "execution_cost_bps_round_trip": 2 * EXECUTION_COST_BPS_PER_SIDE,
        "execution_cost_rate_per_side": EXECUTION_COST_RATE_PER_SIDE,
        "membership_exit_buffer_fraction": MEMBERSHIP_EXIT_BUFFER_FRACTION,
        "material_weight_drift": MATERIAL_WEIGHT_DRIFT,
        "min_action_turnover": MIN_ACTION_TURNOVER,
        "max_individual_weight": MAX_INDIVIDUAL_WEIGHT,
        "sector_cap_fraction": SECTOR_CAP_FRACTION,
        "min_adv_dollar": MIN_ADV_DOLLAR,
        "hard_eligibility_codes": sorted(HARD_ELIGIBILITY_CODES),
        "hard_risk_codes": sorted(HARD_RISK_CODES),
        "note": ("Operational controls derived from existing assumptions (12.5 bps/side "
                 "execution cost, the engine 0.20 exit buffer, the 0.02 REVIEW drift band, "
                 "the engine 0.10 individual-weight cap, 0.25 sector cap and $10M ADV floor). "
                 "These gate ACTION only — no model parameter, weight or champion is changed."),
    }


def _safety() -> dict:
    return {
        "paper_only": True,
        "read_only": True,
        "broker_enabled": False,
        "automation_enabled": False,
        "live_orders_enabled": False,
        "performed_write": False,
        "auto_order_creation": False,
        "auto_target_confirmation": False,
        "model_parameters_changed": False,
        "champion_replaced": False,
        "fast_sleeve_active": False,
        "safety_badges": ["PAPER ONLY", "MANUAL REVIEW", "NO BROKER", "AUTOMATION OFF",
                          "NO LIVE ORDERS", "NO AUTO ORDER CREATION"],
    }


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _r6(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 6)


# --------------------------------------------------------------------------- #
# Presentation (one operator vocabulary per outcome — every page renders these)
# --------------------------------------------------------------------------- #
SEV_GREEN = "green"
SEV_AMBER = "amber"
SEV_RED = "red"

_PRESENTATION = {
    OUTCOME_DATA_NOT_READY: {
        "label": "DATA NOT READY",
        "headline": "MARKET DATA REFRESH REQUIRED",
        "severity": SEV_AMBER,
        "primary_action_label": "Refresh After Market Close",
        "current_task": "Refresh owned EOD market data",
    },
    OUTCOME_NO_ACTION_TODAY: {
        # Phase 28B wording: never claim "today" before today's session has been
        # closed — the decision is anchored to the LATEST COMPLETED close (the
        # assembled payload appends that date when it is known).
        "label": "NO ACTION TODAY",
        "headline": "NO PORTFOLIO CHANGE REQUIRED FROM THE LATEST COMPLETED CLOSE",
        "severity": SEV_GREEN,
        "primary_action_label": "Monitor Holdings and Performance",
        "current_task": "Monitor holdings, NAV, drift and forward performance",
    },
    OUTCOME_PROPOSAL_READY: {
        "label": "PROPOSAL READY",
        "headline": "PORTFOLIO CHANGES PROPOSED — MANUAL REVIEW REQUIRED",
        "severity": SEV_AMBER,
        "primary_action_label": "Review Proposed Changes",
        "current_task": "Review proposed portfolio changes",
    },
    OUTCOME_APPROVAL_REQUIRED: {
        "label": "APPROVAL REQUIRED",
        "headline": "PORTFOLIO CHANGES PROPOSED — MANUAL APPROVAL REQUIRED",
        "severity": SEV_AMBER,
        "primary_action_label": "Review and Approve Proposed Changes",
        "current_task": "Review and approve proposed portfolio changes",
    },
    OUTCOME_ORDERS_SUBMITTED: {
        "label": "ORDERS SUBMITTED",
        "headline": "PAPER ORDERS IN PROGRESS",
        "severity": SEV_AMBER,
        "primary_action_label": "Refresh After Market Close",
        "current_task": "Await the next eligible close for pending paper orders",
    },
    OUTCOME_FORWARD_TRACKING: {
        "label": "FORWARD TRACKING",
        "headline": "FORWARD TRACKING ACTIVE",
        "severity": SEV_GREEN,
        "primary_action_label": "Monitor Holdings and Performance",
        "current_task": "Monitor holdings, NAV, drift and forward performance",
    },
}


def _target_state_for(outcome: str) -> tuple[str, str]:
    """Map the canonical outcome to the ONE operator target state + label."""
    mapping = {
        OUTCOME_DATA_NOT_READY: TARGET_STATE_DATA_NOT_READY,
        OUTCOME_ORDERS_SUBMITTED: TARGET_STATE_ORDERS_SUBMITTED,
        OUTCOME_PROPOSAL_READY: TARGET_STATE_PROPOSAL_READY,
        OUTCOME_APPROVAL_REQUIRED: TARGET_STATE_APPROVAL_REQUIRED,
        OUTCOME_FORWARD_TRACKING: TARGET_STATE_FORWARD_TRACKING,
        OUTCOME_NO_ACTION_TODAY: TARGET_STATE_CURRENT_ALIGNED,
    }
    ts = mapping.get(outcome, TARGET_STATE_CURRENT_ALIGNED)
    return ts, _TARGET_STATE_LABELS[ts]


# --------------------------------------------------------------------------- #
# Canonical daily check builder (Phase 27D). Pure; every status/threshold here.
# --------------------------------------------------------------------------- #
def _mk_check(code: str, status: str, *, as_of: Optional[str], source: str,
              summary: str, affected: Optional[list] = None) -> dict:
    return {"code": code, "label": CHECK_LABELS.get(code, code), "status": status,
            "as_of_date": as_of, "source": source, "summary": summary,
            "affected_tickers": sorted(affected or [])}


def _build_checks(*, holdings: dict, target: dict, removed: set, eligibility: dict,
                  proposed_removals: list, proposed_additions: list,
                  proposed_resizes: list, blocked_changes: list,
                  sector_integrity: list, data_ready: bool, risk_ready: bool,
                  scheduled_review_due: bool, next_review: Optional[str],
                  market_date: Optional[str], pol: dict) -> tuple[list, dict]:
    held = set(holdings)
    checks: list[dict] = []
    src_model = "owned-data model target (multi_horizon_engine)"
    src_market = "owned EOD market data + desk mark store"
    src_risk = "engine risk pack (realized vol / drawdown / ADV)"

    # 1. DATA_FRESHNESS
    if data_ready:
        checks.append(_mk_check(CHECK_DATA_FRESHNESS, CHK_PASS, as_of=market_date,
                      source=src_market,
                      summary="Owned market data and model scores are current (%s)."
                              % (market_date or "latest completed")))
    else:
        checks.append(_mk_check(CHECK_DATA_FRESHNESS, CHK_TRIGGERED, as_of=market_date,
                      source=src_market,
                      summary="Market data / model scores are not current — run the manual "
                              "after-market refresh before evaluating any change."))

    # 2. TARGET_ALIGNMENT
    align_affected = ([r["ticker"] for r in proposed_removals]
                      + [a["ticker"] for a in proposed_additions]
                      + [r["ticker"] for r in proposed_resizes])
    if not data_ready:
        checks.append(_mk_check(CHECK_TARGET_ALIGNMENT, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_model, summary="Target-versus-actual comparison needs current data."))
    elif align_affected:
        checks.append(_mk_check(CHECK_TARGET_ALIGNMENT, CHK_TRIGGERED, as_of=market_date,
                      source=src_model,
                      summary="%d holding(s) differ from the current model target."
                              % len(set(align_affected)), affected=align_affected))
    else:
        checks.append(_mk_check(CHECK_TARGET_ALIGNMENT, CHK_PASS, as_of=market_date,
                      source=src_model, summary="Actual holdings match the current model target."))

    # 3. ELIGIBILITY
    ineligible = sorted(tk for tk in held if (eligibility.get(tk) or {}).get("eligible") is False)
    if not data_ready:
        checks.append(_mk_check(CHECK_ELIGIBILITY, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_model, summary="Eligibility needs current model scores."))
    elif ineligible:
        checks.append(_mk_check(CHECK_ELIGIBILITY, CHK_TRIGGERED, as_of=market_date,
                      source=src_model,
                      summary="%d held name(s) are no longer eligible." % len(ineligible),
                      affected=ineligible))
    else:
        checks.append(_mk_check(CHECK_ELIGIBILITY, CHK_PASS, as_of=market_date,
                      source=src_model, summary="All holdings remain eligible (scoreable, current members)."))

    # 4. CURRENT_UNIVERSE_MEMBERSHIP
    mem_affected = sorted({r["ticker"] for r in proposed_removals
                           if r.get("trigger_category") == TRIGGER_MEMBERSHIP_CHANGE}
                          | {a["ticker"] for a in proposed_additions})
    if not data_ready:
        checks.append(_mk_check(CHECK_UNIVERSE_MEMBERSHIP, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_model, summary="Membership needs current model ranks."))
    elif mem_affected:
        checks.append(_mk_check(CHECK_UNIVERSE_MEMBERSHIP, CHK_TRIGGERED, as_of=market_date,
                      source=src_model,
                      summary="%d material membership change(s) beyond the exit buffer."
                              % len(mem_affected), affected=mem_affected))
    else:
        checks.append(_mk_check(CHECK_UNIVERSE_MEMBERSHIP, CHK_PASS, as_of=market_date,
                      source=src_model,
                      summary="All holdings remain within the combined target (exit buffer %s)."
                              % pol["membership_exit_buffer_fraction"]))

    # 5. LIQUIDITY
    min_adv = pol["min_adv_dollar"]
    adv_pairs = [(tk, _f((holdings.get(tk) or {}).get("adv_dollar"))) for tk in held if tk not in removed]
    below_adv = sorted(tk for tk, a in adv_pairs if a is not None and a < min_adv)
    liq_fail = sorted(tk for tk in held
                      if (eligibility.get(tk) or {}).get("reason") == "LIQUIDITY_FILTER_FAILED")
    liq_affected = sorted(set(below_adv) | set(liq_fail))
    if not data_ready:
        checks.append(_mk_check(CHECK_LIQUIDITY, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_risk, summary="Liquidity needs current market data."))
    elif liq_affected:
        checks.append(_mk_check(CHECK_LIQUIDITY, CHK_TRIGGERED, as_of=market_date,
                      source=src_risk,
                      summary="%d holding(s) fail the $%.0fM ADV liquidity floor."
                              % (len(liq_affected), min_adv / 1e6), affected=liq_affected))
    else:
        checks.append(_mk_check(CHECK_LIQUIDITY, CHK_PASS, as_of=market_date,
                      source=src_risk,
                      summary="Holdings satisfy the $%.0fM ADV liquidity floor." % (min_adv / 1e6)))

    # 6. POSITION_WEIGHT_LIMIT
    max_w = pol["max_individual_weight"]
    weights = {tk: _f((holdings.get(tk) or {}).get("weight")) for tk in held if tk not in removed}
    have_weights = any(w is not None for w in weights.values())
    over_cap = sorted(tk for tk, w in weights.items() if w is not None and w > max_w + _EPS)
    if not have_weights:
        checks.append(_mk_check(CHECK_POSITION_WEIGHT_LIMIT, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_market, summary="Per-holding weights are not available."))
    elif over_cap:
        checks.append(_mk_check(CHECK_POSITION_WEIGHT_LIMIT, CHK_TRIGGERED, as_of=market_date,
                      source=src_market,
                      summary="%d position(s) exceed the %.0f%% individual-weight cap."
                              % (len(over_cap), max_w * 100), affected=over_cap))
    else:
        checks.append(_mk_check(CHECK_POSITION_WEIGHT_LIMIT, CHK_PASS, as_of=market_date,
                      source=src_market,
                      summary="No position exceeds the %.0f%% individual-weight cap." % (max_w * 100)))

    # 7. SECTOR_CONCENTRATION
    sec_cap = pol["sector_cap_fraction"]
    actual_sec = _sector_weights(holdings, exclude=removed)
    over_sectors = sorted(s for s, w in actual_sec.items() if w > sec_cap + _EPS)
    sec_affected = sorted(tk for tk in held if tk not in removed
                          and ((holdings.get(tk) or {}).get("sector") in over_sectors))
    if not actual_sec:
        checks.append(_mk_check(CHECK_SECTOR_CONCENTRATION, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_market, summary="Known-sector weights are not available."))
    elif over_sectors:
        integ = " (model target itself breaches — data/integrity review)" if sector_integrity else ""
        checks.append(_mk_check(CHECK_SECTOR_CONCENTRATION, CHK_TRIGGERED, as_of=market_date,
                      source=src_market,
                      summary="Sector(s) %s exceed the %.0f%% concentration cap%s."
                              % (", ".join(over_sectors), sec_cap * 100, integ),
                      affected=sec_affected))
    else:
        checks.append(_mk_check(CHECK_SECTOR_CONCENTRATION, CHK_PASS, as_of=market_date,
                      source=src_market,
                      summary="No known sector exceeds the %.0f%% concentration cap." % (sec_cap * 100)))

    # 8. MATERIAL_WEIGHT_DRIFT
    drift_active = sorted(r["ticker"] for r in proposed_resizes
                          if r.get("trigger_category") == TRIGGER_WEIGHT_DRIFT)
    drift_blocked = sorted(b["ticker"] for b in blocked_changes
                           if b.get("blocked_reason") == "MATERIALITY_FLOOR_NOT_MET")
    if not have_weights:
        checks.append(_mk_check(CHECK_MATERIAL_WEIGHT_DRIFT, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_market, summary="Per-holding weights are not available."))
    elif drift_active:
        checks.append(_mk_check(CHECK_MATERIAL_WEIGHT_DRIFT, CHK_TRIGGERED, as_of=market_date,
                      source=src_market,
                      summary="%d holding(s) drifted beyond the %.0f%% resize band."
                              % (len(drift_active), pol["material_weight_drift"] * 100),
                      affected=drift_active))
    elif drift_blocked:
        checks.append(_mk_check(CHECK_MATERIAL_WEIGHT_DRIFT, CHK_WARN, as_of=market_date,
                      source=src_market,
                      summary="%d holding(s) drifted past the band but below the materiality "
                              "floor — held (no churn)." % len(drift_blocked), affected=drift_blocked))
    else:
        checks.append(_mk_check(CHECK_MATERIAL_WEIGHT_DRIFT, CHK_PASS, as_of=market_date,
                      source=src_market,
                      summary="No holding drifted beyond the %.0f%% resize band."
                              % (pol["material_weight_drift"] * 100)))

    # 9. RISK_DATA_AVAILABILITY
    if risk_ready:
        checks.append(_mk_check(CHECK_RISK_DATA_AVAILABILITY, CHK_PASS, as_of=market_date,
                      source=src_risk, summary="Owned risk pack (vol / drawdown / ADV) is available."))
    else:
        checks.append(_mk_check(CHECK_RISK_DATA_AVAILABILITY, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_risk, summary="Owned risk pack is not available this session."))

    # 10. VOLATILITY_MONITOR — MONITOR_ONLY (no validated enforcement threshold)
    vol_pairs = [(tk, _f((holdings.get(tk) or {}).get("realized_vol")), _f((holdings.get(tk) or {}).get("weight")))
                 for tk in held if tk not in removed]
    vols = [(tk, v, w) for tk, v, w in vol_pairs if v is not None]
    if vols:
        wsum = sum((w or 0.0) for _, _, w in vols)
        wavg = (sum(v * (w or 0.0) for _, v, w in vols) / wsum) if wsum else None
        worst = max(vols, key=lambda t: t[1])
        detail = ("Portfolio-weighted 63d realized vol %.1f%%; highest %s %.1f%%. No validated "
                  "enforcement threshold is configured — monitor only."
                  % ((wavg * 100) if wavg is not None else float("nan"),
                     worst[0], worst[1] * 100))
        checks.append(_mk_check(CHECK_VOLATILITY_MONITOR, CHK_MONITOR_ONLY, as_of=market_date,
                      source=src_risk, summary=detail))
    else:
        checks.append(_mk_check(CHECK_VOLATILITY_MONITOR, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_risk,
                      summary="No realized-vol measurement available; no enforcement threshold configured."))

    # 11. DRAWDOWN_MONITOR — MONITOR_ONLY (no validated enforcement threshold)
    dd_pairs = [(tk, _f((holdings.get(tk) or {}).get("max_drawdown")))
                for tk in held if tk not in removed]
    dds = [(tk, d) for tk, d in dd_pairs if d is not None]
    if dds:
        worst_dd = min(dds, key=lambda t: t[1])
        detail = ("Worst holding 252d max drawdown %s %.1f%%. No validated enforcement threshold "
                  "is configured — monitor only." % (worst_dd[0], worst_dd[1] * 100))
        checks.append(_mk_check(CHECK_DRAWDOWN_MONITOR, CHK_MONITOR_ONLY, as_of=market_date,
                      source=src_risk, summary=detail))
    else:
        checks.append(_mk_check(CHECK_DRAWDOWN_MONITOR, CHK_NOT_AVAILABLE, as_of=market_date,
                      source=src_risk,
                      summary="No drawdown measurement available; no enforcement threshold configured."))

    # 12. MATERIALITY_COST_CONTROL
    mat_blocked = [b for b in blocked_changes if b.get("blocked_reason") == "MATERIALITY_FLOOR_NOT_MET"]
    if mat_blocked:
        checks.append(_mk_check(CHECK_MATERIALITY_COST_CONTROL, CHK_PASS, as_of=market_date,
                      source=src_model,
                      summary="%d immaterial resize-only change(s) suppressed below the %.0f%% "
                              "turnover floor (modeled %.1f bps/side cost) — no churn."
                              % (len(mat_blocked), pol["min_action_turnover"] * 100,
                                 pol["execution_cost_bps_per_side"]),
                      affected=[b["ticker"] for b in mat_blocked if b.get("ticker")]))
    else:
        checks.append(_mk_check(CHECK_MATERIALITY_COST_CONTROL, CHK_PASS, as_of=market_date,
                      source=src_model,
                      summary="No immaterial resize-only proposal (floor %.0f%% turnover, modeled "
                              "%.1f bps/side cost)." % (pol["min_action_turnover"] * 100,
                                                        pol["execution_cost_bps_per_side"])))

    # 13. SCHEDULED_FULL_REVIEW
    if scheduled_review_due:
        checks.append(_mk_check(CHECK_SCHEDULED_FULL_REVIEW, CHK_TRIGGERED, as_of=next_review,
                      source="operational book review clock",
                      summary="The scheduled full portfolio review is due (%s)."
                              % (next_review or "today")))
    else:
        checks.append(_mk_check(CHECK_SCHEDULED_FULL_REVIEW, CHK_PASS, as_of=next_review,
                      source="operational book review clock",
                      summary="Not due. Next scheduled full review: %s." % (next_review or "pending")))

    summary = _checks_summary(checks)
    return checks, summary


def _checks_summary(checks: list) -> dict:
    total = len(checks)
    by = {CHK_PASS: 0, CHK_WARN: 0, CHK_TRIGGERED: 0, CHK_NOT_AVAILABLE: 0, CHK_MONITOR_ONLY: 0}
    for c in checks:
        by[c["status"]] = by.get(c["status"], 0) + 1
    triggered = by[CHK_TRIGGERED]
    unavailable = by[CHK_NOT_AVAILABLE]
    return {
        "total": total,
        "passed": by[CHK_PASS],
        "warned": by[CHK_WARN],
        "triggered": triggered,
        "not_available": unavailable,
        "monitor_only": by[CHK_MONITOR_ONLY],
        "triggered_codes": [c["code"] for c in checks if c["status"] == CHK_TRIGGERED],
        "line": "%d checks completed · %d triggered · %d unavailable"
                % (total, triggered, unavailable),
    }


def _sector_weights(holdings: dict, exclude: Optional[set] = None) -> dict:
    """Sum current weight by KNOWN sector (Unknown sector exempt, as the engine builds it)."""
    exclude = exclude or set()
    out: dict[str, float] = {}
    for tk, h in holdings.items():
        if tk in exclude:
            continue
        w = _f((h or {}).get("weight"))
        sec = (h or {}).get("sector") or "Unknown"
        if w is None or sec == "Unknown":
            continue
        out[sec] = out.get(sec, 0.0) + w
    return out


def _target_sector_weights(target: dict) -> dict:
    out: dict[str, float] = {}
    for tk, row in target.items():
        w = _f((row or {}).get("weight"))
        sec = (row or {}).get("sector") or "Unknown"
        if w is None or sec == "Unknown":
            continue
        out[sec] = out.get(sec, 0.0) + w
    return out


# --------------------------------------------------------------------------- #
# The pure daily action gate (no I/O; fully deterministic; unit-testable)
# --------------------------------------------------------------------------- #
def evaluate_daily_action_gate(
    *,
    holdings: dict,
    target: dict,
    ranked_current: Optional[dict] = None,
    eligibility: Optional[dict] = None,
    risk_events: Optional[list] = None,
    target_count: Optional[int] = None,
    next_scheduled_full_review: Optional[str] = None,
    scheduled_review_due: bool = False,
    data_ready: bool = True,
    risk_ready: bool = True,
    orders_pending: int = 0,
    book_active: bool = True,
    evaluation_date: Optional[str] = None,
    latest_completed_market_date: Optional[str] = None,
    policy: Optional[dict] = None,
) -> dict:
    """Evaluate the daily event gate for an ACTIVE operational book.

    ``holdings``  : {ticker: {"weight","quantity","sector","adv_dollar","realized_vol",
                    "max_drawdown"}} actually held (all but "weight" optional).
    ``target``    : {ticker: {"weight","rank","sector","adv_dollar"}} recomputed combined target.
    ``ranked_current`` : {ticker: current_combined_rank} for held names (membership hysteresis).
    ``eligibility``    : {ticker: {"eligible": bool, "reason": code}} for held names.
    ``risk_events``    : [{"ticker","reason","detail"}] hard risk / exit conditions on held names.

    Returns the canonical ``daily_action_gate`` contract. Pure — never writes,
    never proposes an order, never confirms a target.
    """
    ranked_current = ranked_current or {}
    eligibility = eligibility or {}
    risk_events = risk_events or []
    holdings = holdings or {}
    target = target or {}
    pol = policy or _policy()

    held = set(holdings)
    tgt = set(target)
    n_target = int(target_count or len(tgt) or 0)
    exit_buffer_rank = (math.ceil(n_target * (1.0 + pol["membership_exit_buffer_fraction"]))
                        if n_target else None)

    removed: set[str] = set()
    proposed_removals: list[dict] = []
    proposed_additions: list[dict] = []
    proposed_resizes: list[dict] = []
    blocked_changes: list[dict] = []
    sector_integrity: list[str] = []
    categories: set[str] = set()
    reasons: list[str] = []

    def _hw(tk: str) -> Optional[float]:
        return _f((holdings.get(tk) or {}).get("weight"))

    # 1. HARD ELIGIBILITY — a held name is no longer eligible / not a member.
    for tk in sorted(held):
        el = eligibility.get(tk) or {}
        if el and el.get("eligible") is False:
            removed.add(tk)
            reason = el.get("reason") or "INELIGIBLE"
            proposed_removals.append({
                "ticker": tk, "current_weight": _r6(_hw(tk)),
                "trigger_category": TRIGGER_HARD_ELIGIBILITY, "reason": reason,
                "hard_event": True,
                "detail": "Held name is no longer eligible (%s)." % reason})
            categories.add(TRIGGER_HARD_ELIGIBILITY)
            reasons.append("%s is no longer eligible (%s)." % (tk, reason))

    # 2. HARD RISK — an exit / limit-breach / integrity event on a held name.
    risk_by_tk = {ev.get("ticker"): ev for ev in risk_events if ev.get("ticker")}
    for tk in sorted(held):
        if tk in removed:
            continue
        ev = risk_by_tk.get(tk)
        if ev:
            removed.add(tk)
            reason = ev.get("reason") or "RISK_EXIT"
            proposed_removals.append({
                "ticker": tk, "current_weight": _r6(_hw(tk)),
                "trigger_category": TRIGGER_HARD_RISK, "reason": reason,
                "hard_event": True,
                "detail": ev.get("detail") or "Risk / exit condition on a held name (%s)." % reason})
            categories.add(TRIGGER_HARD_RISK)
            reasons.append("%s triggered a risk / exit condition (%s)." % (tk, reason))

    # 3a. MATERIAL MEMBERSHIP — a held name left the target beyond the exit buffer.
    for tk in sorted(held - tgt):
        if tk in removed:
            continue
        rank = ranked_current.get(tk)
        inside_buffer = (rank is not None and exit_buffer_rank is not None
                         and rank <= exit_buffer_rank)
        if inside_buffer:
            continue  # hysteresis — within the exit buffer, hold (no churn)
        removed.add(tk)
        detail = ("Current combined rank %s is beyond the exit buffer (> %s)."
                  % (rank, exit_buffer_rank) if rank is not None else
                  "No longer in the ranked combined target universe.")
        proposed_removals.append({
            "ticker": tk, "current_weight": _r6(_hw(tk)),
            "trigger_category": TRIGGER_MEMBERSHIP_CHANGE,
            "reason": "MEMBERSHIP_RANK_BEYOND_BUFFER", "hard_event": False,
            "current_rank": rank, "detail": detail})
        categories.add(TRIGGER_MEMBERSHIP_CHANGE)
        reasons.append("%s left the combined target beyond the exit buffer." % tk)

    # 3b. MATERIAL MEMBERSHIP — a target name that is not yet held (genuine entrant).
    for tk in sorted(tgt - held):
        row = target.get(tk) or {}
        proposed_additions.append({
            "ticker": tk, "target_weight": _r6(_f(row.get("weight"))),
            "trigger_category": TRIGGER_MEMBERSHIP_CHANGE,
            "reason": "ENTERED_COMBINED_TARGET", "hard_event": False,
            "target_rank": row.get("rank"), "sector": row.get("sector"),
            "detail": "Entered the combined Top-%d target (rank %s)." % (n_target, row.get("rank"))})
        categories.add(TRIGGER_MEMBERSHIP_CHANGE)
        reasons.append("%s entered the combined target." % tk)

    resized: set[str] = set()

    # 4. MATERIAL WEIGHT DRIFT — held & targeted names whose weight drifted materially.
    for tk in sorted(held & tgt):
        if tk in removed:
            continue
        cw = _hw(tk)
        tw = _f((target.get(tk) or {}).get("weight"))
        if cw is None or tw is None:
            continue
        drift = cw - tw
        if abs(drift) >= pol["material_weight_drift"]:
            proposed_resizes.append({
                "ticker": tk, "current_weight": _r6(cw), "target_weight": _r6(tw),
                "weight_drift": _r6(drift), "trigger_category": TRIGGER_WEIGHT_DRIFT,
                "reason": "WEIGHT_DRIFT_BEYOND_BAND", "hard_event": False,
                "detail": "Actual weight drifted %.4f from target (>= %.2f band)."
                          % (drift, pol["material_weight_drift"])})
            resized.add(tk)
            categories.add(TRIGGER_WEIGHT_DRIFT)
            reasons.append("%s weight drifted materially from target." % tk)

    # 4b. POSITION WEIGHT LIMIT — a held weight above the hard individual cap.
    max_w = pol["max_individual_weight"]
    position_breaches: list[str] = []
    for tk in sorted(held - removed):
        cw = _hw(tk)
        if cw is None or cw <= max_w + _EPS:
            continue
        position_breaches.append(tk)
        categories.add(TRIGGER_POSITION_LIMIT)
        reasons.append("%s weight %.4f exceeds the %.2f individual-position cap (resize toward target)."
                       % (tk, cw, max_w))
        if tk not in resized:
            tw = _f((target.get(tk) or {}).get("weight"))
            if tw is None or tw > max_w:
                tw = max_w
            proposed_resizes.append({
                "ticker": tk, "current_weight": _r6(cw), "target_weight": _r6(tw),
                "weight_drift": _r6(cw - tw), "trigger_category": TRIGGER_POSITION_LIMIT,
                "reason": "POSITION_WEIGHT_ABOVE_CAP", "hard_event": True,
                "detail": "Weight %.4f exceeds the %.2f cap — deterministic resize toward the "
                          "current target weight (not auto-executed)." % (cw, max_w)})
            resized.add(tk)

    # 4c. SECTOR CONCENTRATION — actual known-sector exposure above the cap.
    sec_cap = pol["sector_cap_fraction"]
    actual_sec = _sector_weights(holdings, exclude=removed)
    target_sec = _target_sector_weights(target)
    sector_breaches: list[str] = []
    for sec in sorted(actual_sec):
        aw = actual_sec[sec]
        if aw <= sec_cap + _EPS:
            continue
        tw_sec = target_sec.get(sec, 0.0)
        if tw_sec <= sec_cap + _EPS:
            # Model target is compliant -> propose resizing over-weight held names toward target.
            sector_breaches.append(sec)
            categories.add(TRIGGER_SECTOR_CONCENTRATION)
            reasons.append("Sector %s actual exposure %.4f exceeds the %.2f cap; resize toward target."
                           % (sec, aw, sec_cap))
            for tk in sorted(held - removed):
                if (holdings.get(tk) or {}).get("sector") != sec or tk not in tgt or tk in resized:
                    continue
                cw = _hw(tk)
                tw = _f((target.get(tk) or {}).get("weight"))
                if cw is None or tw is None or abs(cw - tw) < _EPS:
                    continue
                proposed_resizes.append({
                    "ticker": tk, "current_weight": _r6(cw), "target_weight": _r6(tw),
                    "weight_drift": _r6(cw - tw), "trigger_category": TRIGGER_SECTOR_CONCENTRATION,
                    "reason": "SECTOR_ABOVE_CAP", "hard_event": True, "sector": sec,
                    "detail": "Sector %s exceeds the %.2f cap — resize toward the current target "
                              "weight (not auto-executed)." % (sec, sec_cap)})
                resized.add(tk)
        else:
            # The model target ITSELF violates its cap -> data/integrity blocker, no invented trade.
            sector_integrity.append(sec)
            categories.add(TRIGGER_SECTOR_CONCENTRATION)
            blocked_changes.append({
                "type": "SECTOR_INTEGRITY", "sector": sec, "ticker": None,
                "actual_weight": _r6(aw), "target_weight": _r6(tw_sec),
                "blocked_reason": "TARGET_SECTOR_CAP_VIOLATION",
                "detail": "Sector %s exceeds the %.2f cap in BOTH the actual book (%.4f) and the "
                          "model target (%.4f). Treated as a data/integrity blocker — no trade is "
                          "invented." % (sec, sec_cap, aw, tw_sec)})
            reasons.append("Sector %s exceeds the cap in both actual and target — integrity review "
                           "required; no trade invented." % sec)

    # 5. MATERIALITY / COST-CONTROL — suppress a resize-ONLY proposal below the floor.
    has_hard = bool(categories & {TRIGGER_HARD_ELIGIBILITY, TRIGGER_HARD_RISK})
    has_membership = bool(proposed_additions) or any(
        r["trigger_category"] == TRIGGER_MEMBERSHIP_CHANGE for r in proposed_removals)
    has_concentration = bool(position_breaches) or bool(sector_breaches)
    resize_turnover = 0.5 * sum(abs(r.get("weight_drift") or 0.0) for r in proposed_resizes)
    if proposed_resizes and not has_hard and not has_membership and not has_concentration:
        if resize_turnover < pol["min_action_turnover"]:
            for r in proposed_resizes:
                blocked_changes.append({
                    **r, "blocked_reason": "MATERIALITY_FLOOR_NOT_MET",
                    "detail": ("Resize turnover %.4f below the minimum-action floor %.2f — the "
                               "modeled paper execution cost is not justified by so small a change."
                               % (resize_turnover, pol["min_action_turnover"]))})
            proposed_resizes = []
            categories.discard(TRIGGER_WEIGHT_DRIFT)
            categories.add(TRIGGER_MATERIALITY_COST_CONTROL)
            reasons.append("Weight-drift resizes suppressed by the materiality / cost-control check "
                           "(turnover %.4f < %.2f floor)."
                           % (resize_turnover, pol["min_action_turnover"]))

    # Scheduled full review — a mandatory comprehensive checkpoint (never a suppressor).
    if scheduled_review_due:
        categories.add(TRIGGER_SCHEDULED_REVIEW)
        reasons.append("Scheduled full portfolio review is due (%s)."
                       % (next_scheduled_full_review or "today"))

    # Turnover / cost (one-way turnover fraction; cost at 12.5 bps per traded side).
    traded = (sum(_f(a.get("target_weight")) or 0.0 for a in proposed_additions)
              + sum(_f(r.get("current_weight")) or 0.0 for r in proposed_removals)
              + sum(abs(r.get("weight_drift") or 0.0) for r in proposed_resizes))
    estimated_turnover = _r6(0.5 * traded)
    estimated_cost = _r6(traded * pol.get("execution_cost_rate_per_side",
                                          EXECUTION_COST_RATE_PER_SIDE))

    any_change = bool(proposed_additions or proposed_removals or proposed_resizes)
    target_actual_match = bool(data_ready and held == tgt and not proposed_resizes
                               and not any(eligibility.get(tk, {}).get("eligible") is False
                                           for tk in held))

    # -- outcome --------------------------------------------------------------- #
    if not data_ready:
        outcome = OUTCOME_DATA_NOT_READY
    elif orders_pending:
        outcome = OUTCOME_ORDERS_SUBMITTED
    elif any_change:
        outcome = OUTCOME_PROPOSAL_READY
    else:
        outcome = OUTCOME_NO_ACTION_TODAY

    action_required = outcome in (OUTCOME_DATA_NOT_READY, OUTCOME_PROPOSAL_READY,
                                  OUTCOME_APPROVAL_REQUIRED)

    pres = dict(_PRESENTATION[outcome])
    # Phase 28B wording: the no-action decision is anchored to the latest
    # COMPLETED close, never an ambiguous "today" (the underlying date is
    # untouched — only the label carries it explicitly).
    if outcome == OUTCOME_NO_ACTION_TODAY and latest_completed_market_date:
        pres["headline"] = ("NO PORTFOLIO CHANGE REQUIRED FROM THE LATEST "
                            "COMPLETED CLOSE — %s" % latest_completed_market_date)
    target_state, target_state_label = _target_state_for(outcome)
    explanation = _explanation(outcome, proposed_additions, proposed_removals,
                               proposed_resizes, blocked_changes, scheduled_review_due,
                               next_scheduled_full_review)

    checks_performed, checks_summary = _build_checks(
        holdings=holdings, target=target, removed=removed, eligibility=eligibility,
        proposed_removals=proposed_removals, proposed_additions=proposed_additions,
        proposed_resizes=proposed_resizes, blocked_changes=blocked_changes,
        sector_integrity=sector_integrity, data_ready=data_ready, risk_ready=risk_ready,
        scheduled_review_due=scheduled_review_due, next_review=next_scheduled_full_review,
        market_date=latest_completed_market_date, pol=pol)

    return {
        "phase": PHASE,
        "evaluation_date": evaluation_date,
        "latest_completed_market_date": latest_completed_market_date,
        "next_scheduled_full_review": next_scheduled_full_review,
        "scheduled_review_due": bool(scheduled_review_due),
        "outcome": outcome,
        "outcome_label": pres["label"],
        "target_state": target_state,
        "target_state_label": target_state_label,
        "headline": pres["headline"],
        "explanation": explanation,
        "action_required": bool(action_required),
        "action_severity": pres["severity"],
        "primary_action_label": pres["primary_action_label"],
        "current_task": pres["current_task"],
        "trigger_categories": sorted(categories),
        "trigger_reasons": reasons,
        "proposed_additions": proposed_additions,
        "proposed_removals": proposed_removals,
        "proposed_resizes": proposed_resizes,
        "blocked_changes": blocked_changes,
        "proposed_change_count": len(proposed_additions) + len(proposed_removals)
        + len(proposed_resizes),
        "estimated_turnover": estimated_turnover,
        "estimated_cost": estimated_cost,
        "current_target_count": n_target,
        "actual_holding_count": len(held),
        "target_actual_match": target_actual_match,
        "checks_performed": checks_performed,
        "checks_summary": checks_summary,
        "data_ready": bool(data_ready),
        "risk_ready": bool(risk_ready),
        "book_active": bool(book_active),
        "orders_pending": int(orders_pending or 0),
        "exit_buffer_rank": exit_buffer_rank,
        "outcome_vocabulary": list(ALL_OUTCOMES),
        "target_state_vocabulary": list(ALL_TARGET_STATES),
        "trigger_vocabulary": list(ALL_TRIGGER_CATEGORIES),
        "check_vocabulary": list(ALL_CHECK_CODES),
        "policy": pol,
        "strategy_name": STRATEGY_NAME,
        "target_book_name": TARGET_BOOK_NAME,
        "daily_recalculation_note": (
            "Daily recalculation refreshes current prices, ranks, risk, eligibility, the "
            "combined target and the target-versus-actual comparison from the EXISTING frozen "
            "model. It does not retrain, refit or optimise any model parameter, does not replace "
            "the champion, does not promote a sleeve, and keeps the fast sleeve inactive."),
        "generated_at": _now_iso(),
        **_safety(),
    }


def _explanation(outcome: str, adds: list, rems: list, resizes: list, blocked: list,
                 scheduled_due: bool, next_review: Optional[str]) -> str:
    if outcome == OUTCOME_DATA_NOT_READY:
        return ("The latest completed market data / model scores are not yet available, so no "
                "target-versus-actual comparison can be made today. Run the manual after-market "
                "refresh; no portfolio change is evaluated until the data is current.")
    if outcome == OUTCOME_ORDERS_SUBMITTED:
        return ("Paper orders from a prior proposal are still working. The daily gate defers "
                "until they settle; refresh after the next eligible close. No orders are "
                "created automatically.")
    if outcome == OUTCOME_PROPOSAL_READY:
        return ("A material event produced %d proposed change(s): %d addition(s), %d removal(s), "
                "%d resize(s). Manual review and approval are required — no orders are created "
                "automatically, no broker, no live orders."
                % (len(adds) + len(rems) + len(resizes), len(adds), len(rems), len(resizes)))
    # NO_ACTION_TODAY
    base = ("No material risk, eligibility, target-membership, drift or concentration trigger "
            "fired against the current holdings. Continue monitoring the existing holdings.")
    if scheduled_due:
        base = ("The scheduled full portfolio review ran a complete target-versus-actual "
                "comparison and found no material change. " + base)
    if blocked:
        base += (" %d immaterial change(s) were suppressed by the materiality / cost-control check."
                 % len(blocked))
    if next_review:
        base += " Next scheduled full review: %s." % next_review
    return base


# --------------------------------------------------------------------------- #
# Composition loader — assemble the pure inputs from existing read-only services
# --------------------------------------------------------------------------- #
def _default_engine_current():
    return eng.build_current()


def _default_operational_loader(today: Optional[str] = None):
    from paper_trader.api import operational_book as ob
    return ob.load_operational_book(today=today)


# Injectable seams (mirror the pattern used across the operational modules). Tests
# swap these to run fully offline; production uses the owned-data engine + book.
_ENGINE_CURRENT_LOADER = _default_engine_current
_OPERATIONAL_BOOK_LOADER = _default_operational_loader


def _holdings_from_operational(cs: dict, ob_book: dict) -> dict:
    """Actual held names + current weights + sector from the canonical operational payload."""
    holdings: dict[str, dict] = {}
    for r in (cs.get("holdings_detail") or ob_book.get("holdings_detail") or []):
        tk = r.get("ticker")
        if tk:
            holdings[tk] = {"weight": r.get("current_weight"), "quantity": r.get("quantity"),
                            "sector": r.get("sector")}
    if not holdings:  # degrade: qty map with unknown weights (drift simply not evaluated)
        for tk, q in (ob_book.get("holdings") or {}).items():
            holdings[tk] = {"weight": None, "quantity": q, "sector": None}
    return holdings


def _target_from_current(current: dict) -> tuple[dict, int]:
    """The recomputed combined Top-N target book (owned-data model; no tunnel)."""
    target: dict[str, dict] = {}
    books = current.get("books") or {}
    book = (books.get("books") or {}).get(books.get("primary_book_id"))
    if not book:
        return target, 0
    eqw = book.get("equal_weight")
    for c in (book.get("constituents") or []):
        tk = c.get("ticker")
        if tk:
            target[tk] = {"weight": c.get("weight") if c.get("weight") is not None else eqw,
                          "rank": c.get("rank"), "sector": c.get("sector"),
                          "adv_dollar": c.get("adv_dollar")}
    n = book.get("size_actual") or len(target)
    return target, int(n or 0)


def _enrich_holdings(holdings: dict, target: dict, risk_pack: dict) -> None:
    """Fill sector / ADV / realized-vol / drawdown for held names from the owned risk
    pack + target constituents (in place). Read-only; degrades to None fields."""
    for tk, h in holdings.items():
        rp = risk_pack.get(tk) or {}
        trow = target.get(tk) or {}
        if not h.get("sector"):
            h["sector"] = trow.get("sector") or rp.get("sector")
        adv = trow.get("adv_dollar")
        if adv is None:
            adv = rp.get("adv_dollar_20d")
        h["adv_dollar"] = adv
        h["realized_vol"] = rp.get("realized_vol_63d")
        h["max_drawdown"] = rp.get("max_drawdown_252d")


def _signals_from_recs(current: dict, held: set, size: int) -> tuple[dict, dict, list]:
    """Per-held-name current rank + eligibility + risk events, from the engine's own
    target-versus-holdings diff (holdings supplied as the ``prior`` book, full review
    forced so the comparison is never gated by the monthly review cadence)."""
    ranked_current: dict[str, Any] = {}
    eligibility: dict[str, dict] = {}
    risk_events: list[dict] = []
    prior = {mreg.SLEEVE_COMBINED: {"constituents_top%d" % (size or 25): sorted(held),
                                    "period": None}}
    recs = eng.compute_recommendations(current, prior, mreg.SLEEVE_COMBINED,
                                       size=size or 25, review_due=True)
    for row in (recs.get("recommendations") or []):
        tk = row.get("ticker")
        if not tk:
            continue
        codes = row.get("reason_codes") or []
        ranked_current[tk] = (row.get("model_ranks") or {}).get("current")
        if tk in held and row.get("recommendation") == eng.REC_EXIT:
            hard_elig = next((c for c in codes if c in HARD_ELIGIBILITY_CODES), None)
            hard_risk = next((c for c in codes if c in HARD_RISK_CODES), None)
            if hard_elig:
                eligibility[tk] = {"eligible": False, "reason": hard_elig}
            elif hard_risk:
                risk_events.append({"ticker": tk, "reason": hard_risk,
                                    "detail": "Engine flagged a hard risk / data-integrity "
                                              "exit (%s)." % hard_risk})
    return ranked_current, eligibility, risk_events


def load_daily_action_gate(*, today: Optional[str] = None, current: Optional[dict] = None,
                           operational: Optional[dict] = None) -> dict:
    """Assemble and evaluate the canonical daily action gate. Read-only; degrades to
    ``DATA_NOT_READY`` (never a stack trace) when the owned model inputs are absent."""
    warnings: list[str] = []

    # 1. The ONE operational book — holdings, review clock, lifecycle, pending orders.
    try:
        ops = operational or _OPERATIONAL_BOOK_LOADER(today)
    except Exception as exc:  # noqa: BLE001
        ops = {}
        warnings.append("Operational book unavailable: %s" % str(exc)[:160])
    cs = (ops or {}).get("canonical_state") or {}
    ob_book = (ops or {}).get("operational_book") or {}

    holdings = _holdings_from_operational(cs, ob_book)
    orders_pending = int(cs.get("pending_order_count") or ob_book.get("pending_order_count") or 0)
    fills_count = int(cs.get("fill_count") or ob_book.get("fill_count") or 0)
    book_active = bool((cs.get("lifecycle_stage") == "FILLED" or fills_count)
                       and not orders_pending)
    next_review = cs.get("next_review_date")
    scheduled_review_due = bool(cs.get("review_due"))
    desk_mark_date = cs.get("desk_valuation_date") or cs.get("valuation_date")
    target_count = int(cs.get("target_count") or ob_book.get("target_count") or 0)

    # 2. The recomputed owned-data combined target (frozen model; NO prediction tunnel).
    try:
        cur = current if current is not None else _ENGINE_CURRENT_LOADER()
    except Exception as exc:  # noqa: BLE001
        cur = {"status": eng.STATUS_INPUTS_UNAVAILABLE}
        warnings.append("Model current unavailable: %s" % str(exc)[:160])
    data_engine_ready = (cur.get("status") == eng.STATUS_READY)
    market_date = cur.get("market_as_of_date")
    risk_pack = (cur.get("inputs") or {}).get("risk") or {}
    risk_ready = bool(risk_pack)

    target: dict = {}
    ranked_current: dict = {}
    eligibility: dict = {}
    risk_events: list = []
    if data_engine_ready:
        try:
            target, tc = _target_from_current(cur)
            if tc:
                target_count = tc
            ranked_current, eligibility, risk_events = _signals_from_recs(
                cur, set(holdings), target_count or len(target) or 25)
            _enrich_holdings(holdings, target, risk_pack)
        except Exception as exc:  # noqa: BLE001 — degrade to DATA_NOT_READY, never crash
            warnings.append("Model target diff unavailable: %s" % str(exc)[:160])
            data_engine_ready = False

    # Data is ready only when BOTH the model target and the desk marks are current.
    desk_marks_ready = (cs.get("desk_mark_status") == "DESK_MARK_READY")
    data_ready = bool(data_engine_ready and target)

    result = evaluate_daily_action_gate(
        holdings=holdings, target=target, ranked_current=ranked_current,
        eligibility=eligibility, risk_events=risk_events, target_count=target_count,
        next_scheduled_full_review=next_review, scheduled_review_due=scheduled_review_due,
        data_ready=data_ready, risk_ready=risk_ready, orders_pending=orders_pending,
        book_active=book_active, evaluation_date=(today or date.today().isoformat()),
        latest_completed_market_date=(market_date or desk_mark_date))

    result["status"] = "DAILY_ACTION_GATE_OK"
    result["operational_book_id"] = ob_book.get("book_id") or "alpha_paper_book_1"
    result["operational_book_label"] = ob_book.get("book_label") or "Alpha Paper Book #1"
    result["desk_mark_date"] = desk_mark_date
    result["desk_marks_ready"] = bool(desk_marks_ready)
    result["fill_count"] = fills_count
    result["holdings_count"] = int(cs.get("holdings_count") or ob_book.get("holdings_count") or 0)
    result["nav"] = cs.get("nav")
    result["cash"] = cs.get("cash")
    result["lifecycle_stage"] = cs.get("lifecycle_stage")
    result["review_cadence"] = cs.get("review_cadence") or "MONTHLY"
    # Canonical operational date bundle (Phase 27D — the ONE set operator pages use,
    # never the archived/reconstructed legacy portfolio date).
    result["operational_dates"] = {
        "evaluation_date": result.get("evaluation_date"),
        "latest_completed_market_date": result.get("latest_completed_market_date"),
        "desk_mark_date": desk_mark_date,
        "book_valuation_date": cs.get("valuation_date") or desk_mark_date,
        "next_scheduled_full_review": next_review,
    }
    result["warnings"] = warnings
    result.update(_safety())
    return result


__all__ = [
    "PHASE",
    "OUTCOME_DATA_NOT_READY", "OUTCOME_NO_ACTION_TODAY", "OUTCOME_PROPOSAL_READY",
    "OUTCOME_APPROVAL_REQUIRED", "OUTCOME_ORDERS_SUBMITTED", "OUTCOME_FORWARD_TRACKING",
    "ALL_OUTCOMES",
    "TARGET_STATE_CURRENT_ALIGNED", "TARGET_STATE_PROPOSAL_READY",
    "TARGET_STATE_APPROVAL_REQUIRED", "TARGET_STATE_DATA_NOT_READY",
    "TARGET_STATE_ORDERS_SUBMITTED", "TARGET_STATE_FORWARD_TRACKING", "ALL_TARGET_STATES",
    "TRIGGER_HARD_ELIGIBILITY", "TRIGGER_HARD_RISK", "TRIGGER_MEMBERSHIP_CHANGE",
    "TRIGGER_WEIGHT_DRIFT", "TRIGGER_POSITION_LIMIT", "TRIGGER_SECTOR_CONCENTRATION",
    "TRIGGER_MATERIALITY_COST_CONTROL", "TRIGGER_ECONOMIC_GATE", "TRIGGER_SCHEDULED_REVIEW",
    "ALL_TRIGGER_CATEGORIES",
    "ALL_CHECK_CODES", "CHECK_LABELS",
    "CHK_PASS", "CHK_WARN", "CHK_TRIGGERED", "CHK_NOT_AVAILABLE", "CHK_MONITOR_ONLY",
    "EXECUTION_COST_BPS_PER_SIDE", "MEMBERSHIP_EXIT_BUFFER_FRACTION",
    "MATERIAL_WEIGHT_DRIFT", "MIN_ACTION_TURNOVER", "MAX_INDIVIDUAL_WEIGHT",
    "SECTOR_CAP_FRACTION", "MIN_ADV_DOLLAR",
    "HARD_ELIGIBILITY_CODES", "HARD_RISK_CODES",
    "evaluate_daily_action_gate", "load_daily_action_gate",
]
