r"""Stage 20 — Continuous Active Portfolio Reassessment Engine (pure calculation kernel).

This module is the ONE canonical *portfolio-level* reassessment and economic
change-gate calculation. It is a **pure, deterministic kernel**: it performs NO I/O —
no file, database, network, provider or prediction access — and never mutates its
inputs. Everything it needs arrives as one immutable reassessment-input contract
(built by ``api.portfolio_reassessment`` from the authoritative owners), and it
returns one immutable reassessment result.

The structural gap Stage 20 closes
----------------------------------
Before Stage 20 the Daily Research Cycle ran::

    ASSESS_HOLDING_OPPORTUNITY_COST  ->  BUILD_REALLOCATION_PROPOSAL

*unconditionally*. Every signal refresh therefore produced a change target, and the
only "should we act at all?" judgement happened downstream in Stage 18, derived from
the action counts of a target the allocation engine had **already built**. In other
words the system rebalanced-by-default and asked the operator to say no. There was no
portfolio-level economic gate, no turnover budget, no churn/whipsaw protection and no
immutable record of the decision *not* to act.

Stage 20 inserts the missing owner between them::

    ASSESS_HOLDING_OPPORTUNITY_COST
        -> REASSESS_PORTFOLIO      (this kernel: is change economically justified?)
        -> BUILD_REALLOCATION_PROPOSAL   (only when PROPOSAL_REQUIRED)

What this kernel DOES
---------------------
It aggregates the per-holding Slice-6 Holding Opportunity-Cost assessment
(``engine.holding_opportunity_cost`` — the ONE holding comparison calculation, never
re-derived here) into ONE portfolio-level decision:

  * an expected gross / risk-adjusted / net score improvement for the portfolio,
    weighted by the capital each recommendation would actually move;
  * the expected one-way turnover (== portfolio distance) and the implied transaction
    cost, using the canonical desk cost model — counted exactly ONCE;
  * the concentration consequence of the recommended exits/replacements on the
    RETAINED book (renormalised arithmetic — never an allocation);
  * deterministic churn / whipsaw protection (cooldown, reversal, turnover budget);
  * a point-in-time / data-quality verdict over the declared inputs;
  * ONE decision from the frozen vocabulary NOT_READY / CURRENT_NO_CHANGE /
    CHANGE_CANDIDATE / PROPOSAL_READY / BLOCKED_DATA / BLOCKED_EVIDENCE /
    MANUAL_REVIEW_REQUIRED;
  * a deterministic, generated (never LLM) explanation for every holding.

What this kernel NEVER does
---------------------------
  * It NEVER builds a target portfolio and NEVER assigns capital to a candidate — that
    is owned exactly once by ``engine.reallocation_proposal`` (Slice 7). The
    concentration arithmetic here only renormalises the *retained* incumbents, which is
    the unavoidable consequence of an exit, not an allocation.
  * It NEVER recomputes a holding comparison, a switching cost, a covariance risk
    contribution or a rank — every such value is read from the Slice-6 assessment.
  * It creates NO order, NO fill, NO target; it changes NO holding, cash or NAV.
  * It approves nothing and promotes/recalibrates no model. A PROPOSAL_READY decision
    still requires the Stage-18 manual approval and the Stage-19 order-plan
    confirmation before a single paper order can exist.

Expected *return* is never fabricated: no validated forecast model exists, so every
improvement is a signal-SCORE comparison (percentile points) and is labelled as such.
Switching cost is genuinely in basis points (the canonical desk cost model), so it is
reported in bps.

The reused thresholds come from the Slice-6 / Slice-7 policies and the canonical
``api.multi_horizon_engine`` / ``api.paper_trading_desk`` constants (the API owner
injects the live values). The genuinely-new Stage-20 thresholds are declared once in
:func:`default_policy`, returned in the payload, folded into the deterministic hash and
exercised at their boundaries by the test suite.
"""
from __future__ import annotations

import math
from typing import Any, Optional

from paper_trader.engine import holding_opportunity_cost as hoc_kernel

SCHEMA_VERSION = "portfolio_reassessment.v1"
INPUT_SCHEMA_VERSION = "portfolio_reassessment.input.v1"
REASSESSMENT_POLICY_VERSION = "portfolio_reassessment_policy.v1"
CHURN_POLICY_VERSION = "portfolio_reassessment_churn_policy.v1"

CALCULATION_OWNER = "engine.portfolio_reassessment"

#: The single holding-comparison calculation this kernel aggregates. It is NEVER
#: re-implemented here (asserted by the Stage-20 architecture guard).
HOC_KERNEL_OWNER = hoc_kernel.CALCULATION_OWNER
#: The single target-portfolio calculation. This kernel never builds a target.
TARGET_ENGINE_OWNER = "engine.reallocation_proposal"

# --------------------------------------------------------------------------- #
# Frozen reassessment-decision vocabulary (Workstream B).
# --------------------------------------------------------------------------- #
#: Inputs for a reassessment do not exist yet (no active book / no HOC / no session).
STATE_NOT_READY = "NOT_READY"
#: The current portfolio remains the best risk-adjusted use of capital in the envelope.
STATE_NO_CHANGE = "CURRENT_NO_CHANGE"
#: Attractive replacements exist but portfolio economics are not compelling or a
#: deterministic control (turnover budget / concentration / churn) blocks acting.
STATE_CHANGE_CANDIDATE = "CHANGE_CANDIDATE"
#: The portfolio-level economics clear the gate -> the canonical Slice-7 proposal owner
#: must build (or reuse) the reviewable proposal. NOTHING is approved by this state.
STATE_PROPOSAL_READY = "PROPOSAL_READY"
#: A required input is missing / incomplete beyond policy.
STATE_BLOCKED_DATA = "BLOCKED_DATA"
#: The evidence bound to the assessment no longer describes the current portfolio
#: (e.g. a corporate action was registered after the assessment was produced).
STATE_BLOCKED_EVIDENCE = "BLOCKED_EVIDENCE"
#: A holding breaches a hard constraint that a human must adjudicate.
STATE_MANUAL_REVIEW = "MANUAL_REVIEW_REQUIRED"

REASSESSMENT_STATE_VOCAB = (STATE_NOT_READY, STATE_NO_CHANGE, STATE_CHANGE_CANDIDATE,
                            STATE_PROPOSAL_READY, STATE_BLOCKED_DATA,
                            STATE_BLOCKED_EVIDENCE, STATE_MANUAL_REVIEW)

#: The states in which the Slice-7 proposal owner may be invoked by the cycle.
PROPOSAL_ELIGIBLE_STATES = (STATE_PROPOSAL_READY,)

#: The states that represent a durable, persistable reassessment outcome.
PERSISTABLE_STATES = (STATE_NO_CHANGE, STATE_CHANGE_CANDIDATE, STATE_PROPOSAL_READY,
                      STATE_BLOCKED_DATA, STATE_BLOCKED_EVIDENCE, STATE_MANUAL_REVIEW)

# --- HOC recommendation vocabulary (re-exported, never forked) --------------- #
REC_HOLD = hoc_kernel.REC_HOLD
REC_REDUCE = hoc_kernel.REC_REDUCE
REC_EXIT = hoc_kernel.REC_EXIT
REC_REPLACE = hoc_kernel.REC_REPLACE
REC_ADD = hoc_kernel.REC_ADD

#: Recommendations that would actually move capital out of an incumbent.
ACTIONABLE_RECOMMENDATIONS = (REC_EXIT, REC_REPLACE, REC_REDUCE)

# --- Input-freshness vocabulary (Workstream F) ------------------------------- #
FRESH = "FRESH"
STALE_BUT_VALID = "STALE_BUT_VALID"
UNAVAILABLE = "UNAVAILABLE"
POINT_IN_TIME_GAP = "POINT_IN_TIME_GAP"
PROVIDER_BLOCKED = "PROVIDER_BLOCKED"
FRESHNESS_VOCAB = (FRESH, STALE_BUT_VALID, UNAVAILABLE, POINT_IN_TIME_GAP,
                   PROVIDER_BLOCKED)
#: Classifications that can never satisfy a REQUIRED input.
_FRESHNESS_FATAL = frozenset({UNAVAILABLE, POINT_IN_TIME_GAP, PROVIDER_BLOCKED})

# --- Input-usage vocabulary (Workstream F: what happened to each input) ------ #
USAGE_REFRESHED = "REFRESHED_THIS_RUN"
USAGE_REUSED = "REUSED"
USAGE_STALE = "STALE"
USAGE_MISSING = "MISSING"
USAGE_BLOCKED = "BLOCKED"
USAGE_VOCAB = (USAGE_REFRESHED, USAGE_REUSED, USAGE_STALE, USAGE_MISSING, USAGE_BLOCKED)

# --- Churn / whipsaw reason codes ------------------------------------------- #
CHURN_COOLDOWN = "CHURN_COOLDOWN_ACTIVE"
CHURN_REVERSAL = "REVERSAL_PROTECTION_ACTIVE"
CHURN_TURNOVER_BUDGET = "TURNOVER_BUDGET_EXCEEDED"

# --- Blocker / gate reason codes --------------------------------------------- #
GATE_NO_ACTIONABLE = "NO_ACTIONABLE_HOLDING"
GATE_BELOW_NET_HURDLE = "BELOW_PORTFOLIO_NET_IMPROVEMENT_HURDLE"
GATE_NET_NON_POSITIVE = "NET_IMPROVEMENT_NON_POSITIVE_AFTER_COST"
GATE_CLEARED = "PORTFOLIO_NET_IMPROVEMENT_CLEARS_HURDLE"
GATE_MANDATORY_EXIT = "MANDATORY_EXIT_INELIGIBLE_HOLDING"
GATE_CONCENTRATION = "CONCENTRATION_DETERIORATION_BLOCKS_CHANGE"
GATE_SECTOR_CAP = "SECTOR_CAP_BREACH_BLOCKS_CHANGE"
GATE_LIQUIDITY = "LIQUIDITY_BLOCKS_CHANGE"
GATE_RISK_DETERIORATION = "RISK_DETERIORATION_BLOCKS_CHANGE"
GATE_IMPROVEMENT_UNMEASURABLE = "IMPROVEMENT_NOT_MEASURABLE"

# --- Release 29.3 — WHICH OBJECT EACH CONSTRAINT IS ENTITLED TO JUDGE -------- #
# A constraint may only be decided on the business object that actually determines
# it. This kernel sees the RELEASE SET (which incumbents give capital back); it does
# NOT see the complete target, because released capital is allocated exactly once by
# ``engine.reallocation_proposal``. Concentration, sector concentration, post-change
# risk and the turnover BUDGET are all properties of that complete target, so this
# kernel reports them as PRE-PROPOSAL CONTEXT and never as a blocker.
#
# Evidence that forced the split (live 2026-08-17 reassessment
# prs_2026-08-17_alpha_paper_book_1_7edb4353341f): the release set freed ~49.6% of the
# book, leaving ``retained_invested_weight = 0.504258``. Renormalising the retained
# stub to 1.0 scaled every surviving weight by ~1.98x, so ``max_name_weight`` "rose"
# 0.044184 -> 0.081571 and ``max_sector_weight`` "rose" 0.325195 -> 0.374216 without a
# single dollar moving into any of those names — and the sector comparison was between
# two DIFFERENT sectors ("Unknown" before, "Information Technology" after). Those are
# renormalisation artifacts of an intentionally incomplete portfolio, not economics.
CONSTRAINT_OWNER_COMPLETE_TARGET = TARGET_ENGINE_OWNER
#: Codes this kernel is entitled to raise as blockers (properties of the release set).
RELEASE_SET_BLOCKER_CODES = (GATE_IMPROVEMENT_UNMEASURABLE, GATE_BELOW_NET_HURDLE,
                             GATE_LIQUIDITY)
#: Codes MOVED to the complete-target owner in Release 29.3. This kernel still
#: publishes the underlying arithmetic (as context) but never blocks on it.
COMPLETE_TARGET_CONSTRAINT_CODES = (GATE_CONCENTRATION, GATE_RISK_DETERIORATION,
                                    GATE_SECTOR_CAP, CHURN_TURNOVER_BUDGET)

# --- Mandatory eligibility-exit policy (Release 29.3 — made explicit) -------- #
#: An eligibility exit is a CONSTRAINT breach, not an alpha bet, so it overrides the
#: purely ECONOMIC gates: a sub-hurdle or unmeasurable score improvement must never
#: trap an ineligible name in the book. It NEVER overrides a HARD feasibility blocker
#: (liquidity / churn protection), and it NEVER authorises an order, a naked sell-only
#: plan, or a bypass of the complete-target constraints owned by the proposal engine.
MANDATORY_EXIT_POLICY_VERSION = "mandatory_eligibility_exit_policy.v1"
MANDATORY_EXIT_POLICY = "ELIGIBILITY_EXIT_OVERRIDES_ECONOMIC_GATES_ONLY"
#: The economic gates a mandatory eligibility exit is allowed to override.
MANDATORY_EXIT_OVERRIDES = (GATE_BELOW_NET_HURDLE, GATE_IMPROVEMENT_UNMEASURABLE)
#: The hard feasibility blockers it can NEVER override (a human adjudicates instead).
MANDATORY_EXIT_HARD_BLOCKERS = (GATE_LIQUIDITY, CHURN_COOLDOWN, CHURN_REVERSAL)
#: Raised when an eligibility exit is real but a hard blocker withholds the ASK. The
#: operator wording MUST then say "required if a reallocation proceeds", never
#: "must exit now" — the exit is not an executable obligation in this state.
GATE_MANDATORY_EXIT_WITHHELD = "MANDATORY_EXIT_WITHHELD_BY_HARD_BLOCKER"

#: The improvement unit. There is NO validated expected-return model anywhere in the
#: system, so a portfolio improvement is a SIGNAL-SCORE comparison in percentile points
#: and is never presented as basis points / dollars of expected return.
IMPROVEMENT_BASIS = hoc_kernel.IMPROVEMENT_BASIS
EXPECTED_RETURN_STATE = "EXPECTED_RETURN_NOT_CALIBRATED"

#: Portfolio volatility before/after is owned by the Slice-7 proposal risk block (it
#: needs the proposed target). The reassessment gate never fabricates one.
VOLATILITY_AFTER_STATE_PRE_PROPOSAL = "NOT_AVAILABLE_PRE_PROPOSAL"


# --------------------------------------------------------------------------- #
# Policy (Workstream D + E) — ONE versioned decision policy.
# --------------------------------------------------------------------------- #
def default_policy() -> dict[str, Any]:
    """The single explicit, versioned Stage-20 reassessment + churn policy.

    Values marked ``reused`` mirror the canonical Slice-6 / Slice-7 decision policies
    and the ``api.multi_horizon_engine`` / ``api.paper_trading_desk`` constants; the API
    owner OVERRIDES them with the live values so no threshold is silently forked.

    Values marked ``new`` are genuinely-new Stage-20 thresholds. Each has an explicit
    economic rationale below, is versioned by ``REASSESSMENT_POLICY_VERSION`` /
    ``CHURN_POLICY_VERSION``, is returned in the payload, is folded into the
    deterministic reassessment hash (so changing one produces a NEW assessment rather
    than silently re-labelling an old one), is manually configurable through the API
    owner's policy-override seam, and is exercised at its boundary by the test suite.
    There are no hidden magic numbers: every constant the gate consults lives here.
    """
    return {
        "policy_version": REASSESSMENT_POLICY_VERSION,
        "churn_policy_version": CHURN_POLICY_VERSION,

        # --- reused canonical construction / cost constants ------------------ #
        "target_position_count": 25,      # reused: eng.BOOK_SIZES[0]
        "entry_rank": 25,                 # reused: eng.BOOK_SIZES[0]
        "exit_buffer_rank": 30,           # reused: ceil(N * (1 + EXIT_BUFFER_FRACTION))
        "max_name_weight": 0.10,          # reused: eng.MAX_INDIVIDUAL_WEIGHT
        "sector_cap_fraction": 0.25,      # reused: eng.SECTOR_CAP_FRACTION
        "min_adv_dollar": 1.0e7,          # reused: eng.MIN_ADV_DOLLAR
        "cost_bps_per_side": 12.5,        # reused: desk.COST_BPS_PER_SIDE
        "round_trip_cost_bps": 25.0,      # reused: 2 * desk.COST_BPS_PER_SIDE
        "cost_rate_per_side": 0.00125,    # reused: desk.COST_RATE_PER_SIDE

        # --- reused Slice-6 / Slice-7 decision thresholds -------------------- #
        # The PER-NAME hurdles. The gate never re-derives a per-name decision; these are
        # carried so the portfolio hurdle is expressed on the SAME scale and so the
        # payload records the exact per-name policy the recommendations came from.
        "min_gross_score_improvement": 0.02,   # reused: Slice-6/7 candidate margin
        "min_net_improvement": 0.05,           # reused: Slice-6/7 per-name REPLACE hurdle
        "score_points_per_cost_bp": 0.001,     # reused: bps -> percentile-point hurdle
        "risk_penalty_weight": 0.5,            # reused: Slice-6 risk adjustment weight
        "reduce_fraction": 0.5,                # reused: Slice-7 REDUCE trim fraction
        "material_weight_delta": 1.0e-4,       # reused: Slice-7 materiality band
        "deterioration_rank_worsen_threshold": 5,   # reused: Slice-6 deterioration trip

        # ------------------------------------------------------------------ #
        # --- genuinely-new Stage-20 thresholds (documented + sensitivity-tested)
        # ------------------------------------------------------------------ #

        # (1) The PORTFOLIO-level net-improvement hurdle, in the same percentile-score
        #     points as the per-name hurdle. Economic rationale: a portfolio-level
        #     reallocation moves several positions and pays cost on all of them, so it
        #     must clear at least the same bar a SINGLE name must clear before its own
        #     replacement is justified. Setting it below the per-name hurdle would let a
        #     basket of individually-rejected switches pass in aggregate.
        "min_portfolio_net_improvement": 0.05,                     # new

        # (2) Turnover budget per reassessment == the portfolio-distance limit (the
        #     one-way L1/2 distance between the current and post-action weights).
        #     Economic rationale: at N=25 equal weight, 0.35 one-way is ~8-9 names. More
        #     than a third of the book turning over in a single reassessment is a regime
        #     change, not a reallocation: it is surfaced as a blocked CHANGE_CANDIDATE
        #     for human adjudication rather than auto-proposed.
        "max_one_way_turnover_per_reassessment": 0.35,             # new

        # (3) Cooldown: a name whose weight changed within this many eligible sessions is
        #     protected from being acted on again. Economic rationale: it matches the
        #     shortest return window the model's own signal is measured over (Slice-6
        #     return_windows["5d"]), so the system never trades a name faster than it can
        #     observe whether the previous trade in that name was right.
        "churn_cooldown_trading_days": 5,                          # new

        # (4) Reversal protection: how many prior reassessments are scanned for an
        #     opposite-direction action in the same name. Economic rationale: 10
        #     reassessments is ~2 cooldown windows, long enough to catch a buy-then-sell
        #     whipsaw that a 5-day cooldown alone would let through.
        "reversal_lookback_reassessments": 10,                     # new

        # (5) The minimum fraction of evaluated holdings whose required Slice-6 analytics
        #     are complete before a portfolio-level economic decision may be taken.
        #     Mirrors the Slice-7 covariance-coverage philosophy (min_volatility_coverage
        #     = 0.80): below it the aggregate is not a portfolio measurement, it is an
        #     extrapolation, and the gate BLOCKS honestly instead.
        "min_holdings_data_complete_fraction": 0.80,               # new

        # (6) Maximum tolerated increase in the Herfindahl concentration index caused by
        #     the recommended exits. Economic rationale: a 25-name equal-weight book has
        #     HHI ~= 0.04; +0.01 is a ~25% concentration increase, a material risk
        #     deterioration that must reject a nominal score improvement.
        "max_concentration_increase": 0.01,                        # new

        # (7) A recommendation on a position smaller than this fraction of NAV cannot on
        #     its own drive a portfolio change. Economic rationale: prevents residual
        #     dust (a partially-filled or nearly-exited position) from manufacturing
        #     turnover that the cost model would never justify.
        "min_actionable_weight": 0.01,                             # new

        # (8) The number of most-attractive non-held alternatives carried into the
        #     artifact. Report-only: it assigns no capital and selects no target.
        "strongest_alternatives_max": 10,                          # new
    }


# --------------------------------------------------------------------------- #
# Small numeric helpers (stdlib only, deterministic) — shared with the Slice-6
# kernel's conventions so rounding never diverges between the two artifacts.
# --------------------------------------------------------------------------- #
_f = hoc_kernel._f            # noqa: SLF001 - deliberate reuse of the ONE numeric coercion
_r = hoc_kernel._r            # noqa: SLF001 - deliberate reuse of the ONE rounding rule
_round_money = hoc_kernel._round_money   # noqa: SLF001 - ONE money quantization
stable_hash = hoc_kernel.stable_hash     # noqa: SLF001 - ONE deterministic hash


def _worst_drawdown(rows: list) -> Optional[float]:
    """The most negative 60-close drawdown across the evaluated holdings (None when no
    holding has a measurable drawdown — never substituted with 0)."""
    vals = [_f(r.get("drawdown_60d")) for r in rows]
    vals = [v for v in vals if v is not None and not math.isnan(v)]
    return min(vals) if vals else None


# --------------------------------------------------------------------------- #
# Holdings snapshot identity
# --------------------------------------------------------------------------- #
def holdings_snapshot_hash(holding_reviews: list) -> str:
    """A deterministic identity for the *holdings* half of a reassessment.

    Derived from ticker + current weight + current rank only, so a reassessment run is
    correctly invalidated when the book changes even if the universe snapshot did not.
    """
    rows = sorted(
        [{"t": r.get("ticker"),
          "w": _r(_f(r.get("current_weight")), 6),
          "k": r.get("current_rank")} for r in (holding_reviews or []) if r.get("ticker")],
        key=lambda x: x["t"] or "")
    return stable_hash({"schema": "holdings_snapshot.v1", "rows": rows})


# --------------------------------------------------------------------------- #
# Point-in-time / data-quality classification (Workstream F)
# --------------------------------------------------------------------------- #
def classify_inputs(input_contract: dict, policy: dict) -> dict:
    """Classify every declared reassessment input as fresh / stale-but-valid /
    unavailable / point-in-time gap / provider-blocked, and record what happened to it.

    A slower-moving input becoming STALE_BUT_VALID does NOT kill the cycle: only a
    REQUIRED input in a fatal classification blocks. Nothing is substituted, and no
    current snapshot is ever back-dated into historical evidence.
    """
    declared = list(input_contract.get("inputs") or [])
    rows: list[dict] = []
    blocking: list[str] = []
    degraded: list[str] = []
    for row in declared:
        state = row.get("state")
        if state not in FRESHNESS_VOCAB:
            state = UNAVAILABLE
        usage = row.get("usage")
        if usage not in USAGE_VOCAB:
            usage = {FRESH: USAGE_REFRESHED, STALE_BUT_VALID: USAGE_STALE,
                     UNAVAILABLE: USAGE_MISSING, POINT_IN_TIME_GAP: USAGE_MISSING,
                     PROVIDER_BLOCKED: USAGE_BLOCKED}[state]
        required = bool(row.get("required"))
        entry = {
            "source_id": row.get("source_id"),
            "owner": row.get("owner"),
            "required": required,
            "state": state,
            "usage": usage,
            "as_of_date": row.get("as_of_date"),
            "expected_date": row.get("expected_date"),
            "cadence": row.get("cadence"),
            "detail": row.get("detail"),
        }
        rows.append(entry)
        if required and state in _FRESHNESS_FATAL:
            blocking.append("%s_%s" % (row.get("source_id") or "INPUT", state))
        elif state == STALE_BUT_VALID or (not required and state in _FRESHNESS_FATAL):
            degraded.append("%s_%s" % (row.get("source_id") or "INPUT", state))

    counts = {k: sum(1 for r in rows if r["state"] == k) for k in FRESHNESS_VOCAB}
    usage_counts = {k: sum(1 for r in rows if r["usage"] == k) for k in USAGE_VOCAB}
    return {
        "vocabulary": list(FRESHNESS_VOCAB),
        "usage_vocabulary": list(USAGE_VOCAB),
        "inputs": rows,
        "state_counts": counts,
        "usage_counts": usage_counts,
        "blocking_codes": sorted(set(blocking)),
        "degraded_codes": sorted(set(degraded)),
        "point_in_time_honest": True,
        "note": ("Every input is classified from its OWN authoritative freshness owner. "
                 "A stale slower-cadence input degrades the run; only a REQUIRED input "
                 "that is unavailable / point-in-time gapped / provider blocked stops "
                 "it. No current snapshot is ever substituted into historical evidence."),
    }


# --------------------------------------------------------------------------- #
# Churn / whipsaw control (Workstream E)
# --------------------------------------------------------------------------- #
def evaluate_churn(*, ticker: str, eligible_market_date: Optional[str],
                   direction: str, history: list, policy: dict) -> tuple[bool, list]:
    """Deterministic churn verdict for ONE name.

    ``history`` is the append-only recent-change history supplied by the API owner:
    rows of ``{"eligible_market_date", "ticker", "direction", "source"}`` ordered
    oldest-first, where ``direction`` is ``"OUT"`` (capital left the name) or ``"IN"``
    (capital entered it). Returns ``(protected, reason_codes)``.

    Two independent protections:
      * COOLDOWN  — the name changed within ``churn_cooldown_trading_days`` eligible
        sessions (counted in *history rows*, which the owner emits one per eligible
        session, never in wall-clock days).
      * REVERSAL  — within ``reversal_lookback_reassessments`` the name moved in the
        OPPOSITE direction; acting now would complete a whipsaw round trip and pay two
        sides of cost for a position the model already changed its mind about.
    """
    codes: list[str] = []
    if not ticker or not history:
        return False, codes
    rows = [h for h in history if h.get("ticker") == ticker]
    if not rows:
        return False, codes

    # Distinct eligible sessions observed in the history, newest first. The cooldown is
    # measured in SESSIONS the system actually observed — never in calendar days.
    sessions = sorted({h.get("eligible_market_date") for h in history
                       if h.get("eligible_market_date")}, reverse=True)
    if eligible_market_date and eligible_market_date not in sessions:
        sessions = [eligible_market_date] + sessions
    session_index = {d: i for i, d in enumerate(sessions)}

    cooldown = int(policy["churn_cooldown_trading_days"])
    lookback = int(policy["reversal_lookback_reassessments"])

    for h in rows:
        d = h.get("eligible_market_date")
        idx = session_index.get(d)
        if idx is None:
            continue
        if idx < cooldown:
            codes.append(CHURN_COOLDOWN)
        if idx < lookback and h.get("direction") and h.get("direction") != direction:
            codes.append(CHURN_REVERSAL)
    return (len(codes) > 0), sorted(set(codes))


# --------------------------------------------------------------------------- #
# Concentration arithmetic on the RETAINED book.
#
# This is NOT an allocation: it answers "if the recommended exits happen, what does the
# concentration of what REMAINS look like?". No capital is assigned to any candidate —
# assigning capital is owned exactly once by engine.reallocation_proposal.
# --------------------------------------------------------------------------- #
def _herfindahl(weights: dict) -> Optional[float]:
    tot = sum(w for w in weights.values() if w and w > 0)
    if tot <= 0:
        return None
    return sum((w / tot) ** 2 for w in weights.values() if w and w > 0)


def constraint_ownership() -> dict:
    """Release 29.3 — the explicit, machine-readable statement of which owner is
    entitled to DECIDE each portfolio constraint, and on which business object.

    This kernel judges the RELEASE SET. Anything that can only be known once the
    released capital has been allocated belongs to the complete-target owner. The
    numbers are still published here (as context) so the operator can see the
    magnitude, but they are never a blocker in this artifact.
    """
    return {
        "decided_here": {
            "object": "RELEASE_SET",
            "owner": CALCULATION_OWNER,
            "constraints": list(RELEASE_SET_BLOCKER_CODES),
            "question": "Is there enough economic reason to ASK for a complete target?",
        },
        "deferred_to_complete_target": {
            "object": "COMPLETE_TARGET",
            "owner": CONSTRAINT_OWNER_COMPLETE_TARGET,
            "constraints": list(COMPLETE_TARGET_CONSTRAINT_CODES),
            "question": ("Does the ONE complete target the proposal owner builds satisfy "
                         "turnover, concentration, sector and risk limits?"),
            "reason": ("These are properties of the complete post-change portfolio. This "
                       "kernel can only see the retained stub, which must be renormalised "
                       "to 1.0 to be compared at all — an object nobody will ever hold."),
        },
        "duplicated": False,
    }


def mandatory_exit_policy_block(*, mandatory_exits: list, hard_blockers: list,
                                cleared: bool) -> dict:
    """Release 29.3 — the explicit mandatory eligibility-exit contract.

    ``cleared`` means the eligibility exit was allowed to override the ECONOMIC gates
    and the reassessment therefore ASKS the proposal owner for a complete target. It
    does NOT mean anything may be sold: the target is review-only and still passes the
    complete-target constraints and two manual gates.
    """
    withheld = bool(mandatory_exits) and not cleared
    return {
        "policy": MANDATORY_EXIT_POLICY,
        "policy_version": MANDATORY_EXIT_POLICY_VERSION,
        "overrides": list(MANDATORY_EXIT_OVERRIDES),
        "never_overrides": list(MANDATORY_EXIT_HARD_BLOCKERS),
        "never_overrides_complete_target_constraints": list(
            COMPLETE_TARGET_CONSTRAINT_CODES),
        "tickers": list(mandatory_exits),
        "hard_blockers_present": list(hard_blockers),
        "override_applied": bool(cleared),
        "withheld": withheld,
        # The ONE operator-facing obligation statement. It is never "must exit now":
        # an eligibility exit is executable only through an approved complete target.
        "obligation": ("REQUIRED_IF_REALLOCATION_PROCEEDS" if mandatory_exits
                       else "NONE"),
        "authorizes_order": False,
        "authorizes_sell_only_plan": False,
        "requires_complete_target": True,
        "manual_review_required": True,
        "statement": (
            "No eligibility exit is outstanding." if not mandatory_exits else
            ("%s no longer meet the eligibility rule. Exiting them is required IF a "
             "reallocation proceeds; it is executed only inside an approved complete "
             "target, never as a standalone sell. The portfolio-level economic gates "
             "do not withhold them." % ", ".join(mandatory_exits)) if cleared else
            ("%s no longer meet the eligibility rule. Exiting them is required IF a "
             "reallocation proceeds, but a hard feasibility blocker (%s) withholds the "
             "reallocation, so no exit is executable today and a human adjudicates."
             % (", ".join(mandatory_exits), ", ".join(hard_blockers) or "none"))),
    }


def retained_concentration(*, current_weight: dict, released: dict,
                           sector_of: dict) -> dict:
    """Concentration of the retained book after the recommended releases.

    ``released[ticker]`` is the weight the recommendation would free (the full weight
    for EXIT / REPLACE, ``reduce_fraction`` of it for REDUCE).
    """
    retained = {}
    for tk, w in current_weight.items():
        rem = (w or 0.0) - (released.get(tk) or 0.0)
        if rem > 1e-12:
            retained[tk] = rem
    hhi_before = _herfindahl(current_weight)
    hhi_after = _herfindahl(retained)
    tot_after = sum(retained.values()) or 0.0

    def _max_name(ws: dict) -> tuple[Optional[float], Optional[str]]:
        best, best_tk = None, None
        tot = sum(v for v in ws.values() if v and v > 0)
        if tot <= 0:
            return None, None
        for tk, v in sorted(ws.items()):
            n = v / tot
            if best is None or n > best:
                best, best_tk = n, tk
        return best, best_tk

    def _max_sector(ws: dict) -> tuple[Optional[float], Optional[str]]:
        tot = sum(v for v in ws.values() if v and v > 0)
        if tot <= 0:
            return None, None
        agg: dict[str, float] = {}
        for tk, v in ws.items():
            if not v or v <= 0:
                continue
            agg[sector_of.get(tk) or "Unknown"] = agg.get(
                sector_of.get(tk) or "Unknown", 0.0) + v / tot
        best, best_s = None, None
        for s, v in sorted(agg.items()):
            if best is None or v > best:
                best, best_s = v, s
        return best, best_s

    mn_b, mn_bt = _max_name(current_weight)
    mn_a, mn_at = _max_name(retained)
    ms_b, ms_bs = _max_sector(current_weight)
    ms_a, ms_as = _max_sector(retained)
    return {
        "basis": "RETAINED_BOOK_RENORMALISED",
        "note": ("The arithmetic consequence of the recommended releases on what "
                 "REMAINS. No capital is assigned to any candidate here — the target "
                 "portfolio is built exactly once by %s." % TARGET_ENGINE_OWNER),
        "herfindahl_before": _r(hhi_before, 6),
        "herfindahl_after_retained": _r(hhi_after, 6),
        "herfindahl_change": _r((hhi_after - hhi_before)
                                if (hhi_after is not None and hhi_before is not None)
                                else None, 6),
        "max_name_weight_before": _r(mn_b, 6),
        "max_name_ticker_before": mn_bt,
        "max_name_weight_after_retained": _r(mn_a, 6),
        "max_name_ticker_after_retained": mn_at,
        "max_sector_weight_before": _r(ms_b, 6),
        "max_sector_before": ms_bs,
        "max_sector_weight_after_retained": _r(ms_a, 6),
        "max_sector_after_retained": ms_as,
        "retained_names": len(retained),
        "retained_invested_weight": _r(tot_after, 6),
    }


# --------------------------------------------------------------------------- #
# Deterministic explanation (Workstream K)
# --------------------------------------------------------------------------- #
def _fmt_score(x: Optional[float]) -> str:
    return "n/a" if x is None else ("%+.3f" % float(x))


def _fmt_pct(x: Optional[float]) -> str:
    return "n/a" if x is None else ("%+.1f%%" % (float(x) * 100.0))


def _fmt_usd(x: Optional[float]) -> str:
    return "n/a" if x is None else ("${:,.2f}".format(float(x)))


def _fmt_rank(rank: Optional[int], universe: Optional[int]) -> str:
    if rank is None:
        return "unranked"
    return "%d/%d" % (rank, universe) if universe else "%d" % rank


def explain_holding(review: dict, *, universe_size: Optional[int], policy: dict,
                    churn_codes: Optional[list] = None,
                    actionable: bool = False,
                    below_min_weight: bool = False) -> str:
    """Generate ONE deterministic sentence explaining why a holding is held or acted on.

    Derived SOLELY from the canonical Slice-6 assessment fields plus the Stage-20 policy
    — never from a model, never from an LLM, never from realized P&L. Improvements are
    stated in signal-score percentile points (the system has no calibrated expected
    return); switching cost is stated in the genuinely-known basis points / dollars from
    the canonical desk cost model.
    """
    rec = review.get("recommendation") or REC_HOLD
    tk = review.get("ticker") or "?"
    rank = review.get("current_rank")
    rank_change = review.get("rank_change")
    det = review.get("deterioration_state") or "UNKNOWN"
    rep = review.get("strongest_replacement_ticker")
    rep_rank = review.get("replacement_rank")
    gross = _f(review.get("gross_score_improvement"))
    net = _f(review.get("net_improvement"))
    cost_bps = _f(review.get("switching_cost_bps"))
    cost_usd = _f(review.get("switching_cost_usd"))
    dd = _f(review.get("drawdown_60d"))
    rc = _f(review.get("risk_contribution_pct"))
    liq = review.get("liquidity_state")
    hurdle = policy["min_net_improvement"]

    rank_txt = _fmt_rank(rank, universe_size)
    if rank_change is None:
        move = "prior rank unavailable"
    elif rank_change == 0:
        move = "rank unchanged"
    elif rank_change > 0:
        move = "rank improved %d places" % abs(int(rank_change))
    else:
        move = "rank fell %d places" % abs(int(rank_change))

    if rec == REC_EXIT:
        why = ", ".join(review.get("deterioration_reason_codes") or []) or det
        # Release 29.3 wording contract: an eligibility exit is REQUIRED IF A
        # REALLOCATION PROCEEDS. It is never an executable standalone obligation, so
        # this sentence must never read "must exit now" / "required exit" while the
        # portfolio verdict is monitor / no proposal.
        return ("EXIT — %s is rank %s (%s) and no longer meets the eligibility rule "
                "(%s). Exiting it is required IF a reallocation proceeds; it is carried "
                "out only inside an approved complete target, never as a standalone "
                "sell, and it is not an expected-return forecast."
                % (tk, rank_txt, move, why))

    if rec == REC_REPLACE:
        bits = ["REPLACE — %s is rank %s (%s), signal %s" % (tk, rank_txt, move, det)]
        if dd is not None:
            bits.append("60d drawdown %s" % _fmt_pct(dd))
        head = "; ".join(bits)
        tail = ("%s (rank %s) offers %s score points gross and %s net of a %.1f bps / %s "
                "round-trip switching cost, clearing the %.3f net hurdle."
                % (rep or "the strongest eligible alternative",
                   rep_rank if rep_rank is not None else "n/a",
                   _fmt_score(gross), _fmt_score(net),
                   cost_bps if cost_bps is not None else 0.0,
                   _fmt_usd(cost_usd), hurdle))
        return head + "; " + tail

    if rec == REC_REDUCE:
        breaches = ", ".join(review.get("reason_codes") or []) or "a concentration/risk breach"
        extra = ""
        if rc is not None:
            extra = " Its covariance risk contribution is %.1f%% of portfolio variance." % (
                rc * 100.0)
        return ("REDUCE — %s is rank %s (%s) but breaches a portfolio constraint (%s); "
                "trimming to %d%% of the current weight restores the envelope without "
                "exiting the signal.%s"
                % (tk, rank_txt, move, breaches,
                   int(round((1.0 - policy["reduce_fraction"]) * 100)), extra))

    # HOLD — say precisely WHY we are holding.
    if below_min_weight:
        return ("HOLD — %s is rank %s (%s), signal %s; the position is below the %.1f%% "
                "minimum actionable weight, so acting on it could not repay its own "
                "transaction cost." % (tk, rank_txt, move, det,
                                       policy["min_actionable_weight"] * 100.0))
    if churn_codes:
        return ("HOLD — %s is rank %s (%s), signal %s; a change is withheld by churn "
                "control (%s): the name moved within the last %d eligible sessions, so "
                "acting now would trade faster than the signal can be evaluated."
                % (tk, rank_txt, move, det, ", ".join(churn_codes),
                   int(policy["churn_cooldown_trading_days"])))
    if rep and gross is not None:
        return ("HOLD — %s is rank %s (%s), signal %s; the strongest non-held alternative "
                "%s (rank %s) offers only %s score points gross and %s net of the %.1f bps "
                "round-trip switching cost, below the %.3f net action hurdle."
                % (tk, rank_txt, move, det, rep,
                   rep_rank if rep_rank is not None else "n/a",
                   _fmt_score(gross), _fmt_score(net),
                   cost_bps if cost_bps is not None else 0.0, hurdle))
    if not review.get("required_data_complete"):
        return ("HOLD — %s is rank %s (%s); the required point-in-time analytics for a "
                "comparison are incomplete, so no change is proposed. Incomplete evidence "
                "never becomes an inferred recommendation."
                % (tk, rank_txt, move))
    liq_txt = (" Liquidity is %s." % liq) if liq else ""
    return ("HOLD — %s is rank %s (%s), signal %s; no eligible non-held alternative clears "
            "the %.3f net action hurdle after switching cost.%s"
            % (tk, rank_txt, move, det, hurdle, liq_txt))


def explain_portfolio(result_core: dict, policy: dict) -> str:
    """Generate ONE deterministic portfolio-level sentence for the operator."""
    state = result_core["reassessment_state"]
    d = result_core["decision"]
    n_act = d["actionable_holding_count"]
    net = d["expected_net_improvement"]
    turn = d["expected_one_way_turnover"]
    cost = d["expected_transaction_cost_usd"]
    hurdle = policy["min_portfolio_net_improvement"]

    if state == STATE_NO_CHANGE:
        if n_act == 0:
            return ("The current portfolio remains the best risk-adjusted use of capital: "
                    "no holding has an eligible alternative that clears its own net-of-cost "
                    "action hurdle. No change is proposed.")
        return ("No portfolio change is economically justified: the %d actionable holding(s) "
                "would move %s of the book at an estimated %s transaction cost, and the "
                "expected improvement of %s score points is not positive after that cost."
                % (n_act, _fmt_pct(turn), _fmt_usd(cost), _fmt_score(net)))
    mex = list(d.get("mandatory_exit_tickers") or [])
    if state == STATE_CHANGE_CANDIDATE:
        blockers = ", ".join(d.get("blockers") or []) or "the portfolio hurdle"
        base = ("%d holding(s) have attractive alternatives, but no portfolio change is "
                "proposed: expected net improvement %s score points against a %.3f hurdle, "
                "%s estimated one-way turnover, %s estimated cost — withheld by %s."
                % (n_act, _fmt_score(net), hurdle, _fmt_pct(turn), _fmt_usd(cost), blockers))
        if mex:
            base += (" %s no longer meet the eligibility rule; exiting them is required IF "
                     "a reallocation proceeds, and it is not executable while the "
                     "reallocation itself is withheld." % ", ".join(mex))
        return base
    if state == STATE_PROPOSAL_READY:
        if mex and GATE_MANDATORY_EXIT in (d.get("reason_codes") or []):
            return ("%s no longer meet the eligibility rule, so a complete target is "
                    "requested even though the expected net improvement of %s score points "
                    "does not clear the %.3f economic hurdle on its own: an ineligible name "
                    "is a constraint breach, not an alpha bet. %d actionable holding(s), %s "
                    "estimated one-way turnover, %s estimated cost. The canonical "
                    "reallocation proposal is built for MANUAL REVIEW — nothing is approved "
                    "or executed, and the complete target must still satisfy the turnover, "
                    "concentration, sector and risk limits owned by %s."
                    % (", ".join(mex), _fmt_score(net), hurdle, n_act, _fmt_pct(turn),
                       _fmt_usd(cost), TARGET_ENGINE_OWNER))
        return ("A portfolio change is economically justified: %d actionable holding(s), "
                "expected net improvement %s score points against a %.3f hurdle, %s one-way "
                "turnover at an estimated %s transaction cost. The canonical reallocation "
                "proposal is built for MANUAL REVIEW — nothing is approved or executed."
                % (n_act, _fmt_score(net), hurdle, _fmt_pct(turn), _fmt_usd(cost)))
    if state == STATE_BLOCKED_DATA:
        return ("The portfolio cannot be reassessed against complete evidence: %s. No "
                "change is inferred from incomplete data."
                % (", ".join(d.get("blockers") or []) or "required inputs are missing"))
    if state == STATE_BLOCKED_EVIDENCE:
        return ("The available assessment no longer describes the current portfolio (%s). A "
                "fresh signal refresh and reassessment are required before any change can "
                "be considered." % (", ".join(d.get("blockers") or []) or "bound evidence changed"))
    if state == STATE_MANUAL_REVIEW:
        return ("A holding breaches a hard portfolio constraint that requires human "
                "adjudication (%s)." % (", ".join(d.get("blockers") or []) or "constraint breach"))
    return ("A portfolio reassessment cannot run yet: %s."
            % (", ".join(d.get("blockers") or []) or "inputs are not available"))


# --------------------------------------------------------------------------- #
# Safety / provenance blocks
# --------------------------------------------------------------------------- #
def _safety() -> dict:
    return {
        "read_only": True,
        "paper_only": True,
        "preview_only": True,
        "manual_review": True,
        "created_target": False,
        "created_target_weights": False,
        "created_order_plan": False,
        "created_orders": False,
        "created_fills": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "approved_proposal": False,
        "confirmed_order_plan": False,
        "broker_execution": False,
        "provider_call": False,
        "prediction_call": False,
        "model_promoted": False,
        "model_retrained": False,
        "model_recalibrated": False,
        "automation_enabled": False,
        "scheduled_trading_enabled": False,
        "badges": ["PREVIEW ONLY", "MANUAL REVIEW", "NO LIVE ORDERS", "AUTOMATION OFF"],
    }


def _provenance(ic: dict) -> dict:
    return {
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "holding_comparison_owner": HOC_KERNEL_OWNER,
        "target_engine_owner": TARGET_ENGINE_OWNER,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "corporate_actions_hash": ic.get("corporate_actions_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "universe_input_contract_hash": ic.get("universe_input_contract_hash"),
        "hoc_assessment_hash": ic.get("hoc_assessment_hash"),
        "holdings_snapshot_hash": ic.get("holdings_snapshot_hash"),
        "model_identity": ic.get("model_identity") or {},
        "hoc_decision_policy_version": ic.get("hoc_decision_policy_version"),
        "allocation_policy_version": ic.get("allocation_policy_version"),
        "inputs_as_of_eligible_date": ic.get("inputs_as_of_eligible_date"),
    }


def _empty_result(pol: dict, ic: dict, state: str, blockers: list) -> dict:
    core = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "reassessment_state": state,
        "state_vocabulary": list(REASSESSMENT_STATE_VOCAB),
        "policy": pol,
        "portfolio_summary": {},
        "holding_assessments": [],
        "strongest_alternatives": [],
        "attention": {"reduce": [], "exit": [], "replace": [], "count": 0},
        "recommendation_counts": {k: 0 for k in hoc_kernel.RECOMMENDATION_VOCAB},
        "decision": {
            "decision": state,
            "actionable_holding_count": 0,
            "expected_one_way_turnover": None,
            "expected_two_way_turnover": None,
            "expected_traded_notional": None,
            "expected_transaction_cost_usd": None,
            "expected_transaction_cost_score_points": None,
            "expected_gross_improvement": None,
            "expected_risk_adjusted_improvement": None,
            "expected_net_improvement": None,
            "expected_return_improvement": None,
            "expected_return_state": EXPECTED_RETURN_STATE,
            "expected_concentration_change": None,
            "expected_risk_change": None,
            "portfolio_volatility_after_state": VOLATILITY_AFTER_STATE_PRE_PROPOSAL,
            "target_tracking_error": None,
            "target_tracking_error_owner": TARGET_ENGINE_OWNER,
            "improvement_basis": IMPROVEMENT_BASIS,
            "strongest_evidence": None,
            "blockers": [b.get("code") if isinstance(b, dict) else str(b) for b in blockers],
            "reason_codes": [],
            "proposal_required": False,
        },
        "churn_control": {"policy_version": pol["churn_policy_version"],
                          "protected_tickers": [], "reason_codes": [],
                          "history_rows_considered": 0},
        "concentration": {},
        "input_quality": classify_inputs(ic, pol),
        "blockers": blockers,
        "data_gaps": [],
        "safety": _safety(),
        "provenance": _provenance(ic),
    }
    core["explanation"] = explain_portfolio(core, pol)
    core["reassessment_hash"] = stable_hash(core)
    return core


# --------------------------------------------------------------------------- #
# The kernel (Workstreams B / D / E / F / G / K)
# --------------------------------------------------------------------------- #
def build_reassessment(*, input_contract: dict, policy: Optional[dict] = None) -> dict:
    """Compute the canonical Stage-20 portfolio reassessment (pure, deterministic).

    Never raises on incomplete data — it degrades to an explicit BLOCKED_* / NOT_READY
    decision with named reason codes rather than inferring a change.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)

    ic = input_contract or {}
    eligible = ic.get("eligible_market_date")
    book = ic.get("active_book_id")
    reviews = list(ic.get("holding_reviews") or [])
    hoc_state = ic.get("hoc_assessment_state")
    universe_size = ic.get("eligible_universe_size")

    # --- NOT_READY: the reassessment cannot even be identified ---------------- #
    if not book:
        return _empty_result(pol, ic, STATE_NOT_READY,
                             [{"code": "NO_ACTIVE_BOOK",
                               "detail": "No active operational book was supplied."}])
    if not eligible:
        return _empty_result(pol, ic, STATE_NOT_READY,
                             [{"code": "MISSING_ELIGIBLE_MARKET_DATE"}])
    if not ic.get("hoc_assessment_hash") or hoc_state in (None, "NOT_RUN"):
        return _empty_result(pol, ic, STATE_NOT_READY,
                             [{"code": "HOLDING_OPPORTUNITY_COST_NOT_RUN",
                               "detail": "The Slice-6 assessment for this eligible "
                                         "session does not exist yet."}])

    # --- BLOCKED_EVIDENCE: bound evidence no longer describes the portfolio --- #
    #
    # Stage 21 (Workstream 0E). This gate is only meaningful if it compares LIKE WITH
    # LIKE. It previously compared ``portfolio_state_hash`` — a fingerprint of the
    # WHOLE portfolio-state document, which embeds the Slice-6 assessment's own output
    # hash via api.daily_action_gate. Writing the assessment therefore changed the
    # fingerprint the assessment was about to be judged against, so a FRESH assessment
    # blocked itself on every Daily Research Cycle with zero economic change (verified
    # on the live 2026-08-13 book: capital and positions byte-identical either side).
    #
    # The comparison now binds to the ECONOMIC fingerprint owned by api.portfolio_state
    # (holdings / cash / NAV / order + fill counts / corporate-action registry). A real
    # holdings, cash or corporate-action change still invalidates immediately; a
    # downstream research write can no longer invalidate its own input.
    ev_blockers: list[dict] = []
    if ic.get("corporate_action_stale"):
        ev_blockers.append({"code": "STALE_CORPORATE_ACTION_EVIDENCE",
                            "detail": ic.get("corporate_action_stale_reason")})
    if ic.get("economic_state_hash") and ic.get("hoc_economic_state_hash") \
            and ic["economic_state_hash"] != ic["hoc_economic_state_hash"]:
        ev_blockers.append({
            "code": "PORTFOLIO_STATE_CHANGED_SINCE_ASSESSMENT",
            "detail": ("The economic portfolio (holdings / cash / NAV / corporate "
                       "actions) changed after this assessment was produced.")})
    if ic.get("hoc_eligible_market_date") and ic["hoc_eligible_market_date"] != eligible:
        ev_blockers.append({"code": "ASSESSMENT_ELIGIBLE_DATE_MISMATCH",
                            "detail": "assessment=%s reassessment=%s"
                                      % (ic["hoc_eligible_market_date"], eligible)})
    if ev_blockers:
        return _empty_result(pol, ic, STATE_BLOCKED_EVIDENCE, ev_blockers)

    # --- BLOCKED_DATA: required inputs / assessment completeness -------------- #
    quality = classify_inputs(ic, pol)
    data_blockers: list[dict] = []
    for code in quality["blocking_codes"]:
        data_blockers.append({"code": code, "detail": "A REQUIRED reassessment input is "
                                                      "not usable point-in-time."})
    if hoc_state == "BLOCKED":
        data_blockers.append({"code": "HOLDING_OPPORTUNITY_COST_BLOCKED"})
    if not reviews:
        data_blockers.append({"code": "NO_HOLDINGS_EVALUATED"})

    n_reviews = len(reviews)
    n_complete = sum(1 for r in reviews if r.get("required_data_complete"))
    complete_fraction = (n_complete / n_reviews) if n_reviews else 0.0
    if n_reviews and complete_fraction < pol["min_holdings_data_complete_fraction"] - 1e-12:
        data_blockers.append({
            "code": "INSUFFICIENT_HOLDING_DATA_COMPLETENESS",
            "detail": "%d/%d holdings have complete required analytics (%.2f), below the "
                      "%.2f floor." % (n_complete, n_reviews, complete_fraction,
                                       pol["min_holdings_data_complete_fraction"])})
    if data_blockers:
        res = _empty_result(pol, ic, STATE_BLOCKED_DATA, data_blockers)
        res["input_quality"] = quality
        res["portfolio_summary"] = dict(ic.get("portfolio_summary") or {})
        res["decision"]["holdings_evaluated"] = n_reviews
        res["decision"]["holdings_data_complete"] = n_complete
        res["explanation"] = explain_portfolio(res, pol)
        res["reassessment_hash"] = stable_hash(
            {k: v for k, v in res.items() if k != "reassessment_hash"})
        return res

    # ---------------------------------------------------------------------- #
    # Per-holding assessment + churn evaluation.
    # ---------------------------------------------------------------------- #
    nav = _f(ic.get("nav")) or 0.0
    history = list(ic.get("recent_change_history") or [])
    sector_of = {r.get("ticker"): (r.get("sector") or "Unknown") for r in reviews
                 if r.get("ticker")}
    current_weight = {r.get("ticker"): (_f(r.get("current_weight")) or 0.0)
                      for r in reviews if r.get("ticker")}

    released: dict[str, float] = {}
    assessments: list[dict] = []
    churn_protected: list[str] = []
    churn_codes_all: list[str] = []
    actionable: list[dict] = []
    gross_acc = 0.0
    risk_adj_acc = 0.0
    gross_measurable = True
    mandatory_exits: list[str] = []
    liquidity_blocked: list[str] = []
    constraint_breaches: list[str] = []
    data_gaps_local: set = set()

    for r in reviews:
        tk = r.get("ticker")
        rec = r.get("recommendation") or REC_HOLD
        w = current_weight.get(tk, 0.0)
        is_actionable_rec = rec in ACTIONABLE_RECOMMENDATIONS
        below_min = bool(is_actionable_rec and w < pol["min_actionable_weight"] - 1e-12)
        direction = "OUT" if is_actionable_rec else "IN"
        protected, ccodes = evaluate_churn(ticker=tk, eligible_market_date=eligible,
                                           direction=direction, history=history, policy=pol)
        # Churn protection only ever WITHHOLDS an action; it never manufactures one.
        protected = bool(protected and is_actionable_rec)
        if protected:
            churn_protected.append(tk)
            churn_codes_all.extend(ccodes)

        effective_action = rec if (is_actionable_rec and not protected and not below_min) \
            else REC_HOLD
        withheld_codes: list[str] = []
        if is_actionable_rec and protected:
            withheld_codes.extend(ccodes)
        if below_min:
            withheld_codes.append("BELOW_MIN_ACTIONABLE_WEIGHT")

        # Weight released by the effective action (never an allocation — see module doc).
        if effective_action in (REC_EXIT, REC_REPLACE):
            released[tk] = w
        elif effective_action == REC_REDUCE:
            released[tk] = w * pol["reduce_fraction"]

        # Portfolio-level improvement contribution, capital-weighted. The per-name
        # numbers come from the Slice-6 kernel and are NEVER recomputed here.
        contrib_gross = None
        contrib_risk_adj = None
        moved_weight = released.get(tk, 0.0)
        if effective_action in (REC_EXIT, REC_REPLACE, REC_REDUCE):
            # A MANDATORY exit (an ineligible / structurally broken holding) has no
            # replacement comparison by construction, so Slice 6 reports no improvement for
            # it. That absence is NOT an unmeasurable portfolio: exiting an ineligible name
            # is a constraint action, never an alpha claim. It contributes 0.0 to the
            # improvement aggregate (an honest "no improvement is being claimed") instead of
            # making the whole gate unmeasurable, which would otherwise trap an ineligible
            # holding in the book forever.
            mandatory = (effective_action == REC_EXIT
                         and r.get("deterioration_state") == hoc_kernel.DET_BROKEN)
            g = _f(r.get("gross_score_improvement"))
            ra = _f(r.get("risk_adjusted_improvement"))
            if g is None or ra is None:
                if mandatory:
                    contrib_gross = 0.0
                    contrib_risk_adj = 0.0
                    data_gaps_local.add("MANDATORY_EXIT_IMPROVEMENT_NOT_APPLICABLE")
                else:
                    gross_measurable = False
            else:
                contrib_gross = moved_weight * g
                contrib_risk_adj = moved_weight * ra
                gross_acc += contrib_gross
                risk_adj_acc += contrib_risk_adj
            actionable.append({"ticker": tk, "recommendation": effective_action,
                               "moved_weight": moved_weight,
                               "mandatory": bool(mandatory)})
            if mandatory:
                mandatory_exits.append(tk)
            if r.get("liquidity_state") == hoc_kernel.LIQ_ILLIQUID:
                liquidity_blocked.append(tk)

        for code in (r.get("reason_codes") or []):
            if code in ("NAME_WEIGHT_BREACH", "SECTOR_WEIGHT_BREACH",
                        "RISK_CONTRIBUTION_BREACH"):
                constraint_breaches.append("%s:%s" % (tk, code))

        assessments.append({
            # identity + current position
            "ticker": tk,
            "sector": r.get("sector"),
            "current_weight": _r(w, 6),
            "market_value": r.get("market_value"),
            # signal / rank
            "current_rank": r.get("current_rank"),
            "previous_rank": r.get("previous_rank"),
            "rank_change": r.get("rank_change"),
            "prior_rank_state": ("AVAILABLE" if r.get("previous_rank") is not None
                                 else "PRIOR_RANK_UNAVAILABLE"),
            "signal_score": r.get("current_score"),
            "signal_strength": r.get("signal_strength"),
            "deterioration_state": r.get("deterioration_state"),
            "deterioration_reason_codes": r.get("deterioration_reason_codes") or [],
            # recent performance / risk
            "return_5d": r.get("return_5d"),
            "return_20d": r.get("return_20d"),
            "return_60d": r.get("return_60d"),
            "volatility_20d": r.get("volatility_20d"),
            "volatility_60d": r.get("volatility_60d"),
            "drawdown_60d": r.get("drawdown_60d"),
            "risk_contribution_pct": r.get("risk_contribution_pct"),
            "concentration_contribution": r.get("concentration_contribution"),
            "median_dollar_volume_20d": r.get("median_dollar_volume_20d"),
            "estimated_days_to_liquidate": r.get("estimated_days_to_liquidate"),
            "liquidity_state": r.get("liquidity_state"),
            # the comparison (Slice-6 owned; never recomputed)
            "strongest_replacement_ticker": r.get("strongest_replacement_ticker"),
            "replacement_rank": r.get("replacement_rank"),
            "replacement_score": r.get("replacement_score"),
            "replacement_sector": r.get("replacement_sector"),
            "expected_gross_improvement": r.get("gross_score_improvement"),
            "risk_adjusted_improvement": r.get("risk_adjusted_improvement"),
            "switching_cost_bps": r.get("switching_cost_bps"),
            "switching_cost_usd": r.get("switching_cost_usd"),
            "expected_net_improvement": r.get("net_improvement"),
            "improvement_basis": IMPROVEMENT_BASIS,
            "expected_return_delta": None,
            "expected_return_delta_state": EXPECTED_RETURN_STATE,
            # the Stage-20 verdict
            "source_recommendation": rec,
            "recommendation": effective_action,
            "action_withheld": bool(effective_action != rec),
            "withheld_reason_codes": sorted(set(withheld_codes)),
            "churn_protected": protected,
            "churn_reason_codes": ccodes if protected else [],
            "released_weight": _r(released.get(tk), 6),
            "portfolio_gross_contribution": _r(contrib_gross, 6),
            "portfolio_risk_adjusted_contribution": _r(contrib_risk_adj, 6),
            "required_data_complete": bool(r.get("required_data_complete")),
            "explanation": explain_holding(
                {**r, "recommendation": effective_action}, universe_size=universe_size,
                policy=pol, churn_codes=(ccodes if protected else []),
                actionable=bool(effective_action in ACTIONABLE_RECOMMENDATIONS),
                below_min_weight=below_min),
            "source_explanation": r.get("explanation"),
        })

    # ---------------------------------------------------------------------- #
    # Portfolio-level economics (Workstream D). Cost is counted EXACTLY once.
    # ---------------------------------------------------------------------- #
    one_way_turnover = sum(released.values())
    # Two-way turnover: every released dollar is sold and the freed capital is
    # redeployed, so the traded weight is twice the released weight. This mirrors the
    # Slice-7 cost formula exactly so the two artifacts never disagree.
    two_way_turnover = 2.0 * one_way_turnover
    traded_notional = two_way_turnover * nav
    cost_usd = traded_notional * pol["cost_rate_per_side"]
    cost_score = two_way_turnover * pol["round_trip_cost_bps"] * pol["score_points_per_cost_bp"]

    expected_gross = gross_acc if (actionable and gross_measurable) else (
        0.0 if not actionable else None)
    expected_risk_adj = risk_adj_acc if (actionable and gross_measurable) else (
        0.0 if not actionable else None)
    expected_net = (expected_risk_adj - cost_score) if expected_risk_adj is not None else None

    concentration = retained_concentration(current_weight=current_weight,
                                           released=released, sector_of=sector_of)
    hhi_change = _f(concentration.get("herfindahl_change"))
    max_sector_after = _f(concentration.get("max_sector_weight_after_retained"))
    max_sector_before = _f(concentration.get("max_sector_weight_before"))

    # ---------------------------------------------------------------------- #
    # The economic change gate (deterministic precedence).
    # ---------------------------------------------------------------------- #
    blockers: list[str] = []
    reason_codes: list[str] = []
    mandatory_hard_blockers: list[str] = []
    state = STATE_NO_CHANGE

    if not actionable:
        reason_codes.append(GATE_NO_ACTIONABLE)
        state = STATE_NO_CHANGE
    else:
        if expected_net is None:
            blockers.append(GATE_IMPROVEMENT_UNMEASURABLE)
            state = STATE_CHANGE_CANDIDATE
        else:
            # Release 29.3 — CONSTRAINT OWNERSHIP. Concentration, sector concentration,
            # post-change risk and the turnover BUDGET are properties of the COMPLETE
            # TARGET, which only ``engine.reallocation_proposal`` can build (it is the
            # one owner that allocates the released capital). Judging them here means
            # judging a retained-only stub renormalised to 1.0 — an object nobody will
            # ever hold — so they are DEFERRED, not duplicated. The arithmetic is still
            # published (``concentration`` / ``expected_one_way_turnover``) as explicitly
            # non-binding pre-proposal context, and the binding verdict is reached once,
            # on the complete target, by the proposal owner.
            if liquidity_blocked:
                blockers.append(GATE_LIQUIDITY)
            if churn_protected:
                reason_codes.extend(sorted(set(churn_codes_all)))

            if blockers:
                state = STATE_CHANGE_CANDIDATE
            elif expected_net <= 0.0:
                # Positive-looking raw improvement that does not survive cost is NOT a
                # candidate to keep watching: acting would destroy value.
                reason_codes.append(GATE_NET_NON_POSITIVE)
                state = STATE_NO_CHANGE
            elif expected_net < pol["min_portfolio_net_improvement"] - 1e-12:
                reason_codes.append(GATE_BELOW_NET_HURDLE)
                blockers.append(GATE_BELOW_NET_HURDLE)
                state = STATE_CHANGE_CANDIDATE
            else:
                reason_codes.append(GATE_CLEARED)
                state = STATE_PROPOSAL_READY

        # MANDATORY ELIGIBILITY-EXIT POLICY (Release 29.3 — explicit, versioned).
        # A holding that is no longer ELIGIBLE must leave regardless of score economics:
        # holding an ineligible name is a constraint breach, not an alpha bet. The
        # override therefore defeats the ECONOMIC gates (MANDATORY_EXIT_OVERRIDES) so an
        # unmeasurable or sub-hurdle improvement can never trap an ineligible name in the
        # book — which is exactly what the pre-29.3 implementation did, because it tested
        # ``not blockers`` while GATE_BELOW_NET_HURDLE was itself in ``blockers``.
        # It NEVER defeats a HARD feasibility blocker (MANDATORY_EXIT_HARD_BLOCKERS);
        # in that case the exit stays a REQUIRED-IF-A-REALLOCATION-PROCEEDS signal, is
        # recorded as GATE_MANDATORY_EXIT_WITHHELD, and a human adjudicates. In neither
        # branch does it authorise an order or a sell-only plan: the ONLY thing a cleared
        # mandatory exit does is let the proposal owner build a complete bounded target,
        # which remains review-only behind manual approval.
        mandatory_hard_blockers = sorted(
            set(blockers) & set(MANDATORY_EXIT_HARD_BLOCKERS))
        if mandatory_exits:
            if not mandatory_hard_blockers:
                reason_codes.append(GATE_MANDATORY_EXIT)
                blockers = [b for b in blockers
                            if b not in MANDATORY_EXIT_OVERRIDES]
                state = STATE_PROPOSAL_READY
            else:
                reason_codes.append(GATE_MANDATORY_EXIT_WITHHELD)
                blockers.append(GATE_MANDATORY_EXIT_WITHHELD)
                state = STATE_CHANGE_CANDIDATE

    # A hard constraint breach on a retained name is a human decision.
    if constraint_breaches and state == STATE_NO_CHANGE:
        blockers.extend(sorted(set(constraint_breaches)))
        state = STATE_MANUAL_REVIEW

    # A degraded (but non-blocking) input set never silently upgrades to a proposal
    # without the operator seeing the gap; it is recorded, not hidden.
    degraded_codes = list(quality["degraded_codes"])

    # ---------------------------------------------------------------------- #
    # Strongest non-held alternatives (report-only; no capital assigned).
    # ---------------------------------------------------------------------- #
    held = {r.get("ticker") for r in reviews if r.get("ticker")}
    alternatives: list[dict] = []
    used_by: dict[str, list] = {}
    for a in assessments:
        rep = a.get("strongest_replacement_ticker")
        if rep:
            used_by.setdefault(rep, []).append(a["ticker"])
    for cand in (ic.get("addition_candidates") or []):
        tk = cand.get("ticker")
        if not tk or tk in held:
            # A currently-allocated name can NEVER be surfaced as an external
            # replacement (it is already inside the book).
            continue
        displaced = sorted(used_by.get(tk) or [])
        best_improvement = None
        for inc in displaced:
            row = next((x for x in assessments if x["ticker"] == inc), None)
            v = _f(row.get("expected_net_improvement")) if row else None
            if v is not None and (best_improvement is None or v > best_improvement):
                best_improvement = v
        selected = any(a["ticker"] in displaced
                       and a["recommendation"] == REC_REPLACE for a in assessments)
        if displaced and not selected:
            not_selected = ("The incumbent's net-of-cost improvement did not clear the "
                            "%.3f per-name action hurdle." % pol["min_net_improvement"])
        elif not displaced:
            not_selected = ("Eligible top-%d name not currently held; no incumbent's "
                            "comparison selected it this session." % pol["entry_rank"])
        else:
            not_selected = None
        alternatives.append({
            "ticker": tk,
            "rank": cand.get("rank"),
            "score": cand.get("score"),
            "combined_score": cand.get("combined_score"),
            "sector": cand.get("sector"),
            "expected_contribution_basis": IMPROVEMENT_BASIS,
            "displaced_incumbent": displaced[0] if displaced else None,
            "displaced_incumbents": displaced,
            "expected_net_improvement_vs_incumbent": _r(best_improvement, 6),
            "selected_as_replacement": bool(selected),
            "reason_not_selected": not_selected,
            "allocation": None,
            "allocation_owner": TARGET_ENGINE_OWNER,
        })
        if len(alternatives) >= pol["strongest_alternatives_max"]:
            break

    # ---------------------------------------------------------------------- #
    # Attention grouping (Workstream J: exception-first).
    # ---------------------------------------------------------------------- #
    attention = {
        "exit": [a["ticker"] for a in assessments if a["recommendation"] == REC_EXIT],
        "replace": [a["ticker"] for a in assessments if a["recommendation"] == REC_REPLACE],
        "reduce": [a["ticker"] for a in assessments if a["recommendation"] == REC_REDUCE],
    }
    attention["count"] = len(attention["exit"]) + len(attention["replace"]) \
        + len(attention["reduce"])

    rec_counts = {k: 0 for k in hoc_kernel.RECOMMENDATION_VOCAB}
    for a in assessments:
        rec_counts[a["recommendation"]] = rec_counts.get(a["recommendation"], 0) + 1
    rec_counts[REC_ADD] = len(alternatives)

    strongest = None
    best = None
    for a in assessments:
        v = _f(a.get("expected_net_improvement"))
        if v is not None and (best is None or v > best):
            best, strongest = v, a
    strongest_evidence = None
    if strongest is not None:
        strongest_evidence = {
            "ticker": strongest["ticker"],
            "replacement": strongest.get("strongest_replacement_ticker"),
            "expected_net_improvement": strongest.get("expected_net_improvement"),
            "switching_cost_usd": strongest.get("switching_cost_usd"),
            "recommendation": strongest.get("recommendation"),
            "explanation": strongest.get("explanation"),
        }

    ps = dict(ic.get("portfolio_summary") or {})
    portfolio_summary = {
        "nav": _round_money(nav),
        "cash": ps.get("cash"),
        "invested_value": ps.get("invested_value"),
        "holdings_count": len(reviews),
        "holdings_evaluated": n_reviews,
        "holdings_data_complete": n_complete,
        "gross_exposure": _r(sum(current_weight.values()), 6),
        "net_exposure": _r(sum(current_weight.values()), 6),
        "long_only": True,
        "max_name_weight": ps.get("max_name_weight"),
        "max_name_ticker": ps.get("max_name_ticker"),
        "max_sector_weight": ps.get("max_sector_weight"),
        "max_sector": ps.get("max_sector"),
        "sector_weights": ps.get("sector_weights") or {},
        "herfindahl_index": ps.get("herfindahl_index"),
        "portfolio_variance_daily": ps.get("portfolio_variance_daily"),
        "risk_contribution_state": ps.get("risk_contribution_state"),
        "current_portfolio_score": ic.get("current_portfolio_score"),
        "current_portfolio_score_basis": IMPROVEMENT_BASIS,
        "benchmark_drawdown_context": ic.get("drawdown_context"),
        "worst_holding_drawdown_60d": _r(_worst_drawdown(assessments), 6),
    }

    decision = {
        "decision": state,
        "proposal_required": bool(state == STATE_PROPOSAL_READY),
        "holdings_evaluated": n_reviews,
        "holdings_data_complete": n_complete,
        "holdings_data_complete_fraction": _r(complete_fraction, 4),
        "actionable_holding_count": len(actionable),
        "actionable_holdings": actionable,
        "mandatory_exit_tickers": sorted(set(mandatory_exits)),
        "expected_one_way_turnover": _r(one_way_turnover, 6),
        "expected_two_way_turnover": _r(two_way_turnover, 6),
        "expected_portfolio_distance": _r(one_way_turnover, 6),
        "expected_traded_notional": _round_money(traded_notional),
        "expected_transaction_cost_usd": _round_money(cost_usd),
        "expected_transaction_cost_score_points": _r(cost_score, 6),
        "transaction_cost_counted_once": True,
        "expected_gross_improvement": _r(expected_gross, 6),
        "expected_risk_adjusted_improvement": _r(expected_risk_adj, 6),
        "expected_net_improvement": _r(expected_net, 6),
        "expected_return_improvement": None,
        "expected_return_state": EXPECTED_RETURN_STATE,
        "expected_concentration_change": _r(hhi_change, 6),
        "expected_max_sector_weight_after_retained": _r(max_sector_after, 6),
        "expected_risk_change": _r(
            -sum(_f(a.get("risk_contribution_pct")) or 0.0 for a in assessments
                 if a["recommendation"] in ACTIONABLE_RECOMMENDATIONS), 6),
        "expected_risk_change_basis": "RELEASED_COVARIANCE_RISK_CONTRIBUTION",
        "portfolio_volatility_after_state": VOLATILITY_AFTER_STATE_PRE_PROPOSAL,
        "target_tracking_error": None,
        "target_tracking_error_owner": TARGET_ENGINE_OWNER,
        "improvement_basis": IMPROVEMENT_BASIS,
        "net_improvement_hurdle": pol["min_portfolio_net_improvement"],
        "turnover_budget": pol["max_one_way_turnover_per_reassessment"],
        # Release 29.3 — the turnover / concentration / sector / post-change-risk numbers
        # above are PRE-PROPOSAL ESTIMATES over the release set, published as context.
        # None of them is binding here; the binding verdict is reached exactly once, on
        # the COMPLETE target, by ``engine.reallocation_proposal``.
        "turnover_budget_binding_here": False,
        "expected_turnover_basis": "PRE_PROPOSAL_RELEASE_SET_ESTIMATE",
        "concentration_basis": "PRE_PROPOSAL_RETAINED_BOOK_RENORMALISED",
        "complete_target_constraint_owner": CONSTRAINT_OWNER_COMPLETE_TARGET,
        "constraint_ownership": constraint_ownership(),
        "mandatory_exit_policy": mandatory_exit_policy_block(
            mandatory_exits=sorted(set(mandatory_exits)),
            hard_blockers=mandatory_hard_blockers,
            cleared=bool(mandatory_exits) and not mandatory_hard_blockers),
        "strongest_evidence": strongest_evidence,
        "blockers": sorted(set(blockers)),
        "reason_codes": sorted(set(reason_codes)),
        "degraded_codes": degraded_codes,
    }

    core = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "eligible_market_date": eligible,
        "active_book_id": book,
        "reassessment_state": state,
        "state_vocabulary": list(REASSESSMENT_STATE_VOCAB),
        "policy": pol,
        "portfolio_summary": portfolio_summary,
        "holding_assessments": assessments,
        "strongest_alternatives": alternatives,
        "attention": attention,
        "recommendation_counts": rec_counts,
        "decision": decision,
        "churn_control": {
            "policy_version": pol["churn_policy_version"],
            "cooldown_sessions": pol["churn_cooldown_trading_days"],
            "reversal_lookback": pol["reversal_lookback_reassessments"],
            "turnover_budget": pol["max_one_way_turnover_per_reassessment"],
            "protected_tickers": sorted(set(churn_protected)),
            "reason_codes": sorted(set(churn_codes_all)),
            "history_rows_considered": len(history),
        },
        "concentration": concentration,
        "input_quality": quality,
        "blockers": [{"code": b} for b in sorted(set(blockers))],
        "data_gaps": sorted(set(list(ic.get("hoc_data_gaps") or []) + degraded_codes
                                + list(data_gaps_local))),
        "safety": _safety(),
        "provenance": _provenance(ic),
    }
    core["explanation"] = explain_portfolio(core, pol)
    core["reassessment_hash"] = stable_hash(core)
    return core


__all__ = [
    "SCHEMA_VERSION", "INPUT_SCHEMA_VERSION", "REASSESSMENT_POLICY_VERSION",
    "CHURN_POLICY_VERSION", "CALCULATION_OWNER", "HOC_KERNEL_OWNER", "TARGET_ENGINE_OWNER",
    "STATE_NOT_READY", "STATE_NO_CHANGE", "STATE_CHANGE_CANDIDATE", "STATE_PROPOSAL_READY",
    "STATE_BLOCKED_DATA", "STATE_BLOCKED_EVIDENCE", "STATE_MANUAL_REVIEW",
    "REASSESSMENT_STATE_VOCAB", "PROPOSAL_ELIGIBLE_STATES", "PERSISTABLE_STATES",
    "ACTIONABLE_RECOMMENDATIONS", "FRESHNESS_VOCAB", "USAGE_VOCAB",
    "FRESH", "STALE_BUT_VALID", "UNAVAILABLE", "POINT_IN_TIME_GAP", "PROVIDER_BLOCKED",
    "USAGE_REFRESHED", "USAGE_REUSED", "USAGE_STALE", "USAGE_MISSING", "USAGE_BLOCKED",
    "CHURN_COOLDOWN", "CHURN_REVERSAL", "CHURN_TURNOVER_BUDGET",
    "GATE_NO_ACTIONABLE", "GATE_BELOW_NET_HURDLE", "GATE_NET_NON_POSITIVE", "GATE_CLEARED",
    "GATE_MANDATORY_EXIT", "GATE_CONCENTRATION", "GATE_SECTOR_CAP", "GATE_LIQUIDITY",
    "GATE_MANDATORY_EXIT_WITHHELD", "MANDATORY_EXIT_POLICY", "MANDATORY_EXIT_POLICY_VERSION",
    "MANDATORY_EXIT_OVERRIDES", "MANDATORY_EXIT_HARD_BLOCKERS",
    "RELEASE_SET_BLOCKER_CODES", "COMPLETE_TARGET_CONSTRAINT_CODES",
    "CONSTRAINT_OWNER_COMPLETE_TARGET", "constraint_ownership", "mandatory_exit_policy_block",
    "GATE_RISK_DETERIORATION", "GATE_IMPROVEMENT_UNMEASURABLE",
    "IMPROVEMENT_BASIS", "EXPECTED_RETURN_STATE",
    "default_policy", "build_reassessment", "classify_inputs", "evaluate_churn",
    "retained_concentration", "explain_holding", "explain_portfolio",
    "holdings_snapshot_hash", "stable_hash",
]
