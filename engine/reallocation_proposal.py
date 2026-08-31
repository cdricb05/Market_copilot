r"""Phase 29H Slice 7 — Portfolio Reallocation Proposal Engine (pure calculation kernel).

This module is the ONE canonical portfolio-reallocation calculation (Consolidation
Roadmap Slice 7 / Charter Milestone 3). It is a **pure, deterministic kernel**: it
performs NO I/O — no file, database, network, provider or prediction access — and
never mutates its inputs. Everything it needs arrives as one immutable
reallocation-input contract (built by ``api.reallocation_proposal`` from the
authoritative owners), and it returns one immutable proposal result.

From the canonical CURRENT portfolio state (``api.portfolio_state``) and the Slice 6
Holding Opportunity-Cost assessment (``api.holding_opportunity_cost``) it produces one
coherent PAPER-ONLY proposed target portfolio and answers:

  * which current holdings are RETAINED / INCREASED / REDUCED / EXITED / REPLACED;
  * which replacement / addition candidates receive capital, and at what target weight;
  * how much turnover the proposal requires, and the implied transaction cost;
  * whether the portfolio SCORE improves after switching costs (there is NO validated
    expected-return model, so expected return is always null / NOT_CALIBRATED — the
    improvement is a signal-score comparison, never a fabricated dollar return);
  * what happens to concentration and portfolio risk before vs after.

The proposal is REVIEW ONLY. The result NEVER creates an operational target, an alpha
target, an order, a fill; it changes no holding, cash or NAV; it performs no broker
execution and promotes no model.

The deterministic allocation policy REUSES (never forks) the canonical construction
constants from ``api.multi_horizon_engine`` (target book size / name cap / sector cap /
liquidity floor) and the transaction-cost constant from ``api.paper_trading_desk``
(the API owner injects the live values), plus the covariance risk primitive from the
Slice 6 kernel (``engine.holding_opportunity_cost.compute_risk_contributions``). The
genuinely-new Slice-7 thresholds are declared once in :func:`default_policy`, returned
in the payload and folded into the deterministic ``proposal_hash``.
"""
from __future__ import annotations

import hashlib
import json
import math
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Optional

from paper_trader.engine import constrained_reallocation as _cr
from paper_trader.engine import holding_opportunity_cost as hoc_kernel

SCHEMA_VERSION = "reallocation_proposal.v1"
INPUT_SCHEMA_VERSION = "reallocation_proposal.input.v1"
ALLOCATION_POLICY_VERSION = "reallocation_allocation_policy.v1"
COST_POLICY_VERSION = "reallocation_cost_policy.v1"

CALCULATION_OWNER = "engine.reallocation_proposal"

# --- Frozen proposal-state vocabulary ---------------------------------------- #
STATE_READY = "READY"
STATE_DEGRADED = "DEGRADED"
STATE_BLOCKED = "BLOCKED"
STATE_NO_ACTIVE_BOOK = "NO_ACTIVE_BOOK"
#: Release 29.3 — a COMPLETE target was constructed and is fully explainable, but it
#: does not satisfy a portfolio-level limit that can only be judged on the complete
#: target (turnover budget / concentration / sector concentration / post-change risk).
#: The target is published so the operator can see exactly what was rejected and why;
#: it is NEVER approvable and NEVER produces an order plan. Fail-closed by construction.
#:
#: Release 47 narrowed WHEN this state is reached, and only that. A breached limit now
#: RE-OPTIMISES the target first (``engine.constrained_reallocation``); this state is
#: reached only when the repaired target STILL breaches - i.e. the feasible set is
#: empty. A sector cap, a concentration cap, a name cap, a risk-contribution limit or
#: a turnover budget can no longer, on its own, freeze the portfolio. The fail-closed
#: guarantee is unchanged: WITHHELD remains un-approvable at every layer.
STATE_WITHHELD = "WITHHELD"
PROPOSAL_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_WITHHELD,
                        STATE_NO_ACTIVE_BOOK)
#: States in which the proposal may be offered for manual review / approval.
APPROVABLE_STATES = (STATE_READY, STATE_DEGRADED)

# --- Release 29.3 — complete-target constraint codes ------------------------- #
# These four constraints MOVED here from ``engine.portfolio_reassessment``, which could
# only evaluate them against the retained-only stub renormalised to 1.0 (an object
# nobody will ever hold). They are decided exactly ONCE, here, on the complete target.
CT_TURNOVER_BUDGET = "TURNOVER_BUDGET_EXCEEDED"
CT_CONCENTRATION = "CONCENTRATION_DETERIORATION_BLOCKS_CHANGE"
CT_RISK_DETERIORATION = "RISK_DETERIORATION_BLOCKS_CHANGE"
CT_SECTOR_CAP = "SECTOR_CAP_BREACH_BLOCKS_CHANGE"
#: Release 50 - a cross-asset limit (asset class / sleeve / currency / collateral /
#: gross exposure) breached by the complete target. A RESHAPING limit exactly like
#: the sector cap: it routes the target to the Release-47 re-optimiser, and only an
#: empty feasible set withholds the decision.
CT_CROSS_ASSET_CAP = "CROSS_ASSET_CAP_BREACH_BLOCKS_CHANGE"
COMPLETE_TARGET_CONSTRAINT_CODES = (CT_TURNOVER_BUDGET, CT_CONCENTRATION,
                                    CT_RISK_DETERIORATION, CT_SECTOR_CAP,
                                    CT_CROSS_ASSET_CAP)

#: Release 50 - instrument fields a universe / position row MAY carry. A row without
#: them is a US cash equity, the pre-R50 contract.
_INSTRUMENT_FIELDS = ("asset_class", "sleeve_id", "instrument_type", "currency",
                      "multiplier", "initial_margin_per_unit", "capital_usage_ratio",
                      "unit_notional_usd", "cost_bps_per_side", "score_basis",
                      "execution_convention")
_EQUITY_DEFAULTS = {"asset_class": "US_EQUITY",
                    "sleeve_id": "us_equity_fundamental_momentum_50_50_v1",
                    "instrument_type": "CASH_EQUITY", "currency": "USD",
                    "multiplier": 1.0, "initial_margin_per_unit": 0.0,
                    "capital_usage_ratio": 1.0, "unit_notional_usd": None,
                    "cost_bps_per_side": None,
                    "score_basis": "OPERATIONAL_MODEL_COMBINED_PERCENTILE",
                    "execution_convention": "NEXT_CLOSE"}


def instrument_fields(row: Optional[dict]) -> dict:
    """The instrument contract of a universe / position / allocation row, with the
    equity defaults for every field the row does not carry."""
    r = row or {}
    out = {}
    for k in _INSTRUMENT_FIELDS:
        v = r.get(k)
        out[k] = v if v is not None else _EQUITY_DEFAULTS[k]
    return out
#: The owner that ASKS for a target but must never decide these constraints itself.
ASK_GATE_OWNER = "engine.portfolio_reassessment"

# --- Frozen per-ticker action vocabulary ------------------------------------- #
ACT_RETAIN = "RETAIN"
ACT_INCREASE = "INCREASE"
ACT_REDUCE = "REDUCE"
ACT_EXIT = "EXIT"
ACT_ADD = "ADD"
ACT_REPLACE_OUT = "REPLACE_OUT"
ACT_REPLACE_IN = "REPLACE_IN"
ACTION_VOCAB = (ACT_RETAIN, ACT_INCREASE, ACT_REDUCE, ACT_EXIT, ACT_ADD,
                ACT_REPLACE_OUT, ACT_REPLACE_IN)

# --- HOC recommendation vocabulary (the decision input) ---------------------- #
REC_HOLD = "HOLD"
REC_REDUCE = "REDUCE"
REC_EXIT = "EXIT"
REC_REPLACE = "REPLACE"
REC_ADD = "ADD"

# --- Gap classification (which analytic a carried gap affects) --------------- #
AFFECTS_ALLOCATION = "ALLOCATION"
AFFECTS_RISK = "RISK_ANALYTICS"
AFFECTS_EXPECTED_RETURN = "EXPECTED_RETURN_ANALYTICS"
AFFECTS_INFORMATIONAL = "INFORMATIONAL"

# The improvement basis is always a signal-score comparison; a dollar expected return
# is NEVER claimed (no validated forecast model exists — Charter data-integrity rule).
IMPROVEMENT_BASIS = "PORTFOLIO_COMBINED_PERCENTILE_UPLIFT_NET_OF_MODELED_TURNOVER_COST"
EXPECTED_RETURN_STATE_NOT_CALIBRATED = "NOT_CALIBRATED"
EXPECTED_RETURN_GAP = "EXPECTED_RETURN_NOT_CALIBRATED"
VOLATILITY_AFTER_GAP = "PORTFOLIO_VOLATILITY_AFTER_UNAVAILABLE"
VOLATILITY_BEFORE_GAP = "PORTFOLIO_VOLATILITY_BEFORE_UNAVAILABLE"

# Published volatility state must reflect the EFFECTIVE decision — the raw covariance
# kernel AND the coverage gate — never the raw kernel state alone. Otherwise the contract
# can report AVAILABLE while the value is withheld as a data gap (the live 2026-08-07
# payload: covariance covered ~0.59 of invested weight < the 0.80 floor, so the value was
# null and PORTFOLIO_VOLATILITY_*_UNAVAILABLE was raised, yet the state said AVAILABLE).
# INSUFFICIENT_COVERAGE reuses established canonical vocabulary (alpha_agent.stage12_autopsy,
# alpha_agent.tournament BLOCKED_INSUFFICIENT_COVERAGE, api.alpha_target).
VOL_STATE_AVAILABLE = "AVAILABLE"
VOL_STATE_UNAVAILABLE = "UNAVAILABLE"
VOL_STATE_INSUFFICIENT_COVERAGE = "INSUFFICIENT_COVERAGE"
VOLATILITY_STATE_VOCAB = (VOL_STATE_AVAILABLE, VOL_STATE_UNAVAILABLE,
                          VOL_STATE_INSUFFICIENT_COVERAGE)

# How a carried Slice-6 gap maps onto the analytic it affects for the proposal.
_HOC_GAP_AFFECTS = {
    "PRIOR_RANK_UNAVAILABLE": AFFECTS_INFORMATIONAL,
    "LIQUIDITY_UNAVAILABLE": AFFECTS_INFORMATIONAL,
    "RISK_CONTRIBUTION_UNAVAILABLE": AFFECTS_RISK,
}


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
def default_policy() -> dict[str, Any]:
    """The single explicit, versioned allocation + cost policy.

    Values marked ``reused`` mirror the canonical ``api.multi_horizon_engine`` /
    ``api.paper_trading_desk`` constants and are OVERRIDDEN by the API owner with the
    live values so no threshold is silently forked. Values marked ``new`` are
    genuinely-new Slice-7 thresholds justified in docs/ARCHITECTURE_DECISIONS.md; they
    are declared here once, returned in the payload and folded into the proposal hash,
    and are exercised at their boundaries by the test suite.
    """
    return {
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "cost_policy_version": COST_POLICY_VERSION,
        # --- reused canonical construction / cost constants ------------------- #
        "target_position_count": 25,      # reused: eng.BOOK_SIZES[0] (primary book size N)
        "max_name_weight": 0.10,          # reused: eng.MAX_INDIVIDUAL_WEIGHT
        "sector_cap_fraction": 0.25,      # reused: eng.SECTOR_CAP_FRACTION
        "min_adv_dollar": 1.0e7,          # reused: eng.MIN_ADV_DOLLAR liquidity floor
        "entry_rank": 25,                 # reused: eng.BOOK_SIZES[0]
        "exit_buffer_rank": 30,           # reused: ceil(N*(1+exit_buffer_fraction))
        "cost_bps_per_side": 12.5,        # reused: desk.COST_BPS_PER_SIDE
        "round_trip_cost_bps": 25.0,      # reused: 2 * cost_bps_per_side
        "cost_rate_per_side": 0.00125,    # reused: desk.COST_RATE_PER_SIDE
        # --- genuinely-new Slice-7 allocation thresholds --------------------- #
        # A REDUCE trims the incumbent weight by this fraction (retain at lower weight).
        # Proportional so the proposed weight is ALWAYS strictly below the current one.
        "reduce_fraction": 0.5,                                   # new
        # The ADD / REPLACE candidate pool is the eligible non-held universe with rank
        # within this bound (2*N): a reallocation should not reach deep into the tail.
        "candidate_rank_max": 50,                                # new (= 2 * entry_rank)
        # REPLACE hurdles (mirror the Slice-6 decision policy so the two stay coherent).
        "min_gross_score_improvement": 0.02,                     # new
        "min_net_improvement": 0.05,                             # new
        "score_points_per_cost_bp": 0.001,                       # new: bps -> percentile hurdle
        # --- risk / volatility ------------------------------------------------ #
        "covariance_lookback": 60,                               # reused Slice-6 lookback
        "min_covariance_obs": 40,                                # reused Slice-6 min obs
        "covariance_variance_floor": 1.0e-12,                    # reused Slice-6 floor
        # Minimum fraction of proposed invested weight that must be covered by aligned
        # returns before an after-covariance portfolio volatility is reported.
        "min_volatility_coverage": 0.80,                         # new
        # --- Release 29.3: complete-target portfolio limits ------------------ #
        # MOVED here from engine.portfolio_reassessment (same values, same meaning) so
        # each is judged exactly once, on the object that actually determines it. A
        # breach WITHHOLDS the proposal; it never silently trims the target to fit, and
        # it never relaxes a limit to force a proposal into existence.
        "max_one_way_turnover": 0.35,                 # moved: reassessment turnover budget
        "max_concentration_increase": 0.02,           # moved: reassessment HHI deterioration
        # Both deterioration limits test WORSENING, never a pre-existing breach: a book
        # that already sits above a cap is a standing condition the operator owns and it
        # must not permanently freeze every future reallocation.
        "sector_cap_deterioration_only": True,        # moved: reassessment semantics
        # --- classification / reconciliation tolerances ---------------------- #
        "material_weight_delta": 1.0e-4,                          # new: RETAIN vs INCREASE/REDUCE band
        "weight_reconcile_tol": 1.0e-6,                          # new
        # --- Release 50: cross-asset limits (ONE value each; mirrored into the
        # Release-47 repair kernel through constraint_policy_projection) -------- #
        "max_gross_exposure": 1.0,
        "asset_class_weight_caps": {"US_EQUITY": 1.0, "CASH": 1.0, "DEFAULT_NON_EQUITY": 0.25},
        "sleeve_weight_caps": {"us_equity_fundamental_momentum_50_50_v1": 1.0, "cash_usd": 1.0,
                               "DEFAULT_NON_EQUITY": 0.25},
        "non_usd_currency_cap": 0.20,
        "collateral_cap_fraction": 0.25,
    }


# --------------------------------------------------------------------------- #
# Small numeric / money helpers (stdlib only, deterministic)
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _round_money(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


_TRADING_DAYS_YEAR = 252.0


# --------------------------------------------------------------------------- #
# Stable hashing (proposal_hash excludes generated_at / volatile keys)
# --------------------------------------------------------------------------- #
_VOLATILE_KEYS = frozenset({"generated_at", "evaluated_at", "loaded_at", "built_at",
                            "proposal_hash", "artifact"})


def _strip_volatile(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items() if k not in _VOLATILE_KEYS}
    if isinstance(obj, (list, tuple)):
        return [_strip_volatile(v) for v in obj]
    return obj


def stable_hash(obj: Any) -> str:
    payload = json.dumps(_strip_volatile(obj), sort_keys=True, default=str,
                         separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Portfolio volatility (reuses the Slice-6 covariance primitive — never forked)
# --------------------------------------------------------------------------- #
def _portfolio_volatility(*, weights: dict, aligned_returns: dict, policy: dict) -> dict:
    """Annualized portfolio volatility from date-aligned daily returns, reusing the
    Slice-6 covariance kernel. Returns ``{volatility, state, covered_weight, ...}``.

    ``weights`` need not be normalised — the kernel renormalizes over the covariance-
    eligible names; ``covered_weight`` is the fraction of the supplied (positive) weight
    represented in that covariance universe, used by the caller's coverage gate.
    """
    pos = {tk: w for tk, w in (weights or {}).items() if (_f(w) or 0.0) > 0}
    total_w = sum(pos.values())
    if not pos or total_w <= 0:
        return {"volatility": None, "variance_daily": None, "state": "UNAVAILABLE",
                "covered_weight": 0.0, "included_tickers": [], "observations_used": 0}
    risk = hoc_kernel.compute_risk_contributions(
        weights=pos, aligned_returns=aligned_returns or {}, policy=policy)
    included = risk.get("included_tickers") or []
    covered = sum(pos.get(tk, 0.0) for tk in included) / total_w if total_w > 0 else 0.0
    var_daily = risk.get("portfolio_variance_daily")
    if risk.get("state") != "AVAILABLE" or var_daily is None or var_daily < 0:
        return {"volatility": None, "variance_daily": var_daily, "state": "UNAVAILABLE",
                "covered_weight": round(covered, 6), "included_tickers": included,
                "observations_used": risk.get("observations_used", 0)}
    vol = math.sqrt(var_daily * _TRADING_DAYS_YEAR)
    return {"volatility": vol, "variance_daily": var_daily, "state": "AVAILABLE",
            "covered_weight": round(covered, 6), "included_tickers": included,
            "observations_used": risk.get("observations_used", 0)}


def _effective_volatility(prim: dict, cov_floor: float) -> tuple[str, Optional[float]]:
    """Reconcile the raw covariance-kernel result (``prim`` from ``_portfolio_volatility``)
    with the coverage gate into ONE honest ``(state, volatility)`` decision, so the
    published state can NEVER contradict the published value.

    * kernel could not compute at all            -> ``UNAVAILABLE``, value withheld
    * kernel computed but ``covered_weight`` is
      below ``min_volatility_coverage``          -> ``INSUFFICIENT_COVERAGE``, value withheld
    * kernel computed and coverage meets floor   -> ``AVAILABLE``, value published

    The value is never fabricated: it is published only in the AVAILABLE branch.
    """
    if prim.get("state") != VOL_STATE_AVAILABLE or prim.get("volatility") is None:
        return VOL_STATE_UNAVAILABLE, None
    if (prim.get("covered_weight") or 0.0) < cov_floor:
        return VOL_STATE_INSUFFICIENT_COVERAGE, None
    return VOL_STATE_AVAILABLE, prim.get("volatility")


# --------------------------------------------------------------------------- #
# Concentration primitives
# --------------------------------------------------------------------------- #
def _herfindahl(weights: dict) -> Optional[float]:
    ws = [(_f(w) or 0.0) for w in weights.values()]
    return sum(w * w for w in ws) if ws else None


def _largest_weight(weights: dict) -> Optional[float]:
    ws = [(_f(w) or 0.0) for w in weights.values()]
    return max(ws) if ws else None


def _sector_weights(rows: list, weight_key: str) -> dict:
    out: dict[str, float] = {}
    for r in rows:
        sec = r.get("sector") or "Unknown"
        out[sec] = out.get(sec, 0.0) + (_f(r.get(weight_key)) or 0.0)
    return out


def _max_sector_weight(sector_weights: dict) -> Optional[float]:
    """Largest KNOWN sector weight. Track B — "Unknown" is a missing
    classification (a data-quality state), never a sector: counting it here let a
    31.7% unclassified bucket masquerade as the book's largest "sector" and trip
    the complete-target sector cap. Its weight is reported separately."""
    known = [v for k, v in sector_weights.items()
             if k != hoc_kernel.UNCLASSIFIED_SECTOR]
    return max(known) if known else None


def _unclassified_weight(sector_weights: dict) -> float:
    return float(sector_weights.get(hoc_kernel.UNCLASSIFIED_SECTOR, 0.0) or 0.0)


# --------------------------------------------------------------------------- #
# Release 47 - constraint re-optimisation seam
#
# A breached portfolio limit must CHANGE THE SOLUTION, not freeze the portfolio.
# These three helpers are the entire seam between this kernel (which decides WHAT a
# target should contain) and ``engine.constrained_reallocation`` (which decides what
# a FEASIBLE version of that target looks like). No allocation mathematics moves:
# the repair kernel only caps, redistributes and defers, and everything it produces
# is re-measured here by the SAME functions that measured the ideal target.
# --------------------------------------------------------------------------- #
def _positive(weights: dict) -> dict:
    return {tk: w for tk, w in (weights or {}).items() if (_f(w) or 0.0) > 0.0}


def _reoptimised_action(*, ticker: str, action: str, reason_codes: list,
                        delta: float, proposed: float, held: bool,
                        policy: dict) -> tuple:
    """Re-derive one row's ACTION from the repaired weights, keeping provenance.

    A repaired target may reduce a name the ideal target wanted to add, or leave a
    name untouched that the ideal target wanted to exit. The label must follow the
    weights, never the intention, or the operator reads an action the plan does not
    perform.
    """
    band = float(policy["material_weight_delta"])
    codes = sorted(set(list(reason_codes or []) + ["CONSTRAINT_REOPTIMIZED"]))
    if proposed <= band and held:
        # A REPLACE_OUT keeps its counterparty semantics; anything else is an EXIT.
        return (action if action == ACT_REPLACE_OUT else ACT_EXIT), codes
    if proposed <= band and not held:
        return ACT_EXIT, codes
    if not held:
        return (action if action in (ACT_ADD, ACT_REPLACE_IN) else ACT_ADD), codes
    if delta > band:
        return ACT_INCREASE, codes
    if delta < -band:
        return ACT_REDUCE, codes
    return ACT_RETAIN, codes


def _cr_candidates(*, universe_rows: list, urows: dict, held_set: set,
                   sector_of: dict, pct_fn) -> list:
    """The eligible candidate set in the shape the repair kernel expects.

    It is built from the SAME universe rows the allocation passes used, plus the
    currently held names so a held position can be re-sized rather than only exited.
    Nothing is scored here: ``score`` is the combined percentile the scoring owner
    already published, read through this kernel's own ``_pct``.
    """
    out: dict[str, dict] = {}
    for r in universe_rows or []:
        tk = r.get("ticker")
        if not tk or not r.get("eligible", True):
            continue
        out[tk] = {"ticker": tk, "sector": r.get("sector") or "Unknown",
                   "adv_dollar": _f(r.get("adv_dollar")), "rank": r.get("rank"),
                   "score": _f(r.get("percentile")), **instrument_fields(r)}
    for tk in sorted(held_set):
        if tk in out:
            continue
        u = urows.get(tk) or {}
        # A held name absent from the eligible universe is NOT added here: its exit
        # is mandatory, and inventing eligibility for it would defeat that.
        if not u:
            continue
        out[tk] = {"ticker": tk,
                   "sector": sector_of.get(tk) or u.get("sector") or "Unknown",
                   "adv_dollar": _f(u.get("adv_dollar")), "rank": u.get("rank"),
                   "score": _f(pct_fn(tk)), **instrument_fields(u)}
    return [out[tk] for tk in sorted(out)]


def _reoptimise_if_infeasible(*, measured: dict, current_weight: dict,
                              held_set: set, universe_rows: list, urows: dict,
                              sector_of: dict, hoc_reviews: list, pct_fn,
                              nav: Optional[float], policy: dict) -> dict:
    """Repair the complete target when - and only when - a portfolio limit breaches.

    Returns a ledger of what the repair did. ``applied`` is False when nothing was
    breached (the ideal target is already feasible and is used verbatim) or when the
    repair could not produce a feasible target at all, which is the ONE case that
    still ends in the fail-closed WITHHELD path.
    """
    limits = measured["limits"]
    breach_codes = sorted({b.get("code") for b in (limits.get("breaches") or [])})
    base = {
        "owner": _cr.CALCULATION_OWNER,
        "constraint_policy_version": _cr.CONSTRAINT_POLICY_VERSION,
        "applied": False,
        "ideal_target_was_feasible": not breach_codes,
        # The unconstrained complete target this kernel wanted, kept verbatim so the
        # operator can see exactly what the constraints changed and what they cost.
        "ideal_target": {tk: _r(w, 6) for tk, w in
                         sorted(_positive(measured["proposed_weight"]).items())},
        "ideal_limits": limits,
        "breached_limits": breach_codes,
        "constraints_that_reshaped": [],
        "constraint_adjustments": [],
        "mandatory_exits": [],
        "incumbency_policy": _cr.INCUMBENCY_POLICY,
        "doc": ("A normal portfolio constraint reshapes the solution. Only an empty "
                "feasible set withholds a portfolio decision."),
    }
    if not breach_codes:
        return base

    cands = _cr_candidates(universe_rows=universe_rows, urows=urows,
                           held_set=held_set, sector_of=sector_of, pct_fn=pct_fn)
    contrib = {r.get("ticker"): _f(r.get("risk_contribution"))
               for r in (hoc_reviews or []) if r.get("ticker")
               and _f(r.get("risk_contribution")) is not None}
    solution = _cr.solve_feasible_target(
        current_weight=current_weight,
        ideal_weight=_positive(measured["proposed_weight"]),
        candidates=cands, nav=nav, risk_contributions=contrib,
        policy=constraint_policy_projection(policy))
    base.update({
        "applied": bool(solution["feasible"]),
        "best_feasible_target": solution["best_feasible_target"],
        "constraint_adjustments": solution["constraint_adjustments"],
        "constraints_that_reshaped": solution["constraints_that_reshaped"],
        "mandatory_exits": solution["mandatory_exits"],
        "released_weight": solution["released_weight"],
        "redistributed_weight": solution["redistributed_weight"],
        "turnover": solution["turnover"],
        "verification": solution["verification"],
        "solution_hash": solution["solution_hash"],
        "feasible_set_empty": not solution["feasible"],
        "blockers": solution["blockers"],
    })
    return base


def constraint_policy_projection(policy: dict) -> dict:
    """Project the Slice-7 policy onto the repair kernel's policy so no limit is
    forked: the caps, the budget and the cost rate keep exactly ONE value each."""
    out = {}
    for k in ("max_name_weight", "sector_cap_fraction", "min_adv_dollar",
              "cost_rate_per_side", "round_trip_cost_bps",
              "score_points_per_cost_bp", "target_position_count",
              "max_one_way_turnover", "max_concentration_increase",
              "material_weight_delta"):
        if k in policy:
            out[k] = policy[k]
    for k in ("max_adv_participation", "max_name_risk_contribution",
              "min_position_weight", "min_cash_weight", "max_cash_weight",
              "min_switching_net_improvement", "min_switching_turnover",
              # Release 50 - the cross-asset limits keep ONE value each.
              "max_gross_exposure", "asset_class_weight_caps", "sleeve_weight_caps",
              "non_usd_currency_cap", "collateral_cap_fraction"):
        if k in policy:
            out[k] = policy[k]
    return out


def _equivalent_rank(score: Optional[float], equity_rows: list) -> Optional[int]:
    """Release 50 - where a frontier row's normalised score would sit among the
    equity ranks: one more than the count of equity rows scoring at least as
    well. Equity rows keep their own rank, so an equity-only pool is ordered
    exactly as before; a frontier row is interleaved by score, never appended."""
    if score is None:
        return None
    return 1 + sum(1 for r in equity_rows
                   if _f(r.get("percentile")) is not None and _f(r.get("percentile")) >= score)


# --------------------------------------------------------------------------- #
# Core entry point
# --------------------------------------------------------------------------- #
def build_proposal(*, input_contract: dict, policy: Optional[dict] = None) -> dict:
    """Compute the canonical reallocation proposal (pure, deterministic).

    ``input_contract`` is the immutable reallocation-input contract. Returns the frozen
    proposal contract. Never raises on incomplete data — it degrades to
    DEGRADED / BLOCKED / NO_ACTIVE_BOOK with explicit reason codes and gap classes.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)

    ic = input_contract or {}
    eligible_date = ic.get("eligible_market_date")
    active_book_id = ic.get("active_book_id")
    positions = [dict(p) for p in (ic.get("positions") or [])]
    hoc_reviews = list(ic.get("hoc_reviews") or [])
    universe_rows = list(ic.get("universe_rows") or [])
    nav = _f(ic.get("nav"))
    cash = _f(ic.get("cash"))
    hoc_available = bool(ic.get("hoc_available"))
    hoc_state = ic.get("hoc_assessment_state")
    hoc_gaps = list(ic.get("hoc_data_gaps") or [])

    # --- core-input validation -> BLOCKED / NO_ACTIVE_BOOK ------------------- #
    if not active_book_id:
        return _empty_result(pol, ic, STATE_NO_ACTIVE_BOOK,
                             [{"code": "NO_ACTIVE_BOOK",
                               "detail": "No active operational book was supplied."}])
    blockers: list[dict] = []
    if not eligible_date:
        blockers.append({"code": "MISSING_ELIGIBLE_MARKET_DATE"})
    if nav is None or nav <= 0:
        blockers.append({"code": "MISSING_OR_NONPOSITIVE_NAV"})
    if not ic.get("portfolio_state_hash"):
        blockers.append({"code": "MISSING_PORTFOLIO_STATE_HASH"})
    if not ic.get("universe_scoring_hash"):
        blockers.append({"code": "MISSING_UNIVERSE_SCORING_HASH"})
    if not ic.get("hoc_assessment_hash"):
        blockers.append({"code": "MISSING_HOC_ASSESSMENT_HASH"})
    if not hoc_available or hoc_state not in ("READY", "DEGRADED"):
        blockers.append({"code": "HOC_ASSESSMENT_NOT_AVAILABLE",
                         "detail": "The Slice 6 Holding Opportunity-Cost assessment is "
                                   "not available (state=%s); a trustworthy target "
                                   "portfolio cannot be constructed." % hoc_state})
    if not positions:
        blockers.append({"code": "NO_HOLDINGS"})
    if not universe_rows:
        blockers.append({"code": "MISSING_UNIVERSE_ROWS"})
    if blockers:
        return _empty_result(pol, ic, STATE_BLOCKED, blockers)

    # --- index the inputs ---------------------------------------------------- #
    rec_by_ticker = {r.get("ticker"): r for r in hoc_reviews if r.get("ticker")}
    urows = {r.get("ticker"): r for r in universe_rows if r.get("ticker")}
    held = [p.get("ticker") for p in positions if p.get("ticker")]
    held_set = set(held)
    current_weight = {p.get("ticker"): (_f(p.get("current_weight")) or 0.0) for p in positions
                      if p.get("ticker")}
    current_mv = {p.get("ticker"): (_f(p.get("market_value")) or 0.0) for p in positions
                  if p.get("ticker")}
    sector_of = {}
    pos_by: dict[str, dict] = {}
    for p in positions:
        tk = p.get("ticker")
        if tk:
            # Track B — the literal "Unknown" on a position row is MISSING data and
            # must not shadow the canonical universe-row sector.
            sector_of[tk] = hoc_kernel.known_sector(
                p.get("sector"), (urows.get(tk) or {}).get("sector")) or "Unknown"
            pos_by[tk] = p
    # Release 50 - instrument metadata for the cross-asset limits (equity defaults
    # for every row that carries none, so an equity-only book is unchanged).
    meta_rows = [dict(r, ticker=r.get("ticker")) for r in universe_rows if r.get("ticker")]
    meta_rows += [dict(instrument_fields(p), ticker=p.get("ticker")) for p in positions
                  if p.get("ticker") and p.get("ticker") not in urows]
    inst_meta = _cr.candidate_meta(meta_rows)

    N = int(pol["target_position_count"])
    base_weight = round(min(1.0 / N, pol["max_name_weight"]), 8) if N > 0 else 0.0
    max_per_sector = max(1, int(pol["sector_cap_fraction"] * N))
    reduce_fraction = pol["reduce_fraction"]

    def _pct(tk: str) -> Optional[float]:
        """Combined percentile (signal strength in [0,1]) for a name, if scored."""
        r = urows.get(tk)
        if r is not None:
            return _f(r.get("percentile"))
        rev = rec_by_ticker.get(tk) or {}
        return _f(rev.get("signal_strength"))

    def _combined(tk: str) -> Optional[float]:
        r = urows.get(tk)
        if r is not None:
            return _f(r.get("combined_score"))
        rev = rec_by_ticker.get(tk) or {}
        return _f(rev.get("current_score"))

    # --- candidate pool: eligible, non-held, liquid, within candidate rank --- #
    # Release 50 - a frontier (non-equity) row carries a rank inside ITS sleeve; it
    # is interleaved into the pool by its normalised score (equivalent rank), so the
    # equity ordering is untouched and nothing is appended as a residual sink.
    equity_rows = [r for r in universe_rows if not r.get("frontier_row")]
    pool_rows = []
    for r in universe_rows:
        if not r.get("ticker"):
            continue
        if r.get("frontier_row"):
            eq_rank = _equivalent_rank(_f(r.get("percentile")), equity_rows)
            if eq_rank is None:
                continue
            pool_rows.append((eq_rank, 1, dict(r, pool_rank=eq_rank)))
        elif r.get("rank") is not None:
            pool_rows.append((r.get("rank"), 0, dict(r, pool_rank=r.get("rank"))))
    candidate_pool = []
    for _rk, _kind, r in sorted(pool_rows, key=lambda t: (t[0], t[1], t[2].get("ticker"))):
        tk = r.get("ticker")
        if tk in held_set:
            continue
        if not r.get("eligible", True):
            continue
        if r.get("pool_rank") is not None and r.get("pool_rank") > pol["candidate_rank_max"]:
            continue
        adv = _f(r.get("adv_dollar"))
        if adv is not None and adv < pol["min_adv_dollar"]:
            continue
        candidate_pool.append(r)

    # --- allocation state ---------------------------------------------------- #
    selected: dict[str, dict] = {}   # ticker -> proposed record (proposed weight > 0)
    sector_count: dict[str, int] = {}
    used_candidates: set[str] = set()
    proposed_zero: dict[str, dict] = {}  # held tickers with proposed weight 0

    def _occupy(tk: str, sec: str):
        sector_count[sec] = sector_count.get(sec, 0) + 1

    def _can_place(sec: str) -> bool:
        if len(selected) >= N:
            return False
        if sec != "Unknown" and sector_count.get(sec, 0) >= max_per_sector:
            return False
        return True

    # Pass A — classify current holdings (deterministic order: weight desc, ticker asc).
    replace_pending: list[dict] = []
    for p in sorted(positions, key=lambda x: (-(_f(x.get("current_weight")) or 0.0),
                                              x.get("ticker") or "")):
        tk = p.get("ticker")
        if not tk:
            continue
        sec = sector_of.get(tk, "Unknown")
        rec = (rec_by_ticker.get(tk) or {}).get("recommendation") or REC_HOLD
        if rec == REC_EXIT:
            proposed_zero[tk] = {"action": ACT_EXIT, "reason_codes": ["HOC_EXIT"],
                                 "source_hoc_recommendation": REC_EXIT}
            continue
        if rec == REC_REPLACE:
            replace_pending.append(p)
            continue
        if rec == REC_REDUCE:
            reduced = round((current_weight.get(tk, 0.0)) * (1.0 - reduce_fraction), 8)
            selected[tk] = {"weight": reduced, "action": ACT_REDUCE,
                            "reason_codes": ["HOC_REDUCE", "TRIMMED_TO_%d_PCT_OF_CURRENT"
                                             % int(round((1.0 - reduce_fraction) * 100))],
                            "source_hoc_recommendation": REC_REDUCE}
            _occupy(tk, sec)
            continue
        # HOLD (and any non-EXIT/REPLACE/REDUCE) -> retain at the base weight.
        selected[tk] = {"weight": base_weight, "action": ACT_RETAIN,
                        "reason_codes": ["HOC_HOLD_RETAINED"],
                        "source_hoc_recommendation": rec}
        _occupy(tk, sec)

    # Pass B — REPLACE resolution: each REPLACE_OUT is matched to a specific, traceable
    # eligible non-held candidate that clears the net-of-cost hurdle; unmatched REPLACEs
    # are retained (never a silent exit-to-cash), keeping every REPLACE traceable.
    round_trip_bps = pol["round_trip_cost_bps"]
    cost_hurdle_score = round_trip_bps * pol["score_points_per_cost_bp"]
    for p in sorted(replace_pending, key=lambda x: (-(_f(x.get("current_weight")) or 0.0),
                                                    x.get("ticker") or "")):
        tk = p.get("ticker")
        sec = sector_of.get(tk, "Unknown")
        inc_pct = _pct(tk) or 0.0
        placed = False
        for cand in candidate_pool:
            ctk = cand.get("ticker")
            if ctk in used_candidates or ctk in selected:
                continue
            csec = cand.get("sector") or "Unknown"
            if not _can_place(csec):
                continue
            cand_pct = _f(cand.get("percentile"))
            if cand_pct is None:
                continue
            gross = cand_pct - inc_pct
            net = gross - cost_hurdle_score
            if gross >= pol["min_gross_score_improvement"] - 1e-9 \
                    and net >= pol["min_net_improvement"] - 1e-9:
                selected[ctk] = {"weight": base_weight, "action": ACT_REPLACE_IN,
                                 "reason_codes": ["FUNDS_REPLACEMENT_OF_%s" % tk],
                                 "source_hoc_recommendation": REC_ADD,
                                 "replacement_of": tk}
                _occupy(ctk, csec)
                used_candidates.add(ctk)
                proposed_zero[tk] = {"action": ACT_REPLACE_OUT,
                                     "reason_codes": ["HOC_REPLACE",
                                                      "REPLACED_BY_%s" % ctk],
                                     "source_hoc_recommendation": REC_REPLACE,
                                     "replacement_for": ctk}
                placed = True
                break
        if not placed:
            # No feasible net-positive replacement -> retain the incumbent (traceable).
            selected[tk] = {"weight": base_weight, "action": ACT_RETAIN,
                            "reason_codes": ["HOC_REPLACE",
                                             "REPLACE_DEFERRED_NO_FEASIBLE_CANDIDATE"],
                            "source_hoc_recommendation": REC_REPLACE}
            _occupy(tk, sec)

    # Pass C — fill remaining slots with the best-ranked eligible candidates (ADD).
    for cand in candidate_pool:
        if len(selected) >= N:
            break
        ctk = cand.get("ticker")
        if ctk in used_candidates or ctk in selected:
            continue
        csec = cand.get("sector") or "Unknown"
        if not _can_place(csec):
            continue
        selected[ctk] = {"weight": base_weight, "action": ACT_ADD,
                         "reason_codes": ["ELIGIBLE_TOP_CANDIDATE_NOT_HELD"],
                         "source_hoc_recommendation": REC_ADD}
        _occupy(ctk, csec)
        used_candidates.add(ctk)

    # --- build the allocation rows (all current + all proposed tickers) ------ #
    def _allocation_rows(override: Optional[dict] = None) -> tuple:
        """The allocation rows and proposed weights for ONE candidate target.

        ``override`` is the Release-47 constraint-repaired target. Provenance
        (source recommendation, replacement relationship, reason codes) is carried
        through unchanged, but the ACTION is re-derived from the actual weight
        change, so a repaired row can never keep a label its weights no longer
        support.
        """
        all_tickers = sorted(held_set | set(selected.keys()) | set(override or {}))
        allocations: list[dict] = []
        proposed_weight: dict[str, float] = {}
        for tk in all_tickers:
            cw = current_weight.get(tk, 0.0)
            sel = selected.get(tk)
            if override is not None:
                pw = round(float(override.get(tk, 0.0) or 0.0), 8)
            elif sel is not None:
                pw = round(float(sel["weight"]), 8)
            else:
                pw = 0.0
            proposed_weight[tk] = pw
            delta = round(pw - cw, 8)
            cmv = current_mv.get(tk, 0.0) if tk in held_set else 0.0
            pmv = _round_money(pw * nav)
            capital_change = _round_money(delta * nav)
            # action + provenance
            if sel is not None:
                action = sel["action"]
                reason_codes = list(sel.get("reason_codes") or [])
                src_rec = sel.get("source_hoc_recommendation")
                replacement_relationship = None
                if sel.get("replacement_of"):
                    replacement_relationship = {"role": "REPLACE_IN",
                                                "counterparty": sel["replacement_of"]}
                # RETAIN may actually be an INCREASE if the base weight lifts a thin incumbent.
                if action == ACT_RETAIN and tk in held_set:
                    if delta > pol["material_weight_delta"]:
                        action = ACT_INCREASE
                    elif delta < -pol["material_weight_delta"]:
                        action = ACT_REDUCE
                        reason_codes = sorted(set(reason_codes + ["EQUAL_WEIGHT_TRIM"]))
            else:
                pz = proposed_zero.get(tk) or {"action": ACT_EXIT,
                                               "reason_codes": ["UNCLASSIFIED_ZEROED"],
                                               "source_hoc_recommendation": None}
                action = pz["action"]
                reason_codes = list(pz.get("reason_codes") or [])
                src_rec = pz.get("source_hoc_recommendation")
                replacement_relationship = None
                if pz.get("replacement_for"):
                    replacement_relationship = {"role": "REPLACE_OUT",
                                                "counterparty": pz["replacement_for"]}
            if override is not None:
                action, reason_codes = _reoptimised_action(
                    ticker=tk, action=action, reason_codes=reason_codes, delta=delta,
                    proposed=pw, held=tk in held_set, policy=pol)
            urow = urows.get(tk) or {}
            # Release 50 - the instrument contract behind the row: from the universe /
            # frontier row, else from the held position, else the equity defaults.
            inst = instrument_fields(urow if urow else (pos_by.get(tk) or {}))
            allocations.append({
                "ticker": tk,
                "sector": sector_of.get(tk) or urow.get("sector") or "Unknown",
                "current_weight": _r(cw, 6),
                "proposed_weight": _r(pw, 6),
                "delta_weight": _r(delta, 6),
                "current_market_value": _round_money(cmv),
                "proposed_market_value": pmv,
                "capital_change": capital_change,
                "action": action,
                "source_hoc_recommendation": src_rec,
                "rank": urow.get("rank"),
                "score": _r(_pct(tk), 6),
                "combined_score": _r(_combined(tk), 6),
                "replacement_relationship": replacement_relationship,
                "reason_codes": reason_codes,
                "held": tk in held_set,
                **inst,
            })
        return allocations, proposed_weight

    def _measure(override: Optional[dict] = None) -> dict:
        """Turnover, signal, risk, constraints and the complete-target limits for ONE
        candidate target. Every number for the ideal target and for the repaired one
        is produced by THIS function, so the two are comparable by construction."""
        allocs, pweight = _allocation_rows(override)
        tno = _turnover_and_cost(allocations=allocs, nav=nav, policy=pol,
                                 proposed_zero=proposed_zero, selected=selected)
        sig = _signal_block(current_weight=current_weight, proposed_weight=pweight,
                            pct_fn=_pct, combined_fn=_combined, held_set=held_set,
                            two_way_turnover=tno["two_way_turnover"], policy=pol)
        rsk, rgaps = _risk_block(
            current_weight=current_weight, proposed_weight=pweight,
            sector_of=sector_of, aligned_returns=ic.get("aligned_returns") or {},
            hoc_reviews=hoc_reviews, urows=urows, held_set=held_set, policy=pol)
        cons, hard = _validate_constraints(
            allocations=allocs, proposed_weight=pweight, sector_of=sector_of,
            held_set=held_set, nav=nav, cash=cash, N=N, policy=pol)
        return {"allocations": allocs, "proposed_weight": pweight,
                "turnover": tno, "signal": sig, "risk": rsk, "risk_gaps": rgaps,
                "constraints": cons, "hard_violations": hard,
                "limits": evaluate_complete_target_limits(
                    turnover=tno, risk=rsk, policy=pol,
                    proposed_weight=pweight, instrument_meta=inst_meta)}

    measured = _measure()
    if measured["hard_violations"]:
        return _empty_result(
            pol, ic, STATE_BLOCKED,
            [{"code": "CONSTRAINT_VIOLATION",
              "violations": measured["hard_violations"]}],
            constraints=measured["constraints"])

    # --- Release 47: a breached portfolio limit RE-OPTIMISES the target ------ #
    # This is the whole release in one step. The complete target the passes above
    # built is the IDEAL one; when it breaches a portfolio-level limit the answer is
    # not "withhold and keep the incumbents", it is "solve the best FEASIBLE target
    # under that limit". Only when the feasible set is genuinely EMPTY does the old
    # fail-closed WITHHELD path remain.
    reoptimisation = _reoptimise_if_infeasible(
        measured=measured, current_weight=current_weight, held_set=held_set,
        universe_rows=universe_rows, urows=urows, sector_of=sector_of,
        hoc_reviews=hoc_reviews, pct_fn=_pct, nav=nav, policy=pol)
    if reoptimisation["applied"]:
        repaired = _measure(reoptimisation["best_feasible_target"])
        if repaired["hard_violations"]:
            reoptimisation["applied"] = False
            reoptimisation["abandoned_reason"] = "REPAIRED_TARGET_VIOLATES_CONSTRAINTS"
        else:
            measured = repaired
            reoptimisation["repaired_limits"] = repaired["limits"]

    allocations = measured["allocations"]
    proposed_weight = measured["proposed_weight"]
    turnover = measured["turnover"]
    signal = measured["signal"]
    risk = measured["risk"]
    risk_gaps = measured["risk_gaps"]
    constraints = measured["constraints"]

    # --- portfolio summary --------------------------------------------------- #
    invested_before = sum(current_weight.values()) * nav
    invested_after = sum(proposed_weight.values()) * nav
    proposed_cash = _round_money(nav - invested_after)
    proposed_holding_count = sum(1 for w in proposed_weight.values() if w > 0)
    proposed_groups = _cr._group_weights(_positive(proposed_weight), inst_meta)
    portfolio = {
        "nav": _round_money(nav),
        "current_cash": _round_money(cash),
        "proposed_cash": proposed_cash,
        "current_invested_value": _round_money(invested_before),
        "proposed_invested_value": _round_money(invested_after),
        "current_cash_weight": _r((cash / nav) if (cash is not None and nav) else None, 6),
        "proposed_cash_weight": _r(1.0 - sum(proposed_weight.values()), 6),
        "current_holding_count": len(held_set),
        "proposed_holding_count": proposed_holding_count,
        "target_position_count": N,
        "base_target_weight": _r(base_weight, 6),
        # --- Release 50: the same two portfolios by asset class / sleeve ------- #
        "current_allocation_by_asset_class": _cr.allocation_by(
            _positive(current_weight), inst_meta, "asset_class"),
        "proposed_allocation_by_asset_class": _cr.allocation_by(
            _positive(proposed_weight), inst_meta, "asset_class"),
        "current_allocation_by_sleeve": _cr.allocation_by(
            _positive(current_weight), inst_meta, "sleeve_id"),
        "proposed_allocation_by_sleeve": _cr.allocation_by(
            _positive(proposed_weight), inst_meta, "sleeve_id"),
        "asset_classes_in_target": sorted(proposed_groups["by_class"]),
        "proposed_gross_exposure": _r(sum(proposed_weight.values()), 6),
        "proposed_net_exposure": _r(sum(proposed_weight.values()), 6),
        "proposed_non_usd_exposure": _r(proposed_groups["non_usd"], 6),
        "proposed_collateral_weight": _r(proposed_groups["collateral"], 6),
        "non_equity_position_count_in_target": sum(
            1 for tk, w in proposed_weight.items() if w > 0
            and (inst_meta.get(tk) or {}).get("instrument_type", "CASH_EQUITY") != "CASH_EQUITY"),
        "forced_diversification": False,
    }

    # --- action counts ------------------------------------------------------- #
    action_counts = {a: 0 for a in ACTION_VOCAB}
    for row in allocations:
        action_counts[row["action"]] = action_counts.get(row["action"], 0) + 1

    # --- data gaps (carried + own) ------------------------------------------ #
    data_gaps = _collect_gaps(hoc_gaps=hoc_gaps, risk_gaps=risk_gaps)

    # --- complete-target portfolio limits (Release 29.3 + Release 47) -------- #
    # Judged on the ONE complete target this kernel publishes. Release 47 changed WHEN
    # a breach is decisive, never WHETHER the limits bind: the target is re-optimised
    # under the breached limit FIRST (see ``reoptimisation`` above), and only a target
    # that still breaches after re-optimisation - i.e. an empty feasible set - is
    # withheld. The old fail-closed path is intact; it is simply no longer the first
    # answer to a normal cap.
    complete_target_limits = measured["limits"]

    # --- Release 47: switching economics + the ONE authoritative outcome ----- #
    # Every economic input here is DELEGATED: the score comes from the signal block,
    # the turnover and the cost from the turnover block, the volatility from the risk
    # block. Release 47 owns the HURDLE and nothing else, so the proposal can never
    # report two different answers for the same quantity.
    switching = _cr.switching_economics(
        current_weight=current_weight, target_weight=_positive(proposed_weight),
        nav=nav,
        score_before=signal.get("score_before"),
        score_after=signal.get("score_after"),
        score_cost_hurdle=signal.get("score_cost_hurdle"),
        turnover_one_way=turnover.get("one_way_turnover"),
        transaction_cost=turnover.get("estimated_transaction_cost"),
        risk_before=risk.get("portfolio_volatility_before"),
        risk_after=risk.get("portfolio_volatility_after"),
        mandatory_exits=reoptimisation.get("mandatory_exits") or [], policy=pol)

    # DEGRADED when any non-by-design analytic gap is present; else READY. A
    # complete-target limit breach that SURVIVED re-optimisation outranks both.
    degraded = any(not g["by_design"] for g in data_gaps)
    proposal_state = STATE_DEGRADED if degraded else STATE_READY
    if complete_target_limits["withheld"]:
        proposal_state = STATE_WITHHELD

    verdict = _cr.decide_outcome(
        solution={"feasible": proposal_state != STATE_WITHHELD,
                  "best_feasible_target": _positive(proposed_weight),
                  "blockers": ([{"code": _cr.B_NO_FEASIBLE_PORTFOLIO,
                                 "kind": _cr.KIND_TRUE_BLOCKER,
                                 "violations": complete_target_limits["breaches"],
                                 "detail": ("The complete target still breaches a "
                                            "mandatory portfolio limit after "
                                            "constraint re-optimisation.")}]
                               if proposal_state == STATE_WITHHELD else []),
                  "constraints_that_reshaped": reoptimisation.get(
                      "constraints_that_reshaped") or []},
        economics=switching, true_blockers=[])

    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "eligible_market_date": eligible_date,
        "active_book_id": active_book_id,
        "proposal_state": proposal_state,
        "state_vocabulary": list(PROPOSAL_STATE_VOCAB),
        "action_vocabulary": list(ACTION_VOCAB),
        "policy": pol,
        "policy_version": ALLOCATION_POLICY_VERSION,
        "portfolio": portfolio,
        "action_counts": action_counts,
        "allocations": allocations,
        "turnover": turnover,
        "signal": signal,
        "risk": risk,
        "constraints": constraints,
        "complete_target_limits": complete_target_limits,
        # --- Release 47 ------------------------------------------------------ #
        "constraint_inventory": _cr.constraint_inventory(constraint_policy_projection(pol)),
        "constraint_reoptimization": reoptimisation,
        "switching_economics": switching,
        "reallocation_outcome": verdict,
        "outcome": verdict["outcome"],
        "outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
        # A proposal is offered for approval only when the state permits it AND the
        # switching economics say the change is worth paying for. HOLD_CURRENT_BOOK
        # is a decision the system has already taken; it is not outstanding work.
        "approvable": bool(proposal_state in APPROVABLE_STATES
                           and verdict["outcome"] == _cr.OUTCOME_PROPOSAL_READY),
        "withheld_reasons": complete_target_limits["breaches"],
        "data_gaps": data_gaps,
        "diagnostics": {
            "candidate_pool_size": len(candidate_pool),
            "candidates_used": sorted(used_candidates),
            "retained_count": sum(1 for r in allocations
                                  if r["action"] in (ACT_RETAIN, ACT_INCREASE, ACT_REDUCE)),
            "exited_count": action_counts.get(ACT_EXIT, 0),
            "replaced_out_count": action_counts.get(ACT_REPLACE_OUT, 0),
            "replaced_in_count": action_counts.get(ACT_REPLACE_IN, 0),
            "added_count": action_counts.get(ACT_ADD, 0),
            "sector_count": dict(sorted(sector_count.items())),
            "max_per_sector": max_per_sector,
        },
        "hoc_reference": {
            "assessment_hash": ic.get("hoc_assessment_hash"),
            "assessment_state": hoc_state,
            "recommendation_counts": ic.get("hoc_recommendation_counts") or {},
            "data_gaps": hoc_gaps,
        },
        "safety": _safety(),
        "provenance": _provenance(ic),
    }
    result["proposal_hash"] = stable_hash(result)
    return result


# --------------------------------------------------------------------------- #
# Turnover / cost
# --------------------------------------------------------------------------- #
def _turnover_and_cost(*, allocations: list, nav: float, policy: dict,
                       proposed_zero: dict, selected: dict) -> dict:
    gross_buys = 0.0
    gross_sells = 0.0
    two_way_weight = 0.0
    est_cost = 0.0
    per_instrument = False
    cost_rate = policy["cost_rate_per_side"]
    for row in allocations:
        cc = _f(row.get("capital_change")) or 0.0
        if cc > 0:
            gross_buys += cc
        elif cc < 0:
            gross_sells += -cc
        two_way_weight += abs(_f(row.get("delta_weight")) or 0.0)
        # Release 50 - a row that carries its instrument's declared cost (a future,
        # an FX spot) is charged at that rate; every other row at the desk rate.
        bps = _f(row.get("cost_bps_per_side"))
        if bps is not None:
            per_instrument = True
            est_cost += abs(cc) * (bps / 10000.0)
        else:
            est_cost += abs(cc) * cost_rate
    traded_notional = gross_buys + gross_sells
    # Switching cost = the transaction cost attributable to REPLACE_OUT/REPLACE_IN legs.
    switch_notional = 0.0
    replace_tickers = {row["ticker"] for row in allocations
                       if row["action"] in ("REPLACE_OUT", "REPLACE_IN")}
    for row in allocations:
        if row["ticker"] in replace_tickers:
            switch_notional += abs(_f(row.get("capital_change")) or 0.0)
    switching_costs = switch_notional * cost_rate
    return {
        "gross_buys": _round_money(gross_buys),
        "gross_sells": _round_money(gross_sells),
        "traded_notional": _round_money(traded_notional),
        "one_way_turnover": _r(two_way_weight / 2.0, 6),
        "two_way_turnover": _r(two_way_weight, 6),
        "estimated_transaction_cost": _round_money(est_cost),
        "switching_costs": _round_money(switching_costs),
        "cost_pct_nav": _r((est_cost / nav) if nav else None, 8),
        "cost_rate_per_side": cost_rate,
        "round_trip_cost_bps": policy["round_trip_cost_bps"],
        "per_instrument_cost_rates_applied": per_instrument,
        "cost_basis": ("TRADED_NOTIONAL_TIMES_PER_INSTRUMENT_SIDE_RATE" if per_instrument
                       else "TRADED_NOTIONAL_TIMES_PER_SIDE_RATE"),
    }


# --------------------------------------------------------------------------- #
# Signal (score before/after)
# --------------------------------------------------------------------------- #
def _weighted_score(weights: dict, score_fn) -> Optional[float]:
    total = sum(w for w in weights.values() if w and w > 0)
    if total <= 0:
        return None
    acc = 0.0
    any_score = False
    for tk, w in weights.items():
        if not w or w <= 0:
            continue
        s = score_fn(tk)
        if s is None:
            s = 0.0
        else:
            any_score = True
        acc += (w / total) * s
    return acc if any_score else None


def _signal_block(*, current_weight: dict, proposed_weight: dict, pct_fn, combined_fn,
                  held_set: set, two_way_turnover: Optional[float], policy: dict) -> dict:
    score_before = _weighted_score(current_weight, pct_fn)
    score_after = _weighted_score(proposed_weight, pct_fn)
    combined_before = _weighted_score(current_weight, combined_fn)
    combined_after = _weighted_score(proposed_weight, combined_fn)
    improvement = (score_after - score_before) if (score_before is not None
                                                   and score_after is not None) else None
    # Net-of-cost score hurdle: turnover fraction x per-round-trip percentile cost.
    cost_score = ((two_way_turnover or 0.0)
                  * policy["round_trip_cost_bps"] * policy["score_points_per_cost_bp"])
    net_improvement = (improvement - cost_score) if improvement is not None else None
    return {
        "score_before": _r(score_before, 6),
        "score_after": _r(score_after, 6),
        "score_improvement": _r(improvement, 6),
        "score_cost_hurdle": _r(cost_score, 6),
        "score_improvement_net_of_cost": _r(net_improvement, 6),
        "improves_after_cost": (bool(net_improvement > 0) if net_improvement is not None
                                else None),
        "combined_score_before": _r(combined_before, 6),
        "combined_score_after": _r(combined_after, 6),
        "score_basis": "combined_percentile",
        "improvement_basis": IMPROVEMENT_BASIS,
        # No validated forecast model exists -> expected return is never fabricated.
        "expected_return_before": None,
        "expected_return_after": None,
        "expected_return_improvement": None,
        "expected_return_state": EXPECTED_RETURN_STATE_NOT_CALIBRATED,
    }


# --------------------------------------------------------------------------- #
# Risk (concentration + volatility before/after)
# --------------------------------------------------------------------------- #
def _risk_block(*, current_weight: dict, proposed_weight: dict, sector_of: dict,
                aligned_returns: dict, hoc_reviews: list, urows: dict, held_set: set,
                policy: dict) -> tuple[dict, list]:
    gaps: list[str] = []
    # concentration
    hhi_before = _herfindahl(current_weight)
    hhi_after = _herfindahl(proposed_weight)
    largest_before = _largest_weight(current_weight)
    largest_after = _largest_weight(proposed_weight)
    sec_before = _sector_weights(
        [{"sector": sector_of.get(tk, "Unknown"), "w": w} for tk, w in current_weight.items()],
        "w")
    sec_after = _sector_weights(
        [{"sector": sector_of.get(tk) or (urows.get(tk) or {}).get("sector") or "Unknown",
          "w": w} for tk, w in proposed_weight.items()], "w")
    max_sec_before = _max_sector_weight(sec_before)
    max_sec_after = _max_sector_weight(sec_after)

    # portfolio volatility before / after (reuse the Slice-6 covariance kernel).
    vb = _portfolio_volatility(weights=current_weight, aligned_returns=aligned_returns,
                               policy=policy)
    va = _portfolio_volatility(weights=proposed_weight, aligned_returns=aligned_returns,
                               policy=policy)
    cov_floor = policy["min_volatility_coverage"]
    # ONE reconciled (state, value) decision per side so the published state can never
    # say AVAILABLE while the value is withheld (see VOL_STATE_* note above).
    vb_state, vol_before = _effective_volatility(vb, cov_floor)
    va_state, vol_after = _effective_volatility(va, cov_floor)
    if vol_before is None:
        gaps.append(VOLATILITY_BEFORE_GAP)
    if vol_after is None:
        gaps.append(VOLATILITY_AFTER_GAP)
    vol_delta = (vol_after - vol_before) if (vol_before is not None
                                             and vol_after is not None) else None

    # current-holding drawdown exposure (Slice 6 supplies per-holding 60d drawdown).
    dd_vals = [_f(r.get("drawdown_60d")) for r in hoc_reviews
               if r.get("ticker") in held_set and _f(r.get("drawdown_60d")) is not None]
    worst_dd = min(dd_vals) if dd_vals else None
    wsum = sum(current_weight.get(r.get("ticker"), 0.0) for r in hoc_reviews
               if r.get("ticker") in held_set and _f(r.get("drawdown_60d")) is not None)
    wavg_dd = None
    if wsum > 0:
        wavg_dd = sum((current_weight.get(r.get("ticker"), 0.0)) * _f(r.get("drawdown_60d"))
                      for r in hoc_reviews
                      if r.get("ticker") in held_set and _f(r.get("drawdown_60d")) is not None
                      ) / wsum

    # liquidity coverage (Slice 6 supplies per-holding liquidity state for held names;
    # proposed adds passed the universe liquidity floor at selection time).
    liq_known = sum(1 for r in hoc_reviews if r.get("ticker") in held_set
                    and r.get("liquidity_state") and r.get("liquidity_state") != "UNAVAILABLE")
    liq_total = len(held_set)

    return {
        "concentration_before": _r(hhi_before, 6),
        "concentration_after": _r(hhi_after, 6),
        "concentration_delta": _r((hhi_after - hhi_before)
                                  if (hhi_before is not None and hhi_after is not None)
                                  else None, 6),
        "largest_position_before": _r(largest_before, 6),
        "largest_position_after": _r(largest_after, 6),
        "sector_concentration_before": _r(max_sec_before, 6),
        "sector_concentration_after": _r(max_sec_after, 6),
        # Track B — missing classification reported as data quality, never counted
        # as the largest "sector" of the target.
        "unclassified_sector_weight_before": _r(_unclassified_weight(sec_before), 6),
        "unclassified_sector_weight_after": _r(_unclassified_weight(sec_after), 6),
        "sector_weights_before": {k: _r(v, 6) for k, v in sorted(sec_before.items())},
        "sector_weights_after": {k: _r(v, 6) for k, v in sorted(sec_after.items())},
        "portfolio_volatility_before": _r(vol_before, 6),
        "portfolio_volatility_after": _r(vol_after, 6),
        "portfolio_volatility_delta": _r(vol_delta, 6),
        "volatility_before_state": vb_state,
        "volatility_after_state": va_state,
        "volatility_before_coverage": _r(vb["covered_weight"], 4),
        "volatility_after_coverage": _r(va["covered_weight"], 4),
        "current_holding_worst_drawdown_60d": _r(worst_dd, 6),
        "current_holding_weighted_drawdown_60d": _r(wavg_dd, 6),
        "liquidity_coverage_known": liq_known,
        "liquidity_coverage_total": liq_total,
        "risk_data_gaps": sorted(set(gaps)),
    }, gaps


# --------------------------------------------------------------------------- #
# Constraint validation
# --------------------------------------------------------------------------- #
def _validate_constraints(*, allocations: list, proposed_weight: dict, sector_of: dict,
                          held_set: set, nav: float, cash: Optional[float], N: int,
                          policy: dict) -> tuple[dict, list]:
    violations: list[dict] = []
    cap = policy["max_name_weight"]
    sector_cap = policy["sector_cap_fraction"]
    tol = policy["weight_reconcile_tol"]

    # long-only, name cap, no allocation to an EXIT / REPLACE_OUT
    name_cap_ok = True
    long_only_ok = True
    no_exit_alloc_ok = True
    for row in allocations:
        pw = _f(row.get("proposed_weight")) or 0.0
        if pw < -tol:
            long_only_ok = False
            violations.append({"code": "NEGATIVE_WEIGHT", "ticker": row["ticker"],
                               "weight": pw})
        if pw > cap + 1e-9:
            name_cap_ok = False
            violations.append({"code": "NAME_CAP_EXCEEDED", "ticker": row["ticker"],
                               "weight": pw, "cap": cap})
        if row["action"] in ("EXIT", "REPLACE_OUT") and pw > tol:
            no_exit_alloc_ok = False
            violations.append({"code": "EXIT_HAS_NONZERO_WEIGHT", "ticker": row["ticker"],
                               "weight": pw})

    # no duplicate ticker
    tickers = [row["ticker"] for row in allocations]
    no_duplicate_ok = len(tickers) == len(set(tickers))
    if not no_duplicate_ok:
        violations.append({"code": "DUPLICATE_TICKER"})

    # sector cap (proposed)
    sec_after = {}
    for tk, w in proposed_weight.items():
        sec = sector_of.get(tk, "Unknown")
        sec_after[sec] = sec_after.get(sec, 0.0) + (w or 0.0)
    sector_cap_ok = True
    for sec, w in sec_after.items():
        if sec != "Unknown" and w > sector_cap + 1e-6:
            sector_cap_ok = False
            violations.append({"code": "SECTOR_CAP_EXCEEDED", "sector": sec, "weight": w,
                               "cap": sector_cap})

    # position count
    proposed_count = sum(1 for w in proposed_weight.values() if w > tol)
    position_count_ok = proposed_count <= N
    if not position_count_ok:
        violations.append({"code": "POSITION_COUNT_EXCEEDED", "count": proposed_count,
                           "limit": N})

    # capital reconciliation (no capital creation): invested + cash == NAV
    total_weight = sum(w for w in proposed_weight.values())
    reconciles_ok = total_weight <= 1.0 + tol
    proposed_cash = nav * (1.0 - total_weight)
    if not reconciles_ok or proposed_cash < -abs(nav) * tol - 0.01:
        reconciles_ok = False
        violations.append({"code": "CAPITAL_RECONCILIATION_FAILED",
                           "total_weight": total_weight, "proposed_cash": proposed_cash})

    constraints = {
        "name_cap_ok": name_cap_ok,
        "sector_cap_ok": sector_cap_ok,
        "position_count_ok": position_count_ok,
        "long_only_ok": long_only_ok,
        "no_duplicate_ok": no_duplicate_ok,
        "no_exit_allocation_ok": no_exit_alloc_ok,
        "reconciles_ok": reconciles_ok,
        "all_ok": not violations,
        "violations": violations,
        "name_weight_cap": cap,
        "sector_cap_fraction": sector_cap,
        "target_position_count": N,
    }
    return constraints, violations


# --------------------------------------------------------------------------- #
# Complete-target portfolio limits (Release 29.3 — MOVED from the reassessment)
# --------------------------------------------------------------------------- #
def evaluate_complete_target_limits(*, turnover: dict, risk: dict,
                                    policy: dict, proposed_weight: Optional[dict] = None,
                                    instrument_meta: Optional[dict] = None) -> dict:
    """Judge the portfolio-level limits that require knowing the COMPLETE target.

    Pure arithmetic over values this kernel has already computed on the complete
    target — nothing is recomputed and no economics are re-derived. A limit is never
    relaxed to force a proposal into existence.

    Release 47 changed what a breach DOES, not what a limit IS. A breach now sends the
    target to ``engine.constrained_reallocation`` to be re-solved UNDER that limit; the
    repaired target is then re-measured and judged here again. Only a target that still
    breaches after re-optimisation is withheld (``STATE_WITHHELD``), which is the
    honest statement that the feasible set is empty rather than merely different from
    the ideal. ``engine.portfolio_reassessment`` publishes pre-proposal estimates of
    the same quantities but is explicitly non-binding, so each limit is applied here
    exactly once and transaction cost is never counted twice.
    """
    breaches: list[dict] = []
    one_way = _f(turnover.get("one_way_turnover"))
    budget = _f(policy.get("max_one_way_turnover"))
    if one_way is not None and budget is not None and one_way > budget + 1e-12:
        breaches.append({
            "code": CT_TURNOVER_BUDGET, "value": _r(one_way, 6), "limit": _r(budget, 6),
            "object": "COMPLETE_TARGET",
            "detail": ("The complete target requires %.4f one-way turnover against a "
                       "%.4f budget." % (one_way, budget))})

    hhi_b, hhi_a = _f(risk.get("concentration_before")), _f(risk.get("concentration_after"))
    max_inc = _f(policy.get("max_concentration_increase"))
    if hhi_b is not None and hhi_a is not None and max_inc is not None:
        delta = hhi_a - hhi_b
        if delta > max_inc + 1e-12:
            breaches.append({
                "code": CT_CONCENTRATION, "value": _r(delta, 6), "limit": _r(max_inc, 6),
                "object": "COMPLETE_TARGET",
                "detail": ("The complete target raises the Herfindahl index by %.6f "
                           "against a %.6f limit." % (delta, max_inc))})
            breaches.append({
                "code": CT_RISK_DETERIORATION, "value": _r(delta, 6),
                "limit": _r(max_inc, 6), "object": "COMPLETE_TARGET",
                "detail": "Concentration deterioration on the complete target."})

    sec_b, sec_a = (_f(risk.get("sector_concentration_before")),
                    _f(risk.get("sector_concentration_after")))
    cap = _f(policy.get("sector_cap_fraction"))
    if sec_a is not None and cap is not None:
        worsens = (sec_b is None or sec_a > sec_b + 1e-9)
        if sec_a > cap + 1e-12 and (worsens or not policy.get(
                "sector_cap_deterioration_only", True)):
            breaches.append({
                "code": CT_SECTOR_CAP, "value": _r(sec_a, 6), "limit": _r(cap, 6),
                "object": "COMPLETE_TARGET",
                "detail": ("The complete target's largest sector weight is %.4f against "
                           "a %.4f cap." % (sec_a, cap))})

    # Release 50 - the cross-asset limits, judged on the complete target through the
    # ONE constraint owner's own verification (never a second definition here).
    cross_asset = None
    if proposed_weight is not None and instrument_meta is not None:
        pw = {tk: w for tk, w in proposed_weight.items() if (_f(w) or 0.0) > 0}
        g = _cr._group_weights(pw, instrument_meta)
        cpol = constraint_policy_projection(policy)
        ca_breaches = []
        for ac, v in sorted(g["by_class"].items()):
            lim = _cr._asset_class_cap(dict(_cr.default_policy(), **cpol), ac)
            if v > lim + 1e-9:
                ca_breaches.append({"code": _cr.C_ASSET_CLASS_CAP, "asset_class": ac,
                                    "value": _r(v, 6), "limit": _r(lim, 6)})
        for sl, v in sorted(g["by_sleeve"].items()):
            lim = _cr._sleeve_cap(dict(_cr.default_policy(), **cpol), sl)
            if v > lim + 1e-9:
                ca_breaches.append({"code": _cr.C_SLEEVE_CAP, "sleeve_id": sl,
                                    "value": _r(v, 6), "limit": _r(lim, 6)})
        ccy = _f(policy.get("non_usd_currency_cap"))
        if ccy is not None and g["non_usd"] > ccy + 1e-9:
            ca_breaches.append({"code": _cr.C_CURRENCY_CAP, "value": _r(g["non_usd"], 6),
                                "limit": _r(ccy, 6)})
        coll = _f(policy.get("collateral_cap_fraction"))
        if coll is not None and g["collateral"] > coll + 1e-9:
            ca_breaches.append({"code": _cr.C_COLLATERAL_CAP, "value": _r(g["collateral"], 6),
                                "limit": _r(coll, 6)})
        gross = sum(pw.values())
        mg = _f(policy.get("max_gross_exposure"))
        if mg is not None and gross > mg + 1e-9:
            ca_breaches.append({"code": _cr.C_GROSS_EXPOSURE, "value": _r(gross, 6),
                                "limit": _r(mg, 6)})
        cross_asset = {"breaches": ca_breaches,
                       "group_weights": {"by_class": {k: _r(v, 6) for k, v in g["by_class"].items()},
                                         "by_sleeve": {k: _r(v, 6) for k, v in g["by_sleeve"].items()},
                                         "non_usd": _r(g["non_usd"], 6),
                                         "collateral": _r(g["collateral"], 6)}}
        if ca_breaches:
            breaches.append({
                "code": CT_CROSS_ASSET_CAP, "object": "COMPLETE_TARGET",
                "value": [b["code"] for b in ca_breaches],
                "limit": "see cross_asset.breaches",
                "detail": ("The complete target breaches %d cross-asset limit(s): %s."
                           % (len(ca_breaches), ", ".join(b["code"] for b in ca_breaches)))})

    return {
        "owner": CALCULATION_OWNER,
        "object": "COMPLETE_TARGET",
        "ask_gate_owner": ASK_GATE_OWNER,
        "constraint_codes": list(COMPLETE_TARGET_CONSTRAINT_CODES),
        "evaluated_once": True,
        "cross_asset": cross_asset,
        "one_way_turnover": _r(one_way, 6),
        "one_way_turnover_budget": _r(budget, 6),
        "concentration_before": _r(hhi_b, 6),
        "concentration_after": _r(hhi_a, 6),
        "max_concentration_increase": _r(max_inc, 6),
        "sector_concentration_before": _r(sec_b, 6),
        "sector_concentration_after": _r(sec_a, 6),
        "sector_cap_fraction": _r(cap, 6),
        "breaches": breaches,
        "all_ok": not breaches,
        "withheld": bool(breaches),
        "withheld_codes": sorted({b["code"] for b in breaches}),
    }


# --------------------------------------------------------------------------- #
# Gaps
# --------------------------------------------------------------------------- #
def _collect_gaps(*, hoc_gaps: list, risk_gaps: list) -> list[dict]:
    gaps: list[dict] = []
    # Expected return is a permanent, by-design gap (no validated forecast model).
    gaps.append({"code": EXPECTED_RETURN_GAP, "affects": AFFECTS_EXPECTED_RETURN,
                 "by_design": True,
                 "detail": "No validated expected-return model exists; the proposal "
                           "compares signal scores, never a fabricated dollar return."})
    for g in sorted(set(hoc_gaps)):
        gaps.append({"code": "HOC_%s" % g,
                     "affects": _HOC_GAP_AFFECTS.get(g, AFFECTS_INFORMATIONAL),
                     "by_design": False,
                     "detail": "Carried forward from the Slice 6 Holding "
                               "Opportunity-Cost assessment."})
    for g in sorted(set(risk_gaps)):
        gaps.append({"code": g, "affects": AFFECTS_RISK, "by_design": False,
                     "detail": "Portfolio-risk analytic unavailable for the current inputs."})
    return gaps


# --------------------------------------------------------------------------- #
# Safety / provenance / empty result
# --------------------------------------------------------------------------- #
def _safety() -> dict:
    return {
        "read_only": True,
        "paper_only": True,
        "manual_review": True,
        "preview_only": True,
        "review_only": True,
        "created_operational_target": False,
        "confirmed_alpha_target": False,
        "created_target_weights_authority": False,
        "created_order_plan": False,
        "created_orders": False,
        "created_fills": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
        "performed_broker_execution": False,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "called_provider": False,
        "called_prediction": False,
        "promoted_model": False,
        "recalibrated_model": False,
        "automatic_promotion_allowed": False,
        "automation_off": True,
        "safety_badges": ["PAPER ONLY", "REVIEW ONLY", "NO ORDERS", "NO LIVE ORDERS",
                          "NO AUTOMATION", "MANUAL REVIEW"],
    }


def _provenance(ic: dict) -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "portfolio_source": "api.portfolio_state (positions / weights / NAV / cash / sectors)",
        "opportunity_cost_source": "api.holding_opportunity_cost (per-holding recommendation "
                                   "+ replacement + switching cost)",
        "scoring_source": "api.universe_scoring (rank / score / eligibility / adv_dollar)",
        "risk_primitive_source": "engine.holding_opportunity_cost.compute_risk_contributions (reused)",
        "transaction_cost_source": "api.paper_trading_desk.COST_RATE_PER_SIDE (reused)",
        "construction_policy_source": "api.multi_horizon_engine (book size / name / sector / liquidity)",
        "eligible_market_date": ic.get("eligible_market_date"),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "hoc_assessment_hash": ic.get("hoc_assessment_hash"),
        "allocation_policy_version": ALLOCATION_POLICY_VERSION,
        "cost_policy_version": COST_POLICY_VERSION,
    }


def _empty_result(pol: dict, ic: dict, state: str, blockers: list,
                  constraints: Optional[dict] = None) -> dict:
    """A readable BLOCKED / NO_ACTIVE_BOOK result (never raises; degrade-safe)."""
    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "proposal_state": state,
        "state_vocabulary": list(PROPOSAL_STATE_VOCAB),
        "action_vocabulary": list(ACTION_VOCAB),
        "policy": pol,
        "policy_version": ALLOCATION_POLICY_VERSION,
        "portfolio": {},
        "action_counts": {a: 0 for a in ACTION_VOCAB},
        "allocations": [],
        "turnover": {},
        "signal": {"expected_return_state": EXPECTED_RETURN_STATE_NOT_CALIBRATED,
                   "expected_return_before": None, "expected_return_after": None,
                   "expected_return_improvement": None},
        "risk": {},
        "constraints": constraints or {},
        "data_gaps": [{"code": EXPECTED_RETURN_GAP, "affects": AFFECTS_EXPECTED_RETURN,
                       "by_design": True}],
        "diagnostics": {},
        "hoc_reference": {"assessment_hash": ic.get("hoc_assessment_hash"),
                          "assessment_state": ic.get("hoc_assessment_state")},
        "complete_target_limits": {},
        # Release 47: a BLOCKED / NO_ACTIVE_BOOK result is a genuine TRUE BLOCKER -
        # a required input is missing, so no trustworthy decision exists. It is NOT
        # a constraint breach, and the outcome says exactly that.
        "constraint_inventory": _cr.constraint_inventory(constraint_policy_projection(pol)),
        "constraint_reoptimization": {
            "owner": _cr.CALCULATION_OWNER, "applied": False,
            "ideal_target_was_feasible": None, "breached_limits": [],
            "constraints_that_reshaped": [], "constraint_adjustments": [],
            "mandatory_exits": [],
            "doc": "No target was constructed, so nothing could be re-optimised."},
        "switching_economics": {},
        "reallocation_outcome": {
            "owner": _cr.CALCULATION_OWNER,
            "outcome": _cr.OUTCOME_TRUE_BLOCKER,
            "outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
            "headline": "PORTFOLIO DECISION BLOCKED",
            "reason_codes": sorted({b.get("code") for b in (blockers or [])
                                    if b.get("code")}),
            "feasible_target_exists": False,
            "feasible_alternative_was_computed": False,
            "requires_manual_approval": False,
            "authorises_execution": False, "creates_orders": False},
        "outcome": _cr.OUTCOME_TRUE_BLOCKER,
        "outcome_vocabulary": list(_cr.OUTCOME_VOCAB),
        "approvable": False,
        "withheld_reasons": [],
        "blockers": blockers,
        "safety": _safety(),
        "provenance": _provenance(ic),
    }
    result["proposal_hash"] = stable_hash(result)
    return result
