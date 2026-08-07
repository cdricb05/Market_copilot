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
PROPOSAL_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_NO_ACTIVE_BOOK)

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
        # --- classification / reconciliation tolerances ---------------------- #
        "material_weight_delta": 1.0e-4,                          # new: RETAIN vs INCREASE/REDUCE band
        "weight_reconcile_tol": 1.0e-6,                          # new
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
    return max(sector_weights.values()) if sector_weights else None


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
    for p in positions:
        tk = p.get("ticker")
        if tk:
            sector_of[tk] = p.get("sector") or (urows.get(tk) or {}).get("sector") or "Unknown"

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
    candidate_pool = []
    for r in sorted((r for r in universe_rows
                     if r.get("ticker") and r.get("rank") is not None),
                    key=lambda r: (r.get("rank"), r.get("ticker"))):
        tk = r.get("ticker")
        if tk in held_set:
            continue
        if not r.get("eligible", True):
            continue
        if r.get("rank") is not None and r.get("rank") > pol["candidate_rank_max"]:
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
    all_tickers = sorted(held_set | set(selected.keys()))
    allocations: list[dict] = []
    proposed_weight: dict[str, float] = {}
    for tk in all_tickers:
        cw = current_weight.get(tk, 0.0)
        sel = selected.get(tk)
        if sel is not None:
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
        urow = urows.get(tk) or {}
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
        })

    # --- turnover / cost ----------------------------------------------------- #
    turnover = _turnover_and_cost(allocations=allocations, nav=nav, policy=pol,
                                  proposed_zero=proposed_zero, selected=selected)

    # --- signal (score before/after) ---------------------------------------- #
    signal = _signal_block(current_weight=current_weight, proposed_weight=proposed_weight,
                           pct_fn=_pct, combined_fn=_combined, held_set=held_set,
                           two_way_turnover=turnover["two_way_turnover"], policy=pol)

    # --- risk (concentration + volatility before/after) ---------------------- #
    risk, risk_gaps = _risk_block(
        current_weight=current_weight, proposed_weight=proposed_weight, sector_of=sector_of,
        aligned_returns=ic.get("aligned_returns") or {}, hoc_reviews=hoc_reviews,
        urows=urows, held_set=held_set, policy=pol)

    # --- constraint validation ---------------------------------------------- #
    constraints, hard_violations = _validate_constraints(
        allocations=allocations, proposed_weight=proposed_weight, sector_of=sector_of,
        held_set=held_set, nav=nav, cash=cash, N=N, policy=pol)
    if hard_violations:
        return _empty_result(pol, ic, STATE_BLOCKED,
                             [{"code": "CONSTRAINT_VIOLATION", "violations": hard_violations}],
                             constraints=constraints)

    # --- portfolio summary --------------------------------------------------- #
    invested_before = sum(current_weight.values()) * nav
    invested_after = sum(proposed_weight.values()) * nav
    proposed_cash = _round_money(nav - invested_after)
    proposed_holding_count = sum(1 for w in proposed_weight.values() if w > 0)
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
    }

    # --- action counts ------------------------------------------------------- #
    action_counts = {a: 0 for a in ACTION_VOCAB}
    for row in allocations:
        action_counts[row["action"]] = action_counts.get(row["action"], 0) + 1

    # --- data gaps (carried + own) ------------------------------------------ #
    data_gaps = _collect_gaps(hoc_gaps=hoc_gaps, risk_gaps=risk_gaps)

    # DEGRADED when any non-by-design analytic gap is present; else READY.
    degraded = any(not g["by_design"] for g in data_gaps)
    proposal_state = STATE_DEGRADED if degraded else STATE_READY

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
    for row in allocations:
        cc = _f(row.get("capital_change")) or 0.0
        if cc > 0:
            gross_buys += cc
        elif cc < 0:
            gross_sells += -cc
        two_way_weight += abs(_f(row.get("delta_weight")) or 0.0)
    traded_notional = gross_buys + gross_sells
    cost_rate = policy["cost_rate_per_side"]
    est_cost = traded_notional * cost_rate
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
    vol_before = vb["volatility"] if (vb["state"] == "AVAILABLE"
                                      and vb["covered_weight"] >= cov_floor) else None
    vol_after = va["volatility"] if (va["state"] == "AVAILABLE"
                                     and va["covered_weight"] >= cov_floor) else None
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
        "sector_weights_before": {k: _r(v, 6) for k, v in sorted(sec_before.items())},
        "sector_weights_after": {k: _r(v, 6) for k, v in sorted(sec_after.items())},
        "portfolio_volatility_before": _r(vol_before, 6),
        "portfolio_volatility_after": _r(vol_after, 6),
        "portfolio_volatility_delta": _r(vol_delta, 6),
        "volatility_before_state": vb["state"],
        "volatility_after_state": va["state"],
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
        "blockers": blockers,
        "safety": _safety(),
        "provenance": _provenance(ic),
    }
    result["proposal_hash"] = stable_hash(result)
    return result
