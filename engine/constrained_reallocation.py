r"""Release 47 - CONSTRAINT-RESPECTING ACTIVE REALLOCATION (pure calculation kernel).

The defect this kernel exists to remove
---------------------------------------
Before Release 47 the portfolio pipeline could reach a dead end::

    unconstrained target -> constraint breach -> WITHHELD -> keep the current book

A sector cap, a name cap, a risk-contribution limit or a turnover budget is a
NORMAL portfolio constraint. A normal constraint must CHANGE THE SOLUTION. It must
not freeze the portfolio, and it must never hand the incumbent holdings a victory
they did not earn: "we could not compute a compliant target" is not a finding that
the current book is the best use of capital.

Release 47 replaces the dead end with::

    unconstrained zero-base target
        -> apply the mandatory constraints
        -> SOLVE the best FEASIBLE constrained target      (this kernel)
        -> compare it against the current book             (this kernel)
        -> price the switch: risk, cost, liquidity, turnover
        -> PROPOSAL_READY / HOLD_CURRENT_BOOK / TRUE_BLOCKER

What this kernel owns
---------------------
  1. THE CONSTRAINT INVENTORY - every mandatory limit, and for each one whether it
     RESHAPES the solution or is a genuine TRUE BLOCKER. The classification is
     declared here once, in data, so no caller can quietly re-classify a cap as a
     blocker (:func:`constraint_inventory`).
  2. THE FEASIBLE RE-OPTIMISATION - given an ideal target that breaches one or more
     limits, produce the best feasible target: cap what must be capped, redistribute
     the released capital to the next-best eligible opportunities, and when the
     turnover budget binds, keep the highest-value trades that fit inside it
     (:func:`solve_feasible_target`).
  3. THE SWITCHING ECONOMICS - the explicit, frozen, deterministic hurdle the best
     feasible target must clear against the CURRENT book after transition cost
     (:func:`switching_economics`).
  4. THE THREE AUTHORITATIVE OUTCOMES (:func:`decide_outcome`).

What this kernel is NOT
-----------------------
  * It is NOT a second allocator. The ideal target is produced by the canonical
    owners (``engine.zero_base_allocator`` / ``engine.reallocation_proposal``); this
    kernel only REPAIRS an infeasible one and reports what it changed.
  * It computes no rank, no score, no covariance, no expected return and no cost
    RATE. Every such value arrives from the owner that already owns it.
  * It performs NO I/O - no file, database, network, provider or prediction access -
    creates no order, no fill and no target authority, and mutates nothing.

Ownership rule for incumbency
-----------------------------
The canonical question is *"if all investable capital were cash now, what feasible
portfolio should we own?"*. A current holding therefore receives NO investment
privilege here. It receives exactly one legitimate advantage, priced explicitly:
moving it costs money (transition cost, liquidity, settlement). That advantage lives
in :func:`switching_economics` and nowhere else - never inside the feasibility solve,
which cannot see which names are held except to measure distance from them.

Purity: stdlib only, deterministic, every tie broken by ticker.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

SCHEMA_VERSION = "constrained_reallocation.v1"
INPUT_SCHEMA_VERSION = "constrained_reallocation.input.v1"
CONSTRAINT_POLICY_VERSION = "constrained_reallocation_constraint_policy.v1"
SWITCHING_POLICY_VERSION = "constrained_reallocation_switching_policy.v1"
CALCULATION_OWNER = "engine.constrained_reallocation"
PHASE = "R47"

# --------------------------------------------------------------------------- #
# The three authoritative outcomes.
# --------------------------------------------------------------------------- #
#: A feasible target exists and is sufficiently better than the current portfolio
#: after risk, cost, liquidity and turnover. It is still REVIEW ONLY: it authorises
#: nothing until an operator approves it.
OUTCOME_PROPOSAL_READY = "PROPOSAL_READY"
#: A feasible alternative exists, is fully computed and fully visible, but its
#: expected improvement does not justify paying to switch. This is an ECONOMIC
#: conclusion about the alternative - never "we could not compute one".
OUTCOME_HOLD_CURRENT_BOOK = "HOLD_CURRENT_BOOK"
#: A trustworthy portfolio decision cannot be made at all.
OUTCOME_TRUE_BLOCKER = "TRUE_BLOCKER"
OUTCOME_VOCAB = (OUTCOME_PROPOSAL_READY, OUTCOME_HOLD_CURRENT_BOOK,
                 OUTCOME_TRUE_BLOCKER)

# --------------------------------------------------------------------------- #
# Constraint classification. A constraint is one of exactly two kinds, and the
# kind is DATA declared here - never an inference made at a call site.
# --------------------------------------------------------------------------- #
#: The constraint changes the solution. The optimiser re-solves under it and the
#: portfolio decision continues. This is the normal case.
KIND_RESHAPES = "RESHAPES_THE_SOLUTION"
#: The constraint means no trustworthy decision exists. Fail closed.
KIND_TRUE_BLOCKER = "TRUE_BLOCKER"
CONSTRAINT_KIND_VOCAB = (KIND_RESHAPES, KIND_TRUE_BLOCKER)

# --- Reshaping constraint codes --------------------------------------------- #
C_ELIGIBLE_UNIVERSE = "ELIGIBLE_UNIVERSE_ONLY"
C_LONG_ONLY = "LONG_ONLY"
C_GROSS_EXPOSURE = "GROSS_EXPOSURE_CAP"
C_NAME_CAP = "NAME_WEIGHT_CAP"
C_SECTOR_CAP = "SECTOR_WEIGHT_CAP"
C_RISK_CONTRIBUTION = "RISK_CONTRIBUTION_CAP"
C_LIQUIDITY_PARTICIPATION = "LIQUIDITY_PARTICIPATION_CAP"
C_LIQUIDITY_FLOOR = "LIQUIDITY_ADV_FLOOR"
C_CONCENTRATION = "CONCENTRATION_INCREASE_LIMIT"
C_TURNOVER_BUDGET = "TURNOVER_BUDGET"
C_MIN_POSITION = "MIN_POSITION_WEIGHT"
C_MAX_POSITIONS = "MAX_POSITION_COUNT"
C_CASH_BOUNDS = "CASH_BOUNDS"
# --- Release 50: cross-asset reshaping constraints ---------------------------- #
C_ASSET_CLASS_CAP = "ASSET_CLASS_WEIGHT_CAP"
C_SLEEVE_CAP = "SLEEVE_WEIGHT_CAP"
C_CURRENCY_CAP = "CURRENCY_EXPOSURE_CAP"
C_COLLATERAL_CAP = "COLLATERAL_USAGE_CAP"
C_UNIT_GRANULARITY = "UNIT_GRANULARITY_AT_NAV"

# --- Track B: missing sector classification ----------------------------------- #
#: The token a candidate row carries when its sector could not be established.
#: It is a DATA-QUALITY STATE, never an economic sector: names with no
#: classification share no evidenced common industry factor, so the SECTOR cap
#: neither aggregates them into one fabricated bucket nor charges them against a
#: shared 25% budget. Each unclassified name still faces every per-name limit
#: (name cap, liquidity, risk contribution) individually, and the unclassified
#: weight is reported by :func:`verify_feasibility`. Kept in sync with the same
#: literal in ``engine.holding_opportunity_cost.UNCLASSIFIED_SECTOR`` (this
#: kernel is deliberately pure stdlib and imports nothing from the repo).
UNCLASSIFIED_SECTOR = "Unknown"
RESHAPING_CONSTRAINT_CODES = (
    C_ELIGIBLE_UNIVERSE, C_LONG_ONLY, C_GROSS_EXPOSURE, C_NAME_CAP, C_SECTOR_CAP,
    C_RISK_CONTRIBUTION, C_LIQUIDITY_PARTICIPATION, C_LIQUIDITY_FLOOR,
    C_CONCENTRATION, C_TURNOVER_BUDGET, C_MIN_POSITION, C_MAX_POSITIONS,
    C_CASH_BOUNDS,
    C_ASSET_CLASS_CAP, C_SLEEVE_CAP, C_CURRENCY_CAP, C_COLLATERAL_CAP,
    C_UNIT_GRANULARITY)

#: Release 50 - instrument metadata a candidate row MAY carry. A row without it is
#: a US cash equity (fully paid, USD, the pre-R50 contract), so an equity-only
#: universe solves exactly as before.
_DEFAULT_ASSET_CLASS = "US_EQUITY"
_DEFAULT_SLEEVE = "us_equity_fundamental_momentum_50_50_v1"
_DEFAULT_CURRENCY = "USD"
_CASH_CLASS = "CASH"
_DEFAULT_NON_EQUITY_KEY = "DEFAULT_NON_EQUITY"

# --- True-blocker condition codes (the ONLY admissible blockers) ------------- #
B_STALE_MARKET_DATA = "CRITICAL_STALE_OR_MISSING_MARKET_DATA"
B_POINT_IN_TIME = "POINT_IN_TIME_INTEGRITY_FAILURE"
B_NAV_UNRECONCILED = "NAV_ACCOUNTING_UNRECONCILED"
B_IMPOSSIBLE_LIQUIDITY = "IMPOSSIBLE_LIQUIDITY_OR_CAPACITY"
B_NO_FEASIBLE_PORTFOLIO = "NO_FEASIBLE_PORTFOLIO_UNDER_MANDATORY_CONSTRAINTS"
B_AUTHORIZATION_MISSING = "REQUIRED_MANUAL_AUTHORIZATION_MISSING"
TRUE_BLOCKER_CODES = (B_STALE_MARKET_DATA, B_POINT_IN_TIME, B_NAV_UNRECONCILED,
                      B_IMPOSSIBLE_LIQUIDITY, B_NO_FEASIBLE_PORTFOLIO,
                      B_AUTHORIZATION_MISSING)

# --- What the re-optimiser did, in one vocabulary ---------------------------- #
ADJ_CAPPED = "CAPPED_TO_LIMIT"
ADJ_EXCLUDED = "EXCLUDED_INELIGIBLE_OR_ILLIQUID"
ADJ_REDISTRIBUTED = "REDISTRIBUTED_TO_NEXT_BEST"
ADJ_DEFERRED_TO_CASH = "RELEASED_TO_CASH_NO_FEASIBLE_DESTINATION"
ADJ_DUST_DROPPED = "DROPPED_BELOW_MINIMUM_POSITION"
ADJ_TRADES_DEFERRED = "TRADES_DEFERRED_TO_FIT_TURNOVER_BUDGET"
ADJ_DILUTED = "DILUTED_TO_MEET_CONCENTRATION_LIMIT"
ADJUSTMENT_VOCAB = (ADJ_CAPPED, ADJ_EXCLUDED, ADJ_REDISTRIBUTED,
                    ADJ_DEFERRED_TO_CASH, ADJ_DUST_DROPPED, ADJ_TRADES_DEFERRED,
                    ADJ_DILUTED)

#: A held name is never given an investment advantage by this kernel. The single
#: legitimate advantage - that moving it costs money - is priced in
#: :func:`switching_economics` and nowhere else.
INCUMBENCY_POLICY = "NO_INVESTMENT_PRIVILEGE_ONLY_PRICED_TRANSITION_COST"

#: The improvement unit. There is still NO validated expected-return model, so an
#: improvement is a SIGNAL-SCORE comparison in percentile points, exactly as the
#: Slice-6 / Slice-7 / Stage-20 owners already express it. A dollar expected return
#: is never fabricated here.
IMPROVEMENT_BASIS = ("PORTFOLIO_COMBINED_PERCENTILE_UPLIFT_NET_OF_MODELED_"
                     "TRANSITION_COST")
EXPECTED_RETURN_STATE_NOT_CALIBRATED = "NOT_CALIBRATED"

_TOL = 1.0e-9


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
def default_policy() -> dict[str, Any]:
    """The single explicit, versioned Release-47 constraint + switching policy.

    Values marked ``reused`` MIRROR canonical limits owned elsewhere; the caller
    (``engine.reallocation_proposal``) overrides them with the live values so no
    threshold is silently forked. Values marked ``new`` are genuinely-new Release-47
    thresholds, declared here once, returned in the payload and folded into the
    deterministic hash.

    None of these is tuned on realised outcomes. The switching hurdle in particular
    is frozen BEFORE any decision is measured (see :data:`SWITCHING_POLICY_VERSION`):
    a hurdle fitted to what happened afterwards is not a hurdle.
    """
    return {
        "constraint_policy_version": CONSTRAINT_POLICY_VERSION,
        "switching_policy_version": SWITCHING_POLICY_VERSION,
        # --- reused mandatory portfolio limits ------------------------------- #
        "max_name_weight": 0.10,            # reused: eng.MAX_INDIVIDUAL_WEIGHT
        "sector_cap_fraction": 0.25,        # reused: eng.SECTOR_CAP_FRACTION
        "min_adv_dollar": 1.0e7,            # reused: eng.MIN_ADV_DOLLAR
        "cost_rate_per_side": 0.00125,      # reused: desk.COST_RATE_PER_SIDE
        "round_trip_cost_bps": 25.0,        # reused: 2 * desk.COST_BPS_PER_SIDE
        "score_points_per_cost_bp": 0.001,  # reused: bps -> percentile hurdle
        "target_position_count": 25,        # reused: eng.BOOK_SIZES[0]
        "max_one_way_turnover": 0.35,       # reused: Slice-7 turnover budget
        "max_concentration_increase": 0.02,  # reused: Slice-7 HHI deterioration
        "material_weight_delta": 1.0e-4,    # reused: Slice-7 materiality band
        # --- genuinely-new Release-47 limits --------------------------------- #
        # A position may not exceed this multiple of the name's average dollar
        # volume. Liquidity is a HARD bound on the SIZE of a position, never a veto
        # on the portfolio: a name that cannot carry its ideal weight carries the
        # weight it can, and the remainder is redistributed.
        "max_adv_participation": 1.0,                       # new
        # No single name may account for more than this share of total portfolio
        # risk. Breaching it REDUCES that name and redistributes the released
        # capital - it never rejects the portfolio.
        "max_name_risk_contribution": 0.25,                 # new
        # A weight the book would never actually hold is not a target.
        "min_position_weight": 0.005,                       # new: 0.5% of NAV
        # Cash is a real asset choice, so the bounds are wide by declaration: the
        # allocator may hold anything from fully invested to fully in cash, and a
        # NULL (all-cash) result is a valid answer, not a failure.
        "min_cash_weight": 0.0,                             # new
        "max_cash_weight": 1.0,                             # new
        # THE SWITCHING HURDLE. Expressed in the same percentile points as the
        # per-name and portfolio hurdles the system already uses, so a basket of
        # individually-rejected switches can never pass in aggregate. Frozen.
        "min_switching_net_improvement": 0.05,              # new (== Stage-20 bar)
        # Below this one-way turnover a change is operationally indistinguishable
        # from noise and is treated as HOLD regardless of score arithmetic.
        "min_switching_turnover": 1.0e-4,                   # new
        # Iteration bounds for the deterministic repair loops.
        "max_repair_rounds": 200,                           # new
        # --- Release 50: cross-asset limits (all RESHAPING) ------------------- #
        # Long-only, and the whole book's notional exposure may not exceed NAV: a
        # future enters at its notional, so a fully-paid book plus futures stays
        # inside 100% and every unit of margin is always covered by free cash.
        "max_gross_exposure": 1.0,                          # R50
        "max_net_exposure": 1.0,                            # R50 (== gross; long-only)
        # Per asset class / per sleeve. The approved equity sleeve and cash are
        # uncapped (1.0) so an equity-only book is unchanged; a non-equity class or
        # sleeve is capped at a declared quarter of NAV unless declared otherwise.
        "asset_class_weight_caps": {"US_EQUITY": 1.0, "CASH": 1.0,
                                    _DEFAULT_NON_EQUITY_KEY: 0.25},   # R50
        "sleeve_weight_caps": {_DEFAULT_SLEEVE: 1.0, "cash_usd": 1.0,
                               _DEFAULT_NON_EQUITY_KEY: 0.25},        # R50
        # Unhedged non-USD notional exposure across every instrument.
        "non_usd_currency_cap": 0.20,                       # R50
        # Initial margin encumbered by futures, as a share of NAV.
        "collateral_cap_fraction": 0.25,                    # R50
    }


def _asset_class_cap(policy: dict, asset_class: str) -> float:
    caps = policy.get("asset_class_weight_caps") or {}
    if asset_class in caps:
        return float(caps[asset_class])
    return float(caps.get(_DEFAULT_NON_EQUITY_KEY, 1.0))


def _sleeve_cap(policy: dict, sleeve_id: str) -> float:
    caps = policy.get("sleeve_weight_caps") or {}
    if sleeve_id in caps:
        return float(caps[sleeve_id])
    return float(caps.get(_DEFAULT_NON_EQUITY_KEY, 1.0))


def candidate_meta(candidates: list) -> dict:
    """``{ticker: {asset_class, sleeve_id, currency, capital_usage_ratio,
    is_future, cost_rate}}`` with the pre-R50 equity defaults for rows that carry
    no instrument metadata."""
    meta: dict[str, dict] = {}
    for c in candidates or []:
        tk = c.get("ticker")
        if not tk:
            continue
        it = c.get("instrument_type") or "CASH_EQUITY"
        ratio = _f(c.get("capital_usage_ratio"))
        meta[tk] = {
            "asset_class": c.get("asset_class") or _DEFAULT_ASSET_CLASS,
            "sleeve_id": c.get("sleeve_id") or _DEFAULT_SLEEVE,
            "currency": str(c.get("currency") or _DEFAULT_CURRENCY).upper(),
            "instrument_type": it,
            "is_future": it == "FUTURE",
            "capital_usage_ratio": (ratio if ratio is not None else (0.0 if it == "FUTURE" else 1.0)),
            "cost_rate": (_f(c.get("cost_bps_per_side")) / 10000.0
                          if _f(c.get("cost_bps_per_side")) is not None else None),
            "unit_notional_usd": _f(c.get("unit_notional_usd")),
        }
    return meta


def _group_weights(w: dict, meta: dict) -> dict:
    """Weight totals by asset class, sleeve, non-USD currency and futures collateral."""
    by_class: dict[str, float] = {}
    by_sleeve: dict[str, float] = {}
    non_usd = 0.0
    collateral = 0.0
    for tk, v in w.items():
        if v <= 0:
            continue
        m = meta.get(tk) or {}
        ac = m.get("asset_class") or _DEFAULT_ASSET_CLASS
        sl = m.get("sleeve_id") or _DEFAULT_SLEEVE
        by_class[ac] = by_class.get(ac, 0.0) + v
        by_sleeve[sl] = by_sleeve.get(sl, 0.0) + v
        if (m.get("currency") or _DEFAULT_CURRENCY) != _DEFAULT_CURRENCY:
            non_usd += v
        if m.get("is_future"):
            collateral += v * float(m.get("capital_usage_ratio") or 0.0)
    return {"by_class": by_class, "by_sleeve": by_sleeve, "non_usd": non_usd,
            "collateral": collateral}


#: Public names for the ONE cross-asset grouping / cap definitions (the proposal
#: kernel and the zero-base allocator reuse these; they never redefine them).
def group_weights(w: dict, meta: dict) -> dict:
    return _group_weights(w, meta)


def asset_class_cap(policy: dict, asset_class: str) -> float:
    return _asset_class_cap(policy, asset_class)


def sleeve_cap(policy: dict, sleeve_id: str) -> float:
    return _sleeve_cap(policy, sleeve_id)


def cross_asset_relevant(meta: dict, policy: dict) -> bool:
    """True when ANY cross-asset limit can bind: a non-equity, non-USD, non-default-
    sleeve or futures candidate, or an equity-side cap below 1.0. When False every
    room is provably infinite (the gross budget the callers already apply bounds the
    single equity class), so a solver may skip the room test and run EXACTLY as it
    did before Release 50 - the performance of the equity-only solve is preserved
    by construction, not by tolerance."""
    if _asset_class_cap(policy, _DEFAULT_ASSET_CLASS) < 1.0 or \
            _sleeve_cap(policy, _DEFAULT_SLEEVE) < 1.0:
        return True
    for m in (meta or {}).values():
        m = m or {}
        if (m.get("asset_class") or _DEFAULT_ASSET_CLASS) != _DEFAULT_ASSET_CLASS:
            return True
        if (m.get("sleeve_id") or _DEFAULT_SLEEVE) != _DEFAULT_SLEEVE:
            return True
        if (m.get("currency") or _DEFAULT_CURRENCY) != _DEFAULT_CURRENCY:
            return True
        if m.get("is_future"):
            return True
    return False


def empty_groups() -> dict:
    """The group totals of an empty weight vector (for incremental maintenance)."""
    return {"by_class": {}, "by_sleeve": {}, "non_usd": 0.0, "collateral": 0.0}


def group_add(groups: dict, meta: dict, tk: str, delta: float) -> dict:
    """Move ``delta`` weight of ``tk`` into the group totals - the SAME definition
    as ``_group_weights`` applied one instrument at a time, so a solver that builds
    a vector incrementally keeps the totals in O(1) per step instead of re-summing
    the whole vector per candidate."""
    if delta == 0:
        return groups
    m = meta.get(tk) or {}
    ac = m.get("asset_class") or _DEFAULT_ASSET_CLASS
    sl = m.get("sleeve_id") or _DEFAULT_SLEEVE
    groups["by_class"][ac] = groups["by_class"].get(ac, 0.0) + delta
    groups["by_sleeve"][sl] = groups["by_sleeve"].get(sl, 0.0) + delta
    if (m.get("currency") or _DEFAULT_CURRENCY) != _DEFAULT_CURRENCY:
        groups["non_usd"] += delta
    if m.get("is_future"):
        groups["collateral"] += delta * float(m.get("capital_usage_ratio") or 0.0)
    return groups


def cross_asset_room(tk: str, w: dict, *, meta: dict, policy: dict,
                     exclude_same_group_as: Optional[str] = None,
                     groups: Optional[dict] = None) -> float:
    """How much MORE weight ``tk`` may take before a cross-asset limit binds
    (asset class, sleeve, non-USD currency, futures collateral). Infinite for a
    US cash equity under the default caps, so the equity solve is untouched.
    ``groups`` may carry the caller's already-known group totals for ``w`` (see
    ``group_add``); otherwise they are summed here."""
    m = meta.get(tk) or {}
    ac = m.get("asset_class") or _DEFAULT_ASSET_CLASS
    sl = m.get("sleeve_id") or _DEFAULT_SLEEVE
    donor = meta.get(exclude_same_group_as) or {} if exclude_same_group_as else {}
    g = groups if groups is not None else _group_weights(w, meta)
    room = float("inf")
    if donor.get("asset_class") != ac:
        room = min(room, _asset_class_cap(policy, ac) - g["by_class"].get(ac, 0.0))
    if donor.get("sleeve_id") != sl:
        room = min(room, _sleeve_cap(policy, sl) - g["by_sleeve"].get(sl, 0.0))
    if (m.get("currency") or _DEFAULT_CURRENCY) != _DEFAULT_CURRENCY and \
            (donor.get("currency") or _DEFAULT_CURRENCY) == _DEFAULT_CURRENCY:
        room = min(room, float(policy.get("non_usd_currency_cap", 1.0)) - g["non_usd"])
    ratio = float(m.get("capital_usage_ratio") or 0.0)
    if m.get("is_future") and ratio > 0 and not donor.get("is_future"):
        room = min(room, (float(policy.get("collateral_cap_fraction", 1.0)) - g["collateral"]) / ratio)
    return max(0.0, room)


# --------------------------------------------------------------------------- #
# Small numeric helpers (stdlib only, deterministic)
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _pos(weights: dict) -> dict:
    return {tk: w for tk, w in (weights or {}).items() if (_f(w) or 0.0) > 0.0}


_VOLATILE_KEYS = frozenset({"generated_at", "evaluated_at", "built_at",
                            "decision_timestamp", "solution_hash"})


def _strip_volatile(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in sorted(obj.items())
                if k not in _VOLATILE_KEYS}
    if isinstance(obj, (list, tuple)):
        return [_strip_volatile(v) for v in obj]
    return obj


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(_strip_volatile(obj), sort_keys=True, separators=(",", ":"),
                   default=str).encode("utf-8")).hexdigest()


def one_way_turnover(current: dict, target: dict) -> float:
    """The canonical one-way portfolio distance, defined exactly once here.

    ``0.5 * sum |target_i - current_i|`` over the union of both weight vectors -
    the same definition ``engine.reallocation_proposal`` and
    ``engine.zero_base_allocator`` already publish. It is restated (not forked)
    because the turnover-budget solve needs it on intermediate vectors that no
    other owner ever sees.
    """
    names = set(current or {}) | set(target or {})
    return 0.5 * sum(abs((_f((target or {}).get(tk)) or 0.0)
                         - (_f((current or {}).get(tk)) or 0.0))
                     for tk in names)


def herfindahl(weights: dict) -> Optional[float]:
    ws = [(_f(w) or 0.0) for w in (weights or {}).values()]
    return sum(w * w for w in ws) if ws else None


# --------------------------------------------------------------------------- #
# The constraint inventory
# --------------------------------------------------------------------------- #
def constraint_inventory(policy: Optional[dict] = None) -> dict:
    """Every mandatory limit, its limit value, its owner and - the point of the
    whole release - whether it RESHAPES the solution or genuinely BLOCKS a decision.

    A sector cap, a concentration cap, a name cap, a risk-contribution limit, a
    turnover budget and a liquidity participation cap are all RESHAPING. None of
    them may, on its own, be reported as a portfolio blocker. That statement is data
    here, and the tests assert the data rather than a prose promise.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    rows = [
        {"code": C_ELIGIBLE_UNIVERSE, "kind": KIND_RESHAPES, "limit": None,
         "limit_units": None, "object": "PER_NAME",
         "reshape_action": "Held names outside the eligible universe are exited "
                           "(mandatory) and their capital is redistributed.",
         "owner": "api.universe_scoring"},
        {"code": C_LONG_ONLY, "kind": KIND_RESHAPES, "limit": 0.0,
         "limit_units": "WEIGHT", "object": "PER_NAME",
         "reshape_action": "Negative weight is clipped to zero.",
         "owner": CALCULATION_OWNER},
        {"code": C_GROSS_EXPOSURE, "kind": KIND_RESHAPES, "limit": 1.0,
         "limit_units": "WEIGHT", "object": "PORTFOLIO",
         "reshape_action": "Gross exposure above 100% is scaled back; the residual "
                           "is cash.",
         "owner": CALCULATION_OWNER},
        {"code": C_NAME_CAP, "kind": KIND_RESHAPES,
         "limit": pol["max_name_weight"], "limit_units": "WEIGHT",
         "object": "PER_NAME",
         "reshape_action": "The name is capped and the excess is redistributed to "
                           "the next-best eligible opportunities.",
         "owner": "api.multi_horizon_engine"},
        {"code": C_SECTOR_CAP, "kind": KIND_RESHAPES,
         "limit": pol["sector_cap_fraction"], "limit_units": "WEIGHT",
         "object": "PER_SECTOR",
         "reshape_action": "The sector is capped by trimming its weakest names; "
                           "the excess is redistributed outside that sector.",
         "owner": "api.multi_horizon_engine"},
        {"code": C_RISK_CONTRIBUTION, "kind": KIND_RESHAPES,
         "limit": pol["max_name_risk_contribution"],
         "limit_units": "SHARE_OF_PORTFOLIO_RISK", "object": "PER_NAME",
         "reshape_action": "The position is reduced to the compliant level and the "
                           "released capital is redistributed.",
         "owner": "engine.holding_opportunity_cost"},
        {"code": C_LIQUIDITY_PARTICIPATION, "kind": KIND_RESHAPES,
         "limit": pol["max_adv_participation"], "limit_units": "MULTIPLE_OF_ADV",
         "object": "PER_NAME",
         "reshape_action": "The position is capped at what the book could actually "
                           "trade; the remainder goes to the next feasible name.",
         "owner": "api.universe_scoring"},
        {"code": C_LIQUIDITY_FLOOR, "kind": KIND_RESHAPES,
         "limit": pol["min_adv_dollar"], "limit_units": "USD_ADV",
         "object": "PER_NAME",
         "reshape_action": "An illiquid candidate is skipped and the NEXT feasible "
                           "candidate is used - the proposal is not abandoned.",
         "owner": "api.universe_scoring"},
        {"code": C_CONCENTRATION, "kind": KIND_RESHAPES,
         "limit": pol["max_concentration_increase"], "limit_units": "HHI_DELTA",
         "object": "COMPLETE_TARGET",
         "reshape_action": "Weight is moved from the largest positions into the "
                           "next-best names until the limit is met.",
         "owner": "engine.reallocation_proposal"},
        {"code": C_TURNOVER_BUDGET, "kind": KIND_RESHAPES,
         "limit": pol["max_one_way_turnover"], "limit_units": "ONE_WAY_TURNOVER",
         "object": "COMPLETE_TARGET",
         "reshape_action": "The best feasible target INSIDE the budget is solved: "
                           "constraint-mandated trades first, then the discretionary "
                           "trades with the highest score improvement per unit of "
                           "turnover.",
         "owner": "engine.reallocation_proposal"},
        {"code": C_MIN_POSITION, "kind": KIND_RESHAPES,
         "limit": pol["min_position_weight"], "limit_units": "WEIGHT",
         "object": "PER_NAME",
         "reshape_action": "Dust is dropped to cash rather than proposed.",
         "owner": CALCULATION_OWNER},
        {"code": C_MAX_POSITIONS, "kind": KIND_RESHAPES,
         "limit": pol["target_position_count"], "limit_units": "POSITIONS",
         "object": "COMPLETE_TARGET",
         "reshape_action": "The weakest names beyond the limit are dropped and their "
                           "capital is redistributed inside the limit.",
         "owner": "api.multi_horizon_engine"},
        {"code": C_CASH_BOUNDS, "kind": KIND_RESHAPES,
         "limit": [pol["min_cash_weight"], pol["max_cash_weight"]],
         "limit_units": "WEIGHT", "object": "PORTFOLIO",
         "reshape_action": "Cash is a real asset choice; the bound reshapes how much "
                           "capital may remain unallocated.",
         "owner": CALCULATION_OWNER},
        # --- Release 50: cross-asset limits ------------------------------------ #
        {"code": C_ASSET_CLASS_CAP, "kind": KIND_RESHAPES,
         "limit": dict(pol.get("asset_class_weight_caps") or {}), "limit_units": "WEIGHT",
         "object": "PER_ASSET_CLASS",
         "reshape_action": "An over-cap asset class is trimmed from its weakest name "
                           "upward; the excess is redistributed outside that class "
                           "(cash when nothing has room). Never a forced allocation.",
         "owner": CALCULATION_OWNER},
        {"code": C_SLEEVE_CAP, "kind": KIND_RESHAPES,
         "limit": dict(pol.get("sleeve_weight_caps") or {}), "limit_units": "WEIGHT",
         "object": "PER_SLEEVE",
         "reshape_action": "An over-cap sleeve is trimmed from its weakest name upward; "
                           "the excess is redistributed outside that sleeve.",
         "owner": CALCULATION_OWNER},
        {"code": C_CURRENCY_CAP, "kind": KIND_RESHAPES,
         "limit": pol.get("non_usd_currency_cap"), "limit_units": "WEIGHT",
         "object": "PORTFOLIO",
         "reshape_action": "Unhedged non-USD notional above the cap is trimmed from "
                           "the weakest non-USD name upward and redistributed.",
         "owner": CALCULATION_OWNER},
        {"code": C_COLLATERAL_CAP, "kind": KIND_RESHAPES,
         "limit": pol.get("collateral_cap_fraction"), "limit_units": "SHARE_OF_NAV",
         "object": "PORTFOLIO",
         "reshape_action": "Futures whose initial margin would encumber more cash than "
                           "the cap are trimmed from the weakest upward; the released "
                           "exposure is redistributed.",
         "owner": CALCULATION_OWNER},
        {"code": C_UNIT_GRANULARITY, "kind": KIND_RESHAPES,
         "limit": pol["max_name_weight"], "limit_units": "WEIGHT",
         "object": "PER_NAME",
         "reshape_action": "An instrument whose ONE unit is larger than the name cap at "
                           "this NAV (a full-size contract on a small book) cannot be "
                           "held at any feasible weight; it is skipped and the next "
                           "feasible candidate is used.",
         "owner": CALCULATION_OWNER},
    ]
    blockers = [
        {"code": B_STALE_MARKET_DATA, "kind": KIND_TRUE_BLOCKER,
         "detail": "Prices/marks required to value the book or size a trade are "
                   "missing or stale beyond policy."},
        {"code": B_POINT_IN_TIME, "kind": KIND_TRUE_BLOCKER,
         "detail": "The evidence bound to the decision no longer describes the "
                   "portfolio (e.g. an unpropagated corporate action)."},
        {"code": B_NAV_UNRECONCILED, "kind": KIND_TRUE_BLOCKER,
         "detail": "Holdings, cash and NAV do not reconcile, so no target can be "
                   "sized honestly."},
        {"code": B_IMPOSSIBLE_LIQUIDITY, "kind": KIND_TRUE_BLOCKER,
         "detail": "No eligible name can carry any feasible weight at this NAV."},
        {"code": B_NO_FEASIBLE_PORTFOLIO, "kind": KIND_TRUE_BLOCKER,
         "detail": "The mandatory constraints admit no portfolio at all - the "
                   "feasible set is empty, not merely different from the ideal."},
        {"code": B_AUTHORIZATION_MISSING, "kind": KIND_TRUE_BLOCKER,
         "detail": "Manual authorization required at the execution boundary is "
                   "absent. This blocks EXECUTION, never the decision itself."},
    ]
    return {
        "owner": CALCULATION_OWNER,
        "constraint_policy_version": pol["constraint_policy_version"],
        "kind_vocabulary": list(CONSTRAINT_KIND_VOCAB),
        "constraints": rows,
        "reshaping_codes": [r["code"] for r in rows],
        "true_blocker_conditions": blockers,
        "true_blocker_codes": [b["code"] for b in blockers],
        "reshaping_count": len(rows),
        "true_blocker_count": len(blockers),
        "doc": ("A normal portfolio constraint reshapes the solution. Only the "
                "conditions listed under true_blocker_conditions can stop a "
                "portfolio decision, and a sector / concentration / name / risk / "
                "turnover / liquidity limit is never one of them."),
        "incumbency_policy": INCUMBENCY_POLICY,
    }


def is_true_blocker(code: str) -> bool:
    """Whether a code names a genuine blocker. Anything not declared a true blocker
    is a reshaping constraint - and an UNKNOWN code is NOT silently promoted to a
    blocker, because promoting the unknown is exactly how a cap became a freeze."""
    return str(code) in TRUE_BLOCKER_CODES


# --------------------------------------------------------------------------- #
# Per-name feasible caps
# --------------------------------------------------------------------------- #
def name_caps(*, candidates: list, nav: Optional[float],
              policy: dict) -> tuple[dict, dict]:
    """The binding upper weight bound per eligible name, and WHICH limit binds.

    The bound is the tightest of the name cap, the liquidity participation cap and
    the ADV floor. An illiquid name gets a cap of zero - it is skipped, and the next
    feasible candidate is used. That is a reshape, not a veto.
    """
    caps: dict[str, float] = {}
    binding: dict[str, str] = {}
    part = float(policy["max_adv_participation"])
    floor = float(policy["min_adv_dollar"])
    navv = _f(nav) or 0.0
    for c in candidates or []:
        tk = c.get("ticker")
        if not tk:
            continue
        cap = float(policy["max_name_weight"])
        which = C_NAME_CAP
        adv = _f(c.get("adv_dollar"))
        # Release 50 - unit granularity: one unit larger than the name cap at this
        # NAV means NO feasible weight exists for the instrument (a $108k treasury
        # contract on a $99k book). A reshape, never a veto of the portfolio.
        unit = _f(c.get("unit_notional_usd"))
        if unit is not None and navv > 0 and unit > cap * navv + _TOL:
            cap, which = 0.0, C_UNIT_GRANULARITY
        elif adv is not None and adv < floor:
            cap, which = 0.0, C_LIQUIDITY_FLOOR
        elif adv is not None and navv > 0:
            liq = max(0.0, part * adv / navv)
            if liq < cap:
                cap, which = liq, C_LIQUIDITY_PARTICIPATION
        caps[tk] = cap
        binding[tk] = which
    return caps, binding


# --------------------------------------------------------------------------- #
# The feasible re-optimisation
# --------------------------------------------------------------------------- #
def solve_feasible_target(*, current_weight: dict, ideal_weight: dict,
                          candidates: list, nav: Optional[float],
                          risk_contributions: Optional[dict] = None,
                          policy: Optional[dict] = None) -> dict:
    """Solve the best FEASIBLE constrained target, starting from the ideal one.

    ``candidates`` is the eligible universe as the scoring owner publishes it:
    ``{ticker, sector, adv_dollar, score, rank}``. ``score`` is the combined
    percentile the scoring owner already computed - this kernel derives no score.

    The result is a complete target that satisfies every mandatory constraint, plus
    an explicit ledger of every adjustment made to get there. When the feasible set
    is genuinely empty the result says so with :data:`B_NO_FEASIBLE_PORTFOLIO`; a
    breached cap never produces that verdict.

    Deterministic: candidates are ordered by (score desc, rank asc, ticker asc) and
    every tie is broken by ticker.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)

    cands = [dict(c) for c in (candidates or []) if c.get("ticker")]
    sector_of = {c["ticker"]: (c.get("sector") or "Unknown") for c in cands}
    score_of = {c["ticker"]: (_f(c.get("score")) or 0.0) for c in cands}
    caps, cap_binding = name_caps(candidates=cands, nav=nav, policy=pol)
    # Release 50 - instrument metadata (equity defaults for rows without it) and
    # the cross-asset room function every placement below respects.
    meta = candidate_meta(cands)
    # When no cross-asset limit can bind (the equity-only book under the default
    # caps) every room is provably infinite and the solve runs exactly as before.
    xa_relevant = cross_asset_relevant(meta, pol)

    def _room(tk: str, w_now: dict, donor: Optional[str] = None) -> float:
        if not xa_relevant:
            return float("inf")
        return cross_asset_room(tk, w_now, meta=meta, policy=pol, exclude_same_group_as=donor)
    #: The EFFECTIVE per-name ceiling. It starts at the declared cap and is tightened
    #: by each repair that reduces a specific name, so the redistribution step can
    #: never hand capital straight back to a position a constraint just cut. Without
    #: this the risk-contribution cap and the position-count limit would be cosmetic.
    eff_caps = dict(caps)
    current = {tk: (_f(w) or 0.0) for tk, w in (current_weight or {}).items()}
    ideal = {tk: (_f(w) or 0.0) for tk, w in (ideal_weight or {}).items()}
    contrib = {tk: (_f(v) or 0.0)
               for tk, v in (risk_contributions or {}).items()}

    adjustments: list[dict] = []

    def _note(code: str, action: str, **kw) -> None:
        row = {"constraint": code, "action": action,
               "kind": (KIND_TRUE_BLOCKER if is_true_blocker(code)
                        else KIND_RESHAPES)}
        row.update({k: v for k, v in kw.items() if v is not None})
        adjustments.append(row)

    #: The value ordering used everywhere below. Higher score first; a name with no
    #: score sorts last, never in the middle by accident.
    def _order(tickers) -> list:
        return sorted(tickers, key=lambda tk: (-(score_of.get(tk, 0.0)), tk))

    # --- 0. long-only + eligible universe -------------------------------------- #
    w: dict[str, float] = {}
    released = 0.0
    for tk, v in sorted(ideal.items()):
        if tk not in caps:
            if v > _TOL:
                released += v
                _note(C_ELIGIBLE_UNIVERSE, ADJ_EXCLUDED, ticker=tk,
                      released_weight=_r(v, 8),
                      detail="not in the eligible candidate set")
            continue
        if v < 0:
            _note(C_LONG_ONLY, ADJ_CAPPED, ticker=tk, before=_r(v, 8), after=0.0)
            v = 0.0
        w[tk] = v

    # Mandatory exits: held names the target cannot contain. Their exit is a
    # CONSTRAINT, not an economic choice, so it is never weighed against a hurdle.
    mandatory_exits = sorted(tk for tk, v in current.items()
                             if v > _TOL and (tk not in caps or caps[tk] <= 0.0))

    # --- 1. per-name cap (name / liquidity participation / ADV floor) ---------- #
    for tk in sorted(w):
        cap = caps.get(tk, 0.0)
        if w[tk] > cap + _TOL:
            excess = w[tk] - cap
            released += excess
            code = cap_binding.get(tk, C_NAME_CAP)
            _note(code, ADJ_EXCLUDED if cap <= 0 else ADJ_CAPPED, ticker=tk,
                  before=_r(w[tk], 8), after=_r(cap, 8),
                  limit=_r(cap, 8), released_weight=_r(excess, 8))
            w[tk] = cap

    # --- 2. sector cap: trim the weakest names in an over-cap sector ----------- #
    sector_cap = float(pol["sector_cap_fraction"])
    by_sector: dict[str, list] = {}
    for tk in w:
        by_sector.setdefault(sector_of.get(tk, UNCLASSIFIED_SECTOR), []).append(tk)
    for sec in sorted(by_sector):
        # Track B — unclassified names form no sector: trimming them as one
        # fabricated bucket would enforce a correlation claim nobody evidenced.
        if sec == UNCLASSIFIED_SECTOR:
            continue
        total = sum(w[tk] for tk in by_sector[sec])
        if total <= sector_cap + _TOL:
            continue
        excess = total - sector_cap
        # Trim from the weakest name upward, so the sector keeps its best ideas.
        for tk in reversed(_order(by_sector[sec])):
            if excess <= _TOL:
                break
            take = min(w[tk], excess)
            if take <= 0:
                continue
            w[tk] -= take
            excess -= take
            released += take
            _note(C_SECTOR_CAP, ADJ_CAPPED, ticker=tk, sector=sec,
                  before=_r(w[tk] + take, 8), after=_r(w[tk], 8),
                  limit=_r(sector_cap, 8), released_weight=_r(take, 8))

    # --- 2b. Release 50: asset-class / sleeve / currency / collateral caps ------ #
    # Each is a normal, RESHAPING limit: the over-cap group is trimmed from its
    # weakest name upward and the excess is redistributed outside it.
    def _trim_group(code: str, members: list, excess: float, **kw) -> float:
        nonlocal released
        for tk in reversed(_order(members)):
            if excess <= _TOL:
                break
            take = min(w[tk], excess)
            if take <= 0:
                continue
            w[tk] -= take
            excess -= take
            released += take
            eff_caps[tk] = min(eff_caps.get(tk, w[tk] + take), w[tk])
            _note(code, ADJ_CAPPED, ticker=tk, before=_r(w[tk] + take, 8),
                  after=_r(w[tk], 8), released_weight=_r(take, 8), **kw)
        return excess

    groups = _group_weights(w, meta)
    for ac in sorted(groups["by_class"]):
        cap_ac = _asset_class_cap(pol, ac)
        total = groups["by_class"][ac]
        if total > cap_ac + _TOL:
            _trim_group(C_ASSET_CLASS_CAP,
                        [t for t in w if (meta.get(t) or {}).get("asset_class", _DEFAULT_ASSET_CLASS) == ac],
                        total - cap_ac, asset_class=ac, limit=_r(cap_ac, 8))
    groups = _group_weights(w, meta)
    for sl in sorted(groups["by_sleeve"]):
        cap_sl = _sleeve_cap(pol, sl)
        total = groups["by_sleeve"][sl]
        if total > cap_sl + _TOL:
            _trim_group(C_SLEEVE_CAP,
                        [t for t in w if (meta.get(t) or {}).get("sleeve_id", _DEFAULT_SLEEVE) == sl],
                        total - cap_sl, sleeve_id=sl, limit=_r(cap_sl, 8))
    groups = _group_weights(w, meta)
    ccy_cap = float(pol.get("non_usd_currency_cap", 1.0))
    if groups["non_usd"] > ccy_cap + _TOL:
        _trim_group(C_CURRENCY_CAP,
                    [t for t in w if (meta.get(t) or {}).get("currency", _DEFAULT_CURRENCY) != _DEFAULT_CURRENCY],
                    groups["non_usd"] - ccy_cap, limit=_r(ccy_cap, 8))
    groups = _group_weights(w, meta)
    coll_cap = float(pol.get("collateral_cap_fraction", 1.0))
    if groups["collateral"] > coll_cap + _TOL:
        futs = [t for t in w if (meta.get(t) or {}).get("is_future")]
        excess_coll = groups["collateral"] - coll_cap
        for tk in reversed(_order(futs)):
            if excess_coll <= _TOL:
                break
            ratio = float((meta.get(tk) or {}).get("capital_usage_ratio") or 0.0)
            if ratio <= 0 or w[tk] <= 0:
                continue
            take = min(w[tk], excess_coll / ratio)
            w[tk] -= take
            excess_coll -= take * ratio
            released += take
            eff_caps[tk] = min(eff_caps.get(tk, w[tk] + take), w[tk])
            _note(C_COLLATERAL_CAP, ADJ_CAPPED, ticker=tk, before=_r(w[tk] + take, 8),
                  after=_r(w[tk], 8), released_weight=_r(take, 8), limit=_r(coll_cap, 8),
                  capital_usage_ratio=_r(ratio, 6))

    # --- 3. risk-contribution cap --------------------------------------------- #
    rc_cap = _f(pol.get("max_name_risk_contribution"))
    if rc_cap is not None and contrib:
        for tk in sorted(w):
            share = contrib.get(tk)
            if share is None or share <= rc_cap + _TOL or w[tk] <= 0:
                continue
            # Risk contribution is (to first order) proportional to weight, so the
            # compliant weight is the current weight scaled by cap/share. The exact
            # risk is re-measured by the canonical risk owner afterwards; this is
            # the deterministic REDUCTION, not a second risk model.
            target = w[tk] * (rc_cap / share)
            take = w[tk] - target
            if take <= _TOL:
                continue
            released += take
            _note(C_RISK_CONTRIBUTION, ADJ_CAPPED, ticker=tk,
                  before=_r(w[tk], 8), after=_r(target, 8),
                  risk_contribution=_r(share, 6), limit=_r(rc_cap, 6),
                  released_weight=_r(take, 8))
            w[tk] = target
            eff_caps[tk] = target

    # --- 4. maximum position count -------------------------------------------- #
    max_positions = int(pol["target_position_count"])
    live = [tk for tk in w if w[tk] > _TOL]
    if max_positions > 0 and len(live) > max_positions:
        for tk in _order(live)[max_positions:]:
            released += w[tk]
            _note(C_MAX_POSITIONS, ADJ_CAPPED, ticker=tk, before=_r(w[tk], 8),
                  after=0.0, limit=max_positions, released_weight=_r(w[tk], 8))
            w[tk] = 0.0
            eff_caps[tk] = 0.0

    # --- 5. gross exposure ----------------------------------------------------- #
    gross = sum(w.values())
    max_gross = float(pol.get("max_gross_exposure", 1.0))
    if gross > max_gross + _TOL:
        # Scale back from the weakest name upward; the excess simply never existed.
        excess = gross - max_gross
        for tk in reversed(_order([t for t in w if w[t] > 0])):
            if excess <= _TOL:
                break
            take = min(w[tk], excess)
            w[tk] -= take
            excess -= take
            _note(C_GROSS_EXPOSURE, ADJ_CAPPED, ticker=tk,
                  before=_r(w[tk] + take, 8), after=_r(w[tk], 8), limit=max_gross)

    # --- 6. redistribute the released capital to the next-best opportunities --- #
    redistributed = _redistribute(w, released=released, order=_order(list(caps)),
                                  caps=eff_caps, sector_of=sector_of, policy=pol,
                                  note=_note, room_fn=_room)

    # --- 7. minimum position size (dust never becomes a proposal) -------------- #
    min_w = float(pol["min_position_weight"])
    for tk in sorted(w):
        if 0.0 < w[tk] < min_w - _TOL:
            _note(C_MIN_POSITION, ADJ_DUST_DROPPED, ticker=tk,
                  before=_r(w[tk], 8), after=0.0, limit=_r(min_w, 8))
            w[tk] = 0.0

    # --- 8. concentration limit ------------------------------------------------ #
    w = _dilute_for_concentration(w, current=current, caps=eff_caps,
                                  sector_of=sector_of, order=_order(list(caps)),
                                  policy=pol, note=_note, room_fn=_room)

    # --- 9. cash bounds -------------------------------------------------------- #
    invested = sum(w.values())
    min_cash = float(pol["min_cash_weight"])
    if invested > 1.0 - min_cash + _TOL:
        excess = invested - (1.0 - min_cash)
        for tk in reversed(_order([t for t in w if w[t] > 0])):
            if excess <= _TOL:
                break
            take = min(w[tk], excess)
            w[tk] -= take
            excess -= take
            _note(C_CASH_BOUNDS, ADJ_CAPPED, ticker=tk, after=_r(w[tk], 8),
                  limit=_r(min_cash, 8))

    unconstrained_target = {tk: v for tk, v in w.items() if v > _TOL}

    # --- 10. turnover budget: the best feasible target INSIDE the budget ------- #
    budget = _f(pol.get("max_one_way_turnover"))
    turnover_full = one_way_turnover(current, unconstrained_target)
    turnover_block = {
        "budget": _r(budget, 6),
        "unbudgeted_one_way_turnover": _r(turnover_full, 6),
        "budget_binds": bool(budget is not None
                             and turnover_full > budget + 1.0e-12),
    }
    final = dict(unconstrained_target)
    if turnover_block["budget_binds"]:
        final, tb = _fit_turnover_budget(
            current=current, target=unconstrained_target, budget=float(budget),
            caps=eff_caps, sector_of=sector_of, score_of=score_of,
            mandatory_exits=set(mandatory_exits), policy=pol, note=_note,
            room_fn=_room)
        turnover_block.update(tb)
    turnover_block["achieved_one_way_turnover"] = _r(
        one_way_turnover(current, final), 6)

    final = {tk: round(v, 10) for tk, v in final.items() if v > _TOL}

    verification = verify_feasibility(weights=final, caps=eff_caps,
                                      sector_of=sector_of, current=current,
                                      policy=pol, meta=meta)
    feasible = bool(verification["valid"])
    blockers: list[dict] = []
    if not feasible:
        blockers.append({"code": B_NO_FEASIBLE_PORTFOLIO,
                         "kind": KIND_TRUE_BLOCKER,
                         "violations": verification["violations"],
                         "detail": ("The mandatory constraints admit no portfolio: "
                                    "the repaired target still violates them.")})
    elif not final and not _pos(current):
        # Nothing held and nothing investable - that is an empty feasible set only
        # when there is also no eligible capacity anywhere.
        if not any(c > 0 for c in caps.values()):
            blockers.append({"code": B_IMPOSSIBLE_LIQUIDITY,
                             "kind": KIND_TRUE_BLOCKER,
                             "detail": ("No eligible name can carry any feasible "
                                        "weight at this NAV.")})

    result = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "feasible": feasible and not blockers,
        "ideal_target": {tk: _r(v, 6) for tk, v in sorted(_pos(ideal).items())},
        "ideal_position_count": len(_pos(ideal)),
        "best_feasible_target": {tk: _r(v, 6) for tk, v in sorted(final.items())},
        "best_feasible_position_count": len(final),
        "best_feasible_cash_weight": _r(max(0.0, 1.0 - sum(final.values())), 6),
        "current_position_count": len(_pos(current)),
        "mandatory_exits": mandatory_exits,
        "mandatory_exit_doc": (
            "A held name outside the eligible universe (or with zero feasible "
            "capacity) cannot appear in any target. Its exit is a constraint, not an "
            "economic choice, so it is never weighed against the switching hurdle."),
        "constraint_adjustments": adjustments,
        "constraint_adjustment_count": len(adjustments),
        "constraints_that_reshaped": sorted({a["constraint"] for a in adjustments}),
        "redistributed_weight": _r(redistributed, 8),
        "released_weight": _r(released, 8),
        "turnover": turnover_block,
        "verification": verification,
        "blockers": blockers,
        "incumbency_policy": INCUMBENCY_POLICY,
        "policy": pol,
        # Release 50 - the same target by asset class / sleeve (only groups that
        # carry weight; cash is the residual; never a cosmetic 0% row).
        "best_feasible_allocation_by_asset_class": allocation_by(final, meta, "asset_class"),
        "best_feasible_allocation_by_sleeve": allocation_by(final, meta, "sleeve_id"),
        "current_allocation_by_asset_class": allocation_by(_pos(current), meta, "asset_class"),
        "ideal_allocation_by_asset_class": allocation_by(_pos(ideal), meta, "asset_class"),
        "cross_asset": {
            "instrument_metadata_supplied": any(
                c.get("asset_class") or c.get("instrument_type") for c in cands),
            "forced_diversification": False,
            "long_only": True,
            "group_weights": {k: ({kk: _r(vv, 6) for kk, vv in v.items()} if isinstance(v, dict)
                                  else _r(v, 6)) for k, v in _group_weights(final, meta).items()},
        },
    }
    result["solution_hash"] = stable_hash(result)
    return result


def allocation_by(weights: dict, meta: dict, key: str) -> dict:
    """Aggregate a weight vector by ``asset_class`` / ``sleeve_id``; the residual is
    cash. Only groups with weight are returned."""
    out: dict[str, float] = {}
    total = 0.0
    default = _DEFAULT_ASSET_CLASS if key == "asset_class" else _DEFAULT_SLEEVE
    for tk, v in (weights or {}).items():
        vv = _f(v) or 0.0
        if vv <= 0:
            continue
        g = (meta.get(tk) or {}).get(key) or default
        out[g] = out.get(g, 0.0) + vv
        total += vv
    cash = max(0.0, 1.0 - total)
    if cash > 1e-9:
        out[_CASH_CLASS if key == "asset_class" else "cash_usd"] = cash
    return {k: _r(v, 6) for k, v in sorted(out.items(), key=lambda kv: (-kv[1], kv[0]))}


def _redistribute(w: dict, *, released: float, order: list, caps: dict,
                  sector_of: dict, policy: dict, note, room_fn=None) -> float:
    """Place released capital into the next-best eligible names that have room.

    Greedy in descending value order over a LAMINAR constraint family (name inside
    sector inside the total budget), which is exactly optimal for that family. Any
    capital with no feasible destination stays in cash - cash is a real asset choice,
    not a failure to allocate.
    """
    if released <= _TOL:
        return 0.0
    sector_cap = float(policy["sector_cap_fraction"])
    min_w = float(policy["min_position_weight"])
    max_positions = int(policy["target_position_count"])
    used_sector: dict[str, float] = {}
    for tk, v in w.items():
        used_sector[sector_of.get(tk, "Unknown")] = (
            used_sector.get(sector_of.get(tk, "Unknown"), 0.0) + v)
    invested = sum(w.values())
    remaining = released
    placed = 0.0
    for tk in order:
        if remaining <= _TOL:
            break
        held = w.get(tk, 0.0)
        if held <= _TOL and max_positions > 0 and \
                sum(1 for v in w.values() if v > _TOL) >= max_positions:
            continue
        sec = sector_of.get(tk, UNCLASSIFIED_SECTOR)
        # Track B — an unclassified name consumes no fabricated sector budget.
        sec_room = (float("inf") if sec == UNCLASSIFIED_SECTOR
                    else sector_cap - used_sector.get(sec, 0.0))
        room = min(caps.get(tk, 0.0) - held,
                   sec_room,
                   float(policy.get("max_gross_exposure", 1.0)) - invested,
                   remaining)
        if room_fn is not None:
            room = min(room, room_fn(tk, w))
        if room <= _TOL:
            continue
        if held <= _TOL and room < min_w - _TOL:
            continue          # never create dust
        w[tk] = held + room
        used_sector[sec] = used_sector.get(sec, 0.0) + room
        invested += room
        remaining -= room
        placed += room
        note(C_NAME_CAP if held > _TOL else C_ELIGIBLE_UNIVERSE,
             ADJ_REDISTRIBUTED, ticker=tk, before=_r(held, 8),
             after=_r(w[tk], 8), redistributed_weight=_r(room, 8))
    if remaining > _TOL:
        note(C_CASH_BOUNDS, ADJ_DEFERRED_TO_CASH,
             released_weight=_r(remaining, 8),
             detail=("No eligible name had feasible room for this capital, so it "
                     "remains in cash. Cash is a real asset choice."))
    return placed


def _dilute_for_concentration(w: dict, *, current: dict, caps: dict,
                              sector_of: dict, order: list, policy: dict,
                              note, room_fn=None) -> dict:
    """Move weight from the largest positions into the next-best names until the
    concentration limit is met. Reshapes; never rejects."""
    limit = _f(policy.get("max_concentration_increase"))
    if limit is None:
        return w
    hhi_before = herfindahl(_pos(current))
    if hhi_before is None:
        return w
    sector_cap = float(policy["sector_cap_fraction"])
    min_w = float(policy["min_position_weight"])
    for _ in range(int(policy["max_repair_rounds"])):
        hhi_after = herfindahl(_pos(w)) or 0.0
        if hhi_after - hhi_before <= limit + 1.0e-12:
            return w
        live = [tk for tk in w if w[tk] > _TOL]
        if not live:
            return w
        donor = max(sorted(live), key=lambda tk: w[tk])
        used_sector: dict[str, float] = {}
        for tk, v in w.items():
            used_sector[sector_of.get(tk, "Unknown")] = (
                used_sector.get(sector_of.get(tk, "Unknown"), 0.0) + v)
        step = min(w[donor], max(min_w, 0.01))
        recipient = None
        for tk in order:
            if tk == donor:
                continue
            sec = sector_of.get(tk, UNCLASSIFIED_SECTOR)
            sec_room = (float("inf") if sec == UNCLASSIFIED_SECTOR
                        else sector_cap - used_sector.get(sec, 0.0))
            room = min(caps.get(tk, 0.0) - w.get(tk, 0.0), sec_room)
            if room_fn is not None:
                room = min(room, room_fn(tk, w, donor))
            if room >= step - _TOL and (w.get(tk, 0.0) > _TOL or step >= min_w):
                recipient = tk
                break
        if recipient is None:
            # Nothing can absorb it: releasing to cash also lowers concentration.
            w[donor] = max(0.0, w[donor] - step)
            note(C_CONCENTRATION, ADJ_DILUTED, ticker=donor,
                 after=_r(w[donor], 8), limit=_r(limit, 6),
                 detail="released to cash: no eligible name had room")
            continue
        # A transfer, not new capital: gross exposure is unchanged by construction.
        w[donor] -= step
        w[recipient] = w.get(recipient, 0.0) + step
        note(C_CONCENTRATION, ADJ_DILUTED, ticker=donor,
             after=_r(w[donor], 8), limit=_r(limit, 6),
             redistributed_to=recipient, redistributed_weight=_r(step, 8))
    return w


def _fit_turnover_budget(*, current: dict, target: dict, budget: float,
                         caps: dict, sector_of: dict, score_of: dict,
                         mandatory_exits: set, policy: dict, note,
                         room_fn=None) -> tuple:
    """The best feasible target INSIDE the turnover budget.

    Trades are split in two, and the split is the whole point:

    * MANDATORY legs implement a constraint (an ineligible / illiquid holding must
      leave, a name above its cap must come down). They are not discretionary, so
      they are taken FIRST and, if they alone exceed the budget, they are still
      taken - a budget may not trap the book in a constraint breach. That case is
      recorded explicitly rather than silently.
    * DISCRETIONARY legs are ordered by SCORE IMPROVEMENT PER UNIT OF TURNOVER
      against the current book's own weighted score, and taken while the budget
      lasts. The marginal leg is scaled to fit exactly, so the budget is used, not
      merely respected.

    The density is a first-order (linear) ordering criterion ONLY. The final
    target's economics are then measured exactly by the canonical owners; nothing
    here is published as an expected return.
    """
    min_w = float(policy["min_position_weight"])
    sector_cap = float(policy["sector_cap_fraction"])
    names = sorted(set(current) | set(target))

    ref = _weighted_score(current, score_of)
    if ref is None:
        ref = 0.0

    mandatory, discretionary = [], []
    for tk in names:
        cw = current.get(tk, 0.0)
        tw = target.get(tk, 0.0)
        d = tw - cw
        if abs(d) <= _TOL:
            continue
        cap = caps.get(tk, 0.0)
        is_mandatory = bool(tk in mandatory_exits or (cw > cap + _TOL and d < 0))
        # Density: what the trade does to the portfolio's weighted score per unit
        # of weight moved. Buying above the current average helps; selling below it
        # helps. Both are measured against ONE frozen reference point.
        density = (score_of.get(tk, 0.0) - ref) if d > 0 else (
            ref - score_of.get(tk, 0.0))
        leg = {"ticker": tk, "current_weight": _r(cw, 8),
               "target_weight": _r(tw, 8), "delta": _r(d, 8),
               "turnover_cost": _r(abs(d) / 2.0, 8),
               "score": _r(score_of.get(tk), 6),
               "score_improvement_per_turnover_unit": _r(density, 6),
               "mandatory": is_mandatory}
        (mandatory if is_mandatory else discretionary).append(leg)

    w = dict(current)
    for leg in sorted(mandatory, key=lambda x: x["ticker"]):
        w[leg["ticker"]] = leg["target_weight"]
    mandatory_turnover = one_way_turnover(current, w)

    accepted = [dict(x, accepted_delta=x["delta"], scaled=False)
                for x in mandatory]
    deferred: list[dict] = []
    budget_subordinated = mandatory_turnover > budget + 1.0e-12
    if budget_subordinated:
        note(C_TURNOVER_BUDGET, ADJ_TRADES_DEFERRED,
             limit=_r(budget, 6), before=_r(mandatory_turnover, 6),
             detail=("The constraint-mandated exits alone exceed the turnover "
                     "budget. A budget may not trap the book in a constraint "
                     "breach, so they are kept and every discretionary trade is "
                     "deferred."))

    ordered = sorted(discretionary,
                     key=lambda x: (-(x["score_improvement_per_turnover_unit"]
                                      or 0.0), x["ticker"]))
    for leg in ordered:
        used = one_way_turnover(current, w)
        room_turnover = budget - used
        if room_turnover <= 1.0e-12:
            deferred.append(dict(leg, deferred_reason=C_TURNOVER_BUDGET))
            continue
        tk = leg["ticker"]
        d = (leg["target_weight"] or 0.0) - w.get(tk, 0.0)
        if abs(d) <= _TOL:
            continue
        take = math.copysign(min(abs(d), 2.0 * room_turnover), d)
        if take > 0:
            sec = sector_of.get(tk, UNCLASSIFIED_SECTOR)
            used_sector = sum(v for t, v in w.items()
                              if sector_of.get(t, UNCLASSIFIED_SECTOR) == sec)
            sec_room = (float("inf") if sec == UNCLASSIFIED_SECTOR
                        else sector_cap - used_sector)
            take = min(take, caps.get(tk, 0.0) - w.get(tk, 0.0),
                       sec_room,
                       float(policy.get("max_gross_exposure", 1.0)) - sum(w.values()))
            if room_fn is not None:
                take = min(take, room_fn(tk, w))
        if take <= _TOL and take >= -_TOL:
            deferred.append(dict(leg, deferred_reason=C_TURNOVER_BUDGET))
            continue
        new_w = w.get(tk, 0.0) + take
        if 0.0 < new_w < min_w - _TOL:
            deferred.append(dict(leg, deferred_reason=C_MIN_POSITION))
            continue
        scaled = abs(abs(take) - abs(d)) > _TOL
        w[tk] = new_w
        accepted.append(dict(leg, accepted_delta=_r(take, 8), scaled=scaled))
    if deferred:
        note(C_TURNOVER_BUDGET, ADJ_TRADES_DEFERRED, limit=_r(budget, 6),
             deferred_trades=len(deferred),
             detail=("The best feasible target inside the turnover budget keeps the "
                     "trades with the highest score improvement per unit of "
                     "turnover; the rest are deferred to a later reassessment."))
    w = {tk: v for tk, v in w.items() if v > _TOL}
    return w, {
        "mandatory_turnover": _r(mandatory_turnover, 6),
        "budget_subordinated_to_mandatory_constraints": budget_subordinated,
        "accepted_trades": sorted(accepted, key=lambda x: x["ticker"]),
        "deferred_trades": sorted(deferred, key=lambda x: x["ticker"]),
        "accepted_trade_count": len(accepted),
        "deferred_trade_count": len(deferred),
        "ordering_basis": "SCORE_IMPROVEMENT_PER_UNIT_OF_ONE_WAY_TURNOVER",
        "ordering_reference_score": _r(ref, 6),
        "ordering_is_first_order_only": True,
    }


def _weighted_score(weights: dict, score_of: dict) -> Optional[float]:
    """Invested-weight-normalised combined percentile - the SAME basis the Slice-7
    proposal owner publishes, so before/after are comparable by construction."""
    pos = _pos(weights)
    total = sum(pos.values())
    if total <= 0:
        return None
    seen = False
    acc = 0.0
    for tk, v in pos.items():
        s = score_of.get(tk)
        if s is not None:
            seen = True
        acc += (v / total) * (s if s is not None else 0.0)
    return acc if seen else None


# --------------------------------------------------------------------------- #
# Feasibility verification (independent of the solver that produced the weights)
# --------------------------------------------------------------------------- #
def verify_feasibility(*, weights: dict, caps: dict, sector_of: dict,
                       current: Optional[dict] = None,
                       policy: Optional[dict] = None,
                       meta: Optional[dict] = None) -> dict:
    """Check a target against every mandatory constraint, independently of whoever
    built it. A target that violates its own constraints must never be presented as
    feasible, so the solver's output is verified rather than trusted."""
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    violations: list[dict] = []
    w = {tk: (_f(v) or 0.0) for tk, v in (weights or {}).items()}
    invested = sum(w.values())
    max_gross = float(pol.get("max_gross_exposure", 1.0))
    if invested > max_gross + 1.0e-9:
        violations.append({"code": C_GROSS_EXPOSURE, "value": _r(invested, 8),
                           "limit": max_gross})
    # Release 50 - the cross-asset limits, verified independently of the solver.
    m = meta or {}
    g = _group_weights({k: v for k, v in w.items() if v > 0}, m)
    for ac, v in sorted(g["by_class"].items()):
        if v > _asset_class_cap(pol, ac) + 1.0e-9:
            violations.append({"code": C_ASSET_CLASS_CAP, "asset_class": ac,
                               "value": _r(v, 8), "limit": _r(_asset_class_cap(pol, ac), 8)})
    for sl, v in sorted(g["by_sleeve"].items()):
        if v > _sleeve_cap(pol, sl) + 1.0e-9:
            violations.append({"code": C_SLEEVE_CAP, "sleeve_id": sl,
                               "value": _r(v, 8), "limit": _r(_sleeve_cap(pol, sl), 8)})
    if g["non_usd"] > float(pol.get("non_usd_currency_cap", 1.0)) + 1.0e-9:
        violations.append({"code": C_CURRENCY_CAP, "value": _r(g["non_usd"], 8),
                           "limit": _r(float(pol.get("non_usd_currency_cap", 1.0)), 8)})
    if g["collateral"] > float(pol.get("collateral_cap_fraction", 1.0)) + 1.0e-9:
        violations.append({"code": C_COLLATERAL_CAP, "value": _r(g["collateral"], 8),
                           "limit": _r(float(pol.get("collateral_cap_fraction", 1.0)), 8)})
    for tk in sorted(w):
        if w[tk] < -1.0e-9:
            violations.append({"code": C_LONG_ONLY, "ticker": tk,
                               "value": _r(w[tk], 8)})
        cap = caps.get(tk)
        if cap is None:
            violations.append({"code": C_ELIGIBLE_UNIVERSE, "ticker": tk,
                               "value": _r(w[tk], 8)})
        elif w[tk] > cap + 1.0e-9:
            violations.append({"code": C_NAME_CAP, "ticker": tk,
                               "value": _r(w[tk], 8), "limit": _r(cap, 8)})
        # The minimum position size governs what the target proposes to ESTABLISH,
        # not what the book already holds and this target does not touch. An
        # inherited dust position is a fact about the current portfolio; failing the
        # whole target on it would manufacture a blocker out of a position nobody
        # proposed. A name whose weight CHANGED is held to the floor.
        cur_w = (_f((current or {}).get(tk)) or 0.0)
        untouched = abs(w[tk] - cur_w) <= 1.0e-9
        if (0.0 < w[tk] < float(pol["min_position_weight"]) - 1.0e-9
                and not untouched):
            violations.append({"code": C_MIN_POSITION, "ticker": tk,
                               "value": _r(w[tk], 8),
                               "limit": _r(pol["min_position_weight"], 8)})
    sector_weight: dict[str, float] = {}
    unclassified_tickers: list[str] = []
    for tk, v in w.items():
        sec = sector_of.get(tk, UNCLASSIFIED_SECTOR)
        sector_weight[sec] = sector_weight.get(sec, 0.0) + v
        if sec == UNCLASSIFIED_SECTOR and v > _TOL:
            unclassified_tickers.append(tk)
    for sec in sorted(sector_weight):
        # Track B — unclassified weight is a reported data-quality state, never a
        # fabricated sector that can fail the target's feasibility.
        if sec == UNCLASSIFIED_SECTOR:
            continue
        if sector_weight[sec] > float(pol["sector_cap_fraction"]) + 1.0e-9:
            violations.append({"code": C_SECTOR_CAP, "sector": sec,
                               "value": _r(sector_weight[sec], 8),
                               "limit": _r(pol["sector_cap_fraction"], 8)})
    live = sum(1 for v in w.values() if v > 1.0e-9)
    if int(pol["target_position_count"]) > 0 and \
            live > int(pol["target_position_count"]):
        violations.append({"code": C_MAX_POSITIONS, "value": live,
                           "limit": int(pol["target_position_count"])})
    cash = max(0.0, 1.0 - invested)
    if cash < float(pol["min_cash_weight"]) - 1.0e-9 or \
            cash > float(pol["max_cash_weight"]) + 1.0e-9:
        violations.append({"code": C_CASH_BOUNDS, "value": _r(cash, 8),
                           "limit": [pol["min_cash_weight"],
                                     pol["max_cash_weight"]]})
    return {
        "valid": not violations,
        "violations": violations,
        "checked": list(RESHAPING_CONSTRAINT_CODES),
        "gross_exposure": _r(invested, 6),
        "net_exposure": _r(invested, 6),
        "cash_weight": _r(cash, 6),
        "position_count": live,
        "sector_weights": {k: _r(v, 6) for k, v in sorted(sector_weight.items())},
        "unclassified_sector_weight": _r(
            sector_weight.get(UNCLASSIFIED_SECTOR, 0.0), 6),
        "unclassified_sector_tickers": sorted(unclassified_tickers),
        "asset_class_weights": {k: _r(v, 6) for k, v in sorted(g["by_class"].items())},
        "sleeve_weights": {k: _r(v, 6) for k, v in sorted(g["by_sleeve"].items())},
        "non_usd_exposure": _r(g["non_usd"], 6),
        "collateral_weight": _r(g["collateral"], 6),
        "max_name_weight_observed": _r(max(w.values()) if w else 0.0, 6),
        "one_way_turnover_from_current": _r(
            one_way_turnover(current or {}, w), 6),
    }


# --------------------------------------------------------------------------- #
# Switching economics - the ONE place incumbency is allowed to matter
# --------------------------------------------------------------------------- #
def switching_economics(*, current_weight: dict, target_weight: dict,
                        candidates: Optional[list] = None,
                        nav: Optional[float] = None,
                        risk_before: Optional[float] = None,
                        risk_after: Optional[float] = None,
                        mandatory_exits: Optional[list] = None,
                        score_before: Optional[float] = None,
                        score_after: Optional[float] = None,
                        score_cost_hurdle: Optional[float] = None,
                        turnover_one_way: Optional[float] = None,
                        transaction_cost: Optional[float] = None,
                        policy: Optional[dict] = None) -> dict:
    """Price the switch from the CURRENT book to the best feasible target.

    The hurdle is explicit, frozen and deterministic: the net score improvement
    (after the modelled transition cost) must clear
    ``min_switching_net_improvement``. It is stated in the SAME percentile points as
    the per-name and portfolio hurdles the system already uses, so a basket of
    individually-rejected switches cannot pass in aggregate, and it is never tuned
    on realised outcomes.

    This function is the owner of the HURDLE, not a second owner of the score, the
    turnover or the cost. When the composition owner has already produced those
    numbers from their canonical owners it passes them in (``score_before`` /
    ``score_after`` / ``score_cost_hurdle`` / ``turnover_one_way`` /
    ``transaction_cost``) and they are used verbatim; they are only derived here
    when nobody upstream computed them, and then from the SAME definitions.

    A mandatory exit is a constraint, not a bet: when the only reason to trade is a
    mandatory exit, the hurdle does not apply to it.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    score_of = {c.get("ticker"): (_f(c.get("score")) or 0.0)
                for c in (candidates or []) if c.get("ticker")}
    cur = {tk: (_f(v) or 0.0) for tk, v in (current_weight or {}).items()}
    tgt = {tk: (_f(v) or 0.0) for tk, v in (target_weight or {}).items()}
    navv = _f(nav)

    delegated = {"score_before": score_before is not None,
                 "score_after": score_after is not None,
                 "score_cost_hurdle": score_cost_hurdle is not None,
                 "one_way_turnover": turnover_one_way is not None,
                 "transaction_cost": transaction_cost is not None}
    if score_before is None:
        score_before = _weighted_score(cur, score_of)
    if score_after is None:
        score_after = _weighted_score(tgt, score_of)
    score_before, score_after = _f(score_before), _f(score_after)
    improvement = (None if (score_before is None or score_after is None)
                   else score_after - score_before)

    one_way = (_f(turnover_one_way) if turnover_one_way is not None
               else one_way_turnover(cur, tgt))
    one_way = 0.0 if one_way is None else one_way
    two_way = 2.0 * one_way
    cost_rate = float(pol["cost_rate_per_side"])
    # Release 50 - a per-instrument cost rate when the candidate rows carry one
    # (a rates future is not priced like a cash equity); the canonical desk rate
    # for every row that carries none, which is the whole pre-R50 book.
    meta = candidate_meta(candidates or [])
    per_name = {tk: m["cost_rate"] for tk, m in meta.items() if m.get("cost_rate") is not None}
    if per_name:
        cost_weight = sum(abs(tgt.get(tk, 0.0) - cur.get(tk, 0.0))
                          * float(per_name.get(tk, cost_rate))
                          for tk in set(cur) | set(tgt))
    else:
        cost_weight = two_way * cost_rate
    cost_dollars = (_f(transaction_cost) if transaction_cost is not None
                    else (None if navv is None else round(cost_weight * navv, 2)))
    # The cost hurdle in score points, using the SAME conversion the Slice-6/7
    # owners use (bps -> percentile points), never a second conversion.
    cost_hurdle = (_f(score_cost_hurdle) if score_cost_hurdle is not None
                   else two_way * float(pol["round_trip_cost_bps"])
                   * float(pol["score_points_per_cost_bp"]) / 2.0)
    cost_hurdle = 0.0 if cost_hurdle is None else cost_hurdle
    net = None if improvement is None else improvement - cost_hurdle

    hhi_before = herfindahl(_pos(cur))
    hhi_after = herfindahl(_pos(tgt))
    hurdle = float(pol["min_switching_net_improvement"])
    min_turnover = float(pol["min_switching_turnover"])

    mandatory = sorted(set(mandatory_exits or []))
    mandatory_only = bool(
        mandatory and all(
            tk in mandatory or abs(tgt.get(tk, 0.0) - cur.get(tk, 0.0)) <= _TOL
            for tk in set(cur) | set(tgt)))

    reasons: list[str] = []
    if one_way <= min_turnover:
        clears = False
        reasons.append("TURNOVER_BELOW_MATERIALITY_FLOOR")
    elif mandatory_only:
        clears = True
        reasons.append("MANDATORY_EXIT_NOT_SUBJECT_TO_ECONOMIC_HURDLE")
    elif net is None:
        clears = False
        reasons.append("IMPROVEMENT_NOT_MEASURABLE")
    elif net >= hurdle - 1.0e-12:
        clears = True
        reasons.append("NET_IMPROVEMENT_CLEARS_SWITCHING_HURDLE")
    else:
        clears = False
        reasons.append("NET_IMPROVEMENT_BELOW_SWITCHING_HURDLE")

    return {
        "owner": CALCULATION_OWNER,
        "switching_policy_version": pol["switching_policy_version"],
        "improvement_basis": IMPROVEMENT_BASIS,
        "expected_return_state": EXPECTED_RETURN_STATE_NOT_CALIBRATED,
        "expected_return_before": None,
        "expected_return_after": None,
        "score_before": _r(score_before, 6),
        "score_after": _r(score_after, 6),
        "score_improvement": _r(improvement, 6),
        "score_cost_hurdle": _r(cost_hurdle, 6),
        "score_improvement_net_of_cost": _r(net, 6),
        "switching_hurdle": _r(hurdle, 6),
        "hurdle_frozen": True,
        "hurdle_tuned_on_outcomes": False,
        "clears_switching_hurdle": clears,
        "reason_codes": reasons,
        "one_way_turnover": _r(one_way, 6),
        "two_way_traded_weight": _r(two_way, 6),
        "estimated_transaction_cost_weight": _r(cost_weight, 8),
        "estimated_transaction_cost": cost_dollars,
        "cost_rate_per_side": cost_rate,
        "cost_basis": ("ABSOLUTE_WEIGHT_CHANGE_TIMES_PER_INSTRUMENT_SIDE_RATE" if per_name
                       else "ABSOLUTE_WEIGHT_CHANGE_TIMES_PER_SIDE_RATE"),
        "per_instrument_cost_rates_applied": bool(per_name),
        "allocation_before_by_asset_class": allocation_by(_pos(cur), meta, "asset_class"),
        "allocation_after_by_asset_class": allocation_by(_pos(tgt), meta, "asset_class"),
        "concentration_before": _r(hhi_before, 6),
        "concentration_after": _r(hhi_after, 6),
        "concentration_delta": _r(
            None if (hhi_before is None or hhi_after is None)
            else hhi_after - hhi_before, 6),
        "portfolio_volatility_before": _r(_f(risk_before), 6),
        "portfolio_volatility_after": _r(_f(risk_after), 6),
        "portfolio_volatility_delta": _r(
            None if (_f(risk_before) is None or _f(risk_after) is None)
            else _f(risk_after) - _f(risk_before), 6),
        "position_count_before": len(_pos(cur)),
        "position_count_after": len(_pos(tgt)),
        "cash_weight_before": _r(max(0.0, 1.0 - sum(_pos(cur).values())), 6),
        "cash_weight_after": _r(max(0.0, 1.0 - sum(_pos(tgt).values())), 6),
        "mandatory_exits": mandatory,
        "mandatory_exit_only_change": mandatory_only,
        "incumbency_policy": INCUMBENCY_POLICY,
        "incumbency_advantage_applied": "TRANSITION_COST_ONLY",
        "delegated_inputs": delegated,
        "delegated_doc": ("True means the value was produced by its canonical owner "
                          "and used verbatim here; this module owns the HURDLE, not "
                          "the score, the turnover or the cost."),
    }


# --------------------------------------------------------------------------- #
# The three authoritative outcomes
# --------------------------------------------------------------------------- #
def decide_outcome(*, solution: dict, economics: dict,
                   true_blockers: Optional[list] = None) -> dict:
    """ONE outcome from :data:`OUTCOME_VOCAB`.

    Precedence, and the whole philosophy of the release, in four lines:

      1. a declared TRUE BLOCKER  -> TRUE_BLOCKER   (fail closed)
      2. no feasible portfolio    -> TRUE_BLOCKER   (the feasible set is EMPTY)
      3. feasible but sub-hurdle  -> HOLD_CURRENT_BOOK
      4. otherwise                -> PROPOSAL_READY

    A reshaping constraint can never reach (1) or (2): it changed the solution, and
    the solution it produced is what steps (3) and (4) judge.
    """
    blockers = [dict(b) for b in (true_blockers or [])]
    for b in blockers:
        b.setdefault("kind", KIND_TRUE_BLOCKER)
    blockers += list((solution or {}).get("blockers") or [])
    # A caller may never smuggle a reshaping constraint in as a blocker.
    misclassified = sorted({b.get("code") for b in blockers
                            if not is_true_blocker(b.get("code"))})
    blockers = [b for b in blockers if is_true_blocker(b.get("code"))]

    if blockers:
        outcome = OUTCOME_TRUE_BLOCKER
        reason = sorted({b.get("code") for b in blockers})
        headline = "PORTFOLIO DECISION BLOCKED"
    elif not (solution or {}).get("feasible"):
        outcome = OUTCOME_TRUE_BLOCKER
        reason = [B_NO_FEASIBLE_PORTFOLIO]
        headline = "PORTFOLIO DECISION BLOCKED"
    elif not (economics or {}).get("clears_switching_hurdle"):
        outcome = OUTCOME_HOLD_CURRENT_BOOK
        reason = list((economics or {}).get("reason_codes") or [])
        headline = "HOLD THE CURRENT BOOK"
    else:
        outcome = OUTCOME_PROPOSAL_READY
        reason = list((economics or {}).get("reason_codes") or [])
        headline = "REALLOCATION PROPOSAL READY FOR REVIEW"

    return {
        "owner": CALCULATION_OWNER,
        "outcome": outcome,
        "outcome_vocabulary": list(OUTCOME_VOCAB),
        "headline": headline,
        "reason_codes": reason,
        "true_blockers": blockers,
        "misclassified_blockers": misclassified,
        "misclassified_blocker_doc": (
            "A code that is not a declared true blocker was offered as one. It is "
            "REFUSED, never honoured: that promotion is exactly how a normal cap "
            "used to freeze the portfolio."),
        "feasible_target_exists": bool((solution or {}).get("feasible")),
        "feasible_alternative_was_computed": bool(
            (solution or {}).get("best_feasible_target") is not None),
        "constraints_that_reshaped": list(
            (solution or {}).get("constraints_that_reshaped") or []),
        "requires_manual_approval": outcome == OUTCOME_PROPOSAL_READY,
        "authorises_execution": False,
        "creates_orders": False,
    }


# --------------------------------------------------------------------------- #
# The one entry point
# --------------------------------------------------------------------------- #
def build_constrained_reallocation(*, input_contract: dict,
                                   policy: Optional[dict] = None) -> dict:
    """Solve, price and decide in one pure call. Never raises on incomplete input.

    ``input_contract`` carries ``current_weights``, ``ideal_weights``,
    ``candidates`` (the eligible universe with sector / adv_dollar / score / rank),
    ``nav``, optional ``risk_contributions`` / ``risk_before`` / ``risk_after``, and
    any ``true_blockers`` the composition owner has already established (stale data,
    a point-in-time failure, an unreconciled NAV).
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    ic = input_contract or {}
    solution = solve_feasible_target(
        current_weight=ic.get("current_weights") or {},
        ideal_weight=ic.get("ideal_weights") or {},
        candidates=ic.get("candidates") or [],
        nav=ic.get("nav"),
        risk_contributions=ic.get("risk_contributions") or {},
        policy=pol)
    economics = switching_economics(
        current_weight=ic.get("current_weights") or {},
        target_weight=solution["best_feasible_target"],
        candidates=ic.get("candidates") or [],
        nav=ic.get("nav"),
        risk_before=ic.get("risk_before"), risk_after=ic.get("risk_after"),
        mandatory_exits=solution["mandatory_exits"], policy=pol)
    verdict = decide_outcome(solution=solution, economics=economics,
                             true_blockers=ic.get("true_blockers") or [])
    result = {
        "schema_version": SCHEMA_VERSION,
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "nav": _r(_f(ic.get("nav")), 2),
        "constraint_inventory": constraint_inventory(pol),
        "solution": solution,
        "switching_economics": economics,
        "verdict": verdict,
        "policy": pol,
        "safety": safety_block(),
    }
    result["constrained_reallocation_hash"] = stable_hash(result)
    return result


def safety_block() -> dict:
    return {
        "read_only": True, "preview_only": True, "paper_only": True,
        "manual_review": True, "automation_off": True,
        "created_orders": False, "created_fills": False, "created_target": False,
        "changed_holdings": False, "changed_cash": False, "changed_nav": False,
        "wrote_to_ledger": False, "wrote_to_database": False,
        "called_provider": False, "called_prediction": False,
        "promoted_model": False, "recalibrated_model": False,
        "broker_enabled": False, "live_orders_enabled": False,
        "automatic_approval_allowed": False, "automatic_rebalance_allowed": False,
        "safety_badges": ["PAPER ONLY", "PREVIEW ONLY", "REVIEW ONLY", "NO ORDERS",
                          "NO LIVE ORDERS", "NO BROKER", "MANUAL REVIEW",
                          "AUTOMATION OFF"],
    }


__all__ = [
    "SCHEMA_VERSION", "CALCULATION_OWNER", "PHASE",
    "OUTCOME_PROPOSAL_READY", "OUTCOME_HOLD_CURRENT_BOOK",
    "OUTCOME_TRUE_BLOCKER", "OUTCOME_VOCAB",
    "KIND_RESHAPES", "KIND_TRUE_BLOCKER", "CONSTRAINT_KIND_VOCAB",
    "RESHAPING_CONSTRAINT_CODES", "TRUE_BLOCKER_CODES", "ADJUSTMENT_VOCAB",
    "INCUMBENCY_POLICY", "default_policy", "constraint_inventory",
    "is_true_blocker", "name_caps", "solve_feasible_target",
    "verify_feasibility", "switching_economics", "decide_outcome",
    "build_constrained_reallocation", "one_way_turnover", "herfindahl",
    "stable_hash", "safety_block",
    # Release 50 - cross-asset constraints.
    "C_ASSET_CLASS_CAP", "C_SLEEVE_CAP", "C_CURRENCY_CAP", "C_COLLATERAL_CAP",
    "C_UNIT_GRANULARITY", "candidate_meta", "cross_asset_room", "allocation_by",
    "cross_asset_relevant", "group_add", "empty_groups",
    # Track B - missing sector classification is data quality, not a sector.
    "UNCLASSIFIED_SECTOR",
]
