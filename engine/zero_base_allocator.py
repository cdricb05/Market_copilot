"""engine/zero_base_allocator.py - the canonical zero-base allocation kernel.

Release 30. This module owns TWO distinct objects and never conflates them:

**ZERO-BASE TARGET** - the intrinsic desired allocation. The question it answers
is *"if every investable dollar were cash at this decision timestamp, which
eligible names and how much cash would we choose?"* The current portfolio is not
an input to it. A held name has no advantage over an unheld one; ownership is
not an investment thesis.

**IMPLEMENTABLE TARGET** - the transition-aware version of that same objective,
computed FROM the current portfolio with transaction costs inside the economics.
This is where incumbency legitimately matters: not because a holding is good, but
because moving it costs money.

Keeping them apart is the entire point. A single "proposal" that quietly lets
holdings influence which assets look attractive is the ownership-inertia defect
this kernel exists to remove.

**The objective, stated once.** Over long-only weights ``w`` with cash
``1 - sum(w)``:

    U(w) = mu'w
           - (gamma/2) * w' Sigma_h w                 covariance risk
           - (phi/2)   * sum_i w_i^2 sigma_f_i^2      forecast uncertainty
           - delta     * max(0, -q05(w))              downside shortfall
           - cost(w, w_current)                       transition

    q05(w)   = mu'w - z * tail * sqrt(w'Sigma_h w + sum_i w_i^2 sigma_f_i^2)
    Sigma_h  = daily covariance * policy horizon, from the canonical risk owner
               engine.holding_opportunity_cost.build_covariance
    cost     = sum |w_i - w_current_i| * cost_rate_per_side  (ABSENT for the
               zero-base target, which starts from cash by definition)

The three risk prices are separately identified - covariance risk, forecast
uncertainty and tail shortfall each multiply a different quantity - and all
three, plus ``tail``, are CALIBRATED from walk-forward validation evidence by
the research lane and passed in. None is hand-picked, and every payload reports
whether it used calibrated values or the fail-safe defaults.

Every risk term is a property of the PORTFOLIO, not a sum of per-name penalties.
That distinction is not cosmetic: per-name forecast error is overwhelmingly
idiosyncratic, so charging it name-by-name would price risk that diversification
removes and would reject an entire cross-section that a diversified book handles
comfortably. ``Sigma_h`` and the uncertainty diagonal are positive semi-definite,
``q05`` is concave in ``w`` (an affine function minus a norm), and the feasible
set is convex - so ``U`` is concave and its maximum is unique up to ties, which
are broken by ticker.

**Cash.** Cash is a real choice, not a residual: it earns
``CASH_RETURN`` (0.0) and carries no risk. That zero is a DECLARED policy, not an
oversight - no owned canonical risk-free series is admissible as a
portfolio-construction input (the market-context owner is CONTEXT, never a
signal), and the forecasting layer explicitly does not forecast the market's
level. So a name must earn its place against a genuinely riskless zero, and when
the risk-adjusted opportunity set is poor the optimiser holds cash.

Pure stdlib, pure function: no file, no service, no state, no write.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

SCHEMA_VERSION = "zero_base_allocator.v1"
INPUT_SCHEMA_VERSION = "zero_base_allocator.input.v1"
OBJECTIVE_VERSION = "zero_base_objective.v1"
CONSTRAINT_POLICY_VERSION = "zero_base_constraint_policy.v1"
CALCULATION_OWNER = "engine.zero_base_allocator"
PHASE = "R30"

#: Target kinds. Every payload says which one it is; nothing infers it.
TARGET_ZERO_BASE = "ZERO_BASE_TARGET"
TARGET_IMPLEMENTABLE = "IMPLEMENTABLE_TARGET"
TARGET_KINDS = (TARGET_ZERO_BASE, TARGET_IMPLEMENTABLE)

STATE_READY = "READY"
STATE_DEGRADED = "DEGRADED"
STATE_BLOCKED = "BLOCKED"
STATE_NO_ACTIVE_BOOK = "NO_ACTIVE_BOOK"
STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED, STATE_NO_ACTIVE_BOOK)

#: Cash policy. Declared, versioned, and carried in every payload.
CASH_RETURN = 0.0
CASH_RETURN_POLICY = "ZERO_RETURN_PAPER_ASSUMPTION"
CASH_RETURN_DOC = (
    "Cash is modelled at zero return. No owned canonical risk-free series is "
    "admissible as a portfolio-construction input - the market-context owner is "
    "declared CONTEXT and never a signal - and the forecasting layer does not "
    "forecast the market's level. Cash therefore competes against forecast "
    "cross-sectional EXCESS return net of risk, which is exactly the quantity "
    "the evidence supports.")

#: Position-count policy. The legacy 25 is a CONSTRUCTION default of the
#: equal-weight book, not an investment constraint: nothing in the evidence says
#: 25 is the right number of names. The zero-base objective therefore does not
#: fix a count; it caps a name's weight and lets the count fall out. The legacy
#: value is retained, declared and reported so the difference is visible rather
#: than silently deleted.
LEGACY_TARGET_POSITION_COUNT = 25
POSITION_COUNT_POLICY = "EMERGENT_FROM_WEIGHT_CAPS_NOT_A_FIXED_COUNT"

SAFETY_BADGES = ["PREVIEW ONLY", "READ ONLY", "NO ORDERS", "NO LIVE ORDERS",
                 "AUTOMATION OFF", "MANUAL REVIEW"]

_TRADING_DAYS_YEAR = 252.0


def default_policy() -> dict[str, Any]:
    """The single explicit, versioned allocation policy.

    Values marked ``reused`` mirror canonical constants owned elsewhere and are
    OVERRIDDEN by the API owner with the live values, so no threshold is quietly
    forked. Values marked ``calibrated`` are set from walk-forward evidence by
    the research lane and passed in. Values marked ``new`` are genuinely-new
    Release 30 thresholds, declared here once and exercised at their boundaries
    by ``tests/test_release30_zero_base_allocator.py``.
    """
    return {
        "objective_version": OBJECTIVE_VERSION,
        "constraint_policy_version": CONSTRAINT_POLICY_VERSION,
        # --- reused canonical construction / cost constants ------------------ #
        "max_name_weight": 0.10,        # reused: multi_horizon_engine.MAX_INDIVIDUAL_WEIGHT
        "sector_cap_fraction": 0.25,    # reused: multi_horizon_engine.SECTOR_CAP_FRACTION
        "min_adv_dollar": 1.0e7,        # reused: multi_horizon_engine.MIN_ADV_DOLLAR
        "cost_rate_per_side": 0.00125,  # reused: paper_trading_desk.COST_RATE_PER_SIDE
        "cost_bps_per_side": 12.5,      # reused: paper_trading_desk.COST_BPS_PER_SIDE
        "covariance_lookback": 60,      # reused: Slice-6 lookback
        "min_covariance_obs": 40,       # reused: Slice-6 minimum
        "covariance_variance_floor": 1.0e-12,   # reused: Slice-6 floor
        # --- the objective's risk / uncertainty prices ----------------------- #
        # gamma prices covariance risk, phi prices FORECAST uncertainty, delta
        # prices the tail shortfall and tail_factor is how much fatter the
        # measured left tail is than a normal one. All four are calibrated by the
        # research lane on VALIDATION blocks and passed in; the values below are
        # the fail-safe defaults used when no calibration is supplied, and a
        # payload always reports which of the two it used.
        #
        # The defaults are the NEUTRAL ones, not a guess at an appetite: gamma
        # and phi at 1.0 price a unit of variance at a unit of return, delta at
        # 0.0 refuses to charge for a tail the variance term has already priced,
        # and tail_factor at 1.0 assumes no measured asymmetry. A calibrated run
        # replaces all four.
        "risk_aversion_gamma": 1.0,             # calibrated
        "uncertainty_aversion_phi": 1.0,        # calibrated
        "downside_aversion_delta": 0.0,         # calibrated
        "downside_tail_factor": 1.0,            # calibrated
        "risk_price_source": "DEFAULT",         # or WALK_FORWARD_CALIBRATED
        # --- liquidity ------------------------------------------------------- #
        # A position may not exceed this multiple of a name's 20-session average
        # dollar volume, so the target cannot contain a holding the paper book
        # could not build in a day.
        "max_adv_participation": 1.0,           # new
        # --- policy horizon --------------------------------------------------- #
        # Which forecast horizon prices the objective. 20 sessions matches the
        # cadence at which the owned monthly inputs actually change, so the
        # allocator is never asked to act on information that refreshes slower
        # than its own decisions.
        "policy_horizon_sessions": 20,          # new
        # --- optimiser -------------------------------------------------------- #
        "max_iterations": 1500,                 # new
        # The Frank-Wolfe duality gap is an UPPER BOUND on how much utility the
        # optimiser could still gain, in return units. 1e-7 is one hundredth of a
        # basis point of NAV - about a cent on a $100k paper book - so a target
        # inside this tolerance is converged in every sense that can matter to a
        # portfolio decision. Stating it economically, rather than as a bare
        # epsilon, is what makes "converged" a claim anyone can check.
        "convergence_tolerance": 1.0e-7,        # new
        "huber_delta": 1.0e-4,                  # new: cost smoothing, 1bp of NAV
        "line_search_grid": 64,                 # new
        "polish_rounds": 400,                   # new
        # --- minimum position size --------------------------------------------- #
        # A weight the book would never actually hold is not a target. Without a
        # floor the optimiser is free to express a tilt as a hundred positions of
        # a tenth of a percent, which is arithmetically optimal and operationally
        # meaningless: on a $100k paper book that is a $100 position. The floor is
        # applied AFTER the solve, the released weight goes to cash, and both the
        # dropped names and the utility given up are reported - so the filter is
        # visible rather than a silent tidy-up.
        "min_position_weight": 0.005,           # new: 0.5% of NAV
        # --- reporting -------------------------------------------------------- #
        "material_weight_delta": 1.0e-4,        # new: RETAIN vs INCREASE/REDUCE
        "min_reported_weight": 1.0e-6,          # new
        # --- Release 50: cross-asset limits (ONE definition, owned by the
        # constrained-reallocation kernel; mirrored here so the zero-base and the
        # feasible target are solved under the same caps) --------------------- #
        "max_gross_exposure": 1.0,
        "asset_class_weight_caps": {"US_EQUITY": 1.0, "CASH": 1.0, "DEFAULT_NON_EQUITY": 0.25},
        "sleeve_weight_caps": {"us_equity_fundamental_momentum_50_50_v1": 1.0, "cash_usd": 1.0,
                               "DEFAULT_NON_EQUITY": 0.25},
        "non_usd_currency_cap": 0.20,
        "collateral_cap_fraction": 0.25,
    }


def _meta(candidates: list) -> dict:
    """Instrument metadata per candidate (equity defaults for rows without it),
    read through the ONE cross-asset definition owner."""
    from . import constrained_reallocation as _cr
    return _cr.candidate_meta(candidates or [])


def _xroom(tk: str, w: dict, meta: Optional[dict], policy: dict,
           donor: Optional[str] = None, groups: Optional[dict] = None) -> float:
    """Cross-asset room (asset class / sleeve / non-USD / collateral). Infinite
    for a US cash equity under the default caps, so the equity solve is unchanged.
    ``meta`` is None whenever no cross-asset limit can bind (see ``_solve_meta``),
    and ``groups`` carries the caller's incrementally maintained group totals."""
    if not meta:
        return float("inf")
    from . import constrained_reallocation as _cr
    return _cr.cross_asset_room(tk, w, meta=meta, policy=policy,
                                exclude_same_group_as=donor, groups=groups)


def _solve_meta(meta: Optional[dict], policy: dict) -> Optional[dict]:
    """The metadata the SOLVER sees: None when no cross-asset limit can bind, so
    the equity-only solve pays nothing for Release 50 (the room is provably
    infinite there - the gross budget already bounds the single equity class)."""
    if not meta:
        return None
    from . import constrained_reallocation as _cr
    return meta if _cr.cross_asset_relevant(meta, policy) else None


def _groups_of(w: dict, meta: Optional[dict]) -> Optional[dict]:
    if not meta:
        return None
    from . import constrained_reallocation as _cr
    return _cr.group_weights(w, meta)


def _groups_add(groups: Optional[dict], meta: Optional[dict], tk: str, delta: float) -> None:
    if groups is None or not meta:
        return
    from . import constrained_reallocation as _cr
    _cr.group_add(groups, meta, tk, delta)


# --------------------------------------------------------------------------- #
# small helpers
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


def _money(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


_VOLATILE_KEYS = frozenset({"generated_at", "evaluated_at", "loaded_at",
                            "built_at", "decision_timestamp"})


def _strip_volatile(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in sorted(obj.items())
                if k not in _VOLATILE_KEYS}
    if isinstance(obj, list):
        return [_strip_volatile(v) for v in obj]
    return obj


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(_strip_volatile(obj), sort_keys=True, separators=(",", ":"),
                   default=str).encode("utf-8")).hexdigest()[:32]


# --------------------------------------------------------------------------- #
# The feasible set
# --------------------------------------------------------------------------- #
def name_caps(*, candidates: list, nav: float, policy: dict) -> dict:
    """Per-name upper weight bound: the tighter of the name cap and liquidity.

    Liquidity is a HARD bound, not a penalty. A weight the paper book could not
    actually establish inside one session is not a target; it is a wish.
    """
    caps = {}
    part = float(policy["max_adv_participation"])
    for c in candidates:
        tk = c["ticker"]
        cap = float(policy["max_name_weight"])
        adv = _f(c.get("adv_dollar"))
        if adv is not None and nav > 0:
            cap = min(cap, max(0.0, part * adv / nav))
        # Release 50 - unit granularity: one unit larger than the name cap at this
        # NAV means no feasible weight exists (a full-size contract on a small book).
        unit = _f(c.get("unit_notional_usd"))
        if unit is not None and nav > 0 and unit > float(policy["max_name_weight"]) * nav:
            cap = 0.0
        caps[tk] = cap
    return caps


def _linear_oracle(grad: dict, *, candidates: list, caps: dict,
                   sector_of: dict, policy: dict, meta: Optional[dict] = None) -> dict:
    """Maximise ``grad . v`` over the feasible set - the Frank-Wolfe vertex.

    The constraint family is laminar (name inside sector inside total budget), so
    filling greedily in descending gradient order is exactly optimal. Names whose
    gradient is not positive are left at zero: cash is free and always available,
    so nothing is ever bought at a negative marginal utility. Release 50 adds the
    cross-asset caps (asset class / sleeve / non-USD / collateral) as further
    ceilings; the currency and collateral caps cut across the laminar family, so
    the greedy vertex is a labelled approximation there (exact for the equity book).
    """
    order = sorted((c["ticker"] for c in candidates),
                   key=lambda tk: (-(grad.get(tk) or 0.0), tk))
    sector_cap = float(policy["sector_cap_fraction"])
    budget = float(policy.get("max_gross_exposure", 1.0))
    used_sector: dict = {}
    v = {tk: 0.0 for tk in caps}
    groups = _groups_of(v, meta)          # maintained incrementally (O(1) per take)
    for tk in order:
        if budget <= 0:
            break
        if (grad.get(tk) or 0.0) <= 0:
            break
        sec = sector_of.get(tk) or "Unknown"
        room_sector = sector_cap - used_sector.get(sec, 0.0)
        take = min(caps.get(tk, 0.0), budget, max(0.0, room_sector),
                   _xroom(tk, v, meta, policy, groups=groups))
        if take <= 0:
            continue
        v[tk] = take
        budget -= take
        used_sector[sec] = used_sector.get(sec, 0.0) + take
        _groups_add(groups, meta, tk, take)
    return v


# --------------------------------------------------------------------------- #
# The optimiser
# --------------------------------------------------------------------------- #
def _quad_form(w: dict, cov: dict, keys: list) -> dict:
    """``Sigma w`` restricted to the covariance-included names."""
    out = {}
    active = [k for k in keys if (w.get(k) or 0.0) != 0.0]
    for i in keys:
        row = cov.get(i) or {}
        out[i] = sum(row.get(j, 0.0) * w[j] for j in active)
    return out


#: Normal-tail multiplier for the 5 % portfolio quantile. Arithmetic, not a
#: tuned knob: the DISPERSION it scales is measured, and the measurement is
#: where the calibration lives.
Z05 = 1.6448536269514722
DOWNSIDE_QUANTILE = 0.05


def feasible_start(current: dict, *, caps: dict, sector_of: dict,
                   policy: dict, meta: Optional[dict] = None) -> dict:
    """The current portfolio projected onto the feasible set.

    The implementable solve starts HERE rather than at cash, because the
    question it answers is "what is worth changing from where we are". Starting
    at cash would mean the optimiser had to buy its way back to the status quo,
    and a prohibitive cost rate could then strand it somewhere in between - which
    is an artefact of the search, not an economic conclusion.
    """
    w = {tk: 0.0 for tk in caps}
    sector_used: dict = {}
    total = 0.0
    groups = _groups_of(w, meta)          # maintained incrementally (O(1) per take)
    for tk in sorted(current, key=lambda t: (-(current.get(t) or 0.0), t)):
        if tk not in caps:
            continue          # a mandatory exit: not in the eligible universe
        sec = sector_of.get(tk) or "Unknown"
        room = min(caps[tk], float(policy.get("max_gross_exposure", 1.0)) - total,
                   float(policy["sector_cap_fraction"]) - sector_used.get(sec, 0.0),
                   _xroom(tk, w, meta, policy, groups=groups))
        take = max(0.0, min(float(current.get(tk) or 0.0), room))
        w[tk] = take
        total += take
        sector_used[sec] = sector_used.get(sec, 0.0) + take
        _groups_add(groups, meta, tk, take)
    return w


def optimise(*, candidates: list, mu: dict, sigma_forecast: dict,
             cov_h: dict, cov_included: list, caps: dict, sector_of: dict,
             policy: dict, current_weight: Optional[dict] = None,
             meta: Optional[dict] = None) -> dict:
    """Frank-Wolfe maximisation of the declared objective.

    ``current_weight`` is ``None`` for the ZERO-BASE target - the transition cost
    term is then absent and the search starts from all-cash, which is the formal
    statement of "capital begins entirely in cash". Supplying it produces the
    IMPLEMENTABLE target: the same objective, started from the current book, with
    the cost of moving subtracted.

    ``cov_h`` is ALREADY horizon-scaled by the caller, so this routine holds no
    second opinion about the horizon.

    Deterministic: bounded iteration count, gridded line search, every tie broken
    by ticker.
    """
    tickers = sorted(caps)
    gamma = float(policy["risk_aversion_gamma"])
    phi = float(policy["uncertainty_aversion_phi"])
    delta = float(policy["downside_aversion_delta"])
    rate = float(policy["cost_rate_per_side"])
    hd = float(policy["huber_delta"])
    tail = float(policy["downside_tail_factor"])
    cw = dict(current_weight or {})
    charge_cost = current_weight is not None
    diag = {tk: (sigma_forecast.get(tk) or 0.0) ** 2 for tk in tickers}

    def _parts(w: dict, sw: dict) -> tuple:
        cov_var = max(0.0, sum((w.get(i) or 0.0) * sw[i] for i in cov_included))
        unc_var = sum(diag[tk] * (w.get(tk) or 0.0) ** 2 for tk in tickers)
        return cov_var, unc_var

    def _cost(w: dict) -> float:
        if not charge_cost:
            return 0.0
        total = 0.0
        for tk in set(w) | set(cw):
            x = abs((w.get(tk) or 0.0) - (cw.get(tk) or 0.0))
            total += (x * x / (2 * hd)) if x <= hd else (x - hd / 2.0)
        return rate * total

    def _cost_grad(tk: str, wv: float) -> float:
        if not charge_cost:
            return 0.0
        x = wv - (cw.get(tk) or 0.0)
        return rate * (x / hd if abs(x) <= hd else (1.0 if x > 0 else -1.0))

    def _utility(w: dict) -> float:
        sw = _quad_form(w, cov_h, cov_included)
        cov_var, unc_var = _parts(w, sw)
        er = sum((mu.get(tk) or 0.0) * (w.get(tk) or 0.0) for tk in tickers)
        q05 = er - Z05 * tail * math.sqrt(cov_var + unc_var)
        return (er - 0.5 * gamma * cov_var - 0.5 * phi * unc_var
                - delta * max(0.0, -q05) - _cost(w))

    if charge_cost:
        w = feasible_start(cw, caps=caps, sector_of=sector_of, policy=policy, meta=meta)
    else:
        w = {tk: 0.0 for tk in tickers}
    max_gross = float(policy.get("max_gross_exposure", 1.0))

    iterations = 0
    gap = None
    reason = "ITERATION_LIMIT"
    for iterations in range(1, int(policy["max_iterations"]) + 1):
        sw = _quad_form(w, cov_h, cov_included)
        cov_var, unc_var = _parts(w, sw)
        sd_total = math.sqrt(cov_var + unc_var)
        er = sum((mu.get(tk) or 0.0) * (w.get(tk) or 0.0) for tk in tickers)
        shortfall_active = (er - Z05 * tail * sd_total) < 0.0
        grad = {}
        for tk in tickers:
            # d(cov_var)/dw_i = 2*(Sigma_h w)_i ; d(unc_var)/dw_i = 2*s_i^2*w_i
            dcov = 2.0 * sw.get(tk, 0.0)
            dunc = 2.0 * diag[tk] * w[tk]
            g = (mu.get(tk) or 0.0) - 0.5 * gamma * dcov - 0.5 * phi * dunc
            if shortfall_active:
                dq = (mu.get(tk) or 0.0)
                if sd_total > 0:
                    dq -= Z05 * tail * (dcov + dunc) / (2.0 * sd_total)
                g += delta * dq
            grad[tk] = g - _cost_grad(tk, w[tk])
        v = _linear_oracle(grad, candidates=candidates, caps=caps,
                           sector_of=sector_of, policy=policy, meta=meta)
        d = {tk: v[tk] - w[tk] for tk in tickers}
        gap = sum(grad[tk] * d[tk] for tk in tickers)
        if gap <= float(policy["convergence_tolerance"]):
            reason = "DUALITY_GAP_WITHIN_TOLERANCE"
            break
        # Every quantity in the objective is a QUADRATIC along the ray w + a*d,
        # so its coefficients are computed once per iteration and each candidate
        # step is then O(1) - apart from the smoothed cost, which stays O(n).
        # Without this the line search would dominate the whole solve.
        sd_vec = _quad_form(d, cov_h, cov_included)
        a_cc = sum(d.get(i, 0.0) * sd_vec[i] for i in cov_included)
        b_cc = 2.0 * sum(w.get(i, 0.0) * sd_vec[i] for i in cov_included)
        a_uu = sum(diag[tk] * d[tk] * d[tk] for tk in tickers)
        b_uu = 2.0 * sum(diag[tk] * w[tk] * d[tk] for tk in tickers)
        md = sum((mu.get(tk) or 0.0) * d[tk] for tk in tickers)

        def _u_at(a: float) -> float:
            cv = max(0.0, cov_var + a * b_cc + a * a * a_cc)
            uv = max(0.0, unc_var + a * b_uu + a * a * a_uu)
            e = er + a * md
            q = e - Z05 * tail * math.sqrt(cv + uv)
            u = e - 0.5 * gamma * cv - 0.5 * phi * uv - delta * max(0.0, -q)
            if charge_cost:
                u -= _cost({tk: w[tk] + a * d[tk] for tk in tickers})
            return u

        n_grid = int(policy["line_search_grid"])
        cands_a = sorted({0.0, 1.0, 2.0 / (iterations + 2.0)}
                         | {k / n_grid for k in range(n_grid + 1)})
        best_a, best_u = 0.0, _u_at(0.0)
        for a in cands_a:
            u = _u_at(a)
            if u > best_u + 1e-18:
                best_u, best_a = u, a
        # Refine inside the bracketing grid cell so the step is not limited to
        # the grid's resolution.
        span = 1.0 / n_grid
        for _ in range(4):
            span /= 4.0
            for a in (best_a - 2 * span, best_a - span,
                      best_a + span, best_a + 2 * span):
                if 0.0 <= a <= 1.0:
                    u = _u_at(a)
                    if u > best_u + 1e-18:
                        best_u, best_a = u, a
        if best_a <= 0:
            # For a concave objective over a convex set, the best feasible
            # direction offering no improvement at ANY step size means we are at
            # the optimum to within the line search's resolution. That is a
            # genuine stopping condition, not a failure, so it is named.
            reason = "NO_IMPROVING_FEASIBLE_STEP"
            break
        w = {tk: w[tk] + best_a * d[tk] for tk in tickers}

    # --- pairwise polish ---------------------------------------------------- #
    # Plain Frank-Wolfe moves toward a VERTEX, so when the optimum sits in the
    # interior of a face it zig-zags and its duality gap crawls. Shifting weight
    # directly from the worst held name to the best available one - a pairwise
    # step - removes exactly that stall, and every step stays inside the feasible
    # set by construction.
    polish_moves = 0
    for _ in range(int(policy["polish_rounds"])):
        sw = _quad_form(w, cov_h, cov_included)
        cov_var, unc_var = _parts(w, sw)
        sd_total = math.sqrt(cov_var + unc_var)
        er = sum((mu.get(tk) or 0.0) * (w.get(tk) or 0.0) for tk in tickers)
        shortfall_active = (er - Z05 * tail * sd_total) < 0.0
        grad = {}
        for tk in tickers:
            dcov = 2.0 * sw.get(tk, 0.0)
            dunc = 2.0 * diag[tk] * w[tk]
            g = (mu.get(tk) or 0.0) - 0.5 * gamma * dcov - 0.5 * phi * dunc
            if shortfall_active:
                dq = (mu.get(tk) or 0.0)
                if sd_total > 0:
                    dq -= Z05 * tail * (dcov + dunc) / (2.0 * sd_total)
                g += delta * dq
            grad[tk] = g - _cost_grad(tk, w[tk])

        sector_used: dict = {}
        for tk in tickers:
            sec = sector_of.get(tk) or "Unknown"
            sector_used[sec] = sector_used.get(sec, 0.0) + w[tk]
        invested = sum(w.values())
        sector_cap = float(policy["sector_cap_fraction"])
        # The weight vector is fixed while the candidate moves are scored, so the
        # cross-asset group totals are summed ONCE per iteration, not per pair.
        groups_iter = _groups_of(w, meta)

        def _room(tk: str, donor_sec: str, donor: Optional[str] = None) -> float:
            sec = sector_of.get(tk) or "Unknown"
            r = caps.get(tk, 0.0) - w[tk]
            if sec != donor_sec:
                r = min(r, sector_cap - sector_used.get(sec, 0.0))
            r = min(r, _xroom(tk, w, meta, policy, donor, groups=groups_iter))
            return max(0.0, r)

        givers = [tk for tk in tickers if w[tk] > 0]
        best = None
        for j in givers:
            sec_j = sector_of.get(j) or "Unknown"
            for i in tickers:
                if i == j:
                    continue
                diff = grad[i] - grad[j]
                if diff <= 0:
                    continue
                t_max = min(w[j], _room(i, sec_j, j))
                if t_max <= 0:
                    continue
                if best is None or diff > best[0]:
                    best = (diff, i, j, t_max)
        # Cash is itself a destination: giving weight up without buying anything
        # is admissible whenever the marginal name is destroying utility.
        cash_move = None
        for j in givers:
            if -grad[j] > 0 and (cash_move is None or -grad[j] > cash_move[0]):
                cash_move = (-grad[j], None, j, w[j])
        if cash_move and (best is None or cash_move[0] > best[0]):
            best = cash_move
        # Buying from cash is admissible too, whenever budget remains.
        if invested < max_gross:
            for i in tickers:
                if grad[i] <= 0:
                    continue
                t_max = min(_room(i, "\x00"), max_gross - invested)
                if t_max > 0 and (best is None or grad[i] > best[0]):
                    best = (grad[i], i, None, t_max)
        if best is None or best[0] <= float(policy["convergence_tolerance"]):
            reason = "PAIRWISE_OPTIMAL"
            break
        _, i, j, t_max = best
        d = {tk: 0.0 for tk in tickers}
        if i is not None:
            d[i] = 1.0
        if j is not None:
            d[j] = -1.0
        sd_vec = _quad_form(d, cov_h, cov_included)
        a_cc = sum(d.get(k, 0.0) * sd_vec[k] for k in cov_included)
        b_cc = 2.0 * sum(w.get(k, 0.0) * sd_vec[k] for k in cov_included)
        a_uu = sum(diag[tk] * d[tk] * d[tk] for tk in tickers)
        b_uu = 2.0 * sum(diag[tk] * w[tk] * d[tk] for tk in tickers)
        md = sum((mu.get(tk) or 0.0) * d[tk] for tk in tickers)

        def _u_t(t: float) -> float:
            cv = max(0.0, cov_var + t * b_cc + t * t * a_cc)
            uv = max(0.0, unc_var + t * b_uu + t * t * a_uu)
            e = er + t * md
            q = e - Z05 * tail * math.sqrt(cv + uv)
            u = e - 0.5 * gamma * cv - 0.5 * phi * uv - delta * max(0.0, -q)
            if charge_cost:
                u -= _cost({tk: w[tk] + t * d[tk] for tk in tickers})
            return u

        n_grid = int(policy["line_search_grid"])
        best_t, best_u = 0.0, _u_t(0.0)
        for k in range(n_grid + 1):
            t = t_max * k / n_grid
            u = _u_t(t)
            if u > best_u + 1e-18:
                best_u, best_t = u, t
        span = t_max / n_grid
        for _ in range(4):
            span /= 4.0
            for t in (best_t - 2 * span, best_t - span,
                      best_t + span, best_t + 2 * span):
                if 0.0 <= t <= t_max:
                    u = _u_t(t)
                    if u > best_u + 1e-18:
                        best_u, best_t = u, t
        if best_t <= 0:
            reason = "PAIRWISE_OPTIMAL"
            break
        w = {tk: w[tk] + best_t * d[tk] for tk in tickers}
        polish_moves += 1

    floor = float(policy["min_reported_weight"])
    w = {tk: (0.0 if v < floor else round(v, 10)) for tk, v in w.items()}
    utility_before_floor = _utility(w)

    # Minimum position size. Applied once, after the optimum is found, so the
    # objective is never distorted while it is being solved.
    min_w = float(policy["min_position_weight"])
    dust = sorted(tk for tk, v in w.items() if 0.0 < v < min_w)
    dust_weight = sum(w[tk] for tk in dust)
    for tk in dust:
        w[tk] = 0.0

    return {"weights": w, "iterations": iterations,
            "polish_moves": polish_moves,
            "frank_wolfe_gap": _r(gap, 12), "utility": _utility(w),
            "utility_before_minimum_position_filter": _r(utility_before_floor, 8),
            "min_position_weight": min_w,
            "dust_positions_dropped": len(dust),
            "dust_weight_reallocated_to_cash": _r(dust_weight, 6),
            "dust_tickers": dust,
            "convergence_reason": reason,
            "converged": reason in ("DUALITY_GAP_WITHIN_TOLERANCE",
                                    "NO_IMPROVING_FEASIBLE_STEP",
                                    "PAIRWISE_OPTIMAL")}


# --------------------------------------------------------------------------- #
# Portfolio-level economics
# --------------------------------------------------------------------------- #
def portfolio_economics(*, weights: dict, mu: dict, sigma_forecast: dict,
                        cov_h: dict, cov_included: list, policy: dict,
                        horizon: int) -> dict:
    """Expected return, risk, downside and utility of ONE weight vector.

    Every number is reported on the SAME basis for the current portfolio, the
    zero-base target and the implementable target, so the three are comparable by
    construction rather than by coincidence. ``cov_h`` is already horizon-scaled.
    """
    gamma = float(policy["risk_aversion_gamma"])
    phi = float(policy["uncertainty_aversion_phi"])
    delta = float(policy["downside_aversion_delta"])
    invested = sum(max(0.0, v) for v in weights.values())
    exp_excess = sum((weights.get(tk) or 0.0) * (mu.get(tk) or 0.0)
                     for tk in weights)
    cash_w = max(0.0, 1.0 - invested)
    exp_total = exp_excess + cash_w * CASH_RETURN

    sw = _quad_form(weights, cov_h, cov_included)
    cov_var = max(0.0, sum((weights.get(i) or 0.0) * sw[i]
                           for i in cov_included))
    covered = sum(max(0.0, weights.get(i) or 0.0) for i in cov_included)
    coverage = (covered / invested) if invested > 0 else 0.0
    unc_var = sum(((weights.get(tk) or 0.0) ** 2)
                  * (sigma_forecast.get(tk) or 0.0) ** 2 for tk in weights)
    tail = float(policy["downside_tail_factor"])
    total_var = cov_var + unc_var
    sd = math.sqrt(total_var)
    q05 = exp_excess - Z05 * tail * sd
    vol_state = "AVAILABLE" if cov_included else "UNAVAILABLE"

    utility = (exp_excess - 0.5 * gamma * cov_var - 0.5 * phi * unc_var
               - delta * max(0.0, -q05))
    return {
        "horizon_sessions": int(horizon),
        "invested_weight": _r(invested, 6),
        "cash_weight": _r(cash_w, 6),
        "position_count": sum(1 for v in weights.values() if v > 0),
        "expected_excess_return": _r(exp_excess, 6),
        "expected_return": _r(exp_total, 6),
        "cash_return_assumed": CASH_RETURN,
        "expected_volatility_horizon": _r(sd, 6) if sd > 0 else None,
        "expected_volatility_annualised": (
            _r(math.sqrt(total_var / max(1, horizon) * _TRADING_DAYS_YEAR), 6)
            if total_var > 0 else None),
        "volatility_state": vol_state,
        "covariance_coverage": _r(coverage, 4),
        "covariance_volatility_horizon": (_r(math.sqrt(cov_var), 6)
                                          if cov_var > 0 else None),
        "forecast_uncertainty_horizon": (_r(math.sqrt(unc_var), 6)
                                         if unc_var > 0 else None),
        "expected_downside_q05": _r(q05, 6),
        "downside_quantile": DOWNSIDE_QUANTILE,
        "downside_tail_factor": tail,
        "expected_net_utility": _r(utility, 8),
        "utility_terms": {
            "expected_excess_return": _r(exp_excess, 8),
            "covariance_risk_penalty": _r(-0.5 * gamma * cov_var, 8),
            "forecast_uncertainty_penalty": _r(-0.5 * phi * unc_var, 8),
            "downside_shortfall_penalty": _r(-delta * max(0.0, -q05), 8),
        },
    }


def transition_economics(*, current: dict, target: dict, nav: float,
                         policy: dict) -> dict:
    """Turnover and transaction cost of moving ``current`` to ``target``.

    Cost depends ONLY on the difference between the two weight vectors, so an
    unchanged portfolio costs exactly nothing. That is asserted directly by
    ``tests/test_release30_zero_base_allocator.py`` rather than left to
    inspection.
    """
    rate = float(policy["cost_rate_per_side"])
    names = sorted(set(current) | set(target))
    traded = sum(abs((target.get(tk) or 0.0) - (current.get(tk) or 0.0))
                 for tk in names)
    one_way = traded / 2.0
    cost_weight = traded * rate
    return {
        "one_way_turnover": _r(one_way, 6),
        "two_way_traded_weight": _r(traded, 6),
        "transaction_cost_weight": _r(cost_weight, 8),
        "transaction_cost_dollars": _money(cost_weight * float(nav or 0.0)),
        "cost_rate_per_side": rate,
        "cost_bps_per_side": float(policy["cost_bps_per_side"]),
        "cost_basis": "ABSOLUTE_WEIGHT_CHANGE_TIMES_PER_SIDE_RATE",
        "names_traded": sum(1 for tk in names
                            if abs((target.get(tk) or 0.0)
                                   - (current.get(tk) or 0.0))
                            > float(policy["material_weight_delta"])),
    }


def transition_path(*, current: dict, ideal: dict, mu: dict,
                    sigma_forecast: dict, cov_h: dict,
                    cov_included: list, policy: dict, horizon: int,
                    nav: float, steps: int = 20) -> list:
    """The economics of moving a FRACTION of the way from current to ideal.

    Reported as explanation, not as the answer: it shows the operator the exact
    point at which the next unit of transition stops paying for itself. Because
    the feasible set is convex, every point on this path is itself feasible.
    """
    out = []
    names = sorted(set(current) | set(ideal))
    for k in range(steps + 1):
        a = k / float(steps)
        w = {tk: (1 - a) * (current.get(tk) or 0.0) + a * (ideal.get(tk) or 0.0)
             for tk in names}
        econ = portfolio_economics(weights=w, mu=mu, sigma_forecast=sigma_forecast,
                                   cov_h=cov_h, cov_included=cov_included,
                                   policy=policy, horizon=horizon)
        tr = transition_economics(current=current, target=w, nav=nav,
                                  policy=policy)
        out.append({
            "fraction": _r(a, 4),
            "expected_net_utility": econ["expected_net_utility"],
            "transaction_cost_weight": tr["transaction_cost_weight"],
            "net_of_cost_utility": _r((econ["expected_net_utility"] or 0.0)
                                      - (tr["transaction_cost_weight"] or 0.0), 8),
            "one_way_turnover": tr["one_way_turnover"],
        })
    return out


# --------------------------------------------------------------------------- #
# Constraint verification
# --------------------------------------------------------------------------- #
def verify_constraints(*, weights: dict, caps: dict, sector_of: dict,
                       policy: dict, meta: Optional[dict] = None) -> dict:
    """Check the produced weights against every declared constraint.

    A target that violates its own constraints must never be presented as valid,
    so the optimiser's output is verified independently of the optimiser.
    """
    tol = 1.0e-9
    violations: list = []
    invested = sum(weights.values())
    max_gross = float(policy.get("max_gross_exposure", 1.0))
    if invested > max_gross + tol:
        violations.append({"code": "GROSS_EXPOSURE_ABOVE_100_PCT",
                           "value": _r(invested, 8)})
    # Release 50 - the cross-asset limits, verified through the ONE definition owner.
    cross = {}
    if meta:
        from . import constrained_reallocation as _cr
        g = _cr.group_weights({k: v for k, v in weights.items() if v > 0}, meta)
        for ac, v in sorted(g["by_class"].items()):
            if v > _cr.asset_class_cap(policy, ac) + tol:
                violations.append({"code": "ASSET_CLASS_CAP_BREACH", "asset_class": ac,
                                   "value": _r(v, 8)})
        for sl, v in sorted(g["by_sleeve"].items()):
            if v > _cr.sleeve_cap(policy, sl) + tol:
                violations.append({"code": "SLEEVE_CAP_BREACH", "sleeve_id": sl,
                                   "value": _r(v, 8)})
        if g["non_usd"] > float(policy.get("non_usd_currency_cap", 1.0)) + tol:
            violations.append({"code": "CURRENCY_CAP_BREACH", "value": _r(g["non_usd"], 8)})
        if g["collateral"] > float(policy.get("collateral_cap_fraction", 1.0)) + tol:
            violations.append({"code": "COLLATERAL_CAP_BREACH", "value": _r(g["collateral"], 8)})
        cross = {"asset_class_weights": {k: _r(v, 6) for k, v in sorted(g["by_class"].items())},
                 "sleeve_weights": {k: _r(v, 6) for k, v in sorted(g["by_sleeve"].items())},
                 "non_usd_exposure": _r(g["non_usd"], 6),
                 "collateral_weight": _r(g["collateral"], 6)}
    for tk, v in sorted(weights.items()):
        if v < -tol:
            violations.append({"code": "NEGATIVE_WEIGHT", "ticker": tk,
                               "value": _r(v, 8)})
        cap = caps.get(tk, 0.0)
        if v > cap + tol:
            violations.append({"code": "NAME_CAP_BREACH", "ticker": tk,
                               "value": _r(v, 8), "cap": _r(cap, 8)})
    sector_weight: dict = {}
    for tk, v in weights.items():
        sector_weight[sector_of.get(tk) or "Unknown"] = (
            sector_weight.get(sector_of.get(tk) or "Unknown", 0.0) + v)
    for sec, v in sorted(sector_weight.items()):
        if v > float(policy["sector_cap_fraction"]) + tol:
            violations.append({"code": "SECTOR_CAP_BREACH", "sector": sec,
                               "value": _r(v, 8)})
    return {
        "valid": not violations,
        "violations": violations,
        "long_only": all(v >= -tol for v in weights.values()),
        "gross_exposure": _r(invested, 6),
        "cash_weight": _r(max(0.0, 1.0 - invested), 6),
        "sector_weights": {k: _r(v, 6) for k, v in sorted(sector_weight.items())},
        "max_name_weight_observed": _r(max(weights.values()) if weights else 0.0, 6),
        "checked": ["long_only", "gross_exposure<=1", "name_cap",
                    "liquidity_participation_cap", "sector_cap",
                    "asset_class_cap", "sleeve_cap", "non_usd_currency_cap",
                    "collateral_cap", "unit_granularity"],
        **cross,
    }


def _safety() -> dict:
    return {"badges": list(SAFETY_BADGES), "paper_only": True,
            "creates_orders": False, "creates_signals": False,
            "creates_decisions": False, "mutates_holdings": False,
            "mutates_cash": False, "promotes_models": False,
            "automation_enabled": False, "manual_review_required": True}


# --------------------------------------------------------------------------- #
# The one entry point
# --------------------------------------------------------------------------- #
def build_allocation(*, input_contract: dict,
                     policy: Optional[dict] = None) -> dict:
    """Compute the zero-base target, the implementable target and the economics
    that separate them. Pure and deterministic; never raises on incomplete input.

    ``input_contract`` carries: ``eligible_market_date``, ``active_book_id``,
    ``nav``, ``candidates`` (the eligible universe with sector / adv), ``mu`` /
    ``sigma_forecast`` / ``downside`` from the forecast owner, ``aligned_returns``
    for the canonical covariance builder, and ``current_weights``.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    ic = input_contract or {}
    horizon = int(pol["policy_horizon_sessions"])

    if not ic.get("active_book_id"):
        return _empty(pol, ic, STATE_NO_ACTIVE_BOOK,
                      [{"code": "NO_ACTIVE_BOOK"}])
    blockers: list = []
    nav = _f(ic.get("nav"))
    if not ic.get("eligible_market_date"):
        blockers.append({"code": "MISSING_ELIGIBLE_MARKET_DATE"})
    if nav is None or nav <= 0:
        blockers.append({"code": "MISSING_OR_NONPOSITIVE_NAV"})
    candidates = [dict(c) for c in (ic.get("candidates") or []) if c.get("ticker")]
    if not candidates:
        blockers.append({"code": "NO_ELIGIBLE_CANDIDATES"})
    mu = {k: (_f(v) or 0.0) for k, v in (ic.get("mu") or {}).items()}
    if not mu:
        blockers.append({"code": "NO_EXPECTED_RETURNS",
                         "detail": "the forward-return forecast owner produced "
                                   "no expected excess return for this date"})
    if not ic.get("forecast_model_spec_hash"):
        blockers.append({"code": "MISSING_FORECAST_MODEL_HASH"})
    if blockers:
        return _empty(pol, ic, STATE_BLOCKED, blockers)

    sigma_f = {k: (_f(v) or 0.0) for k, v in (ic.get("sigma_forecast") or {}).items()}
    down = {k: (_f(v) or 0.0) for k, v in (ic.get("downside") or {}).items()}
    sector_of = {c["ticker"]: (c.get("sector") or "Unknown") for c in candidates}
    current = {k: (_f(v) or 0.0)
               for k, v in (ic.get("current_weights") or {}).items()}

    # Candidates without a forecast cannot be judged, so they are excluded BY
    # NAME rather than being given a zero forecast, which would silently assert
    # that we expect them to be exactly average.
    unforecast = sorted(c["ticker"] for c in candidates if c["ticker"] not in mu)
    candidates = [c for c in candidates if c["ticker"] in mu]
    caps = name_caps(candidates=candidates, nav=nav, policy=pol)
    # Release 50 - instrument metadata (equity defaults for rows without it).
    meta = _meta(candidates + [dict(p, ticker=k) for k, p in
                               ((ic.get("current_instruments") or {}).items())])
    # The solver sees the metadata only when a cross-asset limit can bind; the
    # equity-only solve therefore runs exactly as before Release 50 (the reports
    # below still use the full metadata).
    solve_meta = _solve_meta(meta, pol)

    # ONE covariance, from the canonical risk owner, over the union of the
    # candidate set and everything currently held - the current portfolio must be
    # priced on the same matrix as the target or the comparison is meaningless.
    from . import holding_opportunity_cost as _hoc
    cov_universe = sorted(set(caps) | set(current))
    built = _hoc.build_covariance(tickers=cov_universe,
                                  aligned_returns=ic.get("aligned_returns") or {},
                                  policy=pol)
    cov_included = list(built["included_tickers"])
    # Scale the DAILY covariance to the policy horizon exactly once, here, so no
    # downstream routine holds a second opinion about the horizon.
    cov_h = {i: {j: v * float(horizon) for j, v in row.items()}
             for i, row in built["covariance"].items()}

    zb = optimise(candidates=candidates, mu=mu, sigma_forecast=sigma_f,
                  cov_h=cov_h, cov_included=cov_included, caps=caps,
                  sector_of=sector_of, policy=pol, current_weight=None, meta=solve_meta)
    impl = optimise(candidates=candidates, mu=mu, sigma_forecast=sigma_f,
                    cov_h=cov_h, cov_included=cov_included, caps=caps,
                    sector_of=sector_of, policy=pol, current_weight=current, meta=solve_meta)

    def _econ(w):
        return portfolio_economics(weights=w, mu=mu, sigma_forecast=sigma_f,
                                   cov_h=cov_h, cov_included=cov_included,
                                   policy=pol, horizon=horizon)

    zb_w, impl_w = zb["weights"], impl["weights"]
    contrib = _hoc.compute_risk_contributions(
        weights={k: v for k, v in zb_w.items() if v > 0},
        aligned_returns=ic.get("aligned_returns") or {}, policy=pol)

    rows = _rows(zb_w, candidates=candidates, mu=mu, sigma_forecast=sigma_f,
                 downside=down, contributions=contrib.get("contributions") or {},
                 caps=caps, nav=nav, current=current, policy=pol,
                 per_horizon=ic.get("expected_returns_by_horizon") or {})
    impl_rows = _rows(impl_w, candidates=candidates, mu=mu,
                      sigma_forecast=sigma_f, downside=down, contributions={},
                      caps=caps, nav=nav, current=current, policy=pol,
                      per_horizon=ic.get("expected_returns_by_horizon") or {})

    result = {
        "schema_version": SCHEMA_VERSION,
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "state": STATE_READY if zb["converged"] else STATE_DEGRADED,
        "state_vocabulary": list(STATE_VOCAB),
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "nav": _money(nav),
        "policy_horizon_sessions": horizon,
        "objective": objective_declaration(pol),
        "cash_policy": {"cash_return": CASH_RETURN,
                        "policy": CASH_RETURN_POLICY, "doc": CASH_RETURN_DOC},
        "position_count_policy": {
            "policy": POSITION_COUNT_POLICY,
            "legacy_target_position_count": LEGACY_TARGET_POSITION_COUNT,
            "doc": ("The legacy 25-name target is a construction default of the "
                    "equal-weight book, not an investment constraint. It is "
                    "retained and reported, never silently deleted, and it does "
                    "not bind the zero-base objective."),
        },
        "universe": {
            "eligible_candidates": len(candidates),
            "excluded_without_forecast": unforecast,
            "covariance_included": len(cov_included),
            "covariance_excluded": len(built["excluded_tickers"]),
            "covariance_observations": built["observations_used"],
            "current_holdings_in_candidate_set": sorted(
                tk for tk in current if tk in caps),
            "current_holdings_outside_candidate_set": sorted(
                tk for tk in current if tk not in caps),
        },
        "zero_base_target": {
            "target_kind": TARGET_ZERO_BASE,
            "doc": ("Constructed as if every investable dollar were cash. The "
                    "current portfolio is not an input to this optimisation."),
            "rows": rows,
            "economics": _econ(zb_w),
            "constraints": verify_constraints(weights=zb_w, caps=caps,
                                              sector_of=sector_of, policy=pol, meta=meta),
            "allocation_by_asset_class": _alloc_by(zb_w, meta, "asset_class"),
            "allocation_by_sleeve": _alloc_by(zb_w, meta, "sleeve_id"),
            "optimiser": {k: zb[k] for k in
                          ("iterations", "polish_moves",
                           "frank_wolfe_gap", "converged",
                           "convergence_reason", "min_position_weight",
                           "dust_positions_dropped",
                           "dust_weight_reallocated_to_cash",
                           "utility_before_minimum_position_filter")},
            "risk_contribution_state": contrib.get("state"),
        },
        "implementable_target": {
            "target_kind": TARGET_IMPLEMENTABLE,
            "doc": ("The same objective solved FROM the current portfolio with "
                    "transaction costs inside the economics. Incumbency enters "
                    "here and only here."),
            "rows": impl_rows,
            "economics": _econ(impl_w),
            "constraints": verify_constraints(weights=impl_w, caps=caps,
                                              sector_of=sector_of, policy=pol, meta=meta),
            "allocation_by_asset_class": _alloc_by(impl_w, meta, "asset_class"),
            "allocation_by_sleeve": _alloc_by(impl_w, meta, "sleeve_id"),
            "optimiser": {k: impl[k] for k in
                          ("iterations", "polish_moves",
                           "frank_wolfe_gap", "converged",
                           "convergence_reason", "min_position_weight",
                           "dust_positions_dropped",
                           "dust_weight_reallocated_to_cash",
                           "utility_before_minimum_position_filter")},
        },
        "current_portfolio": {
            "weights": {k: _r(v, 6) for k, v in sorted(current.items())},
            "economics": _econ(current),
            "allocation_by_asset_class": _alloc_by(current, meta, "asset_class"),
        },
        "cross_asset": {
            "instrument_metadata_supplied": bool(any(
                c.get("asset_class") or c.get("instrument_type") for c in candidates)),
            "forced_diversification": False,
            "long_only": True,
            "caps_owner": "engine.constrained_reallocation (ONE cross-asset definition)",
        },
        "comparison": _comparison(current=current, zero_base=zb_w,
                                  implementable=impl_w, nav=nav, policy=pol,
                                  sector_of=sector_of),
        "transition": {
            "current_to_zero_base": transition_economics(
                current=current, target=zb_w, nav=nav, policy=pol),
            "current_to_implementable": transition_economics(
                current=current, target=impl_w, nav=nav, policy=pol),
            "path_current_to_zero_base": transition_path(
                current=current, ideal=zb_w, mu=mu, sigma_forecast=sigma_f,
                cov_h=cov_h, cov_included=cov_included, policy=pol,
                horizon=horizon, nav=nav),
            "mandatory_exits": sorted(tk for tk in current if tk not in caps),
            "mandatory_exit_doc": (
                "A held name that is not in the eligible candidate set cannot "
                "appear in any target. Its exit is not an economic choice, so "
                "its cost is counted but never weighed against a hurdle."),
        },
        "forecast_model_spec_hash": ic.get("forecast_model_spec_hash"),
        "feature_snapshot_hash": ic.get("feature_snapshot_hash"),
        "policy": pol,
        "blockers": [],
        "warnings": ([{"code": "CANDIDATES_WITHOUT_FORECAST",
                       "tickers": unforecast}] if unforecast else []),
        "safety": _safety(),
    }
    result["allocation_hash"] = stable_hash(result)
    return result


def _alloc_by(weights: dict, meta: dict, key: str) -> dict:
    from . import constrained_reallocation as _cr
    return _cr.allocation_by({k: v for k, v in (weights or {}).items() if v > 0}, meta or {}, key)


def objective_declaration(policy: dict) -> dict:
    """The exact mathematical objective, in one place, in the payload."""
    return {
        "objective_version": OBJECTIVE_VERSION,
        "formula": ("maximise  (mu - delta*downside)'w  -  0.5 * w'(gamma*Sigma "
                    "+ phi*diag(sigma_forecast^2))w  -  transaction_cost(w, "
                    "w_current)"),
        "terms": {
            "mu": "expected EXCESS return per name at the policy horizon",
            "Sigma": "horizon-scaled daily covariance from "
                     "engine.holding_opportunity_cost.build_covariance",
            "sigma_forecast": "per-name forecast uncertainty from "
                              "engine.return_forecast",
            "downside": "max(0, -q05) of the forecast distribution",
            "transaction_cost": "absent for the zero-base target; the canonical "
                                "per-side rate for the implementable target",
        },
        "constraints": ["long only (w >= 0)", "gross exposure <= 100%",
                        "cash = 1 - sum(w) >= 0",
                        "per-name weight cap", "per-sector weight cap",
                        "liquidity participation cap on average dollar volume",
                        "eligible universe only",
                        # Release 50
                        "per-asset-class weight cap", "per-sleeve weight cap",
                        "non-USD currency exposure cap", "futures collateral cap",
                        "unit granularity at NAV"],
        "risk_aversion_gamma": policy["risk_aversion_gamma"],
        "uncertainty_aversion_phi": policy["uncertainty_aversion_phi"],
        "downside_aversion_delta": policy["downside_aversion_delta"],
        "downside_tail_factor": policy["downside_tail_factor"],
        "downside_quantile": DOWNSIDE_QUANTILE,
        "risk_price_source": policy.get("risk_price_source"),
        "risk_price_doc": (
            "gamma = mean / variance of the candidate model's realised per-period "
            "book excess return on WALK-FORWARD VALIDATION blocks, which is the "
            "risk price at which a fully-invested diversified book is exactly "
            "marginal. delta = max(0, tail_factor - 1), where tail_factor is how "
            "much fatter the MEASURED residual left tail is than a normal one - "
            "so a symmetric forecast distribution charges nothing extra and the "
            "variance term is never double counted."),
        "solver": "FRANK_WOLFE_LAMINAR_GREEDY_ORACLE",
    }


def _rows(weights: dict, *, candidates: list, mu: dict, sigma_forecast: dict,
          downside: dict, contributions: dict, caps: dict, nav: float,
          current: dict, policy: dict, per_horizon: dict) -> list:
    by_tk = {c["ticker"]: c for c in candidates}
    out = []
    for tk, w in weights.items():
        if w <= 0:
            continue
        c = by_tk.get(tk) or {}
        adv = _f(c.get("adv_dollar"))
        dollars = w * float(nav)
        out.append({
            "ticker": tk,
            "sector": c.get("sector") or "Unknown",
            "weight": _r(w, 6),
            "dollar_allocation": _money(dollars),
            "expected_excess_return": _r(mu.get(tk), 6),
            "expected_return_by_horizon": {
                str(h): _r((per_horizon.get(str(h)) or {}).get(tk), 6)
                for h in sorted(per_horizon, key=lambda x: int(x))},
            "forecast_uncertainty": _r(sigma_forecast.get(tk), 6),
            "downside_return_q05": _r(downside.get(tk), 6),
            "risk_contribution": _r(contributions.get(tk), 6),
            "adv_dollar": _money(adv),
            "adv_participation": (_r(dollars / adv, 6)
                                  if (adv and adv > 0) else None),
            "weight_cap": _r(caps.get(tk), 6),
            "cap_binding": bool(abs(w - (caps.get(tk) or 0.0)) < 1e-9),
            "current_weight": _r(current.get(tk, 0.0), 6),
            "weight_change": _r(w - (current.get(tk) or 0.0), 6),
        })
    out.sort(key=lambda r: (-(r["weight"] or 0.0), r["ticker"]))
    return out


def _comparison(*, current: dict, zero_base: dict, implementable: dict,
                nav: float, policy: dict, sector_of: dict) -> dict:
    tol = float(policy["material_weight_delta"])
    held = {tk for tk, v in current.items() if v > tol}
    zb = {tk for tk, v in zero_base.items() if v > tol}
    impl = {tk for tk, v in implementable.items() if v > tol}

    def _block(target_set, target_w, label):
        return {
            "target_kind": label,
            "retained": sorted(held & target_set),
            "removed": sorted(held - target_set),
            "new": sorted(target_set - held),
            "retained_count": len(held & target_set),
            "removed_count": len(held - target_set),
            "new_count": len(target_set - held),
            "position_count": len(target_set),
            "cash_weight": _r(max(0.0, 1.0 - sum(target_w.values())), 6),
            "cash_dollars": _money(max(0.0, 1.0 - sum(target_w.values()))
                                   * float(nav)),
        }

    return {
        "current": {
            "position_count": len(held),
            "cash_weight": _r(max(0.0, 1.0 - sum(current.values())), 6),
            "cash_dollars": _money(max(0.0, 1.0 - sum(current.values()))
                                   * float(nav)),
            "holdings": sorted(held),
        },
        "zero_base": _block(zb, zero_base, TARGET_ZERO_BASE),
        "implementable": _block(impl, implementable, TARGET_IMPLEMENTABLE),
        "zero_base_vs_implementable": {
            "in_both": sorted(zb & impl),
            "zero_base_only": sorted(zb - impl),
            "implementable_only": sorted(impl - zb),
            "doc": ("Names the zero-base target wants but the implementable "
                    "target does not are exactly the positions whose expected "
                    "improvement does not cover the cost of getting into them."),
        },
    }


def _empty(policy: dict, ic: dict, state: str, blockers: list) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "state": state,
        "state_vocabulary": list(STATE_VOCAB),
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "nav": _money(_f(ic.get("nav"))),
        "policy_horizon_sessions": int(policy["policy_horizon_sessions"]),
        "objective": objective_declaration(policy),
        "cash_policy": {"cash_return": CASH_RETURN, "policy": CASH_RETURN_POLICY,
                        "doc": CASH_RETURN_DOC},
        "zero_base_target": {"target_kind": TARGET_ZERO_BASE, "rows": [],
                             "economics": {}, "constraints": {}},
        "implementable_target": {"target_kind": TARGET_IMPLEMENTABLE, "rows": [],
                                 "economics": {}, "constraints": {}},
        "current_portfolio": {"weights": {}, "economics": {}},
        "comparison": {}, "transition": {},
        "policy": policy, "blockers": list(blockers), "warnings": [],
        "safety": _safety(),
        "allocation_hash": None,
    }
