r"""engine/alpha_capital_frontier.py - Release 56: the PURE kernel that turns an
alpha into a capital decision.

It answers two questions no existing owner answers, and it invents no
mathematics to do it:

    1. CASH DEPLOYMENT FRONTIER - what is the marginal best use of the NEXT
       $1,000 / $2,500 / $5,000 / 5% / 10% / 25% / 100% of NAV, and at which
       increment does cash stop losing and start winning?

    2. INCUMBENT OPPORTUNITY COST - what does holding the current book cost
       per policy horizon against the zero-base target, and how long must that
       edge persist before the switch pays for itself (the PAYBACK HORIZON)?

WHAT THIS MODULE DELIBERATELY DOES NOT OWN
------------------------------------------
There is no second optimiser here. The objective, the weights, the covariance,
the caps and the transaction-cost arithmetic all belong to
``engine.zero_base_allocator`` and ``engine.holding_opportunity_cost``, and this
kernel IMPORTS them. A ladder that solved its own portfolio would be a second
opinion about the best portfolio, and the two would disagree the first time a
risk price or a cost rate moved.

What the ladder adds is the CAPITAL AXIS. The allocator answers "which
portfolio"; the ladder answers "how many dollars of it, and does the next
dollar pay". Every point on the ladder is a feasible blend of weight vectors
the allocator already produced, so nothing here can propose a portfolio the
allocator would refuse.

CASH IS A REAL DESTINATION
--------------------------
Cash wins an increment whenever the increment's net-of-cost utility gain is not
positive, or when the eligible targets cannot absorb the money inside their own
caps. Both outcomes are first-class results with named reasons, not failures.

Pure: no I/O, no clock, no randomness, no network, no database. It creates no
signal, no target, no proposal, no decision, no order and no fill, and it
changes no holding, no cash and no NAV.
"""
from __future__ import annotations

from typing import Any, Optional

from . import zero_base_allocator as zba

CALCULATION_OWNER = "engine.alpha_capital_frontier"
SCHEMA_VERSION = "alpha_capital_frontier.v1"
PHASE = "R56"

#: The objective, the caps and the cost arithmetic have exactly one owner and
#: it is not this module. Published so a reader can check the claim.
OBJECTIVE_OWNER = zba.CALCULATION_OWNER
COVARIANCE_OWNER = "engine.holding_opportunity_cost.build_covariance"

TRADING_DAYS_YEAR = 252

# --------------------------------------------------------------------------- #
# Vocabularies
# --------------------------------------------------------------------------- #
DEST_EXISTING_HOLDING = "EXISTING_HOLDING"
DEST_NEW_EQUITY = "NEW_EQUITY"
DEST_OTHER_SLEEVE = "OTHER_VALIDATED_SLEEVE"
DEST_CASH = "CASH"
DESTINATION_VOCAB = (DEST_EXISTING_HOLDING, DEST_NEW_EQUITY, DEST_OTHER_SLEEVE,
                     DEST_CASH)

HURDLE_CLEARS = "HURDLE_CLEARS"
HURDLE_FAILS_NET_UTILITY = "HURDLE_FAILS_NEGATIVE_NET_OF_COST_UTILITY"
HURDLE_FAILS_NO_CAPACITY = "HURDLE_FAILS_NO_ELIGIBLE_DESTINATION_CAPACITY"
HURDLE_FAILS_BELOW_MIN_NOTIONAL = "HURDLE_FAILS_INCREMENT_BELOW_MIN_ORDER_NOTIONAL"
HURDLE_NOT_EVIDENCED = "HURDLE_NOT_EVIDENCED_NO_CALIBRATED_EXPECTED_RETURN"
HURDLE_VOCAB = (HURDLE_CLEARS, HURDLE_FAILS_NET_UTILITY, HURDLE_FAILS_NO_CAPACITY,
                HURDLE_FAILS_BELOW_MIN_NOTIONAL, HURDLE_NOT_EVIDENCED)

#: TWO different capital questions, never one.
#:
#: CASH_ONLY answers "should the cash we are holding be put to work?" - it buys
#: and never sells, so it can never spend more than the cash on hand and it
#: leaves every existing position exactly where it is.
#:
#: REDEPLOYMENT answers "should the BOOK be rotated?" - it walks the allocator's
#: own path, which sells as well as buys, and is funded by cash AND sales.
#:
#: Reporting one of these as the other is how a rebalance gets sold to an
#: operator as a cash decision. They are computed and labelled separately.
MODE_CASH_ONLY = "CASH_ONLY_BUYS_NO_SALES"
MODE_REDEPLOYMENT = "REDEPLOYMENT_BUYS_AND_SALES"
MODE_VOCAB = (MODE_CASH_ONLY, MODE_REDEPLOYMENT)

FUNDING_FROM_CASH = "FROM_AVAILABLE_CASH"
FUNDING_FROM_CASH_AND_SALES = "FROM_AVAILABLE_CASH_AND_SALES"
FUNDING_VOCAB = (FUNDING_FROM_CASH, FUNDING_FROM_CASH_AND_SALES)

PAYBACK_WITHIN_ONE_HORIZON = "PAYS_BACK_WITHIN_ONE_POLICY_HORIZON"
PAYBACK_MULTI_HORIZON = "REQUIRES_MULTIPLE_POLICY_HORIZONS"
PAYBACK_NEVER = "NEVER_PAYS_BACK_AT_THIS_COST_RATE"
PAYBACK_NOT_APPLICABLE = "NO_SWITCH_COST_NOTHING_TO_PAY_BACK"
PAYBACK_VOCAB = (PAYBACK_WITHIN_ONE_HORIZON, PAYBACK_MULTI_HORIZON,
                 PAYBACK_NEVER, PAYBACK_NOT_APPLICABLE)

#: The evidence lane a hurdle is judged on. Kept separate and permanently
#: labelled: a utility computed from a forecasting model the operator does not
#: run is EVIDENCE, and it may never be read as a governed capital instruction.
LANE_RESEARCH_UTILITY = "RESEARCH_UTILITY_MODEL"
LANE_GOVERNED_SCORE = "GOVERNED_SCORE_ELIGIBILITY"
LANE_VOCAB = (LANE_RESEARCH_UTILITY, LANE_GOVERNED_SCORE)

STATE_READY = "READY"
STATE_BLOCKED = "BLOCKED"
STATE_VOCAB = (STATE_READY, STATE_BLOCKED)

SAFETY_BADGES = ["PREVIEW ONLY", "READ ONLY", "RESEARCH ONLY", "NO ORDERS",
                 "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
                 "NO MODEL PROMOTION", "NO SLEEVE ACTIVATION"]

#: The canonical capital increments. Dollar rungs first (an operator thinks in
#: dollars for small money and in NAV share for large money), then NAV shares.
DEFAULT_INCREMENTS = (
    {"label": "$1,000", "kind": "DOLLARS", "amount": 1000.0},
    {"label": "$2,500", "kind": "DOLLARS", "amount": 2500.0},
    {"label": "$5,000", "kind": "DOLLARS", "amount": 5000.0},
    {"label": "5% NAV", "kind": "NAV_FRACTION", "fraction": 0.05},
    {"label": "10% NAV", "kind": "NAV_FRACTION", "fraction": 0.10},
    {"label": "25% NAV", "kind": "NAV_FRACTION", "fraction": 0.25},
    {"label": "100% NAV", "kind": "NAV_FRACTION", "fraction": 1.00},
)

#: Below this the increment cannot buy a sensible line in any name. Owned by the
#: desk in production and injected; this literal is the documented fallback.
DEFAULT_MIN_ORDER_NOTIONAL = 50.0


def default_increments() -> list[dict]:
    return [dict(x) for x in DEFAULT_INCREMENTS]


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _money(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


def _w(d: Optional[dict]) -> dict:
    out: dict = {}
    for k, v in (d or {}).items():
        fv = _f(v)
        if fv is not None and fv > 0.0:
            out[str(k)] = fv
    return out


def _blend(a: dict, b: dict, t: float) -> dict:
    names = set(a) | set(b)
    return {tk: (1.0 - t) * (a.get(tk) or 0.0) + t * (b.get(tk) or 0.0)
            for tk in names}


def _gross_buy(frm: dict, to: dict) -> float:
    names = set(frm) | set(to)
    return sum(max(0.0, (to.get(tk) or 0.0) - (frm.get(tk) or 0.0))
               for tk in names)


def _hhi(w: dict) -> Optional[float]:
    vals = [max(0.0, v) for v in w.values()]
    return round(sum(v * v for v in vals), 6) if vals else None


def concentration(weights: dict) -> dict:
    """Concentration facts of one weight vector (pure description)."""
    w = _w(weights)
    top = sorted(w.items(), key=lambda kv: -kv[1])
    return {
        "position_count": len(w),
        "max_name_weight": _r(top[0][1], 6) if top else None,
        "max_name_ticker": top[0][0] if top else None,
        "top5_weight": _r(sum(v for _k, v in top[:5]), 6) if top else None,
        "herfindahl_index": _hhi(w),
    }


# --------------------------------------------------------------------------- #
# Payback - the number that decides whether an edge is worth its switch cost
# --------------------------------------------------------------------------- #
def payback(*, gain_per_horizon: Optional[float], cost_weight: Optional[float],
            horizon_sessions: int) -> dict:
    """How long the edge must persist before the switch has paid for itself.

    This is the question the transition path implies and never states. A target
    that is better per horizon is NOT automatically worth buying: the switch is
    paid once, up front, and the edge is earned per horizon. Dividing one by the
    other gives the only number that decides the trade.
    """
    g = _f(gain_per_horizon)
    c = _f(cost_weight)
    h = int(horizon_sessions or 0)
    if c is None or c <= 0.0:
        return {"verdict": PAYBACK_NOT_APPLICABLE, "vocabulary": list(PAYBACK_VOCAB),
                "payback_horizons": 0.0, "payback_sessions": 0.0,
                "gain_per_horizon": _r(g, 8), "switch_cost_weight": _r(c, 8),
                "doc": "no weight changes, so nothing is paid and nothing is owed"}
    if g is None or g <= 0.0:
        return {"verdict": PAYBACK_NEVER, "vocabulary": list(PAYBACK_VOCAB),
                "payback_horizons": None, "payback_sessions": None,
                "gain_per_horizon": _r(g, 8), "switch_cost_weight": _r(c, 8),
                "doc": ("the move does not raise expected net utility, so no "
                        "holding period repays its cost")}
    horizons = c / g
    return {
        "verdict": (PAYBACK_WITHIN_ONE_HORIZON if horizons <= 1.0
                    else PAYBACK_MULTI_HORIZON),
        "vocabulary": list(PAYBACK_VOCAB),
        "payback_horizons": _r(horizons, 4),
        "payback_sessions": _r(horizons * h, 2),
        "payback_calendar_months": _r(horizons * h / 21.0, 2) if h else None,
        "gain_per_horizon": _r(g, 8),
        "switch_cost_weight": _r(c, 8),
        "doc": ("switch cost is paid once; the edge is earned per policy "
                "horizon. payback = cost / gain-per-horizon."),
    }


# --------------------------------------------------------------------------- #
# Incumbent opportunity cost
# --------------------------------------------------------------------------- #
def incumbent_opportunity_cost(*, current_weights: dict, zero_base_weights: dict,
                               implementable_weights: dict, mu: dict,
                               sigma_forecast: dict, cov_h: dict,
                               cov_included: list, policy: dict, horizon: int,
                               nav: Optional[float]) -> dict:
    """What the current book costs, per horizon and in dollars, against the two
    targets - and how long each switch takes to repay itself.

    The current book is given NO privilege here beyond the cost of leaving it.
    That cost is real and is charged in full; nothing else about incumbency
    enters the comparison.
    """
    cur = _w(current_weights)
    zb = _w(zero_base_weights)
    impl = _w(implementable_weights)
    navv = _f(nav) or 0.0

    def _econ(w):
        return zba.portfolio_economics(
            weights=w, mu=mu, sigma_forecast=sigma_forecast, cov_h=cov_h,
            cov_included=cov_included, policy=policy, horizon=int(horizon))

    e_cur, e_zb, e_impl = _econ(cur), _econ(zb), _econ(impl)
    per_year = (TRADING_DAYS_YEAR / float(horizon)) if horizon else None

    def _leg(target: dict, e_t: dict, kind: str) -> dict:
        tr = zba.transition_economics(current=cur, target=target, nav=navv,
                                      policy=policy)
        gain = ((e_t.get("expected_net_utility") or 0.0)
                - (e_cur.get("expected_net_utility") or 0.0))
        cost = tr.get("transaction_cost_weight") or 0.0
        return {
            "target_kind": kind,
            "utility_gap_per_horizon": _r(gain, 8),
            "utility_gap_dollars_per_horizon": _money(gain * navv),
            "utility_gap_annualised": (_r(gain * per_year, 6)
                                       if per_year else None),
            "utility_gap_dollars_annualised": (_money(gain * per_year * navv)
                                               if per_year else None),
            "expected_excess_return_gap": _r(
                (e_t.get("expected_excess_return") or 0.0)
                - (e_cur.get("expected_excess_return") or 0.0), 8),
            "risk_gap_horizon": _r(
                (e_t.get("expected_volatility_horizon") or 0.0)
                - (e_cur.get("expected_volatility_horizon") or 0.0), 8),
            "switch_cost_weight": tr.get("transaction_cost_weight"),
            "switch_cost_dollars": tr.get("transaction_cost_dollars"),
            "one_way_turnover": tr.get("one_way_turnover"),
            "names_traded": tr.get("names_traded"),
            "net_of_cost_first_horizon": _r(gain - cost, 8),
            "net_of_cost_first_horizon_dollars": _money((gain - cost) * navv),
            "payback": payback(gain_per_horizon=gain, cost_weight=cost,
                               horizon_sessions=int(horizon)),
            "target_concentration": concentration(target),
        }

    legs = {"zero_base": _leg(zb, e_zb, "ZERO_BASE_TARGET"),
            "implementable": _leg(impl, e_impl, "IMPLEMENTABLE_TARGET")}
    best = max(legs.values(),
               key=lambda l: (l["net_of_cost_first_horizon"] or float("-inf")))
    return {
        "calculation_owner": CALCULATION_OWNER,
        "objective_owner": OBJECTIVE_OWNER,
        "policy_horizon_sessions": int(horizon),
        "nav": _money(navv),
        "current": {"expected_net_utility": e_cur.get("expected_net_utility"),
                    "expected_excess_return": e_cur.get("expected_excess_return"),
                    "expected_volatility_horizon":
                        e_cur.get("expected_volatility_horizon"),
                    "cash_weight": e_cur.get("cash_weight"),
                    "concentration": concentration(cur)},
        "against": legs,
        "best_available_switch": best["target_kind"],
        "best_net_of_cost_first_horizon": best["net_of_cost_first_horizon"],
        "incumbency_privilege": False,
        "incumbency_privilege_doc": (
            "the current book is priced on the same objective, the same "
            "covariance and the same horizon as every target; the only "
            "advantage it holds is that leaving it costs money, and that cost "
            "is charged in full"),
        "semantics": {
            "utility_gap_per_horizon":
                "target expected net utility minus the current book's, per "
                "policy horizon, in return units",
            "payback":
                "switch cost divided by the per-horizon gap; how long the edge "
                "must persist for the move to break even",
            "not_a_forecast_of_dollars":
                "dollar figures are the utility gap scaled by NAV, not a "
                "prediction of realised profit",
        },
    }


# --------------------------------------------------------------------------- #
# Cash deployment ladder
# --------------------------------------------------------------------------- #
def _increment_weight(inc: dict, nav: float) -> Optional[float]:
    kind = str(inc.get("kind") or "").upper()
    if kind == "DOLLARS":
        amt = _f(inc.get("amount"))
        return None if (amt is None or nav <= 0) else amt / nav
    if kind == "NAV_FRACTION":
        return _f(inc.get("fraction"))
    return None


def _two_leg_point(cur: dict, impl: dict, zb: dict, x: float) -> tuple:
    """The feasible portfolio reached by deploying ``x`` of NAV of GROSS BUYS.

    Leg 1 walks current -> implementable (the target that already prices the
    switch); leg 2 continues implementable -> zero-base (the target that gives
    incumbency nothing at all). Both endpoints came from the allocator, and the
    feasible set is convex, so every point on the two-leg path is itself
    feasible. Returns ``(weights, absorbed, capacity, leg)``.
    """
    cap1 = _gross_buy(cur, impl)
    cap2 = _gross_buy(impl, zb)
    capacity = cap1 + cap2
    if x <= 0 or capacity <= 0:
        return dict(cur), 0.0, capacity, "NONE"
    if x <= cap1 or cap2 <= 0:
        t = min(1.0, x / cap1) if cap1 > 0 else 0.0
        return _blend(cur, impl, t), min(x, cap1), capacity, "IMPLEMENTABLE"
    t2 = min(1.0, (x - cap1) / cap2)
    return _blend(impl, zb, t2), min(x, capacity), capacity, "ZERO_BASE"


def _buy_only_direction(cur: dict, impl: dict, zb: dict) -> tuple:
    """The two BUY legs of the allocator's own path, with the sales removed.

    Leg 1 is every name the implementable target wants MORE of than we hold;
    leg 2 is every name the zero-base target wants more of than the
    implementable one. Nothing is sold, so the direction is exactly "where the
    optimiser would put new money" and never "what the optimiser would rotate".
    """
    names = set(cur) | set(impl) | set(zb)
    leg1 = {}
    leg2 = {}
    for tk in names:
        d1 = (impl.get(tk) or 0.0) - (cur.get(tk) or 0.0)
        if d1 > 0:
            leg1[tk] = d1
        d2 = (zb.get(tk) or 0.0) - (impl.get(tk) or 0.0)
        if d2 > 0:
            leg2[tk] = d2
    return leg1, leg2


def _cash_only_point(cur: dict, leg1: dict, leg2: dict, x: float,
                     cash_weight: Optional[float]) -> tuple:
    """The portfolio reached by spending ``x`` of NAV of CASH on buys only.

    Cash is a hard ceiling: a cash-only ladder can never spend money the book
    does not hold, and the shortfall is reported rather than quietly funded by
    an imaginary sale. Returns ``(weights, absorbed, capacity, unfunded, leg)``.
    """
    cap1 = sum(leg1.values())
    cap2 = sum(leg2.values())
    ceiling = cap1 + cap2
    if cash_weight is not None:
        ceiling = min(ceiling, max(0.0, cash_weight))
    if x <= 0 or ceiling <= 0:
        return dict(cur), 0.0, ceiling, max(0.0, x), "NONE"
    take = min(x, ceiling)
    unfunded = max(0.0, x - take)
    w = dict(cur)
    used1 = min(take, cap1)
    if cap1 > 0 and used1 > 0:
        s = used1 / cap1
        for tk, d in leg1.items():
            w[tk] = (w.get(tk) or 0.0) + s * d
    used2 = max(0.0, take - used1)
    if cap2 > 0 and used2 > 0:
        s2 = used2 / cap2
        for tk, d in leg2.items():
            w[tk] = (w.get(tk) or 0.0) + s2 * d
    leg = "ZERO_BASE" if used2 > 0 else "IMPLEMENTABLE"
    return w, take, ceiling, unfunded, leg


def _destinations(cur: dict, target: dict, *, nav: float,
                  meta: Optional[dict], limit: int = 8) -> list[dict]:
    md = meta or {}
    rows = []
    for tk in sorted(set(cur) | set(target)):
        d = (target.get(tk) or 0.0) - (cur.get(tk) or 0.0)
        if d <= 1e-9:
            continue
        m = md.get(tk) or {}
        adv = _f(m.get("adv_dollar"))
        dollars = d * nav
        rows.append({
            "instrument_id": tk,
            "destination_kind": (DEST_EXISTING_HOLDING if (cur.get(tk) or 0.0) > 0
                                 else (m.get("destination_kind") or DEST_NEW_EQUITY)),
            "sector": m.get("sector"),
            "sleeve_id": m.get("sleeve_id"),
            "asset_class": m.get("asset_class"),
            "rank": m.get("rank"),
            "weight_added": _r(d, 6),
            "capital_added_usd": _money(dollars),
            "weight_after": _r(target.get(tk) or 0.0, 6),
            "adv_dollar": _money(adv),
            "adv_participation": (_r(dollars / adv, 6)
                                  if (adv and adv > 0) else None),
        })
    rows.sort(key=lambda r: -(r["capital_added_usd"] or 0.0))
    return rows[:limit]


def _sold(cur: dict, target: dict) -> float:
    names = set(cur) | set(target)
    return sum(max(0.0, (cur.get(tk) or 0.0) - (target.get(tk) or 0.0))
               for tk in names)


def build_deployment_ladder(*, current_weights: dict,
                            implementable_weights: dict,
                            zero_base_weights: dict, mu: dict,
                            sigma_forecast: dict, cov_h: dict,
                            cov_included: list, policy: dict, horizon: int,
                            nav: Optional[float], cash: Optional[float],
                            available_capital: Optional[float] = None,
                            candidate_meta: Optional[dict] = None,
                            increments: Optional[list] = None,
                            min_order_notional: Optional[float] = None,
                            expected_return_state: Optional[str] = None,
                            mode: str = MODE_CASH_ONLY) -> dict:
    """The marginal use of the next dollar, rung by rung.

    Every rung reports the same eight facts an allocator has to weigh -
    destination, expected gain, incremental risk, incremental concentration,
    transaction cost, liquidity participation, turnover and whether the hurdle
    clears - plus the reason cash won when it did.

    ``mode`` decides WHICH capital question is being answered:
    ``MODE_CASH_ONLY`` spends cash and never sells; ``MODE_REDEPLOYMENT`` walks
    the allocator's path and is funded by cash and sales together.
    """
    cur = _w(current_weights)
    impl = _w(implementable_weights)
    zb = _w(zero_base_weights)
    navv = _f(nav) or 0.0
    cashv = _f(cash)
    avail = _f(available_capital)
    if avail is None:
        avail = cashv
    minn = _f(min_order_notional)
    if minn is None:
        minn = DEFAULT_MIN_ORDER_NOTIONAL
    h = int(horizon)
    incs = [dict(i) for i in (increments if increments is not None
                              else default_increments())]

    if navv <= 0 or not cur and not impl and not zb:
        return {
            "schema_version": SCHEMA_VERSION, "calculation_owner": CALCULATION_OWNER,
            "state": STATE_BLOCKED,
            "state_vocabulary": list(STATE_VOCAB),
            "blockers": [{"code": "NO_NAV_OR_NO_WEIGHTS"}],
            "rungs": [], "safety_badges": list(SAFETY_BADGES),
        }

    def _econ(w):
        return zba.portfolio_economics(
            weights=w, mu=mu, sigma_forecast=sigma_forecast, cov_h=cov_h,
            cov_included=cov_included, policy=policy, horizon=h)

    e_cur = _econ(cur)
    u_cur = e_cur.get("expected_net_utility") or 0.0
    vol_cur = e_cur.get("expected_volatility_horizon")
    con_cur = concentration(cur)
    cash_weight_now = e_cur.get("cash_weight")
    cash_only = str(mode) == MODE_CASH_ONLY
    leg1, leg2 = _buy_only_direction(cur, impl, zb)
    avail_weight = ((avail / navv) if (avail is not None and navv > 0)
                    else cash_weight_now)
    if cash_only:
        capacity = min(sum(leg1.values()) + sum(leg2.values()),
                       max(0.0, avail_weight or 0.0))
    else:
        capacity = _gross_buy(cur, impl) + _gross_buy(impl, zb)

    rungs = []
    for inc in incs:
        x = _increment_weight(inc, navv)
        if x is None or x <= 0:
            continue
        requested_usd = x * navv
        if cash_only:
            w_x, absorbed, cap, unfunded_w, leg = _cash_only_point(
                cur, leg1, leg2, x, avail_weight)
            unfunded_usd = unfunded_w * navv
        else:
            w_x, absorbed, cap, leg = _two_leg_point(cur, impl, zb, x)
            unfunded_usd = 0.0
        deployed_usd = absorbed * navv
        unabsorbed_usd = max(0.0, requested_usd - deployed_usd)

        e_x = _econ(w_x)
        tr = zba.transition_economics(current=cur, target=w_x, nav=navv,
                                      policy=policy)
        gain = (e_x.get("expected_net_utility") or 0.0) - u_cur
        cost_w = tr.get("transaction_cost_weight") or 0.0
        net = gain - cost_w
        sold_w = _sold(cur, w_x)
        cash_needed_usd = max(0.0, (absorbed - sold_w) * navv)
        funding = (FUNDING_FROM_CASH
                   if (avail is None or cash_needed_usd <= avail + 1e-6)
                   else FUNDING_FROM_CASH_AND_SALES)

        if absorbed <= 1e-12:
            hurdle = HURDLE_FAILS_NO_CAPACITY
        elif deployed_usd < minn:
            hurdle = HURDLE_FAILS_BELOW_MIN_NOTIONAL
        elif net > 0:
            hurdle = HURDLE_CLEARS
        else:
            hurdle = HURDLE_FAILS_NET_UTILITY
        cash_wins = hurdle != HURDLE_CLEARS
        if hurdle == HURDLE_CLEARS:
            reason = None
        elif hurdle == HURDLE_FAILS_NO_CAPACITY:
            reason = (("there is no cash left to deploy" if cash_only else
                       "the eligible targets are already at their caps") +
                      ": no compliant destination absorbs this money")
        elif hurdle == HURDLE_FAILS_BELOW_MIN_NOTIONAL:
            reason = "the deployable amount is below the minimum order notional"
        else:
            reason = ("the expected utility this increment buys is smaller than "
                      "the transaction cost of buying it")

        con_x = concentration(w_x)
        dests = _destinations(cur, w_x, nav=navv, meta=candidate_meta)
        part = [d["adv_participation"] for d in dests
                if d.get("adv_participation") is not None]

        rungs.append({
            "label": inc.get("label"),
            "kind": inc.get("kind"),
            "requested_usd": _money(requested_usd),
            "requested_weight": _r(x, 6),
            "deployed_usd": _money(deployed_usd),
            "deployed_weight": _r(absorbed, 6),
            "not_deployed_usd": _money(unabsorbed_usd),
            "unfunded_usd": _money(unfunded_usd),
            "capacity_weight": _r(cap, 6),
            "capacity_usd": _money(cap * navv),
            "capacity_exhausted": bool(absorbed + 1e-12 < x),
            "path_leg": leg,
            "funding_source": funding,
            "funding_vocabulary": list(FUNDING_VOCAB),
            "cash_consumed_usd": _money(cash_needed_usd),
            "sold_weight": _r(sold_w, 6),
            "destinations": dests,
            "destination_kinds": sorted({d["destination_kind"] for d in dests}),
            "expected_utility_before": _r(u_cur, 8),
            "expected_utility_after": e_x.get("expected_net_utility"),
            "expected_utility_gain": _r(gain, 8),
            "expected_utility_gain_usd": _money(gain * navv),
            "transaction_cost_weight": tr.get("transaction_cost_weight"),
            "transaction_cost_usd": tr.get("transaction_cost_dollars"),
            "net_of_cost_gain": _r(net, 8),
            "net_of_cost_gain_usd": _money(net * navv),
            "incremental_risk_horizon": _r(
                ((e_x.get("expected_volatility_horizon") or 0.0)
                 - (vol_cur or 0.0)), 8),
            "expected_volatility_after": e_x.get("expected_volatility_horizon"),
            "expected_downside_q05_after": e_x.get("expected_downside_q05"),
            "incremental_concentration_hhi": _r(
                ((con_x.get("herfindahl_index") or 0.0)
                 - (con_cur.get("herfindahl_index") or 0.0)), 8),
            "max_name_weight_after": con_x.get("max_name_weight"),
            "position_count_after": con_x.get("position_count"),
            "cash_weight_after": e_x.get("cash_weight"),
            "one_way_turnover": tr.get("one_way_turnover"),
            "names_traded": tr.get("names_traded"),
            "max_adv_participation": _r(max(part), 6) if part else None,
            "hurdle_state": hurdle,
            "hurdle_vocabulary": list(HURDLE_VOCAB),
            "hurdle_clears": hurdle == HURDLE_CLEARS,
            "cash_wins": cash_wins,
            "cash_wins_reason": reason,
            "payback": payback(gain_per_horizon=gain, cost_weight=cost_w,
                               horizon_sessions=h),
        })

    clearing = [r for r in rungs if r["hurdle_clears"]]
    best = max(clearing, key=lambda r: r["net_of_cost_gain"]) if clearing else None
    marginal = _marginal_dollar(cur=cur, impl=impl, zb=zb, econ=_econ,
                                policy=policy, nav=navv, u_cur=u_cur,
                                cash_only=cash_only, leg1=leg1, leg2=leg2,
                                avail_weight=avail_weight)
    return {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "objective_owner": OBJECTIVE_OWNER,
        "covariance_owner": COVARIANCE_OWNER,
        "phase": PHASE,
        "state": STATE_READY,
        "state_vocabulary": list(STATE_VOCAB),
        "mode": mode,
        "mode_vocabulary": list(MODE_VOCAB),
        "mode_doc": ("CASH_ONLY buys and never sells, so it can never spend "
                     "more cash than the book holds; REDEPLOYMENT walks the "
                     "allocator's own path and is funded by cash AND sales."),
        "policy_horizon_sessions": h,
        "nav": _money(navv),
        "cash": _money(cashv),
        "available_capital": _money(avail),
        "cash_weight": cash_weight_now,
        "min_order_notional": _money(minn),
        "cost_rate_per_side": policy.get("cost_rate_per_side"),
        "cost_bps_per_side": policy.get("cost_bps_per_side"),
        "deployable_capacity_weight": _r(capacity, 6),
        "deployable_capacity_usd": _money(capacity * navv),
        "rungs": rungs,
        "n_rungs": len(rungs),
        "n_rungs_clearing_hurdle": len(clearing),
        "best_clearing_rung": (best or {}).get("label"),
        "best_clearing_net_gain_usd": (best or {}).get("net_of_cost_gain_usd"),
        "all_rungs_retain_cash": len(clearing) == 0,
        "marginal_dollar": marginal,
        "expected_return_state": expected_return_state,
        "hurdle_lane": LANE_RESEARCH_UTILITY,
        "hurdle_lane_vocabulary": list(LANE_VOCAB),
        "cash_is_a_destination": True,
        "cash_is_a_destination_doc": (
            "cash is not the residual of an allocation. It wins an increment "
            "whenever the increment's net-of-cost utility gain is not positive, "
            "or whenever the eligible targets cannot absorb the money inside "
            "their own caps, and the reason is reported by name."),
        "safety_badges": list(SAFETY_BADGES),
        "creates_orders": False,
        "creates_decisions": False,
        "creates_proposal": False,
        "mutates_holdings": False,
        "mutates_cash": False,
    }


def _marginal_dollar(*, cur: dict, impl: dict, zb: dict, econ, policy: dict,
                     nav: float, u_cur: float, cash_only: bool = False,
                     leg1: Optional[dict] = None, leg2: Optional[dict] = None,
                     avail_weight: Optional[float] = None) -> dict:
    """The derivative at the first dollar: does deployment pay AT ALL?

    A ladder whose rungs are all large can hide the fact that the very first
    dollar already loses money. This computes the gradient over a deliberately
    tiny step so the sign is the economics, not the step size.
    """
    eps = 1e-4
    if cash_only:
        w_e, absorbed, _capacity, _unf, _leg = _cash_only_point(
            cur, leg1 or {}, leg2 or {}, eps, avail_weight)
    else:
        w_e, absorbed, _capacity, _leg = _two_leg_point(cur, impl, zb, eps)
    if absorbed <= 0:
        return {"available": False,
                "reason": "no eligible destination capacity at any size"}
    e = econ(w_e)
    tr = zba.transition_economics(current=cur, target=w_e, nav=nav, policy=policy)
    gain = (e.get("expected_net_utility") or 0.0) - u_cur
    cost = tr.get("transaction_cost_weight") or 0.0
    d_gain = gain / absorbed
    d_cost = cost / absorbed
    return {
        "available": True,
        "step_weight": _r(absorbed, 8),
        "marginal_utility_per_unit_deployed": _r(d_gain, 8),
        "marginal_cost_per_unit_deployed": _r(d_cost, 8),
        "marginal_net_per_unit_deployed": _r(d_gain - d_cost, 8),
        "first_dollar_pays": bool(d_gain - d_cost > 0),
        "cost_share_of_marginal_gain": (_r(d_cost / d_gain, 4)
                                        if d_gain > 0 else None),
        "doc": ("the gradient of expected net utility per unit of gross buying, "
                "against the per-unit transaction cost. If cost share exceeds "
                "1.0 the edge is smaller than the toll on the road to it."),
    }


# --------------------------------------------------------------------------- #
# Realised excess, decomposed
# --------------------------------------------------------------------------- #
#: The cash-drag formula is the DESK's, quoted rather than re-derived, so the
#: since-inception decomposition and the desk's per-window attribution can never
#: disagree about what cash drag means.
CASH_DRAG_FORMULA_OWNER = "api.paper_trading_desk (cash_drag_formula)"
BETA_ASSUMPTION = "INVESTED_SLEEVE_BETA_EQUALS_ONE"


def excess_decomposition(*, book_return_pct: Optional[float],
                         benchmark_return_pct: Optional[float],
                         cash_weight: Optional[float],
                         transaction_cost_usd: Optional[float],
                         initial_capital: Optional[float]) -> dict:
    """Split realised excess-vs-benchmark into the terms a manager can act on.

    Three named terms and an explicit residual:

        excess = cash_drag + transaction_cost_drag + unexplained

    ``cash_drag`` uses the desk's own formula. ``transaction_cost_drag`` is the
    cumulative cost as a share of starting capital. Everything left over is
    reported as UNEXPLAINED and is NOT relabelled "selection alpha": it is
    whatever the two named terms do not cover, which in a long-only equity book
    is dominated by name selection but is not PROVEN to be only that.

    The beta assumption is declared, because the decomposition is only as good
    as it: the invested sleeve is treated as beta one to the benchmark.
    """
    br, mr = _f(book_return_pct), _f(benchmark_return_pct)
    cw, tc, cap = _f(cash_weight), _f(transaction_cost_usd), _f(initial_capital)
    if br is None or mr is None:
        return {"available": False, "reason": "book or benchmark return missing"}
    excess = br - mr
    cash_drag = (-cw * mr) if (cw is not None) else None
    cost_drag = (-100.0 * tc / cap) if (tc is not None and cap) else None
    named = sum(x for x in (cash_drag, cost_drag) if x is not None)
    unexplained = excess - named
    terms = [
        {"term": "CASH_DRAG", "pct_points": _r(cash_drag, 4),
         "formula": "-(cash weight) x benchmark return",
         "formula_owner": CASH_DRAG_FORMULA_OWNER,
         "actionable_by": "the cash deployment decision"},
        {"term": "TRANSACTION_COST_DRAG", "pct_points": _r(cost_drag, 4),
         "formula": "-(cumulative transaction cost) / (initial capital)",
         "formula_owner": "api.paper_trading_desk",
         "actionable_by": "the turnover policy"},
        {"term": "UNEXPLAINED_BY_CASH_OR_COST", "pct_points": _r(unexplained, 4),
         "formula": "excess - cash drag - transaction cost drag",
         "formula_owner": CALCULATION_OWNER,
         "actionable_by": "the selection model itself",
         "caveat": ("this is a residual, not a measured selection effect. In a "
                    "long-only single-sleeve equity book it is dominated by "
                    "name selection, but it also absorbs any deviation from "
                    "the declared beta assumption.")},
    ]
    terms.sort(key=lambda t: (t["pct_points"] if t["pct_points"] is not None
                              else 0.0))
    return {
        "available": True,
        "book_return_pct": _r(br, 4),
        "benchmark_return_pct": _r(mr, 4),
        "excess_pct_points": _r(excess, 4),
        "cash_weight": _r(cw, 6),
        "terms": terms,
        "largest_detractor_term": terms[0]["term"] if terms else None,
        "beta_assumption": BETA_ASSUMPTION,
        "sums_to_excess": True,
        "calculation_owner": CALCULATION_OWNER,
    }


# --------------------------------------------------------------------------- #
# Governed lane - what the operator may actually act on today
# --------------------------------------------------------------------------- #
def governed_capital_hurdle(*, expected_return_state: Optional[str],
                            forecast_lane: Optional[str],
                            entry_rank: Optional[int],
                            eligible_destinations: Optional[list] = None) -> dict:
    """The hurdle the GOVERNED operational lane can actually evidence today.

    The operational stack ranks names; it does not price them. Where no
    calibrated expected return exists, an eligibility rank is the only thing a
    governed decision may rest on, and it proves ORDERING, never PROFIT. Saying
    that plainly is the difference between a research surface and a misleading
    one.
    """
    calibrated = str(expected_return_state or "").upper() == "CALIBRATED"
    dests = list(eligible_destinations or [])
    return {
        "lane": LANE_GOVERNED_SCORE,
        "lane_vocabulary": list(LANE_VOCAB),
        "expected_return_state": expected_return_state,
        "forecast_lane": forecast_lane,
        "hurdle_state": (HURDLE_CLEARS if calibrated else HURDLE_NOT_EVIDENCED),
        "hurdle_basis": ("CALIBRATED_EXPECTED_RETURN" if calibrated
                         else "SCORE_RANK_ELIGIBILITY_ONLY"),
        "economic_proof": "PRESENT" if calibrated else "ABSENT",
        "entry_rank": entry_rank,
        "eligible_destinations": dests[:25],
        "n_eligible_destinations": len(dests),
        "doc": ("A score rank orders names. It does not state that any of them "
                "has a positive expected return after cost, so it cannot by "
                "itself clear an economic hurdle for deploying cash. Until the "
                "return forecast is activated, a governed cash deployment is a "
                "MANUAL judgement supported by ordering evidence, not an "
                "economically proven one."),
        "manual_review_required": True,
        "automatic_deployment_allowed": False,
    }


__all__ = [
    "CALCULATION_OWNER", "SCHEMA_VERSION", "PHASE", "OBJECTIVE_OWNER",
    "COVARIANCE_OWNER", "TRADING_DAYS_YEAR",
    "DESTINATION_VOCAB", "DEST_EXISTING_HOLDING", "DEST_NEW_EQUITY",
    "DEST_OTHER_SLEEVE", "DEST_CASH",
    "MODE_VOCAB", "MODE_CASH_ONLY", "MODE_REDEPLOYMENT",
    "HURDLE_VOCAB", "HURDLE_CLEARS", "HURDLE_FAILS_NET_UTILITY",
    "HURDLE_FAILS_NO_CAPACITY", "HURDLE_FAILS_BELOW_MIN_NOTIONAL",
    "HURDLE_NOT_EVIDENCED",
    "FUNDING_VOCAB", "FUNDING_FROM_CASH", "FUNDING_FROM_CASH_AND_SALES",
    "PAYBACK_VOCAB", "PAYBACK_WITHIN_ONE_HORIZON", "PAYBACK_MULTI_HORIZON",
    "PAYBACK_NEVER", "PAYBACK_NOT_APPLICABLE",
    "LANE_VOCAB", "LANE_RESEARCH_UTILITY", "LANE_GOVERNED_SCORE",
    "STATE_READY", "STATE_BLOCKED", "STATE_VOCAB", "SAFETY_BADGES",
    "DEFAULT_INCREMENTS", "DEFAULT_MIN_ORDER_NOTIONAL", "default_increments",
    "concentration", "payback", "incumbent_opportunity_cost",
    "build_deployment_ladder", "governed_capital_hurdle",
    "excess_decomposition", "CASH_DRAG_FORMULA_OWNER", "BETA_ASSUMPTION",
]
