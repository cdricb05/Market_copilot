"""alpha_agent.r46.cost_efficiency - THE owner of economic efficiency.

Release 46.5 got the first matured TRUE_FORWARD observation in the project's
history, and it said something no historical study had ever been able to say:

    r46_eq_xs_rev_5d, h=1        gross  +6.48 bps
                                 cost   -12.00 bps
                                 net    -5.52 bps
                                 cash   +1.53 bps
                                 alpha  -7.05 bps

The signal was RIGHT ABOUT DIRECTION and the trade still lost money. Fifteen
releases of IC, t-stats and hit rates could not have told an operator that,
because none of them priced the implementation. This module exists so the
distinction can never be lost again:

    SIGNAL EDGE    what the model knew          (gross)
    ECONOMIC EDGE  what an operator would keep  (net of cost, net of control)

ONE OWNER, ONE CALCULATION
--------------------------
Every efficiency number in the estate is computed here and nowhere else. The
leaderboard, the judge, the UI, the API, the allocator and the verdict engine
CONSUME these numbers; none of them recompute a ratio of its own. What this
module itself consumes is equally strict:

* realised gross / cost / net / control come from the JUDGE's outcome rows,
  through :func:`alpha_agent.r46.strategy_pnl.matured_summary`, which reads
  closed trades only;
* the cost contract comes from :mod:`alpha_agent.r46.pnl`, which is a proven
  decomposition of the frozen :mod:`alpha_agent.r46.contract` numbers;
* open trades are read from the trade ledger and reported in a SEPARATE tier.

MATURED AND MARK-TO-MARKET ARE NEVER SUMMED. An open trade's mark is an
indication of where a strategy is heading; it is not evidence, it decides no
verdict, and it appears on its own tier with its own label. R46.5 wrote that
rule after watching a 5-day VX carry position look like a winner while holding
zero matured observations, and this module keeps it.

BREAK-EVEN IS KNOWN BEFORE THE OUTCOME
--------------------------------------
The most useful number here needs no outcome at all. A strategy's round-trip
cost is a property of its book and the frozen cost contract, so the gross edge
it MUST produce to break even is computable the day it is frozen. The first
matured result did not fail because of bad luck; it failed because a 100-name
long/short decile book at 6 bps per side needs **more than 12 bps of gross
edge in one session** and reversal at that horizon does not reliably deliver
it. That is a design fact, and it was knowable in advance.

WHAT THIS MODULE MAY NEVER DO
-----------------------------
It may not lower a cost after seeing a loss. It may not change a horizon, a
parameter, a universe or a control. It computes descriptive ECONOMIC states
that sit ALONGSIDE - never replace - the formal scientific verdicts owned by
:mod:`alpha_agent.r46.verdicts`. ``GROSS_EDGE_POSITIVE_COST_DESTROYED`` is an
observation about implementation; ``TOO_EARLY`` remains the science.

RESEARCH ONLY. No order, no holding, no promotion, no operational write.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import contract as C
from . import pnl as PN
from . import strategy_pnl as SP
from . import trades as TR

CALCULATION_OWNER = "alpha_agent.r46.cost_efficiency"

ARTIFACT = "R46_6_COST_EFFICIENCY.json"
RANKINGS_ARTIFACT = "R46_6_COST_DESTRUCTION_RANKINGS.json"
BREAK_EVEN_ARTIFACT = "R46_6_BREAK_EVEN_ECONOMICS.json"

BPS = 1e4

# --------------------------------------------------------------------------- #
# Descriptive economic states - section 6. These are OBSERVATIONS about
# implementation economics. They never replace a formal scientific verdict.
# --------------------------------------------------------------------------- #
TOO_EARLY = "TOO_EARLY"
GROSS_EDGE_NEGATIVE = "GROSS_EDGE_NEGATIVE"
COST_DESTROYED = "GROSS_EDGE_POSITIVE_COST_DESTROYED"
NET_POSITIVE_CONTROL_NEGATIVE = "NET_POSITIVE_CONTROL_NEGATIVE"
POSITIVE_RESIDUAL_ALPHA = "POSITIVE_RESIDUAL_ALPHA"

ECONOMIC_STATES = (TOO_EARLY, GROSS_EDGE_NEGATIVE, COST_DESTROYED,
                   NET_POSITIVE_CONTROL_NEGATIVE, POSITIVE_RESIDUAL_ALPHA)

#: Cost-robustness is a SEPARATE axis from the state above: a strategy with
#: positive residual alpha can still be fragile, and a cost-destroyed one is
#: neither robust nor fragile - it has already lost.
COST_ROBUST = "COST_ROBUST"
COST_FRAGILE = "COST_FRAGILE"
COST_ROBUSTNESS_NOT_APPLICABLE = "NOT_APPLICABLE_NO_POSITIVE_NET"
COST_ROBUSTNESS_UNKNOWN = "UNKNOWN_TOO_EARLY"
ROBUSTNESS_STATES = (COST_ROBUST, COST_FRAGILE,
                     COST_ROBUSTNESS_NOT_APPLICABLE, COST_ROBUSTNESS_UNKNOWN)

#: FROZEN. A descriptive state still needs a sample; one observation describes
#: one observation. Declared here, at the same threshold the verdict engine
#: already uses for "nothing may be said".
CLASSIFICATION_RULES = {
    "version": "R46_6_COST_EFFICIENCY_RULES_v1",
    "min_matured_for_any_economic_state": SP.KILL_RULES[
        "min_closed_trades_before_watch"],
    "descriptive_states_do_not_replace_scientific_verdicts": True,
    "matured_and_mark_to_market_are_never_summed": True,
    "a_cost_may_never_be_lowered_after_seeing_a_loss": True,
    "edge_retention_is_undefined_when_gross_edge_is_not_positive": True,
    "one_observation_is_reported_as_an_observation_not_a_state": True,
}

#: The evidence tiers. A number may live on exactly one of them.
TIER_MATURED = "MATURED_REALISED"
TIER_OPEN_MARK = "OPEN_MARK_TO_MARKET_INDICATIVE"
TIER_EX_ANTE = "EX_ANTE_FROM_THE_COST_CONTRACT"
TIERS = (TIER_MATURED, TIER_OPEN_MARK, TIER_EX_ANTE)


def _bps(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    v = float(x) * BPS
    return round(v, 6) if math.isfinite(v) else None


def _ratio(num: Optional[float], den: Optional[float]) -> Optional[float]:
    if num is None or den is None or den == 0:
        return None
    v = float(num) / float(den)
    return round(v, 8) if math.isfinite(v) else None


# --------------------------------------------------------------------------- #
# Section 7 - break-even economics. Needs NO outcome.
# --------------------------------------------------------------------------- #
def break_even(*, cost_return: float, control_return: float = None,
               cost_return_at_2x: float = None,
               cost_return_at_stress: float = None) -> dict:
    """The gross edge a book MUST produce, per round trip, to survive.

    Pure arithmetic over the frozen cost contract. Every number is knowable
    the day a challenger is frozen, which is precisely why a strategy whose
    required gross edge exceeds anything its mechanism plausibly delivers is
    a design error rather than an unlucky one.
    """
    cost = float(cost_return or 0.0)
    ctl = float(control_return or 0.0)
    c2x = float(cost_return_at_2x if cost_return_at_2x is not None
                else 2.0 * cost)
    cst = float(cost_return_at_stress if cost_return_at_stress is not None
                else 3.0 * cost)
    return {
        "cost_bps": _bps(cost),
        "control_bps": _bps(ctl),
        "break_even_gross_edge_bps": _bps(cost),
        "gross_edge_to_beat_control_bps": _bps(cost + ctl),
        "gross_edge_to_be_positive_at_2x_costs_bps": _bps(c2x),
        "gross_edge_to_beat_control_at_2x_costs_bps": _bps(c2x + ctl),
        "gross_edge_to_be_positive_at_stress_costs_bps": _bps(cst),
        "cost_bps_at_2x": _bps(c2x),
        "cost_bps_at_stress": _bps(cst),
        "known_before_any_outcome": True,
        "charged_on": "TRADED_NOTIONAL_BOTH_SIDES",
    }


def shortfall(*, gross_return: Optional[float], be: dict) -> dict:
    """How far a realised gross edge fell short of what it had to clear."""
    if gross_return is None:
        return {"state": "NO_REALISED_GROSS_EDGE"}
    g = _bps(gross_return)
    out = {"gross_edge_bps": g}
    for label, key in (("vs_break_even", "break_even_gross_edge_bps"),
                       ("vs_control", "gross_edge_to_beat_control_bps"),
                       ("vs_2x_costs",
                        "gross_edge_to_be_positive_at_2x_costs_bps"),
                       ("vs_stress_costs",
                        "gross_edge_to_be_positive_at_stress_costs_bps")):
        req = be.get(key)
        out[label + "_required_bps"] = req
        out[label + "_shortfall_bps"] = (None if req is None
                                         else round(g - req, 6))
        out[label + "_cleared"] = (None if req is None else bool(g >= req))
    return out


# --------------------------------------------------------------------------- #
# Section 5 - the efficiency metrics
# --------------------------------------------------------------------------- #
def metrics(*, gross: Optional[float], cost: Optional[float],
            net: Optional[float], control: Optional[float],
            residual_alpha: Optional[float], net_at_2x: Optional[float],
            net_at_stress: Optional[float] = None,
            turnover: Optional[float] = None,
            capital_usd: Optional[float] = None,
            n_observations: int = 0, tier: str = TIER_MATURED) -> dict:
    """Every section-5 quantity for ONE strategy on ONE evidence tier. Pure.

    ``EDGE RETENTION`` is deliberately undefined unless the gross edge is
    strictly positive: ``net / gross`` on a negative gross edge produces a
    number that LOOKS like a ratio, moves the right way when things get
    worse, and means nothing.
    """
    g = None if gross is None else float(gross)
    c = None if cost is None else float(cost)
    n = None if net is None else float(net)
    if n is None and g is not None and c is not None:
        n = g - c
    ctl = None if control is None else float(control)
    ra = residual_alpha
    if ra is None and n is not None and ctl is not None:
        ra = n - ctl
    n2 = net_at_2x
    if n2 is None and g is not None and c is not None:
        n2 = g - 2.0 * c
    ns = net_at_stress

    gross_positive = g is not None and g > 0
    return {
        "evidence_tier": tier,
        "n_observations": int(n_observations),
        # ---- the raw economics, in basis points ------------------------- #
        "gross_edge_bps": _bps(g),
        "cost_bps": _bps(c),
        "net_edge_bps": _bps(n),
        "control_bps": _bps(ctl),
        "residual_alpha_bps": _bps(ra),
        "net_at_2x_costs_bps": _bps(n2),
        "net_at_stress_costs_bps": _bps(ns),
        # ---- the ratios --------------------------------------------------- #
        "cost_to_gross_edge_ratio": (_ratio(c, g) if gross_positive else None),
        "cost_to_gross_edge_ratio_state": (
            "DEFINED" if gross_positive
            else "UNDEFINED_GROSS_EDGE_NOT_POSITIVE"),
        "pct_of_gross_edge_consumed_by_cost": (
            None if not gross_positive else round(100.0 * float(c) / float(g), 4)),
        "edge_retention_ratio": (_ratio(n, g) if gross_positive else None),
        "edge_retention_state": (
            "DEFINED" if gross_positive
            else "UNDEFINED_GROSS_EDGE_NOT_POSITIVE"),
        # ---- efficiency per unit of what it consumed ---------------------- #
        "turnover": (None if turnover is None else round(float(turnover), 8)),
        "pnl_per_unit_turnover": _ratio(n, turnover),
        "pnl_per_dollar_cost": _ratio(n, c),
        "capital_efficiency": {
            "net_return_on_capital": n,
            "residual_alpha_on_capital": ra,
            "capital_usd": capital_usd,
            "net_pnl_usd": (None if (n is None or capital_usd is None)
                            else round(float(n) * float(capital_usd), 6)),
            "cost_usd": (None if (c is None or capital_usd is None)
                         else round(float(c) * float(capital_usd), 6)),
        },
        # ---- fragility ---------------------------------------------------- #
        "survives_base_costs": (None if n is None else bool(n > 0)),
        "survives_2x_costs": (None if n2 is None else bool(n2 > 0)),
        "survives_stress_costs": (None if ns is None else bool(ns > 0)),
        "beats_control": (None if ra is None else bool(ra > 0)),
    }


# --------------------------------------------------------------------------- #
# Section 6 - the descriptive classification
# --------------------------------------------------------------------------- #
def classify(*, n_observations: int, gross: Optional[float],
             net: Optional[float], residual_alpha: Optional[float],
             net_at_2x: Optional[float]) -> dict:
    """One descriptive economic state. Pure, and never a scientific verdict."""
    minimum = CLASSIFICATION_RULES["min_matured_for_any_economic_state"]
    if n_observations <= 0:
        return {"economic_state": TOO_EARLY,
                "cost_robustness": COST_ROBUSTNESS_UNKNOWN,
                "reasons": ["no matured observation"],
                "is_a_scientific_verdict": False}
    if n_observations < minimum:
        return {"economic_state": TOO_EARLY,
                "cost_robustness": COST_ROBUSTNESS_UNKNOWN,
                "reasons": ["%d matured observation(s); %d needed before a "
                            "descriptive economic state is asserted"
                            % (n_observations, minimum)],
                "observed_only": _observation_note(gross, net, residual_alpha),
                "is_a_scientific_verdict": False}

    g = 0.0 if gross is None else float(gross)
    n = 0.0 if net is None else float(net)
    ra = residual_alpha
    n2 = net_at_2x

    if g <= 0:
        state = GROSS_EDGE_NEGATIVE
        reasons = ["the signal itself produced no gross edge; cost is not the "
                   "binding problem"]
    elif n <= 0:
        state = COST_DESTROYED
        reasons = ["gross edge is positive and net is not: the implementation "
                   "consumed the signal"]
    elif ra is not None and float(ra) <= 0:
        state = NET_POSITIVE_CONTROL_NEGATIVE
        reasons = ["net of cost is positive but it did not beat its declared "
                   "control; holding the control was better"]
    else:
        state = POSITIVE_RESIDUAL_ALPHA
        reasons = ["positive after costs and after the declared control"]

    if state in (GROSS_EDGE_NEGATIVE, COST_DESTROYED) or n <= 0:
        rob = COST_ROBUSTNESS_NOT_APPLICABLE
    elif n2 is not None and float(n2) > 0:
        rob = COST_ROBUST
    else:
        rob = COST_FRAGILE
    return {"economic_state": state, "cost_robustness": rob,
            "reasons": reasons, "is_a_scientific_verdict": False}


def _observation_note(gross, net, residual_alpha) -> dict:
    return {"gross_edge_bps": _bps(gross), "net_edge_bps": _bps(net),
            "residual_alpha_bps": _bps(residual_alpha),
            "observation_economic_state": classify_observation(
                gross=gross, net=net, residual_alpha=residual_alpha)["state"],
            "note": "reported as an observation, not as a strategy state"}


def classify_observation(*, gross: Optional[float], net: Optional[float],
                         residual_alpha: Optional[float]) -> dict:
    """The economic state of ONE matured trade. Not a strategy state.

    Section 6 asks for two things that only look contradictory: the FIRST
    matured reversal result is descriptively ``GROSS_EDGE_POSITIVE_COST_
    DESTROYED``, and one observation must not become a strategy verdict.
    Both hold, because they are statements about different objects. This
    function classifies the TRADE. :func:`classify` classifies the STRATEGY
    and refuses to leave ``TOO_EARLY`` on a sample of one.
    """
    if gross is None and net is None:
        return {"state": TOO_EARLY, "scope": "OBSERVATION",
                "reasons": ["no realised economics on this row"]}
    g = 0.0 if gross is None else float(gross)
    n = (g - 0.0) if net is None else float(net)
    if g <= 0:
        s, why = GROSS_EDGE_NEGATIVE, ("the signal produced no gross edge on "
                                       "this observation")
    elif n <= 0:
        s, why = COST_DESTROYED, ("gross edge was positive and net was not: "
                                  "cost consumed the signal on this "
                                  "observation")
    elif residual_alpha is not None and float(residual_alpha) <= 0:
        s, why = NET_POSITIVE_CONTROL_NEGATIVE, ("net was positive but the "
                                                 "control was better on this "
                                                 "observation")
    else:
        s, why = POSITIVE_RESIDUAL_ALPHA, ("positive after cost and after the "
                                           "control on this observation")
    return {"state": s, "scope": "OBSERVATION", "reasons": [why],
            "is_a_strategy_state": False, "is_a_scientific_verdict": False}


# --------------------------------------------------------------------------- #
# The build - one row per registered strategy, on every tier it has evidence
# --------------------------------------------------------------------------- #
def _open_tier(cid: str, opens: list, marks_by_trade: dict,
               closed_ids: set) -> dict:
    """Mark-to-market economics for a strategy's OPEN trades. INDICATIVE."""
    g = c = n = ctl = ra = n2 = ns = 0.0
    turn = 0.0
    cap = 0.0
    k = 0
    for o in opens:
        if o.get("challenger_id") != cid:
            continue
        tid = o.get("research_trade_id")
        if tid in closed_ids:
            continue
        share = float(o.get("weight_within_strategy") or 0.0)
        mm = marks_by_trade.get(tid) or []
        turn += share * 2.0 * float(
            o.get("gross_exposure_per_unit_capital") or 0.0)
        if o.get("funded"):
            cap += float(((o.get("capital_by_policy") or {}).get(
                _canonical_policy()) or {}).get("capital_usd") or 0.0)
        if not mm:
            # entered, not yet marked: the cost is already fully recognised
            c += share * float(o.get("cost_return") or 0.0)
            n -= share * float(o.get("cost_return") or 0.0)
            n2 -= share * float(o.get("cost_return_at_2x")
                                or 2.0 * float(o.get("cost_return") or 0.0))
            ns -= share * float(o.get("cost_return_at_stress")
                                or 3.0 * float(o.get("cost_return") or 0.0))
            k += 1
            continue
        last = mm[-1]
        g += share * float(last.get("gross_return") or 0.0)
        c += share * float(last.get("cost_return") or 0.0)
        n += share * float(last.get("net_return") or 0.0)
        ctl += share * float(last.get("control_return") or 0.0)
        if last.get("residual_alpha_vs_control") is not None:
            ra += share * float(last.get("residual_alpha_vs_control"))
        if last.get("net_return_at_2x") is not None:
            n2 += share * float(last.get("net_return_at_2x"))
        if last.get("net_return_at_stress") is not None:
            ns += share * float(last.get("net_return_at_stress"))
        k += 1
    if k == 0:
        return None
    return metrics(gross=g, cost=c, net=n, control=ctl, residual_alpha=ra,
                   net_at_2x=n2, net_at_stress=ns, turnover=turn,
                   capital_usd=cap, n_observations=k, tier=TIER_OPEN_MARK)


def _canonical_policy() -> str:
    from . import allocation as AL
    return AL.CANONICAL_POLICY


def _ex_ante(cid: str, opens: list) -> dict:
    """Break-even economics from the cost contract alone - no outcome read.

    Uses the strategy's most recent opened trade as the representative book,
    because a challenger's book structure is frozen with its specification.
    """
    mine = [o for o in opens if o.get("challenger_id") == cid]
    if not mine:
        return None
    o = sorted(mine, key=lambda r: (str(r.get("entry_session")),
                                    str(r.get("research_trade_id"))))[-1]
    ctl = None
    if str(o.get("control")) == C.CONTROL_CASH:
        from . import marketdata as MD
        rf = MD.risk_free_annual().get("annual")
        h = int(o.get("horizon") or 1)
        ctl = (None if rf is None else float(rf) * float(h) / 252.0)
    be = break_even(cost_return=float(o.get("cost_return") or 0.0),
                    control_return=ctl,
                    cost_return_at_2x=o.get("cost_return_at_2x"),
                    cost_return_at_stress=o.get("cost_return_at_stress"))
    be.update({
        "evidence_tier": TIER_EX_ANTE,
        "representative_trade_id": o.get("research_trade_id"),
        "horizon": o.get("horizon"),
        "n_legs": o.get("n_legs"),
        "gross_exposure_per_unit_capital": o.get(
            "gross_exposure_per_unit_capital"),
        "turnover_per_round_trip": 2.0 * float(
            o.get("gross_exposure_per_unit_capital") or 0.0),
        "trade_structure": o.get("trade_structure"),
        "asset_class": o.get("asset_class"),
        "control": o.get("control"),
        "cost_breakdown_bps": o.get("cost_breakdown_bps"),
    })
    return be


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          registry: dict = None, write: bool = True) -> dict:
    """THE cost-efficiency artifact. Every consumer reads this; none recompute."""
    from . import registry as RG
    reg = registry if registry is not None else RG.load(campaign_id)
    entries = {c["challenger_id"]: c for c in (reg.get("challengers") or ())}

    matured = SP.matured_summary(as_of, campaign_id)
    opens = TR.opens(campaign_id)
    closes = [c for c in TR.closes(campaign_id)
              if str(c.get("exit_session")) <= str(as_of)]
    closed_ids = {c["research_trade_id"] for c in closes}
    marks_by_trade: dict = {}
    for m in TR.marks(campaign_id):
        if m["session"] <= str(as_of):
            marks_by_trade.setdefault(m["research_trade_id"], []).append(m)
    for v in marks_by_trade.values():
        v.sort(key=lambda m: m["session"])

    # control, per strategy, taken from the judge's closed rows
    control_by_cid: dict = {}
    for c in closes:
        if c.get("control_return") is not None:
            control_by_cid.setdefault(c["challenger_id"], 0.0)
            o = next((x for x in opens
                      if x["research_trade_id"] == c["research_trade_id"]), {})
            control_by_cid[c["challenger_id"]] += (
                float(o.get("weight_within_strategy") or 0.0)
                * float(c["control_return"]))

    rows = []
    for cid, e in entries.items():
        m = matured.get(cid) or SP.empty_matured(cid)
        cap = 0.0
        for c in closes:
            if c["challenger_id"] != cid:
                continue
            o = next((x for x in opens
                      if x["research_trade_id"] == c["research_trade_id"]), {})
            if o.get("funded"):
                cap += float(((o.get("capital_by_policy") or {})
                              .get(_canonical_policy()) or {})
                             .get("capital_usd") or 0.0)
        mat = metrics(
            gross=m["cum_gross"], cost=m["cum_cost"], net=m["cum_net"],
            control=control_by_cid.get(cid),
            residual_alpha=m["cum_residual_alpha"],
            net_at_2x=m["cum_net_at_2x"], net_at_stress=None,
            turnover=_turnover_closed(cid, opens, closed_ids),
            capital_usd=cap, n_observations=m["n_closed"], tier=TIER_MATURED)
        cls = classify(n_observations=m["n_closed"], gross=m["cum_gross"],
                       net=m["cum_net"],
                       residual_alpha=(m["cum_residual_alpha"]
                                       if m["n_closed"] else None),
                       net_at_2x=m["cum_net_at_2x"])
        ea = _ex_ante(cid, opens)
        rows.append({
            "challenger_id": cid,
            "challenger_version": e.get("challenger_version"),
            "asset_class": e.get("asset_class"),
            "economic_family": e.get("family"),
            "information_family": e.get("information_family"),
            "dependence_cluster": e.get("dependence_cluster"),
            "horizons": e.get("horizons"),
            "economic_state": cls["economic_state"],
            "cost_robustness": cls["cost_robustness"],
            "classification_reasons": cls["reasons"],
            "observed_only": cls.get("observed_only"),
            "is_a_scientific_verdict": False,
            "matured": mat,
            "open_mark_to_market": _open_tier(cid, opens, marks_by_trade,
                                              closed_ids),
            "ex_ante_break_even": ea,
            "shortfall_vs_break_even": (
                shortfall(gross_return=(m["cum_gross"] if m["n_closed"]
                                        else None), be=ea) if ea else None),
            "matured_and_mark_to_market_are_never_summed": True,
        })

    rows.sort(key=lambda r: (
        ECONOMIC_STATES.index(r["economic_state"]),
        -(r["matured"]["residual_alpha_bps"] or 0.0)))

    counts = {s: sum(1 for r in rows if r["economic_state"] == s)
              for s in ECONOMIC_STATES}
    rob = {s: sum(1 for r in rows if r["cost_robustness"] == s)
           for s in ROBUSTNESS_STATES}

    # ---- the OBSERVATION tier: one state per matured trade ---------------- #
    observations = []
    for c in sorted(closes, key=lambda r: (str(r.get("exit_session")),
                                           str(r.get("research_trade_id")))):
        oc = classify_observation(
            gross=c.get("gross_return"), net=c.get("net_return"),
            residual_alpha=c.get("residual_alpha_vs_control"))
        observations.append({
            "research_trade_id": c.get("research_trade_id"),
            "prediction_id": c.get("prediction_id"),
            "challenger_id": c.get("challenger_id"),
            "exit_session": c.get("exit_session"),
            "gross_edge_bps": _bps(c.get("gross_return")),
            "cost_bps": _bps(c.get("cost_return")),
            "net_edge_bps": _bps(c.get("net_return")),
            "control_bps": _bps(c.get("control_return")),
            "residual_alpha_bps": _bps(c.get("residual_alpha_vs_control")),
            "observation_economic_state": oc["state"],
            "reasons": oc["reasons"],
            "is_a_strategy_state": False,
        })
    obs_counts = {s: sum(1 for o in observations
                         if o["observation_economic_state"] == s)
                  for s in ECONOMIC_STATES}

    body = artifact_body(
        "r46_6_cost_efficiency/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        question="which frozen prospective strategies produce enough GROSS "
                 "edge to overcome realistic costs and their correct "
                 "economic control?",
        evidence_tiers=list(TIERS),
        economic_state_vocabulary=list(ECONOMIC_STATES),
        cost_robustness_vocabulary=list(ROBUSTNESS_STATES),
        rules=dict(CLASSIFICATION_RULES),
        cost_contract=PN.contract_summary()["base_per_side_bps"],
        cost_decomposition_matches_frozen_contract=PN.
        decomposition_matches_contract()["all_match"],
        n_strategies=len(rows),
        economic_state_counts=counts,
        cost_robustness_counts=rob,
        cost_destroyed=[r["challenger_id"] for r in rows
                        if r["economic_state"] == COST_DESTROYED],
        gross_edge_negative=[r["challenger_id"] for r in rows
                             if r["economic_state"] == GROSS_EDGE_NEGATIVE],
        positive_residual_alpha=[r["challenger_id"] for r in rows
                                 if r["economic_state"]
                                 == POSITIVE_RESIDUAL_ALPHA],
        net_positive_control_negative=[
            r["challenger_id"] for r in rows
            if r["economic_state"] == NET_POSITIVE_CONTROL_NEGATIVE],
        n_with_matured_evidence=sum(1 for r in rows
                                    if r["matured"]["n_observations"]),
        descriptive_states_never_replace_verdicts=True,
        # ---- the observation tier, kept strictly apart from the strategy
        #      tier: a trade may be COST_DESTROYED while its strategy is
        #      still TOO_EARLY, and both statements are true at once.
        n_matured_observations=len(observations),
        observation_economic_state_counts=obs_counts,
        observations=observations,
        an_observation_state_is_not_a_strategy_state=True,
        first_matured_explained=explain_first_matured(campaign_id),
        rows=rows,
        research_only=True,
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
        write_json(campaign_dir(campaign_id) / RANKINGS_ARTIFACT,
                   rankings(body))
        write_json(campaign_dir(campaign_id) / BREAK_EVEN_ARTIFACT,
                   break_even_report(body))
    return body


def _turnover_closed(cid: str, opens: list, closed_ids: set) -> float:
    return float(sum(
        float(o.get("weight_within_strategy") or 0.0) * 2.0
        * float(o.get("gross_exposure_per_unit_capital") or 0.0)
        for o in opens
        if o.get("challenger_id") == cid
        and o.get("research_trade_id") in closed_ids))


# --------------------------------------------------------------------------- #
# Section 8 - cost dominance. Where does cost beat signal?
# --------------------------------------------------------------------------- #
def rankings(body: dict) -> dict:
    """Rank strategies by economic efficiency, not by headline P&L.

    A strategy earning +50 bps gross against -60 bps of cost must rank BELOW
    one earning +20 against -5. Ranking on net alone already does that; the
    reason it is stated explicitly is that ranking on GROSS - which is what
    an IC or a hit-rate table effectively does - does not.
    """
    have = [r for r in (body.get("rows") or ())
            if r["matured"]["n_observations"]]

    def by(key, reverse=True):
        vals = [(r["challenger_id"], r["matured"].get(key)) for r in have]
        vals = [(c, v) for c, v in vals if v is not None]
        return [{"challenger_id": c, "value": v}
                for c, v in sorted(vals, key=lambda t: t[1], reverse=reverse)]

    return artifact_body(
        "r46_6_cost_destruction_rankings/1", CALCULATION_OWNER,
        as_of=body.get("as_of"),
        n_ranked=len(have),
        ranking_rule="net of cost and net of the declared control; a large "
                     "gross edge that costs more than it earns ranks BELOW a "
                     "small one that does not",
        gross_edge_is_not_the_ranking="a gross-edge ranking is what an IC "
                                      "table produces and it is exactly the "
                                      "number the first matured result "
                                      "showed to be insufficient",
        by_cumulative_gross_edge_bps=by("gross_edge_bps"),
        by_cumulative_cost_drag_bps=by("cost_bps"),
        by_net_edge_bps=by("net_edge_bps"),
        by_residual_alpha_bps=by("residual_alpha_bps"),
        by_pct_of_gross_edge_consumed_by_cost=by(
            "pct_of_gross_edge_consumed_by_cost"),
        by_turnover_efficiency=by("pnl_per_unit_turnover"),
        by_edge_retention_ratio=by("edge_retention_ratio"),
        cost_dominates=[r["challenger_id"] for r in have
                        if r["economic_state"] == COST_DESTROYED],
        n_cost_dominates=sum(1 for r in have
                             if r["economic_state"] == COST_DESTROYED),
        research_only=True,
    )


def break_even_report(body: dict) -> dict:
    """Section 7 - what every strategy must earn GROSS, before any outcome."""
    rows = []
    for r in (body.get("rows") or ()):
        ea = r.get("ex_ante_break_even")
        if not ea:
            continue
        rows.append({
            "challenger_id": r["challenger_id"],
            "asset_class": r["asset_class"],
            "economic_family": r["economic_family"],
            "horizon": ea.get("horizon"),
            "n_legs": ea.get("n_legs"),
            "turnover_per_round_trip": ea.get("turnover_per_round_trip"),
            "cost_bps": ea.get("cost_bps"),
            "break_even_gross_edge_bps": ea.get("break_even_gross_edge_bps"),
            "gross_edge_to_beat_control_bps": ea.get(
                "gross_edge_to_beat_control_bps"),
            "gross_edge_to_be_positive_at_2x_costs_bps": ea.get(
                "gross_edge_to_be_positive_at_2x_costs_bps"),
            "gross_edge_to_be_positive_at_stress_costs_bps": ea.get(
                "gross_edge_to_be_positive_at_stress_costs_bps"),
            "realised_shortfall": r.get("shortfall_vs_break_even"),
            "matured_observations": r["matured"]["n_observations"],
        })
    rows.sort(key=lambda r: -(r["break_even_gross_edge_bps"] or 0.0))
    return artifact_body(
        "r46_6_break_even_economics/1", CALCULATION_OWNER,
        as_of=body.get("as_of"),
        n_strategies=len(rows),
        statement="the gross edge a book must produce per round trip to "
                  "survive its own implementation, computable the day the "
                  "challenger is frozen and needing no outcome at all",
        hardest_to_clear=rows[0]["challenger_id"] if rows else None,
        easiest_to_clear=rows[-1]["challenger_id"] if rows else None,
        rows=rows,
        research_only=True,
    )


# --------------------------------------------------------------------------- #
# Section 31 - explain ONE matured observation in plain economic terms
# --------------------------------------------------------------------------- #
def explain_outcome(outcome: dict, prediction: dict = None) -> dict:
    """A plain-economics explanation of one matured row, generated from it.

    Nothing here is hard-coded to any particular result: every number is read
    from the outcome row the judge appended.
    """
    if not outcome:
        return {"state": "NO_MATURED_OUTCOME"}
    g = outcome.get("realised_gross_return")
    c = outcome.get("realised_cost")
    n = outcome.get("realised_net_return")
    ctl = outcome.get("control_return")
    ra = outcome.get("net_alpha_vs_control")
    ra2 = outcome.get("net_alpha_vs_control_at_2x_costs")
    be = break_even(cost_return=c or 0.0, control_return=ctl)
    steps = []
    if g is not None:
        steps.append("the signal produced %+.2f bps of gross return" % _bps(g))
    if c is not None:
        steps.append("implementation cost consumed %.2f bps" % _bps(c))
    if n is not None:
        steps.append("the net result was %+.2f bps" % _bps(n))
    if ctl is not None:
        steps.append("holding cash instead would have earned %+.2f bps"
                     % _bps(ctl))
    if ra is not None:
        steps.append("residual alpha versus the declared control was "
                     "%+.2f bps" % _bps(ra))
    verdict_word = (
        "the direction was right and the implementation took it"
        if (g is not None and g > 0 and n is not None and n <= 0)
        else "the signal itself did not produce a gross edge"
        if (g is not None and g <= 0)
        else "the trade cleared its costs and its control"
        if (ra is not None and ra > 0)
        else "the trade cleared its costs but not its control")
    return {
        "prediction_id": outcome.get("prediction_id"),
        "challenger_id": outcome.get("challenger_id"),
        "challenger_version": outcome.get("challenger_version"),
        "horizon": outcome.get("horizon"),
        "maturity_date": outcome.get("maturity_date"),
        "gross_edge_bps": _bps(g),
        "cost_bps": _bps(c),
        "net_edge_bps": _bps(n),
        "control_bps": _bps(ctl),
        "residual_alpha_bps": _bps(ra),
        "residual_alpha_at_2x_costs_bps": _bps(ra2),
        "hit": outcome.get("hit"),
        "turnover": outcome.get("turnover"),
        "break_even": be,
        "shortfall": shortfall(gross_return=g, be=be),
        "steps": steps,
        "one_line": " -> ".join(steps),
        "what_it_means": verdict_word,
        "signal_edge_vs_economic_edge": (
            "SIGNAL_EDGE_POSITIVE_ECONOMIC_EDGE_NEGATIVE"
            if (g is not None and g > 0 and (ra is None or ra <= 0))
            else "SIGNAL_EDGE_NEGATIVE" if (g is not None and g <= 0)
            else "ECONOMIC_EDGE_POSITIVE"),
        "observation_economic_state": classify_observation(
            gross=g, net=n, residual_alpha=ra),
        "strategy_state_is_owned_by_classify_and_needs_a_sample": True,
        "one_observation_is_not_a_verdict": True,
        "generated_from_the_outcome_ledger": True,
    }


def explain_first_matured(campaign_id: str = CAMPAIGN_ID) -> dict:
    """Explain the FIRST matured observation this tournament ever produced."""
    from . import ledger as LG
    outs = LG.outcomes(campaign_id)
    if not outs:
        return {"state": "NO_MATURED_OUTCOME"}
    first = sorted(outs, key=lambda r: (str(r.get("maturity_date") or ""),
                                        str(r.get("scored_at_utc") or "")))[0]
    preds = {p.get("prediction_id"): p for p in LG.predictions(campaign_id)}
    return explain_outcome(first, preds.get(first.get("prediction_id")))


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "RANKINGS_ARTIFACT",
           "BREAK_EVEN_ARTIFACT", "ECONOMIC_STATES", "ROBUSTNESS_STATES",
           "TIERS", "TIER_MATURED", "TIER_OPEN_MARK", "TIER_EX_ANTE",
           "TOO_EARLY", "GROSS_EDGE_NEGATIVE", "COST_DESTROYED",
           "NET_POSITIVE_CONTROL_NEGATIVE", "POSITIVE_RESIDUAL_ALPHA",
           "COST_ROBUST", "COST_FRAGILE", "CLASSIFICATION_RULES",
           "break_even", "shortfall", "metrics", "classify", "build",
           "rankings", "break_even_report", "explain_outcome",
           "explain_first_matured"]
