r"""api/alpha_capital.py - Release 56: the ONE read model behind the operator's
ALPHA & CAPITAL surface.

It answers eight questions in the order a portfolio manager asks them, and it
answers every one of them by COMPOSING an owner that already published the fact:

    1 Where is capital now?               api.capital_pool
    2 Where would zero-base capital go?   api.cash_deployment_frontier
    3 Should the cash be deployed?        api.cash_deployment_frontier (ladder)
    4 What are the strongest alpha
      opportunities?                      api.alpha_opportunity_registry
    5 Which challenger is earning the
      best forward paper P&L?             api.shadow_portfolio_evidence +
                                          api.prospective_tournament
    6 What is stopping the portfolio
      earning more?                       the LIMITERS block below
    7 What evidence is immature?          the same owners' own maturity states
    8 What experiment runs next?          the registry's experiment queue

It owns exactly ONE calculation of its own - the LIMITER RANKING - and it is a
ranking of facts other owners published, never a new measurement. Everything
else is projection.

THE SCOREBOARD RULE
-------------------
Nothing is compared unless the comparison is on equal time and equal basis. The
operational book, cash and the benchmark share the desk's own window; the
research shadow book is reported on ITS window and labelled with it; the frozen
portfolio challengers are compared only through the kernel that intersects their
calendars. A leaderboard that quotes four different windows in one column is a
category error, not a summary.

Read-only. It creates no signal, target, proposal, decision or order; it changes
no holding, cash or NAV; it promotes no model and enables no automation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.engine import alpha_capital_frontier as kernel

SCHEMA_VERSION = "alpha_capital.v1"
COMPOSITION_OWNER = "api.alpha_capital"
PHASE = "R56"
ROUTE = "/v1/operations/alpha-capital"

STATE_READY = "READY"
STATE_DEGRADED = "DEGRADED"
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_UNAVAILABLE)

#: ``NO LIVE BROKER ORDERS`` is the canonical Phase-27B.6 wording and the
#: platform badge ``ORDERS DISABLED`` is deliberately NOT used: paper orders are
#: real in this system and exist in the operational book under a governed,
#: manually reviewed workflow. Only live brokerage orders are structurally
#: disabled, and a regression test asserts the misleading wording never appears.
SAFETY_BADGES = ["PREVIEW ONLY", "READ ONLY", "PAPER ONLY", "RESEARCH ONLY",
                 "NO ORDERS", "NO LIVE BROKER ORDERS",
                 "AUTOMATION OFF", "MANUAL REVIEW", "NO MODEL PROMOTION",
                 "NO SLEEVE ACTIVATION"]

# --------------------------------------------------------------------------- #
# Limiter vocabulary - what can stand between this book and more P&L
# --------------------------------------------------------------------------- #
LIM_SIGNAL = "SIGNAL_WEAKNESS"
LIM_MODEL = "MODEL_WEAKNESS"
LIM_CALIBRATION = "NO_CALIBRATED_EXPECTED_RETURN"
LIM_COST = "TRANSACTION_COST_AND_TURNOVER"
LIM_DATA = "MISSING_INFORMATION"
LIM_EVIDENCE = "EVIDENCE_IMMATURITY"
LIM_ELIGIBILITY = "SINGLE_SLEEVE_CAPITAL_ELIGIBILITY"
LIM_CASH = "CASH_DRAG"
LIM_RISK = "RISK_OR_CONCENTRATION_LIMITS"
LIM_REGIME = "MARKET_REGIME"
LIMITER_VOCAB = (LIM_SIGNAL, LIM_MODEL, LIM_CALIBRATION, LIM_COST, LIM_DATA,
                 LIM_EVIDENCE, LIM_ELIGIBILITY, LIM_CASH, LIM_RISK, LIM_REGIME)

SEV_BINDING = "BINDING"          # measured, and it is costing money now
SEV_MATERIAL = "MATERIAL"        # measured, and it matters
SEV_SECONDARY = "SECONDARY"      # measured, and it is small
SEV_NOT_BINDING = "NOT_BINDING"  # measured, and it is not the problem
SEVERITY_VOCAB = (SEV_BINDING, SEV_MATERIAL, SEV_SECONDARY, SEV_NOT_BINDING)
_SEV_ORDER = {SEV_BINDING: 0, SEV_MATERIAL: 1, SEV_SECONDARY: 2,
              SEV_NOT_BINDING: 3}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _r(x, nd):
    v = _f(x)
    return None if v is None else round(v, nd)


def _safe(fn, degraded: list, source: str, default=None):
    try:
        return fn()
    except Exception as exc:                                       # noqa: BLE001
        degraded.append({"source": source, "detail": type(exc).__name__})
        return default


# --------------------------------------------------------------------------- #
# Limiters - the ONE calculation this module owns
# --------------------------------------------------------------------------- #
def rank_limiters(*, decomposition: dict, research_agent: dict,
                  forecast_state: Optional[str],
                  expected_return_state: Optional[str],
                  frontier_summary: dict, registry_summary: dict,
                  tournament: dict, rolling: dict,
                  capital_eligible_classes: list) -> dict:
    """Rank what is actually standing between this book and more paper P&L.

    Every row cites the owner that measured it. Severity is decided by the
    measurement, not by narrative: a limiter is BINDING only when a published
    number says it is costing money now.
    """
    rows = []
    terms = {t["term"]: t for t in (decomposition.get("terms") or [])}
    unexp = (terms.get("UNEXPLAINED_BY_CASH_OR_COST") or {}).get("pct_points")
    cashd = (terms.get("CASH_DRAG") or {}).get("pct_points")
    costd = (terms.get("TRANSACTION_COST_DRAG") or {}).get("pct_points")

    rq = (research_agent or {}).get("ranking_quality") or {}
    health = ((research_agent or {}).get("champion_health") or {}).get(
        "component_summary") or {}
    degradation = ((research_agent or {}).get("degradation") or {}).get(
        "categories") or []

    rows.append({
        "limiter": LIM_SIGNAL,
        "severity": (SEV_BINDING if (unexp is not None and unexp < -1.0)
                     else SEV_MATERIAL if (unexp is not None and unexp < 0)
                     else SEV_NOT_BINDING),
        "measurement": {"unexplained_excess_pct_points": unexp,
                        "forward_rank_ic_mean": rq.get("rank_ic_mean"),
                        "forward_decile_spread_pp": rq.get("decile_spread_pp"),
                        "rank_ic_by_horizon": rq.get("rank_ic_by_horizon"),
                        "observations": rq.get("rank_ic_obs")},
        "measured_by": "api.forward_prediction_skill via api.research_agent",
        "statement": ("the ranking that chooses the names is what the realised "
                      "excess is mostly made of, and it is not being paid for"),
        "what_would_move_it": ("a model whose forward decile spread is positive "
                               "on the live universe"),
    })
    rows.append({
        "limiter": LIM_CALIBRATION,
        "severity": (SEV_BINDING
                     if str(expected_return_state or "").upper() != "CALIBRATED"
                     else SEV_NOT_BINDING),
        "measurement": {"expected_return_state": expected_return_state,
                        "forecast_state": forecast_state,
                        "governed_hurdle_state":
                            frontier_summary.get("governed_hurdle_state"),
                        "governed_economic_proof":
                            frontier_summary.get("governed_economic_proof")},
        "measured_by": "api.opportunity_frontier / api.return_forecast",
        "statement": ("no operational owner can state an expected return, so no "
                      "capital decision can clear an ECONOMIC hurdle: every "
                      "governed deployment today rests on ordering evidence and "
                      "a human"),
        "what_would_move_it": ("activating a calibrated return forecast through "
                               "the existing manual governance"),
    })
    turn = (rolling or {}).get("avg_daily_turnover")
    rows.append({
        "limiter": LIM_COST,
        "severity": (SEV_MATERIAL if (costd is not None and costd < -0.1)
                     else SEV_SECONDARY),
        "measurement": {"transaction_cost_drag_pct_points": costd,
                        "avg_daily_turnover_pct": turn,
                        "turnover_efficiency_health": health.get("turnover_efficiency"),
                        "cost_share_of_marginal_gain":
                            frontier_summary.get("cost_share_of_marginal_gain"),
                        "zero_base_payback_sessions":
                            frontier_summary.get("zero_base_payback_sessions")},
        "measured_by": "api.paper_trading_desk / engine.alpha_capital_frontier",
        "statement": ("cost is not the largest term in the realised shortfall, "
                      "but it consumes most of the marginal gain from any "
                      "further deployment, and the full zero-base rotation does "
                      "not repay itself inside one policy horizon"),
        "what_would_move_it": "a turnover penalty, or a payback-aware replace hurdle",
    })
    rows.append({
        "limiter": LIM_CASH,
        "severity": (SEV_SECONDARY if (cashd is not None and cashd < -0.05)
                     else SEV_NOT_BINDING),
        "measurement": {"cash_drag_pct_points": cashd,
                        "cash_weight": frontier_summary.get("cash_weight"),
                        "cash": frontier_summary.get("cash"),
                        "deployable_capacity_usd":
                            frontier_summary.get("deployable_capacity_usd"),
                        "n_rungs_clearing_hurdle":
                            frontier_summary.get("n_rungs_clearing_hurdle")},
        "measured_by": "api.paper_trading_desk / api.cash_deployment_frontier",
        "statement": ("the idle cash share is small and its drag is small; "
                      "deploying all of it changes the book's expected economics "
                      "by a fraction of one basis point per horizon"),
        "what_would_move_it": "nothing worth doing - this is not where the money is",
    })
    n_classes = len([c for c in (capital_eligible_classes or []) if c != "CASH"])
    rows.append({
        "limiter": LIM_ELIGIBILITY,
        "severity": SEV_MATERIAL if n_classes <= 1 else SEV_NOT_BINDING,
        "measurement": {"capital_eligible_asset_classes": capital_eligible_classes,
                        "non_cash_classes": n_classes,
                        "families_competing_forward":
                            registry_summary.get("n_actionable"),
                        "asset_classes_with_forward_challengers":
                            (tournament or {}).get("asset_classes_active")},
        "measured_by": "api.investability_registry via api.opportunity_frontier",
        "statement": ("every dollar this book can deploy must go into ONE "
                      "US-equity sleeve or stay in cash, while forward "
                      "challengers exist across several other asset classes"),
        "what_would_move_it": ("a sleeve reaching its forward evidence floor and "
                               "then a human activating it - not a research "
                               "result on its own"),
    })
    conf = (tournament or {}).get("forward_evidence_confidence") or {}
    rows.append({
        "limiter": LIM_EVIDENCE,
        "severity": SEV_BINDING,
        "measurement": {"forward_predictions":
                            (tournament or {}).get(
                                "how_many_real_forward_predictions_exist"),
                        "matured": (tournament or {}).get("how_many_have_matured"),
                        "effective_independent_observations":
                            conf.get("total_effective_independent_observations"),
                        "best_cell_evidence_score": conf.get("best_cell_evidence_score"),
                        "forward_confirmed": ((tournament or {}).get("counts")
                                              or {}).get("forward_confirmed")},
        "measured_by": "api.prospective_tournament",
        "statement": ("nothing is FORWARD_CONFIRMED. Every alternative to the "
                      "incumbent is an unproven claim, so there is no evidenced "
                      "replacement to give capital to even if the incumbent is "
                      "weak"),
        "what_would_move_it": "time on the existing forward clocks - nothing else",
    })
    rows.append({
        "limiter": LIM_MODEL,
        "severity": (SEV_MATERIAL if "PERFORMANCE_WEAKNESS" in degradation
                     else SEV_NOT_BINDING),
        "measurement": {"degradation_categories": degradation,
                        "champion_health": health,
                        "research_agent_state": (research_agent or {}).get("state")},
        "measured_by": "api.research_agent",
        "statement": ("the research agent already classifies the champion as "
                      "benchmark-relative WEAK and turnover-inefficient WEAK"),
        "what_would_move_it": "a recalibration study, which is manually gated",
    })
    rows.append({
        "limiter": LIM_DATA,
        "severity": SEV_MATERIAL,
        "measurement": {"closed_families": registry_summary.get("n_closed"),
                        "blocked_asset_classes":
                            registry_summary.get("blocked_asset_classes"),
                        "owned_frontier_verdict":
                            "NEW_ORTHOGONAL_INFORMATION_REQUIRED"},
        "measured_by": "api.alpha_opportunity_registry / api.mathematical_alpha_frontier",
        "statement": ("four independent campaigns concluded that the information "
                      "we own has been searched out; the binding constraint on "
                      "NEW alpha is information, not modelling effort"),
        "what_would_move_it": ("orthogonal new information that passes the "
                               "Information Purchase Gate"),
    })
    # Within a severity band the order is decided by MEASURED IMPACT in points
    # of realised excess, and only then alphabetically. Sorting three BINDING
    # limiters by name would put the smallest one first and call it primary.
    _impact = {LIM_SIGNAL: unexp, LIM_COST: costd, LIM_CASH: cashd}
    for r in rows:
        r["impact_pct_points"] = _r(_impact.get(r["limiter"]), 4)
        r["impact_measured"] = r["impact_pct_points"] is not None
    rows.sort(key=lambda r: (_SEV_ORDER.get(r["severity"], 9),
                             -abs(r["impact_pct_points"] or 0.0), r["limiter"]))
    binding = [r for r in rows if r["severity"] == SEV_BINDING]
    return {
        "calculation_owner": COMPOSITION_OWNER,
        "vocabulary": list(LIMITER_VOCAB),
        "severity_vocabulary": list(SEVERITY_VOCAB),
        "limiters": rows,
        "n_binding": len(binding),
        "binding": [r["limiter"] for r in binding],
        "primary_limiter": rows[0]["limiter"] if rows else None,
        "ranking_rule": ("severity band first, then MEASURED impact in points of "
                         "realised excess, then name. A limiter with no "
                         "measurable point impact never outranks one that has "
                         "it."),
        "doc": ("severity is decided by a published measurement, never by "
                "narrative: a limiter is BINDING only when a number says it is "
                "costing money now"),
    }


# --------------------------------------------------------------------------- #
# Scoreboard
# --------------------------------------------------------------------------- #
def build_scoreboard(*, rolling: dict, capital: dict, tournament: dict,
                     shadow_portfolios: dict) -> dict:
    """One comparison surface, with every row carrying its OWN window.

    Rows from different clocks are never merged into one ranking; the window is
    a first-class column so a reader can see instantly which comparisons are
    like-for-like.
    """
    since = (rolling or {}).get("since_inception") or {}
    spnl = (tournament or {}).get("shadow_pnl") or {}
    econ = (tournament or {}).get("economic_truth") or {}
    rows = [
        {"entity": "OPERATIONAL_BOOK", "label": "Alpha Paper Book #1 (live)",
         "basis": "REALISED_PAPER_PNL", "window": "since inception",
         "n_observations": since.get("n_daily_returns"),
         "return_pct": since.get("return_pct"),
         "pnl_usd": _r((_f(capital.get("nav")) or 0.0)
                       - (_f(capital.get("starting_nav")) or 0.0), 2),
         "benchmark_return_pct": since.get("spy_return_pct"),
         "excess_pct_points": since.get("excess_return_pct"),
         "annualised_volatility_pct": since.get("annualized_volatility_pct"),
         "max_drawdown_pct": since.get("max_drawdown_pct"),
         "hit_rate_pct": since.get("hit_rate_pct"),
         "avg_daily_turnover_pct": since.get("avg_daily_turnover"),
         "cash_weight": capital.get("cash_weight"),
         "owner": "api.paper_trading_desk"},
        {"entity": "CASH", "label": "Cash (zero-return paper policy)",
         "basis": "DECLARED_POLICY", "window": "since inception",
         "n_observations": since.get("n_daily_returns"),
         "return_pct": 0.0, "pnl_usd": 0.0,
         "benchmark_return_pct": since.get("spy_return_pct"),
         "excess_pct_points": (_r(-(_f(since.get("spy_return_pct")) or 0.0), 4)),
         "annualised_volatility_pct": 0.0, "max_drawdown_pct": 0.0,
         "owner": "engine.zero_base_allocator (CASH_RETURN_POLICY)"},
        {"entity": "BENCHMARK", "label": "SPY (passive)",
         "basis": "REALISED_BENCHMARK", "window": "since inception",
         "n_observations": since.get("n_daily_returns"),
         "return_pct": since.get("spy_return_pct"),
         "benchmark_return_pct": since.get("spy_return_pct"),
         "excess_pct_points": 0.0,
         "owner": "api.paper_trading_desk"},
        {"entity": "RESEARCH_SHADOW_BOOK",
         "label": "Release-46 research shadow book (all strategies)",
         "basis": "FORWARD_RESEARCH_PNL",
         "window": "since %s" % (spnl.get("inception") or "-"),
         "n_observations": econ.get("matured_observations"),
         "return_pct": _r(100.0 * (_f(spnl.get("shadow_return")) or 0.0), 4),
         "pnl_usd": spnl.get("cumulative_net_forward_pnl"),
         "excess_pct_points": None,
         "max_drawdown_pct": _r(100.0 * (_f(spnl.get("max_drawdown")) or 0.0), 4),
         "beats_cash": spnl.get("canonical_beats_cash"),
         "minus_cash_usd": spnl.get("canonical_minus_cash_usd"),
         "minus_spy_usd": spnl.get("canonical_minus_passive_spy_usd"),
         "financing_earned_usd": spnl.get("financing_earned"),
         "strategy_pnl_usd": econ.get("strategy_pnl_usd"),
         "cost_drag_usd": spnl.get("cost_drag"),
         "caveat": ("a research scale of $1,000,000 and a different clock; the "
                    "headline NAV is above its start only because collateral "
                    "earned financing"),
         "owner": "alpha_agent.r46 via api.prospective_tournament"},
    ]
    for lb in (shadow_portfolios or {}).get("leaderboard") or []:
        rows.append({
            "entity": "FORWARD_PORTFOLIO_CHALLENGER", "label": lb.get("label"),
            "challenger_id": lb.get("challenger_id"),
            "basis": "FORWARD_PAPER_PORTFOLIO",
            "window": "since inception (%s)" % (
                (shadow_portfolios.get("inception_sessions") or ["-"])[0]),
            "n_observations": lb.get("sessions_scored"),
            "return_pct": (_r(100.0 * (_f(lb.get("net_cumulative_return")) or 0.0), 4)
                           if lb.get("net_cumulative_return") is not None else None),
            "pnl_usd": lb.get("net_cumulative_pnl_usd"),
            "excess_pct_points": lb.get("excess_vs_control_pct_points"),
            "max_drawdown_pct": (_r(100.0 * (_f(lb.get("max_drawdown")) or 0.0), 4)
                                 if lb.get("max_drawdown") is not None else None),
            "annualised_volatility_pct": (
                _r(100.0 * (_f(lb.get("realised_annualised_volatility")) or 0.0), 4)
                if lb.get("realised_annualised_volatility") is not None else None),
            "sharpe": lb.get("sharpe"),
            "evidence_state": lb.get("evidence_state"),
            "cash_weight": lb.get("cash_weight"),
            "owner": "api.shadow_portfolio_evidence"})
    windows = sorted({r.get("window") for r in rows if r.get("window")})
    return {
        "rows": rows, "n_rows": len(rows),
        "distinct_windows": windows,
        "comparability_rule": ("rows sharing a window are directly comparable; "
                               "rows on different windows are NOT, and the "
                               "window column says which is which"),
        "promotion_allowed": False,
        "no_model_is_promoted_by_appearing_here": True,
    }


# --------------------------------------------------------------------------- #
# Read model
# --------------------------------------------------------------------------- #
def load_alpha_capital(*, cash_frontier: Optional[dict] = None,
                       registry: Optional[dict] = None,
                       shadow_portfolios: Optional[dict] = None,
                       tournament: Optional[dict] = None,
                       research_agent: Optional[dict] = None,
                       forward_evidence: Optional[dict] = None,
                       capital_pool: Optional[dict] = None,
                       desk_performance: Optional[dict] = None) -> dict:
    """The GET read surface. Degrade-safe: a missing research owner reduces the
    payload's state, never its availability."""
    from paper_trader.api import alpha_opportunity_registry as aor
    from paper_trader.api import capital_pool as cp
    from paper_trader.api import cash_deployment_frontier as cdf
    from paper_trader.api import forward_evidence as fe
    from paper_trader.api import paper_trading_desk as desk
    from paper_trader.api import prospective_tournament as pt
    from paper_trader.api import research_agent as ra
    from paper_trader.api import shadow_portfolio_evidence as spe

    degraded: list = []
    if capital_pool is None:
        capital_pool = _safe(cp.load_capital_pool, degraded,
                             "api.capital_pool", {}) or {}
    if cash_frontier is None:
        cash_frontier = _safe(cdf.load_cash_deployment_frontier, degraded,
                              "api.cash_deployment_frontier", {}) or {}
    if registry is None:
        registry = _safe(aor.load_alpha_opportunity_registry, degraded,
                         "api.alpha_opportunity_registry", {}) or {}
    if shadow_portfolios is None:
        shadow_portfolios = _safe(spe.load_shadow_portfolio_evidence, degraded,
                                  "api.shadow_portfolio_evidence", {}) or {}
    if tournament is None:
        tournament = _safe(pt.load_prospective_tournament, degraded,
                           "api.prospective_tournament", {}) or {}
    if research_agent is None:
        research_agent = _safe(ra.load_research_agent, degraded,
                               "api.research_agent", {}) or {}
    if forward_evidence is None:
        forward_evidence = _safe(fe.load_forward_evidence, degraded,
                                 "api.forward_evidence", {}) or {}
    if desk_performance is None:
        desk_performance = _safe(desk.load_performance, degraded,
                                 "api.paper_trading_desk", {}) or {}

    rolling = (forward_evidence or {}).get("rolling_evidence") or {}
    since = rolling.get("since_inception") or {}
    summary_perf = (desk_performance or {}).get("current_summary") or {}
    cost_total = ((research_agent or {}).get("turnover_cost") or {}).get(
        "total_transaction_cost")

    decomposition = kernel.excess_decomposition(
        book_return_pct=summary_perf.get("cumulative_return_pct"),
        benchmark_return_pct=summary_perf.get("benchmark_cumulative_return_pct")
        or since.get("spy_return_pct"),
        cash_weight=_f((capital_pool or {}).get("cash_weight")),
        transaction_cost_usd=_f(cost_total),
        initial_capital=_f((capital_pool or {}).get("starting_nav")))

    fs = cdf.summary(cash_frontier) if cash_frontier else {}
    rs = aor.summary(registry) if registry else {}
    rs["blocked_asset_classes"] = [
        r.get("asset_class") for r in (registry or {}).get("asset_class_readiness") or []
        if r.get("readiness") in (aor.AR_BLOCKED, aor.AR_NOT_READY)]
    ss = spe.summary(shadow_portfolios) if shadow_portfolios else {}

    limiters = rank_limiters(
        decomposition=decomposition, research_agent=research_agent,
        forecast_state=((cash_frontier or {}).get("research_lane") or {}).get(
            "forecast_state"),
        expected_return_state=((cash_frontier or {}).get("governed_lane") or {}).get(
            "expected_return_state"),
        frontier_summary=fs, registry_summary=rs, tournament=tournament,
        rolling=since,
        capital_eligible_classes=(registry or {}).get(
            "capital_eligible_asset_classes") or [])

    scoreboard = build_scoreboard(rolling=rolling, capital=capital_pool or {},
                                  tournament=tournament,
                                  shadow_portfolios=shadow_portfolios)

    opp = (cash_frontier or {}).get("incumbent_opportunity_cost") or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE, "route": ROUTE,
        "state": STATE_DEGRADED if degraded else STATE_READY,
        "state_vocabulary": list(READ_STATE_VOCAB),
        "generated_at": _now_iso(),
        "eligible_market_date": (cash_frontier or {}).get("eligible_market_date"),
        "headline": _headline(fs, limiters, rs),

        # 1 - where is capital now
        "capital_now": {
            "nav": (capital_pool or {}).get("nav"),
            "starting_nav": (capital_pool or {}).get("starting_nav"),
            "cash": (capital_pool or {}).get("cash"),
            "available_capital": (capital_pool or {}).get("available_capital"),
            "invested_capital": (capital_pool or {}).get("invested_capital"),
            "cash_weight": (capital_pool or {}).get("cash_weight"),
            "position_count": (capital_pool or {}).get("position_count"),
            "allocation": (capital_pool or {}).get("allocation"),
            "sleeve_exposure": (capital_pool or {}).get("sleeve_exposure"),
            "cumulative_pnl_usd": _r((_f((capital_pool or {}).get("nav")) or 0.0)
                                     - (_f((capital_pool or {}).get("starting_nav")) or 0.0), 2),
            "owner": (capital_pool or {}).get("owner"),
        },
        # 2 + 3 - zero base, transition aware, and the cash decision
        "capital_frontier": fs,
        "cash_decision": _cash_decision(cash_frontier),
        "zero_base": (cash_frontier or {}).get("targets") or {},
        "incumbent_opportunity_cost": opp,
        # 4 - the alpha opportunities
        "alpha_registry": rs,
        "top_opportunities": _top_opportunities(registry),
        # 5 - forward challengers
        "forward_portfolio_challengers": ss,
        "forward_signal_tournament": {
            "state": (tournament or {}).get("state"),
            "active": (tournament or {}).get("how_many_are_active"),
            "forward_predictions": (tournament or {}).get(
                "how_many_real_forward_predictions_exist"),
            "matured": (tournament or {}).get("how_many_have_matured"),
            "forward_confirmed": ((tournament or {}).get("counts") or {}).get(
                "forward_confirmed"),
            "best_net_alpha_bps": (tournament or {}).get(
                "best_net_alpha_vs_control_bps"),
            "evidence_maturity_state": (tournament or {}).get(
                "evidence_maturity_state"),
            "owner": "api.prospective_tournament",
        },
        # 6 - what is stopping more P&L
        "limiters": limiters,
        "realised_excess_decomposition": decomposition,
        # 7 - the scoreboard and the evidence maturity
        "scoreboard": scoreboard,
        "evidence_maturity": {
            "operational_observations": since.get("n_daily_returns"),
            "operational_sample_status": rolling.get("sample_status"),
            "tournament_effective_independent":
                ((tournament or {}).get("forward_evidence_confidence") or {}).get(
                    "total_effective_independent_observations"),
            "forward_portfolio_challengers_with_evidence":
                ss.get("n_with_forward_evidence"),
            "nothing_is_forward_confirmed": not bool(
                ((tournament or {}).get("counts") or {}).get("forward_confirmed")),
        },
        # 8 - what runs next
        "experiment_queue": (registry or {}).get("experiment_queue") or {},
        "degraded_sources": degraded,
        "safety": {
            "badges": list(SAFETY_BADGES), "read_only": True, "paper_only": True,
            "manual_review_only": True, "creates_signals": False,
            "creates_trade_decisions": False, "creates_orders": False,
            "creates_fills": False, "creates_proposal": False,
            "mutates_holdings": False, "mutates_cash": False,
            "promotes_model": False, "activates_sleeve": False,
            "enables_automation": False, "broker_enabled": False,
            "writes_operational_store": False,
            "automatic_promotion_allowed": False,
            "proposal_owner": "engine.reallocation_proposal",
            "decision_owner": "api.portfolio_decision",
        },
    }


def _cash_decision(cash_frontier: Optional[dict]) -> dict:
    """The one-line answer to 'should the cash be deployed today?'."""
    lad = (cash_frontier or {}).get("deployment_ladder") or {}
    gov = (cash_frontier or {}).get("governed_lane") or {}
    rungs = lad.get("rungs") or []
    clearing = [r for r in rungs if r.get("hurdle_clears")]
    best = max(clearing, key=lambda r: (r.get("net_of_cost_gain_usd") or 0.0)) \
        if clearing else None
    return {
        "research_lane_answer": ("DEPLOY" if clearing else "RETAIN_CASH"),
        "research_lane_best_rung": (best or {}).get("label"),
        "research_lane_deployable_usd": (best or {}).get("deployed_usd"),
        "research_lane_net_gain_usd_per_horizon": (best or {}).get("net_of_cost_gain_usd"),
        "research_lane_payback_sessions":
            ((best or {}).get("payback") or {}).get("payback_sessions"),
        "governed_lane_answer": "MANUAL_REVIEW_REQUIRED_NO_ECONOMIC_PROOF",
        "governed_hurdle_state": gov.get("hurdle_state"),
        "governed_economic_proof": gov.get("economic_proof"),
        "governed_eligible_destinations": (gov.get("eligible_destinations") or [])[:10],
        "cash_can_win": True,
        "why_the_two_lanes_differ": (
            "the research lane can price a portfolio because it has a forecast; "
            "the governed lane cannot, because the approved model publishes a "
            "RANKING and not an expected return. The research answer is "
            "evidence; only a human can act on it."),
        "creates_no_order": True,
    }


def _top_opportunities(registry: Optional[dict], limit: int = 8) -> list:
    """The families most worth an operator's attention, best evidence first."""
    fams = [f for f in (registry or {}).get("families") or []
            if f.get("status") in ("ACTIVE", "CHALLENGER", "PROMISING",
                                   "FORWARD_EVIDENCE_NEEDED")]

    def _key(f):
        obs = f.get("forward_evidence_observations") or 0
        alpha = f.get("best_measured_net_alpha_bps")
        return (-(1 if f.get("capital_eligible_today") else 0), -obs,
                -(alpha if alpha is not None else -1e9))

    fams.sort(key=_key)
    return [{"family_id": f["family_id"], "label": f["label"],
             "status": f["status"], "status_class": f.get("status_class"),
             "asset_classes": f["asset_classes"],
             "capital_eligible_today": f.get("capital_eligible_today"),
             "forward_evidence_observations": f.get("forward_evidence_observations"),
             "effective_independent_observations":
                 f.get("effective_independent_observations"),
             "best_measured_net_alpha_bps": f.get("best_measured_net_alpha_bps"),
             "evidence": f.get("evidence"),
             "promotion_allowed": False}
            for f in fams[:limit]]


def _headline(fs: dict, limiters: dict, rs: dict) -> str:
    prim = (limiters or {}).get("primary_limiter") or "UNKNOWN"
    cash = fs.get("cash")
    ans = ("deploying the cash clears the research hurdle"
           if fs.get("n_rungs_clearing_hurdle") else "cash is retained")
    return ("Capital: $%s idle. On the research forecast %s; on the governed "
            "lane no economic hurdle can be evidenced at all. %d alpha families "
            "are actionable and %d are closed. The primary limiter is %s."
            % (cash, ans, rs.get("n_actionable") or 0, rs.get("n_closed") or 0,
               prim))


def summary(payload: Optional[dict] = None, **kwargs) -> dict:
    p = payload if payload is not None else load_alpha_capital(**kwargs)
    return {
        "state": p.get("state"),
        "eligible_market_date": p.get("eligible_market_date"),
        "headline": p.get("headline"),
        "nav": (p.get("capital_now") or {}).get("nav"),
        "cash": (p.get("capital_now") or {}).get("cash"),
        "cash_decision": (p.get("cash_decision") or {}).get("research_lane_answer"),
        "governed_answer": (p.get("cash_decision") or {}).get("governed_lane_answer"),
        "primary_limiter": (p.get("limiters") or {}).get("primary_limiter"),
        "n_binding_limiters": (p.get("limiters") or {}).get("n_binding"),
        "next_experiment": ((p.get("experiment_queue") or {}).get("queued")
                            or [{}])[0].get("experiment_id"),
    }


__all__ = ["SCHEMA_VERSION", "COMPOSITION_OWNER", "PHASE", "ROUTE",
           "STATE_READY", "STATE_DEGRADED", "STATE_UNAVAILABLE",
           "READ_STATE_VOCAB", "SAFETY_BADGES", "LIMITER_VOCAB",
           "SEVERITY_VOCAB", "rank_limiters", "build_scoreboard",
           "load_alpha_capital", "summary"]
