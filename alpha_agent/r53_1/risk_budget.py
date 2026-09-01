r"""alpha_agent.r53_1.risk_budget - risk as a BUDGET, not a veto (SHADOW).

Release 53 proved the score-only switching hurdle is structurally blind to
diversification: a sleeve that lowers portfolio risk can never clear a gate
expressed purely in score percentile. This module is the SHADOW extension of
the canonical allocator that makes the missing quantity explicit:

    "How much expected-return strength do we gain PER UNIT OF INCREMENTAL
     PORTFOLIO RISK, and is the new risk use superior to the current use?"

What is reused (no second allocator, no second risk model):

* volatility/correlation/diversification arithmetic -
  ``engine.cross_asset_risk`` via the R53 advisory seam
  (``alpha_agent.r53.capital_competition._aligned_returns``);
* sleeve signal strength - the SAME rank-normalised percentile scores the
  canonical opportunity frontier consumes
  (``alpha_agent.r53.capital_competition.sleeve_signal_scores``);
* unit granularity - the owned contract probe
  (:mod:`alpha_agent.r53_1.executable_universe`);
* the production policy is read from its owner and NEVER mutated.

EXPECTED-RETURN HONESTY. No sleeve has a calibrated expected return
(``NOT_CALIBRATED`` everywhere). Every "alpha strength" here is the ORDINAL
percentile the canonical frontier already uses, and every return-per-risk
figure is therefore ordinal too: valid for ranking risk uses, meaningless as
a forecast. The artifact says so.

THE FOUR CASES the budget view can recognise (a score-only hurdle cannot):

    CASE 1  modest alpha + large diversification  -> allocation justified
    CASE 2  strong alpha + high correlation       -> smaller allocation
    CASE 3  strong alpha + efficient diversification -> more risk capital
    CASE 4  weak alpha                            -> cash / zero

SHADOW ONLY. Nothing here approves, proposes, orders, or changes the 0.05
production hurdle. The three shadow policies are compared PROSPECTIVELY on
the same opportunities; no outcome-optimisation is performed.
"""
from __future__ import annotations

import math
from typing import Any, Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, research_dir,
               safety_block, write_json)
from ..r53.capital_competition import (EQUITY_BOOK_PROXY, FLAGSHIP_CHALLENGERS,
                                       _aligned_returns, sleeve_signal_scores)
from .executable_universe import probe_contract

CALCULATION_OWNER = "alpha_agent.r53_1.risk_budget"
ARTIFACT = "R53_1_RISK_BUDGET_SHADOW.json"

#: The three SHADOW risk-budget policies, declared a priori (extending the
#: R53 shadow policy family with explicit budgets). NOT tuned on outcomes.
SHADOW_BUDGET_POLICIES = {
    "CURRENT_CONSERVATIVE_POLICY": {
        "vol_budget_multiple": 1.00,   # portfolio vol may not rise at all
        "unit_weight_cap": 0.10,       # the production name cap
        "asset_class_cap": 0.25,       # the production R50 class cap
        "max_one_way_turnover": 0.35,
    },
    "MODERATE_ACTIVE_POLICY": {
        "vol_budget_multiple": 1.15,
        "unit_weight_cap": 0.15,
        "asset_class_cap": 0.30,
        "max_one_way_turnover": 0.50,
    },
    "HIGH_ACTIVE_POLICY": {
        "vol_budget_multiple": 1.35,
        "unit_weight_cap": 0.20,
        "asset_class_cap": 0.35,
        "max_one_way_turnover": 0.75,
    },
}

#: Ordinal thresholds for the case taxonomy. Declared, not searched.
ALPHA_STRONG = 0.70
ALPHA_MODEST = 0.40
RHO_HIGH_DIVERSIFICATION = 0.30
RHO_LOW_DIVERSIFICATION = 0.70

#: Desk cost convention, per side, bps (api.paper_trading_desk canonical).
COST_BPS_PER_SIDE = 12.5

#: The candidate instrument evaluated per sleeve: the flagship contract the
#: R53 competition used, PLUS its owned smaller variant where Track D found
#: one (granularity changes the answer, so both are evaluated).
SLEEVE_CANDIDATES = {
    "sleeve_equity_index_futures": ("&ES", "&MES", "&M2K"),
    "sleeve_volatility_futures": ("&VX",),
    "sleeve_commodity_futures": ("&GC", "&CL"),
    "sleeve_fx_futures": ("&6E", "&6A"),
    "sleeve_rates_futures": ("&ZN",),
    "sleeve_crypto_futures": ("&BTC", "&MBT", "&MET"),
}


def _std(xs: list) -> Optional[float]:
    if len(xs) < 20:
        return None
    m = sum(xs) / len(xs)
    v = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(v) if v > 0 else None


def _corr(a: list, b: list) -> Optional[float]:
    n = min(len(a), len(b))
    if n < 20:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    ca = [x - ma for x in a]
    cb = [x - mb for x in b]
    den = math.sqrt(sum(x * x for x in ca) * sum(x * x for x in cb))
    if den <= 0:
        return None
    return sum(x * y for x, y in zip(ca, cb)) / den


def _max_drawdown(rets: list) -> Optional[float]:
    if len(rets) < 20:
        return None
    level, peak, mdd = 1.0, 1.0, 0.0
    for r in rets:
        level *= (1.0 + r)
        peak = max(peak, level)
        mdd = min(mdd, level / peak - 1.0)
    return mdd


def alpha_strength(sleeve_id: str) -> dict:
    """The sleeve's ordinal signal strength on the frontier's shared scale."""
    ss = sleeve_signal_scores(sleeve_id)
    scores = {k: v for k, v in (ss.get("scores") or {}).items()
              if v is not None}
    if not scores:
        return {"state": ss.get("state") or "NO_SCORES", "percentile": None,
                "challenger_id": ss.get("challenger_id")}
    best = max(scores.values())
    return {"state": "OK", "percentile": round(float(best), 6),
            "percentile_basis": "WITHIN_SLEEVE_ORDINAL - the top leg of a "
                                "rank-normalised sleeve book is ~1.0 by "
                                "construction; NOT comparable across "
                                "sleeves (the shared-scale figures live in "
                                "the R53 competition artifact)",
            "n_long_instruments": len(scores),
            "challenger_id": ss.get("challenger_id")}


def classify_case(percentile: Optional[float], rho: Optional[float]) -> str:
    if percentile is None:
        return "CASE_4_WEAK_OR_UNMEASURED"
    if percentile < ALPHA_MODEST:
        return "CASE_4_WEAK_ALPHA"
    strong = percentile >= ALPHA_STRONG
    if rho is not None and rho <= RHO_HIGH_DIVERSIFICATION:
        return "CASE_3_STRONG_ALPHA_EFFICIENT_DIVERSIFICATION" if strong \
            else "CASE_1_MODEST_ALPHA_LARGE_DIVERSIFICATION"
    if rho is not None and rho >= RHO_LOW_DIVERSIFICATION:
        return "CASE_2_ALPHA_WITH_HIGH_CORRELATION"
    return ("CASE_3_STRONG_ALPHA_MODERATE_DIVERSIFICATION" if strong
            else "CASE_1_MODEST_ALPHA_MODERATE_DIVERSIFICATION")


def evaluate_candidate(*, sleeve_id: str, symbol: str, nav: float,
                       aligned: dict, strength: dict) -> dict:
    """One candidate risk use, evaluated against every shadow budget."""
    row: dict[str, Any] = {"sleeve_id": sleeve_id, "instrument": symbol,
                           "alpha": strength}
    series = aligned["series"]
    book = series.get(EQUITY_BOOK_PROXY)
    cand = series.get(symbol)
    if not book or not cand:
        row["state"] = "NO_ALIGNED_HISTORY"
        return row
    sigma_b, sigma_i = _std(book), _std(cand)
    rho = _corr(book, cand)
    if sigma_b is None or sigma_i is None or rho is None:
        row["state"] = "RISK_NOT_MEASURABLE"
        return row
    contract = probe_contract(symbol)
    unit = contract.get("unit_notional_usd")
    margin = contract.get("initial_margin_per_unit")
    row.update({
        "daily_sigma_book_proxy": round(sigma_b, 6),
        "daily_sigma_candidate": round(sigma_i, 6),
        "correlation_to_book": round(rho, 4),
        "max_drawdown_window": _max_drawdown(cand),
        "unit_notional_usd": unit,
        "initial_margin_per_unit": margin,
        "median_volume_21d": contract.get("median_volume_21d"),
    })
    pct = strength.get("percentile")
    row["case"] = classify_case(pct, rho)
    row["policies"] = {}
    for pol_name, pol in SHADOW_BUDGET_POLICIES.items():
        cap = float(pol["unit_weight_cap"])
        verdictd: dict[str, Any] = {"unit_weight_cap": cap}
        if not unit:
            verdictd["verdict"] = "NOT_PRICEABLE"
            row["policies"][pol_name] = verdictd
            continue
        units = int((cap * nav) // unit)
        w = units * unit / nav
        verdictd.update({"whole_units": units,
                         "achievable_weight": round(w, 4)})
        if units < 1:
            verdictd["verdict"] = "BLOCKED_BY_UNIT_GRANULARITY"
            verdictd["min_nav_for_one_unit"] = round(unit / cap, 2)
            row["policies"][pol_name] = verdictd
            continue
        # pro-rata funding from the book; one candidate at a time (marginal)
        sigma_new = math.sqrt(max(
            (1 - w) ** 2 * sigma_b ** 2 + w ** 2 * sigma_i ** 2
            + 2 * w * (1 - w) * rho * sigma_b * sigma_i, 0.0))
        sigma_undiv = (1 - w) * sigma_b + w * sigma_i
        d_sigma = sigma_new - sigma_b
        verdictd.update({
            "portfolio_sigma_before": round(sigma_b, 6),
            "portfolio_sigma_after": round(sigma_new, 6),
            "delta_sigma": round(d_sigma, 6),
            "diversification_benefit_sigma": round(sigma_undiv - sigma_new, 6),
            "round_trip_cost_return": round(
                2 * COST_BPS_PER_SIDE / 1e4 * w, 6),
        })
        if pct is not None and abs(d_sigma) > 1e-12:
            # ORDINAL: percentile points of signal strength per 1% of
            # incremental portfolio volatility. Ranking only, not a forecast.
            verdictd["ordinal_strength_per_pct_incremental_vol"] = round(
                pct / (abs(d_sigma) / sigma_b * 100.0), 4)
        checks = {
            "vol_budget": sigma_new <= pol["vol_budget_multiple"] * sigma_b
                          + 1e-12,
            "unit_weight_cap": w <= cap + 1e-9,
            "asset_class_cap": w <= pol["asset_class_cap"] + 1e-9,
            "turnover_budget": w <= pol["max_one_way_turnover"] + 1e-9,
            "collateral": (margin or 0.0) * units <= nav,
        }
        verdictd["budget_checks"] = checks
        failed = [k for k, ok in checks.items() if not ok]
        verdictd["verdict"] = ("ALLOCATABLE_WITHIN_BUDGETS" if not failed
                               else "BLOCKED_BY_" + failed[0].upper())
        row["policies"][pol_name] = verdictd
    row["state"] = "OK"
    return row


def run_shadow(nav: Optional[float] = None) -> dict:
    from .executable_universe import actual_nav
    nav = float(nav) if nav else (actual_nav() or 99000.0)
    instruments = sorted({s for syms in SLEEVE_CANDIDATES.values()
                          for s in syms})
    aligned = _aligned_returns(instruments)
    if aligned is None:
        return {"state": "NO_ALIGNED_HISTORY", "nav": nav}
    rows = []
    for sleeve_id, syms in SLEEVE_CANDIDATES.items():
        strength = alpha_strength(sleeve_id)
        for sym in syms:
            rows.append(evaluate_candidate(
                sleeve_id=sleeve_id, symbol=sym, nav=nav,
                aligned=aligned, strength=strength))
    summary = {}
    for pol_name in SHADOW_BUDGET_POLICIES:
        allocatable = [r["instrument"] for r in rows
                       if (r.get("policies") or {}).get(pol_name, {}).get(
                           "verdict") == "ALLOCATABLE_WITHIN_BUDGETS"]
        granularity_blocked = [
            r["instrument"] for r in rows
            if (r.get("policies") or {}).get(pol_name, {}).get(
                "verdict") == "BLOCKED_BY_UNIT_GRANULARITY"]
        summary[pol_name] = {"allocatable": sorted(set(allocatable)),
                             "granularity_blocked": sorted(
                                 set(granularity_blocked))}
    return {"state": "OK", "nav": nav, "rows": rows,
            "policy_summary": summary,
            "cases_present": sorted({r.get("case") for r in rows
                                     if r.get("case")})}


def write_artifact() -> dict:
    res = run_shadow()
    body = artifact_body(
        "r53_1_risk_budget_shadow/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        question="expected-return strength per unit of incremental portfolio "
                 "risk: is the new risk use superior to the current use?",
        expected_return_honesty=(
            "every alpha strength is the frontier's ORDINAL percentile "
            "(expected returns are NOT_CALIBRATED); return-per-risk figures "
            "rank risk uses and forecast nothing"),
        score_only_contrast=(
            "the production switching hurdle (0.05 net score improvement) "
            "rejected every reallocation to date and cannot see the "
            "delta_sigma or diversification_benefit_sigma columns at all; "
            "this artifact is the missing axis, in SHADOW"),
        shadow_budget_policies=SHADOW_BUDGET_POLICIES,
        case_taxonomy={
            "CASE_1": "modest standalone alpha + large diversification value "
                      "=> meaningful allocation may be justified",
            "CASE_2": "strong standalone alpha + high correlation / "
                      "concentration => smaller allocation",
            "CASE_3": "strong alpha + efficient diversification => more "
                      "risk capital",
            "CASE_4": "weak alpha => cash / zero weight",
        },
        thresholds={"alpha_strong": ALPHA_STRONG, "alpha_modest": ALPHA_MODEST,
                    "rho_high_div": RHO_HIGH_DIVERSIFICATION,
                    "rho_low_div": RHO_LOW_DIVERSIFICATION,
                    "declared_not_searched": True},
        flagships=FLAGSHIP_CHALLENGERS,
        production_hurdle_untouched=True,
        **res, **safety_block())
    write_json(research_dir() / ARTIFACT, body)
    return body
