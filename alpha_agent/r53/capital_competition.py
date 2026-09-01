r"""alpha_agent.r53.capital_competition - Track C: the SHADOW cross-market
capital competition. For every serious challenger sleeve: IF it were eligible
today, how much capital would the CURRENT canonical allocator give it, what
would it displace, and which constraint decides?

HOW IT STAYS ON THE ONE ALLOCATOR
---------------------------------
This module invents no scoring, no risk model and no allocator. It drives the
PRODUCTION owners hermetically, in-process, through the seams they already
declare:

* sleeve eligibility  -> ``api.investability_registry.load_investability_registry``
  with the ``approvals=`` INJECTION seam (the R50/R51 hermetic-test seam; the
  registry itself documents that no production path can promote through it);
* opportunity scores  -> each sleeve's flagship FROZEN challenger's CURRENT
  book (``alpha_agent.r46.challengers.build``), rank-normalised by the
  frontier's own ``rank_normalise``;
* the frontier        -> ``api.opportunity_frontier.load_opportunity_frontier``;
* the feasible target -> ``engine.constrained_reallocation.solve_feasible_target``
  under the UNCHANGED production policy, followed by the canonical switching
  economics and outcome rule;
* diversification     -> ``engine.cross_asset_risk.diversification_effect``
  (advisory-only, as that owner declares).

LONG-ONLY TRANSLATION, DECLARED
-------------------------------
The production mandate is long-only. A dollar-neutral challenger book enters
the competition through its LONG legs only (rank-normalised); a sleeve whose
current signal is net SHORT its only instrument receives no long capital and
the artifact says exactly that, because "the allocator would short it" is not
an answer the production mandate can express.

SHADOW ONLY. Zero writes to any operational store; approvals exist only inside
this process; promotion gates, registry files and the production book are
untouched. A sleeve leaves shadow ONLY through the existing manual promotion
governance - never through this module.
"""
from __future__ import annotations

from typing import Any, Optional

from . import (CAMPAIGN_ID, RELEASE, artifact_body, read_json, research_dir,
               safety_block, sha, write_json)

CALCULATION_OWNER = "alpha_agent.r53.capital_competition"
ARTIFACT = "R53_SHADOW_CAPITAL_COMPETITION.json"

#: sleeve_id -> the flagship frozen challenger whose CURRENT book supplies the
#: sleeve's opportunity scores. One flagship per sleeve, declared here: the
#: cell the R51 promotion frontier ranks closest to approval within the
#: sleeve's family (and for commodities the momentum sibling, the family's
#: longest-running cell).
FLAGSHIP_CHALLENGERS = {
    "sleeve_equity_index_futures": "r52_eqidx_xs_rel_mom_12_1",
    "sleeve_volatility_futures": "r46_vx_term_carry_5d",
    "sleeve_commodity_futures": "r46_comdty_xs_mom_252",
    "sleeve_fx_futures": "r51_fx_xs_carry_cip",
    "sleeve_rates_futures": "r52_rates_copper_gold_lead",
}

#: The equity book's return proxy for the advisory diversification numbers:
#: the current production book is ~25 US large caps, whose daily behaviour is
#: dominated by the market factor, and &ES is the owned instrument that
#: carries it. Declared, advisory-only, never a sizing input.
EQUITY_BOOK_PROXY = "&ES"
DIVERSIFICATION_LOOKBACK = 120
DIVERSIFICATION_DELTA = 0.10


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Sleeve signal construction (long legs of the flagship's CURRENT book)
# --------------------------------------------------------------------------- #
def sleeve_signal_scores(sleeve_id: str) -> dict:
    from ..r46 import challengers as CH
    from paper_trader.engine import opportunity_frontier as OF
    cid = FLAGSHIP_CHALLENGERS.get(sleeve_id)
    if not cid:
        return {"state": "NO_FLAGSHIP", "scores": {}, "challenger_id": None}
    spec = CH.spec_by_id(cid)
    if spec is None:
        return {"state": "SPEC_MISSING", "scores": {}, "challenger_id": cid}
    book = CH.build(spec)
    legs = book.get("legs") or []
    long_scores = {l["instrument"]: _f(l.get("score"))
                   for l in legs if _f(l.get("weight")) and l["weight"] > 0}
    long_scores = {k: v for k, v in long_scores.items() if v is not None}
    short_legs = [l["instrument"] for l in legs
                  if _f(l.get("weight")) and l["weight"] < 0]
    if not long_scores:
        state = ("SIGNAL_DIRECTION_SHORT_ONLY" if short_legs else
                 ("CHALLENGER_" + str(book.get("state") or "NO_BOOK")))
        return {"state": state, "scores": {}, "challenger_id": cid,
                "spec_hash": CH.spec_hash(spec),
                "short_legs": short_legs,
                "book_state": book.get("state")}
    return {"state": "OK", "scores": OF.rank_normalise(long_scores),
            "raw_long_scores": long_scores, "short_legs": short_legs,
            "challenger_id": cid, "spec_hash": CH.spec_hash(spec),
            "book_state": book.get("state")}


def build_approvals(sleeve_ids: list[str]) -> tuple[dict, dict]:
    """The hermetic ``approvals=`` injection for the chosen sleeves, plus the
    per-sleeve signal diagnostics. Nothing here persists anywhere."""
    from paper_trader.api import investability_registry as ir
    approvals, diagnostics = {}, {}
    for sid in sleeve_ids:
        sig = sleeve_signal_scores(sid)
        diagnostics[sid] = {k: v for k, v in sig.items() if k != "scores"}
        if sig["state"] == "OK":
            approvals[sid] = {
                "model_approval_state": ir.APPROVED,
                "approval_evidence": {
                    "state": "R53_SHADOW_COMPETITION_HYPOTHETICAL",
                    "meaning": "counterfactual eligibility injected in-process "
                               "for the shadow capital competition; NOT an "
                               "approval and never persisted"},
                "signal_scores": sig["scores"],
            }
    return approvals, diagnostics


# --------------------------------------------------------------------------- #
# Diversification advisory (owner: engine.cross_asset_risk)
# --------------------------------------------------------------------------- #
def _aligned_returns(instruments: list[str]) -> Optional[dict]:
    from paper_trader.api import market_reference_data as mrd
    if not mrd.available():
        return None
    series, dates_ref = {}, None
    for sym in [EQUITY_BOOK_PROXY] + [s for s in instruments
                                      if s != EQUITY_BOOK_PROXY]:
        try:
            bars = mrd.daily_bars(sym)
        except Exception:  # noqa: BLE001 - advisory path, degrade-safe
            continue
        if not bars or len(bars) < DIVERSIFICATION_LOOKBACK + 1:
            continue
        tail = list(bars)[-(DIVERSIFICATION_LOOKBACK + 1):]
        d = [str(row[0]) for row in tail]
        c = [float(row[1]) for row in tail]
        rets = {}
        for i in range(1, len(c)):
            if c[i - 1] > 0:
                rets[d[i]] = c[i] / c[i - 1] - 1.0
        series[sym] = rets
        if sym == EQUITY_BOOK_PROXY:
            dates_ref = [dt for dt in d[1:]]
    if EQUITY_BOOK_PROXY not in series or dates_ref is None:
        return None
    common = [dt for dt in dates_ref
              if all(dt in s for s in series.values())]
    if len(common) < 40:
        return None
    return {"dates": common,
            "series": {sym: [s[dt] for dt in common]
                       for sym, s in series.items()}}


def diversification_advisories(instruments: list[str]) -> dict:
    from paper_trader.engine import cross_asset_risk as XR
    aligned = _aligned_returns(instruments)
    if aligned is None:
        return {"state": "NOT_COMPUTED",
                "reason": "aligned return history unavailable in-process"}
    weights = {EQUITY_BOOK_PROXY: 1.0}
    pol = XR.default_policy()
    risk = XR.portfolio_risk(weights=weights, aligned_returns=aligned,
                             policy=pol)
    out = {"state": "OK", "advisory_only": True,
           "book_proxy": EQUITY_BOOK_PROXY,
           "book_proxy_note": ("the ~25-name US large-cap production book is "
                               "proxied by its owned market-factor "
                               "instrument for this ADVISORY panel only"),
           "delta_weight": DIVERSIFICATION_DELTA, "rows": {}}
    for sym in instruments:
        if sym == EQUITY_BOOK_PROXY or sym not in aligned["series"]:
            continue
        try:
            eff = XR.diversification_effect(
                risk=risk, candidate=sym, aligned_returns=aligned,
                weights=weights, delta_weight=DIVERSIFICATION_DELTA,
                policy=pol)
            out["rows"][sym] = {
                k: v for k, v in eff.items()
                if isinstance(v, (int, float, bool, str)) or v is None}
        except Exception as exc:  # noqa: BLE001
            out["rows"][sym] = {"state": "FAILED",
                                "detail": str(exc)[:160]}
    return out


# --------------------------------------------------------------------------- #
# One competition scenario through the canonical owners
# --------------------------------------------------------------------------- #
def run_scenario(*, scenario_id: str, sleeve_ids: list[str],
                 nav_override: Optional[float] = None) -> dict:
    from paper_trader.api import capital_pool as cp
    from paper_trader.api import opportunity_frontier as api_of
    from paper_trader.api import portfolio_state as psmod
    from paper_trader.engine import constrained_reallocation as CR

    import copy

    ps = psmod.load_portfolio_state()
    cap = ps.get("capital") or {}
    nav = _f(nav_override) or _f(cap.get("nav"))
    if nav_override is not None:
        # A counterfactual NAV must reach the WHOLE canonical path - unit
        # granularity and executability are decided in the registry/frontier,
        # not only in the kernel. Local copy; the loaded state is untouched.
        ps = copy.deepcopy(ps)
        ps.setdefault("capital", {})["nav"] = float(nav_override)
    approvals, diagnostics = build_approvals(sleeve_ids)

    frontier = api_of.load_opportunity_frontier(
        portfolio_state=ps, approvals=approvals, probe=True)

    # Candidates: the scoring owner's equity rows (authoritative, via the
    # frontier's own rows) + the eligible non-equity frontier rows in the
    # kernel's shape.
    from paper_trader.engine import opportunity_frontier as OF
    equity_rows = [r for r in frontier.get("rows") or []
                   if r.get("score_basis") == OF.SB_EQUITY_PERCENTILE
                   and r.get("eligible")]
    candidates = [{"ticker": r["instrument_id"], "sector": r.get("sector"),
                   "adv_dollar": r.get("liquidity_adv_dollar"),
                   "score": r.get("opportunity_score"),
                   "rank": r.get("rank")} for r in equity_rows]
    non_equity = OF.candidate_rows_for_proposal(frontier)
    for row in non_equity:
        row = dict(row)
        row["score"] = row.pop("percentile", None)
        candidates.append(row)

    # Current book weights from the ONE position contract.
    positions = cp.positions_from_state(ps)
    current = {p.get("instrument_id"): _f(p.get("exposure_weight")) or 0.0
               for p in positions or []
               if p.get("instrument_type") != "CASH"}

    # Zero-base ideal: the charter question. Top-N equal weight across the
    # COMBINED frontier on the shared percentile scale.
    pol = CR.default_policy()
    n_target = int(pol["target_position_count"])
    ranked = sorted([c for c in candidates if _f(c.get("score")) is not None],
                    key=lambda c: (-float(c["score"]), str(c["ticker"])))
    ideal = {c["ticker"]: 1.0 / n_target for c in ranked[:n_target]}

    solution = CR.solve_feasible_target(
        current_weight=current, ideal_weight=ideal, candidates=candidates,
        nav=nav, policy=pol)
    target = solution.get("best_feasible_target") or {}
    economics = CR.switching_economics(
        current_weight=current, target_weight=target, candidates=candidates,
        nav=nav, mandatory_exits=solution.get("mandatory_exits") or [],
        policy=pol)
    outcome = CR.decide_outcome(solution=solution, economics=economics)

    # Displacement accounting.
    ne_syms = {row["ticker"] for row in non_equity}
    ne_weight = sum(w for tk, w in target.items() if tk in ne_syms)
    cur_equity = sum(current.values())
    tgt_equity = sum(w for tk, w in target.items() if tk not in ne_syms)
    cash_now = max(0.0, 1.0 - cur_equity)
    cash_tgt = max(0.0, 1.0 - sum(target.values()))
    per_sleeve_weight: dict[str, float] = {}
    for row in non_equity:
        w = target.get(row["ticker"]) or 0.0
        if w:
            per_sleeve_weight[row["sleeve_id"]] = (
                per_sleeve_weight.get(row["sleeve_id"], 0.0) + w)

    unit_blocks = [a for a in solution.get("constraint_adjustments") or []
                   if a.get("constraint") == "UNIT_GRANULARITY"]
    # Exclusions that happened UPSTREAM of the kernel: a sleeve was injected
    # but its instruments never became frontier candidates (typically unit
    # granularity at this NAV, or an ineligible row with its own reason).
    injected_sleeves = set(approvals)
    upstream_exclusions = []
    for r in frontier.get("rows") or []:
        if (r.get("sleeve_id") in injected_sleeves
                and not r.get("eligible")
                and r.get("instrument_type") not in (None, "CASH",
                                                     "CASH_EQUITY")):
            upstream_exclusions.append({
                "instrument": r.get("instrument_id"),
                "sleeve_id": r.get("sleeve_id"),
                "reason": r.get("eligibility_reason"),
                "unit_notional_usd": r.get("unit_notional_usd"),
                "executable_at_nav": r.get("executable_at_nav"),
            })
    return {
        "scenario_id": scenario_id,
        "nav_usd": nav,
        "sleeves_injected": sorted(approvals),
        "sleeves_requested": sorted(sleeve_ids),
        "signal_diagnostics": diagnostics,
        "eligible_non_equity_candidates": len(non_equity),
        "equity_candidates": len(candidates) - len(non_equity),
        "outcome": outcome.get("outcome"),
        "clears_switching_hurdle": economics.get("clears_switching_hurdle"),
        "score_improvement_net_of_cost": economics.get(
            "score_improvement_net_of_cost"),
        "one_way_turnover": economics.get("one_way_turnover"),
        "shadow_target_non_equity_weight": round(ne_weight, 6),
        "shadow_target_by_sleeve": {k: round(v, 6)
                                    for k, v in per_sleeve_weight.items()},
        "shadow_target_non_equity_positions": {
            tk: round(w, 6) for tk, w in sorted(target.items())
            if tk in ne_syms},
        "equity_weight_current": round(cur_equity, 6),
        "equity_weight_shadow": round(tgt_equity, 6),
        "equity_capital_displaced": round(max(0.0, cur_equity - tgt_equity), 6),
        "cash_weight_current": round(cash_now, 6),
        "cash_weight_shadow": round(cash_tgt, 6),
        "cash_displaced": round(max(0.0, cash_now - cash_tgt), 6),
        "constraints_that_reshaped": solution.get("constraints_that_reshaped"),
        "unit_granularity_exclusions": [
            {"ticker": a.get("ticker"), "detail": a.get("detail")}
            for a in unit_blocks],
        "upstream_exclusions": upstream_exclusions,
        "allocation_by_asset_class": solution.get(
            "best_feasible_allocation_by_asset_class"),
        "allocation_by_sleeve": solution.get(
            "best_feasible_allocation_by_sleeve"),
        "solution_hash": solution.get("solution_hash"),
    }


def run_competition(*, nav_levels: Optional[list] = None) -> dict:
    """The full shadow competition: each flagship sleeve alone, then all
    jointly, at the real NAV and at a $1M counterfactual (unit granularity is
    a NAV-dependent constraint and the artifact shows exactly where it flips).
    """
    sleeves = sorted(FLAGSHIP_CHALLENGERS)
    scenarios = []
    for nav in (nav_levels or [None, 1_000_000.0]):
        tag = "NAV_ACTUAL" if nav is None else "NAV_%d" % int(nav)
        for sid in sleeves:
            scenarios.append(run_scenario(
                scenario_id="%s__%s" % (tag, sid), sleeve_ids=[sid],
                nav_override=nav))
        scenarios.append(run_scenario(
            scenario_id="%s__ALL_SLEEVES_JOINT" % tag, sleeve_ids=sleeves,
            nav_override=nav))

    instruments = sorted({tk for s in scenarios
                          for tk in (s.get("shadow_target_non_equity_positions")
                                     or {})})
    all_flagged = sorted({i for s in scenarios
                          for d in (s.get("signal_diagnostics") or {}).values()
                          for i in (d.get("short_legs") or [])})
    advisories = diversification_advisories(
        sorted(set(instruments) | set(all_flagged))[:24])

    body = artifact_body(
        "r53_shadow_capital_competition/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        question=("IF this sleeve were eligible today, how much capital "
                  "would the CURRENT canonical allocator give it, what "
                  "would be displaced, and which constraint decides?"),
        allocator_owner="engine.constrained_reallocation (production policy, "
                        "unchanged)",
        eligibility_seam="api.investability_registry approvals= (hermetic, "
                         "in-process, never persisted)",
        long_only_translation=(
            "a dollar-neutral challenger book enters through its LONG legs "
            "only; a net-short signal receives no long capital and is "
            "reported as SIGNAL_DIRECTION_SHORT_ONLY"),
        flagship_challengers=dict(FLAGSHIP_CHALLENGERS),
        n_scenarios=len(scenarios),
        scenarios=scenarios,
        diversification_advisory=advisories,
        promotion_gates_untouched=True,
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT, body)
    return body
