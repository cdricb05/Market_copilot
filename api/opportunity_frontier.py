r"""api/opportunity_frontier.py - Release 50: composition + read owner of the ONE
cross-asset opportunity frontier.

Sources (never computes):

* equity rankings           -> ``api.universe_scoring`` (the approved model's percentiles)
* sleeve eligibility        -> ``api.investability_registry`` (the ONE registry)
* non-equity descriptors    -> registry + ``api.market_reference_data`` (owned metadata / marks)
* current positions + NAV   -> ``api.portfolio_state`` (position contracts)
* risk inputs               -> ``api.cross_asset_risk`` (the ONE risk state)
* calibrated expected return-> ``api.return_forecast`` operational lane ONLY when calibrated

and runs ``engine.opportunity_frontier.build_frontier`` once. It also owns the
frontier REVIEW of non-equity holdings (the equity holdings are reviewed by the
Holding Opportunity-Cost owner): a non-equity position whose sleeve is no longer
capital-eligible is a mandatory exit; an eligible one is retained and re-sized by
the frontier's own score. Read-only; writes nothing; promotes nothing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.api import investability_registry as ir
from paper_trader.engine import instrument_contract as ic
from paper_trader.engine import opportunity_frontier as kernel

PHASE = "R50"
OWNER = "api.opportunity_frontier"
ROUTE = "/v1/operations/opportunity-frontier"

REVIEW_OWNER = "api.opportunity_frontier (non-equity holdings)"


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def frontier_reviews(frontier: dict, positions: list) -> list[dict]:
    """HOC-shaped review rows for NON-EQUITY holdings (the shape the proposal
    kernel already consumes). Eligibility-based: an ineligible sleeve's position
    is a mandatory EXIT; an eligible one is HOLD (its size is then set by the
    frontier score through the same allocation passes as every other name)."""
    rows = {r["instrument_id"]: r for r in (frontier or {}).get("rows") or []}
    out = []
    for p in positions or []:
        if p.get("instrument_type") in (None, ic.IT_CASH_EQUITY, ic.IT_CASH):
            continue
        tk = p.get("instrument_id")
        r = rows.get(tk) or {}
        eligible = bool(r.get("eligible"))
        out.append({
            "ticker": tk, "recommendation": "HOLD" if eligible else "EXIT",
            "current_rank": r.get("rank"), "current_score": r.get("opportunity_score"),
            "signal_strength": r.get("opportunity_score"),
            "strongest_replacement_ticker": None, "replacement_rank": None,
            "replacement_score": None, "gross_score_improvement": None,
            "net_improvement": None, "switching_cost_usd": None,
            "deterioration_state": "ELIGIBLE" if eligible else "SLEEVE_INELIGIBLE",
            "drawdown_60d": None, "volatility_60d": r.get("volatility_annualised"),
            "liquidity_state": r.get("liquidity_state"),
            "risk_contribution_pct": r.get("risk_contribution"),
            "review_owner": REVIEW_OWNER,
            "reason_codes": ([] if eligible else [r.get("eligibility_reason") or "SLEEVE_NOT_CAPITAL_ELIGIBLE"]),
        })
    return out


def load_opportunity_frontier(*, portfolio_state: Optional[dict] = None,
                              scoring: Optional[dict] = None,
                              registry: Optional[dict] = None,
                              risk_state: Optional[dict] = None,
                              approvals: Optional[dict] = None,
                              expected_returns: Optional[dict] = None,
                              policy: Optional[dict] = None,
                              probe: bool = True) -> dict:
    """The GET read model. Read-only, degrade-safe. ``approvals`` is the hermetic
    injection seam of the registry (never a production input)."""
    from paper_trader.api import capital_pool as cp
    if portfolio_state is None:
        from paper_trader.api import portfolio_state as _ps
        portfolio_state = _ps.load_portfolio_state()
    ps = portfolio_state or {}
    cap = ps.get("capital") or {}
    nav = _f(cap.get("nav"))
    as_of = (ps.get("dates") or {}).get("eligible_market_date")
    if scoring is None:
        try:
            from paper_trader.api import universe_scoring as us
            scoring = us.load_universe_scoring()
        except Exception:  # noqa: BLE001
            scoring = {"rankings": []}
    if registry is None:
        registry = ir.load_investability_registry(approvals=approvals, probe=probe,
                                                  as_of=as_of, nav=nav)
    pol = {"max_name_weight": 0.10, "min_adv_dollar": 1.0e7, "max_adv_participation": 1.0}
    try:
        from paper_trader.api import multi_horizon_engine as eng
        pol.update({"max_name_weight": float(eng.MAX_INDIVIDUAL_WEIGHT),
                    "min_adv_dollar": float(eng.MIN_ADV_DOLLAR)})
    except Exception:  # noqa: BLE001
        pass
    if policy:
        pol.update(policy)
    positions = cp.positions_from_state(ps)
    sm = ir.sleeve_map(registry)
    equity_ok = bool((sm.get(ic.DEFAULT_EQUITY_SLEEVE) or {}).get("capital_eligible"))
    try:
        instruments = ir.eligible_non_equity_instruments(
            registry, nav=nav, as_of=as_of, max_name_weight=float(pol["max_name_weight"]))
    except Exception:  # noqa: BLE001
        instruments = []
    fr = kernel.build_frontier(
        eligible_market_date=as_of, nav=nav,
        equity_rankings=list((scoring or {}).get("rankings") or []),
        equity_sleeve_eligible=equity_ok, non_equity_instruments=instruments,
        positions=positions, risk_state=risk_state, expected_returns=expected_returns,
        policy=pol)
    fr.update({
        "owner": OWNER, "route": ROUTE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "registry_identity": {"capital_eligible_sleeve_ids": registry.get("capital_eligible_sleeve_ids"),
                              "non_equity_eligible_sleeve_ids": registry.get("non_equity_eligible_sleeve_ids"),
                              "approvals_injected": registry.get("approvals_injected")},
        "universe_scoring_hash": (scoring or {}).get("output_hash"),
        "portfolio_state_hash": ps.get("state_hash"),
        "economic_state_hash": ps.get("economic_state_hash"),
        "risk_state_hash": (risk_state or {}).get("risk_state_hash"),
        "non_equity_reviews": frontier_reviews(fr, positions),
        "candidate_rows_for_proposal": kernel.candidate_rows_for_proposal(fr),
        # MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1 - the admission ledger. A frontier
        # whose eligible_non_equity_count is 0 says, per sleeve, WHY: no candidate
        # accruing, a gate not yet passed (with the exact remaining requirements),
        # or instruments listed but not executable at this NAV. A zero is never
        # again hidden behind a nominal "cross-asset" feature.
        "non_equity_admission_ledger": non_equity_admission_ledger(registry, fr),
    })
    fr["eligible_non_equity_count_explanation"] = _explain_non_equity_count(fr)
    return fr


def non_equity_admission_ledger(registry: dict, frontier: dict) -> list[dict]:
    """Per research sleeve: eligibility, blocker, gate state, rows listed / admitted."""
    rows = (frontier or {}).get("rows") or []
    navv = _f((frontier or {}).get("nav"))
    cap = _f(((frontier or {}).get("policy") or {}).get("max_name_weight"))
    by_sleeve: dict[str, dict] = {}
    for r in rows:
        if r.get("instrument_type") in (ic.IT_CASH, ic.IT_CASH_EQUITY):
            continue
        b = by_sleeve.setdefault(str(r.get("sleeve_id")),
                                 {"listed": 0, "admitted": 0, "reasons": {},
                                  "min_nav": None, "cheapest": None})
        b["listed"] += 1
        if r.get("eligible"):
            b["admitted"] += 1
        elif r.get("eligibility_reason"):
            b["reasons"][r["eligibility_reason"]] = b["reasons"].get(r["eligibility_reason"], 0) + 1
    # The CHEAPEST unit in a sleeve sets the NAV at which that sleeve first becomes
    # holdable AT ALL, and this is asked of EVERY non-equity sleeve, including the
    # ones the capital gate has not passed. Granularity sits below the gate: a
    # sleeve whose smallest contract cannot fit the name cap stays unfundable on
    # the day its forward evidence finally arrives. Computing it only for sleeves
    # that already cleared the gate would surface the wall years after the point
    # at which knowing about it could change anything. This admits nothing.
    if cap:
        for d in ir.eligible_non_equity_instruments(
                registry, nav=navv, max_name_weight=cap, include_ineligible_sleeves=True):
            un = _f(d.get("unit_notional_usd"))
            if un is None:
                continue
            b = by_sleeve.setdefault(str(d.get("sleeve_id")),
                                     {"listed": 0, "admitted": 0, "reasons": {},
                                      "min_nav": None, "cheapest": None})
            need = un / cap
            if b["min_nav"] is None or need < b["min_nav"]:
                b["min_nav"], b["cheapest"] = need, d.get("instrument_id")
    out = []
    for s in (registry or {}).get("sleeves") or []:
        if s.get("asset_class") in (ic.AC_US_EQUITY, ic.AC_CASH):
            continue
        gate = s.get("capital_eligibility_gate") or {}
        b = by_sleeve.get(s["sleeve_id"], {"listed": 0, "admitted": 0, "reasons": {},
                                           "min_nav": None, "cheapest": None})
        out.append({
            "sleeve_id": s["sleeve_id"], "asset_class": s.get("asset_class"),
            "capital_eligible": bool(s.get("capital_eligible")),
            "blocker": s.get("capital_ineligible_reason"),
            "operational_signal_candidate": (s.get("operational_signal_candidate") or {}).get(
                "challenger_id"),
            "gate_state": gate.get("state"),
            "gate_remaining": gate.get("remaining_codes") or [],
            "instruments_listed": b["listed"], "instruments_admitted": b["admitted"],
            "instrument_ineligibility_reasons": b["reasons"],
            "minimum_nav_for_one_unit_usd": (round(b["min_nav"], 2)
                                             if b["min_nav"] is not None else None),
            "cheapest_instrument_id": b["cheapest"],
            "nav_multiple_required": (round(b["min_nav"] / navv, 4)
                                      if (b["min_nav"] is not None and navv) else None),
        })
    return out


def _explain_non_equity_count(frontier: dict) -> str:
    n = int((frontier or {}).get("eligible_non_equity_count") or 0)
    ledger = (frontier or {}).get("non_equity_admission_ledger") or []
    if n > 0:
        admitted = [l["sleeve_id"] for l in ledger if l["instruments_admitted"]]
        return ("%d non-equity instrument(s) are eligible, from sleeve(s) %s; they compete "
                "for capital on the sleeve-normalised rank basis." % (n, ", ".join(admitted)))
    parts = []
    for l in ledger:
        if l["capital_eligible"]:
            why = ("sleeve eligible but %d listed instrument(s) failed: %s"
                   % (l["instruments_listed"], l["instrument_ineligibility_reasons"] or "no instrument"))
        elif l.get("operational_signal_candidate"):
            why = "gate %s (%s)" % (l.get("gate_state"), ", ".join(l.get("gate_remaining") or []) or "-")
        else:
            why = l.get("blocker") or "no candidate"
        # Granularity is the one blocker no amount of evidence clears, so where it
        # binds the sentence carries the NAV that would, and names the instrument
        # that sets it. Saying only "not executable" invites a search for a bug.
        if l.get("minimum_nav_for_one_unit_usd") and "UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV" in (
                l.get("instrument_ineligibility_reasons") or {}):
            why += (" [cheapest unit %s needs a $%s book at the declared name cap]"
                    % (l.get("cheapest_instrument_id"),
                       format(l["minimum_nav_for_one_unit_usd"], ",.0f")))
        parts.append("%s: %s" % (l["sleeve_id"], why))
    return ("0 non-equity instruments are eligible. Per sleeve - " + "; ".join(parts)
            if parts else "0 non-equity instruments are eligible; the registry declares no "
                          "research sleeve.")


__all__ = ["PHASE", "OWNER", "ROUTE", "REVIEW_OWNER", "frontier_reviews",
           "load_opportunity_frontier", "non_equity_admission_ledger"]
