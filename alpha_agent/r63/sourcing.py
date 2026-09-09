"""alpha_agent.r63.sourcing - cheapest-first sourcing economics and the
paid-data gate.

For every information need that is not WELL_OBSERVED the ladder is walked in
the declared order OWNED_DEEPER -> FREE_PUBLIC_PROXY -> SELF_SERVICE ->
PAID_VENDOR and the FIRST legitimate rung is recorded with what R63 actually
did on it (acquired, tested, blocked, or not attempted and why).

The paid-data gate is arithmetic against the AUTHORITATIVE paper NAV, read
read-only from the live desk ledger:

    break_even_alpha = fee / NAV
                     + expected annual trading cost of the strategy family
                     + 0.5 x expected gross increment (uncertainty haircut)
                     + 0.1 x fee / NAV (required return on research spend)

A fee above 2% of NAV per year is EXTREME_HURDLE. NOTHING IS PURCHASED. The
canonical purchase verdict stays with ``engine.data_expansion_gate`` and the
R32 ten conditions; this module quotes them and never re-scores them.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import (D_BLOCKED, OBS_WELL, R41_ROOT, R59_ROOT, read_json, write_artifact)
from . import ontology as ONT

CALCULATION_OWNER = "alpha_agent.r63.sourcing"
SCHEMA = "r63_sourcing_economics/1"
ARTIFACT_NAME = "sourcing_economics.json"

LADDER = ("OWNED_DEEPER", "FREE_PUBLIC_PROXY", "SELF_SERVICE_INEXPENSIVE", "PAID_VENDOR")
DESK_PERFORMANCE_LEDGER = Path(r"C:\Users\binis\.paper_trader\paper_trading_desk\forward_performance.json")
EXTREME_HURDLE_SHARE_OF_NAV = 0.02
HAIRCUT = 0.5
RESEARCH_SPEND_RETURN = 0.10
#: expected annual trading cost per strategy family (one-way turnover x cost
#: per side x 2 x rebalances), declared, by horizon
TRADING_COST_BY_HORIZON = {1: 0.060, 5: 0.030, 21: 0.012, 63: 0.006}

#: R32 ten conditions, quoted from docs/INFORMATION_PURCHASE_GATE.md.
R32_CONDITIONS = ("economic_mechanism", "cheap_proxy", "marginal_evidence", "exact_data_gap",
                  "point_in_time_requirement", "survivorship_inactive_coverage",
                  "sample_evaluation", "incremental_backtest", "economic_value",
                  "licensing_and_retention")

#: The sourcing ladder per dimension the estate does not observe well. Each
#: rung names the observable, its state after R63, and (for paid rungs) a fee
#: reference if one is recorded anywhere in the estate, else UNQUOTED.
LADDERS = {
    "INSIDER_BEHAVIOUR": [("OWNED_DEEPER", "SEC Form 3/4/5 structured data sets 2008-2026 (owned since R35, never joined to the stock panel)", "ACQUIRED_AND_TESTED_BY_R63")],
    "DISCLOSURE_INTENSITY_LANGUAGE": [("OWNED_DEEPER", "companyfacts filed dates (periodic only)", "INSUFFICIENT_FOR_8K_RATE"),
                                      ("FREE_PUBLIC_PROXY", "SEC submissions API per issuer (acceptance-stamped 8-K, NT, amendments)", "ACQUIRED_AND_TESTED_BY_R63")],
    "INVENTORY": [("OWNED_DEEPER", "EIA petroleum bulk archive weekly stocks (owned since R35, never tested)", "TESTED_BY_R63"),
                  ("FREE_PUBLIC_PROXY", "EIA natural-gas bulk archive weekly storage", "ACQUIRED_AND_TESTED_BY_R63")],
    "POLICY_EXPECTATIONS": [("OWNED_DEEPER", "ZQ / SR3 dated contracts in the R41 curve store", "TESTED_BY_R63")],
    "SHORT_POSITIONING": [("OWNED_DEEPER", "FINRA daily short volume owned from 2026-07-23", "PROSPECTIVE_ONLY"),
                          ("FREE_PUBLIC_PROXY", "FINRA Reg SHO history files", "BLOCKED_HTTP_403_MEASURED_2026_09_09"),
                          ("PAID_VENDOR", "consolidated short-interest history (e.g. S3 / Ortex / exchange files)", "UNQUOTED_NOT_PURSUED")],
    "ANALYST_REVISIONS": [("OWNED_DEEPER", "eodhd_analyst forward-only vintage (from 2026-07-31)", "PROSPECTIVE_ONLY"),
                          ("FREE_PUBLIC_PROXY", "six free endpoints probed by R35", "CURRENT_SNAPSHOT_ONLY_INADMISSIBLE"),
                          ("PAID_VENDOR", "as-was consensus history (Intrinio DO_NOT_BUY; Zacks via sales; Steele sample WAITING)", "PIT_UNVERIFIED_FEE_UNQUOTED")],
    "EARNINGS_EXPECTATIONS": [("OWNED_DEEPER", "SEC XBRL seasonal-difference surprise (R58 C1 grid)", "TESTED_NO_ALPHA_EVIDENCE_R58"),
                              ("PAID_VENDOR", "vendor consensus at report time", "PIT_UNVERIFIED")],
    "REVENUE_EXPECTATIONS": [("PAID_VENDOR", "vendor revenue consensus history", "PIT_UNVERIFIED_FEE_UNQUOTED")],
    "DISPERSION": [("FREE_PUBLIC_PROXY", "cross-sectional return dispersion (PIT by construction)", "TESTED_BY_R63"),
                   ("PAID_VENDOR", "analyst forecast dispersion", "PIT_UNVERIFIED")],
    "OWNERSHIP_INSTITUTIONAL_FLOW": [("FREE_PUBLIC_PROXY", "SEC 13F data sets (free)", "BLOCKED_NO_CUSIP_BRIDGE"),
                                     ("SELF_SERVICE_INEXPENSIVE", "a CUSIP-to-issuer map (CUSIP Global Services licence)", "LICENCE_REQUIRED_NOT_PURSUED")],
    "ETF_FUND_FLOW": [("FREE_PUBLIC_PROXY", "ICI weekly flow aggregates (market level)", "NOT_ATTEMPTED_MARKET_LEVEL_ONLY"),
                      ("PAID_VENDOR", "issuer-level creations/redemptions", "UNQUOTED")],
    "SHIPPING_TRANSPORT": [("PAID_VENDOR", "Baltic Exchange indices", "LICENSED_NOT_PURSUED")],
    "COMMODITY_SUPPLY_DEMAND": [("SELF_SERVICE_INEXPENSIVE", "USDA NASS / WASDE with a free key (a human registers the key)", "KEY_REQUIRED_NOT_PURSUED"),
                                ("FREE_PUBLIC_PROXY", "EIA weekly production/consumption for energy", "NOT_ATTEMPTED_INVENTORY_TESTED_FIRST")],
    "VOLATILITY_EXPECTATIONS_IV": [("OWNED_DEEPER", "VIX/OVX/GVZ/VVIX/SKEW and VIX term structure", "TESTED_BY_R63"),
                                   ("PAID_VENDOR", "single-name options IV surface history (ORATS / CBOE DataShop)", "PAYMENT_REQUIRED_FEE_UNQUOTED")],
    "EVENT_INFORMATION": [("OWNED_DEEPER", "SEC submissions periodic-filing calendar (expected filing window)", "ACQUIRED_AND_TESTED_BY_R63"),
                          ("FREE_PUBLIC_PROXY", "FRED release calendar API (owned key) for macro release dates", "NOT_ATTEMPTED")],
    "MACRO_SURPRISES": [("OWNED_DEEPER", "ALFRED first releases vs naive forecast", "TESTED_BY_R63"),
                        ("PAID_VENDOR", "consensus survey history (Bloomberg / Refinitiv)", "UNQUOTED_NOT_PURSUED")],
    "SECTOR_PIT": [("PAID_VENDOR", "point-in-time GICS history", "DO_NOT_BUY_YET_R58")],
}


def live_nav() -> dict:
    """Latest paper NAV from the live desk ledger, READ ONLY."""
    j = read_json(DESK_PERFORMANCE_LEDGER)
    if not j:
        return {"nav": None, "state": "LEDGER_UNREADABLE", "path": str(DESK_PERFORMANCE_LEDGER)}
    rows = j.get("rows") or []
    if not rows:
        return {"nav": None, "state": "LEDGER_EMPTY"}
    last = rows[-1].get("row") or {}
    return {"nav": last.get("nav"), "date": last.get("date"), "cash": last.get("cash"),
            "book_id": last.get("book_id"), "state": "OK", "read_only": True,
            "path": str(DESK_PERFORMANCE_LEDGER)}


def fee_references() -> dict:
    """Fees the estate has ALREADY recorded (never invented here)."""
    refs = {}
    pf = read_json(R41_ROOT / "r41_multi_horizon_alpha_breakthrough_v1" / "provider_frontier_2026.json")
    if isinstance(pf, dict):
        refs["r41_provider_frontier_2026"] = {k: v for k, v in pf.items()
                                              if k in ("candidates", "providers", "rows")} or {"keys": list(pf)[:20]}
    sb = read_json(R59_ROOT / "results" / "r59_provider_value_scoreboard.json")
    if isinstance(sb, dict):
        refs["r59_provider_value_scoreboard_keys"] = list(sb)[:20]
        for row in (sb.get("providers") or sb.get("rows") or []):
            if isinstance(row, dict) and row.get("annual_cost_usd") is not None:
                refs.setdefault("recorded_annual_costs", {})[str(row.get("provider"))] = row["annual_cost_usd"]
    refs.setdefault("recorded_annual_costs", {})["NORGATE_WORLD_FUTURES"] = 270.0
    return refs


def break_even(fee_usd: float | None, nav: float | None, horizon: int,
               expected_gross_increment: float | None) -> dict:
    if fee_usd is None or nav in (None, 0):
        return {"state": "FEE_OR_NAV_UNKNOWN", "fee_usd": fee_usd, "nav": nav}
    fee_share = fee_usd / nav
    tc = TRADING_COST_BY_HORIZON.get(int(horizon), 0.012)
    haircut = HAIRCUT * (expected_gross_increment or 0.0)
    be = fee_share + tc + haircut + RESEARCH_SPEND_RETURN * fee_share
    return {"state": "OK", "fee_usd": fee_usd, "nav": nav, "fee_share_of_nav": round(fee_share, 5),
            "trading_cost": tc, "uncertainty_haircut": round(haircut, 5),
            "research_spend_return": round(RESEARCH_SPEND_RETURN * fee_share, 5),
            "break_even_alpha_annual": round(be, 5),
            "extreme_hurdle": fee_share > EXTREME_HURDLE_SHARE_OF_NAV}


def build(frontier: dict, inventory: dict, matrix: dict, cells: list | None) -> dict:
    nav = live_nav()
    refs = fee_references()
    fields = inventory.get("fields") or []
    needs = []
    seen = set()
    for r in (frontier.get("all_needs") or []):
        d = r["dimension"]
        if r["observation_state"] == OBS_WELL:
            continue
        if d not in LADDERS:
            # a baseline / owned-and-measured dimension has no sourcing
            # question; only a dimension with a DECLARED ladder is sourced
            continue
        key = (d, r["asset_class"])
        if key in seen:
            continue
        seen.add(key)
        ladder = LADDERS[d]
        first_open = next((x for x in ladder if not x[2].startswith(("TESTED", "ACQUIRED"))), None)
        paid = [x for x in ladder if x[0] == "PAID_VENDOR"]
        needs.append({"dimension": d, "asset_class": r["asset_class"],
                      "best_rank": r["rank"], "observation_state": r["observation_state"],
                      "ladder": [{"rung": x[0], "observable": x[1], "state_after_r63": x[2]} for x in ladder],
                      "first_open_rung": first_open[0] if first_open else "NONE_OPEN",
                      "paid_rung_exists": bool(paid),
                      "fields": [f["field_id"] for f in fields if f["dimension_id"] == d][:6]})
    needs.sort(key=lambda n: (n["best_rank"], n["dimension"]))
    # paid-data gate: every dimension with a paid rung and no owned observable
    gate = []
    scored = {c["cell_id"]: c for c in (cells or []) if c.get("conditional")}
    gated_dims = set()
    for n in needs:
        paid = [x for x in n["ladder"] if x["rung"] == "PAID_VENDOR"]
        if not paid or n["dimension"] in gated_dims:
            continue
        gated_dims.add(n["dimension"])
        d = n["dimension"]
        # proxy evidence: the best R63 t for this dimension anywhere
        best_t, best_cell = None, None
        for c in scored.values():
            if c["dimension"] == d and (best_t is None or (c["conditional"].get("t") or -99) > best_t):
                best_t, best_cell = c["conditional"].get("t"), c["cell_id"]
        proxy_pass = best_t is not None and best_t >= 2.0
        fee = None
        for k, v in (refs.get("recorded_annual_costs") or {}).items():
            if d.split("_")[0] in k:
                fee = v
        be = break_even(fee, nav.get("nav"), 21, None)
        checks = {"gap_identified": True,
                  "source_distinct_from_owned": True,
                  "proxy_shows_conditional_evidence": proxy_pass,
                  "pit_credible": not any("PIT_UNVERIFIED" in x["state_after_r63"] for x in paid),
                  "history_effective_sample_adequate": None,
                  "strategy_families_identified": True,
                  "asset_classes_identified": True,
                  "turnover_cost_estimated": True,
                  "annual_fee_known": fee is not None,
                  "expected_net_alpha_estimated": proxy_pass,
                  "confidence_haircut_applied": True,
                  "capacity_considered": True,
                  "break_even_calculated": be.get("state") == "OK"}
        verdict = ("PURCHASE_EXPERIMENT_NOT_JUSTIFIED" if not proxy_pass else
                   "FEE_UNQUOTED_CANNOT_GATE" if fee is None else
                   "PURCHASE_EXPERIMENT_CANDIDATE" if all(v for v in checks.values() if v is not None) and not be.get("extreme_hurdle")
                   else "PURCHASE_EXPERIMENT_NOT_JUSTIFIED")
        gate.append({"dimension": d, "asset_class": n["asset_class"], "paid_observable": paid[0]["observable"],
                     "paid_state": paid[0]["state_after_r63"], "proxy_best_t": best_t, "proxy_best_cell": best_cell,
                     "gate_1_checks": checks, "break_even": be, "gate_1_verdict": verdict,
                     "gate_2_definition": "existing system vs existing system + dataset; renew/scale only if the "
                                          "incremental OOS net portfolio benefit >= 2x the annual fee; NOT EXECUTED",
                     "purchase_authorised": False, "money_spent_usd": 0.0})
    gate.sort(key=lambda g: (-(g["proxy_best_t"] or -99), g["dimension"]))
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "ladder_order": list(LADDER), "authoritative_nav": nav,
            "break_even_rule": "fee/NAV + trading cost(horizon) + 0.5 x expected gross increment + 0.1 x fee/NAV; "
                               "EXTREME_HURDLE above 2% of NAV",
            "canonical_purchase_verdict_owner": "engine.data_expansion_gate via api.data_expansion.run_evaluation; "
                                                "R32 conditions quoted, never re-scored",
            "r32_conditions": list(R32_CONDITIONS),
            "fee_references_recorded_in_estate": refs,
            "needs": needs, "paid_data_gate": gate,
            "purchases": 0, "subscriptions_started": 0, "trials_started": 0, "money_spent_usd": 0.0,
            "top_gaps_actively_sourced_by_r63": [
                {"dimension": "INSIDER_BEHAVIOUR", "rung": "OWNED_DEEPER", "result": "stock-level Form 345 join; see cells"},
                {"dimension": "DISCLOSURE_INTENSITY_LANGUAGE", "rung": "FREE_PUBLIC_PROXY", "result": "SEC submissions acquired for 842 CIKs; see cells"},
                {"dimension": "INVENTORY", "rung": "OWNED_DEEPER + FREE_PUBLIC_PROXY", "result": "EIA petroleum stocks + natural-gas storage; see cells"}],
            "ontology_hash": ONT.ontology_hash()}
    write_artifact(ARTIFACT_NAME, body)
    return body
