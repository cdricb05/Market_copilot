"""alpha_agent.r63.gaps - the ranked INFORMATION GAP FRONTIER.

The unit is an INFORMATION NEED: (asset class x horizon x dimension). A
provider is never a row. Every need is scored on the twelve pre-registered
criteria by a declared, deterministic rule from MEASURED inputs (the R63
sensitivity matrix, the orthogonality matrix, the certification, the asset x
horizon map) with declared priors where nothing was measured, and ranked by
expected incremental value x feasibility. Ties break on the cell key, so the
ranking is reproducible byte for byte.

The frontier also carries REMAINING RESEARCH VALUE per cell, which is what the
persistent AlphaAgent consumes: a cell already measured and rejected has
none; a cell measured as a candidate belongs to forward qualification, not
to more research; a cell the estate cannot see is where the value is.
"""
from __future__ import annotations

import math
import re

from . import (AC_COMMODITY, AC_CREDIT, AC_CROSS_ASSET, AC_EQUITY_INDEX, AC_FX,
               AC_RATES, AC_US_EQUITY, AC_VOLATILITY, ASSET_CLASSES, HORIZONS,
               OBS_BLOCKED, OBS_NOT, OBS_PARTIAL, OBS_WELL, PIT_BLOCKED, PIT_LAGGED,
               PIT_MARKET, PIT_TRUE, PIT_UNVERIFIED, write_artifact)
from . import ontology as ONT
from . import sensitivity as S

CALCULATION_OWNER = "alpha_agent.r63.gaps"
SCHEMA = "r63_information_gap_frontier/1"
ARTIFACT_NAME = "information_gap_frontier.json"

CRITERIA = ("marginal_predictive_sensitivity", "orthogonality_to_current_information",
            "pit_quality", "available_history", "effective_independent_sample",
            "cross_asset_applicability", "horizon_applicability", "capital_applicability",
            "likely_turnover", "implementation_complexity", "source_availability",
            "expected_incremental_net_pnl")

#: Declared capital applicability of a scope to the paper book TODAY (the
#: book is US equities; futures scopes are research-ready, not deployed).
CAPITAL_WEIGHT = {AC_US_EQUITY: 1.0, AC_EQUITY_INDEX: 0.8, AC_CROSS_ASSET: 0.7,
                  AC_RATES: 0.6, AC_COMMODITY: 0.6, AC_FX: 0.5, AC_CREDIT: 0.4,
                  AC_VOLATILITY: 0.3}
TURNOVER_SCORE = {1: 0.2, 5: 0.5, 21: 0.8, 63: 1.0}
PIT_SCORE = {PIT_TRUE: 1.0, PIT_MARKET: 1.0, PIT_LAGGED: 0.8, PIT_UNVERIFIED: 0.3, PIT_BLOCKED: 0.0}
SOURCE_SCORE = {"OWNED": 1.0, "FREE": 0.8, "SELF_SERVICE": 0.5, "PAID": 0.2, "NONE": 0.0}
#: Declared sourcing step for dimensions the estate does not observe.
SOURCING_STEP = {
    "ANALYST_REVISIONS": "PAID", "EARNINGS_EXPECTATIONS": "PAID", "REVENUE_EXPECTATIONS": "PAID",
    "DISPERSION": "FREE", "OWNERSHIP_INSTITUTIONAL_FLOW": "FREE", "ETF_FUND_FLOW": "FREE",
    "SHIPPING_TRANSPORT": "PAID", "COMMODITY_SUPPLY_DEMAND": "SELF_SERVICE",
    "SHORT_POSITIONING": "PAID", "FUTURES_BASIS": "OWNED", "EVENT_INFORMATION": "FREE",
}
PRIOR_ORTHOGONALITY = {ONT.IC_PRICE: 0.10, ONT.IC_FUNDAMENTAL: 0.6, ONT.IC_EXPECTATIONS: 0.8,
                       ONT.IC_DISCLOSURE: 0.8, ONT.IC_POSITIONING: 0.8, ONT.IC_CURVE: 0.6,
                       ONT.IC_MACRO: 0.7, ONT.IC_PHYSICAL: 0.9, ONT.IC_RISK: 0.5, ONT.IC_EVENT: 0.7}


def _sig(t: float | None, prior: float = 0.3) -> float:
    if t is None or not math.isfinite(t):
        return prior
    return 1.0 / (1.0 + math.exp(-(t - 1.0)))


def _history_years(fields: list) -> float:
    yrs = []
    for f in fields:
        m = re.findall(r"(19|20)\d{2}", str(f.get("history") or ""))
        if m:
            starts = [int(x) for x in re.findall(r"((?:19|20)\d{2})", str(f.get("history")))]
            if starts:
                yrs.append(2026 - min(starts))
    return float(max(yrs)) if yrs else 0.0


def _source_step(fields: list, dim: str) -> str:
    if any(f["disposition"] != "BLOCKED" for f in fields):
        return "OWNED"
    if fields and all("PROSPECTIVE" in str(f.get("disposition_reason")) for f in fields):
        return "PAID" if dim in ("SHORT_POSITIONING", "ANALYST_REVISIONS") else "FREE"
    return SOURCING_STEP.get(dim, "SELF_SERVICE")


def build(inventory: dict, matrix: dict, cells: list | None, ortho: dict | None) -> dict:
    fields = inventory.get("fields") or []
    by_cell: dict = {}
    for c in cells or []:
        by_cell.setdefault((c.get("scope"), int(c.get("horizon") or 0), c.get("dimension")), []).append(c)
    dim_best_t: dict = {}
    for c in cells or []:
        t = (c.get("conditional") or {}).get("t")
        if t is not None:
            d = c["dimension"]
            dim_best_t[d] = max(dim_best_t.get(d, -99), t)
    rows = []
    aliased = []
    for ac in ASSET_CLASSES:
        for h in HORIZONS:
            mrow = (matrix.get("matrix") or {}).get(ac, {}).get(str(h), {})
            for d, info in mrow.items():
                if info.get("aliased_to"):
                    # the same observable as another dimension in this scope
                    # (a dated-contract slope IS carry IS the basis): one need
                    aliased.append({"cell_key": "%s|%d|%s" % (ac, h, d), "aliased_to": info["aliased_to"]})
                    continue
                dim = ONT.dimension(d)
                fs = [f for f in fields if f["dimension_id"] in (d, info.get("aliased_to")) and ac in f["asset_classes"]]
                measured = [c for c in by_cell.get((ac, h, info.get("aliased_to") or d), []) if c.get("conditional")]
                best = max(measured, key=lambda c: c["conditional"].get("t") or -99) if measured else None
                t = best["conditional"].get("t") if best else None
                # criteria
                if best is not None:
                    sens = _sig(t)
                else:
                    proxy_t = dim_best_t.get(d)
                    sens = 0.5 * _sig(proxy_t) + 0.5 * 0.3 if proxy_t is not None else 0.3
                rs = None
                if ortho and ac in (ortho.get("scopes") or {}):
                    rs = ((ortho["scopes"][ac].get("residual_share_vs_baseline") or {}).get(d) or {}).get("residual_share")
                if best is not None and (best.get("redundancy") or {}).get("residual_share") is not None:
                    rs = best["redundancy"]["residual_share"]
                orth = float(rs) if rs is not None else PRIOR_ORTHOGONALITY.get(dim["information_class"], 0.5)
                pit = max([PIT_SCORE.get(f.get("pit_status"), 0.3) for f in fs] or
                          [0.3 if info["state"] == OBS_NOT else 0.0])
                yrs = _history_years(fs)
                hist = min(1.0, yrs / 20.0) if fs else 0.5
                periods = yrs * 252.0 / h if fs else 250.0 / h * 5
                eff = min(1.0, periods / 500.0)
                xa = len(dim["applies_to"]) / 8.0 if dim["applies_to"] else 1.0
                ha = (len({hh for f in fs for hh in f["horizons"]}) / 4.0) if fs else 0.5
                cap = CAPITAL_WEIGHT[ac]
                turn = TURNOVER_SCORE[h]
                step = _source_step(fs, d)
                complexity = {"OWNED": 1.0, "FREE": 0.7, "SELF_SERVICE": 0.5, "PAID": 0.4, "NONE": 0.1}[step]
                src = SOURCE_SCORE[step]
                econ = (best.get("economics") or {}).get("ann_net_increment") if best else None
                pnl = min(1.0, max(0.0, econ / 0.03)) if econ is not None else 0.5 * sens
                crit = dict(zip(CRITERIA, (sens, orth, pit, hist, eff, xa, ha, cap, turn, complexity, src, pnl)))
                ev = sens * orth * cap * (0.5 + 0.5 * pnl)
                feas = pit * hist * eff * src * complexity * turn
                score = ev * feas
                state = info["state"]
                verdict = best.get("verdict") if best else None
                if verdict == S.V_CANDIDATE:
                    remaining, action = 0.0, "FORWARD_QUALIFICATION"
                elif verdict in (S.V_REDUNDANT, S.V_NO_VALUE):
                    remaining, action = 0.0, "STOP_TESTED_NEGATIVE"
                elif verdict in (S.V_NOT_ECON, S.V_UNSTABLE, S.V_NOT_FDR):
                    remaining, action = score * 0.5, "MORE_RESEARCH_SAME_INFORMATION"
                elif state in (OBS_NOT, OBS_BLOCKED, OBS_PARTIAL):
                    remaining, action = score, ("SOURCE_%s" % step if state != OBS_PARTIAL else "COMPLETE_COVERAGE")
                else:
                    remaining, action = score * 0.25, "MEASURE"
                rows.append({"asset_class": ac, "horizon": h, "dimension": d,
                             "information_class": dim["information_class"],
                             "observation_state": state, "measured": best is not None,
                             "best_cell": best.get("cell_id") if best else None,
                             "best_verdict": verdict, "conditional_t": t,
                             "criteria": {k: round(float(v), 4) for k, v in crit.items()},
                             "expected_incremental_value": round(ev, 5),
                             "feasibility": round(feas, 5), "score": round(score, 6),
                             "remaining_research_value": round(remaining, 6),
                             "next_action": action, "sourcing_step": step,
                             "cell_key": "%s|%d|%s" % (ac, h, d)})
    rows.sort(key=lambda r: (-r["remaining_research_value"], -r["score"], r["cell_key"]))
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    # the VALUE frontier: what the estate cannot see, ranked on expected
    # incremental value alone (feasibility ignored), so an expensive or
    # blocked need is still visible as a need
    unseen = [r for r in rows if r["observation_state"] in (OBS_NOT, OBS_BLOCKED, OBS_PARTIAL)
              and not r["measured"]]
    unseen.sort(key=lambda r: (-r["expected_incremental_value"], r["cell_key"]))
    for i, r in enumerate(unseen, 1):
        r["value_rank"] = i
    by_dim: dict = {}
    for r in rows:
        s = by_dim.setdefault(r["dimension"], {"remaining_total": 0.0, "cells": 0, "blind_cells": 0})
        s["remaining_total"] += r["remaining_research_value"]
        s["cells"] += 1
        s["blind_cells"] += int(r["observation_state"] in (OBS_NOT, OBS_BLOCKED))
    dim_rank = sorted(({"dimension": d, **{k: (round(v, 6) if isinstance(v, float) else v) for k, v in s.items()}}
                       for d, s in by_dim.items()), key=lambda x: (-x["remaining_total"], x["dimension"]))
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "unit_of_analysis": "INFORMATION_NEED (asset class x horizon x dimension)",
            "criteria": list(CRITERIA),
            "score_rule": "expected_incremental_value = sensitivity x orthogonality x capital x (0.5 + 0.5 x pnl); "
                          "feasibility = pit x history x effective_sample x source x complexity x turnover; "
                          "score = value x feasibility; remaining_research_value zeroes tested-negative and "
                          "candidate cells; ties break on the cell key",
            "n_needs": len(rows), "top_gaps": rows[:40], "by_dimension": dim_rank,
            "top_unseen_by_value": unseen[:40], "n_unseen": len(unseen),
            "aliased_cells_excluded": aliased,
            "all_needs": rows, "ontology_hash": ONT.ontology_hash()}
    write_artifact(ARTIFACT_NAME, body)
    return body
