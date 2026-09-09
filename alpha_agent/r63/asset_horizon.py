"""alpha_agent.r63.asset_horizon - the asset x horizon x information map.

For every (asset class, horizon, information dimension) cell the map says
whether the estate can SEE the information, with evidence:

    WELL_OBSERVED        an owned field observes it, PIT is credible, and an
                         R63 cell measured it on the full scope (coverage >=
                         60%) or prior memory evidence exists for the scope
    PARTIALLY_OBSERVED   an owned field exists but coverage, history or PIT is
                         partial, or the cell was measured on covered rows only
    NOT_OBSERVED         no owned or entitled field observes it for this scope
    BLOCKED              every field that could observe it is BLOCKED

Horizons are the four the owned daily panels support; INTRADAY is reported
once, as unsupported, never manufactured.
"""
from __future__ import annotations

from . import (AC_EQUITY_INDEX, AC_US_EQUITY, ASSET_CLASSES, D_BLOCKED, HORIZONS,
               INTRADAY_STATE, OBS_BLOCKED, OBS_NOT, OBS_PARTIAL, OBS_WELL,
               PIT_BLOCKED, PIT_UNVERIFIED, write_artifact)
from . import ontology as ONT
from . import sensitivity as S

CALCULATION_OWNER = "alpha_agent.r63.asset_horizon"
SCHEMA = "r63_asset_horizon_information_matrix/1"
ARTIFACT_NAME = "asset_horizon_information_matrix.json"

# A dimension the estate tests through a DIFFERENT dimension's observable in a
# given scope (a dated-contract slope IS carry, IS the front/deferred basis).
ALIASES = {"FUTURES_BASIS": "CARRY"}

#: Market-level states: constant across a cross-section on a date, so they
#: carry timing information only, measured in the TS lanes.
MARKET_LEVEL_DIMS = frozenset({
    "CREDIT_CONDITIONS", "TERM_STRUCTURE", "CURVE_SHAPE", "RATES_EXPECTATIONS",
    "POLICY_EXPECTATIONS", "INFLATION_EXPECTATIONS", "FUNDING_LIQUIDITY_CONDITIONS",
    "MACRO_LEVELS", "MACRO_CHANGE", "MACRO_SURPRISES", "RISK_APPETITE", "DISPERSION",
    "CROSS_ASSET_TRANSMISSION", "VOLATILITY_EXPECTATIONS_IV"})


def _cells_by_key(cells: list) -> dict:
    out: dict = {}
    for c in cells or []:
        key = (c.get("scope"), int(c.get("horizon") or 0), c.get("dimension"))
        out.setdefault(key, []).append(c)
    return out


def classify(inventory: dict, cells: list | None) -> dict:
    fields = inventory.get("fields") or []
    by_cell = _cells_by_key(cells)
    matrix: dict = {}
    counts = {s: 0 for s in (OBS_WELL, OBS_PARTIAL, OBS_NOT, OBS_BLOCKED)}
    for ac in ASSET_CLASSES:
        matrix[ac] = {}
        for h in HORIZONS:
            row = {}
            for d in ONT.DIMENSION_IDS:
                if not ONT.applies(d, ac):
                    continue
                dd = ALIASES.get(d, d)
                fs = [f for f in fields if f["dimension_id"] in (d, dd) and ac in f["asset_classes"]]
                fs_h = [f for f in fs if h in f["horizons"]]
                measured = by_cell.get((ac, h, dd), []) + by_cell.get((ac, h, d), [])
                proxy_lane = None
                if ac == AC_US_EQUITY and d in MARKET_LEVEL_DIMS and not measured:
                    # a market-level state cannot rank a stock cross-section;
                    # its TIMING value for US equities is measured in the
                    # equity-index lane, which is the same market
                    measured = by_cell.get((AC_EQUITY_INDEX, h, dd), [])
                    proxy_lane = AC_EQUITY_INDEX if measured else None
                    if not fs:
                        fs = [f for f in fields if f["dimension_id"] in (d, dd)
                              and AC_EQUITY_INDEX in f["asset_classes"]]
                        fs_h = [f for f in fs if h in f["horizons"]]
                scored = [c for c in measured if c.get("conditional")]
                best = max(scored, key=lambda c: c["conditional"].get("t") or -99) if scored else None
                full = [c for c in scored if (c.get("coverage") or 0) >= S.COVERAGE_FLOOR]
                # a MEASURED cell is the strongest evidence of observation and
                # takes precedence over the field registry: several dimensions
                # (momentum, reversal, realised volatility, curve shape, ...) are
                # DERIVED from owned price and yield fields and carry no field
                # row of their own
                if full:
                    state, why = OBS_WELL, "measured by R63 on the full scope"
                elif scored:
                    state, why = OBS_PARTIAL, "measured by R63 on covered rows only (coverage %.0f%%)" % (100 * (best.get("coverage") or 0))
                elif not fs:
                    state, why = OBS_NOT, "no owned or entitled field observes this dimension for the scope"
                elif all(f["disposition"] == D_BLOCKED for f in fs):
                    state = OBS_BLOCKED
                    why = "; ".join(sorted({str(f.get("blocker") or f.get("disposition_reason")) for f in fs}))[:300]
                elif not fs_h:
                    state, why = OBS_PARTIAL, "owned field exists but not declared for this horizon"
                elif any(f.get("pit_status") in (PIT_BLOCKED, PIT_UNVERIFIED) for f in fs_h):
                    state, why = OBS_PARTIAL, "field owned but PIT unverified or blocked"
                elif any(f["disposition"] != D_BLOCKED for f in fs_h):
                    state = OBS_PARTIAL
                    why = "owned field, not measured in this scope/horizon by R63 (prior evidence: %s)" % \
                        ",".join(sorted({f["disposition"] for f in fs_h}))
                else:
                    state, why = OBS_BLOCKED, "fields blocked"
                counts[state] += 1
                if proxy_lane and state in (OBS_WELL, OBS_PARTIAL):
                    why += " (timing lane: %s)" % proxy_lane
                row[d] = {"state": state, "why": why, "proxy_lane": proxy_lane,
                          "aliased_to": dd if dd != d else None,
                          "fields": [f["field_id"] for f in fs][:6],
                          "r63_cells": [c["cell_id"] for c in measured][:4],
                          "best_conditional_t": best["conditional"].get("t") if best else None,
                          "best_verdict": best.get("verdict") if best else None,
                          "best_cell": best.get("cell_id") if best else None}
            matrix[ac][str(h)] = row
        matrix[ac]["INTRADAY"] = {"state": INTRADAY_STATE,
                                  "why": "owned panels are daily; no intraday history is manufactured"}
    # under-informed ranking: share of NOT_OBSERVED + BLOCKED per (class, horizon)
    under = []
    for ac in ASSET_CLASSES:
        for h in HORIZONS:
            row = matrix[ac][str(h)]
            n = len(row)
            blind = sum(1 for v in row.values() if v["state"] in (OBS_NOT, OBS_BLOCKED))
            partial = sum(1 for v in row.values() if v["state"] == OBS_PARTIAL)
            under.append({"asset_class": ac, "horizon": h, "dimensions": n,
                          "blind": blind, "partial": partial,
                          "blind_share": round(blind / n, 3) if n else None,
                          "well_observed": n - blind - partial})
    under.sort(key=lambda r: (-(r["blind_share"] or 0), -(r["partial"]), r["asset_class"], r["horizon"]))
    body = {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER,
            "states": [OBS_WELL, OBS_PARTIAL, OBS_NOT, OBS_BLOCKED],
            "horizons": list(HORIZONS), "intraday": INTRADAY_STATE,
            "aliases": ALIASES, "counts": counts, "matrix": matrix,
            "most_under_informed": under[:12], "least_under_informed": under[-6:],
            "ontology_hash": ONT.ontology_hash()}
    write_artifact(ARTIFACT_NAME, body)
    return body
