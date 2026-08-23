"""alpha_agent.r39.integrity - the R38_CELL_TO_EXPERIMENT_INTEGRITY_MAP owner.

Release 38 measured 59 cells as ``NATIVE_DATA_VERIFIED_RESEARCHABLE`` and
executed 13 broad frozen configurations. Those are different claims:
RESEARCHABLE says the bytes exist and pass structure; TESTED says a frozen
experiment produced a judged result. This module closes the semantic gap
cell by cell, with a DECLARED mapping (reviewable code, not narrative), and
refuses two temptations by construction:

* it does NOT manufacture 59 experiments to make the labels match
  (``NO_EXPERIMENTS_ARE_MANUFACTURED_HERE = True``);
* it does NOT call an untested cell a rejected hypothesis
  (``contract.DATA_AVAILABLE_BUT_NOT_TESTED_IS_NOT_A_REJECTED_HYPOTHESIS``).

Statuses come from the frozen vocabulary in :mod:`alpha_agent.r39.contract`.
"""
from __future__ import annotations

from pathlib import Path

from .. import r39
from . import contract as C
from .estate import R38_V4

CALCULATION_OWNER = "alpha_agent.r39.integrity"
SCHEMA = "r39_r38_cell_to_experiment_integrity_map/1"
ARTIFACT_NAME = "r38_cell_to_experiment_integrity_map.json"

NO_EXPERIMENTS_ARE_MANUFACTURED_HERE = True

UNLOCKS_PATH = R38_V4 / "r37_expected_vs_r38_actual_unlocks.json"
REGISTRY_PATH = R38_V4 / "native_futures_experiment_registry.json"

#: The declared coverage map: (market-key prefix, strategy family) -> the R38
#: experiment(s) whose frozen universe contains the cell's markets, plus the
#: coverage type. A cell that IS an experiment (the VX cells - one market,
#: one design) is DIRECTLY_TESTED; a cell folded into a broader frozen book
#: is VALIDLY_REPRESENTED_BY_GROUP_TEST; a pattern with no experiment is
#: DATA_AVAILABLE_BUT_NOT_TESTED. ROLL cells are represented by the carry
#: experiments because the annualised calendar slope IS the roll yield.
_COVERAGE_RULES = [
    ("CMDTY_", "TREND", ["CMDTY_TS_TREND_12M"], "GROUP"),
    ("CMDTY_", "CARRY", ["CMDTY_XS_CARRY"], "GROUP"),
    ("CMDTY_", "ROLL", ["CMDTY_XS_CARRY", "CMDTY_CALENDAR_SPREAD_MR"],
     "GROUP"),
    ("CMDTY_", "CURVE_TERM_STRUCTURE",
     ["CMDTY_XS_CARRY", "CMDTY_CALENDAR_SPREAD_MR"], "GROUP"),
    ("CMDTY_", "CROSS_SECTIONAL",
     ["CMDTY_XS_MOMENTUM_12_1", "CMDTY_XS_CARRY"], "GROUP"),
    ("CMDTY_", "SEASONALITY", ["CMDTY_SEASONALITY"], "GROUP"),
    ("CMDTY_", "POSITIONING", ["CMDTY_COT_HEDGING_PRESSURE"], "GROUP"),
    # inter-market relative value was NOT a frozen R38 design; only the
    # calendar leg was. Untested, and said so.
    ("CMDTY_", "RELATIVE_VALUE", [], "NONE"),
    ("INTL_EQUITY_", "TREND", ["INTL_IDX_TS_TREND"], "GROUP"),
    ("INTL_EQUITY_", "MOMENTUM", ["INTL_IDX_XS_MOMENTUM"], "GROUP"),
    ("INTL_EQUITY_", "CROSS_SECTIONAL", ["INTL_IDX_XS_MOMENTUM"], "GROUP"),
    ("INTL_EQUITY_", "MEAN_REVERSION", [], "NONE"),
    ("INTL_EQUITY_", "RELATIVE_VALUE", [], "NONE"),
    ("INTL_EQUITY_", "MACRO_CONDITIONAL", [], "NONE"),
    ("RATES_", "TREND", ["RATES_TS_TREND"], "GROUP"),
    ("RATES_", "CARRY", ["RATES_XS_CURVE_CARRY"], "GROUP"),
    ("RATES_", "ROLL", ["RATES_XS_CURVE_CARRY"], "GROUP"),
    ("RATES_", "CURVE_TERM_STRUCTURE", ["RATES_XS_CURVE_CARRY"], "GROUP"),
    ("RATES_", "RELATIVE_VALUE", [], "NONE"),
    ("VOL_VIX_FUTURES", "CARRY", ["VX_TERM_STRUCTURE_CARRY"], "DIRECT"),
    ("VOL_VIX_FUTURES", "CURVE_TERM_STRUCTURE",
     ["VX_TERM_STRUCTURE_CARRY", "VX_CALENDAR_SLOPE_MR"], "DIRECT"),
    ("VOL_VIX_FUTURES", "ROLL", ["VX_TERM_STRUCTURE_CARRY"], "GROUP"),
    ("VOL_VIX_FUTURES", "VOLATILITY_RISK_PREMIUM",
     ["VX_TERM_STRUCTURE_CARRY"], "GROUP"),
]


def _classify(cell: dict, executed: set) -> dict:
    status = str(cell.get("r38_status") or "")
    key = str(cell.get("market_key") or "")
    fam = str(cell.get("strategy_family") or "")
    if status.startswith("STILL_BLOCKED"):
        return {"research_status": "STILL_BLOCKED", "experiments": [],
                "coverage_type": "NONE", "r38_data_status": status}
    if status == "PARTIALLY_UNLOCKED":
        return {"research_status": "MISSING_REQUIRED_INFORMATION_LEG",
                "experiments": [], "coverage_type": "NONE",
                "r38_data_status": status}
    for prefix, family, exps, ctype in _COVERAGE_RULES:
        if key.startswith(prefix) and fam == family:
            exps_present = [e for e in exps if e in executed]
            if not exps_present:
                return {"research_status": "DATA_AVAILABLE_BUT_NOT_TESTED",
                        "experiments": [], "coverage_type": "NONE",
                        "r38_data_status": status}
            rs = ("DIRECTLY_TESTED" if ctype == "DIRECT"
                  else "VALIDLY_REPRESENTED_BY_GROUP_TEST")
            return {"research_status": rs, "experiments": exps_present,
                    "coverage_type": ctype, "r38_data_status": status}
    return {"research_status": "DATA_AVAILABLE_BUT_NOT_TESTED",
            "experiments": [], "coverage_type": "NONE",
            "r38_data_status": status}


def build() -> dict:
    unlocks = r39.read_json(UNLOCKS_PATH)
    registry = r39.read_json(REGISTRY_PATH)
    if not unlocks or not registry:
        raise FileNotFoundError(
            "R38 v4 unlock/registry artifacts are required inputs")
    executed = {str(c["name"]) for c in registry["frozen_configurations"]}
    rows = []
    for cell in unlocks["cells"]:
        cls = _classify(cell, executed)
        rows.append({
            "r36_cell": cell["cell_id"],
            "asset_class": cell.get("asset_class"),
            "r38_delivered_data_status": cls["r38_data_status"],
            "r38_experiments_covering": cls["experiments"],
            "coverage_type": cls["coverage_type"],
            "research_status": cls["research_status"],
        })
    counts: dict = {}
    for r in rows:
        counts[r["research_status"]] = counts.get(r["research_status"], 0) + 1
    bad = [r for r in rows if r["research_status"] not in C.RESEARCH_STATUSES]
    if bad:
        raise ValueError("status outside frozen vocabulary: %r" % bad[:3])

    body = r39.artifact_body(SCHEMA, {
        "campaign_id": C.CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "cells": rows,
        "n_cells": len(rows),
        "counts_by_research_status": counts,
        "r38_executed_experiments": sorted(executed),
        "no_experiments_manufactured": NO_EXPERIMENTS_ARE_MANUFACTURED_HERE,
        "untested_is_not_rejected":
            C.DATA_AVAILABLE_BUT_NOT_TESTED_IS_NOT_A_REJECTED_HYPOTHESIS,
        "predecessor_wording_finding": (
            "Release-38 canonical artifacts keep RESEARCHABLE (a data "
            "capability claim) distinct from TESTED (a judged frozen "
            "experiment); no conflating sentence was found in "
            "PROJECT_STATE.md or the R38 write-up. This map closes the gap "
            "cell-by-cell regardless, so the distinction is a table rather "
            "than a reading."),
        "inputs": {
            "unlocks": r39.file_fingerprint(UNLOCKS_PATH),
            "experiment_registry": r39.file_fingerprint(REGISTRY_PATH),
        },
    })
    body["integrity_map_hash"] = r39.sha(body)
    return body


def path_for(campaign_id: str = C.CAMPAIGN_ID) -> Path:
    return r39.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r39.write_json(path_for(body["campaign_id"]), body)
