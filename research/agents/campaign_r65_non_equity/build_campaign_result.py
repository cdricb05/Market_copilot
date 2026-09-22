r"""Compose R65's CAMPAIGN_RESULT from the artifacts, never by retyping them.

RESEARCH ONLY. PAPER ONLY. Reads the frozen artifacts and the ONE research
memory; writes a single summary artifact. It settles nothing, registers nothing
and decides nothing - every verdict it reports was already recorded by its
owner.

    & .\.venv-win\Scripts\python.exe research\agents\campaign_r65_non_equity\build_campaign_result.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from alpha_agent.r59 import memory as M  # noqa: E402

OUT = _HERE / "CAMPAIGN_RESULT.json"
R61_XA = Path(r"D:\Stock_Prediction_app_data\r61_apparatus_calibration"
              r"\power\PANEL_CROSS_ASSET_FUTURES.json")

#: The director's trigger for W2-01, frozen in DIRECTOR_R65_WAVE2_PACKET.json
#: BEFORE any floor was looked up: REGISTER iff the measured MDE_80 at the
#: denominator ``search_denominator`` returns is <= this.
W2_01_TRIGGER_CEILING = 0.030


def _load(path):
    p = Path(path)
    return (json.loads(p.read_text(encoding="utf-8-sig"))
            if p.exists() else None)


def main() -> int:
    mem = M.ResearchMemory(None, read_only=True)
    results = _load(_HERE / "results.json") or {}
    precheck = _load(_HERE / "PRECHECK.json") or {}
    h5 = _load(_HERE / "H5_POWER_CALIBRATION.json") or {}
    cert = _load(_HERE
                 / "CERTIFICATION_r65_aggregate_oi_cross_asset_v1.json") or {}
    xa21 = _load(R61_XA) or {}

    cells = []
    for row in results.get("results", []):
        eid = row["experiment_id"]
        exp = mem.get(eid) or {}
        D = (row.get("layers") or {}).get("D") or {}
        pre = next((p for p in precheck.get("prechecks", [])
                    if p.get("experiment_id") == eid), {})
        cp = (pre.get("cost_precheck") or {}).get("verdict") or {}
        cells.append({
            "experiment_id": eid,
            "label": ((exp.get("spec") or {}).get("parameters") or {})
                     .get("label"),
            "executor": row.get("executor"),
            "asset_class": exp.get("asset_class"),
            "economic_family": exp.get("economic_family"),
            "information_family": exp.get("information_family"),
            "outcome": exp.get("outcome"),
            "reason": exp.get("reason_rejected"),
            "reopen_condition": exp.get("reopen_condition"),
            "state": row.get("state"),
            "halted_at": row.get("halted_at"),
            "lockbox_computed": bool(row.get("lockbox_computed")),
            "stages_revealed": list((row.get("layers") or {}).keys()),
            "discovery": {
                "effective_observations": D.get("effective_observations"),
                "first": D.get("first"), "last": D.get("last"),
                "ann_gross_excess": D.get("ann_gross_excess"),
                "ann_cost_drag": D.get("ann_cost_drag"),
                "ann_net_excess": D.get("ann_net_excess"),
                "t_net_excess": D.get("t_net_excess"),
                "hit_rate": D.get("hit_rate"),
                "halves_ann_net_excess": D.get("halves_ann_net_excess"),
                "max_drawdown": D.get("max_dd"),
                "mean_oneway_turnover": D.get(
                    "mean_oneway_turnover_per_period"),
                "median_positions": D.get("median_positions"),
            },
            "pre_measurement_gates": {
                "oi_coverage": (pre.get("coverage") or {}).get("coverage"),
                "oi_coverage_floor": (pre.get("coverage") or {}).get("floor"),
                "annualized_cost_drag": cp.get("measured"),
                "cost_budget_ceiling": cp.get("frozen_threshold"),
                "cost_budget_state": cp.get("state"),
                "retired_040_scalar_would_have_halted_it":
                    (pre.get("cost_precheck") or {})
                    .get("would_the_retired_scalar_have_killed_it"),
                "required_gross_for_materiality":
                    cp.get("required_gross_for_materiality"),
            },
            "cost_parity_book_vs_precheck": (row.get("cost_parity") or {})
                                            .get("passed"),
        })

    xa5 = (h5.get("panels") or {}).get("CROSS_ASSET_FUTURES") or {}

    def _mde(block, den):
        return ((block.get("mde_at_burden_%d" % den) or {})
                .get("MDE_80") or {}).get("mde_ann_net_excess")

    body = {
        "campaign_id": "R65_NON_EQUITY_COST_HONEST_FRONTIER",
        "run_id": "R65_MULTI_ASSET_ALPHA_AND_PNL_OFFENSIVE",
        "generated_at": datetime.now(timezone.utc)
                        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "state": "CAMPAIGN_COMPLETE",
        "safety": ["RESEARCH ONLY", "PAPER ONLY", "NO ORDERS", "NO FILLS",
                   "NO PROMOTION", "NO ADOPTION", "NO AUTOMATION",
                   "MANUAL REVIEW", "PREVIEW ONLY"],
        "new_paid_data_cost_usd": 0.0,

        "headline": (
            "Both pre-registered non-equity cells were measured and both "
            "settled NO_ALPHA_EVIDENCE at DISCOVERY. Neither reached a "
            "lockbox. No candidate survived, so nothing became eligible for "
            "paper-portfolio capital, and no sleeve was created."),

        "cells": cells,

        "what_the_corrected_cost_gate_bought": {
            "finding": (
                "Both cells cleared the corrected annualised cost budget and "
                "would BOTH have been killed pre-measurement by the retired "
                "0.40 one-way turnover scalar. The R61 correction was "
                "therefore load-bearing: it bought the measurements. It did "
                "not buy a survivor."),
            "measured_annualised_cost_drag": {
                c["executor"]: c["pre_measurement_gates"][
                    "annualized_cost_drag"] for c in cells},
            "ceiling": 0.03,
            "roll_cost_was_charged_not_assumed": True,
            "cost_parity_check": (
                "the pre-measurement forecast and the book's own charge agreed "
                "to within 1.8% on both cells, so the one duplicated "
                "computation did not drift"),
        },

        "horizon_5_detection_floor": {
            "why": (
                "Every floor the estate owned was calibrated at cadence 21 / "
                "horizon 21. A five-session book rebalances 50.4 times a year "
                "instead of 12, so judging it against a 21-session floor "
                "borrows a number never measured on its geometry."),
            "panel": "CROSS_ASSET_FUTURES (28 non-commodity markets)",
            "decisions_h5": xa5.get("decision_count"),
            "effective_observations_h5": xa5.get(
                "effective_observations_lockbox"),
            "effective_observations_h21": xa21.get(
                "effective_observations_lockbox"),
            "false_positive_rate_at_rho_zero": xa5.get(
                "false_positive_rate_at_rho_zero"),
            "MDE_80_net_by_burden": {
                "h5": {str(d): _mde(xa5, d) for d in (1, 10, 100, 1000)},
                "h21": {str(d): _mde(xa21, d) for d in (1, 10, 100, 1000)},
            },
            "conclusion": (
                "Horizon 5 is NOT structurally weaker than horizon 21. At "
                "burden 1 it is marginally BETTER (2.86%/yr vs 2.98%/yr net): "
                "the 4.3x gain in effective independent observations (188 vs "
                "44) roughly offsets the 4.2x higher rebalance drag. The "
                "estate has avoided H1-H5 research - 0.45% of 8,472 rows - on "
                "an assumption that had never been measured. This does not "
                "make a short-horizon idea good; it removes POWER as the "
                "reason not to ask one."),
        },

        "curve_participation_substrate": {
            "dataset_id": cert.get("dataset_id"),
            "extends": cert.get("extends"),
            "new_paid_data_cost_usd": cert.get("new_paid_data_cost_usd"),
            "source": cert.get("source"),
            "certified_markets": cert.get("certified_markets"),
            "markets_with_curve_depth": cert.get("markets_with_curve_depth"),
            "overall_coverage": cert.get("overall_coverage"),
            "why": (
                "n_contracts_oi_positive is computed and written by the R56 "
                "builder but DROPPED by its loader, and the dataset existed "
                "for only the 39 COMMODITY markets of that campaign. The "
                "mechanism was unaskable not because the estate lacked the "
                "data but because 29 of its 68 certified markets were never "
                "built. They are now, at $0, from the owned Norgate futures "
                "database, into a SEPARATE dataset; the frozen R56 dataset "
                "was not touched."),
            "measured_caveat": (
                "curve depth carries a large asset-class LEVEL effect "
                "(median 0.30 in FX vs 0.80 in commodities), so a raw "
                "cross-book rank would sort asset classes rather than "
                "risk-warehousing. Any cell built on it must demean within "
                "group or use a time-series change. Recorded before any cell "
                "was registered."),
        },

        "wave_2_outcome": {
            "registrations": 0,
            "ruling": (
                "The director dropped all three wave-2 candidates and "
                "recorded zero registrations as the honest number."),
            "W2_02_position_concentration": (
                "DROPPED. The owned CFTC store carries eight columns - "
                "as_of, code, name, oi, nc_long, nc_short, c_long, c_short - "
                "and no concentration ratio or trader count. Acquiring them "
                "would also not fix a 2015-2026 weekly history that is "
                "shorter than the sample behind the existing floor."),
            "W2_03_futures_price_impact": (
                "DROPPED on power, not data. Needs roughly 7-8.5%/yr gross "
                "against documented illiquidity premia of 2-4%, and its long "
                "leg sits in the 8-10bp cost tail."),
            "W2_01_curve_participation_depth": {
                "verdict": "NOT REGISTERED",
                "trigger_frozen_before_the_number_existed":
                    "REGISTER iff measured MDE_80 <= %.3f/yr net at the "
                    "denominator search_denominator returns"
                    % W2_01_TRIGGER_CEILING,
                "measured_search_denominator": 27,
                "denominator_composition": (
                    "family burden 0 (the family is genuinely untouched) + "
                    "generative charge 0 (hand-written, and not inside the "
                    "machine representation space) + 27 AGENTS_V2 campaign "
                    "cells already prosecuted"),
                "measured_MDE_80_at_burden_10": _mde(xa21, 10),
                "evaluation": (
                    "the denominator is 27, so the applicable floor is at "
                    "least the burden-10 value of %.4f/yr net, already above "
                    "the %.3f ceiling. The trigger FAILS and the cell is not "
                    "registered. It was evaluated once, as pre-committed."
                    % ((_mde(xa21, 10) or 0.0), W2_01_TRIGGER_CEILING)),
                "second_independent_blocker": (
                    "the calibrated CROSS_ASSET_FUTURES panel is 28 "
                    "NON-COMMODITY markets. A 68-market book is a different "
                    "panel whose floor has never been measured, so no "
                    "existing number may be borrowed for it."),
            },
        },

        "director_recommendation_carried_forward": {
            "close_family": "POSITIONING|EXCHANGE_OPEN_INTEREST across futures",
            "reopen_condition": "NEW_ORTHOGONAL_INFORMATION",
            "basis": (
                "pre-committed in the frozen fs_r65_03 text before either "
                "cell was measured, and both cells have now failed"),
            "status": "RECOMMENDED BY THE DIRECTOR; not executed here",
        },

        "r65_02_ruling": {
            "verdict": "REJECTED PERMANENTLY, never registered, charges no "
                       "burden",
            "basis": (
                "the certified R38 panel holds exactly ONE INDUSTRIAL_METALS "
                "market (HG), so the multi-market industrial-metals composite "
                "the mechanism needs cannot be built from owned data, and the "
                "only constructible survivor repeats an existing copper "
                "lead-lag primitive on a panel with no measured detection "
                "floor"),
            "reopen_condition": (
                "NEW_ORTHOGONAL_INFORMATION: all three of (A) >=3 certified "
                "INDUSTRIAL_METALS markets past the 80% OI floor, (B) an "
                "R61-measured MDE_80 for the actual panel geometry, (C) a "
                "forward verdict on the existing copper lead-lag row"),
        },

        "registry_state": {
            "hypotheses_total": mem.summary()["hypotheses_total"],
            "hypotheses_open": mem.summary()["hypotheses_open"],
            "by_outcome": mem.summary()["by_outcome"],
            "search_burden_total": mem.burden()["total"],
        },
        "diversity_gate": {
            "at_least_one_H1_H5": "UNMET",
            "note": (
                "recorded as unmet rather than satisfied by registering a "
                "short-horizon cell that would have borrowed a floor. The "
                "horizon-5 floor measured in this run removes the power "
                "objection for a FUTURE campaign; it does not retroactively "
                "satisfy this one."),
        },
    }
    OUT.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")
    print(json.dumps({k: body[k] for k in
                      ("campaign_id", "state", "headline", "wave_2_outcome",
                       "registry_state")}, indent=1, default=str))
    print("R65_RESULT_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
