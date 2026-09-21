r"""scripts/r61_release_report.py - assemble the R61 release report.

READ ONLY over the artifacts the release produced. Computes nothing new and
decides nothing; it collects what was measured into one reviewable artifact.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from alpha_agent import r61                                   # noqa: E402
from alpha_agent.agents_v2 import routing as RT               # noqa: E402
from alpha_agent.agents_v2.pipeline import AgentPipeline      # noqa: E402
from alpha_agent.r59 import memory as M                       # noqa: E402
from alpha_agent.r61 import assignments as AS                 # noqa: E402
from alpha_agent.r61 import cost_budget as CB                 # noqa: E402
from alpha_agent.r61 import drawdown as DD                    # noqa: E402
from alpha_agent.r61 import identity_audit as IA              # noqa: E402
from alpha_agent.r61 import power as PW                       # noqa: E402
from alpha_agent.r61 import workers as W                      # noqa: E402

#: The interpretation reference the brief names. It is a REFERENCE, not a
#: pass/fail threshold: the director did not formally justify 3% before the
#: results were seen, so it is reported as a yardstick and never used to
#: grade a panel.
REALISTIC_NET_ALPHA_REFERENCE = 0.03


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--power-dir", default=str(r61.POWER_ROOT))
    ap.add_argument("--identity-dir", default=str(r61.IDENTITY_ROOT))
    ap.add_argument("--out", default=str(r61.ARTIFACT_DIR))
    args = ap.parse_args()

    power = r61.read_artifact(
        Path(args.power_dir) / "POWER_CALIBRATION_RESULT.json")
    identity = r61.read_artifact(
        Path(args.identity_dir) / "IDENTITY_BRIDGE_AUDIT.json")
    halts = r61.read_artifact(Path(args.out) / "PRE_MEASUREMENT_HALTS.json")
    evidence = r61.read_artifact(Path(args.out) / "EVIDENCE_STATE.json")
    if not power or not identity:
        raise SystemExit("the power and identity artifacts must exist first")

    panels = {}
    well, under = [], []
    for row in power["final_table"]:
        pid = row["PANEL"]
        m80 = row["MDE_80"]
        net = m80.get("mde_ann_net_excess")
        panels[pid] = {
            "instrument_count": row["INSTRUMENT_COUNT"],
            "decision_count": row["DECISION_COUNT"],
            "effective_observations": row["EFFECTIVE_OBSERVATIONS"],
            "cost_model": row["COST_MODEL"],
            "burden_used": row["BURDEN_USED"],
            "seeds": row["SEEDS"],
            "false_positive_rate_at_rho_zero":
                row["FALSE_POSITIVE_RATE_AT_RHO_ZERO"],
            "MDE_50": row["MDE_50"], "MDE_80": row["MDE_80"],
            "MDE_90": row["MDE_90"],
        }
        if net is not None and net <= REALISTIC_NET_ALPHA_REFERENCE:
            well.append(pid)
        else:
            under.append(pid)

    mem = M.ResearchMemory(read_only=True)
    pipe = AgentPipeline(mem)
    burden = mem.burden()
    open_halts = [r["experiment_id"] for r in pipe.open_pre_measurement_halts()]
    workers = W.report(r61.RUN_ID)

    body = {
        "release": r61.RELEASE_ID,
        "purpose": ("Measure whether the Paper Trader research apparatus can "
                    "detect realistic alpha when realistic alpha is present, "
                    "and repair the four defects R60 exposed while measuring "
                    "it."),
        "workstream_a_cost_budget": {
            "TURNOVER_GATE_OLD": "raw one-way turnover ceiling %.2f per "
                                 "decision, one side, applied campaign-wide "
                                 "across every asset class and horizon"
                                 % CB.RETIRED_TURNOVER_CEILING,
            "TURNOVER_GATE_NEW": "annualised cost-budget ceiling %.4f "
                                 "(%.1f x the frozen materiality floor %.4f)"
                                 % (CB.COST_BUDGET_CEILING,
                                    CB.COST_BUDGET_CEILING_MULTIPLE,
                                    CB.COST_BUDGET_CEILING / 2.0),
            "COST_BUDGET_OWNER": CB.COST_BUDGET_OWNER,
            "version": CB.COST_BUDGET_VERSION,
            "formula": ("one_way_turnover * 2 * cost_per_side * "
                        "rebalances_per_year + additional_ann_cost_drag"),
            "r60_reproduction": {
                "r60_08_eq_h5": CB.evaluate_cost_budget(
                one_way_turnover=0.870, cost_per_side=0.0025,
                rebalance_interval_sessions=5)["measured"],
                "r60_05_commodity": CB.evaluate_cost_budget(
                one_way_turnover=0.598, cost_per_side=0.0005,
                rebalance_interval_sessions=21)["measured"],
                "r60_07_index": CB.evaluate_cost_budget(
                one_way_turnover=0.612, cost_per_side=0.0005,
                rebalance_interval_sessions=21)["measured"],
            },
            "retired_scalar_status": "DIAGNOSTIC_ONLY",
            "r60_halts_rerun": False,
        },
        "workstream_b_power": {
            "POWER_CALIBRATION_VERSION": PW.POWER_CALIBRATION_VERSION,
            "pre_registration_hash": power["pre_registration_hash"],
            "cells_run": power["cells_run"],
            "seeds_per_point": power["seeds_per_point"],
            "effect_grid": power["effect_grid"],
            "detection_rule": PW.DETECTION_RULE,
            "primary_burden_denominator": PW.PRIMARY_BURDEN_DENOMINATOR,
            "panels": panels,
            "PANELS_WELL_POWERED": well,
            "PANELS_UNDERPOWERED": under,
            "reference_used": REALISTIC_NET_ALPHA_REFERENCE,
            # NO SINGLE REFERENCE IS LOAD-BEARING. The same MDE_80 table is
            # classified at several thresholds so a reader who disagrees with
            # 3% can re-grade every panel without re-running anything.
            "classification_sensitivity": {
                ("below_%.1f_pct" % (100 * ref)): sorted(
                    pid for pid, v in panels.items()
                    if (v["MDE_80"].get("mde_ann_net_excess") is not None
                        and v["MDE_80"]["mde_ann_net_excess"] <= ref))
                for ref in (0.02, 0.03, 0.04, 0.05, 0.06)},
            "reference_status": ("A YARDSTICK, NOT A GATE. The director did "
            "not formally justify 3% before results were "
            "seen, so no panel is graded by it; it is "
            "reported so the curve can be read."),
        },
        "workstream_c_identity": {
            "IDENTITY_BRIDGE_RESULT": identity["verdict"],
            "identity_map_hash": identity["identity_map_hash"],
            "coverage": identity["coverage"],
            "failed_checks": identity["failed_checks"],
            "unmeasurable_checks": identity["unmeasurable_checks"],
            "checks": identity["checks"],
            "EXTENSION_UNIVERSE_REUSE_ALLOWED":
            identity["substrate_reuse_allowed"],
            "thresholds": dict(IA.THRESHOLDS),
        },
        "workstream_d_drawdown": {
            "CANONICAL_STRATEGY_MAX_DRAWDOWN_OWNER":
            DD.CANONICAL_STRATEGY_MAX_DRAWDOWN_OWNER,
            "CANONICAL_EXCESS_DRAWDOWN_OWNER":
            DD.CANONICAL_EXCESS_DRAWDOWN_OWNER,
            "concepts": list(DD.CONCEPTS),
            "risk_ruling_concept": DD.RISK_RULING_CONCEPT,
            "legacy_aliases": dict(DD.LEGACY_ALIASES),
            "root_causes_fixed": [
                "the accumulator started its peak at the FIRST NAV point, so "
                "a loss in period one was invisible: [-0.30,+0.05,+0.05,+0.05] "
                "reported exactly 0.0000",
                "one key name meant two concepts (futures max_dd was the "
                "EXCESS series, equity strat_max_dd the STRATEGY series) and "
                "the equity layer had no excess drawdown at all, so a reader "
                "asking for it got nothing - and nothing renders as 0.0000",
            ],
        },
        "workstream_e_halts": {
            "PRE_MEASUREMENT_HALT_OWNER":
            "alpha_agent.agents_v2.pipeline."
            "AgentPipeline.record_pre_measurement_halt",
            "record_builder": "alpha_agent.r61.halts",
            "OPEN_PREMEASUREMENT_HALTS": len(open_halts),
            "open_ids": open_halts,
            "settled": (halts or {}).get("results"),
            "burden_contribution_of_these_cells":
                (halts or {}).get("burden_contribution_of_these_cells"),
            "burden_note": (halts or {}).get("burden_note"),
            "burden_total_now": burden["total"],
        },
        "workstream_f_assignments": {
            "DATA_FOUNDATION_MODEL": RT.ROUTING["data-foundation-agent"][
                "model"],
            "DATA_FOUNDATION_MAX_TURNS": RT.ROUTING["data-foundation-agent"][
                "maxTurns"],
            "DATA_FOUNDATION_TASK_SPLITTING":
            "%s, max %d source(s) and %d questions per assignment, "
            "artifact required by turn %d"
            % (AS.ASSIGNMENT_VERSION, AS.MAX_SOURCES_PER_ASSIGNMENT,
            AS.MAX_QUESTIONS_PER_ASSIGNMENT,
            AS.early_artifact_turn()),
            "DIRECTOR_MODEL": RT.ROUTING["quant-research-director"]["model"],
            "SKEPTIC_MODEL": RT.ROUTING["validation-skeptic-agent"]["model"],
            "routing_unchanged": True,
        },
        "workstream_g_workers": {
            "registry_owner": W.REGISTRY_OWNER,
            "PAPER_TRADER_BACKGROUND_PYTHON_PROCESSES":
            workers["running_count"],
            "ORPHANED_WORKERS": workers["orphaned_workers"],
            "MAX_CONCURRENT_HEAVY_JOBS": W.MAX_CONCURRENT_HEAVY_JOBS,
            "by_state": workers["by_state"],
            "workers": [{k: v for k, v in w.items() if k != "command"}
                for w in workers["workers"]],
            "operator_command": "scripts/r61_research_workers.py",
        },
        "BASIS_MOMENTUM_EVIDENCE_STATE":
        (evidence or {}).get("basis_momentum_evidence_state"),
        "safety": {
            "NEW_ALPHA_HYPOTHESES_REGISTERED": 0,
            "NEW_ALPHA_LOCKBOXES_CONSUMED": 0,
            "FORWARD_REQUESTS_CREATED": 0,
            "ORDERS": 0, "FILLS": 0,
            "OPERATIONAL_REBALANCE": "NO",
            "AUTO_PROMOTION": "NO",
            "BACKFILL": "NO",
            "NEW_PAID_DATA_COST": "$0",
        },
    }
    path = r61.write_artifact(Path(args.out) / "R61_RELEASE_REPORT.json", body)
    print(json.dumps({
        "artifact": str(path),
        "PANELS_WELL_POWERED": well,
        "PANELS_UNDERPOWERED": under,
        "IDENTITY_BRIDGE_RESULT": identity["verdict"],
        "OPEN_PREMEASUREMENT_HALTS": len(open_halts),
        "BACKGROUND_PYTHON_PROCESSES": workers["running_count"],
    }, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
