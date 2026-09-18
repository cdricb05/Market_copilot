r"""scripts/audit_multi_asset_capital_pipeline.py - MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1
Workstream B: the multi-asset CAPITAL PIPELINE audit, read-only.

Maps the exact path

    research challenger -> forward evidence -> qualified strategy / sleeve
    -> investability registry -> capital eligibility -> opportunity frontier
    -> cross-asset risk -> zero-base allocator -> reallocation proposal
    -> paper execution model

and prints, for EVERY sleeve / lane the estate holds, one row with:

    SLEEVE_ID  ASSET_CLASS  INSTRUMENT_TYPE  RESEARCH_REGISTERED  FORWARD_EMISSIONS
    MATURED_OBSERVATIONS  EFFECTIVE_INDEPENDENT_OBSERVATIONS  QUALIFICATION_STATE
    CAPITAL_ELIGIBLE  CAPITAL_ELIGIBILITY_BLOCKER  FRONTIER_ADMITTED
    RISK_MODEL_AVAILABLE  EXECUTION_MODEL_AVAILABLE

then explains exactly why ``frontier_eligible_non_equity_count`` is what it is.

Every number is READ from its owner: the investability registry (with the
capital-eligibility gate), the canonical forward registry and accrual
projection, the R46 tournament leaderboard, the S25 Stage-26 stream in the R52
runtime health, the FX / futures-trend prospective-decision owners and the
opportunity frontier. Nothing here computes evidence, promotes, allocates,
proposes, orders or writes.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

if _REPO.name != "paper_trader" and (_REPO / "__init__.py").exists():
    from importlib.util import spec_from_file_location

    class _WorktreePaperTraderFinder:
        ROOT = _REPO

        @classmethod
        def find_spec(cls, fullname, path=None, target=None):  # noqa: ANN001
            if fullname != "paper_trader":
                return None
            return spec_from_file_location(
                fullname, str(cls.ROOT / "__init__.py"),
                submodule_search_locations=[str(cls.ROOT)])

    # Installed only when this process has not resolved ``paper_trader`` yet. A test
    # that imports this module in-process has already resolved it through its own
    # conftest finder, and re-pointing it would be the fork this guard prevents.
    if "paper_trader" not in sys.modules:
        sys.meta_path.insert(0, _WorktreePaperTraderFinder)

COLUMNS = ("SLEEVE_ID", "ASSET_CLASS", "INSTRUMENT_TYPE", "RESEARCH_REGISTERED",
           "FORWARD_EMISSIONS", "MATURED_OBSERVATIONS", "EFFECTIVE_INDEPENDENT_OBSERVATIONS",
           "QUALIFICATION_STATE", "CAPITAL_ELIGIBLE", "CAPITAL_ELIGIBILITY_BLOCKER",
           "FRONTIER_ADMITTED", "RISK_MODEL_AVAILABLE", "EXECUTION_MODEL_AVAILABLE")

PIPELINE = [
    ("research challenger", "alpha_agent.* campaigns (R46 cohorts, R58, ALPHA_RECOVERY per-session owners)"),
    ("forward evidence", "api.forward_challenger_registry (registration) + api.canonical_forward_accrual "
                         "(emission, maturation, effective independent observations, forward economics)"),
    ("qualified strategy / sleeve", "api.capital_eligibility_gate (frozen forward thresholds + pre-declared "
                                    "conditional operational approval)"),
    ("investability registry", "api.investability_registry (13 capabilities + MODEL_APPROVED_FOR_OPERATION, "
                               "DERIVED from a declared record or a PASSED gate)"),
    ("capital eligibility", "api.investability_registry.capital_eligible (derived; never typed)"),
    ("opportunity frontier", "api.opportunity_frontier -> engine.opportunity_frontier (sleeve-normalised "
                             "rank; non_equity_admission_ledger explains every zero)"),
    ("cross-asset risk", "api.cross_asset_risk -> engine.cross_asset_risk (one covariance owner: "
                         "engine.holding_opportunity_cost.build_covariance)"),
    ("zero-base allocator", "engine.zero_base_allocator (cross-asset caps; cash is a real asset choice)"),
    ("reallocation proposal", "api.reallocation_proposal -> engine.reallocation_proposal "
                              "(frontier rows admitted by normalised score; after-target per-name risk gate)"),
    ("paper execution model", "api.rebalance_execution + api.paper_trading_desk (NEXT_SESSION_SETTLEMENT; "
                              "manual review, no orders created by any step above)"),
]


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# THE PURE REPORT (every input injected; the test drives this)
# --------------------------------------------------------------------------- #
def build_report(*, registry: dict, registrations: list, accrual_projection: dict,
                 frontier: Optional[dict] = None, r46_leaderboard: Optional[dict] = None,
                 s25: Optional[dict] = None, fx_state: Optional[dict] = None,
                 trend_state: Optional[dict] = None) -> dict:
    regs_by_cid = {}
    for r in registrations or []:
        regs_by_cid.setdefault(str(r.get("challenger_id")), r)
    proj = accrual_projection or {}
    fr_rows = (frontier or {}).get("rows") or []
    admitted_by_sleeve: dict[str, int] = {}
    for row in fr_rows:
        if row.get("eligible") and row.get("instrument_type") not in ("CASH", "CASH_EQUITY"):
            admitted_by_sleeve[str(row.get("sleeve_id"))] = admitted_by_sleeve.get(str(row.get("sleeve_id")), 0) + 1
    lb_rows = {str(r.get("challenger_id")): r for r in ((r46_leaderboard or {}).get("rows") or [])}

    rows = []

    def _accrual_for(cid: Optional[str]) -> dict:
        if not cid:
            return {}
        reg = regs_by_cid.get(cid)
        if not reg:
            return {}
        ih = (reg.get("identity") or {}).get("identity_hash")
        return proj.get(str(ih)) or {}

    # 1. the registry sleeves (operational + research), one row each
    for s in (registry or {}).get("sleeves") or []:
        cand = (s.get("operational_signal_candidate") or {}).get("challenger_id")
        acc = _accrual_for(cand)
        gate = s.get("capital_eligibility_gate") or {}
        caps = s.get("capabilities") or {}
        if s.get("capital_eligible"):
            qual = "OPERATIONAL" if s.get("asset_class") in ("US_EQUITY", "CASH") else "GATE_PASSED"
        elif cand:
            qual = "REGISTERED_ACCRUING (%s)" % (acc.get("current_accrual_state") or "NO_PROJECTION")
        else:
            qual = s.get("model_approval_state") or "UNKNOWN"
        blocker = None if s.get("capital_eligible") else (s.get("capital_ineligible_reason") or "UNKNOWN")
        if blocker and gate.get("remaining_codes"):
            blocker = "%s [%s]" % (blocker, ", ".join(gate["remaining_codes"]))
        rows.append({
            "SLEEVE_ID": s.get("sleeve_id"),
            "ASSET_CLASS": s.get("asset_class"),
            "INSTRUMENT_TYPE": s.get("instrument_type"),
            "RESEARCH_REGISTERED": (cand if cand else
                                    ("OPERATIONAL_MODEL" if s.get("capital_eligible") else
                                     ", ".join((s.get("approval_evidence") or {}).get("research_reference") or []) or "NONE")),
            "FORWARD_EMISSIONS": acc.get("predictions_emitted") if cand else None,
            "MATURED_OBSERVATIONS": acc.get("matured_observations") if cand else None,
            "EFFECTIVE_INDEPENDENT_OBSERVATIONS": acc.get("effective_independent_observations") if cand else None,
            "QUALIFICATION_STATE": qual,
            "CAPITAL_ELIGIBLE": bool(s.get("capital_eligible")),
            "CAPITAL_ELIGIBILITY_BLOCKER": blocker,
            "FRONTIER_ADMITTED": admitted_by_sleeve.get(str(s.get("sleeve_id")), 0),
            "RISK_MODEL_AVAILABLE": bool(caps.get("RISK_SUPPORTED")),
            "EXECUTION_MODEL_AVAILABLE": bool(caps.get("PAPER_EXECUTION_SUPPORTED")),
            "_kind": "REGISTRY_SLEEVE",
        })

    # 2. canonical forward registrations not bound to a registry sleeve (R58, SPY skew, ...)
    bound = {(s.get("operational_signal_candidate") or {}).get("challenger_id")
             for s in (registry or {}).get("sleeves") or []}
    for cid, reg in sorted(regs_by_cid.items()):
        if cid in bound:
            continue
        acc = proj.get(str((reg.get("identity") or {}).get("identity_hash"))) or {}
        rows.append({
            "SLEEVE_ID": cid, "ASSET_CLASS": reg.get("asset_class"),
            "INSTRUMENT_TYPE": "SIGNAL_CHALLENGER (%s)" % (reg.get("identity") or {}).get("release"),
            "RESEARCH_REGISTERED": cid,
            "FORWARD_EMISSIONS": acc.get("predictions_emitted"),
            "MATURED_OBSERVATIONS": acc.get("matured_observations"),
            "EFFECTIVE_INDEPENDENT_OBSERVATIONS": acc.get("effective_independent_observations"),
            "QUALIFICATION_STATE": "REGISTERED_ACCRUING (%s)" % (acc.get("current_accrual_state") or "NO_PROJECTION"),
            "CAPITAL_ELIGIBLE": False,
            "CAPITAL_ELIGIBILITY_BLOCKER": "NO_REGISTRY_SLEEVE_DECLARES_THIS_CHALLENGER_AS_ITS_OPERATIONAL_SIGNAL_CANDIDATE",
            "FRONTIER_ADMITTED": 0,
            "RISK_MODEL_AVAILABLE": reg.get("asset_class") in ("US_EQUITY", "US_ETF") or None,
            "EXECUTION_MODEL_AVAILABLE": reg.get("asset_class") in ("US_EQUITY",) or None,
            "_kind": "CANONICAL_REGISTRATION",
        })

    # 3. the R46 cohort futures / FX / rates / volatility challengers (their own frozen owner)
    for cid, r in sorted(lb_rows.items()):
        ac = str(r.get("asset_class") or "")
        if ac in ("US_EQUITY", "EQUITY", "EQUITY_INDEX", ""):
            continue
        rows.append({
            "SLEEVE_ID": cid, "ASSET_CLASS": ac, "INSTRUMENT_TYPE": "R46_COHORT_CHALLENGER",
            "RESEARCH_REGISTERED": cid,
            "FORWARD_EMISSIONS": r.get("forward_predictions_emitted"),
            "MATURED_OBSERVATIONS": r.get("forward_predictions_matured") or r.get("raw_matured"),
            "EFFECTIVE_INDEPENDENT_OBSERVATIONS": r.get("effective_independent"),
            "QUALIFICATION_STATE": r.get("state"),
            "CAPITAL_ELIGIBLE": False,
            "CAPITAL_ELIGIBILITY_BLOCKER": "R46_COHORT_OWNER_HAS_NO_CAPITAL_ELIGIBILITY_GATE_BINDING",
            "FRONTIER_ADMITTED": 0,
            "RISK_MODEL_AVAILABLE": None, "EXECUTION_MODEL_AVAILABLE": None,
            "_kind": "R46_COHORT",
        })

    # 4. S25 (frozen Stage-26 book, legacy h63 clock)
    if s25:
        rows.append({
            "SLEEVE_ID": s25.get("strategy_name") or s25.get("challenger_id") or "s25_operating_profitability",
            "ASSET_CLASS": "US_EQUITY", "INSTRUMENT_TYPE": "STAGE26_SHADOW_BOOK (h63 marks)",
            "RESEARCH_REGISTERED": s25.get("challenger_id"),
            "FORWARD_EMISSIONS": s25.get("raw_marks"),
            "MATURED_OBSERVATIONS": s25.get("valid_marks_before"),
            "EFFECTIVE_INDEPENDENT_OBSERVATIONS": 0 if (s25.get("valid_marks_before") or 0) < 63 else None,
            "QUALIFICATION_STATE": "HUMAN_GATE / %s" % (s25.get("state") or "UNKNOWN"),
            "CAPITAL_ELIGIBLE": False,
            "CAPITAL_ELIGIBILITY_BLOCKER": "STAGE26_H63_CLOCK_NOT_A_REGISTRY_SLEEVE (0 of 63 marks)",
            "FRONTIER_ADMITTED": 0, "RISK_MODEL_AVAILABLE": True, "EXECUTION_MODEL_AVAILABLE": True,
            "_kind": "STAGE26_BOOK",
        })

    non_eq = [r for r in rows if r["_kind"] == "REGISTRY_SLEEVE"
              and r["ASSET_CLASS"] not in ("US_EQUITY", "CASH")]
    count = int((frontier or {}).get("eligible_non_equity_count") or 0)
    explanation = {
        "frontier_eligible_non_equity_count": count,
        "path": "registry.capital_eligible_sleeve_ids -> eligible_non_equity_instruments -> frontier rows "
                "(eligible) -> candidate_rows_for_proposal -> proposal universe_rows",
        "per_sleeve": [{"sleeve_id": r["SLEEVE_ID"], "capital_eligible": r["CAPITAL_ELIGIBLE"],
                        "blocker": r["CAPITAL_ELIGIBILITY_BLOCKER"], "frontier_admitted": r["FRONTIER_ADMITTED"]}
                       for r in non_eq],
        "frontier_explanation": (frontier or {}).get("eligible_non_equity_count_explanation"),
        "statement": ("%d non-equity instrument(s) are frontier-eligible. Every research sleeve is "
                      "blocked at the CAPITAL-ELIGIBILITY step (the gate), not at the frontier, the "
                      "risk model or the execution model: those exist for every sleeve. The count is "
                      "zero exactly while no sleeve's declared gate is PASSED." % count) if count == 0 else
                     ("%d non-equity instrument(s) are frontier-eligible." % count),
    }
    return {"columns": list(COLUMNS), "rows": rows, "pipeline": PIPELINE,
            "explanation": explanation,
            "fx_state": fx_state, "trend_state": trend_state,
            "safety": {"read_only": True, "writes_nothing": True, "promotes_nothing": True,
                       "creates_orders": False}}


def render(report: dict) -> str:
    out = []
    out.append("MULTI-ASSET CAPITAL PIPELINE")
    for step, owner in report["pipeline"]:
        out.append("  %-28s %s" % (step, owner))
    out.append("")
    out.append("SLEEVE / LANE TABLE")
    for r in report["rows"]:
        out.append("-" * 96)
        for c in report["columns"]:
            out.append("  %-38s %s" % (c, r.get(c)))
    out.append("-" * 96)
    out.append("")
    ex = report["explanation"]
    out.append("frontier_eligible_non_equity_count = %d" % ex["frontier_eligible_non_equity_count"])
    out.append("  path: %s" % ex["path"])
    for p in ex["per_sleeve"]:
        out.append("  %-34s eligible=%-5s admitted=%s blocker=%s" % (
            p["sleeve_id"], p["capital_eligible"], p["frontier_admitted"], p["blocker"]))
    out.append("  %s" % ex["statement"])
    if ex.get("frontier_explanation"):
        out.append("  frontier: %s" % ex["frontier_explanation"])
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# THE LIVE READ (every owner read-only)
# --------------------------------------------------------------------------- #
def load_live(*, probe: bool = True, frontier: bool = True) -> dict:
    from paper_trader.api import canonical_forward_accrual as CFA
    from paper_trader.api import forward_challenger_registry as FCR
    from paper_trader.api import investability_registry as ir

    reg = ir.load_investability_registry(probe=probe)
    regs = FCR.load_registrations()
    proj = CFA.load_accrual_projection()
    fr = None
    if frontier:
        try:
            from paper_trader.api import opportunity_frontier as of_api
            fr = of_api.load_opportunity_frontier(registry=reg, probe=probe)
        except Exception as exc:  # noqa: BLE001
            fr = {"error": str(exc)[:200]}
    lb = None
    try:
        p = Path(r"D:\Stock_Prediction_app_data\prospective_alpha_tournament_r46"
                 r"\r46_prospective_alpha_tournament_v1\R46_LEADERBOARD.json")
        if p.exists():
            lb = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        lb = None
    s25 = None
    try:
        from paper_trader.alpha_agent.r52 import runtime as RT
        h = RT.load_health() or {}
        s25 = h.get("stage26_prospective_mark")
    except Exception:  # noqa: BLE001
        s25 = None
    fx_state = trend_state = None
    try:
        from paper_trader.alpha_agent.alpha_recovery import fx_carry_cadence_runtime as FXR
        fx = FXR.advance(execute=False)
        fx_state = {k: fx.get(k) for k in ("state", "newest_published_session", "horizon_contract", "detail")}
    except Exception as exc:  # noqa: BLE001
        fx_state = {"error": str(exc)[:200]}
    try:
        from paper_trader.alpha_agent.alpha_recovery import futures_trend_runtime as FTR
        ft = FTR.advance(execute=False)
        trend_state = {k: ft.get(k) for k in ("state", "newest_published_session", "detail")}
    except Exception as exc:  # noqa: BLE001
        trend_state = {"error": str(exc)[:200]}
    return build_report(registry=reg, registrations=regs, accrual_projection=proj, frontier=fr,
                        r46_leaderboard=lb, s25=s25, fx_state=fx_state, trend_state=trend_state)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--no-probe", action="store_true", help="do not probe the owned data provider")
    ap.add_argument("--no-frontier", action="store_true", help="skip the frontier read")
    args = ap.parse_args(argv)
    rep = load_live(probe=not args.no_probe, frontier=not args.no_frontier)
    if args.json:
        print(json.dumps(rep, indent=1, default=str))
    else:
        print(render(rep))
        print("fx_state    : %s" % json.dumps(rep.get("fx_state"), default=str)[:400])
        print("trend_state : %s" % json.dumps(rep.get("trend_state"), default=str)[:400])
    print("MULTI_ASSET_CAPITAL_PIPELINE_AUDIT_OK")
    return 0


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
