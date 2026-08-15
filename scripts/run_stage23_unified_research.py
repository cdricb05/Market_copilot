"""
scripts/run_stage23_unified_research.py — Stage 23 unified alpha research CLI.

Deterministic, read-only with respect to every OPERATIONAL store. Reads the two
owned research panels and the released tournament registry, runs the operational
edge autopsy and the bounded owned-data challenger campaign through the RELEASED
evaluator and RELEASED gates, and writes immutable machine-readable artifacts
under the Stage-23 research root.

It never opens a network socket, never connects to PostgreSQL, never calls the
prediction service, never touches a desk/book/order/holding/decision store, and
can never promote a model.

Modes
-----
    --mode report     inventory + capability matrix + priority queue only (fast,
                      no panel evaluation)
    --mode full       everything: attribution + campaign + queue + decision link
    --mode campaign   the owned-data challenger campaign only
    --mode verify     re-read the latest run and validate its documents

Registering campaign results into the released candidate lifecycle is OPT-IN via
``--register``; without it the tournament registry is opened read-only.

Terminal line (exactly one):
    STAGE23_UNIFIED_RESEARCH_READY
    STAGE23_UNIFIED_RESEARCH_DATA_HOLD — <reason>
    STAGE23_UNIFIED_RESEARCH_BLOCKED — <reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_stage23_unified_research.py --mode full --json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import stage23_unified as s23   # noqa: E402
from paper_trader.alpha_agent import tournament as tt          # noqa: E402

DEFAULT_GATE_CONFIG = _REPO / "configs" / "alpha_agent" / "stage9_tournament.json"
DEFAULT_TOURNAMENT_DB = Path(
    r"D:\Stock_Prediction_app_data\alpha_agent\stage8\tournament.sqlite")


def _read_registry_readonly(db_path: Path) -> list:
    """Snapshot the existing candidate registry WITHOUT opening it for writing."""
    if not db_path.exists():
        return []
    uri = "file:%s?mode=ro" % str(db_path).replace("\\", "/")
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    try:
        rows = [tt._decode_candidate(r) for r in
                con.execute("SELECT * FROM candidates")]
    finally:
        con.close()
    return rows


def _inventory(candidates: list) -> dict:
    by_state, by_family, by_evidence = {}, {}, {}
    for c in candidates:
        by_state[c["lifecycle_state"]] = by_state.get(c["lifecycle_state"], 0) + 1
        by_family[c["family"]] = by_family.get(c["family"], 0) + 1
        ev = c.get("evidence_status") or "UNTESTED"
        by_evidence[ev] = by_evidence.get(ev, 0) + 1
    return {
        "registry_owner": "alpha_agent.tournament.CandidateRegistry",
        "total_candidates": len(candidates),
        "by_lifecycle_state": by_state,
        "by_family": by_family,
        "by_evidence_status": by_evidence,
        "keep_for_research_count": by_state.get(tt.KEEP_FOR_RESEARCH, 0),
        "ready_for_manual_review_count": by_state.get(tt.READY_FOR_MANUAL_REVIEW, 0),
        "data_hold": [
            {"candidate_id": c["candidate_id"], "name": c["name"],
             "family": c["family"], "blocker": c.get("blocker"),
             "data_dependencies": c.get("data_dependencies"),
             "pit_status": c.get("pit_status"),
             "feature": (c.get("spec") or {}).get("feature")}
            for c in candidates if c["lifecycle_state"] == tt.DATA_HOLD],
        "rejected_features": sorted(
            (c.get("spec") or {}).get("feature") or c["name"]
            for c in candidates if c["lifecycle_state"] == tt.REJECTED),
    }


def _model_system_map(inventory: dict, attribution: dict | None) -> dict:
    """The ONE canonical Stage-23 view of the model system.

    It consolidates through the existing owners; it is a REPORT, not a second
    registry — every field names the module that owns the fact.
    """
    models = [
        {"model_id": s23.OPERATIONAL_STRATEGY_ID,
         "role": "OPERATIONAL_ENSEMBLE",
         "implementation_owner": s23.OPERATIONAL_KERNEL,
         "read_owner": s23.OPERATIONAL_SCORING_OWNER,
         "contract_owner": "api.multi_horizon_registry",
         "book": s23.OPERATIONAL_BOOK_ID,
         "formula": "0.5 * z(composite_sn) + 0.5 * z(mom_6_1), fixed weights",
         "components": [s23.COMPONENT_FUNDAMENTAL, s23.COMPONENT_MOMENTUM],
         "influences_live_portfolio_rankings": True,
         "promotion_authority": "MANUAL_ONLY (AUTOMATIC_PROMOTION_ALLOWED=False)"},
        {"model_id": s23.COMPONENT_FUNDAMENTAL,
         "role": "PAPER_CHAMPION_COMPONENT",
         "implementation_owner": "frozen Phase 10-L panel column",
         "formula": "equal-weight sector-neutral z(fcf_to_assets) + z(-operating_accruals)",
         "source_datasets": ["owned EODHD fundamentals (Phase 10-B/C/D)"],
         "survivorship_status": "SURVIVOR_BIASED_545_NAME_UNIVERSE",
         "influences_live_portfolio_rankings": True,
         "promotion_authority": "MANUAL_ONLY"},
        {"model_id": s23.COMPONENT_MOMENTUM,
         "role": "PAPER_CHALLENGER_COMPONENT",
         "implementation_owner": "api.monthly_momentum_emitter -> momentum_monthly_panel",
         "formula": "close[m-1]/close[m-7]-1 on month-end TOTALRETURN closes",
         "source_datasets": ["owned Norgate Russell-1000 Current & Past daily NPZ"],
         "survivorship_status": "SURVIVORSHIP_SAFE",
         "influences_live_portfolio_rankings": True,
         "promotion_authority": "MANUAL_ONLY"},
        {"model_id": "composite_sn_repaired",
         "role": "RESEARCH_CHALLENGER",
         "implementation_owner": "api.alpha_factory (CHALLENGER_SIGNAL)",
         "influences_live_portfolio_rankings": False,
         "note": "sector-repaired revalidation of composite_sn (Phase 17-A)",
         "promotion_authority": "MANUAL_ONLY"},
    ]
    out = {
        "stage23_version": s23.STAGE23_VERSION,
        "generated_by": s23.ORIGIN,
        "is_a_report_not_a_registry": True,
        "authoritative_registries": {
            "candidate_lifecycle": "alpha_agent.tournament.CandidateRegistry",
            "research_memory": "alpha_agent.research_registry",
            "model_contracts": "api.multi_horizon_registry",
            "alpha_metadata": "api.alpha_registry",
        },
        "models": models,
        "tournament_inventory": inventory,
        "automatic_promotion_allowed": False,
    }
    if attribution:
        out["measured_edge"] = {
            k: {"rank_ic": v.get("metrics", {}).get("rank_ic"),
                "rank_ic_t": v.get("metrics", {}).get("rank_ic_t"),
                "spread_t": v.get("metrics", {}).get("spread_t"),
                "net25_spread": v.get("metrics", {}).get("net25_spread"),
                "scored_periods": v.get("metrics", {}).get("scored_periods"),
                "gate": v.get("gate", {}).get("target_state")}
            for k, v in (attribution.get("joint_universe") or {}).items()}
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Stage 23 unified alpha research (research-only, read-only).")
    ap.add_argument("--mode", default="full",
                    choices=["report", "full", "campaign", "verify"])
    ap.add_argument("--gate-config", default=str(DEFAULT_GATE_CONFIG))
    ap.add_argument("--tournament-db", default=str(DEFAULT_TOURNAMENT_DB))
    ap.add_argument("--research-root", default=None)
    ap.add_argument("--mom-panel", default=None)
    ap.add_argument("--fund-panel", default=None)
    ap.add_argument("--sector-map", default=None)
    ap.add_argument("--register", action="store_true",
                    help="register campaign results through the RELEASED candidate "
                         "lifecycle (opt-in; still cannot promote anything)")
    ap.add_argument("--evidence-date", default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    try:
        cfg = json.loads(Path(args.gate_config).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        print("%s — cannot read gate config %s: %s" % (s23.BLOCKED, args.gate_config, exc))
        return 2

    root = Path(args.research_root) if args.research_root else None

    if args.mode == "verify":
        base = root or s23.DEFAULT_RESEARCH_ROOT
        latest = Path(base) / "latest.json"
        if not latest.exists():
            print("%s — no Stage-23 run found at %s" % (s23.BLOCKED, latest))
            return 1
        doc = json.loads(latest.read_text(encoding="utf-8"))
        missing = [n for n, d in (doc.get("documents") or {}).items()
                   if not Path(d["path"]).exists()]
        if missing:
            print("%s — run %s is missing documents: %s"
                  % (s23.BLOCKED, doc.get("run_id"), ", ".join(missing)))
            return 1
        if args.json:
            print(json.dumps(doc, indent=1, default=str))
        print(s23.READY)
        return 0

    candidates = _read_registry_readonly(Path(args.tournament_db))
    inventory = _inventory(candidates)

    documents: dict = {}
    attribution = None
    campaign = None

    try:
        panel = s23.load_momentum_panel(args.mom_panel)
        fund = s23.load_fundamental_panel(args.fund_panel)
        sector = s23.load_sector_map(args.sector_map)
    except FileNotFoundError as exc:
        print("%s — required owned panel unavailable: %s" % (s23.DATA_HOLD, exc))
        return 1

    if args.mode in ("full", "campaign"):
        if args.mode == "full":
            attribution = s23.run_edge_attribution(panel, fund, sector, cfg)
            documents["current_model_edge_attribution"] = attribution
        mom_periods, _cov = s23.build_momentum_periods(panel, s23.f_mom_6_1)
        champ = s23.evaluate_cross_sectional_signal(
            mom_periods, feature=s23.COMPONENT_MOMENTUM,
            horizon_days=s23.HORIZON_MONTHLY, cfg=cfg)["series"]
        registry = None
        if args.register:
            registry = tt.CandidateRegistry(args.tournament_db)
        try:
            campaign = s23.run_owned_campaign(panel, cfg, champion_series=champ,
                                              registry=registry)
            for r in campaign["results"]:
                r.pop("_series", None)
            documents["experiment_manifest"] = {
                "stage23_version": s23.STAGE23_VERSION,
                "summary": campaign["summary"],
                "hypotheses": [{k: v for k, v in r.items()
                                if k in ("hypothesis_id", "name", "family",
                                         "economic_rationale", "expected_mechanism",
                                         "expected_sign", "data_basis", "panel",
                                         "horizon_days", "rebalance", "primary_metric",
                                         "rejection_criteria",
                                         "resolves_existing_data_hold",
                                         "near_duplicate_of", "duplicate_note",
                                         "prior_research_priority", "duplicate_check")}
                               for r in campaign["results"]],
            }
            documents["challenger_results"] = {
                "stage23_version": s23.STAGE23_VERSION,
                "summary": campaign["summary"],
                "null_results_preserved": True,
                "results": campaign["results"],
            }
            if registry is not None:
                documents["challenger_registration"] = \
                    s23.register_campaign_candidates(
                        registry, cfg, campaign, evidence_date=args.evidence_date)
        finally:
            if registry is not None:
                registry.close()

    documents["data_capability_matrix"] = s23.build_capability_matrix(
        panel, fund, sector, held_candidates=inventory["data_hold"])
    documents["research_priority_queue"] = s23.build_priority_queue(
        campaign=campaign, held_candidates=inventory["data_hold"],
        attribution=attribution)
    documents["portfolio_decision_research_link"] = s23.build_decision_link(
        attribution or {})
    analyst_holds = [c for c in inventory["data_hold"]
                     if c.get("family") == "ANALYST_EARNINGS"]
    documents["intrinio_readiness"] = s23.build_intrinio_readiness(
        observed_vintage_dates=1 if analyst_holds else None)
    documents["analyst_revision_preregistration"] = s23.build_analyst_preregistration(
        attribution=attribution)
    documents["model_system_map"] = _model_system_map(inventory, attribution)

    rid = s23.run_id_for({
        "version": s23.STAGE23_VERSION,
        "mode": args.mode,
        "mom": panel.fingerprint.get("sha256"),
        "fund": fund["fingerprint"].get("sha256"),
        "sector": sector["fingerprint"].get("sha256"),
        "gate_cfg": s23._sha256_text(s23.canonical_json(cfg)),
    })
    latest = s23.write_artifacts(root, documents, run_id=rid)

    if args.json:
        print(json.dumps({
            "run_id": rid,
            "run_dir": latest["run_dir"],
            "documents": sorted(documents),
            "inventory": {k: inventory[k] for k in
                          ("total_candidates", "by_lifecycle_state",
                           "keep_for_research_count")},
            "campaign_summary": (campaign or {}).get("summary"),
        }, indent=1, default=str))

    print(s23.READY)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
