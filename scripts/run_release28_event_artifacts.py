r"""Release 28 — emit the machine-readable evidence for the event-driven manager.

WHAT IT DOES
------------
Assembles every Release-28 artifact from the canonical owners and writes them into ONE
run directory. Optionally performs the first REAL corpus ingestion into the immutable
event store, which is the ingestion half of the release actually running against owned
data rather than a fixture.

WHAT IT WILL NOT DO
-------------------
It never runs the portfolio side of the cycle against production: no opportunity-cost,
reassessment or proposal artifact is written, no order is created, no target confirmed,
no proposal approved, no model promoted, and no operational ledger, holding, cash or
NAV is touched. The portfolio evidence comes from the HERMETIC replay harness, which
drives the real owners over temporary roots.

Usage:
    python scripts/run_release28_event_artifacts.py --out-dir <dir> [--ingest]
    python scripts/run_release28_event_artifacts.py --out-dir <dir> --no-replay
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

RELEASE = "RELEASE28"
DEFAULT_OUT = Path(r"D:\Stock_Prediction_app_data\event_fabric\release28")
CHALLENGER_ROOT = Path(
    r"D:\Stock_Prediction_app_data\stage26_alpha_challenger_expansion")
SHADOW_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\stage8\shadow_books")
FROZEN_CHALLENGER = "s25_operating_profitability"
OPERATING_MODEL = "fundamental_momentum_50_50_v1"


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def _write(out_dir: Path, name: str, payload) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str),
                    encoding="utf-8", newline="\n")
    return str(path)


# --------------------------------------------------------------------------- #
# Challenger continuity — READ ONLY. Nothing here creates or advances evidence.
# --------------------------------------------------------------------------- #
def challenger_continuity() -> dict:
    latest = _read_json(CHALLENGER_ROOT / "latest.json") or {}
    run_dir = Path(latest.get("run_dir") or "")
    freeze = _read_json(run_dir / "challenger_freeze_contract.json") or {}
    books = []
    if SHADOW_ROOT.exists():
        for d in sorted(SHADOW_ROOT.iterdir()):
            book = _read_json(d / "shadow_book.json")
            if not book:
                continue
            inception = book.get("inception") or {}
            books.append({
                "shadow_book_id": book.get("shadow_book_id"),
                "candidate_id": book.get("candidate_id"),
                "label": book.get("label"),
                "operating_portfolio": book.get("operating_portfolio"),
                "read_only": book.get("read_only"),
                "inception_date": inception.get("date"),
                "notional": inception.get("notional"),
                "benchmark": inception.get("benchmark"),
                "cost_bps": inception.get("cost_bps"),
                "membership_count": len(inception.get("membership") or []),
                "mark_count": len(book.get("marks") or []),
            })
    standalone = freeze.get("standalone") or {}
    registry = freeze.get("registry_identity") or {}
    return {
        "contract_id": "paper_trader.challenger_continuity/1",
        "release": RELEASE,
        "generated_at": _now(),
        "operating_model": OPERATING_MODEL,
        "operating_model_changed": False,
        "frozen_challenger": FROZEN_CHALLENGER,
        "challenger_matches_expected": registry.get("name") == FROZEN_CHALLENGER,
        "freeze_contract_spec_hash": freeze.get("spec_hash"),
        "registry_spec_hash": registry.get("spec_hash"),
        "candidate_id": registry.get("candidate_id"),
        "signal_formula": standalone.get("signal_formula"),
        "horizon_days": standalone.get("horizon_days"),
        "refit_forbidden": freeze.get("refit_forbidden"),
        "shadow_books": books,
        "forward_marks_observed": sum(b["mark_count"] for b in books),
        "forward_state": ("FORWARD_TIME_NO_MARKS_YET"
                          if not any(b["mark_count"] for b in books)
                          else "FORWARD_MARKS_PRESENT"),
        "actions_taken_by_release_28": [],
        "guarantees": {
            "spec_refit": False,
            "shadow_book_reset": False,
            "backfilled_marks": False,
            "retroactive_marks": False,
            "operational_authority_granted": False,
            "automatic_promotion": False,
            "true_forward_backfilled": False,
        },
        "note": ("Read only. Release 28 does not create, advance, reset or back-fill "
                 "challenger evidence. The event fabric is CAPABLE of supplying future "
                 "canonical formation inputs, and is forbidden by the authority table "
                 "from touching the operational target with them."),
    }


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Release 28 artifact emitter.")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--ingest", action="store_true",
                    help="Perform the first REAL corpus ingestion into the immutable "
                         "event store (append-only, idempotent, no portfolio write).")
    ap.add_argument("--fabric-dir", default=None,
                    help="Override the event-fabric store root (tests / dry runs).")
    ap.add_argument("--no-replay", action="store_true",
                    help="Skip the hermetic replay (faster; leaves replay artifacts out).")
    ap.add_argument("--lookback-days", type=int, default=30)
    args = ap.parse_args(argv)

    from paper_trader.api import event_fabric as fabric
    from paper_trader.api import event_replay as replay
    from paper_trader.api import event_signal_refresh as esr
    from paper_trader.api import source_capability as scap
    from paper_trader.engine import event_fabric as ek
    from paper_trader.engine import event_materiality as emat

    out_dir = Path(args.out_dir)
    written: list[str] = []
    started = _now()

    # ---- static contracts ------------------------------------------------- #
    written.append(_write(out_dir, "normalized_event_contract.json",
                          ek.event_contract()))
    written.append(_write(out_dir, "event_dependency_graph.json",
                          ek.build_dependency_graph()))
    written.append(_write(out_dir, "event_materiality_policy.json",
                          emat.policy_contract()))

    graph = ek.build_dependency_graph()
    authority = {
        "contract_id": "paper_trader.signal_authority_matrix/1",
        "release": RELEASE,
        "generated_at": _now(),
        "authority_policy_version": ek.AUTHORITY_POLICY_VERSION,
        "authorities": list(ek.SIGNAL_AUTHORITIES),
        "signal_speeds": list(ek.SIGNAL_SPEEDS),
        "alpha_bearing_authorities": sorted(ek.ALPHA_BEARING_AUTHORITIES),
        "risk_bearing_authorities": sorted(ek.RISK_BEARING_AUTHORITIES),
        "trigger_bearing_authorities": sorted(ek.TRIGGER_BEARING_AUTHORITIES),
        "never_operational_authorities": sorted(ek.NON_OPERATIONAL_AUTHORITIES),
        "families": [{k: f[k] for k in ("family", "record_types", "signal_speed",
                                        "decision_authority", "why_authority",
                                        "may_change_alpha", "may_change_risk",
                                        "may_trigger_reassessment",
                                        "reaches_operational_target")}
                     for f in graph["families"]],
        "unclassified_families": [f["family"] for f in graph["families"]
                                  if f["decision_authority"]
                                  not in ek.SIGNAL_AUTHORITIES],
    }
    written.append(_write(out_dir, "signal_authority_matrix.json", authority))

    # ---- measured source capability + terminal audit ----------------------- #
    matrix = scap.build_capability_matrix()
    written.append(_write(out_dir, "live_source_capability_matrix.json", matrix))

    # ---- optional REAL corpus ingestion (append-only; no portfolio write) --- #
    ingestion = {"performed": False,
                 "reason": "not requested (--ingest not passed)"}
    admitted: list = []
    if args.ingest:
        from paper_trader.api import portfolio_state as ps
        state = ps.load_portfolio_state()
        held = sorted({str(p.get("ticker")).upper()
                       for p in (state.get("positions") or []) if p.get("ticker")})
        entity_index = fabric.build_entity_index(held)
        corpus = fabric.ingest_corpus_lane(tickers=held,
                                           lookback_days=args.lookback_days,
                                           entity_index=entity_index)
        appended = fabric.append_events(corpus["events"], fabric_dir=args.fabric_dir)
        admitted = appended["admitted"]
        watermarks = fabric.advance_watermarks(
            watermarks=fabric.load_watermarks(fabric_dir=args.fabric_dir),
            per_source=corpus["per_source"], admitted=admitted,
            duplicates=appended["duplicates_suppressed"])
        fabric.save_watermarks(watermarks, fabric_dir=args.fabric_dir)
        by_family: dict = {}
        by_authority: dict = {}
        by_novelty: dict = {}
        for e in admitted:
            by_family[e["family"]] = by_family.get(e["family"], 0) + 1
            by_authority[e["decision_authority"]] = by_authority.get(
                e["decision_authority"], 0) + 1
            by_novelty[e["novelty"]] = by_novelty.get(e["novelty"], 0) + 1
        ingestion = {
            "performed": True,
            "held_tickers": held,
            "entity_index_resolved": entity_index.get("tickers_resolved"),
            "lookback_days": args.lookback_days,
            "events_built": corpus["event_count"],
            "files_scanned": corpus["scanned_files"],
            "events_admitted": appended["admitted_count"],
            "duplicates_suppressed": appended["duplicates_suppressed"],
            "by_family": by_family,
            "by_decision_authority": by_authority,
            "by_novelty": by_novelty,
            "unclassified_signal_authority": ek.unclassified_authority_count(admitted),
            "entity_mapped": sum(1 for e in admitted if e["primary_ticker"]),
            "point_in_time": {
                s: sum(1 for e in admitted if e["point_in_time_status"] == s)
                for s in ek.PIT_STATES},
            "store_root": str(fabric.fabric_root(args.fabric_dir)),
            "wrote_portfolio_artifact": False,
            "created_order": False,
        }
    written.append(_write(out_dir, "incremental_rescore_results.json", {
        "contract_id": "paper_trader.incremental_rescore_results/1",
        "release": RELEASE, "generated_at": _now(),
        "corpus_ingestion": ingestion,
        "concepts_invalidated": ek.concepts_for_events(admitted),
        "calculations_that_would_refresh": ek.affected_calculations(
            ek.concepts_for_events(admitted)),
        "calculation_owners": dict(ek.CALCULATION_OWNERS),
        "note": ("The event lane names WHICH calculations the arriving information "
                 "invalidated. Running them is the portfolio half of the cycle and is "
                 "deliberately NOT executed by this emitter against production."),
    }))

    audit = scap.terminal_audit(matrix, events=admitted)
    written.append(_write(out_dir, "source_terminal_audit.json", audit))

    freshness = fabric.build_source_freshness(capability=matrix,
                                              fabric_dir=args.fabric_dir)
    written.append(_write(out_dir, "source_freshness_state.json", freshness))

    # ---- read-only cycle status ------------------------------------------- #
    try:
        status = esr.load_event_signal_refresh_status(fabric_dir=args.fabric_dir,
                                                      limit=120)
    except Exception as exc:  # noqa: BLE001 - an emitter reports, it does not crash
        status = {"error": str(exc)[:300], "state": esr.ST_NOT_RUN}
    written.append(_write(out_dir, "event_signal_refresh_status.json", status))

    # ---- hermetic replay: the portfolio half, proven without production ---- #
    replay_results = None
    hoc_reassess = None
    attribution = None
    latency = None
    if not args.no_replay:
        base = Path(tempfile.mkdtemp(prefix="release28_replay_"))
        replay_results = replay.run_replay(base_dir=base)
        written.append(_write(out_dir, "event_replay_results.json", replay_results))

        roots = {n: base / "artifact" / n for n in
                 ("fabric", "hoc", "reassess", "realloc")}
        for p in roots.values():
            p.mkdir(parents=True, exist_ok=True)
        quiet = replay.run_cycle(world=replay.build_world(), records=[], roots=roots)
        changed_roots = {n: base / "artifact_changed" / n for n in
                         ("fabric", "hoc", "reassess", "realloc")}
        for p in changed_roots.values():
            p.mkdir(parents=True, exist_ok=True)
        changed_world = replay.build_world(holding_ranks={"H01": 240, "H02": 238})
        changed_world["prior_ranking"] = dict(changed_world["prior_ranking"],
                                              H01=18, H02=19)
        changed = replay.run_cycle(world=changed_world, records=[],
                                   roots=changed_roots)

        hoc_reassess = {
            "contract_id": "paper_trader.hoc_event_reassessment_results/1",
            "release": RELEASE, "generated_at": _now(),
            "hermetic": True,
            "canonical_delegates": dict(esr.CANONICAL_CALCULATION_DELEGATES),
            "no_change_case": {
                "state": quiet["state"],
                "reassessment_ran": quiet["reassessment_ran"],
                "reason": quiet["reassessment_reason"],
                "materiality": {k: quiet["materiality"][k] for k in
                                ("change_level", "trigger_count", "trigger_codes",
                                 "suppressed_count")},
            },
            "material_change_case": {
                "state": changed["state"],
                "reassessment_ran": changed["reassessment_ran"],
                "reason": changed["reassessment_reason"],
                "materiality": {k: changed["materiality"][k] for k in
                                ("change_level", "trigger_count", "trigger_codes",
                                 "affected_entities")},
                "holding_opportunity_cost": changed["holding_opportunity_cost"],
                "portfolio_reassessment": changed["portfolio_reassessment"],
            },
            "safety": changed["safety"],
        }
        written.append(_write(out_dir, "hoc_event_reassessment_results.json",
                              hoc_reassess))

        target = changed.get("target_portfolio") or {}
        attribution = {
            "contract_id": "paper_trader.portfolio_change_attribution/1",
            "release": RELEASE, "generated_at": _now(),
            "hermetic": True,
            "change_proposed": bool(changed.get("proposal_built")),
            "why": changed["reassessment_reason"],
            "triggers": changed["materiality"]["triggers"],
            "action_counts": target.get("action_counts"),
            "allocations": target.get("allocations"),
            "turnover": target.get("turnover"),
            "risk_before_after": target.get("risk"),
            "signal_before_after": target.get("signal"),
            "portfolio_before_after": target.get("portfolio"),
            "constraints": target.get("constraints"),
            "data_gaps": target.get("data_gaps"),
            "no_change_counterexample": {
                "state": quiet["state"],
                "why": quiet["reassessment_reason"],
                "proposal_built": quiet["proposal_built"],
            },
            "manual_review_required": changed["manual_review_required"],
            "created_order": False,
            "approved_proposal": False,
        }
        written.append(_write(out_dir, "portfolio_change_attribution.json",
                              attribution))

        latency = {
            "contract_id": "paper_trader.latency_observability/1",
            "release": RELEASE, "generated_at": _now(),
            "measured_from": "hermetic replay + live corpus ingestion",
            "material_change_cycle": changed["latency"],
            "no_change_cycle": quiet["latency"],
            "live_ingestion": ({"events_admitted": ingestion.get("events_admitted"),
                                "files_scanned": ingestion.get("files_scanned")}
                               if ingestion.get("performed") else None),
            "note": ("Measured, never modelled. An event whose source stated no "
                     "publication time contributes no latency figure."),
        }
        written.append(_write(out_dir, "latency_observability.json", latency))

    # ---- challenger continuity -------------------------------------------- #
    continuity = challenger_continuity()
    written.append(_write(out_dir, "challenger_continuity.json", continuity))

    # ---- summary ----------------------------------------------------------- #
    counts = audit["counts"]
    summary = {
        "contract_id": "paper_trader.release28_summary/1",
        "release": RELEASE,
        "title": "Event-driven active portfolio manager & live signal fabric",
        "started_at": started,
        "generated_at": _now(),
        "terminal_audit_counts": counts,
        "release_blocking": audit["release_blocking"],
        "sources_integrated": audit["integrated"],
        "sources_blocked": audit["blocked"],
        "sources_redundant_or_not_useful": audit["redundant_or_not_useful"],
        "event_families": len(ek.EVENT_FAMILY_TABLE),
        "signal_authorities": list(ek.SIGNAL_AUTHORITIES),
        "business_concepts": len(ek.BUSINESS_CONCEPTS),
        "calculations": list(ek.CALCULATION_ORDER),
        "corpus_ingestion": ingestion,
        "replay": (None if replay_results is None else {
            "passed": replay_results["passed"],
            "scenario_count": replay_results["scenario_count"],
            "check_count": replay_results["check_count"],
            "failed_scenarios": replay_results["failed_scenarios"],
        }),
        "challenger": {
            "frozen_challenger": continuity["frozen_challenger"],
            "spec_hash": continuity["freeze_contract_spec_hash"],
            "forward_marks_observed": continuity["forward_marks_observed"],
            "forward_state": continuity["forward_state"],
            "promoted": False,
        },
        "operating_model": OPERATING_MODEL,
        "operating_model_changed": False,
        "scheduler_armed": False,
        "orders_created": 0,
        "operational_mutations": 0,
        "artifacts": sorted(written),
        "safety": {
            "read_only_sources": True,
            "creates_orders": False,
            "confirms_target": False,
            "approves_proposal": False,
            "promotes_model": False,
            "purchases_data": False,
            "runs_prediction_locally": False,
            "enables_automation": False,
        },
    }
    written.append(_write(out_dir, "release28_summary.json", summary))

    print(json.dumps({
        "out_dir": str(out_dir),
        "artifacts": len(written),
        "terminal_audit_counts": counts,
        "release_blocking": audit["release_blocking"],
        "replay_passed": (None if replay_results is None else replay_results["passed"]),
        "ingestion_performed": ingestion["performed"],
        "events_admitted": ingestion.get("events_admitted"),
        "challenger_spec_hash": continuity["freeze_contract_spec_hash"],
        "forward_marks_observed": continuity["forward_marks_observed"],
    }, indent=2, sort_keys=True))
    return 0 if not audit["release_blocking"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
