#!/usr/bin/env python
r"""Phase 9 — bounded, TRUE end-to-end Controlled Autonomous Research Execution Bridge
campaign.

Runs ONE bounded research campaign against the REAL canonical AlphaAgent machinery,
owned data only, and proves the full loop actually executes:

  1. fingerprints the protected operational ledgers BEFORE (via the ONE canonical
     owner ``alpha_agent.runtime.fingerprint_ledgers`` — never a bridge-local copy,
     never a hard-coded ledger path);
  2. reads the current immutable Persistent Alpha Research Agent assessment
     (``api.research_agent``) for the latest eligible market session + active book;
  3. selects the top eligible DISPATCHABLE research opportunity and builds a bounded
     Controlled Research Mandate (``api.research_bridge`` over ``engine.research_bridge``);
  4. dispatches the mandate governance marker IDEMPOTENTLY into the EXISTING alpha_agent
     durable ResearchQueue (never a second queue; the bridge-scoped origin is NOT admitted
     by the scheduled collect drain, so the marker stays inert under cadence);
  5. EXECUTES the mandate THROUGH the shared machinery — ``run_mandate_campaign`` seeds
     REAL pre-registered owned-price revalidation experiments into the SAME queue, runs an
     EXPLICIT bounded operator drain that claims ONLY the bridge's own jobs, executes them
     through the PRODUCTION CAT_EXPERIMENT handler on the owned Norgate panel, and ingests
     the resulting Stage-5 evidence through the canonical tournament ingester;
  6. runs the ONE canonical Alpha Strength Gate over the FRESH per-candidate evidence
     (``alpha_agent.tournament.classify_evidence`` + Benjamini-Hochberg FDR), produces the
     Research Agent re-assessment and persists the immutable result envelope;
  7. fingerprints the protected operational ledgers AFTER and asserts they are UNCHANGED.

Reported ``experiments_executed`` means experiments actually CLAIMED and executed for THIS
mandate (queue attempts>=1) — never ``len(all pre-existing registry rows)``.

RESEARCH GOVERNANCE ONLY. It creates no order/fill, changes no holding/target/cash/NAV,
promotes/recalibrates/retrains no model, runs no prediction, activates no scheduler/cadence,
touches no operational ledger and purchases no data. NO_DEFENSIBLE_ALPHA, DATA_HOLD and
NO_EXECUTABLE_EXPERIMENTS are valid successful conclusions. It never optimises toward a
positive result.

Usage (Windows PowerShell):
  .\.venv-win\Scripts\python.exe scripts\run_research_bridge_campaign.py --json
  .\.venv-win\Scripts\python.exe scripts\run_research_bridge_campaign.py --no-execute --json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.api import research_bridge as bridge      # noqa: E402


def _resolve_latest_assessment(active_book_id, eligible_market_date):
    """Resolve (book, date) from the Research Agent assessment index if not supplied."""
    from paper_trader.api import research_agent as ra
    idx = ra._load_json(ra._index_path()) or {}
    if active_book_id and eligible_market_date:
        return active_book_id, eligible_market_date
    entries = [e for e in idx.values() if isinstance(e, dict)]
    if not entries:
        return active_book_id, eligible_market_date
    latest = max(entries, key=lambda e: (e.get("eligible_market_date") or "",
                                         e.get("generated_at") or ""))
    return (active_book_id or latest.get("active_book_id"),
            eligible_market_date or latest.get("eligible_market_date"))


def _digest(fingerprint: dict) -> str:
    """Compact SHA-256 over the ALREADY-COMPUTED canonical ledger fingerprint map.

    This digests the map produced by ``alpha_agent.runtime.fingerprint_ledgers`` — it
    does NOT walk any operational-ledger root itself (that is the one canonical owner's
    job) and hard-codes no ledger path.
    """
    return hashlib.sha256(
        json.dumps(fingerprint, sort_keys=True).encode("utf-8")).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description="Bounded Research Execution Bridge campaign.")
    ap.add_argument("--active-book", default=None)
    ap.add_argument("--eligible-date", default=None)
    ap.add_argument("--bridge-dir", default=None)
    ap.add_argument("--max-experiments", type=int, default=None,
                    help="Override the mandate experiment budget for this run.")
    ap.add_argument("--no-dispatch", action="store_true",
                    help="Skip enqueuing the mandate governance marker (idempotent).")
    ap.add_argument("--no-execute", action="store_true",
                    help="Skip the real generate/claim/execute/ingest loop (mandate only).")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    # (1) canonical operational-ledger fingerprint BEFORE (reuses the one owner).
    ledger_before = bridge.fingerprint_operational_ledgers()

    book, date = _resolve_latest_assessment(args.active_book, args.eligible_date)
    assessment = bridge.read_latest_assessment(active_book_id=book,
                                               eligible_market_date=date)
    if not assessment:
        out = {"status": "NO_ASSESSMENT", "active_book_id": book,
               "eligible_market_date": date,
               "detail": "No persisted Research Agent assessment for this book/date."}
        print(json.dumps(out, indent=2) if args.json else out["detail"])
        return 0

    normalized = bridge.select_opportunity(assessment=assessment)
    if not normalized or not normalized.get("dispatchable"):
        out = {"status": "NO_DISPATCHABLE_OPPORTUNITY", "active_book_id": book,
               "eligible_market_date": date,
               "opportunity_id": (normalized or {}).get("opportunity_id"),
               "non_dispatchable_reason": (normalized or {}).get("non_dispatchable_reason")}
        print(json.dumps(out, indent=2) if args.json else json.dumps(out))
        return 0

    mrec = bridge.build_and_persist_mandate(normalized_opportunity=normalized,
                                            active_book_id=book, bridge_dir=args.bridge_dir)
    mandate = mrec["mandate"]

    # (4) idempotent governance-marker dispatch into the EXISTING queue.
    dispatch = None
    if not args.no_dispatch:
        dispatch = bridge.dispatch_mandate(mandate=mandate, active_book_id=book,
                                           bridge_dir=args.bridge_dir)

    # (5) EXECUTE through the shared machinery (generate -> claim -> execute -> ingest).
    if args.no_execute:
        candidate_rows = []
        campaign = {"entrypoint": "mandate-only (execution skipped)",
                    "campaign_id": "rbc_%s_%s" % (book, date),
                    "operator_run_bounded_drain": False,
                    "experiments_generated_for_mandate": 0, "jobs_claimed": 0,
                    "experiments_executed": 0, "experiments_completed": 0,
                    "canonical_results_ingested": 0, "generations": 0}
        exec_result = {"census": campaign, "candidate_ids": []}
    else:
        exec_result = bridge.run_mandate_campaign(
            mandate=mandate, active_book_id=book, eligible_market_date=date,
            max_experiments=args.max_experiments)
        candidate_rows = exec_result["candidate_rows"]
        campaign = exec_result["campaign"]

    # (7) canonical operational-ledger fingerprint AFTER; assert unchanged.
    ledger_after = bridge.fingerprint_operational_ledgers()
    unchanged = ledger_before == ledger_after
    ledger_fp = {
        "checked": True, "unchanged": bool(unchanged),
        "fingerprint_owner": "alpha_agent.runtime.fingerprint_ledgers",
        "files_before": len(ledger_before), "files_after": len(ledger_after),
        "digest_before": _digest(ledger_before), "digest_after": _digest(ledger_after),
    }

    # (6) ONE canonical Alpha Strength Gate -> envelope -> reassessment -> persist.
    ingest = bridge.ingest_result(mandate=mandate, candidate_rows=candidate_rows,
                                  campaign=campaign, active_book_id=book,
                                  bridge_dir=args.bridge_dir, ledger_fingerprint=ledger_fp)

    alpha = (ingest.get("envelope") or {}).get("alpha_strength") or {}
    out = {
        "status": "CAMPAIGN_COMPLETE",
        "active_book_id": book,
        "eligible_market_date": date,
        "champion": mandate.get("champion"),
        "opportunity_id": normalized.get("opportunity_id"),
        "opportunity_category": normalized.get("category"),
        "degradation_category": normalized.get("degradation_category"),
        "mandate_id": mandate.get("mandate_id"),
        "dispatch_identity": mandate.get("dispatch_identity"),
        "governance_marker_dispatched": bool(dispatch and dispatch.get("dispatched")),
        "governance_marker_job_id": (dispatch or {}).get("job_id"),
        "allowed_hypothesis_families": mandate.get("allowed_hypothesis_families"),
        "horizons": mandate.get("horizons", {}).get("horizons"),
        "forward_label_horizons": mandate.get("horizons", {}).get("forward_label_horizons"),
        # --- TRUE per-mandate execution counts (never pre-existing registry rows) --- #
        "prior_candidates_considered": campaign.get("prior_candidates_considered"),
        "experiments_generated_for_mandate": campaign.get("experiments_generated_for_mandate"),
        "experiments_generated_this_run": campaign.get("experiments_generated_this_run"),
        "jobs_claimed": campaign.get("jobs_claimed"),
        "experiments_executed": campaign.get("experiments_executed"),
        "experiments_completed": campaign.get("experiments_completed"),
        "experiments_data_hold": campaign.get("experiments_data_hold"),
        "experiments_rejected": campaign.get("experiments_rejected"),
        "canonical_results_ingested": campaign.get("canonical_results_ingested"),
        "candidates_gated": len(candidate_rows),
        "gate_reproduced_all": alpha.get("gate_reproduced_all"),
        "alpha_strength_counts": alpha.get("counts"),
        "fdr_alpha": alpha.get("fdr_alpha"),
        "fdr_survivors": (alpha.get("counts") or {}).get("fdr_survivors"),
        "outcome": ingest.get("outcome"),
        "reassessment_verdict": ingest.get("verdict"),
        "no_defensible_alpha": alpha.get("no_defensible_alpha"),
        "best_challenger": alpha.get("best_challenger"),
        "envelope_id": ingest.get("envelope_id"),
        "operational_ledger_unchanged": bool(unchanged),
        "operational_ledger_fingerprint_owner": "alpha_agent.runtime.fingerprint_ledgers",
        "champion_changed": False,
        "manual_model_review_required": True,
        "scheduled_cadence_admits_bridge_origin": False,
        "safety": "research governance only; no order/fill/target/holding/promotion/cadence/purchase",
    }
    print(json.dumps(out, indent=2) if args.json else json.dumps(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
