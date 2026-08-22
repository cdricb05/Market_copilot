"""Run the Release 37 Native-Market Data Expansion & Purchase Gate campaign.

RESEARCH ONLY. This runner reads owned data, downloads free public samples and
writes immutable research artifacts under the Release-37 research root. It
creates no signal authority, no portfolio target, no proposal, no decision, no
order, no model promotion and no sleeve activation, and it writes nothing to any
operational store.

It spends no money, starts no trial, creates no account, accepts no licence,
submits no payment detail, changes no subscription tier, installs no CUDA,
downloads no model weights and buys no cloud compute. A BUY recommendation is a
recommendation to a person and carries no purchasing authority.

    python scripts/run_release37_native_market_data_gate.py
    python scripts/run_release37_native_market_data_gate.py --no-acquire
    python scripts/run_release37_native_market_data_gate.py --contract-only
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from paper_trader.alpha_agent.r37 import campaign as r37_campaign  # noqa: E402
from paper_trader.alpha_agent.r37 import contract as r37_contract  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Release 37 - Native-Market Data Expansion & Purchase Gate "
                    "(research only; no order, no promotion, no spending)")
    parser.add_argument("--campaign-id", default=r37_contract.CAMPAIGN_ID)
    parser.add_argument("--no-acquire", action="store_true",
                        help="use samples already on disk; make no request")
    parser.add_argument("--contract-only", action="store_true",
                        help="print the frozen contract and stop")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.contract_only:
        body = r37_contract.build(
            campaign_id=args.campaign_id,
            created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(
                timespec="seconds"))
        print(json.dumps(body, indent=1, default=str))
        print("R37_CONTRACT_HASH %s" % body["contract_hash"])
        return 0

    outcome = r37_campaign.run(campaign_id=args.campaign_id,
                               acquire=not args.no_acquire,
                               verbose=not args.quiet)
    verdict = outcome["verdict"]
    scorecard = outcome["scorecard"]

    print("")
    print("R37_VERDICT %s" % verdict["verdict"])
    print("R37_SYSTEM_RESULT %s" % verdict["SYSTEM_RESULT"])
    print("R37_PURCHASE_RECOMMENDATION_RESULT %s"
          % verdict["PURCHASE_RECOMMENDATION_RESULT"])
    print("R37_ALPHA_RESULT %s" % verdict["ALPHA_RESULT"])
    print("R37_DATASETS_REVIEWED %d in %d lanes"
          % (verdict["datasets_reviewed"], verdict["lanes_reviewed"]))
    print("R37_BEST %s EXPECTED to unlock %d of %d blocked cells (%.1f%%)"
          % (verdict["best_dataset"], verdict["best_dataset_cells_unlocked"],
             verdict["blocked_cells_at_release36"],
             100.0 * verdict["best_dataset_share_of_frontier"]))
    print("R37_ACQUISITION_GATE %s decided by %s"
          % (json.dumps(verdict["canonical_acquisition_recommended"]),
             verdict["acquisition_decision_owner"]))
    print("R37_POST_ACQUISITION_PURCHASE_RECOMMENDATIONS %s"
          % json.dumps(verdict["post_acquisition_purchase_recommendations"]))
    print("R37_RANKED %s" % json.dumps(scorecard["ranked_investable"]))
    print("R37_SAMPLES_VALIDATED %d %s"
          % (verdict["samples_validated_count"],
             json.dumps(verdict["samples_validated"])))
    print("R37_BLOCKED_ON_VENDOR_CONTACT %s"
          % json.dumps(verdict["blocked_on_vendor_contact"]))
    # Two different facts, printed as two: hardware capability is not the same
    # as being able to run something today with the libraries actually installed.
    print("R37_ML_RUNNABLE_TODAY %d of %d  HARDWARE_FEASIBLE %d of %d"
          % (verdict["ml_models_currently_runnable"], verdict["ml_models_total"],
             verdict["ml_models_hardware_feasible"], verdict["ml_models_total"]))
    print("R37_ML_READINESS %s" % json.dumps(verdict["ml_readiness_counts"]))
    print("R37_ML_MISSING_LIBRARIES %s"
          % json.dumps(verdict["ml_missing_libraries"]))
    print("R37_ML_BINDING_CONSTRAINTS %s"
          % json.dumps(verdict["ml_binding_constraints"]))
    print("R37_MONEY_SPENT %.2f  TRIALS %d  ACCOUNTS %d  SUBSCRIPTIONS %d"
          % (verdict["money_spent_usd"], verdict["trials_started"],
             verdict["accounts_created"], verdict["subscriptions_changed"]))
    print("R37_ARTIFACTS %s" % json.dumps(outcome["artifacts"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
