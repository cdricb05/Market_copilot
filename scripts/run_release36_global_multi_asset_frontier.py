"""Run the Release 36 Global Multi-Asset Alpha Frontier campaign.

RESEARCH ONLY. This runner reads owned data, downloads free public data and
writes immutable research artifacts under the Release-36 research root. It
creates no signal authority, no portfolio target, no proposal, no decision, no
order, no model promotion and no sleeve activation, and it writes nothing to any
operational store. It spends no money, starts no trial, creates no account and
changes no subscription tier.

    python scripts/run_release36_global_multi_asset_frontier.py
    python scripts/run_release36_global_multi_asset_frontier.py --no-acquire
    python scripts/run_release36_global_multi_asset_frontier.py --contract-only
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

from paper_trader.alpha_agent.r36 import campaign as r36_campaign  # noqa: E402
from paper_trader.alpha_agent.r36 import contract as r36_contract  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Release 36 - Global Multi-Asset Alpha Frontier "
                    "(research only; no order, no promotion, no spending)")
    parser.add_argument("--campaign-id", default=r36_contract.CAMPAIGN_ID)
    parser.add_argument("--no-acquire", action="store_true",
                        help="use payloads already on disk; make no request")
    parser.add_argument("--contract-only", action="store_true",
                        help="print the frozen contract and stop")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.contract_only:
        body = r36_contract.build(
            campaign_id=args.campaign_id,
            created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(
                timespec="seconds"))
        print(json.dumps(body, indent=1, default=str))
        print("R36_CONTRACT_HASH %s" % body["contract_hash"])
        return 0

    outcome = r36_campaign.run(campaign_id=args.campaign_id,
                               acquire=not args.no_acquire,
                               verbose=not args.quiet)
    verdict = outcome["verdict"]
    summary = outcome["coverage_summary"]

    print("")
    print("R36_VERDICT %s" % verdict["verdict"])
    print("R36_SYSTEM_RESULT %s" % verdict["SYSTEM_RESULT"])
    print("R36_RESEARCH_CANDIDATE_RESULT %s"
          % verdict["RESEARCH_CANDIDATE_RESULT"])
    print("R36_ALPHA_RESULT %s" % verdict["ALPHA_RESULT"])
    print("R36_EXECUTED_CONFIGS %d of ceiling %d"
          % (verdict["executed_configurations"], verdict["ceiling"]))
    print("R36_NATIVE_CONFIGS %d" % verdict["native_configurations_executed"])
    print("R36_COVERAGE applicable=%d native=%d proxy_only=%d signal_only=%d "
          "blocked=%d untested=%d"
          % (summary["cells_applicable"], summary["tested_native"],
             summary["tested_proxy_only"], summary["tested_signal_only"],
             summary["blocked"], summary["still_untested_but_executable"]))
    print("R36_ARTIFACTS %s" % json.dumps(outcome["artifacts"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
