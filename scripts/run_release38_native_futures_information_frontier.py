"""Run the Release 38 native-futures information frontier campaign.

Assumes the Phase 1-4 artifacts (entitlement, registries, quality, native
contract layer) are frozen for the campaign id; executes experiments through
the canonical Stage-B gate and the final verdict, freezing every artifact.

Usage:
    python scripts/run_release38_native_futures_information_frontier.py
        [--campaign-id r38_native_futures_information_frontier_v4] [--quiet]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent.r38 import campaign as r38_campaign  # noqa: E402
from paper_trader.alpha_agent.r38 import contract as r38_contract  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-id", default=r38_contract.CAMPAIGN_ID)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    progress = None if args.quiet else (lambda msg: print("..", msg))
    outcome = r38_campaign.run(campaign_id=args.campaign_id,
                               progress=progress)
    verdict = outcome["verdict"]
    print("R38_VERDICT", verdict["verdict"])
    print("R38_SYSTEM_RESULT", verdict["SYSTEM_RESULT"])
    print("R38_DATA_ENTITLEMENT_RESULT", verdict["DATA_ENTITLEMENT_RESULT"])
    print("R38_RESEARCH_CANDIDATE_RESULT",
          verdict["RESEARCH_CANDIDATE_RESULT"])
    print("R38_ALPHA_RESULT", verdict["ALPHA_RESULT"])
    print("R38_POST_ACQUISITION_VALUE_RESULT",
          verdict["POST_ACQUISITION_VALUE_RESULT"])
    print("R38_DELIVERED_MARKETS",
          verdict["DATA_CAPABILITY_RESULT"]["delivered_markets"])
    print("R38_ACTUAL_NATIVE_VERIFIED",
          verdict["DATA_CAPABILITY_RESULT"]["r38_actual_native_verified"])
    print("R38_MONEY_SPENT", verdict["money_spent_during_r38_usd"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
