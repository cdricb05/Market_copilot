"""Release 39 runner - the Autonomous Universal Alpha Discovery Engine.

RESEARCH ONLY. Reads owned data within existing entitlements, writes
immutable research artifacts under the Release-39 research root, spends
nothing, promotes nothing, mutates nothing operational.

Usage (canonical venv python):

    python scripts/run_release39_universal_alpha_discovery.py            # full
    python scripts/run_release39_universal_alpha_discovery.py --preflight
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import r39                          # noqa: E402
from paper_trader.alpha_agent.r39 import (                        # noqa: E402
    campaign,
    contract,
    estate,
    integrity,
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--preflight", action="store_true",
                    help="estate inventory + R38 integrity map only")
    ap.add_argument("--campaign-id", default=contract.CAMPAIGN_ID)
    args = ap.parse_args()

    r39.campaign_dir(args.campaign_id).mkdir(parents=True, exist_ok=True)
    if args.preflight:
        est = estate.build()
        estate.freeze(est)
        imap = integrity.build()
        integrity.freeze(imap)
        print("R39_PREFLIGHT_OK counts=%s"
              % imap["counts_by_research_status"])
        return 0
    return campaign.safe_run(args.campaign_id)


if __name__ == "__main__":
    raise SystemExit(main())
