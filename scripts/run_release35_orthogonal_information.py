"""Run the Release-35 orthogonal-information campaign.

RESEARCH ONLY. This script acquires free public data, reads owned research
stores and writes immutable research artifacts under the Release-35 research
root. It creates no signal authority, no portfolio target, no proposal, no
decision, no order and no operational write; it promotes no model, activates no
sleeve, mutates no holdings and no cash, restarts nothing and spends nothing.

    python scripts/run_release35_orthogonal_information.py
    python scripts/run_release35_orthogonal_information.py --no-acquire
    python scripts/run_release35_orthogonal_information.py --contract-only

``--no-acquire`` reads whatever is already in the acquisition store instead of
going to the network, which is what a re-run or an offline validation wants.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from paper_trader.alpha_agent import r35  # noqa: E402
from paper_trader.alpha_agent.r35 import campaign as _campaign  # noqa: E402
from paper_trader.alpha_agent.r35 import contract as _contract  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Release 35 - orthogonal information acquisition and "
                    "incremental alpha. Research only; no order, no decision, "
                    "no operational write, no money.")
    parser.add_argument("--campaign-id", default=_contract.CAMPAIGN_ID)
    parser.add_argument("--no-acquire", action="store_true",
                        help="use the acquisition store on disk; no network")
    parser.add_argument("--contract-only", action="store_true",
                        help="print the frozen contract hash and exit")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)

    if args.contract_only:
        print(json.dumps({
            "campaign_id": args.campaign_id,
            "contract_hash": _contract.contract_hash(),
            "planned_config_total": _contract.PLANNED_CONFIG_TOTAL,
            "max_primary_configs": _contract.MAX_PRIMARY_CONFIGS,
            "research_root": str(r35.research_root()),
            "may_spend_money": _contract.MAY_SPEND_MONEY,
            "fresh_unseen_evidence_exists":
                _contract.FRESH_UNSEEN_EVIDENCE_EXISTS,
            "alpha_pass_requires": _contract.ALPHA_PASS_REQUIRES,
        }, indent=2))
        return 0

    result = _campaign.run(campaign_id=args.campaign_id,
                           acquire=not args.no_acquire,
                           verbose=not args.quiet)
    verdict = result["verdict"]
    print("")
    print("R35_VERDICT %s" % verdict["primary_verdict"])
    print("R35_SYSTEM_RESULT %s" % verdict["SYSTEM_RESULT"])
    print("R35_RESEARCH_CANDIDATE_RESULT %s"
          % verdict["RESEARCH_CANDIDATE_RESULT"])
    print("R35_ALPHA_RESULT %s" % verdict["ALPHA_RESULT"])
    print("R35_ARTIFACTS %s" % result["root"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
