"""Run the Release 33 Predictive Edge Acquisition campaign.

RESEARCH ONLY. Reads owned Norgate data plus free point-in-time public sources
and writes immutable research artifacts under the Release-33 research root. It
creates no signal authority, no portfolio target, no proposal, no decision, no
order and no model promotion, and it writes nothing operational.

Usage (Windows PowerShell, canonical interpreter):

    & C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
        C:\\Users\\binis\\paper_trader\\scripts\\run_release33_predictive_edge.py

The campaign is bounded by its frozen contract: at most
``MAX_CONFIGS_TOTAL`` executed configurations, at most
``MAX_LOCKBOX_FINALISTS`` lockbox finalists, each opened exactly once.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# The repository root is itself the ``paper_trader`` package, so its PARENT has
# to be importable before anything inside it can be.
_PKG_PARENT = str(Path(__file__).resolve().parents[2])
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import r33                       # noqa: E402
from paper_trader.alpha_agent.r33 import campaign as _campaign  # noqa: E402
from paper_trader.alpha_agent.r33 import contract as _contract  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Release 33 predictive edge")
    parser.add_argument("--campaign-id", default=_contract.CAMPAIGN_ID)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    print(f"research root : {r33.research_root()}")
    print(f"campaign id   : {args.campaign_id}")

    result = _campaign.run(campaign_id=args.campaign_id,
                           repo=Path(__file__).resolve().parents[1],
                           verbose=not args.quiet)
    verdict = result["verdict"]

    print("")
    print("=" * 70)
    print(f"PRIMARY VERDICT : {verdict['primary_verdict']}")
    print(f"SYSTEM_RESULT   : {verdict.get('system_result')}")
    print(f"ALPHA_RESULT    : {verdict['alpha_result']}")
    print(f"qualified       : {verdict.get('qualified_candidates')}")
    print(f"denominator     : "
          f"{verdict.get('denominator_executed_configurations')}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
