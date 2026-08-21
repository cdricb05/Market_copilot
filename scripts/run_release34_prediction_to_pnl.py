"""Run the Release 34 Prediction-to-PnL Conversion campaign.

RESEARCH ONLY. Reads owned Norgate data and writes immutable research artifacts
under the Release-34 research root. It creates no signal authority, no portfolio
target, no proposal, no decision, no order, no model promotion and no sleeve
activation, and it writes nothing operational.

Release 33 is FROZEN. This campaign does not rerun it, does not reopen its
lockbox and does not retune its candidates; it uses its published observations
as hypothesis-generating evidence only.

Usage (Windows PowerShell, canonical interpreter):

    & C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
        C:\\Users\\binis\\paper_trader\\scripts\\run_release34_prediction_to_pnl.py

The campaign is bounded by its frozen contract: at most
``MAX_PRIMARY_CONFIGS`` executed configurations across the declared families.
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

from paper_trader.alpha_agent import r34                        # noqa: E402
from paper_trader.alpha_agent.r34 import campaign as _campaign  # noqa: E402
from paper_trader.alpha_agent.r34 import contract as _contract  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Release 34 prediction-to-PnL conversion")
    parser.add_argument("--campaign-id", default=_contract.CAMPAIGN_ID)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    print(f"research root : {r34.research_root()}")
    print(f"campaign id   : {args.campaign_id}")

    result = _campaign.run(campaign_id=args.campaign_id,
                           repo=Path(__file__).resolve().parents[1],
                           verbose=not args.quiet)
    verdict = result["verdict"]

    print("")
    print("=" * 70)
    print(f"PRIMARY VERDICT : {verdict['primary_verdict']}")
    print(f"SYSTEM_RESULT   : {verdict['system_result']}")
    print(f"ALPHA_RESULT    : {verdict['alpha_result']}")
    print(f"qualified       : {verdict.get('qualified_candidates')}")
    print(f"denominator     : {verdict.get('denominator')}")
    print(f"failed          : {verdict.get('failed_conditions')}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
