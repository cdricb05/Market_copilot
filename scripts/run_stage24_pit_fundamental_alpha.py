"""Stage 24 CLI - point-in-time, survivorship-safe fundamental alpha research.

Read-only with respect to every operational store. Writes only under the Stage-24
research root and (unless --no-register) the existing research tournament
registry. No network, no provider call, no prediction service, no promotion.

Exactly one terminal token is printed.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent import stage24_pit_fundamental as s24  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--research-root", default=None)
    ap.add_argument("--mom-panel", default=None)
    ap.add_argument("--fund-panel", default=None)
    ap.add_argument("--identity-db", default=None)
    ap.add_argument("--cf-index", default=None)
    ap.add_argument("--tournament-config", default=None)
    ap.add_argument("--no-register", action="store_true",
                    help="skip candidate-lifecycle registration (dry research)")
    ap.add_argument("--evidence-date", default=None)
    ap.add_argument("--print-summary", action="store_true")
    args = ap.parse_args(argv)

    res = s24.run(research_root=args.research_root, mom_panel=args.mom_panel,
                  fund_panel=args.fund_panel, identity_db=args.identity_db,
                  cf_index=args.cf_index,
                  tournament_cfg_path=args.tournament_config,
                  register=not args.no_register,
                  evidence_date=args.evidence_date)

    if not res.get("ok"):
        print("reason: %s" % res.get("reason"))
        print(res.get("token") or s24.BLOCKED)
        return 2

    print("run_id  : %s" % res["run_id"])
    print("run_dir : %s" % res["run_dir"])
    for a in res["artifacts"]:
        print("  %-40s %s" % (a["artifact"], a["sha256"]))
    if args.print_summary:
        print(json.dumps(res["summary"], indent=2, sort_keys=True))
    print(res["token"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
