"""scripts/run_alpha_exhaustion_campaign.py — Release-27 CLI.

Executes the autonomous alpha exhaustion campaign and writes the machine-readable
evidence under the campaign research root. Exactly one terminal token is printed,
and it can only be the READY one when the final frontier audit reports zero
executable free/owned high-priority families.

RESEARCH ONLY. Read-only with respect to every operational store and to the
frozen Stage-26 forward challenger: no order, no fill, no signal, no trade
decision, no proposal, no rebalance plan, no Daily Close, no model promotion, no
champion change, no shadow-book reset or backfill, no PostgreSQL write, no
prediction service, no backend restart.

    .venv-win\\Scripts\\python.exe scripts\\run_alpha_exhaustion_campaign.py
        [--families a,b] [--research-root <dir>] [--evidence-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent import stage27_alpha_exhaustion as r27  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--research-root", default=None)
    ap.add_argument("--mom-panel", default=None)
    ap.add_argument("--identity-db", default=None)
    ap.add_argument("--cf-index", default=None)
    ap.add_argument("--issuer-db", default=None)
    ap.add_argument("--shares-index", default=None)
    ap.add_argument("--price-surface", default=None)
    ap.add_argument("--fsds-cache", default=None)
    ap.add_argument("--insider-cache", default=None)
    ap.add_argument("--full-index-cache", default=None)
    ap.add_argument("--tournament-db", default=None)
    ap.add_argument("--evidence-date", default=None)
    ap.add_argument("--first-month", default=None)
    ap.add_argument("--families", default="",
                    help="comma-separated subset of registered families; the "
                         "default runs every one")
    args = ap.parse_args(argv)

    started = time.monotonic()
    families = [f.strip() for f in args.families.split(",") if f.strip()] or None
    res = r27.run(research_root=args.research_root, mom_panel=args.mom_panel,
                  identity_db=args.identity_db, cf_index=args.cf_index,
                  issuer_db=args.issuer_db, shares_index=args.shares_index,
                  price_surface=args.price_surface, fsds_cache=args.fsds_cache,
                  insider_cache=args.insider_cache,
                  full_index_cache=args.full_index_cache,
                  tournament_db=args.tournament_db, families=families,
                  first_month=args.first_month,
                  evidence_date=args.evidence_date)
    if not res.get("ok"):
        print("reason: %s" % res.get("reason"))
        print(res.get("token") or r27.BLOCKED)
        return 2

    p = res["payload"]
    s = p["campaign_summary"]
    print("run id             : %s" % res["run_id"])
    print("run dir            : %s" % res["run_dir"])
    print("artifacts          : %d" % len(res["artifacts"]))
    print("panel formations   : %s   scored rows: %s"
          % (s["panel_formations"], s["panel_scored_rows"]))
    print("families executed  : %s" % s["families_executed"])
    print("hypotheses         : %s" % s["hypotheses_executed"])
    print("by terminal state  : %s" % json.dumps(s["hypotheses_by_terminal_state"]))
    print("survivors          : %s" % (", ".join(s["survivors"]) or "none"))
    print("families considered: %s" % s["families_considered"])
    for f in p["family_execution_ledger"]["families"]:
        print("  %-42s %-34s %d hyp"
              % (f["family"], f["terminal_state"], f["hypotheses"]))
    print("EXECUTABLE_FREE_OWNED_HIGH_PRIORITY_FAMILIES = %s"
          % s["executable_free_owned_high_priority_families"])
    print("challenger hash ok : %s   forward marks: %s"
          % (s["forward_challenger_continuity_ok"], s["forward_marks"]))
    print("paid data decision : %s" % s["paid_data_decision"])
    print("operational muts   : %d   promotion: %s"
          % (s["operational_mutations"], s["model_promotion"]))
    print("next constraint    : %s" % s["next_major_constraint"])
    print("elapsed            : %.1fs" % (time.monotonic() - started))
    print(res["token"])
    return 0 if res["token"] == r27.READY else 1


if __name__ == "__main__":
    raise SystemExit(main())
