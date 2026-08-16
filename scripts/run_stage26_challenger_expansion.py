"""scripts/run_stage26_challenger_expansion.py — Stage 26 CLI.

Executes the Stage-26 research contract and writes the machine-readable evidence
under the Stage-26 research root. Exactly one terminal token is printed.

RESEARCH ONLY. Read-only with respect to every operational store: no order, no
fill, no signal, no trade decision, no proposal, no rebalance plan, no Daily
Close, no model promotion, no champion change, no PostgreSQL write, no prediction
service, no backend restart.

    .venv-win\\Scripts\\python.exe scripts\\run_stage26_challenger_expansion.py
        [--no-activate-shadow] [--research-root <dir>] [--evidence-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent import stage26_challenger_expansion as s26  # noqa: E402


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
    ap.add_argument("--tournament-db", default=None)
    ap.add_argument("--evidence-date", default=None)
    ap.add_argument("--no-activate-shadow", action="store_true",
                    help="run every research workstream but do NOT enrol the "
                         "frozen challenger in the canonical shadow lifecycle")
    args = ap.parse_args(argv)

    started = time.monotonic()
    res = s26.run(research_root=args.research_root, mom_panel=args.mom_panel,
                  identity_db=args.identity_db, cf_index=args.cf_index,
                  issuer_db=args.issuer_db, shares_index=args.shares_index,
                  price_surface=args.price_surface, fsds_cache=args.fsds_cache,
                  tournament_db=args.tournament_db,
                  evidence_date=args.evidence_date,
                  activate_shadow=not args.no_activate_shadow)
    if not res.get("ok"):
        print("reason: %s" % res.get("reason"))
        print(res.get("token") or s26.BLOCKED)
        return 2

    p = res["payload"]
    s = p["stage26_summary"]
    print("run id            : %s" % res["run_id"])
    print("run dir           : %s" % res["run_dir"])
    print("artifacts         : %d" % len(res["artifacts"]))
    print("formations        : %s (%s -> %s)"
          % (s["panel"]["formations"], s["panel"]["first_month"],
             s["panel"]["last_month"]))
    print("scored rows       : %s" % s["panel"]["scored_rows"])
    print("market-equity cov : %s" % s["panel"]["market_equity_coverage_fraction"])
    print("tier-C sector cov : %s" % s["lane_b_information"][
        "pit_sector_tier_c_coverage"])
    print("tier-C vs tier-B  : %s" % s["lane_b_information"][
        "pit_sector_vs_tier_b_agreement"])
    print("sector effects    : %s" % json.dumps(
        s["lane_b_information"]["sector_verdict_effects"]))
    print("valuation family  : %s" % s["lane_b_information"][
        "valuation_family_size"])
    print("  cleared gate    : %s" % s["lane_b_information"][
        "valuation_cleared_gate"])
    print("  survived FDR    : %s" % s["lane_b_information"][
        "valuation_survived_fdr"])
    print("frozen challenger : %s (spec_hash %s)"
          % (s["lane_a_forward"]["frozen_challenger"],
             s["lane_a_forward"]["frozen_spec_hash"]))
    print("shadow activation : %s at %s"
          % (s["lane_a_forward"]["shadow_activation_status"],
             s["lane_a_forward"]["shadow_inception_date"]))
    print("forward marks     : %s" % s["lane_a_forward"]["forward_marks_written"])
    print("best ensemble     : %s" % s["best_research_ensemble"])
    print("purchase gate     : %s" % s["purchase_gate_verdict"])
    print("next constraint   : %s" % s["next_major_constraint"]["constraint"])
    print("operational muts  : %d   promotion: %s"
          % (s["operational_mutations"], s["model_promotion"]))
    print("elapsed           : %.1fs" % (time.monotonic() - started))
    print(res["token"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
