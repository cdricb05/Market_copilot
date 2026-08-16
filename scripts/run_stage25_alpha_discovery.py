"""Run the Stage-25 autonomous multi-source alpha discovery contract.

Research-only and read-only with respect to every operational store. No orders,
fills, signals, trade decisions, proposals, rebalance plans, Daily Close, model
promotion or champion replacement. No network, no provider call, no PostgreSQL,
no prediction service, no backend restart.

Writes are confined to the Stage-25 research root and, unless ``--no-register``
is passed, to the EXISTING research candidate registry - which is the canonical
research lifecycle, not a Stage-25 store.

Prints exactly ONE terminal token.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent import stage25_alpha_discovery as s25  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--research-root", default=None)
    ap.add_argument("--mom-panel", default=None)
    ap.add_argument("--identity-db", default=None)
    ap.add_argument("--cf-index", default=None)
    ap.add_argument("--issuer-db", default=None)
    ap.add_argument("--tournament-cfg", default=None)
    ap.add_argument("--tournament-db", default=None)
    ap.add_argument("--evidence-date", default=None)
    ap.add_argument("--no-register", action="store_true",
                    help="skip the research candidate registry write")
    ap.add_argument("--print-summary", action="store_true")
    args = ap.parse_args(argv)

    started = time.monotonic()
    res = s25.run(research_root=args.research_root, mom_panel=args.mom_panel,
                  identity_db=args.identity_db, cf_index=args.cf_index,
                  issuer_db=args.issuer_db,
                  tournament_cfg_path=args.tournament_cfg,
                  tournament_db=args.tournament_db,
                  register=not args.no_register,
                  evidence_date=args.evidence_date)
    elapsed = time.monotonic() - started

    if not res.get("ok"):
        print("reason: %s" % res.get("reason"))
        print(res.get("token") or s25.BLOCKED)
        return 2

    summary = res["summary"]
    print("run_id        : %s" % res["run_id"])
    print("run_dir       : %s" % res["run_dir"])
    print("formations    : %s  window %s" % (summary["formations"],
                                             summary["window"]))
    print("median names  : %s" % summary["median_cross_section"])
    print("hypotheses    : %s" % summary["hypotheses_tested"])
    print("gate clearing : %s" % (summary["gate_clearing"] or "none"))
    print("fdr survivors : %s" % (summary["fdr_survivors"] or "none"))
    print("R&D verdict   : %s" % summary["rnd_verdict"])
    print("challengers   : %s" % summary["challenger_headline"])
    print("elapsed       : %.1fs" % elapsed)
    if args.print_summary:
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    print(res["token"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
