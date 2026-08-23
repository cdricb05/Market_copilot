"""Release 40 - the ONE prospective research cycle (operator-run).

RESEARCH SHADOW ONLY. AUTOMATION OFF. This script is a thin wrapper over
the canonical callable ``alpha_agent.r40.research_cycle.run_cycle``; it
creates no scheduled task, touches no production store, creates no order,
promotes nothing. Attaching it to the Persistent Daily Research Cycle is a
separate governed operator decision.

One invocation: determines eligible research dates for every registered
shadow (R39 + R40), rebuilds the CURRENT universal state through the
canonical builders, measures input freshness, captures every newly
eligible TRUE_FORWARD decision (contiguous catch-up, lateness recorded),
matures outcomes, updates the always-valid sequential evidence, reports
blocked candidates - and does nothing twice when rerun.

  & C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      C:\\Users\\binis\\paper_trader\\scripts\\run_r40_research_cycle.py `
      --mode capture

Modes: status | capture | mature
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent.r40 import research_cycle as RC  # noqa: E402


def log(msg: str) -> None:
    print("[r40cycle %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="status",
                    choices=("status", "capture", "mature"))
    args = ap.parse_args()
    if args.mode != "status":
        log("rebuilding the CURRENT universal state through the canonical "
            "builders (minutes)")
    body = RC.run_cycle(mode=args.mode)
    log("FORWARD_CAPTURE_STATE = %s" % body["FORWARD_CAPTURE_STATE"])
    if "capture_r39" in body:
        log("captured r39=%d r40=%d; matured r39=%d r40=%d"
            % (body["capture_r39"].get("appended", 0),
               body["capture_r40"].get("appended", 0),
               body["mature_r39"].get("appended", 0),
               body["mature_r40"].get("appended", 0)))
        log("stale sources: %s" % body["input_freshness"].get(
            "stale_sources"))
        for sid, e in body.get("eligibility", {}).items():
            log("  %s: captured=%d remaining=%s next=%s"
                % (sid, e["captured_total"], e["remaining_eligible"],
                   e["next_expected"]))
        if body.get("blocked_candidates"):
            log("blocked: %s" % json.dumps(body["blocked_candidates"])[:800])
    ls = body["ledger_status"]
    log("ledgers: snapshots=%d outcomes=%d chains_intact=%s"
        % (ls["true_forward_snapshots"], ls["true_forward_outcomes"],
           ls["all_chains_intact"]))
    se = body["sequential_evidence"]
    for sid, v in se["per_shadow"].items():
        log("  %s: n=%s e=%s state=%s"
            % (sid, v.get("n_true_forward_outcomes", v.get("n")),
               v.get("e_value"), v.get("decision_state", v.get("state"))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
