"""Release 39 research-shadow forward capture (operator-run, Track H).

RESEARCH SHADOW ONLY - nothing here is operational: no signal authority,
no portfolio, no orders, no promotion, no automation. The three shadows
were frozen by the continuation campaign
(``research_shadow_registry.json``); this script appends TRUE_FORWARD
snapshot rows for newly eligible decision dates and matures outcomes once
their horizon has passed. Historical dates are refused by construction
(eligibility begins strictly after the registry freeze timestamp).

Run it manually (or from the existing research runtime) after each
month-end close; refresh the FRED/VIX/COT acquisitions first if you want
those features live rather than median-imputed:

  & C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      C:\\Users\\binis\\paper_trader\\scripts\\run_r39_shadow_capture.py `
      --mode capture

Modes: status | capture | mature (capture also runs maturation).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent.r39 import research_shadow as RS  # noqa: E402


def log(msg: str) -> None:
    print("[r39shadow %s] %s" % (time.strftime("%H:%M:%S"), msg),
          flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="status",
                    choices=("status", "capture", "mature"))
    args = ap.parse_args()

    registry = RS.load_registry()
    if not registry:
        log("NO_REGISTRY - run the continuation campaign phase H first")
        return 1
    log("registry frozen_at=%s shadows=%d"
        % (registry["frozen_at"], registry["n_shadows"]))

    if args.mode == "status":
        from paper_trader.api import paper_trading_desk as desk
        sdir = RS.shadow_dir()
        snaps = desk._read_ledger(sdir, RS.SNAPSHOT_LEDGER)
        outs = desk._read_ledger(sdir, RS.OUTCOME_LEDGER)
        log("snapshots=%d outcomes=%d" % (len(snaps), len(outs)))
        log("snapshot chain: %s" % json.dumps(
            desk.verify_ledger(sdir, RS.SNAPSHOT_LEDGER)))
        log("outcome chain: %s" % json.dumps(
            desk.verify_ledger(sdir, RS.OUTCOME_LEDGER)))
        return 0

    log("rebuilding CURRENT state through the canonical R38 layer builder "
        "(minutes)")
    fresh = RS.build_fresh_state()
    log("fresh fut rows=%d last=%s"
        % (len(fresh["fut"]),
           fresh["fut"]["decision_date"].max().date()))
    if args.mode in ("capture", "mature"):
        if args.mode == "capture":
            cap = RS.capture(None, fresh_state=fresh)
            log("capture appended=%d chain=%s"
                % (cap["appended"], json.dumps(cap["verify"])))
        mat = RS.mature(fresh_state=fresh)
        log("mature appended=%d" % mat["appended"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
